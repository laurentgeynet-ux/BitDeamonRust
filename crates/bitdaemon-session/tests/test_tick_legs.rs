// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `session_impl::on_tick` legs — the session-time u16 wrap
//! step-back, the auto-scrape scaler for paused auto-managed
//! torrents, and the `peer_turnover` disconnect sweep.

use bitdaemon_common::flags::PeerSourceFlags;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::torrent::Torrent;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn torrent(byte: u8) -> Torrent {
    Torrent::new(
        Sha1Hash::from_bytes(&[byte; 20]).unwrap(),
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    )
}

fn peer_ep(b: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, 9, b)), 6881)
}

#[test]
fn session_time_wrap_steps_peer_stamps() {
    // upstream: `session_time()` is a u16 epoch; past 65000 s the
    // base steps forward 4 h and every `last_connected` /
    // `last_optimistically_unchoked` clamps back the same amount.
    let mut s = Session::new();
    let mut t = torrent(1);
    t.add_swarm_peer(peer_ep(1), PeerSourceFlags::TRACKER, &mut Vec::new());
    let ih = s.add_torrent(t);
    // stamp the peer as connected "now".
    let pi = s.torrents[&ih]
        .peer_list
        .find_peer(peer_ep(1).ip())
        .unwrap();
    s.torrents.get_mut(&ih).unwrap().peer_list.peers_mut()[pi].last_connected = 65000;
    s.torrents.get_mut(&ih).unwrap().peer_list.peers_mut()[pi].last_optimistically_unchoked = 60000;
    s.session_time = 65001;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert_eq!(s.session_time, 65001 - 4 * 60 * 60 + 1);
    let t = &s.torrents[&ih];
    let p = &t.peer_list.peers()[pi];
    assert_eq!(p.last_connected, 65000 - 4 * 60 * 60);
    assert_eq!(p.last_optimistically_unchoked, 60000 - 4 * 60 * 60);
}

#[test]
fn auto_scrape_scales_and_round_robins() {
    // paused + auto_managed torrents get scraped once per
    // `auto_scrape_interval / want_scrape` s, floored at
    // `auto_scrape_min_interval` — one `TrackerScrape` per pass.
    let mut s = Session::new();
    s.apply_settings(SessionSettings {
        torrent_connect_boost: 0,
        // interval/N = 1 s per pass — every tick fires one scrape.
        auto_scrape_interval: std::time::Duration::from_secs(2),
        auto_scrape_min_interval: std::time::Duration::from_secs(1),
        // auto-manage would re-activate the paused torrents (freeing
        // them out of `want_scrape`) — pin the active limit so they
        // stay paused like upstream queue-starved torrents.
        active_limit: 0,
        ..Default::default()
    });
    for b in 1..=2u8 {
        let mut t = torrent(b);
        t.add_tracker(
            format!("http://tr{b}/announce"),
            0,
            bitdaemon_session::tracker_list::SOURCE_CLIENT,
        );
        t.paused = true;
        t.auto_managed = true;
        s.add_torrent(t);
    }
    // scaler inits at 180 upstream — run the ticks.
    let mut scrapes = Vec::new();
    for _ in 0..180 {
        let mut out = Vec::new();
        s.tick(&mut out);
        scrapes.extend(out.iter().filter_map(|o| match o {
            SessionOut::TrackerScrape { url, .. } => Some(url.clone()),
            _ => None,
        }));
    }
    assert_eq!(scrapes.len(), 1, "one scrape fires when the scaler trips");
    // re-armed to interval/2 = 1 s — every tick fires one now, and
    // the round-robin cursor hits the *other* torrent next.
    for _ in 0..4 {
        let mut out = Vec::new();
        s.tick(&mut out);
        scrapes.extend(out.iter().filter_map(|o| match o {
            SessionOut::TrackerScrape { url, .. } => Some(url.clone()),
            _ => None,
        }));
    }
    assert_eq!(scrapes.len(), 5);
    assert_ne!(scrapes[0], scrapes[1], "round-robin scrapes both");
}

#[test]
fn peer_turnover_disconnects_worst_at_cap() {
    // `connections_limit` reached past `peer_turnover_cutoff`% →
    // every `peer_turnover_interval` the largest swarm sheds
    // `peer_turnover`% of its peers (`optimistic_disconnect`).
    let mut s = Session::new();
    s.apply_settings(SessionSettings {
        torrent_connect_boost: 0,
        connections_limit: 6,
        peer_turnover_cutoff: 90,
        peer_turnover: 50,
        peer_turnover_interval: std::time::Duration::from_secs(1),
        ..Default::default()
    });
    let mut t = torrent(1);
    // upstream sizes the sweep off `num_peers()` — the *peer-list*
    // size, not the live-conn count; the last four stay candidates.
    for b in 1..=10u8 {
        t.add_swarm_peer(peer_ep(b), PeerSourceFlags::TRACKER, &mut Vec::new());
    }
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    for pid in 1..=6u64 {
        s.peer_connected(
            &ih,
            pid,
            PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
            peer_ep(pid as u8),
            &mut out,
        );
    }
    assert_eq!(s.torrents[&ih].peers.len(), 6);
    // scaler inits at 90 upstream — run the ticks.
    let mut disconnects = 0;
    for _ in 0..90 {
        let mut out = Vec::new();
        s.tick(&mut out);
        disconnects += out
            .iter()
            .filter(|o| {
                matches!(
                    o,
                    SessionOut::Torrent(
                        bitdaemon_session::torrent::TorrentEffect::DisconnectPeer { .. },
                    )
                )
            })
            .count();
    }
    // 6 conns ≥ 6*90% of the global limit → the largest swarm sheds
    // min(10*50%, num_connect_candidates=4) worst peers.
    assert_eq!(disconnects, 4);
}
