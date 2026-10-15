// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `session_impl::try_connect_more_peers` — per-tick dial pacing:
//! `connection_speed` budget, boost deductions, `smooth_connects`,
//! prio queue, downloading/finished round-robin, and the torrent
//! `do_connect_boost` / candidate-cache machinery underneath.

use bitdaemon_common::flags::PeerSourceFlags;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::pe_crypto::EncPolicy;
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::torrent::Torrent;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn ep(a: u8) -> SocketAddr {
    // public-range IPs — local addresses short-circuit peer-class
    // derivation in unrelated ways upstream.
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, 0, a)), 6881)
}

fn torrent(ih_byte: u8, peers: u8) -> Torrent {
    let mut t = Torrent::new(
        Sha1Hash::from_bytes(&[ih_byte; 20]).unwrap(),
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    );
    for n in 1..=peers {
        t.add_swarm_peer(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, ih_byte, n)), 6881),
            PeerSourceFlags::TRACKER,
            &mut Vec::new(),
        );
    }
    t
}

fn session(s: SessionSettings) -> Session {
    let mut ses = Session::new();
    ses.apply_settings(s);
    ses
}

/// boost off (harness control), generous speed + limits.
fn harness_settings() -> SessionSettings {
    SessionSettings {
        torrent_connect_boost: 0,
        connection_speed: 30,
        connections_limit: 500,
        ..Default::default()
    }
}

#[test]
fn pump_capped_by_connection_speed() {
    let mut s = session(SessionSettings {
        connection_speed: 3,
        ..harness_settings()
    });
    let ih = s.add_torrent(torrent(1, 8));
    let dials = s.try_connect_more_peers(0, EncPolicy::Enabled, None);
    assert_eq!(dials.len(), 3);
    assert!(dials.iter().all(|(i, ..)| *i == ih));
    // a second pass keeps handing out (candidates left).
    assert_eq!(
        s.try_connect_more_peers(1, EncPolicy::Enabled, None).len(),
        3
    );
}

#[test]
fn boost_dials_deduct_from_speed() {
    let mut s = session(SessionSettings {
        connection_speed: 10,
        ..harness_settings()
    });
    let ih = s.add_torrent(torrent(1, 12));
    // simulate four boost dials handed out mid-tick (tracker
    // response path).
    for _ in 0..4 {
        s.inc_boost_connections();
    }
    let dials = s.try_connect_more_peers(0, EncPolicy::Enabled, None);
    assert_eq!(dials.len(), 6);
    assert!(dials.iter().all(|(i, ..)| *i == ih));
    // boost consumed — next pass is the full budget again.
    assert_eq!(
        s.try_connect_more_peers(1, EncPolicy::Enabled, None).len(),
        6
    );
}

#[test]
fn smooth_connects_halves_free_slot_burst() {
    // upstream `smooth_connects`: when the per-tick speed would
    // spend more than half the free conn slots, it clamps.
    let mut s = session(SessionSettings {
        connection_speed: 30,
        connections_limit: 20,
        smooth_connects: true,
        ..harness_settings()
    });
    s.add_torrent(torrent(1, 30));
    // free slots = 20 → cap = (20+1)/2 = 10 < 30 → 10 dials.
    let dials = s.try_connect_more_peers(0, EncPolicy::Enabled, None);
    assert_eq!(dials.len(), 10);
}

#[test]
fn finished_torrents_share_via_ratio() {
    // `connect_seed_every_n_download` = 1 → finished torrents take
    // every other hand-out.
    let mut s = session(SessionSettings {
        connection_speed: 4,
        connect_seed_every_n_download: 1,
        ..harness_settings()
    });
    let dl = s.add_torrent(torrent(1, 6));
    let mut fin = torrent(2, 6);
    fin.finished = true;
    let fin = s.add_torrent(fin);
    let dials = s.try_connect_more_peers(0, EncPolicy::Enabled, None);
    assert_eq!(dials.len(), 4);
    let by_dl = dials.iter().filter(|(i, ..)| *i == dl).count();
    let by_fin = dials.iter().filter(|(i, ..)| *i == fin).count();
    assert_eq!((by_dl, by_fin), (2, 2));
}

#[test]
fn prioritized_torrent_dials_first() {
    let mut s = session(SessionSettings {
        connection_speed: 3,
        connect_seed_every_n_download: 1,
        ..harness_settings()
    });
    let a = s.add_torrent(torrent(1, 6));
    let b = s.add_torrent(torrent(2, 6));
    // upstream `prioritize_connections` — tracker-response torrents
    // get 10 tickets ahead of the round-robin.
    s.prioritize_connections(b);
    let dials = s.try_connect_more_peers(0, EncPolicy::Enabled, None);
    assert!(dials.iter().all(|(i, ..)| *i == b));
    let _ = a;
}

#[test]
fn connections_limit_blocks_the_pump() {
    let mut s = session(SessionSettings {
        connections_limit: 0,
        ..harness_settings()
    });
    s.add_torrent(torrent(1, 4));
    assert!(
        s.try_connect_more_peers(0, EncPolicy::Enabled, None)
            .is_empty()
    );
}

#[test]
fn connect_boost_fires_on_tracker_response() {
    // `torrent_connect_boost` armed at add; the torrent starts
    // peerless so the start-boost early-returns without spending
    // the counter — the announce response's peers then trigger
    // `min(boost, free_slots)` immediate `ConnectPeer` dials
    // (upstream `torrent::tracker_response` → `do_connect_boost`).
    let mut s = session(SessionSettings {
        torrent_connect_boost: 4,
        connection_speed: 2,
        ..harness_settings()
    });
    let ih = s.add_torrent(torrent(1, 0));
    let mut out = Vec::new();
    s.on_announce_response(
        &ih,
        "http://t/announce",
        bitdaemon_common::info_hash::ProtocolVersion::V1,
        Ok(bitdaemon_net::http_tracker::TrackerResponse {
            interval: 1800,
            min_interval: -1,
            complete: -1,
            incomplete: -1,
            downloaded: -1,
            downloaders: -1,
            tracker_id: None,
            failure_reason: None,
            warning_message: None,
            peers: (1..=10u8).map(ep).collect(),
            peer_ids: vec![],
            external_ip: None,
            i2p_peers: vec![],
            hostname_peers: vec![],
        }),
        None,
        &mut out,
    );
    let dials = out
        .iter()
        .filter(|o| matches!(o, SessionOut::ConnectPeer { .. }))
        .count();
    assert_eq!(dials, 4);
    // boost consumed — a second response is a no-op (counter is
    // re-armed by resume, not per announce).
    out.clear();
    s.on_announce_response(
        &ih,
        "http://t/announce",
        bitdaemon_common::info_hash::ProtocolVersion::V1,
        Ok(bitdaemon_net::http_tracker::TrackerResponse {
            interval: 1800,
            min_interval: -1,
            complete: -1,
            incomplete: -1,
            downloaded: -1,
            downloaders: -1,
            tracker_id: None,
            failure_reason: None,
            warning_message: None,
            peers: vec![],
            peer_ids: vec![],
            external_ip: None,
            i2p_peers: vec![],
            hostname_peers: vec![],
        }),
        None,
        &mut out,
    );
    assert!(
        !out.iter()
            .any(|o| matches!(o, SessionOut::ConnectPeer { .. }))
    );
    // and `boost_connections` was credited for the first burst —
    // the next pump pass deducts 4 from `connection_speed` = 2 → 0.
    assert!(
        s.try_connect_more_peers(10, EncPolicy::Enabled, None)
            .is_empty()
    );
}

#[test]
fn utp_flags_follow_dial_outcome() {
    // upstream `peer_connection`: uTP connect success →
    // `confirmed_supports_utp`, `supports_utp` cleared; uTP connect
    // failure → `supports_utp` cleared + fast reconnect (no
    // `last_connected` restamp → retry is not cooled out).
    let mut s = session(harness_settings());
    let ih = s.add_torrent(torrent(1, 2));
    let st = s.peer_list_state();
    // drain one candidate manually so we control the outcome.
    let t = s.torrents.get_mut(&ih).unwrap();
    let (ep1, ..) = t.try_connect_peer(0, &st, EncPolicy::Enabled, &[]).unwrap();
    // uTP failure → supports_utp cleared, no cooldown: upstream
    // `connect_failed` re-dials the SAME entry over TCP right away
    // (`connect_to_peer(pi, ignore_limit)`) — the retry rides
    // `pending_out`, bypassing the candidate scan.
    s.peer_connect_failed(&ih, ep1, true, false);
    let i = s.torrents[&ih].peer_list.find_peer(ep1.ip()).unwrap();
    assert!(!s.torrents[&ih].peer_list.peers()[i].supports_utp);
    assert!(s.pending_out.iter().any(|o| matches!(
        o,
        SessionOut::ConnectPeer { ep, .. } if *ep == ep1
    )));
    // the other candidate still dials normally through the pump.
    let t = s.torrents.get_mut(&ih).unwrap();
    let (ep2, ..) = t.try_connect_peer(1, &st, EncPolicy::Enabled, &[]).unwrap();
    let t = s.torrents.get_mut(&ih).unwrap();
    t.peer_connected_outgoing(ep2, false, 1, 1, false, true, None);
    let i = t.peer_list.find_peer(ep2.ip()).unwrap();
    let p = &t.peer_list.peers()[i];
    assert!(p.confirmed_supports_utp);
    assert!(!p.supports_utp);
}
