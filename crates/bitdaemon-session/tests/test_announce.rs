// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! announce pump — `tracker_announce_due` → `TrackerAnnounce`
//! effect → `on_announce_response` feeds `peer_list`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use bitdaemon_common::flags::PeerSourceFlags;
use bitdaemon_common::info_hash::ProtocolVersion;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::http_tracker::TrackerResponse;
use bitdaemon_net::tracker_client::AnnounceError;
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::torrent::Torrent;

fn ep(a: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, a)), 6881)
}

#[test]
fn announce_cycle() {
    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_tracker(
        "udp://tr/announce".into(),
        0,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    let mut s = Session::new();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert_eq!(out.len(), 1);
    let SessionOut::TrackerAnnounce {
        ih: ai,
        url,
        params,
        ..
    } = &out[0]
    else {
        panic!("announce effect")
    };
    assert_eq!(*ai, ih);
    assert_eq!(url, "udp://tr/announce");
    assert_eq!(params.info_hash, ih.to_bytes());
    // `bytes_left` is `-1` without file metadata → upstream
    // `req.left < 0` sends the 16 KiB magnet fallback.
    assert_eq!(params.left, 16 * 1024);
    // first announce carries `started` (BEP-3 event 2) and the
    // entry is held in-flight — no re-fire while it is out.
    assert_eq!(params.event, 2);
    out.clear();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert!(out.is_empty(), "announce in flight must not re-fire");

    // response: 2 peers + trackerid + 3 seeds
    let resp = TrackerResponse {
        interval: 1800,
        min_interval: -1,
        complete: 3,
        incomplete: 10,
        downloaded: -1,
        downloaders: -1,
        tracker_id: Some(b"tid1".to_vec()),
        failure_reason: None,
        warning_message: None,
        peers: vec![ep(1), ep(2)],
        peer_ids: vec![],
        external_ip: None,
        ..Default::default()
    };
    s.on_announce_response(
        &ih,
        "udp://tr/announce",
        ProtocolVersion::V1,
        Ok(resp),
        None,
        &mut Vec::new(),
    );
    let t = s.torrents.get(&ih).unwrap();
    assert_eq!(t.peer_list.num_peers(), 2);
    // upstream: announce `complete` is scrape population data —
    // `m_num_seeds` counts connected seed peers only (none here).
    assert_eq!(t.complete, 3);
    assert_eq!(t.num_seeds, 0);
    let i = t.trackers.find("udp://tr/announce").unwrap();
    assert_eq!(t.trackers.trackers[i].tracker_id, "tid1");
    assert!(t.trackers.trackers[i].verified);
    // `announce_endpoint::sent` — start_sent latched, next announce
    // scheduled at the tracker interval (no per-tick re-fire), the
    // next announce is a plain interval one (event 0).
    assert!(t.trackers.trackers[i].endpoints[0].v1.start_sent);
    assert!(
        t.trackers.trackers[i].endpoints[0]
            .v1
            .next_announce
            .is_some()
    );
    // TRACKER source on the fed peers
    let pi = t.peer_list.find_peer(ep(1).ip()).unwrap();
    assert!(
        t.peer_list.peers()[pi]
            .source
            .contains(PeerSourceFlags::TRACKER)
    );
    // inside the tracker interval → nothing due; past it → a
    // periodic announce with event `none` (not `started` again).
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert!(out.is_empty());
    let later = Instant::now() + std::time::Duration::from_secs(1801);
    s.tracker_announce_due(later, 0, &mut out);
    assert_eq!(out.len(), 1);
    let SessionOut::TrackerAnnounce { params, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(params.event, 0);
}

/// upstream `unsupported_tracker_protocol_skipped` (e0716cf24):
/// trackers whose scheme we cannot dispatch are never attempted —
/// they stay pristine (`fails == 0`, no error message) while a
/// supported tracker announces. Forcing one by index still dispatches
/// nothing.
#[test]
fn unsupported_tracker_protocol_skipped() {
    use bitdaemon_session::session::ReannounceFlags;
    use bitdaemon_session::tracker_list::SOURCE_CLIENT;

    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    for i in 0..3 {
        t.add_tracker(
            format!("foo://tracker-{i}.example.com/announce"),
            0,
            SOURCE_CLIENT,
        );
    }
    t.add_tracker("http://tracker.com:8080/announce".into(), 1, SOURCE_CLIENT);
    let mut s = Session::new();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);

    // only the http tracker is dispatched — the foo:// entries are
    // never attempted
    assert_eq!(out.len(), 1);
    let SessionOut::TrackerAnnounce { url, .. } = &out[0] else {
        panic!("announce effect")
    };
    assert_eq!(url, "http://tracker.com:8080/announce");

    let t = s.torrents.get(&ih).unwrap();
    for e in &t.trackers.trackers {
        if e.url == "http://tracker.com:8080/announce" {
            continue;
        }
        for ep in &e.endpoints {
            assert_eq!(ep.v1.fails, 0, "{} must stay pristine", e.url);
            assert!(ep.message.is_empty(), "{} must have no error", e.url);
        }
    }

    // forcing a foo:// tracker by index still dispatches nothing
    // (upstream `announce_with_tracker` skip applies to forced picks)
    let foo_idx = t
        .trackers
        .find("foo://tracker-0.example.com/announce")
        .unwrap();
    let mut out = Vec::new();
    s.force_announce(&ih, Some(foo_idx), 0, ReannounceFlags::default(), &mut out);
    assert!(out.is_empty(), "forced foo:// announce must not dispatch");
}

#[test]
fn announce_failure_backs_off() {
    let ih = Sha1Hash::from_bytes(&[6; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_tracker(
        "http://tr".into(),
        0,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    let mut s = Session::new();
    s.add_torrent(t);
    s.on_announce_response(
        &ih,
        "http://tr",
        ProtocolVersion::V1,
        Err(AnnounceError::TimedOut),
        None,
        &mut Vec::new(),
    );
    // just failed → not due
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert!(out.is_empty());
}
