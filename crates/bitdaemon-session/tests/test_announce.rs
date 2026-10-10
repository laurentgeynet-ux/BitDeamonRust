// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! announce pump — `tracker_announce_due` → `TrackerAnnounce`
//! effect → `on_announce_response` feeds `peer_list`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use bitdaemon_common::flags::PeerSourceFlags;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::http_tracker::TrackerResponse;
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::torrent::Torrent;

fn ep(a: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, a)), 6881)
}

#[test]
fn announce_cycle() {
    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_tracker("udp://tr/announce".into(), 0);
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
    assert_eq!(params.left, 4 * 128 * 1024);

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
    };
    s.on_announce_response(&ih, "udp://tr/announce", Ok(resp));
    let t = s.torrents.get(&ih).unwrap();
    assert_eq!(t.peer_list.num_peers(), 2);
    assert_eq!(t.num_seeds, 3);
    let i = t.trackers.find("udp://tr/announce").unwrap();
    assert_eq!(t.trackers.trackers[i].tracker_id, "tid1");
    assert!(t.trackers.trackers[i].verified);
    // TRACKER source on the fed peers
    let pi = t.peer_list.find_peer(ep(1).ip()).unwrap();
    assert!(
        t.peer_list.peers()[pi]
            .source
            .contains(PeerSourceFlags::TRACKER)
    );
}

#[test]
fn announce_failure_backs_off() {
    let ih = Sha1Hash::from_bytes(&[6; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_tracker("http://tr".into(), 0);
    let mut s = Session::new();
    s.add_torrent(t);
    s.on_announce_response(&ih, "http://tr", Err("timeout".into()));
    // just failed → not due
    let mut out = Vec::new();
    s.tracker_announce_due(Instant::now(), 0, &mut out);
    assert!(out.is_empty());
}
