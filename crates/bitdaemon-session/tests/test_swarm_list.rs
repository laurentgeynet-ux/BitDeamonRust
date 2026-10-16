// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `PeerList` × `Torrent` — swarm feed + connect pump.

use bitdaemon_common::flags::PeerSourceFlags;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_list::PeerListState;
use bitdaemon_session::torrent::Torrent;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn t() -> Torrent {
    Torrent::new(
        Sha1Hash::from_bytes(&[3; 20]).unwrap(),
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    )
}

fn ep(a: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, a)), 6881)
}

#[test]
fn tracker_feed_gives_candidates() {
    let mut t = t();
    for n in 1..=4 {
        t.add_swarm_peer(ep(n), PeerSourceFlags::TRACKER, &mut Vec::new());
    }
    let st = PeerListState::default();
    // `connect_one_peer` pops one candidate per dial — drain the
    // cache like upstream's boost loop.
    let mut cands = Vec::new();
    while let Some(c) = t.try_connect_peer(0, &st, &[]).0.into_dial() {
        cands.push(c);
    }
    assert_eq!(cands.len(), 4);
    assert!(cands.iter().any(|c| c.0 == ep(1)));
}

#[test]
fn seed_still_wants_peers_for_upload() {
    let mut t = t();
    t.finished = true;
    // `want_peers` needs a dialable candidate — give the swarm one.
    t.add_swarm_peer(ep(1), PeerSourceFlags::TRACKER, &mut Vec::new());
    // upstream `want_peers_finished`: seeds keep dialing (to serve
    // upload and feed ut_pex) unless seeding_outgoing_connections off
    let on = bitdaemon_session::settings::SessionSettings {
        seeding_outgoing_connections: true,
        ..Default::default()
    };
    assert!(t.want_peers(&on));
    let off = bitdaemon_session::settings::SessionSettings {
        seeding_outgoing_connections: false,
        ..on
    };
    assert!(!t.want_peers(&off));
}

#[test]
fn seed_skips_seed_candidates() {
    let mut t = t();
    t.add_swarm_peer(ep(1), PeerSourceFlags::TRACKER, &mut Vec::new());
    // remote is a seed, and we finished too → not a candidate
    t.finished = true;
    let i = t.peer_list.find_peer(ep(1).ip()).unwrap();
    t.peer_list.set_seed(i, true);
    let st = PeerListState::default();
    assert!(t.try_connect_peer(0, &st, &[]).0.into_dial().is_none());
    // back to downloading → candidate again
    t.finished = false;
    assert!(t.try_connect_peer(0, &st, &[]).0.into_dial().is_some());
}
