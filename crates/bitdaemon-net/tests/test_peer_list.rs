// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `peer_list` — add/candidates/weeding/ban (`test_peer_list`-style).

use bitdaemon_common::PeerSourceFlags;
use bitdaemon_net::peer_list::{PeerList, PeerListState, primary_source, source_rank};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn ep(a: u8, port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, a)), port)
}

#[test]
fn add_and_source() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, new) = l.add_peer(ep(1, 6881), PeerSourceFlags::TRACKER, true);
    assert!(new);
    let (j, new2) = l.add_peer(ep(1, 6881), PeerSourceFlags::PEX, true);
    assert!(!new2);
    assert_eq!(i, j);
    assert_eq!(
        l.peers()[i].source,
        PeerSourceFlags::TRACKER | PeerSourceFlags::PEX
    );
    assert_eq!(
        primary_source(l.peers()[i].source),
        PeerSourceFlags::TRACKER
    );
    assert_eq!(source_rank(l.peers()[i].source), (1 << 5) | (1 << 2));
    assert_eq!(l.num_connect_candidates(), 1);
}

#[test]
fn candidates_and_failcount() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    for n in 1..=5 {
        l.add_peer(ep(n, 6881), PeerSourceFlags::TRACKER, true);
    }
    assert_eq!(l.num_connect_candidates(), 5);
    // three strikes → no longer a candidate
    let i = l.find_peer(ep(1, 6881).ip()).unwrap();
    l.inc_failcount(i);
    l.inc_failcount(i);
    l.inc_failcount(i);
    assert_eq!(l.num_connect_candidates(), 4);
    let cands = l.find_connect_candidates(1000, &st);
    assert_eq!(cands.len(), 4);
    assert!(!cands.contains(&i));
    // connected peer isn't a candidate
    l.set_connected(l.find_peer(ep(2, 6881).ip()).unwrap());
    assert_eq!(l.num_connect_candidates(), 3);
    l.connection_closed(l.find_peer(ep(2, 6881).ip()).unwrap(), 1000);
    // just connected → still skipped by min_reconnect_time
    assert!(
        !l.find_connect_candidates(1000, &st)
            .contains(&l.find_peer(ep(2, 6881).ip()).unwrap())
    );
}

#[test]
fn ranking_prefers_low_failcount_local() {
    let st = PeerListState {
        min_reconnect_time: 0,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    // local peer + a failed remote + a good remote
    l.add_peer(ep(9, 6881), PeerSourceFlags::TRACKER, true); // 10.0.0.9 → "local" per is_local
    l.add_peer(
        SocketAddr::new("8.8.8.8".parse().unwrap(), 6881),
        PeerSourceFlags::TRACKER,
        true,
    );
    l.add_peer(
        SocketAddr::new("1.1.1.1".parse().unwrap(), 6881),
        PeerSourceFlags::TRACKER,
        true,
    );
    let bad = l.find_peer("8.8.8.8".parse().unwrap()).unwrap();
    l.inc_failcount(bad);
    let cands = l.find_connect_candidates(0, &st);
    assert_eq!(cands.len(), 3);
    // lowest failcount first; tie → local first
    assert_eq!(l.peers()[cands[0]].addr.address(), Some(ep(9, 6881).ip()));
    assert_eq!(
        l.peers()[cands[1]].addr.address(),
        Some("1.1.1.1".parse().unwrap())
    );
    assert_eq!(
        l.peers()[cands[2]].addr.address(),
        Some("8.8.8.8".parse().unwrap())
    );
}

#[test]
fn weeding_over_capacity() {
    let st = PeerListState {
        max_peerlist_size: 10,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    for n in 1..=12 {
        l.add_peer(ep(n, 6881), PeerSourceFlags::TRACKER, true);
    }
    // fail one out — over 95% cap it becomes an erase candidate
    let i = l.find_peer(ep(1, 6881).ip()).unwrap();
    l.inc_failcount(i);
    l.inc_failcount(i);
    l.inc_failcount(i);
    l.find_connect_candidates(0, &st);
    assert!(l.num_peers() < 12);
    assert!(l.find_peer(ep(1, 6881).ip()).is_none());
}

#[test]
fn ban_and_filter() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    l.add_peer(ep(1, 6881), PeerSourceFlags::TRACKER, true);
    l.add_peer(ep(2, 6881), PeerSourceFlags::TRACKER, true);
    let i = l.find_peer(ep(1, 6881).ip()).unwrap();
    l.ban_peer(i);
    assert_eq!(l.num_connect_candidates(), 1);
    let banned = l.apply_ip_filter(|a| a == ep(2, 6881).ip());
    assert_eq!(banned, vec![ep(2, 6881).ip()]);
    assert_eq!(l.num_peers(), 1);
}

#[test]
fn update_port() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, _) = l.add_peer(ep(1, 0), PeerSourceFlags::DHT, false);
    assert_eq!(l.num_connect_candidates(), 0);
    assert!(l.update_peer_port(6881, i, PeerSourceFlags::DHT, &st));
    assert_eq!(l.peers()[i].port, 6881);
    assert!(l.peers()[i].connectable);
    assert_eq!(l.num_connect_candidates(), 1);
}
