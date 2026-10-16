// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `peer_list` — add/candidates/weeding/ban (`test_peer_list`-style).

use bitdaemon_common::{PeerSourceFlags, PexFlags};
use bitdaemon_net::peer_list::{PeerList, PeerListState, source_rank};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn ep(a: u8, port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, a)), port)
}

fn add(l: &mut PeerList, e: SocketAddr, src: PeerSourceFlags) -> Option<(usize, bool)> {
    l.add_peer(e, src, PexFlags::NONE, &PeerListState::default())
}

#[test]
fn add_and_source() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, new) = l
        .add_peer(ep(1, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st)
        .unwrap();
    assert!(new);
    let (j, new2) = l
        .add_peer(ep(1, 6881), PeerSourceFlags::PEX, PexFlags::NONE, &st)
        .unwrap();
    assert!(!new2);
    assert_eq!(i, j);
    assert_eq!(
        l.peers()[i].source,
        PeerSourceFlags::TRACKER | PeerSourceFlags::PEX
    );
    assert_eq!(source_rank(l.peers()[i].source), (1 << 5) | (1 << 2));
    assert_eq!(l.num_connect_candidates(), 1);
}

#[test]
fn add_peer_rejects_invalid_endpoints() {
    // upstream `add_peer`: unspecified addr, port 0 and port 1 are
    // dropped before insertion; v6 link-locals likewise.
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    assert!(
        l.add_peer(ep(1, 0), PeerSourceFlags::TRACKER, PexFlags::NONE, &st)
            .is_none()
    );
    assert!(
        l.add_peer(ep(1, 1), PeerSourceFlags::TRACKER, PexFlags::NONE, &st)
            .is_none()
    );
    assert!(
        l.add_peer(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 6881),
            PeerSourceFlags::TRACKER,
            PexFlags::NONE,
            &st,
        )
        .is_none()
    );
    assert!(
        l.add_peer(
            SocketAddr::new("fe80::1".parse().unwrap(), 6881),
            PeerSourceFlags::TRACKER,
            PexFlags::NONE,
            &st,
        )
        .is_none()
    );
    assert_eq!(l.num_peers(), 0);
}

#[test]
fn add_peer_sorted_insert_and_multi_ip() {
    // upstream keeps `m_peers` sorted by address (`peer_address_compare`)
    // and dedups by (addr, port) when multiple connections per IP are
    // allowed.
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    add(&mut l, ep(9, 6881), PeerSourceFlags::TRACKER);
    add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER);
    // indices shift on sorted insert — assert on slot order instead
    assert_eq!(l.peers()[0].addr.address(), Some(ep(1, 0).ip()));
    assert_eq!(l.peers()[1].addr.address(), Some(ep(9, 0).ip()));

    let st_multi = PeerListState {
        allow_multiple_connections_per_ip: true,
        ..Default::default()
    };
    let mut m = PeerList::new(&st_multi);
    let (_a, new_a) = m
        .add_peer(
            ep(1, 6881),
            PeerSourceFlags::TRACKER,
            PexFlags::NONE,
            &st_multi,
        )
        .unwrap();
    // same addr, different port → a second entry upstream (indices
    // aren't stable under sorted insert — count the group instead)
    let (_b, new_b) = m
        .add_peer(
            ep(1, 6882),
            PeerSourceFlags::TRACKER,
            PexFlags::NONE,
            &st_multi,
        )
        .unwrap();
    assert!(new_a && new_b);
    assert_eq!(m.num_peers(), 2);
    // same endpoint again → dedup onto the existing entry
    let (_c, new_c) = m
        .add_peer(ep(1, 6881), PeerSourceFlags::PEX, PexFlags::NONE, &st_multi)
        .unwrap();
    assert!(!new_c);
    assert_eq!(m.num_peers(), 2);
}

#[test]
fn update_peer_tracker_decrements_failcount() {
    // upstream `update_peer`: a tracker re-announce gives one
    // failure credit back.
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.inc_failcount(i);
    l.inc_failcount(i);
    assert_eq!(l.peers()[i].failcount, 2);
    let (j, new) = l
        .add_peer(ep(1, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st)
        .unwrap();
    assert!(!new && j == i);
    assert_eq!(l.peers()[i].failcount, 1);
    // …but a non-tracker source does not refund failures
    let _ = l
        .add_peer(ep(1, 6881), PeerSourceFlags::PEX, PexFlags::NONE, &st)
        .unwrap();
    assert_eq!(l.peers()[i].failcount, 1);
}

#[test]
fn candidates_and_failcount() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    for n in 1..=5 {
        add(&mut l, ep(n, 6881), PeerSourceFlags::TRACKER);
    }
    assert_eq!(l.num_connect_candidates(), 5);
    // three strikes → no longer a candidate
    let i = l.find_peer(ep(1, 6881).ip()).unwrap();
    l.inc_failcount(i);
    l.inc_failcount(i);
    l.inc_failcount(i);
    assert_eq!(l.num_connect_candidates(), 4);
    let cands = l.find_connect_candidates(1000, &st).0;
    assert_eq!(cands.len(), 4);
    // candidate keys are `(PeerAddr, port)` — stable under erasure
    // unlike the Vec indices upstream uses pointers for.
    let key_of = |l: &PeerList, i: usize| (l.peers()[i].addr.clone(), l.peers()[i].port);
    assert!(!cands.contains(&key_of(&l, i)));
    // connected peer isn't a candidate
    l.set_connected(l.find_peer(ep(2, 6881).ip()).unwrap(), 900, false, 7);
    assert_eq!(l.num_connect_candidates(), 3);
    l.connection_closed(
        l.find_peer(ep(2, 6881).ip()).unwrap(),
        1000,
        false,
        false,
        &st,
    );
    // just disconnected → still skipped by min_reconnect_time
    let j = l.find_peer(ep(2, 6881).ip()).unwrap();
    assert!(
        !l.find_connect_candidates(1000, &st)
            .0
            .contains(&key_of(&l, j))
    );
}

#[test]
fn connection_closed_fast_reconnect_keeps_stamp() {
    // upstream: `fast_reconnect` connections keep `last_connected`
    // at the initiate-time value.
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.set_connected(i, 100, true, 7);
    assert_eq!(l.peers()[i].last_connected, 0);
    l.connection_closed(i, 500, true, false, &st);
    assert_eq!(l.peers()[i].last_connected, 0);
    l.set_connected(i, 600, false, 7);
    assert_eq!(l.peers()[i].last_connected, 600);
}

#[test]
fn connection_closed_multi_conn_evicts_anonymous() {
    // upstream `connection_closed`: allow_multiple_connections_per_ip
    // + never-connectable incoming entry → erased on disconnect.
    let st = PeerListState {
        allow_multiple_connections_per_ip: true,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    let i = l.add_incoming(ep(1, 40000), &st).unwrap();
    assert!(!l.peers()[i].connectable);
    l.set_connected(i, 10, false, 7);
    l.connection_closed(i, 20, false, false, &st);
    assert_eq!(l.num_peers(), 0);
}

#[test]
fn set_connected_clears_maybe_upload_only() {
    // upstream `set_connection`: a live conn supersedes the pex_seed hint.
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, _) = l
        .add_peer(ep(1, 6881), PeerSourceFlags::PEX, PexFlags::SEED, &st)
        .unwrap();
    assert!(l.peers()[i].maybe_upload_only);
    l.set_connected(i, 0, false, 7);
    assert!(!l.peers()[i].maybe_upload_only);
}

#[test]
fn ranking_prefers_low_failcount_local() {
    let st = PeerListState {
        min_reconnect_time: 0,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    // local peer + a failed remote + a good remote
    add(&mut l, ep(9, 6881), PeerSourceFlags::TRACKER); // 10.0.0.9 → "local" per is_local
    add(
        &mut l,
        SocketAddr::new("8.8.8.8".parse().unwrap(), 6881),
        PeerSourceFlags::TRACKER,
    );
    add(
        &mut l,
        SocketAddr::new("1.1.1.1".parse().unwrap(), 6881),
        PeerSourceFlags::TRACKER,
    );
    let bad = l.find_peer("8.8.8.8".parse().unwrap()).unwrap();
    l.inc_failcount(bad);
    let cands = l.find_connect_candidates(0, &st).0;
    assert_eq!(cands.len(), 3);
    // lowest failcount first; tie → local first
    assert_eq!(cands[0].0.address(), Some(ep(9, 6881).ip()));
    assert_eq!(cands[1].0.address(), Some("1.1.1.1".parse().unwrap()));
    assert_eq!(cands[2].0.address(), Some("8.8.8.8".parse().unwrap()));
}

#[test]
fn ranking_uses_full_source_mask() {
    // upstream `compare_peer` ranks `source_rank(peer_source())` —
    // the *whole* bitmask accumulates (tracker 1<<5, lsd 1<<4,
    // dht 1<<3, pex 1<<2). An LSD peer also seen on PEX (16+4=20)
    // outranks an LSD-only one (16); ranking only a primary bit
    // would order them the other way.
    let st = PeerListState {
        min_reconnect_time: 0,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    // multi-source: LSD first, PEX merges its bit on the re-announce.
    let (m, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::LSD).unwrap();
    add(&mut l, ep(1, 6881), PeerSourceFlags::PEX);
    assert_eq!(
        l.peers()[m].source,
        PeerSourceFlags::LSD | PeerSourceFlags::PEX
    );
    // single-source LSD.
    let (s, _) = add(&mut l, ep(2, 6881), PeerSourceFlags::LSD).unwrap();
    let cands = l.find_connect_candidates(0, &st).0;
    let pos = |i: usize| {
        cands
            .iter()
            .position(|c| c.0 == l.peers()[i].addr && c.1 == l.peers()[i].port)
            .unwrap()
    };
    assert!(
        pos(m) < pos(s),
        "LSD|PEX (rank 20) must outrank LSD-only (rank 16)"
    );
}

#[test]
fn resume_data_checks_use_exact_mask() {
    // upstream `peer_source() == resume_data` is an *exact-mask*
    // test: an entry carrying resume_data plus another bit is not
    // resume-data-sourced — it survives both the capacity reject and
    // the `should_erase_immediately` sweep.
    let st = PeerListState {
        max_peerlist_size: 2,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    // mixed-source entry: resume_data merged with incoming — the
    // discriminating case (a primary-source projection still
    // reports resume_data for RESUME_DATA|INCOMING).
    let add_st = |l: &mut PeerList, e: SocketAddr, src: PeerSourceFlags| {
        l.add_peer(e, src, PexFlags::NONE, &st)
    };
    add_st(&mut l, ep(1, 6881), PeerSourceFlags::RESUME_DATA);
    add_st(&mut l, ep(1, 6881), PeerSourceFlags::INCOMING);
    assert_eq!(
        l.peers()[l.find_peer(ep(1, 6881).ip()).unwrap()].source,
        PeerSourceFlags::RESUME_DATA | PeerSourceFlags::INCOMING
    );
    // resume-data-only filler reaches the capacity.
    add_st(&mut l, ep(2, 6881), PeerSourceFlags::RESUME_DATA);
    // neither entry is dialable → not a connect candidate, so the
    // sweep consults the source mask at all.
    for i in 0..l.peers().len() {
        l.peers_mut()[i].connectable = false;
    }
    // a resume-data-only *insert* is rejected at capacity
    // (`insert_peer` returns false upstream).
    assert!(add_st(&mut l, ep(3, 6881), PeerSourceFlags::RESUME_DATA).is_none());
    // a normal insert weeds the resume-data-only entry instead.
    assert!(add_st(&mut l, ep(4, 6881), PeerSourceFlags::TRACKER).is_some());
    assert!(l.find_peer(ep(1, 6881).ip()).is_some());
    assert!(l.find_peer(ep(2, 6881).ip()).is_none());
}

#[test]
fn weeding_over_capacity() {
    let st = PeerListState {
        max_peerlist_size: 10,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    for n in 1..=12 {
        l.add_peer(ep(n, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st);
    }
    // fail one out — over the cap it becomes an erase candidate
    let i = l.find_peer(ep(1, 6881).ip()).unwrap();
    l.inc_failcount(i);
    l.inc_failcount(i);
    l.inc_failcount(i);
    let _ = l.find_connect_candidates(0, &st);
    assert!(l.num_peers() < 12);
    assert!(l.find_peer(ep(1, 6881).ip()).is_none());
}

#[test]
fn erase_prefers_worse_peers() {
    // `compare_peer_erase` upstream order: higher failcount first,
    // then resume-data-only, then non-connectable, then low trust.
    // A sweep only runs above the 95% low watermark.
    let st = PeerListState {
        max_peerlist_size: 5,
        ..Default::default()
    };
    let mut m = PeerList::new(&st);
    // five failed peers (past max_failcount → not connect candidates
    // → erase candidates); `a` has the lowest trust_points and must
    // be picked regardless of scan order.
    let mut idxs = Vec::new();
    for n in 1..=5 {
        let (i, _) = m
            .add_peer(ep(n, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st)
            .unwrap();
        m.set_failcount(i, 5);
        idxs.push(i);
    }
    let a = l_find(&m, 1);
    let _ = a;
    let ia = idxs[0];
    m.peers_mut()[ia].trust_points = -5;
    m.peers_mut()[idxs[1]].trust_points = 3;
    m.erase_peers(&st, false);
    // `a` (lower trust) is the better erase candidate → gone first
    assert!(m.find_peer(ep(1, 6881).ip()).is_none());
    assert_eq!(m.num_peers(), 4);

    // failcount ordering beats trust ordering upstream
    let mut m2 = PeerList::new(&st);
    let mut i2 = Vec::new();
    for n in 1..=5 {
        let (i, _) = m2
            .add_peer(ep(n, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st)
            .unwrap();
        i2.push(i);
    }
    // trusted but high-failcount vs untrusted low-failcount: the
    // higher failcount is still the better erase candidate. Both
    // are past max_failcount → both non-candidates → both erasable.
    m2.set_failcount(i2[0], 3);
    m2.peers_mut()[i2[0]].trust_points = -5;
    m2.set_failcount(i2[1], 10);
    m2.peers_mut()[i2[1]].trust_points = 5;
    m2.erase_peers(&st, false);
    assert!(m2.find_peer(ep(2, 6881).ip()).is_none());
    assert!(m2.find_peer(ep(1, 6881).ip()).is_some());
}

fn l_find(l: &PeerList, a: u8) -> usize {
    l.find_peer(ep(a, 0).ip()).unwrap()
}

#[test]
fn resume_data_peers_weeded_immediately() {
    // upstream `should_erase_immediately`: resume-data-only entries
    // are dropped inside the sweep, not deferred to the end.
    let st = PeerListState {
        max_peerlist_size: 6,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    for n in 1..=6 {
        l.add_peer(ep(n, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st);
    }
    // refill one slot with a resume-data peer after a failure made
    // room for it
    let gone = l.find_peer(ep(1, 6881).ip()).unwrap();
    l.set_failcount(gone, 5);
    l.erase_peers(&st, false);
    let added = l.add_peer(
        ep(9, 6881),
        PeerSourceFlags::RESUME_DATA,
        PexFlags::NONE,
        &st,
    );
    assert!(added.is_some());
    // a *connectable* resume peer is still a connect candidate —
    // upstream only weeds it once it's no longer dialable
    let (ri, _) = added.unwrap();
    l.set_failcount(ri, 3);
    // next insert past the cap erases the resume-only entry inline
    l.add_peer(ep(8, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st);
    assert!(l.find_peer(ep(9, 6881).ip()).is_none());
    // and a fresh resume_data add when the list is full is refused
    let st2 = PeerListState {
        max_peerlist_size: 2,
        ..Default::default()
    };
    let mut l2 = PeerList::new(&st2);
    l2.add_peer(ep(1, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st2);
    l2.add_peer(ep(2, 6881), PeerSourceFlags::TRACKER, PexFlags::NONE, &st2);
    assert!(
        l2.add_peer(
            ep(3, 6881),
            PeerSourceFlags::RESUME_DATA,
            PexFlags::NONE,
            &st2
        )
        .is_none()
    );
}

#[test]
fn ban_and_filter() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER);
    add(&mut l, ep(2, 6881), PeerSourceFlags::TRACKER);
    let i = l.find_peer(ep(1, 6881).ip()).unwrap();
    l.ban_peer(i);
    assert_eq!(l.num_connect_candidates(), 1);
    // upstream `apply_ip_filter` erases filtered entries; only
    // *connected* ones are reported in `banned`.
    let banned = l.apply_ip_filter(|a| a == ep(2, 6881).ip());
    assert!(banned.is_empty());
    assert_eq!(l.num_peers(), 1);

    // a connected filtered peer is reported *and* its entry erased
    let (j, _) = add(&mut l, ep(3, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.set_connected(j, 0, false, 7);
    let banned = l.apply_ip_filter(|a| a == ep(3, 6881).ip());
    assert_eq!(banned, vec![ep(3, 6881).ip()]);
    assert!(l.find_peer(ep(3, 6881).ip()).is_none());
}

#[test]
fn update_port() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    // an incoming conn is stored connectable=false — a DHT port
    // message then flips it (upstream `update_peer_port`).
    let i = l.add_incoming(ep(1, 40000), &st).unwrap();
    assert_eq!(l.num_connect_candidates(), 0);
    assert!(l.update_peer_port(6881, i, PeerSourceFlags::DHT, &st));
    assert_eq!(l.peers()[i].port, 6881);
    assert!(l.peers()[i].connectable);
    assert_eq!(l.num_connect_candidates(), 1);
}

#[test]
fn set_seed_and_upload_only() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.set_seed(i, true);
    assert_eq!(l.num_seeds(), 1);
    // finished torrents don't dial seeds → candidate count drops
    let st_f = PeerListState {
        is_finished: true,
        ..Default::default()
    };
    l.recalculate_connect_candidates(&st_f);
    assert_eq!(l.num_connect_candidates(), 0);
    // BEP-21 upload_only is not a full seed — num_seeds untouched
    l.set_upload_only(i, true);
    assert_eq!(l.num_seeds(), 1);
}

// ---------------------------------------------------------------------
// A07 — `peer_list::new_connection` admission legs (upstream
// `peer_list::new_connection`, src/peer_list.cpp).

use bitdaemon_net::peer_list::{AttachedConn, NewConnVerdict};

fn conn(
    remote: SocketAddr,
    local: Option<SocketAddr>,
    outgoing: bool,
    failed: bool,
) -> AttachedConn {
    AttachedConn {
        remote,
        local,
        outgoing,
        failed,
        key: 7,
    }
}

#[test]
fn new_connection_unknown_and_banned() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);

    // unknown endpoint → incoming entry, attach index returned
    let v = l.new_connection(ep(9, 40000), None, &st, &|_| None, false);
    let NewConnVerdict::Attach(i) = v else {
        panic!("unknown endpoint must attach");
    };
    assert!(!l.peers()[i].connectable);
    assert!(l.peers()[i].source.contains(PeerSourceFlags::INCOMING));

    // banned entries are refused before anything else
    let (j, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.ban_peer(j);
    let v = l.new_connection(ep(1, 6881), None, &st, &|_| None, false);
    assert!(matches!(v, NewConnVerdict::Banned));
}

#[test]
fn new_connection_same_direction_dup() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let i = l.add_incoming(ep(1, 40000), &st).unwrap();
    l.set_connected(i, 0, false, 7);
    // a second *incoming* conn to an entry with an incoming conn is
    // dropped unconditionally (upstream `c->is_outgoing()` leg).
    let oc = conn(ep(1, 40000), Some(ep(0, 6881)), false, false);
    let v = l.new_connection(ep(1, 40000), Some(ep(0, 6881)), &st, &|_| Some(oc), false);
    assert!(matches!(v, NewConnVerdict::KillSelf));
}

#[test]
fn new_connection_cross_direction_ports() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    // entry dialed out to ep(1, 40000) — live outgoing conn bound.
    let (i, _) = add(&mut l, ep(1, 40000), PeerSourceFlags::TRACKER).unwrap();
    l.set_connected(i, 0, false, 7);

    // our listen port is *lower* than the existing conn's remote port
    // → upstream `our_port < other_port` keeps their outgoing conn,
    // drops our incoming one.
    let oc = conn(ep(1, 40000), Some(ep(2, 5000)), true, false);
    let v = l.new_connection(ep(1, 9000), Some(ep(0, 6000)), &st, &|_| Some(oc), false);
    assert!(matches!(v, NewConnVerdict::KillSelf));

    // our listen port is *higher* → we keep our outgoing conn and the
    // incoming one replaces it (`m_locked_peer` swap → KillOther).
    let oc = conn(ep(1, 40000), Some(ep(2, 5000)), true, false);
    let v = l.new_connection(ep(1, 9000), Some(ep(0, 50000)), &st, &|_| Some(oc), false);
    assert!(matches!(v, NewConnVerdict::KillOther { index, other_key: 7, .. } if index == i));

    // equal ports → `random(1)` tiebreak: coin=true drops the new conn.
    let v = l.new_connection(ep(1, 9000), Some(ep(0, 40000)), &st, &|_| Some(oc), true);
    assert!(matches!(v, NewConnVerdict::KillSelf));
    let v = l.new_connection(ep(1, 9000), Some(ep(0, 40000)), &st, &|_| Some(oc), false);
    assert!(matches!(v, NewConnVerdict::KillOther { .. }));

    // a *failed* existing conn loses regardless of port order
    // (`disconnect1 &= !i->connection->failed()`).
    let oc_failed = conn(ep(1, 40000), Some(ep(2, 5000)), true, true);
    let v = l.new_connection(
        ep(1, 9000),
        Some(ep(0, 6000)),
        &st,
        &|_| Some(oc_failed),
        false,
    );
    assert!(matches!(v, NewConnVerdict::KillOther { .. }));
}

#[test]
fn new_connection_self_detect() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    // outgoing conn to ep(5, 6881) whose LOCAL endpoint is ep(5, 9000):
    // an inbound conn FROM ep(5,9000) is us connecting to ourselves.
    let (i, _) = add(&mut l, ep(5, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.set_connected(i, 0, false, 7);
    let oc = conn(ep(5, 6881), Some(ep(5, 9000)), true, false);
    let v = l.new_connection(ep(5, 9000), None, &st, &|_| Some(oc), false);
    assert!(matches!(
        v,
        NewConnVerdict::SelfConnect { index, other_key: 7, .. } if index == i
    ));

    // mirror leg: our inbound conn's LOCAL endpoint equals the
    // existing outgoing conn's remote — we dialed out to our own
    // listen address and the SYN just came back in.
    let oc2 = conn(ep(5, 6881), Some(ep(3, 1111)), true, false);
    let v = l.new_connection(ep(5, 9000), Some(ep(5, 6881)), &st, &|_| Some(oc2), false);
    assert!(matches!(v, NewConnVerdict::SelfConnect { .. }));
}

#[test]
fn new_connection_stale_mark_attaches() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, _) = add(&mut l, ep(7, 6881), PeerSourceFlags::TRACKER).unwrap();
    // `connection` marked but the conn is gone (dial slot cleared
    // without unwind) — upstream clears the pointer and attaches.
    l.set_connected(i, 0, false, 7);
    assert_eq!(l.num_connect_candidates(), 0);
    let v = l.new_connection(ep(7, 6881), None, &st, &|_| None, false);
    assert!(matches!(v, NewConnVerdict::Attach(index) if index == i));
    // clearing the stale mark restores candidacy (the mark's
    // `set_connected` had deducted it); the attach-side
    // `set_connected` will deduct it again — net zero.
    assert_eq!(l.num_connect_candidates(), 1);
    let _ = l.set_connected(i, 0, false, 9);
    assert_eq!(l.num_connect_candidates(), 0);
    assert_eq!(l.peers()[i].conn_key, 9);
}

#[test]
fn new_connection_full_list() {
    // cap=1, single entry *connected* so `erase_peers(force)` can't
    // evict it → a new inbound endpoint is Full (upstream
    // `too_many_connections`).
    let st = PeerListState {
        max_peerlist_size: 1,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    let (i, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.set_connected(i, 0, false, 7);
    let v = l.new_connection(ep(9, 40000), None, &st, &|_| None, false);
    assert!(matches!(v, NewConnVerdict::Full));
}

#[test]
fn set_connected_transfers_prev_amount() {
    let st = PeerListState::default();
    let mut l = PeerList::new(&st);
    let (i, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.peers_mut()[i].prev_amount_download = 5;
    l.peers_mut()[i].prev_amount_upload = 3;
    // a real conn (conn_key != 0) collects the parked KiB as bytes.
    let (dl, ul) = l.set_connected(i, 10, false, 7);
    assert_eq!((dl, ul), (5 * 1024, 3 * 1024));
    assert_eq!(l.peers()[i].prev_amount_download, 0);
    assert_eq!(l.peers()[i].prev_amount_upload, 0);
    // conn_key survives a `update_peer_port` rewrite — bookkeeping
    // (`connection_closed`) still finds the entry.
    l.peers_mut()[i].prev_amount_download = 2;
    let _ = l.update_peer_port(9000, i, PeerSourceFlags::DHT, &st);
    assert_eq!(l.peers()[i].conn_key, 7);
    assert_eq!(l.peers()[i].port, 9000);
    // a pending-dial mark (conn_key 0) does NOT collect — totals stay.
    l.connection_closed(i, 20, false, false, &st);
    let (dl0, _) = l.set_connected(i, 30, false, 0);
    assert_eq!(dl0, 0);
    assert_eq!(l.peers()[i].prev_amount_download, 2);
}

#[test]
fn fast_reconnect_rewinds_last_connected() {
    let st = PeerListState {
        min_reconnect_time: 60,
        max_failcount: 3,
        ..Default::default()
    };
    let mut l = PeerList::new(&st);
    let (i, _) = add(&mut l, ep(1, 6881), PeerSourceFlags::TRACKER).unwrap();
    l.set_connected(i, 1000, false, 7);
    l.connection_closed(i, 1000, false, true, &st);
    // rewind by min_reconnect_time * max_failcount → dialable now.
    assert!(l.fast_reconnect(i, 1000, &st));
    assert_eq!(l.peers()[i].last_connected, 1000 - 180);
    assert_eq!(l.peers()[i].failcount, 1);
    // upstream ignores further fast reconnects past the cap leg.
    l.fast_reconnect(i, 1000, &st);
    assert!(!l.fast_reconnect(i, 1000, &st));
}
