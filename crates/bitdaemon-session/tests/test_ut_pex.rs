// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! BEP-11 pex — message build/parse conformance (upstream
//! `test_pex.cpp` semantics).

use std::collections::BTreeSet;
use std::net::SocketAddr;

use bitdaemon_session::ut_pex::{build_pex_message, flags, is_local, parse_pex_message};

fn ep(ip: &str, port: u16) -> SocketAddr {
    format!("{ip}:{port}").parse().unwrap()
}

#[test]
fn build_parse_round_trip() {
    let peers = [
        (ep("1.2.3.4", 6881), flags::PEX_ENCRYPTION | flags::PEX_UTP),
        (ep("[2001:db8::1]", 51413), flags::PEX_SEED),
    ];
    let (body, sent, _in_msg) = build_pex_message(&peers, &BTreeSet::new());
    let (added, dropped) = parse_pex_message(&body, 50, 0).expect("parse");
    assert_eq!(added.len(), 2);
    assert!(dropped.is_empty());
    assert_eq!(sent.len(), 2);

    let a4 = added.iter().find(|p| p.addr.is_ipv4()).unwrap();
    assert_eq!(a4.addr, peers[0].0);
    // PEX_LT_V2 is stripped on receipt even if somehow present
    assert_eq!(a4.flags, flags::PEX_ENCRYPTION | flags::PEX_UTP);
    let a6 = added.iter().find(|p| p.addr.is_ipv6()).unwrap();
    assert_eq!(a6.addr, peers[1].0);
    assert_eq!(a6.flags, flags::PEX_SEED);
}

#[test]
fn diff_added_dropped() {
    let p1 = (ep("1.2.3.4", 1000), 0u8);
    let p2 = (ep("5.6.7.8", 2000), 0u8);
    let (_body, sent, _in_msg) = build_pex_message(&[p1], &BTreeSet::new());
    // next round: p1 still there (re-listed as known, not added),
    // p2 new — but p1 disappears next round
    let (body, sent2, _in_msg) = build_pex_message(&[p1, p2], &sent);
    let (added, dropped) = parse_pex_message(&body, 50, 0).expect("parse");
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].addr, p2.0);
    assert!(dropped.is_empty());

    // now p2 drops out → listed in `dropped`
    let (body, _, _in_msg) = build_pex_message(&[p1], &sent2);
    let (added, dropped) = parse_pex_message(&body, 50, 0).expect("parse");
    assert!(added.is_empty());
    assert_eq!(dropped, vec![p2.0]);
}

#[test]
fn lt_v2_flag_stripped() {
    // a sender that (incorrectly) leaks the internal lt_v2 bit
    let peers = [(ep("1.2.3.4", 6881), flags::PEX_UTP | flags::PEX_LT_V2)];
    let (body, _, _in_msg) = build_pex_message(&peers, &BTreeSet::new());
    let (added, _) = parse_pex_message(&body, 50, 0).expect("parse");
    assert_eq!(added[0].flags, flags::PEX_UTP);
}

#[test]
fn max_peers_cap() {
    let peers: Vec<(SocketAddr, u8)> = (0..10).map(|i| (ep("10.0.0.1", 1000 + i), 0u8)).collect();
    let (body, _, _in_msg) = build_pex_message(&peers, &BTreeSet::new());
    // already at cap — nothing accepted
    let (added, _) = parse_pex_message(&body, 3, 3).expect("parse");
    assert!(added.is_empty());
    // room for 2
    let (added, _) = parse_pex_message(&body, 3, 1).expect("parse");
    assert_eq!(added.len(), 2);
}

#[test]
fn garbage_rejected() {
    assert!(parse_pex_message(b"not bencode!!!", 50, 0).is_none());
    assert!(parse_pex_message(b"i42e", 50, 0).is_none());
}

#[test]
fn is_local_detection() {
    use std::net::IpAddr;
    assert!(is_local(&"10.0.0.1".parse::<IpAddr>().unwrap()));
    assert!(is_local(&"127.0.0.1".parse::<IpAddr>().unwrap()));
    assert!(is_local(&"fe80::1".parse::<IpAddr>().unwrap()));
    assert!(is_local(&"fd00::1".parse::<IpAddr>().unwrap()));
    assert!(!is_local(&"8.8.8.8".parse::<IpAddr>().unwrap()));
    assert!(!is_local(&"2001:4860::8888".parse::<IpAddr>().unwrap()));
}
