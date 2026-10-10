// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `test_dht.cpp` `node_id` — BEP-42 test vectors + distance and
//! secret-id checks.

use std::net::IpAddr;

use bitdaemon_dht::node_id::{
    NodeId, compare_ref, distance, distance_exp, generate_id, generate_id_impl,
    generate_prefix_mask, generate_secret_id, matching_prefix, verify_id, verify_secret_id,
};

#[test]
fn bep42_test_vectors() {
    // verbatim table from `test_dht.cpp` (`TORRENT_TEST(node_id)`)
    let ips = [
        "124.31.75.21",
        "21.75.31.124",
        "65.23.51.170",
        "84.124.73.14",
        "43.213.53.83",
    ];
    let rs = [1u32, 86, 22, 65, 90];
    let prefixes: [[u8; 3]; 5] = [
        [0x5f, 0xbf, 0xbf],
        [0x5a, 0x3c, 0xe9],
        [0xa5, 0xd4, 0x32],
        [0x1b, 0x03, 0x21],
        [0xe5, 0x6f, 0x6c],
    ];
    for i in 0..5 {
        let ip: IpAddr = ips[i].parse().unwrap();
        let id = generate_id_impl(&ip, rs[i]);
        assert_eq!(id[0], prefixes[i][0], "{ips:?}[{i}] byte 0");
        assert_eq!(id[1], prefixes[i][1], "{ips:?}[{i}] byte 1");
        assert_eq!(id[2] & 0xf8, prefixes[i][2] & 0xf8, "{ips:?}[{i}] byte 2");
        assert_eq!(id[19] as u32, rs[i] & 0xff, "{ips:?}[{i}] r byte");
        assert!(verify_id(&id, &ip));
    }
}

#[test]
fn verify_id_rejects_mismatch() {
    let a: IpAddr = "124.31.75.21".parse().unwrap();
    let b: IpAddr = "21.75.31.124".parse().unwrap();
    let id = generate_id_impl(&a, 1);
    assert!(verify_id(&id, &a));
    assert!(!verify_id(&id, &b));
    // local addresses always pass
    assert!(verify_id(&id, &"10.0.0.1".parse().unwrap()));
    assert!(verify_id(&id, &"192.168.1.5".parse().unwrap()));
}

#[test]
fn distance_and_ref() {
    let a = NodeId::from_bytes(&[0xf0; 20]).unwrap();
    let b = NodeId::from_bytes(&[0x0f; 20]).unwrap();
    let r = NodeId::from_bytes(&[0x00; 20]).unwrap();
    // distance is xor-symmetric
    assert_eq!(distance(&a, &b), distance(&b, &a));
    // a xor r = 0xf0… > b xor r = 0x0f… → b closer
    assert!(compare_ref(&b, &a, &r));
    assert!(!compare_ref(&a, &b, &r));
    assert_eq!(distance_exp(&a, &a), 0);
    // differing only in the top bit of byte 0 → distance exp 159
    let mut v = [0xf0u8; 20];
    v[0] = 0x70;
    let c = NodeId::from_bytes(&v).unwrap();
    assert_eq!(distance_exp(&a, &c), 159);
}

#[test]
fn secret_id_round_trip() {
    let id = generate_secret_id();
    assert!(verify_secret_id(&id));
    // a random id doesn't verify
    let other = NodeId::from_bytes(&[0xaa; 20]).unwrap();
    assert!(!verify_secret_id(&other));
}

#[test]
fn prefix_mask_and_matching() {
    let m = generate_prefix_mask(8);
    assert_eq!(m[0], 0xff);
    assert_eq!(m[1], 0x00);
    let m = generate_prefix_mask(5);
    assert_eq!(m[0], 0xf8);
    let m = generate_prefix_mask(160);
    assert!(m.as_bytes().iter().all(|&b| b == 0xff));

    // matching_prefix: (id << offset)[0] & mask == prefix
    let mut bytes = [0u8; 20];
    bytes[0] = 0xab;
    bytes[1] = 0xcd;
    let id = NodeId::from_bytes(&bytes).unwrap();
    assert!(matching_prefix(&id, 0xff, 0xab, 0));
    assert!(!matching_prefix(&id, 0xff, 0xcd, 0));
    // shifting by 8 bits exposes byte 1 at the top
    assert!(matching_prefix(&id, 0xff, 0xcd, 8));

    let _ = generate_id(&"8.8.8.8".parse().unwrap());
}
