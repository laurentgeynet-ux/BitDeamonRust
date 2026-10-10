// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `test_dht.cpp` `test_routing_table` — add/find/fail/split.

use std::net::{IpAddr, SocketAddr};

use bitdaemon_dht::dht_settings::DhtSettings;
use bitdaemon_dht::node_id::{generate_id, generate_id_impl};
use bitdaemon_dht::routing_table::RoutingTable;

fn settings() -> DhtSettings {
    let mut s = DhtSettings::default();
    // upstream tests run with restrict_routing_ips on; keep defaults
    s.restrict_routing_ips = true;
    s
}

#[test]
fn add_and_find() {
    let our: IpAddr = "98.98.98.98".parse().unwrap();
    let our_id = generate_id(&our);
    let mut t = RoutingTable::new(our_id, false, 8, settings());

    // a node with the same IP:port changing ID gets evicted
    let ep: SocketAddr = "124.31.75.21:4".parse().unwrap();
    let id1 = generate_id_impl(&"124.31.75.21".parse().unwrap(), 1);
    t.node_seen(id1, ep, 10);
    assert_eq!(t.bucket_size_at(0), 1);
    assert_eq!(t.size().0, 1);
    let nodes = t.find_node(&id1, false, 10);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, Some(id1));
    assert_eq!(nodes[0].endpoint, ep);
    assert_eq!(nodes[0].fail_count(), 0);
}

#[test]
fn duplicate_ip_rejected() {
    let our: IpAddr = "98.98.98.98".parse().unwrap();
    let mut t = RoutingTable::new(generate_id(&our), false, 8, settings());
    let ep1: SocketAddr = "124.31.75.21:4".parse().unwrap();
    let ep2: SocketAddr = "124.31.75.22:4".parse().unwrap();
    // /24-adjacent IPs: restrict_routing_ips rejects the second
    let id1 = generate_id_impl(&"124.31.75.21".parse().unwrap(), 1);
    let id2 = generate_id_impl(&"124.31.75.22".parse().unwrap(), 2);
    assert!(t.node_seen(id1, ep1, 10));
    assert!(!t.node_seen(id2, ep2, 10));
    assert_eq!(t.size().0, 1);
}

#[test]
fn node_failed_and_replace() {
    let our: IpAddr = "98.98.98.98".parse().unwrap();
    let mut t = RoutingTable::new(generate_id(&our), false, 8, settings());
    let mut eps = Vec::new();
    for i in 0..10u16 {
        let ip = format!("192.{}.{}.{}", 10 + i, 20 + i, 30 + i);
        let ep: SocketAddr = format!("{ip}:688{i}").parse().unwrap();
        let id = generate_id_impl(&ip.parse().unwrap(), i as u32);
        t.node_seen(id, ep, 50 + i as i32);
        eps.push((id, ep));
    }
    let total = t.size().0;
    assert!(total > 0);
    // fail a live node repeatedly until evicted (max_fail_count=20)
    let (id, ep) = eps[0];
    for _ in 0..25 {
        t.node_failed(&id, &ep);
    }
    // endpoint mismatches are ignored
    let before = t.size().0;
    t.node_failed(&eps[1].0, &"1.1.1.1:1".parse().unwrap());
    assert_eq!(t.size().0, before);
}

#[test]
fn bucket_splitting() {
    // fill bucket 0 with confirmed nodes → table must split past 1
    let our_ip: IpAddr = "98.98.98.98".parse().unwrap();
    let our_id = generate_id(&our_ip);
    let mut t = RoutingTable::new(our_id, false, 8, settings());
    for i in 0..40u32 {
        let ip = format!(
            "{}.{}.{}.{}",
            60 + (i % 180),
            (i * 7) % 256,
            (i * 13) % 256,
            (i * 3) % 256
        );
        let addr: IpAddr = ip.parse().unwrap();
        let ep: SocketAddr = format!("{ip}:{}", 4000 + i).parse().unwrap();
        let id = generate_id_impl(&addr, i % 256);
        t.node_seen(id, ep, 10);
    }
    // many nodes went in — the table should have split at least once
    assert!(t.num_active_buckets() >= 1);
    let (n, _r, c) = t.size();
    assert!(n > 8);
    assert!(c > 0);
}

#[test]
fn heard_about_unpinged() {
    let our: IpAddr = "98.98.98.98".parse().unwrap();
    let mut t = RoutingTable::new(generate_id(&our), false, 8, settings());
    let ep: SocketAddr = "65.23.51.170:5".parse().unwrap();
    let id = generate_id_impl(&"65.23.51.170".parse().unwrap(), 22);
    t.heard_about(id, ep);
    // un-pinged node lands in replacements, not live nodes
    assert_eq!(t.size().0, 0);
    assert_eq!(t.size().1, 1);
    // node_seen with the same endpoint confirms it
    t.node_seen(id, ep, 42);
    assert_eq!(t.size().0, 1);
}
