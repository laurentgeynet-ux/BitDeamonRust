// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `dht_state` round-trip + `dht_settings` load/save + `node_entry`.

use std::net::SocketAddr;

use bitdaemon_bencode::{bdecode_checked, bencode};
use bitdaemon_dht::dht_settings::{DhtSettings, dht_load_state, dht_save_state};
use bitdaemon_dht::dht_state::{DhtState, extract_node_ids, read_dht_state, save_dht_state};
use bitdaemon_dht::node_entry::NodeEntry;
use bitdaemon_dht::node_id::{NodeId, generate_id_impl};

#[test]
fn state_round_trip() {
    let mut st = DhtState::default();
    let ip: std::net::IpAddr = "124.31.75.21".parse().unwrap();
    let id = generate_id_impl(&ip, 1);
    st.nids.push((ip, id));
    st.nodes.push("1.2.3.4:6881".parse().unwrap());
    st.nodes6.push("[2001:db8::1]:6881".parse().unwrap());

    let e = save_dht_state(&st);
    let buf = bencode(&e);
    let node = bdecode_checked(&buf).unwrap();
    let back = read_dht_state(&node);
    assert_eq!(back.nids.len(), 1);
    assert_eq!(back.nids[0].1, id);
    assert_eq!(back.nids[0].0, ip);
    assert_eq!(back.nodes, st.nodes);
    assert_eq!(back.nodes6, st.nodes6);
}

#[test]
fn extract_old_style_nid() {
    // a bare 20-byte `node-id` string (old clients)
    let id = NodeId::from_bytes(&[7u8; 20]).unwrap();
    let mut e = bitdaemon_bencode::Entry::Dict(Default::default());
    e.dict_mut().unwrap().insert(
        b"node-id".to_vec(),
        bitdaemon_bencode::Entry::String(id.to_bytes().to_vec()),
    );
    let buf = bencode(&e);
    let node = bdecode_checked(&buf).unwrap();
    let nids = extract_node_ids(&node, b"node-id");
    assert_eq!(nids.len(), 1);
    assert_eq!(nids[0].1, id);
}

#[test]
fn settings_round_trip() {
    let mut s = DhtSettings::default();
    s.max_dht_items = 1234;
    s.read_only = true;
    s.upload_rate_limit = 42;
    let e = dht_save_state(&s);
    let buf = bencode(&e);
    let node = bdecode_checked(&buf).unwrap();
    let mut back = DhtSettings::default();
    dht_load_state(&node, &mut back);
    assert_eq!(back, s);
    // non-dict input leaves defaults untouched
    let node = bdecode_checked(b"i1e").unwrap();
    let mut back2 = DhtSettings::default();
    dht_load_state(&node, &mut back2);
    assert_eq!(back2, DhtSettings::default());
}

#[test]
fn node_entry_rtt_and_flags() {
    let ip: std::net::IpAddr = "8.8.8.8".parse().unwrap();
    let id = generate_id_impl(&ip, 1);
    let ep: SocketAddr = format!("{ip}:6881").parse().unwrap();

    let mut ne = NodeEntry::new(id, ep, 0xffff, false);
    assert!(!ne.pinged());
    assert!(!ne.confirmed());
    assert_eq!(ne.fail_count(), 0);

    ne.set_pinged();
    assert!(ne.pinged());
    assert!(ne.confirmed());

    ne.update_rtt(0xffff);
    assert_eq!(ne.rtt, 0xffff);
    ne.update_rtt(300);
    assert_eq!(ne.rtt, 300);
    ne.update_rtt(600);
    // 300*2/3 + 600/3 = 200 + 200 = 400
    assert_eq!(ne.rtt, 400);

    ne.timed_out();
    assert_eq!(ne.fail_count(), 1);
    assert!(!ne.confirmed());
    ne.reset_fail_count();
    assert!(ne.confirmed());

    // better_than: verified first, then lower rtt — `ne` is already
    // verified (id generated for its ip), so compare against an entry
    // whose id does *not* match its source address
    let bad_id = generate_id_impl(&"21.75.31.124".parse().unwrap(), 86);
    let unverified = NodeEntry::new(bad_id, ep, 0xffff, true);
    assert!(!unverified.verified);
    assert!(ne.better_than(&unverified));
    assert!(!unverified.better_than(&ne));
}
