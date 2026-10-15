// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `dht_tracker::incoming_packet` pre-filters — framing, decode
//! limits — and `node::connection_timeout` cadence.

use std::net::SocketAddr;

use bitdaemon_bencode::{Dictionary, Entry};
use bitdaemon_dht::dht_settings::DhtSettings;
use bitdaemon_dht::dht_state::DhtState;
use bitdaemon_dht::dht_tracker::DhtTracker;
use bitdaemon_dht::node_id::generate_id_impl;

fn tracker() -> DhtTracker {
    let mut t = DhtTracker::default();
    t.bind(
        "0.0.0.0".parse().unwrap(),
        None,
        DhtSettings::default(),
        &DhtState::default(),
    );
    t
}

fn ping(from: SocketAddr) -> Vec<u8> {
    let id = generate_id_impl(&from.ip(), 1);
    let mut a = Dictionary::new();
    a.insert("id".into(), Entry::String(id.as_bytes().to_vec()));
    let mut e = Dictionary::new();
    e.insert("a".into(), Entry::Dict(a));
    e.insert("q".into(), Entry::String(b"ping".to_vec()));
    e.insert("t".into(), Entry::String(b"aa".to_vec()));
    e.insert("y".into(), Entry::String(b"q".to_vec()));
    bitdaemon_bencode::bencode(&Entry::Dict(e))
}

#[test]
fn non_dht_datagrams_do_not_consume_blocker_budget() {
    // upstream `incoming_packet` drops `front != 'd' ||
    // back != 'e' || size <= 20` before the dos-blocker — other
    // UDP protocols sharing the port (LSD `BT-SEARCH`) must not
    // eat the per-source rate budget.
    let mut t = tracker();
    let from: SocketAddr = "8.8.8.8:7000".parse().unwrap();
    for _ in 0..20 {
        assert!(t.incoming(from, b"BT-SEARCH * HTTP/1.1\r\n").is_none());
    }
    // block_ratelimit is 5 per 10 s — if any of the datagrams above
    // had counted, this source would be banned for 5 minutes.
    assert!(t.incoming(from, &ping(from)).is_some());
}

#[test]
fn framing_rejects_short_and_non_dict() {
    let mut t = tracker();
    let from: SocketAddr = "8.8.8.8:7001".parse().unwrap();
    // `de` — a legal bencoded dict but <= 20 bytes: upstream drops
    // it before any DHT accounting.
    assert!(t.incoming(from, b"de").is_none());
    // ends with 'e' but doesn't start with 'd'
    let mut junk = vec![b'x'; 64];
    *junk.last_mut().unwrap() = b'e';
    assert!(t.incoming(from, &junk).is_none());
}

#[test]
fn krpc_decode_limits() {
    // upstream parses KRPC with depth_limit=10, token_limit=500 —
    // a query padded past 500 tokens is dropped, a plain ping
    // answers.
    let mut t = tracker();
    let from: SocketAddr = "8.8.8.8:7002".parse().unwrap();
    let thin = ping(from);
    assert!(t.incoming(from, &thin).is_some());

    let id = generate_id_impl(&from.ip(), 1);
    let mut a = Dictionary::new();
    a.insert("id".into(), Entry::String(id.as_bytes().to_vec()));
    // ~400 junk keys ≈ 800 tokens > 500 — upstream rejects.
    for i in 0..400u32 {
        a.insert(
            format!("j{i:03}").into_bytes(),
            Entry::String(b"x".to_vec()),
        );
    }
    let mut e = Dictionary::new();
    e.insert("a".into(), Entry::Dict(a));
    e.insert("q".into(), Entry::String(b"ping".to_vec()));
    e.insert("t".into(), Entry::String(b"aa".to_vec()));
    e.insert("y".into(), Entry::String(b"q".to_vec()));
    let fat = bitdaemon_bencode::bencode(&Entry::Dict(e));
    assert!(t.incoming(from, &fat).is_none());
}

#[test]
fn storage_tick_is_gated_to_two_minutes() {
    // `connection_timeout` runs `storage.tick()` at most once per
    // 2 min (`m_last_tracker_tick`) — two quick ticks don't
    // re-sweep the storage.
    let mut t = tracker();
    let n = t.v4.as_mut().unwrap();
    n.tick();
    assert_eq!(n.storage_ticks, 1, "first tick sweeps storage");
    n.tick();
    assert_eq!(n.storage_ticks, 1, "second tick within 2 min skips");
}
