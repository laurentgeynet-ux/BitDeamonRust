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
        assert!(
            t.incoming(from, b"BT-SEARCH * HTTP/1.1\r\n", false)
                .is_none()
        );
    }
    // block_ratelimit is 5 per 10 s — if any of the datagrams above
    // had counted, this source would be banned for 5 minutes.
    assert!(t.incoming(from, &ping(from), false).is_some());
}

#[test]
fn framing_rejects_short_and_non_dict() {
    let mut t = tracker();
    let from: SocketAddr = "8.8.8.8:7001".parse().unwrap();
    // `de` — a legal bencoded dict but <= 20 bytes: upstream drops
    // it before any DHT accounting.
    assert!(t.incoming(from, b"de", false).is_none());
    // ends with 'e' but doesn't start with 'd'
    let mut junk = vec![b'x'; 64];
    *junk.last_mut().unwrap() = b'e';
    assert!(t.incoming(from, &junk, false).is_none());
}

#[test]
fn krpc_decode_limits() {
    // upstream parses KRPC with depth_limit=10, token_limit=500 —
    // a query padded past 500 tokens is dropped, a plain ping
    // answers.
    let mut t = tracker();
    let from: SocketAddr = "8.8.8.8:7002".parse().unwrap();
    let thin = ping(from);
    assert!(t.incoming(from, &thin, false).is_some());

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
    assert!(t.incoming(from, &fat, false).is_none());
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

// =====================================================================
// A29 — DHT counter sink
// =====================================================================

use bitdaemon_dht::dht_tracker::DhtStat;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

fn counting_tracker() -> (DhtTracker, Arc<Mutex<BTreeMap<&'static str, i64>>>) {
    let log: Arc<Mutex<BTreeMap<&'static str, i64>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let l2 = Arc::clone(&log);
    let sink: bitdaemon_dht::dht_tracker::DhtCounterSink = Arc::new(move |s: DhtStat, d: i64| {
        let name = match s {
            DhtStat::MessagesIn => "messages_in",
            DhtStat::MessagesInDropped => "messages_in_dropped",
            DhtStat::BytesIn => "bytes_in",
            DhtStat::IpOverheadIn => "ip_overhead_in",
            DhtStat::PingIn => "ping_in",
            DhtStat::FindNodeIn => "find_node_in",
            DhtStat::GetPeersIn => "get_peers_in",
            DhtStat::AnnouncePeerIn => "announce_peer_in",
            DhtStat::GetIn => "get_in",
            DhtStat::PutIn => "put_in",
            DhtStat::SampleInfohashesIn => "sample_infohashes_in",
            DhtStat::PingOut => "ping_out",
            DhtStat::GetPeersOut => "get_peers_out",
            DhtStat::AnnouncePeerOut => "announce_peer_out",
            DhtStat::GetOut => "get_out",
            DhtStat::PutOut => "put_out",
            DhtStat::SampleInfohashesOut => "sample_infohashes_out",
            DhtStat::InvalidAnnounce => "invalid_announce",
            DhtStat::InvalidGetPeers => "invalid_get_peers",
            DhtStat::InvalidFindNode => "invalid_find_node",
            DhtStat::InvalidPut => "invalid_put",
            DhtStat::InvalidGet => "invalid_get",
            DhtStat::InvalidSampleInfohashes => "invalid_sample_infohashes",
        };
        *l2.lock().unwrap().entry(name).or_insert(0) += d;
    });
    let mut t = tracker();
    t.set_counter_sink(Some(sink));
    (t, log)
}

/// `incoming_packet` — after the `d…e` sniff: `dht_bytes_in`,
/// `recv_ip_overhead_bytes` (28 v4 / 48 v6), `dht_messages_in`;
/// a valid ping also lands on `dht_ping_in`.
#[test]
fn sink_counts_bytes_messages_and_query_kind() {
    let (mut t, log) = counting_tracker();
    let from: SocketAddr = "8.8.8.8:7100".parse().unwrap();
    let pkt = ping(from);
    let n = pkt.len() as i64;
    assert!(t.incoming(from, &pkt, false).is_some());
    let g = log.lock().unwrap();
    assert_eq!(g.get("bytes_in"), Some(&n));
    assert_eq!(g.get("ip_overhead_in"), Some(&28), "v4 UDP+IP header");
    assert_eq!(g.get("messages_in"), Some(&1));
    assert_eq!(g.get("ping_in"), Some(&1), "query kind");
}

/// framing rejects run *before* any accounting (non-DHT traffic
/// shares the port); the ip-filtered leg counts `dropped`.
#[test]
fn sink_counts_sniff_then_filtered_drop() {
    let (mut t, log) = counting_tracker();
    let from: SocketAddr = "8.8.8.8:7101".parse().unwrap();
    // fails the sniff — nothing counted.
    assert!(
        t.incoming(from, b"BT-SEARCH * HTTP/1.1\r\n", false)
            .is_none()
    );
    assert!(log.lock().unwrap().is_empty());
    // ip-filtered (`filtered` = apply_filter_to_dht && blocked).
    assert!(t.incoming(from, &ping(from), true).is_none());
    let g = log.lock().unwrap();
    assert_eq!(g.get("messages_in"), Some(&1), "sniff passed");
    assert_eq!(g.get("messages_in_dropped"), Some(&1));
    assert_eq!(g.get("ping_in"), None, "dropped before dispatch");
}

/// `dht_ignore_dark_internet` — class-A sources drop with
/// `dht_messages_in_dropped` (v4 only leg).
#[test]
fn sink_counts_dark_internet_drop() {
    let (mut t, log) = counting_tracker();
    let from: SocketAddr = "11.0.0.1:6881".parse().unwrap(); // class-A 11
    assert!(t.incoming(from, &ping(from), false).is_none());
    let g = log.lock().unwrap();
    assert_eq!(g.get("messages_in_dropped"), Some(&1));
    assert_eq!(g.get("ping_in"), None);
}
