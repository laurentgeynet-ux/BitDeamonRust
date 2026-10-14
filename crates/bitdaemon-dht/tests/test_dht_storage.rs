// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `dht_storage` — announce/get_peers/items/samples.

use std::net::{IpAddr, SocketAddr};

use bitdaemon_common::hash::Sha1Hash;
use bitdaemon_dht::dht_settings::DhtSettings;
use bitdaemon_dht::dht_storage::DhtStorage;

fn ih(b: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[b; 20]).unwrap()
}

fn ep(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

#[test]
fn announce_and_get() {
    let mut s = DhtStorage::new(DhtSettings::default(), vec![]);
    let h = ih(1);
    s.announce_peer(&h, ep("1.2.3.4:6881"), "torrent-name", false);
    s.announce_peer(&h, ep("5.6.7.8:6881"), "", true);
    assert_eq!(s.counters().torrents, 1);
    assert_eq!(s.counters().peers, 2);

    let r: IpAddr = "9.9.9.9".parse().unwrap();
    let g = s.get_peers(&h, false, false, &r);
    assert_eq!(g.name.as_deref(), Some("torrent-name"));
    assert_eq!(g.peers.as_deref().map_or(0, |v| v.len()), 12); // 2 × 6-byte compact v4
    assert!(!g.no_token);

    // noseed filters the seed out
    let g = s.get_peers(&h, true, false, &r);
    assert_eq!(g.peers.as_deref().map_or(0, |v| v.len()), 6);

    // unknown hash
    let g = s.get_peers(&ih(9), false, false, &r);
    assert!(g.peers.is_none_or(|p| p.is_empty()));
    assert!(!g.no_token); // table isn't full
}

#[test]
fn scrape_bloom() {
    let mut s = DhtStorage::new(DhtSettings::default(), vec![]);
    let h = ih(2);
    s.announce_peer(&h, ep("1.2.3.4:1"), "", true);
    s.announce_peer(&h, ep("5.6.7.8:1"), "", false);
    let r: IpAddr = "9.9.9.9".parse().unwrap();
    let g = s.get_peers(&h, false, true, &r);
    assert_eq!(g.bfpe.as_ref().map(|v| v.len()), Some(256));
    assert_eq!(g.bfsd.as_ref().map(|v| v.len()), Some(256));
    assert!(g.peers.is_none_or(|p| p.is_empty()));
}

#[test]
fn items_round_trip() {
    let mut s = DhtStorage::new(DhtSettings::default(), vec![]);
    let a: IpAddr = "1.2.3.4".parse().unwrap();
    let t = ih(7);
    s.put_immutable_item(&t, b"i42e", &a);
    assert_eq!(s.get_immutable_item(&t), Some(&b"i42e"[..]));
    s.put_immutable_item(&t, b"i43e", &a); // overwrite value in place
    assert_eq!(s.get_immutable_item(&t), Some(&b"i43e"[..]));

    let pk = [9u8; 32];
    let sig = [5u8; 64];
    s.put_mutable_item(
        &t,
        bitdaemon_dht::dht_storage::PutMutable {
            value: b"v1",
            sig: &sig,
            seq: 1,
            pk: &pk,
            salt: b"salt",
            addr: &a,
        },
    );
    assert_eq!(s.get_mutable_item_seq(&t), Some(1));
    // seq 0 < stored → value returned
    let m = s.get_mutable_item(&t, 0, false).unwrap();
    assert_eq!(m.seq, 1);
    assert_eq!(m.value, Some(&b"v1"[..]));
    assert_eq!(m.key, &pk);
    assert_eq!(m.sig, &sig);
    assert_eq!(m.salt, b"salt");
    // seq >= stored → no value
    assert!(s.get_mutable_item(&t, 2, false).unwrap().value.is_none());
    // force_fill returns it anyway
    assert_eq!(
        s.get_mutable_item(&t, 2, true).unwrap().value,
        Some(&b"v1"[..])
    );
    // newer seq replaces
    s.put_mutable_item(
        &t,
        bitdaemon_dht::dht_storage::PutMutable {
            value: b"v2",
            sig: &sig,
            seq: 5,
            pk: &pk,
            salt: b"salt",
            addr: &a,
        },
    );
    assert_eq!(s.get_mutable_item_seq(&t), Some(5));
}

#[test]
fn items_eviction() {
    let mut st = DhtSettings::default();
    st.max_dht_items = 2;
    let my_id = ih(0);
    let mut s = DhtStorage::new(st, vec![my_id]);
    let a: IpAddr = "1.2.3.4".parse().unwrap();
    s.put_immutable_item(&ih(10), b"a", &a);
    s.put_immutable_item(&ih(11), b"b", &a);
    s.put_immutable_item(&ih(12), b"c", &a); // exceeds cap → evict
    let n = [ih(10), ih(11), ih(12)]
        .iter()
        .filter(|k| s.get_immutable_item(k).is_some())
        .count();
    assert_eq!(n, 2);
}

#[test]
fn infohashes_sample() {
    let mut st = DhtSettings::default();
    st.sample_infohashes_interval = 0; // always refresh
    let mut s = DhtStorage::new(st, vec![]);
    for i in 0..30u8 {
        s.announce_peer(
            &ih(i),
            ep(&format!("1.2.3.{}:1", i as usize + 1)),
            "",
            false,
        );
    }
    let (interval, num, samples) = s.get_infohashes_sample();
    assert_eq!(interval, 0);
    assert_eq!(num, 30);
    assert_eq!(samples.len(), 20); // capped at max count
}

// the per-/56 limit in dht_storage.rs (upstream `6089cdeea`)
const PEERS_PER_PREFIX: i32 = 16;

// an address in the /56 selected by `prefix`, all within 2001:db8::/48
fn v6_peer(prefix: u32, host: u32) -> SocketAddr {
    format!("[2001:db8:0:{:02x}00::{:x}]:6881", prefix, host + 1)
        .parse()
        .unwrap()
}

fn stored_v6_peers(s: &DhtStorage, h: &Sha1Hash) -> Vec<SocketAddr> {
    use bitdaemon_net::socket_io::{Cursor, read_endpoint};
    let r: IpAddr = "::1".parse().unwrap();
    let g = s.get_peers(h, false, false, &r);
    let Some(peers) = g.peers else {
        return Vec::new();
    };
    let mut ret = Vec::new();
    let mut cur = Cursor::new(&peers);
    while cur.remaining() >= 18 {
        ret.push(read_endpoint(&mut cur, true).unwrap());
    }
    ret
}

/// upstream `peer_prefix_limit` — a single /56 can claim at most
/// `max_peers_per_prefix` entries; the newest entries replace the
/// oldest, and other prefixes are counted separately.
#[test]
fn peer_prefix_limit() {
    let h = ih(3);
    let mut st = DhtSettings::default();
    // IPv6 replies are sized at a quarter of this, make sure we see all
    st.max_peers_reply = 4000;
    let mut s = DhtStorage::new(st, vec![]);

    for i in 0..2 * PEERS_PER_PREFIX {
        s.announce_peer(&h, v6_peer(0, i as u32), "", false);
        assert!(s.counters().peers <= PEERS_PER_PREFIX);
    }
    assert_eq!(s.counters().peers, PEERS_PER_PREFIX);

    // the oldest entries were replaced, the newest survive
    let stored = stored_v6_peers(&s, &h);
    assert_eq!(stored.len() as i32, PEERS_PER_PREFIX);
    for i in 0..2 * PEERS_PER_PREFIX {
        assert_eq!(
            stored.contains(&v6_peer(0, i as u32)),
            i >= PEERS_PER_PREFIX
        );
    }

    // the adjacent /56 is counted separately
    for i in 0..PEERS_PER_PREFIX {
        s.announce_peer(&h, v6_peer(1, i as u32), "", false);
    }
    assert_eq!(s.counters().peers, 2 * PEERS_PER_PREFIX);

    // refreshing a stored endpoint of a full prefix evicts nothing
    s.announce_peer(&h, v6_peer(0, PEERS_PER_PREFIX as u32), "", false);
    assert_eq!(s.counters().peers, 2 * PEERS_PER_PREFIX);
    let stored = stored_v6_peers(&s, &h);
    assert_eq!(stored.len() as i32, 2 * PEERS_PER_PREFIX);
    for i in PEERS_PER_PREFIX..2 * PEERS_PER_PREFIX {
        assert!(stored.contains(&v6_peer(0, i as u32)));
    }
}

/// upstream `peer_prefix_limit_full` — a full prefix keeps replacing
/// its oldest entry even once the torrent is at `dht_max_peers`,
/// since the list doesn't grow.
#[test]
fn peer_prefix_limit_full() {
    let h = ih(4);
    let mut st = DhtSettings::default();
    st.max_peers_reply = 4000;
    st.max_peers = PEERS_PER_PREFIX + 4;
    let mut s = DhtStorage::new(st, vec![]);

    for i in 0..2 * PEERS_PER_PREFIX {
        s.announce_peer(&h, v6_peer(0, i as u32), "", false);
    }
    assert_eq!(s.counters().peers, PEERS_PER_PREFIX);

    // the flooded prefix left room for other prefixes
    for i in 0..10 {
        s.announce_peer(&h, v6_peer(1, i), "", false);
    }
    assert_eq!(s.counters().peers, PEERS_PER_PREFIX + 4);
    s.announce_peer(&h, v6_peer(2, 0), "", false);
    assert_eq!(s.counters().peers, PEERS_PER_PREFIX + 4);

    s.announce_peer(&h, v6_peer(0, 2 * PEERS_PER_PREFIX as u32), "", false);
    assert_eq!(s.counters().peers, PEERS_PER_PREFIX + 4);
    let stored = stored_v6_peers(&s, &h);
    assert_eq!(stored.len() as i32, PEERS_PER_PREFIX + 4);
    assert!(stored.contains(&v6_peer(0, 2 * PEERS_PER_PREFIX as u32)));
    assert!(!stored.contains(&v6_peer(0, PEERS_PER_PREFIX as u32)));
    assert!(!stored.contains(&v6_peer(2, 0)));
}
