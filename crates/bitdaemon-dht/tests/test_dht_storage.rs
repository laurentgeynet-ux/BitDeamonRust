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
