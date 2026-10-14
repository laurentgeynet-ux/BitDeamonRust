// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Wire-level `incoming_request` regression tests — the error
//! codes, reply fields and BEP-44 semantics upstream `node.cpp`
//! produces, checked against `handle_query` byte-for-byte where it
//! matters.

use std::net::SocketAddr;

use bitdaemon_bencode::{BdecodeNode, Entry, bdecode_checked};
use bitdaemon_dht::dht_node::{DhtEvent, DhtNode};
use bitdaemon_dht::dht_settings::DhtSettings;
use bitdaemon_dht::node_id::{NodeId, generate_id_impl};

fn node(ip: &str) -> (DhtNode, SocketAddr) {
    let addr: SocketAddr = format!("{ip}:6881").parse().unwrap();
    let id = generate_id_impl(&addr.ip(), 1);
    (
        DhtNode::new(Some(id), addr.ip(), false, DhtSettings::default()),
        addr,
    )
}

/// build `d{"a": args, "q": q, "t": tid, "y": "q"}e`.
fn query(q: &str, args: &[(&str, Entry)]) -> Vec<u8> {
    let mut am = bitdaemon_bencode::Dictionary::new();
    for (k, v) in args {
        am.insert((*k).into(), v.clone());
    }
    let mut em = bitdaemon_bencode::Dictionary::new();
    em.insert("a".into(), Entry::Dict(am));
    em.insert("q".into(), Entry::String(q.as_bytes().to_vec()));
    em.insert("t".into(), Entry::String(b"aa".to_vec()));
    em.insert("y".into(), Entry::String(b"q".to_vec()));
    bitdaemon_bencode::bencode(&Entry::Dict(em))
}

/// feed a query and decode the reply; the buffer is leaked so the
/// decoded `BdecodeNode` can be inspected freely.
fn run_owned(n: &mut DhtNode, pkt: &[u8], from: SocketAddr) -> (Vec<u8>, BdecodeNode<'static>) {
    let (buf, to) = n
        .incoming(pkt, from, None, true)
        .expect("query produced no reply");
    assert_eq!(to, from);
    let leaked: &'static [u8] = Box::leak(buf.clone().into_boxed_slice());
    (buf, bdecode_checked(leaked).unwrap())
}

fn err_code(r: &BdecodeNode<'_>) -> (i64, Vec<u8>) {
    assert_eq!(r.dict_find_string_value(b"y", b""), b"e");
    let e = r.dict_find_list(b"e");
    (
        e.list_at(0).int_value(),
        e.list_at(1).string_value().to_vec(),
    )
}

fn sender_id() -> NodeId {
    generate_id_impl(&"5.6.7.8".parse().unwrap(), 1)
}

#[test]
fn missing_id_is_protocol_error_203() {
    // `a` without the mandatory 20-byte `id` → e [203, "missing 'id' key"]
    let (mut n, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let pkt = query("ping", &[]);
    let (_buf, rsp) = run_owned(&mut n, &pkt, pb);
    let (code, msg) = err_code(&rsp);
    assert_eq!(code, 203);
    assert!(String::from_utf8_lossy(&msg).contains("id"), "{msg:?}");
}

#[test]
fn reply_has_ip_p_v_and_sender_mirrored() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let pkt = query(
        "ping",
        &[("id", Entry::String(sender_id().as_bytes().to_vec()))],
    );
    let (buf, rsp) = run_owned(&mut a, &pkt, pb);
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"r");
    // `v` — client version stamp (upstream `send_packet`)
    assert_eq!(
        rsp.dict_find_string_value(b"v", b""),
        &bitdaemon_dht::CLIENT_VERSION
    );
    // `ip` — the requester's observed endpoint, compact 6-byte v4
    let ip = rsp.dict_find_string(b"ip");
    assert_eq!(ip.string_length(), 6);
    assert_eq!(&ip.string_value()[..4], &[5, 6, 7, 8]);
    assert_eq!(
        u16::from_be_bytes([ip.string_value()[4], ip.string_value()[5]]),
        6881
    );
    let r = rsp.dict_find_dict(b"r");
    assert_eq!(r.dict_find_string(b"id").string_value(), a.id.as_bytes());
    assert_eq!(r.dict_find_int(b"p").int_value(), 6881);
    let _ = buf;
}

#[test]
fn get_peers_emits_nodes_token_values_and_name() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let ih = NodeId::from_bytes(&[0xabu8; 20]).unwrap();
    // seed storage + a table node so `nodes` isn't empty
    a.storage
        .announce_peer(&ih, "9.8.7.6:51413".parse().unwrap(), "cool-name", false);
    a.table.node_seen(a.id, pb, 10); // any known node
    let pkt = query(
        "get_peers",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("info_hash", Entry::String(ih.as_bytes().to_vec())),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    let r = rsp.dict_find_dict(b"r");
    assert!(r.dict_find_string(b"token").is_valid(), "no token");
    assert!(r.dict_find_string(b"nodes").is_valid(), "no nodes");
    // `n` — torrent name (upstream storage writes `peers["n"]`)
    assert_eq!(r.dict_find_string_value(b"n", b""), b"cool-name");
    let values = r.dict_find_list(b"values");
    assert!(values.is_valid());
    assert_eq!(values.list_size(), 1);
    // one 6-byte compact v4 endpoint
    let p = values.list_at(0);
    assert_eq!(p.string_length(), 6);
    assert_eq!(&p.string_value()[..4], &[9, 8, 7, 6]);
}

#[test]
fn get_peers_unknown_hash_returns_empty_values_and_nodes() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let ih = NodeId::from_bytes(&[0xcbu8; 20]).unwrap();
    let pkt = query(
        "get_peers",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("info_hash", Entry::String(ih.as_bytes().to_vec())),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    let r = rsp.dict_find_dict(b"r");
    assert!(r.dict_find_string(b"token").is_valid(), "no token");
    assert!(r.dict_find_string(b"nodes").is_valid(), "no nodes");
    assert!(!r.dict_find(b"values").is_valid(), "values on unknown hash");
}

#[test]
fn announce_implied_port_and_token_flow() {
    let (mut a, pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let ih = bitdaemon_common::Sha1Hash::from_bytes(&[0x5au8; 20]).unwrap();

    // implied_port=1 → the sender's source port is stored
    let tok = a.generate_token(&pb, &ih);
    let pkt = query(
        "announce_peer",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("info_hash", Entry::String(ih.as_bytes().to_vec())),
            ("port", Entry::Int(1)), // bogus — implied_port overrides
            ("token", Entry::String(tok.to_vec())),
            ("implied_port", Entry::Int(1)),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"r");
    let r = a.storage.get_peers(&ih, false, false, &pa.ip());
    let peers = r.peers.unwrap_or_default();
    assert_eq!(peers.len(), 6);
    // the *source* port (6881 = 0x1ae1) was recorded, not port arg
    assert_eq!(&peers[4..6], &6881u16.to_be_bytes());
}

#[test]
fn unknown_query_with_target_acts_as_find_node() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let pkt = query(
        "future_query",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("target", Entry::String([0x11u8; 20].to_vec())),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"r");
    assert!(
        rsp.dict_find_dict(b"r")
            .dict_find_string(b"nodes")
            .is_valid()
    );
}

#[test]
fn unknown_query_without_target_is_203() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let pkt = query(
        "future_query",
        &[("id", Entry::String(sender_id().as_bytes().to_vec()))],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    let (code, _msg) = err_code(&rsp);
    assert_eq!(code, 203);
}

#[test]
fn ro_sender_is_not_recorded() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let before = a.table.size().0;
    let pkt = query(
        "ping",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("ro", Entry::Int(1)),
        ],
    );
    let _ = run_owned(&mut a, &pkt, pb);
    assert_eq!(a.table.size().0, before, "read-only sender recorded");
}

#[test]
fn put_immutable_round_trip() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let value = b"i42e".to_vec();
    let target = crate_target_immutable(&value);
    let tok = a.generate_token(&pb, &target);

    let pkt = query(
        "put",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("token", Entry::String(tok.to_vec())),
            ("v", Entry::Preformatted(value.clone())),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"r");
    assert_eq!(a.storage.get_immutable_item(&target), Some(&value[..]));

    // `get` returns `v` + `token`
    let pkt = query(
        "get",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("target", Entry::String(target.as_bytes().to_vec())),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    let r = rsp.dict_find_dict(b"r");
    // `v` is the raw bencoded value (`i42e` is an int node, not a
    // string — `data_section` returns the wire bytes).
    assert_eq!(r.dict_find(b"v").data_section(), b"i42e");
    assert!(r.dict_find_string(b"token").is_valid());
}

fn crate_target_immutable(v: &[u8]) -> NodeId {
    bitdaemon_dht::item::item_target_id_immutable(v)
}

#[test]
fn put_rejects_bad_token_and_oversized() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let value = b"i1e".to_vec();
    let target = crate_target_immutable(&value);

    // bad token → 203 "invalid token"
    let pkt = query(
        "put",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("token", Entry::String(b"bad!".to_vec())),
            ("v", Entry::Preformatted(value.clone())),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    assert_eq!(err_code(&rsp).0, 203);

    // >1000-byte *bencoded* value → 205 "message too big"
    // (`v` must itself be valid bencode or the whole datagram is
    // dropped upstream).
    let mut big = b"1100:".to_vec();
    big.extend_from_slice(&[b'x'; 1100]);
    let t2 = crate_target_immutable(&big);
    let tok = a.generate_token(&pb, &t2);
    let pkt = query(
        "put",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("token", Entry::String(tok.to_vec())),
            ("v", Entry::Preformatted(big)),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    assert_eq!(err_code(&rsp).0, 205);
    let _ = target;
}

#[test]
fn put_mutable_signed_then_cas_and_seq_rules() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");

    let seed = bitdaemon_dht::ed25519::ed25519_create_seed();
    let (pk, sk) = bitdaemon_dht::ed25519::ed25519_create_keypair(&seed);
    let salt: &[u8] = b"";
    let value = b"4:test";
    let target = bitdaemon_dht::item::item_target_id_mutable(salt, &pk);
    let seq = 5i64;
    let sig = bitdaemon_dht::item::sign_mutable_item(value, salt, seq, &sk).unwrap();

    let put = |a: &mut DhtNode, seq: i64, sig: &[u8; 64], extra: &[(&str, Entry)]| {
        let mut args: Vec<(&str, Entry)> = vec![
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            (
                "token",
                Entry::String(a.generate_token(&pb, &target).to_vec()),
            ),
            ("v", Entry::Preformatted(value.to_vec())),
            ("k", Entry::String(pk.to_vec())),
            ("sig", Entry::String(sig.to_vec())),
            ("seq", Entry::Int(seq)),
        ];
        args.extend_from_slice(extra);
        let pkt = query("put", &args);
        let (_buf, rsp) = run_owned(a, &pkt, pb);
        rsp
    };

    // valid signed put → accepted
    let rsp = put(&mut a, seq, &sig, &[]);
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"r");

    // CAS mismatch → 301
    let rsp = put(
        &mut a,
        seq + 1,
        &sig_for(value, salt, seq + 1, &sk),
        &[("cas", Entry::Int(999))],
    );
    let (c, m) = err_code(&rsp);
    assert_eq!(c, 301);
    assert_eq!(m, b"CAS mismatch");

    // stale seq → 302
    let old_sig = sig_for(value, salt, seq - 1, &sk);
    let rsp = put(&mut a, seq - 1, &old_sig, &[]);
    let (c, m) = err_code(&rsp);
    assert_eq!(c, 302);
    assert_eq!(m, b"old sequence number");

    // negative seq → 203
    let rsp = put(&mut a, -1, &sig, &[]);
    let (c, m) = err_code(&rsp);
    assert_eq!(c, 203);
    assert_eq!(m, b"invalid (negative) sequence number");

    // bad signature → 206
    let mut bad_sig = sig_for(value, salt, seq + 2, &sk);
    bad_sig[0] ^= 0xff;
    let rsp = put(&mut a, seq + 2, &bad_sig, &[]);
    let (c, m) = err_code(&rsp);
    assert_eq!(c, 206);
    assert_eq!(m, b"invalid signature");
}

fn sig_for(v: &[u8], salt: &[u8], seq: i64, sk: &[u8; 64]) -> [u8; 64] {
    bitdaemon_dht::item::sign_mutable_item(v, salt, seq, sk).unwrap()
}

#[test]
fn get_mutable_honors_seq_filter() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let seed = bitdaemon_dht::ed25519::ed25519_create_seed();
    let (pk, sk) = bitdaemon_dht::ed25519::ed25519_create_keypair(&seed);
    let salt: &[u8] = b"";
    let value = b"4:test";
    let target = bitdaemon_dht::item::item_target_id_mutable(salt, &pk);
    let seq = 7i64;
    let sig = sig_for(value, salt, seq, &sk);
    let tok = a.generate_token(&pb, &target);
    let pkt = query(
        "put",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("token", Entry::String(tok.to_vec())),
            ("v", Entry::Preformatted(value.to_vec())),
            ("k", Entry::String(pk.to_vec())),
            ("sig", Entry::String(sig.to_vec())),
            ("seq", Entry::Int(seq)),
        ],
    );
    let _ = run_owned(&mut a, &pkt, pb);

    // get with seq >= stored → `seq` echoed, no `v`
    let pkt = query(
        "get",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("target", Entry::String(target.as_bytes().to_vec())),
            ("seq", Entry::Int(seq)),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    let r = rsp.dict_find_dict(b"r");
    assert_eq!(r.dict_find_int(b"seq").int_value(), seq);
    assert!(!r.dict_find(b"v").is_valid(), "v echoed despite seq match");

    // get with lower seq → full item
    let pkt = query(
        "get",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("target", Entry::String(target.as_bytes().to_vec())),
            ("seq", Entry::Int(seq - 1)),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    let r = rsp.dict_find_dict(b"r");
    assert_eq!(r.dict_find_string(b"v").string_value(), b"test");
    assert_eq!(r.dict_find_string(b"k").string_value(), &pk[..]);
}

#[test]
fn incoming_announce_event_fires_before_token_check() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let ih = bitdaemon_common::Sha1Hash::from_bytes(&[0x99u8; 20]).unwrap();
    let pkt = query(
        "announce_peer",
        &[
            ("id", Entry::String(sender_id().as_bytes().to_vec())),
            ("info_hash", Entry::String(ih.as_bytes().to_vec())),
            ("port", Entry::Int(6881)),
            ("token", Entry::String(b"bogus".to_vec())),
        ],
    );
    let (_buf, rsp) = run_owned(&mut a, &pkt, pb);
    assert_eq!(err_code(&rsp).0, 203);
    // upstream posts `dht_announce_alert` before the token check
    let saw = a
        .drain_events()
        .iter()
        .any(|e| matches!(e, DhtEvent::IncomingAnnounce { ih: h, .. } if *h == ih));
    assert!(saw, "announce alert lost");
}
