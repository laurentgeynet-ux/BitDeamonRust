// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `dht_node` end-to-end — two nodes exchanging datagrams
//! through their injected outboxes (no real sockets).

use std::net::SocketAddr;

use bitdaemon_bencode::{Entry, bdecode_checked};
use bitdaemon_dht::dht_node::{DhtEvent, DhtNode};
use bitdaemon_dht::dht_settings::DhtSettings;
use bitdaemon_dht::lookups::LookupKind;
use bitdaemon_dht::node_id::{NodeId, generate_id_impl};

fn node(ip: &str) -> (DhtNode, SocketAddr) {
    let addr: SocketAddr = format!("{ip}:6881").parse().unwrap();
    let id = generate_id_impl(&addr.ip(), 1);
    (
        DhtNode::new(Some(id), addr.ip(), false, DhtSettings::default()),
        addr,
    )
}

/// pump datagrams between two nodes until quiescent.
fn pump(a: &mut DhtNode, b: &mut DhtNode, pa: SocketAddr, pb: SocketAddr) {
    for _ in 0..50 {
        let mut moved = false;
        while let Some((pkt, to)) = a.poll() {
            moved = true;
            let _ = to;
            if let Some((rsp, _)) = b.incoming(&pkt, pa, None, true) {
                let _ = a.incoming(&rsp, pb, None, true);
            }
        }
        while let Some((pkt, _)) = b.poll() {
            moved = true;
            if let Some((rsp, _)) = a.incoming(&pkt, pb, None, true) {
                let _ = b.incoming(&rsp, pa, None, true);
            }
        }
        if !moved {
            break;
        }
    }
}

#[test]
fn ping_reply() {
    let (mut a, _pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    // hand-build a ping query
    let mut e = Entry::Dict(Default::default());
    {
        let d = e.dict_mut().unwrap();
        d.insert("y".into(), Entry::String(b"q".to_vec()));
        d.insert("q".into(), Entry::String(b"ping".to_vec()));
        d.insert("t".into(), Entry::String(b"aa".to_vec()));
        let mut av = Entry::Dict(Default::default());
        av.dict_mut()
            .unwrap()
            .insert("id".into(), Entry::String(a.id.as_bytes().to_vec()));
        d.insert("a".into(), av);
    }
    let (pkt, _to) = a.handle_query(
        &bdecode_checked(&bitdaemon_bencode::bencode(&e)).unwrap(),
        pb,
        b"aa".to_vec(),
        None,
    );
    let rsp = bdecode_checked(&pkt).unwrap();
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"r");
    let r = rsp.dict_find_dict(b"r");
    assert_eq!(r.dict_find_string(b"id").string_value(), a.id.as_bytes());
}

#[test]
fn get_peers_round_trip() {
    let (mut a, pa) = node("1.2.3.4");
    let (mut b, pb) = node("5.6.7.8");
    // a knows b in its table so the lookup can seed
    a.table.node_seen(b.id, pb, 10);

    let ih = NodeId::from_bytes(&[0xabu8; 20]).unwrap();
    let li = a.new_lookup(LookupKind::GetPeers, ih);
    let _ = pa;

    // b should receive a get_peers query; pump the exchange
    pump(&mut a, &mut b, pa, pb);

    let events = a.drain_events();
    let saw_data = events
        .iter()
        .any(|e| matches!(e, DhtEvent::Data { lookup, .. } if *lookup == li));
    assert!(saw_data, "expected reply data event: {events:?}");
}

#[test]
fn announce_requires_token() {
    let (mut a, pa) = node("1.2.3.4");
    let (_b, pb) = node("5.6.7.8");
    let ih = bitdaemon_common::Sha1Hash::from_bytes(&[0x55u8; 20]).unwrap();

    // announce with a bad token → error reply
    let mut e = Entry::Dict(Default::default());
    {
        let d = e.dict_mut().unwrap();
        d.insert("y".into(), Entry::String(b"q".to_vec()));
        d.insert("q".into(), Entry::String(b"announce_peer".to_vec()));
        d.insert("t".into(), Entry::String(b"aa".to_vec()));
        let mut av = Entry::Dict(Default::default());
        {
            let am = av.dict_mut().unwrap();
            am.insert("info_hash".into(), Entry::String(ih.as_bytes().to_vec()));
            am.insert("token".into(), Entry::String(b"bad!".to_vec()));
            am.insert("port".into(), Entry::Int(6881));
            am.insert("id".into(), Entry::String(a.id.as_bytes().to_vec()));
        }
        d.insert("a".into(), av);
    }
    let buf = bitdaemon_bencode::bencode(&e);
    let (pkt, _) = a.handle_query(&bdecode_checked(&buf).unwrap(), pb, b"aa".to_vec(), None);
    let rsp = bdecode_checked(&pkt).unwrap();
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"e");

    // correct token → ok
    let tok = a.generate_token(&pb, &ih);
    let mut e = Entry::Dict(Default::default());
    {
        let d = e.dict_mut().unwrap();
        d.insert("y".into(), Entry::String(b"q".to_vec()));
        d.insert("q".into(), Entry::String(b"announce_peer".to_vec()));
        let mut av = Entry::Dict(Default::default());
        {
            let am = av.dict_mut().unwrap();
            am.insert("info_hash".into(), Entry::String(ih.as_bytes().to_vec()));
            am.insert("token".into(), Entry::String(tok.to_vec()));
            am.insert("port".into(), Entry::Int(51413));
            // upstream requires `a.id` (20 B) on every query
            am.insert("id".into(), Entry::String(a.id.as_bytes().to_vec()));
        }
        d.insert("a".into(), av);
    }
    let buf = bitdaemon_bencode::bencode(&e);
    let (pkt, _) = a.handle_query(&bdecode_checked(&buf).unwrap(), pb, b"bb".to_vec(), None);
    let rsp = bdecode_checked(&pkt).unwrap();
    assert_eq!(rsp.dict_find_string_value(b"y", b""), b"r");
    let _ = pa;
}

#[test]
fn announce_write_phase() {
    let (mut a, pa) = node("1.2.3.4");
    let (mut b, pb) = node("5.6.7.8");
    let ih = bitdaemon_common::Sha1Hash::from_bytes(&[0x77u8; 20]).unwrap();

    // a announces to b using the token b would have issued
    let mut targets = std::collections::BTreeMap::new();
    targets.insert(b.id, pb);
    let token = b.generate_token(&pa, &ih);
    a.announce(&ih, 6881, false, false, &targets, |_| Some(token.to_vec()));

    pump(&mut a, &mut b, pa, pb);

    // b stored a as a peer on ih
    let r = b.storage.get_peers(&ih, false, false, &pa.ip());
    assert_eq!(
        r.peers.unwrap_or_default().len(),
        6,
        "expected one compact v4 peer"
    );
    let _ = pb;
}

#[test]
fn incoming_upstream_get_peers_gets_reply() {
    // packet captured verbatim from lt_client dhtseed (alert log):
    // { 'a': { 'id': <20>, 'info_hash': <20>, 'noseed': 1 },
    //   'q': 'get_peers', 't': '{6', 'v': 'LT\x02\x12', 'y': 'q' }
    let mut n = DhtNode::new(
        None,
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        false,
        DhtSettings::default(),
    );
    let mut pkt = Vec::new();
    pkt.extend_from_slice(b"d1:ad2:id20:");
    pkt.extend_from_slice(&[0xf6u8; 20]);
    pkt.extend_from_slice(b"9:info_hash20:");
    pkt.extend_from_slice(&[0x33u8; 20]);
    pkt.extend_from_slice(b"6:noseedi1ee1:q9:get_peers1:t2:{61:v4:LT\x02\x121:y1:qe");
    let from: std::net::SocketAddr = "127.0.0.1:6940".parse().unwrap();
    let reply = n.incoming(&pkt, from, None, true);
    assert!(reply.is_some(), "no reply to get_peers");
    let (buf, to) = reply.unwrap();
    assert_eq!(to, from);
    let s = String::from_utf8_lossy(&buf);
    assert!(s.contains("1:y1:r"), "not a reply: {s}");
    assert!(s.contains("1:t2:{6"), "tid not echoed: {s}");
}

#[test]
fn router_seeded_lookup_yields_announce_tokens() {
    // the real-swarm path: a lookup seeded only by a router node
    // (NO_ID observer) must still surface the responder's write
    // token in `DhtEvent::Done` so the session can announce.
    let (mut a, pa) = node("1.2.3.4");
    let (mut b, pb) = node("5.6.7.8");
    a.table.add_router_node(pb);

    let ih = NodeId::from_bytes(&[0xabu8; 20]).unwrap();
    let li = a.new_lookup(LookupKind::GetPeers, ih);

    pump(&mut a, &mut b, pa, pb);

    let events = a.drain_events();
    let done = events
        .iter()
        .find_map(|e| match e {
            DhtEvent::Done { lookup, tokens } if *lookup == li => Some(tokens),
            _ => None,
        })
        .expect("lookup never finished");
    assert_eq!(done.len(), 1, "expected one write token: {events:?}");
    assert_eq!(done[0].0, b.id, "token under responder id");
    assert_eq!(done[0].1, pb, "token endpoint");
    assert!(!done[0].2.is_empty(), "empty token");
}
