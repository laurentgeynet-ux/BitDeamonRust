// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Specialized traversals — `get_peers`/`get_item`/`find_node`
//! queries and reply extraction.

use std::net::SocketAddr;

use bitdaemon_bencode::{Dictionary, Entry, bdecode_checked};
use bitdaemon_dht::lookups::{Lookup, LookupKind};
use bitdaemon_dht::node_id::NodeId;
use bitdaemon_dht::traversal::TraverseHooks;

fn hooks() -> TraverseHooks {
    TraverseHooks {
        bucket_size: 8,
        aggressive: false,
        restrict_search_ips: true,
        branch_factor: 3,
        router_nodes: vec![],
    }
}

fn id(v: u8) -> NodeId {
    NodeId::from_bytes(&[v; 20]).unwrap()
}

fn ep_any() -> SocketAddr {
    "9.9.9.9:6881".parse().unwrap()
}

fn reply_entry(build: impl FnOnce(&mut Dictionary)) -> Vec<u8> {
    let mut r = Entry::Dict(Default::default());
    build(r.dict_mut().unwrap());
    let mut msg = Entry::Dict(Default::default());
    msg.dict_mut().unwrap().insert("r".into(), r);
    bitdaemon_bencode::bencode(&msg)
}

#[test]
fn get_peers_invoke_shape() {
    let mut l = Lookup::new(LookupKind::GetPeers, id(0xaa), false, &hooks(), vec![]);
    l.set_noseed(true);
    let entry =
        bitdaemon_dht::traversal::ObserverEntry::new(id(0xbb), "8.8.8.8:6881".parse().unwrap());
    let (e, _) = l.invoke_entry(&entry, id(0x99));
    let d = e.dict().unwrap();
    assert_eq!(d[b"q".as_slice()].string().unwrap(), b"get_peers");
    let a = d[b"a".as_slice()].dict().unwrap();
    assert_eq!(
        a[b"info_hash".as_slice()].string().unwrap(),
        id(0xaa).as_bytes()
    );
    assert_eq!(a[b"noseed".as_slice()].integer().unwrap(), 1);
}

#[test]
fn find_node_invoke_shape() {
    let l = Lookup::new(LookupKind::FindData, id(0xcc), false, &hooks(), vec![]);
    let entry =
        bitdaemon_dht::traversal::ObserverEntry::new(id(0xdd), "8.8.8.8:6881".parse().unwrap());
    let (e, _) = l.invoke_entry(&entry, id(0x99));
    let d = e.dict().unwrap();
    assert_eq!(d[b"q".as_slice()].string().unwrap(), b"find_node");
    assert_eq!(
        d[b"a".as_slice()].dict().unwrap()[b"target".as_slice()]
            .string()
            .unwrap(),
        id(0xcc).as_bytes()
    );
}

#[test]
fn reply_extracts_peers_nodes_token() {
    let mut l = Lookup::new(LookupKind::GetPeers, id(0xaa), false, &hooks(), vec![]);
    let nid = id(0x11);
    let peer: SocketAddr = "1.2.3.4:6881".parse().unwrap();
    let node_ep: SocketAddr = "5.6.7.8:51413".parse().unwrap();

    let buf = reply_entry(|r| {
        r.insert("id".into(), Entry::String(nid.as_bytes().to_vec()));
        r.insert("token".into(), Entry::String(b"tok".to_vec()));
        r.insert(
            "values".into(),
            Entry::List(vec![Entry::String(
                bitdaemon_net::socket_io::endpoint_to_bytes(std::net::SocketAddr::V4(match peer {
                    SocketAddr::V4(v) => v,
                    _ => unreachable!(),
                })),
            )]),
        );
        let mut nodes = Vec::new();
        nodes.extend_from_slice(id(0x22).as_bytes());
        nodes.extend_from_slice(&bitdaemon_net::socket_io::endpoint_to_bytes(node_ep));
        r.insert("nodes".into(), Entry::String(nodes));
    });

    let root = bdecode_checked(&buf).unwrap();
    let r = root.dict_find_dict(b"r");
    let out = l.on_reply(&r, node_ep).expect("reply parsed");
    assert_eq!(out.id, Some(nid));
    assert_eq!(out.token.as_deref(), Some(b"tok".as_slice()));
    assert_eq!(out.peers, vec![peer]);
    assert_eq!(out.nodes, vec![(id(0x22), node_ep)]);
    // write token recorded for a later put/announce
    assert_eq!(l.write_tokens()[&nid].1, b"tok".to_vec());
}

#[test]
fn reply_missing_id_is_timeout() {
    let mut l = Lookup::new(LookupKind::FindData, id(0xaa), false, &hooks(), vec![]);
    let buf = reply_entry(|r| {
        r.insert("token".into(), Entry::String(b"t".to_vec()));
    });
    let root = bdecode_checked(&buf).unwrap();
    let r = root.dict_find_dict(b"r");
    assert!(l.on_reply(&r, ep_any()).is_none());
}

#[test]
fn sample_infohashes_reply() {
    let mut l = Lookup::new(
        LookupKind::SampleInfohashes,
        id(0xaa),
        false,
        &hooks(),
        vec![],
    );
    let buf = reply_entry(|r| {
        r.insert("id".into(), Entry::String(id(0x33).as_bytes().to_vec()));
        r.insert("interval".into(), Entry::Int(25));
        r.insert("num".into(), Entry::Int(1000));
        let mut s = Vec::new();
        s.extend_from_slice(id(0x44).as_bytes());
        s.extend_from_slice(id(0x55).as_bytes());
        r.insert("samples".into(), Entry::String(s));
    });
    let root = bdecode_checked(&buf).unwrap();
    let out = l.on_reply(&root.dict_find_dict(b"r"), ep_any()).unwrap();
    assert_eq!(out.interval, Some(25));
    assert_eq!(out.num, Some(1000));
    assert_eq!(out.samples, vec![id(0x44), id(0x55)]);
}

#[test]
fn bootstrap_uses_live_id_and_bs_flag() {
    // upstream refresh.cpp: bootstrap get_peers targets the node's
    // *current* id at invoke time — passed through
    // `make_id_secret` (first 12 bytes kept, last 8 obfuscated) —
    // and marks router (flag_initial) entries with a.bs = 1.
    let stored = id(0xaa);
    let live = id(0xbb);
    let l = Lookup::new(LookupKind::Bootstrap, stored, false, &hooks(), vec![]);
    let mut entry =
        bitdaemon_dht::traversal::ObserverEntry::new(id(0xcc), "8.8.8.8:6881".parse().unwrap());
    entry.flags = bitdaemon_dht::traversal::obs_flags::INITIAL;
    let (e, _) = l.invoke_entry(&entry, live);
    let d = e.dict().unwrap();
    assert_eq!(d[b"q".as_slice()].string().unwrap(), b"get_peers");
    let a = d[b"a".as_slice()].dict().unwrap();
    let wire = a[b"info_hash".as_slice()].string().unwrap();
    assert_eq!(&wire[..12], &live.as_bytes()[..12], "prefix kept");
    assert_ne!(&wire[12..], &live.as_bytes()[12..], "make_id_secret tail");
    assert_eq!(a[b"bs".as_slice()].integer().unwrap(), 1);
    // non-initial entries get no bs
    let entry2 =
        bitdaemon_dht::traversal::ObserverEntry::new(id(0xdd), "1.1.1.1:6881".parse().unwrap());
    let (e2, _) = l.invoke_entry(&entry2, live);
    let a2 = e2.dict().unwrap()[b"a".as_slice()].dict().unwrap();
    assert!(a2.get(b"bs".as_slice()).is_none());
}
