// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/dht_state.cpp` — the `.dht_state` resume blob:
//! `node-id` list (`nid ‖ addr` strings) and `nodes`/`nodes6`
//! compact endpoint lists.

use std::net::{IpAddr, SocketAddr};

use bitdaemon_bencode::{BdecodeNode, Entry, NodeType, bdecode_checked};
use bitdaemon_net::socket_io::{Cursor, read_address, read_endpoint_list, write_address};

use crate::node_id::NodeId;

/// `node_ids_t` — `(address, node_id)` pairs.
pub type NodeIds = Vec<(IpAddr, NodeId)>;

/// `dht_state` — serialized routing bootstrap state.
#[derive(Debug, Default, Clone)]
pub struct DhtState {
    /// `nids` — previously seen `(source ip, node id)` pairs.
    pub nids: NodeIds,
    /// `nodes` — v4 endpoints.
    pub nodes: Vec<SocketAddr>,
    /// `nodes6` — v6 endpoints.
    pub nodes6: Vec<SocketAddr>,
}

impl DhtState {
    /// `dht_state::clear`.
    pub fn clear(&mut self) {
        self.nids = Vec::new();
        self.nids.shrink_to_fit();
        self.nodes = Vec::new();
        self.nodes.shrink_to_fit();
        self.nodes6 = Vec::new();
        self.nodes6.shrink_to_fit();
    }
}

/// `extract_node_ids(e, key)` — reads the `key` member: either an
/// old-style 20-byte string or a list of `nid ‖ addr` strings
/// (20+4 v4, 20+16 v6).
#[must_use]
pub fn extract_node_ids(e: &BdecodeNode<'_>, key: &[u8]) -> NodeIds {
    if e.node_type() != NodeType::Dict {
        return Vec::new();
    }
    let mut ret = Vec::new();
    // old-style plain nid
    let old = e.dict_find_string_value(key, b"");
    if old.len() == 20 {
        if let Some(id) = NodeId::from_bytes(old) {
            ret.push((IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), id));
        }
        return ret;
    }
    let nids = e.dict_find_list(key);
    if nids.node_type() != NodeType::List {
        return ret;
    }
    for i in 0..nids.list_size() {
        let nid = nids.list_at(i);
        if nid.node_type() != NodeType::String {
            continue;
        }
        let s = nid.string_value();
        if s.len() < 20 {
            continue;
        }
        let Some(id) = NodeId::from_bytes(&s[..20]) else {
            continue;
        };
        let addr = match s.len() {
            24 => {
                let mut cur = Cursor::new(&s[20..]);
                read_address(&mut cur, false).ok()
            }
            36 => {
                let mut cur = Cursor::new(&s[20..]);
                read_address(&mut cur, true).ok()
            }
            _ => continue,
        };
        if let Some(addr) = addr {
            ret.push((addr, id));
        }
    }
    ret
}

/// `read_dht_state`.
#[must_use]
pub fn read_dht_state(e: &BdecodeNode<'_>) -> DhtState {
    let mut ret = DhtState::default();
    if e.node_type() != NodeType::Dict {
        return ret;
    }
    ret.nids = extract_node_ids(e, b"node-id");
    let nodes = e.dict_find_list(b"nodes");
    if nodes.node_type() == NodeType::List {
        ret.nodes = read_endpoint_list(&nodes);
    }
    let nodes6 = e.dict_find_list(b"nodes6");
    if nodes6.node_type() == NodeType::List {
        ret.nodes6 = read_endpoint_list(&nodes6);
    }
    ret
}

/// `read_dht_state` on raw bytes.
#[must_use]
pub fn read_dht_state_bytes(buf: &[u8]) -> Option<DhtState> {
    let e = bdecode_checked(buf).ok()?;
    Some(read_dht_state(&e))
}

/// `save_nodes` — compact endpoint strings.
fn save_nodes(nodes: &[SocketAddr]) -> Entry {
    Entry::List(
        nodes
            .iter()
            .map(|ep| {
                let mut v = Vec::new();
                bitdaemon_net::socket_io::write_endpoint(*ep, &mut v);
                Entry::String(v)
            })
            .collect(),
    )
}

/// `save_dht_state` — `{"node-id":[nid‖addr…], "nodes":[…],
/// "nodes6":[…]}`.
#[must_use]
pub fn save_dht_state(state: &DhtState) -> Entry {
    let mut ret = Entry::Dict(Default::default());
    let d = ret.dict_mut().expect("dict");
    let nids: Vec<Entry> = state
        .nids
        .iter()
        .map(|(a, id)| {
            let mut s = Vec::with_capacity(36);
            s.extend_from_slice(id.as_bytes());
            write_address(*a, &mut s);
            Entry::String(s)
        })
        .collect();
    d.insert(b"node-id".to_vec(), Entry::List(nids));
    if !state.nodes.is_empty() {
        d.insert(b"nodes".to_vec(), save_nodes(&state.nodes));
    }
    if !state.nodes6.is_empty() {
        d.insert(b"nodes6".to_vec(), save_nodes(&state.nodes6));
    }
    ret
}
