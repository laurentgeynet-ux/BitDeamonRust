// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Specialized traversals — the `find_data` / `get_peers` /
//! `get_item` / `sample_infohashes` / `put_data` subclasses that
//! turn `Traversal` into concrete DHT lookups.
//!
//! Upstream implements each as a `traversal_algorithm` subclass
//! overriding `invoke` (which `q`/`a` to send) and the observer's
//! `reply` (which `r` keys to consume). Here [`Lookup`] wraps
//! [`Traversal`], [`Lookup::invoke_entry`] builds the query, and
//! [`Lookup::on_reply`] extracts kind-specific response fields.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use bitdaemon_bencode::{BdecodeNode, Dictionary, Entry};
use bitdaemon_net::socket_io::{Cursor, read_endpoint};

use crate::node_id::{NodeId, distance_exp, generate_prefix_mask, generate_random_id};
use crate::traversal::{Traversal, TraverseHooks};

/// Which `q` this traversal issues (the subclass, upstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupKind {
    /// `find_node` / `get_peers` bootstrap traversal.
    FindData,
    /// `get_peers` (`get_peers`).
    GetPeers,
    /// `obfuscated_get_peers` — info_hash truncated to
    /// `shared_prefix + 3` bits per target (privacy).
    ObfuscatedGetPeers,
    /// `get` (BEP-44 item lookup).
    GetItem,
    /// `sample_infohashes` (BEP-51).
    SampleInfohashes,
    /// `put` — second phase after a `get_item`/`find_data`.
    Put,
    /// `bootstrap` (upstream `kademlia/refresh.cpp`) — a
    /// `get_peers` traversal targeted at our own node id; the
    /// `info_hash` is re-read from the node's *current* id on each
    /// invoke (id may rotate) and router seeds get `a.bs = 1`.
    Bootstrap,
}

/// Data extracted from one `r` reply, per [`LookupKind`].
#[derive(Debug, Default)]
pub struct ReplyData {
    /// responder's id (`r.id`).
    pub id: Option<NodeId>,
    /// `r.token` — write token (kept for `put`/`announce_peer`).
    pub token: Option<Vec<u8>>,
    /// `r.values` — compact peer list (`get_peers`).
    pub peers: Vec<SocketAddr>,
    /// `r.nodes` / `r.nodes2` — node lists to traverse into.
    pub nodes: Vec<(NodeId, SocketAddr)>,
    /// `r.interval` (sample_infohashes refresh hint, minutes).
    pub interval: Option<i64>,
    /// `r.num` (BEP-51: how many infohashes the node claims).
    pub num: Option<i64>,
    /// `r.samples` — BEP-51 20-byte infohash sample blob.
    pub samples: Vec<NodeId>,
    /// `r.v` — BEP-44 item payload (raw bencode slice).
    pub item: Option<Vec<u8>>,
    /// `r.k`, `r.seq`, `r.sig` — mutable-item fields.
    pub key: Option<Vec<u8>>,
    /// `r.seq`.
    pub seq: Option<i64>,
    /// `r.sig`.
    pub sig: Option<Vec<u8>>,
}

/// A `traversal_algorithm` subclass instance.
pub struct Lookup {
    /// the underlying search.
    pub algo: Traversal,
    /// which query it issues.
    pub kind: LookupKind,
    /// `m_write_tokens` (`find_data`) — upstream stores
    /// `node_id -> (target_ep, token)` so the announce phase
    /// survives `results` being cleared by `done()`.
    write_tokens: BTreeMap<NodeId, (SocketAddr, Vec<u8>)>,
    /// `m_noseeds` (`get_peers`).
    noseed: bool,
    /// this node's protocol (`m_protocol`) — picks `nodes`/`nodes6`.
    v6: bool,
}

impl Lookup {
    /// `find_data(node, target, cb)` — `seed` are routing-table
    /// `find_node` results (`flag_initial`).
    #[must_use]
    pub fn new(
        kind: LookupKind,
        target: NodeId,
        v6: bool,
        hooks: &TraverseHooks,
        seed: Vec<(NodeId, SocketAddr)>,
    ) -> Self {
        let mut algo = Traversal::new(target, hooks);
        for (id, ep) in seed {
            algo.add_entry(id, ep, crate::traversal::obs_flags::INITIAL, hooks);
        }
        Self {
            algo,
            kind,
            write_tokens: BTreeMap::new(),
            noseed: false,
            v6,
        }
    }

    /// `m_noseeds`.
    pub fn set_noseed(&mut self, noseed: bool) {
        self.noseed = noseed;
    }

    /// `m_write_tokens` — accumulated put tokens with the
    /// responder endpoint (`node_entry`).
    #[must_use]
    pub fn write_tokens(&self) -> &BTreeMap<NodeId, (SocketAddr, Vec<u8>)> {
        &self.write_tokens
    }

    /// `invoke()` — the query dict for `entry`. `current_id` is the
    /// node's *live* id — `bootstrap` re-reads it on every invoke
    /// like upstream `bootstrap::invoke` (the id may rotate mid-
    /// bootstrap); other kinds ignore it.
    ///
    /// Returns the query entry plus the `info_hash` actually put on
    /// the wire (`Some` for `get_peers`-family queries — obfuscated
    /// targets differ from the lookup target; `None` otherwise).
    /// Upstream reports that hash through
    /// `dht_observer::outgoing_get_peers`.
    #[must_use]
    pub fn invoke_entry(
        &self,
        entry: &crate::traversal::ObserverEntry,
        current_id: NodeId,
    ) -> (Entry, Option<NodeId>) {
        let target = if self.kind == LookupKind::Bootstrap {
            current_id
        } else {
            *self.algo_target()
        };
        let mut em = Dictionary::new();
        let mut am = Dictionary::new();
        let mut sent_hash = None;
        match self.kind {
            LookupKind::Bootstrap => {
                // upstream: always the *current* node id (it may
                // change mid-bootstrap), and flag_initial targets
                // get `a.bs = 1` so the router knows we're
                // bootstrapping, not collateral traffic.
                em.insert("q".into(), Entry::String(b"get_peers".to_vec()));
                am.insert(
                    "info_hash".into(),
                    Entry::String(target.as_bytes().to_vec()),
                );
                sent_hash = Some(target);
                if entry.flags & crate::traversal::obs_flags::INITIAL != 0 {
                    am.insert("bs".into(), Entry::Int(1));
                }
            }
            LookupKind::FindData => {
                em.insert("q".into(), Entry::String(b"find_node".to_vec()));
                am.insert("target".into(), Entry::String(target.as_bytes().to_vec()));
            }
            LookupKind::GetPeers | LookupKind::ObfuscatedGetPeers => {
                em.insert("q".into(), Entry::String(b"get_peers".to_vec()));
                let mut hash = target;
                if self.kind == LookupKind::ObfuscatedGetPeers
                    && let Some(id) = entry.id
                {
                    let shared_prefix = 160 - distance_exp(&id, &target);
                    let mask = generate_prefix_mask(shared_prefix + 3);
                    let mut obf = generate_random_id();
                    // obf = (rand & ~mask) | (target & mask)
                    let ob = obf.as_bytes_mut();
                    let tb = target.as_bytes();
                    let mb = mask.as_bytes();
                    for i in 0..20 {
                        ob[i] = (ob[i] & !mb[i]) | (tb[i] & mb[i]);
                    }
                    hash = obf;
                }
                sent_hash = Some(hash);
                am.insert("info_hash".into(), Entry::String(hash.as_bytes().to_vec()));
                if self.noseed {
                    am.insert("noseed".into(), Entry::Int(1));
                }
            }
            LookupKind::GetItem | LookupKind::SampleInfohashes | LookupKind::Put => {
                let q: &[u8] = match self.kind {
                    LookupKind::GetItem => b"get",
                    LookupKind::SampleInfohashes => b"sample_infohashes",
                    _ => b"put",
                };
                em.insert("q".into(), Entry::String(q.to_vec()));
                am.insert("target".into(), Entry::String(target.as_bytes().to_vec()));
            }
        }
        em.insert("a".into(), Entry::Dict(am));
        (Entry::Dict(em), sent_hash)
    }

    fn algo_target(&self) -> &NodeId {
        self.algo.target()
    }

    /// `reply()` per subclass — extract kind-specific fields and
    /// stash the write token under `(ep, token)`
    /// (`find_data_observer::reply` → `m_write_tokens`).
    /// `r` is the response `r` dict, `ep` the observer endpoint.
    pub fn on_reply(&mut self, r: &BdecodeNode<'_>, ep: SocketAddr) -> Option<ReplyData> {
        let id_e = r.dict_find_string(b"id");
        if !id_e.is_valid() || id_e.string_length() != 20 {
            return None; // upstream: timeout()
        }
        let mut out = ReplyData::default();
        let id = NodeId::from_bytes(id_e.string_value())?;
        out.id = Some(id);

        let tok = r.dict_find_string(b"token");
        if !!tok.is_valid() {
            let t = tok.string_value().to_vec();
            self.write_tokens.insert(id, (ep, t.clone()));
            out.token = Some(t);
        }

        match self.kind {
            LookupKind::GetPeers | LookupKind::ObfuscatedGetPeers => {
                let values = r.dict_find_list(b"values");
                if !!values.is_valid() {
                    for i in 0..values.list_size() {
                        let p = values.list_at(i);
                        if p.string_length() == 6 {
                            if let Ok(ep) = read_compact(p.string_value(), false) {
                                out.peers.push(ep);
                            }
                        } else if p.string_length() == 18 {
                            if let Ok(ep) = read_compact(p.string_value(), true) {
                                out.peers.push(ep);
                            }
                        }
                    }
                }
            }
            LookupKind::GetItem => {
                let v = r.dict_find(b"v");
                if !!v.is_valid() {
                    out.item = Some(v.data_section().to_vec());
                }
                let k = r.dict_find_string(b"k");
                if !!k.is_valid() {
                    out.key = Some(k.string_value().to_vec());
                }
                let q = r.dict_find_int(b"seq");
                if !!q.is_valid() {
                    out.seq = Some(q.int_value());
                }
                let s = r.dict_find_string(b"sig");
                if !!s.is_valid() {
                    out.sig = Some(s.string_value().to_vec());
                }
            }
            LookupKind::SampleInfohashes => {
                let iv = r.dict_find_int(b"interval");
                if !!iv.is_valid() {
                    out.interval = Some(iv.int_value());
                }
                let n = r.dict_find_int(b"num");
                if !!n.is_valid() {
                    out.num = Some(n.int_value());
                }
                let s = r.dict_find_string(b"samples");
                if !!s.is_valid() {
                    let b = s.string_value();
                    for chunk in b.chunks_exact(20) {
                        if let Some(id2) = NodeId::from_bytes(chunk) {
                            out.samples.push(id2);
                        }
                    }
                }
            }
            _ => {}
        }

        // `look_for_nodes` — only this node's protocol key
        let key: &[u8] = if self.v6 { b"nodes6" } else { b"nodes" };
        for key in [key] {
            let n = r.dict_find_string(key);
            if !n.is_valid() {
                continue;
            }
            let v6 = key == b"nodes2";
            let stride = if v6 { 38 } else { 26 };
            for chunk in n.string_value().chunks_exact(stride) {
                let Some(nid) = NodeId::from_bytes(&chunk[..20]) else {
                    continue;
                };
                if let Ok(ep) = read_compact(&chunk[20..], v6) {
                    out.nodes.push((nid, ep));
                }
            }
        }
        Some(out)
    }
}

/// Compact endpoint bytes → `SocketAddr` (6 v4 / 18 v6).
fn read_compact(b: &[u8], v6: bool) -> std::io::Result<SocketAddr> {
    let mut c = Cursor::new(b);
    read_endpoint(&mut c, v6)
}
