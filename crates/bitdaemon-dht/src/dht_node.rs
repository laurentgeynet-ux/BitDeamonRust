// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `dht::node` — the orchestrator that wires `RoutingTable`,
//! `RpcManager`, `DhtStorage` and `Lookup`s together.
//!
//! Upstream `node.cpp` owns sockets and posts through
//! `udp_socket`; here the wire side is injected: [`DhtNode::poll`]
//! drains outgoing datagrams the caller forwards to a
//! `DatagramSocket`(bitdaemon_net::udp::DatagramSocket), and
//! [`DhtNode::incoming`] feeds received datagrams back. This is the
//! seam OnionBit uses to run DHT over anonymous circuits.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use bitdaemon_bencode::{BdecodeNode, Dictionary, Entry, bdecode_checked};
use bitdaemon_net::socket_io::endpoint_to_bytes;
use sha1::{Digest as _, Sha1};

use crate::dht_settings::DhtSettings;
use crate::dht_storage::{DhtStorage, PutMutable};
use crate::lookups::{Lookup, LookupKind, ReplyData};
use crate::node_id::{NodeId, generate_id_impl};
use crate::routing_table::RoutingTable;
use crate::rpc::{Expiry, Incoming, RpcManager};
use crate::traversal::TraverseHooks;
use bitdaemon_common::Sha1Hash;

/// `write_token_size` upstream.
const WRITE_TOKEN_SIZE: usize = 4;
/// queue of outgoing datagrams drained by the caller.
type Outbox = Vec<(Vec<u8>, SocketAddr)>;

/// What [`DhtNode::handle_query`] produced for a query.
#[derive(Debug)]
pub enum QueryOutcome {
    /// a reply entry to encode+send.
    Reply(Entry),
    /// protocol error → send `y:"e"`.
    Error(Vec<u8>, String),
}

/// `dht_lookup`-style event surfaced to the caller.
#[derive(Debug)]
pub enum DhtEvent {
    /// lookup `lookup` got data (`get_peers` peers, `get` item…).
    Data { lookup: usize, data: Box<ReplyData> },
    /// lookup finished — carries `(node id, endpoint, write
    /// token)` triples for the announce/put phase (upstream keeps
    /// `m_write_tokens` on the traversal; we hand them over before
    /// the lookup is dropped).
    Done {
        lookup: usize,
        tokens: Vec<(NodeId, SocketAddr, Vec<u8>)>,
    },
    /// `direct_traversal` reply — a `direct_request` got a response;
    /// `response` is the raw `r`/`e` dict bytes (`data_section`).
    Direct {
        /// the tag `direct_request` returned.
        tag: u64,
        /// responder id when the reply carries a valid `r.id`.
        id: Option<NodeId>,
        /// the endpoint the request was sent to (also set on
        /// error/timeout — upstream reports `addr` either way).
        ep: SocketAddr,
        /// raw `r`/`e` dict payload.
        response: Vec<u8>,
        /// true when the reply was `y == "e"` or the transaction
        /// timed out (upstream `direct_observer::timeout`).
        failed: bool,
    },
    /// `dht_observer::get_peers` — an inbound `get_peers` query hit
    /// our node (`dht_get_peers_alert` upstream).
    IncomingGetPeers {
        /// queried info-hash.
        ih: Sha1Hash,
    },
    /// `dht_observer::announce` — an inbound `announce_peer` was
    /// accepted (`dht_announce_alert` upstream).
    IncomingAnnounce {
        /// announced info-hash.
        ih: Sha1Hash,
        /// announcer's address.
        ip: IpAddr,
        /// listen port it claims (source port when `implied_port`).
        port: u16,
    },
    /// `dht_observer::outgoing_get_peers` — our traversal just sent
    /// a `get_peers` query (`dht_outgoing_get_peers_alert` upstream).
    OutgoingGetPeers {
        /// the traversal's target info-hash.
        target: Sha1Hash,
        /// the hash actually put on the wire (obfuscated for
        /// `ObfuscatedGetPeers`).
        sent_target: Sha1Hash,
        /// node the query went to.
        ep: SocketAddr,
    },
    /// `dht_logger` incoming datagram — raw payload for
    /// `dht_pkt_alert` (outgoing direction is emitted by the
    /// caller's send loop, which owns the datagram).
    Pkt {
        /// remote node.
        ep: SocketAddr,
        /// raw datagram bytes.
        bytes: Vec<u8>,
    },
    /// `dht_observer::log` — a `dht_log_alert` line; `module` is
    /// upstream `dht_module_t` (0 tracker, 1 node, 2 routing_table,
    /// 3 rpc_manager, 4 traversal).
    Log {
        /// `dht_module_t`.
        module: u8,
        /// message.
        msg: String,
    },
}

/// `dht::node` — one address-family's Kademlia node.
pub struct DhtNode {
    /// `m_id` — this node's id (BEP-42).
    pub id: NodeId,
    /// `m_table`.
    pub table: RoutingTable,
    /// `m_rpc`.
    pub rpc: RpcManager,
    /// `m_storage`.
    pub storage: DhtStorage,
    /// `m_settings`.
    pub settings: DhtSettings,
    /// v6 node (`m_protocol`).
    v6: bool,
    /// `m_secret[0/1]` — write-token secrets.
    secret: [[u8; 32]; 2],
    /// active traversals; tag = `idx` in this vec.
    lookups: Vec<Option<Lookup>>,
    /// `m_last_bucket_refresh`-ish tick state is caller-driven.
    outbox: Outbox,
    /// `direct_request` tag counter (bit 63 set on every tag).
    direct_tag: u64,
    /// events drained by the caller (`dht_observer` upstream).
    events: Vec<DhtEvent>,
    /// `external_ip` votes collected from `ip` keys in incoming
    /// messages (drained by the session into `ip_voter`).
    ip_votes: Vec<std::net::IpAddr>,
    /// `m_blocker` — per-source rate limit, first gate on every
    /// inbound datagram (ADR-0021).
    blocker: crate::dos_blocker::DosBlocker,
    /// `m_last_self_refresh` — last `node::tick` self-id lookup.
    last_self_refresh: Option<std::time::Instant>,
}

impl DhtNode {
    /// `node(udp protocol, sock, sett, node_id, observer, cnt,
    /// state)` — `node_id` re-derived if empty.
    pub fn new(id: Option<NodeId>, bind_ip: IpAddr, v6: bool, settings: DhtSettings) -> Self {
        let our_id = id.unwrap_or_else(|| generate_id_impl(&bind_ip, 0));
        let mut secret = [[0u8; 32]; 2];
        bitdaemon_common::random_bytes(&mut secret[0]);
        bitdaemon_common::random_bytes(&mut secret[1]);
        Self {
            id: our_id,
            table: RoutingTable::new(our_id, v6, 8, settings.clone()),
            rpc: RpcManager::new(),
            storage: DhtStorage::new(settings.clone(), vec![our_id]),
            settings,
            v6,
            secret,
            lookups: Vec::new(),
            direct_tag: 0,
            outbox: Vec::new(),
            events: Vec::new(),
            ip_votes: Vec::new(),
            blocker: crate::dos_blocker::DosBlocker::new(),
            last_self_refresh: None,
        }
    }

    fn hooks(&self) -> TraverseHooks {
        TraverseHooks {
            bucket_size: self.table.bucket_size(),
            aggressive: self.settings.aggressive_lookups,
            restrict_search_ips: self.settings.restrict_search_ips,
            branch_factor: self.settings.search_branching as i8,
            router_nodes: self.table.router_nodes().copied().collect(),
        }
    }

    /// Outgoing datagrams — the caller forwards them to the
    /// transport. `None` while empty.
    pub fn poll(&mut self) -> Option<(Vec<u8>, SocketAddr)> {
        self.outbox.pop()
    }

    /// Drain observer-level events (lookup data/done).
    pub fn drain_events(&mut self) -> Vec<DhtEvent> {
        std::mem::take(&mut self.events)
    }

    /// `generate_token` — first 4 bytes of
    /// `sha1(addr_str + secret[0] + info_hash)`.
    pub fn generate_token(&self, addr: &SocketAddr, ih: &Sha1Hash) -> [u8; WRITE_TOKEN_SIZE] {
        let mut h = Sha1::new();
        h.update(addr.ip().to_string().as_bytes());
        h.update(self.secret[0]);
        h.update(ih.as_bytes());
        let f = h.finalize();
        [f[0], f[1], f[2], f[3]]
    }

    /// `verify_token` — accepts either secret generation.
    pub fn verify_token(&self, token: &[u8], ih: &Sha1Hash, addr: &SocketAddr) -> bool {
        if token.len() != WRITE_TOKEN_SIZE {
            return false;
        }
        self.secret.iter().any(|s| {
            let mut h = Sha1::new();
            h.update(addr.ip().to_string().as_bytes());
            h.update(s);
            h.update(ih.as_bytes());
            h.finalize()[..WRITE_TOKEN_SIZE] == *token
        })
    }

    /// `rotate_secrets()` — promote secret\[0\] → secret\[1\].
    pub fn rotate_secrets(&mut self) {
        self.secret[1] = self.secret[0];
        bitdaemon_common::random_bytes(&mut self.secret[0]);
    }

    /// Start a new lookup — seeds from `find_node` like
    /// `find_data::start`, then invokes `branch_factor` requests.
    /// Returns the lookup index (the `tag` namespace).
    pub fn new_lookup(&mut self, kind: LookupKind, target: NodeId) -> usize {
        let seed = self
            .table
            .find_node(&target, true, 0)
            .into_iter()
            .filter_map(|n| n.id.map(|i| (i, n.endpoint)))
            .collect();
        let mut l = Lookup::new(kind, target, self.v6, &self.hooks(), seed);
        // `find_data::start` also seeds `m_router_nodes` with
        // no-id observers (flag_initial | no_id).
        for ep in self.table.router_nodes().copied().collect::<Vec<_>>() {
            l.algo.add_entry(
                self.id,
                ep,
                crate::traversal::obs_flags::INITIAL | crate::traversal::obs_flags::NO_ID,
                &self.hooks(),
            );
        }
        self.lookups.push(Some(l));
        let idx = self.lookups.len() - 1;
        self.drive(idx);
        idx
    }

    /// Pump one lookup — invoke entries through `rpc`, pushing
    /// encoded datagrams into `outbox`.
    fn drive(&mut self, idx: usize) {
        if self.lookups.get(idx).is_none() {
            return;
        }
        let hooks = self.hooks();
        let our_id = self.id;
        let read_only = self.settings.read_only;
        let v6 = self.v6;
        // build the query dicts under the lookup borrow, then send
        let mut to_send: Vec<(Entry, SocketAddr, u64, Option<NodeId>)> = Vec::new();
        {
            let Some(l) = self.lookups.get_mut(idx).and_then(Option::as_mut) else {
                return;
            };
            let mut picked: Vec<(usize, SocketAddr)> = Vec::new();
            l.algo.add_requests(&hooks, |i, entry| {
                picked.push((i, entry.ep));
                true
            });
            for (i, ep) in picked {
                if let Some(entry) = l.algo.results().get(i) {
                    let (e, sent) = l.invoke_entry(entry, our_id);
                    to_send.push((e, ep, pack_tag(idx, i), sent));
                }
            }
        }
        let rpc = &mut self.rpc;
        let outbox = &mut self.outbox;
        let events = &mut self.events;
        for (e, ep, tag, sent) in to_send {
            let Some(l) = self.lookups.get(idx).and_then(Option::as_ref) else {
                continue;
            };
            tracing::trace!(%ep, kind = ?l.kind, "dht invoke");
            // `dht_observer::outgoing_get_peers` — upstream hooks
            // `get_peers::invoke`/`obfuscated_get_peers::invoke`.
            if matches!(
                l.kind,
                LookupKind::GetPeers | LookupKind::ObfuscatedGetPeers
            ) && let Some(sent_target) = sent
            {
                events.push(DhtEvent::OutgoingGetPeers {
                    target: *l.algo.target(),
                    sent_target,
                    ep,
                });
            }
            rpc.invoke(e, ep, tag, our_id, read_only, v6, |e2, t| {
                outbox.push((bitdaemon_bencode::bencode(&e2), t));
                true
            });
        }
    }

    /// `incoming(m)` — feed a raw datagram.
    /// drain pending external-ip votes.
    pub fn drain_ip_votes(&mut self) -> Vec<std::net::IpAddr> {
        std::mem::take(&mut self.ip_votes)
    }

    pub fn incoming(&mut self, buf: &[u8], from: SocketAddr) -> Option<(Vec<u8>, SocketAddr)> {
        // `m_blocker.incoming` — drop flood sources before parsing
        // (upstream calls it in `node::incoming` / `on_udp`, pre-bdecode).
        if !self.blocker.incoming(from.ip(), std::time::Instant::now()) {
            tracing::debug!(%from, "dht: dropping datagram from rate-limited source");
            self.events.push(DhtEvent::Log {
                module: 1,
                msg: format!("dropping datagram from rate-limited source {from}"),
            });
            return None;
        }
        // `dht_logger` — every inbound datagram surfaces as
        // `dht_pkt_alert` when the dht_log category is enabled.
        self.events.push(DhtEvent::Pkt {
            ep: from,
            bytes: buf.to_vec(),
        });
        let root = bdecode_checked(buf).ok()?;
        // upstream `dht::node::on_bdecode` — a top-level `ip` key
        // is the peer's view of our external address; vote on it.
        let ip = root.dict_find_string_value(b"ip", b"");
        match ip.len() {
            4 => {
                let mut a = [0u8; 4];
                a.copy_from_slice(ip);
                self.ip_votes.push(std::net::IpAddr::from(a));
            }
            16 => {
                let mut a = [0u8; 16];
                a.copy_from_slice(ip);
                self.ip_votes.push(std::net::IpAddr::from(a));
            }
            _ => {}
        }
        let y = root.dict_find_string_value(b"y", b"");
        let tid = root.dict_find_string_value(b"t", b"").to_vec();
        match y {
            b"q" => Some(self.handle_query(&root, from, tid)),
            b"r" | b"e" => {
                self.handle_reply(&root, from, &tid, y == b"e");
                None
            }
            _ => None,
        }
    }

    /// Reply path — route to rpc then to the owning lookup.
    fn handle_reply(
        &mut self,
        root: &BdecodeNode<'_>,
        from: SocketAddr,
        tid: &[u8],
        is_error: bool,
    ) {
        let r = root.dict_find_dict(b"r");
        let id_e = r.dict_find_string(b"id");
        let resp_id = if id_e.is_valid() && id_e.string_length() == 20 {
            NodeId::from_bytes(id_e.string_value())
        } else {
            None
        };
        let inc = self
            .rpc
            .incoming(from, tid, is_error, resp_id, self.settings.enforce_node_id);
        tracing::trace!(
            %from,
            tid = ?String::from_utf8_lossy(tid),
            variant = match &inc {
                Incoming::Reply { .. } => "reply",
                Incoming::Error { .. } => "error",
                Incoming::Ignored => "ignored",
            },
            "dht reply classified"
        );
        match inc {
            Incoming::Reply { tag, id } => {
                self.table.node_seen(id, from, 0);
                if tag & DIRECT_BIT != 0 {
                    self.events.push(DhtEvent::Direct {
                        tag,
                        id: Some(id),
                        ep: from,
                        response: r.data_section().to_vec(),
                        failed: false,
                    });
                    return;
                }
                let (li, oi) = unpack_tag(tag);
                // the observer endpoint goes into `m_write_tokens`
                // (`find_data_observer::reply` uses `o->target_ep()`).
                let obs_ep = self
                    .lookups
                    .get(li)
                    .and_then(Option::as_ref)
                    .and_then(|l| l.algo.results().get(oi))
                    .map(|o| o.ep)
                    .unwrap_or(from);
                match self
                    .lookups
                    .get_mut(li)
                    .and_then(Option::as_mut)
                    .map(|l| (l.on_reply(&r, obs_ep), l.kind))
                {
                    Some((Some(data), _)) => self.lookup_reply(li, oi, data),
                    other => {
                        tracing::trace!(li, oi, kind = ?other.map(|t| t.1), "dht reply produced no data")
                    }
                }
            }
            Incoming::Error { tag } => {
                if tag & DIRECT_BIT != 0 {
                    self.events.push(DhtEvent::Direct {
                        tag,
                        id: None,
                        ep: from,
                        response: Vec::new(),
                        failed: true,
                    });
                    return;
                }
                let (li, oi) = unpack_tag(tag);
                self.lookup_failed(li, oi, 0);
            }
            Incoming::Ignored => {}
        }
    }

    /// feed `ReplyData` into the traversal + surface events.
    fn lookup_reply(&mut self, li: usize, oi: usize, data: ReplyData) {
        let hooks = self.hooks();
        // traverse into discovered nodes
        let nodes = data.nodes.clone();
        let resp_id = data.id;
        self.events.push(DhtEvent::Data {
            lookup: li,
            data: Box::new(data),
        });
        let table = &mut self.table;
        if let Some(Some(l)) = self.lookups.get_mut(li) {
            // `observer::reply` — the responder's id lands on the
            // observer so `NO_ID` router entries become real nodes
            // for `take_write_tokens` (announce phase).
            if let Some(id) = resp_id {
                l.algo.set_id(oi, id);
            }
            tracing::trace!(li, nodes = nodes.len(), "dht traverse nodes");
            for (id, ep) in nodes {
                l.algo.traverse(id, ep, &hooks, |i2, e2| {
                    table.heard_about(i2, e2);
                });
            }
        }
        // mark the observer finished → schedules more requests
        self.finish_entry(li, oi);
    }

    fn finish_entry(&mut self, li: usize, oi: usize) {
        let hooks = self.hooks();
        let our_id = self.id;
        let mut more: Vec<(Entry, SocketAddr, u64, Option<NodeId>)> = Vec::new();
        if let Some(Some(l)) = self.lookups.get_mut(li) {
            let mut picked: Vec<(usize, SocketAddr)> = Vec::new();
            l.algo.finished(oi, &hooks, |i, entry| {
                picked.push((i, entry.ep));
                true
            });
            tracing::trace!(li, picked = picked.len(), "dht finish_entry picks");
            for (i, ep) in picked {
                if let Some(entry) = l.algo.results().get(i) {
                    let (e, sent) = l.invoke_entry(entry, our_id);
                    more.push((e, ep, pack_tag(li, i), sent));
                }
            }
        }
        let ro = self.settings.read_only;
        let v6 = self.v6;
        let rpc = &mut self.rpc;
        let outbox = &mut self.outbox;
        let events = &mut self.events;
        for (e, ep, tag, sent) in more {
            if let Some(l) = self.lookups.get(li).and_then(Option::as_ref)
                && matches!(
                    l.kind,
                    LookupKind::GetPeers | LookupKind::ObfuscatedGetPeers
                )
                && let Some(sent_target) = sent
            {
                events.push(DhtEvent::OutgoingGetPeers {
                    target: *l.algo.target(),
                    sent_target,
                    ep,
                });
            }
            rpc.invoke(e, ep, tag, our_id, ro, v6, |e2, t| {
                outbox.push((bitdaemon_bencode::bencode(&e2), t));
                true
            });
        }
        // done? surface + drop
        if self
            .lookups
            .get(li)
            .and_then(Option::as_ref)
            .is_some_and(|l| l.algo.is_done())
        {
            self.finish_lookup(li);
        }
    }

    /// collect `(id, ep, token)` triples for the write tokens a
    /// lookup accumulated (`m_write_tokens` on `find_data` — the
    /// `announce`/`put` second phase consumes them).
    fn take_write_tokens(&self, li: usize) -> Vec<(NodeId, SocketAddr, Vec<u8>)> {
        let Some(Some(l)) = self.lookups.get(li) else {
            return Vec::new();
        };
        // `find_data::done` — `m_write_tokens` already carries the
        // responder endpoint; upstream does not consult `m_results`
        // here (they are cleared by `done()`).
        l.write_tokens()
            .iter()
            .map(|(id, (ep, tok))| (*id, *ep, tok.clone()))
            .collect()
    }

    fn lookup_failed(&mut self, li: usize, oi: usize, flags: u8) {
        let hooks = self.hooks();
        let table = &mut self.table;
        if let Some(Some(l)) = self.lookups.get_mut(li) {
            l.algo.failed(
                oi,
                flags,
                &hooks,
                |i2, e2| table.node_failed(&i2, &e2),
                |_, _| true,
            );
        }
        if self
            .lookups
            .get(li)
            .and_then(Option::as_ref)
            .is_some_and(|l| l.algo.is_done())
        {
            self.finish_lookup(li);
        }
    }

    /// Lookup completion — `bootstrap::done` pings every result we
    /// never queried (upstream `add_node` per un-queried observer),
    /// then emits `DhtEvent::Done`.
    fn finish_lookup(&mut self, li: usize) {
        let is_bootstrap = self
            .lookups
            .get(li)
            .and_then(Option::as_ref)
            .is_some_and(|l| l.kind == LookupKind::Bootstrap);
        if is_bootstrap && let Some(Some(l)) = self.lookups.get(li) {
            let eps: Vec<SocketAddr> = l
                .algo
                .results()
                .iter()
                .filter(|o| o.flags & crate::traversal::obs_flags::QUERIED == 0)
                .map(|o| o.ep)
                .collect();
            for ep in eps {
                self.add_node(ep);
            }
        }
        let tokens = self.take_write_tokens(li);
        self.lookups[li] = None;
        self.events.push(DhtEvent::Done { lookup: li, tokens });
    }

    /// `node::add_node` — verify an unknown endpoint with a `ping`
    /// before it can enter the routing table. Tags the request in a
    /// reserved tag space; timeouts decode to `usize::MAX` and are
    /// ignored by the lookup machinery.
    pub fn add_node(&mut self, ep: SocketAddr) {
        let mut em = Dictionary::new();
        em.insert("q".into(), Entry::String(b"ping".to_vec()));
        let e = Entry::Dict(em);
        let our_id = self.id;
        let ro = self.settings.read_only;
        let v6 = self.v6;
        let rpc = &mut self.rpc;
        let outbox = &mut self.outbox;
        rpc.invoke(e, ep, PING_TAG, our_id, ro, v6, |e2, t| {
            outbox.push((bitdaemon_bencode::bencode(&e2), t));
            true
        });
    }

    /// `dht_tracker::sample_infohashes` — BEP-51: query ONE node
    /// (a `direct_request` upstream, not a traversal). The reply
    /// lands as `DhtEvent::Direct` keyed by the returned tag.
    pub fn sample_infohashes(&mut self, ep: SocketAddr, target: NodeId) -> u64 {
        let mut em = Dictionary::new();
        em.insert("q".into(), Entry::String(b"sample_infohashes".to_vec()));
        let mut am = Dictionary::new();
        am.insert("target".into(), Entry::String(target.as_bytes().to_vec()));
        em.insert("a".into(), Entry::Dict(am));
        self.direct_request(Entry::Dict(em), ep)
    }

    /// `node::direct_request` (`kademlia/direct_request`) — send a
    /// query that isn't a traversal and get the reply back as a
    /// `DhtEvent::Direct` event. Returns the request tag.
    pub fn direct_request(&mut self, q: Entry, ep: SocketAddr) -> u64 {
        let tag = DIRECT_BIT | self.direct_tag;
        self.direct_tag = self.direct_tag.wrapping_add(1);
        let our_id = self.id;
        let ro = self.settings.read_only;
        let v6 = self.v6;
        let rpc = &mut self.rpc;
        let outbox = &mut self.outbox;
        rpc.invoke(q, ep, tag, our_id, ro, v6, |e2, t| {
            outbox.push((bitdaemon_bencode::bencode(&e2), t));
            true
        });
        tag
    }

    /// `bootstrap` — `refresh.cpp` traversal: get_peers toward our
    /// own (current) node id; `invoke` re-reads `self.id` via the
    /// target stored at lookup start, so callers that rotate their
    /// id should restart the bootstrap (upstream re-reads the live
    /// id — our `Lookup` target IS `self.id` at call time).
    pub fn bootstrap(&mut self) -> usize {
        self.events.push(DhtEvent::Log {
            module: 0,
            msg: "starting DHT bootstrap".into(),
        });
        self.new_lookup(LookupKind::Bootstrap, self.id)
    }

    /// Query path — `incoming_request`. Builds `r`/`e` reply.
    pub fn handle_query(
        &mut self,
        root: &BdecodeNode<'_>,
        from: SocketAddr,
        tid: Vec<u8>,
    ) -> (Vec<u8>, SocketAddr) {
        let mut em = Dictionary::new();
        em.insert("t".into(), Entry::String(tid));
        em.insert("y".into(), Entry::String(b"r".to_vec()));
        let mut reply = Dictionary::new();
        reply.insert("id".into(), Entry::String(self.id.as_bytes().to_vec()));

        let q = root.dict_find_string_value(b"q", b"");
        let arg = root.dict_find_dict(b"a");

        match q {
            b"ping" => {}
            b"find_node" => {
                if !arg.is_valid() {
                    return self.error(from, em, b"invalid find_node");
                }
                let t = arg.dict_find_string(b"target");
                if !t.is_valid() || t.string_length() != 20 {
                    return self.error(from, em, b"invalid find_node");
                }
                let target = NodeId::from_bytes(t.string_value()).unwrap_or_default();
                self.write_nodes(&target, &arg, &mut reply);
            }
            b"get_peers" => {
                if !arg.is_valid() {
                    return self.error(from, em, b"invalid get_peers");
                }
                let ih = arg.dict_find_string(b"info_hash");
                if !ih.is_valid() || ih.string_length() != 20 {
                    return self.error(from, em, b"invalid get_peers");
                }
                let info_hash = Sha1Hash::from_bytes(ih.string_value()).unwrap_or_default();
                // `dht_observer::get_peers` — surfaced as
                // `dht_get_peers_alert` upstream.
                self.events
                    .push(DhtEvent::IncomingGetPeers { ih: info_hash });
                self.write_nodes(&info_hash, &arg, &mut reply);
                let noseed = arg.dict_find_int_value(b"noseed", 0) != 0;
                let scrape = arg.dict_find_int_value(b"scrape", 0) != 0;
                let r = self
                    .storage
                    .get_peers(&info_hash, noseed, scrape, &from.ip());
                if r.no_token {
                    // full — withhold token to widen the perimeter
                } else {
                    let tok = self.generate_token(&from, &info_hash);
                    reply.insert("token".into(), Entry::String(tok.to_vec()));
                }
                if !r.peers.is_empty() {
                    reply.insert(
                        "values".into(),
                        Entry::List(
                            r.peers
                                .chunks_exact(if self.v6 { 18 } else { 6 })
                                .map(|c| Entry::String(c.to_vec()))
                                .collect(),
                        ),
                    );
                }
                if let Some(bf) = r.bfpe {
                    reply.insert("BFpe".into(), Entry::String(bf));
                }
                if let Some(bf) = r.bfsd {
                    reply.insert("BFsd".into(), Entry::String(bf));
                }
            }
            b"announce_peer" => {
                if !arg.is_valid() {
                    return self.error(from, em, b"invalid announce_peer");
                }
                let ih = arg.dict_find_string(b"info_hash");
                let tok = arg.dict_find_string(b"token");
                let port = arg.dict_find_int(b"port");
                if !ih.is_valid() || ih.string_length() != 20 || !tok.is_valid() || !port.is_valid()
                {
                    return self.error(from, em, b"invalid announce_peer");
                }
                let info_hash = Sha1Hash::from_bytes(ih.string_value()).unwrap_or_default();
                if !self.verify_token(tok.string_value(), &info_hash, &from) {
                    return self.error(from, em, b"invalid token");
                }
                let implied = arg.dict_find_int_value(b"implied_port", 0) != 0;
                let p = if implied {
                    from.port()
                } else {
                    port.int_value() as u16
                };
                // `dht_observer::announce` — surfaced as
                // `dht_announce_alert` upstream.
                self.events.push(DhtEvent::IncomingAnnounce {
                    ih: info_hash,
                    ip: from.ip(),
                    port: p,
                });
                let name = arg.dict_find_string_value(b"name", b"").to_vec();
                let seed = arg.dict_find_int_value(b"seed", 0) != 0;
                self.storage.announce_peer(
                    &info_hash,
                    SocketAddr::new(from.ip(), p),
                    std::str::from_utf8(&name).unwrap_or(""),
                    seed,
                );
            }
            b"get" => {
                if !arg.is_valid() {
                    return self.error(from, em, b"invalid get");
                }
                let t = arg.dict_find_string(b"target");
                if !t.is_valid() || t.string_length() != 20 {
                    return self.error(from, em, b"invalid get");
                }
                let target = NodeId::from_bytes(t.string_value()).unwrap_or_default();
                let tok = self.generate_token(&from, &target);
                reply.insert("token".into(), Entry::String(tok.to_vec()));
                self.write_nodes(&target, &arg, &mut reply);
                let seq = arg.dict_find_int_value(b"seq", -1);
                if let Some(v) = self.storage.get_immutable_item(&target) {
                    reply.insert("v".into(), Entry::Preformatted(v.to_vec()));
                } else if let Some(m) = self.storage.get_mutable_item(&target, seq, true) {
                    reply.insert("seq".into(), Entry::Int(m.seq));
                    if let Some(val) = m.value {
                        reply.insert("v".into(), Entry::Preformatted(val.to_vec()));
                    }
                    if !m.key.is_empty() {
                        reply.insert("k".into(), Entry::String(m.key.to_vec()));
                        reply.insert("sig".into(), Entry::String(m.sig.to_vec()));
                    }
                }
            }
            b"put" => {
                if !arg.is_valid() {
                    return self.error(from, em, b"invalid put");
                }
                match self.handle_put(&arg, &from) {
                    Ok(()) => {}
                    Err(m) => return self.error(from, em, m),
                }
            }
            b"sample_infohashes" => {
                if !arg.is_valid() {
                    return self.error(from, em, b"invalid sample_infohashes");
                }
                let t = arg.dict_find_string(b"target");
                if !t.is_valid() || t.string_length() != 20 {
                    return self.error(from, em, b"invalid sample_infohashes");
                }
                let target = NodeId::from_bytes(t.string_value()).unwrap_or_default();
                let (interval, num, samples) = self.storage.get_infohashes_sample();
                reply.insert("interval".into(), Entry::Int(interval.into()));
                reply.insert("num".into(), Entry::Int(num as i64));
                let mut blob = Vec::new();
                for s in samples {
                    blob.extend_from_slice(s.as_bytes());
                }
                reply.insert("samples".into(), Entry::String(blob));
                self.write_nodes(&target, &arg, &mut reply);
            }
            _ => {
                // unknown q carrying a target/info_hash → treat as
                // find_node (future-compat upstream)
                if arg.is_valid() {
                    let tn = arg.dict_find_string(b"target");
                    let t: &[u8] = if tn.is_valid() {
                        tn.string_value()
                    } else {
                        b""
                    };
                    if t.len() == 20 {
                        let target = NodeId::from_bytes(t).unwrap_or_default();
                        self.write_nodes(&target, &arg, &mut reply);
                        em.insert("r".into(), Entry::Dict(reply));
                        return (bitdaemon_bencode::bencode(&Entry::Dict(em)), from);
                    }
                }
                return self.error(from, em, b"unknown q");
            }
        }

        em.insert("r".into(), Entry::Dict(reply));
        (bitdaemon_bencode::bencode(&Entry::Dict(em)), from)
    }

    /// `put` handling — immutable and mutable (BEP-44) variants.
    fn handle_put(
        &mut self,
        arg: &BdecodeNode<'_>,
        from: &SocketAddr,
    ) -> Result<(), &'static [u8]> {
        let tok = arg.dict_find_string(b"token");
        let t = arg.dict_find_string(b"target");
        let v = arg.dict_find(b"v");
        if !tok.is_valid() || !t.is_valid() || t.string_length() != 20 || !v.is_valid() {
            return Err(b"invalid put");
        }
        let target = NodeId::from_bytes(t.string_value()).unwrap_or_default();
        if !self.verify_token(tok.string_value(), &target, from) {
            return Err(b"invalid token");
        }
        let k = arg.dict_find_string(b"k");
        let sig = arg.dict_find_string(b"sig");
        let seq = arg.dict_find_int(b"seq");
        if k.is_valid() && sig.is_valid() && seq.is_valid() {
            // mutable — verify signature per BEP-44
            if k.string_length() != 32 || sig.string_length() != 64 {
                return Err(b"invalid k/sig");
            }
            let salt = arg.dict_find_string_value(b"salt", b"");
            let _cas = arg.dict_find_int_value(b"cas", -1);
            let mut pk = [0u8; 32];
            pk.copy_from_slice(k.string_value());
            let mut sg = [0u8; 64];
            sg.copy_from_slice(sig.string_value());
            if !crate::item::verify_mutable_item(v.data_section(), salt, seq.int_value(), &pk, &sg)
            {
                return Err(b"signature mismatch");
            }
            self.storage.put_mutable_item(
                &target,
                PutMutable {
                    value: v.data_section(),
                    seq: seq.int_value(),
                    sig: &sg,
                    pk: &pk,
                    salt,
                    addr: &from.ip(),
                },
            );
        } else {
            self.storage
                .put_immutable_item(&target, v.data_section(), &from.ip());
        }
        Ok(())
    }

    /// `write_nodes_entries` — our protocol only (the tracker maps
    /// `want` to sibling nodes upstream; single-node here).
    fn write_nodes(&mut self, target: &NodeId, _arg: &BdecodeNode<'_>, reply: &mut Dictionary) {
        let nodes = self.table.find_node(target, false, 0);
        let mut blob = Vec::new();
        for n in nodes {
            let Some(id) = n.id else { continue };
            blob.extend_from_slice(id.as_bytes());
            blob.extend_from_slice(&endpoint_to_bytes(n.endpoint));
        }
        let key: &[u8] = if self.v6 { b"nodes6" } else { b"nodes" };
        reply.insert(key.into(), Entry::String(blob));
    }

    fn error(
        &self,
        from: SocketAddr,
        mut e: Dictionary,
        msg: &'static [u8],
    ) -> (Vec<u8>, SocketAddr) {
        e.insert("y".into(), Entry::String(b"e".to_vec()));
        e.insert(
            "e".into(),
            Entry::List(vec![Entry::Int(203), Entry::String(msg.to_vec())]),
        );
        (bitdaemon_bencode::bencode(&Entry::Dict(e)), from)
    }

    /// `node::announce` second phase — send `announce_peer` to
    /// every node that gave us a write token (typically the
    /// `write_tokens` of a completed `GetPeers` lookup).
    /// `announce::seed`/`implied_port` flags upstream.
    pub fn announce(
        &mut self,
        info_hash: &Sha1Hash,
        port: u16,
        seed: bool,
        implied_port: bool,
        tokens: &BTreeMap<NodeId, SocketAddr>,
        write_tokens: impl Fn(&NodeId) -> Option<Vec<u8>>,
    ) {
        for (nid, ep) in tokens {
            let Some(token) = write_tokens(nid) else {
                continue;
            };
            let mut em = Dictionary::new();
            em.insert("q".into(), Entry::String(b"announce_peer".to_vec()));
            let mut am = Dictionary::new();
            am.insert(
                "info_hash".into(),
                Entry::String(info_hash.as_bytes().to_vec()),
            );
            am.insert("port".into(), Entry::Int(i64::from(port)));
            am.insert("token".into(), Entry::String(token));
            am.insert("seed".into(), Entry::Int(i64::from(seed)));
            if implied_port {
                am.insert("implied_port".into(), Entry::Int(1));
            }
            em.insert("a".into(), Entry::Dict(am));
            self.send_query(Entry::Dict(em), *ep, u64::MAX);
        }
    }

    /// `put_data::invoke` — send `put` to nodes holding tokens.
    /// `mutable` carries (pk, seq, sig, salt) for BEP-44 items.
    pub fn put(
        &mut self,
        value: &[u8],
        mutable: Option<MutableFields<'_>>,
        targets: Vec<(SocketAddr, Vec<u8>)>,
    ) {
        for (ep, token) in targets {
            let mut em = Dictionary::new();
            em.insert("q".into(), Entry::String(b"put".to_vec()));
            let mut am = Dictionary::new();
            am.insert("v".into(), Entry::Preformatted(value.to_vec()));
            am.insert("token".into(), Entry::String(token));
            if let Some((pk, seq, sig, salt)) = mutable {
                am.insert("k".into(), Entry::String(pk.to_vec()));
                am.insert("seq".into(), Entry::Int(seq));
                am.insert("sig".into(), Entry::String(sig.to_vec()));
                if !salt.is_empty() {
                    am.insert("salt".into(), Entry::String(salt.to_vec()));
                }
            }
            em.insert("a".into(), Entry::Dict(am));
            self.send_query(Entry::Dict(em), ep, u64::MAX);
        }
    }

    /// push one pre-built query through rpc → outbox.
    fn send_query(&mut self, e: Entry, ep: SocketAddr, tag: u64) {
        let our_id = self.id;
        let ro = self.settings.read_only;
        let v6 = self.v6;
        let rpc = &mut self.rpc;
        let outbox = &mut self.outbox;
        rpc.invoke(e, ep, tag, our_id, ro, v6, |e2, t| {
            outbox.push((bitdaemon_bencode::bencode(&e2), t));
            true
        });
    }

    /// `dht_state` — serialize live-node endpoints for `.session`
    /// bootstrap (`dht_tracker::dht_state` upstream).
    #[must_use]
    pub fn dht_state(&self) -> crate::dht_state::DhtState {
        let mut st = crate::dht_state::DhtState::default();
        self.table.for_each_node(
            |n| {
                if self.v6 {
                    st.nodes6.push(n.endpoint);
                } else {
                    st.nodes.push(n.endpoint);
                }
            },
            |_| {},
        );
        st
    }

    /// `dht_tracker::stats` — per-family snapshot for
    /// `dht_stats_alert`: node id, routing `(nodes, replacements,
    /// confirmed)` and running traversals (`active_requests`).
    #[must_use]
    pub fn stats(&self) -> (NodeId, usize, usize, usize, usize) {
        let (nodes, repl, confirmed) = self.table.size();
        (
            self.id,
            nodes,
            repl,
            confirmed,
            self.lookups.iter().flatten().count(),
        )
    }

    /// `dht::live_nodes` — `(node id, endpoint)` pairs that answered
    /// (`flag_alive`) on the `get_peers` traversal for `ih`.
    /// Upstream returns `vector<pair<node_id, udp::endpoint>>`.
    #[must_use]
    pub fn live_nodes_for(&self, ih: &NodeId) -> Vec<(NodeId, SocketAddr)> {
        let mut out = Vec::new();
        for l in self.lookups.iter().flatten() {
            if l.kind != LookupKind::GetPeers || l.algo.target() != ih {
                continue;
            }
            out.extend(
                l.algo
                    .results()
                    .iter()
                    .filter(|e| e.flags & crate::traversal::obs_flags::ALIVE != 0)
                    .filter_map(|e| e.id.map(|id| (id, e.ep))),
            );
        }
        out
    }

    /// `bootstrap` seeding — state-file endpoints aren't verified
    /// node ids, so they land in `router_nodes` (the `flag_initial`
    /// seed list `traversal::start` consumes).
    pub fn load_state(&mut self, st: &crate::dht_state::DhtState) {
        for ep in if self.v6 { &st.nodes6 } else { &st.nodes } {
            self.table.add_router_node(*ep);
        }
    }

    /// `tick()` — rpc timeouts → traversal failures; storage tick.
    pub fn tick(&mut self) -> Duration {
        let mut expirations = Vec::new();
        let wait = self.rpc.tick(|tag, ep, ex| expirations.push((tag, ep, ex)));
        for (tag, ep, ex) in expirations {
            if tag & DIRECT_BIT != 0 || tag == PING_TAG {
                if tag & DIRECT_BIT != 0 {
                    self.events.push(DhtEvent::Direct {
                        tag,
                        id: None,
                        ep,
                        response: Vec::new(),
                        failed: true,
                    });
                }
                continue;
            }
            let (li, oi) = unpack_tag(tag);
            match ex {
                Expiry::ShortTimeout => {
                    self.events.push(DhtEvent::Log {
                        module: 3,
                        msg: format!("short timeout {ep}"),
                    });
                    self.lookup_failed(li, oi, crate::traversal::trav_flags::SHORT_TIMEOUT)
                }
                Expiry::Timeout => {
                    self.events.push(DhtEvent::Log {
                        module: 3,
                        msg: format!("timing out {ep}"),
                    });
                    self.lookup_failed(li, oi, 0)
                }
            }
        }
        self.storage.tick();

        // `node::tick` — every `refresh_timeout` (5 s upstream; our
        // session calls us every tick): self-refresh the routing
        // table's own-id region when shallow (`depth() < 4`), else
        // refresh the least-recently-queried node. While the table
        // is still empty the bootstrap is retried at that cadence —
        // a single lost datagram must not stall discovery for the
        // whole self-refresh interval.
        let now = std::time::Instant::now();
        let empty = self.table.size().0 == 0;
        let stale = self
            .last_self_refresh
            .is_none_or(|t| now.duration_since(t) >= SELF_REFRESH_INTERVAL);
        let retry = empty
            && self
                .last_self_refresh
                .is_none_or(|t| now.duration_since(t) >= BOOTSTRAP_RETRY);
        if (stale && self.table.depth() < 4) || retry {
            self.last_self_refresh = Some(now);
            self.bootstrap();
        } else if let Some((bucket, nid, ep)) = self.table.next_refresh() {
            self.send_single_refresh(bucket, nid, ep);
        }
        wait
    }

    /// `send_single_refresh` — ping a stale node (full bucket) or
    /// `get_peers` toward a random in-bucket id (upstream folds
    /// `find_node` into `get_peers` for the extra `values` chance).
    fn send_single_refresh(&mut self, bucket: usize, nid: NodeId, ep: SocketAddr) {
        let _ = nid;
        let mut em = Dictionary::new();
        em.insert("y".into(), Entry::String(b"q".to_vec()));
        if self.table.is_full(bucket) {
            em.insert("q".into(), Entry::String(b"ping".to_vec()));
        } else {
            em.insert("q".into(), Entry::String(b"get_peers".to_vec()));
            // upstream: `generate_prefix_mask(bucket + 1)`, random
            // id keeping our leading `bucket + 1` bits.
            let mask = crate::node_id::generate_prefix_mask(bucket as i32 + 1);
            let target = (crate::node_id::generate_secret_id() & !mask) | (self.id & mask);
            let mut am = Dictionary::new();
            am.insert(
                "info_hash".into(),
                Entry::String(target.as_bytes().to_vec()),
            );
            em.insert("a".into(), Entry::Dict(am));
        }
        let e = Entry::Dict(em);
        let our_id = self.id;
        let ro = self.settings.read_only;
        let v6 = self.v6;
        let rpc = &mut self.rpc;
        let outbox = &mut self.outbox;
        rpc.invoke(e, ep, PING_TAG, our_id, ro, v6, |e2, t| {
            outbox.push((bitdaemon_bencode::bencode(&e2), t));
            true
        });
    }
}

/// `(pk, seq, sig, salt)` for a mutable `put` (BEP-44).
pub type MutableFields<'a> = (&'a [u8; 32], i64, &'a [u8; 64], &'a [u8]);

/// `m_last_self_refresh` gate — upstream refreshes its own-id
/// region every 10 min while the table stays shallow
/// (`node::tick`).
const SELF_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);
/// Empty-table bootstrap retry cadence — upstream's
/// `refresh_timeout` re-fires `node::tick` every 5 s.
const BOOTSTRAP_RETRY: std::time::Duration = std::time::Duration::from_secs(5);
/// Tag space for `add_node` pings — decodes to `usize::MAX` which
/// never indexes a live lookup.
const PING_TAG: u64 = u64::MAX;
/// Bit 63 marks `direct_request` tags — replies route to
/// `DhtEvent::Direct` instead of the lookup machinery.
const DIRECT_BIT: u64 = 1 << 63;

fn pack_tag(lookup: usize, entry: usize) -> u64 {
    ((lookup as u64) << 32) | entry as u64
}
fn unpack_tag(tag: u64) -> (usize, usize) {
    (
        usize::try_from(tag >> 32).unwrap_or(usize::MAX),
        (tag & 0xffff_ffff) as usize,
    )
}
