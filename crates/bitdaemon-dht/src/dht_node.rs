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

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use bitdaemon_bencode::{BdecodeNode, Dictionary, Entry, NodeType, bdecode_checked};
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
    /// `put_data::done` — every query of one `put` batch resolved
    /// (reply or timeout); `num_success` counts real responses like
    /// upstream's `f(num_writes)` into `dht_put_alert`.
    PutDone {
        /// the tag `put` returned.
        tag: u64,
        /// replies that carried an `r` dict.
        num_success: u32,
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
    /// `put` batches in flight — `put_data`'s success counter:
    /// `pending` decremented per reply/timeout, `ok` incremented
    /// per real response; `PutDone` fires when `pending` hits 0.
    put_batches: HashMap<u64, PutBatch>,
    /// `put` batch tag counter (bit 62 set on every tag).
    put_tag: u64,
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
    /// our listen address (`m_sock.get_local_endpoint().address()`
    /// upstream) — persisted in `dht_state.nids` and used to
    /// re-derive the BEP-42 node id on external-IP change.
    bind_addr: IpAddr,
    /// `ping_observer` state — refresh/add_node probes carry the
    /// probed node's id so a timeout can `node_failed` it
    /// (upstream keeps the id on the observer itself). Keyed by
    /// transaction id; several probes may share a tid.
    probe_targets: HashMap<u16, Vec<(SocketAddr, NodeId)>>,
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
            put_batches: HashMap::new(),
            put_tag: 0,
            outbox: Vec::new(),
            events: Vec::new(),
            ip_votes: Vec::new(),
            blocker: crate::dos_blocker::DosBlocker::new(),
            last_self_refresh: None,
            bind_addr: bind_ip,
            probe_targets: HashMap::new(),
        }
    }

    /// remember `nid` behind a PING_TAG transaction (`tid`).
    fn probe_track(&mut self, tid: Option<u16>, ep: SocketAddr, nid: NodeId) {
        if let Some(tid) = tid {
            self.probe_targets.entry(tid).or_default().push((ep, nid));
        }
    }

    /// a probe transaction resolved — forget its target id.
    fn probe_resolved(&mut self, ep: &SocketAddr) {
        self.probe_targets.retain(|_, v| {
            v.retain(|(e, _)| e != ep);
            !v.is_empty()
        });
    }

    /// a probe errored or timed out — `observer::timeout` upstream
    /// feeds `routing_table::node_failed` for real ids.
    fn probe_failed(&mut self, _tag: u64, ep: &SocketAddr) {
        let mut found = None;
        self.probe_targets.retain(|_, v| {
            if let Some(i) = v.iter().position(|(e, _)| e == ep) {
                found = Some(v.remove(i).1);
            }
            !v.is_empty()
        });
        if let Some(id) = found {
            self.table.node_failed(&id, ep);
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
        // `traversal_algorithm::start` — when the table offered
        // fewer than 3 seeds, fall back to the router nodes as
        // no-id `flag_initial` observers.
        if l.algo.results().len() < 3 {
            for ep in self.table.router_nodes().copied().collect::<Vec<_>>() {
                l.algo.add_entry(
                    NodeId::default(),
                    ep,
                    crate::traversal::obs_flags::INITIAL,
                    &self.hooks(),
                );
            }
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
        // `add_requests` may have declared the traversal complete
        // without sending anything (empty seeds) — surface `Done`.
        if self
            .lookups
            .get(idx)
            .and_then(Option::as_ref)
            .is_some_and(|l| l.algo.is_done())
        {
            self.finish_lookup(idx);
        }
    }

    /// `incoming(m)` — feed a raw datagram.
    /// drain pending external-ip votes.
    pub fn drain_ip_votes(&mut self) -> Vec<std::net::IpAddr> {
        std::mem::take(&mut self.ip_votes)
    }

    /// `node::incoming` — feed a raw datagram. `foreign` is the
    /// sibling-family node (upstream `m_get_foreign_node`) used to
    /// satisfy `want` lists; `quota_ok` is the tracker's
    /// `has_quota()` verdict for answering queries.
    pub fn incoming(
        &mut self,
        buf: &[u8],
        from: SocketAddr,
        foreign: Option<&DhtNode>,
        quota_ok: bool,
    ) -> Option<(Vec<u8>, SocketAddr)> {
        // `m_blocker.incoming` — drop flood sources before parsing
        // (upstream calls it in `dht_tracker::incoming_packet`,
        // pre-bdecode).
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
        if y.len() != 1 {
            // upstream drops non-1-char `y` silently (no reply —
            // magnification protection).
            return None;
        }
        let tid = root.dict_find_string_value(b"t", b"").to_vec();
        match y[0] {
            b'q' => {
                // `dht_read_only` — a read-only node never answers
                // queries (upstream `node::incoming`).
                if self.settings.read_only {
                    return None;
                }
                // `m_sock_man->has_quota()` — upload-quota gate.
                if !quota_ok {
                    return None;
                }
                Some(self.handle_query(&root, from, tid, foreign))
            }
            b'r' | b'e' => {
                self.handle_reply(&root, from, &tid, y[0] == b'e');
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
            Incoming::Reply { tag, id, rtt } => {
                self.table.node_seen(id, from, rtt);
                self.probe_resolved(&from);
                // `ping_observer::reply` — refresh probes harvest
                // the `nodes`/`nodes6` string into the table.
                if tag == PING_TAG {
                    self.heard_about_from(&r);
                    return;
                }
                if tag & PUT_BIT != 0 {
                    self.put_resolved(tag, true);
                    return;
                }
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
                self.probe_failed(tag, &from);
                if tag != PING_TAG && tag & PUT_BIT != 0 {
                    self.put_resolved(tag, false);
                    return;
                }
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
                if tag == PING_TAG {
                    return;
                }
                let (li, oi) = unpack_tag(tag);
                self.lookup_failed(li, oi, 0);
            }
            Incoming::Ignored => {}
        }
    }

    /// `look_for_nodes` on an `r` dict — every well-formed
    /// `(id, endpoint)` in our family's `nodes`/`nodes6` string is
    /// a `heard_about` (unpinged) table candidate.
    fn heard_about_from(&mut self, r: &BdecodeNode<'_>) {
        let key: &[u8] = if self.v6 { b"nodes6" } else { b"nodes" };
        let n = r.dict_find_string(key);
        if !n.is_valid() {
            return;
        }
        let stride = if self.v6 { 38 } else { 26 };
        for chunk in n.string_value().chunks_exact(stride) {
            let Some(id) = NodeId::from_bytes(&chunk[..20]) else {
                continue;
            };
            let mut c = bitdaemon_net::socket_io::Cursor::new(&chunk[20..]);
            if let Ok(ep) = bitdaemon_net::socket_io::read_endpoint(&mut c, self.v6) {
                self.table.heard_about(id, ep);
            }
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
        let oi = if let Some(Some(l)) = self.lookups.get_mut(li) {
            // `observer::reply` — the responder's id lands on the
            // observer so `NO_ID` router entries become real nodes
            // for `take_write_tokens` (announce phase). The resort
            // may move the entry, so `set_id` returns the new slot.
            let oi = resp_id.map_or(oi, |id| l.algo.set_id(oi, id));
            tracing::trace!(li, nodes = nodes.len(), "dht traverse nodes");
            for (id, ep) in nodes {
                l.algo.traverse(id, ep, &hooks, |i2, e2| {
                    table.heard_about(i2, e2);
                });
            }
            oi
        } else {
            oi
        };
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
    /// lookup accumulated — `find_data::done` walks `m_results` in
    /// distance order and keeps at most `bucket_size` entries that
    /// are `flag_alive` *and* hold a write token (a `no_id` entry's
    /// random stand-in id simply won't match).
    fn take_write_tokens(&self, li: usize) -> Vec<(NodeId, SocketAddr, Vec<u8>)> {
        let Some(Some(l)) = self.lookups.get(li) else {
            return Vec::new();
        };
        let mut num = self.table.bucket_size();
        let mut out = Vec::new();
        for o in l.algo.results() {
            if num == 0 {
                break;
            }
            if o.flags & crate::traversal::obs_flags::ALIVE == 0 {
                continue;
            }
            let Some(id) = o.id else { continue };
            if let Some((ep, tok)) = l.write_tokens().get(&id) {
                out.push((id, *ep, tok.clone()));
                num -= 1;
            }
        }
        out
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
    /// never queried (upstream `add_node` per un-queried observer);
    /// `obfuscated_get_peers::done` respawns the search as a plain
    /// `get_peers` seeded by the ≤16 closest `alive` results
    /// ("we failed to switch over to the non-obfuscated mode early
    /// enough"); then emits `DhtEvent::Done`.
    fn finish_lookup(&mut self, li: usize) {
        let kind = self
            .lookups
            .get(li)
            .and_then(Option::as_ref)
            .map(|l| l.kind);
        if kind == Some(LookupKind::Bootstrap)
            && let Some(Some(l)) = self.lookups.get(li)
        {
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
        if kind == Some(LookupKind::ObfuscatedGetPeers) {
            // `obfuscated_get_peers::done` — spawn a regular
            // `get_peers` with the same target/callbacks; the
            // traversal slot (and hence the session's result sink)
            // is reused so `Done` only fires when phase 2 ends.
            let hooks = self.hooks();
            let v6 = self.v6;
            if let Some(Some(l)) = self.lookups.get_mut(li) {
                let target = *l.algo.target();
                let noseed = l.noseed();
                let seeds: Vec<(NodeId, SocketAddr)> = l
                    .algo
                    .results()
                    .iter()
                    .filter(|o| o.flags & crate::traversal::obs_flags::ALIVE != 0)
                    .filter(|o| o.flags & crate::traversal::obs_flags::NO_ID == 0)
                    .filter_map(|o| o.id.map(|id| (id, o.ep)))
                    .take(16)
                    .collect();
                let mut nl = Lookup::new(LookupKind::GetPeers, target, v6, &hooks, seeds);
                nl.set_noseed(noseed);
                *l = nl;
            }
            // upstream falls back to `find_node` table seeds when
            // phase 1 produced nothing (`find_data::start`).
            let empty = self
                .lookups
                .get(li)
                .and_then(Option::as_ref)
                .is_none_or(|l| l.algo.results().is_empty());
            if empty && let Some(Some(l)) = self.lookups.get_mut(li) {
                let target = *l.algo.target();
                for n in self.table.find_node(&target, true, 0) {
                    if let Some(id) = n.id {
                        l.algo.add_entry(
                            id,
                            n.endpoint,
                            crate::traversal::obs_flags::INITIAL,
                            &hooks,
                        );
                    }
                }
            }
            self.drive(li);
            if !self
                .lookups
                .get(li)
                .and_then(Option::as_ref)
                .is_some_and(|l| l.algo.is_done())
            {
                return;
            }
            // phase 2 found nothing to query — fall through to Done
        }
        let tokens = self.take_write_tokens(li);
        self.lookups[li] = None;
        self.events.push(DhtEvent::Done { lookup: li, tokens });
    }

    /// `put` batch resolution — one query's reply (`ok`) or
    /// error/timeout. The last resolution emits `PutDone`.
    fn put_resolved(&mut self, tag: u64, ok: bool) {
        let done = if let Some(b) = self.put_batches.get_mut(&tag) {
            b.pending = b.pending.saturating_sub(1);
            b.ok += usize::from(ok);
            b.pending == 0
        } else {
            return;
        };
        if done && let Some(b) = self.put_batches.remove(&tag) {
            self.events.push(DhtEvent::PutDone {
                tag,
                num_success: b.ok as u32,
            });
        }
    }

    /// Abort a live lookup — `traversal::done()` upstream, used by
    /// immutable `get_item` after the first matching `v` reply
    /// ("there can only be one true immutable item"). Emits the
    /// normal `Done` event so pending write phases still run.
    pub fn abort_lookup(&mut self, li: usize) {
        if self.lookups.get(li).is_some_and(Option::is_some) {
            self.finish_lookup(li);
        }
    }

    /// `node::add_node` — probe an unknown endpoint with a single
    /// refresh (`send_single_refresh` at `num_active_buckets`, i.e.
    /// almost always a `get_peers`); only a reply enters the
    /// routing table. Non-native endpoints are dropped upstream
    /// (`native_address`).
    pub fn add_node(&mut self, ep: SocketAddr) {
        if ep.is_ipv6() != self.v6 {
            return;
        }
        self.send_single_refresh(ep, self.table.num_active_buckets(), NodeId::default());
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

    /// `bootstrap` — `refresh.cpp` traversal: `get_peers` toward
    /// `make_id_secret(nid)` (a secret-marked variant of our id so
    /// responses can be told apart from other nodes' bootstrap
    /// traffic upstream); `invoke` re-reads the live id each time
    /// since it may rotate mid-bootstrap.
    pub fn bootstrap(&mut self) -> usize {
        self.events.push(DhtEvent::Log {
            module: 0,
            msg: "starting DHT bootstrap".into(),
        });
        let mut target = self.id;
        crate::node_id::make_id_secret(&mut target);
        self.last_self_refresh = Some(std::time::Instant::now());
        self.new_lookup(LookupKind::Bootstrap, target)
    }

    /// Query path — `incoming_request`. Builds `r`/`e` reply.
    /// `foreign` is the sibling-family node used to resolve `want`
    /// entries (upstream `m_get_foreign_node` → `dht_tracker::get_node`).
    pub fn handle_query(
        &mut self,
        root: &BdecodeNode<'_>,
        from: SocketAddr,
        tid: Vec<u8>,
        foreign: Option<&DhtNode>,
    ) -> (Vec<u8>, SocketAddr) {
        use crate::msg::{KeyDesc, key_flags as kf};

        let mut em = Dictionary::new();
        em.insert("t".into(), Entry::String(tid));
        em.insert("y".into(), Entry::String(b"r".to_vec()));

        // top-level schema — every query must carry `a.id` (20 B)
        const TOP: &[KeyDesc] = &[
            KeyDesc {
                name: b"q",
                ty: Some(NodeType::String),
                size: 0,
                flags: 0,
            },
            KeyDesc {
                name: b"ro",
                ty: Some(NodeType::Int),
                size: 0,
                flags: kf::OPTIONAL,
            },
            KeyDesc {
                name: b"a",
                ty: Some(NodeType::Dict),
                size: 0,
                flags: kf::PARSE_CHILDREN,
            },
            KeyDesc {
                name: b"id",
                ty: Some(NodeType::String),
                size: 20,
                flags: kf::LAST_CHILD,
            },
        ];
        let mut top: [BdecodeNode<'_>; 4] = std::array::from_fn(|_| BdecodeNode::none());
        let mut err = String::new();
        if !crate::msg::verify_message(root, TOP, &mut top, &mut err) {
            return self.error(from, em, None, err.as_bytes().to_vec(), 203);
        }

        // `e["ip"]` — echo the sender's observed endpoint (compact).
        em.insert("ip".into(), Entry::String(endpoint_to_bytes(from)));

        let arg = top[2].clone();
        let sender_ro = top[1].is_valid() && top[1].int_value() != 0;
        let id = NodeId::from_bytes(top[3].string_value()).unwrap_or_default();

        if self.settings.enforce_node_id && !crate::node_id::verify_id(&id, &from.ip()) {
            return self.error(from, em, None, b"invalid node ID".to_vec(), 203);
        }

        if !sender_ro {
            self.table.heard_about(id, from);
        }

        // `r` — always carries our id + the sender's port (`r.p`).
        let mut reply = Dictionary::new();
        reply.insert("id".into(), Entry::String(self.id.as_bytes().to_vec()));
        reply.insert("p".into(), Entry::Int(i64::from(from.port())));
        let reply = &mut reply;

        match top[0].string_value() {
            b"ping" => {}
            b"get_peers" => {
                const D: &[KeyDesc] = &[
                    KeyDesc {
                        name: b"info_hash",
                        ty: Some(NodeType::String),
                        size: 20,
                        flags: 0,
                    },
                    KeyDesc {
                        name: b"noseed",
                        ty: Some(NodeType::Int),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                    KeyDesc {
                        name: b"scrape",
                        ty: Some(NodeType::Int),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                    KeyDesc {
                        name: b"want",
                        ty: Some(NodeType::List),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                ];
                let mut k: [BdecodeNode<'_>; 4] = std::array::from_fn(|_| BdecodeNode::none());
                if !crate::msg::verify_message(&arg, D, &mut k, &mut err) {
                    return self.error(from, em, Some(reply.clone()), err.into_bytes(), 203);
                }
                let info_hash = Sha1Hash::from_bytes(k[0].string_value()).unwrap_or_default();
                // `dht_observer::get_peers` — `dht_get_peers_alert`.
                self.events
                    .push(DhtEvent::IncomingGetPeers { ih: info_hash });
                self.write_nodes(&info_hash, &k[3], reply, foreign);
                let noseed = k[1].is_valid() && k[1].int_value() != 0;
                let scrape = k[2].is_valid() && k[2].int_value() != 0;
                // full storage → withhold the write token so
                // announces widen the perimeter
                let r = self
                    .storage
                    .get_peers(&info_hash, noseed, scrape, &from.ip());
                if !r.no_token {
                    let tok = self.generate_token(&from, &info_hash);
                    reply.insert("token".into(), Entry::String(tok.to_vec()));
                }
                if let Some(name) = r.name {
                    reply.insert("n".into(), Entry::String(name.into_bytes()));
                }
                if let Some(peers) = r.peers {
                    reply.insert(
                        "values".into(),
                        Entry::List(
                            peers
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
            b"find_node" => {
                const D: &[KeyDesc] = &[
                    KeyDesc {
                        name: b"target",
                        ty: Some(NodeType::String),
                        size: 20,
                        flags: 0,
                    },
                    KeyDesc {
                        name: b"want",
                        ty: Some(NodeType::List),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                ];
                let mut k: [BdecodeNode<'_>; 2] = std::array::from_fn(|_| BdecodeNode::none());
                if !crate::msg::verify_message(&arg, D, &mut k, &mut err) {
                    return self.error(from, em, Some(reply.clone()), err.into_bytes(), 203);
                }
                let target = NodeId::from_bytes(k[0].string_value()).unwrap_or_default();
                self.write_nodes(&target, &k[1], reply, foreign);
            }
            b"announce_peer" => {
                const D: &[KeyDesc] = &[
                    KeyDesc {
                        name: b"info_hash",
                        ty: Some(NodeType::String),
                        size: 20,
                        flags: 0,
                    },
                    KeyDesc {
                        name: b"port",
                        ty: Some(NodeType::Int),
                        size: 0,
                        flags: 0,
                    },
                    KeyDesc {
                        name: b"token",
                        ty: Some(NodeType::String),
                        size: 0,
                        flags: 0,
                    },
                    KeyDesc {
                        name: b"n",
                        ty: Some(NodeType::String),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                    KeyDesc {
                        name: b"seed",
                        ty: Some(NodeType::Int),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                    KeyDesc {
                        name: b"implied_port",
                        ty: Some(NodeType::Int),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                ];
                let mut k: [BdecodeNode<'_>; 6] = std::array::from_fn(|_| BdecodeNode::none());
                if !crate::msg::verify_message(&arg, D, &mut k, &mut err) {
                    return self.error(from, em, Some(reply.clone()), err.into_bytes(), 203);
                }
                // `implied_port` → use the packet's source port
                let mut port = k[1].int_value();
                if k[5].is_valid() && k[5].int_value() != 0 {
                    port = i64::from(from.port());
                }
                if !(0..65536).contains(&port) {
                    return self.error(
                        from,
                        em,
                        Some(reply.clone()),
                        b"invalid port".to_vec(),
                        203,
                    );
                }
                let info_hash = Sha1Hash::from_bytes(k[0].string_value()).unwrap_or_default();
                // `dht_observer::announce` — fired before the token
                // check upstream (`dht_announce_alert` sees it).
                self.events.push(DhtEvent::IncomingAnnounce {
                    ih: info_hash,
                    ip: from.ip(),
                    port: port as u16,
                });
                if !self.verify_token(k[2].string_value(), &info_hash, &from) {
                    return self.error(
                        from,
                        em,
                        Some(reply.clone()),
                        b"invalid token".to_vec(),
                        203,
                    );
                }
                // valid token ⇒ the sender isn't spoofing — let the
                // table add it as a confirmed node (rtt unknown).
                self.table.node_seen(id, from, 0xffff);
                let name = if k[3].is_valid() {
                    k[3].string_value().to_vec()
                } else {
                    Vec::new()
                };
                let seed = k[4].is_valid() && k[4].int_value() != 0;
                self.storage.announce_peer(
                    &info_hash,
                    SocketAddr::new(from.ip(), port as u16),
                    std::str::from_utf8(&name).unwrap_or(""),
                    seed,
                );
            }
            b"put" => match self.handle_put(&arg, &from, id) {
                Ok(()) => {}
                Err((msg, code)) => {
                    return self.error(from, em, Some(reply.clone()), msg.to_vec(), code);
                }
            },
            b"get" => {
                const D: &[KeyDesc] = &[
                    KeyDesc {
                        name: b"seq",
                        ty: Some(NodeType::Int),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                    KeyDesc {
                        name: b"target",
                        ty: Some(NodeType::String),
                        size: 20,
                        flags: 0,
                    },
                    KeyDesc {
                        name: b"want",
                        ty: Some(NodeType::List),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                ];
                let mut k: [BdecodeNode<'_>; 3] = std::array::from_fn(|_| BdecodeNode::none());
                if !crate::msg::verify_message(&arg, D, &mut k, &mut err) {
                    return self.error(from, em, Some(reply.clone()), err.into_bytes(), 203);
                }
                let target = NodeId::from_bytes(k[1].string_value()).unwrap_or_default();
                let tok = self.generate_token(&from, &target);
                reply.insert("token".into(), Entry::String(tok.to_vec()));
                self.write_nodes(&target, &k[2], reply, foreign);
                // a `seq` request is for a mutable item only
                if !k[0].is_valid() {
                    if let Some(v) = self.storage.get_immutable_item(&target) {
                        reply.insert("v".into(), Entry::Preformatted(v.to_vec()));
                    } else {
                        self.write_mutable(&target, 0, true, reply);
                    }
                } else {
                    self.write_mutable(&target, k[0].int_value(), false, reply);
                }
            }
            b"sample_infohashes" => {
                const D: &[KeyDesc] = &[
                    KeyDesc {
                        name: b"target",
                        ty: Some(NodeType::String),
                        size: 20,
                        flags: 0,
                    },
                    KeyDesc {
                        name: b"want",
                        ty: Some(NodeType::List),
                        size: 0,
                        flags: kf::OPTIONAL,
                    },
                ];
                let mut k: [BdecodeNode<'_>; 2] = std::array::from_fn(|_| BdecodeNode::none());
                if !crate::msg::verify_message(&arg, D, &mut k, &mut err) {
                    return self.error(from, em, Some(reply.clone()), err.into_bytes(), 203);
                }
                let target = NodeId::from_bytes(k[0].string_value()).unwrap_or_default();
                let (interval, num, samples) = self.storage.get_infohashes_sample();
                reply.insert("interval".into(), Entry::Int(i64::from(interval)));
                reply.insert("num".into(), Entry::Int(num as i64));
                let mut blob = Vec::new();
                for s in samples {
                    blob.extend_from_slice(s.as_bytes());
                }
                reply.insert("samples".into(), Entry::String(blob));
                self.write_nodes(&target, &k[1], reply, foreign);
            }
            _ => {
                // unrecognized q carrying `target` or `info_hash` →
                // treat as find_node (future-compat upstream)
                let tn = arg.dict_find_string(b"target");
                let t: &[u8] = if tn.is_valid() && tn.string_length() == 20 {
                    tn.string_value()
                } else {
                    let ih = arg.dict_find_string(b"info_hash");
                    if ih.is_valid() && ih.string_length() == 20 {
                        ih.string_value()
                    } else {
                        return self.error(
                            from,
                            em,
                            Some(reply.clone()),
                            b"unknown message".to_vec(),
                            203,
                        );
                    }
                };
                let target = NodeId::from_bytes(t).unwrap_or_default();
                self.write_nodes(&target, &arg.dict_find_list(b"want"), reply, foreign);
            }
        }

        em.insert("r".into(), Entry::Dict(reply.clone()));
        self.emit(em, from)
    }

    /// `get_mutable_item` → `r` dict — `seq` always; `v`,`k`,`sig`
    /// only under the fill condition (upstream writes them together).
    fn write_mutable(&self, target: &NodeId, seq: i64, force_fill: bool, reply: &mut Dictionary) {
        if let Some(m) = self.storage.get_mutable_item(target, seq, force_fill) {
            reply.insert("seq".into(), Entry::Int(m.seq));
            if let Some(val) = m.value {
                reply.insert("v".into(), Entry::Preformatted(val.to_vec()));
                reply.insert("sig".into(), Entry::String(m.sig.to_vec()));
                reply.insert("k".into(), Entry::String(m.key.to_vec()));
            }
        }
    }

    /// `put` handling — immutable and mutable (BEP-44) variants.
    /// Returns `(message, error code)`; codes match upstream
    /// `incoming_error` calls (203 default, 205/206/207/301/302).
    fn handle_put(
        &mut self,
        arg: &BdecodeNode<'_>,
        from: &SocketAddr,
        id: NodeId,
    ) -> Result<(), (&'static [u8], i64)> {
        use crate::msg::{KeyDesc, key_flags as kf};
        // upstream rejects a `put` whose `a` dict has any non-fatal
        // bencode violation — `v` must round-trip byte-identically
        // or the signature would break.
        if arg.has_soft_error().is_some() {
            return Err((b"message has non-fatal errors", 203));
        }
        const D: &[KeyDesc] = &[
            KeyDesc {
                name: b"token",
                ty: Some(NodeType::String),
                size: 0,
                flags: 0,
            },
            KeyDesc {
                name: b"v",
                ty: None,
                size: 0,
                flags: 0,
            },
            KeyDesc {
                name: b"seq",
                ty: Some(NodeType::Int),
                size: 0,
                flags: kf::OPTIONAL,
            },
            KeyDesc {
                name: b"k",
                ty: Some(NodeType::String),
                size: 32,
                flags: kf::OPTIONAL,
            },
            KeyDesc {
                name: b"sig",
                ty: Some(NodeType::String),
                size: 64,
                flags: kf::OPTIONAL,
            },
            KeyDesc {
                name: b"cas",
                ty: Some(NodeType::Int),
                size: 0,
                flags: kf::OPTIONAL,
            },
            KeyDesc {
                name: b"salt",
                ty: Some(NodeType::String),
                size: 0,
                flags: kf::OPTIONAL,
            },
        ];
        let mut k: [BdecodeNode<'_>; 7] = std::array::from_fn(|_| BdecodeNode::none());
        let mut err = String::new();
        if !crate::msg::verify_message(arg, D, &mut k, &mut err) {
            // clone into 'static msg — the error strings are our own
            return Err((b"invalid put", 203));
        }

        let buf = k[1].data_section();
        if buf.len() > 1000 || buf.is_empty() {
            return Err((b"message too big", 205));
        }
        let salt: &[u8] = if k[6].is_valid() {
            k[6].string_value()
        } else {
            b""
        };
        if salt.len() > 64 {
            return Err((b"salt too big", 207));
        }

        let mutable_put = k[2].is_valid() && k[3].is_valid() && k[4].is_valid();
        // the target is derived from the payload — the `a.target`
        // field is *not* read upstream. Upstream quirk: `k` alone
        // (without `seq`/`sig`) selects the mutable target even
        // though the item is then stored as immutable.
        let target = if k[3].is_valid() {
            let mut pk = [0u8; 32];
            pk.copy_from_slice(k[3].string_value());
            crate::item::item_target_id_mutable(salt, &pk)
        } else {
            crate::item::item_target_id_immutable(buf)
        };

        if !self.verify_token(k[0].string_value(), &target, from) {
            return Err((b"invalid token", 203));
        }

        if !mutable_put {
            self.storage.put_immutable_item(&target, buf, &from.ip());
        } else {
            let seq = k[2].int_value();
            if seq < 0 {
                return Err((b"invalid (negative) sequence number", 203));
            }
            let mut pk = [0u8; 32];
            pk.copy_from_slice(k[3].string_value());
            let mut sg = [0u8; 64];
            sg.copy_from_slice(k[4].string_value());
            if !crate::item::verify_mutable_item(buf, salt, seq, &pk, &sg) {
                return Err((b"invalid signature", 206));
            }
            if let Some(item_seq) = self.storage.get_mutable_item_seq(&target) {
                // `cas` — compare-and-swap on the stored seq
                if k[5].is_valid() && item_seq != k[5].int_value() {
                    return Err((b"CAS mismatch", 301));
                }
                if item_seq > seq {
                    return Err((b"old sequence number", 302));
                }
            }
            self.storage.put_mutable_item(
                &target,
                PutMutable {
                    value: buf,
                    seq,
                    sig: &sg,
                    pk: &pk,
                    salt,
                    addr: &from.ip(),
                },
            );
        }

        // a correct token ⇒ real address — confirmed node
        self.table.node_seen(id, *from, 0xffff);
        Ok(())
    }

    /// `write_nodes_entries` — honor the request's `want` list by
    /// answering from the matching family's routing table (self or
    /// the sibling node upstream calls `m_get_foreign_node`).
    fn write_nodes(
        &mut self,
        target: &NodeId,
        want: &BdecodeNode<'_>,
        reply: &mut Dictionary,
        foreign: Option<&DhtNode>,
    ) {
        // no `want` → answer with our own protocol's node list
        if want.node_type() != bitdaemon_bencode::NodeType::List {
            Self::write_nodes_for(self, target, reply);
            return;
        }
        for i in 0..want.list_size() {
            let w = want.list_at(i);
            if w.node_type() != bitdaemon_bencode::NodeType::String {
                continue;
            }
            // `"n4"`/`"n6"` → the node owning that protocol family
            let node = match w.string_value() {
                b"n4" if !self.v6 => Some(&*self),
                b"n6" if self.v6 => Some(&*self),
                b"n4" | b"n6" => foreign,
                _ => None,
            };
            let Some(n) = node else { continue };
            Self::write_nodes_for(n, target, reply);
        }
    }

    /// `write_nodes_entry` — the compact `nodes`/`nodes6` string
    /// for `node`'s table (key follows *its* family).
    fn write_nodes_for(node: &DhtNode, target: &NodeId, reply: &mut Dictionary) {
        let nodes = node.table.find_node_ref(target);
        let mut blob = Vec::new();
        for n in nodes {
            let Some(id) = n.id else { continue };
            blob.extend_from_slice(id.as_bytes());
            blob.extend_from_slice(&endpoint_to_bytes(n.endpoint));
        }
        let key: &[u8] = if node.v6 { b"nodes6" } else { b"nodes" };
        reply.insert(key.into(), Entry::String(blob));
    }

    /// `incoming_error` — `y:"e"` + `[code, msg]`; the reply keeps
    /// whatever `ip`/`r` was already materialized, like upstream.
    fn error(
        &self,
        from: SocketAddr,
        mut e: Dictionary,
        r: Option<Dictionary>,
        msg: Vec<u8>,
        code: i64,
    ) -> (Vec<u8>, SocketAddr) {
        if let Some(r) = r {
            e.insert("r".into(), Entry::Dict(r));
        }
        e.insert("y".into(), Entry::String(b"e".to_vec()));
        e.insert(
            "e".into(),
            Entry::List(vec![Entry::Int(code), Entry::String(msg)]),
        );
        self.emit(e, from)
    }

    /// `send_packet` tail — stamp `v` and bencode the reply.
    fn emit(&self, mut e: Dictionary, from: SocketAddr) -> (Vec<u8>, SocketAddr) {
        e.insert("v".into(), Entry::String(crate::CLIENT_VERSION.to_vec()));
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
    /// Replies and timeouts resolve the returned batch tag; the
    /// last one emits `DhtEvent::PutDone` with the reply count
    /// (upstream `put_data::done` → `f(num_writes)`).
    pub fn put(
        &mut self,
        value: &[u8],
        mutable: Option<MutableFields<'_>>,
        targets: Vec<(SocketAddr, Vec<u8>)>,
    ) -> u64 {
        if targets.is_empty() {
            return PING_TAG;
        }
        let tag = PUT_BIT | self.put_tag;
        self.put_tag = self.put_tag.wrapping_add(1) % PUT_BIT;
        self.put_batches.insert(
            tag,
            PutBatch {
                pending: targets.len(),
                ok: 0,
            },
        );
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
            self.send_query(Entry::Dict(em), ep, tag);
        }
        tag
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

    /// `dht_tracker::state` per node — upstream records the node's
    /// `(local addr, id)` in `nids` and appends *every* live+
    /// replacement endpoint to `nodes` without family separation
    /// (the loader re-sorts by `native_address`).
    #[must_use]
    pub fn dht_state(&self) -> crate::dht_state::DhtState {
        let mut st = crate::dht_state::DhtState::default();
        st.nids.push((self.bind_addr, self.id));
        let mut live = Vec::new();
        let mut repl = Vec::new();
        self.table
            .for_each_node(|n| live.push(n.endpoint), |n| repl.push(n.endpoint));
        st.nodes.extend(live);
        st.nodes.extend(repl);
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
    /// `dht_tracker::start` state replay — persisted endpoints
    /// join the *bootstrap traversal* as `flag_initial` observers
    /// (`concat(own_family, cross_family)`), not the router table.
    /// Upstream's `filtered()` gate needs the session's dht
    /// ip-filter observer which we don't wire — `restrict_search_
    /// ips` still applies inside `add_entry`.
    pub fn load_state(&mut self, st: &crate::dht_state::DhtState) {
        let eps: Vec<SocketAddr> = if self.v6 {
            st.nodes6.iter().chain(st.nodes.iter()).copied().collect()
        } else {
            st.nodes.iter().chain(st.nodes6.iter()).copied().collect()
        };
        if eps.is_empty() {
            return;
        }
        let hooks = self.hooks();
        let idx = self
            .lookups
            .iter()
            .position(|l| l.as_ref().is_some_and(|l| l.kind == LookupKind::Bootstrap))
            .unwrap_or_else(|| self.bootstrap());
        if let Some(Some(l)) = self.lookups.get_mut(idx) {
            for ep in eps {
                l.algo.add_entry(
                    NodeId::default(),
                    ep,
                    crate::traversal::obs_flags::INITIAL | crate::traversal::obs_flags::NO_ID,
                    &hooks,
                );
            }
        }
        self.drive(idx);
    }

    /// `tick()` — rpc timeouts → traversal failures; storage tick.
    pub fn tick(&mut self) -> Duration {
        // upstream `dht_tracker::refresh_timeout` re-pushes the
        // dos-blocker settings on every 5 s refresh.
        self.blocker
            .set_rate_limit(self.settings.block_ratelimit.max(0) as u32);
        self.blocker.set_block_timeout(self.settings.block_timeout);
        let mut expirations = Vec::new();
        let wait = self.rpc.tick(|tag, ep, ex| expirations.push((tag, ep, ex)));
        for (tag, ep, ex) in expirations {
            self.expire_tag(tag, ep, ex);
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
        } else if let Some((nid, ep)) = self.table.next_refresh() {
            // upstream: `bucket = 159 - distance_exp(m_id, ne->id)`
            if nid != self.id {
                let bucket = 159 - crate::node_id::distance_exp(&self.id, &nid);
                self.send_single_refresh(ep, bucket.max(0) as usize, nid);
            }
        }
        wait
    }

    /// `node::unreachable` — a transport-level failure toward `ep`
    /// (ICMP/connection-reset). The pending transaction to it is
    /// failed like a final timeout.
    pub fn unreachable(&mut self, ep: SocketAddr) {
        if let Some(tag) = self.rpc.unreachable(ep) {
            self.expire_tag(tag, ep, Expiry::Timeout);
        }
    }

    /// dispatch one expired/failed transaction — shared by `tick`
    /// expirations and `unreachable`.
    fn expire_tag(&mut self, tag: u64, ep: SocketAddr, ex: Expiry) {
        self.probe_failed(tag, &ep);
        if tag != PING_TAG && tag & PUT_BIT != 0 {
            self.put_resolved(tag, false);
            return;
        }
        if tag & DIRECT_BIT != 0 {
            self.events.push(DhtEvent::Direct {
                tag,
                id: None,
                ep,
                response: Vec::new(),
                failed: true,
            });
            return;
        }
        if tag == PING_TAG {
            return;
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

    /// `node::update_node_id` — re-derive our BEP-42 id when the
    /// external address changed (skip when the current id still
    /// verifies), then re-bucket the routing table and update the
    /// storage distance reference.
    pub fn update_node_id(&mut self, external: IpAddr) {
        if crate::node_id::verify_id(&self.id, &external) {
            return;
        }
        self.id = generate_id_impl(&external, 0);
        self.table.update_node_id(self.id);
        self.storage.node_ids = vec![self.id];
    }

    /// `send_single_refresh` — ping a stale node (full bucket) or
    /// `get_peers` toward a random in-bucket id (upstream folds
    /// `find_node` into `get_peers` for the extra `values` chance).
    /// `nid` is the probed node's known id (`zeros` for `add_node`).
    fn send_single_refresh(&mut self, ep: SocketAddr, bucket: usize, nid: NodeId) {
        // upstream asserts `bucket <= 159` — `add_node` passes
        // `num_active_buckets()` which can be 160 on a full table.
        let bucket = bucket.min(159);
        let mut em = Dictionary::new();
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
        let tid = rpc.invoke(e, ep, PING_TAG, our_id, ro, v6, |e2, t| {
            outbox.push((bitdaemon_bencode::bencode(&e2), t));
            true
        });
        self.probe_track(tid, ep, nid);
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
/// Bit 62 marks `put` batch tags — replies/timouts resolve a
/// `PutBatch` and eventually fire `DhtEvent::PutDone`.
const PUT_BIT: u64 = 1 << 62;

/// One `put` fan-out in flight — upstream `put_data`'s success
/// accounting (`put_item_ctx` sums `ok` across node families).
#[derive(Debug)]
struct PutBatch {
    /// puts still awaiting a reply or timeout.
    pending: usize,
    /// puts answered with a real `r` reply.
    ok: usize,
}

fn pack_tag(lookup: usize, entry: usize) -> u64 {
    ((lookup as u64) << 32) | entry as u64
}
fn unpack_tag(tag: u64) -> (usize, usize) {
    (
        usize::try_from(tag >> 32).unwrap_or(usize::MAX),
        (tag & 0xffff_ffff) as usize,
    )
}
