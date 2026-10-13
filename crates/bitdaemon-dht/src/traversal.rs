// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/traversal_algorithm.cpp` + `observer.hpp` —
//! the iterative Kademlia lookup state machine.
//!
//! Upstream couples the algorithm to `node`/`rpc_manager` through
//! `observer` shared_ptrs; here the observer is a lightweight
//! [`ObserverEntry`] (id, endpoint, flags) and invocation is a
//! caller-supplied closure. Routing-table callbacks
//! (`heard_about`/`node_failed`, router nodes, bucket size) come in
//! via [`TraverseHooks`]. This keeps the search logic identical
//! while letting the wire side live in `rpc`/the session.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};

use crate::node_id::{NodeId, compare_ref};

/// `observer::flag_*`.
pub mod obs_flags {
    /// `flag_queried` — request sent.
    pub const QUERIED: u8 = 0x01;
    /// `flag_initial` — from the node cache/router list (exempt
    /// from `restrict_search_ips`).
    pub const INITIAL: u8 = 0x02;
    /// `flag_no_id` — id was generated locally (router entries).
    pub const NO_ID: u8 = 0x04;
    /// `flag_short_timeout` — branch factor was bumped for it.
    pub const SHORT_TIMEOUT: u8 = 0x08;
    /// `flag_failed`.
    pub const FAILED: u8 = 0x10;
    /// `flag_ipv6_address`.
    pub const IPV6_ADDRESS: u8 = 0x20;
    /// `flag_alive` — replied.
    pub const ALIVE: u8 = 0x40;
    /// `flag_done` — post-completion, don't call finished/failed.
    pub const DONE: u8 = 0x80;
}

/// `traversal_flags` for [`Traversal::failed`].
pub mod trav_flags {
    /// `short_timeout` — >2 s without reply; keep the observer
    /// around but free a branch-factor slot.
    pub const SHORT_TIMEOUT: u8 = 1;
}

/// An observer slot in the results list (`observer` upstream).
#[derive(Debug, Clone)]
pub struct ObserverEntry {
    /// `m_id` — `None` when `NO_ID`.
    pub id: Option<NodeId>,
    /// `target_ep()`.
    pub ep: SocketAddr,
    /// `flags`.
    pub flags: u8,
}

impl ObserverEntry {
    /// `observer(id, ep)`.
    #[must_use]
    pub fn new(id: NodeId, ep: SocketAddr) -> Self {
        Self {
            id: Some(id),
            ep,
            flags: 0,
        }
    }

    fn id(&self) -> NodeId {
        self.id.unwrap_or_default()
    }
}

/// Services the algorithm needs from the DHT node — supplied by
/// the caller each step (upstream reads them off `m_node`).
#[derive(Debug)]
pub struct TraverseHooks {
    /// `m_table.bucket_size()` — k.
    pub bucket_size: usize,
    /// `dht_aggressive_lookups`.
    pub aggressive: bool,
    /// `dht_restrict_search_ips`.
    pub restrict_search_ips: bool,
    /// initial `m_branch_factor` (`node::branch_factor()`).
    pub branch_factor: i8,
    /// `m_table` router nodes (`flag_initial`, no id).
    pub router_nodes: Vec<SocketAddr>,
}

/// `traversal_algorithm`.
pub struct Traversal {
    /// `m_results` — observers sorted by distance to target for
    /// the first `sorted_results` entries.
    results: Vec<ObserverEntry>,
    /// `m_sorted_results`.
    sorted_results: usize,
    /// `m_target`.
    target: NodeId,
    /// `m_invoke_count`.
    invoke_count: i16,
    /// `m_branch_factor`.
    branch_factor: i8,
    /// `m_responses`.
    responses: i16,
    /// `m_timeouts`.
    timeouts: i16,
    /// `m_done`.
    done: bool,
    /// `m_abort`.
    abort: bool,
    /// `m_peer4_prefixes` — /24 dedup (`restrict_search_ips`).
    peer4_prefixes: HashSet<u32>,
    /// `m_peer6_prefixes` — /64 dedup.
    peer6_prefixes: HashSet<u64>,
    /// set by `done()` — the caller stops feeding it.
    completed: bool,
}

/// Result of an `add_requests` pass — entries that should be
/// invoked (upstream calls the virtual `invoke()`).
#[derive(Debug)]
pub enum Step {
    /// keep going — `invoke` was issued for the returned indices.
    Pending(Vec<usize>),
    /// traversal is complete (`done()` ran).
    Done,
}

impl Traversal {
    /// `traversal_algorithm(node, target)`.
    #[must_use]
    pub fn new(target: NodeId, hooks: &TraverseHooks) -> Self {
        Self {
            results: Vec::new(),
            sorted_results: 0,
            target,
            invoke_count: 0,
            branch_factor: hooks.branch_factor,
            responses: 0,
            timeouts: 0,
            done: false,
            abort: false,
            peer4_prefixes: HashSet::new(),
            peer6_prefixes: HashSet::new(),
            completed: false,
        }
    }

    /// `abort()`.
    pub fn abort(&mut self) {
        self.abort = true;
    }
    /// `m_done`.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done
    }
    /// `m_results` (sorted prefix first).
    #[must_use]
    pub fn results(&self) -> &[ObserverEntry] {
        &self.results
    }
    /// `observer::reply` — upstream stamps the responder's id onto
    /// the observer (`o->m_id = id`), which is how `NO_ID` router
    /// entries become addressable for the announce phase.
    pub fn set_id(&mut self, idx: usize, id: NodeId) {
        if let Some(o) = self.results.get_mut(idx) {
            if o.id.is_none() {
                o.id = Some(id);
            }
        }
    }
    /// `target()`.
    #[must_use]
    pub fn target(&self) -> &NodeId {
        &self.target
    }
    /// `invoke_count()`.
    #[must_use]
    pub fn invoke_count(&self) -> i16 {
        self.invoke_count
    }
    /// `branch_factor()`.
    #[must_use]
    pub fn branch_factor(&self) -> i8 {
        self.branch_factor
    }
    /// `num_responses()` / `num_timeouts()`.
    #[must_use]
    pub fn responses(&self) -> i16 {
        self.responses
    }
    /// `m_timeouts`.
    #[must_use]
    pub fn timeouts(&self) -> i16 {
        self.timeouts
    }

    /// `add_entry(id, addr, flags)` — insert sorted by distance,
    /// dedup by id and by CIDR prefix.
    pub fn add_entry(&mut self, id: NodeId, addr: SocketAddr, flags: u8, hooks: &TraverseHooks) {
        if self.done {
            return;
        }
        let mut o = ObserverEntry::new(id, addr);
        o.flags = flags
            | (if addr.is_ipv6() {
                obs_flags::IPV6_ADDRESS
            } else {
                0
            });
        if flags & obs_flags::NO_ID != 0 {
            self.results.push(o);
        } else {
            // sorted insertion among the first sorted_results
            let pos = self.results[..self.sorted_results]
                .partition_point(|e| compare_ref(&e.id(), &o.id(), &self.target));
            let dup = self
                .results
                .get(pos)
                .is_some_and(|e| pos < self.sorted_results && e.id() == id);
            if dup {
                return;
            }
            if hooks.restrict_search_ips && flags & obs_flags::INITIAL == 0 {
                let too_close = match addr.ip() {
                    IpAddr::V4(a) => {
                        let p = u32::from_be_bytes(a.octets()) & 0xffff_ff00;
                        !self.peer4_prefixes.insert(p)
                    }
                    IpAddr::V6(a) => {
                        let o = a.octets();
                        let p =
                            u64::from_be_bytes([o[0], o[1], o[2], o[3], o[4], o[5], o[6], o[7]]);
                        !self.peer6_prefixes.insert(p)
                    }
                };
                if too_close {
                    return;
                }
            }
            self.results.insert(pos, o);
            self.sorted_results += 1;
        }

        // cap at 100 — abandon outstanding queries past the cut
        if self.results.len() > 100 {
            for e in &mut self.results[100..] {
                if e.flags & (obs_flags::QUERIED | obs_flags::FAILED | obs_flags::ALIVE)
                    == obs_flags::QUERIED
                {
                    e.flags |= obs_flags::DONE;
                    self.invoke_count -= 1;
                    if e.flags & obs_flags::SHORT_TIMEOUT != 0 {
                        self.branch_factor -= 1;
                    }
                }
            }
            self.results.truncate(100);
            self.sorted_results = self.sorted_results.min(100);
        }
    }

    /// `traverse(id, addr)` — a node told us about another node.
    /// `heard_about` is the caller's routing-table hook.
    pub fn traverse(
        &mut self,
        id: NodeId,
        addr: SocketAddr,
        hooks: &TraverseHooks,
        mut heard_about: impl FnMut(NodeId, SocketAddr),
    ) {
        if self.done {
            return;
        }
        heard_about(id, addr);
        self.add_entry(id, addr, 0, hooks);
    }

    /// `add_requests` — invoke until `branch_factor` outstanding.
    /// `invoke(i, &entry)` is the wire hook; `true` counts the call.
    pub fn add_requests(
        &mut self,
        hooks: &TraverseHooks,
        mut invoke: impl FnMut(usize, &ObserverEntry) -> bool,
    ) -> Step {
        if self.done {
            return Step::Done;
        }
        let mut results_target = hooks.bucket_size as i32;
        let mut outstanding = 0i16;
        let agg = hooks.aggressive;
        let mut invoked = Vec::new();

        for i in 0..self.results.len() {
            if results_target <= 0
                || if agg {
                    outstanding >= self.branch_factor as i16
                } else {
                    self.invoke_count >= self.branch_factor as i16
                }
            {
                break;
            }
            let o = &mut self.results[i];
            if o.flags & obs_flags::ALIVE != 0 {
                results_target -= 1;
                continue;
            }
            if o.flags & obs_flags::QUERIED != 0 {
                if o.flags & obs_flags::FAILED == 0 {
                    outstanding += 1;
                }
                continue;
            }
            if self.abort {
                continue;
            }
            o.flags |= obs_flags::QUERIED;
            if invoke(i, &self.results[i]) {
                self.invoke_count += 1;
                outstanding += 1;
                invoked.push(i);
            } else {
                self.results[i].flags |= obs_flags::FAILED;
            }
        }

        if (results_target == 0 && outstanding == 0) || self.invoke_count == 0 {
            self.finish();
            Step::Done
        } else {
            Step::Pending(invoked)
        }
    }

    /// `start` — seed from router nodes when empty, then run.
    pub fn start(
        &mut self,
        hooks: &TraverseHooks,
        invoke: impl FnMut(usize, &ObserverEntry) -> bool,
    ) -> Step {
        if self.results.len() < 3 {
            for ep in hooks.router_nodes.clone() {
                self.add_entry(
                    NodeId::default(),
                    ep,
                    obs_flags::INITIAL | obs_flags::NO_ID,
                    hooks,
                );
            }
        }
        self.add_requests(hooks, invoke)
    }

    /// `finished(o)` — observer at `idx` replied.
    pub fn finished(
        &mut self,
        idx: usize,
        hooks: &TraverseHooks,
        invoke: impl FnMut(usize, &ObserverEntry) -> bool,
    ) -> Step {
        if idx >= self.results.len() {
            return Step::Pending(vec![]);
        }
        let o = &mut self.results[idx];
        if o.flags & obs_flags::SHORT_TIMEOUT != 0 {
            self.branch_factor -= 1;
        }
        o.flags |= obs_flags::ALIVE;
        self.responses += 1;
        self.invoke_count -= 1;
        self.add_requests(hooks, invoke)
    }

    /// `failed(o, flags)` — `SHORT_TIMEOUT` frees a branch slot,
    /// final failure marks the entry and reports `node_failed`.
    pub fn failed(
        &mut self,
        idx: usize,
        flags: u8,
        hooks: &TraverseHooks,
        mut node_failed: impl FnMut(NodeId, SocketAddr),
        invoke: impl FnMut(usize, &ObserverEntry) -> bool,
    ) -> Step {
        if idx >= self.results.len() {
            return Step::Pending(vec![]);
        }
        {
            let o = &self.results[idx];
            if o.flags & obs_flags::NO_ID == 0 {
                node_failed(o.id(), o.ep);
            }
        }
        if self.results.is_empty() {
            return Step::Pending(vec![]);
        }

        let mut decrement = false;
        if flags & trav_flags::SHORT_TIMEOUT != 0 {
            let o = &mut self.results[idx];
            if o.flags & obs_flags::SHORT_TIMEOUT == 0 && self.branch_factor < i8::MAX {
                self.branch_factor += 1;
                o.flags |= obs_flags::SHORT_TIMEOUT;
            }
        } else {
            let o = &mut self.results[idx];
            o.flags |= obs_flags::FAILED;
            decrement = o.flags & obs_flags::SHORT_TIMEOUT != 0;
            self.timeouts += 1;
            self.invoke_count -= 1;
        }
        if decrement {
            self.branch_factor -= 1;
            if self.branch_factor <= 0 {
                self.branch_factor = 1;
            }
        }
        self.add_requests(hooks, invoke)
    }

    /// `done()` — mark all outstanding queries done, clear results.
    fn finish(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        for o in &mut self.results {
            if o.flags
                & (obs_flags::QUERIED
                    | obs_flags::ALIVE
                    | obs_flags::DONE
                    | obs_flags::FAILED
                    | obs_flags::SHORT_TIMEOUT)
                == (obs_flags::QUERIED | obs_flags::SHORT_TIMEOUT)
            {
                self.branch_factor -= 1;
            }
            if o.flags & (obs_flags::QUERIED | obs_flags::FAILED) == obs_flags::QUERIED {
                o.flags |= obs_flags::DONE;
            }
        }
        self.completed = true;
        self.results.clear();
        self.sorted_results = 0;
        self.invoke_count = 0;
    }
}
