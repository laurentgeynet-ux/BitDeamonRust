// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/routing_table.cpp` — the k-bucket routing
//! table. Positions are indices (`(bucket, replacement, idx)`)
//! instead of the upstream `node_entry*`/`iterator` pointers;
//! everything else is a faithful transcription.

use std::collections::HashSet;
use std::net::IpAddr;
use std::time::Instant;

use crate::dht_settings::DhtSettings;
use crate::node_entry::NodeEntry;
use crate::node_id::{NodeId, compare_ref, distance_exp, verify_id};

/// `ip_set` — the set of IPs present in the table.
#[derive(Debug, Default)]
struct IpSet {
    set: HashSet<IpAddr>,
}

impl IpSet {
    fn insert(&mut self, a: IpAddr) {
        self.set.insert(a);
    }
    fn exists(&self, a: IpAddr) -> bool {
        self.set.contains(&a)
    }
    fn erase(&mut self, a: IpAddr) {
        self.set.remove(&a);
    }
}

/// `routing_table_node` — one k-bucket (live + replacements).
#[derive(Debug, Default)]
pub struct RoutingTableNode {
    /// `live_nodes`.
    pub live_nodes: Vec<NodeEntry>,
    /// `replacements`.
    pub replacements: Vec<NodeEntry>,
}

/// `add_node_status_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddNodeStatus {
    /// `failed_to_add`.
    Failed,
    /// `node_added`.
    Added,
    /// `need_bucket_split`.
    NeedBucketSplit,
}

/// `mostly_verified_nodes` — ≥ 2/3 verified (and non-empty).
#[must_use]
pub fn mostly_verified_nodes(b: &[NodeEntry]) -> bool {
    let num_verified = b.iter().filter(|e| e.verified).count();
    if num_verified == 0 && !b.is_empty() {
        return false;
    }
    num_verified >= b.len() * 2 / 3
}

/// `compare_ip_cidr` — same /24 (v4) or /64 (v6) ⇒ "too close".
#[must_use]
pub fn compare_ip_cidr(lhs: &IpAddr, rhs: &IpAddr) -> bool {
    match (lhs, rhs) {
        (IpAddr::V6(l), IpAddr::V6(r)) => {
            let l = u64::from_be_bytes(l.octets()[..8].try_into().expect("8"));
            let r = u64::from_be_bytes(r.octets()[..8].try_into().expect("8"));
            l ^ r == 0
        }
        (IpAddr::V4(l), IpAddr::V4(r)) => {
            u32::from_be_bytes(l.octets()) ^ u32::from_be_bytes(r.octets()) <= 0x0000_00ff
        }
        _ => false,
    }
}

/// `all_in_same_bucket` — do `b` plus `id` all share the bit at
/// `bucket_index`?
#[must_use]
pub fn all_in_same_bucket(b: &[NodeEntry], id: &NodeId, bucket_index: usize) -> bool {
    let byte_offset = bucket_index / 8;
    let bit_offset = bucket_index % 8;
    let mask = 0x80u8 >> bit_offset;
    let mut counter = [0usize; 2];
    counter[usize::from(id[byte_offset] & mask != 0)] += 1;
    for e in b {
        counter[usize::from(e.id.is_some_and(|i| i[byte_offset] & mask != 0))] += 1;
    }
    counter[0] == 0 || counter[1] == 0
}

/// `classify_prefix` — which prefix-slot `nid` falls into for
/// bucket `bucket_idx` (`bucket_size` must be a power of two ≤256).
#[must_use]
pub fn classify_prefix(bucket_idx: usize, last_bucket: bool, bucket_size: i32, nid: &NodeId) -> u8 {
    debug_assert!(bucket_size > 0 && bucket_size <= 256);
    let mut mask = (bucket_size as u32) - 1;
    debug_assert_eq!(mask & bucket_size as u32, 0);
    // `count_leading_zeros(little_endian_to_host(mask))`: the value
    // fits in a byte; on LE the first byte is the value, so this is
    // the leading zeros of that byte = clz32 - 24
    let mask_shift = mask.leading_zeros() - 24;
    mask <<= mask_shift;
    let shifted = *nid << (bucket_idx as u32 + u32::from(!last_bucket));
    (shifted[0] & mask as u8) >> mask_shift
}

/// `find_nodes_flags`.
pub const INCLUDE_FAILED: u32 = 1;

/// `routing_table` — per-protocol k-bucket table.
pub struct RoutingTable {
    /// `m_buckets`.
    buckets: Vec<RoutingTableNode>,
    /// `m_id` — our own node id.
    id: NodeId,
    /// `m_ips`.
    ips: IpSet,
    /// `m_router_nodes`.
    router_nodes: HashSet<std::net::SocketAddr>,
    /// `m_protocol` — `true` = v6 (`udp::v6()`).
    v6: bool,
    /// `m_bucket_size`.
    bucket_size: usize,
    /// `m_settings`.
    settings: DhtSettings,
    /// `m_depth` — cached depth hint.
    depth: usize,
}

impl RoutingTable {
    /// `routing_table(id, proto, bucket_size, settings, log)`.
    /// `bucket_size` must be a power of two.
    #[must_use]
    pub fn new(id: NodeId, v6: bool, bucket_size: usize, settings: DhtSettings) -> Self {
        debug_assert_eq!((bucket_size - 1) & bucket_size, 0, "power of two");
        Self {
            buckets: Vec::with_capacity(30),
            id,
            ips: IpSet::default(),
            router_nodes: HashSet::new(),
            v6,
            bucket_size,
            settings,
            depth: 0,
        }
    }

    /// `native_endpoint`.
    fn native_endpoint(&self, ep: &std::net::SocketAddr) -> bool {
        ep.is_ipv6() == self.v6
    }

    /// `bucket_limit` — extended routing table gives the low buckets
    /// extra room ({16, 8, 4, 2}× the base size).
    #[must_use]
    pub fn bucket_limit(&self, bucket: usize) -> usize {
        if !self.settings.extended_routing_table {
            return self.bucket_size;
        }
        const SIZE_EXCEPTIONS: [usize; 4] = [16, 8, 4, 2];
        if bucket < SIZE_EXCEPTIONS.len() {
            return self.bucket_size * SIZE_EXCEPTIONS[bucket];
        }
        self.bucket_size
    }

    /// `size()` — `(nodes, replacements, confirmed)`.
    #[must_use]
    pub fn size(&self) -> (usize, usize, usize) {
        let mut nodes = 0;
        let mut replacements = 0;
        let mut confirmed = 0;
        for b in &self.buckets {
            nodes += b.live_nodes.len();
            confirmed += b.live_nodes.iter().filter(|n| n.confirmed()).count();
            replacements += b.replacements.len();
        }
        (nodes, replacements, confirmed)
    }

    /// `num_global_nodes` — estimate.
    #[must_use]
    pub fn num_global_nodes(&self) -> i64 {
        let mut deepest_bucket = 0i64;
        let mut deepest_size = 0usize;
        for b in &self.buckets {
            deepest_size = b.live_nodes.len();
            if deepest_size < self.bucket_size {
                break;
            }
            deepest_bucket += 1;
        }
        if deepest_bucket == 0 {
            return 1 + deepest_size as i64;
        }
        if deepest_size < self.bucket_size / 2 {
            (1i64 << deepest_bucket) * self.bucket_size as i64
        } else {
            (2i64 << deepest_bucket) * deepest_size as i64
        }
    }

    /// `depth()`.
    pub fn depth(&mut self) -> i32 {
        if self.depth >= self.buckets.len() {
            self.depth = self.buckets.len().saturating_sub(1);
        }
        while self.depth < self.buckets.len().saturating_sub(1)
            && self.buckets[self.depth + 1].live_nodes.len() >= self.bucket_size / 2
        {
            self.depth += 1;
        }
        while self.depth > 0 && self.buckets[self.depth - 1].live_nodes.len() < self.bucket_size / 2
        {
            self.depth -= 1;
        }
        self.depth as i32
    }

    /// `num_active_buckets`.
    #[must_use]
    pub fn num_active_buckets(&self) -> usize {
        self.buckets.len()
    }
    /// `bucket_size(i)` — live nodes in bucket `i`.
    #[must_use]
    pub fn bucket_size_at(&self, bucket: usize) -> usize {
        if self.buckets.is_empty() {
            return 0;
        }
        self.buckets[bucket.min(self.buckets.len() - 1)]
            .live_nodes
            .len()
    }
    /// `m_bucket_size`.
    #[must_use]
    pub fn bucket_size(&self) -> usize {
        self.bucket_size
    }
    /// `m_buckets` (read).
    #[must_use]
    pub fn buckets(&self) -> &[RoutingTableNode] {
        &self.buckets
    }
    /// router node set.
    pub fn router_nodes(&self) -> impl Iterator<Item = &std::net::SocketAddr> {
        self.router_nodes.iter()
    }
    /// `add_router_node`.
    pub fn add_router_node(&mut self, ep: std::net::SocketAddr) {
        self.router_nodes.insert(ep);
    }

    /// `for_each_node(live, replacement)` callbacks.
    pub fn for_each_node(
        &self,
        mut live_cb: impl FnMut(&NodeEntry),
        mut replacements_cb: impl FnMut(&NodeEntry),
    ) {
        for b in &self.buckets {
            for n in &b.live_nodes {
                live_cb(n);
            }
            for n in &b.replacements {
                replacements_cb(n);
            }
        }
    }

    /// `find_bucket(id)` — bucket index for `id`.
    fn find_bucket(&mut self, id: &NodeId) -> usize {
        if self.buckets.is_empty() {
            self.buckets.push(RoutingTableNode::default());
        }
        let num_buckets = self.buckets.len();
        (159 - distance_exp(&self.id, id) as usize).min(num_buckets - 1)
    }

    /// `find_node(ep)` — locate `(bucket_idx, is_replacement, idx)`
    /// for endpoint `ep`.
    fn find_ep(&self, ep: &std::net::SocketAddr) -> Option<(usize, bool, usize)> {
        for (bi, b) in self.buckets.iter().enumerate() {
            for (i, n) in b.replacements.iter().enumerate() {
                if n.endpoint == *ep {
                    return Some((bi, true, i));
                }
            }
            for (i, n) in b.live_nodes.iter().enumerate() {
                if n.endpoint == *ep {
                    return Some((bi, false, i));
                }
            }
        }
        None
    }

    /// `fill_from_replacements`.
    fn fill_from_replacements(&mut self, bucket: usize) {
        let limit = self.bucket_limit(bucket);
        let b = &mut self.buckets[bucket];
        if b.live_nodes.len() >= limit {
            return;
        }
        // sort replacements by (verified, rtt) — `node_entry::operator<`
        b.replacements
            .sort_by_key(|a| (!a.verified, a.rtt));
        while b.live_nodes.len() < limit && !b.replacements.is_empty() {
            if let Some(j) = b.replacements.iter().position(|n| n.pinged()) {
                let n = b.replacements.remove(j);
                b.live_nodes.push(n);
            } else {
                break;
            }
        }
    }

    /// `prune_empty_bucket`.
    fn prune_empty_bucket(&mut self) {
        if let Some(last) = self.buckets.last() {
            if last.live_nodes.is_empty() && last.replacements.is_empty() {
                self.buckets.pop();
            }
        }
    }

    /// `remove_node` — remove the entry at a position.
    fn remove_at(&mut self, bucket: usize, replacement: bool, idx: usize) {
        let b = &mut self.buckets[bucket];
        let list = if replacement {
            &mut b.replacements
        } else {
            &mut b.live_nodes
        };
        let n = list.remove(idx);
        self.ips.erase(n.addr());
    }

    /// `add_node` — full logic: impl + split loop.
    pub fn add_node(&mut self, e: NodeEntry) -> bool {
        let mut s = self.add_node_impl(&e);
        if s == AddNodeStatus::Failed {
            return false;
        }
        if s == AddNodeStatus::Added {
            return true;
        }
        while s == AddNodeStatus::NeedBucketSplit {
            self.split_bucket();
            if self.buckets.len() > 50 {
                s = self.add_node_impl(&e);
                return s == AddNodeStatus::Added;
            }
            if self.buckets.last().map_or(0, |b| b.live_nodes.len())
                > self.bucket_limit(self.buckets.len() - 1)
            {
                continue;
            }
            s = self.add_node_impl(&e);
            if self.buckets.last().is_some_and(|b| b.live_nodes.is_empty()) {
                self.buckets.pop();
            }
            if s == AddNodeStatus::Failed {
                return false;
            }
            if s == AddNodeStatus::Added {
                return true;
            }
        }
        false
    }

    /// `verify_node_address` — `dht_enforce_node_id` gate.
    fn verify_node_address(&self, id: &NodeId, addr: &IpAddr) -> bool {
        !self.settings.enforce_node_id || verify_id(id, addr)
    }

    /// `node_seen` — sign of life; returns whether a refresh is needed.
    pub fn node_seen(&mut self, id: NodeId, ep: std::net::SocketAddr, rtt: i32) -> bool {
        self.verify_node_address(&id, &ep.ip()) && self.add_node(NodeEntry::new(id, ep, rtt, true))
    }

    /// `heard_about` — unverified sign of life.
    pub fn heard_about(&mut self, id: NodeId, ep: std::net::SocketAddr) {
        if self.verify_node_address(&id, &ep.ip()) {
            self.add_node(NodeEntry::new(id, ep, 0xffff, false));
        }
    }

    /// `add_node_impl`.
    fn add_node_impl(&mut self, e: &NodeEntry) -> AddNodeStatus {
        let Some(eid) = e.id else {
            return AddNodeStatus::Failed;
        };
        if !self.native_endpoint(&e.endpoint) {
            return AddNodeStatus::Failed;
        }
        if self.router_nodes.contains(&e.endpoint) {
            return AddNodeStatus::Failed;
        }
        if self.ips.exists(e.addr()) {
            // exact IP already present — duplicate handling
            match self.find_ep(&e.endpoint) {
                None => {
                    if self.settings.restrict_routing_ips {
                        return AddNodeStatus::Failed;
                    }
                }
                Some((bi, repl, idx)) => {
                    let existing = {
                        let b = &self.buckets[bi];
                        let l = if repl { &b.replacements } else { &b.live_nodes };
                        l[idx].clone()
                    };
                    if existing.id == e.id {
                        // same id → refresh fail-count/rtt
                        let b = &mut self.buckets[bi];
                        let l = if repl {
                            &mut b.replacements
                        } else {
                            &mut b.live_nodes
                        };
                        let n = &mut l[idx];
                        n.clear_timeout_count();
                        if e.pinged() {
                            n.update_rtt(i32::from(e.rtt));
                            n.last_queried = e.last_queried;
                        }
                        self.fill_from_replacements(bi);
                        self.prune_empty_bucket();
                        return AddNodeStatus::Added;
                    } else if existing.id.is_none_or(|i| i.is_all_zeros()) {
                        self.remove_at(bi, repl, idx);
                    } else if !e.pinged() {
                        if self.settings.restrict_routing_ips {
                            return AddNodeStatus::Failed;
                        }
                    } else {
                        // same IP+port, new node id → evict (possible
                        // poisoning); re-ping the bucket soon
                        self.remove_at(bi, repl, idx);
                        self.fill_from_replacements(bi);
                        let now = Instant::now();
                        for n in &mut self.buckets[bi].live_nodes {
                            if n.last_queried
                                .is_some_and(|t| t + std::time::Duration::from_secs(300) < now)
                            {
                                n.last_queried = None;
                            }
                        }
                        self.prune_empty_bucket();
                        return AddNodeStatus::Failed;
                    }
                }
            }
        }
        if Some(self.id) == e.id {
            return AddNodeStatus::Failed;
        }

        let bi = self.find_bucket(&eid);
        let bucket_size_limit = self.bucket_limit(bi);
        let mut e = e.clone();

        // already in the live bucket?
        if let Some(j) = self.buckets[bi]
            .live_nodes
            .iter()
            .position(|n| n.id == e.id)
        {
            let j_ep = self.buckets[bi].live_nodes[j].endpoint;
            if j_ep != e.endpoint {
                return AddNodeStatus::Failed;
            }
            let n = &mut self.buckets[bi].live_nodes[j];
            n.clear_timeout_count();
            n.update_rtt(i32::from(e.rtt));
            return AddNodeStatus::Added;
        }

        // in the replacement bucket? pull it out
        if let Some(j) = self.buckets[bi]
            .replacements
            .iter()
            .position(|n| n.id == e.id)
        {
            let n = &mut self.buckets[bi].replacements[j];
            if n.endpoint != e.endpoint {
                return AddNodeStatus::Failed;
            }
            n.clear_timeout_count();
            n.update_rtt(i32::from(e.rtt));
            let n = self.buckets[bi].replacements.remove(j);
            self.ips.erase(n.addr());
            e = n;
        }

        if self.settings.restrict_routing_ips {
            let cmp = e.addr();
            let too_close = self.buckets[bi]
                .live_nodes
                .iter()
                .chain(self.buckets[bi].replacements.iter())
                .any(|a| compare_ip_cidr(&a.addr(), &cmp));
            if too_close {
                return AddNodeStatus::Failed;
            }
        }

        // room in the live bucket → insert (pinged only)
        if e.pinged() && self.buckets[bi].live_nodes.len() < bucket_size_limit {
            self.buckets[bi].live_nodes.push(e.clone());
            self.ips.insert(e.addr());
            return AddNodeStatus::Added;
        }

        let last_bucket = bi + 1 == self.buckets.len();

        // can the bucket split? last bucket, confirmed node,
        // mostly-verified when required, and a split would actually
        // spread the nodes
        let can_split = (bi + 1 == self.buckets.len() && self.buckets.len() < 159)
            && (!self.settings.prefer_verified_node_ids
                || (e.verified && mostly_verified_nodes(&self.buckets[bi].live_nodes)))
            && e.confirmed()
            && (bi == 0 || self.buckets[bi - 1].live_nodes.len() > 1)
            && !all_in_same_bucket(&self.buckets[bi].live_nodes, &eid, bi);
        if can_split {
            return AddNodeStatus::NeedBucketSplit;
        }

        if e.confirmed() {
            let ret = self.replace_node_impl(&e, bi, false, bucket_size_limit, last_bucket);
            if ret != AddNodeStatus::NeedBucketSplit {
                return ret;
            }
        }

        // fall through to the replacement bucket
        if let Some(j) = self.buckets[bi]
            .replacements
            .iter()
            .position(|n| n.id == e.id)
        {
            let n = &mut self.buckets[bi].replacements[j];
            if n.endpoint == e.endpoint {
                n.set_pinged();
            }
            return AddNodeStatus::Added;
        }

        if self.buckets[bi].replacements.len() >= self.bucket_size {
            // full — evict the oldest un-pinged, else try replacing
            if let Some(j) = self.buckets[bi]
                .replacements
                .iter()
                .position(|n| !n.pinged())
            {
                self.remove_at(bi, true, j);
            } else {
                let ret = self.replace_node_impl(&e, bi, true, self.bucket_size, last_bucket);
                return if ret == AddNodeStatus::Added {
                    AddNodeStatus::Added
                } else {
                    AddNodeStatus::Failed
                };
            }
        }

        self.buckets[bi].replacements.push(e.clone());
        self.ips.insert(e.addr());
        AddNodeStatus::Added
    }

    /// `replace_node_impl` — pick the worst node to evict.
    /// `replacement` selects which list in the bucket is targeted.
    fn replace_node_impl(
        &mut self,
        e: &NodeEntry,
        bucket_index: usize,
        replacement: bool,
        bucket_size_limit: usize,
        last_bucket: bool,
    ) -> AddNodeStatus {
        let b = &self.buckets[bucket_index];
        let list = if replacement {
            &b.replacements
        } else {
            &b.live_nodes
        };
        debug_assert!(list.len() >= bucket_size_limit);

        // prefer the node with the most failures
        let worst_fail = list
            .iter()
            .enumerate()
            .max_by_key(|(_, n)| n.fail_count())
            .map(|(i, n)| (i, n.fail_count()));
        if let Some((_, fc)) = worst_fail {
            if fc > 0 {
                let idx = worst_fail.map(|(i, _)| i).unwrap_or(0);
                self.replace_at(bucket_index, replacement, idx, e.clone());
                return AddNodeStatus::Added;
            }
        }

        // then prefix-spread: classify into `bucket_size` slots
        let to_add_prefix = classify_prefix(
            bucket_index,
            last_bucket,
            bucket_size_limit as i32,
            &e.id.unwrap_or_default(),
        ) as usize;

        // group node indices by prefix slot
        let mut slots: Vec<Vec<usize>> = vec![Vec::new(); bucket_size_limit];
        for (i, n) in list.iter().enumerate() {
            let p = classify_prefix(
                bucket_index,
                last_bucket,
                bucket_size_limit as i32,
                &n.id.unwrap_or_default(),
            ) as usize;
            slots[p].push(i);
        }

        let victim = if !slots[to_add_prefix].is_empty() {
            // replace only if e is better than the worst in that slot
            let j = *slots[to_add_prefix]
                .iter()
                .max_by(|&&a, &&b| {
                    if list[a].better_than(&list[b]) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                })
                .unwrap_or(&usize::MAX);
            if j != usize::MAX && e.better_than(&list[j]) {
                Some(j)
            } else {
                None
            }
        } else {
            // no node in that slot — evict the worst node from a
            // prefix slot that has duplicates
            let candidates: Vec<usize> = slots
                .iter()
                .filter(|s| s.len() > 1)
                .flatten()
                .copied()
                .collect();
            candidates
                .iter()
                .max_by(|&&a, &&b| {
                    if list[a].better_than(&list[b]) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                })
                .copied()
        };

        if let Some(idx) = victim {
            self.replace_at(bucket_index, replacement, idx, e.clone());
            AddNodeStatus::Added
        } else {
            AddNodeStatus::NeedBucketSplit
        }
    }

    fn replace_at(&mut self, bucket: usize, replacement: bool, idx: usize, e: NodeEntry) {
        let b = &mut self.buckets[bucket];
        let list = if replacement {
            &mut b.replacements
        } else {
            &mut b.live_nodes
        };
        self.ips.erase(list[idx].addr());
        list[idx] = e;
        self.ips.insert(list[idx].addr());
    }

    /// `split_bucket` — split the last bucket.
    pub fn split_bucket(&mut self) {
        let bucket_index = self.buckets.len() - 1;
        let bucket_size_limit = self.bucket_limit(bucket_index);
        debug_assert!(
            self.buckets[bucket_index].live_nodes.len() >= self.bucket_limit(bucket_index + 1)
        );

        self.buckets.push(RoutingTableNode::default());

        // move nodes whose distance falls into the new bucket
        let mut moved: Vec<NodeEntry> = Vec::new();
        let mut kept: Vec<NodeEntry> = Vec::new();
        for n in self.buckets[bucket_index].live_nodes.drain(..) {
            let d = distance_exp(&self.id, &n.id.unwrap_or_default());
            if (d as usize) >= 159 - bucket_index {
                kept.push(n);
            } else {
                moved.push(n);
            }
        }
        self.buckets[bucket_index].live_nodes = kept;
        self.buckets[bucket_index + 1].live_nodes = moved;

        // overflow the (old) bucket's live list into replacements
        if self.buckets[bucket_index].live_nodes.len() > bucket_size_limit {
            let extra: Vec<NodeEntry> = self.buckets[bucket_index]
                .live_nodes
                .split_off(bucket_size_limit);
            self.buckets[bucket_index].replacements.extend(extra);
        }

        // split the replacement bucket too
        let new_bucket_size = self.bucket_limit(bucket_index + 1);
        let old_repl: Vec<NodeEntry> = std::mem::take(&mut self.buckets[bucket_index].replacements);
        let mut stayed: Vec<NodeEntry> = Vec::new();
        let mut moved_live: Vec<NodeEntry> = Vec::new();
        let mut moved_repl: Vec<NodeEntry> = Vec::new();
        for n in old_repl {
            let d = distance_exp(&self.id, &n.id.unwrap_or_default());
            let live_len = self.buckets[bucket_index].live_nodes.len();
            if (d as usize) >= 159 - bucket_index {
                // belongs to the old side — promote to live if room
                if !n.pinged() || live_len >= bucket_size_limit {
                    stayed.push(n);
                } else {
                    self.buckets[bucket_index].live_nodes.push(n);
                }
            } else if n.pinged() && moved_live.len() < new_bucket_size {
                moved_live.push(n);
            } else {
                moved_repl.push(n);
            }
        }
        self.buckets[bucket_index].replacements = stayed;
        self.buckets[bucket_index + 1].live_nodes.extend(moved_live);
        self.buckets[bucket_index + 1].replacements = moved_repl;
    }

    /// `update_node_id` — re-bucket everything against a new own id.
    pub fn update_node_id(&mut self, id: NodeId) {
        self.id = id;
        self.ips = IpSet::default();
        let old = std::mem::take(&mut self.buckets);
        for b in &old {
            for n in &b.live_nodes {
                self.add_node(n.clone());
            }
        }
        for b in &old {
            for n in &b.replacements {
                self.add_node(n.clone());
            }
        }
    }

    /// `next_refresh` — index into `buckets`+`live_nodes` of the
    /// least-recently-queried node, or `None`. Marks it queried.
    pub fn next_refresh(&mut self) -> Option<(usize, usize, NodeId)> {
        let mut candidate: Option<(usize, usize, bool, Instant)> = None;
        let mut found = false;

        for bi in (0..self.buckets.len()).rev() {
            for (i, n) in self.buckets[bi].live_nodes.iter().enumerate() {
                let Some(nid) = n.id else { continue };
                if nid == self.id {
                    continue;
                }
                let Some(lq) = n.last_queried else {
                    candidate = Some((bi, i, false, Instant::now()));
                    found = true;
                    break;
                };
                let better = match candidate {
                    None => true,
                    Some((_, _, _, t)) => lq < t,
                };
                if better {
                    candidate = Some((bi, i, false, lq));
                }
            }
            if found {
                break;
            }
            // not full or splittable → probe an un-pinged replacement
            // (upstream: `i == rbegin` or live < bucket_limit(bi))
            if bi == self.buckets.len() - 1
                || self.buckets[bi].live_nodes.len() < self.bucket_limit(bi)
            {
                if let Some(i) = self.buckets[bi]
                    .replacements
                    .iter()
                    .position(|n| !n.pinged() && n.last_queried.is_none())
                {
                    candidate = Some((bi, i, true, Instant::now()));
                    break;
                }
            }
        }

        let (bi, i, repl, _) = candidate?;
        let n = if repl {
            &mut self.buckets[bi].replacements[i]
        } else {
            &mut self.buckets[bi].live_nodes[i]
        };
        n.last_queried = Some(Instant::now());
        n.id.map(|id| (bi, i, id))
    }

    /// `node_failed`.
    pub fn node_failed(&mut self, nid: &NodeId, ep: &std::net::SocketAddr) {
        if *nid == self.id {
            return;
        }
        let bi = self.find_bucket(nid);

        if let Some(j) = self.buckets[bi]
            .live_nodes
            .iter()
            .position(|n| n.id == Some(*nid))
        {
            if self.buckets[bi].live_nodes[j].endpoint != *ep {
                return; // different node claiming this id
            }
            if self.buckets[bi].replacements.is_empty() {
                let n = &mut self.buckets[bi].live_nodes[j];
                n.timed_out();
                if n.fail_count() >= self.settings.max_fail_count || !n.pinged() {
                    let addr = n.addr();
                    self.ips.erase(addr);
                    self.buckets[bi].live_nodes.remove(j);
                }
                return;
            }
            self.remove_at(bi, false, j);
            self.fill_from_replacements(bi);
            self.prune_empty_bucket();
            return;
        }

        if let Some(j) = self.buckets[bi]
            .replacements
            .iter()
            .position(|n| n.id == Some(*nid))
        {
            if self.buckets[bi].replacements[j].endpoint == *ep {
                self.buckets[bi].replacements[j].timed_out();
            }
        }
    }

    /// `find_node(target, options, count)` — k closest confirmed.
    #[must_use]
    pub fn find_node(
        &mut self,
        target: &NodeId,
        include_failed: bool,
        count: usize,
    ) -> Vec<NodeEntry> {
        let count = if count == 0 { self.bucket_size } else { count };
        let bi = self.find_bucket(target);
        let mut l: Vec<NodeEntry> = Vec::with_capacity(count);

        let collect = |b: &RoutingTableNode, l: &mut Vec<NodeEntry>| {
            for n in &b.live_nodes {
                if include_failed || n.confirmed() {
                    l.push(n.clone());
                }
            }
        };

        let mut unsorted_start = 0usize;
        // forward from target bucket
        for b in self.buckets.iter().skip(bi) {
            collect(b, &mut l);
            if l.len() == count {
                return l;
            }
            if l.len() > count {
                nth_closest(&mut l, unsorted_start, count, target);
                l.truncate(count);
                return l;
            }
            unsorted_start = l.len();
        }
        // backward
        for b in self.buckets.iter().take(bi).rev() {
            collect(b, &mut l);
            if l.len() == count {
                return l;
            }
            if l.len() > count {
                nth_closest(&mut l, unsorted_start, count, target);
                l.truncate(count);
                return l;
            }
            unsorted_start = l.len();
        }
        l
    }

    /// `is_full`.
    #[must_use]
    pub fn is_full(&self, bucket: usize) -> bool {
        self.buckets[bucket].live_nodes.len() >= self.bucket_limit(bucket)
    }
}

/// `std::nth_element(l[unsorted..], count)` by `compare_ref`.
fn nth_closest(l: &mut [NodeEntry], unsorted_start: usize, count: usize, target: &NodeId) {
    l[unsorted_start..].sort_by(|a, b| {
        if compare_ref(&a.id.unwrap_or_default(), &b.id.unwrap_or_default(), target) {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    });
    let _ = count;
}
