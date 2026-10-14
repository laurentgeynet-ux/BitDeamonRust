// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `peer_class.hpp`/`peer_class.cpp`,
//! `aux_/peer_class_set.hpp`/`peer_class_set.cpp` and
//! `peer_class_type_filter.hpp` — bandwidth classes, the fixed-size
//! set each peer/torrent carries, and the per-socket-type add/remove
//! rules applied when a peer connects.

use crate::bandwidth::BandwidthChannel;
use std::sync::Arc;

/// `upload_channel`/`download_channel` indices into `channel[2]`.
pub const UPLOAD: usize = 0;
/// see above.
pub const DOWNLOAD: usize = 1;

/// `peer_class_info` — the settings applied to a class.
#[derive(Clone, Debug)]
pub struct PeerClassInfo {
    /// `ignore_unchoke_slots` — exempt from the unchoke slot limit.
    pub ignore_unchoke_slots: bool,
    /// `connection_limit_factor` — % of `connections_limit` (100 = all).
    pub connection_limit_factor: i32,
    /// `label` — diagnostics name.
    pub label: String,
    /// `upload_limit` bytes/s (0 = unlimited).
    pub upload_limit: i64,
    /// `download_limit` bytes/s.
    pub download_limit: i64,
    /// `upload_priority`/`download_priority` — 1..=255 throttle weight.
    pub upload_priority: i32,
    pub download_priority: i32,
}

/// Upstream clamp in `peer_class::set_upload_limit`/`set_download_limit`:
/// negative → 0 (unlimited), `0 < l < 10` → 10 B/s floor; `0` stays
/// unlimited (upstream's second test is `limit < 10 && limit > 0`).
fn clamp_limit(limit: i64) -> i64 {
    if limit < 0 {
        0
    } else if limit > 0 && limit < 10 {
        10
    } else {
        limit
    }
}

/// `peer_class` — one bandwidth/throttle class.
#[derive(Debug)]
pub struct PeerClass {
    /// `channel[2]` — rate-limited channels (up/down). `Arc` because
    /// queued `bw_request`s hold channel clones past the class's
    /// lifetime in the pool (upstream keeps raw pointers; pool slots
    /// stay alive as long as requests reference them).
    pub channel: [Arc<BandwidthChannel>; 2],
    /// `ignore_unchoke_slots`.
    pub ignore_unchoke_slots: bool,
    /// `connection_limit_factor`.
    pub connection_limit_factor: i32,
    /// `priority[2]` — bandwidth allocator weights.
    pub priority: [i32; 2],
    /// `label`.
    pub label: String,
    /// `in_use` — slot taken (pool keeps the index stable).
    in_use: bool,
    /// `references` — upstream refcount via `peer_class_set`.
    references: u32,
}

impl PeerClass {
    /// `peer_class(label)` — `in_use = true`, `references = 1`.
    fn new(label: impl Into<String>) -> Self {
        Self {
            channel: [
                Arc::new(BandwidthChannel::new()),
                Arc::new(BandwidthChannel::new()),
            ],
            ignore_unchoke_slots: false,
            connection_limit_factor: 100,
            priority: [1, 1],
            label: label.into(),
            in_use: true,
            references: 1,
        }
    }

    /// `peer_class::clear` — slot released.
    fn clear(&mut self) {
        self.in_use = false;
        self.label.clear();
    }

    /// `peer_class::set_upload_limit` — clamps per upstream before
    /// throttling the upload channel.
    pub fn set_upload_limit(&mut self, limit: i64) {
        debug_assert!(limit >= -1);
        self.channel[UPLOAD].throttle(clamp_limit(limit));
    }

    /// `peer_class::set_download_limit`.
    pub fn set_download_limit(&mut self, limit: i64) {
        debug_assert!(limit >= -1);
        self.channel[DOWNLOAD].throttle(clamp_limit(limit));
    }

    /// `peer_class::set_info` — priorities are clamped to `1..=255`.
    pub fn set_info(&mut self, pci: &PeerClassInfo) {
        self.ignore_unchoke_slots = pci.ignore_unchoke_slots;
        self.connection_limit_factor = pci.connection_limit_factor;
        self.label.clone_from(&pci.label);
        self.set_upload_limit(pci.upload_limit);
        self.set_download_limit(pci.download_limit);
        self.priority[UPLOAD] = pci.upload_priority.clamp(1, 255);
        self.priority[DOWNLOAD] = pci.download_priority.clamp(1, 255);
    }

    /// `peer_class::get_info`.
    #[must_use]
    pub fn get_info(&self) -> PeerClassInfo {
        PeerClassInfo {
            ignore_unchoke_slots: self.ignore_unchoke_slots,
            connection_limit_factor: self.connection_limit_factor,
            label: self.label.clone(),
            upload_limit: self.channel[UPLOAD].throttle_value(),
            download_limit: self.channel[DOWNLOAD].throttle_value(),
            upload_priority: self.priority[UPLOAD],
            download_priority: self.priority[DOWNLOAD],
        }
    }
}

/// `peer_class_t` — index into the pool.
pub type PeerClassIndex = u32;

/// `peer_class_pool` — slot-stable class registry with free-list reuse.
#[derive(Debug, Default)]
pub struct PeerClassPool {
    /// `m_peer_classes` — `aux::deque<peer_class>`.
    classes: Vec<PeerClass>,
    /// `m_free_list` — indices whose slots are not in use.
    free_list: Vec<PeerClassIndex>,
}

impl PeerClassPool {
    /// Upstream seeds `global`, `tcp`, `utp`, … classes; we pre-create
    /// `global` and `tcp`, the session adds the rest.
    #[must_use]
    pub fn new() -> Self {
        let mut p = Self::default();
        p.new_peer_class("global");
        p.new_peer_class("tcp");
        p
    }

    /// `new_peer_class` — reuse a freed slot or grow.
    pub fn new_peer_class(&mut self, label: impl Into<String>) -> PeerClassIndex {
        let label = label.into();
        if let Some(i) = self.free_list.pop() {
            self.classes[i as usize] = PeerClass::new(label);
            return i;
        }
        self.classes.push(PeerClass::new(label));
        self.classes.len() as u32 - 1
    }

    /// `decref` — drops a reference; frees the slot at 0.
    pub fn decref(&mut self, c: PeerClassIndex) {
        let Some(pc) = self.classes.get_mut(c as usize) else {
            return;
        };
        debug_assert!(pc.in_use && pc.references > 0);
        if pc.references == 0 {
            return;
        }
        pc.references -= 1;
        if pc.references != 0 {
            return;
        }
        pc.clear();
        self.free_list.push(c);
    }

    /// `incref`.
    pub fn incref(&mut self, c: PeerClassIndex) {
        if let Some(pc) = self.classes.get_mut(c as usize) {
            debug_assert!(pc.in_use);
            pc.references += 1;
        }
    }

    /// `at` — `None` for unused/out-of-range slots.
    pub fn at(&self, c: PeerClassIndex) -> Option<&PeerClass> {
        self.classes.get(c as usize).filter(|p| p.in_use)
    }

    /// `at` (mut).
    pub fn at_mut(&mut self, c: PeerClassIndex) -> Option<&mut PeerClass> {
        self.classes.get_mut(c as usize).filter(|p| p.in_use)
    }
}

/// `peer_class_set` — upstream `aux::array<peer_class_t, 15>`: a fixed
/// multiset of classes applied to a peer connection or torrent.
pub const PEER_CLASS_SET_SIZE: usize = 15;

/// `peer_class_set`. Refcounts go through the pool on add/remove.
#[derive(Clone, Debug)]
pub struct PeerClassSet {
    /// `m_class[15]` — `u32::MAX` = empty slot.
    classes: [PeerClassIndex; PEER_CLASS_SET_SIZE],
    /// `m_size` — used prefix (upstream packs the tail, not sparse slots).
    size: u8,
}

impl Default for PeerClassSet {
    fn default() -> Self {
        Self {
            classes: [u32::MAX; PEER_CLASS_SET_SIZE],
            size: 0,
        }
    }
}

impl PeerClassSet {
    /// `add_class` — dedup insert, `incref` on success.
    pub fn add_class(&mut self, pool: &mut PeerClassPool, c: PeerClassIndex) {
        if self.has_class(c) {
            return;
        }
        if self.size as usize >= PEER_CLASS_SET_SIZE {
            debug_assert!(false, "peer_class_set overflow");
            return;
        }
        self.classes[self.size as usize] = c;
        pool.incref(c);
        self.size += 1;
    }

    /// `has_class`.
    #[must_use]
    pub fn has_class(&self, c: PeerClassIndex) -> bool {
        self.classes[..self.size as usize].contains(&c)
    }

    /// `remove_class` — swap-with-last removal, `decref`.
    pub fn remove_class(&mut self, pool: &mut PeerClassPool, c: PeerClassIndex) {
        let Some(idx) = self.classes[..self.size as usize]
            .iter()
            .position(|&s| s == c)
        else {
            return;
        };
        let last = self.size as usize - 1;
        if idx < last {
            self.classes[idx] = self.classes[last];
        }
        self.classes[last] = u32::MAX;
        self.size -= 1;
        pool.decref(c);
    }

    /// `num_classes`.
    #[must_use]
    pub fn num_classes(&self) -> usize {
        self.size as usize
    }

    /// `class_at`.
    #[must_use]
    pub fn class_at(&self, i: usize) -> PeerClassIndex {
        self.classes[i]
    }

    /// Iterate the applied classes.
    pub fn for_each(&self, mut f: impl FnMut(PeerClassIndex)) {
        for &c in &self.classes[..self.size as usize] {
            f(c);
        }
    }
}

/// `peer_class_type_filter::socket_type_t` — index into the rule tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FilterSocketType {
    /// `tcp_socket`
    Tcp = 0,
    /// `utp_socket`
    Utp,
    /// `ssl_tcp_socket`
    SslTcp,
    /// `ssl_utp_socket`
    SslUtp,
    /// `i2p_socket`
    I2p,
    /// `rtc_socket`
    Rtc,
}

/// `num_socket_types`.
pub const NUM_SOCKET_TYPES: usize = 6;

/// `peer_class_type_filter` — per-socket-type add/remove rules applied
/// after the IP-based peer-class filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerClassTypeFilter {
    /// `m_peer_class_type_mask` — bits *kept* per socket type.
    mask: [u32; NUM_SOCKET_TYPES],
    /// `m_peer_class_type` — bits *added* per socket type.
    add: [u32; NUM_SOCKET_TYPES],
}

impl Default for PeerClassTypeFilter {
    fn default() -> Self {
        Self {
            mask: [0xffff_ffff; NUM_SOCKET_TYPES],
            add: [0; NUM_SOCKET_TYPES],
        }
    }
}

impl PeerClassTypeFilter {
    /// `add` — peer class added to peers of this socket type.
    /// Classes > 31 are rejected (upstream assert + early return).
    pub fn add(&mut self, st: FilterSocketType, peer_class: u32) {
        if peer_class > 31 {
            return;
        }
        self.add[st as usize] |= 1 << peer_class;
    }

    /// `remove`.
    pub fn remove(&mut self, st: FilterSocketType, peer_class: u32) {
        if peer_class > 31 {
            return;
        }
        self.add[st as usize] &= !(1 << peer_class);
    }

    /// `disallow` — class removed from new peers of this socket type.
    pub fn disallow(&mut self, st: FilterSocketType, peer_class: u32) {
        if peer_class > 31 {
            return;
        }
        self.mask[st as usize] &= !(1 << peer_class);
    }

    /// `allow`.
    pub fn allow(&mut self, st: FilterSocketType, peer_class: u32) {
        if peer_class > 31 {
            return;
        }
        self.mask[st as usize] |= 1 << peer_class;
    }

    /// `apply` — filter then add, per socket type.
    #[must_use]
    pub fn apply(&self, st: FilterSocketType, peer_class_mask: u32) -> u32 {
        (peer_class_mask & self.mask[st as usize]) | self.add[st as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_clamps() {
        let mut c = PeerClass::new("x");
        c.set_upload_limit(-1); // contract: limit >= -1; <0 → 0 (unlimited)
        assert_eq!(c.channel[UPLOAD].throttle_value(), 0);
        c.set_upload_limit(0); // 0 stays 0 — unlimited, no floor
        assert_eq!(c.channel[UPLOAD].throttle_value(), 0);
        c.set_upload_limit(5);
        assert_eq!(c.channel[UPLOAD].throttle_value(), 10);
        c.set_upload_limit(500);
        assert_eq!(c.channel[UPLOAD].throttle_value(), 500);
    }

    #[test]
    fn info_priority_clamp() {
        let mut c = PeerClass::new("x");
        let mut pci = c.get_info();
        pci.upload_priority = 0;
        pci.download_priority = 999;
        c.set_info(&pci);
        assert_eq!(c.priority[UPLOAD], 1);
        assert_eq!(c.priority[DOWNLOAD], 255);
    }

    #[test]
    fn pool_refcount_and_reuse() {
        let mut p = PeerClassPool::default();
        let a = p.new_peer_class("a");
        let b = p.new_peer_class("b");
        p.incref(a);
        p.decref(a); // refs 2→1: still in use
        assert!(p.at(a).is_some());
        p.decref(a); // refs 1→0: freed
        assert!(p.at(a).is_none());
        let c = p.new_peer_class("c"); // reuses freed slot
        assert_eq!(c, a);
        assert_eq!(p.at(b).unwrap().label, "b");
    }

    #[test]
    fn set_add_remove() {
        let mut pool = PeerClassPool::default();
        let a = pool.new_peer_class("a");
        let mut s = PeerClassSet::default();
        s.add_class(&mut pool, a);
        s.add_class(&mut pool, a); // dedup
        assert_eq!(s.num_classes(), 1);
        assert!(s.has_class(a));
        s.remove_class(&mut pool, a);
        assert_eq!(s.num_classes(), 0);
        // creator's own reference keeps the class alive (refs 2→1)
        assert!(pool.at(a).is_some());
        pool.decref(a); // refs 1→0 → freed
        assert!(pool.at(a).is_none());
    }

    #[test]
    fn type_filter_apply() {
        let mut f = PeerClassTypeFilter::default();
        f.add(FilterSocketType::Utp, 3);
        f.disallow(FilterSocketType::Tcp, 1);
        assert_eq!(f.apply(FilterSocketType::Utp, 0b10), 0b10 | 0b1000);
        assert_eq!(f.apply(FilterSocketType::Tcp, 0b11), 0b01);
        f.allow(FilterSocketType::Tcp, 1);
        assert_eq!(f.apply(FilterSocketType::Tcp, 0b11), 0b11);
        // class > 31 rejected
        f.add(FilterSocketType::Tcp, 40);
        assert_eq!(f.apply(FilterSocketType::Tcp, 0), 0);
    }
}
