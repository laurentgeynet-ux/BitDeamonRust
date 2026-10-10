// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `peer_class` + `peer_class_pool` — bandwidth classes. Upstream
//! keeps them in a pool referenced by raw index; same here —
//! `PeerClassIndex` is the slot. `peer_class_set` (the 8-slot
//! fixed array on every peer/torrent) is a plain array.

use crate::bandwidth::BandwidthChannel;

/// `upload_channel`/`download_channel` indices into `channel[2]`.
pub const UPLOAD: usize = 0;
/// see above.
pub const DOWNLOAD: usize = 1;

/// `peer_class_info` — the settings applied to a class.
#[derive(Clone, Debug)]
pub struct PeerClassInfo {
    /// `ignore_unchoke_slots` — exempt from the slot limit.
    pub ignore_unchoke_slots: bool,
    /// `connection_limit_factor` — % of `connections_limit` (100 = all).
    pub connection_limit_factor: i32,
    /// `label` — diagnostics name.
    pub label: String,
    /// `upload_limit` bytes/s (0 = unlimited).
    pub upload_limit: i64,
    /// `download_limit` bytes/s.
    pub download_limit: i64,
    /// `upload_priority`/`download_priority` — 1..255 throttle weight.
    pub upload_priority: u8,
    pub download_priority: u8,
}

/// `peer_class` — one bandwidth/throttle class.
#[derive(Debug)]
pub struct PeerClass {
    /// `channel[2]` — rate-limited channels (up/down).
    pub channel: [BandwidthChannel; 2],
    /// `ignore_unchoke_slots`.
    pub ignore_unchoke_slots: bool,
    /// `connection_limit_factor`.
    pub connection_limit_factor: i32,
    /// `priority[2]`.
    pub priority: [u8; 2],
    /// `label`.
    pub label: String,
    /// `in_use` — slot taken (pool keeps the index stable).
    pub in_use: bool,
}

impl PeerClass {
    /// `peer_class(label)`.
    #[must_use]
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            channel: [BandwidthChannel::default(), BandwidthChannel::default()],
            ignore_unchoke_slots: false,
            connection_limit_factor: 100,
            priority: [1, 1],
            label: label.into(),
            in_use: true,
        }
    }

    /// `set_info`.
    pub fn set_info(&mut self, pci: &PeerClassInfo) {
        self.ignore_unchoke_slots = pci.ignore_unchoke_slots;
        self.connection_limit_factor = pci.connection_limit_factor;
        self.label.clone_from(&pci.label);
        self.channel[UPLOAD].throttle(pci.upload_limit);
        self.channel[DOWNLOAD].throttle(pci.download_limit);
        self.priority[UPLOAD] = pci.upload_priority;
        self.priority[DOWNLOAD] = pci.download_priority;
    }

    /// `get_info`.
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

/// `peer_class_pool::peer_class_t` index.
pub type PeerClassIndex = usize;

/// `peer_class_pool` — slot-stable class registry.
/// `global` (0) and `tcp_peer` (1) exist like upstream.
#[derive(Debug, Default)]
pub struct PeerClassPool {
    classes: Vec<PeerClass>,
}

impl PeerClassPool {
    /// upstream ctor — creates `global`, `tcp`, `utp`, `lsd`…
    /// caller adds its own; we pre-create global + tcp.
    #[must_use]
    pub fn new() -> Self {
        let mut p = Self::default();
        p.new_peer_class("global");
        p.new_peer_class("tcp");
        p
    }

    /// `new_peer_class` — reuse a freed slot or grow.
    pub fn new_peer_class(&mut self, label: &str) -> PeerClassIndex {
        if let Some(i) = self.classes.iter().position(|c| !c.in_use) {
            self.classes[i] = PeerClass::new(label);
            return i;
        }
        self.classes.push(PeerClass::new(label));
        self.classes.len() - 1
    }

    /// `decref`/`free_peer_class` — clear the slot.
    pub fn remove_peer_class(&mut self, i: PeerClassIndex) {
        if let Some(c) = self.classes.get_mut(i) {
            c.in_use = false;
            c.label.clear();
        }
    }

    /// `at`.
    pub fn at(&self, i: PeerClassIndex) -> Option<&PeerClass> {
        self.classes.get(i).filter(|c| c.in_use)
    }
    /// `at` mut.
    pub fn at_mut(&mut self, i: PeerClassIndex) -> Option<&mut PeerClass> {
        self.classes.get_mut(i).filter(|c| c.in_use)
    }
}

/// `peer_class_set` — the fixed multiset each peer/torrent has
/// (upstream: 8 slots; unused = -1 → `usize::MAX`).
pub const PEER_CLASS_SET_SIZE: usize = 8;

/// `peer_class_set`.
#[derive(Clone, Debug)]
pub struct PeerClassSet {
    /// `m_class[8]` — `usize::MAX` = empty.
    pub classes: [PeerClassIndex; PEER_CLASS_SET_SIZE],
}

impl Default for PeerClassSet {
    fn default() -> Self {
        Self {
            classes: [usize::MAX; PEER_CLASS_SET_SIZE],
        }
    }
}

impl PeerClassSet {
    /// `add_class` — insert in the first empty slot (dedup).
    pub fn add_class(&mut self, c: PeerClassIndex) -> bool {
        if self.classes.contains(&c) {
            return false;
        }
        if let Some(slot) = self.classes.iter_mut().find(|s| **s == usize::MAX) {
            *slot = c;
            true
        } else {
            false
        }
    }

    /// `remove_class`.
    pub fn remove_class(&mut self, c: PeerClassIndex) -> bool {
        if let Some(slot) = self.classes.iter_mut().find(|s| **s == c) {
            *slot = usize::MAX;
            true
        } else {
            false
        }
    }

    /// `apply` — every class present.
    pub fn for_each(&self, mut f: impl FnMut(PeerClassIndex)) {
        for c in self.classes.iter().filter(|c| **c != usize::MAX) {
            f(*c);
        }
    }
}
