// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/node_entry.cpp` — routing-table row.

use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

use crate::node_id::{NodeId, verify_id};

/// `node_entry` — a routing-table entry.
#[derive(Debug, Clone)]
pub struct NodeEntry {
    /// `id` — `None` models `node_id{nullptr}` (ping-only entry).
    pub id: Option<NodeId>,
    /// `endpoint`.
    pub endpoint: SocketAddr,
    /// `rtt` — 0xffff means "never measured".
    pub rtt: u16,
    /// `timeout_count` — 0xff means "never pinged".
    timeout_count: u8,
    /// `verified` — BEP-42 check on creation.
    pub verified: bool,
    /// `last_queried` — `min_time()` when never queried.
    pub last_queried: Option<Instant>,
}

impl NodeEntry {
    /// `node_entry(id, ep, rtt, pinged)`.
    #[must_use]
    pub fn new(id: NodeId, ep: SocketAddr, rtt: i32, pinged: bool) -> Self {
        Self {
            id: Some(id),
            endpoint: ep,
            rtt: (rtt & 0xffff) as u16,
            timeout_count: if pinged { 0 } else { 0xff },
            verified: verify_id(&id, &ep.ip()),
            last_queried: pinged.then(Instant::now),
        }
    }

    /// `node_entry(ep)` — ping-only.
    #[must_use]
    pub fn ping_only(ep: SocketAddr) -> Self {
        Self {
            id: None,
            endpoint: ep,
            rtt: 0xffff,
            timeout_count: 0xff,
            verified: false,
            last_queried: None,
        }
    }

    /// `update_rtt` — two-thirds old, one-third new.
    pub fn update_rtt(&mut self, new_rtt: i32) {
        debug_assert!((0..=0xffff).contains(&new_rtt));
        if new_rtt == 0xffff {
            return;
        }
        self.rtt = if self.rtt == 0xffff {
            new_rtt as u16
        } else {
            (i32::from(self.rtt) * 2 / 3 + new_rtt / 3) as u16
        };
    }

    /// `pinged()`.
    #[must_use]
    pub fn pinged(&self) -> bool {
        self.timeout_count != 0xff
    }
    /// `set_pinged()`.
    pub fn set_pinged(&mut self) {
        if self.timeout_count == 0xff {
            self.timeout_count = 0;
        }
    }
    /// `timed_out()`.
    pub fn timed_out(&mut self) {
        if self.pinged() && self.timeout_count < 0xfe {
            self.timeout_count += 1;
        }
    }
    /// `fail_count()`.
    #[must_use]
    pub fn fail_count(&self) -> i32 {
        if self.pinged() {
            i32::from(self.timeout_count)
        } else {
            0
        }
    }
    /// `reset_fail_count()`.
    pub fn reset_fail_count(&mut self) {
        if self.pinged() {
            self.timeout_count = 0;
        }
    }
    /// upstream `timeout_count = 0` (unconditional — turns an
    /// un-pinged entry into a confirmed one).
    pub fn clear_timeout_count(&mut self) {
        self.timeout_count = 0;
    }
    /// `confirmed()` — pinged with zero failures.
    #[must_use]
    pub fn confirmed(&self) -> bool {
        self.timeout_count == 0
    }
    /// `addr()`.
    #[must_use]
    pub fn addr(&self) -> IpAddr {
        self.endpoint.ip()
    }
    /// `port()`.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.endpoint.port()
    }

    /// `operator<` — smaller is better: verified first, then by rtt.
    #[must_use]
    pub fn better_than(&self, rhs: &Self) -> bool {
        (!self.verified, self.rtt) < (!rhs.verified, rhs.rtt)
    }
}
