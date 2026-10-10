// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/dos_blocker.cpp` — per-source rate limiting
//! for incoming DHT traffic: >`rate_limit*10` messages within 10 s
//! bans the source for `block_timeout` seconds.

use std::net::IpAddr;
use std::time::{Duration, Instant};

/// `num_ban_nodes`.
const NUM_BAN_NODES: usize = 20;

/// `node_ban_entry`.
#[derive(Debug, Clone)]
struct NodeBanEntry {
    src: Option<IpAddr>,
    count: u32,
    /// `limit` — window end (10 s) or ban-until time.
    limit: Option<Instant>,
}

/// `dos_blocker`.
#[derive(Debug)]
pub struct DosBlocker {
    /// `m_message_rate_limit` (5).
    message_rate_limit: u32,
    /// `m_block_timeout` (5 min).
    block_timeout: Duration,
    /// `m_ban_nodes`.
    ban_nodes: [NodeBanEntry; NUM_BAN_NODES],
}

impl Default for DosBlocker {
    fn default() -> Self {
        Self::new()
    }
}

impl DosBlocker {
    /// `dos_blocker()` — upstream defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            message_rate_limit: 5,
            block_timeout: Duration::from_secs(5 * 60),
            ban_nodes: std::array::from_fn(|_| NodeBanEntry {
                src: None,
                count: 0,
                limit: None,
            }),
        }
    }

    /// Configured variant (`block_timeout`/`rate_limit` from
    /// `dht_settings` at call time upstream).
    #[must_use]
    pub fn with_limits(rate_limit: u32, block_timeout: Duration) -> Self {
        Self {
            message_rate_limit: rate_limit,
            block_timeout,
            ..Self::new()
        }
    }

    /// `incoming(addr, now)` — `false` when the source is banned.
    pub fn incoming(&mut self, addr: IpAddr, now: Instant) -> bool {
        // find exact match, or the least-loaded slot for a new entry
        let mut match_idx = None;
        let mut min_idx = 0usize;
        for (i, e) in self.ban_nodes.iter().enumerate() {
            if e.src == Some(addr) {
                match_idx = Some(i);
                break;
            }
            let m = &self.ban_nodes[min_idx];
            if e.count < m.count || (e.count == m.count && e.limit < m.limit) {
                min_idx = i;
            }
        }

        if let Some(i) = match_idx {
            let e = &mut self.ban_nodes[i];
            e.count += 1;
            if e.count >= self.message_rate_limit * 10 {
                if let Some(limit) = e.limit {
                    if now < limit {
                        if e.count == self.message_rate_limit * 10 {
                            // too many in <10 s — ban
                            e.limit = Some(now + self.block_timeout);
                        }
                        return false;
                    }
                }
                // window expired — reset
                e.count = 0;
                e.limit = Some(now + Duration::from_secs(10));
            }
            true
        } else {
            let e = &mut self.ban_nodes[min_idx];
            e.count = 1;
            e.limit = Some(now + Duration::from_secs(10));
            e.src = Some(addr);
            true
        }
    }
}
