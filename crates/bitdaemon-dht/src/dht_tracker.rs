// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `dht_tracker` — owns the per-address-family [`DhtNode`]s and
//! drives their periodic work. Upstream binds real sockets and
//! posts through `io_context`; here transports are injected, so
//! the tracker only multiplexes `incoming`/`poll`/`tick`, shares
//! the upload quota (`has_quota`/`send_packet` upstream), and
//! resolves `want`-list cross-family lookups.

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use crate::dht_node::DhtNode;
use crate::dht_settings::DhtSettings;
use crate::node_id::NodeId;

/// secret rotation cadence upstream (`refresh_key_timer`, 5 min).
pub const SECRET_ROTATION: Duration = Duration::from_secs(300);

/// Per-family node map (`m_nodes` keyed by listen socket).
pub struct DhtTracker {
    /// v4 node (`"n4"`/`"nodes"` protocol).
    pub v4: Option<DhtNode>,
    /// v6 node (`"n6"`/`"nodes6"`).
    pub v6: Option<DhtNode>,
    last_key_rotation: Option<Instant>,
    /// `m_send_quota` — bytes we're allowed to send right now
    /// (refilled from `dht_upload_rate_limit`, 3 s burst).
    send_quota: i64,
    /// `m_last_tick` — last `has_quota` accrual timestamp.
    last_quota_tick: Instant,
    /// session-level settings (`m_settings` upstream — shared by
    /// both nodes; the last `bind` wins like upstream's
    /// `session_settings` snapshot).
    settings: DhtSettings,
}

impl Default for DhtTracker {
    fn default() -> Self {
        Self {
            v4: None,
            v6: None,
            last_key_rotation: None,
            send_quota: 0,
            last_quota_tick: Instant::now(),
            settings: DhtSettings::default(),
        }
    }
}

impl DhtTracker {
    /// `new_socket` — create the node for `bind`'s family.
    /// `id` falls back to the `nids` recorded in `st` for this
    /// local address, like `new_socket` does upstream.
    pub fn bind(
        &mut self,
        bind_ip: IpAddr,
        id: Option<NodeId>,
        settings: DhtSettings,
        st: &crate::dht_state::DhtState,
    ) {
        let stored = id.or_else(|| st.nids.iter().find(|(a, _)| *a == bind_ip).map(|(_, i)| *i));
        self.settings = settings.clone();
        self.send_quota = i64::from(settings.upload_rate_limit);
        let node = DhtNode::new(stored, bind_ip, bind_ip.is_ipv6(), settings);
        if bind_ip.is_ipv6() {
            self.v6 = Some(node);
        } else {
            self.v4 = Some(node);
        }
    }

    /// `delete_socket`.
    pub fn unbind(&mut self, v6: bool) {
        if v6 {
            self.v6 = None;
        } else {
            self.v4 = None;
        }
    }

    /// `get_node(v6)` — the foreign-protocol lookup used for `want`.
    #[must_use]
    pub fn get_node(&self, v6: bool) -> Option<&DhtNode> {
        if v6 {
            self.v6.as_ref()
        } else {
            self.v4.as_ref()
        }
    }

    /// `dht_tracker::has_quota` — token-bucket refill; `false` when
    /// the upload limit is exhausted or disabled (≤0 upstream).
    pub fn has_quota(&mut self) -> bool {
        let now = Instant::now();
        let delta = now.duration_since(self.last_quota_tick);
        self.last_quota_tick = now;

        let limit = i64::from(self.settings.upload_rate_limit);
        if limit <= 0 {
            self.send_quota = 0;
            return false;
        }

        // allow 3 s worth of burst
        let max_accrue = (3 * limit).min(i64::from(i32::MAX));

        // accrual saturates at the burst cap; the upstream
        // `total_milliseconds(now - m_last_tick) * limit > INT_MAX`
        // overflow case just refills fully.
        let add = delta
            .as_micros()
            .min(u128::from(i64::MAX as u64))
            .saturating_mul(u128::from(limit.max(0) as u64))
            / 1_000_000;
        let add = i64::try_from(add).unwrap_or(i64::MAX);
        if add >= i64::from(i32::MAX) || max_accrue - self.send_quota < add {
            self.send_quota = max_accrue;
            return true;
        }
        self.send_quota += add;
        self.send_quota > 0
    }

    /// `dht_tracker::incoming_error` — a transport-level failure
    /// (ICMP unreachable / connection reset) toward `ep`; the
    /// matching family's pending transaction is failed.
    pub fn incoming_error(&mut self, ep: SocketAddr) {
        if let Some(n) = self.get_node_mut(ep.is_ipv6()) {
            n.unreachable(ep);
        }
    }

    /// feed a datagram; returns any immediate reply to send.
    pub fn incoming(&mut self, from: SocketAddr, buf: &[u8]) -> Option<(Vec<u8>, SocketAddr)> {
        // `dht_ignore_dark_internet` — drop packets from the class-A
        // ranges that can't be public internet (v4 only).
        if self.settings.ignore_dark_internet
            && let IpAddr::V4(a) = from.ip()
        {
            const CLASS_A: [u8; 17] = [
                3, 6, 7, 9, 11, 19, 21, 22, 25, 26, 28, 29, 30, 33, 34, 48, 56,
            ];
            if CLASS_A.contains(&a.octets()[0]) {
                return None;
            }
        }

        let quota_ok = self.has_quota();
        // dispatch to the node's family; the sibling node (if any)
        // is the `m_get_foreign_node` target for `want` replies.
        let v6 = from.is_ipv6();
        let (node, foreign) = if v6 {
            (self.v6.as_mut(), self.v4.as_ref())
        } else {
            (self.v4.as_mut(), self.v6.as_ref())
        };
        node.and_then(|n| n.incoming(buf, from, foreign, quota_ok))
    }

    /// drain outgoing datagrams from both nodes; each drains the
    /// shared send quota (`send_packet` upstream).
    pub fn poll(&mut self) -> Option<(Vec<u8>, SocketAddr)> {
        if let Some(n) = self.v6.as_mut()
            && let Some(d) = n.poll()
        {
            self.send_quota -= d.0.len() as i64;
            return Some(d);
        }
        match self.v4.as_mut().and_then(DhtNode::poll) {
            Some(d) => {
                self.send_quota -= d.0.len() as i64;
                Some(d)
            }
            None => None,
        }
    }

    /// `update_node_id` — the session detected an external-address
    /// change for `v6`'s listen socket.
    pub fn update_node_id(&mut self, v6: bool, external: IpAddr) {
        if let Some(n) = self.get_node_mut(v6) {
            n.update_node_id(external);
        }
        // upstream `update_storage_node_ids` — storage distances
        // score against *all* live node ids.
        let ids: Vec<NodeId> = [&self.v4, &self.v6]
            .into_iter()
            .flatten()
            .map(|n| n.id)
            .collect();
        for n in [&mut self.v4, &mut self.v6].into_iter().flatten() {
            n.storage.node_ids = ids.clone();
        }
    }

    /// `tick` — rpc timeouts, storage expiry, secret rotation.
    /// Returns the caller's next-wake delay.
    pub fn tick(&mut self) -> Duration {
        let mut wait = Duration::from_secs(60);
        for n in [&mut self.v4, &mut self.v6].into_iter().flatten() {
            wait = wait.min(n.tick());
        }
        let now = Instant::now();
        if self
            .last_key_rotation
            .is_none_or(|t| now.duration_since(t) >= SECRET_ROTATION)
        {
            self.last_key_rotation = Some(now);
            for n in [&mut self.v4, &mut self.v6].into_iter().flatten() {
                n.rotate_secrets();
            }
        }
        wait
    }

    /// mutable access for `load_state` bootstrap seeding.
    pub fn get_node_mut(&mut self, v6: bool) -> Option<&mut DhtNode> {
        if v6 {
            self.v6.as_mut()
        } else {
            self.v4.as_mut()
        }
    }
}
