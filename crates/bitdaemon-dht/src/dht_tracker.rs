// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/dht_tracker.cpp`/`dht_tracker.hpp`.
//!
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

/// Upstream `counters` channels the DHT layer increments, mirroring
/// `performance_counters.hpp`. Emitted through
/// [`DhtTracker::set_counter_sink`]; a `None` sink skips the call like
/// upstream's `inc_stats_counter` on the session counters — a pure
/// observability hook (same pattern as `UtpCounterSink`).
///
/// `dht_find_node_out` is declared upstream but *never*
/// incremented (`find_data::invoke` skips it) — verbatim dead.
/// `dht_messages_out`/`dht_bytes_out`/`dht_messages_out_dropped`
/// and the `sent_ip_overhead_bytes` leg are stamped at the actual
/// send edge (`send_packet` upstream): they live on the session
/// side, where the socket send's success/failure is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DhtStat {
    /// `dht_messages_in` — a datagram that passed the `d…e` sniff.
    MessagesIn,
    /// `dht_messages_in_dropped` — dark-internet, ip-filter,
    /// dos-blocker, bdecode/non-dict drops and quota-refused
    /// queries.
    MessagesInDropped,
    /// `dht_bytes_in` — payload is the datagram size.
    BytesIn,
    /// `recv_ip_overhead_bytes` for a DHT datagram — payload is
    /// 28 (v4) or 48 (v6).
    IpOverheadIn,
    /// `dht_ping_in`.
    PingIn,
    /// `dht_find_node_in`.
    FindNodeIn,
    /// `dht_get_peers_in`.
    GetPeersIn,
    /// `dht_announce_peer_in`.
    AnnouncePeerIn,
    /// `dht_get_in`.
    GetIn,
    /// `dht_put_in`.
    PutIn,
    /// `dht_sample_infohashes_in`.
    SampleInfohashesIn,
    /// `dht_ping_out`.
    PingOut,
    /// `dht_get_peers_out`.
    GetPeersOut,
    /// `dht_announce_peer_out`.
    AnnouncePeerOut,
    /// `dht_get_out`.
    GetOut,
    /// `dht_put_out`.
    PutOut,
    /// `dht_sample_infohashes_out`.
    SampleInfohashesOut,
    /// `dht_invalid_announce`.
    InvalidAnnounce,
    /// `dht_invalid_get_peers`.
    InvalidGetPeers,
    /// `dht_invalid_find_node`.
    InvalidFindNode,
    /// `dht_invalid_put`.
    InvalidPut,
    /// `dht_invalid_get`.
    InvalidGet,
    /// `dht_invalid_sample_infohashes`.
    InvalidSampleInfohashes,
}

/// `m_counters.inc_stats_counter(stat, delta)` sink.
pub type DhtCounterSink = std::sync::Arc<dyn Fn(DhtStat, i64) + Send + Sync>;

/// Per-family node map (`m_nodes` keyed by listen socket).
pub struct DhtTracker {
    /// `m_counters` — session counter bank sink.
    counter_sink: Option<DhtCounterSink>,
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
            counter_sink: None,
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
        let mut node = DhtNode::new(stored, bind_ip, bind_ip.is_ipv6(), settings);
        node.set_counter_sink(self.counter_sink.clone());
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

    /// install the session counter bank — propagates to both live
    /// nodes (and any node `bind` creates later).
    pub fn set_counter_sink(&mut self, sink: Option<DhtCounterSink>) {
        self.counter_sink = sink;
        for n in [&mut self.v4, &mut self.v6].into_iter().flatten() {
            n.set_counter_sink(self.counter_sink.clone());
        }
    }

    /// `dht_tracker::update_stats_counters` — the snapshot gauges
    /// (`dht_torrents`/`dht_peers`/`dht_immutable`/`dht_mutable`
    /// from storage, `dht_nodes`/`dht_node_cache`/
    /// `dht_allocated_observers` summed over the nodes).
    #[must_use]
    pub fn stats_counters(&self) -> DhtGaugeSnapshot {
        let mut snap = DhtGaugeSnapshot::default();
        for n in [&self.v4, &self.v6].into_iter().flatten() {
            let (nodes, repl, _confirmed) = n.table.size();
            snap.nodes += nodes as i64;
            snap.node_cache += repl as i64;
            snap.allocated_observers += n.rpc.outstanding() as i64;
            let sc = n.storage.counters();
            snap.torrents += i64::from(sc.torrents);
            snap.peers += i64::from(sc.peers);
            snap.immutable_data += i64::from(sc.immutable_data);
            snap.mutable_data += i64::from(sc.mutable_data);
        }
        snap
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
    /// `filtered` carries the session's `apply_filter_to_dht` +
    /// `ip_filter.access` verdict (upstream reads the filter inside
    /// `incoming_packet`, after the dark-internet drop).
    pub fn incoming(
        &mut self,
        from: SocketAddr,
        buf: &[u8],
        filtered: bool,
    ) -> Option<(Vec<u8>, SocketAddr)> {
        // `dht_tracker::incoming_packet` — anything that doesn't
        // look like a bencoded dict isn't DHT at all: it is dropped
        // before counters, ip_filter and the dos-blocker, so other
        // UDP protocols sharing the port (LSD's `BT-SEARCH`,
        // tracker replies) never consume rate-limit budget.
        if buf.len() <= 20 || buf.first() != Some(&b'd') || buf.last() != Some(&b'e') {
            return None;
        }
        // upstream counts bytes/messages + IP overhead right after
        // the sniff, before every drop leg below.
        if let Some(s) = &self.counter_sink {
            s(DhtStat::BytesIn, buf.len() as i64);
            s(DhtStat::IpOverheadIn, if from.is_ipv6() { 48 } else { 28 });
            s(DhtStat::MessagesIn, 1);
        }
        // `dht_ignore_dark_internet` — drop packets from the class-A
        // ranges that can't be public internet (v4 only).
        if self.settings.ignore_dark_internet
            && let IpAddr::V4(a) = from.ip()
        {
            const CLASS_A: [u8; 17] = [
                3, 6, 7, 9, 11, 19, 21, 22, 25, 26, 28, 29, 30, 33, 34, 48, 56,
            ];
            if CLASS_A.contains(&a.octets()[0]) {
                if let Some(s) = &self.counter_sink {
                    s(DhtStat::MessagesInDropped, 1);
                }
                return None;
            }
        }
        // `apply_filter_to_dht` + `ip_filter::blocked` — the verdict
        // is computed by the caller (the filter lives in session).
        if filtered {
            if let Some(s) = &self.counter_sink {
                s(DhtStat::MessagesInDropped, 1);
            }
            return None;
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

/// `dht_tracker::update_stats_counters` output — the gauge values
/// the caller `set_value`s into the session bank (upstream zeroes
/// `dht_nodes`/`dht_node_cache`/`dht_allocated_observers` then
/// accumulates per node — equivalent to a plain sum here).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DhtGaugeSnapshot {
    /// `dht_nodes` — live routing-table entries, both families.
    pub nodes: i64,
    /// `dht_node_cache` — replacement cache entries.
    pub node_cache: i64,
    /// `dht_allocated_observers` — in-flight RPC transactions.
    pub allocated_observers: i64,
    /// `dht_torrents` — storage info-hash count.
    pub torrents: i64,
    /// `dht_peers` — stored announce endpoints.
    pub peers: i64,
    /// `dht_immutable_data` — BEP-44 immutable items held.
    pub immutable_data: i64,
    /// `dht_mutable_data` — BEP-44 mutable items held.
    pub mutable_data: i64,
}
