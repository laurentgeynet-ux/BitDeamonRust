// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `aux_/peer_list` — the per-torrent swarm list: add/update
//! peers, connect-candidate selection (`find_connect_candidates`
//! with round-robin + `compare_peer` ranking), weeding when the
//! list grows past `max_peerlist_size`, ban. Rust owns peers in a
//! `Vec` and hands out indices in place of upstream pointers; the
//! allocator/`locked_peer` machinery is replaced by value storage
//! and an explicit `locked` index set around callbacks.

use std::net::{IpAddr, SocketAddr};

use bitdaemon_common::{PeerSourceFlags, PexFlags};

use crate::ip_helpers::is_local;
use crate::ip_voter::ExternalIp;
use crate::torrent_peer::{PeerAddr, TorrentPeer};

/// `peer_source` — the *primary* source a peer was added through:
/// upstream records the first source bit. Used by
/// `is_erase_candidate`/`should_erase_immediately` (resume-data
/// peers are weeded first).
#[must_use]
pub fn primary_source(src: PeerSourceFlags) -> PeerSourceFlags {
    for f in [
        PeerSourceFlags::TRACKER,
        PeerSourceFlags::DHT,
        PeerSourceFlags::PEX,
        PeerSourceFlags::LSD,
        PeerSourceFlags::RESUME_DATA,
        PeerSourceFlags::INCOMING,
    ] {
        if src.contains(f) {
            return f;
        }
    }
    src
}

/// `aux::source_rank` (`request_blocks.cpp`) — tracker 1<<5,
/// lsd 1<<4, dht 1<<3, pex 1<<2.
#[must_use]
pub fn source_rank(src: PeerSourceFlags) -> i32 {
    let mut ret = 0;
    if src.contains(PeerSourceFlags::TRACKER) {
        ret |= 1 << 5;
    }
    if src.contains(PeerSourceFlags::LSD) {
        ret |= 1 << 4;
    }
    if src.contains(PeerSourceFlags::DHT) {
        ret |= 1 << 3;
    }
    if src.contains(PeerSourceFlags::PEX) {
        ret |= 1 << 2;
    }
    ret
}

/// `torrent_state` — the settings/context `peer_list` needs from
/// the torrent (values from `settings_pack` + session state).
#[derive(Debug, Clone)]
pub struct PeerListState {
    /// `is_finished` — torrent is a seed.
    pub is_finished: bool,
    /// `ip` — the `external_ip` aggregate (local/global × v4/v6)
    /// used for BEP-40 `peer_priority` ranking.
    pub external_ip: ExternalIp,
    /// `port` — our external port.
    pub external_port: u16,
    /// `min_reconnect_time` (s) — multiplied by `failcount + 1`.
    pub min_reconnect_time: i32,
    /// `max_peerlist_size` — 0 = unlimited.
    pub max_peerlist_size: i32,
    /// `max_failcount` before a peer stops being a candidate.
    pub max_failcount: i32,
    /// `allow_multiple_connections_per_ip`.
    pub allow_multiple_connections_per_ip: bool,
}

impl Default for PeerListState {
    fn default() -> Self {
        Self {
            is_finished: false,
            external_ip: ExternalIp::default(),
            external_port: 0,
            min_reconnect_time: 60,
            max_peerlist_size: 0,
            max_failcount: 3,
            allow_multiple_connections_per_ip: false,
        }
    }
}

/// `compare_peer` — swarm-connectivity ranking (upstream
/// `peer_list.cpp` anonymous namespace). `true` = `lhs` preferred.
/// Order: failcount, local-first, oldest `last_connected`, finished→
/// deprioritize `maybe_upload_only`, primary-source rank, then the
/// lazy BEP-40 `rank()`.
fn compare_peer(lhs: &TorrentPeer, rhs: &TorrentPeer, st: &PeerListState) -> bool {
    if lhs.failcount != rhs.failcount {
        return lhs.failcount < rhs.failcount;
    }
    let lhs_local = lhs.addr.address().is_some_and(|a| is_local(&a));
    let rhs_local = rhs.addr.address().is_some_and(|a| is_local(&a));
    if lhs_local != rhs_local {
        return lhs_local;
    }
    if lhs.last_connected != rhs.last_connected {
        return lhs.last_connected < rhs.last_connected;
    }
    if st.is_finished && lhs.maybe_upload_only != rhs.maybe_upload_only {
        return rhs.maybe_upload_only;
    }
    let lr = source_rank(primary_source(lhs.source));
    let rr = source_rank(primary_source(rhs.source));
    if lr != rr {
        return lr > rr;
    }
    lhs.rank(&st.external_ip, st.external_port) > rhs.rank(&st.external_ip, st.external_port)
}

/// `peer_list` — `m_peers` is kept sorted by IP address like
/// upstream (`peer_address_compare`), so `find_peers`/`lower_bound`
/// stay `O(log n)`. Connections are tracked by index (`Some(usize)`
/// marks "has a live connection" — the real `peer_connection` lives
/// in the torrent's slab).
pub struct PeerList {
    peers: Vec<TorrentPeer>,
    /// `m_num_connect_candidates`.
    num_connect_candidates: i32,
    /// `m_num_seeds`.
    num_seeds: i32,
    /// `m_finished` — cached copy of `state.is_finished`; a change
    /// triggers `recalculate_connect_candidates`.
    finished: bool,
    /// `m_max_failcount` (settings `max_failcount`).
    max_failcount: i32,
    /// `m_round_robin` — find_connect_candidates cursor.
    round_robin: usize,
    /// `m_locked_peer` — peer index locked by a callback (never
    /// erased).
    locked: Option<usize>,
    /// `m_candidate_cache` — leftover ranked candidates from the
    /// last `find_connect_candidates` fill, popped one per dial by
    /// `connect_one_peer`. Upstream caches `torrent_peer*`
    /// (allocator-stable across `m_peers` vector churn); our
    /// indices shift on erase, so the cache keys by `(addr, port)`
    /// and re-resolves at pop.
    candidate_cache: std::collections::VecDeque<(crate::torrent_peer::PeerAddr, u16)>,
    /// `m_allocator` — upstream's `torrent_peer_allocator`; only the
    /// accounting counters survive the by-value `Vec` storage.
    pub alloc: crate::peer_alloc::TorrentPeerAllocator,
}

impl Drop for PeerList {
    fn drop(&mut self) {
        // `~torrent` releases every remaining `torrent_peer` through
        // the allocator — keep the live counters honest.
        self.alloc.account_free_many(self.peers.len());
    }
}

impl Default for PeerList {
    /// `torrent_state` defaults: `max_failcount = 3` (upstream).
    fn default() -> Self {
        Self::new(&PeerListState::default())
    }
}

impl PeerList {
    /// `peer_list()` — `max_failcount` comes from `state`.
    #[must_use]
    pub fn new(state: &PeerListState) -> Self {
        Self {
            peers: Vec::new(),
            num_connect_candidates: 0,
            num_seeds: 0,
            finished: state.is_finished,
            max_failcount: state.max_failcount,
            round_robin: 0,
            locked: None,
            candidate_cache: std::collections::VecDeque::new(),
            alloc: crate::peer_alloc::TorrentPeerAllocator::default(),
        }
    }

    /// Read access (sorted by address).
    #[must_use]
    pub fn peers(&self) -> &[TorrentPeer] {
        &self.peers
    }

    /// `m_locked_peer` — index locked by an in-flight callback
    /// (never erased by filter sweeps).
    #[must_use]
    pub fn locked_peer(&self) -> Option<usize> {
        self.locked
    }

    /// `torrent::step_session_time` — the session clock is a
    /// u16-seconds epoch upstream; before it wraps, the base steps
    /// forward and every relative stamp clamps back by the same
    /// amount (`clamped_subtract_u16`).
    pub fn step_session_time(&mut self, seconds: u16) {
        for p in &mut self.peers {
            p.last_optimistically_unchoked = p.last_optimistically_unchoked.saturating_sub(seconds);
            p.last_connected = p.last_connected.saturating_sub(seconds);
        }
        // stamps shifted → the cached candidate set is still valid
        // (ordering unchanged), only `last_connected` ordering may
        // have collapsed to 0s — upstream keeps the cache too.
    }

    /// Mutable access — flags patched post-insert (`pe_support`
    /// from a PEX `e` flag or a completed MSE handshake).
    pub fn peers_mut(&mut self) -> &mut [TorrentPeer] {
        &mut self.peers
    }

    /// `num_peers`.
    #[must_use]
    pub fn num_peers(&self) -> usize {
        self.peers.len()
    }

    /// `num_seeds`.
    #[must_use]
    pub fn num_seeds(&self) -> i32 {
        self.num_seeds
    }

    /// `num_connect_candidates`.
    #[must_use]
    pub fn num_connect_candidates(&self) -> i32 {
        self.num_connect_candidates
    }

    /// Index of the peer owning `addr` (any port).
    #[must_use]
    pub fn find_peer(&self, addr: IpAddr) -> Option<usize> {
        self.peers
            .iter()
            .position(|p| p.addr.address() == Some(addr))
    }

    /// Sorted-insert position for `key` — `peer_address_compare`
    /// orders by `address()` (port-free `PeerAddr`); i2p sorts after
    /// all IPs, among themselves by `dest`.
    fn insert_pos(&self, key: &PeerAddr) -> usize {
        self.peers.partition_point(|p| p.addr < *key)
    }

    /// `update_peer` — an existing entry relearned via `src`/`flags`:
    /// always marks `connectable`, refreshes `port`, ORs the source,
    /// drops one `failcount` for tracker-sourced updates, applies the
    /// pex capability flags.
    fn update_peer(
        &mut self,
        index: usize,
        src: PeerSourceFlags,
        flags: PexFlags,
        remote: SocketAddr,
    ) {
        let was = self.is_connect_candidate(&self.peers[index]);
        let p = &mut self.peers[index];
        p.connectable = true;
        p.port = remote.port();
        p.source |= src;
        // a tracker re-announce proves the peer is reachable — give
        // back one failure credit (upstream `update_peer`).
        if p.failcount > 0 && src == PeerSourceFlags::TRACKER {
            p.failcount -= 1;
        }
        // if connected we already know whether it's a seed — only an
        // unconnected peer takes the hint.
        if flags.contains(PexFlags::SEED) && p.connection.is_none() {
            p.maybe_upload_only = true;
        }
        if flags.contains(PexFlags::UTP) {
            p.supports_utp = true;
        }
        if flags.contains(PexFlags::HOLEPUNCH) {
            p.supports_holepunch = true;
        }
        if flags.contains(PexFlags::LT_V2) {
            p.protocol_v2 = true;
        }
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// `insert_peer` — the shared tail of `add_peer`/`add_i2p_peer`:
    /// `max_peerlist_size` cap (resume-data entries never force the
    /// cap, one `erase_peers` sweep otherwise), sorted insert,
    /// `round_robin` fixup, pex flags, candidate bookkeeping.
    /// Returns the final index or `None` when the list stays full.
    fn insert_peer(
        &mut self,
        mut p: TorrentPeer,
        key: PeerAddr,
        flags: PexFlags,
        state: &PeerListState,
        force: bool,
    ) -> Option<usize> {
        let mut pos = self.insert_pos(&key);
        if state.max_peerlist_size > 0 && self.peers.len() as i32 >= state.max_peerlist_size {
            if primary_source(p.source) == PeerSourceFlags::RESUME_DATA {
                return None;
            }
            self.erase_peers(state, force);
            if self.peers.len() as i32 >= state.max_peerlist_size {
                return None;
            }
            // the sweep may have erased entries before `pos`
            pos = self.insert_pos(&key);
        }

        if flags.contains(PexFlags::ENCRYPTION) {
            p.pe_support = true;
        }
        if flags.contains(PexFlags::SEED) {
            p.maybe_upload_only = true;
        }
        if flags.contains(PexFlags::UTP) {
            p.supports_utp = true;
        }
        if flags.contains(PexFlags::HOLEPUNCH) {
            p.supports_holepunch = true;
        }
        if flags.contains(PexFlags::LT_V2) {
            p.protocol_v2 = true;
        }

        let ty = match p.addr {
            PeerAddr::V4(_) => crate::peer_alloc::PeerEntryType::Ipv4,
            PeerAddr::V6(_) => crate::peer_alloc::PeerEntryType::Ipv6,
            PeerAddr::I2p(_) => crate::peer_alloc::PeerEntryType::I2p,
        };
        self.alloc.account_alloc(ty);
        if self.round_robin >= pos {
            self.round_robin += 1;
        }
        let is = self.is_connect_candidate(&p);
        self.peers.insert(pos, p);
        self.update_candidates(false, is);
        Some(pos)
    }

    /// `add_peer` — called per tracker/pex/lsd/dht peer. Upstream
    /// drops unspecified addresses, ports 0/1, and v6 link-locals.
    /// `None` when the cap keeps the entry out (or it was filtered).
    /// Returns `(index, is_new)` on success.
    pub fn add_peer(
        &mut self,
        remote: SocketAddr,
        src: PeerSourceFlags,
        flags: PexFlags,
        state: &PeerListState,
    ) -> Option<(usize, bool)> {
        // obviously invalid entries (upstream `add_peer`)
        if remote.ip().is_unspecified() || remote.port() <= 1 {
            return None;
        }
        // link-local v6 can't be used without a scope id
        if remote.is_ipv6() && crate::ip_helpers::is_link_local(&remote.ip()) {
            return None;
        }

        let key = match remote {
            SocketAddr::V4(a) => PeerAddr::V4(*a.ip()),
            SocketAddr::V6(a) => PeerAddr::V6(*a.ip()),
        };

        let found = if state.allow_multiple_connections_per_ip {
            // same-address entries coexist — match the endpoint
            self.peers
                .iter()
                .position(|p| p.addr.address() == Some(remote.ip()) && p.port == remote.port())
        } else {
            self.find_peer(remote.ip())
        };

        if let Some(i) = found {
            self.update_peer(i, src, flags, remote);
            return Some((i, false));
        }

        let p = match remote {
            SocketAddr::V4(_) => TorrentPeer::new_v4(remote, true, src),
            SocketAddr::V6(_) => TorrentPeer::new_v6(remote, true, src),
        };
        self.insert_peer(p, key, flags, state, false)
            .map(|i| (i, true))
    }

    /// `add_i2p_peer` — an I2P destination learned via `i2p_pex`
    /// (dedup by destination). Upstream creates these `connectable`
    /// — a destination is dialable by definition.
    pub fn add_peer_i2p(
        &mut self,
        dest: Vec<u8>,
        src: PeerSourceFlags,
        flags: PexFlags,
        state: &PeerListState,
    ) -> Option<(usize, bool)> {
        if let Some(i) = self
            .peers
            .iter()
            .position(|p| matches!(&p.addr, PeerAddr::I2p(d) if *d == dest))
        {
            // upstream `update_peer(i2p)` — remote endpoint is empty
            self.update_peer(i, src, flags, SocketAddr::from(([0, 0, 0, 0], 0)));
            return Some((i, false));
        }
        let p = TorrentPeer::new_i2p(dest.clone(), true, src);
        self.insert_peer(p, PeerAddr::I2p(dest), flags, state, false)
            .map(|i| (i, true))
    }

    /// `new_connection` (incoming) — the insert half of upstream's
    /// accept path: `connectable = false`, `source = incoming`,
    /// sorted insert, `force_erase` weeding when the list is full.
    /// `None` = list full even after force-erase (`too_many_connections`
    /// upstream — the caller disconnects).
    pub fn add_incoming(&mut self, remote: SocketAddr, state: &PeerListState) -> Option<usize> {
        if state.allow_multiple_connections_per_ip {
            if let Some(i) = self
                .peers
                .iter()
                .position(|p| p.addr.address() == Some(remote.ip()) && p.port == remote.port())
            {
                return Some(i);
            }
        } else if let Some(i) = self.find_peer(remote.ip()) {
            return Some(i);
        }
        let key = match remote {
            SocketAddr::V4(a) => PeerAddr::V4(*a.ip()),
            SocketAddr::V6(a) => PeerAddr::V6(*a.ip()),
        };
        let p = TorrentPeer::new(key.clone(), remote.port(), false, PeerSourceFlags::INCOMING);
        self.insert_peer(p, key, PexFlags::NONE, state, true)
    }

    /// `update_peer_port` — a peer announces a new listen port
    /// (NAT-PMP/DHT port message). Returns `false` when the update
    /// would collide with an existing connected peer (the caller
    /// should drop this peer).
    pub fn update_peer_port(
        &mut self,
        port: u16,
        index: usize,
        src: PeerSourceFlags,
        state: &PeerListState,
    ) -> bool {
        if index >= self.peers.len() {
            return false;
        }
        // upstream: i2p peers have no port to update
        if self.peers[index].addr.is_i2p() {
            return true;
        }
        if self.peers[index].port == port {
            return true;
        }
        if state.allow_multiple_connections_per_ip {
            if let Some(addr) = self.peers[index].addr.address() {
                // another entry already owns this exact endpoint
                if let Some(other) = self
                    .peers
                    .iter()
                    .position(|p| p.addr.address() == Some(addr) && p.port == port)
                    && other != index
                {
                    if self.peers[other].connection.is_some() {
                        // upstream: disconnect the existing one and
                        // erase this peer.
                        let was = self.is_connect_candidate(&self.peers[other]);
                        self.peers[other].connectable = true;
                        self.peers[other].source |= src;
                        let is = self.is_connect_candidate(&self.peers[other]);
                        self.update_candidates(was, is);
                        self.erase_peer(index);
                        return false;
                    }
                    self.erase_peer(other);
                    // erasing an earlier slot shifts `index` down
                    let next = if other < index { index - 1 } else { index };
                    return self.update_peer_port(port, next, src, state);
                }
            }
        }
        let was = self.is_connect_candidate(&self.peers[index]);
        let p = &mut self.peers[index];
        p.port = port;
        p.source |= src;
        p.connectable = true;
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
        true
    }

    /// `set_seed` — flips the flag, drops connect-candidate credit
    /// when the peer was one, and updates `num_seeds` (skipped for
    /// web seeds, which don't live in `m_peers` upstream).
    pub fn set_seed(&mut self, index: usize, s: bool) {
        let was = self.is_connect_candidate(&self.peers[index]);
        let p = &mut self.peers[index];
        if p.seed == s {
            return;
        }
        p.seed = s;
        if was && !self.is_connect_candidate(&self.peers[index]) {
            self.num_connect_candidates -= 1;
        }
        if self.peers[index].web_seed {
            return;
        }
        self.num_seeds += if s { 1 } else { -1 };
    }

    /// `set_upload_only` — BEP-21 upload-only flag; updates the
    /// candidate count but never `num_seeds` (a partial seed is not
    /// a full seed upstream).
    pub fn set_upload_only(&mut self, index: usize, s: bool) {
        if self.peers[index].upload_only == s {
            return;
        }
        let was = self.is_connect_candidate(&self.peers[index]);
        self.peers[index].upload_only = s;
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// `set_failcount` — success resets to 0 (upstream).
    pub fn set_failcount(&mut self, index: usize, f: u8) {
        let was = self.is_connect_candidate(&self.peers[index]);
        self.peers[index].failcount = f;
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// `inc_failcount` — saturates at 31 (upstream `failcount:5`).
    pub fn inc_failcount(&mut self, index: usize) {
        let f = self.peers[index].failcount;
        if f == 31 {
            return;
        }
        self.set_failcount(index, f + 1);
    }

    /// `ban_peer` — mark banned; a connected peer gets disconnected
    /// by the caller.
    pub fn ban_peer(&mut self, index: usize) {
        let was = self.is_connect_candidate(&self.peers[index]);
        self.peers[index].banned = true;
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// `new_connection`'s attach tail + `set_connection` — mark the
    /// entry connected: clears `maybe_upload_only` (a live conn tells
    /// us the real seed state), stamps `last_connected` unless this
    /// is a fast reconnect (upstream leaves the initiate-time stamp).
    /// Upstream stores the `peer_connection*`; we track the flag and
    /// re-resolve by endpoint on disconnect.
    pub fn set_connected(&mut self, index: usize, session_time: i32, fast_reconnect: bool) {
        let was = self.is_connect_candidate(&self.peers[index]);
        let p = &mut self.peers[index];
        p.connection = Some(std::sync::Arc::new(crate::torrent_peer::NullStats));
        p.maybe_upload_only = false;
        if !fast_reconnect {
            p.last_connected = session_time.max(0) as u16;
        }
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// `connection_closed` — clears the connection, `optimistically_
    /// unchoked`, stamps `last_connected` (unless fast reconnect),
    /// bumps `failcount` when the conn `failed()`, and — under
    /// `allow_multiple_connections_per_ip` — evicts never-connectable
    /// incoming-only entries.
    pub fn connection_closed(
        &mut self,
        index: usize,
        session_time: i32,
        fast_reconnect: bool,
        failed: bool,
        state: &PeerListState,
    ) {
        if index >= self.peers.len() {
            return;
        }
        {
            let p = &mut self.peers[index];
            p.connection = None;
            p.optimistically_unchoked = false;
            if !fast_reconnect {
                p.last_connected = session_time.max(0) as u16;
            }
            if failed && p.failcount < 31 {
                p.failcount += 1;
            }
        }
        if self.is_connect_candidate(&self.peers[index]) {
            self.num_connect_candidates += 1;
        }
        // anonymous incoming entries aren't retained under
        // multi-connections-per-ip (upstream `connection_closed`)
        if state.allow_multiple_connections_per_ip
            && !self.peers[index].connectable
            && self.locked != Some(index)
        {
            self.erase_peer(index);
        }
    }

    /// `is_connect_candidate` — verbatim upstream conditions.
    #[must_use]
    pub fn is_connect_candidate(&self, p: &TorrentPeer) -> bool {
        !(p.connection.is_some()
            || p.banned
            || p.web_seed
            || !p.connectable
            || ((p.seed || p.upload_only) && self.finished)
            || i32::from(p.failcount) >= self.max_failcount)
    }

    /// `is_erase_candidate` — disconnected, non-candidate, and
    /// either failed before or came from resume data.
    #[must_use]
    fn is_erase_candidate(&self, p: &TorrentPeer) -> bool {
        if p.connection.is_some() || self.is_connect_candidate(p) {
            return false;
        }
        p.failcount > 0 || primary_source(p.source) == PeerSourceFlags::RESUME_DATA
    }

    /// `is_force_erase_candidate` — any disconnected peer.
    fn is_force_erase_candidate(&self, p: &TorrentPeer) -> bool {
        p.connection.is_none()
    }

    /// `should_erase_immediately` — resume-data peers first.
    fn should_erase_immediately(&self, p: &TorrentPeer) -> bool {
        primary_source(p.source) == PeerSourceFlags::RESUME_DATA
    }

    /// `find_connect_candidates` — round-robin scan of at most 300
    /// peers, top-10 sorted by `compare_peer`, weeding past 95% of
    /// `max_peerlist_size`. Returns candidate indices.
    pub fn find_connect_candidates(
        &mut self,
        session_time: i32,
        state: &PeerListState,
    ) -> Vec<(crate::torrent_peer::PeerAddr, u16)> {
        const CANDIDATE_COUNT: usize = 10;
        // keys, not indices — `erase_peer` below shifts `m_peers`;
        // upstream holds allocator-stable `torrent_peer*`.
        let mut peers: Vec<(crate::torrent_peer::PeerAddr, u16)> =
            Vec::with_capacity(CANDIDATE_COUNT);
        let mut erase_candidate: Option<usize> = None;

        if self.finished != state.is_finished {
            self.recalculate_connect_candidates(state);
        }

        if self.round_robin >= self.peers.len() {
            self.round_robin = 0;
        }

        let mut iterations = self.peers.len().min(300);
        while iterations > 0 {
            iterations -= 1;
            if self.round_robin >= self.peers.len() {
                self.round_robin = 0;
            }
            let current = self.round_robin;

            if self.peers.len() as i64 >= state.max_peerlist_size as i64 * 95 / 100
                && state.max_peerlist_size > 0
            {
                let pe = &self.peers[current];
                if self.is_erase_candidate(pe)
                    && self.locked != Some(current)
                    && (erase_candidate
                        .is_none_or(|ec| !compare_peer_for_erase(&self.peers[ec], pe)))
                {
                    if self.should_erase_immediately(pe) {
                        if erase_candidate.is_some_and(|ec| ec > current) {
                            erase_candidate = erase_candidate.map(|ec| ec - 1);
                        }
                        self.erase_peer(current);
                        continue;
                    }
                    erase_candidate = Some(current);
                }
            }

            self.round_robin += 1;
            if current >= self.peers.len() {
                continue;
            }
            let pe = &self.peers[current];
            if !self.is_connect_candidate(pe) {
                continue;
            }
            if i32::from(pe.last_connected) > 0
                && session_time - i32::from(pe.last_connected)
                    < (i32::from(pe.failcount) + 1) * state.min_reconnect_time
            {
                continue;
            }
            let key = (pe.addr.clone(), pe.port);

            if peers.len() == CANDIDATE_COUNT {
                let worst_key = &peers[CANDIDATE_COUNT - 1];
                let worst = self
                    .peers
                    .iter()
                    .position(|p| p.addr == worst_key.0 && p.port == worst_key.1);
                if let Some(wi) = worst
                    && compare_peer(&self.peers[wi], pe, state)
                {
                    continue;
                }
            }
            if peers.len() >= CANDIDATE_COUNT {
                peers.truncate(CANDIDATE_COUNT - 1);
            }
            let pos = peers
                .iter()
                .position(|k| {
                    self.peers
                        .iter()
                        .position(|p| p.addr == k.0 && p.port == k.1)
                        .is_none_or(|i| !compare_peer(&self.peers[i], pe, state))
                })
                .unwrap_or(peers.len());
            peers.insert(pos, key);
        }

        if let Some(ec) = erase_candidate {
            self.erase_peer(ec);
        }
        peers
    }

    /// Resolve a `candidate_cache` key back to a live index.
    /// Upstream keeps raw pointers (stable); our key survives
    /// vector churn — `None` means the entry was erased.
    fn resolve_cached(&self, key: &(crate::torrent_peer::PeerAddr, u16)) -> Option<usize> {
        self.peers
            .iter()
            .position(|p| p.addr == key.0 && p.port == key.1)
    }

    /// `connect_one_peer` — pop one connect candidate: stale cache
    /// entries are re-validated (`is_connect_candidate` only — the
    /// reconnect delay already gated them at fill time), an empty
    /// cache is refilled by `find_connect_candidates`, then the
    /// front peer is handed out. Returns the peer's index.
    pub fn connect_one_peer(&mut self, session_time: i32, state: &PeerListState) -> Option<usize> {
        if self.finished != state.is_finished {
            self.recalculate_connect_candidates(state);
        }
        // drop cache entries whose peer was erased or stopped being
        // a candidate (connected, banned, failcount reached…).
        let drained: std::collections::VecDeque<_> = std::mem::take(&mut self.candidate_cache);
        for k in drained {
            if self
                .resolve_cached(&k)
                .is_some_and(|i| self.is_connect_candidate(&self.peers[i]))
            {
                self.candidate_cache.push_back(k);
            }
        }

        if self.candidate_cache.is_empty() {
            self.candidate_cache = self.find_connect_candidates(session_time, state).into();
            self.candidate_cache.front()?;
        }

        let key = self.candidate_cache.pop_front()?;
        let i = self.resolve_cached(&key)?;
        // upstream `TORRENT_ASSERT`s: in_use, !banned, !connection,
        // connectable, finished → !seed — all inside
        // `is_connect_candidate`.
        debug_assert!(self.is_connect_candidate(&self.peers[i]));
        Some(i)
    }

    /// `apply_ip_filter` — every filtered entry is erased; for
    /// *connected* ones upstream also reports the remote address in
    /// `banned` (the caller disconnects the live conn — ours via the
    /// returned list). Locked peers are skipped.
    pub fn apply_ip_filter(&mut self, filter: impl Fn(IpAddr) -> bool) -> Vec<IpAddr> {
        let mut banned = Vec::new();
        let mut i = 0;
        while i < self.peers.len() {
            let addr = self.peers[i].addr.address();
            let blocked = addr.is_some_and(&filter);
            if !blocked || self.locked == Some(i) {
                i += 1;
                continue;
            }
            if self.peers[i].connection.is_some()
                && let Some(a) = addr
            {
                banned.push(a);
            }
            self.erase_peer(i);
        }
        banned
    }

    /// `erase_peers` — one upstream weeding sweep: random-start scan
    /// of ≤300 entries stopping at the 95% low watermark, erasing
    /// `should_erase_immediately` entries as found, plus the single
    /// worst `erase_candidate` (or `force_erase_candidate` when
    /// `force` is set — the new-connection path).
    pub fn erase_peers(&mut self, state: &PeerListState, force: bool) {
        let max = state.max_peerlist_size;
        if max == 0 || self.peers.is_empty() {
            return;
        }

        let mut erase_candidate: Option<usize> = None;
        let mut force_erase_candidate: Option<usize> = None;

        if self.finished != state.is_finished {
            self.recalculate_connect_candidates(state);
        }

        let mut rr = rand::random_range(0..self.peers.len());
        let mut low_watermark = max as usize * 95 / 100;
        if low_watermark as i32 == max {
            low_watermark -= 1;
        }

        let mut iterations = self.peers.len().min(300);
        while iterations > 0 {
            iterations -= 1;
            if self.peers.len() < low_watermark {
                break;
            }
            if rr >= self.peers.len() {
                rr = 0;
            }
            let current = rr;
            let locked = self.locked == Some(current);
            let pe = &self.peers[current];

            if !locked
                && self.is_erase_candidate(pe)
                && erase_candidate.is_none_or(|ec| !compare_peer_for_erase(&self.peers[ec], pe))
            {
                if self.should_erase_immediately(pe) {
                    if erase_candidate.is_some_and(|ec| ec > current) {
                        erase_candidate = erase_candidate.map(|ec| ec - 1);
                    }
                    if force_erase_candidate.is_some_and(|fc| fc > current) {
                        force_erase_candidate = force_erase_candidate.map(|fc| fc - 1);
                    }
                    self.erase_peer(current);
                    continue;
                }
                erase_candidate = Some(current);
            }
            if !locked
                && self.is_force_erase_candidate(pe)
                && force_erase_candidate
                    .is_none_or(|fc| !compare_peer_for_erase(&self.peers[fc], pe))
            {
                force_erase_candidate = Some(current);
            }
            rr += 1;
        }

        if let Some(ec) = erase_candidate {
            self.erase_peer(ec);
        } else if force && let Some(fc) = force_erase_candidate {
            self.erase_peer(fc);
        }
    }

    /// `recalculate_connect_candidates` — recompute the cached
    /// count (called when `finished`/`max_failcount` changes).
    pub fn recalculate_connect_candidates(&mut self, state: &PeerListState) {
        self.finished = state.is_finished;
        self.max_failcount = state.max_failcount;
        self.num_connect_candidates = self
            .peers
            .iter()
            .filter(|p| self.is_connect_candidate(p))
            .count() as i32;
    }

    /// `update_connect_candidates` helper — ±1 on transitions.
    fn update_candidates(&mut self, was: bool, is: bool) {
        self.num_connect_candidates += match (was, is) {
            (false, true) => 1,
            (true, false) => -1,
            _ => 0,
        };
    }

    /// `erase_peer` — removes index, fixes `round_robin`/`locked`.
    pub fn erase_peer(&mut self, index: usize) {
        if index >= self.peers.len() {
            return;
        }
        let was = self.is_connect_candidate(&self.peers[index]);
        let was_seed = self.peers[index].seed;
        // a cached candidate that gets erased must not be popped
        // later (upstream drops the pointer from `m_candidate_cache`).
        let key = (
            crate::torrent_peer::PeerAddr::clone(&self.peers[index].addr),
            self.peers[index].port,
        );
        self.candidate_cache.retain(|k| *k != key);
        self.peers.remove(index);
        self.alloc.account_free();
        self.update_candidates(was, false);
        if was_seed {
            self.num_seeds -= 1;
        }
        if self.round_robin > index {
            self.round_robin -= 1;
        }
        self.locked = self.locked.and_then(|l| match l.cmp(&index) {
            std::cmp::Ordering::Equal => None,
            std::cmp::Ordering::Greater => Some(l - 1),
            std::cmp::Ordering::Less => Some(l),
        });
    }

    /// `clear_peer_prio` — reset every cached BEP-40 `peer_rank`
    /// (upstream clears on `settings`/external-IP changes).
    pub fn clear_peer_prio(&mut self) {
        for p in &self.peers {
            p.peer_rank.set(0);
        }
    }

    /// `lock_peer`/`unlock_peer` — pin an index during callbacks.
    pub fn lock_peer(&mut self, index: usize) {
        self.locked = Some(index);
    }

    /// Release the pin set by `lock_peer`.
    pub fn unlock_peer(&mut self) {
        self.locked = None;
    }
}

/// `compare_peer_erase` — upstream ordering for weeding: `true` =
/// `lhs` is a *better erase candidate* than `rhs` (prefer to drop).
/// Order: higher failcount, resume-data-only source, non-connectable,
/// lower `trust_points`.
fn compare_peer_for_erase(lhs: &TorrentPeer, rhs: &TorrentPeer) -> bool {
    if lhs.failcount != rhs.failcount {
        return lhs.failcount > rhs.failcount;
    }
    let lr = primary_source(lhs.source) == PeerSourceFlags::RESUME_DATA;
    let rr = primary_source(rhs.source) == PeerSourceFlags::RESUME_DATA;
    if lr != rr {
        return lr;
    }
    if lhs.connectable != rhs.connectable {
        return !lhs.connectable;
    }
    lhs.trust_points < rhs.trust_points
}

/// Convenience: `SocketAddr` of a `PeerAddr` (None for i2p).
#[must_use]
pub fn peer_endpoint(p: &TorrentPeer) -> Option<SocketAddr> {
    p.ip()
}
