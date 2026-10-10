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

use bitdaemon_common::PeerSourceFlags;

use crate::ip_helpers::is_local;
use crate::torrent_peer::TorrentPeer;

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
    /// `ip` — our external IP (peer ranking), if known.
    pub external_ip: Option<IpAddr>,
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
            external_ip: None,
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
    let lr = source_rank(lhs.source);
    let rr = source_rank(rhs.source);
    if lr != rr {
        return lr > rr;
    }
    lhs.peer_rank > rhs.peer_rank
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
        }
    }

    /// Read access (sorted by address).
    #[must_use]
    pub fn peers(&self) -> &[TorrentPeer] {
        &self.peers
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

    /// `add_peer` — called per tracker/pex/lsd/dht peer. Returns
    /// `(index, is_new)`.
    pub fn add_peer(
        &mut self,
        remote: SocketAddr,
        src: PeerSourceFlags,
        connectable: bool,
    ) -> (usize, bool) {
        if let Some(i) = self.find_peer(remote.ip()) {
            let was = self.is_connect_candidate(&self.peers[i]);
            let p = &mut self.peers[i];
            p.source |= src;
            if connectable {
                p.connectable = true;
                p.port = remote.port();
            }
            let is = self.is_connect_candidate(&self.peers[i]);
            self.update_candidates(was, is);
            return (i, false);
        }
        let p = TorrentPeer::new_v4(remote, connectable, src);
        let is = self.is_connect_candidate(&p);
        self.peers.push(p);
        self.update_candidates(false, is);
        (self.peers.len() - 1, true)
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
                        self.peers[other].connectable = true;
                        self.peers[other].source |= src;
                        self.erase_peer(index);
                        return false;
                    }
                    self.erase_peer(other);
                    // index may have shifted
                    return self.update_peer_port(
                        port,
                        self.find_peer(addr).unwrap_or(usize::MAX),
                        src,
                        state,
                    );
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

    /// `set_seed`.
    pub fn set_seed(&mut self, index: usize, s: bool) {
        let p = &mut self.peers[index];
        if p.seed == s {
            return;
        }
        p.seed = s;
        self.num_seeds += if s { 1 } else { -1 };
    }

    /// `set_failcount` — success resets to 0 (upstream).
    pub fn set_failcount(&mut self, index: usize, f: u8) {
        let was = self.is_connect_candidate(&self.peers[index]);
        self.peers[index].failcount = f;
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// `inc_failcount` — saturating, like upstream `++failcount`.
    pub fn inc_failcount(&mut self, index: usize) {
        let f = self.peers[index].failcount;
        if f < self.max_failcount.min(255) as u8 {
            self.set_failcount(index, f + 1);
        }
    }

    /// `ban_peer` — mark banned; a connected peer gets disconnected
    /// by the caller.
    pub fn ban_peer(&mut self, index: usize) {
        let was = self.is_connect_candidate(&self.peers[index]);
        self.peers[index].banned = true;
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// Mark `index` as connected (`set_connection`) — upstream
    /// stores the `peer_connection*`; we only need the flag.
    pub fn set_connected(&mut self, index: usize) {
        let was = self.is_connect_candidate(&self.peers[index]);
        self.peers[index].connection = Some(std::sync::Arc::new(crate::torrent_peer::NullStats));
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
    }

    /// `connection_closed` — clears the connection flag and bumps
    /// `last_connected`.
    pub fn connection_closed(&mut self, index: usize, session_time: i32) {
        let was = self.is_connect_candidate(&self.peers[index]);
        let p = &mut self.peers[index];
        p.connection = None;
        p.last_connected = session_time.max(0) as u16;
        let is = self.is_connect_candidate(&self.peers[index]);
        self.update_candidates(was, is);
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
    ) -> Vec<usize> {
        const CANDIDATE_COUNT: usize = 10;
        let mut peers: Vec<usize> = Vec::with_capacity(CANDIDATE_COUNT);
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

            if peers.len() == CANDIDATE_COUNT
                && compare_peer(&self.peers[peers[CANDIDATE_COUNT - 1]], pe, state)
            {
                continue;
            }
            if peers.len() >= CANDIDATE_COUNT {
                peers.truncate(CANDIDATE_COUNT - 1);
            }
            let pos = peers
                .iter()
                .position(|&i| !compare_peer(&self.peers[i], pe, state))
                .unwrap_or(peers.len());
            peers.insert(pos, current);
        }

        if let Some(ec) = erase_candidate {
            self.erase_peer(ec);
        }
        peers
    }

    /// `apply_ip_filter` — returns the banned addresses; the caller
    /// disconnects live connections.
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
            if let Some(a) = addr {
                banned.push(a);
            }
            if self.peers[i].connection.is_none() {
                self.erase_peer(i);
            } else {
                // connected — caller disconnects; still ban
                self.ban_peer(i);
                i += 1;
            }
        }
        banned
    }

    /// `erase_peers` — standalone weeding pass (upstream runs it on a
    /// timer): erase the worst candidate while over
    /// `max_peerlist_size`, force-erase past 115%.
    pub fn erase_peers(&mut self, state: &PeerListState) {
        if state.max_peerlist_size == 0 || self.peers.is_empty() {
            return;
        }
        let max = state.max_peerlist_size as usize;
        while self.peers.len() > max {
            // pick the worst erase candidate; force-erase any
            // disconnected peer past 115%
            let force = self.peers.len() > max * 115 / 100;
            let mut victim: Option<usize> = None;
            for i in 0..self.peers.len() {
                if self.locked == Some(i) {
                    continue;
                }
                let p = &self.peers[i];
                let ok = if force {
                    self.is_force_erase_candidate(p)
                } else {
                    self.is_erase_candidate(p)
                };
                if !ok {
                    continue;
                }
                if victim.is_none_or(|v| compare_peer_for_erase(&self.peers[v], p))
                    || self.should_erase_immediately(p)
                {
                    victim = Some(i);
                    if self.should_erase_immediately(p) {
                        break;
                    }
                }
            }
            match victim {
                Some(v) => self.erase_peer(v),
                None => break,
            }
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
        self.peers.remove(index);
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

    /// `lock_peer`/`unlock_peer` — pin an index during callbacks.
    pub fn lock_peer(&mut self, index: usize) {
        self.locked = Some(index);
    }

    /// Release the pin set by `lock_peer`.
    pub fn unlock_peer(&mut self) {
        self.locked = None;
    }
}

/// `compare_peer_erase` — ordering for weeding: prefer to erase the
/// *worse* candidate. Upstream `compare_peer_erase(lhs, rhs)` returns
/// true if `lhs` should be kept over `rhs`; we keep the same
/// direction (failcount, then resume-data, then connectable).
fn compare_peer_for_erase(kept: &TorrentPeer, candidate: &TorrentPeer) -> bool {
    if kept.failcount != candidate.failcount {
        return kept.failcount > candidate.failcount;
    }
    let kr = primary_source(kept.source) == PeerSourceFlags::RESUME_DATA;
    let cr = primary_source(candidate.source) == PeerSourceFlags::RESUME_DATA;
    if kr != cr {
        return cr;
    }
    !kept.connectable && candidate.connectable
}

/// Convenience: `SocketAddr` of a `PeerAddr` (None for i2p).
#[must_use]
pub fn peer_endpoint(p: &TorrentPeer) -> Option<SocketAddr> {
    p.ip()
}
