// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/torrent_peer_allocator.hpp` + `torrent_peer_allocator.cpp`.
//!
//! Upstream keeps per-address-family `aux::pool`s (500-entry chunked
//! freelists) to avoid heap churn across tens of thousands of peers.
//! Rust has no placement contract for `Box`, so the pooling itself is
//! left to the global allocator; what this port preserves verbatim is
//! the *accounting surface*: `total_bytes`, `total_allocations`,
//! `live_bytes`, `live_allocations` — which `performance_counters`
//! reports upstream.

use bitdaemon_net::torrent_peer::TorrentPeer;

/// `torrent_peer_allocator_interface::{ipv4,ipv6,i2p,rtc}_peer_type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerEntryType {
    /// `ipv4_peer_type`.
    Ipv4,
    /// `ipv6_peer_type`.
    Ipv6,
    /// `i2p_peer_type`.
    I2p,
    /// `rtc_peer_type` (WebRTC peers — Phase E).
    Rtc,
}

/// `torrent_peer_allocator` — allocate/free `TorrentPeer` values with
/// upstream's accounting counters.
#[derive(Debug, Default)]
pub struct TorrentPeerAllocator {
    /// `m_total_bytes` — cumulative.
    total_bytes: u64,
    /// `m_total_allocations` — cumulative.
    total_allocations: u64,
    /// `m_live_bytes`.
    live_bytes: i64,
    /// `m_live_allocations`.
    live_allocations: i64,
}

impl TorrentPeerAllocator {
    /// `allocate_peer_entry` — boxed peer; counters bumped like the
    /// upstream pool (per-type `sizeof(*_peer)` ≈ `TorrentPeer` here —
    /// documented deviation: a single enum replaces the subclasses).
    #[must_use]
    pub fn allocate_peer_entry(
        &mut self,
        _ty: PeerEntryType,
        peer: TorrentPeer,
    ) -> Box<TorrentPeer> {
        let b = std::mem::size_of::<TorrentPeer>();
        self.total_bytes += b as u64;
        self.live_bytes += b as i64;
        self.live_allocations += 1;
        self.total_allocations += 1;
        Box::new(peer)
    }

    /// `free_peer_entry` — drop + decrement live counters.
    pub fn free_peer_entry(&mut self, p: Box<TorrentPeer>) {
        let b = std::mem::size_of::<TorrentPeer>();
        debug_assert!(self.live_bytes >= b as i64 && self.live_allocations > 0);
        self.live_bytes -= b as i64;
        self.live_allocations -= 1;
        drop(p);
    }

    /// `total_bytes`.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// `total_allocations`.
    #[must_use]
    pub fn total_allocations(&self) -> u64 {
        self.total_allocations
    }

    /// `live_bytes`.
    #[must_use]
    pub fn live_bytes(&self) -> i64 {
        self.live_bytes
    }

    /// `live_allocations`.
    #[must_use]
    pub fn live_allocations(&self) -> i64 {
        self.live_allocations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitdaemon_common::PeerSourceFlags;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn counters() {
        let mut a = TorrentPeerAllocator::default();
        let p = TorrentPeer::new_v4(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6881),
            true,
            PeerSourceFlags::default(),
        );
        let b = a.allocate_peer_entry(PeerEntryType::Ipv4, p);
        assert_eq!(a.live_allocations(), 1);
        assert_eq!(a.total_allocations(), 1);
        assert!(a.live_bytes() > 0);
        a.free_peer_entry(b);
        assert_eq!(a.live_allocations(), 0);
        assert_eq!(a.live_bytes(), 0);
        assert_eq!(a.total_allocations(), 1);
    }
}
