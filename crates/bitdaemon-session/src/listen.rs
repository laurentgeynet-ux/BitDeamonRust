// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/listen_socket_t` (in `session_impl.hpp`) +
//! `listen_socket_handle` (`aux_/listen_socket_handle.hpp`) —
//! one per listen interface. In the actor model the session owns
//! `Vec<ListenSocket>`; the handle is a stable `id` that resolves
//! through the session (upstream's `weak_ptr<listen_socket_t>`).

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};

use bitdaemon_net::ip_voter::IpVoter;

static NEXT_ID: AtomicU32 = AtomicU32::new(0);

/// `listen_socket_flags_t` bits.
#[derive(Clone, Copy, Debug, Default)]
pub struct ListenSocketFlags(pub u32);

impl ListenSocketFlags {
    /// `accept_incoming` — accept inbound connections.
    pub const ACCEPT_INCOMING: Self = Self(1 << 0);
    /// `local_network` — interface only reaches the LAN.
    pub const LOCAL_NETWORK: Self = Self(1 << 1);
    /// `was_expanded` — expanded from an unspecified address.
    pub const WAS_EXPANDED: Self = Self(1 << 2);
    /// `proxy` — this socket represents the configured proxy.
    pub const PROXY: Self = Self(1 << 3);

    /// bit test.
    #[must_use]
    pub fn contains(self, f: Self) -> bool {
        self.0 & f.0 != 0
    }
}

/// `listen_port_mapping` — an external port learned via NAT-PMP or
/// UPnP (index 0 = natpmp, 1 = upnp).
pub type PortMapping = i32;

/// `listen_socket_t` — per-interface listen state.
pub struct ListenSocket {
    /// `id` — never reused while the session lives.
    pub id: u32,
    /// `flags` — `accept_incoming`/`local_network`/`was_expanded`/`proxy`.
    pub flags: ListenSocketFlags,
    /// `external_address` — WAN-IP voter fed by trackers/DHT/PEX.
    pub external_address: IpVoter,
    /// `local_endpoint` — the bound TCP endpoint.
    pub local_endpoint: SocketAddr,
    /// `netmask` — for `can_route`'s same-subnet test.
    pub netmask: IpAddr,
    /// `device` — interface name ("" when not bound to one).
    pub device: String,
    /// `original_port` — requested port before any retry.
    pub original_port: u16,
    /// `tcp_port_mapping` — `[natpmp, upnp]` external ports, 0 = none.
    pub tcp_port_mapping: [PortMapping; 2],
    /// `udp_port_mapping` — ditto for UDP.
    pub udp_port_mapping: [PortMapping; 2],
    /// `ssl` — `transport::ssl` sockets announce `ssl` scrape/flags.
    pub ssl: bool,
    /// actual UDP port bound for this socket (`udp_sock->sock`
    /// local port upstream; our transports are injected, so the
    /// session records what the `DatagramSocket` reported).
    pub udp_local_port: u16,
}

impl ListenSocket {
    /// `listen_socket_t` ctor — id from the never-reused counter.
    #[must_use]
    pub fn new(local_endpoint: SocketAddr, netmask: IpAddr, device: String) -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            flags: ListenSocketFlags::ACCEPT_INCOMING,
            external_address: IpVoter::default(),
            local_endpoint,
            netmask,
            device,
            original_port: local_endpoint.port(),
            tcp_port_mapping: [0; 2],
            udp_port_mapping: [0; 2],
            ssl: false,
            udp_local_port: 0,
        }
    }

    /// `can_route` — verbatim: proxy routes everything, family and
    /// v6 scope must match, unspecified/local/subnet pass, else
    /// only when the interface isn't flagged local-only.
    #[must_use]
    pub fn can_route(&self, addr: &IpAddr) -> bool {
        if self.flags.contains(ListenSocketFlags::PROXY) {
            return true;
        }
        if self.local_endpoint.is_ipv4() != addr.is_ipv4() {
            return false;
        }
        // scope_id — our sockets don't carry it yet (link-local v6
        // routing lands with `bind_to_device`); treat as 0 == 0.
        if self.local_endpoint.ip() == *addr {
            return true;
        }
        if self.local_endpoint.ip().is_unspecified() {
            return true;
        }
        if bitdaemon_net::enum_net::match_addr_mask(addr, &self.local_endpoint.ip(), &self.netmask)
        {
            return true;
        }
        !self.flags.contains(ListenSocketFlags::LOCAL_NETWORK)
    }

    /// `get_local_endpoint` — the UDP socket's bound port if bound,
    /// else the TCP endpoint.
    #[must_use]
    pub fn local_endpoint(&self) -> SocketAddr {
        if self.udp_local_port != 0 {
            SocketAddr::new(self.local_endpoint.ip(), self.udp_local_port)
        } else {
            self.local_endpoint
        }
    }

    /// `tcp_external_port` — first non-zero NAT mapping wins
    /// (NAT-PMP before UPnP), else the local port.
    #[must_use]
    pub fn tcp_external_port(&self) -> u16 {
        self.tcp_port_mapping
            .iter()
            .find(|p| **p != 0)
            .map_or(self.local_endpoint.port(), |p| *p as u16)
    }

    /// `udp_external_port` — same, falling back to the bound UDP
    /// port.
    #[must_use]
    pub fn udp_external_port(&self) -> u16 {
        self.udp_port_mapping
            .iter()
            .find(|p| **p != 0)
            .map_or(self.udp_local_port, |p| *p as u16)
    }

    /// `get_external_address` — the current voted WAN address.
    #[must_use]
    pub fn external_addr(&self) -> Option<IpAddr> {
        self.external_address.external_address()
    }
}

/// `listen_socket_handle` — weak reference by stable id; resolves
/// against the session's socket list. Comparisons by `id` only
/// (upstream compares `m_id` too).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListenSocketHandle {
    id: Option<u32>,
}

impl ListenSocketHandle {
    /// wrap a live socket.
    #[must_use]
    pub fn new(s: &ListenSocket) -> Self {
        Self { id: Some(s.id) }
    }

    /// `explicit operator bool` — the socket still exists.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.id.is_some()
    }

    /// `get()` — resolve against a socket list.
    #[must_use]
    pub fn get<'a>(&self, socks: &'a [ListenSocket]) -> Option<&'a ListenSocket> {
        self.id.and_then(|id| socks.iter().find(|s| s.id == id))
    }

    /// `get_external_address`.
    #[must_use]
    pub fn external_address(&self, socks: &[ListenSocket]) -> Option<IpAddr> {
        self.get(socks).and_then(ListenSocket::external_addr)
    }

    /// `get_local_endpoint`.
    #[must_use]
    pub fn local_endpoint(&self, socks: &[ListenSocket]) -> Option<SocketAddr> {
        self.get(socks).map(ListenSocket::local_endpoint)
    }

    /// `can_route`.
    #[must_use]
    pub fn can_route(&self, socks: &[ListenSocket], a: &IpAddr) -> bool {
        self.get(socks).is_some_and(|s| s.can_route(a))
    }

    /// `device`.
    #[must_use]
    pub fn device<'a>(&self, socks: &'a [ListenSocket]) -> &'a str {
        self.get(socks).map_or("", |s| s.device.as_str())
    }

    /// `is_ssl`.
    #[must_use]
    pub fn is_ssl(&self, socks: &[ListenSocket]) -> bool {
        self.get(socks).is_some_and(|s| s.ssl)
    }
}

/// `session_udp_socket` — the per-socket UDP lane: upstream pairs a
/// `udp_socket` with reusable handler storage and a `write_blocked`
/// flag for EAGAIN back-pressure. Our UDP path is the injected
/// `DatagramSocket`, so the struct carries the handle + the flag +
/// the upstream writeable-notification contract.
pub struct SessionUdpSocket {
    /// `sock`'s owner handle.
    pub ls: ListenSocketHandle,
    /// `write_blocked` — last send hit EAGAIN; cleared on the
    /// transport's next writability signal (upstream notifies the
    /// uTP socket manager).
    pub write_blocked: bool,
    /// bound UDP port as reported by the injected datagram socket.
    pub local_port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn sock(ip: IpAddr, flags: u32) -> ListenSocket {
        let mut s = ListenSocket::new(
            SocketAddr::new(ip, 6881),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0)),
            String::new(),
        );
        s.flags = ListenSocketFlags(flags);
        s
    }

    #[test]
    fn can_route_verbatim() {
        // proxy routes everything
        let s = sock(IpAddr::V4(Ipv4Addr::LOCALHOST), 1 << 3);
        assert!(s.can_route(&IpAddr::V6(Ipv6Addr::LOCALHOST)));
        // family mismatch never routes
        let s = sock(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)), 0);
        assert!(!s.can_route(&IpAddr::V6(Ipv6Addr::LOCALHOST)));
        // same subnet via netmask
        assert!(s.can_route(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 9))));
        // off-subnet but not local-only → routes
        assert!(s.can_route(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        // local_network flag blocks off-subnet
        let s = sock(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)), 1 << 1);
        assert!(s.can_route(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 9))));
        assert!(!s.can_route(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        // unspecified local endpoint routes everything
        let s = sock(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1 << 1);
        assert!(s.can_route(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }

    #[test]
    fn external_port_priority() {
        let mut s = sock(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        assert_eq!(s.tcp_external_port(), 6881);
        s.tcp_port_mapping = [0, 7000];
        assert_eq!(s.tcp_external_port(), 7000);
        s.tcp_port_mapping = [6000, 7000]; // natpmp wins
        assert_eq!(s.tcp_external_port(), 6000);
        s.udp_local_port = 6999;
        assert_eq!(s.udp_external_port(), 6999);
        s.udp_port_mapping = [8000, 0];
        assert_eq!(s.udp_external_port(), 8000);
        assert_eq!(s.local_endpoint().port(), 6999);
    }

    #[test]
    fn handle_id_semantics() {
        let a = ListenSocket::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6881),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            String::new(),
        );
        let b = ListenSocket::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6882),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            String::new(),
        );
        assert_ne!(a.id, b.id);
        let h = ListenSocketHandle::new(&a);
        assert!(h.is_valid());
        let socks = [b]; // `a` dropped — handle dangles like weak_ptr
        assert!(h.get(&socks).is_none());
        assert!(!h.is_valid() || h.get(&socks).is_none());
        assert!(!ListenSocketHandle::default().is_valid());
    }
}
