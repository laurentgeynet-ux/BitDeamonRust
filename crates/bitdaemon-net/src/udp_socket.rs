// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/udp_socket` (`udp_socket.cpp`) — the wrapper every
//! UDP lane (uTP, DHT, UDP trackers, LSD, NAT-PMP) shares: SOCKS5
//! UDP-associate relay handling and EAGAIN write blocking.
//!
//! Upstream wraps a `datagram_socket` bound per listen socket; ours
//! wraps an injected [`DatagramSocket`](crate::transport::DatagramSocket) (ADR-0008).

use std::net::SocketAddr;
use std::sync::Arc;

use crate::socks_udp;
use crate::transport::DatagramSocket;

/// `aux::udp_socket`.
#[derive(Debug)]
pub struct UdpSocket {
    /// `m_sock` — the injected datagram socket.
    sock: Arc<dyn DatagramSocket>,
    /// `m_socks5_connection` — the UDP relay address learned from the
    /// SOCKS5 `UDP ASSOCIATE` reply; datagrams outbound are wrapped in
    /// the FRAG-0 header and sent *to the relay*, not the target.
    socks5_relay: Option<SocketAddr>,
    /// `m_write_blocked` — last `send` hit `WouldBlock`; upstream
    /// flips this in the write handler and notifies the uTP socket
    /// manager when it clears.
    pub write_blocked: bool,
    /// `m_force_proxy` — drop non-proxied traffic when a proxy is
    /// configured (upstream `settings.proxy_*` toggle).
    pub force_proxy: bool,
}

impl UdpSocket {
    /// ctor — `proxy_relay` set once the SOCKS5 UDP association lands
    /// (upstream learns it from the associate response).
    #[must_use]
    pub fn new(sock: Arc<dyn DatagramSocket>, socks5_relay: Option<SocketAddr>) -> Self {
        Self {
            sock,
            socks5_relay,
            write_blocked: false,
            force_proxy: socks5_relay.is_some(),
        }
    }

    /// `local_endpoint`.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.sock.local_addr()
    }

    /// `local_port` — port announced to trackers/DHT.
    #[must_use]
    pub fn local_port(&self) -> u16 {
        self.sock.local_addr().port()
    }

    /// `wrap` — `send` path: through the SOCKS5 relay the payload gets
    /// the UDP header and the *relay* is the transport destination.
    /// `force_proxy` with no established relay drops the datagram
    /// (upstream drops silently and reports success to keep queues
    /// from stalling — `wrapped` flag is returned instead).
    #[must_use]
    pub fn wrap(&self, target: SocketAddr, payload: &[u8]) -> (SocketAddr, Vec<u8>) {
        match self.socks5_relay {
            Some(relay) => (relay, socks_udp::socks5_wrap(target, payload)),
            None => (target, payload.to_vec()),
        }
    }

    /// `unwrap` — `on_read` path: strip the SOCKS5 header and return
    /// the real source; plain datagrams pass through (upstream also
    /// detects *source-is-relay* vs plain to learn/track the relay).
    #[must_use]
    pub fn unwrap(buf: &[u8], from: SocketAddr) -> (SocketAddr, &[u8]) {
        match socks_udp::socks5_unwrap(buf) {
            Some((socks_udp::Socks5From::Addr(ep), payload)) => (ep, payload),
            _ => (from, buf),
        }
    }

    /// `send` — proxy-aware send: wraps through the SOCKS5 relay when
    /// one is associated. `Ok(0)` with `force_proxy` and no relay =
    /// upstream's silent drop.
    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        if self.force_proxy && self.socks5_relay.is_none() {
            return Ok(buf.len());
        }
        let (to, wrapped) = self.wrap(target, buf);
        self.sock.send_to(&wrapped, to).await
    }

    /// `recv_from` — receive and SOCKS5-unwrap; returns the real
    /// sender.
    pub async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        let (n, from) = self.sock.recv_from(buf).await?;
        if let Some((socks_udp::Socks5From::Addr(ep), payload)) =
            socks_udp::socks5_unwrap(&buf[..n])
        {
            // the unwrapped payload starts at a fixed 10-byte header —
            // shift it down in place.
            let plen = payload.len();
            buf.copy_within(n - plen..n, 0);
            return Ok((plen, ep));
        }
        Ok((n, from))
    }
}
