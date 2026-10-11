// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `instantiate_connection.cpp` — the upstream decision table
//! that picks a `socket_type` variant from (uTP?, SSL ctx?, I2P proxy?,
//! proxy type, proxy_peer/tracker_connections). We return a
//! `TransportKind` instead of a boost::variant socket; the session
//! maps each variant to the matching `PeerConnector` implementation.

use crate::proxy::{ProxySettings, ProxyType};

/// What a connection is for — upstream `peer_connection` /
/// `tracker_connection` flags of `instantiate_connection`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnKind {
    /// `peer_connection = true` — honors `proxy_peer_connections`.
    Peer,
    /// `tracker_connection = true` — honors `proxy_tracker_connections`.
    Tracker,
}

/// Chosen transport — mirror of the `socket_type` variant upstream
/// would have instantiated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportKind {
    /// `tcp::socket`
    Tcp,
    /// `ssl_stream<tcp::socket>`
    SslTcp,
    /// `http_stream` (HTTP CONNECT proxy)
    HttpStream,
    /// `ssl_stream<http_stream>`
    SslHttpStream,
    /// `socks5_stream` (also covers socks4 + socks5_pw)
    Socks,
    /// `ssl_stream<socks5_stream>`
    SslSocks,
    /// `utp_stream`
    Utp,
    /// `ssl_stream<utp_stream>`
    SslUtp,
    /// `i2p_stream` — SAM connection; SSL over I2P is refused upstream.
    I2p,
}

/// `instantiate_connection` — pick the transport for one outbound
/// connection. `ssl` is "the peer is an SSL torrent / HTTPS tracker".
///
/// Returns `None` only when no variant matches (upstream
/// `TORRENT_ASSERT_FAIL` + throw — we surface it as a value since the
/// caller knows the settings were validated).
pub fn instantiate(
    proxy: &ProxySettings,
    kind: ConnKind,
    ssl: bool,
    utp: bool,
) -> Option<TransportKind> {
    // upstream: uTP is tried before the proxy check — proxied uTP
    // isn't a thing (udp_socket handles the datagram side).
    if utp {
        return Some(if ssl {
            TransportKind::SslUtp
        } else {
            TransportKind::Utp
        });
    }
    if proxy.proxy_type == ProxyType::I2p {
        // "it doesn't make any sense to try ssl over i2p"
        return if ssl { None } else { Some(TransportKind::I2p) };
    }
    // proxy_none, or this connection class isn't proxied → direct.
    let proxied = match kind {
        ConnKind::Peer => proxy.proxy_peer_connections,
        ConnKind::Tracker => proxy.proxy_tracker_connections,
    };
    if proxy.proxy_type == ProxyType::None || !proxied {
        return Some(if ssl {
            TransportKind::SslTcp
        } else {
            TransportKind::Tcp
        });
    }
    match proxy.proxy_type {
        ProxyType::Http | ProxyType::HttpPassword => Some(if ssl {
            TransportKind::SslHttpStream
        } else {
            TransportKind::HttpStream
        }),
        ProxyType::Socks4 | ProxyType::Socks5 | ProxyType::Socks5Password => Some(if ssl {
            TransportKind::SslSocks
        } else {
            TransportKind::Socks
        }),
        ProxyType::None | ProxyType::I2p => None, // handled above
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ps(ty: ProxyType) -> ProxySettings {
        ProxySettings {
            proxy_type: ty,
            hostname: "127.0.0.1".into(),
            port: 9050,
            proxy_peer_connections: true,
            proxy_tracker_connections: true,
            ..ProxySettings::default()
        }
    }

    #[test]
    fn direct_and_ssl() {
        let p = ps(ProxyType::None);
        assert_eq!(
            instantiate(&p, ConnKind::Peer, false, false),
            Some(TransportKind::Tcp)
        );
        assert_eq!(
            instantiate(&p, ConnKind::Peer, true, false),
            Some(TransportKind::SslTcp)
        );
        assert_eq!(
            instantiate(&p, ConnKind::Peer, true, true),
            Some(TransportKind::SslUtp)
        );
    }

    #[test]
    fn proxy_table() {
        assert_eq!(
            instantiate(&ps(ProxyType::Http), ConnKind::Peer, false, false),
            Some(TransportKind::HttpStream)
        );
        assert_eq!(
            instantiate(&ps(ProxyType::Socks5), ConnKind::Tracker, true, false),
            Some(TransportKind::SslSocks)
        );
        assert_eq!(
            instantiate(&ps(ProxyType::I2p), ConnKind::Peer, false, false),
            Some(TransportKind::I2p)
        );
        assert_eq!(
            instantiate(&ps(ProxyType::I2p), ConnKind::Peer, true, false),
            None
        );
    }

    #[test]
    fn unproxied_class_goes_direct() {
        let mut p = ps(ProxyType::Socks5);
        p.proxy_peer_connections = false;
        assert_eq!(
            instantiate(&p, ConnKind::Peer, false, false),
            Some(TransportKind::Tcp)
        );
        assert_eq!(
            instantiate(&p, ConnKind::Tracker, false, false),
            Some(TransportKind::Socks)
        );
    }
}
