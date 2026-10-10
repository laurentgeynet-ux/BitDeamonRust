// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `proxy_settings` + `socks5_stream`/`http_stream` glue — a
//! `SocksConnector`/`HttpConnectConnector` decorates an injected
//! `PeerConnector`: dial the proxy through the inner transport,
//! then run the proxy handshake on the resulting stream. Under
//! OnionBit the "inner" connector may itself be a tunnel lane —
//! the proxy adds no direct-socket escape.

use std::fmt::Debug;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use crate::socks::{SocksCommand, SocksTarget, socks_connect};
use crate::transport::{BoxedPeerStream, PeerConnector};

/// `settings_pack::proxy_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProxyType {
    /// `none` — direct.
    #[default]
    None,
    /// `socks4`.
    Socks4,
    /// `socks5`.
    Socks5,
    /// `socks5_password`.
    Socks5Password,
    /// `http` — CONNECT tunnel.
    Http,
    /// `http_password`.
    HttpPassword,
    /// `i2p_proxy` — SAM bridge (see [`crate::i2p`]).
    I2p,
}

/// `settings_pack::proxy_*` — one proxy configuration shared by
/// peer connections and tracker announces.
#[derive(Debug, Clone, Default)]
pub struct ProxySettings {
    /// `proxy_hostname`.
    pub hostname: String,
    /// `proxy_port`.
    pub port: u16,
    /// `proxy_username`.
    pub username: String,
    /// `proxy_password`.
    pub password: String,
    /// `proxy_type`.
    pub proxy_type: ProxyType,
    /// `proxy_peer_connections`.
    pub proxy_peer_connections: bool,
    /// `proxy_tracker_connections`.
    pub proxy_tracker_connections: bool,
    /// `proxy_hostnames` — let the proxy resolve hostnames
    /// (`SocksTarget::Name` / CONNECT by name instead of IP).
    pub proxy_hostnames: bool,
}

/// `socks5_stream` as a connector — TCP to the proxy via `inner`,
/// then `socks_connect` toward the real target.
#[derive(Debug)]
pub struct SocksConnector {
    inner: Arc<dyn PeerConnector>,
    proxy: SocketAddr,
    version: u8,
    user: String,
    password: String,
}

impl SocksConnector {
    /// `inner` reaches the proxy; `version` is 4 or 5.
    #[must_use]
    pub fn new(
        inner: Arc<dyn PeerConnector>,
        proxy: SocketAddr,
        version: u8,
        user: String,
        password: String,
    ) -> Self {
        Self {
            inner,
            proxy,
            version,
            user,
            password,
        }
    }

    /// build one from `ProxySettings` (`Socks4`/`Socks5*`).
    #[must_use]
    pub fn from_settings(
        inner: Arc<dyn PeerConnector>,
        proxy_addr: SocketAddr,
        p: &ProxySettings,
    ) -> Self {
        Self::new(
            inner,
            proxy_addr,
            match p.proxy_type {
                ProxyType::Socks4 => 4,
                _ => 5,
            },
            p.username.clone(),
            p.password.clone(),
        )
    }
}

impl PeerConnector for SocksConnector {
    fn connect(
        self: Arc<Self>,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>> {
        let inner = self.inner.clone();
        let (proxy, v, user, pass) = (
            self.proxy,
            self.version,
            self.user.clone(),
            self.password.clone(),
        );
        // the trait wants a Sync future; the proxy handshake
        // holds `&mut stream` across awaits (Send, not Sync) —
        // run it in a task and await the handle, which is Sync.
        Box::pin(async move {
            tokio::spawn(async move {
                let mut s = inner.connect(proxy).await?;
                socks_connect(
                    &mut s,
                    v,
                    &user,
                    &pass,
                    &SocksTarget::Addr(addr),
                    SocksCommand::Connect,
                )
                .await
                .map_err(|e| {
                    crate::NetError::Transport(std::io::Error::other(format!("socks: {e}")))
                })?;
                Ok(s)
            })
            .await
            .map_err(|e| crate::NetError::Transport(std::io::Error::other(e.to_string())))?
        })
    }
}

/// `http_stream` as a connector — HTTP `CONNECT` tunnel through
/// the proxy (used for `wss://`/HTTPS upstreams).
#[derive(Debug)]
pub struct HttpConnectConnector {
    inner: Arc<dyn PeerConnector>,
    proxy: SocketAddr,
    user: String,
    password: String,
}

impl HttpConnectConnector {
    /// `inner` reaches the proxy.
    #[must_use]
    pub fn new(
        inner: Arc<dyn PeerConnector>,
        proxy: SocketAddr,
        user: String,
        password: String,
    ) -> Self {
        Self {
            inner,
            proxy,
            user,
            password,
        }
    }
}

impl PeerConnector for HttpConnectConnector {
    fn connect(
        self: Arc<Self>,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>> {
        let inner = self.inner.clone();
        let (proxy, user, pass) = (self.proxy, self.user.clone(), self.password.clone());
        Box::pin(async move {
            tokio::spawn(async move {
                let mut s = inner.connect(proxy).await?;
                crate::http_stream::http_connect(&mut s, addr, None, &user, &pass)
                    .await
                    .map_err(|e| {
                        crate::NetError::Transport(std::io::Error::other(format!("connect: {e}")))
                    })?;
                Ok(s)
            })
            .await
            .map_err(|e| crate::NetError::Transport(std::io::Error::other(e.to_string())))?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::PeerConnector;
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    struct Mem(Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>>);
    impl Debug for Mem {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Mem")
        }
    }
    impl PeerConnector for Mem {
        fn connect(
            self: Arc<Self>,
            _a: SocketAddr,
        ) -> Pin<Box<dyn Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>> {
            let s = self.0.lock().unwrap_or_else(|e| e.into_inner()).pop();
            Box::pin(async move {
                s.ok_or_else(|| crate::NetError::Transport(io::Error::other("no scripted stream")))
                    .map(|s| Box::new(s) as BoxedPeerStream)
            })
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn socks5_connector_end_to_end() {
        // scripted SOCKS5 server: greeting → no-auth → CONNECT →
        // reply success, then echo the target bytes.
        let (client_side, mut srv) = duplex(4096);
        let inner = Arc::new(Mem(Arc::new(std::sync::Mutex::new(vec![client_side]))));
        let server = tokio::spawn(async move {
            let mut g = [0u8; 3];
            srv.read_exact(&mut g).await.unwrap();
            assert_eq!(g, [5, 1, 0]);
            srv.write_all(&[5, 0]).await.unwrap();
            // CONNECT request: ver cmd rsv atyp len addr port
            let mut head = [0u8; 4];
            srv.read_exact(&mut head).await.unwrap();
            assert_eq!(head[..3], [5, 1, 0]);
            let alen = if head[3] == 1 {
                4
            } else if head[3] == 4 {
                16
            } else {
                srv.read_exact(&mut head[..1]).await.unwrap();
                head[0] as usize
            };
            let mut rest = vec![0u8; alen + 2];
            srv.read_exact(&mut rest).await.unwrap();
            srv.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
            let mut b = [0u8; 4];
            srv.read_exact(&mut b).await.unwrap();
            assert_eq!(&b, b"pong");
        });
        let conn = Arc::new(SocksConnector::new(
            inner,
            "127.0.0.1:1080".parse().unwrap(),
            5,
            String::new(),
            String::new(),
        ));
        let mut s = conn
            .connect("10.0.0.1:6881".parse().unwrap())
            .await
            .unwrap();
        s.write_all(b"pong").await.unwrap();
        server.await.unwrap();
    }
}
