//! Port of `session_impl`'s default socket layer (`listen_sockets` /
//! `udp_socket` system side).
//!
//! The **default OS transports** — plain TCP `connect`/`accept` and a
//! real `UdpSocket`. These are the only implementations allowed to
//! touch the OS socket APIs directly (ADR-0008); anonymous sessions
//! inject tunnel transports instead and simply never construct these.

use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::transport::{
    AcceptFut, BoxFut, BoxedPeerStream, DatagramSocket, PeerAcceptor, PeerConnector,
};

/// `TokioTcpConnector` — the default outbound transport
/// (`libtorrent`'s built-in TCP `socket_type`).
#[derive(Debug)]
pub struct TokioTcpConnector;

impl PeerConnector for TokioTcpConnector {
    fn connect(
        self: Arc<Self>,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>> {
        Box::pin(async move {
            let s = TcpStream::connect(addr).await?;
            Ok(Box::new(s) as BoxedPeerStream)
        })
    }
}

/// `TokioTcpAcceptor` — `TcpListener` as a `PeerAcceptor`.
#[derive(Debug)]
pub struct TokioTcpAcceptor {
    listener: TcpListener,
}

impl TokioTcpAcceptor {
    /// Bind the listen socket (`session_impl::open_listen_port`).
    ///
    /// # Errors
    /// Propagates the bind error (`EADDRINUSE` …).
    pub async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr).await?,
        })
    }

    /// Local bound address (post-`bind`).
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl PeerAcceptor for TokioTcpAcceptor {
    fn accept(self: Arc<Self>) -> AcceptFut {
        Box::pin(async move {
            let (s, remote) = self.listener.accept().await?;
            Ok((Box::new(s) as BoxedPeerStream, Some(remote)))
        })
    }
}

/// `TokioUdpSocket` — the default `DatagramSocket` (DHT, UDP
/// trackers, uTP, LSD all run on top of this).
#[derive(Debug)]
pub struct TokioUdpSocket {
    sock: UdpSocket,
}

impl TokioUdpSocket {
    /// Bind a UDP socket (`udp_socket` ctor upstream).
    ///
    /// # Errors
    /// Propagates the bind error.
    pub async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        Ok(Self {
            sock: UdpSocket::bind(addr).await?,
        })
    }
}

impl DatagramSocket for TokioUdpSocket {
    fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> BoxFut<'a, usize> {
        Box::pin(async move { self.sock.send_to(buf, target).await })
    }

    fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> BoxFut<'a, (usize, SocketAddr)> {
        Box::pin(async move { self.sock.recv_from(buf).await })
    }

    fn local_addr(&self) -> SocketAddr {
        self.sock
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), 0))
    }
}
