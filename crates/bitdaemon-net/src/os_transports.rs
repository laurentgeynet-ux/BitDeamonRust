//! Port of `udp_socket.cpp` + `listen_socket_handle.cpp` — the default
//! (OS) socket layer behind `session_impl`'s `listen_sockets`.
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

/// Source address the OS would pick for traffic to `target` —
/// a transient UDP `connect` probe (no traffic is sent). Used
/// where upstream reads `socket.local_endpoint()` after connect
/// (e.g. UPnP `NewInternalClient`).
///
/// # Errors
/// Propagates the bind/connect error.
pub fn local_addr_to(target: SocketAddr) -> std::io::Result<SocketAddr> {
    let bind = if target.is_ipv4() {
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0)
    };
    let s = std::net::UdpSocket::bind(bind)?;
    s.connect(target)?;
    s.local_addr()
}

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
            let _ = s.set_nodelay(true);
            Ok(Box::new(s) as BoxedPeerStream)
        })
    }

    fn connect_with_bind(
        self: Arc<Self>,
        addr: SocketAddr,
        bind: Option<SocketAddr>,
    ) -> Pin<Box<dyn Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>> {
        Box::pin(async move { self.connect_with_bind_report(addr, bind).await.map(|r| r.0) })
    }

    fn connect_with_bind_report(
        self: Arc<Self>,
        addr: SocketAddr,
        bind: Option<SocketAddr>,
    ) -> Pin<
        Box<
            dyn Future<Output = crate::Result<(BoxedPeerStream, Option<SocketAddr>)>> + Send + Sync,
        >,
    > {
        Box::pin(async move {
            let s = match bind {
                None => TcpStream::connect(addr).await?,
                // `bind_outgoing_socket` — the socket binds before
                // connect; family follows the remote (upstream picks
                // `address_v6::any()` for a v6 target).
                Some(b) => {
                    let sk = if addr.is_ipv6() {
                        tokio::net::TcpSocket::new_v6()?
                    } else {
                        tokio::net::TcpSocket::new_v4()?
                    };
                    // upstream `tcp::acceptor::reuse_address` on
                    // non-Windows; Windows gets `exclusive_address_
                    // use` — default reuseaddr=false gives the same
                    // EADDRINUSE-on-taken-port semantics.
                    #[cfg(not(windows))]
                    sk.set_reuseaddr(true)?;
                    sk.bind(b)?;
                    sk.connect(addr).await?
                }
            };
            let _ = s.set_nodelay(true);
            let local = s.local_addr().ok();
            Ok((Box::new(s) as BoxedPeerStream, local))
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
            let _ = s.set_nodelay(true);
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

    /// `lsd` ctor upstream — a *separate* socket bound to the
    /// well-known port 6771, joined to the BEP-14 multicast group
    /// on `interface` (the listen address upstream passes
    /// `m_listen_address`; `UNSPECIFIED` lets the OS pick). This
    /// socket sends *and* receives the BT-SEARCH announces — the
    /// source port is what other clients filter on.
    ///
    /// `reuse_address` mirrors upstream so several clients can
    /// share port 6771 on one host.
    ///
    /// # Errors
    /// Propagates socket/bind errors; multicast join failures are
    /// ignored like upstream (`join_group` under an error_code).
    pub fn bind_lsd(interface: std::net::Ipv4Addr) -> std::io::Result<Self> {
        let sock = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;
        sock.set_reuse_address(true)?;
        sock.set_nonblocking(true)?;
        sock.bind(&SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), 6771).into())?;
        let group = std::net::Ipv4Addr::new(239, 192, 152, 143);
        let _ = sock.join_multicast_v4(&group, &interface);
        let _ = sock.set_multicast_loop_v4(true);
        let _ = sock.set_ttl_v4(32);
        let _ = sock.set_multicast_if_v4(&interface);
        Ok(Self {
            sock: UdpSocket::from_std(sock.into())?,
        })
    }
}

impl TokioUdpSocket {
    /// `set_dont_frag` + `udp_socket::do_send` upstream: for a
    /// `DONT_FRAGMENT` send (uTP MTU probes), `IP_MTU_DISCOVER` is
    /// toggled to `PROBE` for the duration of the send and restored
    /// to `DONT` right after — the upstream socket option is likewise
    /// scoped per-send (`TORRENT_HAS_DONT_FRAGMENT` is conditional
    /// upstream; on platforms without a safe sockopt this degrades
    /// to a plain send, matching upstream's no-op path).
    async fn send_with_flags(
        &self,
        buf: &[u8],
        target: SocketAddr,
        flags: crate::transport::DatagramSendFlags,
    ) -> std::io::Result<usize> {
        #[cfg(target_os = "linux")]
        let df_set = flags.contains(crate::transport::DatagramSendFlags::DONT_FRAGMENT)
            && target.is_ipv4()
            && rustix::net::sockopt::set_ip_mtu_discover(
                &self.sock,
                rustix::net::sockopt::Ipv4PathMtuDiscovery::PROBE,
            )
            .is_ok();
        #[cfg(not(target_os = "linux"))]
        let _ = flags;
        let r = self.sock.send_to(buf, target).await;
        #[cfg(target_os = "linux")]
        if df_set {
            let _ = rustix::net::sockopt::set_ip_mtu_discover(
                &self.sock,
                rustix::net::sockopt::Ipv4PathMtuDiscovery::DONT,
            );
        }
        r
    }
}

impl DatagramSocket for TokioUdpSocket {
    fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> BoxFut<'a, usize> {
        Box::pin(async move { self.sock.send_to(buf, target).await })
    }

    fn send_to_flags<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
        flags: crate::transport::DatagramSendFlags,
    ) -> BoxFut<'a, usize> {
        Box::pin(async move { self.send_with_flags(buf, target, flags).await })
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
