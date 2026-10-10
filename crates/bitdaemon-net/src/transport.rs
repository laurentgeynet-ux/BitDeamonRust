//! Injectable transport traits — the seam every UDP/TCP subsystem
//! (uTP, DHT, UDP trackers, LSD, peer connections) is built upon.
//!
//! Defined by ADR-0008. The motivating downstream consumer is OnionBit
//! (anonymous Tribler port), which routes all BitTorrent traffic through
//! IPv8 onion circuits: peer uTP, DHT and UDP trackers traverse
//! `data`-cell datagram sockets — no real UDP bind is allowed in the
//! anonymous lane. The same seam also powers the deterministic
//! simulation harness (ADR-0006, Phase 8).
//!
//! Upstream mapping: `socket_type`/`polymorphic_socket`/`instantiate_connection`
//! (stream side), `udp_socket`/`session_udp_sockets`/`utp_socket_manager`
//! (datagram side). Unlike libtorrent — and unlike rqbit, which needed
//! vendoring + patching for this — transports are traits from day one.

use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

/// Boxed future returned by transport methods. Object-safe equivalent
/// of `async fn` (RPITIT is not object-safe; keep the rqbit-style
/// `Pin<Box<dyn Future>>` shape so trait objects work).
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + Sync + 'a>>;

/// Object-safe datagram transport — the subset of `tokio::net::UdpSocket`
/// used by the DHT, UDP tracker clients, uTP socket manager and LSD.
///
/// Implementors: the default `TokioUdpSocket` (thin wrapper), and any
/// custom transport — e.g. OnionBit's `TunnelUdpSocket` routing
/// datagrams through onion circuits, or a simulation harness socket.
///
/// Equivalent of the `DatagramSocket` trait that had to be patched into
/// vendored `librqbit-dualstack-sockets` (OnionBit ADR-0007).
// TODO(phase-5): add `poll_send_to`-style vectored send if profiling
// shows copy overhead in uTP/DHT hot paths (rqbit's
// `PollSendToVectored` exists for that reason).
pub trait DatagramSocket: Send + Sync + Debug + 'static {
    /// Send a datagram to `target`.
    fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> BoxFut<'a, usize>;

    /// Receive one datagram into `buf`, returning its length and sender.
    fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> BoxFut<'a, (usize, SocketAddr)>;

    /// Local address the transport is bound to (used for logging and
    /// for tracker `port` announces).
    fn local_addr(&self) -> SocketAddr;
}

/// A connected bidirectional peer byte stream (TCP, uTP, SOCKS5-TCP,
/// WebSocket, or a tunnelled equivalent), boxed for trait-object use.
pub trait PeerStream: AsyncRead + AsyncWrite + Unpin + Send + Debug + 'static {}

impl<T> PeerStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + Debug + 'static {}

/// Boxed peer stream returned by [`PeerConnector`] / [`PeerAcceptor`].
pub type BoxedPeerStream = Box<dyn PeerStream>;

/// Outbound peer connector — the injection point for "how do I reach a
/// peer". Default impls: TCP connect, uTP `connect` over a
/// [`DatagramSocket`], SOCKS5. Custom impls route through tunnels.
///
/// Equivalent of the `UtpConnector` trait patched into vendored rqbit.
pub trait PeerConnector: Send + Sync + Debug + 'static {
    /// Connect to `addr`, yielding a peer byte stream.
    fn connect(
        self: Arc<Self>,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>>;

    /// Connect by destination string — `"host:port"`, an I2P base64/
    /// `.b32.i2p` destination, an onion service name… Default parses
    /// `host:port` into [`PeerConnector::connect`]; transports whose
    /// addressing isn't IP-based (I2P/SAM, onion) override this.
    fn connect_dest(
        self: Arc<Self>,
        dest: &str,
    ) -> Pin<Box<dyn Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync + '_>> {
        let parsed = dest.parse::<SocketAddr>();
        Box::pin(async move {
            match parsed {
                Ok(a) => self.connect(a).await,
                Err(e) => Err(crate::NetError::Transport(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("bad destination {dest}: {e}"),
                ))),
            }
        })
    }
}

/// Inbound peer acceptor — symmetric counterpart of [`PeerConnector`]
/// on the listen path (uTP `accept`, TCP listener, tunnel acceptor).
///
/// Equivalent of the `UtpAcceptor` trait patched into vendored rqbit.
pub trait PeerAcceptor: Send + Sync + Debug + 'static {
    /// Accept the next inbound peer stream, with its remote endpoint
    /// when the transport knows it (`None` e.g. for I2P — the SAM
    /// destination isn't an IP:port).
    fn accept(
        self: Arc<Self>,
    ) -> Pin<
        Box<
            dyn Future<Output = crate::Result<(BoxedPeerStream, Option<SocketAddr>)>> + Send + Sync,
        >,
    >;
}
