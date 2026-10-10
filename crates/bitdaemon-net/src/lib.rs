//! Networking subsystem.
//!
//! Ports the peer wire protocol (`peer_connection`, `bt_peer_connection`,
//! `web_peer_connection`, `web_connection_base`), stream types
//! (`socket_type`, `socks5_stream`, `http_stream`, `ssl_stream`,
//! `i2p_stream`, `utp_stream`, `rtc_stream`, `websocket_stream`),
//! trackers (`http_tracker_connection`, `udp_tracker_connection`,
//! `websocket_tracker_connection`, `tracker_manager`, `announce_entry`),
//! discovery (`upnp`, `natpmp`, `lsd`, `portmap`), crypto
//! (`pe_crypto` — MSE/PE handshake, `rc4`), and HTTP plumbing
//! (`http_connection`, `http_parser`, `parse_url`, `resolver`,
//! `udp_socket`, `enum_net`, `ip_notifier`, `ip_filter`, `ip_voter`).
//!
//! All sockets are tokio-based (ADR-0002) and **all transports are
//! injectable** (ADR-0008): nothing in this crate binds an OS socket
//! directly — UDP subsystems consume `transport::DatagramSocket`, peer
//! connections come from `transport::PeerConnector`/`PeerAcceptor`.
//!
//! Status: **not ported yet** — see `docs/PORTING.md`.

#![forbid(unsafe_code)]

pub mod transport;

/// Networking errors (`boost::system::error_code` usages in
/// `socket.hpp`, `http_connection`, `utp_stream`, trackers…).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NetError {
    /// I/O failure from the underlying transport.
    #[error("transport error: {0}")]
    Transport(#[from] std::io::Error),
    /// The configured transports cannot reach a peer (TCP disabled and
    /// no uTP connector, missing proxy, port 0, …).
    #[error("no usable transport: {0}")]
    NoTransport(&'static str),
}

/// Convenience alias for net operations.
pub type Result<T> = std::result::Result<T, NetError>;

/// Peer wire message codec (`libtorrent::peer_connection` message
/// dispatch + `bt_peer_connection` serialization).
///
/// TODO: implement — length-prefixed framing (4-byte BE len + 1-byte id),
/// messages: choke/unchoke/interested/not-interested/have/bitfield/
/// request/piece/cancel/d-port/suggest/have-all/have-none/reject/
/// allowed-fast + extension protocol (BEP-10) handshake.
/// Shape as a `tokio_util::codec::{Encoder, Decoder}`.
pub struct PeerWireCodec;
