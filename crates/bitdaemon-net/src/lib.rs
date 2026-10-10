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

/// `aux_/bandwidth_*` — token-bucket rate limiter + request queue.
pub mod bandwidth;
/// `aux_/chained_buffer` — heterogeneous send-buffer chain.
pub mod chained_buffer;
/// `ip_filter.hpp` — IPv4/IPv6 + port range access rules.
pub mod ip_filter;
/// `aux_/packet_buffer` — 16-bit-wrapping circular packet store (uTP).
pub mod packet_buffer;
/// `pe_crypto` — MSE/PE: DH-768 key exchange + RC4 (`libTomCrypt`).
pub mod pe_crypto;
/// Peer wire framing + message codec (`bt_peer_connection` packets).
pub mod peer_wire;
/// `aux_/receive_buffer` — cursor-based recv buffer + crypto wrapper.
pub mod receive_buffer;
/// `aux_/socket_io.hpp` — BE wire helpers + endpoint codec.
pub mod socket_io;
/// `aux_/torrent_peer` — per-peer record + BEP-40 `peer_priority`.
pub mod torrent_peer;
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
/// `aux_/http_stream` — HTTP CONNECT proxy handshake.
pub mod http_stream;
/// `socks5_stream` — SOCKS4/5 handshake over any async stream.
pub mod socks;
