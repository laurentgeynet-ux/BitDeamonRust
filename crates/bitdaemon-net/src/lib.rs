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
pub mod resolver;
/// `aux_/socket_io.hpp` — BE wire helpers + endpoint codec.
pub mod socket_io;
/// `aux_/torrent_peer` — per-peer record + BEP-40 `peer_priority`.
pub mod torrent_peer;
/// `tracker_connection` announce exchanges (HTTP + BEP-15 UDP).
pub mod tracker_client;
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
/// BEP-10 extended handshake build/parse + framing.
pub mod bep10;
/// `bt_peer_connection` — async wire driver over PeerStream.
pub mod bt_peer_conn;
/// `aux_/http_parser` — incremental HTTP parser.
pub mod http_conn;

/// `enum_net` — netmask/route/gateway helpers (enumeration injected).
pub mod enum_net;
/// `http_parser.cpp` — incremental status/header/body/chunked parser.
pub mod http_parser;
/// `aux_/http_stream` — HTTP CONNECT proxy handshake.
pub mod http_stream;
/// `http_tracker_connection` — announce query + response parsing.
pub mod http_tracker;
/// `i2p_stream` — SAM handshake (HELLO/SESSION/STREAM/NAMING).
pub mod i2p;
/// `ip_helpers.cpp` + `hash_address` — address classification.
pub mod ip_helpers;
/// `ip_voter.cpp` — external-IP majority voting.
pub mod ip_voter;
/// `lsd` — BEP-14 BT-SEARCH announce parse/render.
pub mod lsd;
/// MSE handshake (PE1-PE4) — `pe_crypto` on `PeerStream`.
pub mod mse;
/// `natpmp` — NAT-PMP/PCP datagram codec.
pub mod natpmp;
/// `parse_url.cpp` — URL components/split/validation.
pub mod parse_url;
/// `peer_class`/`peer_class_pool`/`peer_class_set`.
pub mod peer_class;
/// `aux_/peer_list` — swarm list: candidates, ranking, weeding.
pub mod peer_list;
pub mod proxy;
/// `socks5_stream` — SOCKS4/5 handshake over any async stream.
pub mod socks;
/// `udp_socket::wrap`/`socks5_unwrap` — SOCKS5-UDP relay framing.
pub mod socks_udp;
/// `udp_tracker_connection` — BEP-15 announce/scrape over `DatagramSocket`.
pub mod udp_tracker;
/// `utp_stream` — AsyncRead/Write bridge + PeerConnector over DatagramSocket.
pub mod utp_stream;

/// `upnp.cpp` SOAP core — port-mapping requests + device parsing.
pub mod upnp;
/// uTP wire layer: `utp_header`, extension walk, conn-id demux.
pub mod utp;
/// `utp_socket_impl` — the uTP reliable-stream state machine.
pub mod utp_socket;
/// `web_peer_connection`/`web_connection_base` — url-seed GETs.
pub mod web_seed;
/// `websocket_stream` — RFC 6455 upgrade + frame codec (wss trackers).
pub mod websocket;
pub mod ws_tracker;
