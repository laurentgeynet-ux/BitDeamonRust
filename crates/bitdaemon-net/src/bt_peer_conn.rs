// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `bt_peer_connection.cpp`/`bt_peer_connection.hpp` (async
//! driver half — the packet codec is `net::peer_wire`, the session-side
//! state machine is `session::peer_conn`).
//!
//! `bt_peer_connection` — the async wire driver. Upstream's class
//! owns the socket; here the stream is a `PeerStream` (ADR-0008 —
//! any tunnel/proxy lane works) and each phase is an async fn the
//! caller composes: `run_handshake` → `extended_handshake` →
//! `read_message`/`write_message` in the caller's pump loop.

use bitdaemon_common::{PeerId, Sha1Hash};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::bep10::ExtendedHandshake;
use crate::mse::{MseError, MseResult, mse_initiator, mse_responder_prefix};
use crate::pe_crypto::{EncPolicy, Rc4Handler};
use crate::peer_wire::{
    Handshake, PeerMessage, WireError, decode, encode, parse_handshake, write_handshake,
};
use crate::transport::BoxedPeerStream;

/// `handshake_len` — 68-byte BT handshake.
pub use crate::peer_wire::HANDSHAKE_LEN;

/// `MAX_MESSAGE_LEN` — upstream `on_receive` disconnects
/// `packet_too_large` when the announced length exceeds 1 MiB
/// (`packet_size > 1024 * 1024 || packet_size < 0`).
const MAX_MESSAGE_LEN: usize = 1024 * 1024;

/// Errors from the connection driver (`peer_error` category).
#[derive(Debug, thiserror::Error)]
pub enum PeerConnError {
    /// transport.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// wire codec.
    #[error("wire: {0}")]
    Wire(#[from] WireError),
    /// info_hash mismatch on the remote handshake — carries the
    /// offending hash (upstream inspects it for the DHT
    /// secret-id-fishing check).
    #[error("info_hash mismatch")]
    WrongInfoHash(Sha1Hash),
    /// reserved/protocol violation → disconnect.
    #[error("protocol: {0}")]
    Protocol(&'static str),
    /// MSE/PE crypto handshake.
    #[error("mse: {0}")]
    Mse(#[from] MseError),
    /// `errors::invalid_info_hash` at *normal* severity — responder
    /// leg only: the peer advertised v2 support (reserved `0x10`) on
    /// a conn to a torrent whose metadata is valid but has no v2
    /// hash (`bt_peer_connection.cpp`).
    #[error("peer claims v2 support on a non-v2 torrent")]
    V2OnV1,
}

/// A torrent resolved through one of its wire info-hashes — upstream
/// `session_impl::m_torrents` indexes both `info_hash.get(V1)` and the
/// truncated `get(V2)` (`find_encrypted_torrent`), and the responder
/// accepts either.
#[derive(Debug, Clone, Copy)]
pub struct KnownTorrent {
    /// the canonical session-map key (v1, or the truncated v2 for a
    /// v2-only torrent).
    pub ih: Sha1Hash,
    /// the wire hash this conn resolved — `associated_info_hash()`:
    /// the truncated v2 when the conn speaks `protocol_v2`. On the
    /// responder leg it is recomputed when the peer's reserved `0x10`
    /// bit flips `protocol_v2` (upstream's post-attach order).
    pub wire_ih: Sha1Hash,
    /// the conn resolved through the v2 hash → `protocol_v2`.
    pub protocol_v2: bool,
    /// `info_hashes().has_v2()` — advertises reserved bit 0x10 on
    /// our handshake when the conn stays v1 (`!protocol_v2 &&
    /// has_v2` upstream).
    pub has_v2: bool,
    /// the truncated v2 hash (`info_hash().get(V2)`) when `has_v2`.
    pub v2_ih: Option<Sha1Hash>,
    /// `t->valid_metadata()` — gates the `0x10`-on-v1-only
    /// disconnect upstream (metadata-less magnets keep the claim
    /// speculative and accept).
    pub valid_metadata: bool,
}

impl KnownTorrent {
    /// The responder-side `recv_buffer[7] & 0x10` processing —
    /// upstream marks `protocol_v2` on the peer entry when the
    /// incoming handshake advertises v2 support; a v1-only torrent
    /// with valid metadata rejects it `invalid_info_hash` (normal).
    /// `wire_ih` is re-derived post-flip (`associated_info_hash` =
    /// `ih.get(has_v2 && protocol_v2 ? V2 : V1)` — a metadata-less
    /// v1-only conn keeps the v1 hash on the wire).
    pub fn apply_v2_bit(&mut self, hs: &Handshake) -> Result<(), PeerConnError> {
        if !hs.supports_v2() {
            return Ok(());
        }
        if self.valid_metadata && !self.has_v2 {
            return Err(PeerConnError::V2OnV1);
        }
        self.protocol_v2 = true;
        if let Some(v2) = self.v2_ih {
            self.wire_ih = v2;
        }
        Ok(())
    }

    /// `read_info_hash`'s `else` branch — the conn is **already
    /// attached** (dialed, or inbound SKEY-matched): upstream still
    /// validates the echoed info-hash
    /// (`bt_peer_connection.cpp:3484-3503`). An echo of the truncated
    /// v2 hash flips `protocol_v2` (and `wire_ih` follows — the reply
    /// hash is the post-flip `associated_info_hash()`); any other
    /// mismatch disconnects `invalid_info_hash` at *failure*.
    pub fn check_echo(&mut self, echo: &Sha1Hash) -> Result<(), PeerConnError> {
        if let Some(v2) = self.v2_ih
            && *echo == v2
        {
            self.protocol_v2 = true;
            self.wire_ih = v2;
            return Ok(());
        }
        if *echo != self.wire_ih || self.wire_ih == Sha1Hash::default() {
            return Err(PeerConnError::WrongInfoHash(*echo));
        }
        Ok(())
    }

    /// A plain v1 attached view — tests and injected streams that
    /// carry no torrent v2 context.
    #[must_use]
    pub fn v1(ih: Sha1Hash) -> Self {
        Self {
            ih,
            wire_ih: ih,
            protocol_v2: false,
            has_v2: false,
            v2_ih: None,
            valid_metadata: true,
        }
    }
}

/// Conn crypto/attach state reached during a handshake — mirrors the
/// `m_encrypted`/`m_rc4_encrypted`/skey-attach fields upstream's
/// `disconnect()` reads when classifying a failure (the
/// `error_encrypted_peers`/`error_rc4_peers` counter legs and the
/// alert's torrent handle).
#[derive(Debug, Default, Clone, Copy)]
pub struct HandshakeState {
    /// `m_encrypted` — the MSE negotiation *completed*
    /// (`init_bt_handshake`/`read_pe_ia`, post PE4); upstream sets it
    /// there, not at MSE entry — earlier failures don't count
    /// `error_encrypted_peers`.
    pub encrypted: bool,
    /// `m_rc4_encrypted` — PE4 negotiated RC4 (`select_crypto`).
    pub rc4_encrypted: bool,
    /// `init_bt_handshake` ran — the encrypted portion of the
    /// handshake completed (PE4 sync done). Upstream re-arms the
    /// swarm entry's `pe_support` *here*, before the inner BT
    /// handshake, so a post-PE4 failure still counts as
    /// MSE-capable.
    pub mse_done: bool,
    /// skey/plaintext-matched torrent — set as soon as the remote
    /// info-hash resolves (post-match failures still alert upstream).
    pub torrent: Option<KnownTorrent>,
}

/// `operation_t` (`peer_connection_interface.hpp`) — the failing
/// operation attached to `disconnect()`. Drives the
/// `op != operation_t::connect` gate on the crypto counter legs and
/// lands in the `peer_error_alert`/`peer_disconnected_alert` payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailOp {
    /// `operation_t::connect` — the transport connect() itself.
    Connect,
    /// `operation_t::encryption` — the MSE/PE phases.
    Encryption,
    /// `operation_t::bittorrent` — the BT handshake phase.
    Bittorrent,
    /// `operation_t::sock_recv`/`sock_write` — transport errors
    /// mid-handshake.
    SockIo,
}

impl FailOp {
    /// upstream `print_operation` spelling for alert payloads.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Encryption => "encryption",
            Self::Bittorrent => "bittorrent",
            Self::SockIo => "sock_recv",
        }
    }
}

/// The full classification of a failed dial or accept — everything
/// upstream's `disconnect(ec, op, sev)` needs for counters and
/// alerts (`peer_connection.cpp:4395+`).
#[derive(Debug, Clone, Copy)]
pub struct DialFail {
    /// `disconnect_severity_t` — picks `peer_error_alert` vs
    /// `peer_disconnected_alert` and `m_failed` (failcount).
    pub severity: DisconnectSeverity,
    /// `operation_t`.
    pub op: FailOp,
    /// upstream `errors::*` name — `disconnect_reason_counter` key.
    pub reason: &'static str,
    /// io kind for transport failures (`connrefused_peers` & co.).
    pub io_kind: Option<std::io::ErrorKind>,
    /// raw errno/WSA code — `ErrorKind` can't split EPERM/EACCES
    /// (`perm_peers` vs `no_access_peers` upstream) or see
    /// ENOBUFS/ECANCELED at all (`buffer_peers`/`aborted_peers`).
    pub os_err: Option<i32>,
    /// `supports_encryption()` at failure (MSE entered).
    pub encrypted: bool,
    /// `rc4_encrypted()` at failure.
    pub rc4: bool,
    /// `init_bt_handshake` ran — PE4 completed; upstream re-arms
    /// `pe_support` at that point, so an MSE-established conn that
    /// fails afterwards retries encrypted.
    pub mse_done: bool,
    /// the socket was uTP (`is_utp(m_socket)`).
    pub utp: bool,
}

impl DialFail {
    /// `peer_connection::connect_failed` — the transport refused the
    /// dial. uTP failures get `normal` severity + the deferred TCP
    /// retry upstream posts; TCP refusals are `failure`.
    #[must_use]
    pub fn connect_failed(kind: std::io::ErrorKind, os_err: Option<i32>, utp: bool) -> Self {
        Self {
            severity: if utp {
                DisconnectSeverity::Normal
            } else {
                DisconnectSeverity::Failure
            },
            op: FailOp::Connect,
            reason: "connect failed",
            io_kind: Some(kind),
            os_err,
            encrypted: false,
            rc4: false,
            mse_done: false,
            utp,
        }
    }
}

impl PeerConnError {
    /// The `(severity, operation_t, errors::* name)` triple upstream's
    /// `disconnect(ec, op, sev)` uses for this handshake-phase failure
    /// — the `disconnect(errors::…)` table in
    /// `bt_peer_connection.cpp`.
    pub fn handshake_fail(&self, _outgoing: bool) -> (DisconnectSeverity, FailOp, &'static str) {
        use DisconnectSeverity as Sev;
        match self {
            Self::Io(_) => (
                Sev::Normal,
                FailOp::SockIo,
                match self {
                    Self::Io(e) => match e.kind() {
                        std::io::ErrorKind::UnexpectedEof => "eof",
                        std::io::ErrorKind::ConnectionReset => "connection reset",
                        std::io::ErrorKind::ConnectionRefused => "connection refused",
                        std::io::ErrorKind::TimedOut => "timed out",
                        _ => "io error",
                    },
                    _ => "io error",
                },
            ),
            Self::Wire(_) => (Sev::PeerError, FailOp::Bittorrent, "invalid_message"),
            // upstream `invalid_info_hash` — responder attach
            // (`attach_to_torrent`) and the attached-conn echo check
            // are both `failure`.
            Self::WrongInfoHash(_) => (Sev::Failure, FailOp::Bittorrent, "invalid_info_hash"),
            // 0x10 claim on a v1-only torrent — `normal`.
            Self::V2OnV1 => (Sev::Normal, FailOp::Bittorrent, "invalid_info_hash"),
            Self::Protocol(reason) => match *reason {
                "no incoming encrypted" => {
                    (Sev::Normal, FailOp::Bittorrent, "no_incoming_encrypted")
                }
                "no incoming regular" => (Sev::Normal, FailOp::Bittorrent, "no_incoming_regular"),
                // the peer answered a dial with a non-BT stream —
                // upstream's `invalid_info_hash`/`failure` leg
                // (`m_encrypted || is_outgoing()` on the probe).
                "bad handshake" => (Sev::Failure, FailOp::Bittorrent, "invalid_info_hash"),
                // the handshake deadline — upstream disconnects
                // `timed_out_no_handshake` at default (normal)
                // severity and bumps `connect_timeouts`.
                "handshake timeout" => (Sev::Normal, FailOp::Bittorrent, "timed_out_no_handshake"),
                "message too large" => (Sev::PeerError, FailOp::Bittorrent, "packet_too_large"),
                other => (Sev::PeerError, FailOp::Bittorrent, other),
            },
            Self::Mse(e) => e.handshake_fail(),
        }
    }

    /// compose [`Self::handshake_fail`] + the conn state into a
    /// [`DialFail`] for the session's `disconnect()` accounting.
    pub fn dial_fail(&self, outgoing: bool, st: &HandshakeState, utp: bool) -> DialFail {
        let (severity, op, reason) = self.handshake_fail(outgoing);
        DialFail {
            severity,
            op,
            reason,
            io_kind: match self {
                Self::Io(e) | Self::Mse(MseError::Io(e)) => Some(e.kind()),
                _ => None,
            },
            os_err: match self {
                Self::Io(e) | Self::Mse(MseError::Io(e)) => e.raw_os_error(),
                _ => None,
            },
            encrypted: st.encrypted,
            rc4: st.rc4_encrypted,
            mse_done: st.mse_done,
            utp,
        }
    }
}

impl MseError {
    /// `disconnect(errors::…, operation_t::encryption|bittorrent, sev)`
    /// for an MSE failure — bt_peer_connection.cpp's PE1–PE4 sites.
    pub fn handshake_fail(&self) -> (DisconnectSeverity, FailOp, &'static str) {
        use DisconnectSeverity as Sev;
        match self {
            Self::Io(_) => (Sev::Normal, FailOp::SockIo, "eof"),
            // `compute_secret` degenerate key — 2841.
            Self::DhFailed => (
                Sev::PeerError,
                FailOp::Encryption,
                "invalid_encrypt_handshake",
            ),
            Self::BadSync => (Sev::Failure, FailOp::Encryption, "sync_hash_not_found"),
            Self::BadVc => (
                Sev::PeerError,
                FailOp::Encryption,
                "invalid_encryption_constant",
            ),
            // skey resolved to no torrent — 3025.
            Self::UnknownInfoHash => (Sev::Failure, FailOp::Bittorrent, "invalid_info_hash"),
            // initiator: masked `crypto_select == 0` — 3194.
            Self::UnsupportedEncryptionModeSelected => (
                Sev::PeerError,
                FailOp::Encryption,
                "unsupported_encryption_mode_selected",
            ),
            // responder: no common level — 3178.
            Self::UnsupportedEncryptionMode => (
                Sev::Failure,
                FailOp::Encryption,
                "unsupported_encryption_mode",
            ),
            Self::InvalidPadSize => (Sev::PeerError, FailOp::Encryption, "invalid_pad_size"),
            Self::InvalidEncryptHandshake => (
                Sev::PeerError,
                FailOp::Encryption,
                "invalid_encrypt_handshake",
            ),
        }
    }
}

/// `disconnect_severity_t` (`peer_connection_interface.hpp`) — the
/// severity the initiator attached to `disconnect()`. `Normal` and
/// `Failure` post `peer_disconnected_alert`; `PeerError` posts
/// `peer_error_alert` + `error_peers` instead. Anything `> Normal`
/// stamps `m_failed` (the swarm entry's `failcount` grows).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DisconnectSeverity {
    /// `peer_connection_interface::normal` (0) — clean close.
    Normal,
    /// `failure` (1) — counted as failed, still a
    /// `peer_disconnected_alert`.
    Failure,
    /// `peer_error` (2) — protocol violation, `peer_error_alert`.
    PeerError,
}

/// Why a peer's pump task ended — carried on the engine's `gone`
/// channel so the session can apply upstream `disconnect()` severity
/// semantics (`peer_error_alert` + `error_peers` for protocol
/// violations vs a plain `peer_disconnected_alert` for transport
/// closes).
#[derive(Debug)]
pub enum PeerGone {
    /// Session-initiated `peer_connection::disconnect` — `severity`
    /// and `reason` mirror the upstream `disconnect(ec, op, sev)`
    /// args (`reason` is the error name, e.g. `"invalid have index"`).
    Disconnected {
        /// `disconnect_severity_t`.
        severity: DisconnectSeverity,
        /// Upstream error name.
        reason: &'static str,
    },
    /// Transport/codec failure inside the read pump (`on_receive`
    /// error path): socket EOF/reset or a malformed frame.
    Transport(PeerConnError),
}

/// `bt_peer_connection::on_connected` + handshake read — sends our
/// handshake, reads theirs. `dht`/`fast` advertise the reserved bits
/// upstream sets; reserved byte 7 bit `0x10` goes out when
/// `kt.has_v2 && !kt.protocol_v2` (we accept v2 conns on this
/// torrent). The conn is already attached upstream, so the echoed
/// info-hash is re-validated (`read_info_hash`'s `else` branch): a
/// v2 echo upgrades `kt.protocol_v2`, a wrong hash disconnects
/// `invalid_info_hash` at *failure*.
pub async fn run_handshake(
    s: &mut BoxedPeerStream,
    kt: &mut KnownTorrent,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
) -> Result<Handshake, PeerConnError> {
    let advertise_v2 = kt.has_v2 && !kt.protocol_v2;
    let ours = write_handshake(&kt.wire_ih, our_peer_id, dht, true, fast, advertise_v2);
    s.write_all(&ours).await?;

    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    kt.check_echo(&hs.info_hash)?;
    Ok(hs)
}

/// plaintext responder (`on_incoming`): read the peer's handshake
/// first — we need its `info_hash` to pick the torrent — then reply
/// with ours. `known` resolves the wire hash to a torrent (upstream
/// disconnects on unknown ih); the returned [`KnownTorrent`] carries
/// the conn's `protocol_v2` (v2 hash received) and `has_v2` (our
/// reply advertises `0x10` when the conn stays v1). `st.torrent`
/// records the match so a post-match failure alerts with the
/// attached torrent handle upstream would have (`disconnect()`).
pub async fn run_handshake_responder(
    s: &mut BoxedPeerStream,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    known: impl Fn(&Sha1Hash) -> Option<KnownTorrent>,
    st: &mut HandshakeState,
) -> Result<(Handshake, KnownTorrent), PeerConnError> {
    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    let Some(mut kt) = known(&hs.info_hash) else {
        return Err(PeerConnError::WrongInfoHash(hs.info_hash));
    };
    // the conn is attached before the v2-bit check runs upstream —
    // a `V2OnV1` disconnect still carries the torrent handle.
    st.torrent = Some(kt);
    // upstream order: a received 0x10 flips `protocol_v2` *before*
    // `write_handshake` — the reply advertises `!protocol_v2 &&
    // has_v2` and carries the post-flip `associated_info_hash`.
    kt.apply_v2_bit(&hs)?;
    st.torrent = Some(kt);
    let advertise_v2 = kt.has_v2 && !kt.protocol_v2;
    let ours = write_handshake(&kt.wire_ih, our_peer_id, dht, true, fast, advertise_v2);
    s.write_all(&ours).await?;
    Ok((hs, kt))
}

/// MSE then BT handshake — initiator side (`write_pe1_2_dhkey` +
/// `write_pe3_sync` → `on_connected`). Our handshake rides inside
/// PE3 as the IA (upstream `len(ia) = handshake_len`), so only the
/// peer's handshake is read afterwards. `allowed_enc_level` is the
/// sanitized settings mask — emitted as `crypto_provide` and ANDed
/// into the peer's `crypto_select` (upstream `crypto_field &=
/// allowed_enc_level`). `st` tracks the conn crypto flags the
/// `disconnect()` classification reads on failure (`m_encrypted`
/// sets when the negotiation completes, `m_rc4_encrypted` after
/// PE4). The echoed info-hash is re-validated like a plain
/// attached conn (`read_info_hash`'s `else` branch).
#[allow(clippy::too_many_arguments)]
pub async fn run_handshake_encrypted_initiator(
    s: &mut BoxedPeerStream,
    kt: &mut KnownTorrent,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    allowed_enc_level: u8,
    pad: Option<usize>,
    st: &mut HandshakeState,
) -> Result<(Handshake, MseResult), PeerConnError> {
    let advertise_v2 = kt.has_v2 && !kt.protocol_v2;
    let ours = write_handshake(&kt.wire_ih, our_peer_id, dht, true, fast, advertise_v2);
    let mut mse = mse_initiator(s, &kt.wire_ih, allowed_enc_level, &ours, pad).await?;
    // `init_bt_handshake` — `m_encrypted`/`m_rc4_encrypted` set now
    // that the crypto negotiation completed.
    st.encrypted = true;
    st.mse_done = true;
    st.rc4_encrypted = mse.rc4_encrypted;
    // ours went out as IA — just read theirs
    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    if mse.rc4_encrypted {
        mse.rc4.decrypt(&mut theirs);
    }
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    kt.check_echo(&hs.info_hash)?;
    Ok((hs, mse))
}

/// responder side (`on_incoming` → `write_pe4_sync` → standard
/// handshake). `find_skey` maps the PE3 skey candidate to a torrent
/// (upstream probes `m_torrents` over v1 *and* truncated v2);
/// `select` picks the crypto mode. IA (a handshake piggybacked on
/// PE3) is re-injected as the head of the peer's handshake — a
/// partial IA is legal (`cut(0, 20)` upstream).
#[allow(clippy::too_many_arguments)]
pub async fn run_handshake_encrypted_responder(
    s: &mut BoxedPeerStream,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    pad: Option<usize>,
    find_skey: impl FnMut(&Sha1Hash) -> Option<KnownTorrent>,
    select: impl Fn(u32) -> u32,
    st: &mut HandshakeState,
) -> Result<(Handshake, MseResult), PeerConnError> {
    run_handshake_encrypted_responder_prefix(
        s,
        our_peer_id,
        dht,
        fast,
        pad,
        &[],
        find_skey,
        select,
        st,
    )
    .await
}

/// `run_handshake_encrypted_responder` with the head of the remote
/// DH key already consumed by the plaintext probe (`dh_prefix`,
/// ≤ [`DH_KEY_LEN`](crate::pe_crypto::DH_KEY_LEN) bytes).
#[allow(clippy::too_many_arguments)]
pub async fn run_handshake_encrypted_responder_prefix(
    s: &mut BoxedPeerStream,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    pad: Option<usize>,
    dh_prefix: &[u8],
    find_skey: impl FnMut(&Sha1Hash) -> Option<KnownTorrent>,
    select: impl Fn(u32) -> u32,
    st: &mut HandshakeState,
) -> Result<(Handshake, MseResult), PeerConnError> {
    let (mut mse, mut kt) = mse_responder_prefix(s, dh_prefix, pad, find_skey, select).await?;
    // `init_bt_handshake` — `m_encrypted`/`m_rc4_encrypted` only set
    // once the crypto negotiation completed (PE4 written, IA read).
    st.encrypted = true;
    st.mse_done = true;
    st.torrent = Some(kt);
    st.rc4_encrypted = mse.rc4_encrypted;
    // upstream `cut(0, handshake_len)` re-injects the IA as the head
    // of the peer's BT handshake — a partial IA (0 < len < 68) is
    // legal: the bytes already decrypted inside the cryptofield form
    // the prefix, the remainder is read live (and RC4'd if
    // negotiated — IA sits *inside* the cipher stream).
    let ia = mse.initial_payload.len().min(HANDSHAKE_LEN);
    let mut theirs = [0u8; HANDSHAKE_LEN];
    theirs[..ia].copy_from_slice(&mse.initial_payload[..ia]);
    if ia < HANDSHAKE_LEN {
        s.read_exact(&mut theirs[ia..]).await?;
    }
    if mse.rc4_encrypted {
        mse.rc4.decrypt(&mut theirs[ia..]);
    }
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    // attached-conn `read_info_hash` `else` branch — the skey pinned
    // the torrent, but the echoed ih is still validated: a v2 echo
    // flips `protocol_v2` (and `wire_ih` — the reply then carries the
    // v2 hash, upstream's post-flip `associated_info_hash()`), any
    // other mismatch disconnects `invalid_info_hash` at *failure*.
    kt.check_echo(&hs.info_hash)?;
    st.torrent = Some(kt);
    let advertise_v2 = kt.has_v2 && !kt.protocol_v2;
    write_bt_handshake(
        s,
        &kt.wire_ih,
        our_peer_id,
        dht,
        fast,
        advertise_v2,
        &mut mse,
    )
    .await?;
    Ok((hs, mse))
}

/// `on_receive` — upstream tries the plaintext handshake first and,
/// when the probe bytes aren't `"BitTorrent protocol"`, re-interprets
/// them as the head of an MSE DH key (`bt_peer_connection.cpp`).
/// `st` accumulates the conn crypto flags (MSE entered, RC4
/// negotiated, torrent matched) so the caller can classify a failure
/// exactly like upstream `disconnect()` does. On success the
/// [`KnownTorrent`] the conn attached to is in `st.torrent` — its
/// `ih` is the canonical map key, `wire_ih`/`protocol_v2` describe
/// the conn.
///
/// Policy (upstream):
/// * `in_enc_policy == pe_disabled` + MSE probe → `no_incoming_encrypted`
/// * `in_enc_policy == pe_forced` + plaintext → `no_incoming_regular`
///
/// Returns the handshake plus the negotiated RC4 stream state
/// (`Some` iff `crypto_select == RC4`).
#[allow(clippy::too_many_arguments)]
pub async fn run_handshake_responder_auto(
    s: &mut BoxedPeerStream,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    known: impl Fn(&Sha1Hash) -> Option<KnownTorrent>,
    in_policy: EncPolicy,
    find_skey: impl FnMut(&Sha1Hash) -> Option<KnownTorrent>,
    select: impl Fn(u32) -> u32,
    st: &mut HandshakeState,
) -> Result<(Handshake, Option<Rc4Handler>), PeerConnError> {
    // upstream's first receive buffer is 20 bytes (pstrlen+pstr);
    // anything else means the stream head is a DH key, not a
    // handshake — cut(0) re-interprets it as MSE.
    let mut head = [0u8; 20];
    s.read_exact(&mut head).await?;
    if head[0] == 19 && head[1..] == *b"BitTorrent protocol" {
        if in_policy == EncPolicy::Forced {
            return Err(PeerConnError::Protocol("no incoming regular"));
        }
        let mut theirs = [0u8; HANDSHAKE_LEN];
        theirs[..20].copy_from_slice(&head);
        s.read_exact(&mut theirs[20..]).await?;
        let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
        let Some(mut kt) = known(&hs.info_hash) else {
            return Err(PeerConnError::WrongInfoHash(hs.info_hash));
        };
        // same ordering as `run_handshake_responder`: the conn is
        // attached before the received 0x10 bit flips `protocol_v2`
        // (a `V2OnV1` disconnect still carries the handle).
        st.torrent = Some(kt);
        kt.apply_v2_bit(&hs)?;
        let advertise_v2 = kt.has_v2 && !kt.protocol_v2;
        let ours = write_handshake(&kt.wire_ih, our_peer_id, dht, true, fast, advertise_v2);
        s.write_all(&ours).await?;
        return Ok((hs, None));
    }
    if in_policy == EncPolicy::Disabled {
        return Err(PeerConnError::Protocol("no incoming encrypted"));
    }
    let (hs, mse) = run_handshake_encrypted_responder_prefix(
        s,
        our_peer_id,
        dht,
        fast,
        None,
        &head,
        find_skey,
        select,
        st,
    )
    .await?;
    let rc4 = mse.rc4_encrypted.then(|| {
        let MseResult { rc4, .. } = mse;
        rc4
    });
    Ok((hs, rc4))
}

/// write one BT handshake, RC4-encrypted iff negotiated.
async fn write_bt_handshake(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    advertise_v2: bool,
    mse: &mut MseResult,
) -> Result<(), PeerConnError> {
    let mut ours = write_handshake(info_hash, our_peer_id, dht, true, fast, advertise_v2);
    if mse.rc4_encrypted {
        mse.rc4.encrypt(&mut ours);
    }
    s.write_all(&ours).await?;
    Ok(())
}

/// `send_extended_handshake` — id 0 extended message, RC4'd iff
/// negotiated.
pub async fn send_extended_handshake(
    s: &mut BoxedPeerStream,
    hs: &ExtendedHandshake,
    rc4: Option<&mut Rc4Handler>,
) -> Result<(), PeerConnError> {
    let mut frame = hs.encode();
    if let Some(r) = rc4 {
        r.encrypt(&mut frame);
    }
    s.write_all(&frame).await?;
    Ok(())
}

/// `on_extended_handshake` — read one message, requiring it to be
/// the extended handshake (id 0). Returns the parsed offer.
pub async fn read_extended_handshake(
    s: &mut BoxedPeerStream,
) -> Result<ExtendedHandshake, PeerConnError> {
    match read_message(s).await? {
        PeerMessage::Extended { id: 0, data } => {
            ExtendedHandshake::parse(&data).ok_or(PeerConnError::Protocol("bad ext handshake"))
        }
        _ => Err(PeerConnError::Protocol("expected extended handshake")),
    }
}

/// read one length-prefixed wire message (4-byte BE len → payload;
/// 0 = keep-alive).
pub async fn read_message(s: &mut BoxedPeerStream) -> Result<PeerMessage, PeerConnError> {
    let mut len = [0u8; 4];
    s.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n == 0 {
        return Ok(PeerMessage::Keepalive);
    }
    if n > MAX_MESSAGE_LEN {
        return Err(PeerConnError::Protocol("message too large"));
    }
    let mut buf = vec![0u8; n];
    s.read_exact(&mut buf).await?;
    Ok(decode(&buf)?)
}

/// Cancellation-safe incremental frame decoder: pop one complete
/// length-prefixed message from `buf`, or `Ok(None)` when more
/// bytes are needed. Unlike `read_message` this can sit inside a
/// `select!` loop — no bytes are consumed on partial frames.
pub fn decode_next(buf: &mut bytes::BytesMut) -> Result<Option<PeerMessage>, PeerConnError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let n = u32::from_be_bytes(buf[..4].try_into().unwrap_or_default()) as usize;
    if n > MAX_MESSAGE_LEN {
        return Err(PeerConnError::Protocol("message too large"));
    }
    if buf.len() < 4 + n {
        return Ok(None);
    }
    let _hdr = buf.split_to(4);
    let body = buf.split_to(n);
    Ok(Some(decode(&body)?))
}

/// `write_*` — serialize one message and send it (keep-alive =
/// the zero-length frame).
pub async fn write_message(s: &mut BoxedPeerStream, m: &PeerMessage) -> Result<(), PeerConnError> {
    let frame = encode(m); // includes the 4-byte length prefix
    s.write_all(&frame).await?;
    Ok(())
}

/// `peer_connection::send_buffer` + `append_to_send_buffer` — chain
/// the encoded frames into a `ChainedBuffer` and flush them with one
/// vectored write per round (upstream batches whatever is queued
/// under `send_buffer_watermark`; here the caller decides the batch).
/// Non-vectored streams fall back to sequential `write_all`.
pub async fn write_messages(
    s: &mut BoxedPeerStream,
    msgs: &[PeerMessage],
) -> Result<(), PeerConnError> {
    if msgs.len() == 1 {
        return write_message(s, &msgs[0]).await;
    }
    let mut chain = crate::chained_buffer::ChainedBuffer::default();
    for m in msgs {
        let frame = encode(m);
        let frame = frame.to_vec();
        let used = frame.len();
        chain.append_buffer(frame, used);
    }
    while !chain.is_empty() {
        let iovecs = chain.build_iovec(usize::MAX);
        if s.is_write_vectored() {
            let ios: Vec<std::io::IoSlice<'_>> =
                iovecs.iter().map(|b| std::io::IoSlice::new(b)).collect();
            let n = s.write_vectored(&ios).await?;
            if n == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
            }
            drop(ios);
            chain.pop_front(n);
        } else {
            for b in &iovecs {
                s.write_all(b).await?;
            }
            let n: usize = iovecs.iter().map(|b| b.len()).sum();
            drop(iovecs);
            chain.pop_front(n);
        }
    }
    Ok(())
}

/// `write_messages` + `rc4_encrypt` — post-MSE batched write: the
/// whole batch is encoded contiguously, encrypted once, then sent
/// with a single write (upstream `rc4` wraps the chained buffer).
pub async fn write_messages_rc4(
    s: &mut BoxedPeerStream,
    rc4: &mut Rc4Handler,
    msgs: &[PeerMessage],
) -> Result<(), PeerConnError> {
    let mut frame = Vec::new();
    for m in msgs {
        frame.extend_from_slice(&encode(m));
    }
    rc4.encrypt(&mut frame);
    s.write_all(&frame).await?;
    Ok(())
}

/// `read_message` + `rc4_decrypt` — post-MSE message read.
pub async fn read_message_rc4(
    s: &mut BoxedPeerStream,
    rc4: &mut Rc4Handler,
) -> Result<PeerMessage, PeerConnError> {
    let mut len = [0u8; 4];
    s.read_exact(&mut len).await?;
    rc4.decrypt(&mut len);
    let n = u32::from_be_bytes(len) as usize;
    if n == 0 {
        return Ok(PeerMessage::Keepalive);
    }
    if n > MAX_MESSAGE_LEN {
        return Err(PeerConnError::Protocol("message too large"));
    }
    let mut buf = vec![0u8; n];
    s.read_exact(&mut buf).await?;
    rc4.decrypt(&mut buf);
    Ok(decode(&buf)?)
}

/// `write_message` + `rc4_encrypt`.
pub async fn write_message_rc4(
    s: &mut BoxedPeerStream,
    rc4: &mut Rc4Handler,
    m: &PeerMessage,
) -> Result<(), PeerConnError> {
    let mut frame = encode(m).to_vec();
    rc4.encrypt(&mut frame);
    s.write_all(&frame).await?;
    Ok(())
}
