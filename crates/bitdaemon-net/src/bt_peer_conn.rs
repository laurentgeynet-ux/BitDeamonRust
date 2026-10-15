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
/// handshake, reads and validates theirs (info_hash must match).
/// `dht`/`fast` advertise the reserved bits upstream sets.
pub async fn run_handshake(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
) -> Result<Handshake, PeerConnError> {
    let ours = write_handshake(info_hash, our_peer_id, dht, true, fast, false);
    s.write_all(&ours).await?;

    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    if hs.info_hash != *info_hash {
        return Err(PeerConnError::WrongInfoHash(hs.info_hash));
    }
    Ok(hs)
}

/// plaintext responder (`on_incoming`): read the peer's handshake
/// first — we need its `info_hash` to pick the torrent — then reply
/// with ours. `known` says whether the torrent exists (upstream
/// disconnects on unknown ih). `peer_id` is ours.
pub async fn run_handshake_responder(
    s: &mut BoxedPeerStream,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    known: impl Fn(&Sha1Hash) -> bool,
) -> Result<Handshake, PeerConnError> {
    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    if !known(&hs.info_hash) {
        return Err(PeerConnError::WrongInfoHash(hs.info_hash));
    }
    let ours = write_handshake(&hs.info_hash, our_peer_id, dht, true, fast, false);
    s.write_all(&ours).await?;
    Ok(hs)
}

/// MSE then BT handshake — initiator side (`write_pe1_2_dhkey` +
/// `write_pe3_sync` → `on_connected`). Our handshake rides inside
/// PE3 as the IA (upstream `len(ia) = handshake_len`), so only the
/// peer's handshake is read afterwards. Returns the negotiated
/// crypto state: `rc4_encrypted` decides whether subsequent traffic
/// must go through the `_rc4` variants (plaintext select = plain
/// wire).
pub async fn run_handshake_encrypted_initiator(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    crypto_provide: u32,
    pad_len: usize,
) -> Result<(Handshake, MseResult), PeerConnError> {
    let ours = write_handshake(info_hash, our_peer_id, dht, true, fast, false);
    let mut mse = mse_initiator(s, info_hash, crypto_provide, &ours, pad_len).await?;
    // ours went out as IA — just read theirs
    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    if mse.rc4_encrypted {
        mse.rc4.decrypt(&mut theirs);
    }
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    if hs.info_hash != *info_hash {
        return Err(PeerConnError::WrongInfoHash(hs.info_hash));
    }
    Ok((hs, mse))
}

/// responder side (`on_incoming` → `write_pe4_sync` → standard
/// handshake). `find_skey` maps the PE3 skey candidate to a torrent's
/// info_hash; `select` picks the crypto mode. IA (a handshake
/// piggybacked on PE3) is parsed first when present.
pub async fn run_handshake_encrypted_responder(
    s: &mut BoxedPeerStream,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    pad_len: usize,
    find_skey: impl Fn(&Sha1Hash) -> Option<Sha1Hash>,
    select: impl Fn(u32) -> u32,
) -> Result<(Handshake, MseResult), PeerConnError> {
    run_handshake_encrypted_responder_prefix(
        s,
        our_peer_id,
        dht,
        fast,
        pad_len,
        &[],
        find_skey,
        select,
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
    pad_len: usize,
    dh_prefix: &[u8],
    find_skey: impl Fn(&Sha1Hash) -> Option<Sha1Hash>,
    select: impl Fn(u32) -> u32,
) -> Result<(Handshake, MseResult), PeerConnError> {
    let mut mse = mse_responder_prefix(s, dh_prefix, pad_len, find_skey, select).await?;
    if mse.initial_payload.len() >= HANDSHAKE_LEN {
        let mut buf = [0u8; HANDSHAKE_LEN];
        buf.copy_from_slice(&mse.initial_payload[..HANDSHAKE_LEN]);
        let hs = parse_handshake(&buf).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
        write_bt_handshake(s, &hs.info_hash.clone(), our_peer_id, dht, fast, &mut mse).await?;
        return Ok((hs, mse));
    }
    // upstream resolves the ih via skey; read theirs first, then
    // answer with ours (`write_handshake` after on_incoming).
    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    if mse.rc4_encrypted {
        mse.rc4.decrypt(&mut theirs);
    }
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    write_bt_handshake(s, &hs.info_hash.clone(), our_peer_id, dht, fast, &mut mse).await?;
    Ok((hs, mse))
}

/// `on_receive` — upstream tries the plaintext handshake first and,
/// when the probe bytes aren't `"BitTorrent protocol"`, re-interprets
/// them as the head of an MSE DH key (`bt_peer_connection.cpp`).
/// Returns the handshake, the negotiated RC4 stream state
/// (`Some` iff `crypto_select == RC4`), and whether the connection
/// went through MSE at all (`supports_encryption` for PEX).
///
/// Policy (upstream):
/// * `in_enc_policy == pe_disabled` + MSE probe → `no_incoming_encrypted`
/// * `in_enc_policy == pe_forced` + plaintext → `no_incoming_regular`
#[allow(clippy::too_many_arguments)]
pub async fn run_handshake_responder_auto(
    s: &mut BoxedPeerStream,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    known: impl Fn(&Sha1Hash) -> bool,
    in_policy: EncPolicy,
    find_skey: impl Fn(&Sha1Hash) -> Option<Sha1Hash>,
    select: impl Fn(u32) -> u32,
) -> Result<(Handshake, Option<Rc4Handler>, bool), PeerConnError> {
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
        if !known(&hs.info_hash) {
            return Err(PeerConnError::WrongInfoHash(hs.info_hash));
        }
        let ours = write_handshake(&hs.info_hash, our_peer_id, dht, true, fast, false);
        s.write_all(&ours).await?;
        return Ok((hs, None, false));
    }
    if in_policy == EncPolicy::Disabled {
        return Err(PeerConnError::Protocol("no incoming encrypted"));
    }
    let (hs, mse) = run_handshake_encrypted_responder_prefix(
        s,
        our_peer_id,
        dht,
        fast,
        0,
        &head,
        find_skey,
        select,
    )
    .await?;
    let rc4 = mse.rc4_encrypted.then(|| {
        let MseResult { rc4, .. } = mse;
        rc4
    });
    Ok((hs, rc4, true))
}

/// write one BT handshake, RC4-encrypted iff negotiated.
async fn write_bt_handshake(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    mse: &mut MseResult,
) -> Result<(), PeerConnError> {
    let mut ours = write_handshake(info_hash, our_peer_id, dht, true, fast, false);
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
