// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `bt_peer_connection` — the async wire driver. Upstream's class
//! owns the socket; here the stream is a `PeerStream` (ADR-0008 —
//! any tunnel/proxy lane works) and each phase is an async fn the
//! caller composes: `run_handshake` → `extended_handshake` →
//! `read_message`/`write_message` in the caller's pump loop.

use bitdaemon_common::{PeerId, Sha1Hash};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::bep10::ExtendedHandshake;
use crate::mse::{MseError, MseResult, mse_initiator, mse_responder};
use crate::pe_crypto::Rc4Handler;
use crate::peer_wire::{
    Handshake, PeerMessage, WireError, decode, encode, parse_handshake, write_handshake,
};
use crate::transport::BoxedPeerStream;

/// `handshake_len` — 68-byte BT handshake.
pub use crate::peer_wire::HANDSHAKE_LEN;

/// `MAX_MESSAGE_LEN` — the codec's frame cap.
const MAX_MESSAGE_LEN: usize = 4 * 1024 * 1024;

/// Errors from the connection driver (`peer_error` category).
#[derive(Debug, thiserror::Error)]
pub enum PeerConnError {
    /// transport.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// wire codec.
    #[error("wire: {0}")]
    Wire(#[from] WireError),
    /// info_hash mismatch on the remote handshake.
    #[error("info_hash mismatch")]
    WrongInfoHash,
    /// reserved/protocol violation → disconnect.
    #[error("protocol: {0}")]
    Protocol(&'static str),
    /// MSE/PE crypto handshake.
    #[error("mse: {0}")]
    Mse(#[from] MseError),
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
        return Err(PeerConnError::WrongInfoHash);
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
        return Err(PeerConnError::WrongInfoHash);
    }
    let ours = write_handshake(&hs.info_hash, our_peer_id, dht, true, fast, false);
    s.write_all(&ours).await?;
    Ok(hs)
}

/// MSE then BT handshake — initiator side (`write_pe1_2_dhkey` +
/// `write_pe3_sync` → `on_connected`). Returns the negotiated crypto
/// state: `rc4_encrypted` decides whether subsequent traffic must go
/// through the `_rc4` variants (plaintext select = plain wire).
pub async fn run_handshake_encrypted_initiator(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    crypto_provide: u32,
    pad_len: usize,
) -> Result<(Handshake, MseResult), PeerConnError> {
    let mut mse = mse_initiator(s, info_hash, crypto_provide, pad_len).await?;
    let hs = exchange_handshake(s, info_hash, our_peer_id, dht, fast, &mut mse, true).await?;
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
    let mut mse = mse_responder(s, pad_len, find_skey, select).await?;
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

/// symmetric handshake exchange (initiator): write ours, read theirs,
/// validate the info_hash — RC4 iff negotiated.
async fn exchange_handshake(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    our_peer_id: &PeerId,
    dht: bool,
    fast: bool,
    mse: &mut MseResult,
    check_ih: bool,
) -> Result<Handshake, PeerConnError> {
    write_bt_handshake(s, info_hash, our_peer_id, dht, fast, mse).await?;
    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    if mse.rc4_encrypted {
        mse.rc4.decrypt(&mut theirs);
    }
    let hs = parse_handshake(&theirs).map_err(|_| PeerConnError::Protocol("bad handshake"))?;
    if check_ih && hs.info_hash != *info_hash {
        return Err(PeerConnError::WrongInfoHash);
    }
    Ok(hs)
}
pub async fn send_extended_handshake(
    s: &mut BoxedPeerStream,
    hs: &ExtendedHandshake,
) -> Result<(), PeerConnError> {
    s.write_all(&hs.encode()).await?;
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
