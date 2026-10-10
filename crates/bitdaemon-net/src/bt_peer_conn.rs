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

/// `write_extensions` — send the BEP-10 extended handshake.
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

/// `write_*` — serialize one message and send it (keep-alive =
/// the zero-length frame).
pub async fn write_message(s: &mut BoxedPeerStream, m: &PeerMessage) -> Result<(), PeerConnError> {
    let frame = encode(m); // includes the 4-byte length prefix
    s.write_all(&frame).await?;
    Ok(())
}
