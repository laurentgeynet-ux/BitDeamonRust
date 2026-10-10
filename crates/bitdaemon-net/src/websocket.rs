// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `websocket_stream` — RFC 6455 over an injected stream (upstream
//! wraps beast::websocket; the wire format is what matters here —
//! used by `wss://` trackers). Handshake = HTTP Upgrade request +
//! `Sec-WebSocket-Accept` validation; frames = FIN/opcode header,
//! masked client writes, unmasked reads.

use sha1::Digest as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::transport::BoxedPeerStream;

/// RFC 6455 §4.2.2 magic GUID.
const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// frame opcodes (RFC 6455 §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOpcode {
    /// continuation.
    Continuation,
    /// text.
    Text,
    /// binary (trackers send announce replies here).
    Binary,
    /// close.
    Close,
    /// ping.
    Ping,
    /// pong.
    Pong,
}

/// websocket errors.
#[derive(Debug, thiserror::Error)]
pub enum WsError {
    /// transport.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// upgrade refused / bad accept key.
    #[error("handshake failed: {0}")]
    Handshake(&'static str),
    /// frame violated the protocol.
    #[error("protocol: {0}")]
    Protocol(&'static str),
}

/// `Sec-WebSocket-Accept` = b64(sha1(key | GUID)).
#[must_use]
pub fn ws_accept_key(key: &str) -> String {
    let mut h = sha1::Sha1::new();
    h.update(key.as_bytes());
    h.update(WS_GUID);
    base64(&h.finalize())
}

/// tiny base64 (no dep) — RFC 4648 with padding.
fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        out.push(T[(b[0] >> 2) as usize] as char);
        out.push(T[(((b[0] & 3) << 4) | (b[1] >> 4)) as usize] as char);
        out.push(if c.len() > 1 {
            T[(((b[1] & 0xF) << 2) | (b[2] >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            T[(b[2] & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// client → server upgrade request (beast writes the same fields).
/// Returns the key — the caller validates the reply via
/// [`validate_handshake`].
pub async fn ws_upgrade(
    s: &mut BoxedPeerStream,
    host: &str,
    path: &str,
    user_agent: &str,
    key: &str,
) -> Result<(), WsError> {
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: binary\r\nUser-Agent: {user_agent}\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await?;
    Ok(())
}

/// validate the server's `101` reply: status line + `Sec-WebSocket-Accept`
/// header must equal `ws_accept_key(key)`.
pub fn validate_handshake(status: i32, accept: Option<&str>, key: &str) -> Result<(), WsError> {
    if status != 101 {
        return Err(WsError::Handshake("expected 101"));
    }
    match accept {
        Some(a) if a.trim() == ws_accept_key(key) => Ok(()),
        _ => Err(WsError::Handshake("bad Sec-WebSocket-Accept")),
    }
}

/// write one frame — clients MUST mask (RFC 6455 §5.3).
/// `mask_key` = 4 random bytes.
pub async fn ws_write_frame(
    s: &mut BoxedPeerStream,
    opcode: WsOpcode,
    payload: &[u8],
    mask_key: u32,
) -> Result<(), WsError> {
    let op = match opcode {
        WsOpcode::Continuation => 0x0,
        WsOpcode::Text => 0x1,
        WsOpcode::Binary => 0x2,
        WsOpcode::Close => 0x8,
        WsOpcode::Ping => 0x9,
        WsOpcode::Pong => 0xA,
    };
    let mut hdr = Vec::with_capacity(14);
    hdr.push(0x80 | op); // FIN|opcode
    let mask_bit = 0x80;
    match payload.len() {
        n if n < 126 => hdr.push(mask_bit | n as u8),
        n if n <= 0xFFFF => {
            hdr.push(mask_bit | 126);
            hdr.extend((n as u16).to_be_bytes());
        }
        n => {
            hdr.push(mask_bit | 127);
            hdr.extend((n as u64).to_be_bytes());
        }
    }
    hdr.extend(mask_key.to_be_bytes());
    s.write_all(&hdr).await?;
    let mut masked = payload.to_vec();
    for (i, b) in masked.iter_mut().enumerate() {
        *b ^= mask_key.to_be_bytes()[i % 4];
    }
    s.write_all(&masked).await?;
    Ok(())
}

/// read one frame — returns `(opcode, payload)`. Server→client
/// frames are unmasked; masked input is unmasked transparently.
pub async fn ws_read_frame(
    s: &mut BoxedPeerStream,
) -> Result<(WsOpcode, Vec<u8>), WsError> {
    let mut hdr = [0u8; 2];
    s.read_exact(&mut hdr).await?;
    let fin = hdr[0] & 0x80 != 0;
    if !fin {
        return Err(WsError::Protocol("fragmented frames unsupported"));
    }
    let opcode = match hdr[0] & 0x0F {
        0x0 => WsOpcode::Continuation,
        0x1 => WsOpcode::Text,
        0x2 => WsOpcode::Binary,
        0x8 => WsOpcode::Close,
        0x9 => WsOpcode::Ping,
        0xA => WsOpcode::Pong,
        _ => return Err(WsError::Protocol("bad opcode")),
    };
    let masked = hdr[1] & 0x80 != 0;
    let mut len = u64::from(hdr[1] & 0x7F);
    if len == 126 {
        let mut e = [0u8; 2];
        s.read_exact(&mut e).await?;
        len = u64::from(u16::from_be_bytes(e));
    } else if len == 127 {
        let mut e = [0u8; 8];
        s.read_exact(&mut e).await?;
        len = u64::from_be_bytes(e);
    }
    if len > 64 * 1024 * 1024 {
        return Err(WsError::Protocol("frame too large"));
    }
    let mut mask = [0u8; 4];
    if masked {
        s.read_exact(&mut mask).await?;
    }
    let mut payload = vec![0u8; len as usize];
    s.read_exact(&mut payload).await?;
    if masked {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    Ok((opcode, payload))
}
