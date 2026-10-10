// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `i2p_stream` — the SAM (Simple Anonymous Messaging) handshake,
//! verbatim command lines and response grammar from
//! `i2p_stream.hpp`, driven over any injected [`BoxedPeerStream`].
//!
//! Flow (`cmd` sequence): `HELLO VERSION` → `SESSION CREATE` /
//! `STREAM CONNECT` / `STREAM ACCEPT` / `NAMING LOOKUP` → the
//! stream then carries raw peer data (like SOCKS-established).

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::transport::BoxedPeerStream;

/// `i2p_error_code` — SAM `RESULT=` values mapped verbatim.
#[derive(Debug, thiserror::Error)]
pub enum I2pError {
    /// transport.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// response didn't match the expected tokens / `parse_failed`.
    #[error("parse failed")]
    ParseFailed,
    /// `RESULT=CANT_REACH_PEER`.
    #[error("cant reach peer")]
    CantReachPeer,
    /// `RESULT=I2P_ERROR`.
    #[error("i2p error")]
    I2p,
    /// `RESULT=INVALID_KEY`.
    #[error("invalid key")]
    InvalidKey,
    /// `RESULT=INVALID_ID`.
    #[error("invalid id")]
    InvalidId,
    /// `RESULT=TIMEOUT`.
    #[error("timeout")]
    Timeout,
    /// `RESULT=KEY_NOT_FOUND`.
    #[error("key not found")]
    KeyNotFound,
    /// `RESULT=DUPLICATED_ID`.
    #[error("duplicated id")]
    DuplicatedId,
    /// unknown `RESULT` (`num_errors` upstream).
    #[error("unknown error")]
    Unknown,
}

/// `i2p_session_options` — SESSION CREATE tunnel params.
#[derive(Debug, Clone, Copy)]
pub struct I2pSessionOptions {
    /// `inbound.quantity`.
    pub inbound_quantity: i32,
    /// `outbound.quantity`.
    pub outbound_quantity: i32,
    /// `inbound.length`.
    pub inbound_length: i32,
    /// `outbound.length`.
    pub outbound_length: i32,
    /// `inbound.lengthVariance`.
    pub inbound_length_variance: i32,
    /// `outbound.lengthVariance`.
    pub outbound_length_variance: i32,
}

impl Default for I2pSessionOptions {
    fn default() -> Self {
        Self {
            inbound_quantity: 3,
            outbound_quantity: 3,
            inbound_length: 3,
            outbound_length: 3,
            inbound_length_variance: 0,
            outbound_length_variance: 0,
        }
    }
}

/// the first token pair a response must start with (`expect1`,
/// `expect2` upstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// `HELLO REPLY`.
    Hello,
    /// `STREAM STATUS`.
    Stream,
    /// `SESSION STATUS`.
    Session,
    /// `NAMING REPLY`.
    Naming,
}

impl Expect {
    fn tokens(self) -> (&'static str, &'static str) {
        match self {
            Self::Hello => ("HELLO", "REPLY"),
            Self::Stream => ("STREAM", "STATUS"),
            Self::Session => ("SESSION", "STATUS"),
            Self::Naming => ("NAMING", "REPLY"),
        }
    }
}

/// parsed SAM response — `RESULT` (→ error when != OK) plus the
/// extracted `VALUE`/`DESTINATION` fields.
struct SamReply {
    /// `RESULT` → `I2pError` (or `None` for OK).
    result: Option<I2pError>,
    /// `VALUE=` (NAMING) or `DESTINATION=` (SESSION/ACCEPT).
    destination: String,
}

/// `read_line` — read up to `\n` (byte-at-a-time upstream; buffered
/// until newline here — same semantics, fewer syscalls).
async fn read_line(s: &mut BoxedPeerStream) -> Result<String, I2pError> {
    let mut line = Vec::with_capacity(512);
    loop {
        let mut b = [0u8; 1];
        s.read_exact(&mut b).await?;
        if b[0] == b'\n' {
            break;
        }
        line.push(b[0]);
        if line.len() > 4096 {
            return Err(I2pError::ParseFailed);
        }
    }
    String::from_utf8(line).map_err(|_| I2pError::ParseFailed)
}

/// `read_line` tail — token check + `key=value` loop (quoted values
/// supported, verbatim upstream).
fn parse_reply(line: &str, expect: Expect) -> Result<SamReply, I2pError> {
    let (t1, t2) = expect.tokens();
    let mut parts = line.splitn(3, ' ');
    let first = parts.next().unwrap_or("");
    let second = parts.next().unwrap_or("");
    if first != t1 || second != t2 {
        return Err(I2pError::ParseFailed);
    }
    let mut reply = SamReply {
        result: None,
        destination: String::new(),
    };
    let mut remaining = parts.next().unwrap_or("");
    while !remaining.is_empty() {
        let eq = match remaining.find('=') {
            Some(e) => e,
            None => break,
        };
        let name = &remaining[..eq];
        remaining = &remaining[eq + 1..];
        let value;
        if let Some(stripped) = remaining.strip_prefix('"') {
            let end = stripped.find('"').unwrap_or(stripped.len());
            value = &stripped[..end];
            remaining = stripped.get(end + 1..).unwrap_or("");
        } else {
            let end = remaining.find(' ').unwrap_or(remaining.len());
            value = &remaining[..end];
            remaining = &remaining[end.min(remaining.len())..];
        }
        remaining = remaining.trim_start_matches(' ');
        match name {
            "RESULT" => {
                reply.result = match value {
                    "OK" => None,
                    "CANT_REACH_PEER" => Some(I2pError::CantReachPeer),
                    "I2P_ERROR" => Some(I2pError::I2p),
                    "INVALID_KEY" => Some(I2pError::InvalidKey),
                    "INVALID_ID" => Some(I2pError::InvalidId),
                    "TIMEOUT" => Some(I2pError::Timeout),
                    "KEY_NOT_FOUND" => Some(I2pError::KeyNotFound),
                    "DUPLICATED_ID" => Some(I2pError::DuplicatedId),
                    _ => Some(I2pError::Unknown),
                };
            }
            "VALUE" | "DESTINATION" => reply.destination = value.to_string(),
            _ => {}
        }
    }
    Ok(reply)
}

/// `connected` → HELLO exchange.
async fn sam_hello(s: &mut BoxedPeerStream) -> Result<(), I2pError> {
    s.write_all(b"HELLO VERSION MIN=3.1 MAX=3.1\n").await?;
    let line = read_line(s).await?;
    let r = parse_reply(&line, Expect::Hello)?;
    match r.result {
        None => Ok(()),
        Some(e) => Err(e),
    }
}

/// `send_session_create` — verbatim command line.
pub async fn i2p_session_create(
    s: &mut BoxedPeerStream,
    session_id: &str,
    opts: &I2pSessionOptions,
) -> Result<String, I2pError> {
    sam_hello(s).await?;
    s.write_all(
        format!(
            "SESSION CREATE STYLE=STREAM ID={session_id} DESTINATION=TRANSIENT SIGNATURE_TYPE=7 i2cp.leaseSetEncType=4,0 inbound.quantity={iq} outbound.quantity={oq} inbound.length={il} outbound.length={ol} inbound.lengthVariance={ilv} outbound.lengthVariance={olv}\n",
            iq = opts.inbound_quantity,
            oq = opts.outbound_quantity,
            il = opts.inbound_length,
            ol = opts.outbound_length,
            ilv = opts.inbound_length_variance,
            olv = opts.outbound_length_variance,
        )
        .as_bytes(),
    )
    .await?;
    let r = parse_reply(&read_line(s).await?, Expect::Session)?;
    match r.result {
        None => Ok(r.destination),
        Some(e) => Err(e),
    }
}

/// `send_connect` — `STREAM CONNECT` to `destination`, returning
/// the remote's reported destination on success.
pub async fn i2p_connect(
    s: &mut BoxedPeerStream,
    session_id: &str,
    destination: &str,
) -> Result<String, I2pError> {
    sam_hello(s).await?;
    s.write_all(format!("STREAM CONNECT ID={session_id} DESTINATION={destination}\n").as_bytes())
        .await?;
    let r = parse_reply(&read_line(s).await?, Expect::Stream)?;
    match r.result {
        None => Ok(r.destination),
        Some(e) => Err(e),
    }
}

/// `send_accept` — `STREAM ACCEPT`; after `STREAM STATUS` the
/// bridge sends the peer's destination on a second line
/// (`cmd_incoming` upstream).
pub async fn i2p_accept(s: &mut BoxedPeerStream, session_id: &str) -> Result<String, I2pError> {
    sam_hello(s).await?;
    s.write_all(format!("STREAM ACCEPT ID={session_id}\n").as_bytes())
        .await?;
    let r = parse_reply(&read_line(s).await?, Expect::Stream)?;
    if let Some(e) = r.result {
        return Err(e);
    }
    read_line(s).await
}

/// `send_name_lookup` — `NAMING LOOKUP` → the `.b32.i2p`/base64
/// destination in `VALUE`.
pub async fn i2p_name_lookup(s: &mut BoxedPeerStream, name: &str) -> Result<String, I2pError> {
    sam_hello(s).await?;
    s.write_all(format!("NAMING LOOKUP NAME={name}\n").as_bytes())
        .await?;
    let r = parse_reply(&read_line(s).await?, Expect::Naming)?;
    match r.result {
        None => Ok(r.destination),
        Some(e) => Err(e),
    }
}
