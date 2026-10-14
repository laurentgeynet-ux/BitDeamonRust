// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `i2p_stream` — the SAM (Simple Anonymous Messaging) handshake,
//! verbatim command lines and response grammar from
//! `i2p_stream.hpp`, driven over any injected `BoxedPeerStream`.
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
    /// command too long for the 1024-byte `format_command` buffer
    /// (`asio::error::message_size` upstream).
    #[error("SAM command too long")]
    MessageSize,
    /// unknown `RESULT` (`num_errors` upstream).
    #[error("unknown error")]
    Unknown,
}

/// `format_command` — upstream writes commands into a fixed
/// 1024-byte buffer and fails with `message_size` on overflow.
fn format_command(cmd: String) -> Result<Vec<u8>, I2pError> {
    if cmd.len() >= 1024 {
        return Err(I2pError::MessageSize);
    }
    Ok(cmd.into_bytes())
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

/// `aux::split_string` — first-`sep` split; the separator is
/// consumed and no trimming happens (a `NAME` preceded by stray
/// spaces never matches upstream either).
fn split_at(s: &str, sep: char) -> (&str, &str) {
    match s.find(sep) {
        Some(i) => (&s[..i], &s[i + sep.len_utf8()..]),
        None => (s, ""),
    }
}

/// `read_line` tail — token check + `key=value` loop (quoted values
/// supported, verbatim upstream).
fn parse_reply(line: &str, expect: Expect) -> Result<SamReply, I2pError> {
    let (t1, t2) = expect.tokens();
    let (first, mut remaining) = split_at(line, ' ');
    let (second, r2) = split_at(remaining, ' ');
    remaining = r2;
    if first != t1 || second != t2 {
        return Err(I2pError::ParseFailed);
    }
    let mut reply = SamReply {
        result: None,
        destination: String::new(),
    };
    loop {
        let (name, r) = split_at(remaining, '=');
        remaining = r;
        if name.is_empty() {
            break;
        }
        let value;
        if let Some(stripped) = remaining.strip_prefix('"') {
            let (v, r) = split_at(stripped, '"');
            value = v;
            remaining = r;
        } else {
            let (v, r) = split_at(remaining, ' ');
            value = v;
            remaining = r;
        }
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
    let cmd = format_command(format!(
        "SESSION CREATE STYLE=STREAM ID={session_id} DESTINATION=TRANSIENT SIGNATURE_TYPE=7 i2cp.leaseSetEncType=4,0 inbound.quantity={iq} outbound.quantity={oq} inbound.length={il} outbound.length={ol} inbound.lengthVariance={ilv} outbound.lengthVariance={olv}\n",
        iq = opts.inbound_quantity,
        oq = opts.outbound_quantity,
        il = opts.inbound_length,
        ol = opts.outbound_length,
        ilv = opts.inbound_length_variance,
        olv = opts.outbound_length_variance,
    ))?;
    s.write_all(&cmd).await?;
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
    s.write_all(&format_command(format!(
        "STREAM CONNECT ID={session_id} DESTINATION={destination}\n"
    ))?)
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
    s.write_all(&format_command(format!("STREAM ACCEPT ID={session_id}\n"))?)
        .await?;
    let r = parse_reply(&read_line(s).await?, Expect::Stream)?;
    if let Some(e) = r.result {
        return Err(e);
    }
    read_line(s).await
}

/// `send_name_lookup` — `NAMING LOOKUP` on an *already established*
/// SAM socket (upstream reuses the session socket — no new HELLO).
/// Returns the `.b32.i2p`/base64 destination in `VALUE`.
pub async fn i2p_name_lookup(s: &mut BoxedPeerStream, name: &str) -> Result<String, I2pError> {
    s.write_all(&format_command(format!("NAMING LOOKUP NAME={name}\n"))?)
        .await?;
    let r = parse_reply(&read_line(s).await?, Expect::Naming)?;
    match r.result {
        None => Ok(r.destination),
        Some(e) => Err(e),
    }
}

/// standalone naming lookup on a fresh socket (`HELLO` then
/// `NAMING LOOKUP`) — upstream only ever looks names up on the
/// session socket; this exists for callers without a session.
pub async fn i2p_name_lookup_fresh(
    s: &mut BoxedPeerStream,
    name: &str,
) -> Result<String, I2pError> {
    sam_hello(s).await?;
    i2p_name_lookup(s, name).await
}

/// `i2p_stream`/`i2p_connection` as a `PeerConnector`/`PeerAcceptor`:
/// each outbound dial is `STREAM CONNECT` on a fresh SAM socket,
/// each accept `STREAM ACCEPT` likewise (upstream multiplexes per
/// socket exactly this way). Name lookups go over the **persistent
/// session socket** like upstream's `i2p_connection` (no new HELLO).
/// The SAM bridge itself is reached via `sam_connector` — typically
/// plain TCP to 127.0.0.1:7656.
#[derive(Debug)]
pub struct I2pConnector {
    /// connector used to reach the SAM bridge.
    sam: std::sync::Arc<PeerConnectorDyn>,
    /// SAM bridge endpoint.
    sam_addr: std::net::SocketAddr,
    /// `ID=` of our STREAM session.
    session_id: String,
    /// `m_sam_socket` — the persistent session socket, kept open for
    /// `NAMING LOOKUP` (upstream `i2p_connection` never re-HELLOs).
    session_socket: std::sync::Arc<tokio::sync::Mutex<Option<BoxedPeerStream>>>,
    /// `m_i2p_local_endpoint` — our destination (`NAMING LOOKUP ME`).
    local_endpoint: String,
}

/// alias — the trait-object connector to the SAM bridge.
pub(crate) type PeerConnectorDyn = dyn crate::transport::PeerConnector;

impl I2pConnector {
    /// connector bound to an existing `session_id` — no persistent
    /// session socket (name lookups will dial fresh SAM sockets).
    pub fn new(
        sam: std::sync::Arc<PeerConnectorDyn>,
        sam_addr: std::net::SocketAddr,
        session_id: String,
    ) -> Self {
        Self {
            sam,
            sam_addr,
            session_id,
            session_socket: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            local_endpoint: String::new(),
        }
    }

    /// `i2p_connection::open` + `on_sam_connect` — open the STREAM
    /// session, keep the SAM socket for naming lookups, and resolve
    /// our own destination (`NAMING LOOKUP NAME=ME`, upstream's
    /// `set_local_endpoint`). Returns `(connector, local_endpoint)`.
    pub async fn open_session(
        sam: std::sync::Arc<PeerConnectorDyn>,
        sam_addr: std::net::SocketAddr,
        session_id: String,
        opts: I2pSessionOptions,
    ) -> Result<(Self, String), I2pError> {
        let mut s = sam
            .clone()
            .connect(sam_addr)
            .await
            .map_err(|e| I2pError::Io(std::io::Error::other(e.to_string())))?;
        let _session_dest = i2p_session_create(&mut s, &session_id, &opts).await?;
        let local = i2p_name_lookup(&mut s, "ME").await?;
        Ok((
            Self {
                sam,
                sam_addr,
                session_id,
                session_socket: std::sync::Arc::new(tokio::sync::Mutex::new(Some(s))),
                local_endpoint: local.clone(),
            },
            local,
        ))
    }

    /// `async_name_lookup` — resolve `name` to a destination over
    /// the persistent session socket (fresh socket + HELLO fallback
    /// when it was closed).
    pub async fn name_lookup(&self, name: &str) -> Result<String, I2pError> {
        let mut guard = self.session_socket.lock().await;
        if let Some(s) = guard.as_mut() {
            return i2p_name_lookup(s, name).await;
        }
        drop(guard);
        let mut s = self.dial_sam().await?;
        i2p_name_lookup_fresh(&mut s, name).await
    }

    /// `is_open` — true while the session socket exists.
    pub async fn is_open(&self) -> bool {
        self.session_socket.lock().await.is_some()
    }

    /// `m_i2p_local_endpoint`.
    #[must_use]
    pub fn local_endpoint(&self) -> &str {
        &self.local_endpoint
    }

    /// our session id.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    async fn dial_sam(&self) -> Result<BoxedPeerStream, I2pError> {
        self.sam
            .clone()
            .connect(self.sam_addr)
            .await
            .map_err(|e| I2pError::Io(std::io::Error::other(e.to_string())))
    }
}

impl crate::transport::PeerConnector for I2pConnector {
    fn connect(
        self: std::sync::Arc<Self>,
        _addr: std::net::SocketAddr,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync>,
    > {
        Box::pin(async move {
            Err(crate::NetError::NoTransport(
                "i2p destinations are not ip addresses — use connect_dest",
            ))
        })
    }

    fn connect_dest(
        self: std::sync::Arc<Self>,
        dest: &str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::Result<BoxedPeerStream>> + Send + Sync + '_>,
    > {
        // the trait wants a Sync future; the SAM exchange holds
        // `&mut stream` across awaits (Send, not Sync) — run it in a
        // task and await the handle, which is Sync.
        let dest = dest.to_string();
        Box::pin(async move {
            let me = self.clone();
            tokio::spawn(async move {
                // upstream only resolves names ending in `.i2p`
                // (`torrent::connect_to_peer`); anything else IS the
                // base64/b32 destination string verbatim.
                let destination = if dest.ends_with(".i2p") {
                    me.name_lookup(&dest).await.map_err(|e| {
                        crate::NetError::Transport(std::io::Error::other(e.to_string()))
                    })?
                } else {
                    dest
                };
                let mut s = me.dial_sam().await.map_err(|e| {
                    crate::NetError::Transport(std::io::Error::other(e.to_string()))
                })?;
                i2p_connect(&mut s, &me.session_id, &destination)
                    .await
                    .map_err(|e| {
                        crate::NetError::Transport(std::io::Error::other(e.to_string()))
                    })?;
                Ok(s)
            })
            .await
            .map_err(|e| crate::NetError::Transport(std::io::Error::other(e.to_string())))?
        })
    }
}

/// `PeerAcceptor` counterpart — `STREAM ACCEPT` per call; the
/// returned stream is the peer's tunnel.
#[derive(Debug)]
pub struct I2pAcceptor {
    /// shared connector state (sam dial params + session id).
    conn: std::sync::Arc<I2pConnector>,
}

impl I2pAcceptor {
    /// wrap an open connector.
    #[must_use]
    pub fn new(conn: std::sync::Arc<I2pConnector>) -> Self {
        Self { conn }
    }
}

impl crate::transport::PeerAcceptor for I2pAcceptor {
    fn accept(self: std::sync::Arc<Self>) -> crate::transport::AcceptFut {
        Box::pin(async move {
            let me = self.clone();
            let s: crate::Result<BoxedPeerStream> = tokio::spawn(async move {
                let mut s = me.conn.dial_sam().await.map_err(|e| {
                    crate::NetError::Transport(std::io::Error::other(e.to_string()))
                })?;
                i2p_accept(&mut s, &me.conn.session_id).await.map_err(|e| {
                    crate::NetError::Transport(std::io::Error::other(e.to_string()))
                })?;
                Ok(Box::new(s) as BoxedPeerStream)
            })
            .await
            .map_err(|e| crate::NetError::Transport(std::io::Error::other(e.to_string())))?;
            // I2P peers are destinations, not IP endpoints
            s.map(|s| (s, None))
        })
    }
}
