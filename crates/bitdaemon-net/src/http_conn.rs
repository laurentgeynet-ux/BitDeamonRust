// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `http_connection` — the request/response pump on top of
//! [`HttpParser`], over any injected [`BoxedPeerStream`] (ADR-0008:
//! tunnels/proxies carry trackers and web seeds verbatim).
//! Upstream's class owns socket+resolver+rate-limiter; here each
//! phase is composed by the caller — resolve/connect/proxy first,
//! then `send_request`/`recv_response`.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::http_parser::HttpParser;
use crate::transport::BoxedPeerStream;

/// `default_max_bottled_buffer_size`-scale cap for response buffers.
pub const MAX_RESPONSE: usize = 16 * 1024 * 1024;

/// Errors from the HTTP pump.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// transport.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// `http_parser` hit State::Error.
    #[error("malformed http response")]
    Malformed,
    /// response exceeded `max_response`.
    #[error("response too large")]
    TooLarge,
}

/// `send_request` — verbatim request format upstream writes
/// (`send_hostname`/`user_agent` included by the caller in
/// `headers`). `body` is appended after the blank line (for POST).
pub async fn send_request(
    s: &mut BoxedPeerStream,
    method: &str,
    path: &str,
    host: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<(), HttpError> {
    let mut req = String::with_capacity(256 + path.len() + body.len());
    req.push_str(method);
    req.push(' ');
    req.push_str(path);
    req.push_str(" HTTP/1.1\r\nHost: ");
    req.push_str(host);
    req.push_str("\r\n");
    for (k, v) in headers {
        req.push_str(k);
        req.push_str(": ");
        req.push_str(v);
        req.push_str("\r\n");
    }
    if !body.is_empty() {
        req.push_str("Content-Length: ");
        req.push_str(&body.len().to_string());
        req.push_str("\r\n");
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await?;
    if !body.is_empty() {
        s.write_all(body).await?;
    }
    Ok(())
}

/// the parsed response — thin view over [`HttpParser`]'s state.
pub struct HttpResponse {
    /// the parser — status/headers/chunks all queryable.
    pub parser: HttpParser,
    /// full receive buffer (`get_body` slices into it).
    pub raw: Vec<u8>,
}

impl HttpResponse {
    /// `status_code`.
    #[must_use]
    pub fn status(&self) -> i32 {
        self.parser.status_code()
    }

    /// `get_body` — payload; chunked responses are de-framed via
    /// the parser's `chunks()` ranges (upstream callers do this
    /// per-range; here it's flattened once).
    #[must_use]
    pub fn body(&self) -> Vec<u8> {
        if self.parser.chunked_encoding() {
            let mut out = Vec::new();
            for &(s, e) in self.parser.chunks() {
                let (s, e) = (s.max(0) as usize, e.max(0) as usize);
                out.extend_from_slice(&self.raw[s.min(self.raw.len())..e.min(self.raw.len())]);
            }
            return out;
        }
        self.parser.get_body(&self.raw).to_vec()
    }

    /// `header(name)` — case-insensitive.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.parser.header(name)
    }
}

/// read until `parser.finished()` or EOF — mirrors
/// `http_connection::on_read` looping `parser.incoming` on the
/// whole receive buffer.
pub async fn recv_response(
    s: &mut BoxedPeerStream,
    max_response: usize,
) -> Result<HttpResponse, HttpError> {
    let mut parser = HttpParser::new(0);
    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 8192];
    loop {
        let n = s.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > max_response {
            return Err(HttpError::TooLarge);
        }
        let mut error = false;
        parser.incoming(&buf, &mut error);
        if error {
            return Err(HttpError::Malformed);
        }
        if parser.finished() {
            break;
        }
    }
    Ok(HttpResponse { parser, raw: buf })
}

/// full GET — the common tracker/web-seed shape.
pub async fn http_get(
    s: &mut BoxedPeerStream,
    path: &str,
    host: &str,
    headers: &[(String, String)],
    max_response: usize,
) -> Result<HttpResponse, HttpError> {
    send_request(s, "GET", path, host, headers, &[]).await?;
    recv_response(s, max_response).await
}
