// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `http_connection.cpp`/`http_connection.hpp`.
//!
//! `http_connection` — the request/response pump on top of
//! `HttpParser`, over any injected `BoxedPeerStream` (ADR-0008:
//! tunnels/proxies carry trackers and web seeds verbatim).
//! Upstream's class owns socket+resolver+rate-limiter; here each
//! phase is composed by the caller — resolve/connect/proxy first,
//! then `send_request`/`recv_response`.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::http_parser::HttpParser;

/// `default_max_bottled_buffer_size`-scale cap for response buffers.
pub const MAX_RESPONSE: usize = 16 * 1024 * 1024;

/// Tracker wire-traffic accounting — upstream folds these into
/// `net.sent_tracker_bytes`/`net.recv_tracker_bytes`
/// (`tracker_connection::sent_bytes`/`received_bytes`). The engine
/// hands the totals to the session's counters when an exchange
/// completes. `Option<&mut WireStats>` params let non-tracker
/// callers (web seeds, UPnP) skip accounting.
#[derive(Debug, Default, Clone, Copy)]
pub struct WireStats {
    /// bytes sent (upstream: request url + ~100 header estimate,
    /// or datagram size + 28 for UDP/IP overhead).
    pub sent: i64,
    /// bytes received (raw response buffer; datagram size + 28 UDP).
    pub recv: i64,
}

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
    /// `errors::timed_out` — the per-read deadline elapsed
    /// (`tracker_receive_timeout` upstream).
    #[error("timed out")]
    TimedOut,
    /// non-2xx status (`http_connection` treats the code as the
    /// error; `errors::http_error`).
    #[error("http status {0}")]
    Status(i32),
    /// too many redirects (`errors::http_redirection_depleted`).
    #[error("too many redirects")]
    TooManyRedirects,
    /// 3xx without a `Location` header
    /// (`errors::http_missing_location`).
    #[error("redirect without location")]
    MissingLocation,
    /// redirect to a different origin — upstream `http_connection`
    /// reconnects; our stream is fixed, so the resolved URL is handed
    /// back to the caller (same policy as upstream: auth headers must
    /// not be forwarded cross-origin).
    #[error("redirect to {0}")]
    CrossOriginRedirect(String),
}

/// `send_request` — verbatim request format upstream writes in
/// `http_connection::sendrequest` (`Host` via `format_host_header`,
/// `User-Agent`/`Authorization` come from `headers`, gzip always
/// advertised, one-shot → `Connection: close`). `body` is appended
/// after the blank line (for POST).
pub async fn send_request<S: AsyncWrite + Unpin>(
    s: &mut S,
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
    req.push_str("Accept-Encoding: gzip\r\n");
    if !body.is_empty() {
        req.push_str("Content-Length: ");
        req.push_str(&body.len().to_string());
        req.push_str("\r\n");
    }
    req.push_str("Connection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await?;
    if !body.is_empty() {
        s.write_all(body).await?;
    }
    Ok(())
}

/// the parsed response — thin view over `HttpParser`'s state.
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
            return self.maybe_inflate(&out);
        }
        self.maybe_inflate(self.parser.get_body(&self.raw))
    }

    /// `Content-Encoding: gzip|x-gzip` → inflate (upstream calls
    /// `inflate_gzip` on the buffered body, http_connection.cpp:642).
    fn maybe_inflate(&self, body: &[u8]) -> Vec<u8> {
        match self.parser.header("content-encoding") {
            Some(enc) if enc.eq_ignore_ascii_case("gzip") || enc.eq_ignore_ascii_case("x-gzip") => {
                use std::io::Read;
                let mut d = flate2::read::GzDecoder::new(body);
                let mut out = Vec::with_capacity(body.len() * 2);
                match d.read_to_end(&mut out) {
                    Ok(_) => out,
                    Err(_) => body.to_vec(), // garbled encoding → raw body
                }
            }
            _ => body.to_vec(),
        }
    }

    /// `header(name)` — case-insensitive.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.parser.header(name)
    }
}

/// read until `parser.finished()` or EOF — mirrors
/// `http_connection::on_read` looping `parser.incoming` on the
/// whole receive buffer. `read_timeout` bounds *each* `read`
/// (upstream `http_connection` rearms `tracker_receive_timeout`
/// on every incoming chunk); `None` waits indefinitely.
pub async fn recv_response<S: AsyncRead + Unpin>(
    s: &mut S,
    max_response: usize,
    read_timeout: Option<std::time::Duration>,
) -> Result<HttpResponse, HttpError> {
    let mut parser = HttpParser::new(0);
    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 8192];
    loop {
        let n = match read_timeout {
            Some(t) => match tokio::time::timeout(t, s.read(&mut tmp)).await {
                Ok(r) => r?,
                Err(_) => return Err(HttpError::TimedOut),
            },
            None => s.read(&mut tmp).await?,
        };
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

/// full GET — the common tracker/web-seed shape. `url` is the full
/// request URL (like upstream `http_connection::get`) — the path and
/// `Host` header are derived from it.
///
/// Redirects follow `http_connection::on_redirect`: `location` is
/// resolved against the referrer with `resolve_redirect_location` and
/// retried on the same stream. A redirect to a different origin needs
/// a new connection upstream — surfaced as
/// [`HttpError::CrossOriginRedirect`] carrying the resolved URL. Up to
/// 5 hops (`m_redirects`). Non-redirect statuses return the response
/// — callers apply `is_ok_status` like upstream.
/// `read_timeout` is the per-read deadline (`tracker_receive_timeout`
/// for tracker callers); `stats` accumulates wire bytes when given
/// (each redirect hop counts, like upstream's per-connection
/// `sent_bytes`/`received_bytes`).
pub async fn http_get<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    url: &str,
    headers: &[(String, String)],
    max_response: usize,
    read_timeout: Option<std::time::Duration>,
    mut stats: Option<&mut WireStats>,
) -> Result<HttpResponse, HttpError> {
    let mut url = url.to_string();
    for _ in 0..=5 {
        let c = crate::parse_url::parse_url_components(&url).map_err(|_| HttpError::Malformed)?;
        let default_port = if c.protocol.eq_ignore_ascii_case("https") {
            443
        } else {
            80
        };
        let host = crate::string_util::format_host_header(c.hostname, c.port, default_port);
        send_request(s, "GET", &c.path, &host, headers, &[]).await?;
        // `sent_bytes(url.size() + 100)` — upstream's estimate of
        // request-line + headers overhead per request.
        if let Some(st) = stats.as_deref_mut() {
            st.sent += url.len() as i64 + 100;
        }
        let resp = recv_response(s, max_response, read_timeout).await?;
        if !crate::http_parser::is_redirect(resp.status()) {
            return Ok(resp);
        }
        if let Some(st) = stats.as_deref_mut() {
            // `received_bytes(data.size() + parser.body_start())` —
            // upstream counts the whole receive buffer (headers
            // + payload); `raw` is exactly that.
            st.recv += resp.raw.len() as i64;
        }
        let loc = resp
            .header("location")
            .map(str::trim)
            .filter(|l| !l.is_empty());
        let Some(loc) = loc else {
            return Err(HttpError::MissingLocation);
        };
        let next = crate::http_parser::resolve_redirect_location(&url, loc);
        if !crate::parse_url::same_origin(&url, &next) {
            return Err(HttpError::CrossOriginRedirect(next));
        }
        url = next;
    }
    Err(HttpError::TooManyRedirects)
}
