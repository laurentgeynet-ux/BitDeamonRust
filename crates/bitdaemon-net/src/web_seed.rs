// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `web_peer_connection`/`web_connection_base` — url-seed /
//! http-seed request building and response bookkeeping. Upstream
//! glues this to a socket+parser; here a request is a byte string
//! and the response is consumed by the existing `http_parser`
//! (transport stays injected — web seeds can ride an onion lane).

use bitdaemon_common::types::PieceIndex;
use bitdaemon_core::FileStorage;
use bitdaemon_core::escape_string::escape_path;

/// `escape_file_path` — percent-encode each path segment while
/// keeping `/` separators, `\` normalized first (upstream handles
/// Windows separators before escaping).
fn escape_file_path(path: &[u8]) -> String {
    let fixed: Vec<u8> = path
        .iter()
        .map(|&c| if c == b'\\' { b'/' } else { c })
        .collect();
    String::from_utf8_lossy(&escape_path(&fixed)).into_owned()
}

/// `web_peer_connection` ctor URL normalization:
/// multi-file seeds must end with `/` (filenames get appended);
/// a single-file seed ending in `/` gets the (escaped) file name
/// appended. `path` is the request path, `url` the absolute form
/// used when a proxy is in play.
pub fn normalize_web_seed_url(
    path: &mut String,
    url: &mut String,
    num_files: usize,
    first_file_path: &[u8],
) {
    if num_files != 1 {
        if !path.is_empty() && !path.ends_with('/') {
            path.push('/');
        }
        if !url.is_empty() && !url.ends_with('/') {
            url.push('/');
        }
    } else {
        if path.is_empty() {
            path.push('/');
        }
        if path.ends_with('/') {
            path.push_str(&escape_file_path(first_file_path));
        }
        if url.ends_with('/') {
            url.push_str(&escape_file_path(first_file_path));
        }
    }
}

/// `file_request_t` — one file slice an HTTP GET covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRequest {
    /// `file_index`.
    pub file_index: usize,
    /// `start` — byte offset in the file.
    pub start: i64,
    /// `length`.
    pub length: i64,
    /// `got_get` — false for pad files (they get no GET, so no
    /// response to consume on the wire).
    pub got_get: bool,
}

/// `web_connection_base` request context.
#[derive(Clone, Debug)]
pub struct WebSeedRequest {
    /// HTTP bytes for all gets (concatenated, `m_file_requests`
    /// order preserved).
    pub request: String,
    /// `m_file_requests` — files the request covers (pad files
    /// included — their bytes are zeroed client-side).
    pub files: Vec<FileRequest>,
}

/// `WebSeedHeaders` — `add_headers` knobs (User-Agent, auth,
/// extra headers). `proxy_auth` is `Proxy-Authorization: Basic …`
/// pre-encoded.
#[derive(Clone, Debug, Default)]
pub struct WebSeedHeaders {
    /// `settings_pack::user_agent` (empty = anonymous).
    pub user_agent: String,
    /// `always_send_user_agent` or first request.
    pub send_user_agent: bool,
    /// `Authorization:` header (external auth verbatim).
    pub authorization: String,
    /// `Proxy-Authorization: Basic …` (pre-encoded).
    pub proxy_authorization: String,
    /// `m_extra_headers`.
    pub extra: Vec<(String, String)>,
    /// `format_host_header` result host\[:port\].
    pub host: String,
}

fn add_headers(req: &mut String, h: &WebSeedHeaders) {
    req.push_str("Host: ");
    req.push_str(&h.host);
    if h.send_user_agent && !h.user_agent.is_empty() {
        req.push_str("\r\nUser-Agent: ");
        req.push_str(&h.user_agent);
    }
    if !h.authorization.is_empty() {
        req.push_str("\r\nAuthorization: ");
        req.push_str(&h.authorization);
    }
    if !h.proxy_authorization.is_empty() {
        req.push_str("\r\nProxy-Authorization: ");
        req.push_str(&h.proxy_authorization);
    }
    for (k, v) in &h.extra {
        req.push_str("\r\n");
        req.push_str(k);
        req.push_str(": ");
        req.push_str(v);
    }
}

/// `web_peer_connection::write_request` — build the GET(s) for a
/// block request. `single_file` = BEP-19 url-seed over the whole
/// payload (the path is used verbatim — single-file paths are
/// assumed already encoded, like upstream).
///
/// `url`/`path` mirror `m_url`/`m_path`: `url` when a proxy
/// request needs the absolute form, `path` otherwise.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_piece_request(
    fs: &FileStorage,
    piece_length: i64,
    piece: PieceIndex,
    offset: i64,
    length: i64,
    url: &str,
    path: &str,
    single_file: bool,
    using_proxy: bool,
    headers: &WebSeedHeaders,
) -> WebSeedRequest {
    let mut request = String::new();
    let mut files = Vec::new();

    if single_file {
        let fr = FileRequest {
            file_index: 0,
            start: i64::from(piece.0) * piece_length + offset,
            length,
            got_get: true,
        };
        request.push_str("GET ");
        request.push_str(if using_proxy { url } else { path });
        request.push_str(" HTTP/1.1\r\n");
        add_headers(&mut request, headers);
        request.push_str("\r\nRange: bytes=");
        request.push_str(&fr.start.to_string());
        request.push('-');
        request.push_str(&(fr.start + fr.length - 1).to_string());
        request.push_str("\r\n\r\n");
        files.push(fr);
    } else {
        for sl in fs.map_block(piece, offset, length) {
            let mut fr = FileRequest {
                file_index: sl.file_index.get(),
                start: sl.offset,
                length: sl.size,
                got_get: true,
            };
            if fs.pad_file_at(sl.file_index) {
                fr.got_get = false;
                files.push(fr);
                continue;
            }
            request.push_str("GET ");
            let base = if using_proxy { url } else { path };
            request.push_str(base);
            if !base.ends_with('/') {
                request.push('/');
            }
            request.push_str(&escape_file_path(&fs.file_path(sl.file_index, b"")));
            request.push_str(" HTTP/1.1\r\n");
            add_headers(&mut request, headers);
            request.push_str("\r\nRange: bytes=");
            request.push_str(&fr.start.to_string());
            request.push('-');
            request.push_str(&(fr.start + fr.length - 1).to_string());
            request.push_str("\r\n\r\n");
            files.push(fr);
        }
    }
    WebSeedRequest { request, files }
}

/// `on_received` bookkeeping — `m_file_requests` consumption.
/// Given `bytes` of response body arrived for `files`, returns
/// `(file_index, file_offset, take)` chunks in order — the caller
/// writes them (pad files contribute zeroes upstream synthesizes).
#[must_use]
pub fn consume_body(files: &[FileRequest], mut bytes: i64) -> Vec<(usize, i64, i64)> {
    let mut out = Vec::new();
    for f in files {
        if bytes <= 0 {
            break;
        }
        let take = f.length.min(bytes);
        out.push((f.file_index, f.start, take));
        bytes -= take;
    }
    out
}

/// `http_error` categories surfaced to the caller.
#[derive(Debug, thiserror::Error)]
pub enum WebSeedError {
    /// stream / parser failure.
    #[error("transport: {0}")]
    Transport(#[from] crate::http_conn::HttpError),
    /// non-2xx status with the server's message.
    #[error("http {0}: {1}")]
    Http(i32, String),
    /// response shorter than the requested range.
    #[error("short body: got {got}, want {want}")]
    ShortBody {
        /// received.
        got: i64,
        /// requested.
        want: i64,
    },
}

/// `web_peer_connection`'s read side: write `req.request` and read
/// one HTTP response **per non-pad file request** (upstream issues
/// one GET per file slice; each gets its own 206 response).
///
/// Returns `(file_index, file_offset, bytes)` segments — the caller
/// reassembles the block; pad files yield no segment (upstream
/// synthesizes zeroes).
pub async fn fetch_block(
    s: &mut crate::transport::BoxedPeerStream,
    req: &WebSeedRequest,
    max_response: usize,
) -> Result<Vec<(usize, i64, Vec<u8>)>, WebSeedError> {
    use tokio::io::AsyncWriteExt;
    s.write_all(req.request.as_bytes())
        .await
        .map_err(crate::http_conn::HttpError::Io)?;
    let mut out = Vec::new();
    for f in req.files.iter().filter(|f| f.got_get) {
        let r = crate::http_conn::recv_response(s, max_response).await?;
        if !(200..300).contains(&r.status()) {
            return Err(WebSeedError::Http(
                r.status(),
                r.header("server").unwrap_or("").to_string(),
            ));
        }
        let body = r.body();
        if body.len() as i64 != f.length {
            return Err(WebSeedError::ShortBody {
                got: body.len() as i64,
                want: f.length,
            });
        }
        out.push((f.file_index, f.start, body));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_multi_file() {
        let mut p = "http://s/dir".to_string();
        let mut u = "http://s/dir".to_string();
        normalize_web_seed_url(&mut p, &mut u, 3, b"file.iso");
        assert_eq!(p, "http://s/dir/");
        assert_eq!(u, "http://s/dir/");
    }

    #[test]
    fn normalize_single_file() {
        let mut p = "/seed/".to_string();
        let mut u = String::new();
        normalize_web_seed_url(&mut p, &mut u, 1, b"my file.iso");
        assert_eq!(p, "/seed/my%20file.iso");
        assert_eq!(u, "");
        let mut p2 = "/seed/my file.iso".to_string();
        let mut u2 = String::new();
        normalize_web_seed_url(&mut p2, &mut u2, 1, b"my file.iso");
        assert_eq!(p2, "/seed/my file.iso");
    }

    #[test]
    fn escape_backslash() {
        assert_eq!(escape_file_path(b"dir\x5cf b.txt"), "dir/f%20b.txt");
    }
}
