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

/// `file_request_t` — one file slice an HTTP GET covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRequest {
    /// `file_index`.
    pub file_index: usize,
    /// `start` — byte offset in the file.
    pub start: i64,
    /// `length`.
    pub length: i64,
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
    /// `format_host_header` result host[:port].
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
            let fr = FileRequest {
                file_index: sl.file_index.get(),
                start: sl.offset,
                length: sl.size,
            };
            if fs.pad_file_at(sl.file_index) {
                files.push(fr);
                continue;
            }
            request.push_str("GET ");
            let base = if using_proxy { url } else { path };
            request.push_str(base);
            if !base.ends_with('/') {
                request.push('/');
            }
            request.push_str(&String::from_utf8_lossy(&fs.file_path(sl.file_index, b"")));
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
