// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `web_peer_connection.cpp`/`web_connection_base.cpp`.
//!
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
    /// conn-grouping key — upstream issues every GET of a
    /// `web_peer_connection` request batch on its own connection;
    /// redirected files route through their own (ephemeral) seed.
    /// `split_url(url).0` of the base the GETs target.
    pub base: String,
    /// HTTP bytes for all gets (concatenated, `m_file_requests`
    /// order preserved).
    pub request: String,
    /// `m_file_requests` — files the request covers (pad files
    /// included — their bytes are zeroed client-side).
    pub files: Vec<FileRequest>,
}

/// `web_seed_t::redirects` entry — a per-file redirect target
/// learned from a 3xx `Location` (`resolve_redirect_location` +
/// `maybe_url_encode` applied before storing).
#[derive(Clone, Debug)]
pub struct RedirectTarget {
    /// `redirect_base` — absolute URL up to (not incl.) the path;
    /// proxy GETs send this + `path`, direct GETs send `path`.
    pub url: String,
    /// `redirect_path` — request path verbatim.
    pub path: String,
    /// conn-grouping key — `split_url(url).0`.
    pub base: String,
    /// `Authorization` for this target — upstream forwards
    /// `web_seed_entry::auth` only when the redirect is
    /// `same_origin`; empty otherwise.
    pub authorization: String,
}

/// `WebSeedHeaders` — `add_headers` knobs (User-Agent, auth,
/// extra headers). `proxy_auth` is `Proxy-Authorization: Basic …`
/// pre-encoded.
#[derive(Clone, Debug, Default)]
pub struct WebSeedHeaders {
    /// `settings_pack::user_agent` (empty = anonymous).
    pub user_agent: String,
    /// `settings_pack::always_send_user_agent`.
    pub always_send_user_agent: bool,
    /// `settings_pack::anonymous_mode` — suppresses `User-Agent`.
    pub anonymous_mode: bool,
    /// `Authorization:` header value (`m_external_auth` verbatim,
    /// or `Basic ` + base64 of the URL-embedded credentials).
    pub authorization: String,
    /// `Proxy-Authorization: Basic …` (pre-encoded).
    pub proxy_authorization: String,
    /// `m_extra_headers`.
    pub extra: Vec<(String, String)>,
    /// `format_host_header` result host\[:port\].
    pub host: String,
}

fn add_headers(
    req: &mut String,
    h: &WebSeedHeaders,
    using_proxy: bool,
    auth: Option<&str>,
    first_request: bool,
) {
    req.push_str("Host: ");
    req.push_str(&h.host);
    if (first_request || h.always_send_user_agent) && !h.anonymous_mode && !h.user_agent.is_empty()
    {
        req.push_str("\r\nUser-Agent: ");
        req.push_str(&h.user_agent);
    }
    let auth = auth.unwrap_or(&h.authorization);
    if !auth.is_empty() {
        req.push_str("\r\nAuthorization: ");
        req.push_str(auth);
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
    if using_proxy {
        req.push_str("\r\nProxy-Connection: keep-alive");
    }
    if first_request || using_proxy {
        req.push_str("\r\nConnection: keep-alive");
    }
}

/// One per-connection slice of a block request — the plan half of
/// `write_request` (upstream builds the request string when the
/// connection sends it, so headers/`first_request` resolve at
/// `render` time).
#[derive(Clone, Debug)]
pub struct WebSeedPlan {
    /// conn-grouping key — `split_url(url).0` of the base these
    /// GETs target (the seed's own base or a redirect's).
    pub base: String,
    /// absolute-URL base for proxy GETs (`m_url`).
    pub url: String,
    /// request-path base for direct GETs (`m_path`).
    pub path: String,
    /// `m_file_requests` covered on this base (pads included).
    pub files: Vec<FileRequest>,
    /// per-file `redirects[file_index]` (aligned with `files`).
    pub redirs: Vec<Option<RedirectTarget>>,
    /// BEP-19 whole-payload seed (single-file torrent).
    pub single_file: bool,
}

/// Grouping half of `write_request` — map a block request to file
/// slices (`map_block`), apply `redirects`, group consecutive
/// slices by target base. Pure data; `render_web_request` emits
/// the HTTP bytes when the connection sends.
///
/// `length` is the torrent-contiguous span the request covers —
/// upstream's `map_length`: for v2 torrents, where `req.length`
/// is pad-excluded (`piece_size2`), the caller must pass the span
/// from `(piece, offset)` to the end of the merged request
/// (`pr.piece*piece_length + pr.start + pr.length - piece*
/// piece_length - offset`); our session requests already carry
/// contiguous (pad-inclusive) lengths.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn plan_piece_request(
    fs: &FileStorage,
    piece_length: i64,
    piece: PieceIndex,
    offset: i64,
    length: i64,
    url: &str,
    path: &str,
    single_file: bool,
    redirects: &std::collections::HashMap<usize, RedirectTarget>,
) -> Vec<WebSeedPlan> {
    let default_base = crate::parse_url::split_url(url)
        .map(|(b, _)| b)
        .unwrap_or_else(|_| url.to_string());

    let mut slices: Vec<FileRequest> = Vec::new();
    if single_file {
        slices.push(FileRequest {
            file_index: 0,
            start: i64::from(piece.0) * piece_length + offset,
            length,
            got_get: true,
        });
    } else {
        for sl in fs.map_block(piece, offset, length) {
            slices.push(FileRequest {
                file_index: sl.file_index.get(),
                start: sl.offset,
                length: sl.size,
                got_get: !fs.pad_file_at(sl.file_index),
            });
        }
    }

    let mut plans: Vec<WebSeedPlan> = Vec::new();
    // pads arriving before any GET attach to the next group
    let mut pending_pads: Vec<(FileRequest, Option<RedirectTarget>)> = Vec::new();
    for fr in slices {
        let red = redirects.get(&fr.file_index).cloned();
        if !fr.got_get {
            pending_pads.push((fr, red));
            continue;
        }
        let base = red
            .as_ref()
            .map_or_else(|| default_base.clone(), |r| r.base.clone());
        if plans.last().is_none_or(|p| p.base != base) {
            plans.push(WebSeedPlan {
                base,
                url: url.to_string(),
                path: path.to_string(),
                files: Vec::new(),
                redirs: Vec::new(),
                single_file,
            });
        }
        let i = plans.len() - 1;
        for (p, pr) in pending_pads.drain(..) {
            plans[i].files.push(p);
            plans[i].redirs.push(pr);
        }
        plans[i].files.push(fr);
        plans[i].redirs.push(red);
    }
    // trailing pads (or a pure-pad block): attach to the last group,
    // or stand alone on the default base — no GET either way
    if !pending_pads.is_empty() {
        if plans.is_empty() {
            plans.push(WebSeedPlan {
                base: default_base,
                url: url.to_string(),
                path: path.to_string(),
                files: Vec::new(),
                redirs: Vec::new(),
                single_file,
            });
        }
        let i = plans.len() - 1;
        for (p, pr) in pending_pads.drain(..) {
            plans[i].files.push(p);
            plans[i].redirs.push(pr);
        }
    }
    plans
}

/// `write_request`'s emission — the HTTP/1.1 GET(s) for one
/// `WebSeedPlan`, headers evaluated at send time
/// (`m_first_request` is per-connection upstream).
#[must_use]
pub fn render_web_request(
    fs: &FileStorage,
    plan: &WebSeedPlan,
    headers: &WebSeedHeaders,
    using_proxy: bool,
    first_request: bool,
) -> WebSeedRequest {
    let mut request = String::new();
    // upstream clears `m_first_request` after the first GET of the
    // batch — later GETs in the same request omit UA/keep-alive
    let mut first = first_request;
    for (fr, red) in plan
        .files
        .iter()
        .zip(plan.redirs.iter())
        .filter(|(f, _)| f.got_get)
    {
        request.push_str("GET ");
        if plan.single_file {
            // whole-payload seed: `m_url`/`m_path` verbatim — a
            // redirect replaces them entirely.
            match red {
                Some(r) => request.push_str(if using_proxy { &r.url } else { &r.path }),
                None => request.push_str(if using_proxy { &plan.url } else { &plan.path }),
            }
        } else if let Some(r) = red {
            if using_proxy {
                request.push_str(&r.url);
                // `m_url` ends in '/' — skip a dup slash on the
                // redirect path (upstream appends from index 1)
                let skip = usize::from(r.path.starts_with('/'));
                if !r.url.ends_with('/') && skip == 0 {
                    request.push('/');
                }
                request.push_str(&r.path[skip..]);
            } else {
                request.push_str(&r.path);
            }
        } else {
            let base = if using_proxy { &plan.url } else { &plan.path };
            request.push_str(base);
            if !base.ends_with('/') {
                request.push('/');
            }
            request.push_str(&escape_file_path(&fs.file_path(
                bitdaemon_common::types::FileIndex(fr.file_index as i32),
                b"",
            )));
        }
        request.push_str(" HTTP/1.1\r\n");
        add_headers(
            &mut request,
            headers,
            using_proxy,
            red.as_ref().map(|r| r.authorization.as_str()),
            first,
        );
        first = false;
        request.push_str("\r\nRange: bytes=");
        request.push_str(&fr.start.to_string());
        request.push('-');
        request.push_str(&(fr.start + fr.length - 1).to_string());
        request.push_str("\r\n\r\n");
    }
    WebSeedRequest {
        base: plan.base.clone(),
        request,
        files: plan.files.clone(),
    }
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
    /// non-2xx status with the server's message. `retry_after`
    /// carries a parsed `Retry-After` header when present —
    /// upstream feeds it to `retry_web_seed` verbatim.
    #[error("http {status}: {message}")]
    Http {
        /// response status code.
        status: i32,
        /// `Server` header (or status text upstream interpolates).
        message: String,
        /// `Retry-After` seconds → `Duration`, when sent.
        retry_after: Option<std::time::Duration>,
    },
    /// 3xx — upstream `handle_redirect` stores a per-file redirect
    /// (`web_seed_t::redirects`) and reconnects; the caller can
    /// re-request `file_index` at `location` (resolve it with
    /// `resolve_redirect_location` + `maybe_url_encode`).
    #[error("redirect file {file_index} -> {location}")]
    Redirect {
        /// `m_file_requests.front().file_index`.
        file_index: usize,
        /// raw `Location` header value.
        location: String,
    },
    /// response shorter than the requested range.
    #[error("short body: got {got}, want {want}")]
    ShortBody {
        /// received.
        got: i64,
        /// requested.
        want: i64,
    },
    /// `errors::invalid_range` on the *advertised* range — a 206
    /// with malformed/missing `Content-Range`, or a 200 without
    /// `Content-Length` (`errors::no_content_length`). Upstream
    /// `disable()`s the seed — the caller treats this as terminal
    /// for the session.
    #[error("invalid response range metadata")]
    InvalidRange,
    /// the advertised range parses but isn't the requested one —
    /// upstream `disconnect(errors::invalid_range)` — retryable,
    /// the seed is not disabled.
    #[error("range mismatch: got {start}-{end}, want {want_start}-{want_end}")]
    RangeMismatch {
        /// response `Content-Range` start (0 for non-206).
        start: i64,
        /// response end, exclusive.
        end: i64,
        /// requested file offset.
        want_start: i64,
        /// requested end, exclusive.
        want_end: i64,
    },
}

/// `web_peer_connection`'s read side: write `req.request` and read
/// one HTTP response **per non-pad file request** (upstream issues
/// one GET per file slice; each gets its own 206 response).
///
/// Returns `((file_index, file_offset, bytes) segments, close)` —
/// the caller reassembles the block; pad files yield no segment
/// (upstream synthesizes zeroes). `close` is the `Connection:
/// close` flag of the last response (upstream
/// `m_received_disconnect` → the connection must be rebuilt before
/// the next request, as required by HTTP/1.0-only seeds).
pub async fn fetch_block(
    s: &mut crate::transport::BoxedPeerStream,
    req: &WebSeedRequest,
    max_response: usize,
) -> Result<(Vec<(usize, i64, Vec<u8>)>, bool), WebSeedError> {
    use tokio::io::AsyncWriteExt;
    s.write_all(req.request.as_bytes())
        .await
        .map_err(crate::http_conn::HttpError::Io)?;
    let mut out = Vec::new();
    let mut close = false;
    for f in req.files.iter().filter(|f| f.got_get) {
        let r = crate::http_conn::recv_response(s, max_response).await?;
        // upstream: `!is_ok_status` aborts; `is_redirect` is handled
        // first (redirects pass `is_ok_status` but carry no body).
        if crate::http_parser::is_redirect(r.status()) {
            return Err(WebSeedError::Redirect {
                file_index: f.file_index,
                location: r.header("location").unwrap_or("").to_string(),
            });
        }
        if !crate::http_parser::is_ok_status(r.status()) {
            return Err(WebSeedError::Http {
                status: r.status(),
                message: r.header("server").unwrap_or("").to_string(),
                retry_after: crate::http_parser::header_duration(&r.parser, "retry-after"),
            });
        }
        close = r.parser.connection_close();
        // `get_range` — a 206 must carry a well-formed
        // `Content-Range` (malformed/missing → `invalid_range` →
        // `disable()`); any other ok status must carry
        // `Content-Length` (`no_content_length` → `disable()`).
        let (mut range_start, mut range_end) = r.parser.content_range();
        if r.status() == 206 {
            if range_start < 0 || range_end < range_start {
                return Err(WebSeedError::InvalidRange);
            }
            // the http range is inclusive — exclusive-ify
            range_end += 1;
        } else {
            range_start = 0;
            range_end = r.parser.content_length();
            if range_end < 0 {
                return Err(WebSeedError::InvalidRange);
            }
        }
        // the byte range the server answered must match the file
        // request exactly — a mismatch is a retryable disconnect
        // upstream, not a disable.
        if range_start != f.start || range_end != f.start + f.length {
            return Err(WebSeedError::RangeMismatch {
                start: range_start,
                end: range_end,
                want_start: f.start,
                want_end: f.start + f.length,
            });
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
    Ok((out, close))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitdaemon_common::types::PieceIndex;
    use bitdaemon_core::{FileFlags, FileStorage, calc_num_pieces};

    fn multi_fs() -> FileStorage {
        let mut fs = FileStorage::new();
        // a(2000) + pad(96) + b(2000) — piece 0 (4096) spans all
        // three so one block request exercises pad-skip + grouping
        fs.add_file(None, b"torrent/a.bin", 2000, FileFlags::NONE, 0, b"", None)
            .ok();
        fs.add_file(
            None,
            b"torrent/pad-file",
            96,
            FileFlags::PAD_FILE,
            0,
            b"",
            None,
        )
        .ok();
        fs.add_file(None, b"torrent/b.bin", 2000, FileFlags::NONE, 0, b"", None)
            .ok();
        fs.set_piece_length(4096);
        fs.set_num_pieces(calc_num_pieces(&fs));
        fs
    }

    fn headers() -> WebSeedHeaders {
        WebSeedHeaders {
            host: "s.example".to_string(),
            user_agent: "test/1.0".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn render_first_request_emits_keepalive() {
        let fs = multi_fs();
        let plan = plan_piece_request(
            &fs,
            4096,
            PieceIndex(0),
            0,
            1000,
            "http://s.example/torrent/",
            "/torrent/",
            false,
            &Default::default(),
        );
        let r1 = render_web_request(&fs, &plan[0], &headers(), false, true);
        let r2 = render_web_request(&fs, &plan[0], &headers(), false, false);
        assert!(r1.request.contains("\r\nConnection: keep-alive"));
        assert!(r1.request.contains("User-Agent: test/1.0"));
        assert!(!r2.request.contains("Connection:"));
        assert!(!r2.request.contains("User-Agent:"));
    }

    #[test]
    fn render_proxy_emits_absolute_form_and_proxy_keepalive() {
        let fs = multi_fs();
        let plan = plan_piece_request(
            &fs,
            4096,
            PieceIndex(0),
            0,
            100,
            "http://s.example/torrent/",
            "/torrent/",
            false,
            &Default::default(),
        );
        let r = render_web_request(&fs, &plan[0], &headers(), true, false);
        assert!(
            r.request
                .starts_with("GET http://s.example/torrent/torrent/a.bin")
        );
        assert!(r.request.contains("\r\nProxy-Connection: keep-alive"));
        assert!(r.request.contains("\r\nConnection: keep-alive"));
    }

    #[test]
    fn render_anonymous_suppresses_user_agent() {
        let fs = multi_fs();
        let plan = plan_piece_request(
            &fs,
            4096,
            PieceIndex(0),
            0,
            100,
            "http://s.example/torrent/",
            "/torrent/",
            false,
            &Default::default(),
        );
        let h = WebSeedHeaders {
            anonymous_mode: true,
            ..headers()
        };
        let r = render_web_request(&fs, &plan[0], &h, false, true);
        assert!(!r.request.contains("User-Agent:"));
        assert!(r.request.contains("Connection: keep-alive"));
    }

    #[test]
    fn plan_skips_pad_and_groups_redirects() {
        let fs = multi_fs();
        let mut redirects = std::collections::HashMap::new();
        redirects.insert(
            2usize,
            RedirectTarget {
                url: "http://cdn.example".to_string(),
                path: "/moved/b.bin".to_string(),
                base: "http://cdn.example".to_string(),
                authorization: String::new(),
            },
        );
        // a[0..2000] + pad[96] + b[0..2000] = piece 0 (4096)
        let plans = plan_piece_request(
            &fs,
            4096,
            PieceIndex(0),
            0,
            4096,
            "http://s.example/torrent/",
            "/torrent/",
            false,
            &redirects,
        );
        // file 0 on the seed base, file 2 on the redirect base —
        // the pad rides with the group of the file that follows it
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].base, "http://s.example");
        assert_eq!(plans[1].base, "http://cdn.example");
        assert_eq!(
            plans[0].files.iter().map(|f| f.got_get).collect::<Vec<_>>(),
            vec![true]
        );
        assert_eq!(
            plans[1].files.iter().map(|f| f.got_get).collect::<Vec<_>>(),
            vec![false, true]
        );
        // the redirected file's GET uses the redirect path verbatim
        let r = render_web_request(&fs, &plans[1], &headers(), false, true);
        assert!(r.request.starts_with("GET /moved/b.bin HTTP/1.1"));
        assert!(r.request.contains("Range: bytes=0-1999"));
        let r0 = render_web_request(&fs, &plans[0], &headers(), false, true);
        assert!(r0.request.starts_with("GET /torrent/torrent/a.bin"));
        assert_eq!(r0.request.matches("Range:").count(), 1);
    }

    #[test]
    fn single_file_request_is_range_only() {
        let mut fs = FileStorage::new();
        fs.add_file(None, b"movie.mkv", 8000, FileFlags::NONE, 0, b"", None)
            .ok();
        fs.set_piece_length(4096);
        fs.set_num_pieces(calc_num_pieces(&fs));
        let plans = plan_piece_request(
            &fs,
            4096,
            PieceIndex(1),
            128,
            256,
            "http://s.example/dl/movie.mkv",
            "/dl/movie.mkv",
            true,
            &Default::default(),
        );
        assert_eq!(plans.len(), 1);
        let r = render_web_request(&fs, &plans[0], &headers(), false, true);
        // start = piece*piece_length + offset = 4096+128
        assert!(r.request.contains("Range: bytes=4224-4479"));
        assert!(r.request.starts_with("GET /dl/movie.mkv HTTP/1.1"));
    }

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
