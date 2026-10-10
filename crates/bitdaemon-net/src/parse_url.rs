//! Port of `parse_url.cpp` — URL splitting and validation used by
//! trackers and web seeds.

use thiserror::Error;

/// URL parse errors (`errors::` codes upstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum UrlError {
    /// `errors::unsupported_url_protocol`.
    #[error("unsupported URL protocol")]
    UnsupportedProtocol,
    /// `errors::expected_close_bracket_in_address`.
    #[error("expected closing bracket in address")]
    ExpectedCloseBracket,
    /// `errors::invalid_hostname`.
    #[error("invalid hostname")]
    InvalidHostname,
    /// `errors::invalid_port`.
    #[error("invalid port")]
    InvalidPort,
}

/// `(protocol, auth, hostname, port, path)` —
/// `parse_url_components`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlComponents {
    /// `protocol` (`http`, `udp`, …).
    pub protocol: String,
    /// `user:pass` (may be empty).
    pub auth: String,
    /// `hostname` (brackets stripped for v6 literals).
    pub hostname: String,
    /// `port` — `-1` when absent.
    pub port: i32,
    /// `path` incl. query/fragment; forced to start with `/`.
    pub path: String,
}

/// `parse_url_components` — verbatim grammar incl. auth, v6 literals
/// and the LDH hostname check.
pub fn parse_url_components(url: &str) -> Result<UrlComponents, UrlError> {
    let bytes = url.as_bytes();
    let mut start = 0usize;
    while start < bytes.len() && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    let end = bytes.iter().position(|&b| b == b':');
    let colon = match end {
        Some(c) => c,
        None => {
            return finish(url, start, Err(UrlError::UnsupportedProtocol));
        }
    };
    let protocol = url[start..colon].to_string();
    let mut pos = colon + 1;
    if pos >= bytes.len() || bytes[pos] != b'/' {
        return finish(url, start, Err(UrlError::UnsupportedProtocol));
    }
    pos += 1;
    if pos >= bytes.len() || bytes[pos] != b'/' {
        return finish(url, start, Err(UrlError::UnsupportedProtocol));
    }
    pos += 1;
    start = pos;

    let at = url[pos..].find('@').map(|i| pos + i);
    let colon2 = url[pos..].find(':').map(|i| pos + i);
    let end = [
        url[pos..].find('/'),
        url[pos..].find('?'),
        url[pos..].find('#'),
    ]
    .into_iter()
    .flatten()
    .map(|i| pos + i)
    .min()
    .unwrap_or(bytes.len());

    let mut auth = String::new();
    if let (Some(at), Some(c)) = (at, colon2) {
        if c < at && at < end {
            auth = url[start..at].to_string();
            start = at + 1;
        }
    }

    let mut ipv6_literal = false;
    let mut port_pos;
    let hostname;
    if start < bytes.len() && bytes[start] == b'[' {
        let close = url[start..].find(']').map(|i| start + i);
        let close = match close {
            Some(c) => c,
            None => return finish(url, start, Err(UrlError::ExpectedCloseBracket)),
        };
        hostname = url[start + 1..close].to_string();
        port_pos = url[close..]
            .find(':')
            .map(|i| close + i)
            .unwrap_or(usize::MAX);
        ipv6_literal = true;
    } else {
        port_pos = url[start..]
            .find(':')
            .map(|i| start + i)
            .unwrap_or(usize::MAX);
        if port_pos < end {
            hostname = url[start..port_pos].to_string();
        } else {
            hostname = url[start..end].to_string();
        }
    }

    if hostname.is_empty() {
        return finish(url, start, Err(UrlError::InvalidHostname));
    }
    // LDH; IPv6 literals also allow ':', '%', '_', '~'
    for c in hostname.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
            continue;
        }
        if ipv6_literal && (c == ':' || c == '%' || c == '_' || c == '~') {
            continue;
        }
        return finish(url, start, Err(UrlError::InvalidHostname));
    }

    let mut port = -1;
    if port_pos < end {
        port_pos += 1;
        let digits = &url[port_pos..end];
        if !digits.chars().all(|c| c.is_ascii_digit()) || digits.is_empty() {
            return finish(url, start, Err(UrlError::InvalidPort));
        }
        match digits.parse::<i32>() {
            Ok(p) => port = p,
            Err(_) => return finish(url, start, Err(UrlError::InvalidPort)),
        }
    }

    let start = end;
    finish2(url, start, Ok((protocol, auth, hostname, port)))
}

fn finish<T>(url: &str, start: usize, r: Result<T, UrlError>) -> Result<T, UrlError> {
    // upstream always produces the (possibly `/`-prefixed) path even
    // on error — we only need it for the Ok path
    let _ = (url, start);
    r
}

fn finish2(
    url: &str,
    start: usize,
    r: Result<(String, String, String, i32), UrlError>,
) -> Result<UrlComponents, UrlError> {
    let (protocol, auth, hostname, port) = r?;
    let mut path = url[start..].to_string();
    if path.is_empty() || path.starts_with('?') || path.starts_with('#') {
        path.insert(0, '/');
    }
    Ok(UrlComponents {
        protocol,
        auth,
        hostname,
        port,
        path,
    })
}

/// `split_url` — `(base, path)` split at the first `/` after `://`.
pub fn split_url(url: &str) -> Result<(String, String), UrlError> {
    let bytes = url.as_bytes();
    let pos = match bytes.iter().position(|&b| b == b':') {
        Some(p) if bytes.len() - p >= 3 && bytes[p + 1] == b'/' && bytes[p + 2] == b'/' => p + 3,
        _ => return Err(UrlError::UnsupportedProtocol),
    };
    match url[pos..].find('/') {
        Some(i) => Ok((url[..pos + i].to_string(), url[pos + i..].to_string())),
        None => Ok((url.to_string(), String::new())),
    }
}

/// `is_idna` — an `xn--`-prefixed label.
#[must_use]
pub fn is_idna(hostname: &str) -> bool {
    let mut h = hostname;
    loop {
        let (label, rest) = match h.find('.') {
            Some(d) => (&h[..d], Some(&h[d + 1..])),
            None => (h, None),
        };
        if label.len() >= 4 && label[..2].eq_ignore_ascii_case("xn") && label[2..4] == *"--" {
            return true;
        }
        match rest {
            Some(r) => h = r,
            None => return false,
        }
    }
}

/// `has_tracker_query_string` — any announce arg name present.
#[must_use]
pub fn has_tracker_query_string(query_string: &str) -> bool {
    const TRACKER_ARGS: [&str; 9] = [
        "info_hash",
        "event",
        "port",
        "left",
        "key",
        "uploaded",
        "downloaded",
        "corrupt",
        "peer_id",
    ];
    let mut qs = query_string;
    while !qs.is_empty() {
        let (arg, rest) = match qs.find('&') {
            Some(i) => (&qs[..i], &qs[i + 1..]),
            None => (qs, ""),
        };
        let name = arg.split('=').next().unwrap_or("");
        if TRACKER_ARGS.iter().any(|a| a.eq_ignore_ascii_case(name)) {
            return true;
        }
        qs = rest;
    }
    false
}

/// `same_origin` — scheme/host/port triple, case-insensitive;
/// default ports 443 (https) / 80.
#[must_use]
pub fn same_origin(a: &str, b: &str) -> bool {
    fn origin(u: &str) -> (String, String, i32) {
        match parse_url_components(u) {
            Ok(c) => {
                let port = if c.port == -1 {
                    if c.protocol.eq_ignore_ascii_case("https") {
                        443
                    } else {
                        80
                    }
                } else {
                    c.port
                };
                (c.protocol, c.hostname, port)
            }
            Err(_) => (String::new(), String::new(), 0),
        }
    }
    let oa = origin(a);
    let ob = origin(b);
    !oa.1.is_empty()
        && oa.0.eq_ignore_ascii_case(&ob.0)
        && oa.1.eq_ignore_ascii_case(&ob.1)
        && oa.2 == ob.2
}

/// `is_valid_tracker_url` — scheme allowlist + no control chars +
/// parses cleanly.
#[must_use]
pub fn is_valid_tracker_url(url: &str) -> bool {
    if url.is_empty() {
        return false;
    }
    for c in url.bytes() {
        if c <= 0x20 || c == 0x7f {
            return false;
        }
    }
    let lower = url.to_ascii_lowercase();
    if !(lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("wss://")
        || lower.starts_with("ws://")
        || lower.starts_with("udp://"))
    {
        return false;
    }
    parse_url_components(url).is_ok()
}
