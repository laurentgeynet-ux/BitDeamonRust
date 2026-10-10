//! Port of `lsd.cpp` — BEP-14 local peer discovery: the
//! `BT-SEARCH * HTTP/1.1` multicast announce, both directions
//! (`render_lsd_packet` + `process_packet` semantics).
//!
//! Multicast addresses: `239.192.152.143` / `[ff15::efc0:988f]`,
//! port 6771.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

/// `lsd_multicast_addr4`.
pub const LSD_ADDR4: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
/// `lsd_multicast_addr6`.
pub const LSD_ADDR6: Ipv6Addr = Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f);
/// `lsd_port`.
pub const LSD_PORT: u16 = 6771;

/// `render_lsd_packet` — byte-exact announce.
#[must_use]
pub fn render_lsd_packet(listen_port: u16, info_hash_hex: &str, cookie: u32, v6: bool) -> Vec<u8> {
    let host = if v6 {
        "[ff15::efc0:988f]"
    } else {
        "239.192.152.143"
    };
    format!(
        "BT-SEARCH * HTTP/1.1\r\nHost: {host}:6771\r\nPort: {listen_port}\r\nInfohash: {info_hash_hex}\r\ncookie: {cookie:x}\r\n\r\n\r\n"
    )
    .into_bytes()
}

/// One parsed LSD announce (`peer` + infohashes).
#[derive(Debug, Clone)]
pub struct LsdAnnounce {
    /// `Port:` — the peer's listen port.
    pub port: u16,
    /// `Infohash:` headers (there may be several).
    pub info_hashes: Vec<[u8; 20]>,
}

/// Parse a minimal HTTP request head (method + headers). The LSD
/// datagrams upstream feeds to `http_parser` only need the request
/// line and headers — this returns `None` on malformed input.
fn parse_request_head(buf: &[u8]) -> Option<(String, Vec<(String, String)>)> {
    // headers end at \r\n\r\n (or \n\n)
    let text = std::str::from_utf8(buf).ok()?;
    let end = text.find("\r\n\r\n").or_else(|| text.find("\n\n"))?;
    let head = &text[..end];
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let method = request_line.split_whitespace().next()?.to_string();
    let mut headers = Vec::new();
    for line in lines {
        let (k, v) = line.split_once(':')?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
    }
    Some((method, headers))
}

/// `lsd::process_packet` — parse an incoming BT-SEARCH, filtering out
/// our own cookie.
///
/// `from` is the sender; `our_cookie` is `m_cookie` (ours are
/// echoed back by multicast loopback). Returns the announce on the
/// peer's listen port.
pub fn process_packet(buf: &[u8], _from: SocketAddr, our_cookie: u32) -> Option<LsdAnnounce> {
    let (method, headers) = parse_request_head(buf)?;
    if !method.eq_ignore_ascii_case("bt-search") {
        return None;
    }
    let port_str = headers.iter().find(|(k, _)| k == "port")?.1.clone();
    let port: i64 = port_str.trim().parse().ok()?;
    if !(1..i64::from(u16::MAX)).contains(&port) {
        return None;
    }
    // ignore our own announces (cookie match)
    if let Some((_, c)) = headers.iter().find(|(k, _)| k == "cookie") {
        if u32::from_str_radix(c, 16).ok() == Some(our_cookie) {
            return None;
        }
    }
    let mut info_hashes = Vec::new();
    for (k, v) in &headers {
        if k != "infohash" {
            continue;
        }
        // upstream `from_hex` — 40 hex chars (v1) or 64 (v2 →
        // truncated to 20 bytes like `sha256_hash` truncation)
        if let Ok(h) = bitdaemon_common::hex::from_hex(v.as_bytes()) {
            if h.len() == 20 || h.len() == 32 {
                let mut ih = [0u8; 20];
                ih.copy_from_slice(&h[..20]);
                info_hashes.push(ih);
            }
        }
    }
    if info_hashes.is_empty() {
        return None;
    }
    Some(LsdAnnounce {
        port: port as u16,
        info_hashes,
    })
}
