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

/// `std::strtol` — leading whitespace, optional sign, then a digit
/// *prefix* (trailing junk is ignored, `no digits` → 0 not error).
fn strtol_prefix(s: &str) -> i64 {
    let t = s.trim_start_matches(|c: char| c.is_ascii_whitespace());
    let (neg, t) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let n = t.bytes().take_while(|b| b.is_ascii_digit()).count();
    let v: i64 = t[..n].parse().unwrap_or(0);
    if neg { -v } else { v }
}

/// `std::strtoul(…, 16)` — whitespace, sign, optional `0x`, hex
/// digit prefix; result wraps like `unsigned long`.
fn strtoul_hex(s: &str) -> u64 {
    let t = s.trim_start_matches(|c: char| c.is_ascii_whitespace());
    let (neg, t) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let t = t
        .strip_prefix("0x")
        .or_else(|| t.strip_prefix("0X"))
        .unwrap_or(t);
    let n = t.bytes().take_while(|b| b.is_ascii_hexdigit()).count();
    let v = u64::from_str_radix(&t[..n], 16).unwrap_or(0);
    if neg { v.wrapping_neg() } else { v }
}

/// `lsd::process_packet` — parse an incoming BT-SEARCH, filtering out
/// our own cookie.
///
/// `from` is the sender; `our_cookie` is `m_cookie` (ours are
/// echoed back by multicast loopback). `listen_net` carries the
/// `(m_listen_address, m_netmask)` pair of the socket the datagram
/// arrived on — packets from outside that subnet are ignored like
/// upstream's `match_addr_mask` gate. `None` disables the gate.
/// Returns the announce on the peer's listen port.
pub fn process_packet(
    buf: &[u8],
    from: SocketAddr,
    our_cookie: u32,
    listen_net: Option<(std::net::IpAddr, std::net::IpAddr)>,
) -> Option<LsdAnnounce> {
    if let Some((listen_addr, netmask)) = listen_net
        && !crate::enum_net::match_addr_mask(&from.ip(), &listen_addr, &netmask)
    {
        // out of our network — upstream drops the packet
        return None;
    }
    let (method, headers) = parse_request_head(buf)?;
    // upstream's http_parser already lowercases the method
    if !method.eq_ignore_ascii_case("bt-search") {
        return None;
    }
    let port_str = headers.iter().find(|(k, _)| k == "port")?.1.clone();
    // `strtol` upstream — prefix parse, `port >= u16::MAX` rejected
    let port = strtol_prefix(&port_str);
    if !(1..i64::from(u16::MAX)).contains(&port) {
        return None;
    }
    // ignore our own announces (cookie match — `strtoul` base 16)
    if let Some((_, c)) = headers.iter().find(|(k, _)| k == "cookie") {
        if strtoul_hex(c) == u64::from(our_cookie) {
            return None;
        }
    }
    let mut info_hashes = Vec::new();
    for (k, v) in &headers {
        if k != "infohash" {
            continue;
        }
        // upstream requires exactly 40 hex chars (v1 hashes) and
        // skips all-zero hashes
        if v.len() != 40 {
            continue;
        }
        if let Ok(h) = bitdaemon_common::hex::from_hex(v.as_bytes()) {
            if h.len() == 20 && h.iter().any(|&b| b != 0) {
                let mut ih = [0u8; 20];
                ih.copy_from_slice(&h);
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
