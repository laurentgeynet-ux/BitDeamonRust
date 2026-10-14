// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/string_util.hpp` + `string_util.cpp` — byte-oriented
//! string helpers. `split_string`, `parse_decimal`,
//! `string_equal_no_case`, `url_random`, `escape_*` already live in
//! `bitdaemon-core` (`escape_string`/`peer_id::url_random`) and are
//! not duplicated here.

/// `listen_interface_t` — one `listen_interfaces` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenInterface {
    /// `device` — address or interface name.
    pub device: String,
    /// `port`.
    pub port: i32,
    /// `ssl` — `s` suffix.
    pub ssl: bool,
    /// `local` — `l` suffix (loopback-only bind).
    pub local: bool,
}

/// `aux::is_alpha`.
#[must_use]
pub const fn is_alpha(c: u8) -> bool {
    c.is_ascii_alphabetic()
}

/// `aux::is_print`.
#[must_use]
pub const fn is_print(c: u8) -> bool {
    c >= 32 && c < 127
}

/// `aux::is_space` — space, tab, LF, CR, FF, VT.
#[must_use]
pub const fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

/// `aux::to_lower` — ASCII only.
#[must_use]
pub const fn to_lower(c: u8) -> u8 {
    if c.is_ascii_uppercase() { c + 32 } else { c }
}

/// `string_begins_no_case` — `s2` starts with `s1` ignoring case.
#[must_use]
pub fn string_begins_no_case(s1: &[u8], s2: &[u8]) -> bool {
    s2.len() >= s1.len()
        && s1
            .iter()
            .zip(s2.iter())
            .all(|(&a, &b)| to_lower(a) == to_lower(b))
}

/// `string_ends_with`.
#[must_use]
pub fn string_ends_with(s1: &[u8], s2: &[u8]) -> bool {
    s1.len() >= s2.len() && s1[s1.len() - s2.len()..] == *s2
}

/// `strip_string` — trim whitespace both ends.
#[must_use]
pub fn strip_string(s: &[u8]) -> &[u8] {
    let mut s = s;
    while s.first().is_some_and(|&c| is_space(c)) {
        s = &s[1..];
    }
    while s.last().is_some_and(|&c| is_space(c)) {
        s = &s[..s.len() - 1];
    }
    s
}

/// `ltrim`.
#[must_use]
pub fn ltrim(s: &[u8]) -> &[u8] {
    let mut s = s;
    while s.first().is_some_and(|&c| is_space(c)) {
        s = &s[1..];
    }
    s
}

/// `search` — offset of `src` inside `target`, -1 when absent.
#[must_use]
pub fn search(src: &[u8], target: &[u8]) -> i32 {
    if src.is_empty() || target.len() < src.len() {
        return -1;
    }
    target
        .windows(src.len())
        .position(|w| w == src)
        .map_or(-1, |i| i as i32)
}

/// `split_string_quotes` — split on `sep`, but if the head starts
/// with `"` the separator inside the quoted span doesn't count.
#[must_use]
pub fn split_string_quotes(last: &[u8], sep: u8) -> (&[u8], &[u8]) {
    if last.is_empty() {
        return (b"", b"");
    }
    let mut pos = 0;
    if last[0] == b'"' && sep != b'"' {
        for &c in &last[1..] {
            pos += 1;
            if c == b'"' {
                break;
            }
        }
    }
    let mut found_sep = 0usize;
    for &c in &last[pos..] {
        if c == sep {
            found_sep = 1;
            break;
        }
        pos += 1;
    }
    (&last[..pos], &last[pos + found_sep..])
}

/// `parse_listen_interfaces` — `"ip:port[s][l]"` entries, comma
/// separated, optional quotes; bad entries go to `err`.
#[must_use]
pub fn parse_listen_interfaces(input: &str) -> (Vec<ListenInterface>, Vec<String>) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut rest = input.as_bytes();
    while !rest.is_empty() {
        let (element, r) = split_string_quotes(rest, b',');
        rest = r;
        let mut element = strip_string(element);
        if element.len() > 1 && element[0] == b'"' && element[element.len() - 1] == b'"' {
            element = &element[1..element.len() - 1];
        }
        if element.is_empty() {
            continue;
        }

        let mut iface = ListenInterface {
            device: String::new(),
            port: 0,
            ssl: false,
            local: false,
        };
        let bad = |e: &[u8]| String::from_utf8_lossy(e).into_owned();

        let port: &[u8];
        if element[0] == b'[' {
            let Some(pos) = element.iter().position(|&c| c == b']') else {
                err.push(bad(element));
                continue;
            };
            if pos + 1 >= element.len() || element[pos + 1] != b':' {
                err.push(bad(element));
                continue;
            }
            iface.device = String::from_utf8_lossy(strip_string(&element[1..pos])).into_owned();
            port = strip_string(&element[pos + 2..]);
        } else {
            match element.iter().position(|&c| c == b':') {
                Some(pos) => {
                    iface.device =
                        String::from_utf8_lossy(strip_string(&element[..pos])).into_owned();
                    port = strip_string(&element[pos + 1..]);
                }
                None => {
                    err.push(bad(element));
                    continue;
                }
            }
        }

        // consume digits
        let digit_end = port
            .iter()
            .position(|c| !c.is_ascii_digit())
            .unwrap_or(port.len());
        let port_str = &port[..digit_end];
        if port_str.is_empty() || port_str.len() > 5 {
            err.push(bad(element));
            continue;
        }
        let Ok(port_num) = std::str::from_utf8(port_str).unwrap_or("").parse::<i32>() else {
            err.push(bad(element));
            continue;
        };
        if !(0..=65535).contains(&port_num) {
            err.push(bad(element));
            continue;
        }
        iface.port = port_num;

        for &c in strip_string(&port[digit_end..]) {
            match c {
                b's' => iface.ssl = true,
                b'l' => iface.local = true,
                _ => {}
            }
        }
        out.push(iface);
    }
    (out, err)
}

/// `print_listen_interfaces` — inverse of `parse_listen_interfaces`
/// (IPv6 in brackets, `s`/`l` suffixes).
#[must_use]
pub fn print_listen_interfaces(list: &[ListenInterface]) -> String {
    let mut ret = String::new();
    for i in list {
        if !ret.is_empty() {
            ret.push(',');
        }
        if i.device.parse::<std::net::Ipv6Addr>().is_ok() {
            ret.push('[');
            ret.push_str(&i.device);
            ret.push(']');
        } else {
            ret.push_str(&i.device);
        }
        ret.push(':');
        ret.push_str(&i.port.to_string());
        if i.ssl {
            ret.push('s');
        }
        if i.local {
            ret.push('l');
        }
    }
    ret
}

/// `parse_comma_separated_string_port` — `"host:port,…"` list used by
/// `dht_bootstrap_nodes`; `[]` stripped off IPv6 literals.
#[must_use]
pub fn parse_comma_separated_string_port(input: &str) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut start = 0usize;
    while start < bytes.len() {
        while start < bytes.len() && is_space(bytes[start]) {
            start += 1;
        }
        let end = bytes[start..]
            .iter()
            .position(|&c| c == b',')
            .map_or(bytes.len(), |p| start + p);
        if let Some(colon) = bytes[start..end].iter().rposition(|&c| c == b':') {
            let colon = start + colon;
            if colon > start {
                let port_part = strip_string(&bytes[colon + 1..end]);
                // upstream `parse_decimal` accepts a digit prefix
                if let Some(port) = bitdaemon_core::escape_string::parse_decimal(port_part)
                    .and_then(|p| i32::try_from(p).ok())
                {
                    let mut soft_end = colon;
                    while soft_end > start && is_space(bytes[soft_end - 1]) {
                        soft_end -= 1;
                    }
                    if bytes[start] == b'[' {
                        start += 1;
                    }
                    if soft_end > start && bytes[soft_end - 1] == b']' {
                        soft_end -= 1;
                    }
                    out.push((input[start..soft_end].to_string(), port));
                }
            }
        }
        start = end + 1;
    }
    out
}

/// `parse_comma_separated_string` — plain comma list, spaces trimmed.
#[must_use]
pub fn parse_comma_separated_string(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut start = 0usize;
    while start < bytes.len() {
        while start < bytes.len() && is_space(bytes[start]) {
            start += 1;
        }
        let end = bytes[start..]
            .iter()
            .position(|&c| c == b',')
            .map_or(bytes.len(), |p| start + p);
        let mut soft_end = end;
        while soft_end > start && is_space(bytes[soft_end - 1]) {
            soft_end -= 1;
        }
        // upstream pushes unconditionally — empty elements are kept
        out.push(input[start..soft_end].to_string());
        start = end + 1;
    }
    out
}

/// `format_host_authority` — IPv6 gets brackets; port appended when
/// `port > 0`.
fn format_host_authority(mut host: String, port: i32) -> String {
    let has_port = port > 0;
    if !host.is_empty() && host.starts_with('[') && host.ends_with(']') {
        if has_port {
            host.push(':');
            host.push_str(&port.to_string());
        }
        return host;
    }
    if host.contains(':') {
        host = format!("[{host}]");
    }
    if has_port {
        host.push(':');
        host.push_str(&port.to_string());
    }
    host
}

/// `format_host_for_connect` — authority form; `port == 0` returns
/// the host untouched.
#[must_use]
pub fn format_host_for_connect(host: String, port: u16) -> String {
    if port == 0 {
        return host;
    }
    format_host_authority(host, i32::from(port))
}

/// `format_host_header` — like connect, but the port is omitted when
/// it equals the scheme default (`default_port`).
#[must_use]
pub fn format_host_header(host: String, port: i32, default_port: i32) -> String {
    format_host_authority(
        host,
        if port <= 0 || port == default_port {
            0
        } else {
            port
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn char_classes() {
        assert!(is_alpha(b'a') && is_alpha(b'Z') && !is_alpha(b'0'));
        assert!(is_print(b'~') && !is_print(b'\x1f') && !is_print(127));
        assert!(is_space(b'\x0c') && is_space(b' ') && !is_space(b'a'));
        assert_eq!(to_lower(b'Q'), b'q');
        assert_eq!(to_lower(b'5'), b'5');
    }

    #[test]
    fn begins_ends() {
        assert!(string_begins_no_case(b"HTtP", b"http://x"));
        assert!(!string_begins_no_case(b"https", b"http"));
        assert!(string_ends_with(b"http://x", b"//x"));
    }

    #[test]
    fn strip_and_search() {
        assert_eq!(strip_string(b"  hi \t\n"), b"hi");
        assert_eq!(ltrim(b"  x  "), b"x  ");
        assert_eq!(search(b"lo w", b"hello world"), 3);
        assert_eq!(search(b"zz", b"hello"), -1);
    }

    #[test]
    fn split_quotes() {
        assert_eq!(
            split_string_quotes(b"\"a,b\",c", b','),
            (&b"\"a,b\""[..], &b"c"[..])
        );
        assert_eq!(split_string_quotes(b"a,b", b','), (&b"a"[..], &b"b"[..]));
        assert_eq!(split_string_quotes(b"a", b','), (&b"a"[..], &b""[..]));
    }

    #[test]
    fn listen_interfaces() {
        let (out, err) = parse_listen_interfaces("0.0.0.0:6881,[::1]:6881s,eth0:443sl, \"lo :1\"");
        assert!(err.is_empty(), "{err:?}");
        assert_eq!(out[0].device, "0.0.0.0");
        assert!(out[1].ssl && !out[1].local && out[1].port == 6881);
        assert!(out[2].ssl && out[2].local && out[2].port == 443);
        assert_eq!(out[3].device, "lo");
        let (out2, err2) = parse_listen_interfaces("nocolon,ok:80");
        assert_eq!(err2, vec!["nocolon"]);
        assert_eq!(out2.len(), 1);
    }

    #[test]
    fn print_listen_roundtrip() {
        let (list, _) = parse_listen_interfaces("[::1]:6881s,0.0.0.0:80l");
        assert_eq!(print_listen_interfaces(&list), "[::1]:6881s,0.0.0.0:80l");
    }

    #[test]
    fn comma_lists() {
        let v = parse_comma_separated_string_port("a:1, [b::1]:2 , c:3");
        assert_eq!(v[0], ("a".to_string(), 1));
        assert_eq!(v[1], ("b::1".to_string(), 2));
        assert_eq!(v[2], ("c".to_string(), 3));
        assert_eq!(
            parse_comma_separated_string(" x , y ,z"),
            vec!["x", "y", "z"]
        );
    }

    #[test]
    fn host_authority() {
        assert_eq!(format_host_for_connect("::1".into(), 6881), "[::1]:6881");
        assert_eq!(format_host_for_connect("h".into(), 0), "h");
        assert_eq!(format_host_header("h".into(), 443, 443), "h");
        assert_eq!(format_host_header("h".into(), 444, 443), "h:444");
        assert_eq!(format_host_header("[::1]".into(), 81, 80), "[::1]:81");
    }
}
