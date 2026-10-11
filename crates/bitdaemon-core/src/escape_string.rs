//! `aux_/escape_string.hpp`/`escape_string.cpp` — URL percent-encoding,
//! base32 and small string helpers used by magnet URIs and HTTP layers.
//! All byte-oriented, matching upstream `std::string` semantics.

use crate::error::{Error, Result, TorrentError};

fn hex_char(nibble: u8) -> u8 {
    b"0123456789abcdef"[(nibble & 0xf) as usize]
}

/// `aux::unescape_string` — decode `%XX` escapes and `+` → space.
/// On malformed input returns the partial string decoded so far plus an
/// `InvalidEscapedString` error — the partial result is observable in
/// `Error::PartialUnescape`? Upstream returns `ret` by value with `ec`
/// set; callers here get `Err` carrying the partial bytes.
pub fn unescape_string(s: &[u8]) -> Result<Vec<u8>> {
    let mut ret = Vec::with_capacity(s.len());
    let mut i = 0usize;
    while i < s.len() {
        match s[i] {
            b'+' => ret.push(b' '),
            b'%' => {
                i += 1;
                if i == s.len() {
                    return Err(Error::Torrent(TorrentError::InvalidEscapedString));
                }
                let high = bitdaemon_common::hex::hex_to_int(s[i])
                    .ok_or(Error::Torrent(TorrentError::InvalidEscapedString))?;
                i += 1;
                if i == s.len() {
                    return Err(Error::Torrent(TorrentError::InvalidEscapedString));
                }
                let low = bitdaemon_common::hex::hex_to_int(s[i])
                    .ok_or(Error::Torrent(TorrentError::InvalidEscapedString))?;
                ret.push(high * 16 + low);
            }
            c => ret.push(c),
        }
        i += 1;
    }
    Ok(ret)
}

// http://www.ietf.org/rfc/rfc2396.txt section 2.3 — upstream's table:
// "%+" ;?:@=&,$/ -_!.~*() alnum (offsets 0/10/11 select subsets)
const UNRESERVED_CHARS: &[u8] =
    b"%+;?:@=&,$/-_!.~*()ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn escape_string_impl(s: &[u8], offset: usize) -> Vec<u8> {
    debug_assert!(offset < UNRESERVED_CHARS.len());
    let allowed = &UNRESERVED_CHARS[offset..];
    let mut ret = Vec::with_capacity(s.len());
    for &c in s {
        if c != 0 && allowed.contains(&c) {
            ret.push(c);
        } else {
            ret.push(b'%');
            ret.push(hex_char(c >> 4));
            ret.push(hex_char(c));
        }
    }
    ret
}

/// `aux::escape_string` — for query arguments (offset 11).
#[must_use]
pub fn escape_string(s: &[u8]) -> Vec<u8> {
    escape_string_impl(s, 11)
}

/// `aux::escape_path` — for URL paths (offset 10, `%` allowed).
#[must_use]
pub fn escape_path(s: &[u8]) -> Vec<u8> {
    escape_string_impl(s, 10)
}

/// `aux::need_encoding` — true if `s` contains characters outside the
/// full unreserved set.
#[must_use]
pub fn need_encoding(s: &[u8]) -> bool {
    s.iter().any(|&c| c == 0 || !UNRESERVED_CHARS.contains(&c))
}

/// `aux::base32decode` — RFC 4648 base32 (upper- or lowercase; `1`→`I`,
/// `=` padding). Returns the decoded bytes; an invalid character makes
/// the result empty (upstream `ret.clear()` + bail).
#[must_use]
pub fn base32decode(s: &[u8]) -> Vec<u8> {
    let mut inbuf = [0u8; 8];
    let mut outbuf = [0u8; 5];
    let mut ret = Vec::with_capacity(s.len() * 5 / 8);
    let mut i = 0usize;
    'outer: while i < s.len() {
        let available_input = (s.len() - i).min(8);
        let mut pad_start = 0usize;
        if available_input < 8 {
            pad_start = available_input;
        }
        inbuf.fill(0);
        #[allow(clippy::needless_range_loop)]
        for j in 0..available_input {
            let c = s[i];
            i += 1;
            // uppercase ASCII range; high-bit bytes rejected below
            let in_c = if c.is_ascii_lowercase() {
                c - b'a' + b'A'
            } else {
                c
            };
            if in_c.is_ascii_uppercase() {
                inbuf[j] = in_c - b'A';
            } else if (b'2'..=b'7').contains(&in_c) {
                inbuf[j] = in_c - b'2' + (b'Z' - b'A') + 1;
            } else if in_c == b'=' {
                inbuf[j] = 0;
                if pad_start == 0 {
                    pad_start = j;
                }
            } else if in_c == b'1' {
                inbuf[j] = b'I' - b'A';
            } else {
                ret.clear();
                break 'outer;
            }
            debug_assert_eq!(inbuf[j], inbuf[j] & 0x1f);
        }

        outbuf[0] = (inbuf[0] << 3) | (inbuf[1] >> 2);
        outbuf[1] = ((inbuf[1] & 0x3) << 6) | inbuf[2] << 1 | ((inbuf[3] & 0x10) >> 4);
        outbuf[2] = ((inbuf[3] & 0x0f) << 4) | ((inbuf[4] & 0x1e) >> 1);
        outbuf[3] = ((inbuf[4] & 0x01) << 7) | (inbuf[5] << 2) | ((inbuf[6] & 0x18) >> 3);
        outbuf[4] = ((inbuf[6] & 0x07) << 5) | inbuf[7];

        const INPUT_OUTPUT_MAPPING: [usize; 9] = [5, 1, 1, 2, 2, 3, 4, 4, 5];
        let num_out = INPUT_OUTPUT_MAPPING[pad_start];
        ret.extend_from_slice(&outbuf[..num_out]);
    }
    ret
}

/// `aux::split_string` — split `s` at the first `delim`, returning
/// `(before, after)` (after excludes the delimiter; empty if absent).
pub fn split_string(s: &[u8], delim: u8) -> (&[u8], &[u8]) {
    match s.iter().position(|&c| c == delim) {
        Some(p) => (&s[..p], &s[p + 1..]),
        None => (s, &s[..0]),
    }
}

/// `aux::parse_decimal` — strict decimal parse, no sign, no trailing junk.
pub fn parse_decimal(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(s).ok()?.parse().ok()
}

/// `aux::string_equal_no_case`.
pub fn string_equal_no_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// `aux::base64decode_i2p` — I2P alphabet base64 (`-~` instead of
/// `+/`), used to turn an I2P destination string into its binary form
/// (`hasher256(dest)` → the peer's pex identity). `None` on invalid
/// input (upstream throws `invalid_escaped_string`).
#[must_use]
pub fn base64decode_i2p(s: &[u8]) -> Option<Vec<u8>> {
    fn map(c: u8) -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => u32::from(c - b'A'),
            b'a'..=b'z' => 26 + u32::from(c - b'a'),
            b'0'..=b'9' => 52 + u32::from(c - b'0'),
            b'-' => 62,
            b'~' => 63,
            _ => return None,
        })
    }

    let mut out = Vec::new();
    let mut acc: u32 = 0;
    let mut bit_offset: i32 = 18;
    for &c in s {
        if c == b'=' {
            break;
        }
        acc |= map(c)? << bit_offset;
        if bit_offset == 0 {
            // host_to_network + bytes 1..4 = low 24 bits, BE order
            out.push((acc >> 16) as u8);
            out.push((acc >> 8) as u8);
            out.push(acc as u8);
            acc = 0;
            bit_offset = 18;
        } else {
            bit_offset -= 6;
        }
    }
    if bit_offset < 18 {
        out.push((acc >> 16) as u8);
        if bit_offset < 6 {
            out.push((acc >> 8) as u8);
        }
    }
    Some(out)
}
