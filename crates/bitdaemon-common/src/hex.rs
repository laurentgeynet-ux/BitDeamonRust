//! Hexadecimal conversion utilities.
//!
//! Port of `libtorrent/src/hex.cpp` and `libtorrent/include/libtorrent/hex.hpp`.
//! `to_hex` always produces lowercase output; `from_hex` accepts mixed case.

/// `libtorrent::aux::hex_to_int` — returns `-1` semantics as `None`.
#[must_use]
pub fn hex_to_int(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

/// `libtorrent::aux::is_hex` — true if every byte is a hex digit.
#[must_use]
pub fn is_hex(input: &[u8]) -> bool {
    input.iter().all(|&c| hex_to_int(c).is_some())
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// `libtorrent::aux::to_hex` — lowercase hex of `input` appended to `out`.
///
/// `out` must have room for `input.len() * 2` bytes; this writes exactly that.
pub fn to_hex_into(input: &[u8], out: &mut Vec<u8>) {
    out.reserve(input.len() * 2);
    for &b in input {
        out.push(HEX_CHARS[usize::from(b >> 4)]);
        out.push(HEX_CHARS[usize::from(b & 0xf)]);
    }
}

/// `libtorrent::aux::to_hex` — lowercase hex string.
#[must_use]
pub fn to_hex(input: &[u8]) -> String {
    let mut buf = Vec::with_capacity(input.len() * 2);
    to_hex_into(input, &mut buf);
    // HEX_CHARS is pure ASCII — this can never be invalid UTF-8.
    debug_assert!(buf.is_ascii());
    String::from_utf8(buf).unwrap_or_default()
}

/// Error returned by [`from_hex`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HexError {
    /// Odd number of input characters — hex encodes pairs per byte.
    #[error("odd-length hex string")]
    OddLength,
    /// Non-hex character at byte offset.
    #[error("invalid hex character at offset {0}")]
    InvalidChar(usize),
}

/// `libtorrent::aux::from_hex` — decode pairs of hex digits into bytes.
///
/// Upstream returns `bool`; we return the decoded bytes or a position-tagged
/// error. Empty input decodes to an empty vector.
pub fn from_hex(input: &[u8]) -> std::result::Result<Vec<u8>, HexError> {
    let mut out = Vec::with_capacity(input.len() / 2);
    let mut i = 0;
    while i < input.len() {
        let hi = hex_to_int(input[i]).ok_or(HexError::InvalidChar(i))?;
        if i + 1 == input.len() {
            return Err(HexError::OddLength);
        }
        let lo = hex_to_int(input[i + 1]).ok_or(HexError::InvalidChar(i + 1))?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_hex_basic() {
        assert_eq!(to_hex(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(to_hex(&[]), "");
        assert_eq!(to_hex(&[0x00, 0x0f, 0xf0]), "000ff0");
    }

    #[test]
    fn from_hex_roundtrip() {
        assert_eq!(from_hex(b"deadbeef").unwrap(), vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(from_hex(b"DEADBEEF").unwrap(), vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(from_hex(b"").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn from_hex_errors() {
        assert_eq!(from_hex(b"abc"), Err(HexError::OddLength));
        assert_eq!(from_hex(b"zz"), Err(HexError::InvalidChar(0)));
        assert_eq!(from_hex(b"0z"), Err(HexError::InvalidChar(1)));
    }

    #[test]
    fn predicates() {
        assert!(is_hex(b"0123456789abcdefABCDEF"));
        assert!(!is_hex(b"xyz"));
        assert_eq!(hex_to_int(b'f'), Some(15));
        assert_eq!(hex_to_int(b'g'), None);
    }
}
