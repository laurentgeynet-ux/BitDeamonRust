//! `aux_/utf8.hpp` + `src/utf8.cpp` — UTF-8 codepoint parse/encode used by
//! path sanitization, plus latin1 conversions.
//!
//! Everything operates on raw bytes (`&[u8]`/`Vec<u8>`): torrent paths are
//! byte strings upstream and may be invalid UTF-8 on the wire.

/// `utf8_sequence_length` — bytes in the sequence starting at `b`, or 0 if
/// the lead byte is invalid.
fn utf8_sequence_length(b: u8) -> usize {
    if b < 0b1000_0000 {
        return 1;
    }
    if (b >> 5) == 0b110 {
        return 2;
    }
    if (b >> 4) == 0b1110 {
        return 3;
    }
    if (b >> 3) == 0b1_1110 {
        return 4;
    }
    // invalid prefix, but we still parse it to skip this many bytes
    if (b >> 2) == 0b11_1110 {
        return 5;
    }
    0
}

pub const MAX_CODEPOINT: i32 = 0x10ffff;
pub const SURROGATE_START: i32 = 0xd800;
pub const SURROGATE_END: i32 = 0xdfff;

/// `append_utf8_codepoint` — encode `codepoint` into `ret`; surrogates and
/// values above `MAX_CODEPOINT` become `'_'`.
pub fn append_utf8_codepoint(ret: &mut Vec<u8>, mut codepoint: i32) {
    if (SURROGATE_START..=SURROGATE_END).contains(&codepoint) || codepoint > MAX_CODEPOINT {
        codepoint = i32::from(b'_');
    }

    let seq_len = if codepoint < 0x80 {
        1
    } else if codepoint < 0x800 {
        2
    } else if codepoint < 0x10000 {
        3
    } else {
        4
    };

    match seq_len {
        1 => ret.push(codepoint as u8),
        2 => ret.push(0b1100_0000 | (codepoint >> 6) as u8),
        3 => ret.push(0b1110_0000 | (codepoint >> 12) as u8),
        _ => ret.push(0b1111_0000 | (codepoint >> 18) as u8),
    }

    for i in (0..seq_len - 1).rev() {
        ret.push(0b1000_0000 | ((codepoint >> (6 * i)) & 0b11_1111) as u8);
    }
}

/// `parse_utf8_codepoint` — `(codepoint, bytes_consumed)`; codepoint is -1
/// if the sequence is invalid.
#[must_use]
pub fn parse_utf8_codepoint(s: &[u8]) -> (i32, usize) {
    if s.is_empty() {
        return (-1, 0);
    }

    let sequence_len = utf8_sequence_length(s[0]);

    // most common case
    if sequence_len == 1 {
        return (i32::from(s[0]), sequence_len);
    }

    // invalid sequence length: skip one byte
    if sequence_len == 0 {
        return (-1, 1);
    }

    if sequence_len > s.len() {
        return (-1, s.len());
    }

    if sequence_len > 4 {
        return (-1, sequence_len);
    }

    let mut ch: i32 = match sequence_len {
        2 => i32::from(s[0] & 0b0001_1111),
        3 => i32::from(s[0] & 0b0000_1111),
        _ => i32::from(s[0] & 0b0000_0111),
    };
    for &b in s.iter().take(sequence_len).skip(1) {
        // continuation bytes must start with 10xxxxxx
        if !(0b1000_0000..=0b1011_1111).contains(&b) {
            return (-1, sequence_len);
        }
        ch <<= 6;
        ch += i32::from(b & 0b11_1111);
    }

    // reject overlong encodings
    match sequence_len {
        2 if ch < 0x80 => return (-1, sequence_len),
        3 if ch < 0x800 => return (-1, sequence_len),
        4 if ch < 0x10000 => return (-1, sequence_len),
        _ => {}
    }

    if ch > MAX_CODEPOINT {
        return (-1, sequence_len);
    }

    // per RFC 3629, surrogates must not appear in UTF-8
    if (SURROGATE_START..=SURROGATE_END).contains(&ch) {
        return (-1, sequence_len);
    }

    (ch, sequence_len)
}

/// `latin1_utf8` — ISO-8859-1 → UTF-8 bytes.
#[must_use]
pub fn latin1_utf8(s: &[u8]) -> Vec<u8> {
    let mut ret = Vec::with_capacity(s.len());
    for &c in s {
        append_utf8_codepoint(&mut ret, i32::from(c));
    }
    ret
}

/// `utf8_latin1` — UTF-8 → ISO-8859-1 bytes; `Err` on unrepresentable or
/// invalid input (upstream throws `std::invalid_argument`).
pub fn utf8_latin1(sv: &[u8]) -> Result<Vec<u8>, crate::Error> {
    let mut out = Vec::with_capacity(sv.len());
    let mut rest = sv;
    while !rest.is_empty() {
        let (cp, len) = parse_utf8_codepoint(rest);
        if !(0..=0xff).contains(&cp) {
            return Err(crate::Error::InvalidEncoding(format!(
                "code point out of latin1 range: {cp}"
            )));
        }
        rest = &rest[len..];
        out.push(cp as u8);
    }
    Ok(out)
}

/// `valid_path_character` — the set of characters allowed in path elements.
/// Platform-dependent upstream (Windows forbids `?<>\"|\b*:` etc.); we keep
/// the same `cfg` split.
#[must_use]
pub fn valid_path_character(c: i32) -> bool {
    #[cfg(windows)]
    const INVALID_CHARS: &[u8] = b"?<>\"|\x08*:";
    #[cfg(target_os = "android")]
    const INVALID_CHARS: &[u8] = b"\"*:<>?|";
    #[cfg(not(any(windows, target_os = "android")))]
    const INVALID_CHARS: &[u8] = b"";

    // C0 controls and DEL
    if !(32..).contains(&c) || c == 0x7f {
        return false;
    }
    // C1 controls
    if (0x80..=0x9f).contains(&c) {
        return false;
    }
    if c > 127 {
        return true;
    }
    !INVALID_CHARS.contains(&(c as u8))
}

/// `filter_path_character` — characters removed entirely (not replaced):
/// zero-width/invisible/bidi Unicode formatting, plus `/`, `\`, NUL.
#[must_use]
pub fn filter_path_character(c: i32) -> bool {
    // Unicode formatting characters usable for filename spoofing
    if c == 0x061c
        || (0x200b..=0x200f).contains(&c)
        || (0x202a..=0x202e).contains(&c)
        || (0x2060..=0x2064).contains(&c)
        || (0x2066..=0x2069).contains(&c)
        || c == 0xfeff
    {
        return true;
    }

    if c > 127 {
        return false;
    }
    c == i32::from(b'/') || c == i32::from(b'\\') || c == 0
}

/// `verify_encoding` — rewrites `target` in place to valid UTF-8 (invalid
/// sequences become `'_'`). Returns whether the input was already valid.
pub fn verify_encoding(target: &mut Vec<u8>) -> bool {
    if target.is_empty() {
        return true;
    }

    let mut tmp_path = Vec::with_capacity(target.len());
    let mut valid_encoding = true;

    let mut rest: &[u8] = target;
    while !rest.is_empty() {
        let (mut codepoint, len) = parse_utf8_codepoint(rest);
        if codepoint == -1 {
            codepoint = i32::from(b'_');
            valid_encoding = false;
        }
        rest = &rest[len.min(rest.len())..];
        append_utf8_codepoint(&mut tmp_path, codepoint);
    }

    if !valid_encoding {
        *target = tmp_path;
    }
    valid_encoding
}
