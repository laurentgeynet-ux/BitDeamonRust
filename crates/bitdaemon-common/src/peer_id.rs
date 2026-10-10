//! 20-byte peer identifier (`libtorrent::peer_id` / `peer_id.hpp`) plus
//! `aux::generate_peer_id` (`src/generate_peer_id.cpp`) and `aux::url_random`
//! (`src/string_util.cpp`).

use std::fmt;

use rand::Rng;

/// `libtorrent::peer_id` — fixed 20-byte peer identifier sent in the
/// BitTorrent handshake.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct PeerId {
    bytes: [u8; 20],
}

/// `peer_id::size` — the protocol-fixed length.
pub const PEER_ID_LEN: usize = 20;

/// Character set used by `url_random`: http-accepted characters, excluding
/// `'` which some buggy trackers reject (`src/string_util.cpp`).
const PRINTABLE: &[u8; 70] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_.!~*()";

/// `aux::url_random` — fill `dest` with random URL-safe printable bytes.
///
/// Upstream samples `random(sizeof(printable) - 2)` = `random(69)` over the
/// inclusive range `[0, 69]`, i.e. all 70 characters (`sizeof` includes the
/// NUL terminator and `random(m)` is inclusive).
pub fn url_random(dest: &mut [u8]) {
    let mut rng = rand::rng();
    for b in dest.iter_mut() {
        *b = PRINTABLE[rng.random_range(0..PRINTABLE.len())];
    }
}

/// `aux::generate_peer_id(settings_pack::peer_fingerprint)`.
///
/// Copies `fingerprint` (truncated to 20 bytes) then fills the remainder
/// with `url_random` characters.
#[must_use]
pub fn generate_peer_id(fingerprint: &[u8]) -> PeerId {
    let mut ret = PeerId::default();
    let n = fingerprint.len().min(PEER_ID_LEN);
    ret.bytes[..n].copy_from_slice(&fingerprint[..n]);
    url_random(&mut ret.bytes[n..]);
    ret
}

impl PeerId {
    /// Build from exactly 20 bytes; `None` otherwise.
    #[must_use]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        data.try_into().ok().map(|bytes| Self { bytes })
    }

    /// The raw 20 bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.bytes
    }

    /// Mutable raw bytes.
    #[must_use]
    pub fn as_bytes_mut(&mut self) -> &mut [u8; 20] {
        &mut self.bytes
    }

    /// True if all 20 bytes are zero.
    #[must_use]
    pub fn is_all_zeros(&self) -> bool {
        self.bytes.iter().all(|&b| b == 0)
    }

    /// Lowercase hex form.
    #[must_use]
    pub fn to_hex(&self) -> String {
        crate::hex::to_hex(&self.bytes)
    }
}

impl fmt::Display for PeerId {
    /// Printable ASCII bytes as-is, others as `\xNN` — peer ids are mostly
    /// ASCII ("-UT3530-…"), so this stays readable in logs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for &b in &self.bytes {
            if (0x20..0x7f).contains(&b) && b != b'\\' {
                f.write_str(std::str::from_utf8(&[b]).unwrap_or("\\?"))?;
            } else {
                write!(f, "\\x{b:02x}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction() {
        assert!(PeerId::from_bytes(&[0u8; 19]).is_none());
        assert!(PeerId::from_bytes(&[0u8; 21]).is_none());
        let p = PeerId::from_bytes(&[0x42; 20]).unwrap();
        assert!(!p.is_all_zeros());
        assert_eq!(p.as_bytes(), &[0x42; 20]);
        assert!(PeerId::default().is_all_zeros());
    }

    #[test]
    fn generate_fills_to_20() {
        let p = generate_peer_id(b"-BD0001-");
        assert_eq!(&p.as_bytes()[..8], b"-BD0001-");
        // remainder is url-safe printable
        for &b in &p.as_bytes()[8..] {
            assert!(PRINTABLE.contains(&b), "byte {b:#x} not in charset");
        }

        // fingerprint longer than 20 is truncated
        let p = generate_peer_id(&[b'x'; 40]);
        assert_eq!(p.as_bytes(), &[b'x'; 20]);

        // empty fingerprint → all random printable
        let p = generate_peer_id(b"");
        assert!(p.as_bytes().iter().all(|b| PRINTABLE.contains(b)));
    }

    #[test]
    fn display_escapes() {
        let p = PeerId::from_bytes(b"-BD0001-abcdefghijk?").unwrap();
        assert_eq!(format!("{p}"), "-BD0001-abcdefghijk?");
        let p = PeerId::from_bytes(&[0x01; 20]).unwrap();
        assert_eq!(format!("{p}"), "\\x01".repeat(20));
    }
}
