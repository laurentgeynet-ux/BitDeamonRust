//! Dual-version info-hash (`libtorrent::info_hash_t` / `info_hash.hpp`).
//!
//! Holds a v1 (SHA-1) and/or a v2 (SHA-256) info-hash. An all-zero field
//! means "absent" — same sentinel as upstream.

use std::fmt;

use crate::hash::{Sha1Hash, Sha256Hash};

/// `libtorrent::protocol_version` — BitTorrent version enumerator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProtocolVersion {
    /// The original BitTorrent version, using SHA-1 hashes.
    V1,
    /// Version 2 of the BitTorrent protocol, using SHA-256 hashes.
    V2,
}

/// All protocol versions, in upstream `all_versions` order.
pub const ALL_VERSIONS: [ProtocolVersion; 2] = [ProtocolVersion::V1, ProtocolVersion::V2];

/// `libtorrent::info_hash_t` — the info-hash of a torrent.
///
/// For a hybrid torrent both fields are populated; for a pure v1 or pure v2
/// torrent the unused field is the all-zero hash.
///
/// .. note::
///     If `has_v2()` is false then the v1 hash might actually be a truncated
///     v2 hash (upstream doc-note preserved).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InfoHash {
    /// The v1 (SHA-1) info-hash; all-zero when absent.
    pub v1: Sha1Hash,
    /// The v2 (SHA-256) info-hash; all-zero when absent.
    pub v2: Sha256Hash,
}

impl InfoHash {
    /// `info_hash_t(sha1_hash)` — v1 only.
    #[must_use]
    pub fn from_v1(v1: Sha1Hash) -> Self {
        Self {
            v1,
            v2: Sha256Hash::new(),
        }
    }

    /// `info_hash_t(sha256_hash)` — v2 only.
    #[must_use]
    pub fn from_v2(v2: Sha256Hash) -> Self {
        Self {
            v1: Sha1Hash::new(),
            v2,
        }
    }

    /// `info_hash_t(sha1_hash, sha256_hash)` — hybrid.
    #[must_use]
    pub fn new(v1: Sha1Hash, v2: Sha256Hash) -> Self {
        Self { v1, v2 }
    }

    /// True if a v1 hash is present (`has_v1()`).
    #[must_use]
    pub fn has_v1(&self) -> bool {
        !self.v1.is_all_zeros()
    }

    /// True if a v2 hash is present (`has_v2()`).
    #[must_use]
    pub fn has_v2(&self) -> bool {
        !self.v2.is_all_zeros()
    }

    /// `has(protocol_version)`.
    #[must_use]
    pub fn has(&self, v: ProtocolVersion) -> bool {
        match v {
            ProtocolVersion::V1 => self.has_v1(),
            ProtocolVersion::V2 => self.has_v2(),
        }
    }

    /// `get(protocol_version)` — returns the hash truncated to 20 bytes.
    ///
    /// For v2 this is the first 20 bytes of the SHA-256 digest, matching
    /// upstream's `sha1_hash(v2.data())`.
    #[must_use]
    pub fn get(&self, v: ProtocolVersion) -> Sha1Hash {
        match v {
            ProtocolVersion::V1 => self.v1,
            ProtocolVersion::V2 => {
                Sha1Hash::from_bytes_truncate(self.v2.as_bytes()).unwrap_or_default()
            }
        }
    }

    /// `get_best()` — the (truncated) v2 hash if present, else v1.
    #[must_use]
    pub fn get_best(&self) -> Sha1Hash {
        if self.has_v2() {
            self.get(ProtocolVersion::V2)
        } else {
            self.v1
        }
    }

    /// `for_each(f)` — invoke `f` on each present hash, v1 first.
    /// The callback receives the truncated `Sha1Hash` form, like upstream.
    pub fn for_each<F: FnMut(Sha1Hash, ProtocolVersion)>(&self, mut f: F) {
        if self.has_v1() {
            f(self.v1, ProtocolVersion::V1);
        }
        if self.has_v2() {
            f(self.get(ProtocolVersion::V2), ProtocolVersion::V2);
        }
    }
}

impl fmt::Display for InfoHash {
    /// `["<v1-hex>","<v2-hex>"]` — same shape as upstream's `operator<<`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{},{}]", self.v1, self.v2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presence_flags() {
        let empty = InfoHash::default();
        assert!(!empty.has_v1() && !empty.has_v2());

        let v1 = InfoHash::from_v1(Sha1Hash::from([0x11; 20]));
        assert!(v1.has_v1() && !v1.has_v2());

        let v2 = InfoHash::from_v2(Sha256Hash::from([0x22; 32]));
        assert!(!v2.has_v1() && v2.has_v2());

        let hybrid = InfoHash::new(Sha1Hash::from([0x11; 20]), Sha256Hash::from([0x22; 32]));
        assert!(hybrid.has_v1() && hybrid.has_v2());
    }

    #[test]
    fn get_truncates_v2() {
        let mut raw = [0u8; 32];
        for (i, b) in raw.iter_mut().enumerate() {
            *b = u8::try_from(i).unwrap();
        }
        let ih = InfoHash::from_v2(Sha256Hash::from(raw));
        let got = ih.get(ProtocolVersion::V2);
        assert_eq!(got.as_bytes(), &raw[..20]);
        assert_eq!(ih.get_best(), got);
    }

    #[test]
    fn get_best_prefers_v2() {
        let hybrid = InfoHash::new(Sha1Hash::from([0xaa; 20]), Sha256Hash::from([0xbb; 32]));
        assert_eq!(hybrid.get_best().as_bytes(), &[0xbb; 20]);

        let v1 = InfoHash::from_v1(Sha1Hash::from([0xaa; 20]));
        assert_eq!(v1.get_best().as_bytes(), &[0xaa; 20]);
    }

    #[test]
    fn for_each_order() {
        let hybrid = InfoHash::new(Sha1Hash::from([0xaa; 20]), Sha256Hash::from([0xbb; 32]));
        let mut seen = Vec::new();
        hybrid.for_each(|h, v| seen.push((h, v)));
        assert_eq!(
            seen,
            vec![
                (Sha1Hash::from([0xaa; 20]), ProtocolVersion::V1),
                (Sha1Hash::from([0xbb; 20]), ProtocolVersion::V2),
            ]
        );
    }

    #[test]
    fn ordering() {
        let a = InfoHash::from_v1(Sha1Hash::from([0x01; 20]));
        let b = InfoHash::from_v1(Sha1Hash::from([0x02; 20]));
        assert!(a < b);
        assert_ne!(a, b);
    }
}
