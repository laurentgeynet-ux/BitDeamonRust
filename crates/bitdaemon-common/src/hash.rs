//! Fixed-size digest types (`Sha1Hash`, `Sha256Hash`).
//!
//! Port of `libtorrent/include/libtorrent/sha1_hash.hpp` and
//! `libtorrent/src/sha1_hash.cpp` (`digest32<N>`).
//!
//! Upstream stores the digest as an array of big-endian `u32` words; we store
//! the raw bytes directly. Both representations compare identically —
//! upstream's `<` compares `network_to_host(word)` word-by-word, which is
//! lexicographic byte order, exactly `Ord` on `[u8; N]`.
//!
//! `<<`/`>>` shift the digest as a single big-endian integer, matching the
//! upstream test vectors (`test_sha1_hash.cpp`).

use std::cmp::Ordering;
use std::fmt;
use std::ops::{
    BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Index, Not, Shl, ShlAssign,
    Shr, ShrAssign,
};
use std::str::FromStr;

use crate::hex::{HexError, from_hex, to_hex};

/// An N-byte digest in big-endian ("network") byte order.
///
/// `libtorrent::digest32<N>` where `N` was a bit count; here `BYTES` is the
/// byte count (`digest32<160>` → `Digest<20>`).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest<const BYTES: usize> {
    bytes: [u8; BYTES],
}

/// `libtorrent::sha1_hash` — 20-byte digest.
pub type Sha1Hash = Digest<20>;

/// `libtorrent::sha256_hash` — 32-byte digest.
pub type Sha256Hash = Digest<32>;

impl<const BYTES: usize> Digest<BYTES> {
    /// Digest of all zeros (`digest32()` default construction).
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: [0; BYTES] }
    }

    /// Digest of all `0xff` bytes (`digest32::max()`).
    #[must_use]
    pub const fn max() -> Self {
        Self {
            bytes: [0xff; BYTES],
        }
    }

    /// Digest of all zeros (`digest32::min()`).
    #[must_use]
    pub const fn min() -> Self {
        Self::new()
    }

    /// Build from exactly `BYTES` bytes (`digest32(span)`).
    ///
    /// Returns `None` if `data.len() != BYTES`. Upstream asserts the span is
    /// at least N/8 and copies `min(len, size)`; call sites that may pass
    /// short buffers must use the checked API, so we require an exact fit.
    #[must_use]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        data.try_into().ok().map(|bytes| Self { bytes })
    }

    /// Copy the first `BYTES` bytes of `data`; `None` if `data` is shorter.
    ///
    /// Used where upstream truncates, e.g. `info_hash_t::get_best()` exposes a
    /// SHA-256 hash through a 20-byte SHA-1 field.
    #[must_use]
    pub fn from_bytes_truncate(data: &[u8]) -> Option<Self> {
        if data.len() < BYTES {
            return None;
        }
        Self::from_bytes(&data[..BYTES])
    }

    /// The raw bytes, big-endian (`digest32::data()`).
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; BYTES] {
        &self.bytes
    }

    /// Mutable view of the raw bytes (`digest32::data()` non-const).
    #[must_use]
    pub fn as_bytes_mut(&mut self) -> &mut [u8; BYTES] {
        &mut self.bytes
    }

    /// Set the digest to all zeros (`digest32::clear()`).
    pub fn clear(&mut self) {
        self.bytes = [0; BYTES];
    }

    /// True if every byte is zero (`digest32::is_all_zeros()`).
    #[must_use]
    pub fn is_all_zeros(&self) -> bool {
        self.bytes.iter().all(|&b| b == 0)
    }

    /// Number of leading zero *bits* (`digest32::count_leading_zeroes()`,
    /// `aux::count_leading_zeros`). Used for Kademlia XOR-metric buckets.
    #[must_use]
    pub fn count_leading_zeroes(&self) -> u32 {
        let mut count = 0u32;
        for &b in &self.bytes {
            count += b.leading_zeros();
            if b != 0 {
                break;
            }
        }
        count
    }

    /// Iterate over the raw bytes (`digest32::begin()/end()`).
    pub fn iter(&self) -> std::slice::Iter<'_, u8> {
        self.bytes.iter()
    }

    /// Copy out as a byte string (`digest32::to_string()` — binary, not hex).
    #[must_use]
    pub fn to_bytes(&self) -> [u8; BYTES] {
        self.bytes
    }

    /// Lowercase hex representation (`operator<<` / `aux::to_hex`).
    #[must_use]
    pub fn to_hex(&self) -> String {
        to_hex(&self.bytes)
    }

    /// Parse from a lowercase-or-mixed hex string of exactly `2*BYTES` chars.
    ///
    /// `istream >> digest32` / `aux::from_hex` equivalent.
    pub fn from_hex(input: &[u8]) -> Result<Self, HexError> {
        let bytes = from_hex(input)?;
        Self::from_bytes(&bytes).ok_or(HexError::OddLength)
    }
}

impl<const BYTES: usize> Default for Digest<BYTES> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BYTES: usize> Ord for Digest<BYTES> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl<const BYTES: usize> PartialOrd for Digest<BYTES> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const BYTES: usize> Index<usize> for Digest<BYTES> {
    type Output = u8;

    fn index(&self, index: usize) -> &u8 {
        &self.bytes[index]
    }
}

impl<const BYTES: usize> fmt::Display for Digest<BYTES> {
    /// 2*BYTES lowercase hex digits.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl<const BYTES: usize> fmt::Debug for Digest<BYTES> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest<{}>({})", BYTES, self.to_hex())
    }
}

impl<const BYTES: usize> FromStr for Digest<BYTES> {
    type Err = HexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s.as_bytes())
    }
}

impl<const BYTES: usize> From<[u8; BYTES]> for Digest<BYTES> {
    fn from(bytes: [u8; BYTES]) -> Self {
        Self { bytes }
    }
}

impl<const BYTES: usize> From<Digest<BYTES>> for [u8; BYTES] {
    fn from(d: Digest<BYTES>) -> Self {
        d.bytes
    }
}

impl<const BYTES: usize> AsRef<[u8]> for Digest<BYTES> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl<const BYTES: usize> BitXorAssign for Digest<BYTES> {
    /// `digest32::operator^=` — element-wise XOR.
    fn bitxor_assign(&mut self, rhs: Self) {
        for (a, b) in self.bytes.iter_mut().zip(rhs.bytes.iter()) {
            *a ^= b;
        }
    }
}

impl<const BYTES: usize> BitXor for Digest<BYTES> {
    type Output = Self;
    fn bitxor(self, rhs: Self) -> Self {
        let mut ret = self;
        ret ^= rhs;
        ret
    }
}

impl<const BYTES: usize> BitAndAssign for Digest<BYTES> {
    /// `digest32::operator&=`.
    fn bitand_assign(&mut self, rhs: Self) {
        for (a, b) in self.bytes.iter_mut().zip(rhs.bytes.iter()) {
            *a &= b;
        }
    }
}

impl<const BYTES: usize> BitAnd for Digest<BYTES> {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        let mut ret = self;
        ret &= rhs;
        ret
    }
}

impl<const BYTES: usize> BitOrAssign for Digest<BYTES> {
    /// `digest32::operator|=`.
    fn bitor_assign(&mut self, rhs: Self) {
        for (a, b) in self.bytes.iter_mut().zip(rhs.bytes.iter()) {
            *a |= b;
        }
    }
}

impl<const BYTES: usize> BitOr for Digest<BYTES> {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        let mut ret = self;
        ret |= rhs;
        ret
    }
}

impl<const BYTES: usize> Not for Digest<BYTES> {
    type Output = Self;
    /// `digest32::operator~` — bit-wise negation.
    fn not(self) -> Self {
        let mut ret = self;
        for b in ret.bytes.iter_mut() {
            *b = !*b;
        }
        ret
    }
}

/// `digest32::operator<<=` — numeric left shift of the big-endian integer.
///
/// Verified against `test_sha1_hash.cpp`:
/// `"ffffffffff0000000000ffffffffff0000000000" << 12` yields
/// `"fffffff0000000000ffffffffff0000000000000"`.
fn shl_be(v: &mut [u8], n: u32) {
    let nbytes = usize::try_from(n / 8).unwrap_or(usize::MAX);
    let nbits = n % 8;
    let len = v.len();
    if nbytes >= len {
        v.fill(0);
        return;
    }
    for i in 0..len {
        v[i] = if i + nbytes < len { v[i + nbytes] } else { 0 };
    }
    if nbits > 0 {
        for i in 0..len {
            let carry = if i + 1 < len {
                v[i + 1] >> (8 - nbits)
            } else {
                0
            };
            v[i] = (v[i] << nbits) | carry;
        }
    }
}

/// `digest32::operator>>=` — numeric right shift of the big-endian integer.
fn shr_be(v: &mut [u8], n: u32) {
    let nbytes = usize::try_from(n / 8).unwrap_or(usize::MAX);
    let nbits = n % 8;
    let len = v.len();
    if nbytes >= len {
        v.fill(0);
        return;
    }
    for i in (0..len).rev() {
        v[i] = if i >= nbytes { v[i - nbytes] } else { 0 };
    }
    if nbits > 0 {
        for i in (0..len).rev() {
            let carry = if i > 0 { v[i - 1] << (8 - nbits) } else { 0 };
            v[i] = (v[i] >> nbits) | carry;
        }
    }
}

impl<const BYTES: usize> ShlAssign<u32> for Digest<BYTES> {
    fn shl_assign(&mut self, n: u32) {
        shl_be(&mut self.bytes, n);
    }
}

impl<const BYTES: usize> Shl<u32> for Digest<BYTES> {
    type Output = Self;
    fn shl(self, n: u32) -> Self {
        let mut ret = self;
        ret <<= n;
        ret
    }
}

impl<const BYTES: usize> ShrAssign<u32> for Digest<BYTES> {
    fn shr_assign(&mut self, n: u32) {
        shr_be(&mut self.bytes, n);
    }
}

impl<const BYTES: usize> Shr<u32> for Digest<BYTES> {
    type Output = Self;
    fn shr(self, n: u32) -> Self {
        let mut ret = self;
        ret >>= n;
        ret
    }
}

impl<const BYTES: usize> IntoIterator for Digest<BYTES> {
    type Item = u8;
    type IntoIter = std::array::IntoIter<u8, BYTES>;
    fn into_iter(self) -> Self::IntoIter {
        self.bytes.into_iter()
    }
}

impl<'a, const BYTES: usize> IntoIterator for &'a Digest<BYTES> {
    type Item = &'a u8;
    type IntoIter = std::slice::Iter<'a, u8>;
    fn into_iter(self) -> Self::IntoIter {
        self.bytes.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_hash(s: &str) -> Sha1Hash {
        Sha1Hash::from_str(s).unwrap()
    }

    /// Port of `TORRENT_TEST(shift)` in `test_sha1_hash.cpp`.
    #[test]
    fn bit_ops_and_shifts() {
        let mut h2 = to_hash("2222222222222222222222222222222222222222");
        h2.clear();
        assert!(h2.is_all_zeros());

        h2 = to_hash("ffffffffff0000000000ffffffffff0000000000");
        let mut h1 = to_hash("fffff00000fffff00000fffff00000fffff00000");
        h1 &= h2;
        assert_eq!(h1, to_hash("fffff000000000000000fffff000000000000000"));

        h2 = to_hash("ffffffffff0000000000ffffffffff0000000000");
        h1 = to_hash("fffff00000fffff00000fffff00000fffff00000");
        h1 |= h2;
        assert_eq!(h1, to_hash("fffffffffffffff00000fffffffffffffff00000"));

        h2 = to_hash("0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f");
        h1 ^= h2;
        assert_eq!(h1, to_hash("f0f0f0f0f0f0f0ff0f0ff0f0f0f0f0f0f0ff0f0f"));
        assert_ne!(h1, h2);

        h1 = to_hash("ffffffffff0000000000ffffffffff0000000000");
        h1 <<= 12;
        assert_eq!(h1, to_hash("fffffff0000000000ffffffffff0000000000000"));
        h1 >>= 12;
        assert_eq!(h1, to_hash("000fffffff0000000000ffffffffff0000000000"));

        h1 = to_hash("7000000000000000000000000000000000000000");
        h1 <<= 1;
        assert_eq!(h1, to_hash("e000000000000000000000000000000000000000"));

        h1 = to_hash("0000000000000000000000000000000000000007");
        h1 <<= 1;
        assert_eq!(h1, to_hash("000000000000000000000000000000000000000e"));

        h1 = to_hash("0000000000000000000000000000000000000007");
        h1 >>= 1;
        assert_eq!(h1, to_hash("0000000000000000000000000000000000000003"));

        h1 = to_hash("7000000000000000000000000000000000000000");
        h1 >>= 1;
        assert_eq!(h1, to_hash("3800000000000000000000000000000000000000"));

        h1 = to_hash("7000000000000000000000000000000000000000");
        h1 >>= 32;
        assert_eq!(h1, to_hash("0000000070000000000000000000000000000000"));
        h1 >>= 33;
        assert_eq!(h1, to_hash("0000000000000000380000000000000000000000"));
        h1 <<= 33;
        assert_eq!(h1, to_hash("0000000070000000000000000000000000000000"));
    }

    /// Port of `TORRENT_TEST(count_leading_zeroes)` in `test_sha1_hash.cpp`.
    #[test]
    fn count_leading_zeroes() {
        let cases: &[(&str, u32)] = &[
            ("ffffffffffffffffffffffffffffffffffffffff", 0),
            ("0000000000000000000000000000000000000000", 160),
            ("fff0000000000000000000000000000000000000", 0),
            ("7ff0000000000000000000000000000000000000", 1),
            ("3ff0000000000000000000000000000000000000", 2),
            ("1ff0000000000000000000000000000000000000", 3),
            ("0ff0000000000000000000000000000000000000", 4),
            ("07f0000000000000000000000000000000000000", 5),
            ("03f0000000000000000000000000000000000000", 6),
            ("01f0000000000000000000000000000000000000", 7),
            ("00f0000000000000000000000000000000000000", 8),
            ("0070000000000000000000000000000000000000", 9),
            ("0030000000000000000000000000000000000000", 10),
            ("0010000000000000000000000000000000000000", 11),
            ("0000000ffff00000000000000000000000000000", 28),
            ("00000007fff00000000000000000000000000000", 29),
            ("00000003fff00000000000000000000000000000", 30),
            ("00000001fff00000000000000000000000000000", 31),
            ("00000000fff00000000000000000000000000000", 32),
            ("000000007ff00000000000000000000000000000", 33),
            ("000000003ff00000000000000000000000000000", 34),
            ("000000001ff00000000000000000000000000000", 35),
        ];
        for &(input, expected) in cases {
            assert_eq!(to_hash(input).count_leading_zeroes(), expected, "{input}");
        }
    }

    #[test]
    fn ordering_is_lexicographic() {
        let a = to_hash("0000000000000000000000000000000000000001");
        let b = to_hash("0000000000000000000000000000000000000002");
        let c = to_hash("ff00000000000000000000000000000000000000");
        assert!(a < b && b < c && a < c);
        assert_eq!(Sha1Hash::min(), Sha1Hash::new());
        assert_eq!(Sha1Hash::max().as_bytes(), &[0xff; 20]);
    }

    #[test]
    fn not_and_default() {
        let d = !Sha1Hash::new();
        assert_eq!(d, Sha1Hash::max());
        let d256 = !Sha256Hash::new();
        assert_eq!(d256.as_bytes(), &[0xff; 32]);
    }

    #[test]
    fn from_hex_requires_exact_length() {
        assert!(Sha1Hash::from_str("ff").is_err());
        assert!(
            "0123456789abcdef0123456789abcdef01234567"
                .parse::<Sha1Hash>()
                .is_ok()
        );
    }

    #[test]
    fn truncate_construction() {
        let h256 = Sha256Hash::from([0xab; 32]);
        let h160 = Sha1Hash::from_bytes_truncate(h256.as_bytes()).unwrap();
        assert_eq!(h160.as_bytes(), &[0xab; 20]);
        assert!(Sha1Hash::from_bytes_truncate(&[0u8; 10]).is_none());
    }
}
