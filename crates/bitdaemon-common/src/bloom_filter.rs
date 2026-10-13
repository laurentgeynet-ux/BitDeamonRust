//! `aux::bloom_filter<N>` — 2-bit-index bloom filter over SHA-1 hashes
//! (`bloom_filter.hpp`, `bloom_filter.cpp`). Used by DHT routing.
//!
//! Bit indices derive from the first 4 key bytes, little-endian pairs,
//! modulo `len*8`, LSB-first within a byte — all transcribed verbatim so a
//! serialized filter is byte-compatible with upstream's `to_string`.
//!
//! `set_bits`/`has_bits`/`count_zero_bits` keep upstream's explicit `len`
//! parameter (the filter's byte count, i.e. modulus `len*8`). Accesses use
//! safe indexing — upstream would read out of bounds on inconsistent input,
//! we panic instead.

use crate::hash::Sha1Hash;

/// `aux::set_bits` — set the two bits selected by key `k` in `bits`,
/// where the filter has `len` bytes.
///
/// # Panics
/// If a selected bit lies outside `bits` (upstream would read/write OOB).
pub fn set_bits(k: &[u8; 4], bits: &mut [u8], len: usize) {
    let len_bits = len as u32 * 8;
    let idx1 = (u32::from(k[0]) | (u32::from(k[1]) << 8)) % len_bits;
    let idx2 = (u32::from(k[2]) | (u32::from(k[3]) << 8)) % len_bits;
    bits[(idx1 / 8) as usize] |= 1 << (idx1 & 7);
    bits[(idx2 / 8) as usize] |= 1 << (idx2 & 7);
}

/// `aux::has_bits` — both bits selected by `k` must be set.
#[must_use]
pub fn has_bits(k: &[u8; 4], bits: &[u8], len: usize) -> bool {
    let len_bits = len as u32 * 8;
    let idx1 = (u32::from(k[0]) | (u32::from(k[1]) << 8)) % len_bits;
    let idx2 = (u32::from(k[2]) | (u32::from(k[3]) << 8)) % len_bits;
    (bits[(idx1 / 8) as usize] & (1 << (idx1 & 7))) != 0
        && (bits[(idx2 / 8) as usize] & (1 << (idx2 & 7))) != 0
}

/// `aux::count_zero_bits` — unset bits in `bits` (nibble table).
#[must_use]
pub fn count_zero_bits(bits: &[u8]) -> usize {
    // number of bits _not_ set in a nibble
    const BITCOUNT: [u8; 16] = [4, 3, 3, 2, 3, 2, 2, 1, 3, 2, 2, 1, 2, 1, 1, 0];
    bits.iter()
        .map(|b| {
            usize::from(BITCOUNT[(b & 0xf) as usize]) + usize::from(BITCOUNT[(b >> 4) as usize])
        })
        .sum()
}

/// `aux::bloom_filter<N>` — `N` bytes of filter state.
#[derive(Clone)]
pub struct BloomFilter<const N: usize> {
    bits: [u8; N],
}

impl<const N: usize> BloomFilter<N> {
    /// `bloom_filter()` — zeroed.
    #[must_use]
    pub fn new() -> Self {
        Self { bits: [0; N] }
    }

    /// `find(k)` — may false-positive, never false-negative.
    #[must_use]
    pub fn find(&self, k: &Sha1Hash) -> bool {
        let b = k.as_bytes();
        has_bits(&[b[0], b[1], b[2], b[3]], &self.bits, N)
    }

    /// `set(k)`.
    pub fn set(&mut self, k: &Sha1Hash) {
        let b = k.as_bytes();
        set_bits(&[b[0], b[1], b[2], b[3]], &mut self.bits, N);
    }

    /// `to_string()` — raw filter bytes (upstream serializes as a
    /// `std::string`; we expose the byte array directly).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; N] {
        &self.bits
    }

    /// `from_string` — load raw filter bytes.
    pub fn from_bytes(&mut self, bytes: &[u8; N]) {
        self.bits = *bytes;
    }

    /// `clear()`.
    pub fn clear(&mut self) {
        self.bits = [0; N];
    }

    /// `size()` — estimated element count:
    /// `ln(zero_bits/m) / (2 ln(1 − 1/m))` with `m = N*8`.
    #[must_use]
    pub fn size(&self) -> f32 {
        let m = (N * 8) as f32;
        let c = count_zero_bits(&self.bits).min(N * 8 - 1) as f32;
        (c / m).ln() / (2.0 * (1.0 - 1.0 / m).ln())
    }
}

impl<const N: usize> Default for BloomFilter<N> {
    fn default() -> Self {
        Self::new()
    }
}
impl<const N: usize> std::fmt::Debug for BloomFilter<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BloomFilter<…>")
    }
}
