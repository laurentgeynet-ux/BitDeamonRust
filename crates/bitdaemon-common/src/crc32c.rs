//! `aux::crc32c` — CRC-32C (Castagnoli) helpers (`crc32c.cpp`).
//!
//! Upstream hand-dispatches SSE4.2/ARM intrinsics; we delegate to the
//! `crc32c` crate (same polynomial `0x1EDC6F41`, reflected, init/xorout
//! `0xFFFFFFFF`) — identical results, hardware-accelerated where available.
//!
//! Byte-order note: upstream hashes *in-memory* words — little-endian
//! layout on every supported platform. We hash `to_le_bytes()` explicitly,
//! which is the same byte stream.

/// `aux::crc32c_32(v)` — CRC32C over the 4 bytes of `v` (LE memory order).
#[must_use]
pub fn crc32c_32(v: u32) -> u32 {
    crc32c::crc32c(&v.to_le_bytes())
}

/// `aux::crc32c(buf, num_words)` — CRC32C over `num_words` 64-bit words
/// (LE memory order).
#[must_use]
pub fn crc32c_words(buf: &[u64]) -> u32 {
    // crc32c_append takes/returns the public CRC value (init/xorout handled
    // internally) — seed 0, no final inversion.
    let mut crc = 0u32;
    for w in buf {
        crc = crc32c::crc32c_append(crc, &w.to_le_bytes());
    }
    crc
}

/// Streaming CRC32C over arbitrary bytes — same primitive, byte-oriented.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}
