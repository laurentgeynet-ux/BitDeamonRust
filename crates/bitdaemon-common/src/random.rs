//! `random()`/`aux::random_bytes`/`aux::crypto_random_bytes` —
//! `random.hpp`/`random.cpp` ported onto `rand`.
//!
//! Semantics preserved: [`random`] is **inclusive** of `m`
//! (`uniform_int_distribution(0, m)` upstream). We don't reproduce
//! upstream's raw mt19937 engine since no wire behavior depends on the
//! exact stream.

use rand::{Rng, RngCore, seq::SliceRandom};

/// `random(m)` — uniform in `[0, m]` **inclusive** (upstream's
/// `std::uniform_int_distribution<std::uint32_t>(0, m)`).
#[must_use]
pub fn random(m: u32) -> u32 {
    rand::rng().random_range(0..=m)
}

/// `aux::random_bytes` — pseudo-random bytes (upstream fills from
/// `random(0xff)`; not a crypto API).
pub fn random_bytes(buffer: &mut [u8]) {
    rand::rng().fill_bytes(buffer);
}

/// `aux::crypto_random_bytes` — entropy suitable for secrets.
/// `ThreadRng` is a CSPRNG (ChaCha12 seeded from the OS), matching
/// upstream's CryptGenRandom/RAND_bytes/getrandom quality bar.
pub fn crypto_random_bytes(buffer: &mut [u8]) {
    rand::rng().fill_bytes(buffer);
}

/// `aux::random_shuffle` — Fisher–Yates over a slice.
pub fn random_shuffle<T>(range: &mut [T]) {
    range.shuffle(&mut rand::rng());
}
