// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/ffs.hpp` + `ffs.cpp` — find-first-set helpers over
//! big-endian `u32` words (Kademlia XOR metrics).
//!
//! Upstream ships software + intrinsic variants; Rust's
//! `u32::leading_zeros`/`trailing_zeros` lower to the same hardware
//! ops, so both paths collapse into one (documented deviation —
//! identical results, the `_hw`/`_sw` split exists only for MSVC-era
//! portability).

/// `log2p1` — index of the most significant set bit (0 for `v == 0`,
/// matching upstream's DeBruijn table result for 0).
#[must_use]
#[allow(clippy::cast_possible_wrap)]
pub fn log2p1(v: u32) -> i32 {
    31 - v.leading_zeros() as i32
}

/// `count_leading_zeros` — leading zero bits in a big-endian word
/// range (words arrive in network order upstream; callers pass
/// host-order `u32`s here — byte-swap first if reading raw wire data).
#[must_use]
pub fn count_leading_zeros(buf: &[u32]) -> i32 {
    for (i, &w) in buf.iter().enumerate() {
        if w == 0 {
            continue;
        }
        #[allow(clippy::cast_possible_wrap)]
        return i as i32 * 32 + w.leading_zeros() as i32;
    }
    buf.len() as i32 * 32
}

/// `count_trailing_ones` — trailing one-bits counted from the *end*
/// of the range (upstream counts `(num - i - 1) * 32 + k` over
/// `~word`).
#[must_use]
pub fn count_trailing_ones(buf: &[u32]) -> i32 {
    let num = buf.len();
    for (i, &w) in buf.iter().enumerate().rev() {
        if w == u32::MAX {
            continue;
        }
        #[allow(clippy::cast_possible_wrap)]
        return (num - i - 1) as i32 * 32 + (!w).trailing_zeros() as i32;
    }
    buf.len() as i32 * 32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log2p1_values() {
        assert_eq!(log2p1(1), 0);
        assert_eq!(log2p1(0x8000_0000), 31);
        assert_eq!(log2p1(0xff), 7);
    }

    #[test]
    fn clz() {
        assert_eq!(count_leading_zeros(&[0, 0, 1]), 95); // 2*32 + lz(1)=31
        assert_eq!(count_leading_zeros(&[0x8000_0000]), 0);
        assert_eq!(count_leading_zeros(&[0xffff_ffff, 0]), 0); // first word full → 0
        assert_eq!(count_leading_zeros(&[]), 0);
    }

    #[test]
    fn cto() {
        assert_eq!(count_trailing_ones(&[u32::MAX, u32::MAX, 0xfffe_ffff]), 16);
        assert_eq!(count_trailing_ones(&[0xffff_fffe]), 0);
        assert_eq!(count_trailing_ones(&[u32::MAX; 3]), 96);
    }
}
