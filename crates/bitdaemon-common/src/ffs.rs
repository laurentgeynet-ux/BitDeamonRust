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
    if v == 0 {
        return 0;
    }
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
        // upstream's DeBruijn table yields 0 for 0 — keep that quirk.
        assert_eq!(log2p1(0), 0);
    }

    /// upstream's `to_binary` — hex string → network-order `u32` words.
    /// Our `count_leading_zeros` takes host-order words, so each word
    /// is `from_be_bytes`-decoded (upstream's `network_to_host`).
    fn words(hex: &str) -> [u32; 5] {
        let mut w = [0u32; 5];
        let bytes = crate::hex::from_hex(hex.as_bytes()).unwrap();
        for (word, c) in w.iter_mut().zip(bytes.chunks_exact(4)) {
            *word = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
        }
        w
    }

    /// Port of `TORRENT_TEST(count_leading_zeros)` (`test_ffs.cpp`).
    #[test]
    fn clz() {
        let cases: &[(&str, i32)] = &[
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
        for &(hex, expected) in cases {
            assert_eq!(count_leading_zeros(&words(hex)), expected, "{hex}");
        }
        assert_eq!(count_leading_zeros(&[]), 0);
    }

    /// Port of `TORRENT_TEST(count_trailing_ones_u32)` (`test_ffs.cpp`).
    /// The vectors upstream wraps in `host_to_network` are given here
    /// in host order, matching this port's calling convention.
    #[test]
    fn cto() {
        assert_eq!(count_trailing_ones(&[0]), 0);
        assert_eq!(count_trailing_ones(&[u32::MAX]), 32);
        assert_eq!(count_trailing_ones(&[0xff00_ff00]), 0);
        assert_eq!(count_trailing_ones(&[0xff0f_ff00]), 0);
        assert_eq!(count_trailing_ones(&[0xf0ff_00ff]), 8);
        assert_eq!(count_trailing_ones(&[0xf0ff_0fff]), 12);
        assert_eq!(count_trailing_ones(&[0xf0ff_0fff, u32::MAX]), 44);
        assert_eq!(count_trailing_ones(&[u32::MAX, u32::MAX, 0xfffe_ffff]), 16);
        assert_eq!(count_trailing_ones(&[u32::MAX; 3]), 96);
    }
}
