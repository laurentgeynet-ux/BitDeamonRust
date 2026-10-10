//! Bitfield over pieces/blocks (`libtorrent::bitfield` / `typed_bitfield`).
//!
//! Port of `libtorrent/include/libtorrent/bitfield.hpp` and
//! `libtorrent/src/bitfield.cpp`.
//!
//! Wire order is MSB-first: bit `i` lives at `buf[i / 8] & (0x80 >> (i % 8))`.
//! Upstream stores `u32` words in network order and prefixes the allocation
//! with the bit count; we store a `Vec<u8>` plus a separate `usize`. Both
//! layouts serialize identically.
//!
//! Invariant (matches upstream `TORRENT_ASSERT`): bits at index `>= size()`
//! in the last byte are always 0 — `clear_trailing_bits` runs after every
//! mutation that could dirty them.

/// `libtorrent::bitfield`.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Bitfield {
    /// MSB-first bit storage, exactly `ceil(bits / 8)` bytes.
    /// Bits beyond `bits` are cleared (invariant).
    buf: Vec<u8>,
    /// Number of valid bits.
    bits: usize,
}

impl Bitfield {
    /// `bitfield()` — empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `bitfield(bits)` — `bits` bits, all zero.
    #[must_use]
    pub fn with_size(bits: usize) -> Self {
        Self::with_value(bits, false)
    }

    /// `bitfield(bits, val)` — `bits` bits, all set to `val`.
    #[must_use]
    pub fn with_value(bits: usize, val: bool) -> Self {
        let mut bf = Self {
            buf: vec![if val { 0xff } else { 0 }; bits.div_ceil(8)],
            bits,
        };
        bf.clear_trailing_bits();
        bf
    }

    /// `bitfield(b, bits)` — view `bits` bits taken MSB-first from `b`.
    ///
    /// `None` if `b` holds fewer than `bits` bits (upstream asserts).
    #[must_use]
    pub fn from_bytes(data: &[u8], bits: usize) -> Option<Self> {
        if bits > data.len() * 8 {
            return None;
        }
        let mut bf = Self {
            buf: data[..bits.div_ceil(8)].to_vec(),
            bits,
        };
        bf.clear_trailing_bits();
        Some(bf)
    }

    /// `assign(b, bits)` — replace contents with `bits` bits from `b`.
    ///
    /// `false` if `b` is too short (upstream asserts); the bitfield is left
    /// untouched in that case.
    pub fn assign(&mut self, data: &[u8], bits: usize) -> bool {
        match Self::from_bytes(data, bits) {
            Some(bf) => {
                *self = bf;
                true
            }
            None => false,
        }
    }

    /// `get_bit(index)` / `operator[]`.
    ///
    /// Out-of-range indices return `false` (upstream only asserts).
    #[must_use]
    pub fn get_bit(&self, index: usize) -> bool {
        debug_assert!(index < self.bits);
        if index >= self.bits {
            return false;
        }
        self.buf[index / 8] & (0x80 >> (index % 8)) != 0
    }

    /// `set_bit(index)` — sets bit `index` to 1.
    ///
    /// Out-of-range indices are ignored (upstream only asserts).
    pub fn set_bit(&mut self, index: usize) {
        debug_assert!(index < self.bits);
        if index >= self.bits {
            return;
        }
        self.buf[index / 8] |= 0x80 >> (index % 8);
    }

    /// `clear_bit(index)` — sets bit `index` to 0.
    pub fn clear_bit(&mut self, index: usize) {
        debug_assert!(index < self.bits);
        if index >= self.bits {
            return;
        }
        self.buf[index / 8] &= !(0x80 >> (index % 8));
    }

    /// `all_set()` — true if every bit in `[0, size)` is 1.
    /// Returns `false` for an empty bitfield (upstream semantics).
    #[must_use]
    pub fn all_set(&self) -> bool {
        if self.bits == 0 {
            return false;
        }
        let full = self.bits / 8;
        if self.buf[..full].iter().any(|&b| b != 0xff) {
            return false;
        }
        let rest = self.bits % 8;
        if rest > 0 {
            let mask = 0xffu8 << (8 - rest);
            return self.buf[full] & mask == mask;
        }
        true
    }

    /// `none_set()` — true if every bit is 0 (also true when empty).
    #[must_use]
    pub fn none_set(&self) -> bool {
        self.buf.iter().all(|&b| b == 0)
    }

    /// `size()` — number of bits.
    #[must_use]
    pub fn size(&self) -> usize {
        self.bits
    }

    /// `num_words()` — 32-bit words needed to represent all bits.
    ///
    /// Kept for parity with upstream callers (`num_words()*4` shows up in
    /// buffer-size math in the C++ code).
    #[must_use]
    pub fn num_words(&self) -> usize {
        self.bits.div_ceil(32)
    }

    /// `num_bytes()`.
    #[must_use]
    pub fn num_bytes(&self) -> usize {
        self.bits.div_ceil(8)
    }

    /// `empty()` — zero *bits* (not zero set bits).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bits == 0
    }

    /// `data()` — the MSB-first bytes, `num_bytes()` long.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.buf
    }

    /// `count()` — number of set bits.
    #[must_use]
    pub fn count(&self) -> usize {
        self.buf.iter().map(|b| b.count_ones() as usize).sum()
    }

    /// `find_first_set()` — index of the first set bit.
    #[must_use]
    pub fn find_first_set(&self) -> Option<usize> {
        for (bi, &b) in self.buf.iter().enumerate() {
            if b != 0 {
                return Some(bi * 8 + b.leading_zeros() as usize);
            }
        }
        None
    }

    /// `find_last_clear()` — index of the last cleared bit in `[0, size)`.
    ///
    /// Bits at index `>= size()` don't count (upstream masks the tail word
    /// with `0xffffffff << (32 - (size & 31))`).
    #[must_use]
    pub fn find_last_clear(&self) -> Option<usize> {
        let nbytes = self.num_bytes();
        let tail = self.bits % 8;
        for bi in (0..nbytes).rev() {
            let mut b = self.buf[bi];
            if bi == nbytes - 1 && tail != 0 {
                // pretend out-of-range bits are set so they don't count
                b |= 0xff >> tail;
            }
            if b != 0xff {
                // highest clear index in this byte = lowest set bit of !b
                return Some(bi * 8 + 7 - (!b).trailing_zeros() as usize);
            }
        }
        None
    }

    /// `set_all()` — set every bit to 1.
    pub fn set_all(&mut self) {
        self.buf.fill(0xff);
        self.clear_trailing_bits();
    }

    /// `clear_all()` — set every bit to 0.
    pub fn clear_all(&mut self) {
        self.buf.fill(0);
    }

    /// `clear()` — release storage; `size()` becomes 0.
    pub fn clear(&mut self) {
        self.buf = Vec::new();
        self.bits = 0;
    }

    /// `resize(bits)` — grow or shrink, keeping existing bits.
    /// Newly exposed bits are 0 (upstream zero-fills new words).
    pub fn resize(&mut self, bits: usize) {
        if bits == self.bits {
            return;
        }
        self.buf.resize(bits.div_ceil(8), 0);
        self.bits = bits;
        self.clear_trailing_bits();
    }

    /// `resize(bits, val)` — newly exposed bits get `val`.
    pub fn resize_with(&mut self, bits: usize, val: bool) {
        if bits == self.bits {
            return;
        }
        let old = self.bits;
        let old_bytes = self.num_bytes();
        self.buf.resize(bits.div_ceil(8), 0);
        self.bits = bits;
        if self.bits <= old {
            self.clear_trailing_bits();
            return;
        }
        if val {
            let rem = old % 8;
            if rem != 0 {
                // set the rest of the partially-used old last byte
                self.buf[old_bytes - 1] |= 0xff >> rem;
            }
            let start = old.div_ceil(8);
            let end = self.num_bytes();
            self.buf[start..end].fill(0xff);
        }
        self.clear_trailing_bits();
    }

    /// `swap(rhs)` — swap contents with another bitfield.
    pub fn swap(&mut self, rhs: &mut Self) {
        std::mem::swap(self, rhs);
    }

    /// Iterate bits `[0, size)` in order (`begin()/end()`).
    pub fn iter(&self) -> Iter<'_> {
        Iter { bf: self, index: 0 }
    }

    /// `clear_trailing_bits()` — force bits `>= size()` in the last byte to 0.
    fn clear_trailing_bits(&mut self) {
        let rem = self.bits % 8;
        if rem != 0 {
            let last = self.num_bytes() - 1;
            self.buf[last] &= 0xff << (8 - rem);
        }
    }
}

impl FromIterator<bool> for Bitfield {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let mut bf = Self::new();
        for (i, bit) in iter.into_iter().enumerate() {
            if i == bf.bits {
                bf.buf.resize(bf.num_bytes() + 1, 0);
                bf.bits = i + 1;
            }
            if bit {
                bf.buf[i / 8] |= 0x80 >> (i % 8);
            }
        }
        bf
    }
}

/// `bitfield::const_iterator` — yields `bool` for each index in `[0, size)`.
pub struct Iter<'a> {
    bf: &'a Bitfield,
    index: usize,
}

impl Iterator for Iter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        if self.index >= self.bf.bits {
            return None;
        }
        let v = self.bf.get_bit(self.index);
        self.index += 1;
        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.bf.bits - self.index;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl std::fmt::Debug for Bitfield {
    /// `'1'`/`'0'` per bit, MSB-first — compact and wire-order readable.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Bitfield(")?;
        for bit in self.iter() {
            f.write_str(if bit { "1" } else { "0" })?;
        }
        f.write_str(")")
    }
}
