//! Port of `aux_/packet_buffer.hpp`/`.cpp` — a circular buffer of
//! uTP packets indexed by a 16-bit sequence number (`index_type`,
//! wraps at 0x10000). Slot indices wrap with `& (capacity - 1)`;
//! the window is `[m_first, m_first + m_capacity)` in wrapped space.
//!
//! Upstream stores `unique_ptr<packet[]>`; here `Vec<Option<T>>`.

use bitdaemon_common::compare_less_wrap;

/// `index_mask` — `0xffff`.
const INDEX_MASK: u32 = 0xffff;

/// `is_in_range(idx, first, capacity)` — wrapped window test.
fn is_in_range(idx: u32, first: u32, capacity: u32) -> bool {
    debug_assert!(idx <= INDEX_MASK);
    idx.wrapping_sub(first) & INDEX_MASK < capacity
}

/// `aux::packet_buffer`.
pub struct PacketBuffer<T> {
    /// `m_storage`.
    storage: Vec<Option<T>>,
    /// `m_size` — occupied slots.
    size: usize,
    /// `m_first` — lowest occupied index (window base).
    first: u32,
    /// `m_last` — one past the last used slot.
    last: u32,
}

impl<T> Default for PacketBuffer<T> {
    fn default() -> Self {
        Self {
            storage: Vec::new(),
            size: 0,
            first: 0,
            last: 0,
        }
    }
}

impl<T> PacketBuffer<T> {
    /// `packet_ptr insert(idx, value)` — returns the replaced value.
    pub fn insert(&mut self, idx: u32, value: T) -> Option<T> {
        debug_assert!(idx <= INDEX_MASK);
        if self.size != 0 {
            if !is_in_range(idx, self.first, self.capacity()) {
                self.grow_to_include(idx);
            }
            if compare_less_wrap(self.last, (idx + 1) & INDEX_MASK, INDEX_MASK) {
                self.last = (idx + 1) & INDEX_MASK;
            }
        } else {
            self.first = idx;
            self.last = (idx + 1) & INDEX_MASK;
        }
        if self.capacity() == 0 {
            self.reserve(16);
        }
        let slot = (idx & (self.capacity() - 1)) as usize;
        let old = self.storage[slot].replace(value);
        if self.size == 0 {
            self.first = idx;
        }
        // replacing doesn't grow the count
        if old.is_none() {
            self.size += 1;
        }
        debug_assert!(self.first <= INDEX_MASK);
        old
    }

    /// `at(idx)`.
    #[must_use]
    pub fn at(&self, idx: u32) -> Option<&T> {
        if !is_in_range(idx, self.first, self.capacity()) {
            return None;
        }
        self.storage[(idx & (self.capacity() - 1)) as usize].as_ref()
    }

    /// Mutable `at`.
    pub fn at_mut(&mut self, idx: u32) -> Option<&mut T> {
        if !is_in_range(idx, self.first, self.capacity()) {
            return None;
        }
        let mask = self.capacity() - 1;
        self.storage[(idx & mask) as usize].as_mut()
    }

    /// `remove(idx)` — shrinks the window from either edge.
    pub fn remove(&mut self, idx: u32) -> Option<T> {
        if !is_in_range(idx, self.first, self.capacity()) {
            return None;
        }
        let mask = self.capacity() - 1;
        let old = self.storage[(idx & mask) as usize].take();
        if old.is_some() {
            self.size -= 1;
            if self.size == 0 {
                self.last = self.first;
            }
        }
        if idx == self.first && self.size != 0 {
            self.first += 1;
            for _ in 0..self.capacity() {
                if self.storage[(self.first & mask) as usize].is_some() {
                    break;
                }
                self.first += 1;
            }
            self.first &= INDEX_MASK;
        }
        if (idx + 1) & INDEX_MASK == self.last && self.size != 0 {
            self.last = self.last.wrapping_sub(1) & INDEX_MASK;
            for _ in 0..self.capacity() {
                if self.storage[(self.last & mask) as usize].is_some() {
                    break;
                }
                self.last = self.last.wrapping_sub(1) & INDEX_MASK;
            }
            self.last = self.last.wrapping_add(1) & INDEX_MASK;
        }
        debug_assert!(self.first <= INDEX_MASK);
        old
    }

    /// `size()`.
    #[must_use]
    pub fn size(&self) -> usize {
        self.size
    }

    /// `empty()`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// `capacity()`.
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.storage.len() as u32
    }

    /// `cursor()` — `m_first`.
    #[must_use]
    pub fn cursor(&self) -> u32 {
        self.first
    }

    /// `span()` — `(m_last - m_first) & 0xffff`.
    #[must_use]
    pub fn span(&self) -> u32 {
        self.last.wrapping_sub(self.first) & INDEX_MASK
    }

    /// `reserve(size)` — capacity grows to the next power of two.
    pub fn reserve(&mut self, size: u32) {
        debug_assert!(size <= INDEX_MASK);
        let mut new_size = if self.capacity() == 0 {
            16
        } else {
            self.capacity()
        };
        while new_size < size {
            new_size <<= 1;
        }
        if new_size == self.capacity() {
            return;
        }
        let mut new_storage: Vec<Option<T>> = Vec::new();
        new_storage.resize_with(new_size as usize, || None);
        let old_mask = self.capacity().wrapping_sub(1);
        for i in self.first..self.first + self.capacity() {
            new_storage[(i & (new_size - 1)) as usize] =
                self.storage[(i & old_mask) as usize].take();
        }
        self.storage = new_storage;
    }

    /// `grow_to_include(idx)` — shift the window backward if cheap,
    /// else grow forward to fit `idx`.
    fn grow_to_include(&mut self, idx: u32) {
        if compare_less_wrap(idx, self.first, INDEX_MASK) {
            // idx comes before m_first — shift m_first back if there's
            // room, else grow
            let cap = self.capacity();
            let mut free_space = 0u32;
            let mut i = self.first.wrapping_sub(1) & (cap - 1);
            while i != self.first & (cap - 1) {
                if self.storage[(i & (cap - 1)) as usize].is_some() {
                    break;
                }
                free_space += 1;
                i = i.wrapping_sub(1) & (cap - 1);
            }
            if (self.first.wrapping_sub(idx) & INDEX_MASK) > free_space {
                self.reserve((self.first.wrapping_sub(idx) & INDEX_MASK) + cap - free_space);
            }
            self.first = idx;
        } else {
            // idx beyond the window: grow forward
            self.reserve((idx.wrapping_sub(self.first) & INDEX_MASK) + 1);
        }
    }
}
