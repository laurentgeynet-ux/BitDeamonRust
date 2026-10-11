// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/storage_free_list.hpp` — free-list of
//! `storage_index_t` slots shared by the disk backends (upstream
//! `posix_disk_io`/`mmap_disk_io` reuse removed-torrent indexes).
//!
//! Upstream reserves capacity in `new_index` so `add` can never
//! allocate during torrent removal (removal must be noexcept). We
//! keep the same guarantee with `Vec::reserve` up front.

/// `storage_index_t` — index of a torrent's storage object.
pub use bitdaemon_common::types::StorageIndex;

/// `storage_free_list`.
#[derive(Debug, Default)]
pub struct StorageFreeList {
    free_slots: Vec<StorageIndex>,
}

impl StorageFreeList {
    /// `new_index(next)` — reuse a freed slot or take `next`.
    /// Pre-reserves `next + 1` so a later `add` never allocates.
    pub fn new_index(&mut self, next: StorageIndex) -> StorageIndex {
        self.free_slots
            .reserve(usize::try_from(next.0).unwrap_or(0) + 1);
        self.free_slots.pop().unwrap_or(next)
    }

    /// `add(i)` — return slot `i` to the pool (noexcept by design —
    /// see [`Self::new_index`]).
    pub fn add(&mut self, i: StorageIndex) {
        self.free_slots.push(i);
    }

    /// `size()` — number of pooled slots.
    #[must_use]
    pub fn size(&self) -> usize {
        self.free_slots.len()
    }

    /// `is_empty`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.free_slots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_and_fresh() {
        let mut l = StorageFreeList::default();
        assert_eq!(l.new_index(StorageIndex(0)), StorageIndex(0));
        assert_eq!(l.new_index(StorageIndex(1)), StorageIndex(1));
        l.add(StorageIndex(0));
        // freed slot popped (LIFO, upstream `pop_back`)
        assert_eq!(l.new_index(StorageIndex(2)), StorageIndex(0));
        assert_eq!(l.new_index(StorageIndex(2)), StorageIndex(2));
        l.add(StorageIndex(2));
        l.add(StorageIndex(1));
        assert_eq!(l.new_index(StorageIndex(3)), StorageIndex(1));
        assert_eq!(l.new_index(StorageIndex(3)), StorageIndex(2));
    }
}
