//! Port of `aux_/precomputed_block_hashes.hpp`/`.cpp` — in-memory SHA-256
//! block hashes computed while hashing pieces, handed to the torrent to
//! seed its merkle tree without re-reading blocks.
//!
//! An all-zero hash doubles as the "not present" sentinel (a SHA-256 of a
//! non-empty block is never all zeros).

use std::collections::HashMap;
use std::sync::Mutex;

use bitdaemon_common::Sha256Hash;
use bitdaemon_common::types::PieceIndex;

/// `aux::precomputed_block_hashes`.
#[derive(Default)]
pub struct PrecomputedBlockHashes {
    inner: Mutex<HashMap<PieceIndex, Vec<Sha256Hash>>>,
}

impl PrecomputedBlockHashes {
    /// `new()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `store(piece, block, num_blocks, h)` — sizes the per-piece array
    /// on first store for the piece.
    pub fn store(&self, piece: PieceIndex, block: i32, num_blocks: i32, h: &Sha256Hash) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = g
            .entry(piece)
            .or_insert_with(|| vec![Sha256Hash::default(); num_blocks.max(0) as usize]);
        if let Some(slot) = entry.get_mut(block.max(0) as usize) {
            *slot = *h;
        }
    }

    /// `take(piece)` — remove and return all block hashes of a piece.
    /// Blocks never stored read as all-zero.
    #[must_use]
    pub fn take(&self, piece: PieceIndex) -> Vec<Sha256Hash> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&piece)
            .unwrap_or_default()
    }

    /// `take_block(piece, block)` — remove and return a single stored
    /// hash, or `None`.
    #[must_use]
    pub fn take_block(&self, piece: PieceIndex, block: i32) -> Option<Sha256Hash> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = g.get_mut(&piece)?;
        let idx = block.max(0) as usize;
        if idx >= entry.len() || entry[idx].is_all_zeros() {
            return None;
        }
        let h = entry[idx];
        entry[idx] = Sha256Hash::default();
        if entry.iter().all(Sha256Hash::is_all_zeros) {
            g.remove(&piece);
        }
        Some(h)
    }

    /// `drop(piece)` — discard all hashes of a piece
    /// (`clear_piece` / re-download).
    pub fn drop(&self, piece: PieceIndex) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&piece);
    }
}
