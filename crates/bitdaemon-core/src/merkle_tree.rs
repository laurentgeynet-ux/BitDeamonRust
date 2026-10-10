//! `aux_/merkle_tree.hpp`/`merkle_tree.cpp` — a file's BEP-52 merkle
//! tree with compact storage modes (empty / full / piece-layer /
//! block-layer).

use bitdaemon_common::Sha256Hash;
use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_common::types::PieceIndex;

use crate::merkle::*;

/// `mode_t` — storage density of the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// `uninitialized_tree`.
    #[default]
    Uninitialized,
    /// `empty_tree` — only the root is known.
    Empty,
    /// `full_tree` — every node stored; unknown = zeros.
    Full,
    /// `piece_layer` — only piece-level nodes stored.
    PieceLayer,
    /// `block_layer` — every block hash known+verified.
    BlockLayer,
}

/// `add_hashes_result_t`.
#[derive(Debug, Default)]
pub struct AddHashesResult {
    /// Pieces whose blocks all validated.
    pub passed: Vec<PieceIndex>,
    /// `(piece, failed_block_indices)` — empty vec = unknown which block.
    pub failed: Vec<(PieceIndex, Vec<i32>)>,
}

/// `set_block_result`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetBlockResult {
    /// Block hash verified.
    Ok,
    /// Not enough context to verify yet.
    Unknown,
    /// Subtree root mismatch — all its block hashes were cleared.
    HashFailed,
    /// Block was already verified and `h` mismatched.
    BlockHashFailed,
}

/// `aux::merkle_tree`.
#[derive(Debug, Clone)]
pub struct MerkleTree {
    root: Sha256Hash,
    tree: Vec<Sha256Hash>,
    block_verified: Bitfield,
    num_blocks: usize,
    blocks_per_piece_log: u8,
    mode: Mode,
}

impl Default for MerkleTree {
    fn default() -> Self {
        Self {
            root: Sha256Hash::default(),
            tree: Vec::new(),
            block_verified: Bitfield::new(),
            num_blocks: 0,
            blocks_per_piece_log: 0,
            mode: Mode::Uninitialized,
        }
    }
}

fn log2p1(v: u32) -> u8 {
    // aux::log2p1 — floor(log2(v)) + (v==0 ? 0 : 1)... upstream:
    // number of bits needed; for powers of two it's log2(v)
    debug_assert!(v > 0);
    31 - v.leading_zeros() as u8
}

impl MerkleTree {
    /// `merkle_tree(num_blocks, blocks_per_piece, root)`.
    #[must_use]
    pub fn new(num_blocks: usize, blocks_per_piece: usize, root: Sha256Hash) -> Self {
        debug_assert!(blocks_per_piece.is_power_of_two());
        Self {
            root,
            tree: Vec::new(),
            block_verified: Bitfield::new(),
            num_blocks,
            blocks_per_piece_log: log2p1(blocks_per_piece as u32),
            mode: Mode::Empty,
        }
    }

    /// `root()`.
    #[must_use]
    pub fn root(&self) -> Sha256Hash {
        self.root
    }

    /// `mode()`.
    #[must_use]
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// `size()` — flat node count of the full tree.
    #[must_use]
    pub fn size(&self) -> usize {
        merkle_num_nodes(merkle_num_leafs(self.num_blocks))
    }

    /// `num_leafs()`.
    #[must_use]
    pub fn num_leafs(&self) -> usize {
        merkle_num_leafs(self.num_blocks)
    }

    /// `num_pieces()`.
    #[must_use]
    pub fn num_pieces(&self) -> usize {
        let ps = self.blocks_per_piece();
        (self.num_blocks + ps - 1) >> self.blocks_per_piece_log
    }

    /// `blocks_per_piece()`.
    #[must_use]
    pub fn blocks_per_piece(&self) -> usize {
        1 << self.blocks_per_piece_log
    }

    /// `piece_levels()` — tree levels per piece (0 → block==piece layer).
    #[must_use]
    pub fn piece_levels(&self) -> u8 {
        self.blocks_per_piece_log
    }

    fn block_layer_start(&self) -> usize {
        merkle_first_leaf(merkle_num_leafs(self.num_blocks))
    }

    fn piece_layer_start(&self) -> usize {
        merkle_first_leaf(merkle_num_leafs(self.num_pieces()))
    }

    /// `clear()` — reset to the empty tree.
    pub fn clear(&mut self) {
        self.tree = Vec::new();
        self.block_verified = Bitfield::new();
        self.mode = Mode::Empty;
    }

    /// `load_verified_bits`.
    pub fn load_verified_bits(&mut self, verified: &Bitfield) {
        for (block_index, i) in
            (self.block_layer_start()..).zip(0..verified.size().min(self.num_blocks))
        {
            if verified.get_bit(i) && self.has_node(block_index) {
                self.block_verified.set_bit(i);
            }
        }
    }

    /// `load_tree` — install a full tree (validated against the root).
    pub fn load_tree(&mut self, t: &[Sha256Hash], verified: &Bitfield) {
        if t.is_empty() {
            return;
        }
        if self.root() != t[0] {
            return;
        }
        if self.size() != t.len() {
            return;
        }
        if t.len() == 1 {
            self.optimize_storage();
            return;
        }
        self.allocate_full();
        let root = self.root();
        merkle_validate_copy(t, &mut self.tree, &root, &mut self.block_verified);
        let verified_clone = verified.clone();
        self.load_verified_bits(&verified_clone);
        self.optimize_storage();
        self.optimize_storage_piece_layer();
    }

    /// `load_sparse_tree` — masked nodes + verified bits.
    pub fn load_sparse_tree(&mut self, t: &[Sha256Hash], mask: &Bitfield, verified: &Bitfield) {
        let mask_size = mask.size().min(self.size());
        let first_block = self.block_layer_start();
        let end_block = first_block + self.num_blocks;

        // whole block layer present → go straight to block_layer mode
        if first_block < mask_size
            && end_block <= mask_size
            && (first_block..end_block).all(|i| mask.get_bit(i))
        {
            let block_index = (0..first_block).filter(|&i| mask.get_bit(i)).count();
            if t.len() < block_index + self.num_blocks {
                self.clear();
                return;
            }
            self.tree = t[block_index..block_index + self.num_blocks].to_vec();
            self.mode = Mode::BlockLayer;
            if merkle_root(&self.tree, Sha256Hash::default()) != self.root() {
                self.clear();
            }
            return;
        }

        if self.blocks_per_piece_log > 0 {
            let first_piece = self.piece_layer_start();
            let piece_count = self.num_pieces();
            let end_piece = first_piece + piece_count;
            // whole piece layer, nothing below → piece_layer mode
            if first_piece < mask_size
                && end_piece <= mask_size
                && (first_piece..end_piece).all(|i| mask.get_bit(i))
                && (end_piece..mask_size).all(|i| !mask.get_bit(i))
            {
                let piece_index = (0..first_piece).filter(|&i| mask.get_bit(i)).count();
                if t.len() < piece_index + piece_count {
                    self.clear();
                    return;
                }
                self.tree = t[piece_index..piece_index + piece_count].to_vec();
                self.mode = Mode::PieceLayer;
                let pad = merkle_pad(1 << self.blocks_per_piece_log, 1);
                if merkle_root(&self.tree, pad) != self.root() {
                    self.clear();
                }
                return;
            }
        }

        if t.is_empty() || (0..mask_size).all(|i| !mask.get_bit(i)) {
            self.clear();
            return;
        }

        self.allocate_full();
        let mut cursor = 0;
        for i in 0..mask_size {
            if !mask.get_bit(i) {
                continue;
            }
            if cursor >= t.len() {
                break;
            }
            self.tree[i] = t[cursor];
            cursor += 1;
        }
        merkle_fill_partial_tree(&mut self.tree);
        if self.tree[0] != self.root() {
            self.clear();
            return;
        }
        let verified_clone = verified.clone();
        self.load_verified_bits(&verified_clone);
        self.optimize_storage();
    }

    /// `get_piece_layer`.
    #[must_use]
    pub fn get_piece_layer(&self) -> Vec<Sha256Hash> {
        match self.mode {
            Mode::Uninitialized | Mode::Empty => Vec::new(),
            Mode::Full => {
                let s = self.piece_layer_start();
                self.tree[s..s + self.num_pieces()].to_vec()
            }
            Mode::PieceLayer => self.tree.clone(),
            Mode::BlockLayer => {
                let bpp = self.blocks_per_piece();
                let mut ret = Vec::with_capacity(self.num_pieces());
                let mut scratch = Vec::new();
                let mut b = 0;
                while b < self.tree.len() {
                    let leafs = &self.tree[b..(b + bpp).min(self.tree.len())];
                    ret.push(merkle_root_scratch(
                        leafs,
                        bpp,
                        Sha256Hash::default(),
                        &mut scratch,
                    ));
                    b += bpp;
                }
                ret
            }
        }
    }

    /// `load_piece_layer` — validate + adopt a raw piece layer.
    pub fn load_piece_layer(&mut self, piece_layer: &[u8]) -> bool {
        if self.mode == Mode::BlockLayer {
            return true;
        }
        let npieces = self.num_pieces();
        if piece_layer.len() != npieces * 32 {
            return false;
        }
        if self.num_blocks == 1 {
            let h = Sha256Hash::from_bytes(&piece_layer[..32]).unwrap_or_default();
            if h != self.root() {
                return false;
            }
            self.mode = Mode::Empty;
            self.tree.clear();
            self.block_verified = Bitfield::new();
            return true;
        }
        let pad = merkle_pad(1 << self.blocks_per_piece_log, 1);
        let pieces: Vec<Sha256Hash> = piece_layer
            .chunks_exact(32)
            .map(|c| Sha256Hash::from_bytes(c).unwrap_or_default())
            .collect();
        if merkle_root(&pieces, pad) != self.root() {
            return false;
        }
        self.mode = if self.blocks_per_piece_log == 0 {
            Mode::BlockLayer
        } else {
            Mode::PieceLayer
        };
        self.tree = pieces;
        self.block_verified = Bitfield::new();
        true
    }

    /// `has_node`.
    #[must_use]
    pub fn has_node(&self, idx: usize) -> bool {
        debug_assert!(idx < self.size());
        match self.mode {
            Mode::Uninitialized => false,
            Mode::Empty => idx == 0,
            Mode::Full => !self.tree[idx].is_all_zeros(),
            Mode::PieceLayer => idx < merkle_get_first_child_at(self.piece_layer_start(), 1),
            Mode::BlockLayer => idx < self.block_layer_start() + self.num_blocks,
        }
    }

    /// `compare_node`.
    #[must_use]
    pub fn compare_node(&self, idx: usize, h: &Sha256Hash) -> bool {
        match self.mode {
            Mode::Uninitialized => h.is_all_zeros(),
            Mode::Empty => {
                if idx == 0 {
                    self.root() == *h
                } else {
                    h.is_all_zeros()
                }
            }
            Mode::Full => self.tree[idx] == *h,
            Mode::PieceLayer => {
                let first = self.piece_layer_start();
                let pieces_end = first + self.num_pieces();
                let piece_layer_size = merkle_num_leafs(self.num_pieces());
                let end = first + piece_layer_size;
                if idx >= end {
                    return h.is_all_zeros();
                }
                if idx >= pieces_end {
                    return *h == merkle_pad(1 << self.blocks_per_piece_log, 1);
                }
                if idx >= first {
                    return self.tree[idx - first] == *h;
                }
                self.get(idx) == *h
            }
            Mode::BlockLayer => {
                let first = self.block_layer_start();
                let end = first + self.num_blocks;
                if idx >= end {
                    return h.is_all_zeros();
                }
                if idx >= first {
                    return self.tree[idx - first] == *h;
                }
                self.get(idx) == *h
            }
        }
    }

    /// `operator[]`.
    #[must_use]
    pub fn get(&self, idx: usize) -> Sha256Hash {
        let mut scratch = Vec::new();
        self.get_impl(idx, &mut scratch)
    }

    fn get_impl(&self, mut idx: usize, scratch: &mut Vec<Sha256Hash>) -> Sha256Hash {
        match self.mode {
            Mode::Uninitialized => Sha256Hash::default(),
            Mode::Empty => {
                if idx == 0 {
                    self.root()
                } else {
                    Sha256Hash::default()
                }
            }
            Mode::Full => self.tree[idx],
            Mode::PieceLayer | Mode::BlockLayer => {
                let start = if self.mode == Mode::PieceLayer {
                    self.piece_layer_start()
                } else {
                    self.block_layer_start()
                };
                if self.mode == Mode::PieceLayer && idx >= merkle_get_first_child_at(start, 1) {
                    return Sha256Hash::default();
                }
                let mut layer_size = 1usize;
                while idx < start {
                    idx = merkle_get_first_child(idx);
                    layer_size *= 2;
                }
                idx -= start;
                if idx >= self.tree.len() {
                    return merkle_pad(
                        if self.mode == Mode::PieceLayer {
                            layer_size << self.blocks_per_piece_log
                        } else {
                            layer_size
                        },
                        1,
                    );
                }
                let pad = if self.mode == Mode::PieceLayer {
                    merkle_pad(1 << self.blocks_per_piece_log, 1)
                } else {
                    Sha256Hash::default()
                };
                let layer = &self.tree[idx..(idx + layer_size).min(self.tree.len())];
                merkle_root_scratch(layer, layer_size, pad, scratch)
            }
        }
    }

    /// `build_vector` — the full flat tree.
    #[must_use]
    pub fn build_vector(&self) -> Vec<Sha256Hash> {
        if self.mode == Mode::Uninitialized {
            return Vec::new();
        }
        let mut ret = vec![Sha256Hash::default(); self.size()];
        match self.mode {
            Mode::Uninitialized | Mode::Empty => {}
            Mode::Full => ret.copy_from_slice(&self.tree),
            Mode::PieceLayer => {
                let piece_layer_size = merkle_num_leafs(self.num_pieces());
                let pad = merkle_pad(1 << self.blocks_per_piece_log, 1);
                let start = merkle_first_leaf(piece_layer_size);
                ret[start..start + self.tree.len()].copy_from_slice(&self.tree);
                for h in &mut ret[start + self.tree.len()..start + piece_layer_size] {
                    *h = pad;
                }
                let nodes = merkle_num_nodes(piece_layer_size);
                merkle_fill_tree(&mut ret[..nodes], piece_layer_size);
            }
            Mode::BlockLayer => {
                let num_leafs = merkle_num_leafs(self.num_blocks);
                let start = merkle_first_leaf(num_leafs);
                ret[start..start + self.tree.len()].copy_from_slice(&self.tree);
                merkle_fill_tree(&mut ret, num_leafs);
            }
        }
        ret[0] = self.root();
        ret
    }

    /// `build_sparse_vector` — `(hashes, mask)`.
    #[must_use]
    pub fn build_sparse_vector(&self) -> (Vec<Sha256Hash>, Bitfield) {
        if self.mode == Mode::Uninitialized {
            return (Vec::new(), Bitfield::new());
        }
        let mut mask = Bitfield::with_size(self.size());
        let mut ret = Vec::new();
        match self.mode {
            Mode::Uninitialized | Mode::Empty => {}
            Mode::Full => {
                for (i, h) in self.tree.iter().enumerate() {
                    if h.is_all_zeros() {
                        continue;
                    }
                    ret.push(*h);
                    mask.set_bit(i);
                }
            }
            Mode::PieceLayer => {
                let piece_layer_size = merkle_num_leafs(self.num_pieces());
                let start = merkle_first_leaf(piece_layer_size);
                for i in start..start + self.tree.len() {
                    mask.set_bit(i);
                }
                ret = self.tree.clone();
            }
            Mode::BlockLayer => {
                let num_leafs = merkle_num_leafs(self.num_blocks);
                let start = merkle_first_leaf(num_leafs);
                for i in start..start + self.tree.len() {
                    mask.set_bit(i);
                }
                ret = self.tree.clone();
            }
        }
        (ret, mask)
    }

    /// `verified_leafs`.
    #[must_use]
    pub fn verified_leafs(&self) -> Bitfield {
        match self.mode {
            Mode::Uninitialized | Mode::Empty => {
                Bitfield::with_value(self.num_blocks, self.num_blocks == 1)
            }
            Mode::PieceLayer => Bitfield::with_value(self.num_blocks, self.piece_levels() == 0),
            Mode::BlockLayer => Bitfield::with_value(self.num_blocks, true),
            Mode::Full => self.block_verified.clone(),
        }
    }

    /// `is_complete`.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        match self.mode {
            Mode::Uninitialized => false,
            Mode::Empty => self.num_blocks == 1,
            Mode::PieceLayer => self.piece_levels() == 0,
            Mode::BlockLayer => true,
            Mode::Full => self.block_verified.size() > 0 && self.block_verified.all_set(),
        }
    }

    /// `blocks_verified`.
    #[must_use]
    pub fn blocks_verified(&self, block_idx: usize, num_blocks: usize) -> bool {
        debug_assert!(num_blocks > 0);
        match self.mode {
            Mode::Uninitialized => false,
            Mode::Empty => self.num_blocks == 1,
            Mode::PieceLayer => self.piece_levels() == 0,
            Mode::BlockLayer => true,
            Mode::Full => {
                (block_idx..block_idx + num_blocks).all(|i| self.block_verified.get_bit(i))
            }
        }
    }

    fn allocate_full(&mut self) {
        if self.mode == Mode::Full {
            return;
        }
        debug_assert!(self.mode != Mode::BlockLayer);
        self.tree = self.build_vector();
        self.mode = Mode::Full;
        self.block_verified = Bitfield::with_size(self.num_blocks);
        if self.num_blocks == 1 {
            self.block_verified.set_bit(0);
        }
    }

    fn optimize_storage(&mut self) {
        if self.mode != Mode::Full {
            return;
        }
        if self.num_blocks == 1 {
            self.tree = Vec::new();
            self.mode = Mode::Empty;
            self.block_verified = Bitfield::new();
            return;
        }
        let start = self.block_layer_start();
        if self.block_verified.all_set() {
            self.tree = self.tree[start..start + self.num_blocks].to_vec();
            self.mode = Mode::BlockLayer;
            self.block_verified = Bitfield::new();
        }
    }

    fn optimize_storage_piece_layer(&mut self) {
        if self.mode != Mode::Full {
            return;
        }
        let piece_layer_size = merkle_num_leafs(self.num_pieces());
        let block_start = self.block_layer_start();
        if self.blocks_per_piece_log > 0
            && merkle_validate_single_layer(&self.tree[..merkle_num_nodes(piece_layer_size)])
            && self.tree[block_start..]
                .iter()
                .all(Sha256Hash::is_all_zeros)
        {
            let start = self.piece_layer_start();
            self.tree = self.tree[start..start + self.num_pieces()].to_vec();
            self.mode = Mode::PieceLayer;
            self.block_verified = Bitfield::new();
        }
    }

    /// `add_hashes` — insert leaf hashes + uncle proofs at
    /// `dest_start_idx`. Returns passed/failed pieces.
    pub fn add_hashes(
        &mut self,
        dest_start_idx: usize,
        file_piece_offset: i64,
        hashes: &[Sha256Hash],
        uncle_hashes: &[Sha256Hash],
    ) -> Option<AddHashesResult> {
        let mut ret = AddHashesResult::default();
        if self.mode == Mode::BlockLayer {
            return Some(ret);
        }
        self.allocate_full();

        let leaf_count = merkle_num_leafs(hashes.len());
        let dest_layer_size = 1 << merkle_get_layer(dest_start_idx);
        let dest_layer_offset = merkle_get_layer_offset(dest_start_idx);
        if dest_layer_offset + leaf_count > dest_layer_size {
            return None;
        }

        let mut tree = vec![Sha256Hash::default(); merkle_num_nodes(leaf_count)];
        let leaf_base = tree.len() - leaf_count;
        tree[leaf_base..leaf_base + hashes.len()].copy_from_slice(hashes);

        // end-of-file pad leaves
        if leaf_count > hashes.len() {
            let leaf_layer_size = self.num_leafs();
            let max_proof_layers =
                merkle_num_layers(leaf_layer_size) - merkle_num_layers(leaf_count);
            if uncle_hashes.len() > max_proof_layers {
                return None;
            }
            let insert_layer_size = leaf_count << uncle_hashes.len();
            if leaf_layer_size != insert_layer_size {
                let pad = merkle_pad(leaf_layer_size, insert_layer_size);
                for i in hashes.len()..leaf_count {
                    tree[leaf_base + i] = pad;
                }
            }
        }
        merkle_fill_tree(&mut tree, leaf_count);

        let base_num_layers = merkle_num_layers(leaf_count);
        let insert_root_idx = dest_start_idx >> base_num_layers;

        let num_leafs = merkle_num_leafs(self.num_blocks);
        let first_leaf = merkle_first_leaf(num_leafs);

        let insert_root_already_known = self.has_node(insert_root_idx);
        let sibling_already_known = !uncle_hashes.is_empty()
            && insert_root_idx >= first_leaf
            && insert_root_idx > 0
            && insert_root_idx - first_leaf < self.num_blocks
            && self.has_node(merkle_get_sibling(insert_root_idx));

        if !merkle_validate_and_insert_proofs(
            &mut self.tree,
            insert_root_idx,
            tree[0],
            uncle_hashes,
        ) {
            return None;
        }

        if !uncle_hashes.is_empty()
            && insert_root_idx > 0
            && insert_root_idx >= first_leaf
            && insert_root_idx - first_leaf < self.num_blocks
        {
            let sibling_idx = merkle_get_sibling(insert_root_idx);
            let sibling_block = sibling_idx - first_leaf;
            if sibling_block < self.num_blocks {
                let already = self.block_verified.get_bit(sibling_block);
                self.block_verified.set_bit(sibling_block);
                if self.blocks_per_piece_log == 0 && !already && sibling_already_known {
                    let piece = PieceIndex(sibling_block as i32 + file_piece_offset as i32);
                    if ret.passed.last() != Some(&piece) {
                        ret.passed.push(piece);
                    }
                }
            }
        }

        let mut source_cursor = tree.len() - leaf_count;
        let mut dest_cursor = dest_start_idx;
        let base = self.piece_levels() as usize;

        let mut current_piece = PieceIndex(-1);
        let mut current_piece_matched = 0usize;

        let mut layer_size = leaf_count;
        while layer_size != 0 {
            for i in 0..layer_size {
                let dst_idx = dest_cursor + i;
                let src_idx = source_cursor + i;
                let known = if dst_idx == insert_root_idx {
                    insert_root_already_known
                } else {
                    self.has_node(dst_idx)
                };
                if known {
                    if self.tree[dst_idx] != tree[src_idx] {
                        debug_assert!(dst_idx >= first_leaf);
                        let pos = dst_idx - first_leaf;
                        let block = (pos & ((1 << self.blocks_per_piece_log) - 1)) as i32;
                        let piece = PieceIndex(
                            (pos >> self.blocks_per_piece_log) as i32 + file_piece_offset as i32,
                        );
                        if let Some(last) = ret.failed.last_mut() {
                            if last.0 == piece {
                                last.1.push(block);
                            } else {
                                ret.failed.push((piece, vec![block]));
                            }
                        } else {
                            ret.failed.push((piece, vec![block]));
                        }
                        self.tree[dst_idx] = Sha256Hash::default();
                    } else if dst_idx >= first_leaf {
                        let pos = dst_idx - first_leaf;
                        let block = pos & ((1 << self.blocks_per_piece_log) - 1);
                        let piece = PieceIndex(
                            (pos >> self.blocks_per_piece_log) as i32 + file_piece_offset as i32,
                        );
                        if piece != current_piece {
                            current_piece = piece;
                            current_piece_matched = 0;
                        }
                        let piece_block_start = pos - block;
                        let piece_blocks =
                            (1usize << base).min(self.num_blocks - piece_block_start);
                        current_piece_matched += 1;
                        if current_piece_matched == piece_blocks {
                            ret.passed.push(piece);
                        }
                    }
                }
                if dst_idx >= first_leaf && dst_idx - first_leaf < self.num_blocks {
                    self.block_verified.set_bit(dst_idx - first_leaf);
                }
                self.tree[dst_idx] = tree[src_idx];
            }
            if layer_size == 1 {
                break;
            }
            dest_cursor = merkle_get_parent(dest_cursor);
            source_cursor = merkle_get_parent(source_cursor);
            layer_size /= 2;
        }

        // piece-layer insertions may let us verify previously-received
        // block hashes
        let first_piece_idx = self.piece_layer_start();
        if base != 0
            && dest_start_idx >= first_piece_idx
            && dest_start_idx < first_piece_idx + self.num_pieces()
        {
            let blocks_in_piece = 1 << base;
            for (i, hash) in hashes.iter().enumerate() {
                let piece = dest_start_idx + i;
                if piece - first_piece_idx >= self.num_pieces() {
                    break;
                }
                let block_idx = merkle_get_first_child_at(piece, base);
                let block_end_idx = (block_idx + blocks_in_piece).min(first_leaf + self.num_blocks);
                if self.tree[block_idx..block_end_idx]
                    .iter()
                    .any(Sha256Hash::is_all_zeros)
                {
                    continue;
                }
                merkle_fill_tree_at(&mut self.tree, blocks_in_piece, block_idx);
                if self.tree[piece] != *hash {
                    merkle_clear_tree(&mut self.tree, blocks_in_piece, block_idx);
                    self.tree[piece] = *hash;
                    ret.failed.push((
                        PieceIndex((piece - first_piece_idx) as i32 + file_piece_offset as i32),
                        Vec::new(),
                    ));
                } else {
                    ret.passed.push(PieceIndex(
                        (piece - first_piece_idx) as i32 + file_piece_offset as i32,
                    ));
                    let leafs_start = block_idx - self.block_layer_start();
                    let leafs_end = (leafs_start + blocks_in_piece).min(self.num_blocks);
                    for k in leafs_start..leafs_end {
                        self.block_verified.set_bit(k);
                    }
                }
            }
        }

        self.optimize_storage();
        Some(ret)
    }

    /// `set_block` — verify a downloaded block hash against the tree.
    /// Returns `(result, leafs_start, leafs_size)` — the affected subtree.
    pub fn set_block(
        &mut self,
        block_index: usize,
        h: Sha256Hash,
    ) -> (SetBlockResult, usize, usize) {
        debug_assert!(block_index < self.num_blocks);
        let num_leafs = merkle_num_leafs(self.num_blocks);
        let first_leaf = merkle_first_leaf(num_leafs);
        let block_tree_index = first_leaf + block_index;

        if self.blocks_verified(block_index, 1) {
            if self.compare_node(block_tree_index, &h) {
                return (SetBlockResult::Ok, block_index, 1);
            }
            return (SetBlockResult::BlockHashFailed, block_index, 1);
        }

        self.allocate_full();
        self.tree[block_tree_index] = h;

        let (leafs_start, leafs_size, root_index) =
            merkle_find_known_subtree(&self.tree, block_index, self.num_blocks);
        if self.tree[root_index].is_all_zeros() {
            return (SetBlockResult::Unknown, leafs_start, leafs_size);
        }
        let root = self.tree[root_index];
        merkle_fill_tree_at(&mut self.tree, leafs_size, first_leaf + leafs_start);
        if root != self.tree[root_index] {
            merkle_clear_tree(&mut self.tree, leafs_size, first_leaf + leafs_start);
            self.tree[root_index] = root;
            return (SetBlockResult::HashFailed, leafs_start, leafs_size);
        }
        let leafs_end = self.num_blocks.min(leafs_start + leafs_size);
        for i in leafs_start..leafs_end {
            self.block_verified.set_bit(i);
        }
        if block_index == self.num_blocks - 1 || !self.tree[block_tree_index + 1].is_all_zeros() {
            self.optimize_storage();
        }
        (SetBlockResult::Ok, leafs_start, leafs_size)
    }

    /// `get_hashes` — `count` node hashes at layer `base`/`index` plus
    /// `proof_layers` uncle proofs. `None` if any required node is
    /// unknown.
    #[must_use]
    pub fn get_hashes(
        &self,
        base: usize,
        index: usize,
        count: usize,
        proof_layers: usize,
    ) -> Option<Vec<Sha256Hash>> {
        let base_layer_idx = merkle_num_layers(self.num_leafs()) - base;
        let base_start_idx = merkle_to_flat_index(base_layer_idx, index);
        let layer_start_idx = base_start_idx;

        let mut ret = Vec::with_capacity(count);
        let mut scratch = Vec::new();

        if base == 0 && self.mode == Mode::BlockLayer {
            let blocks_end = (index + count).min(self.num_blocks);
            for i in index..blocks_end {
                ret.push(self.tree[i]);
            }
            ret.resize(count, Sha256Hash::default());
        } else {
            for i in layer_start_idx..layer_start_idx + count {
                if (base != 0 || i < self.num_blocks + layer_start_idx - index) && !self.has_node(i)
                {
                    return None;
                }
                ret.push(self.get_impl(i, &mut scratch));
            }
        }

        let implied_layers = merkle_num_layers(merkle_num_leafs(count));
        let mut proof_idx = layer_start_idx;
        for _ in 0..implied_layers {
            proof_idx = merkle_get_parent(proof_idx);
        }
        let mut i = implied_layers;
        while i <= proof_layers {
            let sibling = merkle_get_sibling(proof_idx);
            let sibling_is_real =
                i > 0 || base != 0 || sibling < self.num_blocks + layer_start_idx - index;
            if !self.has_node(proof_idx) || (sibling_is_real && !self.has_node(sibling)) {
                return None;
            }
            ret.push(if sibling_is_real {
                self.get_impl(sibling, &mut scratch)
            } else {
                Sha256Hash::default()
            });
            proof_idx = merkle_get_parent(proof_idx);
            i += 1;
        }
        Some(ret)
    }
}
