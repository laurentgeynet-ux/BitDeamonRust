//! Port of `aux_/hash_picker.hpp`/`hash_picker.cpp` — v2 merkle hash
//! requests: picks which tree ranges to request from peers, validates
//! proofs, tracks verified blocks.
//!
//! Verbatim semantics: 512-piece piece-layer chunks, per-piece block
//! requests, 3 s `min_request_interval`, request-shape matching (both
//! shapes when `blocks_per_piece == 1`), `hashes_rejected` un-marking.

use std::time::{Duration, Instant};

use bitdaemon_common::Sha256Hash;
use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_common::types::{FileIndex, PieceIndex};
use bitdaemon_core::file_storage::FileStorage;
use bitdaemon_core::merkle::{
    merkle_get_parent, merkle_layer_start, merkle_num_layers, merkle_num_leafs,
    merkle_to_flat_index,
};
use bitdaemon_core::merkle_tree::{MerkleTree, SetBlockResult};

use crate::disk_buffer_pool::DEFAULT_BLOCK_SIZE;

/// `min_request_interval` — upstream `seconds(3)`.
const MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(3);

/// `aux::set_block_hash_result`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetBlockHashStatus {
    /// `success`.
    Success,
    /// `unknown` — node not yet in the tree.
    Unknown,
    /// `block_hash_failed`.
    BlockHashFailed,
    /// `piece_hash_failed` — conflict in a parent node.
    PieceHashFailed,
}

/// `set_block_hash_result` value.
#[derive(Debug, Clone, Copy)]
pub struct SetBlockHashResult {
    /// `status`.
    pub status: SetBlockHashStatus,
    /// `first_verified_block` — offset from the piece's first block.
    pub first_verified_block: i32,
    /// `num_verified`.
    pub num_verified: i32,
}

/// `add_hashes_result`.
#[derive(Debug, Default)]
pub struct AddHashesResult {
    /// `valid` — request + hashes were well-formed.
    pub valid: bool,
    /// `hash_failed` — `(piece, failed block indices)`.
    pub hash_failed: Vec<(PieceIndex, Vec<i32>)>,
    /// `hash_passed`.
    pub hash_passed: Vec<PieceIndex>,
}

impl AddHashesResult {
    fn invalid() -> Self {
        Self::default()
    }
}

/// `(file, node)` index pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeIndex {
    file: FileIndex,
    node: i32,
}

/// `aux::hash_request` — a range of merkle-tree hashes for one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HashRequest {
    /// `file`.
    pub file: FileIndex,
    /// `base` — tree level (0 = block/leaf level).
    pub base: i32,
    /// `index` — first hash at that level.
    pub index: i32,
    /// `count`.
    pub count: i32,
    /// `proof_layers`.
    pub proof_layers: i32,
}

/// `validate_hash_request` — verbatim.
#[must_use]
pub fn validate_hash_request(hr: &HashRequest, fs: &FileStorage) -> bool {
    // count cap 8192 → piece size cap 128 MiB
    if hr.file.0 < 0
        || hr.file >= fs.end_file()
        || hr.base < 0
        || hr.index < 0
        || hr.count <= 0
        || hr.count > 8192
        || hr.proof_layers < 0
    {
        return false;
    }
    if fs.pad_file_at(hr.file) || fs.file_size(hr.file) == 0 {
        return false;
    }

    let num_leafs = merkle_num_leafs(usize::try_from(fs.file_num_blocks(hr.file)).unwrap_or(0));
    let num_layers = merkle_num_layers(num_leafs) as i32;

    if hr.base >= num_layers {
        return false;
    }
    let level_size = (num_leafs as i32) >> hr.base;
    if hr.index >= level_size || hr.index + hr.count > level_size {
        return false;
    }
    hr.proof_layers < num_layers - hr.base
}

/// per-piece-hash request bookkeeping (`piece_hash_request`).
#[derive(Clone, Copy)]
struct PieceHashRequest {
    /// `last_request` — `None` = upstream `min_time()`.
    last_request: Option<Instant>,
    num_requests: i32,
    have: bool,
}

/// `piece_block_request`.
#[derive(Clone, Copy, PartialEq)]
struct PieceBlockRequest {
    file: FileIndex,
    /// piece from the start of the file.
    piece: i32,
    last_request: Option<Instant>,
    num_requests: i32,
}

/// `aux::hash_picker`. Holds references — callers keep the trees alive
/// (upstream stores references too).
pub struct HashPicker<'a> {
    files: &'a FileStorage,
    /// `m_merkle_trees`.
    trees: &'a mut Vec<MerkleTree>,
    piece_layer: i32,
    piece_tree_root_layer: i32,
    /// `m_piece_hash_requested` — per file, per 512-piece chunk.
    piece_hash_requested: Vec<Vec<PieceHashRequest>>,
    piece_block_requests: Vec<PieceBlockRequest>,
}

impl<'a> HashPicker<'a> {
    /// `hash_picker(files, trees)`.
    #[must_use]
    pub fn new(files: &'a FileStorage, trees: &'a mut Vec<MerkleTree>) -> Self {
        let piece_layer = merkle_num_layers(merkle_num_leafs(
            usize::try_from(files.piece_length()).unwrap_or(0) / DEFAULT_BLOCK_SIZE,
        )) as i32;
        let piece_tree_root_layer = piece_layer + merkle_num_layers(512) as i32;

        let mut piece_hash_requested: Vec<Vec<PieceHashRequest>> = vec![Vec::new(); trees.len()];
        for (fidx, tree) in trees.iter().enumerate() {
            let f = FileIndex(fidx as i32);
            if files.pad_file_at(f) {
                continue;
            }
            let v = tree.verified_leafs();
            if files.file_size(f) <= i64::from(files.piece_length()) {
                continue;
            }

            let num_chunks = ((files.file_num_pieces(f) + 511) / 512).max(0) as usize;
            piece_hash_requested[fidx] = vec![
                PieceHashRequest {
                    last_request: None,
                    num_requests: 0,
                    have: false
                };
                num_chunks
            ];

            let piece_layer_idx = merkle_num_layers(merkle_num_leafs(
                usize::try_from(files.file_num_blocks(f)).unwrap_or(0),
            )) as i32
                - piece_layer;
            let piece_layer_start = merkle_layer_start(piece_layer_idx as usize);

            for i in 0..piece_hash_requested[fidx].len() {
                let mut j = i * 512;
                loop {
                    if j == (i + 1) * 512 || j >= files.file_num_pieces(f) as usize {
                        piece_hash_requested[fidx][i].have = true;
                        break;
                    }
                    if (files.piece_length() as usize == DEFAULT_BLOCK_SIZE && !v.get_bit(j))
                        || (files.piece_length() as usize > DEFAULT_BLOCK_SIZE
                            && !tree.has_node(piece_layer_start + j))
                    {
                        break;
                    }
                    j += 1;
                }
            }
        }

        Self {
            files,
            trees,
            piece_layer,
            piece_tree_root_layer,
            piece_hash_requested,
            piece_block_requests: Vec::new(),
        }
    }

    /// `pick_hashes(pieces)` — choose the next range to request.
    #[must_use]
    pub fn pick_hashes(&mut self, pieces: &Bitfield) -> HashRequest {
        let now = Instant::now();

        // per-piece block requests first
        if let Some(pos) = self.piece_block_requests.iter().position(|e| {
            e.last_request
                .is_none_or(|t| now.duration_since(t) > MIN_REQUEST_INTERVAL)
        }) {
            let blocks_per_piece = i64::from(self.files.piece_length()) / DEFAULT_BLOCK_SIZE as i64;
            let file = self.piece_block_requests[pos].file;
            let piece = self.piece_block_requests[pos].piece;
            let first_block = piece as i64 * blocks_per_piece;
            let nidx = NodeIndex {
                file,
                node: (self.files.file_first_block_node(file) as i64 + first_block) as i32,
            };
            let ret = HashRequest {
                file,
                base: 0,
                index: first_block as i32,
                count: blocks_per_piece as i32,
                proof_layers: self.layers_to_verify(nidx),
            };
            let req = &mut self.piece_block_requests[pos];
            req.num_requests += 1;
            req.last_request = Some(now);
            // upstream re-sorts by num_requests (stable)
            self.piece_block_requests.sort_by_key(|r| r.num_requests);
            return ret;
        }

        for fidx in 0..self.piece_hash_requested.len() {
            let f = FileIndex(fidx as i32);
            if self.files.pad_file_at(f) || self.files.file_size(f) == 0 {
                continue;
            }
            let file_first_piece = self.files.file_offset(f) / i64::from(self.files.piece_length());
            let num_layers = self.file_num_layers(f);
            let piece_tree_root_layer = (num_layers - self.piece_tree_root_layer).max(0);
            let piece_tree_root_start = merkle_layer_start(piece_tree_root_layer as usize);

            for (i, r) in self.piece_hash_requested[fidx].iter_mut().enumerate() {
                if r.have
                    || r.last_request
                        .is_some_and(|t| now.duration_since(t) < MIN_REQUEST_INTERVAL)
                {
                    continue;
                }

                // only request chunks containing pieces we want
                let mut have = false;
                let upper = ((i + 1) * 512).min(self.files.file_num_pieces(f) as usize);
                for p in i * 512..upper {
                    if pieces.get_bit(file_first_piece as usize + p) {
                        have = true;
                        break;
                    }
                }
                if !have {
                    continue;
                }

                let piece_tree_root = (piece_tree_root_start + i) as i32;
                r.num_requests += 1;
                r.last_request = Some(now);

                let piece_tree_num_layers = num_layers - piece_tree_root_layer - self.piece_layer;
                let remaining = self.files.file_num_pieces(f) as usize - i * 512;
                return HashRequest {
                    file: f,
                    base: self.piece_layer,
                    index: (i * 512) as i32,
                    count: 512.min(merkle_num_leafs(remaining) as i32),
                    proof_layers: self.layers_to_verify(NodeIndex {
                        file: f,
                        node: piece_tree_root,
                    }) + piece_tree_num_layers,
                };
            }
        }
        HashRequest::default()
    }

    /// `matches_piece_layer_request`.
    fn matches_piece_layer_request(&self, req: &HashRequest) -> bool {
        let num_pieces = self.files.file_num_pieces(req.file);
        let unpadded = (req.count).min(num_pieces - req.index);
        req.base == self.piece_layer
            && req.index % 512 == 0
            && req.index < num_pieces
            && !self.piece_hash_requested[req.file.0 as usize].is_empty()
            && (req.count == 512 || (req.count <= 512 && unpadded == num_pieces - req.index))
    }

    /// `matches_block_request`.
    fn matches_block_request(&self, req: &HashRequest) -> bool {
        let blocks_per_piece = i64::from(self.files.piece_length()) / DEFAULT_BLOCK_SIZE as i64;
        let bpp = blocks_per_piece as i32;
        req.base == 0
            && bpp > 0
            && req.index % bpp == 0
            && req.index < self.files.file_num_blocks(req.file)
            && req.count == bpp
    }

    /// `add_hashes(req, hashes)` — verify and insert a proof.
    pub fn add_hashes(&mut self, req: &HashRequest, hashes: &[Sha256Hash]) -> AddHashesResult {
        debug_assert!(validate_hash_request(req, self.files));

        let leaf_count = merkle_num_leafs(req.count as usize);
        let base_num_layers = merkle_num_layers(leaf_count) as i32;
        let num_uncle_hashes = (req.proof_layers - base_num_layers + 1).max(0) as usize;

        if req.count as usize + num_uncle_hashes != hashes.len() {
            return AddHashesResult::invalid();
        }

        let blocks_per_piece =
            (i64::from(self.files.piece_length()) / DEFAULT_BLOCK_SIZE as i64) as i32;

        let valid_piece_layer = self.matches_piece_layer_request(req);
        let valid_block = self.matches_block_request(req);
        if !valid_piece_layer && !valid_block {
            return AddHashesResult::invalid();
        }

        let (hashes, uncle_hashes) = hashes.split_at(req.count as usize);
        debug_assert_eq!(uncle_hashes.len(), num_uncle_hashes);

        let base_layer_idx = self.file_num_layers(req.file) - req.base;
        if base_layer_idx <= 0 {
            return AddHashesResult::invalid();
        }

        let dest_start_idx = merkle_to_flat_index(base_layer_idx as usize, req.index as usize);
        let file_piece_offset = i64::from(self.files.piece_index_at_file(req.file).0);
        let dst_tree = &mut self.trees[req.file.0 as usize];
        if dst_tree.size() == 0 || dest_start_idx >= dst_tree.size() {
            return AddHashesResult::invalid();
        }

        let Some(results) =
            dst_tree.add_hashes(dest_start_idx, file_piece_offset, hashes, uncle_hashes)
        else {
            return AddHashesResult::invalid();
        };

        let ret = AddHashesResult {
            valid: true,
            hash_failed: results.failed,
            hash_passed: results.passed,
        };

        if valid_piece_layer {
            let mut i = req.index;
            while i < req.index + req.count {
                self.piece_hash_requested[req.file.0 as usize][(i / 512) as usize].have = true;
                i += 512;
            }
        }
        if valid_block {
            let resolved = PieceBlockRequest {
                file: req.file,
                piece: req.index / blocks_per_piece,
                last_request: None,
                num_requests: 0,
            };
            if let Some(pos) = self
                .piece_block_requests
                .iter()
                .position(|r| r.file == resolved.file && r.piece == resolved.piece)
            {
                self.piece_block_requests.remove(pos);
            }
        }
        ret
    }

    /// `set_block_hash(piece, offset, h)` — a downloaded block's hash
    /// (of data already on disk).
    pub fn set_block_hash(
        &mut self,
        piece: PieceIndex,
        offset: i32,
        h: &Sha256Hash,
    ) -> SetBlockHashResult {
        let f = self.files.file_index_at_piece(piece);
        if self.files.pad_file_at(f) {
            return SetBlockHashResult {
                status: SetBlockHashStatus::Success,
                first_verified_block: 0,
                num_verified: 0,
            };
        }
        let file_first_piece = self.files.piece_index_at_file(f);
        let block_offset = i64::from(piece.0) * i64::from(self.files.piece_length())
            + i64::from(offset)
            - self.files.file_offset(f);
        let block_index = (block_offset / DEFAULT_BLOCK_SIZE as i64) as usize;

        if h.is_all_zeros() {
            return SetBlockHashResult {
                status: SetBlockHashStatus::BlockHashFailed,
                first_verified_block: 0,
                num_verified: 0,
            };
        }

        let (result, leafs_index, leafs_size) = self.trees[f.0 as usize].set_block(block_index, *h);
        match result {
            SetBlockResult::Unknown => {
                return SetBlockHashResult {
                    status: SetBlockHashStatus::Unknown,
                    first_verified_block: 0,
                    num_verified: 0,
                };
            }
            SetBlockResult::BlockHashFailed => {
                return SetBlockHashResult {
                    status: SetBlockHashStatus::BlockHashFailed,
                    first_verified_block: 0,
                    num_verified: 0,
                };
            }
            _ => {}
        }
        let status = if result == SetBlockResult::HashFailed {
            SetBlockHashStatus::PieceHashFailed
        } else {
            SetBlockHashStatus::Success
        };
        let blocks_per_piece =
            (i64::from(self.files.piece_length()) / DEFAULT_BLOCK_SIZE as i64) as i32;
        let piece_rel = piece.0 - file_first_piece.0;
        SetBlockHashResult {
            status,
            first_verified_block: leafs_index as i32 - piece_rel * blocks_per_piece,
            num_verified: (leafs_size as i32)
                .min(self.files.file_num_pieces(f) * blocks_per_piece - leafs_index as i32),
        }
    }

    /// `hashes_rejected(req)` — un-mark so the range gets re-requested.
    pub fn hashes_rejected(&mut self, req: &HashRequest) {
        let blocks_per_piece =
            (i64::from(self.files.piece_length()) / DEFAULT_BLOCK_SIZE as i64) as i32;
        if self.matches_block_request(req) {
            let resolved = PieceBlockRequest {
                file: req.file,
                piece: req.index / blocks_per_piece,
                last_request: None,
                num_requests: 0,
            };
            if let Some(r) = self
                .piece_block_requests
                .iter_mut()
                .find(|r| r.file == resolved.file && r.piece == resolved.piece)
            {
                r.last_request = None;
                r.num_requests = (r.num_requests - 1).max(0);
                return;
            }
        }
        if self.matches_piece_layer_request(req) {
            let mut i = req.index;
            while i < req.index + req.count {
                let r = &mut self.piece_hash_requested[req.file.0 as usize][(i / 512) as usize];
                r.last_request = None;
                r.num_requests = (r.num_requests - 1).max(0);
                i += 512;
            }
        }
    }

    /// `verify_block_hashes(index)` — request the block layer of a
    /// piece (once).
    pub fn verify_block_hashes(&mut self, index: PieceIndex) {
        let fidx = self.files.file_index_at_piece(index);
        let piece = index.0 - self.files.piece_index_at_file(fidx).0;
        if self
            .piece_block_requests
            .iter()
            .any(|r| r.file == fidx && r.piece == piece)
        {
            return;
        }
        self.piece_block_requests.insert(
            0,
            PieceBlockRequest {
                file: fidx,
                piece,
                last_request: None,
                num_requests: 0,
            },
        );
    }

    /// `have_hash(index)` — do we know the piece-layer hash.
    #[must_use]
    pub fn have_hash(&self, index: PieceIndex) -> bool {
        let f = self.files.file_index_at_piece(index);
        if self.files.file_size(f) <= i64::from(self.files.piece_length()) {
            return true;
        }
        let file_first_piece = self.files.file_offset(f) / i64::from(self.files.piece_length());
        self.trees[f.0 as usize].has_node(
            (self.files.file_first_piece_node(f) as i64 + i64::from(index.0) - file_first_piece)
                as usize,
        )
    }

    /// `have_all(file)`.
    #[must_use]
    pub fn have_all_file(&self, file: FileIndex) -> bool {
        self.trees[file.0 as usize].is_complete()
    }

    /// `have_all()`.
    #[must_use]
    pub fn have_all(&self) -> bool {
        self.trees.iter().all(MerkleTree::is_complete)
    }

    /// `piece_verified(piece)`.
    #[must_use]
    pub fn piece_verified(&self, piece: PieceIndex) -> bool {
        let f = self.files.file_index_at_piece(piece);
        let file_first_piece =
            (self.files.file_offset(f) / i64::from(self.files.piece_length())) as i32;
        let blocks_per_piece =
            (i64::from(self.files.piece_length()) / DEFAULT_BLOCK_SIZE as i64) as i32;
        let block_offset = (piece.0 - file_first_piece) * blocks_per_piece;
        let blocks_in_piece = self.files.blocks_in_piece2(piece);
        self.trees[f.0 as usize].blocks_verified(block_offset as usize, blocks_in_piece as usize)
    }

    /// `piece_layer()`.
    #[must_use]
    pub fn piece_layer(&self) -> i32 {
        self.piece_layer
    }

    /// Borrowed merkle trees (`m_merkle_trees`).
    #[must_use]
    pub fn trees(&self) -> &[MerkleTree] {
        self.trees
    }

    /// `layers_to_verify` — proof layers needed for `idx`.
    fn layers_to_verify(&self, idx: NodeIndex) -> i32 {
        let mut idx = idx;
        // the root has no sibling — -1 = no proof required
        if idx.node == 0 {
            return -1;
        }
        let mut layers = 0;
        let file_internal_layers = merkle_num_layers(merkle_num_leafs(
            usize::try_from(self.files.file_num_pieces(idx.file)).unwrap_or(0),
        )) as i32
            - 1;
        let tree = &self.trees[idx.file.0 as usize];
        loop {
            idx.node = merkle_get_parent(idx.node as usize) as i32;
            if tree.has_node(idx.node as usize) {
                break;
            }
            layers += 1;
            if layers == file_internal_layers {
                return layers;
            }
        }
        layers
    }

    /// `file_num_layers`.
    fn file_num_layers(&self, idx: FileIndex) -> i32 {
        merkle_num_layers(merkle_num_leafs(
            usize::try_from(self.files.file_num_blocks(idx)).unwrap_or(0),
        )) as i32
    }
}
