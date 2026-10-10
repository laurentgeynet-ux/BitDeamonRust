//! `aux_/merkle.hpp`/`merkle.cpp` — BEP-52 merkle tree math.
//!
//! Layout identical to upstream: a binary tree stored breadth-first,
//! `merkle_num_nodes(leafs) = 2*leafs - 1`, layer 0 = root.
//! Only the pure functions needed by metainfo parsing now; the full
//! `merkle_tree` type (sparse trees, proven-node APIs) lands with the
//! disk/hashing machinery in Phase 3.

use bitdaemon_common::Sha256Hash;
use sha2::{Digest as _, Sha256};

/// `aux::merkle_num_nodes`.
#[must_use]
pub fn merkle_num_nodes(leafs: usize) -> usize {
    debug_assert!(leafs > 0);
    debug_assert!(leafs <= usize::MAX / 2 + 1);
    debug_assert!(leafs.is_power_of_two());
    leafs * 2 - 1
}

/// `aux::merkle_first_leaf` — index of the first leaf in the tree array.
#[must_use]
pub fn merkle_first_leaf(num_leafs: usize) -> usize {
    debug_assert!(num_leafs.is_power_of_two());
    debug_assert!(num_leafs > 0);
    num_leafs - 1
}

/// `aux::merkle_num_leafs` — round `blocks` up to a power of two.
#[must_use]
pub fn merkle_num_leafs(blocks: usize) -> usize {
    debug_assert!(blocks > 0);
    debug_assert!(blocks <= i32::MAX as usize / 2);
    let mut ret = 1usize;
    while blocks > ret {
        ret <<= 1;
    }
    ret
}

/// `aux::merkle_num_layers`.
#[must_use]
pub fn merkle_num_layers(mut leaves: usize) -> usize {
    debug_assert!(leaves.is_power_of_two());
    let mut layers = 0;
    while leaves > 1 {
        layers += 1;
        leaves >>= 1;
    }
    layers
}

/// `aux::merkle_get_parent` — `(idx - 1) / 2`.
#[must_use]
pub fn merkle_get_parent(idx: usize) -> usize {
    (idx - 1) / 2
}

/// `aux::merkle_get_first_child`.
#[must_use]
pub fn merkle_get_first_child(idx: usize) -> usize {
    idx * 2 + 1
}

/// `aux::merkle_get_layer` — layer 0 = root.
#[must_use]
pub fn merkle_get_layer(idx: usize) -> usize {
    let mut layer = 1usize;
    while idx > (1 << layer) - 2 {
        layer += 1;
    }
    layer - 1
}

/// `aux::merkle_get_layer_offset`.
#[must_use]
pub fn merkle_get_layer_offset(idx: usize) -> usize {
    idx - ((1 << merkle_get_layer(idx)) - 1)
}

/// `aux::merkle_root` — root of the tree built from `leaves`, padding
/// missing leaves with `pad` (which itself is re-hashed per level).
/// `merkle_root_scratch` upstream; the scratch vec is an allocation
/// detail, kept internal.
#[must_use]
pub fn merkle_root(leaves: &[Sha256Hash], mut pad: Sha256Hash) -> Sha256Hash {
    let mut num_leafs = merkle_num_leafs(leaves.len());
    debug_assert!(num_leafs > 0);
    if num_leafs == 1 {
        return leaves[0];
    }

    let hash_pair = |a: &Sha256Hash, b: &Sha256Hash| {
        let mut h = Sha256::new();
        h.update(a.as_bytes());
        h.update(b.as_bytes());
        Sha256Hash::from_bytes(&h.finalize()).unwrap_or_default()
    };

    let mut cur: Vec<Sha256Hash> = leaves.to_vec();
    while num_leafs > 1 {
        let mut next = Vec::with_capacity(cur.len() / 2 + 1);
        for i in 0..cur.len() / 2 {
            next.push(hash_pair(&cur[i * 2], &cur[i * 2 + 1]));
        }
        if cur.len() & 1 == 1 {
            // odd leaf count — boundary hash spans payload + pad
            next.push(hash_pair(&cur[cur.len() - 1], &pad));
        }
        // re-hash the pad for the next level up
        pad = hash_pair(&pad, &pad);
        cur = next;
        num_leafs /= 2;
    }

    debug_assert_eq!(cur.len(), 1);
    cur[0]
}

/// `aux::merkle_pad` — pad hash for the level with `pieces` nodes when the
/// full tree has `blocks` leaves.
#[must_use]
pub fn merkle_pad(blocks: usize, pieces: usize) -> Sha256Hash {
    debug_assert!(blocks >= pieces);
    let mut ret = Sha256Hash::default();
    let mut pieces = pieces;
    while pieces < blocks {
        let mut h = Sha256::new();
        h.update(ret.as_bytes());
        h.update(ret.as_bytes());
        ret = Sha256Hash::from_bytes(&h.finalize()).unwrap_or_default();
        pieces *= 2;
    }
    ret
}

/// `merkle_tree::load_piece_layer` — the validation part: reconstruct the
/// file root from the piece-layer hashes and compare with `expected_root`.
/// `blocks_per_piece` must be a power of two (BEP-52).
#[must_use]
pub fn validate_piece_layer(
    piece_layer: &[Sha256Hash],
    blocks_per_piece: usize,
    expected_root: &Sha256Hash,
    num_blocks: usize,
) -> bool {
    if num_blocks == 1 {
        // trees that only have a root hash
        return piece_layer.len() == 1 && &piece_layer[0] == expected_root;
    }
    let pad = merkle_pad(blocks_per_piece, 1);
    merkle_root(piece_layer, pad) == *expected_root
}
