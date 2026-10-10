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

/// `merkle_layer_start` — flat index of the first node in `layer`.
#[must_use]
pub fn merkle_layer_start(layer: usize) -> usize {
    (1 << layer) - 1
}

/// `merkle_to_flat_index`.
#[must_use]
pub fn merkle_to_flat_index(layer: usize, offset: usize) -> usize {
    merkle_layer_start(layer) + offset
}

/// `merkle_get_sibling`.
#[must_use]
pub fn merkle_get_sibling(idx: usize) -> usize {
    debug_assert!(idx > 0);
    if idx & 1 == 0 { idx - 1 } else { idx + 1 }
}

/// `merkle_get_first_child(idx, depth)`.
#[must_use]
pub fn merkle_get_first_child_at(idx: usize, depth: usize) -> usize {
    idx * (1 << depth) + (1 << depth) - 1
}

fn sha256_pair(l: &Sha256Hash, r: &Sha256Hash) -> Sha256Hash {
    let mut h = Sha256::new();
    h.update(l.as_bytes());
    h.update(r.as_bytes());
    Sha256Hash::from_bytes(&h.finalize()).unwrap_or_default()
}

/// `merkle_fill_tree(tree, num_leafs, level_start)` — compute parent
/// hashes for the subtree whose leaves start at `level_start`.
pub fn merkle_fill_tree_at(tree: &mut [Sha256Hash], num_leafs: usize, mut level_start: usize) {
    debug_assert!(num_leafs >= 1);
    let mut level_size = num_leafs;
    while level_size > 1 {
        let mut parent = merkle_get_parent(level_start);
        let mut i = level_start;
        while i < level_start + level_size {
            tree[parent] = sha256_pair(&tree[i], &tree[i + 1]);
            i += 2;
            parent += 1;
        }
        level_start = merkle_get_parent(level_start);
        level_size /= 2;
    }
}

/// `merkle_fill_tree(tree, num_leafs)` — leaf layer at the end of `tree`.
pub fn merkle_fill_tree(tree: &mut [Sha256Hash], num_leafs: usize) {
    let start = merkle_num_nodes(num_leafs) - num_leafs;
    merkle_fill_tree_at(tree, num_leafs, start);
}

/// `merkle_fill_partial_tree` — compute interior nodes where both
/// children are known, then clear orphans that can't validate.
pub fn merkle_fill_partial_tree(tree: &mut [Sha256Hash]) {
    let num_nodes = tree.len();
    debug_assert!((num_nodes + 1) & num_nodes == 0);
    let num_leafs = num_nodes.div_ceil(2);
    let mut level_size = num_leafs;
    let mut level_start = merkle_first_leaf(num_leafs);
    while level_size > 1 {
        level_start = merkle_get_parent(level_start);
        level_size /= 2;
        for i in level_start..level_start + level_size {
            let child = merkle_get_first_child(i);
            if tree[child].is_all_zeros() || tree[child + 1].is_all_zeros() {
                continue;
            }
            tree[i] = sha256_pair(&tree[child], &tree[child + 1]);
        }
    }
    let mut parent = 0;
    let mut i = 1;
    while i < tree.len() {
        if tree[parent].is_all_zeros() {
            tree[i] = Sha256Hash::default();
            tree[i + 1] = Sha256Hash::default();
        } else if tree[i + 1].is_all_zeros() {
            tree[i] = Sha256Hash::default();
        } else if tree[i].is_all_zeros() {
            tree[i + 1] = Sha256Hash::default();
        }
        i += 2;
        parent += 1;
    }
}

/// `merkle_clear_tree`.
pub fn merkle_clear_tree(tree: &mut [Sha256Hash], num_leafs: usize, level_start: usize) {
    let mut level_size = num_leafs;
    let mut level_start = level_start;
    loop {
        for node in tree[level_start..level_start + level_size].iter_mut() {
            *node = Sha256Hash::default();
        }
        if level_size == 1 {
            break;
        }
        level_start = merkle_get_parent(level_start);
        level_size /= 2;
    }
}

/// `merkle_root_scratch` — like `merkle_root` but `leaves` may be fewer
/// than `num_leafs` (implied zero-padding beyond the given span).
#[must_use]
pub fn merkle_root_scratch(
    leaves: &[Sha256Hash],
    num_leafs: usize,
    mut pad: Sha256Hash,
    scratch: &mut Vec<Sha256Hash>,
) -> Sha256Hash {
    debug_assert!((num_leafs - 1) & num_leafs == 0);
    if num_leafs == 1 {
        return leaves[0];
    }
    // upstream walks `leaves` then `scratch` alternately; we keep two
    // buffers to satisfy the borrow checker — `scratch` is reused for
    // the "next level" writes
    let mut level: Vec<Sha256Hash> = leaves.to_vec();
    let mut num_leafs = num_leafs;
    while num_leafs > 1 {
        scratch.clear();
        scratch.reserve(level.len().div_ceil(2));
        let mut i = 0;
        while i < level.len() / 2 {
            scratch.push(sha256_pair(&level[i * 2], &level[i * 2 + 1]));
            i += 1;
        }
        if level.len() & 1 == 1 {
            scratch.push(sha256_pair(&level[i * 2], &pad));
        }
        pad = sha256_pair(&pad, &pad);
        std::mem::swap(&mut level, scratch);
        num_leafs /= 2;
    }
    level[0]
}

/// `merkle_validate_node`.
#[must_use]
pub fn merkle_validate_node(left: &Sha256Hash, right: &Sha256Hash, parent: &Sha256Hash) -> bool {
    sha256_pair(left, right) == *parent
}

/// `merkle_validate_and_insert_proofs` — walk `node`'s uncle hashes up
/// `target_tree`, inserting nodes until a known ancestor validates the
/// chain. On failure the partial inserts are undone.
pub fn merkle_validate_and_insert_proofs(
    target_tree: &mut [Sha256Hash],
    target_node_idx: usize,
    node: Sha256Hash,
    uncle_hashes: &[Sha256Hash],
) -> bool {
    if node.is_all_zeros() {
        return false;
    }
    if target_node_idx == 0 && !uncle_hashes.is_empty() {
        return false;
    }
    let target_matches = target_tree[target_node_idx] == node;
    if !target_matches && !target_tree[target_node_idx].is_all_zeros() {
        return false;
    }
    if uncle_hashes.is_empty() {
        return target_matches;
    }

    let mut cursor = target_node_idx;
    target_tree[cursor] = node;
    let mut wrote_sibling: u32 = 0;
    for (iter, proof) in uncle_hashes.iter().enumerate() {
        debug_assert!(iter < 32);
        let proof_idx = merkle_get_sibling(cursor);
        let parent = merkle_get_parent(cursor);
        if target_tree[proof_idx].is_all_zeros() {
            target_tree[proof_idx] = *proof;
            wrote_sibling |= 1 << iter;
        } else if target_tree[proof_idx] != *proof {
            break;
        }
        let left = proof_idx.min(cursor);
        let hash = sha256_pair(&target_tree[left], &target_tree[left + 1]);
        if target_tree[parent] == hash {
            return true;
        }
        if !target_tree[parent].is_all_zeros() {
            if wrote_sibling & (1 << iter) != 0 {
                target_tree[proof_idx] = Sha256Hash::default();
                wrote_sibling &= !(1 << iter);
            }
            break;
        }
        cursor = parent;
        target_tree[cursor] = hash;
    }

    // never reached a known hash → undo the inserts
    let mut clear_cursor = target_node_idx;
    let mut undo_iter = 0;
    while clear_cursor > cursor {
        let proof_idx = merkle_get_sibling(clear_cursor);
        if clear_cursor != target_node_idx || !target_matches {
            target_tree[clear_cursor] = Sha256Hash::default();
        }
        if wrote_sibling & (1 << undo_iter) != 0 {
            target_tree[proof_idx] = Sha256Hash::default();
        }
        clear_cursor = merkle_get_parent(clear_cursor);
        undo_iter += 1;
    }
    if cursor != target_node_idx || !target_matches {
        target_tree[cursor] = Sha256Hash::default();
    }
    false
}

/// `merkle_validate_copy` — copy the nodes of `src` into `dst` that can
/// `merkle_validate_copy` — copy the nodes of `src` into `dst` that can
/// be proven correct against `root`, marking verified leaf bits.
pub fn merkle_validate_copy(
    src: &[Sha256Hash],
    dst: &mut [Sha256Hash],
    root: &Sha256Hash,
    verified_leafs: &mut bitdaemon_common::bitfield::Bitfield,
) {
    debug_assert!(src.len() == dst.len());
    let num_leafs = dst.len().div_ceil(2);
    if src.is_empty() {
        return;
    }
    if src[0] != *root {
        return;
    }
    dst[0] = src[0];
    let leaf_layer_start = src.len() - num_leafs;
    for i in 0..leaf_layer_start {
        if dst[i].is_all_zeros() {
            continue;
        }
        let left_child = merkle_get_first_child(i);
        let right_child = left_child + 1;
        if merkle_validate_node(&src[left_child], &src[right_child], &dst[i]) {
            dst[left_child] = src[left_child];
            dst[right_child] = src[right_child];
            if left_child >= leaf_layer_start {
                let block_idx = left_child - leaf_layer_start;
                if block_idx < verified_leafs.size() {
                    verified_leafs.set_bit(block_idx);
                    if block_idx + 1 < verified_leafs.size() {
                        verified_leafs.set_bit(block_idx + 1);
                    }
                }
            }
        }
    }
}

/// `merkle_validate_single_layer`.
#[must_use]
pub fn merkle_validate_single_layer(tree: &[Sha256Hash]) -> bool {
    if tree.len() == 1 {
        return true;
    }
    let num_leafs = tree.len().div_ceil(2);
    debug_assert!(num_leafs & (num_leafs - 1) == 0);
    let mut idx = merkle_first_leaf(num_leafs);
    while idx < tree.len() {
        if !merkle_validate_node(&tree[idx], &tree[idx + 1], &tree[merkle_get_parent(idx)]) {
            return false;
        }
        idx += 2;
    }
    true
}

/// `merkle_find_known_subtree` — `(leafs_start, leafs_size, root_index)`:
/// the largest computable subtree covering `block_index`.
#[must_use]
pub fn merkle_find_known_subtree(
    tree: &[Sha256Hash],
    block_index: usize,
    num_valid_leafs: usize,
) -> (usize, usize, usize) {
    let mut leafs_start = block_index;
    let mut leafs_size = 1usize;
    let first_leaf = tree.len() / 2;
    let mut root_index = merkle_get_sibling(first_leaf + block_index);
    let mut i = block_index;
    loop {
        let first_check_index = if i & 1 == 1 {
            leafs_start - leafs_size
        } else {
            leafs_start + leafs_size
        };
        let check_end = leafs_size.min(num_valid_leafs.saturating_sub(first_check_index));
        if tree[first_leaf + first_check_index..first_leaf + first_check_index + check_end]
            .iter()
            .any(Sha256Hash::is_all_zeros)
        {
            return (leafs_start, leafs_size, root_index);
        }
        if i & 1 == 1 {
            leafs_start -= leafs_size;
        }
        leafs_size *= 2;
        root_index = merkle_get_parent(root_index);
        if !tree[root_index].is_all_zeros() {
            break;
        }
        i >>= 1;
    }
    (leafs_start, leafs_size, root_index)
}
