//! Conformance port of `test_merkle_tree.cpp`.
#![allow(clippy::needless_range_loop)] // tree-index loops mirror upstream

use bitdaemon_common::Sha256Hash;
use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_core::merkle::*;
use bitdaemon_core::merkle_tree::{MerkleTree, Mode, SetBlockResult};

const NUM_BLOCKS: usize = 259;

fn sha256_repeat(v: u32) -> Sha256Hash {
    let mut b = [0u8; 32];
    for c in b.chunks_exact_mut(4) {
        c.copy_from_slice(&v.to_le_bytes());
    }
    Sha256Hash::from_bytes(&b).unwrap()
}

/// `build_tree(size)` — deterministic full tree like the upstream test.
fn build_tree(size: usize) -> Vec<Sha256Hash> {
    let num_leafs = merkle_num_leafs(size);
    let mut full = vec![Sha256Hash::default(); merkle_num_nodes(num_leafs)];
    let first = full.len() - num_leafs;
    for i in 0..size {
        full[first + i] = sha256_repeat(i as u32 + 1);
    }
    merkle_fill_tree(&mut full, num_leafs);
    full
}

fn none_set(n: usize) -> Bitfield {
    Bitfield::with_size(n)
}

fn all_set(n: usize) -> Bitfield {
    Bitfield::with_value(n, true)
}

fn f() -> Vec<Sha256Hash> {
    build_tree(NUM_BLOCKS)
}

fn test_roundtrip(t: &MerkleTree, block_count: usize, blocks_per_piece: usize) {
    let (tree, mask) = t.build_sparse_vector();
    let mut t2 = MerkleTree::new(block_count, blocks_per_piece, f()[0]);
    t2.load_sparse_tree(&tree, &mask, &none_set(NUM_BLOCKS));
    assert_eq!(t.build_vector(), t2.build_vector());
    for i in 0..t.size() {
        assert_eq!(t.get(i), t2.get(i), "node {i}");
        assert_eq!(t.has_node(i), t2.has_node(i), "has_node {i}");
        if !t.has_node(i) {
            assert!(t.get(i).is_all_zeros());
        }
        if !t2.has_node(i) {
            assert!(t2.get(i).is_all_zeros());
        }
        assert!(t.compare_node(i, &t2.get(i)));
        assert!(t2.compare_node(i, &t.get(i)));
    }
}

#[test]
fn load_tree() {
    let f = f();
    let num_nodes = f.len();
    let num_pad_leafs = merkle_num_leafs(NUM_BLOCKS) - NUM_BLOCKS;

    // full tree + valid root
    {
        let mut t = MerkleTree::new(NUM_BLOCKS, 1, f[0]);
        t.load_tree(&f, &none_set(NUM_BLOCKS));
        for i in 0..num_nodes - num_pad_leafs {
            assert!(t.has_node(i), "{i}");
            assert!(t.compare_node(i, &f[i]));
        }
        for i in num_nodes - num_pad_leafs..num_nodes {
            assert!(!t.has_node(i), "{i}");
            assert!(t.compare_node(i, &f[i]));
        }
    }

    // mismatching root
    {
        let bad = sha256_repeat(0xdead);
        let mut t = MerkleTree::new(NUM_BLOCKS, 1, bad);
        t.load_tree(&f, &none_set(NUM_BLOCKS));
        assert!(t.has_node(0));
        for i in 1..num_nodes {
            assert!(!t.has_node(i), "{i}");
        }
    }

    // mismatching size
    {
        let mut t = MerkleTree::new(NUM_BLOCKS, 1, f[0]);
        t.load_tree(&f[..f.len() - 1], &none_set(NUM_BLOCKS));
        assert!(t.has_node(0));
        for i in 1..num_nodes {
            assert!(!t.has_node(i), "{i}");
        }
    }
}

#[test]
fn roundtrip_empty_tree() {
    let t = MerkleTree::new(NUM_BLOCKS, 1, f()[0]);
    test_roundtrip(&t, NUM_BLOCKS, 1);
}

#[test]
fn roundtrip_full_tree() {
    let mut t = MerkleTree::new(NUM_BLOCKS, 1, f()[0]);
    t.load_tree(&f(), &none_set(NUM_BLOCKS));
    test_roundtrip(&t, NUM_BLOCKS, 1);
}

#[test]
fn roundtrip_piece_layer_tree() {
    let f = f();
    let mut sparse = f.clone();
    for i in sparse.len() / 2..sparse.len() {
        sparse[i] = Sha256Hash::default();
    }
    let mut t = MerkleTree::new(NUM_BLOCKS, 2, f[0]);
    t.load_tree(&sparse, &none_set(NUM_BLOCKS));
    test_roundtrip(&t, NUM_BLOCKS, 2);
}

#[test]
fn roundtrip_partial_tree() {
    let f = f();
    let mut sparse = f.clone();
    for i in sparse.len() / 4..sparse.len() {
        if i % 3 == 0 {
            sparse[i] = Sha256Hash::default();
        }
    }
    let mut t = MerkleTree::new(NUM_BLOCKS, 2, f[0]);
    t.load_tree(&sparse, &none_set(NUM_BLOCKS));
    test_roundtrip(&t, NUM_BLOCKS, 2);
}

#[test]
fn roundtrip_more_partial_tree() {
    let f = f();
    let mut sparse = f.clone();
    for i in sparse.len() / 4..sparse.len() {
        if i % 4 == 0 {
            sparse[i] = Sha256Hash::default();
        }
    }
    let mut t = MerkleTree::new(NUM_BLOCKS, 2, f[0]);
    t.load_tree(&sparse, &none_set(NUM_BLOCKS));
    test_roundtrip(&t, NUM_BLOCKS, 2);
}

#[test]
fn roundtrip_one_block_tree() {
    let f = f();
    let mut t = MerkleTree::new(1, 256, f[0]);
    t.load_tree(&f[..1], &none_set(1));
    test_roundtrip(&t, 1, 256);
}

#[test]
fn roundtrip_two_block_tree() {
    let f = f();
    let mut t = MerkleTree::new(2, 256, f[0]);
    t.load_tree(&f[..3], &none_set(2));
    test_roundtrip(&t, 2, 256);
}

#[test]
fn roundtrip_two_block_partial_tree() {
    let f = f();
    let mut pf = f[..3].to_vec();
    pf[2] = Sha256Hash::default();
    let mut t = MerkleTree::new(2, 256, f[0]);
    t.load_tree(&pf, &none_set(2));
    test_roundtrip(&t, 2, 256);
}

#[test]
fn small_tree() {
    let f = f();
    let t = MerkleTree::new(1, 256, f[0]);
    assert_eq!(t.build_vector(), vec![f[0]]);
}

#[test]
fn sparse_merkle_tree_block_layer() {
    let f = f();
    let mut t = MerkleTree::new(NUM_BLOCKS, 2, f[0]);
    t.load_tree(&f[..t.size()], &none_set(NUM_BLOCKS));
    for i in 0..t.size() {
        assert_eq!(t.get(i), f[i], "{i}");
    }
}

#[test]
fn get_piece_layer() {
    let f = f();
    let mut t = MerkleTree::new(NUM_BLOCKS, 8, f[0]);
    assert_eq!(t.verified_leafs(), none_set(NUM_BLOCKS));
    t.load_tree(&f[..t.size()], &none_set(NUM_BLOCKS));

    // build the expected piece layer: merkle roots of each 8-block span
    let mut expect = Vec::new();
    let num_leafs = merkle_num_leafs(NUM_BLOCKS);
    let first_leaf = merkle_first_leaf(num_leafs);
    let mut scratch = Vec::new();
    for i in (0..NUM_BLOCKS).step_by(8) {
        let end = (i + 8).min(NUM_BLOCKS);
        expect.push(merkle_root_scratch(
            &f[first_leaf + i..first_leaf + end],
            8,
            Sha256Hash::default(),
            &mut scratch,
        ));
    }
    assert_eq!(t.get_piece_layer(), expect);
}

#[test]
fn get_piece_layer_piece_layer_mode() {
    let f = f();
    // only the piece layer is known (load via load_piece_layer raw)
    let mut full = MerkleTree::new(NUM_BLOCKS, 8, f[0]);
    full.load_tree(&f, &all_set(NUM_BLOCKS));
    let layer = full.get_piece_layer();
    let raw: Vec<u8> = layer.iter().flat_map(|h| *h.as_bytes()).collect();

    let mut t = MerkleTree::new(NUM_BLOCKS, 8, f[0]);
    assert!(t.load_piece_layer(&raw));
    assert_eq!(t.mode(), Mode::PieceLayer);
    assert_eq!(t.get_piece_layer(), layer);
}

#[test]
fn set_block_full_block_layer() {
    // 4 blocks, 1 block per piece
    let tree = build_tree(4);
    let root = tree[0];
    let first_leaf = merkle_first_leaf(4);
    let mut t = MerkleTree::new(4, 1, root);
    t.load_tree(&tree, &all_set(4));
    assert_eq!(t.mode(), Mode::BlockLayer);

    // already-verified block with matching hash → ok
    let h = tree[first_leaf + 2];
    let (r, _s, _sz) = t.set_block(2, h);
    assert_eq!(r, SetBlockResult::Ok);
}

#[test]
fn set_block_invalid_full_block_layer() {
    let tree = build_tree(4);
    let root = tree[0];
    let first_leaf = merkle_first_leaf(4);
    let mut t = MerkleTree::new(4, 1, root);
    t.load_tree(&tree, &all_set(4));

    // mismatched hash on a verified block → block_hash_failed
    let mut h = tree[first_leaf + 2];
    let mut b = *h.as_bytes();
    b[0] ^= 1;
    h = Sha256Hash::from_bytes(&b).unwrap();
    let (r, _s, _sz) = t.set_block(2, h);
    assert_eq!(r, SetBlockResult::BlockHashFailed);
}

#[test]
fn set_block_empty_tree() {
    // upstream: blocks_per_piece=4, 259 blocks; `unknown` until the last
    // block completes the tree → `ok(0, num_leafs)`
    let f = f();
    let mut t = MerkleTree::new(NUM_BLOCKS, 4, f[0]);
    let first_leaf = merkle_first_leaf(merkle_num_leafs(NUM_BLOCKS));
    for block in 0..NUM_BLOCKS - 1 {
        let (r, _s, _sz) = t.set_block(block, f[first_leaf + block]);
        assert_eq!(r, SetBlockResult::Unknown, "block {block}");
        assert_eq!(t.verified_leafs(), none_set(NUM_BLOCKS));
    }
    let (r, s, sz) = t.set_block(NUM_BLOCKS - 1, f[first_leaf + NUM_BLOCKS - 1]);
    assert_eq!(r, SetBlockResult::Ok);
    assert_eq!(s, 0);
    assert_eq!(sz, merkle_num_leafs(NUM_BLOCKS));
    assert_eq!(t.verified_leafs(), all_set(NUM_BLOCKS));
}

#[test]
fn set_block_invalid_empty_tree() {
    let f = f();
    let mut t = MerkleTree::new(NUM_BLOCKS, 4, f[0]);
    for block in 0..NUM_BLOCKS {
        let (r, _s, _sz) = t.set_block(block, sha256_repeat(0xffff_0000 + block as u32));
        if block == NUM_BLOCKS - 1 {
            assert_eq!(r, SetBlockResult::HashFailed);
        } else {
            assert_eq!(r, SetBlockResult::Unknown, "block {block}");
        }
        assert_eq!(t.verified_leafs(), none_set(NUM_BLOCKS));
    }
}

/// `build_proof` — uncle hashes from `target` up to (excl.) `end`.
fn build_proof(tree: &[Sha256Hash], mut target: usize, end: usize) -> Vec<Sha256Hash> {
    let mut ret = Vec::new();
    while target > end {
        ret.push(tree[merkle_get_sibling(target)]);
        target = merkle_get_parent(target);
    }
    ret
}

fn set_range(mut bits: Bitfield, start: usize, count: usize) -> Bitfield {
    for i in start..start + count {
        bits.set_bit(i);
    }
    bits
}

#[test]
fn add_hashes_full_tree() {
    let f = f();
    for blocks_per_piece in [1usize, 2, 4] {
        let mut t = MerkleTree::new(NUM_BLOCKS, blocks_per_piece, f[0]);
        let res = t
            .add_hashes(511, 1, &f[511..511 + 512], &[])
            .expect("add_hashes");
        assert_eq!(res.passed.len(), 0);
        assert_eq!(res.failed.len(), 0);
        for i in 127..255 {
            assert_eq!(t.get(i), f[i], "piece layer {i}");
        }
        for i in 511..1023 {
            assert_eq!(t.get(i), f[i], "block layer {i}");
        }
        assert_eq!(t.verified_leafs(), all_set(NUM_BLOCKS));
    }
}

#[test]
fn add_hashes_one_piece() {
    let f = f();
    let blocks_per_piece = 4;
    for piece_index in [0usize, 64, 5, 30] {
        let mut t = MerkleTree::new(NUM_BLOCKS, blocks_per_piece, f[0]);
        let insert_idx = 127 + piece_index;
        let res = t
            .add_hashes(
                511 + piece_index * blocks_per_piece,
                1,
                &f[511 + piece_index * blocks_per_piece
                    ..511 + piece_index * blocks_per_piece + blocks_per_piece],
                &build_proof(&f, insert_idx, 0),
            )
            .expect("add_hashes");
        assert_eq!(res.passed.len(), 0);
        assert_eq!(res.failed.len(), 0);

        // the trail of proof hashes
        let mut i = insert_idx;
        while i > 0 {
            assert_eq!(t.get(i), f[i]);
            assert_eq!(t.get(merkle_get_sibling(i)), f[merkle_get_sibling(i)]);
            i = merkle_get_parent(i);
        }
        // piece layer: only the inserted piece + its proof sibling
        for i in 127..255 {
            if i == 127 + piece_index || merkle_get_sibling(i) == 127 + piece_index {
                assert_eq!(t.get(i), f[i]);
            } else {
                assert!(t.get(i).is_all_zeros(), "piece layer {i}");
            }
        }
        // block layer
        for i in 511..1023 {
            if (511 + piece_index * blocks_per_piece..511 + piece_index * blocks_per_piece + 4)
                .contains(&i)
            {
                assert_eq!(t.get(i), f[i]);
            } else {
                assert!(t.get(i).is_all_zeros(), "block layer {i}");
            }
        }
        let start_block = piece_index * blocks_per_piece;
        let end_block = blocks_per_piece.min(NUM_BLOCKS - start_block);
        assert_eq!(
            t.verified_leafs(),
            set_range(none_set(NUM_BLOCKS), start_block, end_block)
        );
    }
}

#[test]
fn add_hashes_block_layer_no_padding() {
    let f = f();
    let mut t = MerkleTree::new(NUM_BLOCKS, 4, f[0]);
    let res = t
        .add_hashes(511, 1, &f[511..511 + NUM_BLOCKS], &[])
        .expect("add");
    assert_eq!(res.passed.len(), 0);
    assert_eq!(res.failed.len(), 0);
    for i in 0..1023 {
        assert_eq!(t.get(i), f[i]);
    }
    assert_eq!(t.verified_leafs(), all_set(NUM_BLOCKS));
}

#[test]
fn add_hashes_piece_layer_no_padding() {
    let f = f();
    let mut t = MerkleTree::new(NUM_BLOCKS, 4, f[0]);
    let num_pieces = NUM_BLOCKS.div_ceil(4);
    let res = t
        .add_hashes(127, 1, &f[127..127 + num_pieces], &[])
        .expect("add");
    assert_eq!(res.passed.len(), 0);
    assert_eq!(res.failed.len(), 0);
    for i in 0..255 {
        assert_eq!(t.get(i), f[i]);
    }
    for i in 255..1023 {
        assert!(t.get(i).is_all_zeros(), "{i}");
    }
}

#[test]
fn add_hashes_one_piece_invalid_proof() {
    let f = f();
    let mut t = MerkleTree::new(NUM_BLOCKS, 4, f[0]);
    // corrupt a proof hash → must return nullopt
    let mut proof = build_proof(&f, 127, 0);
    let mut b = *proof[0].as_bytes();
    b[0] ^= 0x26;
    proof[0] = Sha256Hash::from_bytes(&b).unwrap();
    assert!(t.add_hashes(511, 1, &f[511..511 + 4], &proof).is_none());
}
