//! Conformance tests for `HashPicker` — ported from upstream
//! `test_hash_picker.cpp` (this checkout's single-request `pick_hashes`
//! API, called in a loop where upstream's batched overload is tested
//! elsewhere).

use bitdaemon_common::Sha256Hash;
use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_common::hex::from_hex;
use bitdaemon_common::types::{FileIndex, PieceIndex};
use bitdaemon_core::file_storage::FileFlags;
use bitdaemon_core::file_storage::FileStorage;
use bitdaemon_core::merkle::{
    merkle_fill_tree, merkle_first_leaf, merkle_get_parent, merkle_get_sibling, merkle_num_leafs,
    merkle_num_nodes,
};
use bitdaemon_core::merkle_tree::MerkleTree;
use bitdaemon_disk::hash_picker::{HashPicker, HashPickerState, HashRequest};

const BLOCK: usize = 0x4000;

fn sha256_seed(i: u32) -> Sha256Hash {
    // upstream build_tree: each 32-bit word of the leaf = i + 1
    let word = (i + 1).to_le_bytes();
    let mut buf = [0u8; 32];
    for c in buf.chunks_exact_mut(4) {
        c.copy_from_slice(&word);
    }
    Sha256Hash::from_bytes(&buf).unwrap_or_default()
}

/// `build_tree(size)` from upstream `test_utils`.
fn build_tree(size: usize) -> Vec<Sha256Hash> {
    let num_leafs = merkle_num_leafs(size);
    let mut full = vec![Sha256Hash::default(); merkle_num_nodes(num_leafs)];
    for i in 0..size {
        let idx = full.len() - num_leafs + i;
        full[idx] = sha256_seed(i as u32);
    }
    merkle_fill_tree(&mut full, num_leafs);
    full
}

fn root1() -> Sha256Hash {
    let v = from_hex(b"0000000000000000000000000000000000000000000000000000000000000001")
        .unwrap_or_default();
    Sha256Hash::from_bytes(&v).unwrap_or_default()
}

fn add_file(fs: &mut FileStorage, path: &str, size: i64) {
    fs.add_file(None, path.as_bytes(), size, FileFlags::NONE, 0, b"", None)
        .unwrap_or_else(|e| panic!("add_file {path}: {e}"));
}

#[test]
fn pick_piece_layer() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(16 * 1024);
    add_file(&mut fs, "test/tmp1", 4 * 512 * 16 * 1024);
    add_file(&mut fs, "test/tmp2", 4 * 512 * 16 * 1024);

    let n = merkle_num_nodes(merkle_num_leafs(4 * 512));
    let mut trees = vec![
        MerkleTree::new(n, 1, root1()),
        MerkleTree::new(n, 1, root1()),
    ];

    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);

    let mut pieces = Bitfield::with_size(8 * 512);
    pieces.set_all();

    // upstream batched pick (2 requests, then 3) == sequential picks
    for (file, index) in [
        (FileIndex(0), 0),
        (FileIndex(0), 512),
        (FileIndex(0), 1024),
        (FileIndex(0), 1536),
        (FileIndex(1), 0),
        (FileIndex(1), 512),
        (FileIndex(1), 1024),
        (FileIndex(1), 1536),
    ] {
        let picked = picker.pick_hashes(&pieces);
        assert_eq!(picked.file, file, "index {index}");
        assert_eq!(picked.base, 0, "index {index}");
        assert_eq!(picked.count, 512, "index {index}");
        assert_eq!(picked.index, index);
        assert_eq!(picked.proof_layers, 10, "index {index}");
    }
    // exhausted — default (empty) request
    assert_eq!(picker.pick_hashes(&pieces), HashRequest::default());
}

#[test]
fn hash_picker_empty_trees() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(16 * 1024);
    add_file(&mut fs, "test/tmp1", 4 * 512 * 16 * 1024);

    let mut trees: Vec<MerkleTree> = Vec::new();
    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);

    let pieces = Bitfield::with_value(4 * 512, true);
    assert_eq!(picker.pick_hashes(&pieces).count, 0);
}

#[test]
fn hash_picker_reject_piece_hashes() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(16 * 1024);
    add_file(&mut fs, "test/tmp1", 100 * 16 * 1024);

    let mut trees = vec![MerkleTree::new(100 * 16, 16, root1())];
    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);
    picker.verify_block_hashes(PieceIndex(40));

    let pieces = Bitfield::with_value(100, true);
    let picked = picker.pick_hashes(&pieces);
    assert_eq!(picked.base, 0);
    // index counts *blocks*, not pieces — rejecting must not corrupt the
    // piece-layer bookkeeping
    picker.hashes_rejected(&picked);
}

#[test]
fn block_hash_request_reissued_after_timeout() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(16 * BLOCK as i32);
    add_file(&mut fs, "test/tmp1", 100 * 16 * BLOCK as i64);

    let mut trees = vec![MerkleTree::new(100 * 16, 16, root1())];
    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);
    picker.verify_block_hashes(PieceIndex(40));

    // no piece data → only the block-hash request can be picked
    let pieces = Bitfield::with_value(100, false);
    let picked = picker.pick_hashes(&pieces);
    assert_eq!(picked.base, 0);

    // still within min_request_interval — must not reissue
    assert_eq!(picker.pick_hashes(&pieces), HashRequest::default());

    std::thread::sleep(std::time::Duration::from_secs(4));
    assert_eq!(picker.pick_hashes(&pieces), picked);
}

#[test]
fn block_hash_request_resolved_stops_reissue() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(16 * BLOCK as i32);
    add_file(&mut fs, "test/tmp1", 100 * 16 * BLOCK as i64);

    let full_tree = build_tree(100 * 16);
    let verified = Bitfield::with_value(merkle_num_leafs(100 * 16), true);
    let mut server_tree = MerkleTree::new(100 * 16, 16, full_tree[0]);
    server_tree.load_tree(&full_tree, &verified);

    let mut trees = vec![MerkleTree::new(100 * 16, 16, full_tree[0])];

    // establish the piece layer so layers_to_verify() finds an ancestor
    let piece_layer_start = merkle_first_leaf(merkle_num_leafs(100));
    assert!(
        trees[0]
            .add_hashes(
                piece_layer_start,
                0,
                &full_tree[piece_layer_start..piece_layer_start + 100],
                &[],
            )
            .is_some()
    );

    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);
    picker.verify_block_hashes(PieceIndex(40));

    let pieces = Bitfield::with_value(100, false);
    let picked = picker.pick_hashes(&pieces);
    assert_eq!(picked.base, 0);

    let hashes = server_tree
        .get_hashes(
            picked.base as usize,
            picked.index as usize,
            picked.count as usize,
            picked.proof_layers as usize,
        )
        .unwrap_or_default();
    assert!(picker.add_hashes(&picked, &hashes).valid);

    std::thread::sleep(std::time::Duration::from_secs(4));
    // resolved — never reissued
    assert_eq!(picker.pick_hashes(&pieces), HashRequest::default());
}

#[test]
fn block_hash_request_cleared_when_piece_and_block_layer_coincide() {
    let mut fs = FileStorage::new();
    // 1 block per piece → m_piece_layer == 0, layers coincide
    fs.set_piece_length(BLOCK as i32);
    // piece 512 starts a fresh 512-piece bucket of size 1 — the request
    // also has the tail-chunk piece-layer shape
    add_file(&mut fs, "test/tmp1", 513 * BLOCK as i64);

    let full_tree = build_tree(513);
    let verified = Bitfield::with_value(merkle_num_leafs(513), true);
    let mut trees = vec![MerkleTree::new(513, 1, full_tree[0])];
    trees[0].load_tree(&full_tree, &verified);

    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);
    picker.verify_block_hashes(PieceIndex(512));

    let pieces = Bitfield::with_value(513, false);
    let picked = picker.pick_hashes(&pieces);
    assert_eq!(picked.base, 0);
    assert_eq!(picked.index, 512);
    assert_eq!(picked.count, 1);
    assert_eq!(picked.proof_layers, 0);

    let hashes = picker.trees()[0]
        .get_hashes(
            picked.base as usize,
            picked.index as usize,
            picked.count as usize,
            picked.proof_layers as usize,
        )
        .unwrap_or_default();
    assert!(picker.add_hashes(&picked, &hashes).valid);

    std::thread::sleep(std::time::Duration::from_secs(4));
    // satisfied both shapes — cleared regardless of branch
    assert_eq!(picker.pick_hashes(&pieces), HashRequest::default());
}

#[test]
fn block_hash_request_retried_when_piece_and_block_layer_coincide() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(BLOCK as i32);
    add_file(&mut fs, "test/tmp1", 513 * BLOCK as i64);

    let full_tree = build_tree(513);
    let unverified = Bitfield::with_value(merkle_num_leafs(513), false);
    let mut trees = vec![MerkleTree::new(513, 1, full_tree[0])];
    trees[0].load_tree(&full_tree, &unverified);

    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);
    picker.verify_block_hashes(PieceIndex(512));

    let pieces = Bitfield::with_value(513, false);
    let picked = picker.pick_hashes(&pieces);
    assert_eq!(picked.base, 0);
    assert_eq!(picked.index, 512);
    assert_eq!(picked.count, 1);

    picker.hashes_rejected(&picked);
    // both shapes at once — rejection resets the block bookkeeping so the
    // request is immediately retryable
    assert_eq!(picker.pick_hashes(&pieces), picked);
}

#[test]
fn add_leaf_hashes() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(16 * 1024);
    add_file(&mut fs, "test/tmp1", 4 * 512 * 16 * 1024);

    let full_tree = build_tree(4 * 512);
    let mut trees = vec![MerkleTree::new(4 * 512, 1, full_tree[0])];

    {
        let mut hp_state = HashPickerState::default();
        let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);

        let pieces_start = full_tree.len() - merkle_num_leafs(4 * 512);
        let mut hashes: Vec<Sha256Hash> = (0..512).map(|i| full_tree[pieces_start + i]).collect();
        let mut i = 3;
        while i > 0 {
            hashes.push(full_tree[merkle_get_sibling(i)]);
            i = merkle_get_parent(i);
        }
        let req = HashRequest {
            file: FileIndex(0),
            base: 0,
            index: 0,
            count: 512,
            proof_layers: 10,
        };
        assert!(picker.add_hashes(&req, &hashes).valid);

        let start = full_tree.len() - merkle_num_leafs(4 * 512) + 512;
        let req = HashRequest {
            file: FileIndex(0),
            base: 0,
            index: 512,
            count: 512,
            proof_layers: 0,
        };
        assert!(
            picker
                .add_hashes(&req, &full_tree[start..start + 512])
                .valid
        );

        let mut hashes: Vec<Sha256Hash> =
            (1024..1536).map(|i| full_tree[pieces_start + i]).collect();
        let mut i = 5;
        while i > 0 {
            hashes.push(full_tree[merkle_get_sibling(i)]);
            i = merkle_get_parent(i);
        }
        let req = HashRequest {
            file: FileIndex(0),
            base: 0,
            index: 1024,
            count: 512,
            proof_layers: 10,
        };
        assert!(picker.add_hashes(&req, &hashes).valid);

        let start = full_tree.len() - merkle_num_leafs(4 * 512) + 1536;
        let req = HashRequest {
            file: FileIndex(0),
            base: 0,
            index: 1536,
            count: 512,
            proof_layers: 0,
        };
        assert!(
            picker
                .add_hashes(&req, &full_tree[start..start + 512])
                .valid
        );
    }

    assert_eq!(trees[0].build_vector(), full_tree);
}

#[test]
fn add_piece_hashes() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(4 * 16 * 1024);
    add_file(&mut fs, "test/tmp1", 4 * 1024 * 16 * 1024);

    let full_tree = build_tree(4 * 1024);
    let mut trees = vec![MerkleTree::new(4 * 1024, 4, full_tree[0])];

    {
        let mut hp_state = HashPickerState::default();
        let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);

        let pieces_start = merkle_num_nodes(1024) - 1024;

        let mut hashes: Vec<Sha256Hash> = full_tree[pieces_start..pieces_start + 512].to_vec();
        hashes.push(full_tree[2]);
        let req = HashRequest {
            file: FileIndex(0),
            base: 2,
            index: 0,
            count: 512,
            proof_layers: 9,
        };
        assert!(picker.add_hashes(&req, &hashes).valid);

        let hashes = full_tree[pieces_start + 512..pieces_start + 1024].to_vec();
        let req = HashRequest {
            file: FileIndex(0),
            base: 2,
            index: 512,
            count: 512,
            proof_layers: 8,
        };
        assert!(picker.add_hashes(&req, &hashes).valid);
    }

    let cmp = trees[0].build_vector();
    assert_eq!(
        &cmp[..merkle_num_nodes(1024)],
        &full_tree[..merkle_num_nodes(1024)]
    );
}

#[test]
fn add_piece_hashes_marks_request_complete() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(4 * 16 * 1024);
    add_file(&mut fs, "test/tmp1", 4 * 512 * 16 * 1024);

    let full_tree = build_tree(4 * 512);
    let mut trees = vec![MerkleTree::new(4 * 512, 4, full_tree[0])];

    let mut hp_state = HashPickerState::default();
    let mut picker = HashPicker::new(&fs, &mut trees, &mut hp_state);
    let pieces = Bitfield::with_value(512, true);

    let picked = picker.pick_hashes(&pieces);
    assert_eq!(picked.file, FileIndex(0));
    assert_eq!(picked.base, 2);
    assert_eq!(picked.index, 0);
    assert_eq!(picked.count, 512);

    // simulate the retry interval expiring before the response is processed
    picker.hashes_rejected(&picked);

    let pieces_start = merkle_num_nodes(512) - 512;
    let hashes = full_tree[pieces_start..pieces_start + 512].to_vec();
    assert!(picker.add_hashes(&picked, &hashes).valid);

    // resolved — not picked again
    assert_eq!(picker.pick_hashes(&pieces).count, 0);
}
