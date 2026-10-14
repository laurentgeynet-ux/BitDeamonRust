// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `MmapStorage` read/write/hash round-trip + pool LRU behavior.

use std::path::PathBuf;
use std::sync::Arc;

use bitdaemon_common::types::{DownloadPriority, FileIndex, PieceIndex, StorageIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage, RenamedFiles};
use bitdaemon_disk::open_mode::OpenMode;
use bitdaemon_disk::storage::{DiskStorage, MoveFlags, StorageMode, StorageParams};
use bitdaemon_mmap::{FileViewPool, MmapStorage};

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("bitdaemon-mmap-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn make_fs() -> FileStorage {
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(1024);
    fs.add_file(None, b"t/a.bin", 1000, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.add_file(None, b"t/b.bin", 1000, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.set_num_pieces(2);
    fs
}

fn storage(fs: &FileStorage, d: &std::path::Path, pool: &Arc<FileViewPool>) -> MmapStorage {
    MmapStorage::new(
        StorageParams {
            files: fs,
            renamed: &RenamedFiles::default(),
            path: d.to_path_buf(),
            part_file_dir: PathBuf::new(),
            mode: StorageMode::Sparse,
            priorities: &[DownloadPriority::DEFAULT, DownloadPriority::DEFAULT],
            info_hash: [7u8; 20],
            v1: true,
            v2: false,
        },
        pool.clone(),
    )
}

#[test]
fn write_read_across_files() {
    let d = dir("rw");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    // 1500 B spanning a.bin (1000) + b.bin (500) starting at piece 0
    let buf: Vec<u8> = (0..1500u32).map(|i| (i % 251) as u8).collect();
    let n = st
        .write(&buf, PieceIndex(0), 0, OpenMode::WRITE, false, false)
        .unwrap();
    assert_eq!(n, 1500);

    let mut out = vec![0u8; 1500];
    let n = st
        .read(
            &mut out,
            PieceIndex(0),
            0,
            OpenMode::READ_ONLY,
            false,
            false,
        )
        .unwrap();
    assert_eq!(n, 1500);
    assert_eq!(out, buf);

    // hash feed covers the same bytes
    let mut hashed = Vec::new();
    st.hash_with(
        |d| hashed.extend_from_slice(d),
        1500,
        PieceIndex(0),
        0,
        OpenMode::READ_ONLY,
        false,
        false,
    )
    .unwrap();
    assert_eq!(hashed, buf);
}

#[test]
fn pool_evicts_lru() {
    let d = dir("pool");
    let pool = FileViewPool::new(1);
    let p1 = d.join("a.bin");
    let p2 = d.join("b.bin");
    // first write-open carries TRUNCATE (m_file_created) — the file
    // must be sized before CreateFileMapping on a 0-byte file.
    let v1 = pool
        .open_file_at(
            (StorageIndex(0), FileIndex(0)),
            &p1,
            64,
            OpenMode::WRITE | OpenMode::TRUNCATE,
        )
        .unwrap();
    let v2 = pool
        .open_file_at(
            (StorageIndex(0), FileIndex(1)),
            &p2,
            64,
            OpenMode::WRITE | OpenMode::TRUNCATE,
        )
        .unwrap();
    assert_eq!(pool.len(), 1);
    // v1 still usable after eviction (shared mapping)
    v1.write_at(0, b"xy").unwrap();
    let mut b = [0u8; 2];
    assert_eq!(v1.read_at(0, &mut b).unwrap(), 2);
    assert_eq!(&b, b"xy");
    let _ = v2;
}

#[test]
fn release_and_delete() {
    let d = dir("del");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);
    st.write(b"hello", PieceIndex(0), 0, OpenMode::WRITE, false, false)
        .unwrap();
    st.release_files().unwrap();
    assert_eq!(pool.len(), 0);
    st.delete_files(bitdaemon_disk::storage::remove_flags::DELETE_FILES)
        .unwrap();
    assert!(!d.join("t/a.bin").exists());
    assert!(!d.join("t/b.bin").exists());
}

/// Regression: a zero-sized file gets an opened fd but no mapping
/// (upstream `no_mmap` path); `initialize` must not fail on it and
/// reads on the neighbouring file still work.
#[test]
fn zero_sized_file_initialize() {
    let d = dir("zero");
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(1024);
    fs.add_file(None, b"t/empty.bin", 0, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.add_file(None, b"t/data.bin", 512, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.set_num_pieces(1);
    let pool = Arc::new(FileViewPool::new(40));
    let mut st = storage(&fs, &d, &pool);

    st.initialize().unwrap();
    assert!(d.join("t/empty.bin").exists());

    let n = st
        .write(
            &[0xabu8; 512],
            PieceIndex(0),
            0,
            OpenMode::WRITE,
            false,
            false,
        )
        .unwrap();
    assert_eq!(n, 512);
    let mut out = [0u8; 512];
    let n = st
        .read(
            &mut out,
            PieceIndex(0),
            0,
            OpenMode::READ_ONLY,
            false,
            false,
        )
        .unwrap();
    assert_eq!(n, 512);
    assert_eq!(out, [0xabu8; 512]);
}

/// Files at or below `mmap_file_size_cutoff` (40 × 16 KiB upstream
/// default) open `no_mmap` — `has_memory_map() == false`, I/O still
/// works through the fd fallback.
#[test]
fn below_cutoff_uses_fd_fallback() {
    let d = dir("cutoff");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    st.write(
        &[0x5au8; 1000],
        PieceIndex(0),
        0,
        OpenMode::WRITE,
        false,
        false,
    )
    .unwrap();
    // the pooled view for file 0 exists but is unmapped
    let view = pool
        .open_file_at(
            (st.storage_index(), FileIndex(0)),
            &d.join("t/a.bin"),
            1000,
            OpenMode::READ_ONLY,
        )
        .unwrap();
    assert!(!view.has_memory_map());

    let mut out = [0u8; 1000];
    st.read(
        &mut out,
        PieceIndex(0),
        0,
        OpenMode::READ_ONLY,
        false,
        false,
    )
    .unwrap();
    assert_eq!(out, [0x5au8; 1000]);
}

/// `open_mode::read_only` opens `O_RDONLY` — no `create`. A missing
/// file must surface ENOENT and must not create parent directories.
#[test]
fn read_missing_file_no_dirs() {
    let d = dir("enoent");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    let mut out = [0u8; 64];
    let err = st
        .read(
            &mut out,
            PieceIndex(0),
            0,
            OpenMode::READ_ONLY,
            false,
            false,
        )
        .unwrap_err();
    assert_eq!(err.io.kind(), std::io::ErrorKind::NotFound);
    assert!(!d.join("t").exists());
}

/// B1 regression: exporting part-file data into a file at index > 0
/// must write at the *file-relative* offset the callback hands over
/// (upstream `range().subspan(file_offset)`), not the torrent offset.
#[test]
fn part_file_export_lands_in_file() {
    let d = dir("export");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let mut st = storage(&fs, &d, &pool);

    // b.bin (index 1, torrent offset 1000) unwanted → writes park in
    // the part file.
    st.set_file_priority(&[DownloadPriority::DEFAULT, DownloadPriority::DONT_DOWNLOAD])
        .unwrap();
    let b_bytes: Vec<u8> = (0..1000u32).map(|i| (i % 193) as u8).collect();
    // piece 0: a.bin[0..1000] + b.bin[0..24]; piece 1: b.bin[24..1000]
    let mut p0 = vec![0x11u8; 1000];
    p0.extend_from_slice(&b_bytes[..24]);
    st.write(&p0, PieceIndex(0), 0, OpenMode::WRITE, false, false)
        .unwrap();
    st.write(
        &b_bytes[24..],
        PieceIndex(1),
        0,
        OpenMode::WRITE,
        false,
        false,
    )
    .unwrap();

    // lift b.bin back to wanted → export must populate the real file
    st.set_file_priority(&[DownloadPriority::DEFAULT, DownloadPriority::DEFAULT])
        .unwrap();

    let on_disk = std::fs::read(d.join("t/b.bin")).unwrap();
    assert_eq!(on_disk.len(), 1000);
    assert_eq!(on_disk, b_bytes);
}

/// `volatile_read`/`flush_piece` job flags translate to `dont_need`/
/// `page_out` advice on the mapped range — best-effort, so the test
/// pins correctness of the round-trip with hints enabled.
#[test]
fn volatile_and_flush_hints_roundtrip() {
    let d = dir("hints");
    // a file above the mmap cutoff so the mapping path runs
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(0x40000);
    fs.add_file(
        None,
        b"t/big.bin",
        0x40000 * 41,
        FileFlags::NONE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.set_num_pieces(41);
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    let buf = vec![0xc3u8; 0x10000];
    st.write(&buf, PieceIndex(0), 0, OpenMode::WRITE, false, true)
        .unwrap();
    let mut out = vec![0u8; 0x10000];
    st.read(&mut out, PieceIndex(0), 0, OpenMode::READ_ONLY, true, true)
        .unwrap();
    assert_eq!(out, buf);
}

/// `store_precomputed_v2`/`take_precomputed_v2_block`/`drop_precomputed_v2`
/// round-trip per piece/block (upstream `m_precomputed_v2`).
#[test]
fn precomputed_v2_roundtrip() {
    let d = dir("v2");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    let h = bitdaemon_common::Sha256Hash::from([0x42u8; 32]);
    st.store_precomputed_v2(PieceIndex(0), 0, &h);
    st.store_precomputed_v2(PieceIndex(0), 1, &h);
    assert_eq!(st.take_precomputed_v2_block(PieceIndex(0), 0), Some(h));
    // taken entries are consumed
    assert_eq!(st.take_precomputed_v2_block(PieceIndex(0), 0), None);
    st.drop_precomputed_v2(PieceIndex(0));
    assert_eq!(st.take_precomputed_v2_block(PieceIndex(0), 1), None);
}

/// Pool keys are `(StorageIndex, FileIndex)` — releasing storage A's
/// views must leave storage B's untouched.
#[test]
fn release_is_scoped_to_storage() {
    let d1 = dir("scope-a");
    let d2 = dir("scope-b");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st_a = storage(&fs, &d1, &pool);
    st_a.set_storage_index(StorageIndex(1));
    let st_b = storage(&fs, &d2, &pool);
    st_b.set_storage_index(StorageIndex(2));

    st_a.write(&[1u8; 64], PieceIndex(0), 0, OpenMode::WRITE, false, false)
        .unwrap();
    st_b.write(&[2u8; 64], PieceIndex(0), 0, OpenMode::WRITE, false, false)
        .unwrap();
    assert_eq!(pool.len(), 2);

    st_a.release_files().unwrap();
    assert_eq!(pool.len(), 1);
    // B's view survives — the read hits the cached view, not ENOENT
    let mut out = [0u8; 64];
    let n = st_b
        .read(
            &mut out,
            PieceIndex(0),
            0,
            OpenMode::READ_ONLY,
            false,
            false,
        )
        .unwrap();
    assert_eq!(n, 64);
    assert_eq!(out, [2u8; 64]);
}

/// A view handed out to an external `Arc` holder stays usable after
/// `delete_files` — deferred destroy, like upstream's `shared_ptr`
/// file view entries.
#[test]
fn external_view_survives_delete() {
    let d = dir("extview");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    st.write(
        &[0x77u8; 64],
        PieceIndex(0),
        0,
        OpenMode::WRITE,
        false,
        false,
    )
    .unwrap();
    let ext = pool
        .open_file_at(
            (st.storage_index(), FileIndex(0)),
            &d.join("t/a.bin"),
            1000,
            OpenMode::READ_ONLY,
        )
        .unwrap();

    st.delete_files(bitdaemon_disk::storage::remove_flags::DELETE_FILES)
        .unwrap();
    assert!(!d.join("t/a.bin").exists());
    // the Arc-kept mapping still serves bytes (mmap.cpp: keep view
    // alive while referenced)
    let mut b = [0u8; 64];
    assert_eq!(ext.read_at(0, &mut b).unwrap(), 64);
    assert_eq!(b, [0x77u8; 64]);
}

/// `file_status` surfaces the pool's open views per storage;
/// `close_oldest` evicts the LRU view.
#[test]
fn status_and_close_oldest() {
    let d = dir("status");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    st.write(&[0u8; 64], PieceIndex(0), 0, OpenMode::WRITE, false, false)
        .unwrap();
    st.write(&[0u8; 64], PieceIndex(1), 0, OpenMode::WRITE, false, false)
        .unwrap();
    assert_eq!(st.file_status().len(), 2);

    st.close_oldest();
    assert_eq!(pool.len(), 1);
    assert_eq!(st.file_status().len(), 1);
}

/// `move_storage` releases views before moving (Windows won't rename
/// a mapped file) — a read afterwards re-opens from the new path.
#[test]
fn move_storage_reopens_at_new_path() {
    let d = dir("mv-src");
    let d2 = dir("mv-dst");
    let fs = make_fs();
    let pool = Arc::new(FileViewPool::new(40));
    let st = storage(&fs, &d, &pool);

    st.write(
        &[0xabu8; 64],
        PieceIndex(0),
        0,
        OpenMode::WRITE,
        false,
        false,
    )
    .unwrap();
    assert_eq!(pool.len(), 1);

    let (status, _path, err) = st.move_storage(d2.clone(), MoveFlags::FailIfExist);
    assert!(err.is_none(), "move failed: {err:?}");
    assert_eq!(status, 0);
    assert!(d2.join("t/a.bin").exists());

    let mut out = [0u8; 64];
    let n = st
        .read(
            &mut out,
            PieceIndex(0),
            0,
            OpenMode::READ_ONLY,
            false,
            false,
        )
        .unwrap();
    assert_eq!(n, 64);
    assert_eq!(out, [0xabu8; 64]);
}
