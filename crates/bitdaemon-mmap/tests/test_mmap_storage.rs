// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `MmapStorage` read/write/hash round-trip + pool LRU behavior.

use std::path::PathBuf;
use std::sync::Arc;

use bitdaemon_common::types::{DownloadPriority, PieceIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage, RenamedFiles};
use bitdaemon_disk::open_mode::OpenMode;
use bitdaemon_disk::storage::{StorageMode, StorageParams};
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
    let n = st.write(&buf, PieceIndex(0), 0, OpenMode::WRITE).unwrap();
    assert_eq!(n, 1500);

    let mut out = vec![0u8; 1500];
    let n = st
        .read(&mut out, PieceIndex(0), 0, OpenMode::READ_ONLY, false)
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
    let v1 = pool.open(&p1, 64).unwrap();
    let v2 = pool.open(&p2, 64).unwrap();
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
    st.write(b"hello", PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();
    st.release_files().unwrap();
    assert_eq!(pool.len(), 0);
    st.delete_files(bitdaemon_disk::storage::remove_flags::DELETE_FILES)
        .unwrap();
    assert!(!d.join("t/a.bin").exists());
    assert!(!d.join("t/b.bin").exists());
}
