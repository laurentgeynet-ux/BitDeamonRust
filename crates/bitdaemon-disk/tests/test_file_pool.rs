//! Tests for `file_pool` — LRU eviction, mode upgrades, stats.
//! (Upstream `test_file_pool` equivalent scenarios.)

use std::fs as stdfs;
use std::path::PathBuf;

use bitdaemon_common::types::{FileIndex, StorageIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage};
use bitdaemon_disk::file_pool::FilePool;
use bitdaemon_disk::open_mode::OpenMode;

fn make_fs() -> FileStorage {
    let mut fs = FileStorage::new();
    for i in 0..5 {
        fs.add_file(
            None,
            format!("test_storage/file{i}.bin").as_bytes(),
            128,
            FileFlags::NONE,
            0,
            b"",
            None,
        )
        .unwrap();
    }
    fs
}

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("bitdaemon_fp_{}_{}", std::process::id(), name));
    let _ = stdfs::remove_dir_all(&d);
    stdfs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn open_hit_and_miss() {
    let d = dir("hit");
    let fs = make_fs();
    let pool = FilePool::new(40);
    let st = StorageIndex(0);
    let h1 = pool
        .open_file(st, &d, FileIndex(0), &fs, OpenMode::WRITE)
        .unwrap();
    let h2 = pool
        .open_file(st, &d, FileIndex(0), &fs, OpenMode::WRITE)
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(&h1, &h2));
    let (hits, misses, _, _, open) = pool.stats_counters();
    assert_eq!((hits, misses, open), (1, 1, 1));
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn lru_eviction() {
    let d = dir("lru");
    let fs = make_fs();
    let pool = FilePool::new(3);
    let st = StorageIndex(0);
    // open 4 files → first evicted
    for i in 0..4 {
        pool.open_file(st, &d, FileIndex(i), &fs, OpenMode::WRITE)
            .unwrap();
    }
    let (_, misses, _, _, open) = pool.stats_counters();
    assert_eq!(misses, 4);
    assert_eq!(open, 2); // size-1 kept like upstream (size 3 → cap 2)
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn read_write_upgrade() {
    let d = dir("rw");
    let fs = make_fs();
    // READ_ONLY open of a missing file fails (no mkdir retry) —
    // create the file on disk first
    stdfs::create_dir_all(d.join("test_storage")).unwrap();
    stdfs::write(d.join("test_storage/file1.bin"), vec![0u8; 128]).unwrap();
    let pool = FilePool::new(40);
    let st = StorageIndex(0);
    let ro = pool
        .open_file(st, &d, FileIndex(1), &fs, OpenMode::READ_ONLY)
        .unwrap();
    // a write request must NOT get the ro handle
    let rw = pool
        .open_file(st, &d, FileIndex(1), &fs, OpenMode::WRITE)
        .unwrap();
    assert!(!std::sync::Arc::ptr_eq(&ro, &rw));
    // a subsequent read is served by the rw handle
    let ro2 = pool
        .open_file(st, &d, FileIndex(1), &fs, OpenMode::READ_ONLY)
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(&rw, &ro2));
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn release_and_resize() {
    let d = dir("rel");
    let fs = make_fs();
    let pool = FilePool::new(40);
    let st = StorageIndex(0);
    for i in 0..4 {
        pool.open_file(st, &d, FileIndex(i), &fs, OpenMode::WRITE)
            .unwrap();
    }
    pool.release_one(st, FileIndex(0));
    assert_eq!(pool.stats_counters().4, 3);
    pool.resize(2);
    assert_eq!(pool.stats_counters().4, 2);
    pool.release_storage(st);
    assert_eq!(pool.stats_counters().4, 0);
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn open_creates_parent_dirs() {
    // upstream open_file_impl retries after mkdir on ENOENT when
    // opening for write
    let d = dir("mkdir");
    let fs = make_fs();
    let pool = FilePool::new(40);
    let st = StorageIndex(0);
    pool.open_file(st, &d, FileIndex(0), &fs, OpenMode::WRITE)
        .unwrap();
    assert!(d.join("test_storage/file0.bin").exists());
    stdfs::remove_dir_all(&d).unwrap();
}
