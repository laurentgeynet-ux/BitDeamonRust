//! End-to-end `pread_storage` tests: piece-level read/write across
//! file boundaries, part-file redirection for `dont_download`,
//! `initialize`, `delete_files`, `verify_resume_data`.

use std::fs as stdfs;
use std::path::PathBuf;
use std::sync::Arc;

use bitdaemon_common::types::{DownloadPriority, PieceIndex, StorageIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage, RenamedFiles};
use bitdaemon_disk::file_pool::FilePool;
use bitdaemon_disk::open_mode::OpenMode;
use bitdaemon_disk::storage::{PreadStorage, StorageParams, disk_status};

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("bitdaemon_st_{}_{}", std::process::id(), name));
    let _ = stdfs::remove_dir_all(&d);
    stdfs::create_dir_all(&d).unwrap();
    d
}

/// two 1000-byte files, piece_length 1024 → piece 0 spans both
fn make_fs() -> FileStorage {
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(1024);
    fs.add_file(None, b"t/a.bin", 1000, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.add_file(None, b"t/b.bin", 1000, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.set_num_pieces(2); // (2000 + 1023) / 1024
    fs
}

fn storage(fs: &FileStorage, d: &std::path::Path, prio: &[DownloadPriority]) -> PreadStorage {
    PreadStorage::new(
        StorageParams {
            files: fs,
            renamed: &RenamedFiles::default(),
            path: d.to_path_buf(),
            part_file_dir: PathBuf::new(),
            mode: bitdaemon_disk::storage::StorageMode::Sparse,
            priorities: prio,
            info_hash: [7u8; 20],
            v1: true,
            v2: false,
        },
        Arc::new(FilePool::new(40)),
    )
}

#[test]
fn write_read_across_files() {
    let d = dir("rw");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));

    // piece 0: bytes 900..1024 land in a.bin tail and b.bin head
    let data = vec![0x5au8; 1024];
    let n = st.write(&data, PieceIndex(0), 0, OpenMode::WRITE).unwrap();
    assert_eq!(n, 1024);

    let mut out = vec![0u8; 1024];
    let n = st.read(&mut out, PieceIndex(0), 0, OpenMode::READ_ONLY, false);
    assert_eq!(n.unwrap(), 1024);
    assert_eq!(out, data);

    // partial-range read crossing the file boundary; b.bin only got
    // 24 bytes of piece 0 (the rest lives in piece 1, unwritten)
    let mut slice = vec![0u8; 124];
    st.read(&mut slice, PieceIndex(0), 900, OpenMode::READ_ONLY, false)
        .unwrap();
    assert_eq!(slice, vec![0x5au8; 124]);
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn part_file_redirection() {
    let d = dir("part");
    let fs = make_fs();
    // file 0 dont_download → its bytes go to the part file
    let prio = vec![DownloadPriority::DONT_DOWNLOAD, DownloadPriority::DEFAULT];
    let mut st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.initialize().unwrap();

    let data = vec![0x33u8; 1024];
    st.write(&data, PieceIndex(0), 0, OpenMode::WRITE).unwrap();

    // a.bin must NOT have been written to disk
    assert!(!d.join("t/a.bin").exists());
    // b.bin exists with the data
    assert!(d.join("t/b.bin").exists());

    // reads still work — a.bin bytes come from the part file
    let mut out = vec![0u8; 1024];
    st.read(&mut out, PieceIndex(0), 0, OpenMode::READ_ONLY, false)
        .unwrap();
    assert_eq!(out, data);

    // the part file exists
    assert!(
        d.join(".0707070707070707070707070707070707070707.parts")
            .exists()
    );
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn initialize_creates_zero_files() {
    let d = dir("init");
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(1024);
    fs.add_file(None, b"t/empty.bin", 0, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.set_num_pieces(0);
    let prio = vec![DownloadPriority::DEFAULT];
    let mut st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.initialize().unwrap();
    assert!(d.join("t/empty.bin").exists());
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn delete_files_removes_all() {
    let d = dir("del");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.write(&[1u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();
    st.delete_files(bitdaemon_disk::storage::remove_flags::DELETE_FILES)
        .unwrap();
    assert!(!d.join("t/a.bin").exists());
    assert!(!d.join("t/b.bin").exists());
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn verify_resume_seed_requires_sizes() {
    let d = dir("verify");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));

    let mut have = bitdaemon_common::bitfield::Bitfield::new();
    have.resize_with(2, false);
    // not a seed → missing files are tolerated when no piece is marked
    assert!(st.verify_resume_data(&have, false, &[]).unwrap());

    // seed with wrong/missing files → mismatching_file_size error
    have.set_all();
    let r = st.verify_resume_data(&have, false, &[]);
    assert!(r.is_err());

    // write real data (both pieces → complete files) → verify passes
    st.write(&[9u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();
    st.write(&[9u8; 976], PieceIndex(1), 0, OpenMode::WRITE)
        .unwrap();
    assert!(st.verify_resume_data(&have, false, &[]).unwrap());
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn sparse_files_grow_organically_allocate_extends() {
    // ADR-0022: sparse files are NOT extended to final size at open —
    // upstream leaves them at 0 and writes extend them where data
    // lands (FSCTL_SET_SPARSE handles zero-fill). Allocate mode still
    // sizes the file up front.
    let d = dir("sparse");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];

    // sparse: write 24 bytes of b.bin (piece 0 tail) → on-disk size
    // stays 24, not the declared 1000.
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.write(&[7u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();
    let len = stdfs::metadata(d.join("t/b.bin")).unwrap().len();
    assert_eq!(len, 24, "sparse file must grow organically, got {len}");

    // allocate: same write → file is extended to its declared size.
    let d2 = dir("alloc");
    let st2 = PreadStorage::new(
        StorageParams {
            files: &fs,
            renamed: &RenamedFiles::default(),
            path: d2.to_path_buf(),
            part_file_dir: PathBuf::new(),
            mode: bitdaemon_disk::storage::StorageMode::Allocate,
            priorities: &prio,
            info_hash: [7u8; 20],
            v1: true,
            v2: false,
        },
        Arc::new(FilePool::new(40)),
    );
    st2.set_storage_index(StorageIndex(0));
    st2.write(&[7u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();
    let len2 = stdfs::metadata(d2.join("t/b.bin")).unwrap().len();
    assert_eq!(len2, 1000, "allocate file must be pre-sized, got {len2}");

    stdfs::remove_dir_all(&d).unwrap();
    stdfs::remove_dir_all(&d2).unwrap();
}

#[test]
fn move_storage_replaces_path() {
    let d = dir("move_src");
    let d2 = dir("move_dst");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.write(&[2u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();

    let (status, new_path) = st
        .move_storage(
            d2.clone(),
            bitdaemon_disk::storage::MoveFlags::AlwaysReplaceFiles,
        )
        .unwrap();
    assert_eq!(status & !disk_status::NEED_FULL_CHECK, 0);
    assert!(new_path.join("t/b.bin").exists());
    stdfs::remove_dir_all(&d).unwrap();
    stdfs::remove_dir_all(&d2).unwrap();
}
