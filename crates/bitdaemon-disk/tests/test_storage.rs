//! End-to-end `pread_storage` tests: piece-level read/write across
//! file boundaries, part-file redirection for `dont_download`,
//! `initialize`, `delete_files`, `verify_resume_data`.

use std::fs as stdfs;
use std::path::PathBuf;
use std::sync::Arc;

use bitdaemon_common::types::{DownloadPriority, FileIndex, PieceIndex, StorageIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage, RenamedFiles};
use bitdaemon_disk::file_pool::FilePool;
use bitdaemon_disk::open_mode::OpenMode;
use bitdaemon_disk::storage::{MoveFlags, PreadStorage, StorageParams, disk_status, remove_flags};
use bitdaemon_disk::storage_error::Operation;
use bitdaemon_disk::storage_utils::move_storage;

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

/// Lifting `dont_download` on a file at index > 0 exports the parked
/// bytes into the real file at the *file-relative* offset (regression:
/// adding `file_offset` again wrote past EOF and dropped the data).
#[test]
fn part_file_export_lands_in_file() {
    let d = dir("export");
    let fs = make_fs();
    // start: a.bin wanted, b.bin parked in the part file
    let prio = vec![DownloadPriority::DEFAULT, DownloadPriority::DONT_DOWNLOAD];
    let mut st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.initialize().unwrap();

    let b_bytes: Vec<u8> = (0..1000u32).map(|i| (i % 193) as u8).collect();
    // piece 0: a.bin[0..1000] + b.bin[0..24]; piece 1: b.bin[24..1000]
    let mut p0 = vec![0x11u8; 1000];
    p0.extend_from_slice(&b_bytes[..24]);
    st.write(&p0, PieceIndex(0), 0, OpenMode::WRITE).unwrap();
    st.write(&b_bytes[24..], PieceIndex(1), 0, OpenMode::WRITE)
        .unwrap();

    st.set_file_priority(&[DownloadPriority::DEFAULT, DownloadPriority::DEFAULT])
        .unwrap();

    let on_disk = stdfs::read(d.join("t/b.bin")).unwrap();
    assert_eq!(on_disk.len(), 1000);
    assert_eq!(on_disk, b_bytes);
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
    // (upstream `verify_resume_data` → `(true, no error)`)
    let (ok, err) = st.verify_resume_data(&have, false, &[]);
    assert!(ok && err.is_none());

    // seed with wrong/missing files → `(false, mismatching_file_size)`
    // — upstream's verdict+error pair, not a hard Err
    have.set_all();
    let (ok, err) = st.verify_resume_data(&have, false, &[]);
    assert!(!ok);
    assert!(err.is_some());

    // write real data (both pieces → complete files) → verify passes
    st.write(&[9u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();
    st.write(&[9u8; 976], PieceIndex(1), 0, OpenMode::WRITE)
        .unwrap();
    let (ok, err) = st.verify_resume_data(&have, false, &[]);
    assert!(ok);
    assert!(err.is_none());
    stdfs::remove_dir_all(&d).unwrap();
}

#[test]
fn sparse_files_grow_organically_allocate_extends() {
    // ADR-0022 + file.cpp:607-697: a sparse-mode file is still
    // `ftruncate`d to its declared size on the first write-open —
    // POSIX keeps a full `st_size` made of holes; Windows does not
    // extend sparse files (FSCTL_SET_SPARSE + organic growth).
    let d = dir("sparse");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];

    // write 24 bytes of b.bin (piece 0 tail)
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.write(&[7u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();
    let len = stdfs::metadata(d.join("t/b.bin")).unwrap().len();
    #[cfg(unix)]
    assert_eq!(
        len, 1000,
        "posix sparse: ftruncate sizes the file, got {len}"
    );
    #[cfg(windows)]
    assert_eq!(len, 24, "windows sparse files grow organically, got {len}");

    // allocate: posix_fallocate reserves the declared size up front;
    // on Windows upstream only extends when enable_set_file_valid_data
    // is set (default off → organic growth, non-sparse zero-fill).
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
    #[cfg(unix)]
    assert_eq!(len2, 1000, "posix_fallocate must pre-size, got {len2}");
    #[cfg(windows)]
    assert_eq!(len2, 24, "windows default grows organically, got {len2}");

    // windows allocate + enable_set_file_valid_data → extended + SFVD.
    #[cfg(windows)]
    {
        let d3 = dir("alloc_svfd");
        let st3 = PreadStorage::new(
            StorageParams {
                files: &fs,
                renamed: &RenamedFiles::default(),
                path: d3.to_path_buf(),
                part_file_dir: PathBuf::new(),
                mode: bitdaemon_disk::storage::StorageMode::Allocate,
                priorities: &prio,
                info_hash: [7u8; 20],
                v1: true,
                v2: false,
            },
            Arc::new(FilePool::new(40)),
        );
        st3.set_storage_index(StorageIndex(0));
        bitdaemon_disk::storage::DiskStorage::set_open_mode_extra(
            &st3,
            OpenMode::ALLOW_SET_FILE_VALID_DATA,
        );
        st3.write(&[7u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
            .unwrap();
        let len3 = stdfs::metadata(d3.join("t/b.bin")).unwrap().len();
        assert_eq!(len3, 1000, "SFVD mode must pre-size, got {len3}");
        stdfs::remove_dir_all(&d3).unwrap();
    }

    stdfs::remove_dir_all(&d).unwrap();
    stdfs::remove_dir_all(&d2).unwrap();
}

/// `m_file_created` — the `truncate` resize must happen exactly once
/// per file per session: after the first write-open, rewriting must
/// not re-size the file (a manual shrink must survive further opens).
#[test]
fn truncate_happens_once_per_file() {
    let d = dir("once");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.write(&[7u8; 1024], PieceIndex(0), 0, OpenMode::WRITE)
        .unwrap();

    // shrink below the declared size, then write again — a second
    // `truncate` would grow it back to 1000; upstream must not.
    let path = d.join("t/b.bin");
    stdfs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(64)
        .unwrap();
    st.release_files().unwrap();
    st.write(&[9u8; 4], PieceIndex(1), 0, OpenMode::WRITE)
        .unwrap();
    let len = stdfs::metadata(&path).unwrap().len();
    assert_eq!(len, 64, "second open must not re-truncate, got {len}");
    stdfs::remove_dir_all(&d).unwrap();
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

    let (status, new_path, err) = st.move_storage(
        d2.clone(),
        bitdaemon_disk::storage::MoveFlags::AlwaysReplaceFiles,
    );
    assert!(err.is_none());
    assert_eq!(status & !disk_status::NEED_FULL_CHECK, 0);
    assert!(new_path.join("t/b.bin").exists());
    stdfs::remove_dir_all(&d).unwrap();
    stdfs::remove_dir_all(&d2).unwrap();
}

// ---------------------------------------------------------------------------
// upstream `test_storage.cpp` move-storage rollback regression trio
// (delta fiche #01 — storage_utils.cpp:162-211)
// ---------------------------------------------------------------------------

/// `payload/{first,second}` — upstream's fixture shape (no torrent
/// name prefix; files sit under `payload/` directly).
fn move_fs() -> FileStorage {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    fs.add_file(None, b"payload/first", 1, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.add_file(None, b"payload/second", 1, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.set_num_pieces(1);
    fs
}

/// upstream `move_storage_rolls_back_after_partfile_error`: a part-file
/// move failure after every file renamed must roll the whole move
/// back — `fatal_disk_error`, unchanged save path, `partfile_move`
/// operation, source files restored, destination files gone.
#[test]
fn move_storage_rolls_back_after_partfile_error() {
    let src = dir("ms_rb_src");
    // upstream leaves the destination absent — `move_storage` creates it
    let dst = std::env::temp_dir().join(format!("bitdaemon_st_{}_ms_rb_dst", std::process::id()));
    let _ = stdfs::remove_dir_all(&dst);

    let fs = move_fs();
    stdfs::create_dir_all(src.join("payload")).unwrap();
    stdfs::write(src.join("payload/first"), b"x").unwrap();
    stdfs::write(src.join("payload/second"), b"x").unwrap();

    let attempted = std::cell::Cell::new(false);
    let (status, save_path, err) = move_storage(
        &fs,
        &RenamedFiles::default(),
        src.clone(),
        dst.clone(),
        |_| {
            attempted.set(true);
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        },
        MoveFlags::AlwaysReplaceFiles,
    );

    assert!(attempted.get(), "part-file move must be attempted");
    assert_ne!(status & disk_status::FATAL_DISK_ERROR, 0);
    assert_eq!(save_path, src, "save path stays the old one on failure");
    let e = err.expect("part-file failure must surface");
    assert_eq!(e.operation, Operation::PartfileMove);
    // upstream `ec.file(error_file_partfile)` = -6
    assert_eq!(e.file, FileIndex(-6));
    for name in ["first", "second"] {
        assert!(src.join("payload").join(name).exists());
        assert!(!dst.join("payload").join(name).exists());
    }
    stdfs::remove_dir_all(&src).unwrap();
    stdfs::remove_dir_all(&dst).unwrap();
}

/// upstream `move_storage_rollback_preserves_skipped_files`: under
/// `dont_replace`, a file that exists at the destination is skipped —
/// its source stays put and a later failure must not roll it back
/// (nor touch the pre-existing destination copy).
#[test]
fn move_storage_rollback_preserves_skipped_files() {
    let src = dir("ms_skip_src");
    let dst = dir("ms_skip_dst");
    let fs = move_fs();
    stdfs::create_dir_all(src.join("payload")).unwrap();
    stdfs::create_dir_all(dst.join("payload")).unwrap();
    stdfs::write(src.join("payload/first"), b"s").unwrap();
    stdfs::write(src.join("payload/second"), b"m").unwrap();
    stdfs::write(dst.join("payload/first"), b"d").unwrap();

    let (status, save_path, err) = move_storage(
        &fs,
        &RenamedFiles::default(),
        src.clone(),
        dst.clone(),
        |_| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        MoveFlags::DontReplace,
    );

    assert_ne!(status & disk_status::FATAL_DISK_ERROR, 0);
    assert_eq!(save_path, src);
    assert!(err.is_some());
    assert_eq!(stdfs::read(src.join("payload/first")).unwrap(), b"s");
    assert_eq!(stdfs::read(dst.join("payload/first")).unwrap(), b"d");
    assert_eq!(stdfs::read(src.join("payload/second")).unwrap(), b"m");
    assert!(!dst.join("payload/second").exists());
    stdfs::remove_dir_all(&src).unwrap();
    stdfs::remove_dir_all(&dst).unwrap();
}

/// upstream `move_storage_rollback_preserves_missing_source_files`: a
/// declared-but-missing source file is `ENOENT`-skipped; a pre-existing
/// unrelated destination file with the same name must survive the
/// rollback untouched (rollback only sweeps files actually renamed).
#[test]
fn move_storage_rollback_preserves_missing_source_files() {
    let src = dir("ms_miss_src");
    let dst = dir("ms_miss_dst");
    let fs = move_fs();
    stdfs::create_dir_all(src.join("payload")).unwrap();
    stdfs::create_dir_all(dst.join("payload")).unwrap();
    // `first` declared but absent at source; unrelated `first` at dest
    stdfs::write(src.join("payload/second"), b"m").unwrap();
    stdfs::write(dst.join("payload/first"), b"d").unwrap();

    let (status, save_path, err) = move_storage(
        &fs,
        &RenamedFiles::default(),
        src.clone(),
        dst.clone(),
        |_| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        MoveFlags::AlwaysReplaceFiles,
    );

    assert_ne!(status & disk_status::FATAL_DISK_ERROR, 0);
    assert_eq!(save_path, src);
    assert!(err.is_some());
    assert!(!src.join("payload/first").exists());
    assert_eq!(stdfs::read(dst.join("payload/first")).unwrap(), b"d");
    assert_eq!(stdfs::read(src.join("payload/second")).unwrap(), b"m");
    assert!(!dst.join("payload/second").exists());
    stdfs::remove_dir_all(&src).unwrap();
    stdfs::remove_dir_all(&dst).unwrap();
}

/// upstream `delete_files` error channel (storage_utils.cpp:319-355):
/// `ec.ec` is overwritten by *each* file's delete result — the **last**
/// file wins, so a mid-list failure followed by a success clears; only
/// then does the **first** directory-cleanup failure surface
/// (`FileIndex(-1)`). A failing *last* file keeps its index.
#[test]
fn delete_files_error_ordering() {
    let make3 = || {
        let mut fs = FileStorage::new();
        fs.set_name(b"t");
        fs.set_piece_length(1024);
        for n in ["a.bin", "b.bin", "c.bin"] {
            fs.add_file(
                None,
                format!("t/{n}").as_bytes(),
                100,
                FileFlags::NONE,
                0,
                b"",
                None,
            )
            .unwrap();
        }
        fs.set_num_pieces(1);
        fs
    };

    // mid-list failure cleared by a later success → dir error wins
    let d = dir("del_mid");
    let fs = make3();
    let prio = vec![DownloadPriority::DEFAULT; 3];
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    stdfs::create_dir_all(d.join("t")).unwrap();
    stdfs::write(d.join("t/a.bin"), b"a").unwrap();
    // b.bin is a non-empty *directory* — remove fails on every OS
    stdfs::create_dir_all(d.join("t/b.bin")).unwrap();
    stdfs::write(d.join("t/b.bin/x"), b"x").unwrap();
    stdfs::write(d.join("t/c.bin"), b"c").unwrap();

    let e = st
        .delete_files(remove_flags::DELETE_FILES)
        .expect_err("dir cleanup must report the failure");
    // file phase ended clean (c succeeded after b failed) → the first
    // dir-remove failure is the reported one, not b's FileIndex(1)
    assert_eq!(e.file, FileIndex(-1));
    assert_eq!(e.operation, Operation::FileRemove);
    assert!(!d.join("t/a.bin").exists());
    assert!(!d.join("t/c.bin").exists());
    assert!(d.join("t/b.bin").is_dir(), "failed file must remain");
    stdfs::remove_dir_all(&d).unwrap();

    // last file fails → its error is reported; dir failures masked
    let d2 = dir("del_last");
    let st2 = storage(&fs, &d2, &prio);
    st2.set_storage_index(StorageIndex(0));
    stdfs::create_dir_all(d2.join("t")).unwrap();
    stdfs::write(d2.join("t/a.bin"), b"a").unwrap();
    stdfs::write(d2.join("t/b.bin"), b"b").unwrap();
    stdfs::create_dir_all(d2.join("t/c.bin")).unwrap();
    stdfs::write(d2.join("t/c.bin/x"), b"x").unwrap();

    let e = st2
        .delete_files(remove_flags::DELETE_FILES)
        .expect_err("last file failure must surface");
    assert_eq!(e.file, FileIndex(2));
    assert_eq!(e.operation, Operation::FileRemove);
    assert!(!d2.join("t/a.bin").exists());
    assert!(!d2.join("t/b.bin").exists());
    assert!(d2.join("t/c.bin").is_dir());
    stdfs::remove_dir_all(&d2).unwrap();
}

/// `disk_io_write_mode == write_through` — `sync_file` runs after
/// each landed write and its errors are swallowed (upstream
/// `pread_storage.cpp` calls it without an `ec` out-param). The
/// write must succeed and the data must read back.
#[test]
fn write_through_syncs_and_lands() {
    use bitdaemon_disk::storage::DiskStorage;

    let d = dir("wt");
    let fs = make_fs();
    let prio = vec![DownloadPriority::DEFAULT; 2];
    let st = storage(&fs, &d, &prio);
    st.set_storage_index(StorageIndex(0));
    st.set_write_through(true);

    let data = vec![0x41u8; 1024];
    st.write(&data, PieceIndex(0), 0, OpenMode::WRITE).unwrap();
    let mut out = vec![0u8; 1024];
    st.read(&mut out, PieceIndex(0), 0, OpenMode::READ_ONLY, false)
        .unwrap();
    assert_eq!(out, data);
    stdfs::remove_dir_all(&d).unwrap();
}
