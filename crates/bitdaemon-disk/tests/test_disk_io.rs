//! End-to-end `PreadDiskIo` — jobs flow through fence → pool →
//! completion, in both 0-thread (`immediate_execute`) and threaded mode.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::types::{DownloadPriority, PieceIndex, StorageIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage, RenamedFiles};
use bitdaemon_disk::disk_buffer_pool::DiskBufferHolder;
use bitdaemon_disk::disk_io::PreadDiskIo;
use bitdaemon_disk::disk_job::{DiskJob, JobKind, ReadJob, WriteJob};
use bitdaemon_disk::file_pool::FilePool;
use bitdaemon_disk::storage::{PreadStorage, StorageMode, StorageParams};

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("bitdaemon_dio_{}_{}", std::process::id(), name));
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

fn storage(fs: &FileStorage, d: &Path) -> PreadStorage {
    PreadStorage::new(
        StorageParams {
            files: fs,
            renamed: &RenamedFiles::default(),
            path: d.to_path_buf(),
            part_file_dir: PathBuf::new(),
            mode: StorageMode::Sparse,
            priorities: &[DownloadPriority::DEFAULT; 2],
            info_hash: [7u8; 20],
            v1: true,
            v2: false,
        },
        Arc::new(FilePool::new(40)),
    )
}

fn make_io(threads: i64) -> (Arc<PreadDiskIo>, Arc<AtomicUsize>) {
    let drained = Arc::new(AtomicUsize::new(0));
    let d2 = Arc::clone(&drained);
    let io: Arc<PreadDiskIo>;
    let io = {
        // poster must call io.completed().call_job_handlers — but io
        // doesn't exist yet. Post to a pending flag and drain manually
        // in the test (the session would wire its event loop here).
        io = PreadDiskIo::new(
            threads,
            Arc::new(move || {
                d2.fetch_add(1, Ordering::Relaxed);
            }),
        );
        io
    };
    (io, drained)
}

fn write_job(piece: i32, fill: u8, io: &PreadDiskIo, done: Arc<AtomicUsize>) -> Box<DiskJob> {
    let mut buf = io.buffer_pool().allocate_buffer("test");
    for b in buf.data_mut().unwrap() {
        *b = fill;
    }
    Box::new(DiskJob {
        storage: StorageIndex(u32::MAX),
        flags: 0,
        ret: 0,
        error: Default::default(),
        action: JobKind::Write(WriteJob {
            handler: Box::new(move |_| {
                done.fetch_add(1, Ordering::Relaxed);
            }),
            buf,
            piece: PieceIndex(piece),
            offset: 0,
            buffer_size: 1024,
        }),
    })
}

fn read_job(piece: i32, got: Arc<Mutex<Vec<u8>>>, done: Arc<AtomicUsize>) -> Box<DiskJob> {
    Box::new(DiskJob {
        storage: StorageIndex(u32::MAX),
        flags: 0,
        ret: 0,
        error: Default::default(),
        action: JobKind::Read(ReadJob {
            handler: Box::new(move |b: DiskBufferHolder, _| {
                *got.lock().unwrap() = b.data().unwrap_or(&[]).to_vec();
                done.fetch_add(1, Ordering::Relaxed);
            }),
            buf: DiskBufferHolder::empty(),
            buffer_size: 1024,
            piece: PieceIndex(piece),
            offset: 0,
            volatile: false,
        }),
    })
}

#[test]
fn immediate_execute_round_trip() {
    let d = dir("imm");
    let (io, _drained) = make_io(0);
    io.new_torrent(StorageIndex(0), storage(&make_fs(), &d));

    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));

    io.add_job(StorageIndex(0), write_job(0, 0xAB, &io, Arc::clone(&done)));

    // reads need an allocated buffer — do it via a job that gets buf
    // from the pool upstream-side
    let buf = io.buffer_pool().allocate_buffer("test");
    let mut j = read_job(0, Arc::clone(&got), Arc::clone(&done));
    if let JobKind::Read(r) = &mut j.action {
        r.buf = buf;
    }
    io.add_job(StorageIndex(0), j);

    // 0 threads → immediate_execute ran the jobs; completions pending
    assert_eq!(io.completed().call_job_handlers(), 2);
    assert_eq!(done.load(Ordering::Relaxed), 2);
    assert_eq!(got.lock().unwrap()[..4], [0xAB; 4]);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn threaded_pool_executes_and_completes() {
    let d = dir("thr");
    let (io, _drained) = make_io(4);
    io.new_torrent(StorageIndex(0), storage(&make_fs(), &d));

    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));

    io.add_job(StorageIndex(0), write_job(0, 0xCD, &io, Arc::clone(&done)));
    let buf = io.buffer_pool().allocate_buffer("test");
    let mut j = read_job(0, Arc::clone(&got), Arc::clone(&done));
    if let JobKind::Read(r) = &mut j.action {
        r.buf = buf;
    }
    io.add_job(StorageIndex(0), j);

    // wait for completions to land (posted via our drain counter)
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while done.load(Ordering::Relaxed) < 2 && std::time::Instant::now() < deadline {
        io.completed().call_job_handlers();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(done.load(Ordering::Relaxed), 2);
    assert_eq!(got.lock().unwrap()[..4], [0xCD; 4]);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn hash_job_computes_piece_sha1() {
    let d = dir("hash");
    let (io, _drained) = make_io(0);
    io.new_torrent(StorageIndex(0), storage(&make_fs(), &d));
    let done = Arc::new(AtomicUsize::new(0));

    io.add_job(StorageIndex(0), write_job(0, 0x11, &io, Arc::clone(&done)));

    let got = Arc::new(Mutex::new(Sha1Hash::default()));
    let g2 = Arc::clone(&got);
    let d2 = Arc::clone(&done);
    io.add_job(
        StorageIndex(0),
        Box::new(DiskJob {
            storage: StorageIndex(u32::MAX),
            flags: 0,
            ret: 0,
            error: Default::default(),
            action: JobKind::Hash(bitdaemon_disk::disk_job::HashJob {
                handler: Box::new(move |_, h, _| {
                    *g2.lock().unwrap() = h;
                    d2.fetch_add(1, Ordering::Relaxed);
                }),
                piece: PieceIndex(0),
                block_hashes: Vec::new(),
                piece_hash: Sha1Hash::default(),
            }),
        }),
    );

    assert_eq!(io.completed().call_job_handlers(), 2);
    // sha1(1024 * 0x11)
    use sha1::{Digest as _, Sha1};
    let expect = Sha1::digest([0x11u8; 1024]);
    assert_eq!(&got.lock().unwrap().as_bytes()[..], &expect[..]);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}
