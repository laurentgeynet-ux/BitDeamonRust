//! End-to-end `PreadDiskIo` — jobs flow through fence → pool →
//! completion, in both 0-thread (`immediate_execute`) and threaded mode.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::types::{DownloadPriority, FileIndex, PieceIndex, StorageIndex};
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

/// real-swarm catch: a burst of `async_read` uploads (seed serving
/// a leech) must all complete — buffers and handlers must not get
/// stuck behind the pool worker machinery.
#[test]
fn read_burst_all_complete() {
    use bitdaemon_disk::disk_interface::DiskIo;

    let d = dir("read_burst");
    std::fs::create_dir_all(d.join("t")).unwrap();
    std::fs::write(d.join("t/a.bin"), vec![0xABu8; 1000]).unwrap();
    std::fs::write(d.join("t/b.bin"), vec![0xCDu8; 1000]).unwrap();

    let (io, _drained) = make_io(4);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));

    const N: usize = 300;
    let done = Arc::new(AtomicUsize::new(0));
    for i in 0..N {
        let d2 = Arc::clone(&done);
        io.async_read(
            StorageIndex(0),
            bitdaemon_common::types::PeerRequest {
                piece: PieceIndex((i % 2) as i32),
                start: 0,
                length: 1000,
            },
            Box::new(move |b, e| {
                assert!(!e.is_err());
                // read jobs return the whole 16 KiB disk block; the
                // requester slices `length` out of it
                let data = b.data().unwrap_or(&[]);
                assert!(data.len() >= 1000);
                d2.fetch_add(1, Ordering::Relaxed);
            }),
            0,
        );
    }
    for _ in 0..500 {
        io.completed().call_job_handlers();
        if done.load(Ordering::Relaxed) == N {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("only {}/{N} reads completed", done.load(Ordering::Relaxed));
}

/// real-swarm catch: `check_fastresume` with **no resume data** must
/// return `need_full_check` (ret bit0 = 0) when files exist on disk,
/// and `accepted` only when the directory is empty — upstream
/// `pread_disk_io::do_job(check_fastresume)`:
/// `!contains_resume_data → has_any_file ? need_full_check : ok`.
/// Previously the empty `have` bitfield made `verify_resume_data`
/// answer "ok", so existing data was never re-hashed.
#[test]
fn check_fastresume_no_resume_needs_full_check_when_files_exist() {
    use bitdaemon_disk::disk_interface::DiskIo;

    let wait = |io: &PreadDiskIo, done: &AtomicUsize| {
        for _ in 0..200 {
            io.completed().call_job_handlers();
            if done.load(Ordering::Relaxed) > 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("check_fastresume never completed");
    };

    // files on disk, no resume data → need_full_check
    let d = dir("nocheck_full");
    let (io, _drained) = make_io(1);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));
    std::fs::create_dir_all(d.join("t")).unwrap();
    std::fs::write(d.join("t/a.bin"), vec![7u8; 1000]).unwrap();
    let done = Arc::new(AtomicUsize::new(0));
    let ret = Arc::new(AtomicUsize::new(usize::MAX));
    let (d2, r2) = (Arc::clone(&done), Arc::clone(&ret));
    io.async_check_files(
        StorageIndex(0),
        None,
        Vec::new(),
        Box::new(move |st, _err| {
            r2.store(st as usize, Ordering::Relaxed);
            d2.fetch_add(1, Ordering::Relaxed);
        }),
    );
    wait(&io, &done);
    assert_eq!(
        ret.load(Ordering::Relaxed) & 0xff,
        0,
        "must need_full_check"
    );

    // empty directory, no resume data → accepted (nothing to check)
    let d2 = dir("nocheck_empty");
    io.insert_storage(StorageIndex(1), Box::new(storage(&make_fs(), &d2)));
    let done2 = Arc::new(AtomicUsize::new(0));
    let ret2 = Arc::new(AtomicUsize::new(usize::MAX));
    let (dd, rr) = (Arc::clone(&done2), Arc::clone(&ret2));
    io.async_check_files(
        StorageIndex(1),
        None,
        Vec::new(),
        Box::new(move |st, _err| {
            rr.store(st as usize, Ordering::Relaxed);
            dd.fetch_add(1, Ordering::Relaxed);
        }),
    );
    wait(&io, &done2);
    assert_eq!(ret2.load(Ordering::Relaxed) & 0xff, 1, "must accept");
}

#[test]
fn immediate_execute_round_trip() {
    let d = dir("imm");
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));

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
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));

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
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));
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
                handler: Box::new(move |_, h, _v2, _| {
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

#[test]
fn hash_parked_behind_inflight_write_is_released() {
    use std::sync::Barrier;

    use bitdaemon_disk::open_mode::OpenMode;
    use bitdaemon_disk::storage::{DiskStorage, MoveFlags};
    use bitdaemon_disk::storage_error::StorageResult;

    // `write` blocks on a barrier so the piece_gate holds the hash job
    // deterministically until the test releases the worker.
    struct GatedStorage {
        inner: PreadStorage,
        gate: Arc<Barrier>,
    }
    impl DiskStorage for GatedStorage {
        fn set_storage_index(&self, idx: StorageIndex) {
            self.inner.set_storage_index(idx);
        }
        fn files(&self) -> &FileStorage {
            self.inner.files()
        }
        fn file_priorities(&self) -> &[DownloadPriority] {
            self.inner.file_priorities()
        }
        fn read(
            &self,
            buf: &mut [u8],
            piece: PieceIndex,
            offset: i64,
            mode: OpenMode,
            volatile: bool,
        ) -> StorageResult<i64> {
            self.inner.read(buf, piece, offset, mode, volatile)
        }
        fn write(
            &self,
            buf: &[u8],
            piece: PieceIndex,
            offset: i64,
            mode: OpenMode,
        ) -> StorageResult<i64> {
            self.gate.wait();
            self.inner.write(buf, piece, offset, mode)
        }
        fn hash_with(
            &self,
            feed: &mut dyn FnMut(&[u8]),
            len: i64,
            piece: PieceIndex,
            offset: i64,
            mode: OpenMode,
            volatile: bool,
        ) -> StorageResult<i64> {
            self.inner
                .hash_with(feed, len, piece, offset, mode, volatile)
        }
        fn release_files(&self) -> StorageResult<()> {
            self.inner.release_files()
        }
        fn move_storage(&self, _p: PathBuf, _f: MoveFlags) -> StorageResult<(u8, PathBuf)> {
            unimplemented!()
        }
        fn delete_files(&self, _o: u8) -> StorageResult<()> {
            unimplemented!()
        }
        fn rename_file(&mut self, _i: FileIndex, _n: &[u8]) -> StorageResult<()> {
            unimplemented!()
        }
        fn set_file_priority(&mut self, _p: &[DownloadPriority]) -> StorageResult<()> {
            unimplemented!()
        }
        fn verify_resume_data(
            &self,
            _h: &bitdaemon_common::bitfield::Bitfield,
            _s: bool,
            _l: &[PathBuf],
        ) -> StorageResult<bool> {
            unimplemented!()
        }
    }

    let d = dir("gate");
    let (io, _drained) = make_io(1);
    let gate = Arc::new(Barrier::new(2));
    io.insert_storage(
        StorageIndex(0),
        Box::new(GatedStorage {
            inner: storage(&make_fs(), &d),
            gate: Arc::clone(&gate),
        }),
    );

    let done = Arc::new(AtomicUsize::new(0));
    io.add_job(StorageIndex(0), write_job(0, 0x5A, &io, Arc::clone(&done)));

    // the write is registered against piece 0 and cannot complete
    // while the barrier holds — this hash job parks in piece_gate
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
                handler: Box::new(move |_, h, _v2, _| {
                    *g2.lock().unwrap() = h;
                    d2.fetch_add(1, Ordering::Relaxed);
                }),
                piece: PieceIndex(0),
                block_hashes: Vec::new(),
                piece_hash: Sha1Hash::default(),
            }),
        }),
    );

    gate.wait();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while done.load(Ordering::Relaxed) < 2 && std::time::Instant::now() < deadline {
        io.completed().call_job_handlers();
        std::thread::sleep(Duration::from_millis(5));
    }
    // released hash jobs must reach the pool (not re-enter the fence)
    assert_eq!(done.load(Ordering::Relaxed), 2);
    use sha1::{Digest as _, Sha1};
    let expect = Sha1::digest([0x5Au8; 1024]);
    assert_eq!(&got.lock().unwrap().as_bytes()[..], &expect[..]);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn file_progress_accounts_per_file() {
    use bitdaemon_common::bitfield::Bitfield;
    use bitdaemon_disk::file_progress::FileProgress;
    let fs = make_fs();
    let mut fp = FileProgress::new();
    let mut have = Bitfield::new();
    have.resize_with(2, false);
    have.set_bit(0);
    fp.init(&have, &fs);
    // piece 0 = 24 bytes in a.bin tail + 1000 in b.bin head? no —
    // piece 0 covers bytes 0..1024: a.bin[0..1000] + b.bin[0..24]
    let mut v = Vec::new();
    fp.export_progress(&mut v);
    assert_eq!(v, vec![1000, 24]);
    assert_eq!(fp.total_on_disk(), 1024);

    // completing piece 1 (remaining 976 bytes of b.bin)
    let mut completed = Vec::new();
    fp.update(&fs, PieceIndex(1), |f| completed.push(f));
    fp.export_progress(&mut v);
    assert_eq!(v, vec![1000, 1000]);
    assert_eq!(completed, vec![FileIndex(1)]);
}

#[test]
fn disabled_backend_completes_noop() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::{DisabledDiskIo, DiskIo};
    let io = DisabledDiskIo::new();
    let hit = Arc::new(AtomicUsize::new(0));
    let h = Arc::clone(&hit);
    io.async_read(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 0x4000,
        },
        Box::new(move |_, _| {
            h.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );
    assert_eq!(hit.load(Ordering::Relaxed), 1);
}

/// real-swarm catch (Ubuntu 6.5 GiB swarm): a flush whose `write`
/// fails must NOT mark its blocks `flushed` — `run_flushed` used to
/// retire them unconditionally, leaving verified pieces with
/// never-written zero holes on disk. The buffers must return to the
/// cache and retry instead.
#[test]
fn flush_error_restores_buffers() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;
    use bitdaemon_disk::open_mode::OpenMode;
    use bitdaemon_disk::storage::{DiskStorage, MoveFlags};
    use bitdaemon_disk::storage_error::{Operation, StorageError, StorageResult};
    use std::sync::atomic::AtomicBool;

    struct FlakyStorage {
        inner: PreadStorage,
        fail_once: AtomicBool,
    }
    impl DiskStorage for FlakyStorage {
        fn set_storage_index(&self, idx: StorageIndex) {
            self.inner.set_storage_index(idx);
        }
        fn files(&self) -> &FileStorage {
            self.inner.files()
        }
        fn file_priorities(&self) -> &[DownloadPriority] {
            self.inner.file_priorities()
        }
        fn read(
            &self,
            buf: &mut [u8],
            piece: PieceIndex,
            offset: i64,
            mode: OpenMode,
            volatile: bool,
        ) -> StorageResult<i64> {
            self.inner.read(buf, piece, offset, mode, volatile)
        }
        fn write(
            &self,
            buf: &[u8],
            piece: PieceIndex,
            offset: i64,
            mode: OpenMode,
        ) -> StorageResult<i64> {
            if self.fail_once.swap(false, Ordering::Relaxed) {
                return Err(StorageError::new(
                    std::io::Error::other("flaky"),
                    FileIndex(0),
                    Operation::FileWrite,
                ));
            }
            self.inner.write(buf, piece, offset, mode)
        }
        fn hash_with(
            &self,
            feed: &mut dyn FnMut(&[u8]),
            len: i64,
            piece: PieceIndex,
            offset: i64,
            mode: OpenMode,
            volatile: bool,
        ) -> StorageResult<i64> {
            self.inner
                .hash_with(feed, len, piece, offset, mode, volatile)
        }
        fn release_files(&self) -> StorageResult<()> {
            self.inner.release_files()
        }
        fn move_storage(&self, _p: PathBuf, _f: MoveFlags) -> StorageResult<(u8, PathBuf)> {
            unimplemented!()
        }
        fn delete_files(&self, _o: u8) -> StorageResult<()> {
            unimplemented!()
        }
        fn rename_file(&mut self, _i: FileIndex, _n: &[u8]) -> StorageResult<()> {
            unimplemented!()
        }
        fn set_file_priority(&mut self, _p: &[DownloadPriority]) -> StorageResult<()> {
            unimplemented!()
        }
        fn verify_resume_data(
            &self,
            _h: &bitdaemon_common::bitfield::Bitfield,
            _s: bool,
            _l: &[PathBuf],
        ) -> StorageResult<bool> {
            unimplemented!()
        }
    }

    let d = dir("flaky");
    let (io, _drained) = make_io(2);
    io.insert_storage(
        StorageIndex(0),
        Box::new(FlakyStorage {
            inner: storage(&make_fs(), &d),
            fail_once: AtomicBool::new(true),
        }),
    );

    // one full piece (1024 B = 1 cached block) → piece_full → flush job
    let mut buf = io.buffer_pool().allocate_buffer("t");
    buf.data_mut().unwrap()[..1024].fill(0x5A);
    let hit = Arc::new(AtomicUsize::new(0));
    let h = Arc::clone(&hit);
    io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 1024,
        },
        buf,
        None,
        Box::new(move |_| {
            h.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );

    // the first flush attempt must fail; the block goes back dirty
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while DiskIo::flush_error_count(&*io) == 0 && std::time::Instant::now() < deadline {
        io.completed().call_job_handlers();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(DiskIo::flush_error_count(&*io), 1);

    // retry — buffers restored, so the second attempt lands the data
    io.flush_all();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        io.completed().call_job_handlers();
        let a = std::fs::read(d.join("t/a.bin")).unwrap_or_default();
        let b = std::fs::read(d.join("t/b.bin")).unwrap_or_default();
        // piece 0 spans a.bin[0..1000] + b.bin[0..24]
        if a.len() == 1000
            && a.iter().all(|&x| x == 0x5A)
            && b.len() >= 24
            && b[..24].iter().all(|&x| x == 0x5A)
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "data never flushed");
        std::thread::sleep(Duration::from_millis(5));
    }
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}
