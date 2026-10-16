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
            force_flush: false,
            volatile: false,
        }),
    })
}

/// `write` blocks on a barrier so in-flight work holds
/// deterministically until the test releases the worker.
struct GatedStorage {
    inner: PreadStorage,
    gate: Arc<std::sync::Barrier>,
}
impl bitdaemon_disk::storage::DiskStorage for GatedStorage {
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
        mode: bitdaemon_disk::open_mode::OpenMode,
        volatile: bool,
    ) -> bitdaemon_disk::storage_error::StorageResult<i64> {
        self.inner.read(buf, piece, offset, mode, volatile)
    }
    fn write(
        &self,
        buf: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: bitdaemon_disk::open_mode::OpenMode,
    ) -> bitdaemon_disk::storage_error::StorageResult<i64> {
        self.gate.wait();
        self.inner.write(buf, piece, offset, mode)
    }
    fn hash_with(
        &self,
        feed: &mut dyn FnMut(&[u8]),
        len: i64,
        piece: PieceIndex,
        offset: i64,
        mode: bitdaemon_disk::open_mode::OpenMode,
        volatile: bool,
    ) -> bitdaemon_disk::storage_error::StorageResult<i64> {
        self.inner
            .hash_with(feed, len, piece, offset, mode, volatile)
    }
    fn release_files(&self) -> bitdaemon_disk::storage_error::StorageResult<()> {
        self.inner.release_files()
    }
    fn move_storage(
        &self,
        _p: PathBuf,
        _f: bitdaemon_disk::storage::MoveFlags,
    ) -> (
        u8,
        PathBuf,
        Option<bitdaemon_disk::storage_error::StorageError>,
    ) {
        unimplemented!()
    }
    fn delete_files(&self, _o: u8) -> bitdaemon_disk::storage_error::StorageResult<()> {
        unimplemented!()
    }
    fn rename_file(
        &mut self,
        _i: FileIndex,
        _n: &[u8],
    ) -> bitdaemon_disk::storage_error::StorageResult<()> {
        unimplemented!()
    }
    fn set_file_priority(
        &mut self,
        _p: &[DownloadPriority],
    ) -> bitdaemon_disk::storage_error::StorageResult<()> {
        unimplemented!()
    }
    fn verify_resume_data(
        &self,
        _h: &bitdaemon_common::bitfield::Bitfield,
        _s: bool,
        _l: &[PathBuf],
    ) -> (bool, Option<bitdaemon_disk::storage_error::StorageError>) {
        unimplemented!()
    }
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
            flush: false,
            sequential: false,
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
/// return `need_full_check` (`ret & status_t::need_full_check`) when
/// files exist on disk, and `0` only when the directory is empty —
/// upstream `pread_disk_io::do_job(check_fastresume)`:
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
    // `ret` is a pure `status_t` bitmask: `need_full_check` (bit 1)
    // must be set — files exist but no resume data describes them.
    assert_ne!(
        ret.load(Ordering::Relaxed)
            & bitdaemon_disk::storage::disk_status::NEED_FULL_CHECK as usize,
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
    // empty directory + no resume data → `status_t` clean (accepted)
    assert_eq!(ret2.load(Ordering::Relaxed), 0, "must accept");
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
    // (write → cache-insert → piece-full flush + read = 3 completed jobs)
    assert_eq!(io.completed().call_job_handlers(), 3);
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
                handler: Box::new(move |_, h, _v2, _digests, _| {
                    *g2.lock().unwrap() = h;
                    d2.fetch_add(1, Ordering::Relaxed);
                }),
                piece: PieceIndex(0),
                block_hashes: Vec::new(),
                block_digests: Vec::new(),
                piece_hash: Sha1Hash::default(),
                v1_hash: true,
                volatile: false,
                flush: false,
                sequential: false,
            }),
        }),
    );

    // write → cache-insert → piece-full flush + hash = 3 completions
    assert_eq!(io.completed().call_job_handlers(), 3);
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
                handler: Box::new(move |_, h, _v2, _digests, _| {
                    *g2.lock().unwrap() = h;
                    d2.fetch_add(1, Ordering::Relaxed);
                }),
                piece: PieceIndex(0),
                block_hashes: Vec::new(),
                block_digests: Vec::new(),
                piece_hash: Sha1Hash::default(),
                v1_hash: true,
                volatile: false,
                flush: false,
                sequential: false,
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
/// fails completes every parked writer with the error — upstream
/// `flush_cache_blocks` sets `j2->error` and still marks the visited
/// blocks `flushed` (the buffer is dropped, not retried; the peer
/// layer re-requests the piece). The write handler must observe the
/// failure exactly once, and a later `flush_all` must not resurrect
/// the dropped block.
#[test]
fn flush_error_propagates_to_writer() {
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
        fn move_storage(&self, _p: PathBuf, _f: MoveFlags) -> (u8, PathBuf, Option<StorageError>) {
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
        ) -> (bool, Option<StorageError>) {
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
    let saw_err = Arc::new(AtomicBool::new(false));
    let (h, se) = (Arc::clone(&hit), Arc::clone(&saw_err));
    io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 1024,
        },
        buf,
        None,
        Box::new(move |e| {
            // upstream: the write handler receives the *flush* error —
            // a cached write is not durable until the block lands.
            se.store(e.is_err(), Ordering::Relaxed);
            h.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );

    // the first flush attempt must fail; the writer completes with
    // the propagated error exactly once.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while hit.load(Ordering::Relaxed) == 0 && std::time::Instant::now() < deadline {
        io.completed().call_job_handlers();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(hit.load(Ordering::Relaxed), 1);
    assert!(
        saw_err.load(Ordering::Relaxed),
        "writer must see the flush error"
    );
    assert_eq!(DiskIo::flush_error_count(&*io), 1);

    // upstream marks the visited span flushed even on error — no
    // restore, no silent retry: `flush_all` must not resurrect it.
    io.flush_all();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        io.completed().call_job_handlers();
        std::thread::sleep(Duration::from_millis(20));
        if io.completed().pending() == 0 {
            break;
        }
    }
    let a = std::fs::read(d.join("t/a.bin")).unwrap_or_default();
    assert!(
        !a.iter().all(|&x| x == 0x5A) || a.len() < 1000,
        "failed block must not be re-flushed"
    );
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

// ---------------------------------------------------------------------------
// `prepare_read` — read-side cache consult (upstream `pread_disk_io`)
// ---------------------------------------------------------------------------

/// one 64 KiB piece = four 16 KiB cache blocks — big enough for
/// spanning (unaligned) read coverage, unlike `make_fs`'s 1 KiB pieces.
fn make_fs_big() -> FileStorage {
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(0x10000);
    fs.add_file(None, b"t/a.bin", 0x8000, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.add_file(None, b"t/b.bin", 0x8000, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.set_num_pieces(1);
    fs
}

/// `async_write` through the production path — the block lands in the
/// write cache and completes through the queue.
fn aw(io: &PreadDiskIo, start: i32, length: i32, fill: u8, done: Arc<AtomicUsize>) -> bool {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;
    let mut buf = io.buffer_pool().allocate_buffer("w");
    for b in buf.data_mut().unwrap() {
        *b = fill;
    }
    let d = Arc::clone(&done);
    io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start,
            length,
        },
        buf,
        None,
        Box::new(move |_| {
            d.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    )
}

fn ar(io: &PreadDiskIo, start: i32, length: i32, got: Arc<Mutex<Vec<u8>>>, done: Arc<AtomicUsize>) {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;
    let (g2, d2) = (Arc::clone(&got), Arc::clone(&done));
    io.async_read(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start,
            length,
        },
        Box::new(move |b, e| {
            assert!(!e.is_err());
            *g2.lock().unwrap() = b.data().unwrap_or(&[]).to_vec();
            d2.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );
}

fn wait_done(io: &PreadDiskIo, done: &AtomicUsize, n: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while done.load(Ordering::Relaxed) < n && std::time::Instant::now() < deadline {
        io.completed().call_job_handlers();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(done.load(Ordering::Relaxed), n);
}

/// upstream `prepare_read`: an unaligned request spanning two cached
/// blocks is served entirely from the write cache — no disk I/O.
/// The data files deliberately do not exist, so any disk access
/// would surface as an error.
#[test]
fn spanning_read_fully_cached_needs_no_disk() {
    let d = dir("span_cache");
    std::fs::create_dir_all(d.join("t")).unwrap();
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs_big(), &d)));

    // two dirty blocks of a four-block piece → stays in the cache;
    // the writers stay parked on their blocks — upstream completes a
    // write when the flush makes it durable, not at insert.
    let wdone = Arc::new(AtomicUsize::new(0));
    aw(&io, 0, 0x4000, 0xAA, Arc::clone(&wdone));
    aw(&io, 0x4000, 0x4000, 0xBB, Arc::clone(&wdone));

    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0x2000, 0x4000, Arc::clone(&got), Arc::clone(&done));

    wait_done(&io, &done, 1);
    let got = got.lock().unwrap();
    assert!(got.len() >= 0x4000);
    assert!(got[..0x2000].iter().all(|&b| b == 0xAA));
    assert!(got[0x2000..0x4000].iter().all(|&b| b == 0xBB));
    drop(got);
    // nothing flushed yet → the writers are still pending.
    assert_eq!(wdone.load(Ordering::Relaxed), 0);
    io.flush_all();
    wait_done(&io, &wdone, 2);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// upstream `prepare_read` one-sided hit → `partial_read`: the cached
/// side is prefilled at the gate, only the missing side is read from
/// disk.
#[test]
fn spanning_read_one_side_cached_runs_partial_read() {
    let d = dir("span_partial");
    std::fs::create_dir_all(d.join("t")).unwrap();
    std::fs::write(d.join("t/a.bin"), vec![0xCCu8; 0x8000]).unwrap();
    std::fs::write(d.join("t/b.bin"), vec![0xCCu8; 0x8000]).unwrap();
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs_big(), &d)));

    // only block 0 is dirty in the cache; block 1 exists on disk
    // only — the write stays parked until a flush lands it.
    let wdone = Arc::new(AtomicUsize::new(0));
    aw(&io, 0, 0x4000, 0xAA, Arc::clone(&wdone));

    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0x2000, 0x4000, Arc::clone(&got), Arc::clone(&done));

    wait_done(&io, &done, 1);
    let got = got.lock().unwrap();
    assert!(got.len() >= 0x4000);
    // first half from the cached block, second half read off disk
    assert!(got[..0x2000].iter().all(|&b| b == 0xAA));
    assert!(got[0x2000..0x4000].iter().all(|&b| b == 0xCC));
    drop(got);
    io.flush_all();
    wait_done(&io, &wdone, 1);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// upstream `prepare_read`: nothing cached → the spanning request
/// stays a normal disk read.
#[test]
fn spanning_read_uncached_reads_disk() {
    let d = dir("span_uncached");
    std::fs::create_dir_all(d.join("t")).unwrap();
    std::fs::write(d.join("t/a.bin"), vec![0x77u8; 0x8000]).unwrap();
    std::fs::write(d.join("t/b.bin"), vec![0x88u8; 0x8000]).unwrap();
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs_big(), &d)));

    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0x2000, 0x4000, Arc::clone(&got), Arc::clone(&done));

    wait_done(&io, &done, 1);
    let got = got.lock().unwrap();
    assert!(got.len() >= 0x4000);
    // piece 0 spans a.bin[0..0x8000] + b.bin[0..0x8000]; 0x2000..0x6000
    // lands in a.bin only — all 0x77.
    assert!(got[..0x4000].iter().all(|&b| b == 0x77));
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// an aligned request fully covered by a cached block is served from
/// the cache — again with the data files absent to prove no disk I/O.
#[test]
fn aligned_read_served_from_cache() {
    let d = dir("aligned_cache");
    std::fs::create_dir_all(d.join("t")).unwrap();
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs_big(), &d)));

    let wdone = Arc::new(AtomicUsize::new(0));
    aw(&io, 0x4000, 0x4000, 0xDD, Arc::clone(&wdone));

    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0x4000, 0x4000, Arc::clone(&got), Arc::clone(&done));

    wait_done(&io, &done, 1);
    let got = got.lock().unwrap();
    assert!(got.len() >= 0x4000);
    assert!(got[..0x4000].iter().all(|&b| b == 0xDD));
    drop(got);
    // the writer completes once `flush_all` makes the block durable.
    io.flush_all();
    wait_done(&io, &wdone, 1);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// upstream `async_read`: `errors::invalid_request` on empty or
/// negative requests — the job never reaches the queue.
#[test]
fn invalid_read_request_fails_fast() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;

    let d = dir("invalid_read");
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));

    for (start, length) in [(0i32, 0i32), (-1, 16), (0, -16)] {
        let hit = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hit);
        io.async_read(
            StorageIndex(0),
            PeerRequest {
                piece: PieceIndex(0),
                start,
                length,
            },
            Box::new(move |_, e| {
                assert!(e.is_err());
                h.fetch_add(1, Ordering::Relaxed);
            }),
            0,
        );
        // inline completion — never queued
        assert_eq!(hit.load(Ordering::Relaxed), 1);
    }
    assert_eq!(io.completed().pending(), 0);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// upstream `async_write`/`is_blocked`: a write submitted while a
/// fence is up is *parked* (returns false) — inserting into the cache
/// immediately would race the exclusive job. When the fence lowers
/// the repost inserts it: the subsequent read is served from the
/// cache (files absent → a disk read would fail).
#[test]
fn write_parked_behind_fence_inserts_on_repost() {
    use std::sync::Barrier;

    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;
    use bitdaemon_disk::open_mode::OpenMode;
    use bitdaemon_disk::storage::{DiskStorage, MoveFlags};
    use bitdaemon_disk::storage_error::{StorageError, StorageResult};

    // `release_files` is a fence job — hold it mid-flight so the
    // fence stays raised while the write arrives.
    struct GateRel {
        inner: PreadStorage,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }
    impl DiskStorage for GateRel {
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
            self.entered.wait();
            self.release.wait();
            self.inner.release_files()
        }
        fn move_storage(&self, _p: PathBuf, _f: MoveFlags) -> (u8, PathBuf, Option<StorageError>) {
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
        ) -> (bool, Option<StorageError>) {
            unimplemented!()
        }
    }

    let d = dir("fence_write");
    std::fs::create_dir_all(d.join("t")).unwrap();
    let (io, _drained) = make_io(1);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    io.insert_storage(
        StorageIndex(0),
        Box::new(GateRel {
            inner: storage(&make_fs_big(), &d),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }),
    );

    // raise the fence — release_files executes on the worker and
    // parks at `release`, holding the fence.
    let rf_done = Arc::new(AtomicUsize::new(0));
    let rfd = Arc::clone(&rf_done);
    io.async_release_files(
        StorageIndex(0),
        Box::new(move || {
            rfd.fetch_add(1, Ordering::Relaxed);
        }),
    );
    entered.wait();

    // fence up → the write parks, returns false like upstream.
    let wdone = Arc::new(AtomicUsize::new(0));
    let mut buf = io.buffer_pool().allocate_buffer("w");
    for b in buf.data_mut().unwrap() {
        *b = 0xE5;
    }
    let d2 = Arc::clone(&wdone);
    let pressure = io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0x8000,
            length: 0x4000,
        },
        buf,
        None,
        Box::new(move |_| {
            d2.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );
    assert!(!pressure, "parked write must report no back-pressure");
    assert_eq!(
        wdone.load(Ordering::Relaxed),
        0,
        "parked write must not complete"
    );

    // upstream `9ce2f49cf`: a write parked behind a fence still counts
    // toward back-pressure (`account_pending_write` runs before
    // `is_blocked`). With max_size = 1 block the level is already 2
    // after this second parked write → `exceeded` is returned.
    io.set_max_queued_bytes(0x4000);
    let wdone2 = Arc::new(AtomicUsize::new(0));
    let mut buf2 = io.buffer_pool().allocate_buffer("w2");
    for b in buf2.data_mut().unwrap() {
        *b = 0x5A;
    }
    let d3 = Arc::clone(&wdone2);
    let pressure2 = io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0x4000,
            length: 0x4000,
        },
        buf2,
        None,
        Box::new(move |_| {
            d3.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );
    assert!(
        pressure2,
        "parked write over the limit must report back-pressure"
    );
    assert_eq!(
        wdone2.load(Ordering::Relaxed),
        0,
        "parked write must not complete"
    );

    // let the fence job finish — the repost inserts the block into
    // the cache; the writer stays parked on it (not yet durable).
    release.wait();
    wait_done(&io, &rf_done, 1);

    // the block landed in the cache at repost — the read is served
    // without touching disk (the data files do not exist).
    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0x8000, 0x4000, Arc::clone(&got), Arc::clone(&done));
    wait_done(&io, &done, 1);
    let done2 = Arc::new(AtomicUsize::new(0));
    let got2 = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0x4000, 0x4000, Arc::clone(&got2), Arc::clone(&done2));
    wait_done(&io, &done2, 1);
    let got = got.lock().unwrap();
    assert!(got.len() >= 0x4000);
    assert!(got[..0x4000].iter().all(|&b| b == 0xE5));
    drop(got);
    let got2 = got2.lock().unwrap();
    assert!(got2.len() >= 0x4000);
    assert!(got2[..0x4000].iter().all(|&b| b == 0x5A));
    drop(got2);
    // upstream: the write handler fires only when the flush lands
    // the block on disk.
    io.flush_all();
    wait_done(&io, &wdone, 1);
    wait_done(&io, &wdone2, 1);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn stacked_fence_flushes_reposted_write_before_running() {
    use std::sync::Barrier;

    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;
    use bitdaemon_disk::open_mode::OpenMode;
    use bitdaemon_disk::storage::{DiskStorage, MoveFlags};
    use bitdaemon_disk::storage_error::{StorageError, StorageResult};

    // `release_files` is a fence job. Its first invocation holds the
    // fence at a barrier while a write and a *stacked* fence queue
    // behind it; the second invocation is the stacked fence itself —
    // it samples the data file: upstream's `m_fence_flush` must have
    // drained the reposted write's block to disk before the fence can
    // run (upstream writes stay outstanding until flushed; the port
    // drains them as jobs counted outstanding past `is_blocked`).
    struct StackRel {
        inner: PreadStorage,
        calls: AtomicUsize,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        probed: Arc<Mutex<Option<Vec<u8>>>>,
    }
    impl DiskStorage for StackRel {
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
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.entered.wait();
                self.release.wait();
            } else {
                // stacked fence running — the reposted write's block
                // must already be durable.
                let mut buf = vec![0u8; 0x4000];
                let r =
                    self.inner
                        .read(&mut buf, PieceIndex(0), 0x8000, OpenMode::READ_ONLY, false);
                *self.probed.lock().unwrap() = r.ok().map(|_| buf);
            }
            self.inner.release_files()
        }
        fn move_storage(&self, _p: PathBuf, _f: MoveFlags) -> (u8, PathBuf, Option<StorageError>) {
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
        ) -> (bool, Option<StorageError>) {
            unimplemented!()
        }
    }

    let d = dir("stacked_fence");
    std::fs::create_dir_all(d.join("t")).unwrap();
    let (io, _drained) = make_io(1);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let probed = Arc::new(Mutex::new(None));
    io.insert_storage(
        StorageIndex(0),
        Box::new(StackRel {
            inner: storage(&make_fs_big(), &d),
            calls: AtomicUsize::new(0),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            probed: Arc::clone(&probed),
        }),
    );

    // F1 raises the fence and holds it mid-flight.
    let f1_done = Arc::new(AtomicUsize::new(0));
    let f1d = Arc::clone(&f1_done);
    io.async_release_files(
        StorageIndex(0),
        Box::new(move || {
            f1d.fetch_add(1, Ordering::Relaxed);
        }),
    );
    entered.wait();

    // the write parks behind F1.
    let done = Arc::new(AtomicUsize::new(0));
    let mut buf = io.buffer_pool().allocate_buffer("w");
    for b in buf.data_mut().unwrap() {
        *b = 0xE5;
    }
    let d2 = Arc::clone(&done);
    io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0x8000,
            length: 0x4000,
        },
        buf,
        None,
        Box::new(move |_| {
            d2.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );

    // F2 stacks behind F1 — its release_files will probe the file.
    let f2_done = Arc::new(AtomicUsize::new(0));
    let f2d = Arc::clone(&f2_done);
    io.async_release_files(
        StorageIndex(0),
        Box::new(move || {
            f2d.fetch_add(1, Ordering::Relaxed);
        }),
    );

    // F1 finishes → repost inserts the write under a still-raised
    // stacked fence → the fence-flush drains it to disk → F2 runs.
    release.wait();
    wait_done(&io, &f2_done, 1);
    wait_done(&io, &done, 1);
    wait_done(&io, &f1_done, 1);

    let probed = probed.lock().unwrap();
    let buf = probed
        .as_ref()
        .expect("stacked fence probed the file before running");
    assert!(
        buf.iter().all(|&b| b == 0xE5),
        "fence flush must land the reposted write before the fence runs"
    );
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

// ---------------------------------------------------------------------------
// v2 block-boundary hashing on hybrid torrents (piece_size2 cut)
// ---------------------------------------------------------------------------

/// hybrid `FileStorage`: a single 0x5000 B v2 file under piece_length
/// 32 KiB — `add_file` auto-appends the 0x3000 B pad to the piece
/// boundary. `piece_size(0)` = 0x8000, `piece_size2(0)` = 0x5000,
/// `blocks_in_piece2(0)` = 2.
fn make_hybrid_fs() -> FileStorage {
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(0x8000);
    fs.add_file(
        None,
        b"t/a.bin",
        0x5000,
        FileFlags::NONE,
        0,
        b"",
        Some(bitdaemon_common::Sha256Hash::from([1; 32])),
    )
    .unwrap();
    fs.set_num_pieces(1);
    fs
}

/// Regression for the `piece_size2` bounds (upstream `do_job(hash)` /
/// `do_job(hash2)` and `disk_cache::hash2`/`v2_hash_queue_entry`):
/// block 1 of the hybrid piece covers `[0x4000..0x5000)` — hashing the
/// full 16 KiB would fold the pad region (or whatever bytes the writer
/// left there) into the SHA-256 and corrupt the v2 merkle layer.
/// Exercises all three paths: write-cache `get`, flush-stored
/// `block_hash`, and the disk read in `hash_job`/`hash2_job`.
#[test]
fn hash_v2_blocks_stop_at_file_boundary() {
    use bitdaemon_common::Sha256Hash;
    use bitdaemon_disk::disk_interface::{DiskIo, flags};

    let d = dir("hashv2_bnd");
    std::fs::create_dir_all(d.join("t")).unwrap();
    let data: Vec<u8> = (0..0x5000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(d.join("t/a.bin"), &data).unwrap();

    let fs = make_hybrid_fs();
    let prio = vec![DownloadPriority::DEFAULT; fs.num_files() as usize];
    let (io, _drained) = make_io(1);
    let idx = io.new_torrent(&StorageParams {
        files: &fs,
        renamed: &RenamedFiles::default(),
        path: d.to_path_buf(),
        part_file_dir: PathBuf::new(),
        mode: StorageMode::Sparse,
        priorities: &prio,
        info_hash: [7u8; 20],
        v1: true,
        v2: true,
    });

    use sha2::{Digest as _, Sha256};
    let expect_b0 = Sha256::digest(&data[..0x4000]);
    let expect_b1 = Sha256::digest(&data[0x4000..]); // only 0x1000 bytes

    let hash2_block = |io: &Arc<PreadDiskIo>, off: i32| {
        let done = Arc::new(AtomicUsize::new(0));
        let got = Arc::new(Mutex::new(Sha256Hash::default()));
        let (d2, g2) = (Arc::clone(&done), Arc::clone(&got));
        io.async_hash2(
            idx,
            PieceIndex(0),
            off,
            0,
            Box::new(move |_, h, e| {
                assert!(!e.is_err(), "hash2 must succeed: {e:?}");
                *g2.lock().unwrap() = h;
                d2.fetch_add(1, Ordering::Relaxed);
            }),
        );
        wait_done(io, &done, 1);
        let g = got.lock().unwrap();
        *g
    };

    // 1) write-cache path — block 1 sits dirty in the store buffer;
    //    a wrong bound would hash the 0xA5 marker tail too.
    let wdone = Arc::new(AtomicUsize::new(0));
    let mut buf = io.buffer_pool().allocate_buffer("w");
    {
        let b = buf.data_mut().unwrap();
        b[..0x1000].copy_from_slice(&data[0x4000..]);
        for x in &mut b[0x1000..0x4000] {
            *x = 0xA5;
        }
    }
    {
        use bitdaemon_common::types::PeerRequest;
        let wd = Arc::clone(&wdone);
        io.async_write(
            idx,
            PeerRequest {
                piece: PieceIndex(0),
                start: 0x4000,
                length: 0x4000,
            },
            buf,
            None,
            Box::new(move |_| {
                wd.fetch_add(1, Ordering::Relaxed);
            }),
            0,
        );
    }
    let h = hash2_block(&io, 0x4000);
    assert_eq!(
        &h.as_bytes()[..],
        &expect_b1[..],
        "cached v2 block must stop at piece_size2"
    );

    // 2) flush-stored path — `flush_all` lands the block and stores the
    //    per-block SHA-256 (bounded, like upstream's `hash_len`).
    io.flush_all();
    wait_done(&io, &wdone, 1);
    let h = hash2_block(&io, 0x4000);
    assert_eq!(
        &h.as_bytes()[..],
        &expect_b1[..],
        "flush-stored v2 block hash must stop at piece_size2"
    );

    // 3) full-piece hash — SHA-1 over the v1 span (pad = zeroes) and
    //    per-block SHA-256 clipped to piece_size2.
    let done = Arc::new(AtomicUsize::new(0));
    let got_sha1 = Arc::new(Mutex::new(Sha1Hash::default()));
    let got_blocks = Arc::new(Mutex::new(Vec::<Sha256Hash>::new()));
    let (d2, s2, b2) = (
        Arc::clone(&done),
        Arc::clone(&got_sha1),
        Arc::clone(&got_blocks),
    );
    io.async_hash(
        idx,
        PieceIndex(0),
        vec![Sha256Hash::default(); 2],
        flags::V1_HASH,
        Box::new(move |_, h, blocks, _digests, e| {
            assert!(!e.is_err(), "hash must succeed: {e:?}");
            *s2.lock().unwrap() = h;
            *b2.lock().unwrap() = blocks;
            d2.fetch_add(1, Ordering::Relaxed);
        }),
    );
    wait_done(&io, &done, 1);
    let mut piece_bytes = data.clone();
    piece_bytes.extend_from_slice(&[0u8; 0x3000]);
    use sha1::Sha1;
    let expect_sha1 = Sha1::digest(&piece_bytes);
    assert_eq!(&got_sha1.lock().unwrap().as_bytes()[..], &expect_sha1[..]);
    let b = got_blocks.lock().unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!(&b[0].as_bytes()[..], &expect_b0[..]);
    assert_eq!(&b[1].as_bytes()[..], &expect_b1[..]);

    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// Port of upstream `test_disk_io_flush_after_hash_with_pad`
/// (`e4e0474cc`): the piece's last block is all pad — never written,
/// so the piece is never cache-complete and `async_hash` runs on the
/// hashing pool reading the pad as zeros. Once the piece hash
/// returns, the dirty block must still be flushed (its write handler
/// fires) — without `flush_piece` and with no other disk job around
/// to kick a generic thread.
#[test]
fn flush_after_hash_with_pad() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::{DiskIo, flags};

    let d = dir("hash_pad_flush");
    std::fs::create_dir_all(d.join("t")).unwrap();

    // piece = 2 blocks; file-0 is 0x4000-100 bytes, a pad file covers
    // the rest of the piece → block 1 [0x4000..0x8000) is all pad.
    let mut fs = FileStorage::new();
    fs.set_name(b"t");
    fs.set_piece_length(0x8000);
    fs.add_file(
        None,
        b"t/a.bin",
        0x4000 - 100,
        FileFlags::NONE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.add_file(
        None,
        b"t/.pad/16484",
        0x8000 - (0x4000 - 100),
        FileFlags::PAD_FILE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.add_file(None, b"t/b.bin", 0x8000, FileFlags::NONE, 0, b"", None)
        .unwrap();
    fs.set_num_pieces(2);

    let prio = vec![DownloadPriority::DEFAULT; fs.num_files() as usize];
    let (io, _drained) = make_io(1);
    io.set_hash_threads(1);
    let idx = io.new_torrent(&StorageParams {
        files: &fs,
        renamed: &RenamedFiles::default(),
        path: d.to_path_buf(),
        part_file_dir: PathBuf::new(),
        mode: StorageMode::Sparse,
        priorities: &prio,
        info_hash: [7u8; 20],
        v1: true,
        v2: false,
    });

    // upstream `generate_piece` + zeroed pad tail: pattern inside
    // file-0's range, zeros from file_size on.
    let mut piece = vec![0u8; 0x8000];
    for (i, b) in piece[..0x4000 - 100].iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let wdone = Arc::new(AtomicUsize::new(0));
    let mut buf = io.buffer_pool().allocate_buffer("w");
    buf.data_mut().unwrap().copy_from_slice(&piece[..0x4000]);
    let wd = Arc::clone(&wdone);
    io.async_write(
        idx,
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 0x4000,
        },
        buf,
        None,
        Box::new(move |e| {
            assert!(!e.is_err(), "write must succeed: {e:?}");
            wd.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );

    let hashed = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Sha1Hash::default()));
    let (h2, g2) = (Arc::clone(&hashed), Arc::clone(&got));
    io.async_hash(
        idx,
        PieceIndex(0),
        Vec::new(),
        flags::V1_HASH,
        Box::new(move |_, h, _, _digests, e| {
            assert!(!e.is_err(), "hash must succeed: {e:?}");
            *g2.lock().unwrap() = h;
            h2.fetch_add(1, Ordering::Relaxed);
        }),
    );

    // no flush_piece flag, no flush_all — the hash completion alone
    // must schedule the piece flush (upstream wakes a generic thread).
    wait_done(&io, &hashed, 1);
    wait_done(&io, &wdone, 1);

    use sha1::{Digest as _, Sha1};
    let expect = Sha1::digest(&piece);
    assert_eq!(&got.lock().unwrap().as_bytes()[..], &expect[..]);

    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// upstream `stop_torrent` → `release_files`: every pooled handle of
/// the stopped storage is closed (file_pool `get_status` empties).
#[test]
fn stop_torrent_releases_pooled_files() {
    use bitdaemon_disk::disk_interface::DiskIo;

    let d = dir("stop_rel");
    std::fs::create_dir_all(d.join("t")).unwrap();
    std::fs::write(d.join("t/a.bin"), vec![7u8; 1000]).unwrap();
    std::fs::write(d.join("t/b.bin"), vec![8u8; 1000]).unwrap();

    let (io, _drained) = make_io(1);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));

    // a read pulls the file into the pool (lazy open)
    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0, 16, Arc::clone(&got), Arc::clone(&done));
    wait_done(&io, &done, 1);
    assert!(
        !io.get_status(StorageIndex(0)).is_empty(),
        "read must leave a pooled handle"
    );

    let stopped = Arc::new(AtomicUsize::new(0));
    let s2 = Arc::clone(&stopped);
    io.async_stop_torrent(
        StorageIndex(0),
        Box::new(move || {
            s2.fetch_add(1, Ordering::Relaxed);
        }),
    );
    wait_done(&io, &stopped, 1);
    assert!(
        io.get_status(StorageIndex(0)).is_empty(),
        "stop_torrent must release the storage's pooled handles"
    );
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// upstream `stop_torrent` → `abort_hash_jobs`: a *volatile* hash job
/// still queued in the hash pool when the torrent stops must complete
/// aborted — never touching the storage — while an in-flight
/// non-volatile hash runs to completion.
#[test]
fn stop_torrent_aborts_queued_volatile_hash() {
    use std::sync::Barrier;

    use bitdaemon_disk::disk_interface::{DiskIo, flags};
    use bitdaemon_disk::open_mode::OpenMode;
    use bitdaemon_disk::storage::{DiskStorage, MoveFlags};
    use bitdaemon_disk::storage_error::{StorageError, StorageResult};

    // `read` parks the hash worker on a barrier so a second hash job
    // stays queued while `stop_torrent` scans the hash pool.
    struct GatedRead {
        inner: PreadStorage,
        entered: Arc<AtomicUsize>,
        gate: Arc<Barrier>,
    }
    impl DiskStorage for GatedRead {
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
            // only the FIRST read parks — if the abort regressed, the
            // second job runs to completion instead of deadlocking
            // the test (the `entered == 1` assert still catches it).
            if self.entered.fetch_add(1, Ordering::Relaxed) == 0 {
                self.gate.wait();
            }
            self.inner.read(buf, piece, offset, mode, volatile)
        }
        fn write(
            &self,
            buf: &[u8],
            piece: PieceIndex,
            offset: i64,
            mode: OpenMode,
        ) -> StorageResult<i64> {
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
        fn move_storage(&self, _p: PathBuf, _f: MoveFlags) -> (u8, PathBuf, Option<StorageError>) {
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
        ) -> (bool, Option<StorageError>) {
            unimplemented!()
        }
    }

    let d = dir("stop_abort");
    std::fs::create_dir_all(d.join("t")).unwrap();
    std::fs::write(d.join("t/a.bin"), vec![3u8; 1000]).unwrap();
    std::fs::write(d.join("t/b.bin"), vec![4u8; 1000]).unwrap();

    let (io, _drained) = make_io(1);
    // give the hash pool a worker of its own — hash jobs only route
    // there when `max_threads > 0` (upstream `pool_for`; the generic
    // `set_max_threads` does NOT size `hashing_threads`)
    io.set_hash_threads(1);
    let entered = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Barrier::new(2));
    io.insert_storage(
        StorageIndex(0),
        Box::new(GatedRead {
            inner: storage(&make_fs(), &d),
            entered: Arc::clone(&entered),
            gate: Arc::clone(&gate),
        }),
    );

    // job A — non-volatile, occupies the single hash worker inside
    // the gated read.
    let a_done = Arc::new(AtomicUsize::new(0));
    let ad = Arc::clone(&a_done);
    io.async_hash(
        StorageIndex(0),
        PieceIndex(0),
        Vec::new(),
        flags::V1_HASH,
        Box::new(move |_, _h, _b, _d, e| {
            assert!(!e.is_err(), "in-flight hash must complete: {e:?}");
            ad.fetch_add(1, Ordering::Relaxed);
        }),
    );
    // wait until A is inside the storage read — B then stays queued
    // behind the single busy hash worker.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while entered.load(Ordering::Relaxed) == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(entered.load(Ordering::Relaxed), 1);

    // job B — volatile (a checking hash) — sits in the hash pool queue.
    let b_done = Arc::new(AtomicUsize::new(0));
    let b_err = Arc::new(AtomicUsize::new(0));
    let (bd, be) = (Arc::clone(&b_done), Arc::clone(&b_err));
    io.async_hash(
        StorageIndex(0),
        PieceIndex(1),
        Vec::new(),
        flags::V1_HASH | flags::VOLATILE_READ,
        Box::new(move |_, _h, _b, _d, e| {
            if e.is_err() {
                be.fetch_add(1, Ordering::Relaxed);
            }
            bd.fetch_add(1, Ordering::Relaxed);
        }),
    );

    let stopped = Arc::new(AtomicUsize::new(0));
    let sd = Arc::clone(&stopped);
    io.async_stop_torrent(
        StorageIndex(0),
        Box::new(move || {
            sd.fetch_add(1, Ordering::Relaxed);
        }),
    );

    // release A → it completes → the stop fence drains → B dequeues
    // aborted.
    gate.wait();
    wait_done(&io, &a_done, 1);
    wait_done(&io, &b_done, 1);
    wait_done(&io, &stopped, 1);

    assert_eq!(
        b_err.load(Ordering::Relaxed),
        1,
        "queued volatile hash must complete aborted"
    );
    assert_eq!(
        entered.load(Ordering::Relaxed),
        1,
        "aborted hash must never reach the storage"
    );
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

// ---------------------------------------------------------------------------
// `num_fenced_*`/`blocked_disk_jobs` gauges + `disk_observer` wiring
// ---------------------------------------------------------------------------

/// A fence job counts in at `add_fence_job` (`num_fenced_read +
/// get_type()`) and out when its `FENCE`-flagged completion lands;
/// jobs parked behind the raised fence lift `blocked_disk_jobs`
/// until `job_complete` reposts them (upstream
/// `add_completed_jobs_impl`'s `inc_stats_counter(…, -ret)`).
#[test]
fn fenced_and_blocked_counters_balance() {
    use std::sync::Barrier;

    use bitdaemon_disk::disk_interface::DiskIo;
    use bitdaemon_disk::disk_job::JobAction;

    let d = dir("fence_ctrs");
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
    // piece 0 completes on insert → the eager Flush job's
    // storage.write() parks the single worker on the barrier;
    // `outstanding` stays > 0 so the fence below queues instead of
    // posting immediately.
    io.add_job(StorageIndex(0), write_job(0, 0x5A, &io, Arc::clone(&done)));

    io.async_release_files(
        StorageIndex(0),
        Box::new({
            let d2 = Arc::clone(&done);
            move || {
                d2.fetch_add(1, Ordering::Relaxed);
            }
        }),
    );
    let q = io.queue_stats();
    assert_eq!(
        q.fenced_jobs[JobAction::ReleaseFiles.index()],
        1,
        "fence job counts in at submit"
    );
    assert_eq!(
        q.blocked_disk_jobs, 1,
        "fence job parked behind the in-flight write"
    );

    // a normal read queues behind the raised fence
    let got = Arc::new(Mutex::new(Vec::new()));
    io.add_job(
        StorageIndex(0),
        read_job(0, Arc::clone(&got), Arc::clone(&done)),
    );
    assert_eq!(io.queue_stats().blocked_disk_jobs, 2);

    // release the worker: the write lands, the fence drains and runs,
    // then the parked read reposts — every gauge settles at 0.
    gate.wait();
    wait_done(&io, &done, 3);

    let q = io.queue_stats();
    assert!(q.fenced_jobs.iter().all(|&n| n == 0));
    assert_eq!(
        q.blocked_disk_jobs, 0,
        "reposted jobs leave the blocked gauge"
    );
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// `disk_observer` wiring — `async_write`'s `o` registers with
/// `has_back_pressure` when the queue level reaches the max; once the
/// queue drains below the low watermark the observer's `on_disk`
/// fires (upstream posts it to the network thread — the default
/// poster runs it inline on the disk path).
#[test]
fn back_pressure_observer_fires_below_low_watermark() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::back_pressure::DiskObserver;
    use bitdaemon_disk::disk_interface::DiskIo;

    struct Obs(AtomicUsize);
    impl DiskObserver for Obs {
        fn on_disk(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let d = dir("bp_obs");
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));
    // max = 1 block — the first write pushes the queue to the limit;
    // low watermark = 0, so the drain fires the observer at once.
    io.set_max_queued_bytes(0x4000);

    let obs = Arc::new(Obs(AtomicUsize::new(0)));
    let o: Arc<dyn DiskObserver> = obs.clone();
    let done = Arc::new(AtomicUsize::new(0));
    let exceeded = io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 1024,
        },
        DiskBufferHolder::from_owned(vec![0x33; 1024]),
        Some(o),
        Box::new({
            let d2 = Arc::clone(&done);
            move |_| {
                d2.fetch_add(1, Ordering::Relaxed);
            }
        }),
        0,
    );
    assert!(exceeded, "level 1 >= max_size 1 → back-pressure signalled");
    // the 0-thread io flushed the piece inline — `write_blocks` is
    // back to 0, below the low watermark → the observer already fired.
    assert_eq!(obs.0.load(Ordering::Relaxed), 1);
    io.completed().call_job_handlers();
    assert_eq!(done.load(Ordering::Relaxed), 1);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// `settings_pack::file_pool_size` hot-resize — `set_file_pool_size`
/// reaches the backend's live `FilePool` (upstream
/// `settings_updated` → `m_file_pool.resize`): shrinking evicts the
/// LRU handles immediately.
#[test]
fn set_file_pool_size_resizes_live_pool() {
    use bitdaemon_disk::disk_interface::DiskIo;

    let d = dir("fpsize");
    let (io, _drained) = make_io(0);
    let fs = make_fs();
    // `new_torrent` is the production path — the factory-built
    // storage shares the io's `file_pool` (unlike `insert_storage`).
    let idx = io.new_torrent(&StorageParams {
        files: &fs,
        renamed: &RenamedFiles::default(),
        path: d.clone(),
        part_file_dir: PathBuf::new(),
        mode: StorageMode::Sparse,
        priorities: &[DownloadPriority::DEFAULT; 2],
        info_hash: [7u8; 20],
        v1: true,
        v2: false,
    });
    // a write spanning a.bin's tail into b.bin's head opens both
    // files (piece 0 = a[0..1000] + b[0..24]).
    let done = Arc::new(AtomicUsize::new(0));
    io.add_job(idx, write_job(0, 0x77, &io, Arc::clone(&done)));
    io.completed().call_job_handlers();
    assert_eq!(
        io.get_status(idx).len(),
        2,
        "both files pooled after the write"
    );

    io.set_file_pool_size(1);
    assert_eq!(io.get_status(idx).len(), 1, "shrink evicts the LRU handle");

    io.set_file_pool_size(40);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// A16 D2 — a duplicate write on a cached block supersedes the parked
/// writer; its `write_blocks` back-pressure slot must be released
/// (previously the CACHED flag suppressed the decrement and every
/// duplicate leaked one slot forever).
#[test]
fn duplicate_write_releases_back_pressure_slot() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;

    let d = dir("dup_write_bp");
    let (io, _drained) = make_io(0);
    io.insert_storage(StorageIndex(0), Box::new(storage(&make_fs(), &d)));
    io.set_max_queued_bytes(100 * 0x4000);

    let done = Arc::new(AtomicUsize::new(0));
    let req = PeerRequest {
        piece: PieceIndex(0),
        start: 0,
        length: 1024,
    };
    let mk = |done: &Arc<AtomicUsize>| {
        let d2 = Arc::clone(done);
        Box::new(move |_: bitdaemon_disk::storage_error::StorageError| {
            d2.fetch_add(1, Ordering::Relaxed);
        }) as Box<dyn FnOnce(bitdaemon_disk::storage_error::StorageError) + Send>
    };
    io.async_write(
        StorageIndex(0),
        req,
        DiskBufferHolder::from_owned(vec![0x5a; 1024]),
        None,
        mk(&done),
        0,
    );
    io.async_write(
        StorageIndex(0),
        req,
        DiskBufferHolder::from_owned(vec![0x5a; 1024]),
        None,
        mk(&done),
        0,
    );
    io.completed().call_job_handlers();
    // the piece is a single block here, so the surviving write
    // flushed inline and released its slot; the superseded writer's
    // slot is gone too — nothing leaks.
    assert_eq!(
        io.queue_stats().write_blocks,
        0,
        "duplicate write must not leak a back-pressure slot"
    );
    // both handlers ran (the superseded one aborted).
    assert_eq!(done.load(Ordering::Relaxed), 2);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

// ---------------------------------------------------------------------------
// A16 round-2 — incremental piece hasher, borrowed_buf reads, fence drain
// ---------------------------------------------------------------------------

/// Storage probe for the A16 tests: `read`/`hash_with` are counted
/// (a digest or data byte coming from disk would trip the assert);
/// `write` can hold at a barrier to keep a flush in flight.
struct ProbeStorage {
    inner: PreadStorage,
    reads: Arc<AtomicUsize>,
    /// `(entered, release)` — `write` signals arrival then holds.
    write_gate: Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>,
}
impl bitdaemon_disk::storage::DiskStorage for ProbeStorage {
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
        mode: bitdaemon_disk::open_mode::OpenMode,
        volatile: bool,
    ) -> bitdaemon_disk::storage_error::StorageResult<i64> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read(buf, piece, offset, mode, volatile)
    }
    fn write(
        &self,
        buf: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: bitdaemon_disk::open_mode::OpenMode,
    ) -> bitdaemon_disk::storage_error::StorageResult<i64> {
        if let Some((entered, release)) = &self.write_gate {
            entered.wait();
            release.wait();
        }
        self.inner.write(buf, piece, offset, mode)
    }
    fn hash_with(
        &self,
        feed: &mut dyn FnMut(&[u8]),
        len: i64,
        piece: PieceIndex,
        offset: i64,
        mode: bitdaemon_disk::open_mode::OpenMode,
        volatile: bool,
    ) -> bitdaemon_disk::storage_error::StorageResult<i64> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner
            .hash_with(feed, len, piece, offset, mode, volatile)
    }
    fn release_files(&self) -> bitdaemon_disk::storage_error::StorageResult<()> {
        self.inner.release_files()
    }
    fn move_storage(
        &self,
        _p: PathBuf,
        _f: bitdaemon_disk::storage::MoveFlags,
    ) -> (
        u8,
        PathBuf,
        Option<bitdaemon_disk::storage_error::StorageError>,
    ) {
        unimplemented!()
    }
    fn delete_files(&self, _o: u8) -> bitdaemon_disk::storage_error::StorageResult<()> {
        unimplemented!()
    }
    fn rename_file(
        &mut self,
        _i: FileIndex,
        _n: &[u8],
    ) -> bitdaemon_disk::storage_error::StorageResult<()> {
        unimplemented!()
    }
    fn set_file_priority(
        &mut self,
        _p: &[DownloadPriority],
    ) -> bitdaemon_disk::storage_error::StorageResult<()> {
        unimplemented!()
    }
    fn verify_resume_data(
        &self,
        _h: &bitdaemon_common::bitfield::Bitfield,
        _s: bool,
        _l: &[PathBuf],
    ) -> (bool, Option<bitdaemon_disk::storage_error::StorageError>) {
        unimplemented!()
    }
}

/// A16 E1 — `try_hash_piece`'s `job_completed` case: with the
/// incremental `piece_hasher` fed at insert time, `async_hash` on a
/// fully-flushed piece answers from the stored digest — upstream
/// keeps the cpe alive until `piece_hash_returned` precisely so the
/// hash never re-reads the file.
#[test]
fn hash_served_from_stored_digest_without_reread() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::{DiskIo, flags};

    let d = dir("hash_inline");
    std::fs::create_dir_all(d.join("t")).unwrap();
    let (io, _drained) = make_io(1);
    let reads = Arc::new(AtomicUsize::new(0));
    io.insert_storage(
        StorageIndex(0),
        Box::new(ProbeStorage {
            inner: storage(&make_fs(), &d),
            reads: Arc::clone(&reads),
            write_gate: None,
        }),
    );

    // one full 1024-byte piece — the block lands complete, feeds the
    // incremental hasher and force-flushes.
    let wdone = Arc::new(AtomicUsize::new(0));
    let wd = Arc::clone(&wdone);
    io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 1024,
        },
        DiskBufferHolder::from_owned(vec![0x5A; 1024]),
        None,
        Box::new(move |e| {
            assert!(!e.is_err(), "write must succeed: {e:?}");
            wd.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );
    wait_done(&io, &wdone, 1);

    let hashed = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Sha1Hash::default()));
    let (h2, g2) = (Arc::clone(&hashed), Arc::clone(&got));
    io.async_hash(
        StorageIndex(0),
        PieceIndex(0),
        Vec::new(),
        flags::V1_HASH,
        Box::new(move |_, h, _, _digests, e| {
            assert!(!e.is_err(), "hash must succeed: {e:?}");
            *g2.lock().unwrap() = h;
            h2.fetch_add(1, Ordering::Relaxed);
        }),
    );
    wait_done(&io, &hashed, 1);

    use sha1::{Digest as _, Sha1};
    let expect = Sha1::digest([0x5Au8; 1024]);
    assert_eq!(&got.lock().unwrap().as_bytes()[..], &expect[..]);
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "the piece hash must come from the incremental digest, not a re-read"
    );
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// A16 E2 — `borrowed_buf`: while a Flush job is in flight the
/// block's buffer stays on the cache slot, so `prepare_read` still
/// serves the (correct) bytes instead of falling through to a stale
/// disk read.
#[test]
fn read_during_inflight_flush_serves_cached_bytes() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;

    let d = dir("read_inflight");
    std::fs::create_dir_all(d.join("t")).unwrap();
    // two threads: the flush job holds one at the write gate, the
    // read needs the other.
    let (io, _drained) = make_io(2);
    let reads = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    io.insert_storage(
        StorageIndex(0),
        Box::new(ProbeStorage {
            inner: storage(&make_fs(), &d),
            reads: Arc::clone(&reads),
            write_gate: Some((Arc::clone(&entered), Arc::clone(&release))),
        }),
    );

    // complete 1024-byte piece → insert → eager flush → the writev
    // blocks at the gate with the buffer in flight.
    let wdone = Arc::new(AtomicUsize::new(0));
    let wd = Arc::clone(&wdone);
    io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 1024,
        },
        DiskBufferHolder::from_owned(vec![0x77; 1024]),
        None,
        Box::new(move |e| {
            assert!(!e.is_err(), "write must succeed: {e:?}");
            wd.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );
    entered.wait(); // the flush is now inside storage::write

    let done = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    ar(&io, 0, 1024, Arc::clone(&got), Arc::clone(&done));
    wait_done(&io, &done, 1);
    {
        let got = got.lock().unwrap();
        assert!(got.len() >= 1024);
        assert!(
            got[..1024].iter().all(|&b| b == 0x77),
            "in-flight flush must still serve the write's bytes"
        );
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "a borrowed buffer must not fall through to disk"
    );

    release.wait(); // release the writev — flush lands, writer completes
    wait_done(&io, &wdone, 1);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
}

/// A16 E6 — upstream `async_write` runs `insert_write` →
/// `schedule_flush` even for a write parked behind a fence: the
/// parked charge still counts toward `buffer_level()`, so an
/// over-watermark park must schedule the drain immediately (the
/// Flush job itself queues behind the fence like upstream's pool
/// job, and runs at repost).
#[test]
fn parked_write_over_watermark_schedules_flush() {
    use std::sync::Barrier;

    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_disk::disk_interface::DiskIo;
    use bitdaemon_disk::open_mode::OpenMode;
    use bitdaemon_disk::storage::{DiskStorage, MoveFlags};
    use bitdaemon_disk::storage_error::{StorageError, StorageResult};

    struct GateRel {
        inner: PreadStorage,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }
    impl DiskStorage for GateRel {
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
            self.entered.wait();
            self.release.wait();
            self.inner.release_files()
        }
        fn move_storage(&self, _p: PathBuf, _f: MoveFlags) -> (u8, PathBuf, Option<StorageError>) {
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
        ) -> (bool, Option<StorageError>) {
            unimplemented!()
        }
    }

    let d = dir("fence_sched");
    std::fs::create_dir_all(d.join("t")).unwrap();
    let d2 = dir("fence_sched2");
    std::fs::create_dir_all(d2.join("t")).unwrap();
    let (io, _drained) = make_io(2);
    // storage 0 carries the fence; storage 1 holds a dirty block the
    // global drain must reach while the fence is still up.
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    io.insert_storage(
        StorageIndex(0),
        Box::new(GateRel {
            inner: storage(&make_fs_big(), &d),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }),
    );
    io.insert_storage(StorageIndex(1), Box::new(storage(&make_fs_big(), &d2)));

    // a dirty partial-piece block on storage 1 — buffered under the
    // default watermark, writer still pending.
    let bdone = Arc::new(AtomicUsize::new(0));
    let bd = Arc::clone(&bdone);
    io.async_write(
        StorageIndex(1),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 0x4000,
        },
        DiskBufferHolder::from_owned(vec![0xAB; 0x4000]),
        None,
        Box::new(move |e| {
            assert!(!e.is_err(), "storage-1 write must succeed: {e:?}");
            bd.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );

    // fence up on storage 0.
    let rf_done = Arc::new(AtomicUsize::new(0));
    let rfd = Arc::clone(&rf_done);
    io.async_release_files(
        StorageIndex(0),
        Box::new(move || {
            rfd.fetch_add(1, Ordering::Relaxed);
        }),
    );
    entered.wait();

    // high watermark = 1 block; the parked write on the fenced
    // storage charges level 2 → upstream `insert_write` would still
    // `schedule_flush`, and the global drain reaches storage 1's
    // dirty block *now* — its writer completes while the fence holds.
    io.set_max_queued_bytes(0x4000);
    let wdone = Arc::new(AtomicUsize::new(0));
    let wd = Arc::clone(&wdone);
    let mut buf = io.buffer_pool().allocate_buffer("w");
    for b in buf.data_mut().unwrap() {
        *b = 0xCD;
    }
    io.async_write(
        StorageIndex(0),
        PeerRequest {
            piece: PieceIndex(0),
            start: 0x4000,
            length: 0x4000,
        },
        buf,
        None,
        Box::new(move |_| {
            wd.fetch_add(1, Ordering::Relaxed);
        }),
        0,
    );
    assert_eq!(
        wdone.load(Ordering::Relaxed),
        0,
        "the fenced write must stay parked"
    );
    // storage 1's dirty block flushed on schedule — its writer
    // completes even though storage 0's fence is still raised.
    wait_done(&io, &bdone, 1);

    release.wait();
    wait_done(&io, &rf_done, 1);
    // the reposted write lands, flushes, completes.
    wait_done(&io, &wdone, 1);
    io.abort_jobs(true);
    std::fs::remove_dir_all(&d).unwrap();
    std::fs::remove_dir_all(&d2).unwrap();
}
