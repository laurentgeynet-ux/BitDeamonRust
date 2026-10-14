//! Port of `pread_disk_io.cpp` — the orchestrator: job submission,
//! per-torrent fences, worker pool dispatch, completion queue.
//!
//! Model (identical to upstream):
//! - `add_job` runs `is_blocked` on the torrent's fence; a raised fence
//!   parks the job, otherwise it's counted in-flight and pushed to the
//!   worker pool.
//! - exclusive jobs (`move_storage`, `delete_files`, `check_fastresume`,
//!   `rename_file`, `file_priority`, `release_files`, `stop_torrent`,
//!   `clear_piece`) go through `add_fence_job` → `raise_fence`.
//! - workers execute `perform_job` on the storage, then
//!   `fence.job_complete` reposts blocked jobs *under the fence mutex*
//!   — preserving submission order — before the job lands on the
//!   completed queue for the network thread to drain.
//! - `max_threads == 0` ⇒ `immediate_execute`: jobs run inline
//!   (upstream fallback, also the disabled-for-tests path).
//!
//! No OS sockets, no async runtime dependency: the completion drain is
//! posted through the injected `DrainPoster` (`disk_completed_queue`).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bitdaemon_common::types::{FileIndex, PieceIndex, StorageIndex};
use bitdaemon_common::{Sha1Hash, Sha256Hash};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::disk_buffer_pool::DEFAULT_BLOCK_SIZE;
use crate::disk_completed::DiskCompletedQueue;
use crate::disk_job::{
    DiskJob, Hash2Job, HashJob, JobKind, PartialReadJob, ReadJob, Status, WriteJob, job_flags,
};
use crate::disk_job_fence::{DiskJobFence, FENCE_POST_FENCE, FenceCounters};
use crate::disk_pool::{DiskIoThreadPool, JobQueue, WaitResult};
use crate::open_mode::OpenMode;
use crate::storage::{DiskStorage, StorageFactory, StorageParams};
use crate::storage_error::{Operation, StorageError};

/// Per-torrent state owned by the disk io — upstream the fence hangs
/// off `storage_interface`; here both live behind one entry.
struct StorageEntry {
    /// `RwLock`: read/write/hash jobs only need `&self` access — a
    /// plain `Mutex` would serialize all disk I/O for a torrent onto
    /// one logical thread regardless of pool size. `&mut self` ops
    /// (`rename_file`, `set_file_priority`) take the write side; they
    /// are fence jobs so exclusive access is already guaranteed.
    storage: RwLock<Box<dyn DiskStorage>>,
    fence: DiskJobFence,
    /// Per-piece write→hash ordering. Upstream hashes pieces out of
    /// the disk buffer pool, so a `hash` job can never observe a
    /// piece whose block writes haven't landed. We hash from the
    /// file — a hash running concurrently with a write to the same
    /// piece on another pool thread reads torn data and the piece
    /// fails verification forever. Write jobs increment the piece's
    /// in-flight count at submit time; hash jobs posted while the
    /// count is non-zero queue behind it and are reposted when the
    /// last write completes.
    piece_gate: Mutex<HashMap<i32, PieceGate>>,
    /// `m_disk_cache` — write cache (`store_buffer` role): received
    /// blocks live here until a contiguous-region flush (ADR-0012).
    /// Read/hash jobs consult it before the file (ADR-0014).
    cache: Mutex<crate::disk_cache::DiskCache>,
    /// Not an upstream mechanism — libtorrent doesn't distinguish
    /// disk media. Held only around the actual `st.write()` syscall
    /// of a Flush job: several pool threads each issuing their own
    /// `coalesce_writes` run concurrently can still land on scattered
    /// offsets (the elevator sweep orders *selection*, not execution
    /// overlap), and on a rotating disk, two writers seeking at once
    /// cost strictly more than one writer seeking twice — there is
    /// only one physical head. Forcing one in-flight write per
    /// storage turns concurrent seeks into a sequential sweep; reads
    /// and hashing are untouched and keep full pool parallelism.
    write_serializer: Mutex<()>,
    /// the torrent serves v1 (SHA-1 pieces).
    v1: bool,
    /// the torrent serves v2 (merkle / per-block SHA-256).
    v2: bool,
}

/// `piece_gate` entry — outstanding write jobs for one piece plus the
/// hash jobs waiting for them to land.
#[derive(Default)]
struct PieceGate {
    writes: i64,
    blocked: VecDeque<Box<DiskJob>>,
}

/// What `gate_consult` decided for a job past the fence — the
/// deferred side-effects run after the caller releases any fence
/// mutex (upstream defers exactly these out of its in-lock repost
/// callback: pool dispatch, flush scheduling, completions).
enum PostGate {
    /// satisfied read — complete through the normal path.
    Done(Box<DiskJob>),
    /// write landed in the cache — the job itself stays parked on
    /// its cache block (upstream `blk.write_state = disk_job*`) and
    /// completes with the flush, so nothing returns here; `loc`
    /// needs deferred flush scheduling (`schedule_flush`); `extra`
    /// are `m_fence_flush` drains already counted outstanding —
    /// they still need the deferred pool dispatch.
    Inserted(crate::disk_cache::PieceLocation, crate::disk_pool::JobQueue),
    /// dispatch to the job pool (deferred — `immediate_execute`
    /// would re-enter the fence through `job_complete`).
    Pool(Box<DiskJob>),
    /// consumed (parked in the piece gate).
    Parked,
}

/// `pread_disk_io` — positioned-I/O disk orchestrator.
pub struct PreadDiskIo {
    storages: Mutex<HashMap<StorageIndex, Arc<StorageEntry>>>,
    /// `m_free_slots` (upstream `storage_free_list`) + the high-water
    /// `next` index — freed slot reuse happens here.
    slots: Mutex<crate::storage_free_list::StorageFreeList>,
    next_index: AtomicU32,
    pool: Arc<DiskIoThreadPool>,
    /// `m_hash_threads` — dedicated pool for `hash`/`hash2` jobs
    /// (upstream `hashing_threads`, default 1); 0 threads → hash jobs
    /// run on the generic pool.
    hash_pool: Arc<DiskIoThreadPool>,
    /// Not an upstream pool split — libtorrent dispatches `write_job`
    /// to the generic `aio_threads` pool like any other job. Routing
    /// `Flush` here instead serializes them through the job queue
    /// (no contention) rather than a mutex every `aio_threads` worker
    /// fights over: on a seek-bound rotating disk, `N` threads each
    /// issuing their own ~tens-of-KiB write concurrently cost `N`
    /// concurrent seeks for one physical head, and forcing them
    /// through a shared `Mutex` just traded that for lock-convoy
    /// scheduling overhead of the same order. Default 1 thread; 0
    /// threads → flush jobs fall back to the generic pool (SSD/NVMe
    /// where concurrent writers don't fight a single head).
    flush_pool: Arc<DiskIoThreadPool>,
    completed: Arc<DiskCompletedQueue>,
    fence_counters: FenceCounters,
    /// `m_abort` — after abort, add_job posts straight to completions.
    abort: AtomicBool,
    buffer_pool: Arc<crate::disk_buffer_pool::DiskBufferPool>,
    /// `storage_constructor` — injected (ADR-0008).
    factory: StorageFactory,
    file_pool: Arc<crate::file_pool::FilePool>,
    /// `m_back_pressure`.
    back_pressure: crate::back_pressure::BackPressure,
    /// `settings_pack::disk_io_write_mode` — `disable_os_cache` on writes.
    write_no_cache: AtomicBool,
    /// `disk_io_write_mode == write_through` — `sync_file` after each
    /// write (upstream `pread_storage::write`, pread_storage.cpp:522).
    write_through: AtomicBool,
    /// `settings_pack::cache_expiry` — seconds a cached block may sit
    /// dirty before the periodic flush (upstream default 60).
    cache_expiry: std::sync::atomic::AtomicU64,
    /// `m_blocks` — write blocks queued/in flight; the back-pressure
    /// level (upstream `disk_cache::m_blocks`).
    write_blocks: std::sync::atomic::AtomicI64,
    /// `counters::num_blocks_written` (ADR-0017).
    num_blocks_written: std::sync::atomic::AtomicI64,
    /// `counters::num_blocks_read`.
    num_blocks_read: std::sync::atomic::AtomicI64,
    /// `counters::num_blocks_hashed`.
    num_blocks_hashed: std::sync::atomic::AtomicI64,
    /// `counters::num_write_ops`.
    num_write_ops: std::sync::atomic::AtomicI64,
    /// `counters::num_read_ops`.
    num_read_ops: std::sync::atomic::AtomicI64,
    /// `disk_io_error` count — failed cache flushes; the session
    /// surfaces them as `file_error_alert` (upstream
    /// `handle_disk_error`).
    flush_errors: std::sync::atomic::AtomicI64,
    /// perf probe (temporary): cumulative µs spent inside
    /// `perform_job` for Flush jobs + their count, and µs spent in
    /// the completion path (`add_completed_job` bookkeeping).
    flush_us: std::sync::atomic::AtomicU64,
    flush_n: std::sync::atomic::AtomicU64,
    complete_us: std::sync::atomic::AtomicU64,
    /// perf probe (temporary): µs inside `Storage::write` only and
    /// total bytes flushed — separates syscall time from per-job
    /// overhead (flat copy, pool lookup).
    write_us: std::sync::atomic::AtomicU64,
    flush_bytes: std::sync::atomic::AtomicU64,
    /// extra `OpenMode` bits pushed to every storage at
    /// `new_torrent` (upstream `open_file` reads
    /// `enable_set_file_valid_data`/`no_atime_storage` from
    /// `settings_interface` — ADR-0022).
    open_mode_extra: std::sync::atomic::AtomicU32,
    /// `settings_pack::no_recheck_incomplete_resume` — when set,
    /// `check_fastresume` never emits `need_full_check` (upstream
    /// reads it straight from `settings_interface` in `do_job`).
    no_recheck_incomplete_resume: AtomicBool,
    /// `m_need_tick` — storages a write marked dirty (part-file
    /// metadata); entries are due 2 min after the write
    /// (upstream `time_now() + minutes(2)`). Resolved by index at
    /// drain — a removed torrent simply isn't found (upstream's
    /// `weak_ptr` expiry).
    need_tick: Mutex<VecDeque<(std::time::Instant, StorageIndex)>>,
    /// `settings_pack::close_file_interval` (default 240 s) —
    /// `close_oldest` cadence in the maintenance pass.
    close_file_interval: AtomicI64,
    /// upstream `m_file_pool.close_oldest()` throttle state —
    /// `next_close_oldest_file` in `thread_fun`.
    close_oldest_due: Mutex<std::time::Instant>,
    /// upstream `thread_id == pool.first_thread_id()`: exactly one
    /// generic-pool worker runs the maintenance pass. Claimed on the
    /// first popped job, released on thread exit so a respawned
    /// worker picks it up.
    maintenance_claimed: AtomicBool,
    /// `settings_pack::mmap_file_size_cutoff` (default 40, 16 KiB
    /// blocks) — pushed to every storage; only the mmap backend
    /// consumes it.
    mmap_file_size_cutoff: AtomicI64,
    /// `settings_pack::disk_write_mode` (`mmap_write_mode_t` ordinal)
    /// — pushed to storages; the mmap backend resolves
    /// `m_use_mmap_writes` from it in `initialize`.
    disk_write_mode: AtomicI64,
    /// `settings_pack::disk_disable_copy_on_write` — pushed to
    /// storages; `open_mode::no_cow` on truncate-opens (mmap only).
    disk_disable_cow: AtomicBool,
}

impl PreadDiskIo {
    /// `pread_disk_io(ios, cnt)` — `poster` drains completions on the
    /// network thread.
    pub fn new(max_threads: i64, poster: crate::disk_completed::DrainPoster) -> Arc<Self> {
        Self::with_factory(
            max_threads,
            poster,
            crate::storage::default_storage_factory(),
        )
    }

    /// `pread_disk_io` with an injected `StorageFactory` (ADR-0008 —
    /// downstream private/encrypted storage).
    pub fn with_factory(
        max_threads: i64,
        poster: crate::disk_completed::DrainPoster,
        factory: StorageFactory,
    ) -> Arc<Self> {
        let buffer_pool = Arc::new(crate::disk_buffer_pool::DiskBufferPool::new());
        let completed = Arc::new(DiskCompletedQueue::new(poster));
        // the worker loop needs the orchestrator — two-phase init:
        // pool is created with a worker that captures a Weak we fill in.
        let io_slot: Arc<Mutex<Option<std::sync::Weak<PreadDiskIo>>>> = Arc::new(Mutex::new(None));
        let mk_worker = |slot: &Arc<Mutex<Option<std::sync::Weak<PreadDiskIo>>>>| {
            let slot = Arc::clone(slot);
            Arc::new(move |pool: Arc<DiskIoThreadPool>| {
                let io = slot
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade);
                if let Some(io) = io {
                    io.worker_loop(&pool);
                }
            })
        };
        let pool = DiskIoThreadPool::new(mk_worker(&io_slot));
        let hash_pool = DiskIoThreadPool::new(mk_worker(&io_slot));
        let flush_pool = DiskIoThreadPool::new(mk_worker(&io_slot));
        let io = Arc::new(Self {
            storages: Mutex::new(HashMap::new()),
            slots: Mutex::new(crate::storage_free_list::StorageFreeList::default()),
            next_index: AtomicU32::new(0),
            pool,
            completed,
            fence_counters: FenceCounters::default(),
            abort: AtomicBool::new(false),
            buffer_pool,
            factory,
            file_pool: Arc::new(crate::file_pool::FilePool::new(40)),
            back_pressure: crate::back_pressure::BackPressure::new(),
            write_blocks: std::sync::atomic::AtomicI64::new(0),
            num_blocks_written: std::sync::atomic::AtomicI64::new(0),
            num_blocks_read: std::sync::atomic::AtomicI64::new(0),
            num_blocks_hashed: std::sync::atomic::AtomicI64::new(0),
            num_write_ops: std::sync::atomic::AtomicI64::new(0),
            num_read_ops: std::sync::atomic::AtomicI64::new(0),
            flush_errors: std::sync::atomic::AtomicI64::new(0),
            flush_us: std::sync::atomic::AtomicU64::new(0),
            flush_n: std::sync::atomic::AtomicU64::new(0),
            complete_us: std::sync::atomic::AtomicU64::new(0),
            write_us: std::sync::atomic::AtomicU64::new(0),
            flush_bytes: std::sync::atomic::AtomicU64::new(0),
            write_no_cache: AtomicBool::new(false),
            write_through: AtomicBool::new(false),
            cache_expiry: std::sync::atomic::AtomicU64::new(60),
            hash_pool,
            flush_pool,
            open_mode_extra: std::sync::atomic::AtomicU32::new(0),
            no_recheck_incomplete_resume: AtomicBool::new(false),
            need_tick: Mutex::new(VecDeque::new()),
            // `settings_pack` defaults (settings_pack.cpp):
            // close_file_interval 240 on Windows, 0 elsewhere;
            // mmap_file_size_cutoff 40; disk_write_mode 1
            // (`always_mmap_write` — resolved to `use_mmap_writes` at
            // `initialize`, like upstream whose member default is
            // also "write through mmap").
            close_file_interval: AtomicI64::new(if cfg!(windows) { 240 } else { 0 }),
            close_oldest_due: Mutex::new(std::time::Instant::now()),
            maintenance_claimed: AtomicBool::new(false),
            mmap_file_size_cutoff: AtomicI64::new(40),
            disk_write_mode: AtomicI64::new(1),
            disk_disable_cow: AtomicBool::new(false),
        });
        // upstream `pread_disk_io` ctor:
        // `m_cache.set_max_size(max_queued_disk_bytes / 0x4000)` with
        // the setting default 100 MiB → 6400 blocks.
        io.back_pressure.set_max_size(100 * 1024 * 1024 / 0x4000);
        *io_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::downgrade(&io));
        io.pool.set_max_threads(max_threads);
        io
    }

    /// `settings_updated` — apply `max_queued_disk_bytes` (upstream
    /// divides by `default_block_size` = 16 KiB).
    pub fn set_max_queued_bytes(&self, bytes: i64) {
        self.back_pressure.set_max_size(bytes / 0x4000);
    }

    /// the buffer pool (jobs' `disk_buffer_holder`s come from here).
    #[must_use]
    pub fn buffer_pool(&self) -> Arc<crate::disk_buffer_pool::DiskBufferPool> {
        Arc::clone(&self.buffer_pool)
    }

    /// Graceful-shutdown flush — enqueue a `Flush` for every dirty
    /// run regardless of age (upstream's teardown drains the write
    /// cache before resume data is written, or the restored torrent
    /// re-hashes holes).
    pub fn flush_all(&self) {
        let entries: Vec<(StorageIndex, Arc<StorageEntry>)> = self
            .storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(i, e)| (*i, Arc::clone(e)))
            .collect();
        for (idx, e) in entries {
            let runs = e
                .cache
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take_expired_runs(std::time::Duration::ZERO);
            for ((_s, piece), first, bufs, writers) in runs {
                self.add_job(
                    idx,
                    self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                        piece,
                        first_block: first,
                        spans: vec![(piece, first, bufs.len())],
                        bufs,
                        writers: writers.into_iter().flatten().collect(),
                    })),
                );
            }
        }
    }

    /// `cache_expiry` periodic flush (upstream `m_disk_cache`
    /// flush timer): every dirty run older than `cache_expiry` is
    /// enqueued as a coalesced Flush. Called once per session tick.
    pub fn flush_expired(&self) {
        let expiry = self.cache_expiry.load(Ordering::Relaxed);
        let entries: Vec<(StorageIndex, Arc<StorageEntry>)> = self
            .storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(i, e)| (*i, Arc::clone(e)))
            .collect();
        for (idx, e) in entries {
            let runs = e
                .cache
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take_expired_runs(std::time::Duration::from_secs(expiry));
            for ((_s, piece), first, bufs, writers) in runs {
                self.add_job(
                    idx,
                    self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                        piece,
                        first_block: first,
                        spans: vec![(piece, first, bufs.len())],
                        bufs,
                        writers: writers.into_iter().flatten().collect(),
                    })),
                );
            }
        }
    }

    /// `settings_pack::cache_expiry` setter (seconds).
    pub fn set_cache_expiry(&self, secs: u64) {
        self.cache_expiry.store(secs, Ordering::Relaxed);
    }

    /// `settings_pack::no_recheck_incomplete_resume` — checked inside
    /// `do_job(check_fastresume)` upstream.
    pub fn set_no_recheck_incomplete_resume(&self, v: bool) {
        self.no_recheck_incomplete_resume
            .store(v, Ordering::Relaxed);
    }

    /// the completion queue (network thread drains via the poster).
    #[must_use]
    pub fn completed(&self) -> Arc<DiskCompletedQueue> {
        Arc::clone(&self.completed)
    }

    /// `new_torrent(storage_params, torrent)` — build the storage via
    /// the injected `StorageFactory`, register it, return its index
    /// (upstream `storage_holder`).
    pub fn new_torrent(&self, params: &StorageParams<'_>) -> StorageIndex {
        // `m_free_slots.new_index(m_torrents.size())` upstream —
        // reuse a freed slot else take the high-water mark.
        let idx = {
            let mut free = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            let next = StorageIndex(self.next_index.load(Ordering::Relaxed));
            let got = free.new_index(next);
            if got == next {
                self.next_index.store(next.0 + 1, Ordering::Relaxed);
            }
            got
        };
        let mut g = self.storages.lock().unwrap_or_else(|e| e.into_inner());
        let storage = (self.factory)(params, Arc::clone(&self.file_pool));
        storage.set_storage_index(idx);
        self.push_storage_settings(&*storage);
        g.insert(
            idx,
            Arc::new(StorageEntry {
                storage: RwLock::new(storage),
                fence: DiskJobFence::new(),
                piece_gate: Mutex::new(HashMap::new()),
                cache: Mutex::new(crate::disk_cache::DiskCache::default()),
                write_serializer: Mutex::new(()),
                v1: params.v1,
                v2: params.v2,
            }),
        );
        idx
    }

    /// Register a pre-built storage (tests / custom wiring).
    pub fn insert_storage(&self, idx: StorageIndex, storage: Box<dyn DiskStorage>) {
        storage.set_storage_index(idx);
        self.push_storage_settings(&*storage);
        self.storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                idx,
                Arc::new(StorageEntry {
                    storage: RwLock::new(storage),
                    fence: DiskJobFence::new(),
                    piece_gate: Mutex::new(HashMap::new()),
                    cache: Mutex::new(crate::disk_cache::DiskCache::default()),
                    write_serializer: Mutex::new(()),
                    v1: true,
                    v2: false,
                }),
            );
    }

    /// `remove_torrent(idx)` — drop the storage; its queued jobs fail
    /// fast at execution (`NotFound`) like upstream's aborted path.
    pub fn remove_torrent(&self, idx: StorageIndex) {
        self.add_fence_job(
            idx,
            self.mk_job(JobKind::RemoveTorrent(crate::disk_job::RemoveTorrentJob {
                handler: Box::new(|| {}),
            })),
        );
    }

    /// `abort_jobs(wait)` — stop the pools; queued jobs complete aborted.
    pub fn abort_jobs(&self, wait: bool) {
        self.abort.store(true, Ordering::Relaxed);
        let mut drained = self.pool.drain_jobs();
        drained.append(&mut self.hash_pool.drain_jobs());
        drained.append(&mut self.flush_pool.drain_jobs());
        // hash jobs parked behind the piece gate never reached the
        // pool — drain them too so their completions still post.
        // Same for jobs parked inside the per-torrent fence (writes
        // waiting on a clear_piece/move, fence jobs queued behind an
        // active fence): their handlers must still run.
        for e in self
            .storages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
        {
            for g in e
                .piece_gate
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .values_mut()
            {
                drained.append(&mut g.blocked);
            }
            let mut blocked = e.fence.take_blocked();
            for _ in 0..blocked.len() {
                self.fence_counters.dec_blocked();
            }
            drained.append(&mut blocked);
            // writers still parked on cache blocks — their data never
            // flushed; abort them so their handlers still run
            // (upstream `abort_jobs` fails every outstanding job).
            e.cache
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .abort_all_writers();
            for w in e
                .cache
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take_orphans()
            {
                drained.push_back(w);
            }
        }
        for j in &mut drained {
            crate::disk_completed::abort(j);
            self.job_done(j);
        }
        self.pool.abort(wait);
        self.hash_pool.abort(wait);
        self.flush_pool.abort(wait);
        self.completed.append(drained);
    }

    /// A job reached the completed queue — release its write-block
    /// slot and release observers when the level drains below the
    /// low watermark (upstream `disk_cache::check_buffer_level` on
    /// every erase).
    fn job_done(&self, j: &mut DiskJob) {
        // upstream `add_completed_jobs_impl`: a `fence`-flagged job
        // leaves its `num_fenced_read + get_type()` slot here — the
        // matching increment ran at `add_fence_job` submit.
        if j.flags & job_flags::FENCE != 0 {
            self.fence_counters.dec_fenced(j.get_type());
        }
        match &mut j.action {
            // a write that errored *before reaching the cache*
            // (aborted while parked behind a fence) releases its
            // back-pressure slot; a cached write (`CACHED`) keeps its
            // slot until the Flush retires the block — upstream
            // `m_blocks` counts cached blocks, so decrements live in
            // the Flush arm. (A superseded writer completing aborted
            // also carries CACHED — its slot transferred to the
            // duplicate that overwrote the block.)
            JobKind::Write(_) if j.error.is_err() && j.flags & job_flags::CACHED == 0 => {
                let level = self.write_blocks.fetch_sub(1, Ordering::Relaxed) - 1;
                self.back_pressure.check_buffer_level(level);
            }
            JobKind::Flush(f) => {
                // upstream `flush_piece` retires blocks only after a
                // successful `writev` — on error (or abort) the buffers
                // go back to the cache so the next watermark/expiry
                // flush retries them instead of leaving un-written
                // holes under a `flushed` flag (silent corruption).
                // `spans` splits a coalesced write back into per-piece
                // runs for the bookkeeping.
                if let Some(e) = self.storage(j.storage) {
                    let mut cache = e.cache.lock().unwrap_or_else(|p| p.into_inner());
                    if j.error.is_ok() {
                        let mut at = 0usize;
                        for &(piece, first, n) in &f.spans {
                            cache.run_flushed(
                                (j.storage, piece),
                                usize::try_from(first).unwrap_or(0),
                                f.bufs[at..at + n].len(),
                            );
                            at += n;
                        }
                        self.num_blocks_written
                            .fetch_add(f.bufs.len() as i64, Ordering::Relaxed);
                        // upstream counts the op only when the iovec
                        // group didn't fail (`if (!failed)`).
                        self.num_write_ops.fetch_add(1, Ordering::Relaxed);
                    } else {
                        // upstream `flush_cache_blocks`: a failed
                        // writev completes every writer of the visited
                        // span with the error *and marks the blocks
                        // flushed anyway* (`flushed.set_bit`) — the
                        // buffer is freed, the data is dropped, and
                        // the peer layer re-requests the piece. The
                        // buffers are NOT put back for retry (an
                        // un-visited span would stay dirty, but our
                        // flush is a single iovec group).
                        tracing::error!(
                            piece = f.piece.0,
                            error = %j.error,
                            "flush failed; buffers retired, writers complete with error"
                        );
                        self.flush_errors.fetch_add(1, Ordering::Relaxed);
                        for &(piece, first, n) in &f.spans {
                            cache.run_flushed(
                                (j.storage, piece),
                                usize::try_from(first).unwrap_or(0),
                                n,
                            );
                        }
                    }
                }
                let level = self
                    .write_blocks
                    .fetch_sub(f.bufs.len() as i64, Ordering::Relaxed)
                    - f.bufs.len() as i64;
                self.back_pressure.check_buffer_level(level);
            }
            JobKind::Read(_) | JobKind::PartialRead(_) => {
                self.num_blocks_read.fetch_add(1, Ordering::Relaxed);
                self.num_read_ops.fetch_add(1, Ordering::Relaxed);
            }
            JobKind::Hash(h) => {
                let blocks = self
                    .storage(j.storage)
                    .map(|e| {
                        let st = e.storage.read().unwrap_or_else(|p| p.into_inner());
                        i64::from(
                            st.files().piece_size(h.piece)
                                / crate::disk_cache::DEFAULT_BLOCK_SIZE as i32,
                        )
                        .max(1)
                    })
                    .unwrap_or(1);
                self.num_blocks_hashed.fetch_add(blocks, Ordering::Relaxed);
                if let Some(e) = self.storage(j.storage) {
                    // upstream `force_flush` + optimistic flush: a
                    // hashed piece is written out promptly in one
                    // contiguous run, regardless of the watermark.
                    let runs = {
                        let mut cache = e.cache.lock().unwrap_or_else(|p| p.into_inner());
                        cache.hash_returned((j.storage, h.piece));
                        if j.error.is_ok() {
                            cache.take_all_runs((j.storage, h.piece))
                        } else {
                            Vec::new()
                        }
                    };
                    for (first, bufs, writers) in runs {
                        self.add_job(
                            j.storage,
                            self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                                piece: h.piece,
                                first_block: first,
                                spans: vec![(h.piece, first, bufs.len())],
                                bufs,
                                writers: writers.into_iter().flatten().collect(),
                            })),
                        );
                    }
                }
            }
            JobKind::Hash2(_) => {
                // upstream `do_job(hash2)` — `m_cache.hash2` is a pure
                // lookup + fallback; it does NOT mark the piece
                // `piece_hash_returned`/`force_flush` (that flag means
                // the *whole* piece hash was delivered).
                self.num_blocks_hashed.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn storage(&self, idx: StorageIndex) -> Option<Arc<StorageEntry>> {
        self.storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&idx)
            .cloned()
    }

    /// `add_job(j)` — non-exclusive path.
    pub fn add_job(&self, idx: StorageIndex, j: Box<DiskJob>) {
        if self.abort.load(Ordering::Relaxed) {
            let mut j = j;
            crate::disk_completed::abort(&mut j);
            self.job_done(&mut j);
            self.completed.push(j);
            return;
        }
        let mut j = j;
        j.storage = idx;
        if let Some(entry) = self.storage(idx) {
            if let Some(j) = entry.fence.is_blocked(j, &self.fence_counters) {
                // upstream `add_job`: satisfied at the consult point
                // (cache-hit read / cache-inserted write) — complete
                // through the normal bookkeeping path.
                if let Some(done) = self.gate_or_enqueue(&entry, j) {
                    self.add_completed_job(done, Some(&entry));
                }
            }
        } else {
            // no storage — job is stale; complete it aborted
            let mut j = j;
            crate::disk_completed::abort(&mut j);
            self.job_done(&mut j);
            self.completed.push(j);
        }
    }

    /// `add_fence_job(j)` — exclusive path.
    pub fn add_fence_job(&self, idx: StorageIndex, j: Box<DiskJob>) {
        if self.abort.load(Ordering::Relaxed) {
            let mut j = j;
            crate::disk_completed::abort(&mut j);
            self.job_done(&mut j);
            self.completed.push(j);
            return;
        }
        match self.storage(idx) {
            Some(entry) => {
                // upstream `raise_fence` drains the cache first — a
                // fence job (delete/move/clear_piece) must observe a
                // fully-durable storage, so every dirty region is
                // enqueued as a coalesced Flush and counted against
                // `outstanding` before the fence raises.
                let runs = entry
                    .cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take_all_storage_runs();
                for ((_s, piece), first, bufs, writers) in runs {
                    self.add_job(
                        idx,
                        self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                            piece,
                            first_block: first,
                            spans: vec![(piece, first, bufs.len())],
                            bufs,
                            writers: writers.into_iter().flatten().collect(),
                        })),
                    );
                }
                let mut j = j;
                j.storage = idx;
                // upstream `add_fence_job` counts the job under
                // `num_fenced_read + get_type()` before `raise_fence`
                // — the gauge covers the job's whole life in the
                // system (blocked or running); the matching decrement
                // fires in `job_done` when the `FENCE`-flagged job
                // reaches the completed queue.
                self.fence_counters.inc_fenced(j.get_type());
                let (ret, job) = entry.fence.raise_fence(j, &self.fence_counters);
                if ret == FENCE_POST_FENCE {
                    if let Some(j) = job {
                        self.enqueue(j);
                    }
                }
            }
            None => {
                let mut j = j;
                crate::disk_completed::abort(&mut j);
                self.job_done(&mut j);
                self.completed.push(j);
            }
        }
    }

    /// The pure consult — safe under the fence mutex: only cache and
    /// piece-gate ops, never a pool push, a flush enqueue or a
    /// completion (upstream's in-lock repost callback does exactly
    /// this much). `fence_up` is `has_fence()` sampled by the caller —
    /// inside the repost callback the lock is already held, so it is
    /// handed in from `job_complete`'s repost.
    fn gate_consult(&self, e: &StorageEntry, mut j: Box<DiskJob>, fence_up: bool) -> PostGate {
        // a write that made it past the fence lands in the cache now —
        // fresh or reposted alike. With a stacked fence still raised,
        // upstream's `m_fence_flush` drains the storage right away so
        // the queued fence observes durable files; those flush jobs
        // are counted outstanding past `is_blocked` (a parked job
        // can't hold a fence — it must run *before* it).
        if matches!(j.action, JobKind::Write(_)) {
            // an aborted write reposted through the fence/gate must
            // not be inserted — complete it with its abort error.
            if j.flags & job_flags::ABORTED == job_flags::ABORTED {
                return PostGate::Done(j);
            }
            // upstream `insert_write`: the job is stored on its cache
            // block (`write_state = disk_job*`) and completes when the
            // flush that carries it finishes — not now.
            let storage = j.storage;
            let loc = self.insert_write(e, j);
            let mut extra = JobQueue::new();
            if fence_up {
                for fj in self.flush_for_fence(e, storage) {
                    // `account_outstanding` already applied in
                    // `flush_for_fence`; the consult registers the
                    // in-flight write against the piece gate — a Flush
                    // only ever lands on `Pool`.
                    if let PostGate::Pool(j) = self.gate_consult(e, fj, fence_up) {
                        extra.push_back(j);
                    }
                }
            }
            return PostGate::Inserted(loc, extra);
        }
        // the read-side consult point (`prepare_read`): full cache
        // hits complete without any disk I/O, one-sided spanning hits
        // become `PartialRead` for the missing side.
        if matches!(j.action, JobKind::Read(_)) && prepare_read(e, &mut j) {
            return PostGate::Done(j);
        }
        // a coalesced flush touches every piece in `spans`; hash jobs
        // park behind any in-flight write on their piece.
        let pieces: Vec<i32> = match &j.action {
            JobKind::Flush(f) => f.spans.iter().map(|s| s.0.0).collect(),
            JobKind::Hash(h) => vec![h.piece.0],
            JobKind::Hash2(h) => vec![h.piece.0],
            _ => Vec::new(),
        };
        if pieces.is_empty() {
            return PostGate::Pool(j);
        }
        let is_write = matches!(j.action, JobKind::Flush(_));
        let mut gate = e.piece_gate.lock().unwrap_or_else(|x| x.into_inner());
        if is_write {
            for p in pieces {
                gate.entry(p).or_default().writes += 1;
            }
            PostGate::Pool(j)
        } else if pieces
            .iter()
            .any(|p| gate.get(p).is_some_and(|g| g.writes > 0))
        {
            let p = pieces[0];
            tracing::debug!(piece = p, "piece_gate: hash parked behind writes");
            gate.entry(p).or_default().blocked.push_back(j);
            PostGate::Parked
        } else {
            PostGate::Pool(j)
        }
    }

    /// Post-fence routing for jobs NOT under the fence mutex —
    /// `add_job`'s happy path and the `piece_gate` release path:
    /// consult, then run the deferred side-effects inline.
    /// Returns the job back when it completed inline — the caller must
    /// run it through the normal completion path.
    fn gate_or_enqueue(&self, entry: &StorageEntry, j: Box<DiskJob>) -> Option<Box<DiskJob>> {
        match self.gate_consult(entry, j, entry.fence.has_fence()) {
            PostGate::Done(j) => Some(j),
            PostGate::Inserted(loc, extra) => {
                self.flush_after_insert(entry, std::iter::once(loc));
                for x in extra {
                    self.enqueue(x);
                }
                // writers superseded by the insert complete aborted —
                // their handlers must still run exactly once
                // (upstream aborts the displaced `write_state` job).
                self.drain_cache_orphans(entry);
                None
            }
            PostGate::Pool(j) => {
                self.enqueue(j);
                None
            }
            PostGate::Parked => None,
        }
    }

    /// Complete write jobs the cache evicted/superseded since the
    /// last drain — each was already stamped aborted; it still has to
    /// run through fence bookkeeping (`job_complete` decrements
    /// `outstanding`) before landing on the completed queue.
    fn drain_cache_orphans(&self, entry: &StorageEntry) {
        let orphans = entry
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take_orphans();
        for w in orphans {
            self.add_completed_job(w, Some(entry));
        }
    }

    /// `pread_disk_io::insert_write` — the pure cache insert of a
    /// write job's block. Upstream stores the job itself on the block
    /// (`blk.write_state = write_job`): the write only completes when
    /// a flush makes its buffer durable (or the piece is torn down),
    /// so the caller's handler sees real flush errors. Safe under the
    /// fence mutex — cache ops only. Returns the inserted location
    /// for deferred flush scheduling.
    fn insert_write(
        &self,
        entry: &StorageEntry,
        mut j: Box<DiskJob>,
    ) -> crate::disk_cache::PieceLocation {
        let JobKind::Write(w) = &mut j.action else {
            // upstream `TORRENT_ASSERT(j->action == write)` — only
            // write jobs reach `insert_write`. Abort explicitly and
            // run the normal completion path: a silent `completed`
            // push would skip `job_done` (the `write_blocks`
            // back-pressure slot stays charged forever) and
            // `job_complete` (the fence's `outstanding` count never
            // drains — a pending fence would wait on it).
            debug_assert!(matches!(j.action, JobKind::Write(_)));
            let storage = j.storage;
            crate::disk_completed::abort(&mut j);
            self.add_completed_job(j, Some(entry));
            return (storage, PieceIndex(-1));
        };
        let (piece_size, piece_size2) = {
            let st = entry.storage.read().unwrap_or_else(|e| e.into_inner());
            let fs = st.files();
            (
                i64::from(if entry.v1 {
                    fs.piece_size(w.piece)
                } else {
                    fs.piece_size2(w.piece)
                }),
                i64::from(fs.piece_size2(w.piece)),
            )
        };
        let block_idx =
            usize::try_from(w.offset / crate::disk_cache::DEFAULT_BLOCK_SIZE as i32).unwrap_or(0);
        let data = {
            let want = usize::from(w.buffer_size);
            match w.buf.data() {
                Some(b) => b[..want.min(b.len())].to_vec(),
                None => Vec::new(),
            }
        };
        let force_flush = w.force_flush;
        let loc = (j.storage, w.piece);
        // `job_flags::CACHED` — the write_blocks slot stays charged
        // until the Flush retires the block; a `job_done` on this job
        // must not decrement it again.
        j.flags |= job_flags::CACHED;
        let mut cache = entry.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.insert(
            loc,
            block_idx,
            Some(data),
            Some(j),
            force_flush,
            crate::disk_cache::PieceParams {
                piece_size,
                piece_size2,
                v1: entry.v1,
                v2: entry.v2,
            },
        );
        loc
    }

    /// `try_flush_blocks`/`schedule_flush` — flush selection after an
    /// insert. Enqueues `Flush` jobs through `add_job`, so it must run
    /// off the fence mutex (upstream defers `schedule_flush` out of
    /// the repost callback for the same reason).
    fn flush_after_insert(
        &self,
        entry: &StorageEntry,
        inserted: impl IntoIterator<Item = crate::disk_cache::PieceLocation>,
    ) {
        let mut runs = Vec::new();
        {
            let mut cache = entry.cache.lock().unwrap_or_else(|e| e.into_inner());
            for loc in inserted {
                // upstream's first flush pass: complete pieces flush as
                // one contiguous `writev`; `force_flush_flag` pieces
                // (the `flush_piece` write flag, or hashed) drain all
                // their dirty runs regardless of the watermark.
                let Some(p) = cache.piece(loc) else {
                    continue;
                };
                let complete = p.blocks.iter().all(|b| b.buf.is_some() || b.flushed);
                let forced = p.flags & crate::disk_cache::piece_flags::FORCE_FLUSH != 0;
                if complete {
                    if let Some((first, bufs, writers)) = cache.take_flush_run(loc) {
                        runs.push((loc, first, vec![(loc.1, first, bufs.len())], bufs, writers));
                    }
                } else if forced {
                    for (first, bufs, writers) in cache.take_all_runs(loc) {
                        runs.push((loc, first, vec![(loc.1, first, bufs.len())], bufs, writers));
                    }
                }
            }
            // upstream `check_buffer_level`/`try_flush_blocks`: above
            // the high watermark (or while the exceeded flag is
            // sticky) keep flushing runs until the cache drains to the
            // low watermark.
            while let Some(low) = self.back_pressure.should_flush(cache.num_cached_blocks) {
                if cache.num_cached_blocks <= low {
                    break;
                }
                // upstream `flush_to_disk` order under a whole-piece
                // hasher: force-flush/ready pieces first (pass 1),
                // then ascending `(storage, piece)` — upstream's
                // pass-4 `get<0>` order. Its pass 2 (largest
                // `hasher_cursor - flushed_cursor` run) is empty here
                // because `hasher_cursor` only moves when the hash
                // job runs. `coalesce_writes` extends each run across
                // piece boundaries so write-through writes stay
                // large.
                let Some(run) = cache.take_coalesced(128) else {
                    break;
                };
                runs.push(run);
            }
        }
        for (run_loc, first, spans, bufs, writers) in runs {
            self.add_job(
                run_loc.0,
                self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                    piece: run_loc.1,
                    first_block: first,
                    spans,
                    bufs,
                    writers: writers.into_iter().flatten().collect(),
                })),
            );
        }
    }

    /// Upstream `m_fence_flush`/`flush_storage` — drain the storage's
    /// whole write cache when a stacked fence is waiting on
    /// just-inserted blocks. Each run becomes a `Flush` job counted
    /// outstanding *past* `is_blocked` (a parked job can't hold the
    /// fence — it must run before it, like upstream's dedicated
    /// fence-flush pass on a generic thread).
    fn flush_for_fence(&self, e: &StorageEntry, storage: StorageIndex) -> JobQueue {
        let runs = e
            .cache
            .lock()
            .unwrap_or_else(|x| x.into_inner())
            .take_all_storage_runs();
        runs.into_iter()
            .map(|((_s, piece), first, bufs, writers)| {
                let mut fj = self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                    piece,
                    first_block: first,
                    spans: vec![(piece, first, bufs.len())],
                    bufs,
                    writers: writers.into_iter().flatten().collect(),
                }));
                fj.storage = storage;
                e.fence.account_outstanding(&mut fj);
                fj
            })
            .collect()
    }

    /// Pool routing — upstream `job::thread_type()`: `hash`/`hash2`
    /// jobs go to `m_hash_threads` when it has threads. `Flush` isn't
    /// an upstream split (see [`Self::flush_pool`]'s doc) but follows
    /// the same opt-in pattern; everything else goes to the generic
    /// `aio_threads` pool.
    fn job_pool(&self, j: &DiskJob) -> &Arc<DiskIoThreadPool> {
        if matches!(j.action, JobKind::Hash(_) | JobKind::Hash2(_))
            && self.hash_pool.max_threads() > 0
        {
            &self.hash_pool
        } else if matches!(j.action, JobKind::Flush(_)) && self.flush_pool.max_threads() > 0 {
            &self.flush_pool
        } else {
            &self.pool
        }
    }

    fn enqueue(&self, j: Box<DiskJob>) {
        let pool = self.job_pool(&j);
        pool.push_back(j);
        if pool.max_threads() == 0 {
            self.immediate_execute(pool);
        } else {
            pool.submit_jobs();
        }
    }

    /// `immediate_execute` — 0-thread fallback: run queued jobs inline.
    /// Upstream still runs the thread_fun maintenance block when the
    /// pool has no threads (jobs execute on the io_context thread), so
    /// the same pass runs here.
    fn immediate_execute(&self, pool: &Arc<DiskIoThreadPool>) {
        loop {
            let Some(j) = pool.pop_front() else {
                break;
            };
            self.maintenance();
            self.execute_and_complete(j);
        }
    }

    /// `mmap_disk_io::thread_fun`'s maintenance block — drains due
    /// `m_need_tick` entries (deferred part-file metadata flush,
    /// ~2 min after the write) and sheds pooled fds/views on
    /// `close_file_interval` cadence (≤ 0 → recheck in one minute,
    /// in case the setting changed). Upstream gates this on
    /// `thread_id == pool.first_thread_id()` of the generic pool.
    fn maintenance(&self) {
        let now = std::time::Instant::now();
        loop {
            let idx = {
                let mut q = self.need_tick.lock().unwrap_or_else(|e| e.into_inner());
                match q.front() {
                    Some((due, _)) if *due < now => q.pop_front().map(|(_, i)| i),
                    _ => None,
                }
            };
            let Some(idx) = idx else { break };
            // upstream re-locks around `st->tick()` — ours pops first,
            // then ticks; a removed torrent resolves to `None` here
            // (the `weak_ptr` expiry upstream).
            if let Some(e) = self.storage(idx) {
                e.storage
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .do_tick();
            }
        }
        let mut due = self
            .close_oldest_due
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if now > *due {
            let interval = self.close_file_interval.load(Ordering::Relaxed);
            if interval <= 0 {
                // check again in one minute, in case the setting changed
                *due = now + std::time::Duration::from_secs(60);
            } else {
                *due = now + std::time::Duration::from_secs(u64::try_from(interval).unwrap_or(0));
                self.file_pool.close_oldest();
                // `m_file_pool` under the mmap backend *is* the view
                // pool — each storage forwards the call to its own
                // pooled resource (a no-op for storages without one;
                // pools dedupe the per-storage fan-out internally).
                for e in self
                    .storages
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .values()
                {
                    e.storage
                        .read()
                        .unwrap_or_else(|p| p.into_inner())
                        .close_oldest();
                }
            }
        }
    }

    /// worker loop (`thread_fun`) — wait, pop, perform, complete.
    fn worker_loop(&self, pool: &Arc<DiskIoThreadPool>) {
        // `thread_id == pool.first_thread_id()` — one generic-pool
        // worker owns the maintenance pass. Claimed on the first
        // popped job, released on exit so a respawned worker picks it
        // up (upstream's `first_thread_id` is fixed; ours must be
        // re-claimable because threads exit on reap).
        let mut owns_maintenance = false;
        loop {
            match pool.wait_for_job() {
                WaitResult::NewJob => {
                    let Some(j) = pool.pop_front() else { continue };
                    if Arc::ptr_eq(pool, &self.pool)
                        && (owns_maintenance
                            || self
                                .maintenance_claimed
                                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                                .is_ok())
                    {
                        owns_maintenance = true;
                        self.maintenance();
                    }
                    self.execute_and_complete(j);
                }
                WaitResult::Interrupt => continue,
                // `exit_thread` — the exit token was consumed inside
                // `wait_for_job` (upstream `try_thread_exit` runs in
                // the wait loop, not the worker); just end the thread.
                WaitResult::ExitThread => {
                    if owns_maintenance {
                        self.maintenance_claimed.store(false, Ordering::Release);
                    }
                    return;
                }
            }
        }
    }

    fn execute_and_complete(&self, mut j: Box<DiskJob>) {
        let entry = self.storage(j.storage);
        let is_flush = matches!(j.action, JobKind::Flush(_));
        let t0 = std::time::Instant::now();
        if self.perform_job(&mut j, entry.as_deref()) {
            // repost: the job observed in-flight writes on its piece —
            // it re-enters at the post-fence consult point, where the
            // piece gate parks it behind the flush (upstream parks
            // hash jobs on `cached_piece_entry` the same way).
            match entry.as_deref() {
                Some(e) => {
                    if let Some(done) = self.gate_or_enqueue(e, j) {
                        self.add_completed_job(done, Some(e));
                    }
                    return;
                }
                // storage vanished mid-flight — complete aborted.
                None => crate::disk_completed::abort(&mut j),
            }
        }
        if is_flush {
            self.flush_us
                .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
            self.flush_n.fetch_add(1, Ordering::Relaxed);
        }
        let t1 = std::time::Instant::now();
        self.add_completed_job(j, entry.as_deref());
        self.complete_us
            .fetch_add(t1.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    /// `add_completed_jobs` — fence bookkeeping then post completions.
    fn add_completed_job(&self, j: Box<DiskJob>, entry: Option<&StorageEntry>) {
        let mut q = JobQueue::new();
        q.push_back(j);
        self.add_completed_jobs(q, entry);
    }

    fn add_completed_jobs(&self, mut jobs: JobQueue, entry: Option<&StorageEntry>) {
        // fence bookkeeping: for each completed job on this storage,
        // repost blocked jobs — *before* the job lands on completions,
        // matching upstream ordering. Iterated as a worklist so a job
        // satisfied inline at repost (cache-hit read, cache-inserted
        // write) completes through the same bookkeeping without
        // recursing back into `job_complete`.
        let mut done = JobQueue::new();
        let mut work: VecDeque<Box<DiskJob>> = std::mem::take(&mut jobs);
        while let Some(mut j) = work.pop_front() {
            if let Some(e) = entry {
                // piece gate: a completed flush (which may span
                // pieces) releases hash jobs queued behind it —
                // upstream's hasher never observes a piece whose
                // writes haven't landed.
                let gate_pieces: Vec<i32> = match &j.action {
                    JobKind::Flush(f) => f.spans.iter().map(|s| s.0.0).collect(),
                    _ => Vec::new(),
                };
                if !gate_pieces.is_empty() {
                    let mut released = VecDeque::new();
                    {
                        let mut gate = e.piece_gate.lock().unwrap_or_else(|p| p.into_inner());
                        for p in gate_pieces {
                            if let Some(g) = gate.get_mut(&p) {
                                g.writes -= 1;
                                if g.writes <= 0 {
                                    released.append(&mut g.blocked);
                                    gate.remove(&p);
                                }
                            }
                        }
                    }
                    for bj in released {
                        // parked jobs already passed the fence once —
                        // they carry IN_PROGRESS and stay counted in
                        // `outstanding`, so they re-enter at the
                        // post-fence consult point like reposts;
                        // re-entering add_job would double-account
                        // them (and trip the fence's debug_assert).
                        if let Some(done_j) = self.gate_or_enqueue(e, bj) {
                            work.push_back(done_j);
                        }
                    }
                }
                // the repost callback fires while `has_fence` is held
                // (upstream's in-lock repost) — only the pure consult
                // may run inside; pool dispatch, flush scheduling and
                // completions are deferred until it returns.
                let mut repost_pool: Vec<Box<DiskJob>> = Vec::new();
                let mut repost_locs: Vec<crate::disk_cache::PieceLocation> = Vec::new();
                let mut repost_done: Vec<Box<DiskJob>> = Vec::new();
                let reposted = e.fence.job_complete(&mut j, &mut |bj, fence_up| match self
                    .gate_consult(e, bj, fence_up)
                {
                    PostGate::Done(j) => repost_done.push(j),
                    PostGate::Inserted(loc, extra) => {
                        repost_locs.push(loc);
                        repost_pool.extend(extra);
                    }
                    PostGate::Pool(j) => repost_pool.push(j),
                    PostGate::Parked => {}
                });
                // upstream `add_completed_jobs_impl`:
                // `inc_stats_counter(blocked_disk_jobs, -ret)` — every
                // job `job_complete` popped out of the blocked queue
                // leaves the gauge.
                for _ in 0..reposted {
                    self.fence_counters.dec_blocked();
                }
                // deferred, in upstream order: `schedule_flush` for
                // the inserted blocks, the inline completions re-enter
                // the completion path (they can lower further fences),
                // then the pool dispatch.
                if !repost_locs.is_empty() {
                    self.flush_after_insert(e, repost_locs);
                }
                work.extend(repost_done);
                for bj in repost_pool {
                    self.enqueue(bj);
                }
                // writers evicted/superseded during this job's
                // execution or the reposts above (piece clear,
                // duplicate insert) complete aborted through the
                // normal path — their handlers must still run once.
                work.extend(
                    e.cache
                        .lock()
                        .unwrap_or_else(|x| x.into_inner())
                        .take_orphans(),
                );
                // upstream `flush_piece`→`take_write_job`: the write
                // jobs parked on the flushed blocks complete *here*,
                // with the flush's result — durable write or the
                // disk error that lost it. They flow back through
                // `work` so each gets fence bookkeeping
                // (`job_complete` decrements `outstanding`).
                if let JobKind::Flush(f) = &mut j.action {
                    for mut w in std::mem::take(&mut f.writers) {
                        w.error = j.error.clone();
                        work.push_back(w);
                    }
                }
            }
            self.job_done(&mut j);
            done.push_back(j);
        }
        self.completed.append(done);
    }

    /// `perform_job` — execute on the disk thread. Returns `true`
    /// when the job must be *reposted* instead of completed: a hash
    /// that caught blocks out in an in-flight flush re-enters through
    /// the piece gate and parks there until the write lands (upstream
    /// parks hash jobs on the piece entry; a bounded sleep would race
    /// the flush and could hash a zero hole).
    fn perform_job(&self, j: &mut DiskJob, entry: Option<&StorageEntry>) -> bool {
        if j.flags & job_flags::ABORTED == job_flags::ABORTED {
            j.error = StorageError::new(
                std::io::Error::from(std::io::ErrorKind::Interrupted),
                FileIndex(-1),
                Operation::Unknown,
            );
            return false;
        }
        let Some(e) = entry else {
            // job without storage (no-op kinds) or stale storage
            match &j.action {
                JobKind::ReleaseFiles(_) | JobKind::StopTorrent(_) | JobKind::ClearPiece(_) => {}
                _ => {
                    j.error = StorageError::new(
                        std::io::Error::from(std::io::ErrorKind::NotFound),
                        FileIndex(-1),
                        Operation::Unknown,
                    );
                }
            }
            return false;
        };
        // `&mut self` ops (rename, file priorities, check_fastresume)
        // are exclusive fence jobs — they take the write side.
        // Everything else is `&self` and runs concurrently across
        // pool threads on the read side; upstream `mmap_disk_io`
        // likewise does not hold a per-storage mutex around
        // positioned I/O.
        if matches!(
            j.action,
            JobKind::RenameFile(_) | JobKind::FilePriority(_) | JobKind::CheckFastresume(_)
        ) {
            let mut st_guard = e.storage.write().unwrap_or_else(|p| p.into_inner());
            let st: &mut dyn DiskStorage = &mut **st_guard;
            j.error = match &mut j.action {
                JobKind::RenameFile(r) => {
                    match st.rename_file(r.file_index, r.name.as_os_str().as_encoded_bytes()) {
                        Ok(()) => StorageError::default(),
                        Err(e) => e,
                    }
                }
                JobKind::FilePriority(f) => {
                    // exclusive job — the fence guarantees sole access;
                    // upstream writes the adjusted priorities back into
                    // the job for the handler
                    match st.set_file_priority(&f.prio) {
                        Ok(()) => {
                            f.prio = st.file_priorities().to_vec();
                            StorageError::default()
                        }
                        Err(e) => {
                            // upstream `restore_prio` writes
                            // `prio = m_file_priority` back — the
                            // handler sees the storage's actual
                            // priorities, not the request that
                            // failed partway
                            f.prio = st.file_priorities().to_vec();
                            e
                        }
                    }
                }
                JobKind::CheckFastresume(c) => {
                    // upstream `do_job(check_fastresume)` — `ret` is a
                    // pure `status_t` bitmask (`disk_status`):
                    //   initialize() bits ride back always;
                    //   initialize() *error* → fatal_disk_error;
                    //   a verify error is *recorded* in j.error (for
                    //   the alert) but not fatal;
                    //   no_recheck_incomplete_resume → no verdict;
                    //   no resume data → need_full_check iff files
                    //   exist; else verify verdict → need_full_check.
                    match st.initialize() {
                        Err(e) => {
                            j.ret = Status::from(crate::storage::disk_status::FATAL_DISK_ERROR);
                            e
                        }
                        Ok(init_status) => {
                            let empty = bitdaemon_common::bitfield::Bitfield::new();
                            let (pieces, seed) =
                                c.resume_data.as_ref().map_or((&empty, false), |r| {
                                    (
                                        &r.have_pieces,
                                        r.flags.contains(
                                            bitdaemon_common::flags::TorrentFlags::SEED_MODE,
                                        ),
                                    )
                                });
                            let links: Vec<std::path::PathBuf> =
                                c.links.iter().map(std::path::PathBuf::from).collect();
                            let (verify_ok, verify_err) =
                                st.verify_resume_data(pieces, seed, &links);
                            let status =
                                if self.no_recheck_incomplete_resume.load(Ordering::Relaxed) {
                                    0
                                } else {
                                    // `contains_resume_data` — a populated
                                    // have-pieces bitfield or seed_mode
                                    let has_resume = c.resume_data.as_ref().is_some_and(|r| {
                                        r.have_pieces.size() > 0
                                            || r.flags.contains(
                                                bitdaemon_common::flags::TorrentFlags::SEED_MODE,
                                            )
                                    });
                                    if !has_resume {
                                        // no resume data: still trigger a
                                        // full check if *any* files exist
                                        // (upstream ignores the stat error
                                        // here — `storage_error ignore`)
                                        if st.has_any_file().unwrap_or(false) {
                                            crate::storage::disk_status::NEED_FULL_CHECK
                                        } else {
                                            0
                                        }
                                    } else if verify_ok {
                                        0
                                    } else {
                                        crate::storage::disk_status::NEED_FULL_CHECK
                                    }
                                };
                            j.ret = Status::from(status | init_status);
                            verify_err.unwrap_or_default()
                        }
                    }
                }
                _ => StorageError::default(),
            };
            return false;
        }
        // hash jobs must not run while blocks of their piece are out
        // in an in-flight flush — the write isn't on disk yet and the
        // cache no longer holds it, so hashing now would read a stale
        // hole. Repost: the piece gate parks the job behind the write.
        match &j.action {
            JobKind::Hash(h) if piece_in_flight(e, j.storage, h.piece) => return true,
            JobKind::Hash2(h) if piece_in_flight(e, j.storage, h.piece) => return true,
            _ => {}
        }
        let st_guard = e.storage.read().unwrap_or_else(|p| p.into_inner());
        let st: &dyn DiskStorage = &**st_guard;
        let cache = &e.cache;
        j.error = match &mut j.action {
            JobKind::Read(r) => read_job(st, r, j.storage, cache),
            JobKind::PartialRead(r) => partial_read_job(st, r, j.storage, cache),
            // upstream `do_job(write)` is `TORRENT_ASSERT_FAIL` — a
            // write never reaches the pool: `gate_or_enqueue` folds
            // it into the cache at the post-fence consult point.
            // Reaching here is a bug, not a code path.
            JobKind::Write(_) => {
                debug_assert!(false, "write job reached perform_job");
                StorageError::new(
                    std::io::Error::other("write job reached perform_job"),
                    FileIndex(-1),
                    Operation::FileWrite,
                )
            }
            JobKind::Flush(f) => {
                // coalesced region write — contiguous `store_buffer`
                // blocks concatenated into one syscall (ADR-0012);
                // `Storage::write` already flattens internally.
                let flat_len: usize = f.bufs.iter().map(Vec::len).sum();
                let mut flat = Vec::with_capacity(flat_len);
                for b in &f.bufs {
                    flat.extend_from_slice(b);
                }
                // `no_cache` opens come from `PreadStorage::open_file`
                // (keyed on `disk_io_write_mode`, all opens) — the job
                // mode stays `write` like upstream `do_job(flush)`.
                let mode = OpenMode::WRITE;
                // upstream `j->flags` on the write job reach
                // `st->write`: `volatile_read` → `dont_need`,
                // `flush_piece` → `page_out` on the range (mmap
                // backend). Our coalesced flush aggregates the
                // parked writers' flags.
                let (volatile, flush) = f.writers.iter().fold((false, false), |(v, p), w| {
                    let JobKind::Write(w) = &w.action else {
                        return (v, p);
                    };
                    (v || w.volatile, p || w.force_flush)
                });
                let tw = std::time::Instant::now();
                // one physical write in flight per storage — see
                // `StorageEntry::write_serializer`.
                let r = {
                    let _serial = e.write_serializer.lock().unwrap_or_else(|p| p.into_inner());
                    st.write_with_hints(
                        &flat,
                        f.piece,
                        i64::from(f.first_block) * crate::disk_cache::DEFAULT_BLOCK_SIZE,
                        mode,
                        volatile,
                        flush,
                    )
                };
                self.write_us
                    .fetch_add(tw.elapsed().as_micros() as u64, Ordering::Relaxed);
                self.flush_bytes
                    .fetch_add(flat_len as u64, Ordering::Relaxed);
                match r {
                    Ok(_) => {
                        // upstream `do_write`: a landed write marks the
                        // storage for a deferred part-file metadata
                        // flush (~2 min, drained by the maintenance
                        // pass). Upstream marks only its uncached
                        // write path — our Flush *is* the physical
                        // write path, so marking here is the upstream
                        // intent (cached `writev` upstream misses it).
                        if !st.set_need_tick() {
                            self.need_tick
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .push_back((tw + std::time::Duration::from_secs(120), j.storage));
                        }
                        // `piece_hasher` role (ADR-0016): SHA-256 per
                        // flushed block, stored on the cache entry —
                        // `hash2` jobs reuse them without a re-read.
                        // Each block hashes only
                        // `min(buf, piece_size2 - block_offset)` bytes
                        // (upstream `v2_hash_queue_entry::hash_len`):
                        // a block reaching past the file boundary must
                        // not hash the pad bytes it also carries.
                        // Blocks entirely past `piece_size2` get no
                        // stored hash, like upstream never queueing
                        // them — and they always trail (a pad file
                        // ends on a piece boundary), so a prefix per
                        // span covers it.
                        if e.v2 {
                            let mut cache = e.cache.lock().unwrap_or_else(|p| p.into_inner());
                            let mut at = 0usize;
                            for &(piece, first, n) in &f.spans {
                                let piece_size2 = i64::from(st.files().piece_size2(piece));
                                let mut hashes: Vec<Sha256Hash> = Vec::with_capacity(n);
                                for (i, b) in f.bufs[at..at + n].iter().enumerate() {
                                    let block_off = (i64::from(first) + i as i64)
                                        * crate::disk_cache::DEFAULT_BLOCK_SIZE;
                                    let v2_len = (b.len() as i64).min(piece_size2 - block_off);
                                    if v2_len <= 0 {
                                        break;
                                    }
                                    let mut s = Sha256::new();
                                    s.update(&b[..usize::try_from(v2_len).unwrap_or(0)]);
                                    hashes.push(
                                        Sha256Hash::from_bytes(&s.finalize()).unwrap_or_default(),
                                    );
                                }
                                cache.set_block_hashes(
                                    (j.storage, piece),
                                    usize::try_from(first).unwrap_or(0),
                                    &hashes,
                                );
                                at += n;
                            }
                        }
                        StorageError::default()
                    }
                    Err(e) => e,
                }
            }
            JobKind::Hash(h) => hash_job(st, h, j.storage, cache),
            JobKind::Hash2(h) => hash2_job(st, h, j.storage, cache),
            JobKind::MoveStorage(m) => {
                // upstream `do_job(move_storage)`: ret = status_t, the
                // job's `path` out-param gets the *resulting* save
                // path (old path on failure), and the storage_error
                // rides along even for non-fatal verdicts like
                // `file_exist`.
                let (status, path, err) = st.move_storage(m.path.clone(), m.move_flags);
                j.ret = Status::from(status);
                m.path = path;
                err.unwrap_or_default()
            }
            JobKind::ReleaseFiles(_) => st.release_files().err().unwrap_or_default(),
            JobKind::DeleteFiles(d) => st
                .delete_files(u8::try_from(d.flags).unwrap_or(0))
                .err()
                .unwrap_or_default(),
            JobKind::CheckFastresume(_) => {
                // handled in the exclusive `&mut` arm above —
                // `initialize()` needs write access.
                StorageError::default()
            }
            JobKind::RemoveTorrent(_) => {
                // upstream `disk_io_thread::remove_torrent` — the
                // queued teardown: pending jobs already ran behind
                // the fence, drop the storage entry now so later
                // submissions fail `NotFound`.
                // Writers still parked in the cache are aborted first
                // — their handlers complete through the normal queue
                // (upstream `abort_jobs` on the storage's entries).
                {
                    let mut cache = e.cache.lock().unwrap_or_else(|p| p.into_inner());
                    cache.abort_all_writers();
                }
                self.storages
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&j.storage);
                // freed at job-execution time (behind the fence) so a
                // slot is never handed out while jobs still target it.
                self.slots
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .add(j.storage);
                StorageError::default()
            }
            JobKind::ClearPiece(c) => {
                // `disk_cache::free_piece` — the fence already flushed
                // the region; drop any residue (failed writes, partial
                // state) so stale blocks can't be re-read later.
                // Writers still parked on the piece land in `orphans`
                // and complete aborted downstream; freed buffers
                // release their `write_blocks` back-pressure slots.
                let freed = e
                    .cache
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .free_piece((j.storage, c.piece));
                if freed > 0 {
                    let level = self.write_blocks.fetch_sub(freed, Ordering::Relaxed) - freed;
                    self.back_pressure.check_buffer_level(level);
                }
                st.drop_precomputed_v2(c.piece);
                // upstream `clear_piece_jobs` — hash jobs parked on the
                // piece are stale after the clear; abort them so they
                // complete instead of hashing the zeroed residue.
                let drained: Vec<Box<DiskJob>> = {
                    let mut gate = e.piece_gate.lock().unwrap_or_else(|x| x.into_inner());
                    gate.get_mut(&c.piece.0)
                        .map(|g| g.blocked.drain(..).collect())
                        .unwrap_or_default()
                };
                for mut bj in drained {
                    crate::disk_completed::abort(&mut bj);
                    self.job_done(&mut bj);
                    self.add_completed_job(bj, Some(e));
                }
                StorageError::default()
            }
            JobKind::StopTorrent(_) => {
                // upstream `do_job(stop_torrent)`: the fence already
                // drained the write cache (`flush_storage`); the
                // opportunistic-hasher wait folds away (our hashing
                // only runs inside fenced jobs); then the storage's
                // pooled handles, stat cache and part-file metadata
                // are released so the stopped torrent holds nothing.
                st.release_files().err().unwrap_or_default()
            }
            JobKind::RenameFile(_) | JobKind::FilePriority(_) => StorageError::default(),
        };
        false
    }

    /// `update_stats` → pool counters snapshot.
    #[must_use]
    pub fn num_threads(&self) -> usize {
        self.pool.num_threads() + self.hash_pool.num_threads() + self.flush_pool.num_threads()
    }
}

/// `try_read` overlay — dirty blocks still parked in the write cache
/// are newer than what the file holds; copy them over the disk-read
/// range (upstream `mmap_disk_io::try_read` / `store_buffer::get`).
fn overlay_cached(
    cache: &Mutex<crate::disk_cache::DiskCache>,
    storage: StorageIndex,
    piece: PieceIndex,
    offset: i64,
    buf: &mut [u8],
) {
    const BS: i64 = crate::disk_cache::DEFAULT_BLOCK_SIZE;
    let g = cache.lock().unwrap_or_else(|e| e.into_inner());
    let loc = (storage, piece);
    let first_blk = offset.div_euclid(BS);
    let last_blk = (offset + buf.len() as i64 - 1).div_euclid(BS);
    for blk in first_blk..=last_blk {
        let Some(c) = g.get(loc, usize::try_from(blk).unwrap_or(0)) else {
            continue;
        };
        let blk_start = blk * BS;
        let lo = offset.max(blk_start);
        let hi = (offset + buf.len() as i64).min(blk_start + c.len() as i64);
        if lo < hi {
            buf[usize::try_from(lo - offset).unwrap_or(0)
                ..usize::try_from(hi - offset).unwrap_or(0)]
                .copy_from_slice(
                    &c[usize::try_from(lo - blk_start).unwrap_or(0)
                        ..usize::try_from(hi - blk_start).unwrap_or(0)],
                );
        }
    }
}

/// `true` while any block of `piece` has its buffer out inside an
/// in-flight flush job (`write_state == disk_buffer_ref` upstream).
/// A hash that observes this must repost through the piece gate — the
/// data is on neither disk nor cache until the write lands.
fn piece_in_flight(e: &StorageEntry, storage: StorageIndex, piece: PieceIndex) -> bool {
    let g = e.cache.lock().unwrap_or_else(|x| x.into_inner());
    g.piece((storage, piece))
        .is_some_and(|p| p.blocks.iter().any(|b| b.has_write_job && !b.flushed))
}

/// `file_mode_for_job` — read opens are `read_only` plus
/// `sequential_access` when the job asks for it (pread_disk_io.cpp:45).
/// `disk_io_read_mode` never produces open flags upstream — it sets
/// the `volatile_read` job flag (`advise_dont_need` after the read).
fn read_mode(sequential: bool) -> OpenMode {
    let mut m = OpenMode::READ_ONLY;
    if sequential {
        m |= OpenMode::SEQUENTIAL_ACCESS;
    }
    m
}

fn read_job(
    st: &dyn DiskStorage,
    r: &mut ReadJob,
    storage: StorageIndex,
    cache: &Mutex<crate::disk_cache::DiskCache>,
) -> StorageError {
    let Some(buf) = r.buf.data_mut() else {
        return StorageError::new(
            std::io::Error::from(std::io::ErrorKind::OutOfMemory),
            FileIndex(-1),
            Operation::AllocCachePiece,
        );
    };
    let want = usize::from(r.buffer_size).min(buf.len());
    match st.read_with_hints(
        &mut buf[..want],
        r.piece,
        i64::from(r.offset),
        read_mode(r.sequential),
        r.volatile,
        r.flush,
    ) {
        Ok(n) => {
            r.buffer_size = u16::try_from(n).unwrap_or(0);
            let n = usize::try_from(n).unwrap_or(0);
            overlay_cached(cache, storage, r.piece, i64::from(r.offset), &mut buf[..n]);
            StorageError::default()
        }
        Err(e) => e,
    }
}

/// `pread_disk_io::prepare_read` — the single point a read job
/// consults the write cache, run once the job is past the fence gate
/// so it always sees the current cache:
///
/// - fully cached request → buffer filled, returns true (the caller
///   completes the job — no disk I/O at all);
/// - unaligned request spanning two `default_block_size` blocks with
///   only one side cached → that side is copied into the buffer and
///   the job is turned into `JobKind::PartialRead` for the missing
///   side (returns false — the caller enqueues it);
/// - nothing cached → returns false (a plain `Read` executes;
///   `read_job` still overlays dirty blocks that land before it runs).
fn prepare_read(e: &StorageEntry, j: &mut DiskJob) -> bool {
    const BS: i32 = crate::disk_cache::DEFAULT_BLOCK_SIZE as i32;
    let JobKind::Read(r) = &mut j.action else {
        return false;
    };
    let loc = (j.storage, r.piece);
    // upstream derives the cache coordinates the same way: the cache
    // is indexed by `default_block_size` blocks, the request may be
    // unaligned.
    let block_offset = r.offset - r.offset % BS;
    let block_idx = usize::try_from(r.offset / BS).unwrap_or(0);
    let read_offset = r.offset - block_offset;
    let buf_len = r.buf.data().map_or(0, <[u8]>::len);
    let size = usize::from(r.buffer_size).min(buf_len);

    if read_offset + r.buffer_size as i32 > BS {
        // unaligned, spans two blocks — either, both or neither may
        // be in the cache (`disk_cache::get2`).
        let len1 = usize::try_from(BS - read_offset).unwrap_or(0);
        let ro = usize::try_from(read_offset).unwrap_or(0);
        let (have1, have2) = {
            let g = e.cache.lock().unwrap_or_else(|x| x.into_inner());
            let (b1, b2) = g.get2(loc, block_idx);
            // a cached entry shorter than the range it would serve
            // can't happen upstream (`TORRENT_ASSERT_VAL`) — treat a
            // short block as a miss rather than panic.
            let have1 = b1.is_some_and(|s| s.len() >= ro + len1);
            let have2 = b2.is_some_and(|s| s.len() >= size - len1);
            if !have1 && !have2 {
                return false;
            }
            let Some(buf) = r.buf.data_mut() else {
                // upstream: no_memory/`alloc_cache_piece`, fatal —
                // the job completes with the error.
                j.error = StorageError::new(
                    std::io::Error::from(std::io::ErrorKind::OutOfMemory),
                    FileIndex(-1),
                    Operation::AllocCachePiece,
                );
                return true;
            };
            if let Some(s) = b1 {
                if have1 {
                    buf[..len1].copy_from_slice(&s[ro..ro + len1]);
                }
            }
            if let Some(s) = b2 {
                if have2 {
                    buf[len1..size].copy_from_slice(&s[..size - len1]);
                }
            }
            (have1, have2)
        };
        if have1 && have2 {
            return true;
        }
        // one side served, one missing — turn the job into a
        // `partial_read` for the missing side. This is the only
        // place `PartialRead` jobs are created (upstream comment).
        let piece = r.piece;
        let offset = r.offset;
        let volatile = r.volatile;
        let flush = r.flush;
        let sequential = r.sequential;
        let handler = std::mem::replace(&mut r.handler, Box::new(|_, _| {}));
        let buf = std::mem::replace(
            &mut r.buf,
            crate::disk_buffer_pool::DiskBufferHolder::empty(),
        );
        let (buffer_offset, missing_size, missing_offset) = if have1 {
            // first side cached → read the remainder past `len1`.
            (
                u16::try_from(len1).unwrap_or(0),
                u16::try_from(size - len1).unwrap_or(0),
                block_offset + BS,
            )
        } else {
            // second side cached → read `len1` bytes at offset 0.
            (0, u16::try_from(len1).unwrap_or(0), offset)
        };
        j.action = JobKind::PartialRead(PartialReadJob {
            handler,
            buf,
            buffer_offset,
            buffer_size: missing_size,
            piece,
            offset: missing_offset,
            volatile,
            flush,
            sequential,
        });
        return false;
    }

    // an aligned request inside one block.
    let g = e.cache.lock().unwrap_or_else(|x| x.into_inner());
    let Some(c) = g.get(loc, block_idx) else {
        return false;
    };
    let ro = usize::try_from(read_offset).unwrap_or(0);
    if c.len() < ro + size {
        return false;
    }
    let Some(buf) = r.buf.data_mut() else {
        j.error = StorageError::new(
            std::io::Error::from(std::io::ErrorKind::OutOfMemory),
            FileIndex(-1),
            Operation::AllocCachePiece,
        );
        return true;
    };
    buf[..size].copy_from_slice(&c[ro..ro + size]);
    true
}

fn partial_read_job(
    st: &dyn DiskStorage,
    r: &mut PartialReadJob,
    storage: StorageIndex,
    cache: &Mutex<crate::disk_cache::DiskCache>,
) -> StorageError {
    // upstream `do_job(partial_read)`: read only the missing side —
    // into `buf` at `buffer_offset` — the cached side was prefilled
    // when the job was converted at the fence gate. The cache isn't
    // consulted again upstream; we do overlay the read range so a
    // dirty block that landed between the gate consult and this
    // read still wins over stale file bytes.
    let Some(buf) = r.buf.data_mut() else {
        return StorageError::new(
            std::io::Error::from(std::io::ErrorKind::OutOfMemory),
            FileIndex(-1),
            Operation::AllocCachePiece,
        );
    };
    let off = usize::from(r.buffer_offset);
    let want = usize::from(r.buffer_size).min(buf.len().saturating_sub(off));
    match st.read_with_hints(
        &mut buf[off..off + want],
        r.piece,
        i64::from(r.offset),
        read_mode(r.sequential),
        r.volatile,
        r.flush,
    ) {
        Ok(n) => {
            let n = usize::try_from(n).unwrap_or(0);
            overlay_cached(
                cache,
                storage,
                r.piece,
                i64::from(r.offset),
                &mut buf[off..off + n],
            );
            StorageError::default()
        }
        Err(e) => e,
    }
}

// upstream has no `do_job(write)` — `TORRENT_ASSERT_FAIL`; a write
// job's only destination is the cache insert at the post-fence
// consult point (`add_write_to_cache`).

fn hash_job(
    st: &dyn DiskStorage,
    h: &mut HashJob,
    storage: StorageIndex,
    cache: &Mutex<crate::disk_cache::DiskCache>,
) -> StorageError {
    // SHA-1 over the whole piece when `v1_hash` is set (upstream
    // `do_job(hash)`: v2-only hashing skips the SHA-1 pass entirely);
    // also fills per-block SHA-256 for the caller-provided
    // `block_hashes` span (v2). Only the spans absent from the write
    // cache are read from disk — a fully buffered piece needs no read
    // at all (upstream `partial_hash` hashes cached blocks in place
    // and issues the iovec only for the rest).
    let v2 = !h.block_hashes.is_empty();
    let len = i64::from(if h.v1_hash {
        st.files().piece_size(h.piece)
    } else if v2 {
        // `v1 ? piece_size : piece_size2` — a v2-only hash reads the
        // v2 span only (no pad bytes, no SHA-1).
        st.files().piece_size2(h.piece)
    } else {
        st.files().piece_size(h.piece)
    });
    let piece_len = usize::try_from(len).unwrap_or(0);
    const BS: usize = crate::disk_cache::DEFAULT_BLOCK_SIZE as usize;
    let nblocks = piece_len.div_ceil(BS);
    let loc = (storage, h.piece);
    // `fill_precomputed_v2` — slots still zero get any block hash the
    // storage already knows (upstream preserves non-zero entries and
    // only hashes what's still missing).
    for (i, bh) in h.block_hashes.iter_mut().enumerate() {
        if bh.is_all_zeros()
            && let Some(ph) =
                st.take_precomputed_v2_block(h.piece, i32::try_from(i).unwrap_or(i32::MAX))
        {
            *bh = ph;
        }
    }
    let needed: Vec<bool> = h
        .block_hashes
        .iter()
        .map(Sha256Hash::is_all_zeros)
        .collect();
    let mut bufs: Vec<Option<Vec<u8>>> = std::iter::repeat_with(|| None).take(nblocks).collect();
    // Blocks whose buffer was moved into an in-flight flush job are
    // neither in the cache nor on disk yet — the piece gate parked
    // this job behind the write (`perform_job` reposts when it still
    // observes them), so by the time we run, no block is in flight:
    // a `buf == None` block here is genuinely absent.
    {
        let g = cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(p) = g.piece(loc) {
            for (i, b) in p.blocks.iter().enumerate().take(nblocks) {
                if bufs[i].is_none()
                    && let Some(d) = b.data()
                {
                    bufs[i] = Some(d.to_vec());
                }
            }
        }
    }
    let mut buf = vec![0u8; piece_len];
    let mut r: Result<(), StorageError> = Ok(());
    let mut i = 0usize;
    while i < nblocks {
        if let Some(b) = bufs[i].take() {
            let off = i * BS;
            let n = b.len().min(piece_len - off);
            buf[off..off + n].copy_from_slice(&b[..n]);
            i += 1;
            continue;
        }
        let start = i;
        while i < nblocks && bufs[i].is_none() {
            i += 1;
        }
        let off = start * BS;
        let end = (i * BS).min(piece_len);
        if r.is_ok() {
            r = st
                .read_with_hints(
                    &mut buf[off..end],
                    h.piece,
                    i64::try_from(off).unwrap_or(0),
                    read_mode(h.sequential),
                    h.volatile,
                    h.flush,
                )
                .map(|_| ());
        }
    }
    let mut sha1 = Sha1::new();
    let mut block = Sha256::new();
    let mut in_block = 0usize;
    let blocks_wanted = v2;
    let mut block_idx = 0usize;
    // upstream `do_job(hash)` bounds each v2 block to
    // `min(BS, piece_size2 - offset)` — bytes past `piece_size2`
    // (pad region, next file's head) belong to a *different* v2
    // piece layer and must never enter this block's SHA-256.
    let piece_len2 = usize::try_from(i64::from(st.files().piece_size2(h.piece)))
        .unwrap_or(0)
        .min(piece_len);
    if h.v1_hash {
        sha1.update(&buf);
    }
    if blocks_wanted {
        let mut d = &buf[..piece_len2];
        while !d.is_empty() {
            let take = (DEFAULT_BLOCK_SIZE - in_block).min(d.len());
            // only hash blocks still missing their SHA-256 —
            // precomputed slots skip the hasher like upstream's
            // `v2_done` check (`block.update` is the cost).
            if needed.get(block_idx).copied().unwrap_or(false) {
                block.update(&d[..take]);
            }
            in_block += take;
            d = &d[take..];
            if in_block == DEFAULT_BLOCK_SIZE {
                if needed.get(block_idx).copied().unwrap_or(false)
                    && block_idx < h.block_hashes.len()
                {
                    h.block_hashes[block_idx] =
                        Sha256Hash::from_bytes(&block.finalize_reset()).unwrap_or_default();
                }
                block_idx += 1;
                in_block = 0;
            }
        }
    }
    if in_block > 0
        && needed.get(block_idx).copied().unwrap_or(false)
        && block_idx < h.block_hashes.len()
    {
        h.block_hashes[block_idx] =
            Sha256Hash::from_bytes(&block.finalize_reset()).unwrap_or_default();
    }
    match r {
        Ok(_) => {
            if h.v1_hash {
                h.piece_hash = Sha1Hash::from_bytes(&sha1.finalize()).unwrap_or_default();
            }
            // `store_precomputed_v2` — v2 block hashes are kept on the
            // storage so later `hash2` piece-layer requests answer
            // without a re-read (upstream does the same in the hasher).
            if blocks_wanted {
                for (i, bh) in h.block_hashes.iter().enumerate() {
                    if !bh.is_all_zeros() {
                        st.store_precomputed_v2(h.piece, i as i32, bh);
                    }
                }
            }
            StorageError::default()
        }
        Err(e) => e,
    }
}

fn hash2_job(
    st: &dyn DiskStorage,
    h: &mut Hash2Job,
    storage: StorageIndex,
    cache: &Mutex<crate::disk_cache::DiskCache>,
) -> StorageError {
    let mut sha = Sha256::new();
    let blk =
        usize::try_from(i64::from(h.offset) / crate::disk_cache::DEFAULT_BLOCK_SIZE).unwrap_or(0);
    // upstream `do_job(hash2)`: `len = min(BS, piece_size2 - offset)`
    // — the last v2 block stops at the file boundary; hashing a full
    // block would pull pad bytes (or the next file's head) into the
    // SHA-256. The write-cache path gets the same bound
    // (`disk_cache::hash2` uses `min(BS, piece_size2 - blk * BS)`).
    let piece_size2 = i64::from(st.files().piece_size2(h.piece));
    let len = crate::disk_cache::DEFAULT_BLOCK_SIZE
        .min(piece_size2 - i64::from(h.offset))
        .max(0);
    {
        let g = cache.lock().unwrap_or_else(|e| e.into_inner());
        let loc = (storage, h.piece);
        // `piece_hasher` result stored at flush — reuse, no re-read.
        if let Some(hash) = g.block_hash(loc, blk) {
            h.piece_hash2 = hash;
            return StorageError::default();
        }
        // block still parked in the write cache → hash it from RAM.
        if let Some(c) = g.get(loc, blk) {
            let take = usize::try_from(len).unwrap_or(0).min(c.len());
            sha.update(&c[..take]);
            h.piece_hash2 = Sha256Hash::from_bytes(&sha.finalize()).unwrap_or_default();
            return StorageError::default();
        }
    }
    // `take_precomputed_v2_block` — a piece hash already computed
    // this block's SHA-256; no disk read needed.
    if let Some(hash) = st.take_precomputed_v2_block(h.piece, i32::try_from(blk).unwrap_or(0)) {
        h.piece_hash2 = hash;
        return StorageError::default();
    }
    let r = st.hash_with_hints(
        &mut |d| sha.update(d),
        len,
        h.piece,
        i64::from(h.offset),
        read_mode(h.sequential),
        h.volatile,
        h.flush,
    );
    match r {
        Ok(_) => {
            h.piece_hash2 = Sha256Hash::from_bytes(&sha.finalize()).unwrap_or_default();
            StorageError::default()
        }
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// `disk_interface` implementation
// ---------------------------------------------------------------------------

use crate::back_pressure::DiskObserver;
use crate::disk_interface::{DiskIo, flags as io_flags};
use bitdaemon_common::types::{DownloadPriority, PeerRequest};
use bitdaemon_core::add_torrent_params::AddTorrentParams;
use std::path::PathBuf;

/// field — declared on `PreadDiskIo` struct below via include-style.
impl PreadDiskIo {
    fn mk_job(&self, action: JobKind) -> Box<DiskJob> {
        Box::new(DiskJob {
            storage: StorageIndex(u32::MAX),
            flags: 0,
            ret: 0,
            error: StorageError::default(),
            action,
        })
    }

    /// `settings_updated` — applies max_threads from the caller's
    /// settings snapshot (upstream reads settings_interface).
    pub fn set_max_threads(&self, n: i64) {
        self.pool.set_max_threads(n);
    }

    /// `settings_pack::hashing_threads` — size of the dedicated
    /// `hash`/`hash2` pool (upstream `m_hash_threads`, default 1).
    pub fn set_hash_threads(&self, n: i64) {
        self.hash_pool.set_max_threads(n);
    }

    /// `disk_io_thread_pool::reap_idle_threads` — upstream gives each
    /// pool a 60 s `deadline_timer`; the session drives all pools from
    /// `tick` at the same cadence.
    pub fn reap_idle_threads(&self) {
        self.pool.reap_idle_threads();
        self.hash_pool.reap_idle_threads();
        self.flush_pool.reap_idle_threads();
    }

    /// Not an upstream setting — size of the dedicated `flush_pool`. `0`
    /// routes `Flush` jobs back to the generic pool.
    pub fn set_flush_threads(&self, n: i64) {
        self.flush_pool.set_max_threads(n);
    }

    /// `settings_pack::enable_set_file_valid_data` /
    /// `no_atime_storage` — extra `OpenMode` bits pushed to every
    /// storage at `new_torrent`/`insert_storage` (ADR-0022). Upstream
    /// reads the settings live inside `open_file`, so we also
    /// propagate to storages already constructed.
    pub fn set_open_mode_extra(&self, mode: crate::open_mode::OpenMode) {
        self.open_mode_extra.store(mode.bits(), Ordering::Relaxed);
        for e in self
            .storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            e.storage
                .read()
                .unwrap_or_else(|e2| e2.into_inner())
                .set_open_mode_extra(mode);
        }
    }

    /// push a storage-scoped setting to every constructed storage —
    /// same live-update semantics as [`Self::set_open_mode_extra`]
    /// (upstream reads `settings_interface` inside `open_file`/
    /// `initialize`).
    fn fan_out_storages(&self, f: impl Fn(&dyn DiskStorage)) {
        for e in self
            .storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            f(&**e.storage.read().unwrap_or_else(|e2| e2.into_inner()));
        }
    }

    /// `settings_pack::mmap_file_size_cutoff` — 16 KiB blocks; files
    /// at or below it take the fd path under the mmap backend.
    pub fn set_mmap_file_size_cutoff(&self, blocks: i64) {
        self.mmap_file_size_cutoff.store(blocks, Ordering::Relaxed);
        self.fan_out_storages(|st| st.set_mmap_file_size_cutoff(blocks));
    }

    /// `settings_pack::disk_write_mode` (`mmap_write_mode_t` ordinal)
    /// — `always_pwrite`/`always_mmap_write`/`auto_mmap_write`.
    pub fn set_disk_write_mode(&self, mode: i32) {
        self.disk_write_mode
            .store(i64::from(mode), Ordering::Relaxed);
        self.fan_out_storages(|st| st.set_disk_write_mode(mode));
    }

    /// `settings_pack::disk_disable_copy_on_write` — `no_cow` opens
    /// (mmap backend upstream).
    pub fn set_no_cow(&self, v: bool) {
        self.disk_disable_cow.store(v, Ordering::Relaxed);
        self.fan_out_storages(|st| st.set_no_cow(v));
    }

    /// `settings_pack::disk_io_write_mode == write_through` —
    /// `sync_file` after each write lands (upstream keys this off
    /// the setting inside `pread_storage::write`).
    pub fn set_write_through(&self, v: bool) {
        self.write_through.store(v, Ordering::Relaxed);
        self.fan_out_storages(|st| st.set_write_through(v));
    }

    /// `settings_pack::close_file_interval` — seconds between
    /// `close_oldest` kicks in the maintenance pass.
    pub fn set_close_file_interval(&self, secs: i64) {
        self.close_file_interval.store(secs, Ordering::Relaxed);
    }

    /// storage-scoped settings pushed on construction — mirrors the
    /// `open_file`/`initialize` reads upstream makes from
    /// `settings_interface`.
    fn push_storage_settings(&self, storage: &dyn DiskStorage) {
        storage.set_open_mode_extra(self.open_mode_extra());
        storage.set_mmap_file_size_cutoff(self.mmap_file_size_cutoff.load(Ordering::Relaxed));
        storage.set_disk_write_mode(
            i32::try_from(self.disk_write_mode.load(Ordering::Relaxed)).unwrap_or(1),
        );
        storage.set_no_cow(self.disk_disable_cow.load(Ordering::Relaxed));
        storage.set_write_through(self.write_through.load(Ordering::Relaxed));
        storage.set_write_no_cache(self.write_no_cache.load(Ordering::Relaxed));
    }

    /// currently applied extra open flags.
    #[must_use]
    pub fn open_mode_extra(&self) -> crate::open_mode::OpenMode {
        crate::open_mode::OpenMode::from_bits(self.open_mode_extra.load(Ordering::Relaxed))
    }
}

impl DiskIo for PreadDiskIo {
    fn new_torrent(&self, p: &StorageParams<'_>, _torrent: Arc<dyn Send + Sync>) -> StorageIndex {
        PreadDiskIo::new_torrent(self, p)
    }

    fn remove_torrent(&self, idx: StorageIndex) {
        PreadDiskIo::remove_torrent(self, idx);
    }

    fn async_read(
        &self,
        storage: StorageIndex,
        r: PeerRequest,
        handler: Box<dyn FnOnce(crate::disk_buffer_pool::DiskBufferHolder, StorageError) + Send>,
        flags: u8,
    ) {
        // upstream `async_read` rejects empty/negative requests
        // inline (`errors::invalid_request`, `operation_t::file_read`)
        // before the job is even queued.
        if r.length <= 0 || r.start < 0 {
            handler(
                crate::disk_buffer_pool::DiskBufferHolder::empty(),
                StorageError::new(
                    std::io::Error::from(std::io::ErrorKind::InvalidInput),
                    FileIndex(-1),
                    Operation::FileRead,
                ),
            );
            return;
        }
        let buf = self.buffer_pool.allocate_buffer("read");
        let length = r.length;
        let j = self.mk_job(JobKind::Read(ReadJob {
            handler,
            buf,
            buffer_size: u16::try_from(length).unwrap_or(u16::MAX),
            piece: r.piece,
            offset: r.start,
            volatile: flags & io_flags::VOLATILE_READ != 0,
            flush: flags & io_flags::FLUSH_PIECE != 0,
            sequential: flags & io_flags::SEQUENTIAL_ACCESS != 0,
        }));
        self.add_job(storage, j);
    }

    fn async_write(
        &self,
        storage: StorageIndex,
        r: PeerRequest,
        buf: crate::disk_buffer_pool::DiskBufferHolder,
        o: Option<Arc<dyn DiskObserver>>,
        handler: Box<dyn FnOnce(StorageError) + Send>,
        flags: u8,
    ) -> bool {
        // upstream `pread_disk_io::async_write`: the write job carries
        // its buffer through the fence check — a raised fence parks it
        // (`is_blocked`) because inserting into the cache now would
        // race the exclusive job: `clear_piece` resets the entry,
        // storage-wide fences tear the storage down. Reposted writes
        // are inserted into the cache when the fence lowers. With no
        // fence the block lands in the write cache inline and the job
        // completes through the completed queue like everything else.
        let Some(entry) = self.storage(storage) else {
            handler(StorageError::new(
                std::io::Error::from(std::io::ErrorKind::NotFound),
                FileIndex(-1),
                Operation::FileWrite,
            ));
            return false;
        };
        // upstream asserts the request is one block-aligned block —
        // the wire layer guarantees it.
        debug_assert_eq!(r.start % crate::disk_cache::DEFAULT_BLOCK_SIZE as i32, 0);
        debug_assert!(r.length <= crate::disk_cache::DEFAULT_BLOCK_SIZE as i32);
        let mut j = self.mk_job(JobKind::Write(WriteJob {
            handler,
            buf,
            piece: r.piece,
            offset: r.start,
            buffer_size: u16::try_from(r.length).unwrap_or(u16::MAX),
            force_flush: flags & io_flags::FLUSH_PIECE == io_flags::FLUSH_PIECE,
            volatile: flags & io_flags::VOLATILE_READ != 0,
        }));
        j.storage = storage;
        // upstream `m_abort`: a write submitted during shutdown is
        // aborted directly — it can't be counted outstanding, its
        // completion would never drain.
        if self.abort.load(Ordering::Relaxed) {
            crate::disk_completed::abort(&mut j);
            self.completed.push(j);
            return false;
        }
        // counted at submit (upstream notes the parked path escapes
        // the cache's back-pressure — counting here closes that gap:
        // parked writes still push the level).
        let level = self.write_blocks.fetch_add(1, Ordering::Relaxed) + 1;
        let pressure = self.back_pressure.has_back_pressure(level, o);
        match entry.fence.is_blocked(j, &self.fence_counters) {
            // parked behind the fence — upstream still returns
            // `exceeded`: `account_pending_write` ran before
            // `is_blocked`, so the parked block counts toward
            // back-pressure like an inserted one.
            None => pressure,
            Some(j) => {
                // past the fence — `gate_or_enqueue` folds the write
                // into the cache (upstream `insert_write`), schedules
                // the flush passes, and the job completes through the
                // normal path (fence bookkeeping + counters).
                if let Some(done) = self.gate_or_enqueue(&entry, j) {
                    self.add_completed_job(done, Some(&entry));
                }
                pressure
            }
        }
    }

    fn async_hash(
        &self,
        storage: StorageIndex,
        piece: PieceIndex,
        mut v2: Vec<Sha256Hash>,
        flags: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha1Hash, Vec<Sha256Hash>, StorageError) + Send>,
    ) {
        // upstream wipes the v2 span on entry: non-zero entries are the
        // internal "already known" marker, a caller that chained pieces
        // through one buffer must not leak stale hashes in.
        for h in &mut v2 {
            *h = Sha256Hash::default();
        }
        let j = self.mk_job(JobKind::Hash(HashJob {
            handler,
            piece,
            block_hashes: v2,
            piece_hash: Sha1Hash::default(),
            v1_hash: flags & io_flags::V1_HASH != 0,
            volatile: flags & io_flags::VOLATILE_READ != 0,
            flush: flags & io_flags::FLUSH_PIECE != 0,
            sequential: flags & io_flags::SEQUENTIAL_ACCESS != 0,
        }));
        self.add_job(storage, j);
    }

    fn async_hash2(
        &self,
        storage: StorageIndex,
        piece: PieceIndex,
        offset: i32,
        flags: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha256Hash, StorageError) + Send>,
    ) {
        let j = self.mk_job(JobKind::Hash2(Hash2Job {
            handler,
            piece,
            offset,
            piece_hash2: Sha256Hash::default(),
            volatile: flags & io_flags::VOLATILE_READ != 0,
            flush: flags & io_flags::FLUSH_PIECE != 0,
            sequential: flags & io_flags::SEQUENTIAL_ACCESS != 0,
        }));
        self.add_job(storage, j);
    }

    fn async_move_storage(
        &self,
        storage: StorageIndex,
        path: PathBuf,
        flags: crate::storage::MoveFlags,
        handler: Box<dyn FnOnce(Status, PathBuf, StorageError) + Send>,
    ) {
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::MoveStorage(crate::disk_job::MoveStorageJob {
                handler,
                path,
                move_flags: flags,
            })),
        );
    }

    fn async_release_files(&self, storage: StorageIndex, handler: Box<dyn FnOnce() + Send>) {
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::ReleaseFiles(crate::disk_job::ReleaseFilesJob {
                handler,
            })),
        );
    }

    fn async_check_files(
        &self,
        storage: StorageIndex,
        resume_data: Option<Box<AddTorrentParams>>,
        links: Vec<String>,
        handler: Box<dyn FnOnce(Status, StorageError) + Send>,
    ) {
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::CheckFastresume(
                crate::disk_job::CheckFastresumeJob {
                    handler,
                    links,
                    resume_data,
                },
            )),
        );
    }

    fn async_stop_torrent(&self, storage: StorageIndex, handler: Box<dyn FnOnce() + Send>) {
        // upstream `abort_hash_jobs` — a stopped torrent doesn't need
        // its queued volatile (checking) hash jobs to grind on; mark
        // them aborted so they complete fast when dequeued.
        self.hash_pool.mark_aborted(|j| {
            j.storage == storage
                && match &j.action {
                    JobKind::Hash(h) => h.volatile,
                    JobKind::Hash2(h) => h.volatile,
                    _ => false,
                }
        });
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::StopTorrent(crate::disk_job::StopTorrentJob {
                handler,
            })),
        );
    }

    fn async_rename_file(
        &self,
        storage: StorageIndex,
        index: FileIndex,
        name: PathBuf,
        handler: Box<dyn FnOnce(PathBuf, FileIndex, StorageError) + Send>,
    ) {
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::RenameFile(crate::disk_job::RenameFileJob {
                handler,
                file_index: index,
                name,
            })),
        );
    }

    fn async_delete_files(
        &self,
        storage: StorageIndex,
        options: u8,
        handler: Box<dyn FnOnce(StorageError) + Send>,
    ) {
        // upstream `abort_hash_jobs` — a delete during a full recheck
        // shouldn't grind through the queued volatile hash jobs:
        // they're restartable, mark them aborted so they complete
        // fast when dequeued (hash pool only, like upstream's
        // `m_hash_threads.visit_jobs`).
        self.hash_pool.mark_aborted(|j| {
            j.storage == storage
                && match &j.action {
                    JobKind::Hash(h) => h.volatile,
                    JobKind::Hash2(h) => h.volatile,
                    _ => false,
                }
        });
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::DeleteFiles(crate::disk_job::DeleteFilesJob {
                handler,
                flags: i32::from(options),
            })),
        );
    }

    fn async_set_file_priority(
        &self,
        storage: StorageIndex,
        prio: Vec<DownloadPriority>,
        handler: Box<dyn FnOnce(StorageError, Vec<DownloadPriority>) + Send>,
    ) {
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::FilePriority(crate::disk_job::FilePriorityJob {
                handler,
                prio,
            })),
        );
    }

    fn async_clear_piece(
        &self,
        storage: StorageIndex,
        index: PieceIndex,
        handler: Box<dyn FnOnce(PieceIndex) + Send>,
    ) {
        self.add_fence_job(
            storage,
            self.mk_job(JobKind::ClearPiece(crate::disk_job::ClearPieceJob {
                handler,
                piece: index,
            })),
        );
    }

    fn get_status(&self, storage: StorageIndex) -> Vec<crate::file_pool::OpenFileState> {
        // upstream asks the backend's own pool — `m_file_pool` is the
        // fd pool under pread and the view pool under mmap; the
        // storage reports whichever pooled resource it holds.
        self.storage(storage)
            .map(|e| {
                e.storage
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .file_status()
            })
            .unwrap_or_default()
    }

    fn abort(&self, wait: bool) {
        self.abort_jobs(wait);
    }

    fn call_job_handlers(&self) -> usize {
        self.completed.call_job_handlers()
    }

    fn set_max_queued_bytes(&self, bytes: i64) {
        PreadDiskIo::set_max_queued_bytes(self, bytes);
    }

    /// `settings_pack::file_pool_size` — `m_file_pool.resize` inside
    /// upstream `settings_updated`; shrinking evicts LRU handles.
    fn set_file_pool_size(&self, n: i32) {
        self.file_pool.resize(n.max(1));
    }

    fn set_write_no_cache(&self, v: bool) {
        self.write_no_cache.store(v, Ordering::Relaxed);
        self.fan_out_storages(|st| st.set_write_no_cache(v));
    }

    fn set_write_through(&self, v: bool) {
        PreadDiskIo::set_write_through(self, v);
    }

    fn set_cache_expiry(&self, secs: u64) {
        PreadDiskIo::set_cache_expiry(self, secs);
    }

    fn set_no_recheck_incomplete_resume(&self, v: bool) {
        PreadDiskIo::set_no_recheck_incomplete_resume(self, v);
    }

    fn set_open_mode_extra(&self, mode: crate::open_mode::OpenMode) {
        PreadDiskIo::set_open_mode_extra(self, mode);
    }

    fn set_mmap_file_size_cutoff(&self, blocks: i64) {
        PreadDiskIo::set_mmap_file_size_cutoff(self, blocks);
    }

    fn set_disk_write_mode(&self, mode: i32) {
        PreadDiskIo::set_disk_write_mode(self, mode);
    }

    fn set_no_cow(&self, v: bool) {
        PreadDiskIo::set_no_cow(self, v);
    }

    fn set_close_file_interval(&self, secs: i64) {
        PreadDiskIo::set_close_file_interval(self, secs);
    }

    fn set_max_threads(&self, n: i64) {
        PreadDiskIo::set_max_threads(self, n);
    }

    fn set_hash_threads(&self, n: i64) {
        PreadDiskIo::set_hash_threads(self, n);
    }

    fn reap_idle_threads(&self) {
        PreadDiskIo::reap_idle_threads(self);
    }

    fn flush_expired(&self) {
        PreadDiskIo::flush_expired(self);
    }

    fn write_queue_exceeded(&self) -> bool {
        self.back_pressure.exceeded()
    }

    fn flush_error_count(&self) -> i64 {
        self.flush_errors.load(Ordering::Relaxed)
    }

    fn flush_perf(&self) -> (u64, u64, u64, u64, u64, u64) {
        #[cfg(windows)]
        let os = crate::file::win32::OS_WRITE_US.load(Ordering::Relaxed);
        #[cfg(not(windows))]
        let os = 0;
        (
            self.flush_us.load(Ordering::Relaxed),
            self.flush_n.load(Ordering::Relaxed),
            self.complete_us.load(Ordering::Relaxed),
            self.write_us.load(Ordering::Relaxed),
            self.flush_bytes.load(Ordering::Relaxed),
            os,
        )
    }

    /// `pread_disk_io::update_stats_counters` (ADR-0017).
    fn queue_stats(&self) -> crate::disk_interface::DiskQueueStats {
        let mut s = crate::disk_interface::DiskQueueStats {
            write_blocks: self.write_blocks.load(Ordering::Relaxed),
            threads: self.pool.num_threads()
                + self.hash_pool.num_threads()
                + self.flush_pool.num_threads(),
            queued_disk_jobs: self.pool.queue_size(),
            blocked_disk_jobs: self
                .fence_counters
                .blocked_disk_jobs
                .load(Ordering::Relaxed),
            disk_blocks_in_use: self.buffer_pool.in_use(),
            queued_write_bytes: self.write_blocks.load(Ordering::Relaxed)
                * crate::disk_cache::DEFAULT_BLOCK_SIZE,
            num_blocks_written: self.num_blocks_written.load(Ordering::Relaxed),
            num_blocks_read: self.num_blocks_read.load(Ordering::Relaxed),
            num_blocks_hashed: self.num_blocks_hashed.load(Ordering::Relaxed),
            num_write_ops: self.num_write_ops.load(Ordering::Relaxed),
            num_read_ops: self.num_read_ops.load(Ordering::Relaxed),
            ..Default::default()
        };
        let (hits, misses, stalls, races, num_files) = self.file_pool.stats_counters();
        s.file_pool_hits = hits;
        s.file_pool_misses = misses;
        s.file_pool_thread_stall = stalls;
        s.file_pool_race = races;
        s.file_pool_size = num_files;
        // `num_fenced_read + get_type()` per-kind gauges — fence jobs
        // still inside the fence lifetime (submit → completion).
        for (i, n) in s.fenced_jobs.iter_mut().enumerate() {
            *n = self.fence_counters.fenced_jobs[i].load(Ordering::Relaxed);
        }
        // per-kind counts maintained on push/pop — walking the queue
        // here held the worker lock for O(queue) every stats tick and
        // stalled all disk threads mid-burst.
        (s.queued_writes, s.queued_hashes, s.queued_other) = self.pool.kind_counts();
        // `Flush` jobs sitting in the dedicated pool's queue are
        // still "other" from the display's point of view.
        s.queued_other += self.flush_pool.queue_size();
        for e in self
            .storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            let gate = e.piece_gate.lock().unwrap_or_else(|g| g.into_inner());
            s.parked_hashes += gate.values().map(|g| g.blocked.len()).sum::<usize>();
            let cache = e.cache.lock().unwrap_or_else(|c| c.into_inner());
            s.cached_blocks += cache.num_cached_blocks;
            s.num_unhashed += cache.num_unhashed;
        }
        s
    }

    fn submit_jobs(&self) {
        self.pool.submit_jobs();
    }

    fn settings_updated(&self) {}
}
