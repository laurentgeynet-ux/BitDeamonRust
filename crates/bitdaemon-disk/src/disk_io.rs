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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
    /// `disk_io_read_mode=disable_os_cache` (ADR-0015).
    read_no_cache: AtomicBool,
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
    /// extra `OpenMode` bits pushed to every storage at
    /// `new_torrent` (upstream `open_file` reads
    /// `enable_set_file_valid_data`/`no_atime_storage` from
    /// `settings_interface` — ADR-0022).
    open_mode_extra: std::sync::atomic::AtomicU32,
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
            write_no_cache: AtomicBool::new(false),
            read_no_cache: AtomicBool::new(false),
            cache_expiry: std::sync::atomic::AtomicU64::new(60),
            hash_pool,
            open_mode_extra: std::sync::atomic::AtomicU32::new(0),
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
            for ((_s, piece), first, bufs) in runs {
                self.add_job(
                    idx,
                    self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                        piece,
                        first_block: first,
                        bufs,
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
            for ((_s, piece), first, bufs) in runs {
                self.add_job(
                    idx,
                    self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                        piece,
                        first_block: first,
                        bufs,
                    })),
                );
            }
        }
    }

    /// `settings_pack::cache_expiry` setter (seconds).
    pub fn set_cache_expiry(&self, secs: u64) {
        self.cache_expiry.store(secs, Ordering::Relaxed);
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
        storage.set_open_mode_extra(self.open_mode_extra());
        g.insert(
            idx,
            Arc::new(StorageEntry {
                storage: RwLock::new(storage),
                fence: DiskJobFence::new(),
                piece_gate: Mutex::new(HashMap::new()),
                cache: Mutex::new(crate::disk_cache::DiskCache::default()),
                v1: params.v1,
                v2: params.v2,
            }),
        );
        idx
    }

    /// Register a pre-built storage (tests / custom wiring).
    pub fn insert_storage(&self, idx: StorageIndex, storage: Box<dyn DiskStorage>) {
        storage.set_storage_index(idx);
        storage.set_open_mode_extra(self.open_mode_extra());
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
        // hash jobs parked behind the piece gate never reached the
        // pool — drain them too so their completions still post.
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
        }
        for j in &mut drained {
            crate::disk_completed::abort(j);
            self.job_done(j);
        }
        self.pool.abort(wait);
        self.hash_pool.abort(wait);
        self.completed.append(drained);
    }

    /// A job reached the completed queue — release its write-block
    /// slot and release observers when the level drains below the
    /// low watermark (upstream `disk_cache::check_buffer_level` on
    /// every erase).
    fn job_done(&self, j: &DiskJob) {
        match &j.action {
            JobKind::Write(_) => {
                self.num_blocks_written.fetch_add(1, Ordering::Relaxed);
                self.num_write_ops.fetch_add(1, Ordering::Relaxed);
                let level = self.write_blocks.fetch_sub(1, Ordering::Relaxed) - 1;
                self.back_pressure.check_buffer_level(level);
            }
            JobKind::Flush(f) => {
                // a completed (or aborted) flush retires its cached
                // blocks and drains the buffer level (ADR-0012/0013).
                if let Some(e) = self.storage(j.storage) {
                    e.cache
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .run_flushed(
                            (j.storage, f.piece),
                            usize::try_from(f.first_block).unwrap_or(0),
                            f.bufs.len(),
                        );
                }
                self.num_blocks_written
                    .fetch_add(f.bufs.len() as i64, Ordering::Relaxed);
                self.num_write_ops.fetch_add(1, Ordering::Relaxed);
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
                    e.cache
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .hash_returned((j.storage, h.piece));
                }
            }
            JobKind::Hash2(h) => {
                self.num_blocks_hashed.fetch_add(1, Ordering::Relaxed);
                if let Some(e) = self.storage(j.storage) {
                    e.cache
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .hash_returned((j.storage, h.piece));
                }
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
            self.job_done(&j);
            self.completed.push(j);
            return;
        }
        let mut j = j;
        j.storage = idx;
        if let Some(entry) = self.storage(idx) {
            if let Some(j) = entry.fence.is_blocked(j, &self.fence_counters) {
                self.gate_or_enqueue(&entry, j);
            }
        } else {
            // no storage — job is stale; complete it aborted
            let mut j = j;
            crate::disk_completed::abort(&mut j);
            self.job_done(&j);
            self.completed.push(j);
        }
    }

    /// `add_fence_job(j)` — exclusive path.
    pub fn add_fence_job(&self, idx: StorageIndex, j: Box<DiskJob>) {
        if self.abort.load(Ordering::Relaxed) {
            let mut j = j;
            crate::disk_completed::abort(&mut j);
            self.job_done(&j);
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
                for ((_s, piece), first, bufs) in runs {
                    self.add_job(
                        idx,
                        self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                            piece,
                            first_block: first,
                            bufs,
                        })),
                    );
                }
                let mut j = j;
                j.storage = idx;
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
                self.job_done(&j);
                self.completed.push(j);
            }
        }
    }

    /// Piece-gate routing after the fence check: writes register
    /// against their piece; hash jobs for a piece with in-flight
    /// writes queue behind them instead of racing the file.
    fn gate_or_enqueue(&self, entry: &StorageEntry, j: Box<DiskJob>) {
        let piece = match &j.action {
            JobKind::Write(w) => Some((w.piece.0, true)),
            JobKind::Flush(f) => Some((f.piece.0, true)),
            JobKind::Hash(h) => Some((h.piece.0, false)),
            JobKind::Hash2(h) => Some((h.piece.0, false)),
            _ => None,
        };
        let Some((p, is_write)) = piece else {
            self.enqueue(j);
            return;
        };
        let mut gate = entry.piece_gate.lock().unwrap_or_else(|e| e.into_inner());
        if is_write {
            gate.entry(p).or_default().writes += 1;
            drop(gate);
            self.enqueue(j);
        } else if gate.get(&p).is_some_and(|g| g.writes > 0) {
            tracing::debug!(piece = p, "piece_gate: hash parked behind writes");
            gate.entry(p).or_default().blocked.push_back(j);
        } else {
            drop(gate);
            self.enqueue(j);
        }
    }

    /// Pool routing — upstream `job::thread_type()`: `hash`/`hash2`
    /// jobs go to `m_hash_threads` when it has threads, everything
    /// else to `m_generic_threads`.
    fn job_pool(&self, j: &DiskJob) -> &Arc<DiskIoThreadPool> {
        if matches!(j.action, JobKind::Hash(_) | JobKind::Hash2(_))
            && self.hash_pool.max_threads() > 0
        {
            &self.hash_pool
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
    fn immediate_execute(&self, pool: &Arc<DiskIoThreadPool>) {
        loop {
            let Some(j) = pool.pop_front() else {
                break;
            };
            self.execute_and_complete(j);
        }
    }

    /// worker loop (`thread_fun`) — wait, pop, perform, complete.
    fn worker_loop(&self, pool: &Arc<DiskIoThreadPool>) {
        loop {
            match pool.wait_for_job() {
                WaitResult::NewJob => {
                    let Some(j) = pool.pop_front() else { continue };
                    self.execute_and_complete(j);
                }
                WaitResult::Interrupt => continue,
                WaitResult::ExitThread => {
                    if pool.try_thread_exit() {
                        return;
                    }
                }
            }
        }
    }

    fn execute_and_complete(&self, mut j: Box<DiskJob>) {
        let entry = self.storage(j.storage);
        self.perform_job(&mut j, entry.as_deref());
        self.add_completed_job(j, entry.as_deref());
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
        // matching upstream ordering.
        let mut done = JobQueue::new();
        for mut j in jobs.drain(..) {
            if let Some(e) = entry {
                // piece gate: a completed write (or coalesced flush)
                // may release hash jobs queued behind it.
                let gate_piece = match &j.action {
                    JobKind::Write(w) => Some(w.piece.0),
                    JobKind::Flush(f) => Some(f.piece.0),
                    _ => None,
                };
                if let Some(p) = gate_piece {
                    let mut released = VecDeque::new();
                    {
                        let mut gate = e.piece_gate.lock().unwrap_or_else(|p| p.into_inner());
                        if let Some(g) = gate.get_mut(&p) {
                            g.writes -= 1;
                            if g.writes <= 0 {
                                released.append(&mut g.blocked);
                                gate.remove(&p);
                            }
                        }
                    }
                    for bj in released {
                        // parked jobs already passed the fence once —
                        // they carry IN_PROGRESS and stay counted in
                        // `outstanding`, so they go straight to the pool;
                        // re-entering add_job would double-account them
                        // (and trip the fence's debug_assert).
                        self.enqueue(bj);
                    }
                }
                let mut reposted = Vec::new();
                e.fence.job_complete(&mut j, &mut |bj| reposted.push(bj));
                for bj in reposted {
                    let pool = self.job_pool(&bj);
                    pool.push_back(bj);
                    pool.submit_jobs();
                }
            }
            self.job_done(&j);
            done.push_back(j);
        }
        self.completed.append(done);
        let _ = &mut jobs;
    }

    /// `perform_job` — execute on the disk thread.
    fn perform_job(&self, j: &mut DiskJob, entry: Option<&StorageEntry>) {
        if j.flags & job_flags::ABORTED == job_flags::ABORTED {
            j.error = StorageError::new(
                std::io::Error::from(std::io::ErrorKind::Interrupted),
                FileIndex(-1),
                Operation::Unknown,
            );
            return;
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
            return;
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
                        Err(e) => e,
                    }
                }
                JobKind::CheckFastresume(c) => {
                    // `storage_holder::initialize` — deferred setup
                    // runs before the resume verdict so status bits
                    // (`oversized_file`…) ride back with `ret`.
                    // `ret` layout: bit 0 = resume accepted,
                    // bits 8..16 = `disk_status` flags from
                    // `initialize()`.
                    let init_status = st.initialize().unwrap_or(0);
                    let empty = bitdaemon_common::bitfield::Bitfield::new();
                    let (pieces, seed) = c.resume_data.as_ref().map_or((&empty, false), |r| {
                        (
                            &r.have_pieces,
                            r.flags
                                .contains(bitdaemon_common::flags::TorrentFlags::SEED_MODE),
                        )
                    });
                    match st.verify_resume_data(
                        pieces,
                        seed,
                        &c.links
                            .iter()
                            .map(std::path::PathBuf::from)
                            .collect::<Vec<_>>(),
                    ) {
                        Ok(ok) => {
                            // upstream `do_job(check_fastresume)`: without
                            // resume data (`contains_resume_data`) the verdict
                            // is `need_full_check` whenever files exist on
                            // disk — an empty `have` bitfield is not a
                            // trustworthy "accepted" answer, it just means
                            // "nothing to verify".
                            let has_resume = c.resume_data.as_ref().is_some_and(|r| {
                                r.have_pieces.size() > 0
                                    || r.flags
                                        .contains(bitdaemon_common::flags::TorrentFlags::SEED_MODE)
                            });
                            let accepted = if has_resume {
                                ok
                            } else {
                                !st.has_any_file().unwrap_or(false)
                            };
                            j.ret = Status::from(accepted) | (Status::from(init_status) << 8);
                            StorageError::default()
                        }
                        Err(e) => e,
                    }
                }
                _ => StorageError::default(),
            };
            return;
        }
        let st_guard = e.storage.read().unwrap_or_else(|p| p.into_inner());
        let st: &dyn DiskStorage = &**st_guard;
        let cache = &e.cache;
        j.error = match &mut j.action {
            JobKind::Read(r) => read_job(
                st,
                r,
                j.storage,
                cache,
                self.read_no_cache.load(Ordering::Relaxed),
            ),
            JobKind::PartialRead(r) => {
                partial_read_job(st, r, self.read_no_cache.load(Ordering::Relaxed))
            }
            JobKind::Write(w) => write_job(st, w, self.write_no_cache.load(Ordering::Relaxed)),
            JobKind::Flush(f) => {
                // coalesced region write — contiguous `store_buffer`
                // blocks concatenated into one syscall (ADR-0012);
                // `Storage::write` already flattens internally.
                let flat_len: usize = f.bufs.iter().map(Vec::len).sum();
                let mut flat = Vec::with_capacity(flat_len);
                for b in &f.bufs {
                    flat.extend_from_slice(b);
                }
                let mode = if self.write_no_cache.load(Ordering::Relaxed) {
                    OpenMode::WRITE | OpenMode::NO_CACHE
                } else {
                    OpenMode::WRITE
                };
                match st.write(
                    &flat,
                    f.piece,
                    i64::from(f.first_block) * crate::disk_cache::DEFAULT_BLOCK_SIZE,
                    mode,
                ) {
                    Ok(_) => {
                        // `piece_hasher` role (ADR-0016): SHA-256 per
                        // flushed block, stored on the cache entry —
                        // `hash2` jobs reuse them without a re-read.
                        if e.v2 {
                            let hashes: Vec<Sha256Hash> = f
                                .bufs
                                .iter()
                                .map(|b| {
                                    let mut s = Sha256::new();
                                    s.update(b);
                                    Sha256Hash::from_bytes(&s.finalize()).unwrap_or_default()
                                })
                                .collect();
                            e.cache
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .set_block_hashes(
                                    (j.storage, f.piece),
                                    usize::try_from(f.first_block).unwrap_or(0),
                                    &hashes,
                                );
                        }
                        StorageError::default()
                    }
                    Err(e) => e,
                }
            }
            JobKind::Hash(h) => hash_job(
                st,
                h,
                j.storage,
                cache,
                self.read_no_cache.load(Ordering::Relaxed),
            ),
            JobKind::Hash2(h) => hash2_job(
                st,
                h,
                j.storage,
                cache,
                self.read_no_cache.load(Ordering::Relaxed),
            ),
            JobKind::MoveStorage(m) => match st.move_storage(m.path.clone(), m.move_flags) {
                Ok((status, path)) => {
                    j.ret = Status::from(status);
                    m.path = path;
                    StorageError::default()
                }
                Err(e) => e,
            },
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
                e.cache
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .free_piece((j.storage, c.piece));
                st.drop_precomputed_v2(c.piece);
                StorageError::default()
            }
            JobKind::StopTorrent(_) | JobKind::RenameFile(_) | JobKind::FilePriority(_) => {
                StorageError::default()
            }
        };
    }

    /// `update_stats` → pool counters snapshot.
    #[must_use]
    pub fn num_threads(&self) -> usize {
        self.pool.num_threads() + self.hash_pool.num_threads()
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

/// `disk_io_read_mode=disable_os_cache` → `open_mode::no_cache` on
/// read opens (ADR-0015); same flag upstream ORs in.
fn read_mode(no_cache: bool) -> OpenMode {
    if no_cache {
        OpenMode::READ_ONLY | OpenMode::NO_CACHE
    } else {
        OpenMode::READ_ONLY
    }
}

fn read_job(
    st: &dyn DiskStorage,
    r: &mut ReadJob,
    storage: StorageIndex,
    cache: &Mutex<crate::disk_cache::DiskCache>,
    no_cache: bool,
) -> StorageError {
    let Some(buf) = r.buf.data_mut() else {
        return StorageError::new(
            std::io::Error::from(std::io::ErrorKind::OutOfMemory),
            FileIndex(-1),
            Operation::AllocCachePiece,
        );
    };
    let want = usize::from(r.buffer_size).min(buf.len());
    match st.read(
        &mut buf[..want],
        r.piece,
        i64::from(r.offset),
        read_mode(no_cache),
        r.volatile,
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

fn partial_read_job(st: &dyn DiskStorage, r: &mut PartialReadJob, no_cache: bool) -> StorageError {
    // unaligned read into buf at buffer_offset (upstream semantics)
    let Some(buf) = r.buf.data_mut() else {
        return StorageError::new(
            std::io::Error::from(std::io::ErrorKind::OutOfMemory),
            FileIndex(-1),
            Operation::AllocCachePiece,
        );
    };
    let off = usize::from(r.buffer_offset);
    let want = usize::from(r.buffer_size).min(buf.len().saturating_sub(off));
    match st.read(
        &mut buf[off..off + want],
        r.piece,
        i64::from(r.offset),
        read_mode(no_cache),
        false,
    ) {
        Ok(_) => StorageError::default(),
        Err(e) => e,
    }
}

fn write_job(st: &dyn DiskStorage, w: &mut WriteJob, no_cache: bool) -> StorageError {
    let Some(buf) = w.buf.data() else {
        return StorageError::new(
            std::io::Error::from(std::io::ErrorKind::InvalidInput),
            FileIndex(-1),
            Operation::FileWrite,
        );
    };
    let want = usize::from(w.buffer_size).min(buf.len());
    // upstream `mmap_storage`: `disk_io_write_mode=disable_os_cache`
    // ORs `open_mode::no_cache` into the file open flags.
    let mode = if no_cache {
        OpenMode::WRITE | OpenMode::NO_CACHE
    } else {
        OpenMode::WRITE
    };
    match st.write(&buf[..want], w.piece, i64::from(w.offset), mode) {
        Ok(n) => {
            w.buffer_size = u16::try_from(n).unwrap_or(0);
            StorageError::default()
        }
        Err(e) => e,
    }
}

fn hash_job(
    st: &dyn DiskStorage,
    h: &mut HashJob,
    storage: StorageIndex,
    cache: &Mutex<crate::disk_cache::DiskCache>,
    no_cache: bool,
) -> StorageError {
    // SHA-1 over the whole piece; also fills per-block SHA-256 when the
    // caller provided a block_hashes span (v2) — upstream hash job.
    // The piece bytes are materialized once so dirty `store_buffer`
    // blocks can be overlaid before hashing (ADR-0014) — upstream
    // hashes through the cache identically.
    let len = i64::from(st.files().piece_size(h.piece));
    let mut buf = vec![0u8; usize::try_from(len).unwrap_or(0)];
    let r = st
        .read(&mut buf, h.piece, 0, read_mode(no_cache), false)
        .map(|_| ());
    if r.is_ok() {
        overlay_cached(cache, storage, h.piece, 0, &mut buf);
    }
    let mut sha1 = Sha1::new();
    let mut block = Sha256::new();
    let mut in_block = 0usize;
    let blocks_wanted = !h.block_hashes.is_empty();
    let mut block_idx = 0usize;
    let mut feed = |data: &[u8]| {
        sha1.update(data);
        if blocks_wanted {
            let mut d = data;
            while !d.is_empty() {
                let take = (DEFAULT_BLOCK_SIZE - in_block).min(d.len());
                block.update(&d[..take]);
                in_block += take;
                d = &d[take..];
                if in_block == DEFAULT_BLOCK_SIZE {
                    if block_idx < h.block_hashes.len() {
                        h.block_hashes[block_idx] =
                            Sha256Hash::from_bytes(&block.finalize_reset()).unwrap_or_default();
                    }
                    block_idx += 1;
                    in_block = 0;
                }
            }
        }
    };
    feed(&buf);
    if in_block > 0 && block_idx < h.block_hashes.len() {
        h.block_hashes[block_idx] =
            Sha256Hash::from_bytes(&block.finalize_reset()).unwrap_or_default();
    }
    match r {
        Ok(_) => {
            h.piece_hash = Sha1Hash::from_bytes(&sha1.finalize()).unwrap_or_default();
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
    no_cache: bool,
) -> StorageError {
    let mut sha = Sha256::new();
    let blk =
        usize::try_from(i64::from(h.offset) / crate::disk_cache::DEFAULT_BLOCK_SIZE).unwrap_or(0);
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
            sha.update(c);
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
    let r = st.hash_with(
        &mut |d| sha.update(d),
        i64::try_from(DEFAULT_BLOCK_SIZE).unwrap_or(0),
        h.piece,
        i64::from(h.offset),
        read_mode(no_cache),
        false,
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

    /// `settings_pack::enable_set_file_valid_data` /
    /// `no_atime_storage` — extra `OpenMode` bits pushed to every
    /// storage at `new_torrent`/`insert_storage` (ADR-0022).
    pub fn set_open_mode_extra(&self, mode: crate::open_mode::OpenMode) {
        self.open_mode_extra.store(mode.bits(), Ordering::Relaxed);
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
        let buf = self.buffer_pool.allocate_buffer("read");
        let length = r.length;
        let j = self.mk_job(JobKind::Read(ReadJob {
            handler,
            buf,
            buffer_size: u16::try_from(length).unwrap_or(u16::MAX),
            piece: r.piece,
            offset: r.start,
            volatile: flags & io_flags::VOLATILE_READ != 0,
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
        _flags: u8,
    ) -> bool {
        // upstream `mmap_disk_io::async_write`: the block lands in the
        // per-storage cache (`store_buffer`/`disk_cache`) and the
        // caller completes once it's safely held — the *flush* to
        // files is a separate, coalesced mechanism (ADR-0012). The
        // bool return only signals the peer to stop requesting until
        // the cache drains below the low watermark.
        let Some(entry) = self.storage(storage) else {
            handler(StorageError::new(
                std::io::Error::from(std::io::ErrorKind::NotFound),
                FileIndex(-1),
                Operation::FileWrite,
            ));
            return false;
        };
        let (piece_size, piece_size2) = {
            let st = entry.storage.read().unwrap_or_else(|e| e.into_inner());
            let fs = st.files();
            (
                i64::from(fs.piece_size(r.piece)),
                i64::from(fs.piece_size2(r.piece)),
            )
        };
        let block_idx =
            usize::try_from(r.start / crate::disk_cache::DEFAULT_BLOCK_SIZE as i32).unwrap_or(0);
        let data = {
            let b = buf.data();
            let want = usize::try_from(r.length).unwrap_or(0);
            let n = want.min(b.map_or(0, |b| b.len()));
            b.map_or_else(Vec::new, |b| b[..n].to_vec())
        };
        let level = self.write_blocks.fetch_add(1, Ordering::Relaxed) + 1;
        let pressure = self.back_pressure.has_back_pressure(level, o);
        let mut cache = entry.cache.lock().unwrap_or_else(|e| e.into_inner());
        let loc = (storage, r.piece);
        cache.insert(
            loc,
            block_idx,
            Some(data),
            false,
            crate::disk_cache::PieceParams {
                piece_size,
                piece_size2,
                v1: entry.v1,
                v2: entry.v2,
            },
        );
        // the upstream write "job" is complete once the block sits in
        // the cache — clear `has_write_job` so the flush can take it.
        cache.write_job_done(loc, block_idx, false);
        // upstream `try_flush_blocks`: a piece whose blocks all arrived
        // flushes as one contiguous `writev`; over-limit caches flush
        // whatever contiguous dirty run exists.
        let piece_full = cache
            .piece(loc)
            .is_some_and(|p| p.blocks.iter().all(|b| b.buf.is_some() || b.flushed));
        // upstream `check_buffer_level`/`try_flush_blocks`: above the
        // high watermark (or while the exceeded flag is sticky) keep
        // flushing runs until the cache drains to the low watermark;
        // a complete piece always flushes as one contiguous run.
        let mut runs = Vec::new();
        if piece_full && let Some(run) = cache.take_flush_run(loc) {
            runs.push((loc, run.0, run.1));
        }
        while let Some(low) = self.back_pressure.should_flush(cache.num_cached_blocks) {
            if cache.num_cached_blocks <= low {
                break;
            }
            let Some(run) = cache.take_oldest_run() else {
                break;
            };
            runs.push(run);
        }
        drop(cache);
        for (run_loc, first, bufs) in runs {
            self.add_job(
                storage,
                self.mk_job(JobKind::Flush(crate::disk_job::FlushJob {
                    piece: run_loc.1,
                    first_block: first,
                    bufs,
                })),
            );
        }
        handler(StorageError::default());
        pressure
    }

    fn async_hash(
        &self,
        storage: StorageIndex,
        piece: PieceIndex,
        v2: Vec<Sha256Hash>,
        _flags: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha1Hash, Vec<Sha256Hash>, StorageError) + Send>,
    ) {
        let j = self.mk_job(JobKind::Hash(HashJob {
            handler,
            piece,
            block_hashes: v2,
            piece_hash: Sha1Hash::default(),
        }));
        self.add_job(storage, j);
    }

    fn async_hash2(
        &self,
        storage: StorageIndex,
        piece: PieceIndex,
        offset: i32,
        _flags: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha256Hash, StorageError) + Send>,
    ) {
        let j = self.mk_job(JobKind::Hash2(Hash2Job {
            handler,
            piece,
            offset,
            piece_hash2: Sha256Hash::default(),
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
        self.file_pool.get_status(storage)
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

    fn set_read_no_cache(&self, v: bool) {
        self.read_no_cache.store(v, Ordering::Relaxed);
    }

    fn set_write_no_cache(&self, v: bool) {
        self.write_no_cache.store(v, Ordering::Relaxed);
    }

    fn set_cache_expiry(&self, secs: u64) {
        PreadDiskIo::set_cache_expiry(self, secs);
    }

    fn flush_expired(&self) {
        PreadDiskIo::flush_expired(self);
    }

    fn write_queue_exceeded(&self) -> bool {
        self.back_pressure.exceeded()
    }

    /// `pread_disk_io::update_stats_counters` (ADR-0017).
    fn queue_stats(&self) -> crate::disk_interface::DiskQueueStats {
        let mut s = crate::disk_interface::DiskQueueStats {
            write_blocks: self.write_blocks.load(Ordering::Relaxed),
            threads: self.pool.num_threads() + self.hash_pool.num_threads(),
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
        self.pool.visit_jobs(|j| match &j.action {
            JobKind::Write(_) => s.queued_writes += 1,
            JobKind::Hash(_) | JobKind::Hash2(_) => s.queued_hashes += 1,
            _ => s.queued_other += 1,
        });
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
