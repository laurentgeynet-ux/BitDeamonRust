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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bitdaemon_common::types::{FileIndex, StorageIndex};
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
use crate::storage::PreadStorage;
use crate::storage_error::{Operation, StorageError};

/// Per-torrent state owned by the disk io — upstream the fence hangs
/// off `storage_interface`; here both live behind one entry.
struct StorageEntry {
    storage: Mutex<PreadStorage>,
    fence: DiskJobFence,
}

/// `pread_disk_io` — positioned-I/O disk orchestrator.
pub struct PreadDiskIo {
    storages: Mutex<HashMap<StorageIndex, Arc<StorageEntry>>>,
    pool: Arc<DiskIoThreadPool>,
    completed: Arc<DiskCompletedQueue>,
    fence_counters: FenceCounters,
    /// `m_abort` — after abort, add_job posts straight to completions.
    abort: AtomicBool,
    buffer_pool: Arc<crate::disk_buffer_pool::DiskBufferPool>,
}

impl PreadDiskIo {
    /// `pread_disk_io(ios, cnt)` — `poster` drains completions on the
    /// network thread.
    pub fn new(max_threads: i64, poster: crate::disk_completed::DrainPoster) -> Arc<Self> {
        let buffer_pool = Arc::new(crate::disk_buffer_pool::DiskBufferPool::new());
        let completed = Arc::new(DiskCompletedQueue::new(poster));
        // the worker loop needs the orchestrator — two-phase init:
        // pool is created with a worker that captures a Weak we fill in.
        let io_slot: Arc<Mutex<Option<std::sync::Weak<PreadDiskIo>>>> = Arc::new(Mutex::new(None));
        let io_slot2 = Arc::clone(&io_slot);
        let pool = DiskIoThreadPool::new(Arc::new(move |pool| {
            let io = io_slot2
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            if let Some(io) = io {
                io.worker_loop(&pool);
            }
        }));
        let io = Arc::new(Self {
            storages: Mutex::new(HashMap::new()),
            pool,
            completed,
            fence_counters: FenceCounters::default(),
            abort: AtomicBool::new(false),
            buffer_pool,
        });
        *io_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::downgrade(&io));
        io.pool.set_max_threads(max_threads);
        io
    }

    /// the buffer pool (jobs' `disk_buffer_holder`s come from here).
    #[must_use]
    pub fn buffer_pool(&self) -> Arc<crate::disk_buffer_pool::DiskBufferPool> {
        Arc::clone(&self.buffer_pool)
    }

    /// the completion queue (network thread drains via the poster).
    #[must_use]
    pub fn completed(&self) -> Arc<DiskCompletedQueue> {
        Arc::clone(&self.completed)
    }

    /// `new_torrent(storage_index, params)` — register a storage.
    pub fn new_torrent(&self, idx: StorageIndex, storage: PreadStorage) {
        storage.set_storage_index(idx);
        self.storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                idx,
                Arc::new(StorageEntry {
                    storage: Mutex::new(storage),
                    fence: DiskJobFence::new(),
                }),
            );
    }

    /// `remove_torrent(idx)` — drop the storage; its queued jobs fail
    /// fast at execution (`NotFound`) like upstream's aborted path.
    pub fn remove_torrent(&self, idx: StorageIndex) {
        self.storages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&idx);
    }

    /// `abort_jobs(wait)` — stop the pool; queued jobs complete aborted.
    pub fn abort_jobs(&self, wait: bool) {
        self.abort.store(true, Ordering::Relaxed);
        let mut drained = self.pool.drain_jobs();
        for j in &mut drained {
            crate::disk_completed::abort(j);
        }
        self.pool.abort(wait);
        self.completed.append(drained);
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
            self.completed.push(j);
            return;
        }
        let mut j = j;
        j.storage = idx;
        if let Some(entry) = self.storage(idx) {
            if let Some(j) = entry.fence.is_blocked(j, &self.fence_counters) {
                self.enqueue(j);
            }
        } else {
            // no storage — job is stale; complete it aborted
            let mut j = j;
            crate::disk_completed::abort(&mut j);
            self.completed.push(j);
        }
    }

    /// `add_fence_job(j)` — exclusive path.
    pub fn add_fence_job(&self, idx: StorageIndex, j: Box<DiskJob>) {
        if self.abort.load(Ordering::Relaxed) {
            let mut j = j;
            crate::disk_completed::abort(&mut j);
            self.completed.push(j);
            return;
        }
        match self.storage(idx) {
            Some(entry) => {
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
                self.completed.push(j);
            }
        }
    }

    fn enqueue(&self, j: Box<DiskJob>) {
        self.pool.push_back(j);
        if self.pool.max_threads() == 0 {
            self.immediate_execute();
        } else {
            self.pool.submit_jobs();
        }
    }

    /// `immediate_execute` — 0-thread fallback: run queued jobs inline.
    fn immediate_execute(&self) {
        loop {
            let Some(j) = self.pool.pop_front() else {
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
                let mut reposted = Vec::new();
                e.fence.job_complete(&mut j, &mut |bj| reposted.push(bj));
                for bj in reposted {
                    self.pool.push_back(bj);
                }
                if self.pool.max_threads() > 0 {
                    self.pool.submit_jobs();
                }
            }
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
        let mut st_guard = e.storage.lock().unwrap_or_else(|p| p.into_inner());
        let st = &mut *st_guard;
        j.error = match &mut j.action {
            JobKind::Read(r) => read_job(st, r),
            JobKind::PartialRead(r) => partial_read_job(st, r),
            JobKind::Write(w) => write_job(st, w),
            JobKind::Hash(h) => hash_job(st, h),
            JobKind::Hash2(h) => hash2_job(st, h),
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
            JobKind::CheckFastresume(c) => {
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
                        j.ret = Status::from(ok);
                        StorageError::default()
                    }
                    Err(e) => e,
                }
            }
            JobKind::RenameFile(r) => {
                match st.rename_file(r.file_index, r.name.as_os_str().as_encoded_bytes()) {
                    Ok(()) => StorageError::default(),
                    Err(e) => e,
                }
            }
            JobKind::StopTorrent(_) => StorageError::default(),
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
            JobKind::ClearPiece(c) => {
                let _ = c;
                StorageError::default()
            }
        };
    }

    /// `update_stats` → pool counters snapshot.
    #[must_use]
    pub fn num_threads(&self) -> usize {
        self.pool.num_threads()
    }
}

fn read_job(st: &PreadStorage, r: &mut ReadJob) -> StorageError {
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
        OpenMode::READ_ONLY,
        r.volatile,
    ) {
        Ok(n) => {
            r.buffer_size = u16::try_from(n).unwrap_or(0);
            StorageError::default()
        }
        Err(e) => e,
    }
}

fn partial_read_job(st: &PreadStorage, r: &mut PartialReadJob) -> StorageError {
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
        OpenMode::READ_ONLY,
        false,
    ) {
        Ok(_) => StorageError::default(),
        Err(e) => e,
    }
}

fn write_job(st: &PreadStorage, w: &mut WriteJob) -> StorageError {
    let Some(buf) = w.buf.data() else {
        return StorageError::new(
            std::io::Error::from(std::io::ErrorKind::InvalidInput),
            FileIndex(-1),
            Operation::FileWrite,
        );
    };
    let want = usize::from(w.buffer_size).min(buf.len());
    match st.write(&buf[..want], w.piece, i64::from(w.offset), OpenMode::WRITE) {
        Ok(n) => {
            w.buffer_size = u16::try_from(n).unwrap_or(0);
            StorageError::default()
        }
        Err(e) => e,
    }
}

fn hash_job(st: &PreadStorage, h: &mut HashJob) -> StorageError {
    // SHA-1 over the whole piece; also fills per-block SHA-256 when the
    // caller provided a block_hashes span (v2) — upstream hash job.
    let mut sha1 = Sha1::new();
    let mut block = Sha256::new();
    let mut in_block = 0usize;
    let blocks_wanted = !h.block_hashes.is_empty();
    let mut block_idx = 0usize;
    let len = i64::from(st.files().piece_size(h.piece));
    let r = st.hash_with(
        |data| {
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
        },
        len,
        h.piece,
        0,
        OpenMode::READ_ONLY,
        false,
    );
    if in_block > 0 && block_idx < h.block_hashes.len() {
        h.block_hashes[block_idx] =
            Sha256Hash::from_bytes(&block.finalize_reset()).unwrap_or_default();
    }
    match r {
        Ok(_) => {
            h.piece_hash = Sha1Hash::from_bytes(&sha1.finalize()).unwrap_or_default();
            StorageError::default()
        }
        Err(e) => e,
    }
}

fn hash2_job(st: &PreadStorage, h: &mut Hash2Job) -> StorageError {
    let mut sha = Sha256::new();
    let r = st.hash_with(
        |d| sha.update(d),
        i64::try_from(DEFAULT_BLOCK_SIZE).unwrap_or(0),
        h.piece,
        i64::from(h.offset),
        OpenMode::READ_ONLY,
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
