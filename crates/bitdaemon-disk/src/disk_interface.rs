//! Port of `disk_interface.hpp` — the `DiskIo` trait every disk
//! backend implements, `storage_holder`, and `disabled_disk_io`.
//!
//! Upstream calls all `async_*` from the network thread; completions
//! are posted back through the io_context (our `DrainPoster`/`Handler`
//! callbacks — invoked by `disk_completed_queue::call_job_handlers`).

use std::path::PathBuf;
use std::sync::{Arc, Weak};

use bitdaemon_common::types::{FileIndex, PeerRequest, PieceIndex, StorageIndex};
use bitdaemon_common::{Sha1Hash, Sha256Hash};
use bitdaemon_core::add_torrent_params::AddTorrentParams;

use crate::back_pressure::DiskObserver;
use crate::disk_buffer_pool::DiskBufferHolder;
use crate::disk_job::Status;
use crate::file_pool::OpenFileState;
use crate::storage::{MoveFlags, StorageParams};
use crate::storage_error::StorageError;

/// `disk_job_flags_t` extra bits (upstream `disk_interface` constants —
/// `fence`/`in_progress`/`aborted` live in `disk_job::job_flags`).
pub mod flags {
    /// `force_copy` — copy the cached block before overwrite.
    pub const FORCE_COPY: u8 = 0x01;
    /// `sequential_access` hint.
    pub const SEQUENTIAL_ACCESS: u8 = 0x08;
    /// `volatile_read` — don't cache.
    pub const VOLATILE_READ: u8 = 0x10;
    /// `v1_hash` — compute the SHA-1 piece hash.
    pub const V1_HASH: u8 = 0x20;
    /// `flush_piece` — hash job just completed the piece; flush it.
    pub const FLUSH_PIECE: u8 = 0x80;
}

/// `lt::disk_interface`. Handlers are `Box<dyn FnOnce>` run on the
/// network thread when the job lands on the completed queue.
pub trait DiskIo: Send + Sync {
    /// `new_torrent(p, torrent)` — create storage and return its
    /// index; callers wrap it in `StorageHolder` to tie lifetimes
    /// (upstream returns `storage_holder` — impossible from `&self`,
    /// so the wrap is one line at the call site). `torrent` keeps the
    /// per-torrent state alive across jobs.
    fn new_torrent(&self, p: &StorageParams<'_>, torrent: Arc<dyn Send + Sync>) -> StorageIndex;

    /// `remove_torrent`.
    fn remove_torrent(&self, idx: StorageIndex);

    /// `async_read` — unaligned requests allowed.
    fn async_read(
        &self,
        storage: StorageIndex,
        r: PeerRequest,
        handler: Box<dyn FnOnce(DiskBufferHolder, StorageError) + Send>,
        flags: u8,
    );

    /// `async_write` — block-aligned; returns true when back-pressure
    /// applies (the peer then waits on `o`).
    fn async_write(
        &self,
        storage: StorageIndex,
        r: PeerRequest,
        buf: DiskBufferHolder,
        o: Option<Arc<dyn DiskObserver>>,
        handler: Box<dyn FnOnce(StorageError) + Send>,
        flags: u8,
    ) -> bool;

    /// `async_hash` — `v2` pre-sizes the out block-hash vec; the
    /// computed per-16KiB SHA-256s are handed back (upstream keeps a
    /// caller-owned span). SHA-1 only when `flags & V1_HASH`.
    fn async_hash(
        &self,
        storage: StorageIndex,
        piece: PieceIndex,
        v2: Vec<Sha256Hash>,
        flags: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha1Hash, Vec<Sha256Hash>, StorageError) + Send>,
    );

    /// `async_hash2` — single-block SHA-256.
    fn async_hash2(
        &self,
        storage: StorageIndex,
        piece: PieceIndex,
        offset: i32,
        flags: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha256Hash, StorageError) + Send>,
    );

    /// `async_move_storage`.
    fn async_move_storage(
        &self,
        storage: StorageIndex,
        path: PathBuf,
        flags: MoveFlags,
        handler: Box<dyn FnOnce(Status, PathBuf, StorageError) + Send>,
    );

    /// `async_release_files`.
    fn async_release_files(&self, storage: StorageIndex, handler: Box<dyn FnOnce() + Send>);

    /// `async_check_files` — validate resume data / create hard links.
    fn async_check_files(
        &self,
        storage: StorageIndex,
        resume_data: Option<Box<AddTorrentParams>>,
        links: Vec<String>,
        handler: Box<dyn FnOnce(Status, StorageError) + Send>,
    );

    /// `async_stop_torrent`.
    fn async_stop_torrent(&self, storage: StorageIndex, handler: Box<dyn FnOnce() + Send>);

    /// `async_rename_file`.
    fn async_rename_file(
        &self,
        storage: StorageIndex,
        index: FileIndex,
        name: PathBuf,
        handler: Box<dyn FnOnce(PathBuf, FileIndex, StorageError) + Send>,
    );

    /// `async_delete_files`.
    fn async_delete_files(
        &self,
        storage: StorageIndex,
        options: u8,
        handler: Box<dyn FnOnce(StorageError) + Send>,
    );

    /// `async_set_file_priority` — handler gets the adjusted vector.
    fn async_set_file_priority(
        &self,
        storage: StorageIndex,
        prio: Vec<bitdaemon_common::types::DownloadPriority>,
        handler: Box<
            dyn FnOnce(StorageError, Vec<bitdaemon_common::types::DownloadPriority>) + Send,
        >,
    );

    /// `async_clear_piece` — fence-sync before re-requesting blocks.
    fn async_clear_piece(
        &self,
        storage: StorageIndex,
        index: PieceIndex,
        handler: Box<dyn FnOnce(PieceIndex) + Send>,
    );

    /// `get_status` — open files of a storage.
    fn get_status(&self, storage: StorageIndex) -> Vec<OpenFileState>;

    /// `abort(wait)` — flush writes, cancel hashes, stop threads.
    fn abort(&self, wait: bool);

    /// `submit_jobs` — batch wake-up.
    fn submit_jobs(&self);

    /// `disk_completed_queue::call_job_handlers` — run pending
    /// completion callbacks on the caller's thread; returns how many
    /// ran. The session's disk drain must call this or completions
    /// never reach their handlers (upstream posts this on the
    /// io_context via the `DrainPoster`; sessions that drive `tick`
    /// may call it directly instead). Default: no completions.
    fn call_job_handlers(&self) -> usize {
        0
    }

    /// `settings_pack::max_queued_disk_bytes` — resize the write
    /// back-pressure limit (upstream `m_cache.set_max_size`). No-op
    /// on backends without a buffer limit.
    fn set_max_queued_bytes(&self, _bytes: i64) {}

    /// `settings_pack::disk_io_write_mode` — `disable_os_cache`
    /// variant: when true, data files open with write-through
    /// semantics (`FILE_FLAG_WRITE_THROUGH` on Windows / `O_DIRECT`
    /// hint on POSIX), so dirty pages can't accumulate and stall the
    /// host's cache manager.
    fn set_write_no_cache(&self, _v: bool) {}

    /// `settings_pack::disk_io_read_mode` — `disable_os_cache`:
    /// read/hash jobs open files with `no_cache` (ADR-0015).
    fn set_read_no_cache(&self, _v: bool) {}

    /// `settings_pack::cache_expiry` — seconds a dirty block may sit
    /// in the write cache before the periodic flush (upstream 60).
    fn set_cache_expiry(&self, _secs: u64) {}

    /// periodic flush of expired cache regions — called once per
    /// session tick (upstream's disk-thread flush timer).
    fn flush_expired(&self) {}

    /// `m_cache.exceeded_max_size()` — true while the write queue is
    /// above `max_queued_disk_bytes`. The session gates new block
    /// requests on this so incoming payloads can't grow the job queue
    /// (and its buffers) without bound.
    fn write_queue_exceeded(&self) -> bool {
        false
    }

    /// live job-queue snapshot — diagnostics only (`performance
    /// counters` cover this upstream). Default: empty.
    fn queue_stats(&self) -> DiskQueueStats {
        DiskQueueStats::default()
    }

    /// cumulative count of failed cache flushes — the session posts a
    /// `file_error_alert` per increment (upstream `handle_disk_error`
    /// path). Default: 0.
    fn flush_error_count(&self) -> i64 {
        0
    }

    /// `settings_updated`.
    fn settings_updated(&self);
}

/// Snapshot of the disk job queue — diagnostics for download
/// orchestration (write backlog vs hash starvation).
#[derive(Debug, Default, Clone, Copy)]
pub struct DiskQueueStats {
    /// jobs waiting in the pool queue, per kind.
    pub queued_writes: usize,
    /// hash jobs waiting in the pool queue.
    pub queued_hashes: usize,
    /// other jobs waiting (read, flush, move…).
    pub queued_other: usize,
    /// hash jobs parked behind in-flight writes (`piece_gate`).
    pub parked_hashes: usize,
    /// `m_blocks` — write blocks queued + in flight (back-pressure
    /// level against `max_queued_disk_bytes`).
    pub write_blocks: i64,
    /// pool worker threads.
    pub threads: usize,
    /// `counters::queued_disk_jobs` — total jobs queued.
    pub queued_disk_jobs: usize,
    /// `counters::blocked_disk_jobs` — parked behind a raised fence.
    pub blocked_disk_jobs: usize,
    /// `counters::disk_blocks_in_use` — buffers checked out.
    pub disk_blocks_in_use: i64,
    /// `counters::cached_blocks` — dirty blocks in `store_buffer`.
    pub cached_blocks: i64,
    /// `counters::num_unhashed` — cached blocks not yet hashed.
    pub num_unhashed: i64,
    /// `counters::queued_write_bytes` — cached+queued write bytes.
    pub queued_write_bytes: i64,
    /// `counters::num_blocks_written` — cumulative.
    pub num_blocks_written: i64,
    /// `counters::num_blocks_read` — cumulative.
    pub num_blocks_read: i64,
    /// `counters::num_blocks_hashed` — cumulative.
    pub num_blocks_hashed: i64,
    /// `counters::num_write_ops` — cumulative write syscalls.
    pub num_write_ops: i64,
    /// `counters::num_read_ops` — cumulative read syscalls.
    pub num_read_ops: i64,
    /// `counters::file_pool_size` + hit/miss/stall/race gauges.
    pub file_pool_size: i64,
    /// `counters::file_pool_hits`.
    pub file_pool_hits: i64,
    /// `counters::file_pool_misses`.
    pub file_pool_misses: i64,
    /// `counters::file_pool_thread_stall`.
    pub file_pool_thread_stall: i64,
    /// `counters::file_pool_race`.
    pub file_pool_race: i64,
}

/// `storage_holder` — RAII ownership of a torrent's storage index;
/// `reset`/drop calls `remove_torrent` on the disk io.
pub struct StorageHolder {
    io: Option<Weak<dyn DiskIo>>,
    idx: StorageIndex,
}

impl StorageHolder {
    /// `storage_holder(idx, disk_io)`.
    #[must_use]
    pub fn new(idx: StorageIndex, io: &Arc<dyn DiskIo>) -> Self {
        Self {
            io: Some(Arc::downgrade(io)),
            idx,
        }
    }

    /// `storage_holder()` — empty.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            io: None,
            idx: StorageIndex(0),
        }
    }

    /// `operator storage_index_t`.
    #[must_use]
    pub fn index(&self) -> StorageIndex {
        self.idx
    }

    /// `operator bool`.
    #[must_use]
    pub fn is_some(&self) -> bool {
        self.io.is_some()
    }

    /// `reset()`.
    pub fn reset(&mut self) {
        if let Some(io) = self.io.take().and_then(|w| w.upgrade()) {
            io.remove_torrent(self.idx);
        }
    }
}

impl Drop for StorageHolder {
    fn drop(&mut self) {
        self.reset();
    }
}

/// `disabled_disk_io` — every operation completes immediately with a
/// default (no-op) result. Used in tests and the no-disk configuration.
pub struct DisabledDiskIo;

impl DisabledDiskIo {
    /// `disabled_disk_io(ios, cnt)` — handlers run synchronously.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for DisabledDiskIo {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskIo for DisabledDiskIo {
    fn new_torrent(&self, _p: &StorageParams<'_>, _t: Arc<dyn Send + Sync>) -> StorageIndex {
        StorageIndex(u32::MAX)
    }
    fn remove_torrent(&self, _idx: StorageIndex) {}
    fn async_read(
        &self,
        _s: StorageIndex,
        r: PeerRequest,
        handler: Box<dyn FnOnce(DiskBufferHolder, StorageError) + Send>,
        _flags: u8,
    ) {
        let _ = r;
        // upstream posts the handler on the ios — calling it directly
        // is the synchronous equivalent for the disabled backend
        handler(DiskBufferHolder::empty(), StorageError::default());
    }
    fn async_write(
        &self,
        _s: StorageIndex,
        _r: PeerRequest,
        _b: DiskBufferHolder,
        _o: Option<Arc<dyn DiskObserver>>,
        handler: Box<dyn FnOnce(StorageError) + Send>,
        _flags: u8,
    ) -> bool {
        handler(StorageError::default());
        false
    }
    fn async_hash(
        &self,
        _s: StorageIndex,
        piece: PieceIndex,
        _v2: Vec<Sha256Hash>,
        _f: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha1Hash, Vec<Sha256Hash>, StorageError) + Send>,
    ) {
        handler(
            piece,
            Sha1Hash::default(),
            Vec::new(),
            StorageError::default(),
        );
    }
    fn async_hash2(
        &self,
        _s: StorageIndex,
        piece: PieceIndex,
        _o: i32,
        _f: u8,
        handler: Box<dyn FnOnce(PieceIndex, Sha256Hash, StorageError) + Send>,
    ) {
        handler(piece, Sha256Hash::default(), StorageError::default());
    }
    fn async_move_storage(
        &self,
        _s: StorageIndex,
        path: PathBuf,
        _f: MoveFlags,
        handler: Box<dyn FnOnce(Status, PathBuf, StorageError) + Send>,
    ) {
        handler(0, path, StorageError::default());
    }
    fn async_release_files(&self, _s: StorageIndex, handler: Box<dyn FnOnce() + Send>) {
        handler();
    }
    fn async_check_files(
        &self,
        _s: StorageIndex,
        _r: Option<Box<AddTorrentParams>>,
        _l: Vec<String>,
        handler: Box<dyn FnOnce(Status, StorageError) + Send>,
    ) {
        handler(0, StorageError::default());
    }
    fn async_stop_torrent(&self, _s: StorageIndex, handler: Box<dyn FnOnce() + Send>) {
        handler();
    }
    fn async_rename_file(
        &self,
        _s: StorageIndex,
        index: FileIndex,
        name: PathBuf,
        handler: Box<dyn FnOnce(PathBuf, FileIndex, StorageError) + Send>,
    ) {
        handler(name, index, StorageError::default());
    }
    fn async_delete_files(
        &self,
        _s: StorageIndex,
        _o: u8,
        handler: Box<dyn FnOnce(StorageError) + Send>,
    ) {
        handler(StorageError::default());
    }
    fn async_set_file_priority(
        &self,
        _s: StorageIndex,
        prio: Vec<bitdaemon_common::types::DownloadPriority>,
        handler: Box<
            dyn FnOnce(StorageError, Vec<bitdaemon_common::types::DownloadPriority>) + Send,
        >,
    ) {
        handler(StorageError::default(), prio);
    }
    fn async_clear_piece(
        &self,
        _s: StorageIndex,
        index: PieceIndex,
        handler: Box<dyn FnOnce(PieceIndex) + Send>,
    ) {
        handler(index);
    }
    fn get_status(&self, _s: StorageIndex) -> Vec<OpenFileState> {
        Vec::new()
    }
    fn abort(&self, _wait: bool) {}
    fn submit_jobs(&self) {}
    fn settings_updated(&self) {}
}
