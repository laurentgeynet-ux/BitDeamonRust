//! Port of `aux_/disk_job.hpp`/`disk_job.cpp`.
//!
//! C++ `disk_job` is an intrusive-list node posted between the network
//! and disk threads as a raw pointer. In Rust jobs are `Box<DiskJob>` in
//! `VecDeque`s — the fence and the job pool own them outright, and the
//! completion queue hands them back to the caller. Handlers are boxed
//! `FnOnce` — the job's completion callback.

use std::path::PathBuf;
use std::sync::Arc;

use bitdaemon_common::types::{DownloadPriority, FileIndex, PieceIndex, StorageIndex};
use bitdaemon_common::{Sha1Hash, Sha256Hash};
use bitdaemon_core::add_torrent_params::AddTorrentParams;

use crate::disk_buffer_pool::DiskBufferHolder;
use crate::storage::MoveFlags;
use crate::storage_error::StorageError;

/// `aux::job_action_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobAction {
    /// `read`.
    Read,
    /// `write`.
    Write,
    /// `hash` — SHA-1 piece hash (+ optional per-block SHA-256).
    Hash,
    /// `hash2` — single-block SHA-256.
    Hash2,
    /// `move_storage`.
    MoveStorage,
    /// `release_files`.
    ReleaseFiles,
    /// `delete_files`.
    DeleteFiles,
    /// `check_fastresume`.
    CheckFastresume,
    /// `rename_file`.
    RenameFile,
    /// `stop_torrent`.
    StopTorrent,
    /// `file_priority`.
    FilePriority,
    /// `clear_piece`.
    ClearPiece,
    /// `partial_read` — unaligned peer requests.
    PartialRead,
    /// `remove_torrent` — upstream `disk_io_thread::remove_torrent`,
    /// a queued fence job (not an inline map erase).
    RemoveTorrent,
}

impl JobAction {
    /// `num_job_ids` — counter array slots for the `num_fenced_*`
    /// gauges (`num_fenced_read + get_type()`).
    pub const COUNT: usize = 14;

    /// `static_cast<int>(get_type())` — index into the
    /// `num_fenced_read + …` counter range.
    #[must_use]
    pub fn index(self) -> usize {
        self as usize
    }
}

/// `disk_job` flag bits (`fence`, `in_progress`, `aborted`).
pub mod job_flags {
    /// `disk_job::fence` — exclusive-storage job that raised the fence.
    pub const FENCE: u8 = 1;
    /// `disk_job::in_progress`.
    pub const IN_PROGRESS: u8 = 2;
    /// `disk_job::aborted` (upstream `6_bit` = `1 << 6`).
    pub const ABORTED: u8 = 0x40;
    /// Internal: the write job is parked on a cache block (upstream
    /// `write_state == disk_job*`) — its back-pressure slot is owned
    /// by the cache entry, so `job_done` must not decrement
    /// `write_blocks` again when the writer completes.
    pub const CACHED: u8 = 0x20;
}

/// `status_t` upstream.
pub type Status = i32;

/// `job::read`.
pub struct ReadJob {
    /// completion handler — `(buffer, error)`.
    pub handler: Box<dyn FnOnce(DiskBufferHolder, StorageError) + Send>,
    /// `buf` — passed out.
    pub buf: DiskBufferHolder,
    /// `buffer_size` — passed in/out.
    pub buffer_size: u16,
    /// `piece`.
    pub piece: PieceIndex,
    /// `offset` into the piece.
    pub offset: i32,
    /// `volatile_read` — hint not to cache (upstream flag).
    pub volatile: bool,
    /// `flush_piece` — `page_out` the read range (upstream passes
    /// `j->flags` wholesale into `storage::read`; mmap-only effect).
    pub flush: bool,
    /// `sequential_access` — more sequential reads follow
    /// (upstream `file_mode_for_job` → `open_mode::sequential_access`).
    pub sequential: bool,
}
pub struct PartialReadJob {
    /// handler — `(buffer, error)`.
    pub handler: Box<dyn FnOnce(DiskBufferHolder, StorageError) + Send>,
    /// `buf` — in/out.
    pub buf: DiskBufferHolder,
    /// `buffer_offset` — bytes to skip into the destination.
    pub buffer_offset: u16,
    /// `buffer_size`.
    pub buffer_size: u16,
    /// `piece`.
    pub piece: PieceIndex,
    /// `offset`.
    pub offset: i32,
    /// carried over from the `Read` job — upstream keeps `j->flags`
    /// when the job is converted, so `volatile_read` still applies.
    pub volatile: bool,
    /// `flush_piece`, same carry-over.
    pub flush: bool,
    /// `sequential_access`, same carry-over.
    pub sequential: bool,
}

/// `job::write`.
pub struct WriteJob {
    /// handler — `(error)`.
    pub handler: Box<dyn FnOnce(StorageError) + Send>,
    /// `buf` — the block to write.
    pub buf: DiskBufferHolder,
    /// `piece`.
    pub piece: PieceIndex,
    /// `offset` — always block-aligned upstream.
    pub offset: i32,
    /// `buffer_size`.
    pub buffer_size: u16,
    /// upstream `j->flags & flush_piece` — the caller asked for an
    /// immediate flush once the block lands in the cache.
    pub force_flush: bool,
    /// upstream `j->flags & volatile_read` — `dont_need` the written
    /// range (mmap backend; the flush run ORs the writers' flags).
    pub volatile: bool,
}

/// `job::hash` handler — `(piece, sha1, block_hashes, block_digests,
/// error)`.
pub type HashHandler =
    Box<dyn FnOnce(PieceIndex, Sha1Hash, Vec<Sha256Hash>, Vec<Sha1Hash>, StorageError) + Send>;

/// `job::hash` — SHA-1 over a whole piece (+ block SHA-256s).
pub struct HashJob {
    /// handler — `(piece, sha1, block_hashes, block_digests, error)`.
    pub handler: HashHandler,
    /// `piece`.
    pub piece: PieceIndex,
    /// `block_hashes` — out: per-16KiB-block SHA-256 (v2). Zero entries
    /// mean "unknown" — they're filled from precomputed storage or
    /// hashed (upstream's caller clears the span before submit).
    pub block_hashes: Vec<Sha256Hash>,
    /// out: per-16KiB-block SHA-1 digests — the job already reads
    /// every byte of the piece, so the `smart_ban` plugin's block CRC
    /// record (upstream re-reads each block from disk) rides along
    /// for free. Only filled when `v1_hash` is set and the read
    /// succeeded — the caller treats an empty vec like upstream's
    /// `on_read_failed_block` error leg (entry skipped).
    pub block_digests: Vec<Sha1Hash>,
    /// `piece_hash` — out. Only set when `v1_hash` is set (upstream
    /// skips the SHA-1 pass entirely for v2-only hashes).
    pub piece_hash: Sha1Hash,
    /// `v1_hash` flag — compute the v1 piece SHA-1.
    pub v1_hash: bool,
    /// `volatile_read` — `advise_dont_need` on the read path.
    pub volatile: bool,
    /// `flush_piece` — `page_out` the hashed range (mmap backend;
    /// `verify_piece` sets it for `write_through`/`disable_os_cache`).
    pub flush: bool,
    /// `sequential_access` — `fadvise(SEQUENTIAL)`/SCAN on open.
    pub sequential: bool,
}

/// `job::hash2` — SHA-256 over one block.
pub struct Hash2Job {
    /// handler — `(piece, sha256, error)`.
    pub handler: Box<dyn FnOnce(PieceIndex, Sha256Hash, StorageError) + Send>,
    /// `piece`.
    pub piece: PieceIndex,
    /// `offset` — first block still referenced.
    pub offset: i32,
    /// `piece_hash2` — out.
    pub piece_hash2: Sha256Hash,
    /// `volatile_read` — `advise_dont_need` on the read path.
    pub volatile: bool,
    /// `flush_piece` — `page_out` the hashed range (mmap backend).
    pub flush: bool,
    /// `sequential_access` — `fadvise(SEQUENTIAL)`/SCAN on open.
    pub sequential: bool,
}

/// `job::move_storage`.
pub struct MoveStorageJob {
    /// handler — `(status, new_path, error)`.
    pub handler: Box<dyn FnOnce(Status, PathBuf, StorageError) + Send>,
    /// `path`.
    pub path: PathBuf,
    /// `move_flags`.
    pub move_flags: MoveFlags,
}

/// `job::release_files`.
pub struct ReleaseFilesJob {
    /// handler.
    pub handler: Box<dyn FnOnce() + Send>,
}

/// `job::delete_files`.
pub struct DeleteFilesJob {
    /// handler — `(error)`.
    pub handler: Box<dyn FnOnce(StorageError) + Send>,
    /// `flags` (`remove_flags_t`).
    pub flags: i32,
}

/// `job::check_fastresume`.
pub struct CheckFastresumeJob {
    /// handler — `(status, error)`.
    pub handler: Box<dyn FnOnce(Status, StorageError) + Send>,
    /// `links` — hard-link targets per file (upstream `vector<string>*`).
    pub links: Vec<String>,
    /// `resume_data`.
    pub resume_data: Option<Box<AddTorrentParams>>,
}

/// `job::rename_file`.
pub struct RenameFileJob {
    /// handler — `(new_name, file_index, error)`.
    pub handler: Box<dyn FnOnce(PathBuf, FileIndex, StorageError) + Send>,
    /// `file_index` — in/out.
    pub file_index: FileIndex,
    /// `name` — in/out.
    pub name: PathBuf,
}

/// `job::stop_torrent`.
pub struct StopTorrentJob {
    /// handler.
    pub handler: Box<dyn FnOnce() + Send>,
}

/// `disk_io_thread::remove_torrent` — queued behind the fence like
/// every other job upstream; pending jobs for the storage complete
/// first, submissions after it fail `NotFound`.
pub struct RemoveTorrentJob {
    /// handler.
    pub handler: Box<dyn FnOnce() + Send>,
}

/// `job::file_priority`.
pub struct FilePriorityJob {
    /// handler — `(error, new_priorities)`.
    pub handler: Box<dyn FnOnce(StorageError, Vec<DownloadPriority>) + Send>,
    /// `prio` — in/out.
    pub prio: Vec<DownloadPriority>,
}

/// `job::clear_piece`.
pub struct ClearPieceJob {
    /// handler — `(piece)`.
    pub handler: Box<dyn FnOnce(PieceIndex) + Send>,
    /// `piece`.
    pub piece: PieceIndex,
}

/// Internal flush job — no upstream `job_type` equivalent; upstream
/// folds this into `disk_io_thread`'s cache flush. Drains a
/// contiguous `store_buffer` region through one `writev`.
pub struct FlushJob {
    /// `piece` — the first piece covered (routing + gate key).
    pub piece: PieceIndex,
    /// index of the first block covered.
    pub first_block: i32,
    /// `coalesce_writes` — when a run reaches the end of its piece and
    /// the next piece starts dirty at block 0, the two are contiguous
    /// in file space and upstream flushes them in a single writev.
    /// `spans` splits `bufs` back into `(piece, first_block, n_bufs)`
    /// for completion bookkeeping; empty for a single-piece flush.
    pub spans: Vec<(PieceIndex, i32, usize)>,
    /// contiguous block buffers (≤ 16 KiB each, last may be short) —
    /// `Arc` clones of the cache slots: upstream's `borrowed_buf`, the
    /// block keeps serving its bytes to reads until `run_flushed`.
    pub bufs: Vec<Arc<Vec<u8>>>,
    /// Write jobs parked on the flushed blocks (upstream
    /// `cached_block_entry::write_state = disk_job*`, moved out by
    /// `take_write_job`): they complete *here*, with this job's
    /// result — a write isn't done until its data is durable.
    pub writers: Vec<Box<DiskJob>>,
}

/// The `std::variant<...>` job payload.
pub enum JobKind {
    /// read.
    Read(ReadJob),
    /// write.
    Write(WriteJob),
    /// internal cache flush — a coalesced region write; counts as a
    /// write for fencing/gating purposes.
    Flush(FlushJob),
    /// hash.
    Hash(HashJob),
    /// hash2.
    Hash2(Hash2Job),
    /// move_storage.
    MoveStorage(MoveStorageJob),
    /// release_files.
    ReleaseFiles(ReleaseFilesJob),
    /// delete_files.
    DeleteFiles(DeleteFilesJob),
    /// check_fastresume.
    CheckFastresume(CheckFastresumeJob),
    /// rename_file.
    RenameFile(RenameFileJob),
    /// stop_torrent.
    StopTorrent(StopTorrentJob),
    /// file_priority.
    FilePriority(FilePriorityJob),
    /// clear_piece.
    ClearPiece(ClearPieceJob),
    /// partial_read.
    PartialRead(PartialReadJob),
    /// `remove_torrent`.
    RemoveTorrent(RemoveTorrentJob),
}

/// `aux::disk_job` — flags + result + payload.
///
/// Manual `Debug`: `action` holds `FnOnce` handlers (not `Debug`) —
/// report the job type name instead.
impl std::fmt::Debug for DiskJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskJob")
            .field("storage", &self.storage)
            .field("flags", &self.flags)
            .field("ret", &self.ret)
            .field("type", &self.get_type())
            .finish()
    }
}

pub struct DiskJob {
    /// `storage` — the torrent this job belongs to (upstream
    /// `j->storage` pointer, bound at submit time).
    pub storage: StorageIndex,
    /// `flags` (`fence` | `in_progress` | `aborted`).
    pub flags: u8,
    /// `ret` — out: `status_t`.
    pub ret: Status,
    /// `error` — out.
    pub error: StorageError,
    /// `action` variant.
    pub action: JobKind,
    /// `disk_job_pool` ownership — `mk_job` allocates with the pool's
    /// counters and the `read`/`write` tag (`is_same_v<JobType>`
    /// split upstream; flush/hash only count in `jobs_in_use`).
    /// `Drop` is `free_job` — the tag survives `call_callback`'s
    /// `action` replacement, so the dec stays in the right bucket.
    /// `None` for hand-constructed jobs (tests).
    #[doc(hidden)]
    pub pool_counters: Option<(std::sync::Arc<crate::disk_pool::JobPoolCounters>, u8)>,
}

impl Drop for DiskJob {
    /// `free_job` — `get_type()`-keyed dec of the pool counters
    /// (verbatim: `read` → `read_jobs`, `write` → `write_jobs`,
    /// everything else only `jobs_in_use`).
    fn drop(&mut self) {
        if let Some((c, kind)) = self.pool_counters.take() {
            use std::sync::atomic::Ordering::Relaxed;
            c.jobs_in_use.fetch_sub(1, Relaxed);
            match kind {
                1 => {
                    c.read_jobs.fetch_sub(1, Relaxed);
                }
                2 => {
                    c.write_jobs.fetch_sub(1, Relaxed);
                }
                _ => {}
            }
        }
    }
}

impl DiskJob {
    /// `get_type()`.
    #[must_use]
    pub fn get_type(&self) -> JobAction {
        match &self.action {
            JobKind::Read(_) => JobAction::Read,
            // a flush is a coalesced write — same fence/gate class
            JobKind::Write(_) | JobKind::Flush(_) => JobAction::Write,
            JobKind::Hash(_) => JobAction::Hash,
            JobKind::Hash2(_) => JobAction::Hash2,
            JobKind::MoveStorage(_) => JobAction::MoveStorage,
            JobKind::ReleaseFiles(_) => JobAction::ReleaseFiles,
            JobKind::DeleteFiles(_) => JobAction::DeleteFiles,
            JobKind::CheckFastresume(_) => JobAction::CheckFastresume,
            JobKind::RenameFile(_) => JobAction::RenameFile,
            JobKind::StopTorrent(_) => JobAction::StopTorrent,
            JobKind::FilePriority(_) => JobAction::FilePriority,
            JobKind::ClearPiece(_) => JobAction::ClearPiece,
            JobKind::PartialRead(_) => JobAction::PartialRead,
            JobKind::RemoveTorrent(_) => JobAction::RemoveTorrent,
        }
    }

    /// `call_callback` — invokes the job's handler once with the
    /// recorded results.
    pub fn call_callback(&mut self) {
        match std::mem::replace(
            &mut self.action,
            JobKind::ReleaseFiles(ReleaseFilesJob {
                handler: Box::new(|| {}),
            }),
        ) {
            JobKind::Read(mut j) => (j.handler)(
                std::mem::replace(&mut j.buf, DiskBufferHolder::empty()),
                std::mem::take(&mut self.error),
            ),
            JobKind::PartialRead(mut j) => (j.handler)(
                std::mem::replace(&mut j.buf, DiskBufferHolder::empty()),
                std::mem::take(&mut self.error),
            ),
            JobKind::Write(j) => (j.handler)(std::mem::take(&mut self.error)),
            // internal flush: no user handler — bookkeeping is done in
            // `add_completed_jobs` before this callback path runs.
            JobKind::Flush(_) => {}
            JobKind::Hash(mut j) => (j.handler)(
                j.piece,
                j.piece_hash,
                std::mem::take(&mut j.block_hashes),
                std::mem::take(&mut j.block_digests),
                std::mem::take(&mut self.error),
            ),
            JobKind::Hash2(j) => {
                (j.handler)(j.piece, j.piece_hash2, std::mem::take(&mut self.error))
            }
            JobKind::MoveStorage(mut j) => {
                let path = std::mem::take(&mut j.path);
                (j.handler)(self.ret, path, std::mem::take(&mut self.error));
            }
            JobKind::ReleaseFiles(j) => (j.handler)(),
            JobKind::DeleteFiles(j) => (j.handler)(std::mem::take(&mut self.error)),
            JobKind::CheckFastresume(j) => (j.handler)(self.ret, std::mem::take(&mut self.error)),
            JobKind::RenameFile(mut j) => {
                let name = std::mem::take(&mut j.name);
                (j.handler)(name, j.file_index, std::mem::take(&mut self.error));
            }
            JobKind::StopTorrent(j) => (j.handler)(),
            JobKind::FilePriority(mut j) => {
                let prio = std::mem::take(&mut j.prio);
                (j.handler)(std::mem::take(&mut self.error), prio);
            }
            JobKind::ClearPiece(j) => (j.handler)(j.piece),
            JobKind::RemoveTorrent(j) => (j.handler)(),
        }
    }
}
