//! Port of `aux_/disk_job.hpp`/`disk_job.cpp`.
//!
//! C++ `disk_job` is an intrusive-list node posted between the network
//! and disk threads as a raw pointer. In Rust jobs are `Box<DiskJob>` in
//! `VecDeque`s — the fence and the job pool own them outright, and the
//! completion queue hands them back to the caller. Handlers are boxed
//! `FnOnce` — the job's completion callback.

use std::path::PathBuf;

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

/// `disk_job` flag bits (`fence`, `in_progress`, `aborted`).
pub mod job_flags {
    /// `disk_job::fence` — exclusive-storage job that raised the fence.
    pub const FENCE: u8 = 1;
    /// `disk_job::in_progress`.
    pub const IN_PROGRESS: u8 = 2;
    /// `disk_job::aborted` (upstream `6_bit` = bits 2|4 — both set).
    pub const ABORTED: u8 = 0b0001_1000;
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
}

/// `job::hash` — SHA-1 over a whole piece (+ block SHA-256s).
pub struct HashJob {
    /// handler — `(piece, sha1, block_hashes, error)`.
    pub handler: Box<dyn FnOnce(PieceIndex, Sha1Hash, Vec<Sha256Hash>, StorageError) + Send>,
    /// `piece`.
    pub piece: PieceIndex,
    /// `block_hashes` — out: per-16KiB-block SHA-256 (v2).
    pub block_hashes: Vec<Sha256Hash>,
    /// `piece_hash` — out.
    pub piece_hash: Sha1Hash,
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

/// The `std::variant<...>` job payload.
pub enum JobKind {
    /// read.
    Read(ReadJob),
    /// write.
    Write(WriteJob),
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
}

impl DiskJob {
    /// `get_type()`.
    #[must_use]
    pub fn get_type(&self) -> JobAction {
        match &self.action {
            JobKind::Read(_) => JobAction::Read,
            JobKind::Write(_) => JobAction::Write,
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
            JobKind::Hash(mut j) => (j.handler)(
                j.piece,
                j.piece_hash,
                std::mem::take(&mut j.block_hashes),
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
