//! Port of `storage_error.hpp` — error carrying which operation
//! failed and on which file, matching upstream `storage_error`
//! (ec + file + operation_t).

use std::fmt;
use std::io;

use bitdaemon_common::types::FileIndex;

/// `libtorrent::operation_t` — the operation that failed.
/// Enum order matches upstream `operation_t` values used by alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// `operation_t::unknown`.
    Unknown,
    /// `operation_t::file_open`.
    FileOpen,
    /// `operation_t::file_read`.
    FileRead,
    /// `operation_t::file_write`.
    FileWrite,
    /// `operation_t::file_seek`.
    FileSeek,
    /// `operation_t::file_stat`.
    FileStat,
    /// `operation_t::file_rename`.
    FileRename,
    /// `operation_t::file_remove`.
    FileRemove,
    /// `operation_t::file_copy`.
    FileCopy,
    /// `operation_t::file_hard_link`.
    FileHardLink,
    /// `operation_t::file_fallocate`.
    FileFallocate,
    /// `operation_t::file_fsync`.
    FileFsync,
    /// `operation_t::mkdir`.
    Mkdir,
    /// `operation_t::check_resume`.
    CheckResume,
    /// `operation_t::alloc_cache_piece`.
    AllocCachePiece,
    /// `operation_t::partfile_*` / misc disk ops.
    PartfileWrite,
    /// see above.
    PartfileRead,
    /// see above.
    PartfileMove,
    /// see above.
    CheckFiles,
    /// `operation_t::save_resume_data`.
    SaveResumeData,
}

/// `lt::storage_error` — which op failed, on which file, and the
/// underlying `io::Error`.
#[derive(Debug)]
pub struct StorageError {
    /// `error_code` value (kind + raw OS code preserved).
    pub io: io::Error,
    /// `file()` — torrent file index the op was acting on (-1 = none).
    pub file: FileIndex,
    /// `operation()`.
    pub operation: Operation,
}

impl StorageError {
    /// Construct from an `io::Error`.
    pub fn new(io: io::Error, file: FileIndex, operation: Operation) -> Self {
        Self {
            io,
            file,
            operation,
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} on file {} failed: {}",
            self.operation, self.file.0, self.io
        )
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.io)
    }
}

/// `storage_error` default — no error recorded yet.
impl Default for StorageError {
    fn default() -> Self {
        Self {
            io: io::Error::from(io::ErrorKind::Other),
            file: FileIndex(-1),
            operation: Operation::Unknown,
        }
    }
}

/// Convenience alias.
pub type StorageResult<T> = Result<T, StorageError>;
