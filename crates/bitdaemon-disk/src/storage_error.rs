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
    /// `fsync`/`FlushFileBuffers` failure — our extension (upstream
    /// reports it under `file`/`file_write`).
    FileFsync,
    /// `operation_t::file_truncate` — `ftruncate`/`SetEndOfFile`.
    FileTruncate,
    /// `operation_t::mkdir`.
    Mkdir,
    /// `operation_t::symlink` — create/readlink of a symlink.
    Symlink,
    /// `operation_t::check_resume`.
    CheckResume,
    /// `operation_t::file_mmap` — `mmap`/`MapViewOfFile` failure.
    FileMmap,
    /// `operation_t::alloc_cache_piece`.
    AllocCachePiece,
    /// `operation_t::partfile_*` / misc disk ops.
    PartfileWrite,
    /// see above.
    PartfileRead,
    /// see above.
    PartfileMove,
    /// recheck failures — our extension (upstream `check_resume`).
    CheckFiles,
    /// resume-data write failure — our extension.
    SaveResumeData,
}

impl Operation {
    /// `operation_name()` — upstream alert text for this op
    /// (alert.cpp `names[]`; `FileFsync` is our extension for sync
    /// failures, upstream reports them under `file`).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::FileOpen => "file_open",
            Self::FileRead => "file_read",
            Self::FileWrite => "file_write",
            Self::FileSeek => "file_seek",
            Self::FileStat => "file_stat",
            Self::FileRename => "file_rename",
            Self::FileRemove => "file_remove",
            Self::FileCopy => "file_copy",
            Self::FileHardLink => "file_hard_link",
            Self::FileFallocate => "file_fallocate",
            Self::FileFsync => "file_fsync",
            Self::FileTruncate => "file_truncate",
            Self::Mkdir => "mkdir",
            Self::Symlink => "symlink",
            Self::CheckResume => "check_resume",
            Self::FileMmap => "file_mmap",
            Self::AllocCachePiece => "alloc_cache_piece",
            Self::PartfileWrite => "partfile_write",
            Self::PartfileRead => "partfile_read",
            Self::PartfileMove => "partfile_move",
            Self::CheckFiles => "check_files",
            Self::SaveResumeData => "save_resume_data",
        }
    }
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
    /// `!storage_error` — success sentinel (`default()` = no error).
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.file == FileIndex(-1) && self.operation == Operation::Unknown
    }

    /// `error_code` present — the op failed.
    #[must_use]
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }

    /// Construct from an `io::Error`.
    pub fn new(io: io::Error, file: FileIndex, operation: Operation) -> Self {
        Self {
            io,
            file,
            operation,
        }
    }
}

/// Manual `Clone` — `io::Error` isn't `Clone`; reconstruct it with
/// the same kind and rendered message (raw OS code text becomes the
/// kind's generic text — upstream `error_code` copies preserve
/// code+category, we preserve kind+message).
impl Clone for StorageError {
    fn clone(&self) -> Self {
        Self {
            io: io::Error::new(self.io.kind(), self.io.to_string()),
            file: self.file,
            operation: self.operation,
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} on file {} failed: {}",
            self.operation.name(),
            self.file.0,
            self.io
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
