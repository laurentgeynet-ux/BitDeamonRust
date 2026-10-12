//! Port of the filesystem half of `aux_/path.hpp`/`path.cpp`,
//! `directory.cpp`, `copy_file.cpp`, `drive_info.cpp`.
//!
//! The pure string helpers (`lsplit_path`, `combine_path`, …) live in
//! `bitdaemon_core::path`; this module covers everything that touches
//! the OS. Upstream takes `std::string` paths (native narrow); Rust
//! uses `&Path` — conversion from torrent-internal byte paths is the
//! caller's job (`bitdaemon_core::path::bytes_to_path`).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use bitdaemon_common::bitfield_flag;

bitfield_flag!(
    /// `aux::file_attributes_t`.
    FileAttributes, u32
);
impl FileAttributes {
    /// `file_status::directory` — the entry is a directory.
    pub const DIRECTORY: Self = Self::bit(0);
    /// `hidden`.
    pub const HIDDEN: Self = Self::bit(1);
    /// `executable`.
    pub const EXECUTABLE: Self = Self::bit(2);
    /// `symlink`.
    pub const SYMLINK: Self = Self::bit(3);
}

bitfield_flag!(
    /// `aux::file_status_flag_t`.
    FileStatusFlags, u32
);
impl FileStatusFlags {
    /// `dont_follow_links`.
    pub const DONT_FOLLOW_LINKS: Self = Self::bit(0);
}

/// `aux::file_status` — portable stat result.
#[derive(Debug, Clone, Default)]
pub struct FileStatus {
    /// `file_size` — 0 for directories.
    pub file_size: i64,
    /// `atime` — seconds since epoch.
    pub atime: u64,
    /// `mtime` — seconds since epoch.
    pub mtime: u64,
    /// `ctime` — seconds since epoch.
    pub ctime: u64,
    /// `mode` — attribute bitmask.
    pub mode: FileAttributes,
}

fn to_unix(t: io::Result<std::time::SystemTime>) -> u64 {
    t.ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `aux::stat_file`.
pub fn stat_file(f: &Path, flags: FileStatusFlags) -> io::Result<FileStatus> {
    let meta = if flags.contains(FileStatusFlags::DONT_FOLLOW_LINKS) {
        fs::symlink_metadata(f)?
    } else {
        fs::metadata(f)?
    };
    let mut mode = FileAttributes::NONE;
    if meta.is_dir() {
        mode |= FileAttributes::DIRECTORY;
    }
    if meta.file_type().is_symlink() {
        mode |= FileAttributes::SYMLINK;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o111 != 0 {
            mode |= FileAttributes::EXECUTABLE;
        }
        let file_name_hidden = f
            .file_name()
            .map(|n| {
                use std::os::unix::ffi::OsStrExt;
                n.as_bytes().first() == Some(&b'.')
            })
            .unwrap_or(false);
        if file_name_hidden {
            mode |= FileAttributes::HIDDEN;
        }
        let _ = meta;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if meta.file_attributes() & 0x2 != 0 {
            // FILE_ATTRIBUTE_HIDDEN
            mode |= FileAttributes::HIDDEN;
        }
    }
    let size = if meta.is_dir() { 0 } else { meta.len() };
    Ok(FileStatus {
        file_size: size.try_into().unwrap_or(i64::MAX),
        atime: to_unix(meta.accessed()),
        mtime: to_unix(meta.modified()),
        ctime: {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                u64::try_from(meta.ctime()).unwrap_or(0)
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                // Windows FileTime (100ns since 1601) → unix seconds
                (meta.creation_time() / 10_000_000).saturating_sub(11_644_473_600)
            }
        },
        mode,
    })
}

/// `aux::rename` — must overwrite the destination (POSIX `rename`).
/// `std::fs::rename` already replaces on Windows (MoveFileEx
/// `MOVEFILE_REPLACE_EXISTING`-equivalent) and POSIX.
pub fn rename(f: &Path, newf: &Path) -> io::Result<()> {
    fs::rename(f, newf)
}

/// `aux::create_directories`.
pub fn create_directories(f: &Path) -> io::Result<()> {
    fs::create_dir_all(f)
}

/// `aux::create_directory` — succeeds if the directory already exists.
pub fn create_directory(f: &Path) -> io::Result<()> {
    match fs::create_dir(f) {
        Ok(()) => Ok(()),
        Err(_) if f.is_dir() => Ok(()),
        Err(e) => Err(e),
    }
}

/// `aux::remove` — removes a file or *empty* directory.
pub fn remove(f: &Path) -> io::Result<()> {
    match fs::remove_file(f) {
        Ok(()) => Ok(()),
        Err(_) if f.is_dir() => fs::remove_dir(f),
        Err(e) => Err(e),
    }
}

/// `aux::exists` — true if `f` exists (files and dirs).
#[must_use]
pub fn exists(f: &Path, flags: FileStatusFlags) -> bool {
    stat_file(f, flags).is_ok()
}

/// `aux::is_directory`.
#[must_use]
pub fn is_directory(f: &Path, flags: FileStatusFlags) -> bool {
    stat_file(f, flags)
        .map(|s| s.mode.contains(FileAttributes::DIRECTORY))
        .unwrap_or(false)
}

/// `aux::hard_link` — creates `dst` as a hard link to `src`.
/// Upstream fails with `EEXIST`/`ERROR_FILE_EXISTS` when `dst`
/// exists; `std::fs::hard_link` matches.
pub fn hard_link(src: &Path, dst: &Path) -> io::Result<()> {
    fs::hard_link(src, dst)
}

/// `aux::copy_file` — upstream tries `CopyFile`, then hard-link
/// heuristics on POSIX. The observable contract: `dst` is a byte-for-
/// byte copy of `src` after return; on failure `dst` may not exist.
/// We try hard-link first (cheap, same-fs case) then fall back to a
/// real copy — matching upstream's preference order.
pub fn copy_file(src: &Path, dst: &Path) -> io::Result<()> {
    if hard_link(src, dst).is_ok() {
        return Ok(());
    }
    fs::copy(src, dst).map(|_| ())
}

/// `aux::drive_info` — filesystem free-space info.
#[derive(Debug, Clone, Copy, Default)]
pub struct DriveInfo {
    /// `available_space` — quota-adjusted space usable by us.
    pub available_space: i64,
    /// `free_space` — total free blocks on the volume.
    pub free_space: i64,
    /// `capacity` — total size of the volume.
    pub capacity: i64,
}

/// `aux::free_space`/`drive_info` — stat the volume containing `path`.
pub fn drive_info(path: &Path) -> io::Result<DriveInfo> {
    Ok(DriveInfo {
        available_space: i64::try_from(fs2::available_space(path)?).unwrap_or(i64::MAX),
        free_space: i64::try_from(fs2::free_space(path)?).unwrap_or(i64::MAX),
        capacity: i64::try_from(fs2::total_space(path)?).unwrap_or(i64::MAX),
    })
}

/// `aux::canonicalize_path` — resolve `.`/`..`/links to an absolute
/// normalized path. Falls back to the input when the path doesn't
/// exist yet (upstream tolerates this for files being created).
pub fn canonicalize_path(f: &Path) -> PathBuf {
    fs::canonicalize(f).unwrap_or_else(|_| complete(f))
}

/// `aux::complete` — make relative paths absolute against the cwd.
#[must_use]
pub fn complete(f: &Path) -> PathBuf {
    if f.is_absolute() {
        f.to_path_buf()
    } else {
        current_working_directory().join(f)
    }
}

/// `aux::current_working_directory`.
pub fn current_working_directory() -> PathBuf {
    std::env::current_dir().unwrap_or_default()
}

/// `aux::remove_all` — recursive delete.
pub fn remove_all(f: &Path) -> io::Result<()> {
    if f.is_dir() && !f.is_symlink() {
        fs::remove_dir_all(f)
    } else {
        fs::remove_file(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_and_exists() {
        let dir = std::env::temp_dir().join("bitdaemon_fs_test");
        let _ = fs::remove_dir_all(&dir);
        create_directories(&dir).unwrap();
        let file = dir.join("f.bin");
        fs::write(&file, b"hello").unwrap();
        assert!(exists(&file, FileStatusFlags::NONE));
        let st = stat_file(&file, FileStatusFlags::NONE).unwrap();
        assert_eq!(st.file_size, 5);
        assert!(!st.mode.contains(FileAttributes::DIRECTORY));
        assert!(is_directory(&dir, FileStatusFlags::NONE));
        let dst = dir.join("g.bin");
        copy_file(&file, &dst).unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"hello");
        rename(&dst, &dir.join("h.bin")).unwrap();
        assert!(exists(&dir.join("h.bin"), FileStatusFlags::NONE));
        remove(&dir.join("h.bin")).unwrap();
        remove_all(&dir).unwrap();
        assert!(!exists(&dir, FileStatusFlags::NONE));
    }

    #[test]
    fn drive_info_smoke() {
        let info = drive_info(&std::env::temp_dir()).unwrap();
        assert!(info.free_space > 0);
        assert!(info.capacity > 0);
    }
}
