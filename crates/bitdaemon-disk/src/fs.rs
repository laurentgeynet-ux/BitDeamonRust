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
use std::path::{Component, Path, PathBuf};

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

/// `aux::copy_file` — a real copy, never a link: upstream's loop
/// (`copy_file.cpp`) copies the file contents and preserves sparse
/// regions via `SEEK_DATA`/`SEEK_HOLE` or
/// `FSCTL_QUERY_ALLOCATED_RANGES`. A hard link would share the inode
/// — the move/rollback callers expect an *independent* destination
/// (upstream's move_storage only deletes the source *after* the copy
/// completes, and `dont_replace` bookkeeping assumes distinct
/// entries). `fs::copy` delegates to `copy_file_range`/`CopyFile`,
/// which do the sector-aware copy in-kernel — upstream's manual
/// buffer sizing (`PhysicalBytesPerSectorForPerformance`) exists only
/// for the userspace loop and has nothing to port.
pub fn copy_file(src: &Path, dst: &Path) -> io::Result<()> {
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

/// `aux::drive_info` — rotational/solid-state class of the volume
/// hosting a path (`drive_info.cpp`). Only the mmap backend consumes
/// it: `remote` disables full allocation and `auto_mmap_write` keys
/// off `SsdDax`. (Distinct from [`drive_info`] which reports free
/// space — upstream overloads the name too.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DriveKind {
    /// `drive_info::spinning` — upstream's conservative fallback for
    /// every detection failure.
    #[default]
    Spinning,
    /// `drive_info::ssd_disk` — flash without DAX.
    SsdDisk,
    /// `drive_info::ssd_dax` — DAX or RAM-backed volume (tmpfs/ramfs
    /// on linux, `DRIVE_RAMDISK`/`FILE_DAX_VOLUME` on Windows).
    SsdDax,
    /// `drive_info::remote` — network filesystem (NFS/SMB/FUSE),
    /// where mmap writes are unreliable upstream.
    Remote,
}

/// `aux::get_drive_info` — classify the volume hosting `path`.
/// Failures return `Spinning`, upstream's conservative default.
#[must_use]
pub fn drive_kind(path: &Path) -> DriveKind {
    drive_kind_impl(path)
}

/// linux — `statfs` magic + `/sys/block` rotational/dax scan
/// (drive_info.cpp:99-175). Sole `unsafe` site beside `file::posix`/
/// `win32` (ADR-0022 §3): one read-only `statfs` FFI call.
#[cfg(target_os = "linux")]
fn drive_kind_impl(path: &Path) -> DriveKind {
    linux_drive::classify(path)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod linux_drive {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    use super::DriveKind;

    // filesystem magics — `linux/magic.h` (upstream defines them
    // locally for old kernel headers, drive_info.cpp:106-109).
    const TMPFS_MAGIC: u64 = 0x0102_1994;
    const RAMFS_MAGIC: u64 = 0x8584_58f6;
    const FUSE_MAGIC: u64 = 0x6573_5546;
    const NFS_MAGIC: u64 = 0x6969;
    const SMB_MAGIC: u64 = 0x517B;
    const CIFS_MAGIC: u64 = 0xFF53_4D42;
    const SMB2_MAGIC: u64 = 0xFE53_4D42;

    pub(super) fn classify(path: &Path) -> DriveKind {
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
            return DriveKind::Spinning;
        };
        // SAFETY: `c_path` is a live NUL-terminated string; `st` is a
        // valid out-param of statfs.
        let mut st = unsafe { std::mem::zeroed::<libc::statfs>() };
        if unsafe { libc::statfs(c_path.as_ptr(), &mut st) } == 0 {
            // f_type is signed (__fsword_t); the magics are unsigned —
            // upstream casts to unsigned for the comparison.
            #[allow(clippy::cast_sign_loss)]
            let f_type = st.f_type as u64;
            // tmpfs/ramfs are RAM-backed — mmap is free there.
            if f_type == TMPFS_MAGIC || f_type == RAMFS_MAGIC {
                return DriveKind::SsdDax;
            }
            // fuse/nfs/smb/cifs don't handle memory-mapped files well.
            if f_type == FUSE_MAGIC
                || f_type == NFS_MAGIC
                || f_type == SMB_MAGIC
                || f_type == CIFS_MAGIC
                || f_type == SMB2_MAGIC
            {
                return DriveKind::Remote;
            }
        }
        // map st_dev → /sys/block/<dev>, then `queue/rotational`/`dax`.
        let Ok(md) = std::fs::metadata(path) else {
            return DriveKind::Spinning;
        };
        // SAFETY: pure arithmetic on a dev_t.
        let device_id = unsafe { format!("{}:{}\n", libc::major(md.dev()), libc::minor(md.dev())) };
        let Ok(entries) = std::fs::read_dir("/sys/block") else {
            return DriveKind::Spinning;
        };
        for entry in entries.flatten() {
            let base = entry.path();
            let Ok(devno) = std::fs::read_to_string(base.join("dev")) else {
                continue;
            };
            if devno != device_id {
                continue;
            }
            match std::fs::read_to_string(base.join("queue/rotational")).as_deref() {
                Ok("1\n") => return DriveKind::Spinning,
                Ok("0\n") => {
                    return if std::fs::read_to_string(base.join("queue/dax")).as_deref()
                        == Ok("1\n")
                    {
                        DriveKind::SsdDax
                    } else {
                        DriveKind::SsdDisk
                    };
                }
                _ => continue,
            }
        }
        DriveKind::Spinning
    }
}

/// windows — `GetVolumePathNameW` + `GetDriveTypeW` +
/// `FILE_DAX_VOLUME` (drive_info.cpp:179-206). The upstream
/// seek-penalty `DeviceIoControl` chain only distinguishes `ssd_disk`
/// from `spinning` — a distinction the sole consumer (`auto` mmap
/// writes / remote check) never makes — so we return the conservative
/// default it would produce.
#[cfg(windows)]
fn drive_kind_impl(path: &Path) -> DriveKind {
    win32_drive::classify(path)
}

/// Scoped `#[allow(unsafe_code)]` for the volume-classification FFI —
/// same ADR-0022 §3 carve-out as `file::{win32,posix}`: read-only
/// volume queries, no safe-std equivalent.
#[cfg(windows)]
#[allow(unsafe_code)]
mod win32_drive {
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    use windows_sys::Win32::Storage::FileSystem::{
        GetDriveTypeW, GetVolumeInformationW, GetVolumePathNameW,
    };

    use super::DriveKind;

    // winbase.h — stable API constants.
    const DRIVE_REMOTE: u32 = 4;
    const DRIVE_RAMDISK: u32 = 6;
    const FILE_DAX_VOLUME: u32 = 0x2000_0000;

    pub fn classify(path: &Path) -> DriveKind {
        let mut native: Vec<u16> = path.as_os_str().encode_wide().collect();
        native.push(0);
        let mut volume = [0u16; 300];
        // SAFETY: NUL-terminated `native`; `volume` is a writable
        // 300-wchar buffer per the API contract.
        unsafe {
            if GetVolumePathNameW(native.as_ptr(), volume.as_mut_ptr(), volume.len() as u32) == 0 {
                return DriveKind::Spinning;
            }
            match GetDriveTypeW(volume.as_ptr()) {
                DRIVE_REMOTE => return DriveKind::Remote,
                DRIVE_RAMDISK => return DriveKind::SsdDax,
                _ => {}
            }
            let mut fs_flags = 0u32;
            if GetVolumeInformationW(
                volume.as_ptr(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut fs_flags,
                std::ptr::null_mut(),
                0,
            ) != 0
                && fs_flags & FILE_DAX_VOLUME != 0
            {
                return DriveKind::SsdDax;
            }
        }
        DriveKind::Spinning
    }
}

/// non-linux unix + everything else — upstream returns `spinning`
/// (drive_info.cpp:283-286).
#[cfg(all(unix, not(target_os = "linux")))]
fn drive_kind_impl(_path: &Path) -> DriveKind {
    DriveKind::Spinning
}

/// `aux::canonicalize_path` (path.cpp:770 — `TORRENT_USE_UNC_PATHS`)
/// — a *lexical* cleanup: `.` elements are dropped, `..` pops the
/// previous element. No filesystem access and no symlink resolution —
/// upstream never stats the path.
pub fn canonicalize_path(f: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in f.components() {
        match c {
            Component::CurDir => {}
            // upstream erases the `..` even at the head of the path —
            // `pop()` on an empty/rooted path is a no-op, same effect.
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// `aux::complete` — make relative paths absolute against the cwd;
/// a leading `./` element is stripped first (path.cpp:933-934).
#[must_use]
pub fn complete(f: &Path) -> PathBuf {
    if f.is_absolute() {
        return f.to_path_buf();
    }
    let f: PathBuf = match f.components().next() {
        Some(Component::CurDir) => f.components().skip(1).collect(),
        _ => f.to_path_buf(),
    };
    current_working_directory().join(f)
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
