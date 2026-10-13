//! Port of `aux_/file.hpp`/`file.cpp` — positioned-I/O file handle.
//!
//! Upstream exposes raw `handle_type` (fd/HANDLE) plus free functions
//! `pwrite_all`/`pread_all`/`pwritev_all`. Here the handle is a
//! `std::fs::File` and the free functions take `&File`, matching the
//! "write the whole buffer or fail" loop semantics (a zero-byte
//! transfer maps to `UnexpectedEof`, upstream `file_too_short`/EOF).
//!
//! Upstream `open_mode::truncate` does NOT mean `O_TRUNC`/`TRUNCATE_EXISTING`
//! — the file is never truncated to zero at `open(2)`/`CreateFileW` time.
//! It means "set the file to its final `size`", applied once per session
//! (the storage's `m_file_created` bitset gates it): POSIX always
//! `ftruncate`s — sparse files keep full `st_size` with holes — then
//! `posix_fallocate`s when the file isn't sparse; Windows extends +
//! `SetFileValidData` only when `enable_set_file_valid_data` opted in
//! (otherwise files grow organically and NTFS zero-fills each extension).

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use crate::open_mode::OpenMode;
use crate::storage_error::Operation;

/// Error from [`FileHandle::open`]/[`open_raw`] — carries the upstream
/// `operation_t` that failed so `StorageError`/`file_error_alert`
/// reports `file_open`/`file_truncate`/`file_fallocate`/`file_seek`
/// like upstream's thrown `storage_error`.
#[derive(Debug)]
pub struct OpenError {
    /// `operation_t` — which step failed.
    pub operation: Operation,
    /// underlying OS error.
    pub source: io::Error,
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} failed: {}", self.operation, self.source)
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// `aux::file_handle` — owns the descriptor; closes on drop.
#[derive(Debug)]
pub struct FileHandle {
    file: File,
    /// `m_open_mode` — kept so `Drop` can clear the sparse attribute
    /// on a write file that never grew sparse regions (upstream
    /// `file_handle::close`, Windows only).
    #[cfg_attr(not(windows), allow(dead_code))]
    mode: OpenMode,
}

impl FileHandle {
    /// `file_handle(name, size, mode)` — open `name`; when `mode` has
    /// `truncate`, size the file to `size` bytes and, for non-sparse
    /// files, reserve the space (`posix_fallocate` on POSIX,
    /// `SetEndOfFile` + `SetFileValidData` on Windows — upstream
    /// file.cpp:584-698). `size` is ignored without `truncate`.
    pub fn open(name: &Path, size: u64, mode: OpenMode) -> Result<Self, OpenError> {
        let file = open_raw(name, mode)?;
        if mode.contains(OpenMode::TRUNCATE) {
            truncate_size(&file, size, mode)?;
        }
        Ok(Self { file, mode })
    }

    /// Access the underlying `File`. Positioned I/O on it is
    /// thread-safe on every platform: `pread`/`pwrite` on unix, and
    /// `ReadFile`/`WriteFile` with an `OVERLAPPED` offset on Windows
    /// (`win32::pread`/`pwrite`) — neither touches the shared file
    /// pointer, so no lock is needed (upstream file.cpp:198/234).
    #[must_use]
    pub fn file(&self) -> &File {
        &self.file
    }

    /// `get_size()`.
    pub fn get_size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        // file.cpp:730-749 — a write-opened file marked sparse that
        // never actually grew sparse regions gets the attribute
        // cleared (Vista+); best-effort like upstream.
        #[cfg(windows)]
        if self.mode.contains(OpenMode::WRITE)
            && self.mode.contains(OpenMode::SPARSE)
            && !win32::is_sparse(&self.file)
        {
            win32::clear_sparse(&self.file);
        }
    }
}

/// Shared open path — same `OpenOptions` mapping, `SeManageVolumePrivilege`
/// pre-acquire and post-open hints as [`FileHandle::open`], minus the
/// `truncate` sizing pass, for callers managing their own handle type
/// (the `bitdaemon-mmap` view pool, part files).
pub fn open_raw(name: &Path, mode: OpenMode) -> Result<File, OpenError> {
    #[cfg(windows)]
    if mode.contains(OpenMode::ALLOW_SET_FILE_VALID_DATA) {
        // file.cpp:490-499 — the privilege must be in the process
        // token before `CreateFileW` for `SetFileValidData` to work.
        win32::ensure_manage_volume_privilege();
    }
    let file = open_with_retry(name, mode)?;
    #[cfg(windows)]
    {
        // file.cpp:578 — FSCTL_SET_SPARSE on write opens marked
        // sparse; failure tolerated (non-NTFS volumes).
        if mode.contains(OpenMode::WRITE) && mode.contains(OpenMode::SPARSE) {
            win32::set_sparse(&file);
        }
    }
    #[cfg(unix)]
    {
        // file.cpp:700-719 — post-open cache hints: F_NOCACHE/
        // F_NODIRECT (apple), fadvise(SEQUENTIAL). Upstream runs them
        // after `ftruncate`; the order is advisory-only either way.
        if mode.contains(OpenMode::NO_CACHE) {
            posix::set_nocache(&file);
        }
        if mode.contains(OpenMode::SEQUENTIAL_ACCESS) {
            posix::fadvise_sequential(&file);
        }
    }
    Ok(file)
}

/// `OpenOptions::open` + upstream's `O_NOATIME` retry (file.cpp:548-556):
/// the flag can fail for files we don't own; it's best-effort.
fn open_with_retry(name: &Path, mode: OpenMode) -> Result<File, OpenError> {
    let res = open_with_options(name, mode);
    #[cfg(unix)]
    let res = match res {
        Err(_) if mode.contains(OpenMode::NO_ATIME) => {
            open_with_options(name, mode & !OpenMode::NO_ATIME)
        }
        r => r,
    };
    res.map_err(|source| OpenError {
        operation: Operation::FileOpen,
        source,
    })
}

/// `OpenOptions` mapping for `mode`. Upstream never maps `truncate` to
/// `O_TRUNC`/`TRUNCATE_EXISTING` — the file is sized explicitly after
/// open (see [`truncate_size`]).
fn open_with_options(name: &Path, mode: OpenMode) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true);
    if mode.contains(OpenMode::WRITE) {
        opts.write(true).create(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut custom = 0;
        if mode.contains(OpenMode::NO_CACHE) {
            // file.cpp:525 — no_cache → O_SYNC (synchronous writes,
            // no dirty pages) — not O_DIRECT, which would require
            // aligned I/O.
            custom |= libc::O_SYNC;
        }
        if mode.contains(OpenMode::NO_ATIME) {
            custom |= posix::O_NOATIME;
        }
        if custom != 0 {
            opts.custom_flags(custom);
        }
        if mode.contains(OpenMode::EXECUTABLE) {
            // file_perms: 0666 | exec → 0777, filtered by umask.
            opts.mode(0o777);
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // winbase.h constants — avoids a windows-sys dep for 2 values
        const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
        const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
        const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
        // upstream file_flags() — deliberately no
        // FILE_FLAG_RANDOM_ACCESS (that flag stops the cache manager
        // from trimming pages; KB2549369), no BACKUP_SEMANTICS, and
        // `no_atime` has no Windows mapping upstream.
        let mut flags = 0;
        if mode.contains(OpenMode::NO_CACHE) {
            flags |= FILE_FLAG_WRITE_THROUGH;
        }
        if mode.contains(OpenMode::SEQUENTIAL_ACCESS) {
            flags |= FILE_FLAG_SEQUENTIAL_SCAN;
        }
        opts.custom_flags(flags);
        if mode.contains(OpenMode::HIDDEN) {
            opts.attributes(FILE_ATTRIBUTE_HIDDEN);
        }
    }
    opts.open(name)
}

/// `file.cpp:617-698` — the `truncate` path: `ftruncate` to `size`
/// (sparse files included — they keep a full `st_size` made of holes),
/// then `posix_fallocate`/`F_PREALLOCATE` for non-sparse files.
/// `no_cow` must land on the still-empty file, before `ftruncate`.
#[cfg(unix)]
fn truncate_size(file: &File, size: u64, mode: OpenMode) -> Result<(), OpenError> {
    if mode.contains(OpenMode::NO_COW) {
        set_no_cow(file);
    }
    file.set_len(size).map_err(|source| OpenError {
        operation: Operation::FileTruncate,
        source,
    })?;
    if !mode.contains(OpenMode::SPARSE) {
        fallocate(file, size).map_err(|source| OpenError {
            operation: Operation::FileFallocate,
            source,
        })?;
    }
    Ok(())
}

/// `file.cpp:584-601` — Windows extends + `SetFileValidData` only when
/// `enable_set_file_valid_data` opted in (upstream default is off):
/// otherwise the file stays small and NTFS zero-fills each extension.
/// `SetFilePointerEx` and `SetEndOfFile` are separate calls upstream so
/// they report separate operations (`file_seek`/`file_truncate`).
#[cfg(windows)]
fn truncate_size(file: &File, size: u64, mode: OpenMode) -> Result<(), OpenError> {
    if !mode.contains(OpenMode::SPARSE) && mode.contains(OpenMode::ALLOW_SET_FILE_VALID_DATA) {
        win32::set_end_of_file(file, size)?;
        win32::set_file_valid_data(file, size);
    }
    Ok(())
}

/// Fallback for platforms with neither POSIX nor Windows allocation.
#[cfg(not(any(unix, windows)))]
fn truncate_size(file: &File, size: u64, _mode: OpenMode) -> Result<(), OpenError> {
    file.set_len(size).map_err(|source| OpenError {
        operation: Operation::FileTruncate,
        source,
    })
}

fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buf, offset)
    }
    #[cfg(windows)]
    {
        win32::pread(file, buf, offset)
    }
}

fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_at(buf, offset)
    }
    #[cfg(windows)]
    {
        win32::pwrite(file, buf, offset)
    }
}

/// `aux::pwrite_all` — write the entire `buf` at `file_offset`,
/// retrying on short writes; error on EOF (zero-byte write).
pub fn pwrite_all(file: &File, buf: &[u8], file_offset: u64) -> io::Result<usize> {
    let mut ret = 0;
    let mut offset = file_offset;
    let mut rest = buf;
    while !rest.is_empty() {
        let n = write_at(file, rest, offset)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "file too short"));
        }
        ret += n;
        offset += n as u64;
        rest = &rest[n..];
    }
    Ok(ret)
}

/// `aux::pwritev_all` — consecutive buffer writes at increasing
/// offsets (upstream falls back to per-buffer `pwrite_all`).
pub fn pwritev_all(file: &File, bufs: &[&[u8]], file_offset: u64) -> io::Result<usize> {
    let mut ret = 0;
    let mut offset = file_offset;
    for b in bufs {
        ret += pwrite_all(file, b, offset)?;
        offset += b.len() as u64;
    }
    Ok(ret)
}

/// `aux::pread_all` — read until `buf` is full; a zero-byte read is
/// `UnexpectedEof` (upstream returns bytes so far with `eof` set —
/// callers treat `n < buf.len()` as short; we surface it as an error
/// carrying the partial count via `bytes_read` in `read_all`).
pub fn pread_all(file: &File, buf: &mut [u8], file_offset: u64) -> io::Result<usize> {
    let mut ret = 0;
    let mut offset = file_offset;
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = read_at(file, &mut buf[filled..], offset)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
        }
        ret += n;
        offset += n as u64;
        filled += n;
    }
    Ok(ret)
}

/// Upstream-compatible variant: returns bytes actually read even on
/// EOF (no error for a short final read).
pub fn pread_best_effort(file: &File, buf: &mut [u8], file_offset: u64) -> io::Result<usize> {
    let mut ret = 0;
    let mut offset = file_offset;
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = read_at(file, &mut buf[filled..], offset)?;
        if n == 0 {
            break;
        }
        ret += n;
        offset += n as u64;
        filled += n;
    }
    Ok(ret)
}

/// `aux::advise_dont_need` — hint the OS to drop the range from the
/// page cache (`posix_fadvise(POSIX_FADV_DONTNEED)` upstream).
/// Advisory; upstream no-ops where `POSIX_FADV_DONTNEED` doesn't
/// exist (apple, solaris), same gates here.
pub fn advise_dont_need(file: &File, offset: u64, len: u64) {
    #[cfg(unix)]
    posix::fadvise_dontneed(file, offset, len);
    #[cfg(windows)]
    let _ = (file, offset, len);
}

/// `aux::sync_file` — `fdatasync`/`FlushFileBuffers` over the range
/// (upstream flushes the whole file on platforms without ranged sync).
pub fn sync_file(file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    file.sync_data()
}

/// `file.cpp:647` — physically reserve the file's blocks
/// (`posix_fallocate`, `F_PREALLOCATE` on apple). EINVAL/ENOTSUP
/// (filesystem without support, e.g. NFS) are tolerated upstream —
/// the file just stays `ftruncate`-sized; everything else (ENOSPC!)
/// propagates as `file_fallocate`. Exposed for `truncate_size` and
/// the mmap backend.
#[cfg(unix)]
pub fn fallocate(file: &File, size: u64) -> io::Result<()> {
    posix::fallocate(file, size)
}

/// `file.cpp:622-631` — `FS_IOC_SETFLAGS|FS_NOCOW_FL` on the still
/// empty file (Linux/Android; btrfs CoW opt-out). No-op elsewhere.
#[cfg(unix)]
pub fn set_no_cow(file: &File) {
    posix::set_no_cow(file);
}

/// `file.cpp:587-593` — `SetFilePointerEx` + `SetEndOfFile`, the
/// two-step Windows resize. `OpenError` ops mirror upstream:
/// `file_seek` for the pointer move, `file_truncate` for the EOF
/// set. Exposed for the mmap backend.
#[cfg(windows)]
pub fn set_end_of_file(file: &File, size: u64) -> Result<(), OpenError> {
    win32::set_end_of_file(file, size)
}

/// `file.cpp:600` — `SetFileValidData` after the file has been
/// extended; needs `SeManageVolumePrivilege` (enabled at open,
/// best-effort). Exposed for the mmap backend.
#[cfg(windows)]
pub fn set_file_valid_data(file: &File, size: u64) {
    win32::set_file_valid_data(file, size);
}

/// `file.cpp:347` — `FSCTL_QUERY_ALLOCATED_RANGES`: true when the
/// file has unallocated regions (or the query fails, like upstream).
#[cfg(windows)]
pub fn is_sparse(file: &File) -> bool {
    win32::is_sparse(file)
}

/// `file.cpp:745-748` — `FSCTL_SET_SPARSE` with `SetSparse=FALSE`.
#[cfg(windows)]
pub fn clear_sparse(file: &File) {
    win32::clear_sparse(file);
}

/// POSIX file controls — `posix_fadvise`, `F_NOCACHE`/`F_NODIRECT`
/// fcntls, `O_NOATIME`, `posix_fallocate`, `FS_NOCOW_FL`. Sole `unsafe`
/// site beside `win32` in this crate (ADR-0022 §3): every FFI call is
/// reviewed, best-effort — upstream ignores hint failures too.
#[cfg(unix)]
#[allow(unsafe_code)]
mod posix {
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    /// `O_NOATIME` — linux/android only (upstream `#ifdef O_NOATIME`).
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub const O_NOATIME: i32 = libc::O_NOATIME;
    /// `O_NOATIME` — linux/android only (upstream `#ifdef O_NOATIME`).
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub const O_NOATIME: i32 = 0;

    /// `advise_dont_need` — errors ignored upstream.
    pub fn fadvise_dontneed(file: &File, offset: u64, len: u64) {
        // SAFETY: fd is borrowed for the call's duration only;
        // posix_fadvise is advisory and cannot corrupt state.
        // Platforms where libc lacks posix_fadvise (apple, solaris)
        // no-op — upstream compiles this out identically.
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "dragonfly"
        ))]
        unsafe {
            libc::posix_fadvise(
                file.as_raw_fd(),
                offset as libc::off_t,
                len as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            );
        }
        let _ = (file, offset, len);
    }

    /// open-time `sequential_access` hint — file.cpp:718, errors
    /// ignored upstream.
    pub fn fadvise_sequential(file: &File) {
        // SAFETY: see fadvise_dontneed.
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "dragonfly"
        ))]
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
        let _ = file;
    }

    /// `no_cache` post-open — BSD/Mac `F_NOCACHE` (+ `F_NODIRECT`:
    /// caching written pages temporarily is fine). Upstream
    /// file.cpp:702-710; Linux instead sets `O_SYNC` at open.
    /// Errors ignored upstream.
    pub fn set_nocache(file: &File) {
        // SAFETY: fcntl on a borrowed live fd, advisory hints only.
        #[cfg(target_vendor = "apple")]
        unsafe {
            libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
            libc::fcntl(file.as_raw_fd(), libc::F_NODIRECT, 1);
        }
        let _ = file;
    }

    /// `file.cpp:642-654` — `posix_fallocate(fd, 0, size)`: reserve the
    /// blocks physically so `storage_mode_allocate` really allocates
    /// and ENOSPC surfaces at first write-open. It returns the errno
    /// directly; EINVAL/ENOTSUP (filesystem without support, e.g.
    /// NFS) are tolerated upstream.
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    pub fn fallocate(file: &File, size: u64) -> io::Result<()> {
        // SAFETY: fd borrowed for the call's duration; reserving
        // blocks of an owned file cannot corrupt state.
        let ret = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, size as libc::off_t) };
        match ret {
            0 | libc::EINVAL | libc::ENOTSUP => Ok(()),
            e => Err(io::Error::from_raw_os_error(e)),
        }
    }

    /// `file.cpp:669-695` — apple `fcntl(F_PREALLOCATE)`: contiguous
    /// first; ENOSPC retries non-contiguous; EINVAL/ENOTSUP (APFS)
    /// skip silently.
    #[cfg(target_vendor = "apple")]
    pub fn fallocate(file: &File, size: u64) -> io::Result<()> {
        let fd = file.as_raw_fd();
        let mut fs: libc::fstore_t = unsafe { std::mem::zeroed() };
        fs.fst_flags = libc::F_ALLOCATECONTIG;
        fs.fst_posmode = libc::F_PEOFPOSMODE;
        fs.fst_offset = 0;
        fs.fst_length = size as libc::off_t;
        // SAFETY: fstore_t valid for the call's duration; fd borrowed.
        if unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &fs) } == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        if err == libc::EINVAL || err == libc::ENOTSUP {
            return Ok(());
        }
        if err != libc::ENOSPC {
            return Err(io::Error::from_raw_os_error(err));
        }
        // contiguous allocation failed for space — upstream retries
        // with F_ALLOCATEALL.
        fs.fst_flags = libc::F_ALLOCATEALL;
        if unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &fs) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// No preallocate primitive on this platform (solaris
    /// `F_ALLOCSP64` not ported — upstream `#elif` chain) — the file
    /// stays `ftruncate`-sized.
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_vendor = "apple"
    )))]
    pub fn fallocate(_file: &File, _size: u64) -> io::Result<()> {
        Ok(())
    }

    /// `file.cpp:622-631` — `FS_IOC_SETFLAGS|FS_NOCOW_FL`; must be set
    /// on the still-empty file, before `ftruncate`. Best-effort.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn set_no_cow(file: &File) {
        // linux/fs.h UAPI constants (stable ABI, not exported by libc
        // on all targets).
        const FS_IOC_GETFLAGS: libc::c_ulong = 0x8008_6601;
        const FS_IOC_SETFLAGS: libc::c_ulong = 0x4008_6602;
        const FS_NOCOW_FL: libc::c_int = 0x0080_0000;
        // SAFETY: get/set the flag word on a borrowed fd; both calls
        // best-effort like upstream.
        unsafe {
            let mut attr: libc::c_int = 0;
            if libc::ioctl(file.as_raw_fd(), FS_IOC_GETFLAGS, &mut attr) == -1 {
                return;
            }
            attr |= FS_NOCOW_FL;
            libc::ioctl(file.as_raw_fd(), FS_IOC_SETFLAGS, &attr);
        }
    }

    /// see above — no-op outside Linux/Android.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub fn set_no_cow(_file: &File) {}
}

/// Windows NTFS file controls — `FSCTL_SET_SPARSE` /
/// `FSCTL_QUERY_ALLOCATED_RANGES` and `SetFileValidData` have no
/// safe-std equivalent. Sole `unsafe` site of this crate
/// (ADR-0022 §3): every FFI call is reviewed, best-effort (errors
/// ignored like upstream).
#[cfg(windows)]
#[allow(unsafe_code)]
pub(crate) mod win32 {
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;
    use std::sync::Once;

    use crate::storage_error::Operation;
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_HANDLE_EOF, HANDLE};
    use windows_sys::Win32::Security::{
        AdjustTokenPrivileges, LUID_AND_ATTRIBUTES, LookupPrivilegeValueW, SE_MANAGE_VOLUME_NAME,
        SE_PRIVILEGE_ENABLED, TOKEN_ADJUST_PRIVILEGES, TOKEN_PRIVILEGES, TOKEN_QUERY,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileSizeEx, ReadFile, SetFileValidData, WriteFile,
    };
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::{
        FILE_ALLOCATED_RANGE_BUFFER, FILE_SET_SPARSE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
        FSCTL_SET_SPARSE,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    /// Positioned read — `file.cpp:198` upstream: `ReadFile` with the
    /// byte offset inside `OVERLAPPED`. The operation runs at the
    /// given offset without touching the handle's shared file
    /// pointer, so concurrent calls on one handle are race-free (the
    /// Windows equivalent of POSIX `pread`). Returns `Ok(0)` at EOF
    /// (`ERROR_HANDLE_EOF`), matching `FileExt::seek_read`.
    pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use windows_sys::Win32::System::IO::OVERLAPPED;
        let mut ol: OVERLAPPED = unsafe { std::mem::zeroed() };
        // only the byte-offset variant is set (hEvent stays null).
        ol.Anonymous.Anonymous.Offset = (offset & 0xffff_ffff) as u32;
        ol.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        let mut n: u32 = 0;
        let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
        // SAFETY: `buf` is a valid writable slice that outlives the
        // call; `ol` is a valid `OVERLAPPED` for the call's duration;
        // the handle was opened synchronously so the call blocks
        // until the transfer completes.
        let ok = unsafe {
            ReadFile(
                file.as_raw_handle() as HANDLE,
                buf.as_mut_ptr(),
                len,
                &mut n,
                &mut ol,
            )
        };
        if ok != 0 {
            return Ok(n as usize);
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(ERROR_HANDLE_EOF as i32) {
            return Ok(0);
        }
        Err(err)
    }

    /// perf probe (temporary) — cumulative µs inside `WriteFile`
    /// itself, isolating kernel time (dirty-page throttle etc.) from
    /// the surrounding storage code.
    pub static OS_WRITE_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// Positioned write — `file.cpp:234` upstream: `WriteFile` with
    /// the byte offset inside `OVERLAPPED`; race-free under
    /// concurrency (POSIX `pwrite` equivalent — see [`pread`]).
    pub fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        use windows_sys::Win32::System::IO::OVERLAPPED;
        let mut ol: OVERLAPPED = unsafe { std::mem::zeroed() };
        // only the byte-offset variant is set (hEvent stays null).
        ol.Anonymous.Anonymous.Offset = (offset & 0xffff_ffff) as u32;
        ol.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
        let mut n: u32 = 0;
        let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
        let t = std::time::Instant::now();
        // SAFETY: `buf` is a valid readable slice that outlives the
        // call; `ol` is a valid `OVERLAPPED` for the call's duration;
        // synchronous handle → the call blocks until completion.
        let ok = unsafe {
            WriteFile(
                file.as_raw_handle() as HANDLE,
                buf.as_ptr(),
                len,
                &mut n,
                &mut ol,
            )
        };
        OS_WRITE_US.fetch_add(
            t.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        if ok != 0 {
            return Ok(n as usize);
        }
        Err(io::Error::last_os_error())
    }

    /// `file.cpp:578` — `DeviceIoControl(FSCTL_SET_SPARSE)`. Errors
    /// ignored upstream (non-NTFS volume, permission) — same here.
    pub fn set_sparse(file: &File) {
        let mut ret: u32 = 0;
        unsafe {
            DeviceIoControl(
                file.as_raw_handle() as HANDLE,
                FSCTL_SET_SPARSE,
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                0,
                &mut ret,
                std::ptr::null_mut(),
            );
        }
    }

    /// `file.cpp:745-748` — `FSCTL_SET_SPARSE` with `SetSparse=FALSE`;
    /// clears the attribute on files that never became sparse
    /// (Vista+; errors ignored upstream).
    pub fn clear_sparse(file: &File) {
        let b = FILE_SET_SPARSE_BUFFER { SetSparse: 0 };
        let mut ret: u32 = 0;
        unsafe {
            DeviceIoControl(
                file.as_raw_handle() as HANDLE,
                FSCTL_SET_SPARSE,
                &b as *const FILE_SET_SPARSE_BUFFER as *const _,
                std::mem::size_of::<FILE_SET_SPARSE_BUFFER>() as u32,
                std::ptr::null_mut(),
                0,
                &mut ret,
                std::ptr::null_mut(),
            );
        }
    }

    /// `file.cpp:347-381` — `FSCTL_QUERY_ALLOCATED_RANGES` over the
    /// whole file: a failed query counts as sparse upstream (the flag
    /// is left alone); zero or multiple ranges → sparse; one range
    /// must cover the full `file_size`.
    pub fn is_sparse(file: &File) -> bool {
        let mut size: i64 = 0;
        // SAFETY: out-param query on a live handle.
        if unsafe { GetFileSizeEx(file.as_raw_handle() as HANDLE, &mut size) } == 0 {
            return false;
        }
        let input = FILE_ALLOCATED_RANGE_BUFFER {
            FileOffset: 0,
            Length: size,
        };
        let mut out: [FILE_ALLOCATED_RANGE_BUFFER; 2] = unsafe { std::mem::zeroed() };
        let mut returned: u32 = 0;
        let ok = unsafe {
            DeviceIoControl(
                file.as_raw_handle() as HANDLE,
                FSCTL_QUERY_ALLOCATED_RANGES,
                &input as *const FILE_ALLOCATED_RANGE_BUFFER as *const _,
                std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>() as u32,
                out.as_mut_ptr().cast(),
                (std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>() * 2) as u32,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return true;
        }
        if returned as usize != std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>() {
            return true;
        }
        out[0].Length != size
    }

    /// `file.cpp:587-593` — the `truncate` resize: `SetFilePointerEx`
    /// (→ `file_seek`) then `SetEndOfFile` (→ `file_truncate`), kept
    /// as two calls so the error operation matches upstream's thrown
    /// `storage_error`.
    pub fn set_end_of_file(file: &File, size: u64) -> Result<(), crate::file::OpenError> {
        use windows_sys::Win32::Storage::FileSystem::{FILE_BEGIN, SetEndOfFile, SetFilePointerEx};
        let ok = unsafe {
            SetFilePointerEx(
                file.as_raw_handle() as HANDLE,
                size as i64,
                std::ptr::null_mut(),
                FILE_BEGIN,
            )
        };
        if ok == 0 {
            return Err(crate::file::OpenError {
                operation: Operation::FileSeek,
                source: io::Error::last_os_error(),
            });
        }
        if unsafe { SetEndOfFile(file.as_raw_handle() as HANDLE) } == 0 {
            return Err(crate::file::OpenError {
                operation: Operation::FileTruncate,
                source: io::Error::last_os_error(),
            });
        }
        Ok(())
    }

    /// `file.cpp:490-499` — grant `SE_MANAGE_VOLUME_NAME` once per
    /// process, BEFORE the file is opened: upstream does it inside
    /// `create_file` when `allow_set_file_valid_data` is set.
    pub fn ensure_manage_volume_privilege() {
        static PRIV: Once = Once::new();
        PRIV.call_once(enable_manage_volume_privilege);
    }

    /// `file.cpp:600` — `SetFileValidData` after the file has been
    /// extended; errors ignored upstream.
    pub fn set_file_valid_data(file: &File, size: u64) {
        ensure_manage_volume_privilege();
        unsafe {
            SetFileValidData(file.as_raw_handle() as HANDLE, size as i64);
        }
    }

    /// `file.cpp:492` — grant `SE_MANAGE_VOLUME_NAME` to the process
    /// token so `SetFileValidData` is permitted. Failure tolerated.
    fn enable_manage_volume_privilege() {
        unsafe {
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(
                GetCurrentProcess(),
                TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY,
                &mut token,
            ) == 0
            {
                return;
            }
            let mut luid = std::mem::zeroed();
            if LookupPrivilegeValueW(std::ptr::null(), SE_MANAGE_VOLUME_NAME, &mut luid) == 0 {
                CloseHandle(token);
                return;
            }
            let tp = TOKEN_PRIVILEGES {
                PrivilegeCount: 1,
                Privileges: [LUID_AND_ATTRIBUTES {
                    Luid: luid,
                    Attributes: SE_PRIVILEGE_ENABLED,
                }],
            };
            AdjustTokenPrivileges(token, 0, &tp, 0, std::ptr::null_mut(), std::ptr::null_mut());
            CloseHandle(token);
        }
    }
}
