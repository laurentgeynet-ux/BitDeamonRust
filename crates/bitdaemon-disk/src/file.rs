//! Port of `aux_/file.hpp`/`file.cpp` — positioned-I/O file handle.
//!
//! Upstream exposes raw `handle_type` (fd/HANDLE) plus free functions
//! `pwrite_all`/`pread_all`/`pwritev_all`. Here the handle is a
//! `std::fs::File` and the free functions take `&File`, matching the
//! "write the whole buffer or fail" loop semantics (a zero-byte
//! transfer maps to `UnexpectedEof`, upstream `file_too_short`/EOF).

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use crate::open_mode::OpenMode;

/// `aux::file_handle` — owns the descriptor; closes on drop.
#[derive(Debug)]
pub struct FileHandle {
    file: File,
}

impl FileHandle {
    /// `file_handle(name, size, mode)` — open `name`, preallocating
    /// `size` bytes when opened for writing. Upstream opens with
    /// `OPEN_EXISTING`/`OPEN_ALWAYS` + `TRUNCATE_EXISTING` when the
    /// `truncate` flag is set; `set_size` is a separate call upstream
    /// (via `file::set_size`) — here we fold `size > 0` into
    /// `set_len` after open, matching `file_handle`'s ctor behavior
    /// of extending the file when `mode & write`.
    pub fn open(name: &Path, size: u64, mode: OpenMode) -> io::Result<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true);
        if mode.contains(OpenMode::WRITE) {
            opts.write(true).create(true);
        }
        if mode.contains(OpenMode::TRUNCATE) {
            opts.truncate(true);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            if mode.contains(OpenMode::NO_CACHE) {
                // upstream maps no_cache → O_SYNC
                opts.custom_flags(libc_sync_flag());
            }
            if mode.contains(OpenMode::EXECUTABLE) {
                opts.mode(0o755);
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // winbase.h constants — avoids a windows-sys dep for 5 values
            const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
            const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
            const FILE_FLAG_OPEN_NO_RECALL: u32 = 0x0010_0000;
            const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
            // Upstream deliberately avoids FILE_FLAG_RANDOM_ACCESS (see
            // src/file.cpp): that flag stops the Windows cache manager from
            // trimming pages, so cached data grows unbounded and stalls the
            // whole system (KB2549369).
            let mut flags = FILE_FLAG_BACKUP_SEMANTICS;
            if mode.contains(OpenMode::NO_CACHE) {
                flags |= FILE_FLAG_WRITE_THROUGH;
            }
            if mode.contains(OpenMode::SEQUENTIAL_ACCESS) {
                flags |= FILE_FLAG_SEQUENTIAL_SCAN;
            }
            if mode.contains(OpenMode::NO_ATIME) {
                flags |= FILE_FLAG_OPEN_NO_RECALL;
            }
            opts.custom_flags(flags);
            if mode.contains(OpenMode::HIDDEN) {
                opts.attributes(FILE_ATTRIBUTE_HIDDEN);
            }
        }
        let file = opts.open(name)?;
        #[cfg(windows)]
        {
            // upstream file.cpp:578 — FSCTL_SET_SPARSE on write opens
            // marked sparse; failure tolerated (non-NTFS volumes).
            if mode.contains(OpenMode::WRITE) && mode.contains(OpenMode::SPARSE) {
                win32::set_sparse(&file);
            }
        }
        // Upstream does NOT extend sparse files — they grow organically
        // at each write (sparse holes read as zero without VDL cost).
        // Only non-sparse (allocate-mode) files are sized up-front; on
        // Windows `set_len` sets EOF without advancing the Valid Data
        // Length, so `SetFileValidData` is what actually skips the
        // per-write synchronous zero-fill (file.cpp:585-600).
        if mode.contains(OpenMode::WRITE) && size > 0 && !mode.contains(OpenMode::SPARSE) {
            file.set_len(size)?;
            #[cfg(windows)]
            {
                if mode.contains(OpenMode::ALLOW_SET_FILE_VALID_DATA) {
                    win32::set_file_valid_data(&file, size);
                }
            }
        }
        Ok(Self { file })
    }

    /// Access the underlying `File`.
    #[must_use]
    pub fn file(&self) -> &File {
        &self.file
    }

    /// `get_size()`.
    pub fn get_size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// `file::set_size` — on Windows, `allow_set_file_valid_data`
    /// (with `!sparse`) replaces the implicit zero-fill of the
    /// extension with `SetFileValidData` (upstream file.cpp:585-600).
    pub fn set_size(&self, size: u64, mode: OpenMode) -> io::Result<()> {
        self.file.set_len(size)?;
        #[cfg(windows)]
        {
            if mode.contains(OpenMode::ALLOW_SET_FILE_VALID_DATA)
                && !mode.contains(OpenMode::SPARSE)
            {
                win32::set_file_valid_data(&self.file, size);
            }
        }
        let _ = mode;
        Ok(())
    }
}

#[cfg(unix)]
const fn libc_sync_flag() -> i32 {
    // upstream `no_cache` → O_SYNC (synchronous writes, no dirty
    // pages) — not O_DIRECT, which would require aligned I/O.
    #[cfg(target_os = "linux")]
    {
        0o4010000 // O_SYNC
    }
    #[cfg(target_os = "macos")]
    {
        0x0080 // O_SYNC
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(buf, offset)
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
        use std::os::windows::fs::FileExt;
        file.seek_write(buf, offset)
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

/// `aux::advise_dont_need` — hint the OS to drop the range from cache
/// (`posix_fadvise(DONTNEED)` upstream). Advisory only; no-op here —
/// `#![forbid(unsafe_code)]` rules out the raw libc call and the
/// effect is a cache hint, not observable behavior.
pub fn advise_dont_need(_file: &File, _offset: u64, _len: u64) {}

/// `aux::sync_file` — `fdatasync`/`FlushFileBuffers` over the range
/// (upstream flushes the whole file on platforms without ranged sync).
pub fn sync_file(file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    file.sync_data()
}

/// Windows NTFS file controls — `FSCTL_SET_SPARSE` and
/// `SetFileValidData` have no safe-std equivalent. Sole `unsafe`
/// site of this crate (ADR-0022 §3): every FFI call is reviewed,
/// best-effort (errors ignored like upstream).
#[cfg(windows)]
#[allow(unsafe_code)]
mod win32 {
    use std::fs::File;
    use std::os::windows::io::AsRawHandle;
    use std::sync::Once;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Security::{
        AdjustTokenPrivileges, LookupPrivilegeValueW, LUID_AND_ATTRIBUTES,
        SE_MANAGE_VOLUME_NAME, SE_PRIVILEGE_ENABLED, TOKEN_ADJUST_PRIVILEGES, TOKEN_PRIVILEGES,
    };
    use windows_sys::Win32::Storage::FileSystem::SetFileValidData;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

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

    /// `file.cpp:600` — `SetFileValidData` after the file has been
    /// extended; needs `SE_MANAGE_VOLUME_NAME` (enabled once per
    /// process, best-effort as upstream file.cpp:490).
    pub fn set_file_valid_data(file: &File, size: u64) {
        static PRIV: Once = Once::new();
        PRIV.call_once(enable_manage_volume_privilege);
        unsafe {
            SetFileValidData(file.as_raw_handle() as HANDLE, size as i64);
        }
    }

    /// `file.cpp:492` — grant `SE_MANAGE_VOLUME_NAME` to the process
    /// token so `SetFileValidData` is permitted. Failure tolerated.
    fn enable_manage_volume_privilege() {
        unsafe {
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &mut token) == 0 {
                return;
            }
            let mut luid = std::mem::zeroed();
            if LookupPrivilegeValueW(std::ptr::null(), SE_MANAGE_VOLUME_NAME, &mut luid) == 0 {
                return;
            }
            let tp = TOKEN_PRIVILEGES {
                PrivilegeCount: 1,
                Privileges: [LUID_AND_ATTRIBUTES {
                    Luid: luid,
                    Attributes: SE_PRIVILEGE_ENABLED,
                }],
            };
            AdjustTokenPrivileges(
                token,
                0,
                &tp,
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }
    }
}
