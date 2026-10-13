//! Port of `aux_/open_mode.hpp`.

use bitdaemon_common::bitfield_flag;

bitfield_flag!(
    /// `aux::open_mode_t` — file open mode bitmask.
    OpenMode, u32
);

impl OpenMode {
    /// `aux::open_mode::read_only` — the empty set.
    pub const READ_ONLY: Self = Self::NONE;
    /// `aux::open_mode::write` — open for writing.
    pub const WRITE: Self = Self::bit(0);
    /// `no_cache` — hint: don't keep data in the OS cache.
    pub const NO_CACHE: Self = Self::bit(1);
    /// `truncate` — size the file to its declared `file_size` on this
    /// open (added once per file per session, `m_file_created`);
    /// never `O_TRUNC`/`TRUNCATE_EXISTING`.
    pub const TRUNCATE: Self = Self::bit(2);
    /// `no_atime` — don't update access time.
    pub const NO_ATIME: Self = Self::bit(3);
    /// `sequential_access` — hint: sequential scan.
    pub const SEQUENTIAL_ACCESS: Self = Self::bit(4);
    /// `hidden` — set the hidden attribute (Windows).
    pub const HIDDEN: Self = Self::bit(5);
    /// `sparse` — allocate sparse file (Windows).
    pub const SPARSE: Self = Self::bit(6);
    /// `executable` — mark file executable (POSIX).
    pub const EXECUTABLE: Self = Self::bit(7);
    /// `allow_set_file_valid_data` — Windows `SetFileValidData` attempt.
    pub const ALLOW_SET_FILE_VALID_DATA: Self = Self::bit(8);
    /// `no_mmap` — don't consider the file for memory-mapping.
    pub const NO_MMAP: Self = Self::bit(9);
    /// `no_cow` — `FS_NOCOW_FL` on creation (Linux/btrfs).
    pub const NO_COW: Self = Self::bit(10);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_is_empty() {
        assert_eq!(OpenMode::READ_ONLY, OpenMode::NONE);
        assert!(!OpenMode::READ_ONLY.is_set());
        assert!(OpenMode::WRITE.is_set());
        assert!((OpenMode::READ_ONLY | OpenMode::WRITE).contains(OpenMode::WRITE));
    }
}
