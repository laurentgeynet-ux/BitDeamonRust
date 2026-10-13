//! Port of `aux_/stat_cache.hpp`/`stat_cache.cpp`.
//!
//! Caches `stat()` results per file index. Negative `file_size`
//! entries mean: `NOT_IN_CACHE` (no data) or `-(error index + 2)`
//! pointing into `m_errors` — same wire-visible scheme as upstream.

use std::io;
use std::path::Path;
use std::sync::Mutex;

use bitdaemon_common::types::FileIndex;
use bitdaemon_core::file_storage::FileStorage;
use bitdaemon_core::path::bytes_to_path;

use crate::fs::stat_file;

const NOT_IN_CACHE: i64 = -1;
const FILE_ERROR: i64 = -2; // first index in m_errors

/// `aux::stat_cache`.
#[derive(Debug, Default)]
pub struct StatCache {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// one entry per file
    stat: Vec<i64>,
    /// recorded stat() errors; cache entries index into this
    errors: Vec<CachedError>,
}

/// Reconstructable `std::io::Error` (io::Error isn't Clone).
#[derive(Debug, Clone)]
struct CachedError {
    kind: io::ErrorKind,
    raw_os: Option<i32>,
}

impl CachedError {
    fn into_io(self) -> io::Error {
        match self.raw_os {
            Some(code) => io::Error::from_raw_os_error(code),
            None => io::Error::from(self.kind),
        }
    }
}

impl StatCache {
    /// `stat_cache()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `reserve(num_files)`.
    pub fn reserve(&self, num_files: usize) {
        let mut g = self.lock();
        g.stat.resize(num_files, NOT_IN_CACHE);
    }

    /// `get_filesize` — cached `stat_file` of `save_path/file_path(i)`.
    ///
    /// Errors are cached: a second call for the same file returns the
    /// recorded error without hitting the disk (upstream behavior).
    pub fn get_filesize(
        &self,
        i: FileIndex,
        fs: &FileStorage,
        save_path: &Path,
    ) -> io::Result<i64> {
        // upstream: always pretend symlinks don't exist — they get
        // special create/validate logic, and statting the target
        // blindly risks a cycle of references.
        if fs
            .file_flags(i)
            .contains(bitdaemon_core::file_storage::FileFlags::SYMLINK)
        {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }
        let path = fs.file_path(i, &[]);
        let path = bytes_to_path(&path);
        let full = save_path.join(&path);
        self.get_filesize_path(i, &full)
    }

    /// Same lookup given the already-resolved absolute path.
    pub fn get_filesize_path(&self, i: FileIndex, path: &Path) -> io::Result<i64> {
        let idx = usize::try_from(i.0).unwrap_or(0);
        {
            let g = self.lock();
            if let Some(&v) = g.stat.get(idx) {
                if v >= 0 {
                    return Ok(v);
                }
                if v <= FILE_ERROR {
                    let err_idx = usize::try_from(-(v - FILE_ERROR)).unwrap_or(0);
                    if let Some(e) = g.errors.get(err_idx) {
                        return Err(e.clone().into_io());
                    }
                }
            }
        }
        match stat_file(path, crate::fs::FileStatusFlags::NONE) {
            Ok(st) => {
                self.set_cache(i, st.file_size);
                Ok(st.file_size)
            }
            Err(e) => {
                self.set_error(i, &e);
                Err(e)
            }
        }
    }

    /// `set_dirty(i)` — invalidate the entry.
    pub fn set_dirty(&self, i: FileIndex) {
        let mut g = self.lock();
        let idx = usize::try_from(i.0).unwrap_or(0);
        if idx < g.stat.len() {
            g.stat[idx] = NOT_IN_CACHE;
        }
    }

    /// `clear()`.
    pub fn clear(&self) {
        let mut g = self.lock();
        g.stat.clear();
        g.errors.clear();
    }

    /// `set_cache` — internal, exposed for tests.
    pub fn set_cache(&self, i: FileIndex, size: i64) {
        let mut g = self.lock();
        let idx = usize::try_from(i.0).unwrap_or(0);
        if idx >= g.stat.len() {
            g.stat.resize(idx + 1, NOT_IN_CACHE);
        }
        g.stat[idx] = size;
    }

    /// `set_error` — internal, exposed for tests.
    pub fn set_error(&self, i: FileIndex, ec: &io::Error) {
        let mut g = self.lock();
        let err_idx = g
            .errors
            .iter()
            .position(|e| e.kind == ec.kind() && e.raw_os == ec.raw_os_error());
        let err_idx = match err_idx {
            Some(n) => n,
            None => {
                g.errors.push(CachedError {
                    kind: ec.kind(),
                    raw_os: ec.raw_os_error(),
                });
                g.errors.len() - 1
            }
        };
        let idx = usize::try_from(i.0).unwrap_or(0);
        if idx >= g.stat.len() {
            g.stat.resize(idx + 1, NOT_IN_CACHE);
        }
        g.stat[idx] = FILE_ERROR - i64::try_from(err_idx).unwrap_or(0);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // poison-tolerant (matches session rules)
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{self as dfs, FileStatusFlags};

    #[test]
    fn cache_hit_and_dirty() {
        let dir = std::env::temp_dir().join("bitdaemon_stat_cache_test");
        let _ = dfs::remove_all(&dir);
        dfs::create_directories(&dir).unwrap();
        let file = dir.join("f.bin");
        std::fs::write(&file, b"12345").unwrap();

        let sc = StatCache::new();
        let idx = FileIndex(0);
        assert_eq!(sc.get_filesize_path(idx, &file).unwrap(), 5);
        // cached — deleting the file still returns 5
        std::fs::remove_file(&file).unwrap();
        assert_eq!(sc.get_filesize_path(idx, &file).unwrap(), 5);
        sc.set_dirty(idx);
        assert!(sc.get_filesize_path(idx, &file).is_err());
        // error is cached too: recreating the file still errors
        std::fs::write(&file, b"abc").unwrap();
        assert!(sc.get_filesize_path(idx, &file).is_err());
        sc.set_dirty(idx);
        assert_eq!(sc.get_filesize_path(idx, &file).unwrap(), 3);
        assert!(dfs::exists(&file, FileStatusFlags::NONE));
        dfs::remove_all(&dir).unwrap();
    }
}
