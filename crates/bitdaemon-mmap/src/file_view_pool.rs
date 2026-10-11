// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/file_view_pool.cpp` (`file_view_pool.hpp`,
//! `mmap.hpp`). Upstream's pool is `file_pool_impl<file_view_entry>`:
//! an LRU table of `file_mapping` views keyed by file identity. Ours
//! caches `memmap2::MmapMut` views per path; the least-recently-used
//! view is evicted when the cap is reached (`file_pool_impl` reaps on
//! `last_use` order the same way).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use memmap2::MmapMut;

/// `file_view_entry` — one open mapping plus bookkeeping.
struct ViewEntry {
    map: MmapMut,
    /// keep the `File` alive so the mapping stays valid
    /// (`file_mapping` holds the handle upstream).
    _file: File,
    /// `last_use`.
    last_use: Instant,
}

/// Shared handle handed to readers/writers (`file_mapping` /
/// `file_view` upstream).
#[derive(Clone)]
pub struct FileView {
    inner: Arc<Mutex<ViewEntry>>,
}

impl FileView {
    /// `mapping::read` — copy `buf` from `range`. Returns bytes
    /// actually copied (clamped to the view length — upstream `pread`
    /// short-read semantics).
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let off = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
        if off >= g.map.len() {
            return Ok(0);
        }
        let n = (g.map.len() - off).min(buf.len());
        buf[..n].copy_from_slice(&g.map[off..off + n]);
        Ok(n)
    }

    /// `mapping::write` — copy `buf` into `range`. Dirty pages flush
    /// lazily like upstream (`msync`/`FlushViewOfFile` on unmap or
    /// [`FileView::flush`]).
    pub fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let off = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
        if off >= g.map.len() {
            return Ok(0);
        }
        let n = (g.map.len() - off).min(buf.len());
        g.map[off..off + n].copy_from_slice(&buf[..n]);
        Ok(n)
    }

    /// `file_mapping` dirty flush — `msync`/`FlushViewOfFile`.
    pub fn flush(&self) -> io::Result<()> {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.map.flush()
    }

    /// mapped byte length (`file_size`).
    #[must_use]
    pub fn len(&self) -> u64 {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.map.len() as u64
    }

    /// view is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for ViewEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewEntry").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for FileView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileView").finish_non_exhaustive()
    }
}

/// `file_view_pool` — LRU cache of writable file mappings.
#[derive(Debug)]
pub struct FileViewPool {
    /// `m_size` — max cached views (`settings_pack::file_pool_size`).
    max_size: usize,
    entries: Mutex<HashMap<PathBuf, Arc<Mutex<ViewEntry>>>>,
}

impl FileViewPool {
    /// `file_view_pool(size)`.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size: max_size.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// `open_file` — fetch (or create) a writable mapping for `path`
    /// sized to `size` bytes. A size mismatch drops the stale view
    /// (upstream `file_view_entry` is keyed on size+mode). Parent
    /// dirs are created lazily like upstream's ENOENT→mkdir retry.
    pub fn open(&self, path: &Path, size: u64) -> io::Result<FileView> {
        let mut g = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = g.get_mut(path) {
            let mut v = e.lock().unwrap_or_else(|p| p.into_inner());
            if v.map.len() as u64 == size {
                v.last_use = Instant::now();
                return Ok(FileView { inner: e.clone() });
            }
        }
        // stale-size entries are dropped before mapping the new one
        g.remove(path);

        // evict LRU when at capacity (`file_pool_impl` shrink)
        while g.len() >= self.max_size {
            let victim = g
                .iter()
                .min_by_key(|(_, e)| e.lock().unwrap_or_else(|p| p.into_inner()).last_use)
                .map(|(k, _)| k.clone());
            let Some(k) = victim else { break };
            g.remove(&k);
        }
        drop(g);

        let file = Self::open_file(path, size)?;
        let map = Self::map(&file)?;
        let entry = Arc::new(Mutex::new(ViewEntry {
            map,
            _file: file,
            last_use: Instant::now(),
        }));
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path.to_path_buf(), entry.clone());
        Ok(FileView { inner: entry })
    }

    /// `file_handle(name, size, rw)` — open rw, create + set_len.
    fn open_file(path: &Path, size: u64) -> io::Result<File> {
        let open = || -> io::Result<File> {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            if f.metadata()?.len() != size {
                f.set_len(size)?;
            }
            Ok(f)
        };
        match open() {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                open()
            }
            r => r,
        }
    }

    /// `file_mapping` — `MapViewOfFile`/`mmap` r/w.
    fn map(file: &File) -> io::Result<MmapMut> {
        // SAFETY: the only unsafe surface of the workspace
        // (ADR-0010). The `File` outlives the mapping inside
        // `ViewEntry`; callers only touch the mapping under the
        // entry lock so read/write can't alias a second &mut.
        unsafe { MmapMut::map_mut(file) }
    }

    /// `release_files` / `notify_file_removed` — drop a cached view.
    pub fn remove(&self, path: &Path) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(path);
    }

    /// drop every view (`~file_view_pool` per-torrent teardown).
    pub fn clear(&self) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// current cached view count (`size()`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// pool is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for FileViewPool {
    fn default() -> Self {
        Self::new(40) // settings_pack::file_pool_size default
    }
}
