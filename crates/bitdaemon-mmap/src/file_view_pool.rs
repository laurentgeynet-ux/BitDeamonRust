// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/file_view_pool.cpp` (`file_view_pool.hpp`,
//! `mmap.hpp`). Upstream's pool is `file_pool_impl<file_view_entry>`:
//! an LRU table of `file_mapping` views keyed by `(storage_index,
//! file_index)` — the same machinery as `bitdaemon_disk::file_pool`
//! (one opener per file, hits/misses/stalls/races counters, deferred
//! destruction), pooled resource swapped for `memmap2` mappings.
//!
//! A `no_mmap` open or a zero-length file yields a view *without* a
//! mapping: `read_at`/`write_at` fall back to `pread`/`pwrite` on the
//! fd — upstream `file_mapping::has_memory_map() == false`.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::Instant;

use bitdaemon_common::types::{FileIndex, StorageIndex};
use bitdaemon_disk::file_pool::{FileId, OpenFileState, to_file_open_mode};
use bitdaemon_disk::open_mode::OpenMode;
use bitdaemon_disk::storage_error::{Operation, StorageError};
use memmap2::{Mmap, MmapMut, MmapOptions};

/// `file_mapping`'s mapping half — `None` for `no_mmap`/empty files.
enum Mapping {
    /// `PROT_READ`/`FILE_MAP_READ` — file opened read-only.
    Read(Mmap),
    /// shared writable map (`PROT_READ|PROT_WRITE`/`PAGE_READWRITE`).
    Write(MmapMut),
}

/// `file_view_entry` — one open mapping (or bare fd) + bookkeeping.
struct ViewEntry {
    /// `m_file` — the `file_handle`: keeps the fd alive under the
    /// mapping and serves the `no_mmap` fallback.
    file: File,
    map: Option<Mapping>,
    /// `m_size` — `memory_map_size()`: declared `file_size` for write
    /// opens, `min(actual_size, file_size)` for read opens.
    map_len: usize,
    /// `m_open_mode` — Windows `Drop` clears the sparse attribute on
    /// write+sparse files that never grew regions (file.cpp:730-749).
    mode: OpenMode,
    /// `m_open_unmap_lock` — serializes `CreateFileMapping` vs
    /// `UnmapViewOfFile` on the same file across pool generations
    /// (upstream passes a per-file `std::mutex` into the mapping).
    /// Held by every view so stale views unmap under it after
    /// eviction; the pool keeps a `Weak` per `FileId`.
    unmap_lock: Arc<Mutex<()>>,
}

impl Drop for ViewEntry {
    fn drop(&mut self) {
        // `file_mapping::close` — flush (Windows), then unmap under
        // the per-file lock.
        #[cfg(windows)]
        if let Some(Mapping::Write(m)) = &self.map {
            let _ = m.flush();
        }
        let _g = self.unmap_lock.lock().unwrap_or_else(|e| e.into_inner());
        #[cfg(windows)]
        if self.mode.contains(OpenMode::WRITE)
            && self.mode.contains(OpenMode::SPARSE)
            && !bitdaemon_disk::file::is_sparse(&self.file)
        {
            bitdaemon_disk::file::clear_sparse(&self.file);
        }
        drop(self.map.take());
    }
}

/// Shared handle handed to readers/writers (`file_mapping` /
/// `file_view` upstream).
#[derive(Clone)]
pub struct FileView {
    inner: Arc<Mutex<ViewEntry>>,
}

impl FileView {
    fn lock(&self) -> MutexGuard<'_, ViewEntry> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `mapping::read` — copy `buf` out of the view at `offset`,
    /// `pread` when the file isn't mapped (`has_memory_map()==false`
    /// → `pread_all` upstream). Returns bytes actually produced
    /// (clamped to the view length — short-read semantics).
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let g = self.lock();
        match &g.map {
            Some(map) => {
                let (m, off) = match (map, usize::try_from(offset)) {
                    (Mapping::Read(m), Ok(off)) => (&m[..], off),
                    (Mapping::Write(m), Ok(off)) => (&m[..], off),
                    (_, Err(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "offset overflow",
                        ));
                    }
                };
                if off >= m.len() {
                    return Ok(0);
                }
                let n = (m.len() - off).min(buf.len());
                buf[..n].copy_from_slice(&m[off..off + n]);
                Ok(n)
            }
            None => bitdaemon_disk::file::pread_best_effort(&g.file, buf, offset),
        }
    }

    /// `mapping::write` — copy `buf` into the writable view; without
    /// a writable map this is `pwrite` on the fd (a read-only view's
    /// fd fails like upstream's `PROT_READ` fault). Returns the bytes
    /// actually consumed (clamped to the view length).
    pub fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.lock();
        match &mut g.map {
            Some(Mapping::Write(m)) => {
                let off = usize::try_from(offset)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
                if off >= m.len() {
                    return Ok(0);
                }
                let n = (m.len() - off).min(buf.len());
                m[off..off + n].copy_from_slice(&buf[..n]);
                Ok(n)
            }
            _ => bitdaemon_disk::file::pwrite_all(&g.file, buf, offset),
        }
    }

    /// `pwrite` even when the file is mapped — upstream
    /// `!m_use_mmap_writes` path (`disk_write_mode::always_pwrite`).
    pub fn write_at_fd(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let g = self.lock();
        bitdaemon_disk::file::pwrite_all(&g.file, buf, offset)
    }

    /// `file_mapping::flush` — `msync`/`FlushViewOfFile` on the dirty
    /// range; no-op without a writable map.
    pub fn flush(&self) -> io::Result<()> {
        let g = self.lock();
        match &g.map {
            Some(Mapping::Write(m)) => m.flush(),
            _ => Ok(()),
        }
    }

    /// `file_mapping::dont_need` — hint that the range won't be needed
    /// soon. Best-effort; no-op without a map (the fd path has no
    /// pages to drop — upstream too).
    pub fn dont_need(&self, offset: u64, len: usize) {
        let g = self.lock();
        let Some(map) = &g.map else { return };
        let (base, map_len) = match map {
            Mapping::Read(m) => (m.as_ptr(), m.len()),
            Mapping::Write(m) => (m.as_ptr(), m.len()),
        };
        let Ok(off) = usize::try_from(offset) else {
            return;
        };
        if off >= map_len || len == 0 {
            return;
        }
        let n = (map_len - off).min(len);
        // upstream mmap.cpp:266-291 — MADV_COLD on Linux (it marks
        // pages reclaimable without touching dirty data; upstream
        // explicitly avoids MADV_DONTNEED there because it can
        // destroy dirty pages of a shared writable map),
        // MADV_DONTNEED on other unix, then msync(MS_INVALIDATE).
        // Best-effort like upstream — errors are ignored.
        #[cfg(unix)]
        unsafe {
            // SAFETY: `start..start+n` lies inside the mapping, which
            // outlives the call (entry lock held). These hints change
            // page residency, never contents — MADV_COLD/DONTNEED on a
            // *file-backed* map cannot invalidate data.
            let start = base.add(off).cast_mut().cast::<libc::c_void>();
            #[cfg(target_os = "linux")]
            let _ = libc::madvise(start, n, libc::MADV_COLD);
            #[cfg(not(target_os = "linux"))]
            let _ = libc::madvise(start, n, libc::MADV_DONTNEED);
            let _ = libc::msync(start, n, libc::MS_INVALIDATE);
        }
        #[cfg(not(unix))]
        let _ = (base, n);
    }

    /// `file_mapping::page_out` — flush the dirty pages of a range
    /// (upstream: `FlushViewOfFile` on Windows, `MADV_PAGEOUT` +
    /// `msync(MS_ASYNC)` on unix). Best-effort; no-op without a
    /// writable map.
    pub fn page_out(&self, offset: u64, len: usize) {
        let g = self.lock();
        let Some(Mapping::Write(m)) = &g.map else {
            return;
        };
        let Ok(off) = usize::try_from(offset) else {
            return;
        };
        if off >= m.len() || len == 0 {
            return;
        }
        let n = (m.len() - off).min(len);
        // upstream: `#ifdef MADV_PAGEOUT` (linux), `sync_file_range`
        // fallback — memmap2's `flush_async_range` is `msync(MS_ASYNC)`
        // / `FlushViewOfFile`, upstream's tail on every platform.
        #[cfg(target_os = "linux")]
        unsafe {
            // SAFETY: `start..start+n` lies inside the mapping;
            // MADV_PAGEOUT only schedules writeback/reclaim.
            let _ = libc::madvise(
                m.as_ptr().add(off).cast_mut().cast::<libc::c_void>(),
                n,
                libc::MADV_PAGEOUT,
            );
        }
        let _ = m.flush_async_range(off, n);
    }

    /// `has_memory_map`.
    #[must_use]
    pub fn has_memory_map(&self) -> bool {
        self.lock().map.is_some()
    }

    /// mapped (or declared) byte length — `m_size`.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.lock().map_len as u64
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

struct PoolEntry {
    view: Arc<Mutex<ViewEntry>>,
    mode: OpenMode,
    /// declared file size at open — a size change invalidates the
    /// view (upstream identity is the `FileId`; the declared size is
    /// fixed per torrent file, so a mismatch means stale state).
    size: u64,
    /// LRU ordering key (monotonic tick like `file_pool`).
    last_use: u64,
    /// `last_use` as reported by `get_status`.
    last_use_time: Instant,
}

/// One pending opener's wait object (`opening` wait queue upstream).
struct Waiter {
    m: Mutex<Option<Result<Arc<Mutex<ViewEntry>>, WaitError>>>,
    c: Condvar,
}

/// `StorageError` isn't `Clone` — carry a reconstruction to waiters.
#[derive(Debug, Clone)]
struct WaitError {
    kind: io::ErrorKind,
    raw: Option<i32>,
    file: FileIndex,
    operation: Operation,
}

impl WaitError {
    fn from_storage(e: &StorageError) -> Self {
        Self {
            kind: e.io.kind(),
            raw: e.io.raw_os_error(),
            file: e.file,
            operation: e.operation,
        }
    }
    fn into_storage(self) -> StorageError {
        StorageError::new(
            self.raw
                .map_or_else(|| io::Error::from(self.kind), io::Error::from_raw_os_error),
            self.file,
            self.operation,
        )
    }
}

struct OpeningEntry {
    mode: OpenMode,
    waiters: Vec<Arc<Waiter>>,
}

#[derive(Default)]
struct Inner {
    views: HashMap<FileId, PoolEntry>,
    /// `(last_use_tick, file_id)` → ordered by last use; front is LRU.
    lru: BTreeMap<(u64, FileId), ()>,
    opening: HashMap<FileId, OpeningEntry>,
    hits: i64,
    misses: i64,
    stalls: i64,
    read_write_race: i64,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("views", &self.views.len())
            .field("opening", &self.opening.len())
            .finish_non_exhaustive()
    }
}

/// `file_view_pool` — `file_pool_impl<file_view_entry>` upstream:
/// LRU pool of memory-mapped views keyed `(storage_index, file_index)`.
#[derive(Debug)]
pub struct FileViewPool {
    m: Mutex<Inner>,
    /// `m_size` — max cached views (`settings_pack::file_pool_size`).
    size: Mutex<i32>,
    tick: AtomicU64,
    /// `m_file_open_unmap_lock[file]` — per-file map/unmap mutex,
    /// `Weak` so the record dies with the last view. Upstream keys it
    /// off the storage's own array.
    map_locks: Mutex<HashMap<FileId, Weak<Mutex<()>>>>,
    /// last `close_oldest` eviction — dedupes the disk loop's
    /// per-storage fan-out (see [`Self::close_oldest`]).
    last_close_oldest: Mutex<Option<Instant>>,
}

impl FileViewPool {
    /// `file_view_pool(size)` — upstream default is
    /// `settings_pack::file_pool_size` (40).
    #[must_use]
    pub fn new(max_size: i32) -> Self {
        Self {
            m: Mutex::new(Inner::default()),
            size: Mutex::new(max_size),
            tick: AtomicU64::new(1),
            map_locks: Mutex::new(HashMap::new()),
            last_close_oldest: Mutex::new(None),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.m.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed)
    }

    /// `stats_counters()` → `(hits, misses, stalls, read_write_race,
    /// open)` — same tuple shape as `file_pool`.
    #[must_use]
    pub fn stats_counters(&self) -> (i64, i64, i64, i64, i64) {
        let g = self.lock();
        (
            g.hits,
            g.misses,
            g.stalls,
            g.read_write_race,
            i64::try_from(g.views.len()).unwrap_or(i64::MAX),
        )
    }

    /// `open_file(storage_index, save_path, file_index, filenames, m)`
    /// — returns a shared view for `file_path` under `mode`. Read
    /// requests may be served by a write view, never the reverse;
    /// concurrent opens of the same file join a wait queue and share
    /// the result (or its error), like upstream.
    pub fn open_file_at(
        &self,
        file_key: FileId,
        file_path: &Path,
        size: u64,
        mode: OpenMode,
    ) -> Result<FileView, StorageError> {
        let mut defer1: Option<Arc<Mutex<ViewEntry>>> = None;

        {
            let mut g = self.lock();
            let Inner {
                views,
                lru,
                hits,
                misses,
                stalls,
                opening,
                ..
            } = &mut *g;
            // serve a read from a rw view, never a write from ro; a
            // size change invalidates the cached view.
            let hit = views.get(&file_key).is_some_and(|e| {
                e.size == size
                    && (!mode.contains(OpenMode::WRITE) || e.mode.contains(OpenMode::WRITE))
            });
            if hit {
                let new_tick = self.next_tick();
                if let Some(e) = views.get_mut(&file_key) {
                    lru.remove(&(e.last_use, file_key));
                    e.last_use = new_tick;
                    e.last_use_time = Instant::now();
                    lru.insert((new_tick, file_key), ());
                    *hits += 1;
                    return Ok(FileView {
                        inner: e.view.clone(),
                    });
                }
            }

            // a file already being opened — join its waiters
            let can_join = opening.get(&file_key).is_some_and(|oe| {
                !mode.contains(OpenMode::WRITE) || oe.mode.contains(OpenMode::WRITE)
            });
            if can_join {
                *stalls += 1;
                let waiter = Arc::new(Waiter {
                    m: Mutex::new(None),
                    c: Condvar::new(),
                });
                if let Some(oe) = opening.get_mut(&file_key) {
                    oe.waiters.push(waiter.clone());
                }
                drop(g);
                let mut r = waiter.m.lock().unwrap_or_else(|e| e.into_inner());
                while r.is_none() {
                    r = waiter.c.wait(r).unwrap_or_else(|e| e.into_inner());
                }
                return match r.take() {
                    Some(Ok(v)) => Ok(FileView { inner: v }),
                    Some(Err(e)) => Err(e.into_storage()),
                    None => unreachable!("waiter resolved without a value"),
                };
            }

            // evict LRU when at capacity (size-1 like upstream)
            let size_cap = *self.size.lock().unwrap_or_else(|e| e.into_inner());
            if i64::try_from(views.len()).unwrap_or(0) >= i64::from(size_cap) - 1 {
                defer1 = Self::remove_oldest(views, lru);
            }

            *misses += 1;
            opening.entry(file_key).or_insert(OpeningEntry {
                mode,
                waiters: Vec::new(),
            });
            // drop lock while performing the actual open
        }

        let result = self.open_view_impl(file_key, file_path, size, mode, defer1);

        // re-acquire and notify waiters (same shape as file_pool)
        let mut g = self.lock();
        let Inner {
            views,
            lru,
            read_write_race,
            opening,
            ..
        } = &mut *g;
        let oe = opening.remove(&file_key);
        match &result {
            Ok(view) => {
                let tick = self.next_tick();
                let existing = views.get(&file_key).map(|e| (e.mode, e.size));
                match existing {
                    Some((em, esize)) => {
                        let write_upgrade =
                            mode.contains(OpenMode::WRITE) && !em.contains(OpenMode::WRITE);
                        if write_upgrade || esize != size {
                            if write_upgrade {
                                *read_write_race += 1;
                            }
                            if let Some(e) = views.get_mut(&file_key) {
                                lru.remove(&(e.last_use, file_key));
                                let old = std::mem::replace(
                                    e,
                                    PoolEntry {
                                        view: view.inner.clone(),
                                        mode,
                                        size,
                                        last_use: tick,
                                        last_use_time: Instant::now(),
                                    },
                                );
                                lru.insert((tick, file_key), ());
                                drop(old); // deferred unmap, off-lock below
                            }
                        } else if let Some(e) = views.get_mut(&file_key) {
                            let old_tick = e.last_use;
                            e.last_use = tick;
                            e.last_use_time = Instant::now();
                            lru.remove(&(old_tick, file_key));
                            lru.insert((tick, file_key), ());
                        }
                    }
                    None => {
                        views.insert(
                            file_key,
                            PoolEntry {
                                view: view.inner.clone(),
                                mode,
                                size,
                                last_use: tick,
                                last_use_time: Instant::now(),
                            },
                        );
                        lru.insert((tick, file_key), ());
                    }
                }
                if let Some(oe) = oe {
                    for w in oe.waiters {
                        *w.m.lock().unwrap_or_else(|e| e.into_inner()) =
                            Some(Ok(view.inner.clone()));
                        w.c.notify_all();
                    }
                }
            }
            Err(se) => {
                if let Some(oe) = oe {
                    for w in oe.waiters {
                        *w.m.lock().unwrap_or_else(|e| e.into_inner()) =
                            Some(Err(WaitError::from_storage(se)));
                        w.c.notify_all();
                    }
                }
            }
        }
        result
    }

    /// Per-file map/unmap lock — `Weak` so it dies with the last view
    /// while still serializing unmaps of evicted views against new
    /// maps of the same file (upstream's `m_file_open_unmap_lock`).
    fn map_lock(&self, file_key: FileId) -> Arc<Mutex<()>> {
        let mut locks = self.map_locks.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(l) = locks.get(&file_key).and_then(Weak::upgrade) {
            return l;
        }
        let l = Arc::new(Mutex::new(()));
        locks.insert(file_key, Arc::downgrade(&l));
        l
    }

    /// `file_view_entry` ctor — open the file and map it unless
    /// `no_mmap`/zero-length (the fd fallback upstream takes when
    /// `has_memory_map()==false`). `ENOENT` on a write open retries
    /// after creating the parent dir; a read open propagates.
    fn open_view_impl(
        &self,
        file_key: FileId,
        file_path: &Path,
        size: u64,
        mode: OpenMode,
        defer_close: Option<Arc<Mutex<ViewEntry>>>,
    ) -> Result<FileView, StorageError> {
        drop(defer_close); // unmap outside the lock, as upstream
        let map_lock = self.map_lock(file_key);
        match Self::open_view(file_path, size, mode, &map_lock, file_key.1) {
            Ok(entry) => Ok(FileView {
                inner: Arc::new(Mutex::new(entry)),
            }),
            Err(e) => {
                let missing_dir =
                    e.io.kind() == io::ErrorKind::NotFound || e.io.raw_os_error() == Some(1117);
                // only write opens retry after `create_directories` —
                // upstream never makes dirs for a read open.
                if !(mode.contains(OpenMode::WRITE) && missing_dir) {
                    return Err(e);
                }
                if let Some(parent) = file_path.parent() {
                    bitdaemon_disk::fs::create_directories(parent)
                        .map_err(|e2| StorageError::new(e2, file_key.1, Operation::Mkdir))?;
                }
                Self::open_view(file_path, size, mode, &map_lock, file_key.1).map(|entry| {
                    FileView {
                        inner: Arc::new(Mutex::new(entry)),
                    }
                })
            }
        }
    }

    /// `file_handle` ctor + `file_mapping` ctor: open `path`, apply the
    /// `truncate` sizing pass for write opens, then map
    /// `memory_map_size()` bytes — or leave `map: None` when `no_mmap`
    /// or the range is empty (an empty file still gets opened like
    /// upstream: it exists on disk, it's just never mapped).
    fn open_view(
        path: &Path,
        size: u64,
        mode: OpenMode,
        map_lock: &Arc<Mutex<()>>,
        file_index: FileIndex,
    ) -> Result<ViewEntry, StorageError> {
        // upstream open_mode::read_only opens `O_RDONLY` — no create;
        // a write open adds the `file_handle` truncate sizing pass.
        let f = bitdaemon_disk::file::open_raw(path, mode)
            .map_err(|e| StorageError::new(e.source, file_index, e.operation))?;
        if mode.contains(OpenMode::WRITE) {
            #[cfg(unix)]
            if mode.contains(OpenMode::TRUNCATE) && mode.contains(OpenMode::NO_COW) {
                // file.cpp:622-631 — must land before the resize.
                bitdaemon_disk::file::set_no_cow(&f);
            }
            // `file_handle`'s truncate pass — sized only on a
            // truncate-open (`m_file_created` makes that the first
            // write-open per session); Windows CreateFileMapping
            // extends anyway but POSIX mmap wouldn't reach past EOF.
            if mode.contains(OpenMode::TRUNCATE)
                && f.metadata().map(|m| m.len()).unwrap_or(size) != size
            {
                f.set_len(size)
                    .map_err(|e| StorageError::new(e, file_index, Operation::FileTruncate))?;
            }
            #[cfg(unix)]
            if mode.contains(OpenMode::TRUNCATE) && !mode.contains(OpenMode::SPARSE) {
                bitdaemon_disk::file::fallocate(&f, size)
                    .map_err(|e| StorageError::new(e, file_index, Operation::FileFallocate))?;
            }
            #[cfg(windows)]
            if mode.contains(OpenMode::TRUNCATE)
                && !mode.contains(OpenMode::SPARSE)
                && mode.contains(OpenMode::ALLOW_SET_FILE_VALID_DATA)
            {
                bitdaemon_disk::file::set_file_valid_data(&f, size);
            }
        }
        let (map, map_len) = Self::map_file(&f, mode, size, map_lock, file_index)?;
        Ok(ViewEntry {
            file: f,
            map,
            map_len,
            mode,
            unmap_lock: Arc::clone(map_lock),
        })
    }

    /// `file_mapping` ctor — `MapViewOfFile`/`mmap` of
    /// `memory_map_size()` bytes: `file_size` for write opens,
    /// `min(actual_size, file_size)` for read opens. `no_mmap` or a
    /// zero range leave the file unmapped (`has_memory_map()==false`,
    /// callers fall back to `pread`/`pwrite` on the fd).
    fn map_file(
        file: &File,
        mode: OpenMode,
        size: u64,
        map_lock: &Arc<Mutex<()>>,
        file_index: FileIndex,
    ) -> Result<(Option<Mapping>, usize), StorageError> {
        let map_size = if mode.contains(OpenMode::WRITE) {
            size
        } else {
            file.metadata()
                .map_err(|e| StorageError::new(e, file_index, Operation::FileStat))?
                .len()
                .min(size)
        };
        if mode.contains(OpenMode::NO_MMAP) || map_size == 0 {
            return Ok((None, usize::try_from(map_size).unwrap_or(0)));
        }
        let len = usize::try_from(map_size).map_err(|_| {
            StorageError::new(
                io::Error::new(io::ErrorKind::InvalidInput, "map size overflow"),
                file_index,
                Operation::FileMmap,
            )
        })?;
        // serialize against a concurrent unmap of a previous view of
        // the same file (upstream `m_open_unmap_lock`).
        let _g = map_lock.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY (ADR-0010): the `File` outlives the mapping inside
        // `ViewEntry`; all mapping access runs under the entry mutex
        // so reads/writes can't alias a second `&mut`.
        let map = unsafe {
            if mode.contains(OpenMode::WRITE) {
                MmapOptions::new()
                    .len(len)
                    .map_mut(file)
                    .map(Mapping::Write)
            } else {
                MmapOptions::new().len(len).map(file).map(Mapping::Read)
            }
        }
        .map_err(|e| StorageError::new(e, file_index, Operation::FileMmap))?;
        // mmap.cpp:164-182 — post-map `madvise`: `MADV_SEQUENTIAL` for
        // sequential opens plus the core-dump/fork exclusions upstream
        // applies where the platform has them. Best-effort.
        #[cfg(unix)]
        {
            let (base, mlen) = match &map {
                Mapping::Read(m) => (m.as_ptr().cast_mut().cast::<libc::c_void>(), m.len()),
                Mapping::Write(m) => (m.as_ptr().cast_mut().cast::<libc::c_void>(), m.len()),
            };
            let mut advise = 0;
            if mode.contains(OpenMode::SEQUENTIAL_ACCESS) {
                advise |= libc::MADV_SEQUENTIAL;
            }
            #[cfg(target_os = "linux")]
            {
                advise |= libc::MADV_DONTDUMP | libc::MADV_DONTFORK;
            }
            #[cfg(any(target_os = "freebsd", target_os = "netbsd", target_os = "openbsd"))]
            {
                advise |= libc::MADV_NOCORE;
            }
            if advise != 0 {
                // SAFETY: `base..base+mlen` is the live mapping; the
                // advice only marks residency/dump policy.
                unsafe {
                    let _ = libc::madvise(base, mlen, advise);
                }
            }
        }
        Ok((Some(map), len))
    }

    /// `release(st, file_index)` — drop one cached view (unmap happens
    /// off-lock when the last `FileView` lets go).
    pub fn release_one(&self, st: StorageIndex, file_index: FileIndex) {
        let entry = {
            let mut g = self.lock();
            let key = (st, file_index);
            if let Some(e) = g.views.remove(&key) {
                g.lru.remove(&(e.last_use, key));
                Some(e.view)
            } else {
                None
            }
        };
        drop(entry);
    }

    /// `release(st)` — drop every cached view of a torrent.
    pub fn release_storage(&self, st: StorageIndex) {
        let views: Vec<Arc<Mutex<ViewEntry>>> = {
            let mut g = self.lock();
            let keys: Vec<FileId> = g.views.keys().filter(|k| k.0 == st).copied().collect();
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                if let Some(e) = g.views.remove(&k) {
                    g.lru.remove(&(e.last_use, k));
                    out.push(e.view);
                }
            }
            out
        };
        drop(views);
    }

    /// `release()` — drop everything.
    pub fn release_all(&self) {
        let views: Vec<Arc<Mutex<ViewEntry>>> = {
            let mut g = self.lock();
            g.lru.clear();
            g.views.drain().map(|(_, e)| e.view).collect()
        };
        drop(views);
    }

    /// `close_oldest()` — one LRU kick per call, like upstream.
    /// Upstream issues exactly one call per `close_file_interval` on
    /// the shared pool; our disk loop fans the call out per storage
    /// holding this pool, so bursts within a second collapse to a
    /// single eviction — same cadence upstream produces.
    pub fn close_oldest(&self) {
        {
            let mut last = self
                .last_close_oldest
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if last.is_some_and(|t| t.elapsed() < std::time::Duration::from_secs(1)) {
                return;
            }
            *last = Some(Instant::now());
        }
        let v = {
            let mut g = self.lock();
            let Inner { views, lru, .. } = &mut *g;
            Self::remove_oldest(views, lru)
        };
        drop(v);
    }

    /// `resize(size)` — shrink evicts LRU entries.
    pub fn resize(&self, size: i32) {
        assert!(size > 0);
        let mut evicted = Vec::new();
        {
            let mut size_g = self.size.lock().unwrap_or_else(|e| e.into_inner());
            if size == *size_g {
                return;
            }
            *size_g = size;
            let mut g = self.lock();
            let Inner { views, lru, .. } = &mut *g;
            while i64::try_from(views.len()).unwrap_or(0) > i64::from(size) {
                if let Some(v) = Self::remove_oldest(views, lru) {
                    evicted.push(v);
                } else {
                    break;
                }
            }
        }
        drop(evicted);
    }

    /// `get_status(st)` — per-file open state for a torrent, `mmapped`
    /// bit set per upstream `to_file_open_mode`.
    #[must_use]
    pub fn get_status(&self, st: StorageIndex) -> Vec<OpenFileState> {
        let g = self.lock();
        g.views
            .iter()
            .filter(|(k, _)| k.0 == st)
            .map(|(k, e)| OpenFileState {
                file_index: k.1,
                open_mode: to_file_open_mode(
                    e.mode,
                    e.view
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .map
                        .is_some(),
                ),
                last_use: e.last_use_time,
            })
            .collect()
    }

    fn remove_oldest(
        views: &mut HashMap<FileId, PoolEntry>,
        lru: &mut BTreeMap<(u64, FileId), ()>,
    ) -> Option<Arc<Mutex<ViewEntry>>> {
        let (&(tick, key), _) = lru.iter().next()?;
        lru.remove(&(tick, key));
        views.remove(&key).map(|e| e.view)
    }

    /// current cached view count (`size()`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().views.len()
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
