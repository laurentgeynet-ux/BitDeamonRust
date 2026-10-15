//! Port of `aux_/file_pool_impl.hpp`/`file_pool_impl.cpp`.
//!
//! LRU pool of open `FileHandle`s keyed `(StorageIndex, FileIndex)`.
//! Faithful semantics:
//! - at most `m_size` open handles; exceeding that evicts the least
//!   recently used entry *before* opening a new one
//! - a read-open may be served by a read-write handle, but a write
//!   request never is; the reverse races are tracked (`read_write_race`)
//! - one opener per file: concurrent openers of the same `FileId` block
//!   on a wait queue and all receive the same handle (or error)
//! - evicted/closed handles are dropped *after* releasing the mutex
//!   (close may be slow — upstream `m_deferred_destruction`)

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use bitdaemon_common::types::{FileIndex, StorageIndex};
use bitdaemon_core::file_storage::FileStorage;
use bitdaemon_core::path::bytes_to_path;

use crate::file::FileHandle;
use crate::fs;
use crate::open_mode::OpenMode;
use crate::storage_error::{Operation, StorageError};

/// `aux::file_id`.
pub type FileId = (StorageIndex, FileIndex);

/// `file_open_mode_t` — reported by `get_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOpenMode {
    /// `read_only`.
    ReadOnly,
    /// `write_only`.
    WriteOnly,
    /// `read_write`.
    ReadWrite,
    /// `sparse`.
    Sparse,
    /// `no_atime`.
    NoAtime,
    /// `random_access`.
    RandomAccess,
    /// `mmapped`.
    Mmapped,
}

/// `aux::to_file_open_mode`.
#[must_use]
pub fn to_file_open_mode(mode: OpenMode, has_mapping: bool) -> u8 {
    let mut v = if mode.contains(OpenMode::WRITE) {
        1 << 1 // read_write
    } else {
        0 // read_only
    };
    if mode.contains(OpenMode::NO_ATIME) {
        v |= 1 << 3;
    }
    if has_mapping {
        v |= 1 << 7;
    }
    v
}

/// `open_file_state`.
#[derive(Debug, Clone)]
pub struct OpenFileState {
    /// `file_index`.
    pub file_index: FileIndex,
    /// `open_mode` bitmask (`file_open_mode_t`).
    pub open_mode: u8,
    /// `last_use`.
    pub last_use: Instant,
}

struct PoolEntry {
    handle: Arc<FileHandle>,
    mode: OpenMode,
    /// LRU ordering key (monotonic tick — cheaper than `Instant`
    /// comparisons and immune to clock issues).
    last_use: u64,
    /// `last_use` as reported by `get_status` — upstream stores
    /// `aux::time_now()` on the entry itself.
    last_use_time: Instant,
}

/// One pending opener's wait object.
struct Waiter {
    m: Mutex<Option<Result<Arc<FileHandle>, WaitError>>>,
    c: Condvar,
}

/// `io::Error` isn't `Clone`/`Sync` friendly across the waiter list —
/// carry a reconstruction.
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
    /// `opening_file_entry::file_key` — upstream `m_opening_files` is a
    /// *list*: several opens of the same file may be in flight at once
    /// (e.g. a write request can't join a read-only open). Each opener
    /// notifies only its own `waiters` (`notify_file_open`), so entries
    /// are keyed by `id`, not just `file_key`.
    file_key: FileId,
    mode: OpenMode,
    waiters: Vec<Arc<Waiter>>,
    id: u64,
}

#[derive(Default)]
struct Inner {
    files: HashMap<FileId, PoolEntry>,
    /// `(last_use_tick, file_id)` → ordered by last use; front is LRU.
    lru: BTreeMap<(u64, FileId), ()>,
    /// `m_opening_files` — a list upstream: one entry per in-flight
    /// open, several per `file_key` allowed.
    opening: Vec<OpeningEntry>,
    hits: i64,
    misses: i64,
    stalls: i64,
    read_write_race: i64,
}

/// `aux::file_pool_impl<file_pool_entry>`.
pub struct FilePool {
    m: Mutex<Inner>,
    /// `m_size` — max open handles.
    size: Mutex<i32>,
    tick: AtomicU64,
}

impl FilePool {
    /// `file_pool_impl(size = 40)`.
    #[must_use]
    pub fn new(size: i32) -> Self {
        Self {
            m: Mutex::new(Inner::default()),
            size: Mutex::new(size),
            tick: AtomicU64::new(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.m.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lock_size(&self) -> MutexGuard<'_, i32> {
        self.size.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed)
    }

    /// `stats_counters()` → `(hits, misses, stalls, read_write_race, open)`.
    #[must_use]
    pub fn stats_counters(&self) -> (i64, i64, i64, i64, i64) {
        let g = self.lock();
        (
            g.hits,
            g.misses,
            g.stalls,
            g.read_write_race,
            i64::try_from(g.files.len()).unwrap_or(i64::MAX),
        )
    }

    /// `open_file(st, p, file_index, fn, m)` — returns a shared handle
    /// to `fn.file_path(file_index)` under `p`, opened per `m`.
    ///
    /// `to_open` resolves the path (defaults to `FileStorage::file_path`
    /// under `save_path`) — `filenames` upstream; callers with a
    /// different path layout (private storage) inject their own.
    pub fn open_file(
        &self,
        st: StorageIndex,
        p: &Path,
        file_index: FileIndex,
        fstorage: &FileStorage,
        mode: OpenMode,
    ) -> Result<Arc<FileHandle>, StorageError> {
        let file_key: FileId = (st, file_index);
        let rel = fstorage.file_path(file_index, &[]);
        let file_path = p.join(bytes_to_path(&rel));
        let size = fstorage.file_size(file_index);
        self.open_file_at(file_key, &file_path, size, mode)
    }

    /// `open_file` with a caller-resolved path (the rename overlay
    /// lives in the storage, not in the pool).
    pub fn open_file_at(
        &self,
        file_key: FileId,
        file_path: &Path,
        size: i64,
        mode: OpenMode,
    ) -> Result<Arc<FileHandle>, StorageError> {
        let mut defer1: Option<Arc<FileHandle>> = None;
        // assigned on the miss path — every control flow reaching
        // `open_file_impl` pushed an `opening` entry
        let opening_id: u64;

        {
            let mut g = self.lock();
            let Inner {
                files,
                lru,
                hits,
                misses,
                stalls,
                opening,
                ..
            } = &mut *g;
            // serve a read from a rw handle, never a write from ro
            let hit = files.get(&file_key).is_some_and(|e| {
                !mode.contains(OpenMode::WRITE) || e.mode.contains(OpenMode::WRITE)
            });
            if hit {
                let new_tick = self.next_tick();
                if let Some(e) = files.get_mut(&file_key) {
                    lru.remove(&(e.last_use, file_key));
                    e.last_use = new_tick;
                    e.last_use_time = Instant::now();
                    lru.insert((new_tick, file_key), ());
                    *hits += 1;
                    return Ok(e.handle.clone());
                }
            }

            // a file already being opened — join its waiters. Upstream
            // only scans `m_opening_files` when the file is absent from
            // the pool (`i == key_view.end()`); a pooled entry with an
            // insufficient mode falls through to a fresh open.
            if !files.contains_key(&file_key) {
                let joinable = opening.iter_mut().find(|oe| {
                    oe.file_key == file_key
                        && (!mode.contains(OpenMode::WRITE) || oe.mode.contains(OpenMode::WRITE))
                });
                if let Some(oe) = joinable {
                    *stalls += 1;
                    let waiter = Arc::new(Waiter {
                        m: Mutex::new(None),
                        c: Condvar::new(),
                    });
                    oe.waiters.push(waiter.clone());
                    drop(g);
                    let mut r = waiter.m.lock().unwrap_or_else(|e| e.into_inner());
                    while r.is_none() {
                        r = waiter.c.wait(r).unwrap_or_else(|e| e.into_inner());
                    }
                    return match r.take() {
                        Some(Ok(h)) => Ok(h),
                        Some(Err(e)) => Err(e.into_storage()),
                        None => unreachable!(),
                    };
                }
            }

            // evict LRU when at capacity (size-1 like upstream)
            let size = *self.lock_size();
            if i64::try_from(files.len()).unwrap_or(0) >= i64::from(size) - 1 {
                defer1 = remove_oldest(files, lru);
            }

            *misses += 1;
            opening_id = self.next_tick();
            opening.push(OpeningEntry {
                file_key,
                mode,
                waiters: Vec::new(),
                id: opening_id,
            });
            // drop lock while performing the actual open
        }

        let result = self.open_file_impl(file_path, size, mode, file_key.1, defer1);

        // re-acquire and notify waiters
        let mut g = self.lock();
        let Inner {
            files,
            lru,
            read_write_race,
            opening,
            ..
        } = &mut *g;
        // notify only THIS opener's waiters — a parallel opener of the
        // same file keeps its own entry (upstream `notify_file_open`
        // erases `s_iterator_to(ofe)`, not "any entry for this key")
        let oe = opening
            .iter()
            .position(|e| e.id == opening_id)
            .map(|i| opening.remove(i));
        match &result {
            Ok(handle) => {
                let tick = self.next_tick();
                let existing_mode = files.get(&file_key).map(|e| e.mode);
                match existing_mode {
                    Some(em) => {
                        if mode.contains(OpenMode::WRITE) && !em.contains(OpenMode::WRITE) {
                            *read_write_race += 1;
                            if let Some(e) = files.get_mut(&file_key) {
                                lru.remove(&(e.last_use, file_key));
                                let old = std::mem::replace(
                                    e,
                                    PoolEntry {
                                        handle: handle.clone(),
                                        mode,
                                        last_use: tick,
                                        last_use_time: Instant::now(),
                                    },
                                );
                                lru.insert((tick, file_key), ());
                                drop(old); // deferred close
                            }
                        } else if let Some(e) = files.get_mut(&file_key) {
                            let old_tick = e.last_use;
                            e.last_use = tick;
                            e.last_use_time = Instant::now();
                            lru.remove(&(old_tick, file_key));
                            lru.insert((tick, file_key), ());
                        }
                    }
                    None => {
                        files.insert(
                            file_key,
                            PoolEntry {
                                handle: handle.clone(),
                                mode,
                                last_use: tick,
                                last_use_time: Instant::now(),
                            },
                        );
                        lru.insert((tick, file_key), ());
                    }
                }
                if let Some(oe) = oe {
                    for w in oe.waiters {
                        *w.m.lock().unwrap_or_else(|e| e.into_inner()) = Some(Ok(handle.clone()));
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

    /// `open_file_impl` — open the resolved `file_path`, creating
    /// parent directories on `ENOENT` when opening for write
    /// (upstream retry-on-missing-dir semantics).
    fn open_file_impl(
        &self,
        file_path: &Path,
        size: i64,
        mode: OpenMode,
        file_index: FileIndex,
        defer_close: Option<Arc<FileHandle>>,
    ) -> Result<Arc<FileHandle>, StorageError> {
        drop(defer_close); // close outside the lock, as upstream
        match FileHandle::open(file_path, u64::try_from(size).unwrap_or(0), mode) {
            Ok(fh) => Ok(Arc::new(fh)),
            Err(e) => {
                // upstream retries on `no_such_file_or_directory`; on
                // Windows shared drives a missing directory surfaces as
                // ERROR_IO_DEVICE (1117) instead — same retry there.
                // (raw_os_error 1117 never collides with a POSIX errno,
                // so the check is safe off-Windows.)
                let missing_dir = e.source.kind() == io::ErrorKind::NotFound
                    || e.source.raw_os_error() == Some(1117);
                if !(mode.contains(OpenMode::WRITE) && missing_dir) {
                    return Err(StorageError::new(e.source, file_index, e.operation));
                }
                if let Some(parent) = file_path.parent() {
                    fs::create_directories(parent)
                        .map_err(|e2| StorageError::new(e2, file_index, Operation::Mkdir))?;
                }
                FileHandle::open(file_path, u64::try_from(size).unwrap_or(0), mode)
                    .map(Arc::new)
                    .map_err(|e2| StorageError::new(e2.source, file_index, e2.operation))
            }
        }
    }

    /// `release(st, file_index)` — close one file (after releasing
    /// the lock, as upstream).
    pub fn release_one(&self, st: StorageIndex, file_index: FileIndex) {
        let handle = {
            let mut g = self.lock();
            if let Some(e) = g.files.remove(&(st, file_index)) {
                g.lru.remove(&(e.last_use, (st, file_index)));
                Some(e.handle)
            } else {
                None
            }
        };
        drop(handle); // close outside the mutex
    }

    /// `release(st)` — close all files of a torrent.
    pub fn release_storage(&self, st: StorageIndex) {
        let handles: Vec<Arc<FileHandle>> = {
            let mut g = self.lock();
            let keys: Vec<FileId> = g.files.keys().filter(|k| k.0 == st).copied().collect();
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                if let Some(e) = g.files.remove(&k) {
                    g.lru.remove(&(e.last_use, k));
                    out.push(e.handle);
                }
            }
            out
        };
        drop(handles);
    }

    /// `release()` — close everything.
    pub fn release_all(&self) {
        let handles: Vec<Arc<FileHandle>> = {
            let mut g = self.lock();
            g.lru.clear();
            g.files.drain().map(|(_, e)| e.handle).collect()
        };
        drop(handles);
    }

    /// `resize(size)` — shrink evicts LRU entries.
    pub fn resize(&self, size: i32) {
        assert!(size > 0);
        let mut evicted = Vec::new();
        {
            let mut size_g = self.lock_size();
            if size == *size_g {
                return;
            }
            *size_g = size;
            let mut g = self.lock();
            let Inner { files, lru, .. } = &mut *g;
            while i64::try_from(files.len()).unwrap_or(0) > i64::from(size) {
                if let Some(h) = remove_oldest(files, lru) {
                    evicted.push(h);
                } else {
                    break;
                }
            }
        }
        drop(evicted);
    }

    /// `close_oldest()` — external LRU kick.
    pub fn close_oldest(&self) {
        let h = {
            let mut g = self.lock();
            let Inner { files, lru, .. } = &mut *g;
            remove_oldest(files, lru)
        };
        drop(h);
    }

    /// `get_status(st)` — per-file open state for a torrent.
    pub fn get_status(&self, st: StorageIndex) -> Vec<OpenFileState> {
        let g = self.lock();
        g.files
            .iter()
            .filter(|(k, _)| k.0 == st)
            .map(|(k, e)| OpenFileState {
                file_index: k.1,
                open_mode: to_file_open_mode(e.mode, false),
                last_use: e.last_use_time,
            })
            .collect()
    }
}

fn remove_oldest(
    files: &mut HashMap<FileId, PoolEntry>,
    lru: &mut BTreeMap<(u64, FileId), ()>,
) -> Option<Arc<FileHandle>> {
    let (&(tick, key), _) = lru.iter().next()?;
    lru.remove(&(tick, key));
    files.remove(&key).map(|e| e.handle)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "bitdaemon_fp_intra_{}_{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// `opening_file_entry` bookkeeping for a fake in-flight open.
    fn push_opening(
        pool: &FilePool,
        key: FileId,
        mode: OpenMode,
        waiters: Vec<Arc<Waiter>>,
    ) -> u64 {
        let mut g = pool.lock();
        let id = pool.next_tick();
        g.opening.push(OpeningEntry {
            file_key: key,
            mode,
            waiters,
            id,
        });
        id
    }

    /// Upstream `m_opening_files` is a list — a write request may NOT
    /// join an in-flight read-only open; it registers its own entry and
    /// opens in parallel. When it completes, only its own waiters are
    /// notified — the other entry and its waiters stay untouched
    /// (`notify_file_open` erases `s_iterator_to(ofe)`).
    #[test]
    fn parallel_openers_notify_only_own_waiters() {
        let d = test_dir("parallel");
        let path = d.join("f.bin");
        std::fs::write(&path, b"xy").unwrap();
        let pool = FilePool::new(40);
        let key: FileId = (StorageIndex(0), FileIndex(0));

        // a read-only open "in flight" with one waiter already queued
        let foreign_waiter = Arc::new(Waiter {
            m: Mutex::new(None),
            c: Condvar::new(),
        });
        let e1 = push_opening(
            &pool,
            key,
            OpenMode::READ_ONLY,
            vec![foreign_waiter.clone()],
        );

        // a write request can't join the ro entry — it opens in parallel
        let h = pool.open_file_at(key, &path, 2, OpenMode::WRITE).unwrap();
        drop(h);

        let g = pool.lock();
        // the ro entry survived the write opener's completion…
        assert_eq!(g.opening.len(), 1);
        assert_eq!(g.opening[0].id, e1);
        // …and the pool now holds the rw handle
        assert_eq!(g.files.len(), 1);
        drop(g);
        // …and its waiter was NOT woken by a completion it didn't join
        assert!(foreign_waiter.m.lock().unwrap().is_none());

        fs::remove_all(&d).unwrap();
    }

    /// Symmetric failure case: when the *parallel* opener fails, the
    /// still-pending entry's waiters must not receive that error —
    /// upstream delivers errors per `opening_file_entry`.
    #[test]
    fn parallel_opener_failure_does_not_poison_waiters() {
        let d = test_dir("parallel_fail");
        let path = d.join("f.bin");
        std::fs::write(&path, b"xy").unwrap();
        // `blocker` is a regular file: `create_directories(blocker)`
        // fails, so a write-open under it always errors.
        let blocker = d.join("blocker");
        std::fs::write(&blocker, b"z").unwrap();
        let bad_path = blocker.join("f.bin");

        let pool = FilePool::new(40);
        let key: FileId = (StorageIndex(0), FileIndex(0));
        let foreign_waiter = Arc::new(Waiter {
            m: Mutex::new(None),
            c: Condvar::new(),
        });
        let e1 = push_opening(
            &pool,
            key,
            OpenMode::READ_ONLY,
            vec![foreign_waiter.clone()],
        );

        // write request → own entry → open fails (uncreatable dir)
        assert!(
            pool.open_file_at(key, &bad_path, 2, OpenMode::WRITE)
                .is_err()
        );

        let g = pool.lock();
        // the foreign entry is still registered, its waiter still pending
        assert_eq!(g.opening.len(), 1);
        assert_eq!(g.opening[0].id, e1);
        drop(g);
        assert!(foreign_waiter.m.lock().unwrap().is_none());
        assert!(pool.lock().files.is_empty());

        fs::remove_all(&d).unwrap();
    }

    /// Joining still works: a compatible request queues on the in-flight
    /// entry (`m_stalls` up) and receives the opener's handle.
    #[test]
    fn waiter_receives_inflight_result() {
        let d = test_dir("join");
        let path = d.join("f.bin");
        std::fs::write(&path, b"xy").unwrap();
        let pool = FilePool::new(40);
        let key: FileId = (StorageIndex(0), FileIndex(0));
        push_opening(&pool, key, OpenMode::READ_ONLY, Vec::new());

        std::thread::scope(|s| {
            let pool = &pool;
            let path = &path;
            let t = s.spawn(move || pool.open_file_at(key, path, 2, OpenMode::READ_ONLY));
            // wait until the requester has queued its waiter
            loop {
                if pool.lock().opening[0].waiters.len() == 1 {
                    break;
                }
                std::thread::yield_now();
            }
            // the "opener" completes — wake the queued waiter ourselves
            let h = Arc::new(FileHandle::open(path, 2, OpenMode::READ_ONLY).unwrap());
            {
                let mut g = pool.lock();
                let oe = g.opening.remove(0);
                for w in oe.waiters {
                    *w.m.lock().unwrap() = Some(Ok(h.clone()));
                    w.c.notify_all();
                }
            }
            let got = t.join().unwrap().unwrap();
            assert!(Arc::ptr_eq(&got, &h));
        });
        let (_, _, stalls, _, _) = pool.stats_counters();
        assert_eq!(stalls, 1);
        fs::remove_all(&d).unwrap();
    }
}
