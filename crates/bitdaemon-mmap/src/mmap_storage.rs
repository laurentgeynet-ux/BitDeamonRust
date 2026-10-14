// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `mmap_storage.cpp` + `mmap_disk_io.cpp`/`mmap_disk_job.hpp`
//! — the mmap storage backend (`storage_interface` upstream).
//!
//! Same surface as `PreadStorage` but file I/O goes through
//! [`FileViewPool`] mappings instead of `pread`/`pwrite`. Everything
//! that isn't view I/O (part file, priorities, pad files, stat cache,
//! move/delete/verify) shares upstream's semantics via
//! `bitdaemon-disk`'s `storage_utils`/`readwrite` walk.
//!
//! Upstream `mmap_disk_io` only differs from `pread_disk_io` in the
//! storage constructor — the job machinery lives in `bitdaemon-disk`,
//! so this crate exports a `StorageFactory` ([`mmap_storage_constructor`]).

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use bitdaemon_common::types::{DownloadPriority, FileIndex, PieceIndex, StorageIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage, RenamedFiles};
use bitdaemon_core::path::bytes_to_path;
use bitdaemon_disk::file_pool::OpenFileState;
use bitdaemon_disk::fs::DriveKind;
use bitdaemon_disk::open_mode::OpenMode;
use bitdaemon_disk::part_file::PartFile;
use bitdaemon_disk::precomputed_block_hashes::PrecomputedBlockHashes;
use bitdaemon_disk::readwrite::{readwrite_buf, readwrite_mut};
use bitdaemon_disk::stat_cache::StatCache;
use bitdaemon_disk::storage::{
    DiskStorage, MoveFlags, StorageMode, StorageParams, disk_status, remove_flags,
};
use bitdaemon_disk::storage_error::{Operation, StorageError, StorageResult};
use bitdaemon_disk::storage_utils;

use crate::file_view_pool::{FileView, FileViewPool};

/// `mmap_storage` — view-backed torrent storage.
pub struct MmapStorage {
    files: FileStorage,
    renamed: RenamedFiles,
    /// `m_save_path` — `move_storage` commits the new path back into
    /// the storage (upstream `std::tie(ret, m_save_path) = ...`).
    save_path: Mutex<PathBuf>,
    part_file_dir: PathBuf,
    part_file_name: PathBuf,
    pool: Arc<FileViewPool>,
    /// `m_allocate_files` — `storage_mode_allocate`: reserve the
    /// file's blocks (`posix_fallocate`/`SetFileValidData`) on its
    /// first write-open. `initialize` clears it on remote drives
    /// (mmap_storage.cpp:328-333).
    allocate_files: bool,
    /// `m_file_created` — files already sized to their final length
    /// this session (upstream adds `open_mode::truncate` on the first
    /// write-open only).
    file_created: Mutex<Vec<bool>>,
    /// `open_mode_extra` — settings-driven flags snapshot
    /// (`allow_set_file_valid_data`, `no_atime`) pushed by `DiskIo`.
    open_mode_extra: AtomicU32,
    v1: bool,
    v2: bool,
    file_priority: Vec<DownloadPriority>,
    use_partfile: Vec<bool>,
    part_file: Mutex<Option<PartFile>>,
    stat_cache: StatCache,
    storage_index: Mutex<StorageIndex>,
    need_tick: AtomicBool,
    /// `m_precomputed_v2` — v2 block hashes saved by hash jobs so
    /// later `hash2` requests skip the disk read.
    precomputed: PrecomputedBlockHashes,
    /// `m_use_mmap_writes` — resolved in `initialize` from
    /// `settings_pack::disk_write_mode` (+ `drive_info` for `auto`).
    /// Upstream member default is `false` (mmap_storage.hpp:137).
    use_mmap_writes: AtomicBool,
    /// pushed `settings_pack::disk_write_mode` (`mmap_write_mode_t`:
    /// 0=`always_pwrite`, 1=`always_mmap_write`, 2=`auto_mmap_write`).
    disk_write_mode: AtomicI64,
    /// pushed `settings_pack::mmap_file_size_cutoff` — files at or
    /// below this many 16 KiB blocks open `no_mmap`, served through
    /// the fd fallback (upstream default 40).
    mmap_file_size_cutoff: AtomicI64,
    /// pushed `settings_pack::disk_disable_copy_on_write` →
    /// `open_mode::no_cow` on truncate-opens (mmap_storage.cpp:948 —
    /// upstream applies it in the mmap backend only).
    no_cow: AtomicBool,
}

impl MmapStorage {
    /// `mmap_storage(params, pool)`.
    #[must_use]
    pub fn new(params: StorageParams<'_>, pool: Arc<FileViewPool>) -> Self {
        debug_assert!(params.v1 || params.v2, "storage must be v1 or v2");
        debug_assert!(params.files.num_files() > 0);
        let mut name = b".0000000000000000000000000000000000000000.parts".to_vec();
        for (i, b) in params.info_hash.iter().enumerate() {
            name[1 + i * 2] = b"0123456789abcdef"[usize::from(b >> 4)];
            name[2 + i * 2] = b"0123456789abcdef"[usize::from(b & 0xf)];
        }
        Self {
            files: params.files.clone(),
            renamed: params.renamed.clone(),
            save_path: Mutex::new(bitdaemon_disk::fs::complete(&params.path)),
            part_file_dir: params.part_file_dir,
            part_file_name: bytes_to_path(&name),
            pool,
            allocate_files: params.mode == StorageMode::Allocate,
            file_created: Mutex::new(vec![false; params.files.num_files() as usize]),
            open_mode_extra: AtomicU32::new(0),
            v1: params.v1,
            v2: params.v2,
            file_priority: params.priorities.to_vec(),
            use_partfile: Vec::new(),
            part_file: Mutex::new(None),
            stat_cache: StatCache::new(),
            storage_index: Mutex::new(StorageIndex(0)),
            need_tick: AtomicBool::new(false),
            precomputed: PrecomputedBlockHashes::new(),
            use_mmap_writes: AtomicBool::new(false),
            // `settings_pack::disk_write_mode` default
            // (`always_mmap_write`)
            disk_write_mode: AtomicI64::new(1),
            // `settings_pack::mmap_file_size_cutoff` default
            mmap_file_size_cutoff: AtomicI64::new(40),
            no_cow: AtomicBool::new(false),
        }
    }

    /// `files()`.
    #[must_use]
    pub fn files(&self) -> &FileStorage {
        &self.files
    }

    /// `v1()`/`v2()`.
    #[must_use]
    pub fn v1(&self) -> bool {
        self.v1
    }

    /// see [`Self::v1`].
    #[must_use]
    pub fn v2(&self) -> bool {
        self.v2
    }

    /// `storage_index()`.
    #[must_use]
    pub fn storage_index(&self) -> StorageIndex {
        *self.storage_index.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `set_storage_index`.
    pub fn set_storage_index(&self, st: StorageIndex) {
        *self.storage_index.lock().unwrap_or_else(|e| e.into_inner()) = st;
    }

    /// `set_need_tick`.
    pub fn set_need_tick(&self) -> bool {
        self.need_tick
            .swap(true, std::sync::atomic::Ordering::Relaxed)
    }

    /// `do_tick`.
    pub fn do_tick(&self) {
        self.need_tick
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.tick();
    }

    fn use_partfile(&self, index: FileIndex) -> bool {
        self.use_partfile.get(index.get()).copied().unwrap_or(true)
    }

    fn set_use_partfile(&mut self, index: FileIndex, b: bool) {
        if index.get() >= self.use_partfile.len() {
            if b {
                return;
            }
            self.use_partfile.resize(index.get() + 1, true);
        }
        self.use_partfile[index.get()] = b;
    }

    fn resolve_path(&self, index: FileIndex) -> PathBuf {
        let rel = self.renamed.file_path(&self.files, index, &[]);
        self.save_path().join(bytes_to_path(&rel))
    }

    /// `mmap_storage::open_file` (mmap_storage.cpp:879-927) — same
    /// mode resolution as `pread_storage::open_file`: the first
    /// write-open of a file per session gets `truncate` (`m_file_created`),
    /// `file_flags` → exec/hidden, sparse when `!m_allocate_files` or
    /// priority 0; `extra_mode` folds in `no_atime`/
    /// `allow_set_file_valid_data`.
    fn open_file(&self, index: FileIndex, mode: OpenMode) -> Result<FileView, StorageError> {
        let mut mode = mode | self.extra_mode();
        if mode.contains(OpenMode::WRITE) && !mode.contains(OpenMode::TRUNCATE) {
            let created = self.file_created.lock().unwrap_or_else(|e| e.into_inner());
            if index.get() < created.len() && !created[index.get()] {
                mode |= OpenMode::TRUNCATE;
            }
        }
        let flags = self.files.file_flags(index);
        if flags.contains(FileFlags::EXECUTABLE) {
            mode |= OpenMode::EXECUTABLE;
        }
        if flags.contains(FileFlags::HIDDEN) {
            mode |= OpenMode::HIDDEN;
        }
        if !self.allocate_files
            || (index.get() < self.file_priority.len()
                && self.file_priority[index.get()] == DownloadPriority::DONT_DOWNLOAD)
        {
            mode |= OpenMode::SPARSE;
        }
        // mmap_storage.cpp:948 — btrfs CoW opt-out (truncate path).
        if self.no_cow.load(Ordering::Relaxed) {
            mode |= OpenMode::NO_COW;
        }
        // mmap_storage.cpp:951-953 — small files take the fd path:
        // `no_mmap` leaves `has_memory_map() == false` and every op
        // falls back to positioned I/O.
        if self.files.file_size(index) / 0x4000
            <= self.mmap_file_size_cutoff.load(Ordering::Relaxed)
        {
            mode |= OpenMode::NO_MMAP;
        }
        let view = self.pool.open_file_at(
            (self.storage_index(), index),
            &self.resolve_path(index),
            self.files.file_size(index) as u64,
            mode,
        )?;
        if mode.contains(OpenMode::TRUNCATE) {
            self.file_created.lock().unwrap_or_else(|e| e.into_inner())[index.get()] = true;
        }
        Ok(view)
    }

    /// settings-driven extra open flags snapshot (`extra_mode` —
    /// upstream reads `settings_interface` inside `open_file`).
    fn extra_mode(&self) -> OpenMode {
        OpenMode::from_bits(
            self.open_mode_extra
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// current save path (`m_save_path`).
    fn save_path(&self) -> PathBuf {
        self.save_path
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `combine_path(save_path, part_file_dir)` or `save_path` — the
    /// part file lives *under* the save path when a dir is set.
    fn part_file_dir_path(&self) -> PathBuf {
        let save = self.save_path();
        if self.part_file_dir.as_os_str().is_empty() {
            save
        } else {
            save.join(&self.part_file_dir)
        }
    }

    /// full path of the `.parts` file.
    fn part_file_path(&self) -> PathBuf {
        self.part_file_dir_path().join(&self.part_file_name)
    }

    fn need_partfile(&self) {
        let mut g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_none() {
            *g = Some(PartFile::new(
                self.part_file_dir_path(),
                self.part_file_name.clone(),
                self.files.num_pieces(),
                self.files.piece_length(),
            ));
        }
    }

    /// `read(buffer, piece, offset, mode, flags)` — views instead of
    /// `pread`; pad files yield zeroes; `dont_download` files are
    /// served from the part file. `volatile`/`flush` map to upstream's
    /// `volatile_read`/`flush_piece` job flags: `dont_need` /
    /// `page_out` hints on the mapped range (mmap_storage.cpp:577-580).
    pub fn read(
        &self,
        buffer: &mut [u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile_read: bool,
        flush_piece: bool,
    ) -> StorageResult<i64> {
        readwrite_mut(
            &self.files,
            buffer,
            piece,
            offset,
            |file_index, file_offset, buf| {
                if self.files.pad_file_at(file_index) {
                    buf.fill(0);
                    return Ok(buf.len());
                }
                if file_index.get() < self.file_priority.len()
                    && self.file_priority[file_index.get()] == DownloadPriority::DONT_DOWNLOAD
                    && self.use_partfile(file_index)
                {
                    let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
                    let Some(pf) = g.as_ref() else {
                        return Err(StorageError::new(
                            io::Error::new(io::ErrorKind::NotFound, "part file not open"),
                            file_index,
                            Operation::PartfileRead,
                        ));
                    };
                    let map = self.files.map_file(file_index, file_offset, 0);
                    let n = pf
                        .read(buf, map.piece, map.start)
                        .map_err(|e| StorageError::new(e, file_index, Operation::PartfileRead))?;
                    return Ok(usize::try_from(n).unwrap_or(0));
                }
                let view = self.open_file(file_index, mode)?;
                let off = u64::try_from(file_offset).unwrap_or(0);
                let n = view
                    .read_at(off, buf)
                    .map_err(|e| StorageError::new(e, file_index, Operation::FileRead))?;
                if volatile_read {
                    view.dont_need(off, n);
                }
                if flush_piece {
                    view.page_out(off, n);
                }
                Ok(n)
            },
        )
    }

    /// `write(buffer, piece, offset, mode, flags)` — views instead of
    /// `pwrite` (`!m_use_mmap_writes || !has_memory_map()` → `pwrite`
    /// upstream, mmap_storage.cpp:659); `dont_download` files go to the
    /// part file; `volatile`/`flush` are the job's `volatile_read`/
    /// `flush_piece` flags.
    pub fn write(
        &self,
        buffer: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile_read: bool,
        flush_piece: bool,
    ) -> StorageResult<i64> {
        readwrite_buf(
            &self.files,
            buffer,
            piece,
            offset,
            |file_index, file_offset, buf| {
                if self.files.pad_file_at(file_index) {
                    return Ok(buf.len()); // pad writes are dropped (upstream)
                }
                if file_index.get() < self.file_priority.len()
                    && self.file_priority[file_index.get()] == DownloadPriority::DONT_DOWNLOAD
                    && self.use_partfile(file_index)
                {
                    let mut g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
                    let Some(pf) = g.as_mut() else {
                        return Err(StorageError::new(
                            io::Error::new(io::ErrorKind::NotFound, "part file not open"),
                            file_index,
                            Operation::PartfileWrite,
                        ));
                    };
                    let map = self.files.map_file(file_index, file_offset, 0);
                    let n = pf
                        .write(buf, map.piece, map.start)
                        .map_err(|e| StorageError::new(e, file_index, Operation::PartfileWrite))?;
                    return Ok(usize::try_from(n).unwrap_or(0));
                }
                let view = self.open_file(file_index, mode | OpenMode::WRITE)?;
                let off = u64::try_from(file_offset).unwrap_or(0);
                // writes invalidate the cached stat entry —
                // `m_stat_cache.set_dirty(file_index)` upstream
                // (mmap_storage.cpp:647); the pread backend does the
                // same in `storage.rs`.
                self.stat_cache.set_dirty(file_index);
                if !self.use_mmap_writes.load(Ordering::Relaxed) || !view.has_memory_map() {
                    return view
                        .write_at_fd(off, buf)
                        .map_err(|e| StorageError::new(e, file_index, Operation::FileWrite));
                }
                let n = view
                    .write_at(off, buf)
                    .map_err(|e| StorageError::new(e, file_index, Operation::FileWrite))?;
                if volatile_read {
                    view.dont_need(off, buf.len());
                }
                if flush_piece {
                    view.page_out(off, buf.len());
                }
                Ok(n)
            },
        )
    }

    /// `hash`/`hash2` — feed every byte of the range to `feed` (view
    /// reads; the fd path falls back to `pread` like upstream
    /// `!has_memory_map()` → `pread_all`); `volatile`/`flush` →
    /// `dont_need`/`page_out` on the mapped range.
    #[allow(clippy::too_many_arguments)]
    pub fn hash_with(
        &self,
        mut feed: impl FnMut(&[u8]),
        len: i64,
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile_read: bool,
        flush_piece: bool,
    ) -> StorageResult<i64> {
        let mut scratch: Vec<u8> = Vec::new();
        let len_usize = usize::try_from(len).unwrap_or(0);
        readwrite_buf(
            &self.files,
            &vec![0u8; len_usize],
            piece,
            offset,
            |file_index, file_offset, buf| {
                if self.files.pad_file_at(file_index) {
                    let zeroes = vec![0u8; buf.len()];
                    feed(&zeroes);
                    return Ok(buf.len());
                }
                if file_index.get() < self.file_priority.len()
                    && self.file_priority[file_index.get()] == DownloadPriority::DONT_DOWNLOAD
                    && self.use_partfile(file_index)
                {
                    let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
                    let Some(pf) = g.as_ref() else {
                        return Err(StorageError::new(
                            io::Error::new(io::ErrorKind::NotFound, "part file not open"),
                            file_index,
                            Operation::PartfileRead,
                        ));
                    };
                    let map = self.files.map_file(file_index, file_offset, 0);
                    let data = pf
                        .hash_feed(map.piece, map.start, buf.len())
                        .map_err(|e| StorageError::new(e, file_index, Operation::PartfileRead))?;
                    feed(&data);
                    return Ok(data.len());
                }
                let view = self.open_file(file_index, mode)?;
                let off = u64::try_from(file_offset).unwrap_or(0);
                scratch.resize(buf.len(), 0);
                let n = view
                    .read_at(off, &mut scratch)
                    .map_err(|e| StorageError::new(e, file_index, Operation::FileRead))?;
                feed(&scratch[..n]);
                if volatile_read {
                    view.dont_need(off, n);
                }
                if flush_piece {
                    view.page_out(off, n);
                }
                Ok(n)
            },
        )
    }

    /// current per-file priorities.
    #[must_use]
    pub fn file_priorities(&self) -> &[DownloadPriority] {
        &self.file_priority
    }

    /// `release_files` — drop this storage's cached views, flush the
    /// part file and clear the stat cache (upstream order and
    /// coverage — `m_pool.release(storage_index())` is storage-scoped
    /// upstream, not a pool-wide clear).
    pub fn release_files(&self) -> StorageResult<()> {
        self.tick();
        self.pool.release_storage(self.storage_index());
        self.stat_cache.clear();
        Ok(())
    }

    /// `tick` — flush part-file metadata if dirty.
    pub fn tick(&self) {
        let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pf) = g.as_ref() {
            let _ = pf.flush_metadata();
        }
    }

    /// `rename_file` — unmap the file first (a live view pins the
    /// name on Windows; upstream releases exactly `(storage, index)`
    /// — mmap_storage.cpp:420), then rename on disk or record it.
    pub fn rename_file(&mut self, index: FileIndex, new_filename: &[u8]) -> StorageResult<()> {
        self.pool.release_one(self.storage_index(), index);
        let save_path = self.save_path();
        storage_utils::rename_file(
            &self.files,
            &mut self.renamed,
            index,
            new_filename,
            &save_path,
        )
    }

    /// `initialize` — directories + part-file bookkeeping (upstream
    /// `mmap_storage::initialize` shares `pread_storage` logic; the
    /// only difference is files are touched through views). Also
    /// resolves `m_allocate_files`/`m_use_mmap_writes` from
    /// `drive_info` + `disk_write_mode` like upstream
    /// (mmap_storage.cpp:328-340).
    pub fn initialize(&mut self) -> StorageResult<u8> {
        self.stat_cache
            .reserve(usize::try_from(self.files.num_files()).unwrap_or(0));
        let di = bitdaemon_disk::fs::drive_kind(&self.save_path());
        if di == DriveKind::Remote {
            // don't do full file allocations on network drives
            self.allocate_files = false;
        }
        match self.disk_write_mode.load(Ordering::Relaxed) {
            // `always_pwrite`
            0 => self.use_mmap_writes.store(false, Ordering::Relaxed),
            // `auto_mmap_write` — mmap writes only pay off on
            // RAM-backed/DAX volumes upstream.
            2 => self
                .use_mmap_writes
                .store(di == DriveKind::SsdDax, Ordering::Relaxed),
            // `always_mmap_write` (default)
            _ => self.use_mmap_writes.store(true, Ordering::Relaxed),
        }
        let mut ret = 0u8;
        for i in 0..self.files.num_files() {
            let i = FileIndex(i);
            let fs_file_size = self.files.file_size(i);
            let is_pad = self.files.pad_file_at(i);
            if i.get() >= self.file_priority.len()
                || self.file_priority[i.get()] != DownloadPriority::DONT_DOWNLOAD
                || is_pad
            {
                continue;
            }
            let p = self.resolve_path(i);
            match self.stat_cache.get_filesize_path(i, &p) {
                Ok(size) if size > 0 => {
                    self.set_use_partfile(i, false);
                    if size > fs_file_size {
                        ret |= disk_status::OVERSIZED_FILE;
                    }
                }
                _ => {
                    self.set_use_partfile(i, true);
                    self.need_partfile();
                }
            }
        }
        let stat_cache_ptr: &StatCache = &self.stat_cache;
        storage_utils::initialize_storage(
            &self.files,
            &self.renamed,
            &self.save_path(),
            stat_cache_ptr,
            &self.file_priority,
            |i| self.open_file(i, OpenMode::WRITE).map(|_| ()),
            |_i, _size| ret |= disk_status::OVERSIZED_FILE,
        )?;
        // close files that were opened in write mode
        // (mmap_storage.cpp:383-384 — they pin sparse/alloc state).
        self.pool.release_storage(self.storage_index());
        Ok(ret)
    }

    /// `move_storage` — unmap first (Windows won't rename mapped
    /// files), then move files + part file and commit `m_save_path`
    /// only on success. Returns the upstream `(status, path, error)`
    /// triple.
    pub fn move_storage(
        &self,
        new_save_path: PathBuf,
        flags: MoveFlags,
    ) -> (u8, PathBuf, Option<StorageError>) {
        self.pool.release_storage(self.storage_index()); // unmap first (Windows locks)
        let save_path = self.save_path();
        let part_dir = self.part_file_dir.clone();
        let mut pf = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        let (status, path, error) = storage_utils::move_storage(
            &self.files,
            &self.renamed,
            save_path,
            new_save_path,
            |path| {
                let dir = if part_dir.as_os_str().is_empty() {
                    path.to_path_buf()
                } else {
                    let d = path.join(&part_dir);
                    bitdaemon_disk::fs::create_directories(&d)?;
                    d
                };
                if let Some(p) = pf.as_mut() {
                    p.move_partfile(dir)
                } else {
                    Ok(())
                }
            },
            flags,
        );
        if status & disk_status::FATAL_DISK_ERROR == 0 && status & disk_status::FILE_EXIST == 0 {
            *self.save_path.lock().unwrap_or_else(|e| e.into_inner()) = path.clone();
        }
        self.stat_cache.clear();
        (status, path, error)
    }

    /// `delete_files` — unmap first (Windows won't delete mapped
    /// files) and destroy the part file (flushes its metadata), then
    /// delegate.
    pub fn delete_files(&self, options: u8) -> StorageResult<()> {
        if options & remove_flags::DELETE_FILES != 0 {
            self.pool.release_storage(self.storage_index());
        }
        self.part_file
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let part_file = self.part_file_path();
        storage_utils::delete_files(
            &self.files,
            &self.renamed,
            &self.save_path(),
            &part_file,
            options,
        )
    }

    /// `set_file_priority` — export out of the part file through the
    /// view on `dont_download`→wanted transitions. Upstream assigns
    /// `m_file_priority[i] = new_prio` eagerly and restores it on
    /// error (`scope_end`), so a failed file keeps its old priority
    /// for retry while earlier files keep theirs.
    pub fn set_file_priority(&mut self, prio: &[DownloadPriority]) -> StorageResult<()> {
        if prio.len() > self.file_priority.len() {
            self.file_priority
                .resize(prio.len(), DownloadPriority::DEFAULT);
        }
        for (idx, &new) in prio.iter().enumerate() {
            let Some(i32_idx) = i32::try_from(idx).ok() else {
                break;
            };
            let i = FileIndex(i32_idx);
            if i >= self.files.end_file() || self.files.pad_file_at(i) {
                continue;
            }
            let old_prio = self.file_priority[idx];
            // upstream sets the new priority *before* the transition —
            // `open_file` during export must see it (a priority-0 view
            // would open `sparse`).
            self.file_priority[idx] = new;

            let transition = |st: &mut Self| -> StorageResult<()> {
                if old_prio == DownloadPriority::DONT_DOWNLOAD
                    && new != DownloadPriority::DONT_DOWNLOAD
                {
                    // move stuff out of the part file — upstream opens
                    // the target file unconditionally, even with no
                    // part file (the view open is what creates/sizes
                    // it: mmap_storage.cpp:190).
                    let view = st.open_file(i, OpenMode::WRITE)?;
                    let has_part = st
                        .part_file
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .is_some();
                    if has_part && st.use_partfile(i) {
                        let file_offset = st.files.file_offset(i);
                        let size = st.files.file_size(i);
                        let g = st.part_file.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(pf) = g.as_ref() {
                            // `off` is relative to the exported range
                            // — write it into the *file-relative* view
                            // (upstream: `range().subspan(file_offset)`
                            // for mapped, `pwrite` at `file_offset`
                            // for fd; `write_at` picks by map state).
                            pf.export_file(file_offset, size, |off, buf| {
                                view.write_at(u64::try_from(off).unwrap_or(0), buf)
                                    .map(|_| ())
                            })
                            .map_err(|e| StorageError::new(e, i, Operation::PartfileWrite))?;
                        }
                    }
                } else if old_prio != DownloadPriority::DONT_DOWNLOAD
                    && new == DownloadPriority::DONT_DOWNLOAD
                {
                    // move stuff into the part file — not implemented
                    // upstream either; just stop using the part file
                    // when the file exists on disk.
                    let p = st.resolve_path(i);
                    let exists = p
                        .try_exists()
                        .map_err(|e| StorageError::new(e, i, Operation::FileStat))?;
                    st.set_use_partfile(i, !exists);
                }
                Ok(())
            };
            if let Err(e) = transition(self) {
                // `restore_prio` — the failed file reverts so a retry
                // can run again.
                self.file_priority[idx] = old_prio;
                return Err(e);
            }

            if self.file_priority[idx] == DownloadPriority::DONT_DOWNLOAD && self.use_partfile(i) {
                self.need_partfile();
            }
        }
        let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pf) = g.as_ref() {
            pf.flush_metadata()
                .map_err(|e| StorageError::new(e, FileIndex(-1), Operation::PartfileWrite))?;
        }
        Ok(())
    }

    /// `verify_resume_data` — `(verdict, error)` like upstream.
    pub fn verify_resume_data(
        &self,
        have_pieces: &bitdaemon_common::bitfield::Bitfield,
        seed_mode: bool,
        links: &[PathBuf],
    ) -> (bool, Option<StorageError>) {
        storage_utils::verify_resume_data(
            &self.files,
            &self.renamed,
            have_pieces,
            seed_mode,
            links,
            &self.file_priority,
            &self.stat_cache,
            &self.save_path(),
        )
    }

    /// `has_any_file` — any positive-size file on disk, or a part
    /// file present (upstream `mmap_storage::has_any_file` is the
    /// same as `pread_storage`'s).
    pub fn has_any_file(&self) -> StorageResult<bool> {
        if storage_utils::has_any_file(
            &self.files,
            &self.renamed,
            &self.save_path(),
            &self.stat_cache,
        )? {
            return Ok(true);
        }
        match bitdaemon_disk::fs::stat_file(
            &self.part_file_path(),
            bitdaemon_disk::fs::FileStatusFlags::NONE,
        ) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::new(e, FileIndex(-1), Operation::FileStat)),
        }
    }
}

impl Drop for MmapStorage {
    fn drop(&mut self) {
        // ~mmap_storage (mmap_storage.cpp:109-117) — flush part-file
        // metadata (its own `Drop` runs on the `take`) and release
        // this torrent's views; may run on a foreign thread upstream
        // — ours is `Send`-safe.
        if let Some(pf) = self
            .part_file
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = pf.flush_metadata();
        }
        let idx = *self
            .storage_index
            .get_mut()
            .unwrap_or_else(|e| e.into_inner());
        self.pool.release_storage(idx);
    }
}

impl DiskStorage for MmapStorage {
    fn set_storage_index(&self, idx: StorageIndex) {
        MmapStorage::set_storage_index(self, idx);
    }
    fn files(&self) -> &FileStorage {
        &self.files
    }
    fn file_priorities(&self) -> &[DownloadPriority] {
        &self.file_priority
    }
    fn read(
        &self,
        buf: &mut [u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
    ) -> StorageResult<i64> {
        MmapStorage::read(self, buf, piece, offset, mode, volatile, false)
    }
    fn read_with_hints(
        &self,
        buf: &mut [u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
        flush: bool,
    ) -> StorageResult<i64> {
        MmapStorage::read(self, buf, piece, offset, mode, volatile, flush)
    }
    fn write(
        &self,
        buf: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
    ) -> StorageResult<i64> {
        MmapStorage::write(self, buf, piece, offset, mode, false, false)
    }
    fn write_with_hints(
        &self,
        buf: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
        flush: bool,
    ) -> StorageResult<i64> {
        MmapStorage::write(self, buf, piece, offset, mode, volatile, flush)
    }
    fn hash_with(
        &self,
        feed: &mut dyn FnMut(&[u8]),
        len: i64,
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
    ) -> StorageResult<i64> {
        MmapStorage::hash_with(self, |d| feed(d), len, piece, offset, mode, volatile, false)
    }
    #[allow(clippy::too_many_arguments)]
    fn hash_with_hints(
        &self,
        feed: &mut dyn FnMut(&[u8]),
        len: i64,
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
        flush: bool,
    ) -> StorageResult<i64> {
        MmapStorage::hash_with(self, |d| feed(d), len, piece, offset, mode, volatile, flush)
    }
    fn release_files(&self) -> StorageResult<()> {
        MmapStorage::release_files(self)
    }
    fn move_storage(&self, path: PathBuf, flags: MoveFlags) -> (u8, PathBuf, Option<StorageError>) {
        MmapStorage::move_storage(self, path, flags)
    }
    fn delete_files(&self, options: u8) -> StorageResult<()> {
        MmapStorage::delete_files(self, options)
    }
    fn rename_file(&mut self, index: FileIndex, new_name: &[u8]) -> StorageResult<()> {
        MmapStorage::rename_file(self, index, new_name)
    }
    fn set_file_priority(&mut self, prio: &[DownloadPriority]) -> StorageResult<()> {
        MmapStorage::set_file_priority(self, prio)
    }
    fn verify_resume_data(
        &self,
        have_pieces: &bitdaemon_common::bitfield::Bitfield,
        seed_mode: bool,
        links: &[PathBuf],
    ) -> (bool, Option<StorageError>) {
        MmapStorage::verify_resume_data(self, have_pieces, seed_mode, links)
    }

    fn has_any_file(&self) -> StorageResult<bool> {
        MmapStorage::has_any_file(self)
    }
    fn set_open_mode_extra(&self, mode: OpenMode) {
        self.open_mode_extra.store(mode.bits(), Ordering::Relaxed);
    }
    fn set_need_tick(&self) -> bool {
        MmapStorage::set_need_tick(self)
    }
    fn do_tick(&self) {
        MmapStorage::do_tick(self);
    }
    fn close_oldest(&self) {
        // the mmap backend's pooled resource is the view pool —
        // upstream's `m_file_pool.close_oldest()` under mmap_disk_io.
        self.pool.close_oldest();
    }
    fn file_status(&self) -> Vec<OpenFileState> {
        self.pool.get_status(self.storage_index())
    }
    fn set_mmap_file_size_cutoff(&self, blocks: i64) {
        self.mmap_file_size_cutoff.store(blocks, Ordering::Relaxed);
    }
    fn set_disk_write_mode(&self, mode: i32) {
        self.disk_write_mode
            .store(i64::from(mode), Ordering::Relaxed);
    }
    fn set_no_cow(&self, v: bool) {
        self.no_cow.store(v, Ordering::Relaxed);
    }
    fn store_precomputed_v2(
        &self,
        piece: PieceIndex,
        block: i32,
        h: &bitdaemon_common::Sha256Hash,
    ) {
        // upstream forwards to `m_precomputed_v2.store` with
        // `files().blocks_in_piece2(piece)` (mmap_storage.cpp:126-130)
        // — same computation `PreadStorage` performs.
        let bs = bitdaemon_disk::disk_cache::DEFAULT_BLOCK_SIZE as i32;
        let num = (self.files.piece_size2(piece) + bs - 1) / bs;
        self.precomputed.store(piece, block, num, h);
    }
    fn take_precomputed_v2_block(
        &self,
        piece: PieceIndex,
        block: i32,
    ) -> Option<bitdaemon_common::Sha256Hash> {
        self.precomputed.take_block(piece, block)
    }
    fn drop_precomputed_v2(&self, piece: PieceIndex) {
        self.precomputed.drop(piece);
    }
}

/// `mmap_disk_io`'s `default_storage_constructor` — the
/// `StorageFactory` downstream installs to use mmap storage
/// (upstream swaps `disk_interface` impl; our seam is the factory).
#[must_use]
pub fn mmap_storage_constructor(
    pool: Arc<FileViewPool>,
) -> bitdaemon_disk::storage::StorageFactory {
    Arc::new(move |params: &StorageParams<'_>, _file_pool| {
        Box::new(MmapStorage::new(
            StorageParams {
                files: params.files,
                renamed: params.renamed,
                path: params.path.clone(),
                part_file_dir: params.part_file_dir.clone(),
                mode: params.mode,
                priorities: params.priorities,
                info_hash: params.info_hash,
                v1: params.v1,
                v2: params.v2,
            },
            pool.clone(),
        )) as Box<dyn DiskStorage>
    })
}
