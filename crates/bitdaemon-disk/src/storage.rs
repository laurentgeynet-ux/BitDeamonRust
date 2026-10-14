//! Port of `aux_/pread_storage.hpp`/`pread_storage.cpp` — the
//! positioned-I/O storage backend (`storage_interface` upstream).
//!
//! Covers the same surface: `read`/`write`/`hash`/`hash2` (via the
//! shared `readwrite` piece→file walk, part-file redirection for
//! `dont_download` files, pad files yielding zeroes), `initialize`,
//! `set_file_priority` (export out of / consult the part file),
//! `move_storage`, `delete_files`, `rename_file`, `release_files`,
//! `has_any_file`, `verify_resume_data`, `tick` (flush part-file
//! metadata).

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bitdaemon_common::types::{DownloadPriority, FileIndex, PieceIndex, StorageIndex};
use bitdaemon_core::file_storage::{FileFlags, FileStorage, RenamedFiles};
use bitdaemon_core::path::bytes_to_path;

use crate::file::pwrite_all;
use crate::file_pool::FilePool;
use crate::fs;
use crate::open_mode::OpenMode;
use crate::part_file::PartFile;
use crate::readwrite::{readwrite_buf, readwrite_mut};
use crate::stat_cache::StatCache;
use crate::storage_error::{Operation, StorageError, StorageResult};

/// `move_flags_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveFlags {
    /// `always_replace_files`.
    AlwaysReplaceFiles,
    /// `fail_if_exist`.
    FailIfExist,
    /// `dont_replace`.
    DontReplace,
    /// `reset_save_path`.
    ResetSavePath,
    /// `reset_save_path_unchecked`.
    ResetSavePathUnchecked,
}

/// `remove_flags_t` — `session_handle::delete_files`/`delete_partfile`.
pub mod remove_flags {
    /// `session_handle::delete_files`.
    pub const DELETE_FILES: u8 = 1;
    /// `session_handle::delete_partfile`.
    pub const DELETE_PARTFILE: u8 = 2;
}

/// `status_t` — result bitmask of `initialize`/`move_storage`/
/// `verify_resume_data` (`disk_status` upstream).
pub mod disk_status {
    /// `disk_status::fatal_disk_error`.
    pub const FATAL_DISK_ERROR: u8 = 1;
    /// `disk_status::need_full_check`.
    pub const NEED_FULL_CHECK: u8 = 2;
    /// `disk_status::file_exist`.
    pub const FILE_EXIST: u8 = 4;
    /// `disk_status::oversized_file`.
    pub const OVERSIZED_FILE: u8 = 8;
    /// `disk_status::job_deferred`.
    pub const JOB_DEFERRED: u8 = 16;
}

/// `storage_mode_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageMode {
    /// `storage_mode_sparse` — write only where data lands.
    #[default]
    Sparse,
    /// `storage_mode_allocate` — pre-allocate full files.
    Allocate,
}

/// `storage_params` — parameter pack for constructing a storage.
#[derive(Clone)]
pub struct StorageParams<'a> {
    /// `files` — the torrent's file layout.
    pub files: &'a FileStorage,
    /// `renamed_files` — rename overlay.
    pub renamed: &'a RenamedFiles,
    /// `path` — save directory.
    pub path: PathBuf,
    /// `part_file_dir` — dir for `.parts` (empty → under `path`).
    pub part_file_dir: PathBuf,
    /// `mode`.
    pub mode: StorageMode,
    /// `priorities` — per-file download priorities.
    pub priorities: &'a [DownloadPriority],
    /// `info_hash` — SHA-1 (used to name the `.parts` file).
    pub info_hash: [u8; 20],
    /// `v1`/`v2` — protocol version flags.
    pub v1: bool,
    /// see `v1`.
    pub v2: bool,
}

/// `aux::pread_storage` — reference storage implementation using
/// positioned reads/writes through the shared `FilePool`.
pub struct PreadStorage {
    files: FileStorage,
    renamed: RenamedFiles,
    /// `m_save_path` — upstream `move_storage` assigns the new path
    /// back into the storage (`std::tie(ret, m_save_path) = ...`), so
    /// every subsequent resolve/`open_file` targets the *new*
    /// location. Interior mutability because the upstream entry point
    /// runs under the fence (shared access).
    save_path: Mutex<PathBuf>,
    part_file_dir: PathBuf,
    part_file_name: PathBuf,
    pool: Arc<FilePool>,
    /// `m_allocate_files` — `storage_mode_allocate`: files are
    /// physically reserved (`posix_fallocate`/`SetFileValidData`) on
    /// their first write-open.
    allocate_files: bool,
    v1: bool,
    v2: bool,
    file_priority: Vec<DownloadPriority>,
    use_partfile: Vec<bool>,
    part_file: Mutex<Option<PartFile>>,
    stat_cache: StatCache,
    storage_index: Mutex<StorageIndex>,
    /// `m_file_created` — which files have already been sized to their
    /// final length this session (upstream adds `open_mode::truncate`
    /// on the first write-open only).
    file_created: Mutex<Vec<bool>>,
    need_tick: std::sync::atomic::AtomicBool,
    /// `m_precomputed` — v2 block hashes saved by hash jobs for
    /// later `hash2` piece-layer replies (upstream
    /// `pread_storage::m_precomputed_v2`).
    precomputed: crate::precomputed_block_hashes::PrecomputedBlockHashes,
    /// `settings_interface` flags snapshot — `allow_set_file_valid_data`,
    /// `no_atime`, applied to every `open_file` (upstream reads the
    /// settings inside `open_file`; we can't reach them, so the
    /// `DiskIo` pushes them via `set_open_mode_extra`).
    open_mode_extra: std::sync::atomic::AtomicU32,
    /// `disk_io_write_mode == write_through` — `sync_file` after each
    /// landed write (upstream reads the setting inside `write`).
    write_through: std::sync::atomic::AtomicBool,
    /// `disk_io_write_mode ∈ {disable_os_cache, write_through}` —
    /// `open_mode::no_cache` on EVERY open, reads included (upstream
    /// `pread_storage::open_file`, pread_storage.cpp:780-785).
    no_cache_open: std::sync::atomic::AtomicBool,
}

impl PreadStorage {
    /// `pread_storage(params, pool)`.
    #[must_use]
    pub fn new(params: StorageParams<'_>, pool: Arc<FilePool>) -> Self {
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
            save_path: Mutex::new(crate::fs::complete(&params.path)),
            part_file_dir: params.part_file_dir,
            part_file_name: bytes_to_path(&name),
            pool,
            allocate_files: params.mode == StorageMode::Allocate,
            v1: params.v1,
            v2: params.v2,
            file_priority: params.priorities.to_vec(),
            use_partfile: Vec::new(),
            part_file: Mutex::new(None),
            stat_cache: StatCache::new(),
            storage_index: Mutex::new(StorageIndex(0)),
            file_created: Mutex::new(vec![false; params.files.num_files() as usize]),
            need_tick: std::sync::atomic::AtomicBool::new(false),
            precomputed: crate::precomputed_block_hashes::PrecomputedBlockHashes::new(),
            open_mode_extra: std::sync::atomic::AtomicU32::new(0),
            write_through: std::sync::atomic::AtomicBool::new(false),
            no_cache_open: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// `files()` — the file layout.
    #[must_use]
    pub fn files(&self) -> &FileStorage {
        &self.files
    }

    /// `v1()`/`v2()` — protocol-version flags.
    #[must_use]
    pub fn v1(&self) -> bool {
        self.v1
    }

    /// see [`Self::v1`].
    #[must_use]
    pub fn v2(&self) -> bool {
        self.v2
    }

    /// `storage_index()`/`set_storage_index`.
    #[must_use]
    pub fn storage_index(&self) -> StorageIndex {
        *self.storage_index.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// see [`Self::storage_index`].
    pub fn set_storage_index(&self, st: StorageIndex) {
        *self.storage_index.lock().unwrap_or_else(|e| e.into_inner()) = st;
    }

    /// `set_need_tick`/`do_tick` — cheap flush deferral.
    pub fn set_need_tick(&self) -> bool {
        self.need_tick
            .swap(true, std::sync::atomic::Ordering::Relaxed)
    }

    /// see [`Self::set_need_tick`].
    pub fn do_tick(&self) {
        self.need_tick
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.tick();
    }

    /// `use_partfile(index)` — sparse vec, defaults to true.
    fn use_partfile(&self, index: FileIndex) -> bool {
        self.use_partfile.get(index.get()).copied().unwrap_or(true)
    }

    fn set_use_partfile(&mut self, index: FileIndex, b: bool) {
        if index.get() >= self.use_partfile.len() {
            if b {
                return; // default is already true
            }
            self.use_partfile.resize(index.get() + 1, true);
        }
        self.use_partfile[index.get()] = b;
    }

    /// current save path (`m_save_path` — mutable via `move_storage`).
    fn save_path(&self) -> PathBuf {
        self.save_path
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `combine_path(m_save_path, m_part_file_dir)` when a custom
    /// part-file dir is configured, else `m_save_path` — upstream's
    /// part-file base (upstream joins it *under* the save path; a
    /// bare `part_file_dir` would escape the torrent dir).
    fn part_file_dir_path(&self) -> PathBuf {
        let save = self.save_path();
        if self.part_file_dir.as_os_str().is_empty() {
            save
        } else {
            save.join(&self.part_file_dir)
        }
    }

    /// full path of the `.parts` file (dir + name).
    fn part_file_path(&self) -> PathBuf {
        self.part_file_dir_path().join(&self.part_file_name)
    }

    fn resolve_path(&self, index: FileIndex) -> PathBuf {
        let rel = self.renamed.file_path(&self.files, index, &[]);
        self.save_path().join(bytes_to_path(&rel))
    }

    /// `open_file` — pooled open honoring the rename overlay.
    /// `pread_storage::open_file` (pread_storage.cpp:708-768): the
    /// first write-open of a file per session gets `truncate` —
    /// `file_handle` then sizes it to `file_size` and, when not
    /// sparse, reserves the space (`posix_fallocate`/`SetFileValidData`);
    /// `m_file_created` remembers it. `file_flags` → exec/hidden;
    /// sparse for every file when `!m_allocate_files` and for
    /// priority-0 files unconditionally; `extra_mode` folds in
    /// settings-driven flags (`allow_set_file_valid_data`, `no_atime`).
    fn open_file(
        &self,
        index: FileIndex,
        mode: OpenMode,
    ) -> Result<Arc<crate::file::FileHandle>, StorageError> {
        let mut mode = mode | self.extra_mode();
        if mode.contains(OpenMode::WRITE) && !mode.contains(OpenMode::TRUNCATE) {
            let mut created = self.file_created.lock().unwrap_or_else(|e| e.into_inner());
            // upstream resizes defensively first (pread_storage.cpp:720)
            let n = usize::try_from(self.files.num_files()).unwrap_or(0);
            if created.len() != n {
                created.resize(n, false);
            }
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
        // `write_mode ∈ {disable_os_cache, write_through}` → no_cache
        // on every open — the cache already holds the data, so it
        // shouldn't be left in the OS cache too (pread_storage.cpp:780).
        if self
            .no_cache_open
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            mode |= OpenMode::NO_CACHE;
        }
        let handle = self.pool.open_file_at(
            (self.storage_index(), index),
            &self.resolve_path(index),
            self.files.file_size(index),
            mode,
        )?;
        if mode.contains(OpenMode::TRUNCATE) {
            // sized once per session — upstream sets the bit after a
            // successful open (pool hit or real open alike).
            self.file_created.lock().unwrap_or_else(|e| e.into_inner())[index.get()] = true;
        }
        Ok(handle)
    }

    /// settings-driven extra open flags (upstream reads
    /// `settings_interface` inside `open_file` — we snapshot it at
    /// construction).
    fn extra_mode(&self) -> OpenMode {
        OpenMode::from_bits(
            self.open_mode_extra
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// `need_partfile` — lazily construct the part file at
    /// `combine_path(save_path, part_file_dir)` (or `save_path` when
    /// no custom dir is set).
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

    /// `read(buffer, piece, offset, mode, flags)` — fill `buffer` with
    /// piece data; pad files yield zeroes; `dont_download` files are
    /// served from the part file.
    pub fn read(
        &self,
        buffer: &mut [u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile_read: bool,
    ) -> StorageResult<i64> {
        readwrite_mut(
            &self.files,
            buffer,
            piece,
            offset,
            |file_index, file_offset, buf| {
                // pad files yield zeroes
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
                        // invariant kept by `initialize`/`set_file_priority`
                        // (upstream relies on `TORRENT_ASSERT(m_part_file)`)
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
                let handle = self.open_file(file_index, mode)?;
                // `pread_all` — a file physically shorter than its
                // declared range is `file_too_short` upstream, not a
                // silent slide into the next file's bytes
                let n = crate::file::pread_all(
                    handle.file(),
                    buf,
                    u64::try_from(file_offset).unwrap_or(0),
                )
                .map_err(|e| StorageError::new(e, file_index, Operation::FileRead))?;
                if volatile_read {
                    crate::file::advise_dont_need(
                        handle.file(),
                        u64::try_from(file_offset).unwrap_or(0),
                        buf.len() as u64,
                    );
                }
                Ok(n)
            },
        )
    }

    /// `write(buffer, piece, offset, mode, flags)` — single-buffer
    /// write; `dont_download` files go to the part file.
    pub fn write(
        &self,
        buffer: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
    ) -> StorageResult<i64> {
        self.writev(&[buffer], piece, offset, mode)
    }

    /// `write(buffers, piece, offset, mode, flags)` — scatter write
    /// (upstream `readwrite_vec` variant).
    pub fn writev(
        &self,
        buffers: &[&[u8]],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
    ) -> StorageResult<i64> {
        // flatten: equivalent to upstream's vec walk; the on-wire
        // semantics are identical since we keep the same order
        let flat_len: usize = buffers.iter().map(|b| b.len()).sum();
        let mut flat = Vec::with_capacity(flat_len);
        for b in buffers {
            flat.extend_from_slice(b);
        }
        readwrite_buf(
            &self.files,
            &flat,
            piece,
            offset,
            |file_index, file_offset, buf| {
                if self.files.pad_file_at(file_index) {
                    return Ok(buf.len()); // discard writes to pad files
                }
                if file_index.get() < self.file_priority.len()
                    && self.file_priority[file_index.get()] == DownloadPriority::DONT_DOWNLOAD
                    && self.use_partfile(file_index)
                {
                    self.need_partfile();
                    let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
                    let Some(pf) = g.as_ref() else {
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
                let handle = self.open_file(file_index, mode | OpenMode::WRITE)?;
                let n = pwrite_all(handle.file(), buf, u64::try_from(file_offset).unwrap_or(0))
                    .map_err(|e| StorageError::new(e, file_index, Operation::FileWrite))?;
                // upstream: writes invalidate the cached stat entry
                self.stat_cache.set_dirty(file_index);
                // `write_mode == write_through` → `sync_file` after
                // each write (upstream pread_storage.cpp:522-523);
                // sync errors are swallowed upstream (no ec out-param)
                if self
                    .write_through
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    let _ = crate::file::sync_file(
                        handle.file(),
                        u64::try_from(file_offset).unwrap_or(0),
                        buf.len() as u64,
                    );
                }
                Ok(n)
            },
        )
    }

    /// `hash(ph, len, piece, offset)` — feed `len` bytes of piece data
    /// to `feed` (a SHA-1 `update` closure); pad files contribute
    /// zeroes; part-file-backed files hash from the part file.
    pub fn hash_with(
        &self,
        mut feed: impl FnMut(&[u8]),
        len: i64,
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile_read: bool,
    ) -> StorageResult<i64> {
        let mut scratch: Vec<u8> = Vec::new();
        let len_usize = usize::try_from(len).unwrap_or(0);
        // drive readwrite with a phantom buffer of `len` bytes
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
                let handle = self.open_file(file_index, mode)?;
                scratch.resize(buf.len(), 0);
                // `pread_all` like upstream — a short file is
                // `file_too_short`, never a partial feed
                let n = crate::file::pread_all(
                    handle.file(),
                    &mut scratch,
                    u64::try_from(file_offset).unwrap_or(0),
                )
                .map_err(|e| StorageError::new(e, file_index, Operation::FileRead))?;
                feed(&scratch[..n]);
                if volatile_read {
                    crate::file::advise_dont_need(
                        handle.file(),
                        u64::try_from(file_offset).unwrap_or(0),
                        buf.len() as u64,
                    );
                }
                Ok(n)
            },
        )
    }

    /// current per-file priorities (upstream `m_file_priority`).
    #[must_use]
    pub fn file_priorities(&self) -> &[DownloadPriority] {
        &self.file_priority
    }

    /// `has_any_file` — upstream: any file (pad/zero-size included —
    /// the `sz > 0` check inside does the filtering) with a positive
    /// size on disk, **or** a part file present.
    pub fn has_any_file(&self) -> StorageResult<bool> {
        if crate::storage_utils::has_any_file(
            &self.files,
            &self.renamed,
            &self.save_path(),
            &self.stat_cache,
        )? {
            return Ok(true);
        }
        // also check the part file (upstream stats it with
        // `error_file_partfile` — a part file full of priority-0 data
        // still means "we have data on disk")
        match fs::stat_file(&self.part_file_path(), fs::FileStatusFlags::NONE) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            // upstream `ec.file(torrent_status::error_file_partfile)` = -6
            Err(e) => Err(StorageError::new(e, FileIndex(-6), Operation::FileStat)),
        }
    }

    /// `release_files` — flush part-file metadata, close all pooled
    /// handles, and drop the stat cache (upstream clears it so the
    /// next access re-stats a possibly-externally-changed disk).
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

    /// `rename_file(index, new_filename)` — release the pooled handle
    /// first (upstream: a pooled handle would pin the old name on
    /// Windows), then rename on disk when the file exists, else record
    /// the rename for later creation.
    pub fn rename_file(&mut self, index: FileIndex, new_filename: &[u8]) -> StorageResult<()> {
        self.pool.release_one(self.storage_index(), index);
        let save_path = self.save_path();
        crate::storage_utils::rename_file(
            &self.files,
            &mut self.renamed,
            index,
            new_filename,
            &save_path,
        )
    }

    /// `initialize` — create directories, allocate files when
    /// requested, detect oversized files; fill `use_partfile` for
    /// priority-0 files (existing files take precedence, upstream
    /// backward-compat rule).
    pub fn initialize(&mut self) -> StorageResult<u8> {
        self.stat_cache
            .reserve(usize::try_from(self.files.num_files()).unwrap_or(0));
        // upstream (pread_storage.cpp:233-244): don't do full file
        // allocations on network drives — Windows-only in the pread
        // backend (`#ifdef TORRENT_WINDOWS` + `GetDriveTypeW`; the
        // mmap backend uses the cross-platform `get_drive_info`).
        #[cfg(windows)]
        if crate::fs::drive_kind(&self.save_path()) == crate::fs::DriveKind::Remote {
            self.allocate_files = false;
        }
        // `m_file_created.resize(num_files, false)` — extends with
        // false but keeps the bits already set this session.
        self.file_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .resize(usize::try_from(self.files.num_files()).unwrap_or(0), false);
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
            // upstream `stat_cache::get_filesize` pretends symlink-
            // flagged files don't exist — they get special
            // create/validate logic instead of a blind stat of the
            // target (stat_cache.cpp:60-68).
            let res = if self.files.file_flags(i).contains(FileFlags::SYMLINK) {
                Err(io::Error::from(io::ErrorKind::NotFound))
            } else {
                self.stat_cache.get_filesize_path(i, &p)
            };
            match res {
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
        crate::storage_utils::initialize_storage(
            &self.files,
            &self.renamed,
            &self.save_path(),
            stat_cache_ptr,
            &self.file_priority,
            |i| self.open_file(i, OpenMode::WRITE).map(|_| ()),
            // upstream reports oversized *non*-priority-0 files
            // through this callback too (priority-0 ones were flagged
            // in the pass above)
            |_i, _size| ret |= disk_status::OVERSIZED_FILE,
        )?;
        // upstream releases files it opened during initialize (they
        // were opened in write mode and would pin sparse/alloc state)
        self.pool.release_storage(self.storage_index());
        Ok(ret)
    }

    /// `move_storage(new_save_path, flags)` — release every pooled
    /// handle first (Windows refuses renames of open files), move all
    /// files + the part file, then commit `m_save_path` and drop the
    /// stat cache. Returns upstream's `(status, new_path, error)`
    /// triple — on failure the path is the *unchanged* old one.
    pub fn move_storage(
        &self,
        new_save_path: PathBuf,
        flags: MoveFlags,
    ) -> (u8, PathBuf, Option<StorageError>) {
        self.pool.release_storage(self.storage_index());
        let save_path = self.save_path();
        let part_dir = self.part_file_dir.clone();
        let mut pf = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        let (status, path, error) = crate::storage_utils::move_storage(
            &self.files,
            &self.renamed,
            save_path,
            new_save_path,
            |path| {
                let dir = if part_dir.as_os_str().is_empty() {
                    path.to_path_buf()
                } else {
                    // upstream `combine_path(p, m_part_file_dir)` +
                    // the `create_directories` it does first
                    let d = path.join(&part_dir);
                    fs::create_directories(&d)?;
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

    /// `delete_files(options)` — release the pool and destroy the
    /// part-file object *first* (upstream order: a live handle would
    /// pin the file we're about to delete; destroying it flushes the
    /// metadata), then delete the torrent files + part file.
    pub fn delete_files(&self, options: u8) -> StorageResult<()> {
        self.pool.release_storage(self.storage_index());
        // `delete(m_part_file)` upstream — Drop flushes metadata
        self.part_file
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let part_file = self.part_file_path();
        crate::storage_utils::delete_files(
            &self.files,
            &self.renamed,
            &self.save_path(),
            &part_file,
            options,
        )
    }

    /// `set_file_priority` — move data in/out of the part file as
    /// priorities cross the `dont_download` boundary. Upstream applies
    /// each file's new priority *in place* (`m_file_priority[i]`), so
    /// on error the files processed so far keep their new priority and
    /// the failing one keeps its old — mirrored here (no
    /// all-or-nothing copy).
    pub fn set_file_priority(&mut self, prio: &[DownloadPriority]) -> StorageResult<()> {
        // extend our file priorities in case it's truncated; the
        // default assumed priority is 4 (the default)
        if prio.len() > self.file_priority.len() {
            self.file_priority
                .resize(prio.len(), DownloadPriority::DEFAULT);
        }
        for (idx, &new) in prio.iter().enumerate() {
            let Some(i32_idx) = i32::try_from(idx).ok() else {
                break;
            };
            let i = FileIndex(i32_idx);
            // pad files always have priority 0
            if i >= self.files.end_file() || self.files.pad_file_at(i) {
                continue;
            }
            let old_prio = self.file_priority[idx];
            // upstream assigns `m_file_priority[i] = new_prio` *before*
            // the transition work and restores it on failure
            // (`scope_end` guard) — a failed file keeps its old
            // priority for retry while earlier files keep theirs.
            // Assigning early also means the priority-lift `open_file`
            // below doesn't open `sparse` (prio-0 → sparse upstream).
            self.file_priority[idx] = new;

            let transition = |st: &mut Self| -> StorageResult<()> {
                if old_prio == DownloadPriority::DONT_DOWNLOAD
                    && new != DownloadPriority::DONT_DOWNLOAD
                {
                    // move stuff out of the part file — upstream opens
                    // the file unconditionally (pread_storage.cpp
                    // mirror of mmap_storage.cpp:190), *then* exports
                    // if `m_part_file && use_partfile(i)`.
                    let handle = st.open_file(i, OpenMode::WRITE)?;
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
                            // `off` is relative to the exported range —
                            // the target offset inside file `i`. (The
                            // old code added `file_offset` again —
                            // for file index > 0 it wrote past EOF
                            // while the part-file slots were freed.)
                            pf.export_file(file_offset, size, |off, buf| {
                                pwrite_all(handle.file(), buf, u64::try_from(off).unwrap_or(0))
                                    .map(|_| ())
                            })
                            .map_err(|e| StorageError::new(e, i, Operation::PartfileWrite))?;
                        }
                    }
                } else if old_prio != DownloadPriority::DONT_DOWNLOAD
                    && new == DownloadPriority::DONT_DOWNLOAD
                {
                    // move stuff into the part file — not implemented
                    // upstream either; just stop using the part file when
                    // the file exists on disk
                    let p = st.resolve_path(i);
                    let exists = p
                        .try_exists()
                        .map_err(|e| StorageError::new(e, i, Operation::FileStat))?;
                    st.set_use_partfile(i, !exists);
                }
                Ok(())
            };
            if let Err(e) = transition(self) {
                self.file_priority[idx] = old_prio;
                return Err(e);
            }

            if self.file_priority[idx] == DownloadPriority::DONT_DOWNLOAD && self.use_partfile(i) {
                self.need_partfile();
            }
        }
        let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pf) = g.as_ref() {
            // upstream `ec.file(error_file_partfile)` = -6
            pf.flush_metadata()
                .map_err(|e| StorageError::new(e, FileIndex(-6), Operation::PartfileWrite))?;
        }
        Ok(())
    }

    /// `verify_resume_data` — check the fastresume record against
    /// on-disk state. Returns `(verify_success, error)` — upstream's
    /// `bool` + `storage_error&` pair; `false` is a *verdict* (the
    /// caller turns it into `need_full_check`), not a hard failure.
    pub fn verify_resume_data(
        &self,
        have_pieces: &bitdaemon_common::bitfield::Bitfield,
        seed_mode: bool,
        links: &[std::path::PathBuf],
    ) -> (bool, Option<StorageError>) {
        crate::storage_utils::verify_resume_data(
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
}

impl Drop for PreadStorage {
    fn drop(&mut self) {
        // ~pread_storage (pread_storage.cpp:61-68) — release this
        // torrent's pooled handles; the part file's own `Drop`
        // flushes its metadata.
        let idx = *self
            .storage_index
            .get_mut()
            .unwrap_or_else(|e| e.into_inner());
        self.pool.release_storage(idx);
    }
}

/// `DiskStorage` — object-safe view of a torrent storage, the seam the
/// disk-io orchestrator dispatches through. This is the downstream
/// **injection point** (ADR-0008): OnionBit's `PrivateStorageFactory`
/// supplies its own implementation; the default is `PreadStorage`.
///
/// Upstream libtorrent 2.x removed `storage_interface` as a
/// customization point (see `storage.hpp` — "customize disk_interface
/// instead"); we keep a narrower trait because OnionBit replaces
/// *storage* (encrypted, opaque names), not the job machinery.
pub trait DiskStorage: Send + Sync {
    /// `storage_index`/`set_storage_index`.
    fn set_storage_index(&self, idx: StorageIndex);
    /// `files()`.
    fn files(&self) -> &FileStorage;
    /// `file_priorities`.
    fn file_priorities(&self) -> &[DownloadPriority];
    /// piece-aligned read (part-file aware).
    fn read(
        &self,
        buf: &mut [u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
    ) -> StorageResult<i64>;
    /// piece-aligned write.
    fn write(
        &self,
        buf: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
    ) -> StorageResult<i64>;
    /// `hash` — feed every byte of the range to `feed`.
    fn hash_with(
        &self,
        feed: &mut dyn FnMut(&[u8]),
        len: i64,
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
    ) -> StorageResult<i64>;
    /// `release_files`.
    fn release_files(&self) -> StorageResult<()>;
    /// `move_storage` — upstream returns `(status_t, new_path)` plus a
    /// `storage_error&`; on `fatal_disk_error`/`file_exist` the path
    /// is the *unchanged* old one. Modeled as `(status, path, error)`.
    fn move_storage(&self, path: PathBuf, flags: MoveFlags) -> (u8, PathBuf, Option<StorageError>);
    /// `delete_files`.
    fn delete_files(&self, options: u8) -> StorageResult<()>;
    /// `rename_file`.
    fn rename_file(&mut self, index: FileIndex, new_name: &[u8]) -> StorageResult<()>;
    /// `set_file_priority`.
    fn set_file_priority(&mut self, prio: &[DownloadPriority]) -> StorageResult<()>;
    /// `verify_resume_data` — `(verify_success, error)`; `false` is a
    /// verdict (`need_full_check`), not a hard failure.
    fn verify_resume_data(
        &self,
        have_pieces: &bitdaemon_common::bitfield::Bitfield,
        seed_mode: bool,
        links: &[PathBuf],
    ) -> (bool, Option<StorageError>);
    /// `has_any_file` — true if any non-pad file exists on disk;
    /// drives `need_full_check` when there is no resume data.
    /// Default `false` keeps injected storages source-compatible.
    fn has_any_file(&self) -> StorageResult<bool> {
        Ok(false)
    }
    /// `storage_holder::initialize` — deferred storage setup
    /// (upstream calls it on the first job that touches files).
    /// Returns `disk_status` bits (`oversized_file` among them).
    /// Default no-op: custom storages may not allocate files.
    fn initialize(&mut self) -> StorageResult<u8> {
        Ok(0)
    }
    /// `pread_storage::store_precomputed_v2` — keep a block SHA-256
    /// computed during piece hashing so later `hash2` requests skip
    /// the disk read. Default no-op (custom storages may skip it).
    fn store_precomputed_v2(
        &self,
        _piece: PieceIndex,
        _block: i32,
        _h: &bitdaemon_common::Sha256Hash,
    ) {
    }
    /// `take_precomputed_v2_block` — consume a stored block hash.
    fn take_precomputed_v2_block(
        &self,
        _piece: PieceIndex,
        _block: i32,
    ) -> Option<bitdaemon_common::Sha256Hash> {
        None
    }
    /// `drop_precomputed_v2` — `clear_piece` evicts the stored hashes.
    fn drop_precomputed_v2(&self, _piece: PieceIndex) {}
    /// Settings-driven extra `OpenMode` bits applied on every file
    /// open (`allow_set_file_valid_data`, `no_atime`) — upstream reads
    /// them from `settings_interface` inside `open_file`. Default
    /// no-op keeps injected storages source-compatible.
    fn set_open_mode_extra(&self, _mode: OpenMode) {}
    /// `set_need_tick` — a write job marked this storage's part-file
    /// metadata dirty; the disk-io loop enqueues it for a deferred
    /// `do_tick` (~2 min upstream, `m_need_tick`). The first setter
    /// returns `false` so the caller enqueues the storage once
    /// (upstream's `if (!st->set_need_tick()) push`).
    fn set_need_tick(&self) -> bool {
        false
    }
    /// `do_tick`/`tick` — deferred maintenance: flush part-file
    /// metadata. Driven by the disk-io worker's maintenance pass.
    fn do_tick(&self) {}
    /// `file_pool_impl::close_oldest` — shed the storage's LRU pooled
    /// resource on `close_file_interval` cadence. The shared fd pool
    /// is closed by `disk_io` itself; this targets storage-owned
    /// pools (the mmap view pool).
    fn close_oldest(&self) {}
    /// `file_pool::get_status(storage_index)` — per-file open state
    /// reported through `disk_io::get_status` (`open_file_state`
    /// upstream — the mmapped bit distinguishes view vs fd opens).
    fn file_status(&self) -> Vec<crate::file_pool::OpenFileState> {
        Vec::new()
    }
    /// `settings_pack::mmap_file_size_cutoff` — files at or below
    /// this many 16 KiB blocks open `no_mmap` (mmap backend only).
    fn set_mmap_file_size_cutoff(&self, _blocks: i64) {}
    /// `settings_pack::disk_write_mode` (`mmap_write_mode_t` ordinal:
    /// 0=`always_pwrite`, 1=`always_mmap_write`, 2=`auto_mmap_write`)
    /// — resolved in `initialize` (mmap backend only).
    fn set_disk_write_mode(&self, _mode: i32) {}
    /// `settings_pack::disk_disable_copy_on_write` — `no_cow` on
    /// truncate-opens (upstream applies it in the mmap backend only).
    fn set_no_cow(&self, _v: bool) {}
    /// `settings_pack::disk_io_write_mode == write_through` —
    /// `sync_file` after each landed write (upstream reads the
    /// setting inline inside `pread_storage::write`).
    fn set_write_through(&self, _v: bool) {}
    /// `settings_pack::disk_io_write_mode ∈ {disable_os_cache,
    /// write_through}` — `open_mode::no_cache` on every open
    /// (`pread_storage::open_file` / `mmap_storage::open_file`).
    fn set_write_no_cache(&self, _v: bool) {}
    /// `read` + `disk_job_flags_t` — upstream's mmap backend also
    /// honors `flush_piece` (`page_out` the read range); fd storages
    /// have no page machinery so the default folds to `read`.
    fn read_with_hints(
        &self,
        buf: &mut [u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
        flush: bool,
    ) -> StorageResult<i64> {
        let _ = flush;
        self.read(buf, piece, offset, mode, volatile)
    }
    /// `write` + `disk_job_flags_t` — same deal: `volatile_read` →
    /// `dont_need`, `flush_piece` → `page_out` on the mapped range.
    fn write_with_hints(
        &self,
        buf: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
        volatile: bool,
        flush: bool,
    ) -> StorageResult<i64> {
        let _ = (volatile, flush);
        self.write(buf, piece, offset, mode)
    }
    /// `hash`/`hash2` + `disk_job_flags_t` — same deal.
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
        let _ = flush;
        self.hash_with(feed, len, piece, offset, mode, volatile)
    }
}

impl DiskStorage for PreadStorage {
    fn set_storage_index(&self, idx: StorageIndex) {
        PreadStorage::set_storage_index(self, idx);
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
        PreadStorage::read(self, buf, piece, offset, mode, volatile)
    }
    fn write(
        &self,
        buf: &[u8],
        piece: PieceIndex,
        offset: i64,
        mode: OpenMode,
    ) -> StorageResult<i64> {
        PreadStorage::write(self, buf, piece, offset, mode)
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
        PreadStorage::hash_with(self, |d| feed(d), len, piece, offset, mode, volatile)
    }
    fn release_files(&self) -> StorageResult<()> {
        PreadStorage::release_files(self)
    }
    fn move_storage(&self, path: PathBuf, flags: MoveFlags) -> (u8, PathBuf, Option<StorageError>) {
        PreadStorage::move_storage(self, path, flags)
    }
    fn delete_files(&self, options: u8) -> StorageResult<()> {
        PreadStorage::delete_files(self, options)
    }
    fn rename_file(&mut self, index: FileIndex, new_name: &[u8]) -> StorageResult<()> {
        PreadStorage::rename_file(self, index, new_name)
    }
    fn set_file_priority(&mut self, prio: &[DownloadPriority]) -> StorageResult<()> {
        PreadStorage::set_file_priority(self, prio)
    }
    fn verify_resume_data(
        &self,
        have_pieces: &bitdaemon_common::bitfield::Bitfield,
        seed_mode: bool,
        links: &[PathBuf],
    ) -> (bool, Option<StorageError>) {
        PreadStorage::verify_resume_data(self, have_pieces, seed_mode, links)
    }
    fn has_any_file(&self) -> StorageResult<bool> {
        PreadStorage::has_any_file(self)
    }
    fn initialize(&mut self) -> StorageResult<u8> {
        PreadStorage::initialize(self)
    }
    fn store_precomputed_v2(
        &self,
        piece: PieceIndex,
        block: i32,
        h: &bitdaemon_common::Sha256Hash,
    ) {
        let bs = crate::disk_cache::DEFAULT_BLOCK_SIZE as i32;
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
    fn set_open_mode_extra(&self, mode: OpenMode) {
        self.open_mode_extra
            .store(mode.bits(), std::sync::atomic::Ordering::Relaxed);
    }
    fn set_write_through(&self, v: bool) {
        self.write_through
            .store(v, std::sync::atomic::Ordering::Relaxed);
    }
    fn set_write_no_cache(&self, v: bool) {
        self.no_cache_open
            .store(v, std::sync::atomic::Ordering::Relaxed);
    }
    fn set_need_tick(&self) -> bool {
        PreadStorage::set_need_tick(self)
    }
    fn do_tick(&self) {
        PreadStorage::do_tick(self);
    }
    fn file_status(&self) -> Vec<crate::file_pool::OpenFileState> {
        self.pool.get_status(self.storage_index())
    }
}

/// `StorageFactory` — injected constructor (upstream's removed
/// `storage_constructor` customization point, kept as a seam per
/// ADR-0008). Given `storage_params` + shared `FilePool`, produce the
/// torrent's storage.
pub type StorageFactory = Arc<
    dyn for<'a> Fn(&StorageParams<'a>, Arc<crate::file_pool::FilePool>) -> Box<dyn DiskStorage>
        + Send
        + Sync,
>;

/// `default_storage_constructor` — the upstream default.
#[must_use]
pub fn default_storage_factory() -> StorageFactory {
    Arc::new(|params, pool| Box::new(PreadStorage::new(params.clone(), pool)))
}
