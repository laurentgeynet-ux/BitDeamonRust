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
use bitdaemon_core::file_storage::{FileStorage, RenamedFiles};
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
    save_path: PathBuf,
    part_file_dir: PathBuf,
    part_file_name: PathBuf,
    pool: Arc<FilePool>,
    #[allow(dead_code)]
    allocate_files: bool,
    v1: bool,
    v2: bool,
    file_priority: Vec<DownloadPriority>,
    use_partfile: Vec<bool>,
    part_file: Mutex<Option<PartFile>>,
    stat_cache: StatCache,
    storage_index: Mutex<StorageIndex>,
    #[allow(dead_code)]
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
}

impl PreadStorage {
    /// `pread_storage(params, pool)`.
    #[must_use]
    pub fn new(params: StorageParams<'_>, pool: Arc<FilePool>) -> Self {
        assert!(params.v1 || params.v2, "storage must be v1 or v2");
        assert!(params.files.num_files() > 0);
        let mut name = b".0000000000000000000000000000000000000000.parts".to_vec();
        for (i, b) in params.info_hash.iter().enumerate() {
            name[1 + i * 2] = b"0123456789abcdef"[usize::from(b >> 4)];
            name[2 + i * 2] = b"0123456789abcdef"[usize::from(b & 0xf)];
        }
        Self {
            files: params.files.clone(),
            renamed: params.renamed.clone(),
            save_path: crate::fs::complete(&params.path),
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

    fn resolve_path(&self, index: FileIndex) -> PathBuf {
        let rel = self.renamed.file_path(&self.files, index, &[]);
        self.save_path.join(bytes_to_path(&rel))
    }

    /// `open_file` — pooled open honoring the rename overlay.
    /// `pread_storage::open_file_impl` (pread_storage.cpp:760-768) —
    /// resolves the effective `OpenMode` for `index`: sparse for
    /// every file when `!m_allocate_files`, and for priority-0 files
    /// unconditionally; `extra_mode` folds in settings-driven flags
    /// (`allow_set_file_valid_data`, `no_atime`).
    fn open_file(
        &self,
        index: FileIndex,
        mode: OpenMode,
    ) -> Result<Arc<crate::file::FileHandle>, StorageError> {
        let mut mode = mode | self.extra_mode();
        if !self.allocate_files
            || (index.get() < self.file_priority.len()
                && self.file_priority[index.get()] == DownloadPriority::DONT_DOWNLOAD)
        {
            mode |= OpenMode::SPARSE;
        }
        self.pool.open_file_at(
            (self.storage_index(), index),
            &self.resolve_path(index),
            self.files.file_size(index),
            mode,
        )
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

    fn need_partfile(&self) {
        let mut g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_none() {
            let dir = if self.part_file_dir.as_os_str().is_empty() {
                self.save_path.clone()
            } else {
                self.part_file_dir.clone()
            };
            *g = Some(PartFile::new(
                dir,
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
                    let pf = g.as_ref().expect("partfile exists for dont_download files");
                    let map = self.files.map_file(file_index, file_offset, 0);
                    let n = pf
                        .read(buf, map.piece, map.start)
                        .map_err(|e| StorageError::new(e, file_index, Operation::PartfileRead))?;
                    return Ok(usize::try_from(n).unwrap_or(0));
                }
                let handle = self.open_file(file_index, mode)?;
                let n = crate::file::pread_best_effort(
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
                    let pf = g.as_ref().expect("partfile allocated on demand");
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
                    let pf = g.as_ref().expect("partfile exists for dont_download files");
                    let map = self.files.map_file(file_index, file_offset, 0);
                    let data = pf
                        .hash_feed(map.piece, map.start, buf.len())
                        .map_err(|e| StorageError::new(e, file_index, Operation::PartfileRead))?;
                    feed(&data);
                    return Ok(data.len());
                }
                let handle = self.open_file(file_index, mode)?;
                scratch.resize(buf.len(), 0);
                let n = crate::file::pread_best_effort(
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

    /// `has_any_file` — true if any non-pad file exists on disk.
    pub fn has_any_file(&self) -> StorageResult<bool> {
        let fs = &self.files;
        for i in fs.file_range() {
            if fs.pad_file_at(i) || fs.file_size(i) == 0 {
                continue;
            }
            let rel = self.renamed.file_path(fs, i, &[]);
            let p = self.save_path.join(bytes_to_path(&rel));
            match fs::stat_file(&p, fs::FileStatusFlags::DONT_FOLLOW_LINKS) {
                // a dangling symlink still counts as existing (upstream)
                Ok(_) => return Ok(true),
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(StorageError::new(e, i, Operation::FileStat)),
            }
        }
        Ok(false)
    }

    /// `release_files` — close all pooled handles for this torrent and
    /// flush part-file metadata.
    pub fn release_files(&self) -> StorageResult<()> {
        self.pool.release_storage(self.storage_index());
        self.tick();
        Ok(())
    }

    /// `tick` — flush part-file metadata if dirty.
    pub fn tick(&self) {
        let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pf) = g.as_ref() {
            let _ = pf.flush_metadata();
        }
    }

    /// `rename_file(index, new_filename)` — rename on disk when the
    /// file exists, else record the rename for later creation.
    pub fn rename_file(&mut self, index: FileIndex, new_filename: &[u8]) -> StorageResult<()> {
        crate::storage_utils::rename_file(
            &self.files,
            &mut self.renamed,
            index,
            new_filename,
            &self.save_path,
        )
    }

    /// `initialize` — create directories, allocate files when
    /// requested, detect oversized files; fill `use_partfile` for
    /// priority-0 files (existing files take precedence, upstream
    /// backward-compat rule).
    pub fn initialize(&mut self) -> StorageResult<u8> {
        self.stat_cache
            .reserve(usize::try_from(self.files.num_files()).unwrap_or(0));
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
        crate::storage_utils::initialize_storage(
            &self.files,
            &self.renamed,
            &self.save_path,
            stat_cache_ptr,
            &self.file_priority,
            |i| self.open_file(i, OpenMode::WRITE).map(|_| ()),
            |_target, _link| Ok(()), // symlinks: not created on Windows by default
            |_i, _size| {},          // oversized flag collected above
        )?;
        Ok(ret)
    }

    /// `move_storage(new_save_path, flags)` — move all files; returns
    /// `(status, new_path)`.
    pub fn move_storage(
        &self,
        new_save_path: PathBuf,
        flags: MoveFlags,
    ) -> StorageResult<(u8, PathBuf)> {
        let save_path = self.save_path.clone();
        let mut pf = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        crate::storage_utils::move_storage(
            &self.files,
            &self.renamed,
            save_path,
            new_save_path,
            |path| {
                if let Some(p) = pf.as_mut() {
                    p.move_partfile(path.to_path_buf())
                } else {
                    Ok(())
                }
            },
            flags,
        )
    }

    /// `delete_files(options)` — delete torrent files / partfile.
    pub fn delete_files(&self, options: u8) -> StorageResult<()> {
        let part_file = self.save_path.join(&self.part_file_name);
        crate::storage_utils::delete_files(
            &self.files,
            &self.renamed,
            &self.save_path,
            &part_file,
            options,
        )
    }

    /// `set_file_priority` — move data in/out of the part file as
    /// priorities cross the `dont_download` boundary.
    pub fn set_file_priority(&mut self, prio: &[DownloadPriority]) -> StorageResult<()> {
        let mut new_prio = self.file_priority.clone();
        for idx in 0..self.files.num_files() as usize {
            let i = FileIndex(idx as i32);
            if idx >= prio.len() || idx >= new_prio.len() {
                continue;
            }
            let old_prio = new_prio[idx];
            let new = prio[idx];
            new_prio[idx] = new;

            if old_prio == DownloadPriority::DONT_DOWNLOAD && new != DownloadPriority::DONT_DOWNLOAD
            {
                // move data out of the part file
                let handle = self.open_file(i, OpenMode::WRITE)?;
                if self.use_partfile(i) {
                    let file_offset = self.files.file_offset(i);
                    let size = self.files.file_size(i);
                    let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(pf) = g.as_ref() {
                        let mut write_err = None;
                        pf.export_file(file_offset, size, |off, buf| {
                            let abs = file_offset + off;
                            if write_err.is_none() {
                                if let Err(e) =
                                    pwrite_all(handle.file(), buf, u64::try_from(abs).unwrap_or(0))
                                {
                                    write_err = Some(e);
                                }
                            }
                        })
                        .map_err(|e| {
                            new_prio[idx] = old_prio;
                            StorageError::new(e, i, Operation::PartfileWrite)
                        })?;
                        if let Some(e) = write_err {
                            new_prio[idx] = old_prio;
                            return Err(StorageError::new(e, i, Operation::PartfileWrite));
                        }
                    }
                    drop(g);
                }
            } else if old_prio != DownloadPriority::DONT_DOWNLOAD
                && new == DownloadPriority::DONT_DOWNLOAD
            {
                // moving into the part file is not implemented upstream
                // either — just stop using the part file if the file
                // exists on disk
                let p = self.resolve_path(i);
                let exists = p
                    .try_exists()
                    .map_err(|e| StorageError::new(e, i, Operation::FileStat))?;
                self.set_use_partfile(i, !exists);
            }
        }
        self.file_priority = new_prio;
        let g = self.part_file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pf) = g.as_ref() {
            pf.flush_metadata()
                .map_err(|e| StorageError::new(e, FileIndex(-1), Operation::PartfileWrite))?;
        }
        Ok(())
    }

    /// `verify_resume_data` — check the fastresume record against
    /// on-disk state (delegated to `storage_utils`).
    pub fn verify_resume_data(
        &self,
        have_pieces: &bitdaemon_common::bitfield::Bitfield,
        seed_mode: bool,
        links: &[std::path::PathBuf],
    ) -> StorageResult<bool> {
        crate::storage_utils::verify_resume_data(
            &self.files,
            &self.renamed,
            have_pieces,
            seed_mode,
            links,
            &self.file_priority,
            &self.stat_cache,
            &self.save_path,
        )
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
    /// `move_storage` — `(status, new_path)`.
    fn move_storage(&self, path: PathBuf, flags: MoveFlags) -> StorageResult<(u8, PathBuf)>;
    /// `delete_files`.
    fn delete_files(&self, options: u8) -> StorageResult<()>;
    /// `rename_file`.
    fn rename_file(&mut self, index: FileIndex, new_name: &[u8]) -> StorageResult<()>;
    /// `set_file_priority`.
    fn set_file_priority(&mut self, prio: &[DownloadPriority]) -> StorageResult<()>;
    /// `verify_resume_data`.
    fn verify_resume_data(
        &self,
        have_pieces: &bitdaemon_common::bitfield::Bitfield,
        seed_mode: bool,
        links: &[PathBuf],
    ) -> StorageResult<bool>;
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
    fn move_storage(&self, path: PathBuf, flags: MoveFlags) -> StorageResult<(u8, PathBuf)> {
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
    ) -> StorageResult<bool> {
        PreadStorage::verify_resume_data(self, have_pieces, seed_mode, links)
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
