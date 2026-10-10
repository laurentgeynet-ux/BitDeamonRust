//! `file_storage.hpp`/`file_storage.cpp` — the torrent file list and its
//! piece↔file mapping.
//!
//! Layout notes vs upstream:
//! - `aux::file_entry` packs everything into 16 bytes (borrowed `name`
//!   pointer, `root` pointer into the .torrent buffer, path index into a
//!   shared `m_paths` table). We keep the same *data model* but own the
//!   strings: `name: Vec<u8>`, `root: Option<Sha256Hash>` — one heap string
//!   per file instead of upstream's pointer tangles (ADR-0001).
//! - `path_index` semantics are preserved: `NO_PATH` (single-file torrent),
//!   `PATH_IS_ABSOLUTE` (file renamed to an absolute path), or an index
//!   into `paths`.
//! - `pad_file`/`hidden`/`executable`/`symlink` flags map to
//!   [`FileFlags`] bits 0-3 as upstream.
//! - v2 tail padding (`pad` files after each real file) is reproduced
//!   verbatim, including the `.pad` path entry.

use std::collections::{HashMap, HashSet};

use bitdaemon_common::index_range::index_range;
use bitdaemon_common::{FileIndex, IndexRange, PeerRequest, PieceIndex, Sha256Hash};

use crate::error::{Error, TorrentError};
use crate::path::{
    append_path, combine_path, has_parent_path, is_complete, lsplit_path, lsplit_path_at,
    rsplit_path,
};

/// `aux::path_index_t` — index into `FileStorage::paths`, or a sentinel.
pub type PathIndex = u32;

/// `aux::file_entry::no_path`.
pub const NO_PATH: PathIndex = (1 << 30) - 1;
/// `aux::file_entry::path_is_absolute`.
pub const PATH_IS_ABSOLUTE: PathIndex = (1 << 30) - 2;
/// `aux::file_entry::not_a_symlink` — sentinel `symlink_index`.
pub const NOT_A_SYMLINK: u32 = (1 << 15) - 1;

bitdaemon_common::bitfield_flag!(
    /// `file_flags_t` — per-file attribute bits (`file_storage.hpp`).
    FileFlags, u8
);

impl FileFlags {
    /// `file_storage::flag_pad_file` — not written to disk, zeros required.
    pub const PAD_FILE: Self = Self::bit(0);
    /// `file_storage::flag_hidden` — windows hidden attribute.
    pub const HIDDEN: Self = Self::bit(1);
    /// `file_storage::flag_executable`.
    pub const EXECUTABLE: Self = Self::bit(2);
    /// `file_storage::flag_symlink`.
    pub const SYMLINK: Self = Self::bit(3);
}

/// `disk_interface::default_block_size` — 16 kiB (`disk_interface.hpp`).
/// File-merkle block granularity; defined here to keep `common` free of
/// disk-layer constants. Upstream: `default_block_size = 0x4000`.
pub const DEFAULT_BLOCK_SIZE: i64 = 0x4000;

/// `aux::file_entry` — internal per-file record.
#[derive(Debug, Clone)]
struct FileEntry {
    /// `offset:48` upstream.
    offset: u64,
    /// `size:48` upstream.
    size: u64,
    /// `name` — owned leaf name (upstream borrows a pointer).
    name: Vec<u8>,
    /// `no_root_dir` — name excludes the torrent root dir.
    no_root_dir: bool,
    /// `symlink_index` into `FileStorage::symlinks`, `NOT_A_SYMLINK` if not.
    symlink_index: u32,
    /// `pad_file`.
    pad_file: bool,
    /// `hidden_attribute`.
    hidden: bool,
    /// `executable_attribute`.
    executable: bool,
    /// `symlink_attribute`.
    symlink: bool,
    /// `root` — v2 merkle root hash (upstream: borrowed pointer).
    root: Option<Sha256Hash>,
    /// `path_index`.
    path_index: PathIndex,
}

impl FileEntry {
    fn new() -> Self {
        Self {
            offset: 0,
            size: 0,
            name: Vec::new(),
            no_root_dir: false,
            symlink_index: NOT_A_SYMLINK,
            pad_file: false,
            hidden: false,
            executable: false,
            symlink: false,
            root: None,
            path_index: NO_PATH,
        }
    }
}

/// `file_slice` — a window of a file in a torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSlice {
    /// The index of the file.
    pub file_index: FileIndex,
    /// Byte offset within the file.
    pub offset: i64,
    /// Size of the window.
    pub size: i64,
}

/// `file_storage` — the file list + piece geometry of a torrent.
#[derive(Debug, Clone, Default)]
pub struct FileStorage {
    /// `m_piece_length`.
    piece_length: i32,
    /// `m_num_pieces`.
    num_pieces: i32,
    /// `m_v2`.
    v2: bool,
    /// `m_files`.
    files: Vec<FileEntry>,
    /// `m_symlinks` — targets of symlink files.
    symlinks: Vec<Vec<u8>>,
    /// `m_mtime` — sparse: grows to `files.len()` when first nonzero mtime
    /// is added (upstream semantics).
    mtime: Vec<i64>,
    /// `m_paths` — unique branch paths (root name excluded).
    paths: Vec<Vec<u8>>,
    /// `m_name` — torrent name / root directory.
    name: Vec<u8>,
    /// `m_total_size` — including pad files.
    total_size: i64,
    /// `m_size_on_disk` — excluding pad files.
    size_on_disk: i64,
}

impl FileStorage {
    /// `file_storage()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `file_storage::max_file_size` — internal limit
    /// (`min(2^48-1, (i32::MAX/2)*default_block_size)`).
    pub const MAX_FILE_SIZE: i64 = (1i64 << 48) - 1;
    /// `file_storage::max_file_offset`.
    pub const MAX_FILE_OFFSET: i64 = (1i64 << 48) - 1;
    /// `file_storage::max_num_pieces` — `2^30 - 1`.
    pub const MAX_NUM_PIECES: i32 = (1 << 30) - 1;
    /// `file_storage::max_piece_size` — `(2^15 - 1) * 0x4000`.
    pub const MAX_PIECE_SIZE: i32 = ((1 << 15) - 1) * 0x4000;

    /// `is_valid()` — piece length initialized.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.piece_length > 0
    }

    /// `reserve(num_files)`.
    pub fn reserve(&mut self, num_files: usize) {
        self.files.reserve(num_files);
    }

    /// `add_file(path, size, flags, mtime, symlink_path, root_hash)`.
    ///
    /// `filename` is `Some` when the leaf name is already known
    /// (`add_file_borrow` upstream — we always copy). `root_hash` marks the
    /// file as v2.
    // signature mirrors upstream `add_file`/`add_file_borrow` — the
    // argument list is part of the fidelity contract
    #[allow(clippy::too_many_arguments)]
    pub fn add_file(
        &mut self,
        filename: Option<&[u8]>,
        path: &[u8],
        file_size: i64,
        file_flags: FileFlags,
        mtime: i64,
        symlink_path: &[u8],
        root_hash: Option<Sha256Hash>,
    ) -> Result<(), Error> {
        debug_assert!(file_size >= 0);
        debug_assert!(filename.is_none_or(|f| !is_complete(f)));

        if file_size > Self::MAX_FILE_SIZE {
            return Err(TorrentError::TorrentInvalidLength.into());
        }
        if Self::MAX_FILE_OFFSET - self.total_size < file_size {
            return Err(TorrentError::TorrentInvalidLength.into());
        }

        if let Some(f) = filename {
            if f.len() >= (1 << 12) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "filename too long",
                )));
            }
        } else if crate::path::filename(path).len() >= (1 << 12) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "filename too long",
            )));
        }

        if !has_parent_path(path) {
            // single-file torrent: the path IS the name; all other files
            // need the same top dir — only legal as the first file
            debug_assert!(self.files.is_empty());
            self.name = path.to_vec();
        } else if self.files.is_empty() {
            self.name = lsplit_path(path).0.to_vec();
        }

        // files without root_hash are v1 — except symlinks which are
        // v1/v2-ambiguous (they have no root hash)
        if symlink_path.is_empty() && file_size > 0 {
            let v2 = root_hash.is_some();
            // true if all files added so far were symlinks (this is the
            // first "real" file) or all were empty
            if self.files.len() == self.symlinks.len() || self.total_size == 0 {
                self.v2 = v2;
            } else if self.v2 != v2 {
                // cannot mix v1 and v2 files
                return Err(if self.v2 {
                    TorrentError::TorrentMissingPiecesRoot
                } else {
                    TorrentError::TorrentInconsistentFiles
                }
                .into());
            }
        }

        self.files.push(FileEntry::new());
        let e_idx = self.files.len() - 1;

        // if filename is empty, copy the leaf from path inside
        // update_path_index; otherwise we set it after (upstream checks
        // `filename.empty()`, so Some(b"") behaves like None)
        self.update_path_index(e_idx, path, filename.is_none_or(|f| f.is_empty()))?;
        if let Some(f) = filename {
            if !f.is_empty() {
                self.files[e_idx].name = f.to_vec();
            }
        }
        {
            let e = &mut self.files[e_idx];
            e.size = file_size as u64;
            e.offset = self.total_size as u64;
            e.pad_file = file_flags.contains(FileFlags::PAD_FILE);
            e.hidden = file_flags.contains(FileFlags::HIDDEN);
            e.executable = file_flags.contains(FileFlags::EXECUTABLE);
            e.symlink = file_flags.contains(FileFlags::SYMLINK);
            e.root = root_hash;
        }

        if !symlink_path.is_empty() && self.symlinks.len() < (NOT_A_SYMLINK as usize) - 1 {
            self.files[e_idx].symlink_index = self.symlinks.len() as u32;
            self.symlinks.push(symlink_path.to_vec());
        } else {
            self.files[e_idx].symlink = false;
        }
        if mtime != 0 {
            if self.mtime.len() < self.files.len() {
                self.mtime.resize(self.files.len(), 0);
            }
            let last = FileIndex(self.files.len() as i32 - 1);
            self.mtime[last.get()] = mtime;
        }

        self.total_size += self.files[e_idx].size as i64;
        if !file_flags.contains(FileFlags::PAD_FILE) {
            self.size_on_disk += self.files[e_idx].size as i64;
        }

        debug_assert!(self.total_size >= self.size_on_disk);

        // v2 torrents: pad the end of each file to a piece boundary
        // (at the end of files, per BEP52 reference implementation)
        if self.v2 && self.total_size % i64::from(self.piece_length) != 0 {
            let pad_size =
                i64::from(self.piece_length) - (self.total_size % i64::from(self.piece_length));
            debug_assert!(pad_size != i64::from(self.piece_length));
            debug_assert!(pad_size > 0);
            if self.total_size > Self::MAX_FILE_OFFSET - pad_size {
                return Err(TorrentError::TorrentInvalidLength.into());
            }

            let pad_path_index = self.get_or_add_path(b".pad");
            self.files.push(FileEntry {
                size: pad_size as u64,
                offset: self.total_size as u64,
                path_index: pad_path_index,
                name: pad_size.to_string().into_bytes(),
                pad_file: true,
                ..FileEntry::new()
            });
            self.total_size += pad_size;
        }
        Ok(())
    }

    /// `update_path_index` — split `path` into branch/leaf, strip the
    /// torrent-name root if present, register the branch in `paths`.
    /// `set_name`: also copy the leaf into `e.name`.
    fn update_path_index(
        &mut self,
        e_idx: usize,
        path: &[u8],
        set_name: bool,
    ) -> Result<(), Error> {
        if is_complete(path) {
            debug_assert!(set_name);
            self.files[e_idx].name = path.to_vec();
            self.files[e_idx].path_index = PATH_IS_ABSOLUTE;
            return Ok(());
        }

        debug_assert!(path.first() != Some(&b'/'));

        let (mut branch_path, leaf) = rsplit_path(path);

        if branch_path.is_empty() {
            if set_name {
                self.files[e_idx].name = leaf.to_vec();
            }
            self.files[e_idx].path_index = NO_PATH;
            return Ok(());
        }

        // strip the torrent name root if it's the first element
        let no_root_dir = if lsplit_path(branch_path).0 == self.name.as_slice() {
            branch_path = lsplit_path(branch_path).1;
            // strip duplicate separators
            while !branch_path.is_empty()
                && (branch_path[0] == crate::path::SEPARATOR
                    || (cfg!(windows) && branch_path[0] == b'/'))
            {
                branch_path = &branch_path[1..];
            }
            false
        } else {
            true
        };

        let path_index = self.get_or_add_path(branch_path);
        let e = &mut self.files[e_idx];
        e.no_root_dir = no_root_dir;
        e.path_index = path_index;
        if set_name {
            e.name = leaf.to_vec();
        }
        Ok(())
    }

    /// `get_or_add_path` — linear search (from the back) then append.
    fn get_or_add_path(&mut self, path: &[u8]) -> PathIndex {
        if let Some(i) = self.paths.iter().rposition(|p| p.as_slice() == path) {
            return i as PathIndex;
        }
        debug_assert!(path.is_empty() || path[0] != b'/');
        self.paths.push(path.to_vec());
        (self.paths.len() - 1) as PathIndex
    }

    /// `map_block` — pieces `(piece, offset, size)` → file windows.
    #[must_use]
    pub fn map_block(&self, piece: PieceIndex, offset: i64, mut size: i64) -> Vec<FileSlice> {
        debug_assert!(piece >= PieceIndex(0) && piece < self.end_piece());
        debug_assert!(!self.files.is_empty());
        debug_assert!(size >= 0);
        let mut ret = Vec::new();
        if self.files.is_empty() {
            return ret;
        }

        let target = piece.0 as i64 * i64::from(self.piece_length) + offset;
        debug_assert!(target <= self.total_size - size);

        // fix up size past the end
        if target > self.total_size - size {
            size = self.total_size - target;
        }

        // first file whose offset > target, minus one
        let mut idx = self
            .files
            .partition_point(|f| f.offset <= target as u64)
            .saturating_sub(1);

        let mut file_offset = target - self.files[idx].offset as i64;
        while size > 0 {
            let f = &self.files[idx];
            if file_offset < f.size as i64 {
                let slice_size = (f.size as i64 - file_offset).min(size);
                ret.push(FileSlice {
                    file_index: FileIndex(idx as i32),
                    offset: file_offset,
                    size: slice_size,
                });
                debug_assert!(slice_size <= size);
                size -= slice_size;
                file_offset += slice_size;
            }
            debug_assert!(size >= 0);
            file_offset -= f.size as i64;
            idx += 1;
        }
        ret
    }

    /// `map_file` — file byte range → `peer_request`.
    #[must_use]
    pub fn map_file(&self, file_index: FileIndex, file_offset: i64, size: i32) -> PeerRequest {
        debug_assert!(file_index < self.end_file());

        if file_index >= self.end_file() {
            return PeerRequest {
                piece: self.end_piece(),
                start: 0,
                length: 0,
            };
        }

        let offset = file_offset + self.file_offset(file_index);

        if offset >= self.total_size {
            return PeerRequest {
                piece: self.end_piece(),
                start: 0,
                length: 0,
            };
        }
        let piece_length = i64::from(self.piece_length);
        let mut length = size as i64;
        if offset + length > self.total_size {
            length = self.total_size - offset;
        }
        PeerRequest {
            piece: PieceIndex((offset / piece_length) as i32),
            start: (offset % piece_length) as i32,
            length: length as i32,
        }
    }

    /// `num_files()`.
    #[must_use]
    pub fn num_files(&self) -> i32 {
        self.files.len() as i32
    }

    /// `end_file()`.
    #[must_use]
    pub fn end_file(&self) -> FileIndex {
        FileIndex(self.num_files())
    }

    /// `file_range()`.
    #[must_use]
    pub fn file_range(&self) -> IndexRange<FileIndex> {
        index_range(FileIndex(0), self.end_file())
    }

    /// `total_size()` — includes pad files.
    #[must_use]
    pub fn total_size(&self) -> i64 {
        self.total_size
    }

    /// `size_on_disk()` — excluding pad files.
    #[must_use]
    pub fn size_on_disk(&self) -> i64 {
        self.size_on_disk
    }

    /// `set_num_pieces`.
    pub fn set_num_pieces(&mut self, n: i32) {
        self.num_pieces = n;
    }

    /// `num_pieces()`.
    #[must_use]
    pub fn num_pieces(&self) -> i32 {
        debug_assert!(self.piece_length > 0);
        self.num_pieces
    }

    /// `end_piece()`.
    #[must_use]
    pub fn end_piece(&self) -> PieceIndex {
        PieceIndex(self.num_pieces)
    }

    /// `last_piece()` — may be shorter than the others.
    #[must_use]
    pub fn last_piece(&self) -> PieceIndex {
        PieceIndex(self.num_pieces - 1)
    }

    /// `piece_range()`.
    #[must_use]
    pub fn piece_range(&self) -> IndexRange<PieceIndex> {
        index_range(PieceIndex(0), self.end_piece())
    }

    /// `set_piece_length` — must be a power of two ≥ 16 kiB upstream.
    pub fn set_piece_length(&mut self, l: i32) {
        self.piece_length = l;
    }

    /// `piece_length()`.
    #[must_use]
    pub fn piece_length(&self) -> i32 {
        debug_assert!(self.piece_length > 0);
        self.piece_length
    }

    /// `piece_size(index)` — `piece_length` except the last piece.
    #[must_use]
    pub fn piece_size(&self, index: PieceIndex) -> i32 {
        debug_assert!(index >= PieceIndex(0) && index < self.end_piece());
        if index == self.last_piece() {
            let size_except_last = i64::from(self.num_pieces - 1) * i64::from(self.piece_length);
            let size = self.total_size - size_except_last;
            debug_assert!(size > 0 && size <= i64::from(self.piece_length));
            size as i32
        } else {
            self.piece_length
        }
    }

    /// `piece_size2(index)` — for v2: piece truncated to the file it starts
    /// in (pad files are not part of the piece).
    #[must_use]
    pub fn piece_size2(&self, index: PieceIndex) -> i32 {
        debug_assert!(index >= PieceIndex(0) && index < self.end_piece());
        let target = i64::from(self.piece_length) * i64::from(index.0);

        // first file whose offset > target
        let next = self.files.partition_point(|f| f.offset <= target as u64);
        if next >= self.files.len() {
            return self.piece_size(index);
        }
        (self.files[next].offset as i64 - target).min(i64::from(self.piece_length)) as i32
    }

    /// `blocks_in_piece2(index)` — `DEFAULT_BLOCK_SIZE` blocks, rounded up.
    #[must_use]
    pub fn blocks_in_piece2(&self, index: PieceIndex) -> i32 {
        (self.piece_size2(index) + DEFAULT_BLOCK_SIZE as i32 - 1) / DEFAULT_BLOCK_SIZE as i32
    }

    /// `blocks_per_piece()` — blocks in a typical piece.
    #[must_use]
    pub fn blocks_per_piece(&self) -> i32 {
        (self.piece_length + DEFAULT_BLOCK_SIZE as i32 - 1) / DEFAULT_BLOCK_SIZE as i32
    }

    /// `set_name` — torrent name / root dir for multi-file torrents.
    pub fn set_name(&mut self, n: &[u8]) {
        self.name = n.to_vec();
    }

    /// `name()`.
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// `swap`.
    pub fn swap(&mut self, other: &mut Self) {
        std::mem::swap(self, other);
    }

    /// `root(index)` — v2 merkle root, zeros for v1.
    #[must_use]
    pub fn root(&self, index: FileIndex) -> Sha256Hash {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        self.files[index.get()].root.unwrap_or_default()
    }

    /// `symlink(index)` — full path (with torrent name) the file links to,
    /// or empty.
    #[must_use]
    pub fn symlink(&self, index: FileIndex) -> Vec<u8> {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        let fe = &self.files[index.get()];
        if fe.symlink_index == NOT_A_SYMLINK {
            return Vec::new();
        }
        debug_assert!((fe.symlink_index as usize) < self.symlinks.len());
        let link = &self.symlinks[fe.symlink_index as usize];
        let mut ret = Vec::with_capacity(self.name.len() + link.len() + 1);
        ret.extend_from_slice(&self.name);
        append_path(&mut ret, link);
        ret
    }

    /// `internal_symlink` — raw link target (no name prepended).
    #[must_use]
    pub fn internal_symlink(&self, index: FileIndex) -> &[u8] {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        let fe = &self.files[index.get()];
        debug_assert!((fe.symlink_index as usize) < self.symlinks.len());
        &self.symlinks[fe.symlink_index as usize]
    }

    /// `mtime(index)` — 0 if not present.
    #[must_use]
    pub fn mtime(&self, index: FileIndex) -> i64 {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        self.mtime.get(index.get()).copied().unwrap_or(0)
    }

    /// `file_path(index, save_path)` — full on-disk path.
    #[must_use]
    pub fn file_path(&self, index: FileIndex, save_path: &[u8]) -> Vec<u8> {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        let fe = &self.files[index.get()];

        if fe.path_index == PATH_IS_ABSOLUTE {
            return fe.name.clone();
        }
        if fe.path_index == NO_PATH {
            let mut ret = Vec::with_capacity(save_path.len() + fe.name.len() + 1);
            ret.extend_from_slice(save_path);
            append_path(&mut ret, &fe.name);
            return ret;
        }
        if fe.no_root_dir {
            let p = &self.paths[fe.path_index as usize];
            let mut ret = Vec::with_capacity(save_path.len() + p.len() + fe.name.len() + 2);
            ret.extend_from_slice(save_path);
            append_path(&mut ret, p);
            append_path(&mut ret, &fe.name);
            ret
        } else {
            let p = &self.paths[fe.path_index as usize];
            let mut ret =
                Vec::with_capacity(save_path.len() + self.name.len() + p.len() + fe.name.len() + 3);
            ret.extend_from_slice(save_path);
            append_path(&mut ret, &self.name);
            append_path(&mut ret, p);
            append_path(&mut ret, &fe.name);
            ret
        }
    }

    /// `internal_file_path` — path within the torrent (no root name, no
    /// save_path).
    #[must_use]
    pub fn internal_file_path(&self, index: FileIndex) -> Vec<u8> {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        let fe = &self.files[index.get()];

        if fe.path_index != PATH_IS_ABSOLUTE && fe.path_index != NO_PATH {
            let mut ret =
                Vec::with_capacity(self.paths[fe.path_index as usize].len() + fe.name.len() + 2);
            append_path(&mut ret, &self.paths[fe.path_index as usize]);
            append_path(&mut ret, &fe.name);
            ret
        } else {
            fe.name.clone()
        }
    }

    /// `file_name(index)` — leaf only.
    #[must_use]
    pub fn file_name(&self, index: FileIndex) -> &[u8] {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        &self.files[index.get()].name
    }

    /// `file_size(index)`.
    #[must_use]
    pub fn file_size(&self, index: FileIndex) -> i64 {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        self.files[index.get()].size as i64
    }

    /// `pad_file_at(index)`.
    #[must_use]
    pub fn pad_file_at(&self, index: FileIndex) -> bool {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        self.files[index.get()].pad_file
    }

    /// `file_offset(index)` — byte offset in torrent space.
    #[must_use]
    pub fn file_offset(&self, index: FileIndex) -> i64 {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        self.files[index.get()].offset as i64
    }

    /// `file_first_piece_node(index)` — flat tree index of the first
    /// piece-layer node for the file's merkle tree.
    #[must_use]
    pub fn file_first_piece_node(&self, index: FileIndex) -> usize {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        debug_assert!(self.piece_length() > 0);
        let layer = crate::merkle::merkle_num_leafs(
            usize::try_from(self.file_num_pieces(index)).unwrap_or(0),
        );
        crate::merkle::merkle_num_nodes(layer) - layer
    }

    /// `file_first_block_node(index)` — flat tree index of the first
    /// block-layer (leaf) node.
    #[must_use]
    pub fn file_first_block_node(&self, index: FileIndex) -> usize {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        let layer = crate::merkle::merkle_num_leafs(
            usize::try_from(self.file_num_blocks(index)).unwrap_or(0),
        );
        crate::merkle::merkle_num_nodes(layer) - layer
    }

    /// `file_num_pieces(index)` — pieces spanned (v2: file is piece-aligned).
    #[must_use]
    pub fn file_num_pieces(&self, index: FileIndex) -> i32 {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        debug_assert!(self.piece_length > 0);
        let f = &self.files[index.get()];
        if f.size == 0 {
            return 0;
        }
        // v2 only: aligned to pieces
        debug_assert!(!f.pad_file);
        debug_assert!((f.offset as i64 % i64::from(self.piece_length)) == 0);
        ((f.size as i64 + i64::from(self.piece_length) - 1) / i64::from(self.piece_length)) as i32
    }

    /// `file_piece_range(file)` — `0..file_num_pieces`.
    #[must_use]
    pub fn file_piece_range(&self, file: FileIndex) -> IndexRange<i32> {
        index_range(0, self.file_num_pieces(file))
    }

    /// `file_num_blocks(index)` — `default_block_size` blocks (v2).
    #[must_use]
    pub fn file_num_blocks(&self, index: FileIndex) -> i32 {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        debug_assert!(self.piece_length > 0);
        let f = &self.files[index.get()];
        if f.size == 0 {
            return 0;
        }
        debug_assert!(!f.pad_file);
        debug_assert!((f.offset as i64 % i64::from(self.piece_length)) == 0);
        ((f.size as i64 + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE) as i32
    }

    /// `file_flags(index)`.
    #[must_use]
    pub fn file_flags(&self, index: FileIndex) -> FileFlags {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        let fe = &self.files[index.get()];
        (if fe.pad_file {
            FileFlags::PAD_FILE
        } else {
            FileFlags::NONE
        }) | (if fe.hidden {
            FileFlags::HIDDEN
        } else {
            FileFlags::NONE
        }) | (if fe.executable {
            FileFlags::EXECUTABLE
        } else {
            FileFlags::NONE
        }) | (if fe.symlink {
            FileFlags::SYMLINK
        } else {
            FileFlags::NONE
        })
    }

    /// `file_absolute_path(index)` — was the file renamed to an absolute path?
    #[must_use]
    pub fn file_absolute_path(&self, index: FileIndex) -> bool {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        self.files[index.get()].path_index == PATH_IS_ABSOLUTE
    }

    /// `file_path_hash` — CRC32C over the lowercased full path (save path
    /// + name + branch + leaf). Used to detect duplicate filenames.
    #[must_use]
    pub fn file_path_hash(&self, index: FileIndex, save_path: &[u8]) -> u32 {
        debug_assert!(index >= FileIndex(0) && index < self.end_file());
        let fe = &self.files[index.get()];

        let mut buf = Vec::new();
        fn process_string_lowercase(buf: &mut Vec<u8>, s: &[u8]) {
            buf.extend(s.iter().map(u8::to_ascii_lowercase));
        }

        if fe.path_index == PATH_IS_ABSOLUTE {
            process_string_lowercase(&mut buf, &fe.name);
        } else if fe.path_index == NO_PATH {
            if !save_path.is_empty() {
                process_string_lowercase(&mut buf, save_path);
                debug_assert!(save_path.last() != Some(&crate::path::SEPARATOR));
                buf.push(crate::path::SEPARATOR);
            }
            process_string_lowercase(&mut buf, &fe.name);
        } else if fe.no_root_dir {
            if !save_path.is_empty() {
                process_string_lowercase(&mut buf, save_path);
                debug_assert!(save_path.last() != Some(&crate::path::SEPARATOR));
                buf.push(crate::path::SEPARATOR);
            }
            let p = &self.paths[fe.path_index as usize];
            if !p.is_empty() {
                process_string_lowercase(&mut buf, p);
                debug_assert!(p.last() != Some(&crate::path::SEPARATOR));
                buf.push(crate::path::SEPARATOR);
            }
            process_string_lowercase(&mut buf, &fe.name);
        } else {
            if !save_path.is_empty() {
                process_string_lowercase(&mut buf, save_path);
                debug_assert!(save_path.last() != Some(&crate::path::SEPARATOR));
                buf.push(crate::path::SEPARATOR);
            }
            process_string_lowercase(&mut buf, &self.name);
            debug_assert!(!self.name.is_empty());
            debug_assert!(self.name.last() != Some(&crate::path::SEPARATOR));
            buf.push(crate::path::SEPARATOR);

            let p = &self.paths[fe.path_index as usize];
            if !p.is_empty() {
                process_string_lowercase(&mut buf, p);
                debug_assert!(!p.is_empty());
                debug_assert!(p.last() != Some(&crate::path::SEPARATOR));
                buf.push(crate::path::SEPARATOR);
            }
            process_string_lowercase(&mut buf, &fe.name);
        }

        bitdaemon_common::crc32c::crc32c(&buf)
    }

    /// `all_path_hashes` — insert into `table` the CRC32C of every branch
    /// path prefix (lowercased, name-prefixed).
    pub fn all_path_hashes(&self, table: &mut HashSet<u32>) {
        // `process_path_lowercase` receives the name-prefixed crc *by
        // value* upstream — each path starts from the same state.
        let mut prefix = Vec::new();
        if !self.name.is_empty() {
            prefix.extend(self.name.iter().map(u8::to_ascii_lowercase));
            debug_assert!(self.name.last() != Some(&crate::path::SEPARATOR));
            prefix.push(crate::path::SEPARATOR);
        }

        let mut buf = Vec::with_capacity(prefix.len() + 64);
        for p in &self.paths {
            if p.is_empty() {
                continue;
            }
            buf.clear();
            buf.extend_from_slice(&prefix);
            // insert the hash at each separator (hash of the prefix so
            // far) and at the end
            for &c in p.iter() {
                if c == crate::path::SEPARATOR {
                    table.insert(bitdaemon_common::crc32c::crc32c(&buf));
                }
                buf.push(c.to_ascii_lowercase());
            }
            table.insert(bitdaemon_common::crc32c::crc32c(&buf));
        }
    }

    /// `file_index_at_offset(offset)`.
    #[must_use]
    pub fn file_index_at_offset(&self, offset: i64) -> FileIndex {
        debug_assert!(offset >= 0);
        debug_assert!(offset < self.total_size);
        // last file whose offset <= target
        let idx = self
            .files
            .partition_point(|f| f.offset <= offset as u64)
            .saturating_sub(1);
        FileIndex(idx as i32)
    }

    /// `file_index_at_piece(piece)`.
    #[must_use]
    pub fn file_index_at_piece(&self, piece: PieceIndex) -> FileIndex {
        self.file_index_at_offset(i64::from(piece.0) * i64::from(self.piece_length))
    }

    /// `last_file_index_at_piece(piece)` — file containing the last byte
    /// of `piece`.
    #[must_use]
    pub fn last_file_index_at_piece(&self, piece: PieceIndex) -> FileIndex {
        self.file_index_at_offset(
            i64::from(piece.0) * i64::from(self.piece_length) + i64::from(self.piece_size(piece))
                - 1,
        )
    }

    /// `file_index_for_root(root_hash)` — `-1` if absent.
    #[must_use]
    pub fn file_index_for_root(&self, root_hash: &Sha256Hash) -> FileIndex {
        for i in self.file_range() {
            if &self.root(i) == root_hash {
                return i;
            }
        }
        FileIndex(-1)
    }

    /// `piece_index_at_file(f)` — first piece overlapping `f`.
    #[must_use]
    pub fn piece_index_at_file(&self, f: FileIndex) -> PieceIndex {
        PieceIndex((self.file_offset(f) / i64::from(self.piece_length)) as i32)
    }

    /// `last_piece_index_at_file(f)`.
    #[must_use]
    pub fn last_piece_index_at_file(&self, f: FileIndex) -> PieceIndex {
        PieceIndex(
            ((self.file_offset(f) + self.file_size(f) - 1) / i64::from(self.piece_length)) as i32,
        )
    }

    /// `paths()` — the unique branch paths (internal).
    #[must_use]
    pub fn paths(&self) -> &[Vec<u8>] {
        &self.paths
    }

    /// `v2()`.
    #[must_use]
    pub fn v2(&self) -> bool {
        self.v2
    }

    /// `remove_tail_padding` — drop a trailing `.pad` file added by
    /// libtorrent 2.0.0–2.0.7 hybrid torrents (backwards compat).
    pub fn remove_tail_padding(&mut self) {
        let mut f = self.end_file().0;
        while f > 0 {
            f -= 1;
            let fi = FileIndex(f);
            // empty files and symlinks are skipped
            if self.file_size(fi) == 0 {
                continue;
            }
            if self.pad_file_at(fi) {
                self.total_size -= self.file_size(fi);
                self.files.remove(f as usize);
                // fix up offsets of trailing empty files
                let mut g = f;
                while g < self.end_file().0 {
                    self.files[g as usize].offset = self.total_size as u64;
                    debug_assert!(self.files[g as usize].size == 0);
                    g += 1;
                }
            }
            // last non-empty file isn't a pad → done
            return;
        }
    }

    /// `sanitize_symlinks` — validate symlink targets stay inside the
    /// torrent; invalid ones are made to point to themselves.
    /// Transcribed from upstream (two passes: concrete targets, then
    /// targets depending on other symlinks).
    pub fn sanitize_symlinks(&mut self) {
        // symlinks are unusual, this function is optimized assuming there
        // are no symbolic links in the torrent. If we find one, we build
        // the hash table of files it's allowed to refer to, but don't pay
        // that price up-front.
        let mut file_map: HashMap<Vec<u8>, FileIndex> = HashMap::new();
        let mut file_map_initialized = false;

        // sorted view of `paths` for `is_directory()`
        let mut sorted_paths: Option<Vec<Vec<u8>>> = None;

        // symbolic links that point to directories
        let mut dir_links: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        // we validate symlinks in (potentially) 2 passes over the files.
        // remaining symlinks to validate after the first pass
        let mut symlinks_to_validate: Vec<FileIndex> = Vec::new();

        macro_rules! is_directory {
            ($target:expr) => {{
                let target: &[u8] = $target;
                let sorted = sorted_paths.get_or_insert_with(|| {
                    let mut v: Vec<Vec<u8>> = self.paths.clone();
                    v.sort();
                    v
                });
                // exact match
                let exact = sorted
                    .binary_search_by(|p| p.as_slice().cmp(target))
                    .is_ok();
                if exact {
                    true
                } else {
                    // a separate search is needed: characters such as '.'
                    // (0x2E) sort before '/' (0x2F), so paths like "a.foo"
                    // appear between "a" and "a/" — we can't conclude from
                    // a single binary_search.
                    let mut prefix = target.to_vec();
                    prefix.push(crate::path::SEPARATOR);
                    let idx = sorted.partition_point(|p| p.as_slice() < prefix.as_slice());
                    idx < sorted.len() && sorted[idx].starts_with(&prefix)
                }
            }};
        }

        for i in self.file_range() {
            if !self.file_flags(i).contains(FileFlags::SYMLINK) {
                continue;
            }

            if !file_map_initialized {
                for j in self.file_range() {
                    file_map.insert(self.internal_file_path(j), j);
                }
                file_map_initialized = true;
            }

            let sym_idx = self.files[i.get()].symlink_index as usize;
            debug_assert!(sym_idx < self.symlinks.len());

            // symlink targets are only allowed to point to files or
            // directories in this torrent.
            {
                let target = &self.symlinks[sym_idx];

                if is_complete(target) {
                    // a symlink target is not allowed to be an absolute
                    // path, ever. This symlink is invalid, make it point
                    // to itself.
                    self.symlinks[sym_idx] = self.internal_file_path(i);
                    continue;
                }

                if let Some(&j) = file_map.get(target.as_slice()) {
                    if self.file_flags(j).contains(FileFlags::SYMLINK) {
                        // we don't know whether this symlink is a file or
                        // a directory, so make the conservative assumption
                        // that it's a directory
                        dir_links.insert(self.internal_file_path(i), target.clone());
                    }
                    continue;
                }

                // it may point to a directory that doesn't have any files
                // (but only other directories), in which case it won't
                // show up in `paths`
                if is_directory!(target) {
                    // it points to a sub directory within the torrent
                    let target = target.clone();
                    dir_links.insert(self.internal_file_path(i), target);
                    continue;
                }
            }

            // for backwards compatibility, allow paths relative to the
            // link as well
            if self.files[i.get()].path_index < PATH_IS_ABSOLUTE {
                let mut target = self.paths[self.files[i.get()].path_index as usize].clone();
                let link = self.symlinks[sym_idx].clone();
                append_path(&mut target, &link);

                // if it points to a directory, that's OK
                if let Some(found) = self.paths.iter().find(|p| **p == target) {
                    let found = found.clone();
                    self.symlinks[sym_idx] = found.clone();
                    dir_links.insert(self.internal_file_path(i), found);
                    continue;
                }

                if is_directory!(&target) {
                    // it points to a sub directory within the torrent
                    self.symlinks[sym_idx] = target.clone();
                    dir_links.insert(self.internal_file_path(i), target);
                    continue;
                }

                if let Some(&j) = file_map.get(&target) {
                    self.symlinks[sym_idx] = target.clone();
                    if self.file_flags(j).contains(FileFlags::SYMLINK) {
                        // conservative assumption: directory
                        dir_links.insert(self.internal_file_path(i), target);
                    }
                    continue;
                }
            }

            // we don't know whether this symlink is a file or a
            // directory, so make the conservative assumption that it's a
            // directory
            dir_links.insert(self.internal_file_path(i), self.symlinks[sym_idx].clone());
            symlinks_to_validate.push(i);
        }

        // in case there were some "complex" symlinks, we need a second
        // pass to validate those. For example, symlinks whose target rely
        // on other symlinks
        for i in symlinks_to_validate {
            let sym_idx = self.files[i.get()].symlink_index as usize;
            debug_assert!(sym_idx < self.symlinks.len());

            let mut target = self.symlinks[sym_idx].clone();

            // to avoid getting stuck in an infinite loop, we only allow
            // traversing a symlink once
            let mut traversed: HashSet<Vec<u8>> = HashSet::new();

            // this is where we check every path element for existence. If
            // it's not among the concrete paths, it may be a symlink,
            // which is also OK. Note that we won't iterate through this
            // for the last step, where the filename is included. The
            // filename is validated after the loop.
            let mut failed = false;
            let mut branch_len = lsplit_path(&target).0.len();
            while branch_len < target.len() {
                let branch = &target[..branch_len];

                // this is a concrete directory
                if is_directory!(branch) {
                    // advance: `lsplit_path(target, branch.size() + 1)`
                    branch_len = lsplit_path_at(&target, branch_len + 1).0.len();
                    continue;
                }

                let Some(link) = dir_links.get(branch) else {
                    failed = true;
                    break;
                };
                if traversed.contains(branch) {
                    failed = true;
                    break;
                }
                traversed.insert(branch.to_vec());

                // this path element is a symlink — substitute the branch
                // so far by the link target, then start over with the
                // new (concrete) path
                let rest = target[branch_len + 1..].to_vec();
                target = combine_path(link, &rest);
                branch_len = lsplit_path(&target).0.len();
            }

            // the final (resolved) target must be a valid file or
            // directory
            if !failed && !file_map.contains_key(&target) && !is_directory!(&target) {
                failed = true;
            }

            if failed {
                // this symlink is invalid, make it point to itself
                self.symlinks[sym_idx] = self.internal_file_path(i);
            }
        }
    }
}

/// `aux::files_compatible` — v1/v2 hybrid consistency check: same layout.
#[must_use]
pub fn files_compatible(lhs: &FileStorage, rhs: &FileStorage) -> bool {
    if lhs.num_files() != rhs.num_files() {
        return false;
    }
    if lhs.total_size() != rhs.total_size() {
        return false;
    }
    if lhs.piece_length() != rhs.piece_length() {
        return false;
    }

    for i in lhs.file_range() {
        let lhs_relevant = !lhs.pad_file_at(i) && lhs.file_size(i) > 0;
        let rhs_relevant = !rhs.pad_file_at(i) && rhs.file_size(i) > 0;
        if lhs_relevant != rhs_relevant {
            return false;
        }
        if !lhs_relevant {
            continue;
        }

        if lhs.pad_file_at(i) != rhs.pad_file_at(i)
            || lhs.file_size(i) != rhs.file_size(i)
            || lhs.file_path(i, b"") != rhs.file_path(i, b"")
            || lhs.file_offset(i) != rhs.file_offset(i)
        {
            return false;
        }

        if lhs.file_flags(i).contains(FileFlags::SYMLINK) && lhs.symlink(i) != rhs.symlink(i) {
            return false;
        }
    }
    true
}

/// `aux::file_piece_range_exclusive` — pieces entirely inside `file`
/// (edge partial pieces excluded).
#[must_use]
pub fn file_piece_range_exclusive(fs: &FileStorage, file: FileIndex) -> IndexRange<PieceIndex> {
    let range = fs.map_file(file, 0, 1);
    let file_size = fs.file_size(file);
    let piece_size = i64::from(fs.piece_length());
    let begin = if range.start == 0 {
        range.piece
    } else {
        PieceIndex(range.piece.0 + 1)
    };
    // last piece is potentially smaller — if this is the last file, it
    // contains the whole last piece
    let end = if file == FileIndex(fs.num_files() - 1) {
        fs.end_piece()
    } else {
        PieceIndex(
            ((i64::from(range.piece.0) * piece_size + i64::from(range.start) + file_size + 1)
                / piece_size) as i32,
        )
    };
    index_range(begin, end)
}

/// `aux::file_piece_range_inclusive` — pieces overlapping `file`.
#[must_use]
pub fn file_piece_range_inclusive(fs: &FileStorage, file: FileIndex) -> IndexRange<PieceIndex> {
    let range = fs.map_file(file, 0, 1);
    let file_size = fs.file_size(file);
    let piece_size = i64::from(fs.piece_length());
    let end = PieceIndex(
        ((i64::from(range.piece.0) * piece_size + i64::from(range.start) + file_size - 1)
            / piece_size
            + 1) as i32,
    );
    index_range(range.piece, end)
}

/// `aux::calc_num_pieces`.
#[must_use]
pub fn calc_num_pieces(fs: &FileStorage) -> i32 {
    ((fs.total_size() + i64::from(fs.piece_length()) - 1) / i64::from(fs.piece_length())) as i32
}

/// `aux::rename_entry` mode for `RenamedFiles`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameMode {
    /// `full_path` — appended to the torrent name.
    FullPath,
    /// `no_root_path` — relative to the save path, drops torrent name.
    NoRootPath,
    /// `absolute_path`.
    AbsolutePath,
}

/// `renamed_files` — rename overlay that doesn't mutate `FileStorage`.
#[derive(Debug, Default, Clone)]
pub struct RenamedFiles {
    renamed: HashMap<FileIndex, (RenameMode, Vec<u8>)>,
}

impl RenamedFiles {
    /// `renamed_files::file_path`.
    #[must_use]
    pub fn file_path(&self, fs: &FileStorage, index: FileIndex, save_path: &[u8]) -> Vec<u8> {
        let Some(&(mode, ref path)) = self.renamed.get(&index) else {
            return fs.file_path(index, save_path);
        };
        match mode {
            RenameMode::FullPath => {
                let mut ret =
                    Vec::with_capacity(save_path.len() + fs.name().len() + path.len() + 2);
                ret.extend_from_slice(save_path);
                append_path(&mut ret, fs.name());
                append_path(&mut ret, path);
                ret
            }
            RenameMode::NoRootPath => {
                let mut ret = Vec::with_capacity(save_path.len() + path.len() + 1);
                ret.extend_from_slice(save_path);
                append_path(&mut ret, path);
                ret
            }
            RenameMode::AbsolutePath => path.clone(),
        }
    }

    /// `renamed_files::file_name` — leaf of the rename, or the real name.
    #[must_use]
    pub fn file_name<'a>(&'a self, fs: &'a FileStorage, index: FileIndex) -> &'a [u8] {
        match self.renamed.get(&index) {
            None => fs.file_name(index),
            Some((_, path)) => crate::path::filename(path),
        }
    }

    /// `file_absolute_path`.
    #[must_use]
    pub fn file_absolute_path(&self, fs: &FileStorage, index: FileIndex) -> bool {
        match self.renamed.get(&index) {
            None => fs.file_absolute_path(index),
            Some((mode, _)) => *mode == RenameMode::AbsolutePath,
        }
    }

    /// `rename_file` — records an on-disk rename; `FileStorage` untouched.
    ///
    /// Upstream splits `new_filename` on torrent-name/`save_path` prefixes
    /// to pick the mode.
    pub fn rename_file(&mut self, fs: &FileStorage, index: FileIndex, new_filename: &[u8]) {
        if is_complete(new_filename) {
            self.renamed
                .insert(index, (RenameMode::AbsolutePath, new_filename.to_vec()));
            return;
        }
        // "name/file" → full_path; "file" → full_path as well (upstream
        // treats bare names as relative to the torrent root dir)
        let _ = fs;
        self.renamed
            .insert(index, (RenameMode::FullPath, new_filename.to_vec()));
    }

    /// `import_filenames`.
    pub fn import_filenames(&mut self, fs: &FileStorage, renamed: &HashMap<FileIndex, Vec<u8>>) {
        for (&i, name) in renamed {
            if i >= fs.end_file() {
                continue;
            }
            self.renamed.insert(i, (RenameMode::FullPath, name.clone()));
        }
    }

    /// `export_filenames` — the current renames as a flat map
    /// (full_path entries only, matching upstream's export shape).
    #[must_use]
    pub fn export_filenames(&self, _fs: &FileStorage) -> HashMap<FileIndex, Vec<u8>> {
        self.renamed
            .iter()
            .map(|(&i, (_, p))| (i, p.clone()))
            .collect()
    }
}

/// `filenames` — read-only view pairing a `FileStorage` with renames.
/// Rust lifetimes replace upstream's "references must outlive the view"
/// contract.
pub struct Filenames<'a> {
    /// The file list.
    pub files: &'a FileStorage,
    /// The rename overlay.
    pub renames: &'a RenamedFiles,
}

impl Filenames<'_> {
    /// `file_flags`.
    #[must_use]
    pub fn file_flags(&self, index: FileIndex) -> FileFlags {
        self.files.file_flags(index)
    }

    /// `end_file`.
    #[must_use]
    pub fn end_file(&self) -> FileIndex {
        self.files.end_file()
    }

    /// `pad_file_at`.
    #[must_use]
    pub fn pad_file_at(&self, index: FileIndex) -> bool {
        self.files.pad_file_at(index)
    }

    /// `file_size`.
    #[must_use]
    pub fn file_size(&self, index: FileIndex) -> i64 {
        self.files.file_size(index)
    }

    /// `file_offset`.
    #[must_use]
    pub fn file_offset(&self, index: FileIndex) -> i64 {
        self.files.file_offset(index)
    }

    /// `file_path` — renames applied.
    #[must_use]
    pub fn file_path(&self, index: FileIndex, save_path: &[u8]) -> Vec<u8> {
        self.renames.file_path(self.files, index, save_path)
    }

    /// `file_name`.
    #[must_use]
    pub fn file_name(&self, index: FileIndex) -> &[u8] {
        self.renames.file_name(self.files, index)
    }

    /// `file_absolute_path`.
    #[must_use]
    pub fn file_absolute_path(&self, index: FileIndex) -> bool {
        self.renames.file_absolute_path(self.files, index)
    }

    /// `symlink`.
    #[must_use]
    pub fn symlink(&self, index: FileIndex) -> Vec<u8> {
        self.files.symlink(index)
    }

    /// `file_range`.
    #[must_use]
    pub fn file_range(&self) -> IndexRange<FileIndex> {
        self.files.file_range()
    }

    /// `num_files`.
    #[must_use]
    pub fn num_files(&self) -> i32 {
        self.files.num_files()
    }

    /// `end_piece`.
    #[must_use]
    pub fn end_piece(&self) -> PieceIndex {
        self.files.end_piece()
    }

    /// `num_pieces`.
    #[must_use]
    pub fn num_pieces(&self) -> i32 {
        self.files.num_pieces()
    }

    /// `map_block`.
    #[must_use]
    pub fn map_block(&self, piece: PieceIndex, offset: i64, size: i64) -> Vec<FileSlice> {
        self.files.map_block(piece, offset, size)
    }

    /// `map_file`.
    #[must_use]
    pub fn map_file(&self, file: FileIndex, offset: i64, size: i32) -> PeerRequest {
        self.files.map_file(file, offset, size)
    }

    /// `root`.
    #[must_use]
    pub fn root(&self, index: FileIndex) -> Sha256Hash {
        self.files.root(index)
    }

    /// `piece_length`.
    #[must_use]
    pub fn piece_length(&self) -> i32 {
        self.files.piece_length()
    }
}
