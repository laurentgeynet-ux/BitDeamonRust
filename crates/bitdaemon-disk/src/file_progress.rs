//! Port of `aux_/file_progress.hpp`/`file_progress.cpp`.
//!
//! Per-file downloaded-byte accounting; lazily populated. The only
//! upstream dependency on `piece_picker` is `have_piece(piece)` in
//! `init` — we take the have-pieces `Bitfield` directly (documented
//! deviation; identical semantics, no picker dependency).

use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_common::types::{FileIndex, PieceIndex};
use bitdaemon_core::file_storage::FileStorage;

/// `aux::file_progress`.
#[derive(Default)]
pub struct FileProgress {
    /// `m_total_on_disk` — bytes downloaded to non-pad files.
    total_on_disk: i64,
    /// `m_file_progress` — per-file passed-hash bytes.
    file_progress: Vec<i64>,
}

impl FileProgress {
    /// `file_progress()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `init(picker, fs)` — upstream iterates pieces asking the picker;
    /// we take the same `have_pieces` bitfield the picker wraps.
    pub fn init(&mut self, have_pieces: &Bitfield, fs: &FileStorage) {
        let num_files = usize::try_from(fs.num_files()).unwrap_or(0);
        self.file_progress.clear();
        self.file_progress.resize(num_files, 0);
        self.total_on_disk = 0;

        let piece_size = i64::from(fs.piece_length());
        let total_size = fs.total_size();
        let mut off = 0i64;
        let mut file_index = FileIndex(0);
        let mut piece = PieceIndex(0);
        while piece < fs.end_piece() {
            let mut file_offset = off - fs.file_offset(file_index);
            while file_offset >= fs.file_size(file_index) {
                file_index = FileIndex(file_index.0 + 1);
                file_offset = off - fs.file_offset(file_index);
            }

            if usize::try_from(piece.0).is_ok_and(|p| have_pieces.get_bit(p)) {
                let mut size = piece_size.min(total_size - off);
                while size > 0 {
                    let add = size.min(fs.file_size(file_index) - file_offset);
                    if !fs.pad_file_at(file_index) {
                        self.total_on_disk += add;
                    }
                    self.file_progress[file_index.0 as usize] += add;
                    debug_assert!(
                        self.file_progress[file_index.0 as usize] <= fs.file_size(file_index)
                    );
                    size -= add;
                    if size > 0 {
                        file_index = FileIndex(file_index.0 + 1);
                        file_offset = 0;
                    }
                }
            }
            off += piece_size;
            piece = PieceIndex(piece.0 + 1);
        }
    }

    /// `export_progress(fp)` — copy the vector out.
    pub fn export_progress(&self, fp: &mut Vec<i64>) {
        fp.clear();
        fp.extend_from_slice(&self.file_progress);
    }

    /// `total_on_disk()`.
    #[must_use]
    pub fn total_on_disk(&self) -> i64 {
        self.total_on_disk
    }

    /// `empty()`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.file_progress.is_empty()
    }

    /// `clear()`.
    pub fn clear(&mut self) {
        self.total_on_disk = 0;
        self.file_progress = Vec::new();
    }

    /// `update(fs, index, completed_cb)` — a piece passed its hash
    /// check; credit its bytes and fire `completed_cb` for files that
    /// just finished.
    pub fn update(
        &mut self,
        fs: &FileStorage,
        index: PieceIndex,
        mut completed_cb: impl FnMut(FileIndex),
    ) {
        if self.file_progress.is_empty() {
            return;
        }

        let piece_size = i64::from(fs.piece_length());
        let mut off = i64::from(index.0) * piece_size;
        let mut file_index = fs.file_index_at_offset(off);
        let mut size = i64::from(fs.piece_size(index));
        while size > 0 {
            let file_offset = off - fs.file_offset(file_index);
            debug_assert!(file_index != fs.end_file());
            debug_assert!(file_offset <= fs.file_size(file_index));
            let add = (fs.file_size(file_index) - file_offset).min(size);

            let is_pad = fs.pad_file_at(file_index);
            if !is_pad {
                self.total_on_disk += add;
            }
            self.file_progress[file_index.0 as usize] += add;
            debug_assert!(self.file_progress[file_index.0 as usize] <= fs.file_size(file_index));

            if self.file_progress[file_index.0 as usize] >= fs.file_size(file_index) && !is_pad {
                completed_cb(file_index);
            }
            size -= add;
            off += add;
            file_index = FileIndex(file_index.0 + 1);
        }
    }
}
