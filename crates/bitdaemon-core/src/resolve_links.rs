//! Port of `aux_/resolve_links.hpp`/`.cpp` — mutable torrents: discover
//! identical files shared with other torrents (by piece hashes for v1,
//! by merkle root for v2).

use std::collections::HashMap;
use std::sync::Arc;

use bitdaemon_common::Sha256Hash;
use bitdaemon_common::types::FileIndex;

use crate::file_storage::{FileStorage, RenamedFiles};
use crate::torrent_info::TorrentInfo;

/// `aux::resolve_links`.
pub struct ResolveLinks {
    /// `m_torrent_file` — the torrent we're finding files for.
    torrent_file: Arc<TorrentInfo>,
    /// `m_links` — per-file duplicate path (empty = no link).
    links: Vec<Vec<u8>>,
    /// `m_file_sizes` — size → file index in `m_torrent_file`.
    file_sizes: HashMap<i64, Vec<FileIndex>>,
    /// `m_file_roots` — v2 merkle root → file index.
    file_roots: HashMap<Sha256Hash, Vec<FileIndex>>,
}

impl ResolveLinks {
    /// `resolve_links(ti)`.
    #[must_use]
    pub fn new(ti: Arc<TorrentInfo>) -> Self {
        let fs = ti.files();
        let v1 = ti.v1();
        let v2 = ti.v2();
        let piece_size = i64::from(fs.piece_length());

        let mut file_sizes: HashMap<i64, Vec<FileIndex>> = HashMap::new();
        let mut file_roots: HashMap<Sha256Hash, Vec<FileIndex>> = HashMap::new();
        for i in fs.file_range() {
            // files match by piece hashes — skip pads and unaligned files
            if fs.pad_file_at(i) {
                continue;
            }
            if fs.file_offset(i) % piece_size != 0 {
                continue;
            }
            if v1 {
                file_sizes.entry(fs.file_size(i)).or_default().push(i);
            }
            if v2 {
                file_roots.entry(fs.root(i)).or_default().push(i);
            }
        }
        Self {
            links: vec![Vec::new(); ti.num_files().max(0) as usize],
            torrent_file: ti,
            file_sizes,
            file_roots,
        }
    }

    /// `match(ti, filenames, save_path)` — look for shared files.
    /// `renamed` is upstream's `filenames` overlay (empty = no renames).
    pub fn matches(
        &mut self,
        ti: &TorrentInfo,
        fs: &FileStorage,
        renamed: &RenamedFiles,
        save_path: &[u8],
    ) {
        if self.torrent_file.v2() && ti.v2() {
            self.match_v2(fs, renamed, save_path);
        }
        if self.torrent_file.v1() && ti.v1() {
            self.match_v1(ti, fs, renamed, save_path);
        }
    }

    /// `match_v1` — same piece size, compare piece hashes.
    fn match_v1(
        &mut self,
        ti: &TorrentInfo,
        fs: &FileStorage,
        renamed: &RenamedFiles,
        save_path: &[u8],
    ) {
        if fs.piece_length() != self.torrent_file.files().piece_length() {
            return;
        }
        let piece_size = i64::from(fs.piece_length());

        for i in fs.file_range() {
            if fs.file_offset(i) % piece_size != 0 {
                continue;
            }
            if fs.pad_file_at(i) {
                continue;
            }
            let file_size = fs.file_size(i);

            let Some(candidates) = self.file_sizes.get(&file_size).cloned() else {
                continue;
            };
            for idx in candidates {
                debug_assert!(idx >= FileIndex(0));
                debug_assert!(idx < self.torrent_file.files().end_file());

                if !self.links[idx.0 as usize].is_empty() {
                    continue;
                }

                let their_piece = fs.map_file(i, 0, 0).piece;
                let our_piece = self.torrent_file.files().map_file(idx, 0, 0).piece;
                let num_pieces = (file_size + piece_size - 1) / piece_size;

                let mut matches = true;
                for p in 0..num_pieces {
                    if self
                        .torrent_file
                        .hash_for_piece(bitdaemon_common::types::PieceIndex(our_piece.0 + p as i32))
                        != ti.hash_for_piece(bitdaemon_common::types::PieceIndex(
                            their_piece.0 + p as i32,
                        ))
                    {
                        matches = false;
                        break;
                    }
                }
                if !matches {
                    continue;
                }

                self.links[idx.0 as usize] = renamed.file_path(fs, i, save_path);
                // dedup — one link per file
                if let Some(v) = self.file_sizes.get_mut(&file_size) {
                    v.retain(|&x| x != idx);
                }
                break;
            }
        }
    }

    /// `match_v2` — compare merkle roots.
    fn match_v2(&mut self, fs: &FileStorage, renamed: &RenamedFiles, save_path: &[u8]) {
        for i in fs.file_range() {
            if fs.pad_file_at(i) {
                continue;
            }
            let root = fs.root(i);
            let Some(candidates) = self.file_roots.get(&root).cloned() else {
                continue;
            };
            for idx in candidates {
                debug_assert!(idx >= FileIndex(0));
                debug_assert!(idx < self.torrent_file.files().end_file());
                if !self.links[idx.0 as usize].is_empty() {
                    continue;
                }
                self.links[idx.0 as usize] = renamed.file_path(fs, i, save_path);
                if let Some(v) = self.file_roots.get_mut(&root) {
                    v.retain(|&x| x != idx);
                }
                break;
            }
        }
    }

    /// `get_links()`.
    #[must_use]
    pub fn get_links(&self) -> &[Vec<u8>] {
        &self.links
    }

    /// `get_links() &&`.
    #[must_use]
    pub fn into_links(self) -> Vec<Vec<u8>> {
        self.links
    }
}
