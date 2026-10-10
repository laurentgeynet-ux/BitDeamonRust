//! `create_torrent.hpp`/`create_torrent.cpp` — `.torrent` construction.
//!
//! `generate()` builds an `Entry` tree (our `Dictionary` is a `BTreeMap`,
//! so encoding order is the canonical one upstream's streaming writer
//! produces). `set_piece_hashes`/`list_files` use synchronous `std::fs`
//! I/O — upstream's disk-thread pipeline is scheduling detail; the
//! produced hashes are identical.

use std::io::{Read as _, Seek as _};

use bitdaemon_bencode::{Dictionary, Entry, bencode};
use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::{FileIndex, Sha1Hash, Sha256Hash};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::error::{Error, Result, TorrentError};
use crate::file_storage::{DEFAULT_BLOCK_SIZE, FileFlags, FileStorage};
use crate::merkle::{merkle_num_leafs, merkle_pad, merkle_root};
use crate::path::{combine_path, has_parent_path, lsplit_path};

/// `create_flags_t` — `create_torrent` construction flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CreateFlags(pub u32);

impl CreateFlags {
    /// `create_torrent::modification_time`.
    pub const MODIFICATION_TIME: Self = Self(1 << 2);
    /// `create_torrent::symlinks`.
    pub const SYMLINKS: Self = Self(1 << 3);
    /// `create_torrent::v2_only`.
    pub const V2_ONLY: Self = Self(1 << 5);
    /// `create_torrent::v1_only`.
    pub const V1_ONLY: Self = Self(1 << 6);
    /// `create_torrent::canonical_files`.
    pub const CANONICAL_FILES: Self = Self(1 << 7);
    /// `create_torrent::no_attributes`.
    pub const NO_ATTRIBUTES: Self = Self(1 << 8);
    /// `create_torrent::canonical_files_no_tail_padding`.
    pub const CANONICAL_FILES_NO_TAIL_PADDING: Self = Self(1 << 9);
    /// `create_torrent::allow_odd_piece_size` (hidden).
    pub const ALLOW_ODD_PIECE_SIZE: Self = Self(1 << 31);
    /// Empty flag set.
    pub const NONE: Self = Self(0);

    /// `flags & f`.
    #[must_use]
    pub fn contains(self, f: Self) -> bool {
        self.0 & f.0 != 0
    }
}

impl std::ops::BitOr for CreateFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// `create_file_entry` — one file to include in the torrent.
#[derive(Debug, Clone)]
pub struct CreateFileEntry {
    /// Path relative to torrent root (`filename`).
    pub filename: Vec<u8>,
    /// `size` in bytes.
    pub size: i64,
    /// `file_flags_t`.
    pub flags: FileFlags,
    /// `mtime` (0 = omit).
    pub mtime: i64,
    /// `symlink` target (BEP 47, torrent-root relative).
    pub symlink: Vec<u8>,
}

/// `aux::canonicalize` — sort by path, strip existing pad files, insert
/// `.pad/N` pads so every file ends on a piece boundary (BEP 52 layout).
/// `backwards_compatible` = `canonical_files_no_tail_padding` (pre-2.0.8
/// layout: pad *before* each file, none after the last).
#[must_use]
pub fn canonicalize(
    mut files: Vec<CreateFileEntry>,
    piece_length: i64,
    backwards_compatible: bool,
) -> (Vec<CreateFileEntry>, i64) {
    files.sort_by(|a, b| a.filename.cmp(&b.filename));
    let mut off = 0i64;
    if files.is_empty() {
        return (files, off);
    }
    let top_level = lsplit_path(&files[0].filename).0.to_vec();

    let mut new_files = Vec::with_capacity(files.len() * 2);
    let add_pad = |files: &mut Vec<CreateFileEntry>, fe: &CreateFileEntry, off: &mut i64| {
        if *off % piece_length != 0 && fe.size > 0 {
            let pad_size = piece_length - (*off % piece_length);
            let name = format!(".pad/{pad_size}");
            files.push(CreateFileEntry {
                filename: combine_path(&top_level, name.as_bytes()),
                size: pad_size,
                flags: FileFlags::PAD_FILE,
                mtime: 0,
                symlink: Vec::new(),
            });
            *off += pad_size;
        }
    };

    for fe in &files {
        if fe.flags.contains(FileFlags::PAD_FILE) {
            continue;
        }
        if backwards_compatible {
            add_pad(&mut new_files, fe, &mut off);
        }
        new_files.push(fe.clone());
        off += fe.size;
        // v2: pad the end of each file to a piece boundary — not for
        // single-file torrents (would break hybrid)
        if !backwards_compatible && files.len() > 1 {
            add_pad(&mut new_files, fe, &mut off);
        }
    }
    (new_files, off)
}

/// `libtorrent::create_torrent`.
#[derive(Debug)]
pub struct CreateTorrent {
    files: Vec<CreateFileEntry>,
    total_size: i64,
    piece_length: i32,
    num_pieces: i32,
    name: Vec<u8>,
    urls: Vec<(Vec<u8>, i32)>,
    url_seeds: Vec<Vec<u8>>,
    piece_hash: Vec<Sha1Hash>,
    file_piece_hash: Vec<Vec<Sha256Hash>>,
    similar: Vec<Sha1Hash>,
    collections: Vec<Vec<u8>>,
    nodes: Vec<(Vec<u8>, i64)>,
    creation_date: i64,
    comment: Vec<u8>,
    created_by: Vec<u8>,
    root_cert: Vec<u8>,
    multifile: bool,
    private: bool,
    include_mtime: bool,
    include_symlinks: bool,
    v2_only: bool,
    v1_only: bool,
}

fn compute_total_size(files: &[CreateFileEntry]) -> Result<i64> {
    let mut ret = 0i64;
    for f in files {
        ret = ret
            .checked_add(f.size)
            .ok_or(Error::Torrent(TorrentError::TorrentInvalidLength))?;
    }
    Ok(ret)
}

fn calc_num_pieces(total: i64, piece: i64) -> i32 {
    ((total + piece - 1) / piece) as i32
}

fn file_num_pieces(size: i64, piece_length: i64) -> i64 {
    (size + piece_length - 1) / piece_length
}

impl CreateTorrent {
    /// `create_torrent(files, piece_size, flags)`.
    ///
    /// # Errors
    /// `NoFilesInTorrent`, `TorrentInvalidLength`, `InvalidPieceSize`.
    pub fn new(files: Vec<CreateFileEntry>, piece_size: i32, flags: CreateFlags) -> Result<Self> {
        if files.is_empty() {
            return Err(Error::Torrent(TorrentError::NoFilesInTorrent));
        }
        let mut files = files;
        let mut total_size = compute_total_size(&files)?;
        if total_size == 0 {
            return Err(Error::Torrent(TorrentError::TorrentInvalidLength));
        }

        let mut multifile = files.len() > 1;
        if !multifile && has_parent_path(&files[0].filename) {
            multifile = true;
        }
        let name = if multifile {
            lsplit_path(&files[0].filename).0.to_vec()
        } else {
            files[0].filename.clone()
        };

        let mut piece_size = piece_size;
        if piece_size == 0 {
            // target piece count ≈ sqrt(total)/hash — upstream size table
            const SIZE_TABLE: [i64; 10] = [
                2_684_355,
                10_737_418,
                42_949_673,
                171_798_692,
                687_194_767,
                2_748_779_069,
                10_995_116_278,
                43_980_465_111,
                175_921_860_444,
                703_687_441_777,
            ];
            let i = SIZE_TABLE.iter().take_while(|&&s| s < total_size).count();
            piece_size = (DEFAULT_BLOCK_SIZE as i32) << i;
        }

        if !flags.contains(CreateFlags::V1_ONLY) {
            piece_size = piece_size.max(16 * 1024);
            if piece_size & (piece_size - 1) != 0 {
                return Err(Error::Torrent(TorrentError::InvalidPieceSize));
            }
        } else if piece_size % (16 * 1024) != 0
            && piece_size & (piece_size - 1) != 0
            && !flags.contains(CreateFlags::ALLOW_ODD_PIECE_SIZE)
        {
            return Err(Error::Torrent(TorrentError::InvalidPieceSize));
        }
        if piece_size > 128 * 1024 * 1024 {
            return Err(Error::Torrent(TorrentError::InvalidPieceSize));
        }

        let piece_length = piece_size;
        if !flags.contains(CreateFlags::V1_ONLY)
            || flags.contains(CreateFlags::CANONICAL_FILES)
            || flags.contains(CreateFlags::CANONICAL_FILES_NO_TAIL_PADDING)
        {
            let (f, t) = canonicalize(
                files,
                i64::from(piece_length),
                flags.contains(CreateFlags::CANONICAL_FILES_NO_TAIL_PADDING),
            );
            files = f;
            total_size = t;
        }
        let num_pieces = calc_num_pieces(total_size, i64::from(piece_length));

        Ok(Self {
            files,
            total_size,
            piece_length,
            num_pieces,
            name,
            urls: Vec::new(),
            url_seeds: Vec::new(),
            piece_hash: Vec::new(),
            file_piece_hash: Vec::new(),
            similar: Vec::new(),
            collections: Vec::new(),
            nodes: Vec::new(),
            creation_date: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            comment: Vec::new(),
            created_by: Vec::new(),
            root_cert: Vec::new(),
            multifile,
            private: false,
            include_mtime: flags.contains(CreateFlags::MODIFICATION_TIME),
            include_symlinks: flags.contains(CreateFlags::SYMLINKS),
            v2_only: flags.contains(CreateFlags::V2_ONLY),
            v1_only: flags.contains(CreateFlags::V1_ONLY),
        })
    }

    /// `file_list()`.
    #[must_use]
    pub fn file_list(&self) -> &[CreateFileEntry] {
        &self.files
    }
    /// `total_size()`.
    #[must_use]
    pub fn total_size(&self) -> i64 {
        self.total_size
    }
    /// `piece_length()`.
    #[must_use]
    pub fn piece_length(&self) -> i32 {
        self.piece_length
    }
    /// `num_pieces()`.
    #[must_use]
    pub fn num_pieces(&self) -> i32 {
        self.num_pieces
    }
    /// `is_v2_only()`.
    #[must_use]
    pub fn is_v2_only(&self) -> bool {
        self.v2_only
    }
    /// `is_v1_only()`.
    #[must_use]
    pub fn is_v1_only(&self) -> bool {
        self.v1_only
    }
    /// `priv()`/`set_priv`.
    #[must_use]
    pub fn priv_(&self) -> bool {
        self.private
    }
    /// `set_priv`.
    pub fn set_priv(&mut self, p: bool) {
        self.private = p;
    }
    /// `file_at`.
    #[must_use]
    pub fn file_at(&self, f: FileIndex) -> &CreateFileEntry {
        &self.files[f.0 as usize]
    }
    /// `file_range`.
    pub fn file_range(&self) -> impl Iterator<Item = usize> {
        0..self.files.len()
    }
    /// `file_piece_range` — piece count for file `f`.
    #[must_use]
    pub fn file_num_pieces(&self, f: FileIndex) -> i64 {
        file_num_pieces(self.files[f.0 as usize].size, i64::from(self.piece_length))
    }
    /// `piece_size(i)`.
    #[must_use]
    pub fn piece_size(&self, i: PieceIndex) -> i32 {
        if i.0 != self.num_pieces - 1 {
            return self.piece_length;
        }
        let size_except_last = i64::from(self.num_pieces - 1) * i64::from(self.piece_length);
        (self.total_size - size_except_last) as i32
    }
    /// `end_piece`.
    #[must_use]
    pub fn end_piece(&self) -> i32 {
        self.num_pieces
    }

    /// `set_comment`.
    pub fn set_comment(&mut self, s: &[u8]) {
        self.comment = s.to_vec();
    }
    /// `set_creator`.
    pub fn set_creator(&mut self, s: &[u8]) {
        self.created_by = s.to_vec();
    }
    /// `set_creation_date` — 0 omits the field.
    pub fn set_creation_date(&mut self, ts: i64) {
        self.creation_date = ts;
    }
    /// `set_root_cert`.
    pub fn set_root_cert(&mut self, cert: &[u8]) {
        self.root_cert = cert.to_vec();
    }
    /// `add_similar_torrent`.
    pub fn add_similar_torrent(&mut self, ih: Sha1Hash) {
        self.similar.push(ih);
    }
    /// `add_collection`.
    pub fn add_collection(&mut self, c: &[u8]) {
        self.collections.push(c.to_vec());
    }
    /// `add_node`.
    pub fn add_node(&mut self, host: &[u8], port: i64) {
        self.nodes.push((host.to_vec(), port));
    }
    /// `add_url_seed`.
    pub fn add_url_seed(&mut self, url: &[u8]) {
        self.url_seeds.push(url.to_vec());
    }
    /// `add_tracker` — dedup'd, sorted by tier.
    pub fn add_tracker(&mut self, url: &[u8], tier: i32) {
        if url.is_empty() || self.urls.iter().any(|(u, _)| u == url) {
            return;
        }
        self.urls.push((url.to_vec(), tier));
        self.urls.sort_by_key(|(_, t)| *t);
    }

    /// `set_hash` — v1 piece hash.
    pub fn set_hash(&mut self, index: PieceIndex, h: Sha1Hash) -> Result<()> {
        if self.v2_only {
            return Err(Error::Torrent(TorrentError::InvalidHashEntry));
        }
        if self.piece_hash.is_empty() {
            self.piece_hash
                .resize(self.num_pieces as usize, Sha1Hash::default());
        }
        self.piece_hash[index.0 as usize] = h;
        Ok(())
    }

    /// `set_hash2` — v2 per-file piece root.
    pub fn set_hash2(&mut self, file: FileIndex, piece: i64, h: Sha256Hash) -> Result<()> {
        if self.v1_only {
            return Err(Error::Torrent(TorrentError::InvalidHashEntry));
        }
        if self.file_piece_hash.is_empty() {
            self.file_piece_hash.resize(self.files.len(), Vec::new());
        }
        let f = &self.files[file.0 as usize];
        let npieces = file_num_pieces(f.size, i64::from(self.piece_length)) as usize;
        let fh = &mut self.file_piece_hash[file.0 as usize];
        if fh.is_empty() {
            fh.resize(npieces, Sha256Hash::default());
        }
        fh[piece as usize] = h;
        Ok(())
    }

    fn validate_v2_hashes(&self) -> bool {
        if self.file_piece_hash.len() != self.files.len() {
            return false;
        }
        for (i, f) in self.files.iter().enumerate() {
            let hashes = &self.file_piece_hash[i];
            if f.flags.contains(FileFlags::PAD_FILE) {
                if !hashes.is_empty() {
                    return false;
                }
                continue;
            }
            if hashes.len() as i64 != file_num_pieces(f.size, i64::from(self.piece_length)) {
                return false;
            }
            if hashes.iter().any(Sha256Hash::is_all_zeros) {
                return false;
            }
        }
        true
    }

    fn validate_v1_hashes(&self) -> bool {
        if self.piece_hash.len() as i64
            != file_num_pieces(self.total_size, i64::from(self.piece_length))
        {
            return false;
        }
        !self.piece_hash.iter().any(Sha1Hash::is_all_zeros)
    }

    /// `generate()` — the `.torrent` dict as an `Entry`.
    ///
    /// # Errors
    /// `TorrentMissingFileTree`, `InvalidHashEntry`,
    /// `TorrentInconsistentFiles` (path/dir conflicts in the v2 tree).
    pub fn generate(&self) -> Result<Entry> {
        if self.files.is_empty() || self.total_size == 0 {
            return Err(Error::Torrent(TorrentError::TorrentMissingFileTree));
        }
        let make_v2 = self.validate_v2_hashes();
        let make_v1 = self.validate_v1_hashes();
        if !make_v1 && !make_v2 {
            return Err(Error::Torrent(TorrentError::InvalidHashEntry));
        }

        let mut dict = Dictionary::new();
        let mut info = Dictionary::new();
        let ins = |d: &mut Dictionary, k: &[u8], v: Entry| {
            d.insert(k.to_vec(), v);
        };

        if !self.urls.is_empty() {
            ins(
                &mut dict,
                b"announce",
                Entry::String(self.urls[0].0.clone()),
            );
        }
        if !self.nodes.is_empty() {
            let nodes: Vec<Entry> = self
                .nodes
                .iter()
                .map(|(h, p)| Entry::List(vec![Entry::String(h.clone()), Entry::Int(*p)]))
                .collect();
            ins(&mut dict, b"nodes", Entry::List(nodes));
        }
        if self.urls.len() > 1 {
            let mut trackers: Vec<Entry> = Vec::new();
            let mut tier: Vec<Entry> = Vec::new();
            let mut current_tier = self.urls[0].1;
            for (url, t) in &self.urls {
                if *t != current_tier {
                    current_tier = *t;
                    trackers.push(Entry::List(std::mem::take(&mut tier)));
                }
                tier.push(Entry::String(url.clone()));
            }
            trackers.push(Entry::List(tier));
            ins(&mut dict, b"announce-list", Entry::List(trackers));
        }
        if !self.comment.is_empty() {
            ins(&mut dict, b"comment", Entry::String(self.comment.clone()));
        }
        if self.creation_date != 0 {
            ins(&mut dict, b"creation date", Entry::Int(self.creation_date));
        }
        if !self.created_by.is_empty() {
            ins(
                &mut dict,
                b"created by",
                Entry::String(self.created_by.clone()),
            );
        }
        if !self.url_seeds.is_empty() {
            if self.url_seeds.len() == 1 {
                ins(
                    &mut dict,
                    b"url-list",
                    Entry::String(self.url_seeds[0].clone()),
                );
            } else {
                ins(
                    &mut dict,
                    b"url-list",
                    Entry::List(self.url_seeds.iter().cloned().map(Entry::String).collect()),
                );
            }
        }

        // v2: per-file roots + piece layers
        let mut fileroots = vec![Sha256Hash::default(); self.files.len()];
        let mut piece_layers = Dictionary::new();
        if make_v2 {
            let pad_hash = merkle_pad(
                usize::try_from(self.piece_length).unwrap_or(1) / DEFAULT_BLOCK_SIZE as usize,
                1,
            );
            for fi in self.file_range() {
                let f = &self.files[fi];
                if f.flags.contains(FileFlags::PAD_FILE) || f.size == 0 {
                    continue;
                }
                fileroots[fi] = merkle_root(&self.file_piece_hash[fi], pad_hash);
                // single-piece files: root IS the piece hash, no layer
                if self.file_piece_hash[fi].len() < 2 {
                    continue;
                }
                let mut pieces = Vec::with_capacity(self.file_piece_hash[fi].len() * 32);
                for h in &self.file_piece_hash[fi] {
                    pieces.extend_from_slice(h.as_bytes());
                }
                piece_layers.insert(fileroots[fi].as_bytes().to_vec(), Entry::String(pieces));
            }
            ins(&mut dict, b"piece layers", Entry::Dict(piece_layers));
        }

        if !self.collections.is_empty() {
            ins(
                &mut info,
                b"collections",
                Entry::List(
                    self.collections
                        .iter()
                        .cloned()
                        .map(Entry::String)
                        .collect(),
                ),
            );
        }
        if !self.similar.is_empty() {
            ins(
                &mut info,
                b"similar",
                Entry::List(
                    self.similar
                        .iter()
                        .map(|h| Entry::String(h.as_bytes().to_vec()))
                        .collect(),
                ),
            );
        }
        ins(&mut info, b"name", Entry::String(self.name.clone()));
        if !self.root_cert.is_empty() {
            ins(
                &mut info,
                b"ssl-cert",
                Entry::String(self.root_cert.clone()),
            );
        }
        if self.private {
            ins(&mut info, b"private", Entry::Int(1));
        }

        if make_v1 {
            if !self.multifile {
                let file = &self.files[0];
                if self.include_mtime && file.mtime != 0 {
                    ins(&mut info, b"mtime", Entry::Int(file.mtime));
                }
                ins(&mut info, b"length", Entry::Int(file.size));
                add_file_attrs(&mut info, file.flags, self.include_symlinks);
                if self.include_symlinks && file.flags.contains(FileFlags::SYMLINK) {
                    add_symlink_path(&mut info, &file.symlink);
                }
            } else {
                let mut fl = Vec::with_capacity(self.files.len());
                for f in &self.files {
                    let mut fe = Dictionary::new();
                    if self.include_mtime && f.mtime != 0 {
                        fe.insert(b"mtime".to_vec(), Entry::Int(f.mtime));
                    }
                    fe.insert(b"length".to_vec(), Entry::Int(f.size));
                    // skip the torrent-name element
                    let mut path_elems = Vec::new();
                    let mut rest = lsplit_path(&f.filename).1;
                    loop {
                        let (elem, r) = lsplit_path(rest);
                        if elem.is_empty() {
                            break;
                        }
                        path_elems.push(Entry::String(elem.to_vec()));
                        rest = r;
                    }
                    fe.insert(b"path".to_vec(), Entry::List(path_elems));
                    add_file_attrs(&mut fe, f.flags, self.include_symlinks);
                    if self.include_symlinks && f.flags.contains(FileFlags::SYMLINK) {
                        add_symlink_path(&mut fe, &f.symlink);
                    }
                    fl.push(Entry::Dict(fe));
                }
                ins(&mut info, b"files", Entry::List(fl));
            }
        }

        if make_v2 {
            let mut tree = Dictionary::new();
            // files ordered by path → correctly ordered file tree
            let mut sorted: Vec<usize> = self
                .file_range()
                .filter(|&i| !self.files[i].flags.contains(FileFlags::PAD_FILE))
                .collect();
            sorted.sort_by(|&a, &b| self.files[a].filename.cmp(&self.files[b].filename));

            for i in sorted {
                let f = &self.files[i];
                let elems: Vec<&[u8]> = {
                    let split = if self.multifile {
                        lsplit_path(&f.filename).1
                    } else {
                        &f.filename[..]
                    };
                    let mut v = Vec::new();
                    let mut rest = split;
                    loop {
                        let (e, r) = lsplit_path(rest);
                        if e.is_empty() {
                            break;
                        }
                        v.push(e);
                        rest = r;
                    }
                    v
                };
                // walk/create nested dicts
                let mut node = &mut tree;
                for e in &elems {
                    let next = node
                        .entry(e.to_vec())
                        .or_insert_with(|| Entry::Dict(Dictionary::new()));
                    match next {
                        Entry::Dict(d) => {
                            if d.contains_key(b"".as_slice()) {
                                // a file already exists at this path
                                return Err(Error::Torrent(TorrentError::TorrentInconsistentFiles));
                            }
                            node = d;
                        }
                        _ => {
                            return Err(Error::Torrent(TorrentError::TorrentInconsistentFiles));
                        }
                    }
                }
                if !node.is_empty() {
                    // a directory already exists at this path
                    return Err(Error::Torrent(TorrentError::TorrentInconsistentFiles));
                }
                let mut fe = Dictionary::new();
                if self.include_mtime && f.mtime != 0 {
                    fe.insert(b"mtime".to_vec(), Entry::Int(f.mtime));
                }
                add_file_attrs(&mut fe, f.flags, self.include_symlinks);
                if self.include_symlinks && f.flags.contains(FileFlags::SYMLINK) {
                    add_symlink_path(&mut fe, &f.symlink);
                } else {
                    if f.size > 0 {
                        fe.insert(
                            b"pieces root".to_vec(),
                            Entry::String(fileroots[i].as_bytes().to_vec()),
                        );
                    }
                    fe.insert(b"length".to_vec(), Entry::Int(f.size));
                }
                node.insert(b"".to_vec(), Entry::Dict(fe));
            }
            ins(&mut info, b"file tree", Entry::Dict(tree));
            ins(&mut info, b"meta version", Entry::Int(2));
        }

        ins(
            &mut info,
            b"piece length",
            Entry::Int(i64::from(self.piece_length)),
        );
        if make_v1 {
            let mut pieces = Vec::with_capacity(self.piece_hash.len() * 20);
            for h in &self.piece_hash {
                pieces.extend_from_slice(h.as_bytes());
            }
            ins(&mut info, b"pieces", Entry::String(pieces));
        }

        ins(&mut dict, b"info", Entry::Dict(info));
        Ok(Entry::Dict(dict))
    }

    /// `generate_buf()` — bencoded `.torrent`.
    pub fn generate_buf(&self) -> Result<Vec<u8>> {
        Ok(bencode(&self.generate()?))
    }
}

fn get_file_attrs(flags: FileFlags, include_symlinks: bool) -> Option<Vec<u8>> {
    let mask = FileFlags::PAD_FILE | FileFlags::HIDDEN | FileFlags::EXECUTABLE | FileFlags::SYMLINK;
    if !flags.contains(mask) {
        return None;
    }
    let mut attr = Vec::new();
    if flags.contains(FileFlags::PAD_FILE) {
        attr.push(b'p');
    }
    if flags.contains(FileFlags::HIDDEN) {
        attr.push(b'h');
    }
    if flags.contains(FileFlags::EXECUTABLE) {
        attr.push(b'x');
    }
    if include_symlinks && flags.contains(FileFlags::SYMLINK) {
        attr.push(b'l');
    }
    Some(attr)
}

fn add_file_attrs(d: &mut Dictionary, flags: FileFlags, include_symlinks: bool) {
    if let Some(attr) = get_file_attrs(flags, include_symlinks) {
        d.insert(b"attr".to_vec(), Entry::String(attr));
    }
}

fn add_symlink_path(d: &mut Dictionary, symlink_path: &[u8]) {
    let mut elems = Vec::new();
    let mut rest = symlink_path;
    loop {
        let (e, r) = lsplit_path(rest);
        if e.is_empty() {
            break;
        }
        elems.push(Entry::String(e.to_vec()));
        rest = r;
    }
    d.insert(b"symlink path".to_vec(), Entry::List(elems));
}

/// `list_files` — recursive file enumeration for torrent creation.
/// `pred` filters files/dirs (dirs return false → not traversed).
///
/// # Errors
/// Propagates `std::io` failures.
pub fn list_files(
    path: &std::path::Path,
    pred: &dyn Fn(std::path::PathBuf) -> bool,
    flags: CreateFlags,
) -> std::io::Result<Vec<CreateFileEntry>> {
    let parent = path.parent().unwrap_or(std::path::Path::new(""));
    let leaf = path
        .file_name()
        .map(|n| n.as_encoded_bytes().to_vec())
        .unwrap_or_default();
    let mut out = Vec::new();
    add_files_impl(&mut out, parent, leaf, pred, flags);
    Ok(out)
}

fn add_files_impl(
    files: &mut Vec<CreateFileEntry>,
    parent: &std::path::Path,
    rel: Vec<u8>,
    pred: &dyn Fn(std::path::PathBuf) -> bool,
    flags: CreateFlags,
) {
    let full = parent.join(crate::path::bytes_to_path(&rel));
    if !pred(full.clone()) {
        return;
    }
    // symlink-aware metadata (don't follow for the flag check)
    let meta = if flags.contains(CreateFlags::SYMLINKS) {
        std::fs::symlink_metadata(&full)
    } else {
        full.metadata()
    };
    let Ok(meta) = meta else { return };

    let is_symlink = meta.file_type().is_symlink();
    if meta.is_dir() && !(is_symlink && flags.contains(CreateFlags::SYMLINKS)) {
        let Ok(rd) = std::fs::read_dir(&full) else {
            return;
        };
        let mut names: Vec<std::ffi::OsString> =
            rd.filter_map(|e| e.ok().map(|e| e.file_name())).collect();
        names.sort();
        for n in names {
            let leaf = n.as_encoded_bytes();
            if leaf == b"." || leaf == b".." {
                continue;
            }
            let mut child = rel.clone();
            if !child.is_empty() {
                child.push(crate::path::SEPARATOR);
            }
            child.extend_from_slice(leaf);
            add_files_impl(files, parent, child, pred, flags);
        }
        return;
    }

    let mut fflags = FileFlags::NONE;
    if !flags.contains(CreateFlags::NO_ATTRIBUTES) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if meta.permissions().mode() & 0o111 != 0 {
                fflags |= FileFlags::EXECUTABLE;
            }
        }
        if is_symlink {
            fflags |= FileFlags::SYMLINK;
        }
        if rel
            .rsplit(|&c| c == crate::path::SEPARATOR)
            .next()
            .is_some_and(|leaf| leaf.starts_with(b"."))
        {
            fflags |= FileFlags::HIDDEN;
        }
    }
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    if is_symlink && flags.contains(CreateFlags::SYMLINKS) {
        let Ok(target) = std::fs::read_link(&full) else {
            return;
        };
        // BEP 47: target stored torrent-root relative; drop links that
        // escape the root
        let resolved = if target.is_absolute() {
            target
        } else {
            full.parent().unwrap_or(parent).join(&target)
        };
        let root = parent.join(crate::path::bytes_to_path(crate::path::lsplit_path(&rel).0));
        let Ok(rel_target) = resolved.strip_prefix(&root).map(|p| p.to_path_buf()) else {
            return;
        };
        let sym = crate::path::path_to_bytes(&rel_target);
        if sym.is_empty() || !pred(root.join(&rel_target)) {
            return;
        }
        files.push(CreateFileEntry {
            filename: rel,
            size: 0,
            flags: fflags,
            mtime,
            symlink: sym,
        });
    } else {
        files.push(CreateFileEntry {
            filename: rel,
            size: meta.len() as i64,
            flags: fflags,
            mtime,
            symlink: Vec::new(),
        });
    }
}

/// `set_piece_hashes` — read files under `base` and compute all v1/v2
/// hashes synchronously (upstream pipelines this through the disk
/// threads; output identical). `on_piece` is invoked per completed
/// piece index.
///
/// # Errors
/// `NoFilesInTorrent`, `TorrentInvalidLength`, `std::io`.
pub fn set_piece_hashes(
    t: &mut CreateTorrent,
    base: &std::path::Path,
    on_piece: &mut dyn FnMut(PieceIndex),
) -> Result<()> {
    if t.file_list().is_empty() {
        return Err(Error::Torrent(TorrentError::NoFilesInTorrent));
    }
    if t.total_size() == 0 {
        return Err(Error::Torrent(TorrentError::TorrentInvalidLength));
    }

    // map torrent-offset → files, then stream the data sequentially
    // piece by piece (files are piece-aligned for v2 layouts)
    let mut fs = FileStorage::new();
    fs.set_piece_length(t.piece_length);
    for f in t.file_list() {
        fs.add_file(
            None,
            &f.filename,
            f.size,
            f.flags,
            f.mtime,
            &f.symlink,
            None,
        )?;
    }
    fs.set_num_pieces(crate::file_storage::calc_num_pieces(&fs));

    let blocks_per_piece =
        usize::try_from(t.piece_length).unwrap_or(1) / DEFAULT_BLOCK_SIZE as usize;
    // per-piece accumulators
    let mut v1 = Sha1::new();
    let mut v2_blocks: Vec<Sha256Hash> = Vec::with_capacity(blocks_per_piece);
    let mut block = Sha256::new();
    let mut block_bytes = 0usize;

    let flush_piece = |t: &mut CreateTorrent,
                       fs: &FileStorage,
                       piece: i32,
                       v1: &mut Sha1,
                       v2_blocks: &mut Vec<Sha256Hash>|
     -> Result<()> {
        if !t.is_v2_only() {
            let h = Sha1Hash::from_bytes(&v1.finalize_reset()).unwrap_or_default();
            t.set_hash(PieceIndex(piece), h)?;
        }
        if !t.is_v1_only() {
            let current_file = fs.file_index_at_piece(PieceIndex(piece));
            let fe = &t.file_list()[current_file.0 as usize];
            if !fe.flags.contains(FileFlags::PAD_FILE) {
                let file_first_piece = fe_size_offset(fs, current_file, t.piece_length);
                let file_piece_offset = i64::from(piece) - file_first_piece;
                let file_size = fs.file_size(current_file);
                let file_blocks = fs.file_num_blocks(current_file);
                let piece_blocks = fs.blocks_in_piece2(PieceIndex(piece));
                let num_leafs = merkle_num_leafs(file_blocks as usize);
                let padded_leafs = if file_size < i64::from(t.piece_length) {
                    num_leafs
                } else {
                    blocks_per_piece
                };
                v2_blocks.resize(padded_leafs, Sha256Hash::default());
                for b in v2_blocks
                    .iter_mut()
                    .take(padded_leafs)
                    .skip(piece_blocks as usize)
                {
                    *b = Sha256Hash::default();
                }
                let root = merkle_root(&v2_blocks[..padded_leafs], Sha256Hash::default());
                t.set_hash2(current_file, file_piece_offset, root)?;
            }
        }
        v2_blocks.clear();
        Ok(())
    };

    let mut piece_data = vec![0u8; t.piece_length as usize];
    for piece_idx in 0..t.num_pieces() {
        let size = t.piece_size(PieceIndex(piece_idx)) as usize;
        let slices = fs.map_block(PieceIndex(piece_idx), 0, size as i64);
        let mut written = 0usize;
        for slice in slices {
            if slice.file_index == FileIndex(-1) {
                // hole (shouldn't happen — files are contiguous)
                piece_data[written..written + slice.size as usize].fill(0);
                written += slice.size as usize;
                continue;
            }
            let fe = &t.file_list()[slice.file_index.0 as usize];
            let disk_path = base.join(crate::path::bytes_to_path(&fe.filename));
            if fe.flags.contains(FileFlags::PAD_FILE) || fe.flags.contains(FileFlags::SYMLINK) {
                piece_data[written..written + slice.size as usize].fill(0);
                written += slice.size as usize;
                continue;
            }
            let mut f = std::fs::File::open(&disk_path)?;
            f.seek(std::io::SeekFrom::Start(slice.offset as u64))?;
            f.read_exact(&mut piece_data[written..written + slice.size as usize])?;
            written += slice.size as usize;
        }
        // hash the piece
        let pdata = &piece_data[..written];
        if !t.is_v2_only() {
            v1.update(pdata);
        }
        if !t.is_v1_only() {
            let mut off = 0usize;
            while off < pdata.len() {
                let n = (pdata.len() - off).min(DEFAULT_BLOCK_SIZE as usize - block_bytes);
                block.update(&pdata[off..off + n]);
                block_bytes += n;
                off += n;
                if block_bytes == DEFAULT_BLOCK_SIZE as usize {
                    v2_blocks
                        .push(Sha256Hash::from_bytes(&block.finalize_reset()).unwrap_or_default());
                    block_bytes = 0;
                }
            }
        }
        flush_piece(t, &fs, piece_idx, &mut v1, &mut v2_blocks)?;
        on_piece(PieceIndex(piece_idx));
    }
    Ok(())
}

/// `file_offset(file) / piece_length` — first piece of the file.
fn fe_size_offset(fs: &FileStorage, f: FileIndex, piece_length: i32) -> i64 {
    fs.file_offset(f) / i64::from(piece_length)
}
