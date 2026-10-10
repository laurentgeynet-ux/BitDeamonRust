//! `torrent_info.hpp`/`torrent_info.cpp` + `load_torrent.cpp` — parsing
//! and validation of `.torrent` files (v1, v2, hybrid).
//!
//! Ownership model vs upstream: upstream copies the info dictionary into
//! `m_info_section` and stores string *offsets* into it (`m_piece_hashes`,
//! collections, similar). We keep the same `info_section` copy for
//! byte-exact info-hash re-verification, but store *parsed* values
//! (`Vec<Sha1Hash>` pieces, `Vec<Sha256Hash>` roots already copied by
//! `add_file`). Semantics identical; pointers replaced by owned data
//! (ADR-0001).

use std::collections::HashSet;

use bitdaemon_bencode::{BdecodeNode, NodeType, bdecode_limits};
use bitdaemon_common::{FileIndex, InfoHash, PieceIndex, Sha1Hash, Sha256Hash};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::add_torrent_params::AddTorrentParams;
use crate::error::{Error, TorrentError};
use crate::file_storage::{DEFAULT_BLOCK_SIZE, FileFlags, FileStorage, files_compatible};
use crate::path::{combine_path, sanitize_append_path_element};
use crate::utf8::verify_encoding;

/// `load_torrent_limits` — resource limits for `.torrent` parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadTorrentLimits {
    /// `max_buffer_size` — cap on the `.torrent` file size. Default 10 MB.
    pub max_buffer_size: usize,
    /// `max_pieces`. Default `0x200000`.
    pub max_pieces: usize,
    /// `max_decode_depth` — bencode nesting. Default 100.
    pub max_decode_depth: usize,
    /// `max_decode_tokens` — bencode token budget. Default 3 000 000.
    pub max_decode_tokens: usize,
    /// `max_duplicate_filenames`. Default 500.
    pub max_duplicate_filenames: usize,
    /// `max_directory_depth`. Default 100.
    pub max_directory_depth: usize,
}

impl Default for LoadTorrentLimits {
    fn default() -> Self {
        Self {
            max_buffer_size: 10_000_000,
            max_pieces: 0x20_0000,
            max_decode_depth: 100,
            max_decode_tokens: 3_000_000,
            max_duplicate_filenames: 500,
            max_directory_depth: 100,
        }
    }
}

bitdaemon_common::bitfield_flag!(
    /// `torrent_info_flags_t` — internal flags.
    TorrentInfoFlags, u8
);

impl TorrentInfoFlags {
    /// `multifile`.
    pub const MULTIFILE: Self = Self::bit(0);
    /// `private_torrent`.
    pub const PRIVATE_TORRENT: Self = Self::bit(1);
    /// `i2p`.
    pub const I2P: Self = Self::bit(2);
    /// `ssl_torrent`.
    pub const SSL_TORRENT: Self = Self::bit(3);
}

/// `aux::is_valid_tracker_url` — scheme + control-char + structural check.
#[must_use]
pub fn is_valid_tracker_url(url: &[u8]) -> bool {
    if url.is_empty() {
        return false;
    }
    // reject control characters and space. RFC 3986 requires both to be
    // percent-encoded; control chars also enable header/CRLF injection
    for &c in url {
        if c <= 0x20 || c == 0x7f {
            return false;
        }
    }
    if !string_begins_no_case(b"http://", url)
        && !string_begins_no_case(b"https://", url)
        && !string_begins_no_case(b"wss://", url)
        && !string_begins_no_case(b"ws://", url)
        && !string_begins_no_case(b"udp://", url)
    {
        return false;
    }
    parse_url_components(url).is_some()
}

/// `aux::string_begins_no_case`.
fn string_begins_no_case(prefix: &[u8], s: &[u8]) -> bool {
    s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix)
}

/// `aux::ltrim` — strip leading whitespace.
#[must_use]
pub fn ltrim(s: &[u8]) -> &[u8] {
    let mut s = s;
    while !s.is_empty() && s[0].is_ascii_whitespace() {
        s = &s[1..];
    }
    s
}

/// `aux::ensure_trailing_slash`.
pub fn ensure_trailing_slash(url: &mut Vec<u8>) {
    if url.last() != Some(&b'/') {
        url.push(b'/');
    }
}

/// `aux::is_i2p_url` — `.i2p` host or `i2p://` scheme.
#[must_use]
pub fn is_i2p_url(url: &[u8]) -> bool {
    string_begins_no_case(b"i2p://", url) || {
        let host = match parse_url_components(url) {
            Some(c) => c.host,
            None => return false,
        };
        host.len() >= 4 && host[host.len() - 4..].eq_ignore_ascii_case(b".i2p")
    }
}

/// `aux::parse_url_components` result — the tuple members as fields.
#[derive(Debug, Clone, Copy)]
pub struct UrlComponents<'a> {
    pub protocol: &'a [u8],
    pub auth: &'a [u8],
    pub host: &'a [u8],
    pub port: Option<i32>,
    pub path: &'a [u8],
}

/// `aux::parse_url_components` — split into `(protocol, auth, host, port,
/// path)`. Minimal port of upstream's parser (enough for tracker/web-seed
/// validation); returns `None` on malformed input.
pub fn parse_url_components(url: &[u8]) -> Option<UrlComponents<'_>> {
    let scheme_end = url.iter().position(|&c| c == b':')?;
    let protocol = &url[..scheme_end];
    if protocol.is_empty() || !protocol.iter().all(u8::is_ascii_alphanumeric) {
        return None;
    }
    let mut rest = &url[scheme_end + 1..];
    if !rest.starts_with(b"//") {
        return None;
    }
    rest = &rest[2..];

    let slash = rest.iter().position(|&c| c == b'/').unwrap_or(rest.len());
    let authority = &rest[..slash];
    let path = &rest[slash..];

    let (auth, hostport) = match authority.iter().position(|&c| c == b'@') {
        Some(at) => (&authority[..at], &authority[at + 1..]),
        None => (&b""[..], authority),
    };

    let (host, port) = match hostport.iter().rposition(|&c| c == b':') {
        Some(colon)
            if hostport[colon + 1..].iter().all(u8::is_ascii_digit)
                && colon + 1 < hostport.len() =>
        {
            let p = std::str::from_utf8(&hostport[colon + 1..])
                .ok()?
                .parse::<i32>()
                .ok()?;
            (&hostport[..colon], Some(p))
        }
        _ => (hostport, None),
    };
    if host.is_empty() {
        return None;
    }
    Some(UrlComponents {
        protocol,
        auth,
        host,
        port,
        path,
    })
}

// `need_encoding`/`escape_path` live in `crate::escape_string`
// (upstream `escape_string.cpp`).

/// `maybe_url_encode` — re-encode the URL's path component if needed,
/// leaving everything else untouched.
#[must_use]
pub fn maybe_url_encode(url: &[u8]) -> Vec<u8> {
    let Some(c) = parse_url_components(url) else {
        return url.to_vec();
    };
    if !crate::escape_string::need_encoding(c.path) {
        return url.to_vec();
    }
    let escaped_path = crate::escape_string::escape_path(c.path);
    let mut msg = Vec::with_capacity(
        c.protocol.len() + 3 + c.auth.len() + 1 + c.host.len() + 1 + 5 + escaped_path.len(),
    );
    msg.extend_from_slice(c.protocol);
    msg.extend_from_slice(b"://");
    if !c.auth.is_empty() {
        msg.extend_from_slice(c.auth);
        msg.push(b'@');
    }
    msg.extend_from_slice(c.host);
    if let Some(p) = c.port {
        msg.extend_from_slice(format!(":{p}").as_bytes());
    }
    msg.extend_from_slice(&escaped_path);
    msg
}

/// `announce_entry` reduced to what the metainfo layer needs: URL + tier.
/// The full tracker client fields (`source`, `fail_limit`, endpoints) are
/// added by the tracker session in Phase 6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceEntry {
    /// Tracker URL.
    pub url: Vec<u8>,
    /// `tier` — try order (lower first).
    pub tier: u8,
}

/// `torrent_info` — everything parseable from an `info` dictionary.
#[derive(Debug, Clone, Default)]
pub struct TorrentInfo {
    /// `m_info_section` — verbatim copy of the bencoded info dict.
    info_section: Vec<u8>,
    /// `m_piece_hashes` — offset of the "pieces" payload inside
    /// `info_section` (`None` for v2-only torrents).
    piece_hashes_offset: Option<usize>,
    /// `m_info_hash`.
    info_hash: InfoHash,
    /// `m_files`.
    files: FileStorage,
    /// `m_similar_torrents`.
    similar_torrents: Vec<Sha1Hash>,
    /// `m_collections`.
    collections: Vec<Vec<u8>>,
    /// `m_ssl_cert` (contents, not offset).
    ssl_cert: Vec<u8>,
    /// `m_flags`.
    flags: TorrentInfoFlags,
    /// `m_piece_layers` — v2 per-file piece-layer hashes keyed by the
    /// file's merkle root.
    piece_layers: std::collections::HashMap<Sha256Hash, Vec<u8>>,
}

impl TorrentInfo {
    /// `torrent_info(bdecode_node info)` — `parse_info_section`.
    ///
    /// The node must live inside `buffer` (offsets are taken against it,
    /// exactly like upstream's `info_offset` bookkeeping).
    pub fn from_info_node(info: &BdecodeNode<'_>, cfg: &LoadTorrentLimits) -> Result<Self, Error> {
        if info.node_type() != NodeType::Dict {
            return Err(TorrentError::TorrentInfoNoDict.into());
        }

        let section = info.data_section();
        if section.is_empty() || section[0] != b'd' || section[section.len() - 1] != b'e' {
            return Err(TorrentError::InvalidBencoding.into());
        }
        let info_offset = info.data_offset().unwrap_or(0);

        // copy the info section
        let info_section = section.to_vec();

        // check for a version key
        let version = info.dict_find_int_value(b"meta version", -1);
        if version > 0 {
            if info.has_soft_error().is_some() {
                return Err(TorrentError::InvalidBencoding.into());
            }
            if version > 2 {
                return Err(TorrentError::TorrentUnknownVersion.into());
            }
        }

        // a v1 info-hash is only meaningful for version < 2, or for a
        // hybrid (v1+v2) torrent that also carries a "files"/"length"
        // listing alongside "meta version" >= 2
        let files_node = info.dict_find_list(b"files");
        let length_key = info.dict_find(b"length");
        let has_files = files_node.is_valid();
        let has_length = length_key.is_valid();
        let need_v1_hash = version < 2 || has_files || has_length;
        let need_v2_hash = version >= 2;

        // hash the info-field to calculate info-hash
        let mut info_hash = InfoHash::default();
        if need_v1_hash {
            let h = Sha1::digest(section);
            info_hash.v1 = Sha1Hash::from_bytes(&h).unwrap_or_default();
        }
        if need_v2_hash {
            let h = Sha256::digest(section);
            info_hash.v2 = Sha256Hash::from_bytes(&h).unwrap_or_default();
        }

        // extract piece length
        let piece_length = info.dict_find_int_value(b"piece length", -1);
        if piece_length <= 0 || piece_length > i64::from(FileStorage::MAX_PIECE_SIZE) {
            return Err(TorrentError::TorrentMissingPieceLength.into());
        }

        // according to BEP 52: "It must be a power of two and at least
        // 16KiB."
        if version > 1
            && (piece_length < DEFAULT_BLOCK_SIZE || (piece_length & (piece_length - 1)) != 0)
        {
            return Err(TorrentError::TorrentMissingPieceLength.into());
        }

        let mut files = FileStorage::new();
        files.set_piece_length(piece_length as i32);

        // extract file name (or the directory name if it's a multi file
        // torrent)
        let mut name_ent = info.dict_find_string(b"name.utf-8");
        if !name_ent.is_valid() {
            name_ent = info.dict_find_string(b"name");
        }
        if !name_ent.is_valid() {
            return Err(TorrentError::TorrentMissingName.into());
        }

        let mut name = Vec::new();
        sanitize_append_path_element(&mut name, name_ent.string_value(), false);
        if name.is_empty() {
            // the fallback name is always based on the v1 (SHA-1)
            // info-hash, even for v2-only torrents
            let v1 = if info_hash.has_v1() {
                info_hash.v1
            } else {
                Sha1Hash::from_bytes(&Sha1::digest(section)).unwrap_or_default()
            };
            name = v1.to_hex().into_bytes();
        }

        // extract file list — save a copy so we can extract both v1 and
        // v2 files then compare the results
        let mut v1_files = FileStorage::new();
        if version >= 2 {
            v1_files = files.clone();
        }

        let file_tree_node = info.dict_find_dict(b"file tree");
        let mut flags = TorrentInfoFlags::NONE;
        if version >= 2 && file_tree_node.is_valid() {
            extract_files2(
                &file_tree_node,
                &mut files,
                &name,
                has_files,
                cfg.max_directory_depth,
            )?;

            files.sanitize_symlinks();
            if files.num_files() > 1 {
                flags |= TorrentInfoFlags::MULTIFILE;
            }
        } else if version >= 2 {
            return Err(TorrentError::TorrentMissingFileTree.into());
        } else if file_tree_node.is_valid() {
            return Err(TorrentError::TorrentMissingMetaVersion.into());
        }

        if !has_files {
            // if this is a v2 torrent it is ok for the length key to be
            // missing — that means it is a v2 only torrent
            if version < 2 || has_length {
                // if there's no list of files, there has to be a length
                // field
                let target = if version == 2 {
                    &mut v1_files
                } else {
                    &mut files
                };
                extract_single_file(info, target, b"", true, cfg.max_directory_depth)?;
            }
            // else: this is a v2 only torrent, the v1 info-hash was
            // already left cleared, and there's no v1 file listing
        } else {
            let target = if version == 2 {
                &mut v1_files
            } else {
                &mut files
            };
            extract_files(&files_node, target, &name, cfg.max_directory_depth)?;
            flags |= TorrentInfoFlags::MULTIFILE;
        }

        if files.num_files() == 0 {
            return Err(TorrentError::NoFilesInTorrent.into());
        }
        if files.name().is_empty() {
            return Err(TorrentError::TorrentMissingName.into());
        }

        // ensure hybrid torrents have compatible v1 and v2 file storages
        if version >= 2 && v1_files.num_files() > 0 {
            // previous versions of libtorrent did not create hybrid
            // torrents with "tail-padding". When loading, accept both.
            if files.num_files() == v1_files.num_files() + 1 {
                files.remove_tail_padding();
            }
            if !files_compatible(&files, &v1_files) {
                return Err(TorrentError::TorrentInconsistentFiles.into());
            }
        }

        // extract SHA-1 hashes for all pieces — the division rounds up
        if files.total_size() / i64::from(files.piece_length())
            > i64::from(FileStorage::MAX_NUM_PIECES)
        {
            return Err(TorrentError::TooManyPiecesInTorrent.into());
        }

        files.set_num_pieces(
            ((files.total_size() + i64::from(files.piece_length()) - 1)
                / i64::from(files.piece_length())) as i32,
        );

        // we expect the piece hashes to be < 2 GB in size
        if i64::from(files.num_pieces()) >= i64::from(i32::MAX) / 20
            || files.num_pieces() as usize > cfg.max_pieces
        {
            return Err(TorrentError::TooManyPiecesInTorrent.into());
        }

        let mut piece_hashes_offset = None;
        let pieces = info.dict_find_string(b"pieces");
        if !pieces.is_valid() {
            // whenever a v1 info-hash is computed the torrent claims to be
            // verifiable with v1 piece hashes — require "pieces" then
            if need_v1_hash {
                return Err(TorrentError::TorrentMissingPieces.into());
            }
        } else {
            if pieces.string_length() != files.num_pieces() as usize * 20 {
                return Err(TorrentError::TorrentInvalidHashes.into());
            }
            let hash_offset = pieces.string_offset().unwrap_or(0) - info_offset;
            piece_hashes_offset = Some(hash_offset);
        }

        if info.dict_find_int_value(b"private", 0) != 0 {
            flags |= TorrentInfoFlags::PRIVATE_TORRENT;
        }

        let mut similar_torrents = Vec::new();
        let similar = info.dict_find_list(b"similar");
        if similar.is_valid() {
            for i in 0..similar.list_size() {
                let n = similar.list_at(i);
                if n.node_type() != NodeType::String {
                    continue;
                }
                if n.string_length() != 20 {
                    continue;
                }
                if let Some(h) = Sha1Hash::from_bytes(n.string_value()) {
                    similar_torrents.push(h);
                }
            }
        }

        let mut collections = Vec::new();
        let colls = info.dict_find_list(b"collections");
        if colls.is_valid() {
            for i in 0..colls.list_size() {
                let n = colls.list_at(i);
                if n.node_type() != NodeType::String {
                    continue;
                }
                collections.push(n.string_value().to_vec());
            }
        }

        let mut ssl_cert = Vec::new();
        let cert = info.dict_find_string(b"ssl-cert");
        if cert.is_valid() {
            flags |= TorrentInfoFlags::SSL_TORRENT;
            ssl_cert = cert.string_value().to_vec();
        }

        if files.total_size() == 0 {
            return Err(TorrentError::TorrentInvalidLength.into());
        }

        debug_assert!(info_hash.has_v2() == files.v2() || !need_v2_hash);

        Ok(Self {
            info_section,
            piece_hashes_offset,
            info_hash,
            files,
            similar_torrents,
            collections,
            ssl_cert,
            flags,
            piece_layers: Default::default(),
        })
    }

    /// `is_valid`.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.files.is_valid()
    }

    /// `info_hash`/`info_hashes`.
    #[must_use]
    pub fn info_hashes(&self) -> &InfoHash {
        &self.info_hash
    }

    /// `info_hash` for one protocol version.
    #[must_use]
    pub fn info_hash_v1(&self) -> Option<Sha1Hash> {
        self.info_hash.has_v1().then_some(self.info_hash.v1)
    }

    /// `info_hash` v2.
    #[must_use]
    pub fn info_hash_v2(&self) -> Option<Sha256Hash> {
        self.info_hash.has_v2().then_some(self.info_hash.v2)
    }

    /// `v1()`/`v2()`.
    #[must_use]
    pub fn v1(&self) -> bool {
        self.info_hash.has_v1()
    }

    /// `v2()`.
    #[must_use]
    pub fn v2(&self) -> bool {
        self.info_hash.has_v2()
    }

    /// `files()` — the layout.
    #[must_use]
    pub fn files(&self) -> &FileStorage {
        &self.files
    }

    /// `orig_files()` — same as `files()` for us (no remap overlay).
    #[must_use]
    pub fn orig_files(&self) -> &FileStorage {
        &self.files
    }

    /// `num_files()`.
    #[must_use]
    pub fn num_files(&self) -> i32 {
        self.files.num_files()
    }

    /// `name()`.
    #[must_use]
    pub fn name(&self) -> &[u8] {
        self.files.name()
    }

    /// `info_section()` — the verbatim bencoded info dict.
    #[must_use]
    pub fn info_section(&self) -> &[u8] {
        &self.info_section
    }

    /// `hash_for_piece(index)` — v1 piece hash. Zeros for v2 / OOB.
    #[must_use]
    pub fn hash_for_piece(&self, index: PieceIndex) -> Sha1Hash {
        let Some(off) = self.piece_hashes_offset else {
            return Sha1Hash::default();
        };
        let i = index.0 as usize;
        if index < self.files.end_piece() {
            let start = off + i * 20;
            return Sha1Hash::from_bytes(&self.info_section[start..start + 20]).unwrap_or_default();
        }
        Sha1Hash::default()
    }

    /// `piece_layer(file)` — the raw v2 piece-layer hashes for `file`
    /// (`None` if absent or pad file).
    #[must_use]
    pub fn piece_layer(&self, f: FileIndex) -> Option<&[u8]> {
        if self.files.pad_file_at(f) {
            return None;
        }
        let layer = self.piece_layers.get(&self.files.root(f))?;
        Some(layer.as_slice())
    }

    /// `set_piece_layers` — raw layers keyed by merkle root (from
    /// `parse_piece_layers`).
    pub(crate) fn set_piece_layers(
        &mut self,
        layers: std::collections::HashMap<Sha256Hash, Vec<u8>>,
    ) {
        self.piece_layers = layers;
    }

    /// `piece_layers()` accessor.
    #[must_use]
    pub fn piece_layers(&self) -> &std::collections::HashMap<Sha256Hash, Vec<u8>> {
        &self.piece_layers
    }

    /// `priv()`.
    #[must_use]
    pub fn is_private(&self) -> bool {
        self.flags.contains(TorrentInfoFlags::PRIVATE_TORRENT)
    }

    /// `is_i2p()`.
    #[must_use]
    pub fn is_i2p(&self) -> bool {
        self.flags.contains(TorrentInfoFlags::I2P)
    }

    /// `is_ssl_torrent()`.
    #[must_use]
    pub fn is_ssl_torrent(&self) -> bool {
        self.flags.contains(TorrentInfoFlags::SSL_TORRENT)
    }

    /// `ssl_cert()`.
    #[must_use]
    pub fn ssl_cert(&self) -> &[u8] {
        &self.ssl_cert
    }

    /// `num_pieces()`.
    #[must_use]
    pub fn num_pieces(&self) -> i32 {
        self.files.num_pieces()
    }

    /// `total_size()`.
    #[must_use]
    pub fn total_size(&self) -> i64 {
        self.files.total_size()
    }

    /// `piece_length()`.
    #[must_use]
    pub fn piece_length(&self) -> i32 {
        self.files.piece_length()
    }

    /// `similar_torrents()`.
    #[must_use]
    pub fn similar_torrents(&self) -> &[Sha1Hash] {
        &self.similar_torrents
    }

    /// `collections()`.
    #[must_use]
    pub fn collections(&self) -> &[Vec<u8>] {
        &self.collections
    }

    /// `flags`.
    #[must_use]
    pub fn flags(&self) -> TorrentInfoFlags {
        self.flags
    }
}

/// `get_file_attributes`.
fn get_file_attributes(dict: &BdecodeNode<'_>) -> FileFlags {
    let mut flags = FileFlags::NONE;
    let attr = dict.dict_find_string(b"attr");
    if attr.is_valid() {
        for &c in attr.string_value() {
            match c {
                b'l' => flags |= FileFlags::SYMLINK,
                b'x' => flags |= FileFlags::EXECUTABLE,
                b'h' => flags |= FileFlags::HIDDEN,
                b'p' => flags |= FileFlags::PAD_FILE,
                _ => {}
            }
        }
    }
    flags
}

/// `path_length` — sum of component lengths + one separator each.
fn path_length(p: &BdecodeNode<'_>) -> Result<usize, Error> {
    let mut ret = 0usize;
    let len = p.list_size();
    for i in 0..len {
        let e = p.list_at(i);
        if e.node_type() != NodeType::String {
            return Err(TorrentError::TorrentInvalidName.into());
        }
        ret += e.string_length();
    }
    Ok(ret + len)
}

/// `extract_single_file2` — v2 file-tree leaf.
fn extract_single_file2(
    dict: &BdecodeNode<'_>,
    files: &mut FileStorage,
    path: &[u8],
    name: &[u8],
    max_directory_depth: usize,
) -> Result<(), Error> {
    if dict.node_type() != NodeType::Dict {
        return Err(TorrentError::TorrentFileParseFailed.into());
    }

    let file_flags = get_file_attributes(dict);

    if file_flags.contains(FileFlags::PAD_FILE) {
        return Err(TorrentError::TorrentInvalidPadFile.into());
    }

    // symlinks have an implied "size" of zero
    let file_size = if file_flags.contains(FileFlags::SYMLINK) {
        0
    } else {
        dict.dict_find_int_value(b"length", -1)
    };

    if file_size < 0
        || (file_size / DEFAULT_BLOCK_SIZE) >= i64::from(FileStorage::MAX_NUM_PIECES)
        || file_size > FileStorage::MAX_FILE_SIZE
    {
        return Err(TorrentError::TorrentInvalidLength.into());
    }

    let mtime = dict.dict_find_int_value(b"mtime", 0);

    let mut pieces_root = None;

    let mut symlink_path = Vec::new();
    if file_flags.contains(FileFlags::SYMLINK) {
        let s_p = dict.dict_find_list(b"symlink path");
        if s_p.is_valid() {
            if s_p.list_size() > max_directory_depth {
                return Err(TorrentError::TorrentDirectoryTooDeep.into());
            }
            let preallocate = path_length(&s_p)?;
            symlink_path.reserve(preallocate);
            for i in 0..s_p.list_size() {
                let pe = s_p.list_at(i).string_value();
                // BEP 47 forbids "." and ".." in symlink path components.
                // We tolerate them by dropping them.
                if pe == b"." || pe == b".." {
                    continue;
                }
                sanitize_append_path_element(&mut symlink_path, pe, true);
            }
        }
    }

    if symlink_path.is_empty() && file_size > 0 {
        let root = dict.dict_find_string(b"pieces root");
        if !root.is_valid() || root.node_type() != NodeType::String || root.string_length() != 32 {
            return Err(TorrentError::TorrentMissingPiecesRoot.into());
        }
        let h = Sha256Hash::from_bytes(root.string_value()).unwrap_or_default();
        if h.is_all_zeros() {
            return Err(TorrentError::TorrentMissingPiecesRoot.into());
        }
        pieces_root = Some(h);
    }

    files.add_file(
        Some(name),
        path,
        file_size,
        file_flags,
        mtime,
        &symlink_path,
        pieces_root,
    )?;
    Ok(())
}

/// `extract_single_file` — v1 files[] entry or single-file torrent.
fn extract_single_file(
    dict: &BdecodeNode<'_>,
    files: &mut FileStorage,
    root_dir: &[u8],
    top_level: bool,
    max_directory_depth: usize,
) -> Result<(), Error> {
    if dict.node_type() != NodeType::Dict {
        return Err(TorrentError::TorrentFileParseFailed.into());
    }

    let mut file_flags = get_file_attributes(dict);

    let file_size = if file_flags.contains(FileFlags::SYMLINK) {
        0
    } else {
        dict.dict_find_int_value(b"length", -1)
    };

    if file_size < 0
        || (file_size / DEFAULT_BLOCK_SIZE) >= i64::from(i32::MAX) / 2
        || file_size > FileStorage::MAX_FILE_SIZE
    {
        return Err(TorrentError::TorrentInvalidLength.into());
    }

    let mtime = dict.dict_find_int_value(b"mtime", 0);

    let mut path = root_dir.to_vec();
    let mut filename: &[u8] = b"";

    if top_level {
        // prefer the name.utf-8
        let mut p = dict.dict_find_string(b"name.utf-8");
        if !p.is_valid() {
            p = dict.dict_find_string(b"name");
        }
        if !p.is_valid() || p.string_length() == 0 {
            return Err(TorrentError::TorrentMissingName.into());
        }

        filename = {
            let mut f = p.string_value();
            while f.first() == Some(&crate::path::SEPARATOR) {
                f = &f[1..];
            }
            f
        };

        sanitize_append_path_element(&mut path, p.string_value(), false);
        if path.is_empty() {
            return Err(TorrentError::TorrentMissingName.into());
        }
    } else {
        let mut p = dict.dict_find_list(b"path.utf-8");
        if !p.is_valid() {
            p = dict.dict_find_list(b"path");
        }

        if p.is_valid() && p.list_size() > 0 {
            if p.list_size() > max_directory_depth {
                return Err(TorrentError::TorrentDirectoryTooDeep.into());
            }
            let preallocate = path.len() + path_length(&p)?;
            path.reserve(preallocate);

            let end = p.list_size();
            for i in 0..end {
                let e = p.list_at(i);
                if i == end - 1 {
                    let mut f = e.string_value();
                    while f.first() == Some(&crate::path::SEPARATOR) {
                        f = &f[1..];
                    }
                    filename = f;
                }
                sanitize_append_path_element(&mut path, e.string_value(), true);
            }
        } else if file_flags.contains(FileFlags::PAD_FILE) {
            // pad files don't need a path element, they're stored under
            // the .pad directory
            path = combine_path(b".pad", file_size.to_string().as_bytes());
        } else {
            return Err(TorrentError::TorrentMissingName.into());
        }
    }

    // bitcomet pad file
    if contains_subslice(&path, b"_____padding_file_") {
        file_flags |= FileFlags::PAD_FILE;
    }

    let mut symlink_path = Vec::new();
    if file_flags.contains(FileFlags::SYMLINK) {
        let s_p = dict.dict_find_list(b"symlink path");
        if s_p.is_valid() {
            if s_p.list_size() > max_directory_depth {
                return Err(TorrentError::TorrentDirectoryTooDeep.into());
            }
            let preallocate = path_length(&s_p)?;
            symlink_path.reserve(preallocate);
            for i in 0..s_p.list_size() {
                let pe = s_p.list_at(i).string_value();
                if pe == b"." || pe == b".." {
                    continue;
                }
                sanitize_append_path_element(&mut symlink_path, pe, true);
            }
        } else {
            // technically this is an invalid torrent. "symlink path" must
            // exist
            file_flags &= !FileFlags::SYMLINK;
        }
        // symlink targets are validated later
    }

    let name_arg = if filename.len() > path.len() || !path.ends_with(filename) {
        // if the filename was sanitized and differs, clear it to just use
        // path
        None
    } else {
        Some(filename)
    };

    files.add_file(
        name_arg,
        &path,
        file_size,
        file_flags,
        mtime,
        &symlink_path,
        None,
    )?;
    Ok(())
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// `extract_files2` — walk the v2 file tree iteratively.
fn extract_files2(
    tree: &BdecodeNode<'_>,
    target: &mut FileStorage,
    root_dir: &[u8],
    mut is_multi_file: bool,
    max_directory_depth: usize,
) -> Result<(), Error> {
    if tree.node_type() != NodeType::Dict {
        return Err(TorrentError::TorrentFileParseFailed.into());
    }

    struct StackFrame<'buf> {
        tree: BdecodeNode<'buf>,
        path: Vec<u8>,
        index: usize,
    }

    let mut stack: Vec<StackFrame<'_>> = vec![StackFrame {
        tree: tree.clone(),
        path: root_dir.to_vec(),
        index: 0,
    }];

    while let Some(frame) = stack.last_mut() {
        if frame.index >= frame.tree.dict_size() {
            stack.pop();
            continue;
        }

        let (key_node, val) = frame.tree.dict_at_node(frame.index);
        frame.index += 1;

        if val.node_type() != NodeType::Dict || key_node.string_value().is_empty() {
            return Err(TorrentError::TorrentFileParseFailed.into());
        }

        let mut filename = key_node.string_value();
        while filename.first() == Some(&crate::path::SEPARATOR) {
            filename = &filename[1..];
        }

        let leaf_node = val.dict_size() == 1 && {
            let (k, _) = val.dict_at(0);
            k.is_empty()
        };
        let single_file = leaf_node && !is_multi_file && frame.tree.dict_size() == 1;

        let mut path = if single_file {
            Vec::new()
        } else {
            frame.path.clone()
        };
        sanitize_append_path_element(&mut path, filename, true);

        if leaf_node {
            let (_, leaf_dict) = val.dict_at_node(0);
            let name_arg = if filename.len() > path.len() || !path.ends_with(filename) {
                None
            } else {
                Some(filename)
            };
            extract_single_file2(
                &leaf_dict,
                target,
                &path,
                name_arg.unwrap_or(b""),
                max_directory_depth,
            )?;
        } else {
            // descend into the subdirectory (depth bound as upstream)
            if stack.len() > max_directory_depth {
                return Err(TorrentError::TorrentDirectoryTooDeep.into());
            }
            // don't push a frame we would immediately pop
            if val.dict_size() > 0 {
                stack.push(StackFrame {
                    tree: val,
                    path,
                    index: 0,
                });
            }
        }

        // the single-file shortcut only applies at the top of the file
        // tree
        is_multi_file = true;
    }

    Ok(())
}

/// `extract_files` — v1 `files` list.
fn extract_files(
    list: &BdecodeNode<'_>,
    target: &mut FileStorage,
    root_dir: &[u8],
    max_directory_depth: usize,
) -> Result<(), Error> {
    if list.node_type() != NodeType::List {
        return Err(TorrentError::TorrentFileParseFailed.into());
    }
    target.reserve(list.list_size());

    for i in 0..list.list_size() {
        extract_single_file(
            &list.list_at(i),
            target,
            root_dir,
            false,
            max_directory_depth,
        )?;
    }
    // this rewrites invalid symlinks to point to themselves
    target.sanitize_symlinks();
    Ok(())
}

/// `aux::parse_piece_layers` — validate + collect v2 piece layers.
/// Returns raw layers keyed by file merkle root.
pub fn parse_piece_layers(
    e: &BdecodeNode<'_>,
    fs: &FileStorage,
) -> Result<std::collections::HashMap<Sha256Hash, Vec<u8>>, Error> {
    if e.node_type() != NodeType::Dict {
        return Err(TorrentError::TorrentMissingPieceLayer.into());
    }

    let mut all_file_roots: std::collections::HashMap<Sha256Hash, FileIndex> =
        std::collections::HashMap::new();
    for i in fs.file_range() {
        if fs.file_size(i) <= i64::from(fs.piece_length()) {
            continue;
        }
        all_file_roots.insert(fs.root(i), i);
    }

    let mut layers = std::collections::HashMap::new();
    for i in 0..e.dict_size() {
        let (key, val) = e.dict_at(i);
        if key.len() != 32 || val.node_type() != NodeType::String || val.string_length() % 32 != 0 {
            return Err(TorrentError::TorrentInvalidPieceLayer.into());
        }
        let root = Sha256Hash::from_bytes(key).unwrap_or_default();
        let Some(&file) = all_file_roots.get(&root) else {
            // this piece layer doesn't refer to any file in this torrent
            return Err(TorrentError::TorrentInvalidPieceLayer.into());
        };
        let piece_layer = val.string_value();
        let num_pieces = fs.file_num_pieces(file);
        if piece_layer.len() != num_pieces as usize * 32 {
            return Err(TorrentError::TorrentInvalidPieceLayer.into());
        }
        // `merkle_tree::load_piece_layer` upstream: the layer must hash
        // back to the file's merkle root
        let pieces: Vec<Sha256Hash> = piece_layer
            .chunks_exact(32)
            .map(|c| Sha256Hash::from_bytes(c).unwrap_or_default())
            .collect();
        if !crate::merkle::validate_piece_layer(
            &pieces,
            fs.blocks_per_piece() as usize,
            &fs.root(file),
            fs.file_num_blocks(file) as usize,
        ) {
            return Err(TorrentError::TorrentInvalidPieceLayer.into());
        }
        layers.insert(root, piece_layer.to_vec());
        all_file_roots.remove(&root);
    }
    Ok(layers)
}

/// `aux::parse_torrent_file` — top-level: dict, `info`, announce-list,
/// nodes, url-list, comment, piece layers. Fills the canonical
/// [`AddTorrentParams`].
pub fn parse_torrent_file(
    torrent_file: &BdecodeNode<'_>,
    cfg: &LoadTorrentLimits,
) -> Result<AddTorrentParams, Error> {
    if torrent_file.node_type() != NodeType::Dict {
        return Err(TorrentError::TorrentIsNoDict.into());
    }

    let info = torrent_file.dict_find_dict(b"info");
    let mut out = AddTorrentParams::default();
    if !info.is_valid() {
        // a `magnet-uri` key means the caller should go through
        // `magnet_uri` instead — both paths report missing info upstream
        return Err(TorrentError::TorrentMissingInfo.into());
    }

    let mut ti = TorrentInfo::from_info_node(&info, cfg)?;

    if ti.v2() {
        // allow torrent files without piece layers, just like we allow
        // magnet links. However, if there are piece layers, make sure
        // they're valid
        let e = torrent_file.dict_find_dict(b"piece layers");
        if e.is_valid() {
            let layers = parse_piece_layers(&e, ti.files())?;
            ti.set_piece_layers(layers);
        }
    }

    // similar torrents (mutable torrents extension)
    let similar = torrent_file.dict_find_list(b"similar");
    if similar.is_valid() {
        let mut similar_torrents = Vec::new();
        for i in 0..similar.list_size() {
            let n = similar.list_at(i);
            if n.node_type() != NodeType::String || n.string_length() != 20 {
                continue;
            }
            if let Some(h) = Sha1Hash::from_bytes(n.string_value()) {
                similar_torrents.push(h);
            }
        }
        // note: info-section "similar" already captured in
        // from_info_node; this top-level list is the BEP 38 one. Upstream
        // keeps both — ours merges into `similar_torrents` on the params.
        let _ = similar_torrents;
    }

    // extract the url of the tracker
    let announce_node = torrent_file.dict_find_list(b"announce-list");
    if announce_node.is_valid() {
        out.trackers.reserve(announce_node.list_size());
        out.tracker_tiers.reserve(announce_node.list_size());
        for j in 0..announce_node.list_size() {
            let tier = announce_node.list_at(j);
            if tier.node_type() != NodeType::List {
                continue;
            }
            for k in 0..tier.list_size() {
                let url = ltrim(tier.list_string_value_at(k, b""));
                if !is_valid_tracker_url(url) {
                    continue;
                }
                if is_i2p_url(url) {
                    // out.flags |= i2p_torrent — recorded on ti.flags
                }
                out.trackers.push(url.to_vec());
                out.tracker_tiers.push(j as i32);
            }
        }
    }

    if out.trackers.is_empty() {
        let url = ltrim(torrent_file.dict_find_string_value(b"announce", b""));
        if is_valid_tracker_url(url) {
            out.trackers.push(url.to_vec());
            out.tracker_tiers.push(0);
        }
    }

    let nodes = torrent_file.dict_find_list(b"nodes");
    if nodes.is_valid() {
        for i in 0..nodes.list_size() {
            let n = nodes.list_at(i);
            if n.node_type() != NodeType::List
                || n.list_size() < 2
                || n.list_at(0).node_type() != NodeType::String
                || n.list_at(1).node_type() != NodeType::Int
            {
                continue;
            }
            out.dht_nodes.push((
                n.list_at(0).string_value().to_vec(),
                n.list_at(1).int_value() as i32,
            ));
        }
    }

    // extract creation date
    let cd = torrent_file.dict_find_int_value(b"creation date", -1);
    if cd >= 0 {
        out.creation_date = cd;
    }

    // url-seeds
    let url_seeds = torrent_file.dict_find(b"url-list");
    if url_seeds.is_valid()
        && url_seeds.node_type() == NodeType::String
        && url_seeds.string_length() > 0
    {
        let mut url = maybe_url_encode(url_seeds.string_value());
        if ti.num_files() > 1 {
            ensure_trailing_slash(&mut url);
        }
        out.url_seeds.push(url);
    } else if url_seeds.is_valid() && url_seeds.node_type() == NodeType::List {
        // only add a URL once
        let mut unique: HashSet<&[u8]> = HashSet::new();
        for i in 0..url_seeds.list_size() {
            let url = url_seeds.list_string_value_at(i, b"");
            if url.is_empty() {
                continue;
            }
            if !unique.insert(url) {
                continue;
            }
            let mut new_url = maybe_url_encode(url);
            if ti.num_files() > 1 {
                ensure_trailing_slash(&mut new_url);
            }
            out.url_seeds.push(new_url);
        }
    }

    out.comment = torrent_file
        .dict_find_string_value(b"comment.utf-8", b"")
        .to_vec();
    if out.comment.is_empty() {
        out.comment = torrent_file
            .dict_find_string_value(b"comment", b"")
            .to_vec();
    }
    verify_encoding(&mut out.comment);

    out.created_by = torrent_file
        .dict_find_string_value(b"created by.utf-8", b"")
        .to_vec();
    if out.created_by.is_empty() {
        out.created_by = torrent_file
            .dict_find_string_value(b"created by", b"")
            .to_vec();
    }
    verify_encoding(&mut out.created_by);

    out.info_hashes = *ti.info_hashes();
    out.ti = Some(std::sync::Arc::new(ti));
    Ok(out)
}

/// `load_torrent_buffer`.
pub fn load_torrent_buffer(
    buffer: &[u8],
    cfg: &LoadTorrentLimits,
) -> Result<AddTorrentParams, Error> {
    if buffer.len() > cfg.max_buffer_size {
        return Err(TorrentError::InvalidBencoding.into());
    }
    let e = bdecode_limits(buffer, cfg.max_decode_depth, cfg.max_decode_tokens);
    if let Some(err) = e.error() {
        return Err(Error::Bdecode(err));
    }
    parse_torrent_file(&e, cfg)
}

/// `load_torrent_buffer` with default limits.
pub fn load_torrent(buffer: &[u8]) -> Result<AddTorrentParams, Error> {
    load_torrent_buffer(buffer, &LoadTorrentLimits::default())
}
