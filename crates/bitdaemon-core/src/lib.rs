//! Torrent metainfo and file layout.
//!
//! Ports `torrent_info` (parse/validate .torrent files incl. v2 hybrid
//! torrents and merkle trees), `file_storage`, `create_torrent`,
//! `magnet_uri`, `load_torrent`, `read_resume_data` / `write_resume_data`,
//! `announce_entry`, `web_seed_entry`, `tracker_list`, `fingerprint`,
//! `identify_client`, `resolve_links`, `resolve_duplicate_filenames`.
//!
//! Status: Phase 2 in progress — `file_storage`, `path` and `utf8`
//! ported; `torrent_info`/`magnet_uri`/resume-data still pending. See
//! `docs/PORTING.md`.

#![forbid(unsafe_code)]

/// `lt::errors` torrent error codes.
/// `lt::version_str` — embedded in the `libtorrent-version` resume key.
/// Mirrors upstream's version string; bumped with the ported release.
pub const VERSION_STR: &[u8] = b"2.1.2.0";

/// `add_torrent_params.hpp` — the session-add parameter bundle.
pub mod add_torrent_params;
pub mod error;
/// `aux_/escape_string.hpp`/`escape_string.cpp` — URL encoding, base32.
pub mod escape_string;
/// `aux::file_storage` — the torrent file list and piece↔file mapping.
pub mod file_storage;
/// `magnet_uri.hpp`/`magnet_uri.cpp` — magnet link parse/serialize.
pub mod magnet_uri;
/// `aux_/merkle.hpp`/`merkle.cpp` — BEP-52 merkle tree math.
pub mod merkle;
/// `aux_/path.hpp`/`path.cpp` — pure path-string helpers (byte-oriented).
pub mod path;
/// `read_resume_data.cpp`/`write_resume_data.cpp` — fastresume wire format.
pub mod resume_data;
/// `torrent_info.cpp` + `load_torrent.cpp` — `.torrent` parsing
/// (v1/v2/hybrid, piece layers, trackers, web seeds).
pub mod torrent_info;
/// `aux_/utf8.hpp`/`utf8.cpp` — UTF-8 decode/encode + path-character rules.
pub mod utf8;

pub use add_torrent_params::{AddTorrentParams, StorageMode};
pub use error::{Error, Result, TorrentError};
pub use file_storage::{
    DEFAULT_BLOCK_SIZE, FileFlags, FileSlice, FileStorage, Filenames, NO_PATH, NOT_A_SYMLINK,
    PATH_IS_ABSOLUTE, RenameMode, RenamedFiles, calc_num_pieces, file_piece_range_exclusive,
    file_piece_range_inclusive, files_compatible,
};
pub use torrent_info::{
    AnnounceEntry, LoadTorrentLimits, TorrentInfo, TorrentInfoFlags, is_i2p_url,
    is_valid_tracker_url, load_torrent, load_torrent_buffer, maybe_url_encode, parse_piece_layers,
    parse_torrent_file,
};

/// Builder for new torrents (`libtorrent::create_torrent`).
///
/// TODO: implement — piece-size selection, v1/v2/hybrid generation,
/// merkle tree computation, `set_hashes` from disk.
pub struct CreateTorrent;
