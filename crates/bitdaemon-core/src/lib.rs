//! Torrent metainfo and file layout.
//!
//! Ports `torrent_info` (parse/validate .torrent files incl. v2 hybrid
//! torrents and merkle trees), `file_storage`, `create_torrent`,
//! `magnet_uri`, `load_torrent`, `read_resume_data` / `write_resume_data`,
//! `announce_entry`, `web_seed_entry`, `tracker_list`, `fingerprint`,
//! `identify_client`, `resolve_links`, `resolve_duplicate_filenames`.
//!
//! Status: **not ported yet** — see `docs/PORTING.md`.

#![forbid(unsafe_code)]

/// Parsed contents of a `.torrent` file (`libtorrent::torrent_info`).
///
/// TODO: implement — v1 info-hash (SHA-1 over bencoded `info` dict),
/// v2 info-hash (SHA-256), hybrid torrents, `parse_info_section`,
/// file list, piece hashes, web seeds, trackers, SSL/cert fields.
pub struct TorrentInfo;

/// File list and piece↔file mapping (`libtorrent::file_storage`).
///
/// TODO: implement — `file_path`, `file_offset`, `map_block`,
/// pad files, symlinks, canonical-path sanitization (`sanitize_append_path_element`).
pub struct FileStorage;

/// Builder for new torrents (`libtorrent::create_torrent`).
///
/// TODO: implement — piece-size selection, v1/v2/hybrid generation,
/// merkle tree computation, `set_hashes` from disk.
pub struct CreateTorrent;
