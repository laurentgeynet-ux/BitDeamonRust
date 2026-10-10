//! `add_torrent_params.hpp` — the parameter bundle used by magnet
//! parsing and the resume-data wire format. Session-only fields
//! (`userdata`, tracker-client internals) arrive with later phases.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bitdaemon_common::flags::TorrentFlags;
use bitdaemon_common::types::DownloadPriority;
use bitdaemon_common::{Bitfield, InfoHash, Sha256Hash};

use crate::torrent_info::TorrentInfo;

/// `storage_mode_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageMode {
    /// `storage_mode_sparse` — default.
    #[default]
    Sparse,
    /// `storage_mode_allocate`.
    Allocate,
}

/// `add_torrent_params`.
#[derive(Debug, Clone)]
pub struct AddTorrentParams {
    /// `info_hashes`.
    pub info_hashes: InfoHash,
    /// `name` (`dn=`/`name`).
    pub name: Vec<u8>,
    /// `trackers` (`tr=`).
    pub trackers: Vec<Vec<u8>>,
    /// `tracker_tiers` — aligned with `trackers`.
    pub tracker_tiers: Vec<i32>,
    /// `url_seeds` (`ws=`/BEP17 `url-list`).
    pub url_seeds: Vec<Vec<u8>>,
    /// `http_seeds` (BEP19 `httpseeds`).
    pub http_seeds: Vec<Vec<u8>>,
    /// `dht_nodes` (`dht=`/`.torrent` `nodes`).
    pub dht_nodes: Vec<(Vec<u8>, i32)>,
    /// `peers` (`x.pe=`/resume `peers`/`peers6`).
    pub peers: Vec<SocketAddr>,
    /// `banned_peers`.
    pub banned_peers: Vec<SocketAddr>,
    /// `file_priorities` — `so=` selection or resume `file_priority`.
    pub file_priorities: Vec<DownloadPriority>,
    /// `piece_priorities`.
    pub piece_priorities: Vec<DownloadPriority>,
    /// `flags` — `torrent_flags_t`.
    pub flags: TorrentFlags,
    /// `ti` — resolved torrent metadata.
    pub ti: Option<Arc<TorrentInfo>>,
    /// `storage_mode`.
    pub storage_mode: StorageMode,
    /// `save_path`.
    pub save_path: Vec<u8>,
    /// `part_file_dir`.
    pub part_file_dir: Vec<u8>,
    /// `root_certificate` (SSL torrents).
    pub root_certificate: Vec<u8>,
    /// `renamed_files` — `mapped_files`.
    pub renamed_files: BTreeMap<i32, Vec<u8>>,
    /// `have_pieces`.
    pub have_pieces: Bitfield,
    /// `verified_pieces`.
    pub verified_pieces: Bitfield,
    /// `unfinished_pieces` — piece index → blocks-have bitmask.
    pub unfinished_pieces: BTreeMap<i32, Bitfield>,
    /// `merkle_trees` — per-file flat node arrays (v2).
    pub merkle_trees: Vec<Vec<Sha256Hash>>,
    /// `verified_leaf_hashes`.
    pub verified_leaf_hashes: Vec<Bitfield>,
    /// `merkle_tree_mask`.
    pub merkle_tree_mask: Vec<Bitfield>,
    /// `total_uploaded`.
    pub total_uploaded: i64,
    /// `total_downloaded`.
    pub total_downloaded: i64,
    /// `active_time` (seconds).
    pub active_time: i64,
    /// `finished_time` (seconds).
    pub finished_time: i64,
    /// `seeding_time` (seconds).
    pub seeding_time: i64,
    /// `last_seen_complete` (unix).
    pub last_seen_complete: i64,
    /// `last_download` (unix).
    pub last_download: i64,
    /// `last_upload` (unix).
    pub last_upload: i64,
    /// `added_time` (unix).
    pub added_time: i64,
    /// `completed_time` (unix).
    pub completed_time: i64,
    /// `creation_date` (unix).
    pub creation_date: i64,
    /// `created_by`.
    pub created_by: Vec<u8>,
    /// `comment`.
    pub comment: Vec<u8>,
    /// `num_complete` — scrape cache, `-1` unknown.
    pub num_complete: i64,
    /// `num_incomplete`.
    pub num_incomplete: i64,
    /// `num_downloaded`.
    pub num_downloaded: i64,
    /// `max_uploads`, `-1` = session default.
    pub max_uploads: i64,
    /// `max_connections`.
    pub max_connections: i64,
    /// `upload_limit` (bytes/s).
    pub upload_limit: i64,
    /// `download_limit` (bytes/s).
    pub download_limit: i64,
}

impl Default for AddTorrentParams {
    fn default() -> Self {
        Self {
            info_hashes: InfoHash::default(),
            name: Vec::new(),
            trackers: Vec::new(),
            tracker_tiers: Vec::new(),
            url_seeds: Vec::new(),
            http_seeds: Vec::new(),
            dht_nodes: Vec::new(),
            peers: Vec::new(),
            banned_peers: Vec::new(),
            file_priorities: Vec::new(),
            piece_priorities: Vec::new(),
            flags: TorrentFlags::DEFAULT_FLAGS,
            ti: None,
            storage_mode: StorageMode::Sparse,
            save_path: Vec::new(),
            part_file_dir: Vec::new(),
            root_certificate: Vec::new(),
            renamed_files: BTreeMap::new(),
            have_pieces: Bitfield::new(),
            verified_pieces: Bitfield::new(),
            unfinished_pieces: BTreeMap::new(),
            merkle_trees: Vec::new(),
            verified_leaf_hashes: Vec::new(),
            merkle_tree_mask: Vec::new(),
            total_uploaded: 0,
            total_downloaded: 0,
            active_time: 0,
            finished_time: 0,
            seeding_time: 0,
            last_seen_complete: 0,
            last_download: 0,
            last_upload: 0,
            added_time: 0,
            completed_time: 0,
            creation_date: 0,
            created_by: Vec::new(),
            comment: Vec::new(),
            num_complete: -1,
            num_incomplete: -1,
            num_downloaded: -1,
            max_uploads: -1,
            max_connections: -1,
            upload_limit: -1,
            download_limit: -1,
        }
    }
}
