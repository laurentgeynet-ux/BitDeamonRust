// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `torrent_status.hpp` + `torrent_status.cpp` — the
//! per-swarm status snapshot returned by `torrent_handle::status`
//! and broadcast by `state_update_alert`.

use bitdaemon_common::{Bitfield, Sha1Hash};

/// `status_flags_t` — selects which expensive fields
/// `torrent::status` populates (`torrent_handle::status(flags)`,
/// `post_status`, `post_torrent_updates`, `get_torrent_status`,
/// `refresh_torrent_status` all take one). Bit positions match
/// upstream exactly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatusFlags(pub u32);

impl StatusFlags {
    /// `query_distributed_copies` (0_bit) — `distributed_*`.
    pub const QUERY_DISTRIBUTED_COPIES: Self = Self(1 << 0);
    /// `query_accurate_download_counters` (1_bit) — include
    /// finished/writing blocks of in-flight pieces in
    /// `total_done`/`total_wanted_done`.
    pub const QUERY_ACCURATE_DOWNLOAD_COUNTERS: Self = Self(1 << 1);
    /// `query_last_seen_complete` (2_bit) — upstream deprecated the
    /// gate (`last_seen_complete` fills unconditionally); kept for
    /// flag-bit parity.
    pub const QUERY_LAST_SEEN_COMPLETE: Self = Self(1 << 2);
    /// `query_pieces` (3_bit) — `pieces` bitfield.
    pub const QUERY_PIECES: Self = Self(1 << 3);
    /// `query_verified_pieces` (4_bit) — `verified_pieces`.
    pub const QUERY_VERIFIED_PIECES: Self = Self(1 << 4);
    /// `query_torrent_file` (5_bit) — `torrent_file`.
    pub const QUERY_TORRENT_FILE: Self = Self(1 << 5);
    /// `query_name` (6_bit) — `name`.
    pub const QUERY_NAME: Self = Self(1 << 6);
    /// `query_save_path` (7_bit) — `save_path`.
    pub const QUERY_SAVE_PATH: Self = Self(1 << 7);
    /// `query_renamed_files` (8_bit) — `renamed_files`.
    pub const QUERY_RENAMED_FILES: Self = Self(1 << 8);
    /// every field — convenience for callers that want it all.
    pub const ALL: Self = Self(0x1ff);

    /// `flags & other`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    /// `flags | other`.
    #[must_use]
    pub const fn or(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// `torrent_status::error_file_none` — the error was not
/// file-scoped.
pub const ERROR_FILE_NONE: i32 = -1;
/// `error_file_ssl_ctx` — the SSL context failed to build.
pub const ERROR_FILE_SSL_CTX: i32 = -3;
/// `error_file_metadata` — the error came from loading metadata.
pub const ERROR_FILE_METADATA: i32 = -4;
/// `error_file_exception` — a serious non-file error was reported.
pub const ERROR_FILE_EXCEPTION: i32 = -5;
/// `error_file_partfile` — the error touched the part file.
pub const ERROR_FILE_PARTFILE: i32 = -6;

/// `torrent_status::state_t` — the coarse lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TorrentState {
    /// `checking_resume_data` — verifying fastresume on disk.
    CheckingResumeData,
    /// `checking_files` — full piece re-check in flight.
    CheckingFiles,
    /// `queued_for_checking`.
    QueuedForChecking,
    /// `downloading_metadata` — magnet, info dict pending.
    DownloadingMetadata,
    /// `downloading`.
    Downloading,
    /// `finished` — payload done, still uploading.
    Finished,
    /// `seeding` — done and was already complete at add/check.
    Seeding,
}

impl TorrentState {
    /// upstream `torrent_status::state_t` name (as printed by
    /// `state_str` / serialized in `state_changed_alert` messages).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CheckingResumeData => "checking_resume_data",
            Self::CheckingFiles => "checking_files",
            Self::QueuedForChecking => "queued_for_checking",
            Self::DownloadingMetadata => "downloading_metadata",
            Self::Downloading => "downloading",
            Self::Finished => "finished",
            Self::Seeding => "seeding",
        }
    }
}

/// `torrent_status` — an immutable snapshot of one torrent.
#[derive(Clone, Debug)]
pub struct TorrentStatus {
    /// `handle` — identifies the swarm.
    pub info_hash: Sha1Hash,
    /// `info_hashes` — v1 + v2 (v2 zero for v1-only torrents).
    pub info_hashes: bitdaemon_common::info_hash::InfoHash,
    /// `state`.
    pub state: TorrentState,
    /// `paused`.
    pub paused: bool,
    /// `auto_managed`.
    pub auto_managed: bool,
    /// `is_seed`.
    pub is_seed: bool,
    /// `is_seeding` — finished *and* seeding state.
    pub is_seeding: bool,
    /// `is_finished` — payload complete (≠ seed flag).
    pub finished: bool,
    /// `current_tracker` — the tracker we're announcing to (or last
    /// contacted), "" if none.
    pub current_tracker: String,
    /// `has_metadata` — the info dict is known (magnet resolved or
    /// regular torrent).
    pub has_metadata: bool,
    /// `has_incoming` — at least one *inbound* connection is up.
    pub has_incoming: bool,
    /// `moving_storage` — a `move_storage` job is in flight.
    pub moving_storage: bool,
    /// `announcing_to_trackers` — an announce request is outstanding.
    pub announcing_to_trackers: bool,
    /// `announcing_to_lsd`.
    pub announcing_to_lsd: bool,
    /// `announcing_to_dht`.
    pub announcing_to_dht: bool,
    /// `need_save_resume` — resume data is dirty.
    pub need_save_resume: bool,
    /// `ip_filter_applies` — the session IP filter applies to this
    /// torrent's swarm.
    pub ip_filter_applies: bool,
    /// `flags` — the `torrent_flags_t` snapshot upstream fills with
    /// `this->flags()` (paused, auto_managed, sequential_download,
    /// seed_mode, upload_mode, share_mode, super_seeding,
    /// stop_when_ready, apply_ip_filter…).
    pub flags: bitdaemon_common::flags::TorrentFlags,
    /// `sequential_download` — `m_sequential_download`.
    pub sequential_download: bool,
    /// `seed_mode` — `m_seed_mode` (resume promised all pieces).
    pub seed_mode: bool,
    /// `upload_mode` — `m_upload_mode`.
    pub upload_mode: bool,
    /// `share_mode` — `m_share_mode`.
    pub share_mode: bool,
    /// `super_seeding` — `m_super_seeding`.
    pub super_seeding: bool,
    /// `stop_when_ready` — auto-pause once the disk check lands.
    pub stop_when_ready: bool,
    /// `storage_mode` — `m_storage_mode` (allocate/sparse).
    pub storage_mode: bitdaemon_core::StorageMode,
    /// `name` — torrent display name (`query_name`-gated upstream).
    pub name: String,
    /// `save_path` (`query_save_path`-gated).
    pub save_path: String,
    /// `error` — last tracker/disk error string ("" = none).
    pub error: String,
    /// `error_file` — index of the file the error touched, or a
    /// negative sentinel (`ERROR_FILE_NONE`/`_SSL_CTX`/`_METADATA`/
    /// `_EXCEPTION`/`_PARTFILE`).
    pub error_file: i32,
    /// `num_peers` — connected peers.
    pub num_peers: usize,
    /// `num_seeds` — connected seeds (0 when unknown; upstream
    /// only counts connected peers that are seeds).
    pub num_seeds: usize,
    /// `num_candidates` — `peer_list` swarm size.
    pub num_candidates: usize,
    /// `num_complete` — best tracker scrape `complete` (-1 = none).
    pub num_complete: i64,
    /// `num_incomplete` — best tracker scrape `incomplete`.
    pub num_incomplete: i64,
    /// `downloaded` — best tracker scrape `downloaded`.
    pub downloaded: i64,
    /// `total_done` — bytes verified.
    pub total_done: i64,
    /// `total_wanted` — bytes of wanted payload.
    pub total_wanted: i64,
    /// `total_upload` — lifetime payload bytes sent.
    pub total_upload: i64,
    /// `total_download` — lifetime payload bytes received.
    pub total_download: i64,
    /// `total_payload_download` — payload bytes received this session.
    pub total_payload_download: i64,
    /// `total_payload_upload` — payload bytes sent this session.
    pub total_payload_upload: i64,
    /// `total_failed_bytes` — bytes downloaded then rejected by hash.
    pub total_failed_bytes: i64,
    /// `total_redundant_bytes` — bytes received for data already had.
    pub total_redundant_bytes: i64,
    /// `total_wanted_done` — verified bytes among wanted pieces.
    pub total_wanted_done: i64,
    /// `all_time_upload` — `total_upload` plus pre-add resume counter.
    pub all_time_upload: i64,
    /// `all_time_download` — `total_download` plus resume counter.
    pub all_time_download: i64,
    /// `last_upload` — epoch seconds of last payload sent (0 = never).
    pub last_upload: i64,
    /// `last_download` — epoch seconds of last payload received.
    pub last_download: i64,
    /// `active_duration` — seconds since the torrent was started.
    pub active_duration: i64,
    /// `finished_duration` — seconds since payload completion.
    pub finished_duration: i64,
    /// `seeding_duration` — seconds spent in seeding state.
    pub seeding_duration: i64,
    /// `download_payload_rate` — B/s, sampled at each tick.
    pub download_rate: i64,
    /// `upload_payload_rate` — B/s.
    pub upload_rate: i64,
    /// `download_payload_rate` — same value; our rates only count
    /// payload bytes.
    pub download_payload_rate: i64,
    /// `upload_payload_rate`.
    pub upload_payload_rate: i64,
    /// `list_seeds` — seeds known in the swarm list.
    pub list_seeds: i32,
    /// `list_peers` — non-seed peers known in the swarm list.
    pub list_peers: i32,
    /// `connect_candidates` — peers we may want to connect to.
    pub connect_candidates: i32,
    /// `num_uploads` — peers currently unchoked by us.
    pub num_uploads: i32,
    /// `num_connections` — open peer connections.
    pub num_connections: i32,
    /// `uploads_limit` — `max_uploads` (-1 = session default).
    pub uploads_limit: i32,
    /// `connections_limit` — `max_connections`.
    pub connections_limit: i32,
    /// `upload_limit` — B/s cap (-1 = unlimited).
    pub upload_limit: i32,
    /// `download_limit`.
    pub download_limit: i32,
    /// `up_bandwidth_queue` — peers waiting on the upload queue.
    pub up_bandwidth_queue: i32,
    /// `down_bandwidth_queue` — peers waiting on the download queue.
    pub down_bandwidth_queue: i32,
    /// `seed_rank` — auto-manage seed priority score (upstream formula
    /// verbatim, flags in high bits).
    pub seed_rank: i32,
    /// `queue_position` — 0-based position in the download queue
    /// (-1 = not queued).
    pub queue_position: i32,
    /// `distributed_full_copies` — integer part of availability.
    pub distributed_full_copies: i32,
    /// `distributed_fraction` — per-mille fraction of the rarest piece.
    pub distributed_fraction: i32,
    /// `progress` 0..1.
    pub progress: f64,
    /// `progress_ppm` — `progress * 1e6`.
    pub progress_ppm: i32,
    /// `pieces` — our have-bitfield.
    pub pieces: Bitfield,
    /// `num_pieces`.
    pub num_pieces: i32,
    /// `piece_length`.
    pub piece_length: i64,
    /// `block_size` — 16 KiB request unit.
    pub block_size: i32,
    /// `distributed_copies` — `min + fraction` availability of
    /// the rarest piece (upstream float).
    pub distributed_copies: f64,
    /// `added_time` — seconds since epoch.
    pub added_time: i64,
    /// `last_scrape` — seconds since the best scrape (0 = never).
    pub last_scrape: i64,
    /// `next_announce` — epoch seconds of the earliest tracker retry.
    pub next_announce: i64,
    /// `completed_time` — epoch seconds when the download finished
    /// (0 = unfinished).
    pub completed_time: i64,
    /// `last_seen_complete` — `m_swarm_last_seen_complete`: the most
    /// recent moment *any* source reported a complete copy — our own
    /// `m_last_seen_complete` (seed-peer payload, resume data) maxed
    /// over conns' BEP-21 `complete_ago` reports (-1 = never).
    pub last_seen_complete: i64,
    /// `total` — `calc_bytes(all_pieces)`: the full payload size
    /// (upstream field; differs from `total_wanted` when some
    /// pieces/files are `dont_download`).
    pub total: i64,
    /// `time_since_upload` — seconds since last payload sent
    /// (-1 = never). Upstream `st->time_since_upload`.
    pub time_since_upload: i64,
    /// `time_since_download` — seconds since last payload received.
    pub time_since_download: i64,
    /// `active_time` — deprecated duplicate of `active_duration`
    /// (upstream still fills both).
    pub active_time: i64,
    /// `finished_time` — duplicate of `finished_duration`.
    pub finished_time: i64,
    /// `seeding_time` — duplicate of `seeding_duration`.
    pub seeding_time: i64,
    /// `torrent_file` — the metainfo (`query_torrent_file`-gated;
    /// `None` for magnets until the info dict lands).
    pub torrent_file: Option<std::sync::Arc<bitdaemon_core::TorrentInfo>>,
    /// `verified_pieces` — `m_verified` bitfield: pieces confirmed
    /// on disk while in `seed_mode` (`query_verified_pieces`-gated;
    /// empty outside seed mode).
    pub verified_pieces: Bitfield,
    /// `renamed_files` — `file_index → renamed path`
    /// (`query_renamed_files`-gated).
    pub renamed_files: std::collections::BTreeMap<i32, String>,
}
