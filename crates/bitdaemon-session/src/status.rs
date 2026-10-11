// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `torrent_status.hpp` + `torrent_status.cpp` — the
//! per-swarm status snapshot returned by `torrent_handle::status`
//! and broadcast by `state_update_alert`.

use bitdaemon_common::{Bitfield, Sha1Hash};

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

/// `torrent_status` — an immutable snapshot of one torrent.
#[derive(Clone, Debug)]
pub struct TorrentStatus {
    /// `handle` — identifies the swarm.
    pub info_hash: Sha1Hash,
    /// `state`.
    pub state: TorrentState,
    /// `paused`.
    pub paused: bool,
    /// `auto_managed`.
    pub auto_managed: bool,
    /// `is_seed`.
    pub is_seed: bool,
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
    /// `name` — torrent display name.
    pub name: String,
    /// `save_path`.
    pub save_path: String,
    /// `error` — last tracker/disk error string ("" = none).
    pub error: String,
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
}
