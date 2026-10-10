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
    /// `download_payload_rate` — B/s, sampled at each tick.
    pub download_rate: i64,
    /// `upload_payload_rate` — B/s.
    pub upload_rate: i64,
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
