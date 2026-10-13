// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `torrent` — per-torrent state machine. Owns the piece picker,
//! the peer set (`PeerConn`s keyed by an opaque id), the have-set,
//! and the disk-storage index. Wire and disk stay outside: the
//! session feeds decoded messages and drains `TorrentEffect`s
//! (send, write, read, announce, alerts).
//!
//! Port of `torrent.cpp`'s core: `peer_has`/`peer_lost` refcount
//! glue, `add_piece`/`piece_finished` verification flow, seeder
//! transitions — minus upstream's io_service entanglement.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bitdaemon_common::types::PieceBlock;
use bitdaemon_common::types::{FileIndex, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, PeerRequest, Sha1Hash};

use bitdaemon_bencode::bdecode_checked;
use bitdaemon_core::add_torrent_params::AddTorrentParams;
use bitdaemon_core::torrent_info::{LoadTorrentLimits, TorrentInfo};
use bitdaemon_net::peer_wire::PeerMessage;

use crate::peer_conn::{PeerConn, PeerEffect};
use crate::piece_picker::{BlockState, PickerOptions, PiecePicker};
use crate::smart_ban::{BanDecision, SmartBan};
use crate::ut_metadata::{MetadataResult, UtMetadataTracker};

/// Side-effect the session must perform — the disk/tracker/wire
/// calls upstream makes directly on `session_impl`.
#[derive(Debug)]
pub enum TorrentEffect {
    /// send `msg` to peer `peer`.
    SendTo { peer: u64, msg: PeerMessage },
    /// issue a read job serving a peer's request.
    DiskRead { peer: u64, req: PeerRequest },
    /// `dht_tracker::add_node` — a peer announced its DHT port;
    /// ping the node into the routing table.
    DhtNode(std::net::SocketAddr),
    /// write a downloaded block. `peer` is the connection that sent
    /// it (upstream `incoming_piece`'s `this`) — drives the per-peer
    /// `bw_disk` read gate, not just completion routing.
    DiskWrite {
        peer: u64,
        req: PeerRequest,
        data: Vec<u8>,
    },
    /// verify a piece's hash (`verify_piece` — resume restore and
    /// post-write verification).
    DiskHash { piece: PieceIndex },
    /// `read_piece` — read the whole piece back from disk and
    /// post `read_piece_alert` (`alert_when_available` path).
    ReadPiece { piece: PieceIndex },
    /// failed read-piece request (`alert_when_available` on an
    /// invalid/dead torrent).
    ReadPieceFailed { piece: PieceIndex, error: String },
    /// piece passed verification → `have` broadcast pending.
    PiecePassed(PieceIndex),
    /// piece failed verification → re-download.
    PieceFailed(PieceIndex),
    /// file finished — alert upstream `file_completed`.
    FileCompleted(FileIndex),
    /// the torrent just became a seed.
    BecameSeed,
    /// disconnect a misbehaving peer.
    DisconnectPeer { peer: u64, reason: &'static str },
    /// `yourip` seen in a BEP-10 handshake — routed to the
    /// session's `ip_voter` (`source_peer`).
    YourIp(std::net::IpAddr),
    /// magnet `ut_metadata` assembled + hash-verified →
    /// `metadata_received_alert`.
    MetadataReceived,
    /// assembled metadata failed the info-hash check →
    /// `metadata_failed_alert`.
    MetadataFailed,
    /// post an alert on the session bus (`torrent`-level alerts
    /// carry this torrent's `ih`).
    Alert(crate::alerts::Alert),
}

/// `torrent` — one swarm.
pub struct Torrent {
    /// `m_torrent_file->info_hash()`.
    pub info_hash: Sha1Hash,
    /// `num_pieces`.
    pub num_pieces: i32,
    /// `piece_length`.
    pub piece_length: i64,
    /// `m_have_piece` — our verified bitfield.
    pub have: Bitfield,
    /// `m_torrent_file` piece hashes — `hash_for_piece` table
    /// (v1). `None` until metadata arrives (`ut_metadata`).
    pub piece_hashes: Option<Vec<Sha1Hash>>,
    /// `m_picker` — `None` until `init_picker` (no metadata yet).
    pub picker: Option<PiecePicker>,
    /// `m_connections` — peers keyed by caller-chosen id.
    pub peers: BTreeMap<u64, PeerConn>,
    /// `m_trackers` — tier-ordered announce entries.
    pub trackers: crate::tracker_list::TrackerList,
    /// `m_hash_requests` — (file, chunk-index) → last-request time;
    /// a fresh `HashPicker` is built per call (it derives
    /// `piece_hash_requested.have` from the trees) so the only state
    /// worth keeping is the send throttle.
    pub hash_requests_sent: std::collections::HashMap<(i32, i32), std::time::Instant>,
    /// `m_merkle_trees` — per-file v2 merkle trees (empty for v1).
    /// `HashPicker` borrows it per verification call.
    pub merkle_trees: Vec<bitdaemon_core::merkle_tree::MerkleTree>,
    /// `m_torrent_file` upstream — the parsed metainfo once we have
    /// it (None for unresolved magnets).
    pub ti: Option<std::sync::Arc<TorrentInfo>>,
    /// `m_web_seeds` — BEP-17/19 url-seed endpoints.
    pub web_seeds: Vec<String>,
    /// `m_torrent_file->files()` — file layout for web-seed
    /// range mapping.
    pub files: Option<std::sync::Arc<bitdaemon_core::FileStorage>>,
    /// round-robin cursor over `web_seeds`.
    pub web_seed_idx: usize,
    /// blocks in flight toward web seeds (`m_requests` of the
    /// virtual web peer).
    pub web_seed_outstanding: std::collections::BTreeSet<bitdaemon_common::types::PieceBlock>,
    /// the virtual web peer's bitfield was refcounted once
    /// (`web_peer_connection` bumps availability for every piece
    /// on connect — the seed serves the full payload).
    web_seed_refcounted: bool,
    /// `m_peer_list` — known swarm members (tracker/PEX/DHT/LSD/
    /// resume candidates). Upstream stores `torrent_peer`s owned by
    /// the list; ours are `TorrentPeer`s in `PeerList`.
    pub peer_list: bitdaemon_net::peer_list::PeerList,
    /// `m_storage` — disk job routing index.
    pub storage: StorageIndex,
    /// `m_seeds` / peers count for `state()`.
    pub num_seeds: usize,
    /// `m_paused`.
    pub paused: bool,
    /// `m_finished` — all pieces verified.
    pub finished: bool,
    /// `m_seeding` — was complete from the start or passed check.
    pub seeding: bool,
    /// bytes verified total (`m_total_downloaded`-ish).
    pub verified_bytes: i64,
    /// `m_queued_time_critical` aggregate — peers in endgame.
    pub want_peers: bool,
    /// `m_resume_data` fields the picker needs at init.
    pub piece_priority_dirty: bool,
    /// `ut_metadata` download state — magnet-added torrents only.
    pub metadata_tracker: UtMetadataTracker,
    /// raw info dict (the bytes `ut_metadata` serves). Set by
    /// `set_metadata`/`from_params`.
    pub metadata_raw: Option<Vec<u8>>,
    /// our local extension id for `ut_metadata` (the id WE put in
    /// our handshake; peers address it back).
    pub local_ut_metadata_id: u8,
    /// `m_magnet_link` — added without metadata.
    pub magnet: bool,
    /// `m_private` (BEP-27) — disables LSD/PEX/DHT peer discovery.
    pub private: bool,
    /// `smart_ban` plugin state (`extensions/smart_ban.cpp`).
    pub smart_ban: SmartBan,
    /// `m_auto_managed` — queue promotion/demotion is the
    /// session's job (`active_downloads`/`active_seeds`).
    pub auto_managed: bool,
    /// `is_torrent_queued` — held back by the auto-managed queue
    /// (distinct from user `paused`, like upstream
    /// `graceful_pause` vs the queue state).
    pub queued: bool,
    /// last `ut_pex` broadcast (`m_last_pex` timing state).
    pub last_pex: Option<Instant>,
    /// `m_added_time`.
    pub added_time: std::time::SystemTime,
    /// `m_save_path`.
    pub save_path: String,
    /// `m_storage_mode` — `atp.storage_mode` kept so deferred storage
    /// creation (magnets) honors the caller's Sparse/Allocate choice.
    pub storage_mode: bitdaemon_core::StorageMode,
    /// `atp.part_file_dir` — `.parts` override dir ("" = under
    /// `save_path`), kept for deferred storage creation.
    pub part_file_dir: String,
    /// display name (`ti->name()` or `atp.name`).
    pub name: String,
    /// a `check_files` job is in flight (`m_checking_piece` coarse).
    pub checking: bool,
    /// payload rate samples for `torrent_status`
    /// (`stat().download_payload_rate()` — sampled per tick).
    pub down_rate: i64,
    /// upload payload B/s.
    pub up_rate: i64,
    /// last sample instant.
    pub stat_sample: Option<std::time::Instant>,
    /// `m_file_priority` — per-file `DownloadPriority` (empty =
    /// all default). Drives `piece_priority` upstream-style.
    pub file_priorities: Vec<bitdaemon_common::types::DownloadPriority>,
    /// `m_renamed_files` — user renames (`filename_mappings`):
    /// `file_index → new name`.
    pub renamed_files: std::collections::BTreeMap<i32, String>,
    /// `client_data_t` — opaque user data (`torrent_handle::userdata`).
    pub userdata: u64,
    /// `m_last_seen_complete` — last time a tracker reported a
    /// positive `complete` count (epoch seconds, -1 = never).
    pub last_seen_complete: i64,
    /// `m_sequential_download` — sequential pick order (streaming).
    pub sequential: bool,
    /// pieces left in an in-flight `force_recheck` pass
    /// (`checking` clears when it reaches 0).
    pub checking_pieces: i32,
    /// `m_error` — last tracker/disk error string for
    /// `torrent_status::error` ("" = no error).
    pub last_error: String,
    /// `m_channel` per-torrent rate limits — `upload_limit`/
    /// `download_limit` bytes/s, 0 = unlimited (upstream
    /// `torrent::set_upload_limit` → `peer_class` channel).
    pub upload_limit: i64,
    /// download bytes/s limit.
    pub download_limit: i64,
    /// `m_need_save_resume_data` — set by state changes
    /// (verified piece, priorities, metadata, check verdict);
    /// cleared when a `save_resume_data` dump is produced.
    pub need_save_resume: bool,
    /// last resume dump instant (`auto_save_resume` cadence).
    pub last_saved_resume: Option<Instant>,
    /// `m_sequence_number` — auto-managed queue order (assigned
    /// by the session at `add_torrent`, rewritten by
    /// `queue_position_set`).
    pub queue_seq: i64,
    /// optimistic-unchoke round-robin cursor (upstream rotates one
    /// choked interested peer every `optimistic_unchoke_interval`).
    pub optimistic_cursor: usize,
    /// `m_graceful_pause` — pausing without cancelling in-flight
    /// writes; requests stop immediately (`pause_flags_t::
    /// graceful_pause` upstream).
    pub graceful_pause: bool,
    /// `m_super_seeding` (flag) — initial-seed: leak one piece at
    /// a time per peer (`super_seeding()` upstream).
    pub super_seeding: bool,
    /// `upload_mode` (flag) — never request pieces, upload only.
    pub upload_mode: bool,
    /// `share_mode` (flag) — ratio-keeping download throttle.
    pub share_mode: bool,
    /// `m_seed_mode` — resume data promised every piece is present;
    /// while set, `is_seed()` is true and verification is lazy
    /// (`m_verified`/`m_verifying` track slow background checks).
    pub seed_mode: bool,
    /// `m_verified` — pieces confirmed while in seed_mode
    /// (lazy re-check bookkeeping; cleared on leave).
    pub seed_verified: Bitfield,
    /// `m_verifying` — pieces currently being hashed in seed_mode.
    pub seed_verifying: Bitfield,
    /// `m_num_verified`.
    pub num_verified: i32,
    /// `max_connections` per-torrent (-1 = use session limit).
    pub max_connections: i32,
    /// `max_uploads` per-torrent (-1 = use session limit).
    pub max_uploads: i32,
    /// `m_time_critical_pieces` — `set_piece_deadline` entries:
    /// piece → (deadline, alert_when_available). Sorted list
    /// upstream; BTreeMap + deadline ordering when consumed.
    pub time_critical: std::collections::BTreeMap<PieceIndex, (std::time::Instant, bool)>,
    /// monotonic ms since the torrent's first `update_rates` —
    /// the clock every peer pipeline timestamp is measured against.
    pub now_ms: i64,
    /// previous `update_rates` instant (advances `now_ms`).
    pub last_rates_instant: Option<Instant>,
    /// session-set: disk write queue above `max_queued_disk_bytes` —
    /// peers stop requesting until it drains below the low watermark
    /// (upstream's `disk_observer` wait; bounded write buffers).
    pub disk_back_pressure: bool,
    /// `m_pinned` — `set_pinned`: pinned torrents are never
    /// auto-managed out of the active set.
    pub pinned: bool,
    /// `m_stop_when_ready` — `stop_when_ready`: pause the torrent as
    /// soon as the download completes.
    pub stop_when_ready: bool,
    /// `m_ratio` — `set_ratio` upload/download ratio target (0 = none).
    pub ratio: f32,
    /// `m_tracker_login` — `set_tracker_login` user/pass.
    pub tracker_login: Option<(String, String)>,
    /// `m_ssl_certificate` — `set_ssl_certificate` (SSL torrents):
    /// PEM cert chain + private key (+ unused DH params, kept for
    /// upstream signature parity; rustls is ECDHE-only).
    pub ssl_cert: Option<String>,
    /// `set_ssl_certificate` private key PEM.
    pub ssl_key: Option<String>,
    /// `m_net_interface` — `use_interface` bind override.
    pub net_interface: Option<String>,
    /// `m_apply_ip_filter` — `apply_ip_filter` flag.
    pub apply_ip_filter: bool,
    /// per-peer rate limits — `set_peer_upload_limit` /
    /// `set_peer_download_limit` (upstream stores them on the peer
    /// object; we keep them keyed by remote endpoint).
    pub peer_limits: std::collections::HashMap<std::net::SocketAddr, (i64, i64)>,
    /// `m_extensions` — instantiated `torrent_plugin`s (upstream
    /// stores plugin objects; the factories that produced them are
    /// invoked by `TorrentHandle::add_extension` →
    /// `add_extension_plugin`).
    pub extensions: Vec<std::sync::Arc<dyn crate::alerts::SessionPlugin>>,
    /// `m_files_checked` — storage check completed at least once;
    /// late-added extensions get `on_files_checked` immediately.
    pub files_checked: bool,
    /// `i2p_pex_plugin` per-torrent state (message cache + rebuild
    /// cadence). Always allocated like upstream's plugin list; the
    /// plugin itself no-ops unless the torrent is i2p.
    pub i2p_pex_state: crate::i2p_pex::I2pPexState,
    /// `m_total_failed` — bytes discarded after hash failure.
    pub failed_bytes: i64,
    /// `m_total_redundant_bytes`.
    pub redundant_bytes: i64,
    /// `m_total_uploaded` seed-side pre-add accumulator (resume).
    pub all_time_upload: i64,
    /// `m_total_downloaded` ditto.
    pub all_time_download: i64,
    /// last tick a payload byte was received (`st.last_download`).
    pub last_payload_down: Option<Instant>,
    /// last tick a payload byte was sent (`st.last_upload`).
    pub last_payload_up: Option<Instant>,
    /// `m_started` — when the torrent entered the running state.
    pub started: Instant,
    /// `m_finished_time` — first moment the payload completed.
    pub finished_at: Option<Instant>,
    /// `m_seeding_time` — when seeding began.
    pub seeding_since: Option<Instant>,
    /// `m_moving_storage` — `move_storage` in flight.
    pub moving_storage: bool,
    /// outstanding tracker announces (`is_announcing`).
    pub outstanding_announces: i32,
    /// DHT `announce_peer` lookup outstanding (`m_announce_to_dht`).
    pub dht_announcing: bool,
    /// LSD broadcast issued this interval (`m_announce_to_lsd`).
    pub lsd_announcing: bool,
    /// `m_abort` — set by `abort()` (torrent teardown in flight).
    pub aborted: bool,
    /// `m_num_connecting` — in-flight dials (connect cap accounting).
    pub num_connecting: i32,
    /// `m_upload_only` — all wanted pieces done, serve only.
    pub upload_only: bool,
    /// `m_announcing` — tracker announces enabled
    /// (`start_announcing`/`stop_announcing` gate `update_tracker_timer`).
    pub announcing: bool,
    /// `m_announce_to_dht` flag (distinct from `dht_announcing`,
    /// which tracks an *in-flight* announce).
    pub announce_dht: bool,
    /// `m_announce_to_lsd` flag.
    pub announce_lsd: bool,
    /// `m_announce_to_trackers` flag.
    pub announce_trackers: bool,
    /// `m_suggest_pieces` — BEP-6 suggestions from peers.
    pub suggest_pieces: std::collections::VecDeque<PieceIndex>,
    /// `m_predictive_pieces` — speculatively-claimed pieces
    /// (predictive piece announce mode).
    pub predictive: Bitfield,
    /// `m_progress_ppm` — last progress snapshot, parts per million.
    pub progress_ppm: i64,
}

/// `std::function<std::shared_ptr<torrent_plugin>(torrent_handle,
/// client_data_t)>` — a per-torrent plugin factory registered via
/// `torrent_handle::add_extension`.
pub type ExtensionFactory = std::sync::Arc<
    dyn Fn(
            &crate::session_handle::TorrentHandle,
        ) -> std::sync::Arc<dyn crate::alerts::SessionPlugin>
        + Send
        + Sync,
>;

/// `torrent::set_metadata` verdict — upstream distinguishes
/// `mismatching_info_hash` (retry) from `invalid_swarm_metadata`
/// (hash-correct but unparseable → `set_error` + `pause`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataVerdict {
    /// Metadata accepted.
    Ok,
    /// SHA-1 doesn't match the info-hash — retry from other peers.
    HashMismatch,
    /// Hash-correct but bdecode/`torrent_info` parse failed —
    /// upstream sets `invalid_swarm_metadata` and pauses.
    ParseError,
}

impl Torrent {
    /// `peer_connection::send_allowed_set` — BEP-6: when the peer
    /// negotiated the FAST extension, advertise a deterministic
    /// set of pieces it may request even while choked. Seed =
    /// SHA-1(remote_ip_bytes || info_hash), re-hashed per batch of
    /// 5 u32 draws — port verbatim.
    pub fn send_allowed_set(&mut self, peer_id: u64, out: &mut Vec<TorrentEffect>) {
        use sha1::{Digest, Sha1};
        const NUM_ALLOWED: usize = 5; // settings.allowed_fast_set_size
        if self.super_seeding
            || self.metadata_raw.is_none() && self.picker.is_none()
            || self.num_pieces <= 0
        {
            return;
        }
        let Some(c) = self.peers.get(&peer_id) else {
            return;
        };
        if !c.supports_fast || c.remote_upload_only {
            return;
        }
        let Some(remote) = c.remote else { return };
        // upstream: when the set covers all pieces, allow every
        // piece the peer lacks (skip hash determinism entirely).
        if NUM_ALLOWED >= self.num_pieces as usize {
            for i in 0..self.num_pieces {
                if !self.have.get_bit(i as usize) || c.has_piece(PieceIndex(i)) {
                    continue;
                }
                out.push(TorrentEffect::SendTo {
                    peer: peer_id,
                    msg: PeerMessage::AllowedFast(PieceIndex(i)),
                });
            }
            return;
        }
        let mut x = Vec::new();
        match remote.ip() {
            std::net::IpAddr::V4(a) => x.extend_from_slice(&a.octets()),
            std::net::IpAddr::V6(a) => x.extend_from_slice(&a.octets()),
        }
        x.extend_from_slice(self.info_hash.as_bytes());
        let mut hash: [u8; 20] = Sha1::digest(&x).into();
        let mut accept: Vec<PieceIndex> = Vec::with_capacity(10);
        let mut attempts = 0usize;
        let mut loops = 0usize;
        loop {
            for i in 0..(hash.len() / 4) {
                loops += 1;
                let Some(chunk) = hash
                    .get(i * 4..i * 4 + 4)
                    .and_then(|s| <[u8; 4]>::try_from(s).ok())
                else {
                    break;
                };
                let p = u32::from_be_bytes(chunk) % self.num_pieces as u32;
                let piece = PieceIndex(p as i32);
                if accept.contains(&piece) {
                    if loops > 500 {
                        return;
                    }
                    continue;
                }
                // pieces the peer already has are pointless.
                let has = self.peers.get(&peer_id).is_some_and(|c| c.has_piece(piece));
                if self.have.get_bit(piece.0 as usize) && !has {
                    out.push(TorrentEffect::SendTo {
                        peer: peer_id,
                        msg: PeerMessage::AllowedFast(piece),
                    });
                    accept.push(piece);
                }
                attempts += 1;
                if attempts >= NUM_ALLOWED {
                    return;
                }
            }
            hash = Sha1::digest(hash).into();
        }
    }

    /// `peer_connection::has_outstanding_block_requests` — true
    /// while any peer still has blocks in flight (used to lift
    /// `graceful_pause` once the last write lands).
    pub fn requests_outstanding(&self) -> bool {
        self.peers.values().any(|c| !c.outstanding().is_empty())
    }

    /// `torrent` ctor — `have` from resume data (`verified_pieces`),
    /// `None` when checking is still needed.
    #[must_use]
    pub fn new(
        info_hash: Sha1Hash,
        num_pieces: i32,
        piece_length: i64,
        have: Bitfield,
        storage: StorageIndex,
    ) -> Self {
        Self {
            info_hash,
            num_pieces,
            piece_length,
            piece_hashes: None,
            have,
            picker: None,
            peers: BTreeMap::new(),
            trackers: crate::tracker_list::TrackerList::default(),
            hash_requests_sent: std::collections::HashMap::new(),
            merkle_trees: Vec::new(),
            ti: None,
            web_seeds: Vec::new(),
            files: None,
            web_seed_idx: 0,
            web_seed_outstanding: std::collections::BTreeSet::new(),
            web_seed_refcounted: false,
            peer_list: bitdaemon_net::peer_list::PeerList::default(),
            storage,
            num_seeds: 0,
            paused: false,
            finished: false,
            seeding: false,
            verified_bytes: 0,
            want_peers: true,
            piece_priority_dirty: false,
            metadata_tracker: UtMetadataTracker::new(),
            metadata_raw: None,
            local_ut_metadata_id: 1,
            magnet: false,
            private: false,
            smart_ban: SmartBan::new(),
            auto_managed: true,
            last_pex: None,
            queued: false,
            added_time: std::time::SystemTime::now(),
            save_path: String::new(),
            storage_mode: bitdaemon_core::StorageMode::default(),
            part_file_dir: String::new(),
            name: String::new(),
            checking: false,
            down_rate: 0,
            up_rate: 0,
            stat_sample: None,
            file_priorities: Vec::new(),
            renamed_files: std::collections::BTreeMap::new(),
            userdata: 0,
            last_seen_complete: -1,
            sequential: false,
            checking_pieces: 0,
            last_error: String::new(),
            upload_limit: 0,
            download_limit: 0,
            need_save_resume: false,
            last_saved_resume: None,
            queue_seq: 0,
            time_critical: std::collections::BTreeMap::new(),
            optimistic_cursor: 0,
            now_ms: 0,
            last_rates_instant: None,
            disk_back_pressure: false,
            graceful_pause: false,
            super_seeding: false,
            upload_mode: false,
            share_mode: false,
            seed_mode: false,
            seed_verified: Bitfield::default(),
            seed_verifying: Bitfield::default(),
            num_verified: 0,
            max_connections: -1,
            max_uploads: -1,
            pinned: false,
            stop_when_ready: false,
            ratio: 0.0,
            tracker_login: None,
            ssl_cert: None,
            ssl_key: None,
            net_interface: None,
            apply_ip_filter: false,
            peer_limits: std::collections::HashMap::new(),
            extensions: Vec::new(),
            files_checked: false,
            i2p_pex_state: crate::i2p_pex::I2pPexState::default(),
            failed_bytes: 0,
            redundant_bytes: 0,
            all_time_upload: 0,
            all_time_download: 0,
            last_payload_down: None,
            last_payload_up: None,
            started: Instant::now(),
            finished_at: None,
            seeding_since: None,
            moving_storage: false,
            outstanding_announces: 0,
            dht_announcing: false,
            lsd_announcing: false,
            aborted: false,
            num_connecting: 0,
            upload_only: false,
            announcing: true,
            announce_dht: true,
            announce_lsd: true,
            announce_trackers: true,
            suggest_pieces: std::collections::VecDeque::new(),
            predictive: Bitfield::default(),
            progress_ppm: 0,
        }
    }

    /// `torrent` ctor path — build a torrent from `AddTorrentParams`:
    /// `ti` metadata → sizes/hashes/picker, `have`/`verified` and
    /// `unfinished_pieces` via `apply_resume_data`, `peers`/`banned_peers`
    /// into `peer_list`, `paused`/`magnet` flags. `None` when `ti` is
    /// absent and `info_hash` can't be derived (upstream requires the
    /// info-hash).
    #[must_use]
    pub fn from_params(
        atp: &AddTorrentParams,
        storage: StorageIndex,
        out: &mut Vec<TorrentEffect>,
    ) -> Option<Self> {
        // v1 swarm id when present; v2-only torrents key the session
        // map with the truncated v2 hash (upstream `get(V2)`).
        let ih = if atp.info_hashes.has_v1() {
            atp.info_hashes.v1
        } else if atp.info_hashes.has_v2() {
            atp.info_hashes
                .get(bitdaemon_common::info_hash::ProtocolVersion::V2)
        } else {
            atp.ti.as_ref().and_then(|ti| ti.info_hash_v1())?
        };
        let mut t = match &atp.ti {
            Some(ti) => {
                let mut t = Self::new(
                    ih,
                    ti.num_pieces(),
                    i64::from(ti.piece_length()),
                    Bitfield::with_size(ti.num_pieces() as usize),
                    storage,
                );
                t.piece_hashes = Some(
                    (0..ti.num_pieces())
                        .map(|i| ti.hash_for_piece(PieceIndex(i)))
                        .collect(),
                );
                t.ti = Some(ti.clone());
                if ti.v2() {
                    t.build_merkle_trees(ti);
                }
                t.metadata_raw = Some(ti.info_section().to_vec());
                t.files = Some(std::sync::Arc::new(ti.files().clone()));
                t.init_picker(false);
                // upstream `check_resume` order: file priorities
                // first, explicit `piece_priorities` override after
                if !atp.file_priorities.is_empty() {
                    t.prioritize_files(&atp.file_priorities);
                }
                t.apply_resume_data(atp, out);
                t
            }
            None => {
                // magnet add — metadata pending
                let mut t = Self::new(ih, 0, 0, Bitfield::default(), storage);
                t.magnet = true;
                t
            }
        };
        t.web_seeds = atp
            .url_seeds
            .iter()
            .map(|u| String::from_utf8_lossy(u).into_owned())
            .collect();
        // upstream `torrent_info` adds `atp.http_seeds` to
        // `m_web_seeds` as deprecated-typed entries — BEP-17 fetching
        // was removed ("removed support for BEP 17 web seeds (in
        // favor of BEP 19)"), they ride the same BEP-19 path. The
        // `httpseeds` resume key is write-only upstream
        // (`read_resume_data` never reads it back), so this is the
        // only place `atp.http_seeds` matters.
        t.web_seeds.extend(
            atp.http_seeds
                .iter()
                .map(|u| String::from_utf8_lossy(u).into_owned()),
        );
        for &ep in &atp.peers {
            t.add_swarm_peer(
                ep,
                bitdaemon_common::flags::PeerSourceFlags::RESUME_DATA,
                true,
            );
        }
        for &ep in &atp.banned_peers {
            let (i, _) = t.peer_list.add_peer(
                ep,
                bitdaemon_common::flags::PeerSourceFlags::RESUME_DATA,
                true,
            );
            t.peer_list.ban_peer(i);
        }
        t.paused = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::PAUSED);
        t.sequential = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::SEQUENTIAL_DOWNLOAD);
        t.upload_mode = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::UPLOAD_MODE);
        t.share_mode = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::SHARE_MODE);
        t.super_seeding = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::SUPER_SEEDING);
        // upstream `torrent::torrent`: seed_mode only sticks when the
        // resume data is consistent — no dont_download priorities and
        // every piece claimed present. Otherwise the flag is dropped
        // (upstream warns it "can be very confusing" to seed by
        // accident).
        let prios_ok = !atp
            .file_priorities
            .contains(&bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD)
            && !atp
                .piece_priorities
                .contains(&bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD)
            && atp.have_pieces.all_set();
        if atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::SEED_MODE)
            && prios_ok
            && atp.have_pieces.size() > 0
        {
            t.seed_mode = true;
            t.seed_verified = Bitfield::with_size(t.num_pieces.max(0) as usize);
            t.seed_verifying = Bitfield::with_size(t.num_pieces.max(0) as usize);
        }
        t.save_path = String::from_utf8_lossy(&atp.save_path).into_owned();
        t.storage_mode = atp.storage_mode;
        t.part_file_dir = String::from_utf8_lossy(&atp.part_file_dir).into_owned();
        if !atp.name.is_empty() {
            t.name = String::from_utf8_lossy(&atp.name).into_owned();
        } else if let Some(ti) = &atp.ti {
            t.name = String::from_utf8_lossy(ti.name()).into_owned();
        }
        // `torrent::torrent` seeds `m_trackers` from the params
        // (`announce-list` + `announce`, tiered); without this the
        // tracker list stayed empty and no announce ever fired.
        for (i, url) in atp.trackers.iter().enumerate() {
            let tier = atp.tracker_tiers.get(i).copied().unwrap_or(0);
            t.add_tracker(String::from_utf8_lossy(url).into_owned(), tier);
        }
        Some(t)
    }

    /// `set_metadata` — a magnet just resolved: SHA-1 of the raw
    /// info section must equal `info_hash` (BEP-9). On match the
    /// torrent_info is parsed and picker+hashes initialized.
    /// Returns `false` on hash mismatch (`failed_hash_check`
    /// upstream — caller penalizes sources via
    /// `metadata_tracker.reset_on_hash_failure`).
    pub fn set_metadata(&mut self, metadata: &[u8], cfg: &LoadTorrentLimits) -> bool {
        // effect-less callers (tests, handle APIs routed by the
        // session) get the verdict; the `SendTo` effects it produces
        // are discarded here
        let mut scratch = Vec::new();
        matches!(
            self.set_metadata_verdict(metadata, cfg, &mut scratch),
            MetadataVerdict::Ok
        )
    }

    /// `torrent::set_metadata` — typed verdict so callers can tell
    /// `mismatching_info_hash` (retry from other sources, upstream
    /// penalizes the serving peers) from a hash-valid but
    /// unparseable payload (upstream `set_error(invalid_swarm_metadata)`
    /// + `pause()`).
    pub fn set_metadata_verdict(
        &mut self,
        metadata: &[u8],
        cfg: &LoadTorrentLimits,
        out: &mut Vec<TorrentEffect>,
    ) -> MetadataVerdict {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(metadata);
        let actual = Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default();
        if actual != self.info_hash {
            return MetadataVerdict::HashMismatch;
        }
        let node = match bdecode_checked(metadata) {
            Ok(n) => n,
            Err(_) => return MetadataVerdict::ParseError,
        };
        let Ok(info) = TorrentInfo::from_info_node(&node, cfg) else {
            return MetadataVerdict::ParseError;
        };
        self.num_pieces = info.num_pieces();
        self.piece_length = i64::from(info.piece_length());
        if self.name.is_empty() {
            self.name = String::from_utf8_lossy(info.name()).into_owned();
        }
        self.have = Bitfield::with_size(info.num_pieces() as usize);
        self.piece_hashes = Some(
            (0..info.num_pieces())
                .map(|i| info.hash_for_piece(PieceIndex(i)))
                .collect(),
        );
        if info.v2() {
            self.build_merkle_trees(&info);
        }
        self.ti = Some(std::sync::Arc::new(info));
        self.metadata_raw = Some(metadata.to_vec());
        self.magnet = false;
        self.init_picker(true);
        self.need_save_resume = true;
        // peers that connected pre-metadata: their bitfields were
        // stored at whatever size arrived (`RefcountBulk` was a no-op
        // without a picker) — reconcile sizes and replay availability
        // into the fresh picker, then re-evaluate interest.
        let npieces = self.num_pieces.max(0) as usize;
        let mut peer_bits = Vec::new();
        for c in self.peers.values_mut() {
            if c.pre_metadata_all {
                let mut bf = Bitfield::with_size(npieces);
                bf.set_all();
                c.peer_pieces = bf.clone();
                c.pre_metadata_all = false;
            } else if c.peer_pieces.size() > npieces {
                c.peer_pieces.resize_with(npieces, false);
            }
            if !c.peer_pieces.none_set() {
                peer_bits.push(c.peer_pieces.clone());
            }
        }
        if let Some(p) = self.picker.as_mut() {
            for bits in &peer_bits {
                p.inc_refcount_bitfield(bits);
            }
        }
        // `torrent::init`/`start` — interested was never sent (our
        // `have` only materialized now): tell whoever holds pieces
        // we want, else the swarm keeps us choked forever.
        self.update_interest_all(out);
        MetadataVerdict::Ok
    }

    /// `on_metadata` — one `ut_metadata` `data` packet arrived.
    /// `peer` = the peer that served it (source bookkeeping).
    /// Returns the outcome; on `Complete` the caller feeds
    /// `metadata_tracker.metadata()` to [`Torrent::set_metadata`].
    pub fn on_ut_metadata_data(
        &mut self,
        buf: &[u8],
        piece: i32,
        total_size: i64,
        peer: u64,
        max_metadata_size: i64,
    ) -> MetadataResult {
        let r = self.metadata_tracker.received_metadata(
            buf,
            piece,
            total_size,
            max_metadata_size,
            self.piece_hashes.is_some(),
        );
        if matches!(r, MetadataResult::Stored | MetadataResult::Complete) {
            self.metadata_tracker.set_source(piece, peer);
        }
        r
    }

    /// `on_metadata` dispatch — a `ut_metadata` body arrived:
    /// `request` → serve our info dict (or reject); `data` → feed
    /// the tracker (complete → `set_metadata`, bad hash →
    /// `reset_on_hash_failure`); `reject` → ignored (the scheduler
    /// retries per `metadata_request` backoff).
    fn on_metadata_packet(&mut self, peer: u64, body: &[u8], out: &mut Vec<TorrentEffect>) {
        let Some((msg, piece, total, data)) = crate::ut_metadata::parse_metadata_packet(body)
        else {
            return;
        };
        match msg {
            crate::ut_metadata::MetadataMsg::Request => {
                let resp = match &self.metadata_raw {
                    Some(md) => crate::ut_metadata::metadata_packet(
                        crate::ut_metadata::MetadataMsg::Piece,
                        piece as i32,
                        true,
                        md,
                    ),
                    None => crate::ut_metadata::metadata_packet(
                        crate::ut_metadata::MetadataMsg::Reject,
                        piece as i32,
                        false,
                        &[],
                    ),
                };
                out.push(TorrentEffect::SendTo {
                    peer,
                    msg: PeerMessage::Extended {
                        id: self.local_ut_metadata_id,
                        data: resp.into(),
                    },
                });
            }
            crate::ut_metadata::MetadataMsg::Piece => {
                let r = self.on_ut_metadata_data(data, piece as i32, total, peer, 30 * 1024 * 1024);
                if matches!(r, MetadataResult::Complete) {
                    let md = self.metadata_tracker.metadata().to_vec();
                    // `metadata_received{,_failed}_alert` — the
                    // info-hash verdict (upstream posts in
                    // `torrent::on_metadata`/`set_metadata`). A
                    // hash-valid but unparseable payload sets
                    // `invalid_swarm_metadata` and pauses upstream.
                    match self.set_metadata_verdict(&md, &LoadTorrentLimits::default(), out) {
                        MetadataVerdict::Ok => {
                            self.metadata_tracker.clear();
                            out.push(TorrentEffect::MetadataReceived);
                        }
                        MetadataVerdict::HashMismatch => {
                            self.metadata_tracker.reset_on_hash_failure();
                            out.push(TorrentEffect::MetadataFailed);
                        }
                        MetadataVerdict::ParseError => {
                            self.metadata_tracker.reset_on_hash_failure();
                            out.push(TorrentEffect::MetadataFailed);
                            // upstream `set_metadata`:
                            // `set_error(invalid_swarm_metadata)` +
                            // `pause()` — hash-correct but
                            // unparseable is fatal until fixed
                            // (no disconnect_all — `pause` only).
                            self.set_error("invalid swarm metadata");
                            out.push(TorrentEffect::Alert(crate::alerts::Alert::TorrentError {
                                ih: self.info_hash,
                                error: "invalid swarm metadata".into(),
                                file: String::new(),
                            }));
                            self.paused = true;
                        }
                    }
                }
            }
            crate::ut_metadata::MetadataMsg::Reject => {}
        }
    }

    /// `ut_metadata_plugin` tick — while metadata is missing, ask
    /// peers that advertised `ut_metadata` for the next piece.
    pub fn metadata_tick(&mut self, out: &mut Vec<TorrentEffect>) {
        if self.piece_hashes.is_some() {
            return;
        }
        let ids: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| c.ext_ut_metadata != 0 && c.metadata_size.is_some())
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.metadata_tick_peer(id, out);
        }
    }

    /// ask `peer` for the next needed metadata piece.
    fn metadata_tick_peer(&mut self, peer: u64, out: &mut Vec<TorrentEffect>) {
        if self.piece_hashes.is_some() {
            return;
        }
        let ext = self.peers.get(&peer).map_or(0, |c| c.ext_ut_metadata);
        if ext == 0 {
            return;
        }
        let piece = self.metadata_request();
        if piece < 0 {
            return;
        }
        let body = crate::ut_metadata::metadata_packet(
            crate::ut_metadata::MetadataMsg::Request,
            piece,
            false,
            &[],
        );
        out.push(TorrentEffect::SendTo {
            peer,
            msg: PeerMessage::Extended {
                id: ext,
                data: body.into(),
            },
        });
    }

    /// `ut_pex` receive — parse the diff and feed the swarm list
    /// (upstream `on_pex_message` → `peer_list.add_peer` per added).
    /// `max_pex_peers` mirrors `settings_pack::max_pex_peers` (50).
    pub fn on_pex_packet(&mut self, body: &[u8]) {
        let Some((added, _dropped)) = crate::ut_pex::parse_pex_message(
            body,
            crate::ut_pex::MAX_PEER_ENTRIES / 2,
            self.peer_list.num_peers(),
        ) else {
            return;
        };
        for p in added {
            // upstream skips local addresses unless the sender is
            // local — our peers are anonymous endpoints, keep all
            let (i, _new) = self.peer_list.add_peer(
                p.addr,
                bitdaemon_common::flags::PeerSourceFlags::PEX,
                true,
            );
            // `added.f` pex_encryption → `pi->pe_support` (upstream
            // `ut_pex::on_message`); drives the MSE dial decision.
            if p.flags & crate::ut_pex::flags::PEX_ENCRYPTION != 0
                && let Some(peer) = self.peer_list.peers_mut().get_mut(i)
            {
                peer.pe_support = true;
            }
        }
    }

    /// `second_tick`'s pex section — every `PEX_INTERVAL` send each
    /// `ut_pex`-capable peer a diff of our connected endpoints.
    pub fn pex_tick(&mut self, now: Instant, out: &mut Vec<TorrentEffect>) {
        const PEX_INTERVAL: Duration = Duration::from_secs(60);
        if self
            .last_pex
            .is_some_and(|t| now.duration_since(t) < PEX_INTERVAL)
        {
            return;
        }
        self.last_pex = Some(now);
        // upstream `send_peer`: advertise only bittorrent conns that
        // either we dialed or that shared their listen port — and are
        // past the handshake. Inbound endpoints get their advertised
        // `p` port (the source port is ephemeral); flags carry
        // `pex_seed` for seeds.
        let connected: Vec<(SocketAddr, u8)> = self
            .peers
            .values()
            .filter(|c| (c.outgoing || c.received_listen_port) && !c.connecting && c.handshake_done)
            .filter_map(|c| {
                let mut ep = c.remote?;
                if !c.outgoing
                    && let Some(p) = c.peer_listen_port
                {
                    ep.set_port(p);
                }
                let mut flags = if c.peer_pieces.count() == c.num_pieces as usize {
                    crate::ut_pex::flags::PEX_SEED
                } else {
                    0
                };
                // `pex_encryption` — advertised when the conn went
                // through MSE (`supports_encryption()` upstream).
                if c.supports_encryption {
                    flags |= crate::ut_pex::flags::PEX_ENCRYPTION;
                }
                Some((ep, flags))
            })
            .collect();
        let ids: Vec<(u64, u8)> = self
            .peers
            .iter()
            .filter(|(_, c)| c.ext_ut_pex != 0)
            .map(|(id, c)| (*id, c.ext_ut_pex))
            .collect();
        for (id, ext) in ids {
            let c = match self.peers.get_mut(&id) {
                Some(c) => c,
                None => continue,
            };
            let (body, sent) = crate::ut_pex::build_pex_message(&connected, &c.pex_sent);
            c.pex_sent = sent;
            out.push(TorrentEffect::SendTo {
                peer: id,
                msg: PeerMessage::Extended {
                    id: ext,
                    data: body.into(),
                },
            });
        }
        // `i2p_pex` — the torrent-plugin tick rebuilds the shared diff;
        // then each i2p conn sends it (or the full list the first time).
        let disable = self
            .flags()
            .contains(bitdaemon_common::flags::TorrentFlags::DISABLE_PEX);
        self.i2p_pex_state.tick(now, self.peers.values(), disable);
        // prebuilt full list for first-time senders (`send_i2p_peer_list`).
        let full = crate::i2p_pex::build_full_list(self.peers.values());
        for (id, c) in self.peers.iter_mut() {
            if c.ext_i2p_pex == 0 || c.i2p.is_none() {
                continue;
            }
            let st = &self.i2p_pex_state;
            if let Some(msg) = c.i2p_pex.tick(now, c.ext_i2p_pex, st, &full, disable) {
                out.push(TorrentEffect::SendTo { peer: *id, msg });
            }
        }
    }

    /// `i2p_pex` receive — add destination hashes to the swarm list;
    /// flood/bad messages disconnect the peer (upstream verdicts).
    pub fn on_i2p_pex_packet(&mut self, peer_id: u64, body: &[u8], out: &mut Vec<TorrentEffect>) {
        let now = std::time::Instant::now();
        let disable = self
            .flags()
            .contains(bitdaemon_common::flags::TorrentFlags::DISABLE_PEX);
        if disable {
            return;
        }
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        match c.i2p_pex.on_message(now, body, body.len()) {
            crate::i2p_pex::I2pPexVerdict::Added(hashes) => {
                if crate::i2p_pex::apply_added(self, &hashes) {
                    // `do_connect_boost` — new peers → want to dial.
                    self.want_peers = true;
                }
            }
            crate::i2p_pex::I2pPexVerdict::Disconnect(reason) => {
                out.push(TorrentEffect::DisconnectPeer {
                    peer: peer_id,
                    reason,
                });
            }
            crate::i2p_pex::I2pPexVerdict::Nothing => {}
        }
    }

    /// `metadata_request` — which ut_metadata piece to ask for next
    /// (-1 = nothing requestable right now). Caller wraps it in a
    /// `ut_metadata` `request` packet via `metadata_packet`.
    pub fn metadata_request(&mut self) -> i32 {
        let peers = &self.peers;
        self.metadata_tracker
            .metadata_request(self.piece_hashes.is_some(), |src| peers.contains_key(&src))
    }

    /// `init_picker` — create the picker (metadata arrived or
    /// resume check done). `priorities`/`have` seeded from resume.
    pub fn init_picker(&mut self, now: bool) {
        if self.picker.is_none() {
            // upstream feeds `m_torrent_file->total_size()` — the
            // real payload size — because `num_pieces * piece_length`
            // rounds up: for a ragged tail that residue is 0 and the
            // last piece would wrongly require a full block count
            // (blocks that can never arrive → piece never finishes).
            let total = self
                .files
                .as_ref()
                .map_or(i64::from(self.num_pieces) * self.piece_length, |f| {
                    f.total_size()
                });
            self.picker = Some(PiecePicker::new(total, self.piece_length as i32));
            let _ = now;
        }
    }

    /// `write_resume_data` (upstream `torrent::write_resume_data`) —
    /// snapshot the picker state into `AddTorrentParams` fields.
    /// `verified`/`have` bitfields, `piece_priorities`, and
    /// `unfinished_pieces` (downloading pieces with ≥1
    /// finished/writing block → finished-block bitmask).
    pub fn to_resume_data(&self, atp: &mut AddTorrentParams) {
        // both hashes when we have metainfo — v2-only torrents must
        // carry `info-hash2` or `read_resume_data` rejects the
        // embedded info dict (MismatchingInfoHash).
        if let Some(ti) = &self.ti {
            atp.info_hashes = *ti.info_hashes();
        } else {
            atp.info_hashes.v1 = self.info_hash;
        }
        // `m_save_path` — without it a restored torrent silently
        // downloads into the caller's CWD instead of its real
        // save directory.
        atp.save_path = self.save_path.clone().into_bytes();
        atp.part_file_dir = self.part_file_dir.clone().into_bytes();
        atp.name = self.name.clone().into_bytes();
        atp.storage_mode = self.storage_mode;
        atp.flags = self.flags();
        atp.have_pieces = self.have.clone();
        // upstream: `verified` only meaningful in seed mode.
        if self.seed_mode {
            atp.verified_pieces = self.seed_verified.clone();
        }
        // `m_trackers` — url + tier pairs.
        for e in self.trackers.trackers.iter() {
            atp.trackers.push(e.url.clone().into_bytes());
            atp.tracker_tiers.push(e.tier);
        }
        // `m_peer_list` — connectable, non-banned, working peers
        // (upstream skips failcount>0, trust<0, non-connectable;
        // never-seen peers fill up to 100).
        let mut deferred: Vec<SocketAddr> = Vec::new();
        for p in self.peer_list.peers() {
            let Some(addr) = p.ip() else { continue };
            if p.banned {
                atp.banned_peers.push(addr);
                continue;
            }
            if !p.connectable || p.failcount > 0 || p.trust_points < 0 {
                continue;
            }
            if p.last_connected == 0 {
                if deferred.len() < 100 {
                    deferred.push(addr);
                }
                continue;
            }
            atp.peers.push(addr);
        }
        for p in deferred {
            if atp.peers.len() >= 100 {
                break;
            }
            atp.peers.push(p);
        }
        // `m_file_priority` — mutually exclusive with piece
        // priorities upstream; skipped in seed mode.
        if !self.file_priorities.is_empty() && !self.seed_mode {
            atp.file_priorities = self.file_priorities.clone();
            atp.piece_priorities.clear();
        }
        for (i, name) in &self.renamed_files {
            atp.renamed_files.insert(*i, name.clone().into_bytes());
        }
        atp.total_uploaded = self.total_upload();
        atp.total_downloaded = self.total_download();
        atp.upload_limit = self.upload_limit;
        atp.download_limit = self.download_limit;
        atp.max_connections = i64::from(self.max_connections);
        atp.max_uploads = i64::from(self.max_uploads);
        atp.last_seen_complete = self.last_seen_complete;
        atp.added_time = self
            .added_time
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        atp.active_time = self.started.elapsed().as_secs() as i64;
        atp.finished_time = self.finished_at.map_or(0, |t| t.elapsed().as_secs() as i64);
        atp.seeding_time = self
            .seeding_since
            .map_or(0, |t| t.elapsed().as_secs() as i64);
        // upstream `torrent::write_resume_data`: every non-removed,
        // non-ephemeral `m_web_seeds` entry folds into
        // `ret.url_seeds` — http_seed-type entries included (the
        // `httpseeds` resume key is never re-emitted).
        atp.url_seeds = self
            .web_seeds
            .iter()
            .map(|u| u.clone().into_bytes())
            .collect();
        // upstream embeds the `info` dict when the torrent has
        // metadata — rebuild `ti` from our raw copy so
        // `write_resume_data_buf` emits it and `load` restores
        // piece count + hashes.
        if let Some(raw) = &self.metadata_raw {
            let node = bitdaemon_bencode::bdecode(raw);
            if let Ok(ti) = TorrentInfo::from_info_node(&node, &LoadTorrentLimits::default()) {
                atp.ti = Some(std::sync::Arc::new(ti));
            }
        }
        // v2: export sparse trees + masks + verified leafs
        // (upstream `resume_data` "trees" table).
        if !self.merkle_trees.is_empty() {
            atp.merkle_trees.clear();
            atp.merkle_tree_mask.clear();
            atp.verified_leaf_hashes.clear();
            for tree in &self.merkle_trees {
                let (sparse, mask) = tree.build_sparse_vector();
                atp.merkle_trees.push(sparse);
                atp.merkle_tree_mask.push(mask);
                atp.verified_leaf_hashes.push(tree.verified_leafs());
            }
        }
        if let Some(p) = self.picker.as_ref() {
            atp.piece_priorities = (0..self.num_pieces)
                .map(|i| p.piece_priority(PieceIndex(i)))
                .collect();
            for dp in p.get_download_queue() {
                if dp.finished == 0 && dp.writing == 0 {
                    continue;
                }
                let mut mask = Bitfield::with_size(p.blocks_in_piece(dp.index) as usize);
                for (i, b) in p.blocks_for_piece(&dp).iter().enumerate() {
                    if b.state == BlockState::Finished {
                        mask.set_bit(i);
                    }
                }
                atp.unfinished_pieces.insert(dp.index.0, mask);
            }
        }
    }

    /// `check_resume` (upstream `torrent::check_resume`) — apply
    /// `AddTorrentParams` to a torrent whose picker is initialized:
    /// seed `have`/`verified` bitfields, `piece_priorities`, and
    /// `mark_as_finished` every bit set in `unfinished_pieces`.
    /// Returns effects for pieces that just became complete
    /// (`verify_piece` upstream → we emit `Hash` requests via
    /// `PieceReady`-style effect: here `HashPiece` alerts).
    pub fn apply_resume_data(&mut self, atp: &AddTorrentParams, out: &mut Vec<TorrentEffect>) {
        // v2: restore sparse merkle trees + verified-leaf masks from
        // the resume `trees` table (upstream `set_merkle_trees`).
        for (f, sparse) in atp.merkle_trees.iter().enumerate() {
            let Some(tree) = self.merkle_trees.get_mut(f) else {
                break;
            };
            if sparse.is_empty() {
                continue;
            }
            let empty = Default::default();
            let mask = atp.merkle_tree_mask.get(f).unwrap_or(&empty);
            let verified = atp.verified_leaf_hashes.get(f).unwrap_or(&empty);
            tree.load_sparse_tree(sparse, mask, verified);
        }
        self.init_picker(false);
        let picker = self.picker.as_mut();
        let Some(picker) = picker else { return };

        // resume `pieces` — the claimed `have` bitfield (upstream
        // `m_have_pieces` starts optimistic and `check_fastresume`
        // either confirms or clears it).
        for i in 0..self.num_pieces.min(atp.have_pieces.size() as i32) {
            if atp.have_pieces.get_bit(i as usize) {
                self.have.set_bit(i as usize);
            }
        }
        for i in 0..self.num_pieces.min(atp.verified_pieces.size() as i32) {
            if atp.verified_pieces.get_bit(i as usize) && !self.have.get_bit(i as usize) {
                self.have.set_bit(i as usize);
            }
        }
        // `total_done` counts resume-claimed bytes too (upstream
        // progress is `total_done/total_size`) — a rejected
        // fastresume later rewinds both `have` and this counter.
        self.verified_bytes = (0..self.num_pieces).fold(0i64, |acc, i| {
            if self.have.get_bit(i as usize) {
                acc + self.files.as_ref().map_or(self.piece_length, |f| {
                    i64::from(f.piece_size(PieceIndex(i)))
                })
            } else {
                acc
            }
        });
        for (i, prio) in atp.piece_priorities.iter().enumerate() {
            if i as i32 >= self.num_pieces {
                break;
            }
            picker.set_piece_priority(PieceIndex(i as i32), *prio);
        }
        self.need_save_resume = true;
        for (piece, blocks) in &atp.unfinished_pieces {
            let idx = PieceIndex(*piece);
            if *piece < 0 || *piece >= self.num_pieces || self.have.get_bit(*piece as usize) {
                continue;
            }
            if picker.have_piece(idx) {
                picker.we_dont_have(idx);
            }
            let nbits = picker.blocks_in_piece(idx).min(blocks.size() as i32);
            for k in 0..nbits {
                if blocks.get_bit(k as usize) {
                    picker.mark_as_finished(
                        PieceBlock {
                            piece_index: idx,
                            block_index: k,
                        },
                        None,
                    );
                }
            }
            if picker.is_piece_finished(idx) {
                // upstream `verify_piece` — needs a hash pass
                out.push(TorrentEffect::DiskHash { piece: idx });
            }
        }
        if self.have.count() as i32 == self.num_pieces && self.num_pieces > 0 {
            self.finished = true;
            self.seeding = true;
            self.finished_at.get_or_insert_with(Instant::now);
            self.seeding_since.get_or_insert_with(Instant::now);
        }
    }

    /// `get_peer_info` — build the public `peer_info` snapshot for
    /// one connected peer (`Torrent::get_peer_info` upstream fills
    /// from `peer_connection` counters).
    #[must_use]
    pub fn peer_info(
        &self,
        id: u64,
        remote: Option<std::net::SocketAddr>,
    ) -> Option<crate::peer_info::PeerInfo> {
        use crate::peer_info::{BandwidthState, ConnectionType, PeerFlags, PeerInfo};
        let c = self.peers.get(&id)?;
        let mut flags = PeerFlags::default();
        if c.interesting {
            flags = flags.with(PeerFlags::INTERESTING);
        }
        if c.choked {
            flags = flags.with(PeerFlags::CHOKED);
        }
        if c.peer_interested {
            flags = flags.with(PeerFlags::REMOTE_INTERESTED);
        }
        if c.peer_choked {
            flags = flags.with(PeerFlags::REMOTE_CHOKED);
        }
        if c.bitfield_received {
            flags = flags.with(PeerFlags::HANDSHAKE);
        }
        if c.remote_upload_only {
            flags = flags.with(PeerFlags::UPLOAD_ONLY);
        }
        if c.pipeline.snubbed {
            flags = flags.with(PeerFlags::SNUBBED);
        }
        if c.pipeline.endgame {
            flags = flags.with(PeerFlags::ENDGAME_MODE);
        }
        if c.num_pieces >= self.num_pieces {
            flags = flags.with(PeerFlags::SEED);
        }
        Some(PeerInfo {
            flags,
            source: bitdaemon_common::flags::PeerSourceFlags::INCOMING,
            up_speed: c.rate_up_speed,
            down_speed: c.rate_down_speed,
            payload_up_speed: c.rate_up_speed,
            payload_down_speed: c.rate_down_speed,
            pid: c.peer_id.unwrap_or_default(),
            pieces: c.peer_pieces.clone(),
            queue_bytes: c.pipeline.outstanding_bytes.min(i64::from(i32::MAX)) as i32,
            request_timeout: if c.pipeline.download_queue.is_empty() {
                -1
            } else {
                c.pipeline.request_timeout_setting
            },
            send_buffer_size: 0,
            used_send_buffer: 0,
            receive_buffer_size: 0,
            used_receive_buffer: 0,
            receive_buffer_watermark: 0,
            num_hashfails: 0,
            download_queue_length: c.pipeline.download_queue.len() as i32,
            timed_out_requests: c
                .pipeline
                .download_queue
                .iter()
                .filter(|b| b.timed_out)
                .count() as i32,
            busy_requests: c.pipeline.download_queue.iter().filter(|b| b.busy).count() as i32,
            requests_in_buffer: c.pipeline.request_queue.len() as i32,
            target_dl_queue_length: i32::from(c.pipeline.desired_queue_size),
            upload_queue_length: c.upload_queue.len() as i32,
            failcount: 0,
            downloading_piece_index: c
                .pipeline
                .download_queue
                .first()
                .map_or(-1, |b| b.block.piece_index.0),
            downloading_block_index: c
                .pipeline
                .download_queue
                .first()
                .map_or(-1, |b| b.block.block_index),
            downloading_progress: 0,
            downloading_total: 0,
            connection_type: ConnectionType::STANDARD_BITTORRENT,
            pending_disk_bytes: 0,
            pending_disk_read_bytes: 0,
            send_quota: 0,
            receive_quota: 0,
            rtt: 0,
            num_pieces: c.num_pieces,
            download_rate_peak: 0,
            upload_rate_peak: 0,
            progress_ppm: if self.num_pieces > 0 {
                (i64::from(c.num_pieces) * 1_000_000 / i64::from(self.num_pieces)) as i32
            } else {
                0
            },
            remote_endpoint: remote,
            local_endpoint: None,
            read_state: BandwidthState::NETWORK,
            write_state: BandwidthState::NETWORK,
            client: if c.client_version.is_empty() {
                // `peer_connection::peer_info_struct` fallback —
                // `identify_client` on the handshake peer id.
                c.peer_id.as_ref().map_or_else(Vec::new, |pid| {
                    bitdaemon_common::identify_client::identify_client(pid).into_bytes()
                })
            } else {
                c.client_version.clone()
            },
        })
    }

    /// `files_checked` (upstream `torrent::files_checked`) — verdict
    /// of `async_check_files` on the fastresume data. Accepted →
    /// `have` stands and a complete torrent becomes `seeding`
    /// (`no_error` + `seed_mode`). Rejected (`need_full_check`) →
    /// the resume-supplied bits are cleared; the caller reschedules
    /// per-piece `DiskHash` jobs for the real check.
    pub fn on_files_checked(&mut self, accepted: bool, out: &mut Vec<TorrentEffect>) {
        self.checking = false;
        self.need_save_resume = true;
        // `m_files_checked`/`m_connections_initialized` — gates the
        // late-add `on_files_checked` replay in `add_extension`.
        self.files_checked = true;
        for ext in &self.extensions {
            ext.on_files_checked();
        }
        if !accepted {
            // the picker's have-tracking only knows pieces that
            // passed `piece_verified` — resume bits live solely in
            // `self.have`, clearing it is sufficient.
            self.have.clear_all();
            self.verified_bytes = 0;
            self.finished = false;
            self.seeding = false;
            return;
        }
        // `is_finished()` — all *wanted* pieces (upstream
        // `files_checked` → `finished()`); `seeding` needs the
        // full set (`completed()` → `torrent_status::seeding`).
        let all = self.num_pieces > 0 && self.have.count() as i32 == self.num_pieces;
        let want_done = all || self.picker.as_ref().is_some_and(|p| p.is_finished());
        if want_done {
            self.finished = true;
            self.finished_at.get_or_insert_with(Instant::now);
            out.push(TorrentEffect::BecameSeed);
        }
        if all {
            self.seeding = true;
            self.seeding_since.get_or_insert_with(Instant::now);
        }
    }

    /// `add_tracker_url` — upstream `add_tracker_url`.
    pub fn add_tracker(&mut self, url: String, tier: i32) {
        if self.trackers.find(&url).is_none() {
            self.trackers
                .add(crate::tracker_list::AnnounceEntry::new(url, tier));
        }
    }

    /// `announce_req` — the `AnnounceParams` for a due tracker
    /// (`torrent::tracker_request`). `port`/`key`/`peer_id` come
    /// from the session. `left` is approximated as
    /// `num_pieces * piece_length - verified` (the tail-piece
    /// delta needs `total_size` — close enough for `left=`).
    #[must_use]
    pub fn announce_params(
        &self,
        peer_id: bitdaemon_common::PeerId,
        port: u16,
        event: i32,
        key: i32,
    ) -> bitdaemon_net::tracker_client::AnnounceParams {
        bitdaemon_net::tracker_client::AnnounceParams {
            info_hash: self.info_hash.to_bytes(),
            peer_id: *peer_id.as_bytes(),
            port,
            uploaded: 0,
            downloaded: self.verified_bytes,
            left: (i64::from(self.num_pieces) * self.piece_length - self.verified_bytes).max(0),
            event,
            num_want: -1,
            key,
        }
    }

    /// `is_seed`.
    #[must_use]
    pub fn is_seed(&self) -> bool {
        // upstream `is_seed()` includes `m_seed_mode` — the flag
        // asserts we have everything before verification proves it.
        self.seeding || self.finished || self.seed_mode
    }

    /// `add_peer` (swarm entry, not connection) — tracker/PEX/DHT/
    /// LSD feed. Returns the `PeerList` index.
    pub fn add_swarm_peer(
        &mut self,
        remote: std::net::SocketAddr,
        src: bitdaemon_common::flags::PeerSourceFlags,
        connectable: bool,
    ) -> usize {
        self.peer_list.add_peer(remote, src, connectable).0
    }

    /// `connect_to_peer` (upstream `torrent_handle::connect_peer`)
    /// — insert `remote` in the peer list as an immediate
    /// connect candidate (failcount untouched → next pump picks it).
    pub fn connect_peer(&mut self, remote: std::net::SocketAddr) -> usize {
        self.add_swarm_peer(remote, bitdaemon_common::flags::PeerSourceFlags::NONE, true)
    }

    /// `piece_availability` — swarm availability vector
    /// (`torrent::piece_availability` → picker availability).
    #[must_use]
    pub fn piece_availability(&self) -> Vec<i32> {
        let mut v = Vec::new();
        if let Some(p) = &self.picker {
            p.get_availability_vec(&mut v);
        }
        v
    }

    /// `get_download_queue` — in-flight partial pieces
    /// (`partial_piece_info` upstream: finished/writing/requested
    /// counts per downloading piece).
    #[must_use]
    pub fn get_download_queue(&self) -> Vec<crate::piece_picker::DownloadingPiece> {
        self.picker.as_ref().map_or_else(
            Vec::new,
            crate::piece_picker::PiecePicker::get_download_queue,
        )
    }

    /// `find_connect_candidates` — the session's connect pump asks
    /// for the next endpoints to dial (`torrent::connect_to_peer`).
    #[must_use]
    pub fn connect_candidates(
        &mut self,
        session_time: i32,
        state: &bitdaemon_net::peer_list::PeerListState,
        out_policy: bitdaemon_net::pe_crypto::EncPolicy,
    ) -> Vec<(std::net::SocketAddr, bool)> {
        // keep `finished` in sync so seeds skip seeds/upload-only
        let mut st = state.clone();
        st.is_finished = self.is_seed();
        let idxs = self.peer_list.find_connect_candidates(session_time, &st);
        // `set_connection` at dial start (upstream's peer_connection
        // ctor) — the entry stops being a candidate until
        // `peer_connect_failed`/`peer_disconnected` reports back.
        for i in &idxs {
            self.peer_list.set_connected(*i);
        }
        idxs.into_iter()
            .filter_map(|i| {
                let p = self.peer_list.peers_mut().get_mut(i)?;
                let ep = p.ip()?;
                // upstream `bt_peer_connection::start`: pe_forced →
                // MSE always; pe_enabled → MSE iff `pi->pe_support`,
                // with the self-toggling flag: a plain dial pre-sets
                // it (a failure retries encrypted), an MSE dial
                // clears it (success re-arms via peer_connected).
                let encrypted = match out_policy {
                    bitdaemon_net::pe_crypto::EncPolicy::Forced => true,
                    bitdaemon_net::pe_crypto::EncPolicy::Disabled => false,
                    bitdaemon_net::pe_crypto::EncPolicy::Enabled => {
                        if p.pe_support {
                            p.pe_support = false;
                            true
                        } else {
                            p.pe_support = true;
                            false
                        }
                    }
                };
                Some((ep, encrypted))
            })
            .collect()
    }

    /// `connection_closed` for a dial that never completed —
    /// bumps `last_connected` and `failcount` so the peer isn't
    /// retried immediately (`inc_failcount` + timestamp upstream).
    pub fn peer_connect_failed(&mut self, remote: std::net::SocketAddr, session_time: i32) {
        if let Some(i) = self.peer_index(remote) {
            self.peer_list.connection_closed(i, session_time);
            self.peer_list.inc_failcount(i);
        }
    }

    /// `set_connection` — mark the swarm entry for `remote` as
    /// connected (also covers inbound accepts). `encrypted` = the
    /// conn negotiated MSE → upstream re-arms `pi->pe_support`.
    pub fn peer_connected_addr(&mut self, remote: std::net::SocketAddr, encrypted: bool) {
        if let Some(i) = self.peer_index(remote) {
            self.peer_list.set_connected(i);
            if let Some(p) = self.peer_list.peers_mut().get_mut(i) {
                p.pe_support = encrypted;
            }
        }
    }

    /// `connection_closed` — clear the connection flag + stamp
    /// `last_connected` for the dial backoff.
    pub fn peer_disconnected_addr(&mut self, remote: std::net::SocketAddr, session_time: i32) {
        if let Some(i) = self.peer_index(remote) {
            self.peer_list.connection_closed(i, session_time);
        }
    }

    fn peer_index(&self, remote: std::net::SocketAddr) -> Option<usize> {
        self.peer_list
            .peers()
            .iter()
            .position(|p| p.ip() == Some(remote))
    }

    /// `torrent::add_extension` — register an instantiated plugin.
    /// Upstream then walks every existing connection through
    /// `new_connection` (producing per-peer `peer_plugin`s) and
    /// calls `on_files_checked` when the storage check already ran.
    pub fn add_extension_plugin(&mut self, ext: std::sync::Arc<dyn crate::alerts::SessionPlugin>) {
        self.extensions.push(ext);
        let Some(ext_ref) = self.extensions.last().cloned() else {
            return;
        };
        // disjoint field borrows: extensions is already pushed, so
        // iterate the clone while mutating peer conns.
        for c in self.peers.values_mut() {
            let Some(remote) = c.remote else { continue };
            if let Some(pp) = ext_ref.new_peer_connection(&remote) {
                c.plugins.push(pp);
            }
        }
        if self.files_checked {
            ext_ref.on_files_checked();
        }
    }

    /// `add_peer` — a new connection registered. Upstream runs
    /// `torrent_plugin::new_connection` for each extension at
    /// attach time (`peer_connection::init`).
    pub fn add_peer(&mut self, id: u64, mut conn: PeerConn) {
        if let Some(remote) = conn.remote {
            for ext in &self.extensions {
                if let Some(pp) = ext.new_peer_connection(&remote) {
                    conn.plugins.push(pp);
                }
            }
        }
        self.peers.insert(id, conn);
    }

    /// `peer_disconnected` — refcount down everything it had and
    /// abort its in-flight requests (upstream `peer_lost` +
    /// `clear_peer` semantics).
    pub fn remove_peer(&mut self, id: u64, eff: &mut Vec<TorrentEffect>) {
        let Some(c) = self.peers.remove(&id) else {
            return;
        };
        if let Some(p) = self.picker.as_mut() {
            let key = c.peer_key();
            for i in 0..c.peer_pieces.size() {
                if c.peer_pieces.get_bit(i) {
                    p.dec_refcount(PieceIndex(i as i32));
                }
            }
            for b in c.outstanding() {
                p.abort_download(b, key);
            }
        }
        let _ = eff;
    }

    /// `peer_connection::fill_send_buffer` — issue disk reads for the
    /// requests the peer has queued (`upload_queue` ≡ `m_requests`)
    /// while `send_buffer + reading_bytes` stays under the dynamic
    /// watermark:
    /// `clamp(uploaded_last_second * factor/100, low, high)`.
    /// Requests for pieces we no longer have are rejected upstream-
    /// style (`write_reject_request`, FAST-gated).
    pub fn fill_send_buffer(
        &mut self,
        peer_id: u64,
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) {
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        let watermark = (i64::from(c.rate_up_speed) * i64::from(s.send_buffer_watermark_factor)
            / 100)
            .clamp(s.send_buffer_low_watermark, s.send_buffer_watermark as i64)
            .max(0) as usize;
        while !c.upload_queue.is_empty() && c.send_buffer + c.reading_bytes < watermark {
            let r = c.upload_queue[0];
            if (r.piece.0 as usize) < self.have.size() && self.have.get_bit(r.piece.0 as usize) {
                c.reading_bytes += r.length.max(0) as usize;
                c.upload_queue.remove(0);
                out.push(TorrentEffect::DiskRead {
                    peer: peer_id,
                    req: r,
                });
            } else {
                c.upload_queue.remove(0);
                if c.supports_fast {
                    out.push(TorrentEffect::SendTo {
                        peer: peer_id,
                        msg: PeerMessage::RejectRequest(r),
                    });
                }
            }
        }
    }

    /// `peer_has`/`peer_lost` — apply [`PeerEffect`]s from a peer
    /// dispatch into picker/disk intents.
    pub fn apply_peer_effects(
        &mut self,
        peer_id: u64,
        effects: Vec<PeerEffect>,
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) {
        let key = self.peers.get(&peer_id).and_then(PeerConn::peer_key);
        let remote = self
            .peers
            .get(&peer_id)
            .and_then(|c| c.remote)
            .map_or_else(String::new, |ep| ep.to_string());
        for e in effects {
            match e {
                PeerEffect::RefcountInc(i) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.inc_refcount(i);
                    }
                }
                PeerEffect::RefcountDec(i) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.dec_refcount(i);
                    }
                }
                PeerEffect::RefcountBulk(bits, inc) => {
                    if let Some(p) = self.picker.as_mut() {
                        if inc {
                            p.inc_refcount_bitfield(&bits);
                        } else {
                            p.dec_refcount_bitfield(&bits);
                        }
                    }
                }
                PeerEffect::DiskWrite(req, data) => {
                    out.push(TorrentEffect::DiskWrite {
                        peer: peer_id,
                        req,
                        data,
                    });
                }
                PeerEffect::ReadRequest(req) => {
                    // super-seeding: requests outside the piece we
                    // leaked get rejected outright (upstream
                    // `incoming_request` → `write_reject_request`
                    // + `invalid_request_alert`).
                    if self.super_seeding {
                        // `super_seeded_piece` — either of the two
                        // leaked slots honors the request.
                        let leaked = self
                            .peers
                            .get(&peer_id)
                            .is_some_and(|c| c.superseed_pieces.contains(&Some(req.piece)));
                        if !leaked {
                            if let Some(c) = self.peers.get_mut(&peer_id) {
                                c.num_invalid_requests += 1;
                                // rejected requests never enter
                                // `m_requests` upstream — drop the
                                // entry peer_conn queued.
                                c.upload_queue
                                    .retain(|u| !(u.piece == req.piece && u.start == req.start));
                                // `write_reject_request` is gated
                                // on the FAST extension upstream.
                                if c.supports_fast {
                                    out.push(TorrentEffect::SendTo {
                                        peer: peer_id,
                                        msg: PeerMessage::RejectRequest(req),
                                    });
                                }
                            }
                            out.push(TorrentEffect::Alert(crate::alerts::Alert::InvalidRequest {
                                ih: self.info_hash,
                                remote: remote.clone(),
                                msg: "piece not super-seeded".into(),
                            }));
                            continue;
                        }
                    }
                    // upstream `incoming_request`: a peer we choke
                    // only gets BEP-6 `allowed_fast` pieces; a piece
                    // we don't have gets a FAST `reject` (or silent
                    // drop when fast isn't negotiated) — never a
                    // disk read. Accepted requests post
                    // `incoming_request_alert`; rejected ones post
                    // `invalid_request_alert`. A flood beyond
                    // `max_allowed_in_request_queue` is rejected too.
                    let queue_full = self.peers.get(&peer_id).is_some_and(|c| {
                        c.upload_queue.len() as i32 > s.max_allowed_in_request_queue
                    });
                    let allowed = !queue_full
                        && self.peers.get(&peer_id).is_some_and(|c| {
                            (!c.choked || c.allowed_fast.contains(&req.piece))
                                && (req.piece.0 as usize) < self.have.size()
                                && self.have.get_bit(req.piece.0 as usize)
                        });
                    if allowed {
                        out.push(TorrentEffect::Alert(
                            crate::alerts::Alert::IncomingRequest {
                                ih: self.info_hash,
                                remote: remote.clone(),
                                piece: req.piece,
                                start: req.start,
                                length: req.length,
                            },
                        ));
                        // the request is already in `upload_queue`
                        // (≡ `m_requests`) — `fill_send_buffer`
                        // admits reads under the send-buffer
                        // watermark.
                        self.fill_send_buffer(peer_id, s, out);
                    } else {
                        if let Some(c) = self.peers.get_mut(&peer_id) {
                            c.upload_queue
                                .retain(|u| !(u.piece == req.piece && u.start == req.start));
                        }
                        out.push(TorrentEffect::Alert(crate::alerts::Alert::InvalidRequest {
                            ih: self.info_hash,
                            remote: remote.clone(),
                            msg: "peer choked or piece not available".into(),
                        }));
                        if self.peers.get(&peer_id).is_some_and(|c| c.supports_fast) {
                            out.push(TorrentEffect::SendTo {
                                peer: peer_id,
                                msg: PeerMessage::RejectRequest(req),
                            });
                        }
                    }
                }
                PeerEffect::CancelRequest(_) => {}
                PeerEffect::UnwantedBlock(b) => {
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::UnwantedBlock {
                        ih: self.info_hash,
                        remote: remote.clone(),
                        piece: b.piece_index,
                        block: b.block_index as u32,
                    }));
                }
                PeerEffect::BlockFinished(b) => {
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::BlockFinished {
                        ih: self.info_hash,
                        piece: b.piece_index,
                        block: b.block_index as u32,
                    }));
                }
                PeerEffect::Unsnub => {
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerUnsnubbed {
                        ih: self.info_hash,
                        remote: remote.clone(),
                    }));
                }
                PeerEffect::InvalidRequest(msg) => {
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::InvalidRequest {
                        ih: self.info_hash,
                        remote: remote.clone(),
                        msg,
                    }));
                }
                PeerEffect::RejectRequest(req) => {
                    // `write_reject_request` — BEP-6 fast ext only.
                    if self.peers.get(&peer_id).is_some_and(|c| c.supports_fast) {
                        out.push(TorrentEffect::SendTo {
                            peer: peer_id,
                            msg: PeerMessage::RejectRequest(req),
                        });
                    }
                }
                PeerEffect::MarkDownloaded(b) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.mark_as_writing(b, key);
                    }
                }
                PeerEffect::AbortDownload(b) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.abort_download(b, key);
                    }
                }
                PeerEffect::MaybePick => self.drive_peer(peer_id, out),
                PeerEffect::ExtHandshake => {
                    // peer just advertised ut_metadata — ask right away
                    self.metadata_tick_peer(peer_id, out);
                    // v2: peers that know `hash_request` can fill
                    // missing merkle layers (`maybe_send_hash_request`).
                    self.maybe_send_hash_request(peer_id, out);
                }
                PeerEffect::MetadataPacket(body) => {
                    self.on_metadata_packet(peer_id, &body, out);
                }
                PeerEffect::PexPacket(body) => {
                    self.on_pex_packet(&body);
                }
                PeerEffect::I2pPexPacket(body) => {
                    self.on_i2p_pex_packet(peer_id, &body, out);
                }
                PeerEffect::Holepunch(hp) => {
                    self.on_holepunch(peer_id, hp, out);
                }
                PeerEffect::DhtPort(port) => {
                    // peer's DHT listen port → add the node to the
                    // routing table (upstream `session_impl::
                    // on_dht_port` → `m_dht->add_node`).
                    if let Some(c) = self.peers.get(&peer_id)
                        && c.supports_dht
                        && let Some(mut addr) = c.remote
                    {
                        addr.set_port(port);
                        out.push(TorrentEffect::DhtNode(addr));
                    }
                }
                PeerEffect::YourIp(ip) => {
                    out.push(TorrentEffect::YourIp(ip));
                }
                PeerEffect::Disconnect(r) => {
                    out.push(TorrentEffect::DisconnectPeer {
                        peer: peer_id,
                        reason: r,
                    });
                }
            }
        }
        // upstream re-evaluates `interesting`→`interested` after any
        // incoming peer message (bitfield/have/have_all flip the
        // `m_interesting` bit).
        self.update_interest(peer_id, out);
    }

    /// `on_holepunch` dispatch — BEP-55 relay logic. `rendezvous`
    /// either fails the requester, drops (local bridging), or emits
    /// two `connect` messages (requester + target). `connect` adds
    /// the endpoint to the swarm as a uTP-capable connect candidate
    /// (upstream `add_peer` + `connect_to_peer` + `supports_utp`).
    /// `failed` is log-only upstream — we count it for tests.
    fn on_holepunch(
        &mut self,
        peer_id: u64,
        hp: crate::ut_holepunch::Hp,
        out: &mut Vec<TorrentEffect>,
    ) {
        use crate::ut_holepunch::{HpMessage, encode_hp, rendezvous};
        let Some(c) = self.peers.get(&peer_id) else {
            return;
        };
        let remote = c.remote;
        let remote_local = remote
            .map(|a| bitdaemon_net::ip_helpers::is_local(&a.ip()))
            .unwrap_or(false);
        let req_ext = c.ext_holepunch;
        if req_ext == 0 {
            return;
        }
        match hp.msg {
            HpMessage::Rendezvous => {
                if remote == Some(hp.ep) {
                    // `p == this` → `no_self`
                    let msg = PeerMessage::Extended {
                        id: req_ext,
                        data: encode_hp(
                            HpMessage::Failed,
                            &hp.ep,
                            crate::ut_holepunch::HpError::NoSelf,
                        )
                        .into(),
                    };
                    out.push(TorrentEffect::SendTo { peer: peer_id, msg });
                    return;
                }
                let target = self
                    .peers
                    .iter()
                    .find(|(_, c)| c.remote == Some(hp.ep))
                    .map(|(id, c)| (*id, c.ext_holepunch));
                let decision = rendezvous(
                    &hp.ep,
                    target.map(|(_, ext)| (ext != 0, false)),
                    remote_local,
                );
                match decision {
                    crate::ut_holepunch::Rendezvous::Bridge => {
                        if let (Some((tid, text)), Some(req)) = (target, remote) {
                            // connect(ep) back to the requester
                            out.push(TorrentEffect::SendTo {
                                peer: peer_id,
                                msg: PeerMessage::Extended {
                                    id: req_ext,
                                    data: encode_hp(
                                        HpMessage::Connect,
                                        &hp.ep,
                                        crate::ut_holepunch::HpError::NoSuchPeer,
                                    )
                                    .into(),
                                },
                            });
                            // connect(requester) to the target
                            out.push(TorrentEffect::SendTo {
                                peer: tid,
                                msg: PeerMessage::Extended {
                                    id: text,
                                    data: encode_hp(
                                        HpMessage::Connect,
                                        &req,
                                        crate::ut_holepunch::HpError::NoSuchPeer,
                                    )
                                    .into(),
                                },
                            });
                        }
                    }
                    crate::ut_holepunch::Rendezvous::Fail(e) => {
                        out.push(TorrentEffect::SendTo {
                            peer: peer_id,
                            msg: PeerMessage::Extended {
                                id: req_ext,
                                data: encode_hp(HpMessage::Failed, &hp.ep, e).into(),
                            },
                        });
                    }
                    crate::ut_holepunch::Rendezvous::Drop => {}
                }
            }
            HpMessage::Connect => {
                if crate::ut_holepunch::connect_allowed(&hp.ep, remote_local) {
                    // upstream `add_peer` + `connect_to_peer(true)`
                    // + `supports_utp` — the uTP flag lives on the
                    // transport choice; swarm-side it's a connectable
                    // pex-sourced candidate.
                    self.add_swarm_peer(hp.ep, bitdaemon_common::flags::PeerSourceFlags::PEX, true);
                }
            }
            HpMessage::Failed => {}
        }
    }

    /// feed one decoded message from `peer` — `dispatch` + effects.
    /// `now` is `session_time` (s) for inactivity bookkeeping.
    /// `s` feeds the send-buffer watermark (`m_settings` upstream).
    pub fn on_peer_message(
        &mut self,
        peer_id: u64,
        msg: &PeerMessage,
        now: i32,
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) {
        match msg {
            // `on_hash_request`/`on_hashes`/`on_hash_reject` upstream:
            // hash messages are only legal on a `protocol_v2`
            // connection (handshake keyed by the v2 info-hash) —
            // anything else is a protocol violation (`invalid_message`).
            PeerMessage::HashRequest(req) => {
                if self.hash_protocol_ok(peer_id) {
                    self.on_hash_request(peer_id, req, out);
                } else {
                    self.peer_error_effect(peer_id, "hash_request on non-v2 connection", out);
                }
                return;
            }
            PeerMessage::Hashes { req, hashes } => {
                if self.hash_protocol_ok(peer_id) {
                    self.on_hashes(req, hashes, out);
                } else {
                    self.peer_error_effect(peer_id, "hashes on non-v2 connection", out);
                }
                return;
            }
            PeerMessage::HashReject(req) => {
                if self.hash_protocol_ok(peer_id) {
                    self.on_hash_reject(req);
                } else {
                    self.peer_error_effect(peer_id, "hash_reject on non-v2 connection", out);
                }
                return;
            }
            _ => {}
        }
        let mut effects = Vec::new();
        // super-seeding: a `have` for a piece we leaked triggers
        // the next superseed pick (upstream `incoming_have` runs
        // this before updating `m_have_piece`, so the announced
        // piece is still a candidate — the second_tick recovery
        // below unsticks the rare re-pick stall).
        if self.super_seeding {
            if let PeerMessage::Have(p) = msg {
                if self
                    .peers
                    .get(&peer_id)
                    .is_some_and(|c| c.superseed_pieces.contains(&Some(*p)))
                {
                    self.on_superseeded_have(peer_id, *p, out);
                }
            }
        }
        if let Some(c) = self.peers.get_mut(&peer_id) {
            c.last_active = now;
            if let PeerMessage::Piece { data, .. } = msg {
                let n = data.len() as i64;
                c.stat_down_total += n;
                c.stat_down_round += n;
                self.last_payload_down = Some(Instant::now());
            }
            // `peer_plugin::on_*` — plugins observe the decoded
            // message before our dispatch consumes it (upstream
            // fires the per-message hooks inside `incoming_*`).
            for p in &c.plugins {
                p.on_message(msg);
            }
            c.dispatch(msg, self.num_pieces, &mut effects);
        }
        self.apply_peer_effects(peer_id, effects, s, out);
    }

    /// `!peer_info_struct()->protocol_v2 || !t->valid_metadata()` —
    /// the upstream guard shared by all three hash handlers.
    fn hash_protocol_ok(&self, peer_id: u64) -> bool {
        self.metadata_raw.is_some() && self.peers.get(&peer_id).is_some_and(|c| c.protocol_v2)
    }

    /// `peer_error_alert` + disconnect — upstream
    /// `peer_connection::disconnect` posts the alert on protocol
    /// violations before dropping the connection.
    fn peer_error_effect(&self, peer_id: u64, msg: &'static str, out: &mut Vec<TorrentEffect>) {
        let remote = self
            .peers
            .get(&peer_id)
            .and_then(|c| c.remote)
            .map_or_else(String::new, |ep| ep.to_string());
        out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerError {
            ih: self.info_hash,
            remote,
            op: "bittorrent".into(),
            error: msg.into(),
        }));
        out.push(TorrentEffect::DisconnectPeer {
            peer: peer_id,
            reason: "invalid message",
        });
    }

    /// `torrent::get_hashes` — serve a `hash_request` from our
    /// merkle trees. Out-of-range file/root or missing tree →
    /// `hash_reject` (upstream drops invalid requests the same way).
    pub fn on_hash_request(
        &mut self,
        peer: u64,
        req: &bitdaemon_net::peer_wire::HashRequestWire,
        out: &mut Vec<TorrentEffect>,
    ) {
        let resp = (|| {
            let fs = self.files.as_ref()?;
            let f = fs.file_index_for_root(&req.file_root);
            if usize::try_from(f.0).ok()? >= self.merkle_trees.len() {
                return None;
            }
            self.merkle_trees[f.0 as usize].get_hashes(
                req.base as usize,
                req.index as usize,
                req.count as usize,
                req.proof_layers as usize,
            )
        })();
        let msg = match resp {
            Some(hashes) => PeerMessage::Hashes {
                req: req.clone(),
                hashes,
            },
            None => PeerMessage::HashReject(req.clone()),
        };
        out.push(TorrentEffect::SendTo { peer, msg });
    }

    /// `torrent::add_hashes` — feed received `hashes` into the
    /// merkle trees; verified pieces flip to `have`, mismatches
    /// mark the piece failed for re-download.
    pub fn on_hashes(
        &mut self,
        req: &bitdaemon_net::peer_wire::HashRequestWire,
        hashes: &[bitdaemon_common::Sha256Hash],
        out: &mut Vec<TorrentEffect>,
    ) {
        let Some(fs) = self.files.clone() else {
            return;
        };
        let f = fs.file_index_for_root(&req.file_root);
        let Ok(fidx) = usize::try_from(f.0) else {
            return;
        };
        if fidx >= self.merkle_trees.len() {
            return;
        }
        let hp_req = bitdaemon_disk::hash_picker::HashRequest {
            file: f,
            base: req.base,
            index: req.index,
            count: req.count,
            proof_layers: req.proof_layers,
        };
        let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(&fs, &mut self.merkle_trees);
        let result = picker.add_hashes(&hp_req, hashes);
        drop(picker);
        // the served chunk is no longer throttled
        self.hash_requests_sent.remove(&(f.0, req.index));
        for (piece, _blocks) in result.hash_failed {
            self.piece_verified(piece, false, out);
        }
        for piece in result.hash_passed {
            if !self.have.get_bit(piece.0 as usize) {
                self.piece_verified(piece, true, out);
            }
        }
    }

    /// `bt_peer_connection::maybe_send_hash_request` — pick the next
    /// hash layer the peer can serve us (its have-bitfield gates the
    /// request) and queue the wire message.
    pub fn maybe_send_hash_request(&mut self, peer: u64, out: &mut Vec<TorrentEffect>) {
        if self.merkle_trees.is_empty() {
            return;
        }
        let Some(fs) = self.files.clone() else {
            return;
        };
        let (Some(c), Some(pieces)) = (
            self.peers.get(&peer),
            self.peers.get(&peer).map(|c| c.peer_pieces.clone()),
        ) else {
            return;
        };
        // upstream only drives `hash_request` on `protocol_v2`
        // connections; on a v1-protocol peer the message is a
        // protocol violation and gets the connection dropped.
        if !c.protocol_v2 {
            return;
        }
        let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(&fs, &mut self.merkle_trees);
        let req = picker.pick_hashes(&pieces);
        if req.file.0 < 0 {
            return;
        }
        // throttle: upstream keeps `last_request` per chunk in the
        // picker; ours lives here since the picker is per-call.
        let key = (req.file.0, req.index);
        let now = std::time::Instant::now();
        if self
            .hash_requests_sent
            .get(&key)
            .is_some_and(|t| now.duration_since(*t) < std::time::Duration::from_secs(3))
        {
            return;
        }
        self.hash_requests_sent.insert(key, now);
        let Some(root_idx) = usize::try_from(req.file.0).ok() else {
            return;
        };
        let _ = root_idx;
        out.push(TorrentEffect::SendTo {
            peer,
            msg: PeerMessage::HashRequest(bitdaemon_net::peer_wire::HashRequestWire {
                file_root: fs.root(req.file),
                base: req.base,
                index: req.index,
                count: req.count,
                proof_layers: req.proof_layers,
            }),
        });
    }

    /// `torrent::hashes_rejected` — the remote refused; upstream
    /// un-marks the request so another peer can be asked.
    pub fn on_hash_reject(&mut self, req: &bitdaemon_net::peer_wire::HashRequestWire) {
        let Some(fs) = self.files.clone() else {
            return;
        };
        let f = fs.file_index_for_root(&req.file_root);
        let hp_req = bitdaemon_disk::hash_picker::HashRequest {
            file: f,
            base: req.base,
            index: req.index,
            count: req.count,
            proof_layers: req.proof_layers,
        };
        let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(&fs, &mut self.merkle_trees);
        picker.hashes_rejected(&hp_req);
        drop(picker);
        self.hash_requests_sent.remove(&(f.0, req.index));
    }

    /// `torrent::get_piece_to_super_seed` — pick the lowest-
    /// availability piece the remote lacks, preferring pieces no
    /// other peer is already super-seeded on.
    #[must_use]
    pub fn get_piece_to_super_seed(&self, remote_bits: &Bitfield) -> Option<PieceIndex> {
        self.get_piece_to_super_seed_ex(remote_bits, None)
    }

    /// `get_piece_to_super_seed` variant that also counts an extra
    /// connection — used during `send_bitfield`, when the new conn
    /// is not registered in `peers` yet but its leaked slots must
    /// still bias the second bootstrap pick (upstream iterates
    /// `*this`, which already includes the sending connection).
    #[must_use]
    pub fn get_piece_to_super_seed_ex(
        &self,
        remote_bits: &Bitfield,
        extra: Option<&crate::peer_conn::PeerConn>,
    ) -> Option<PieceIndex> {
        let mut min_avail = 9999i32;
        let mut avail: Vec<PieceIndex> = Vec::new();
        for i in 0..self.num_pieces {
            if (i as usize) < remote_bits.size() && remote_bits.get_bit(i as usize) {
                continue;
            }
            let mut availability = 0i32;
            if let Some(c) = extra {
                if c.superseed_pieces.contains(&Some(PieceIndex(i))) {
                    availability = 999;
                } else if (i as usize) < c.peer_pieces.size() && c.peer_pieces.get_bit(i as usize) {
                    availability += 1;
                }
            }
            for c in self.peers.values() {
                if availability == 999 {
                    break;
                }
                if c.superseed_pieces.contains(&Some(PieceIndex(i))) {
                    availability = 999;
                    break;
                }
                if (i as usize) < c.peer_pieces.size() && c.peer_pieces.get_bit(i as usize) {
                    availability += 1;
                }
            }
            if availability > min_avail {
                continue;
            }
            if availability < min_avail {
                min_avail = availability;
                avail.clear();
            }
            avail.push(PieceIndex(i));
        }
        if avail.is_empty() {
            return None;
        }
        Some(avail[(rand::random::<u32>() as usize) % avail.len()])
    }

    /// `peer_connection::superseed_piece` — leak `new` to the peer
    /// (a `have`), replacing `replace` in the two leaked slots;
    /// `None` ends super-seeding for this peer by sending the real
    /// bitfield (`have_all` when FAST was negotiated).
    fn superseed_piece(
        &mut self,
        peer: u64,
        replace: Option<PieceIndex>,
        new: Option<PieceIndex>,
        out: &mut Vec<TorrentEffect>,
    ) {
        match new {
            None => {
                let active = self
                    .peers
                    .get(&peer)
                    .is_some_and(|c| c.superseed_pieces.iter().any(Option::is_some));
                if !active {
                    return;
                }
                let fast = self.peers.get(&peer).is_some_and(|c| c.supports_fast);
                if let Some(c) = self.peers.get_mut(&peer) {
                    c.superseed_pieces = [None, None];
                }
                // `write_bitfield` — full state ends super-seeding;
                // a super-seeder is a seed, so FAST peers get
                // `have_all`, others the full bitfield.
                let bits: Vec<u8> = self.have.bytes().into();
                let msg = if fast {
                    PeerMessage::HaveAll
                } else {
                    PeerMessage::Bitfield(bits.into())
                };
                out.push(TorrentEffect::SendTo { peer, msg });
            }
            Some(p) => {
                out.push(TorrentEffect::SendTo {
                    peer,
                    msg: PeerMessage::Have(p),
                });
                if let Some(c) = self.peers.get_mut(&peer) {
                    // upstream moves the replaced piece to the tail
                    // slot so both leaked pieces stay requestable.
                    if c.superseed_pieces[0] == replace {
                        c.superseed_pieces.swap(0, 1);
                    }
                    c.superseed_pieces[1] = c.superseed_pieces[0];
                    c.superseed_pieces[0] = Some(p);
                }
            }
        }
    }

    /// `peer_connection::second_tick` super-seed recovery — a peer
    /// uninterested for > 10 s might be waiting on a leaked piece it
    /// already has (re-pick stall): leak it a fresh piece.
    pub fn super_seed_second_tick(&mut self, out: &mut Vec<TorrentEffect>) {
        // upstream gate: `super_seeding && ready_for_connections`.
        if !self.super_seeding || self.paused || self.now_ms <= 0 {
            return;
        }
        let stale: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| !c.peer_interested && c.became_uninterested_ms + 10_000 < self.now_ms)
            .map(|(pid, _)| *pid)
            .collect();
        for pid in stale {
            let bits = self
                .peers
                .get(&pid)
                .map(|c| c.peer_pieces.clone())
                .unwrap_or_default();
            let next = self.get_piece_to_super_seed(&bits);
            // `superseed_piece(-1, …)` — no replace.
            self.superseed_piece(pid, None, next, out);
        }
    }

    /// `peer_connection::incoming_have` super-seed hook: when the
    /// remote completed a piece we super-seeded it, pick the next
    /// one and `have`-announce it.
    fn on_superseeded_have(&mut self, peer: u64, piece: PieceIndex, out: &mut Vec<TorrentEffect>) {
        let bits = self
            .peers
            .get(&peer)
            .map(|c| c.peer_pieces.clone())
            .unwrap_or_default();
        let next = self.get_piece_to_super_seed(&bits);
        self.superseed_piece(peer, Some(piece), next, out);
    }

    /// `request_a_block` for one peer — `drive_requests` emits the
    /// wire `Request`s the caller sends.
    /// `torrent::update_peer_interest` + `peer_connection::
    /// update_interest` — recompute whether the peer has anything
    /// we want, then send `Interested`/`NotInterested` on the wire
    /// when the state flips. Upstream recalcs per peer on
    /// bitfield/have/dont_have and when our own bitfield changes
    /// (piece verified → maybe not interesting anymore).
    pub fn update_interest(&mut self, peer_id: u64, out: &mut Vec<TorrentEffect>) {
        let seed = self.is_seed();
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        // recompute `m_interesting` — upstream re-evaluates the
        // full predicate each time (sticky-set is a bug: we'd never
        // send NotInterested after gaining the last wanted piece).
        c.interesting = (0..self.num_pieces)
            .any(|i| c.has_piece(PieceIndex(i)) && !self.have.get_bit(i as usize));
        // seeds are never interested (upstream: interesting only
        // evaluated for downloaders).
        let want = !seed && c.interesting && !c.remote_upload_only;
        if want == c.we_sent_interested {
            return;
        }
        c.we_sent_interested = want;
        out.push(TorrentEffect::SendTo {
            peer: peer_id,
            msg: if want {
                PeerMessage::Interested
            } else {
                PeerMessage::NotInterested
            },
        });
    }

    /// `peer_connection::update_interest` broadcast — after our own
    /// bitfield changed (`piece_verified`), every peer's interest
    /// may flip.
    pub fn update_interest_all(&mut self, out: &mut Vec<TorrentEffect>) {
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.update_interest(id, out);
        }
    }

    pub fn drive_peer(&mut self, peer_id: u64, out: &mut Vec<TorrentEffect>) {
        // the full drive needs picker + pipeline borrow — collect
        // requests then translate to effects
        if self.picker.is_none() {
            return;
        }
        let mut opts = if self.sequential {
            PickerOptions::RAREST_FIRST | PickerOptions::SEQUENTIAL
        } else {
            PickerOptions::RAREST_FIRST
        };
        if !self.time_critical.is_empty() {
            opts = opts | PickerOptions::TIME_CRITICAL;
        }
        // build peer ctx from an immutable snapshot first
        let (
            key,
            allowed_fast,
            suggested,
            peer_pieces,
            peer_choked,
            outstanding,
            desired_queue_size,
            payload_rate,
        ) = {
            let Some(c) = self.peers.get(&peer_id) else {
                return;
            };
            if c.peer_choked || c.peer_pieces.size() == 0 {
                return;
            }
            (
                c.peer_key(),
                c.allowed_fast.clone(),
                c.suggested_pieces.clone(),
                c.peer_pieces.clone(),
                c.peer_choked,
                c.outstanding(),
                c.pipeline.desired_queue_size(),
                i64::from(c.rate_down_speed),
            )
        };
        // v2 hash exchange — request missing tree layers alongside
        // data requests (upstream `maybe_send_hash_request`).
        if !self.merkle_trees.is_empty() {
            self.maybe_send_hash_request(peer_id, out);
        }
        let mut picked = Vec::new();
        // simple drive: ask the picker for blocks this peer has
        let mut pc_stats = crate::piece_picker::NullPickerStats;
        let tctx = crate::request_blocks::TorrentRequestCtx {
            is_seed: self.is_seed(),
            upload_mode: self.upload_mode,
            valid_metadata: true,
            files_checked: true,
            // graceful pause stops picking new blocks
            graceful_pause: self.graceful_pause,
            time_critical_mode: false,
            num_peers: self.peers.len() as i32,
            whole_pieces_threshold: 0,
            strict_end_game_mode: false,
            piece_length: self.piece_length,
            block_size: 0x4000,
        };
        let pctx = crate::request_blocks::PeerRequestCtx {
            no_download: false,
            disconnecting: false,
            desired_queue_size,
            outstanding: &outstanding,
            prefer_contiguous_blocks: 0,
            download_payload_rate: payload_rate,
            peer_choked,
            allowed_fast: &allowed_fast,
            peer_pieces: &peer_pieces,
            peer_key: key,
            picker_options: opts,
            suggested_pieces: &suggested,
        };
        let Some(p) = self.picker.as_mut() else {
            return;
        };
        let (_invoked, eg) = crate::request_blocks::request_a_block_out(
            p,
            &tctx,
            &pctx,
            &mut pc_stats,
            |b, busy| {
                picked.push((b, busy));
                true
            },
        );
        // `request_blocks` upstream posts `picker_log_alert` — the
        // picker's interesting-block set for this decision.
        if !picked.is_empty() {
            let mut bits = vec![0u8; (self.num_pieces.max(0) as usize).div_ceil(8)];
            for (b, _) in &picked {
                let i = b.piece_index.0 as usize;
                if i / 8 < bits.len() {
                    bits[i / 8] |= 1 << (7 - (i % 8));
                }
            }
            let remote = self
                .peers
                .get(&peer_id)
                .and_then(|c| c.remote)
                .map_or_else(String::new, |ep| ep.to_string());
            let _ = remote;
            out.push(TorrentEffect::Alert(crate::alerts::Alert::PickerLog {
                ih: self.info_hash,
                picker_flags: 0,
                blocks: bits,
            }));
        }
        if let Some(e) = eg
            && let Some(c2) = self.peers.get_mut(&peer_id)
        {
            // `m_endgame_mode` — drives `desired_queue_size()` → 1
            // and lets the picker serve busy blocks.
            c2.pipeline.endgame = e;
        }
        for (b, busy) in picked {
            if self
                .picker
                .as_mut()
                .is_some_and(|p| p.mark_as_downloading(b, key, opts))
            {
                let now_ms = self.now_ms;
                let start = b.block_index * 0x4000;
                let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                    i64::from(f.piece_size(b.piece_index))
                });
                let req_len = (psize - i64::from(start)).min(0x4000) as i32;
                if let Some(c2) = self.peers.get_mut(&peer_id) {
                    c2.send_request(b, busy, false, req_len, now_ms);
                }
                // `peer_connection::add_request` posts
                // `block_downloading_alert` once the block is
                // marked downloading in the picker.
                let remote = self
                    .peers
                    .get(&peer_id)
                    .and_then(|c| c.remote)
                    .map_or_else(String::new, |ep| ep.to_string());
                out.push(TorrentEffect::Alert(
                    crate::alerts::Alert::BlockDownloading {
                        ih: self.info_hash,
                        remote,
                        piece: b.piece_index,
                        block: b.block_index as u32,
                    },
                ));
                // upstream
                // clamp la longueur a  — le
                // dernier bloc d'une piece partielle n'est jamais
                // 0x4000 (les pairs stricts droppent une requete
                // hors limites).
                out.push(TorrentEffect::SendTo {
                    peer: peer_id,
                    msg: PeerMessage::Request(PeerRequest {
                        piece: b.piece_index,
                        start,
                        length: req_len,
                    }),
                });
            }
        }
    }

    /// `web_seed_peer` pseudo-identity in the picker — a web seed
    /// counts as a virtual peer upstream (`web_peer_connection`
    /// derives `peer_connection`); blocks it serves are tracked
    /// under this key so real peers never double-request them.
    pub const WEB_SEED_PEER: u64 = u64::MAX;

    /// `web_peer_connection::write_request` drive — pick up to
    /// `max_blocks` blocks the web seed can serve (it has every
    /// piece) and mark them `requested` under [`Self::WEB_SEED_PEER`].
    /// Returns the url (round-robin over `web_seeds`) and the
    /// picked block requests, or `None` when nothing applies.
    pub fn next_web_seed_blocks(&mut self, max_blocks: i32) -> Option<(String, Vec<PeerRequest>)> {
        if self.web_seeds.is_empty()
            || self.is_seed()
            || self.picker.is_none()
            || self.files.is_none()
        {
            return None;
        }
        if !self.web_seed_refcounted {
            // virtual `peer_has` — the seed has every piece
            if let Some(p) = self.picker.as_mut() {
                p.inc_refcount_all();
            }
            self.web_seed_refcounted = true;
        }
        let all = Bitfield::with_value(self.num_pieces as usize, true);
        let tctx = crate::request_blocks::TorrentRequestCtx {
            is_seed: false,
            upload_mode: false,
            valid_metadata: true,
            files_checked: true,
            graceful_pause: self.graceful_pause,
            time_critical_mode: false,
            num_peers: (self.peers.len() + 1) as i32,
            whole_pieces_threshold: 0,
            strict_end_game_mode: false,
            piece_length: self.piece_length,
            block_size: 0x4000,
        };
        let allowed_fast = Vec::new();
        let suggested = Vec::new();
        let outstanding: Vec<_> = self.web_seed_outstanding.iter().copied().collect();
        let pctx = crate::request_blocks::PeerRequestCtx {
            no_download: false,
            disconnecting: false,
            desired_queue_size: max_blocks,
            outstanding: &outstanding,
            prefer_contiguous_blocks: 0,
            download_payload_rate: 0,
            peer_choked: false,
            allowed_fast: &allowed_fast,
            peer_pieces: &all,
            peer_key: Some(Self::WEB_SEED_PEER),
            picker_options: PickerOptions::RAREST_FIRST,
            suggested_pieces: &suggested,
        };
        let mut picked = Vec::new();
        let mut pc_stats = crate::piece_picker::NullPickerStats;
        let picker = self.picker.as_mut()?;
        let (got, _) = crate::request_blocks::request_a_block_out(
            picker,
            &tctx,
            &pctx,
            &mut pc_stats,
            |b, busy| {
                picked.push((b, busy));
                picked.len() < max_blocks as usize
            },
        );
        if !got || picked.is_empty() {
            return None;
        }
        let mut reqs = Vec::with_capacity(picked.len());
        for (b, _busy) in picked {
            if !self.picker.as_mut().is_some_and(|p| {
                p.mark_as_downloading(b, Some(Self::WEB_SEED_PEER), PickerOptions::RAREST_FIRST)
            }) {
                continue;
            }
            self.web_seed_outstanding.insert(b);
            if !(0..self.num_pieces).contains(&b.piece_index.0) {
                continue;
            }
            let start = b.block_index * 0x4000;
            let piece_size = self.files.as_ref().map_or(self.piece_length, |f| {
                i64::from(f.piece_size(b.piece_index))
            });
            let length = (piece_size - i64::from(start)).min(0x4000) as i32;
            reqs.push(PeerRequest {
                piece: b.piece_index,
                start,
                length,
            });
        }
        if reqs.is_empty() {
            return None;
        }
        let url = self.web_seeds[self.web_seed_idx % self.web_seeds.len()].clone();
        self.web_seed_idx += 1;
        Some((url, reqs))
    }

    /// one web-seed block arrived — `mark_as_writing` + emit the
    /// disk write (the same path a wire `Piece` takes).
    pub fn on_web_seed_block(
        &mut self,
        req: PeerRequest,
        data: Vec<u8>,
        out: &mut Vec<TorrentEffect>,
    ) {
        let b = bitdaemon_common::types::PieceBlock {
            piece_index: req.piece,
            block_index: req.start / 0x4000,
        };
        self.web_seed_outstanding.remove(&b);
        if let Some(p) = self.picker.as_mut() {
            p.mark_as_writing(b, Some(Self::WEB_SEED_PEER));
        }
        out.push(TorrentEffect::DiskWrite {
            peer: Self::WEB_SEED_PEER,
            req,
            data,
        });
    }

    /// a web-seed fetch failed — release the blocks so they can be
    /// re-requested from a real peer or another seed (`web_seed`
    /// `fail_count`→`abort_download` upstream path).
    pub fn on_web_seed_error(&mut self, reqs: &[PeerRequest]) {
        for r in reqs {
            let b = bitdaemon_common::types::PieceBlock {
                piece_index: r.piece,
                block_index: r.start / 0x4000,
            };
            self.web_seed_outstanding.remove(&b);
            if let Some(p) = self.picker.as_mut() {
                p.abort_download(b, Some(Self::WEB_SEED_PEER));
            }
        }
    }

    /// `is_seed`-side helper — all blocks of `piece` have been
    /// written (`picker` tracks open pieces; a piece leaves the
    /// open queue when every block is finished).
    /// `on_disk_write_complete` — the block landed on disk;
    /// upstream `picker().mark_as_finished(block, nullptr)`.
    pub fn mark_block_finished(&mut self, piece: PieceIndex, start: i32) {
        if let Some(p) = self.picker.as_mut() {
            p.mark_as_finished(
                bitdaemon_common::types::PieceBlock {
                    piece_index: piece,
                    block_index: start / 16384,
                },
                None,
            );
        }
    }

    pub fn piece_complete(&self, piece: PieceIndex) -> bool {
        // conservative: written when our have-bit set OR the picker
        // reports the piece fully downloaded; full fidelity needs
        // per-block state → treat picker-open as incomplete.
        if (piece.0 as usize) < self.have.size() && self.have.get_bit(piece.0 as usize) {
            return true;
        }
        // `is_piece_finished` = every block written/finished
        self.picker
            .as_ref()
            .is_some_and(|p| p.is_piece_finished(piece))
    }

    /// `we_have(piece)` — the piece passed its hash and sits in
    /// our bitfield.
    #[must_use]
    pub fn have_piece(&self, piece: PieceIndex) -> bool {
        (piece.0 as usize) < self.have.size() && self.have.get_bit(piece.0 as usize)
    }

    /// `picker.is_hashing(piece)` — a hash job is in flight for
    /// `piece`; upstream gates re-hashing on it in
    /// `on_disk_write_complete`.
    #[must_use]
    pub fn is_hashing(&self, piece: PieceIndex) -> bool {
        self.picker.as_ref().is_some_and(|p| p.is_hashing(piece))
    }

    /// `picker.started_hash_job(piece)` — mark the hash job in flight.
    pub fn hashing_started(&mut self, piece: PieceIndex) {
        if let Some(p) = self.picker.as_mut() {
            p.started_hash_job(piece);
        }
    }

    /// `picker.completed_hash_job(piece)` — the hash job landed;
    /// tolerant of callers that verify without a prior `started`.
    pub fn hashing_done(&mut self, piece: PieceIndex) {
        if let Some(p) = self.picker.as_mut() {
            if p.is_hashing(piece) {
                p.completed_hash_job(piece);
            }
        }
    }

    /// `hash_for_piece` — the expected SHA-1, if metadata held.
    #[must_use]
    pub fn expected_hash(&self, piece: PieceIndex) -> Option<Sha1Hash> {
        self.piece_hashes
            .as_ref()
            .and_then(|h| h.get(piece.0 as usize).copied())
            // v2-only torrents have no v1 piece layer — upstream
            // `hash_for_piece` yields the zero hash there, and the
            // block-level SHA-256 merkle path verifies instead.
            .filter(|h| *h != Sha1Hash::default())
    }

    /// `piece_finished` — verification result. `passed` → have-bit
    /// + broadcast; failed → restore want + smart_ban hook point.
    pub fn piece_verified(
        &mut self,
        piece: PieceIndex,
        passed: bool,
        out: &mut Vec<TorrentEffect>,
    ) {
        // the hash job landed either way — clear `dp.hashing`
        // (`picker.completed_hash_job` upstream).
        self.hashing_done(piece);
        // `torrent_plugin::on_piece_pass`/`on_piece_failed` —
        // upstream fires both inside `piece_finished`.
        for ext in &self.extensions {
            if passed {
                ext.on_piece_pass(piece);
            } else {
                ext.on_piece_failed(piece);
            }
        }
        if !passed {
            // `m_total_failed` — the discarded payload bytes.
            self.failed_bytes += self
                .files
                .as_ref()
                .map_or(self.piece_length, |f| i64::from(f.piece_size(piece)));
        }
        if passed {
            if (piece.0 as usize) < self.have.size() && !self.have.get_bit(piece.0 as usize) {
                self.have.set_bit(piece.0 as usize);
                // `total_done` — last piece counts its tail size,
                // not a full `piece_length` (`files` gives the
                // exact per-piece size).
                let psize = self
                    .files
                    .as_ref()
                    .map_or(self.piece_length, |f| i64::from(f.piece_size(piece)));
                self.verified_bytes += psize;
            }
            // `picker.piece_passed` — passed_hash_check +
            // `account_have` + flush; without it the piece stays in
            // the download queue and every in-flight write
            // re-triggers a hash job.
            if let Some(p) = self.picker.as_mut() {
                p.piece_passed(piece);
            }
            self.need_save_resume = true;
            out.push(TorrentEffect::PiecePassed(piece));
            // broadcast `have` to every connected peer
            for id in self.peers.keys().copied().collect::<Vec<_>>() {
                out.push(TorrentEffect::SendTo {
                    peer: id,
                    msg: PeerMessage::Have(piece),
                });
            }
            // upstream `torrent::finished()` fires on `is_finished()`
            // — all *wanted* pieces, not necessarily all pieces (a
            // file priority of 0 makes its pieces unwanted).
            let all = self.have.count() as i32 == self.num_pieces;
            let want_done = all || self.picker.as_ref().is_some_and(|p| p.is_finished());
            if !self.finished && want_done {
                self.finished = true;
                self.finished_at.get_or_insert_with(Instant::now);
                out.push(TorrentEffect::BecameSeed);
                // upstream `finished()` drops upload-only peers —
                // nothing more to exchange either way.
                let uo: Vec<u64> = self
                    .peers
                    .iter()
                    .filter(|(_, c)| c.remote_upload_only)
                    .map(|(id, _)| *id)
                    .collect();
                for id in uo {
                    out.push(TorrentEffect::DisconnectPeer {
                        peer: id,
                        reason: "torrent finished",
                    });
                }
                // `stop_when_ready` — pause as soon as the download
                // completes (upstream pauses the torrent in
                // `files_checked`/piece-finish path).
                if self.stop_when_ready && !self.paused {
                    self.paused = true;
                }
            }
            if all {
                self.seeding = true;
                self.seeding_since.get_or_insert_with(Instant::now);
            }
            // our bitfield changed → peers may stop being
            // interesting (`update_peer_interest` upstream).
            self.update_interest_all(out);
            // `remove_time_critical_piece` — the piece just
            // verified; `alert_when_available` → read it back
            if self
                .time_critical
                .remove(&piece)
                .is_some_and(|(_, alert)| alert)
            {
                out.push(TorrentEffect::ReadPiece { piece });
            }
        } else {
            if let Some(p) = self.picker.as_mut() {
                p.restore_piece(piece, &[]);
            }
            out.push(TorrentEffect::PieceFailed(piece));
            for id in self.peers.keys().copied().collect::<Vec<_>>() {
                self.drive_peer(id, out);
            }
        }
    }

    /// `piece_verified` + the `smart_ban` bookkeeping upstream does
    /// in `on_piece_finished`/`on_piece_failed`: on failure the
    /// per-block CRC of every downloader is recorded, on success the
    /// stored CRCs are re-checked. `addr` resolves a peer id, and
    /// `read_digest` re-reads a block's SHA-1 from storage (the
    /// session performs the disk read; `None` = skip, like upstream
    /// ignoring read errors). Bans become `DisconnectPeer` effects.
    pub fn piece_verified_ban(
        &mut self,
        piece: PieceIndex,
        piece_size: i64,
        passed: bool,
        out: &mut Vec<TorrentEffect>,
        addr: impl FnMut(u64) -> std::net::IpAddr,
        mut read_digest: impl FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) {
        if passed {
            for d in self
                .smart_ban
                .on_piece_passed(piece, piece_size, &mut read_digest)
            {
                if let BanDecision::Ban(peer) = d {
                    self.remove_peer(peer, out);
                    out.push(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "smart_ban: corrupt block",
                    });
                }
            }
        } else {
            let downloaders = self
                .picker
                .as_ref()
                .map(|p| p.get_downloaders(piece))
                .unwrap_or_default();
            if let BanDecision::Ban(peer) =
                self.smart_ban
                    .on_piece_failed(piece, piece_size, &downloaders, addr, read_digest)
            {
                self.remove_peer(peer, out);
                out.push(TorrentEffect::DisconnectPeer {
                    peer,
                    reason: "smart_ban: corrupt block",
                });
            }
        }
        self.piece_verified(piece, passed, out);
    }

    /// Build the per-file v2 merkle trees from `piece_layers`
    /// (upstream materializes them from `atp.merkle_trees` at
    /// `add_torrent`; same result — verified sparse vector or
    /// full layer).
    fn build_merkle_trees(&mut self, ti: &TorrentInfo) {
        let fs = ti.files();
        self.merkle_trees.clear();
        for i in fs.file_range() {
            // pad and empty files have no content and no merkle tree —
            // upstream `generate()` emits no `pieces root` for them.
            // Keep a placeholder so merkle_trees stays indexed by file
            // index.
            if fs.pad_file_at(i) || fs.file_size(i) == 0 {
                self.merkle_trees
                    .push(bitdaemon_core::merkle_tree::MerkleTree::default());
                continue;
            }
            let mut tree = bitdaemon_core::merkle_tree::MerkleTree::new(
                fs.file_num_blocks(i) as usize,
                fs.blocks_per_piece() as usize,
                fs.root(i),
            );
            if let Some(layer) = ti.piece_layer(i) {
                tree.load_piece_layer(layer);
            }
            self.merkle_trees.push(tree);
        }
    }

    /// v2-only torrent (no v1 piece layer to compare).
    #[must_use]
    pub fn is_v2_only(&self) -> bool {
        self.ti
            .as_ref()
            .is_some_and(|t| t.v2() && t.info_hash_v1().is_none())
    }

    /// The `block_hashes` span to pass to `async_hash`: sized to
    /// `blocks_in_piece2` for v2-only torrents, empty otherwise
    /// (upstream `hash_job` only fills v2 hashes when requested).
    #[must_use]
    pub fn hash_job_v2_span(&self, piece: PieceIndex) -> Vec<bitdaemon_common::Sha256Hash> {
        if !self.is_v2_only() {
            return Vec::new();
        }
        self.files
            .as_ref()
            .map(|fs| {
                vec![bitdaemon_common::Sha256Hash::default(); fs.blocks_in_piece2(piece) as usize]
            })
            .unwrap_or_default()
    }

    /// `on_blocks_hashed` — feed computed block SHA-256s into the
    /// file's merkle tree; true when the piece is fully verified
    /// (upstream `torrent::on_blocks_hashed` return semantics).
    pub fn verify_v2_blocks(
        &mut self,
        piece: PieceIndex,
        block_hashes: &[bitdaemon_common::Sha256Hash],
    ) -> bool {
        let Some(fs) = self.files.clone() else {
            return false;
        };
        let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(&fs, &mut self.merkle_trees);
        for (i, h) in block_hashes.iter().enumerate() {
            let r = picker.set_block_hash(piece, (i * 16384) as i32, h);
            match r.status {
                bitdaemon_disk::hash_picker::SetBlockHashStatus::Success
                | bitdaemon_disk::hash_picker::SetBlockHashStatus::Unknown => {}
                bitdaemon_disk::hash_picker::SetBlockHashStatus::BlockHashFailed
                | bitdaemon_disk::hash_picker::SetBlockHashStatus::PieceHashFailed => {
                    return false;
                }
            }
        }
        picker.piece_verified(piece)
    }

    /// `flags()` — assemble the `torrent_flags` bitset from the
    /// scattered per-flag fields (upstream keeps one bitfield).
    #[must_use]
    pub fn flags(&self) -> bitdaemon_common::flags::TorrentFlags {
        use bitdaemon_common::flags::TorrentFlags as F;
        let mut f = F::NONE;
        if self.paused {
            f |= F::PAUSED;
        }
        if self.auto_managed {
            f |= F::AUTO_MANAGED;
        }
        if self.sequential {
            f |= F::SEQUENTIAL_DOWNLOAD;
        }
        if self.upload_mode {
            f |= F::UPLOAD_MODE;
        }
        if self.share_mode {
            f |= F::SHARE_MODE;
        }
        if self.super_seeding {
            f |= F::SUPER_SEEDING;
        }
        if self.need_save_resume {
            f |= F::NEED_SAVE_RESUME;
        }
        if self.seed_mode {
            f |= F::SEED_MODE;
        }
        f
    }

    /// `set_flags`/`unset_flags` — apply `flags` under `mask`
    /// (upstream `torrent::set_flags`): only the masked bits are
    /// touched. Returns whether `paused` changed (session decides
    /// the announce side-effects).
    pub fn set_flags(
        &mut self,
        flags: bitdaemon_common::flags::TorrentFlags,
        mask: bitdaemon_common::flags::TorrentFlags,
    ) -> bool {
        use bitdaemon_common::flags::TorrentFlags as F;
        let was_paused = self.paused;
        if mask.contains(F::PAUSED) {
            self.paused = flags.contains(F::PAUSED);
        }
        if mask.contains(F::AUTO_MANAGED) {
            self.auto_managed = flags.contains(F::AUTO_MANAGED);
        }
        if mask.contains(F::SEQUENTIAL_DOWNLOAD) {
            self.sequential = flags.contains(F::SEQUENTIAL_DOWNLOAD);
        }
        if mask.contains(F::UPLOAD_MODE) {
            self.upload_mode = flags.contains(F::UPLOAD_MODE);
        }
        if mask.contains(F::SHARE_MODE) {
            self.share_mode = flags.contains(F::SHARE_MODE);
        }
        if mask.contains(F::SUPER_SEEDING) {
            self.super_seeding = flags.contains(F::SUPER_SEEDING);
        }
        if mask.contains(F::NEED_SAVE_RESUME) {
            self.need_save_resume = flags.contains(F::NEED_SAVE_RESUME);
        }
        // upstream: clearing seed_mode via flags leaves seed mode the
        // "check files" way — promise was wrong → recheck.
        if mask.contains(F::SEED_MODE) && self.seed_mode && !flags.contains(F::SEED_MODE) {
            self.leave_seed_mode(true);
        }
        self.paused != was_paused
    }

    /// `leave_seed_mode` — `check_files` mirrors upstream
    /// `seed_mode_t::check_files`: the promise was false → drop
    /// have_all, re-enter `downloading` and force a recheck.
    /// `seed_mode_t::skip_checking` (`check_files = false`) keeps
    /// the data as-is (used by the fastresume-ok path).
    pub fn leave_seed_mode(&mut self, check_files: bool) {
        if !self.seed_mode {
            return;
        }
        self.seed_mode = false;
        // upstream leaves `checking_resume_data` untouched; our
        // recheck request rides `need_recheck` which the session
        // turns into a disk verify pass.
        if check_files {
            self.seeding = false;
            self.finished = false;
            // drop every unverified bit → picker re-requests them
            for i in 0..self.have.size() {
                if !self.seed_verified.get_bit(i) {
                    self.have.clear_bit(i);
                }
            }
            // `force_recheck` — reuse the armed full-recheck path.
            self.checking = false;
            self.arm_full_recheck();
        }
        self.num_verified = 0;
        self.seed_verified = Bitfield::default();
        self.seed_verifying = Bitfield::default();
        self.need_save_resume = true;
    }

    /// `verified(piece)` — seed-mode lazy verification counter.
    pub fn seed_verified_piece(&mut self, piece: PieceIndex) {
        debug_assert!(!self.seed_verified.get_bit(piece.0 as usize));
        self.num_verified += 1;
        self.seed_verified.set_bit(piece.0 as usize);
        self.need_save_resume = true;
    }

    /// `replace_trackers` (upstream `torrent::replace_trackers`) —
    /// swap the whole announce list.
    pub fn replace_trackers(&mut self, trackers: &[(String, i32)]) {
        self.trackers = crate::tracker_list::TrackerList::default();
        for (url, tier) in trackers {
            self.add_tracker(url.clone(), *tier);
        }
        self.need_save_resume = true;
    }

    /// `torrent::write_resume_data` — serialize this torrent's state
    /// through `to_resume_data` + `write_resume_data_buf`
    /// (byte-compatible with upstream `write_resume_data`).
    #[must_use]
    pub fn write_resume_data(&self) -> Vec<u8> {
        let mut atp = AddTorrentParams::default();
        self.to_resume_data(&mut atp);
        bitdaemon_core::resume_data::write_resume_data_buf(&atp)
    }

    /// `torrent::get_full_peer_list` — every known swarm peer
    /// (connected or not); upstream `peer_list_entry` ≈
    /// `TorrentPeer` here.
    #[must_use]
    pub fn full_peer_list(&self) -> &[bitdaemon_net::torrent_peer::TorrentPeer] {
        self.peer_list.peers()
    }

    /// `torrent_handle::add_http_seed` — **deprecated upstream
    /// no-op**: libtorrent 2.x removed BEP-17 fetching ("removed
    /// support for BEP 17 web seeds (in favor of BEP 19)"); the
    /// handle method is `add_http_seed(std::string const&) {}`.
    /// `atp.http_seeds` (torrent_info ctor input) is the only path
    /// that still lands — merged into `web_seeds` at add time.
    pub fn add_http_seed(&mut self, _url: String) {}

    /// `torrent_handle::remove_http_seed` — deprecated upstream
    /// no-op (`remove_http_seed(std::string const&) {}`).
    pub fn remove_http_seed(&mut self, _url: &str) {}

    /// `torrent_handle::http_seeds` — upstream returns an empty
    /// set (`http_seeds() const { return {}; }`).
    #[must_use]
    pub fn http_seeds(&self) -> &[String] {
        &[]
    }

    /// `set_peer_upload_limit` / `set_peer_download_limit` —
    /// per-peer channel throttles keyed by remote endpoint.
    pub fn set_peer_upload_limit(&mut self, remote: std::net::SocketAddr, limit: i64) {
        self.peer_limits.entry(remote).or_default().0 = limit;
    }

    /// `set_peer_download_limit`.
    pub fn set_peer_download_limit(&mut self, remote: std::net::SocketAddr, limit: i64) {
        self.peer_limits.entry(remote).or_default().1 = limit;
    }

    /// `get_peer_upload_limit` — `-1` when unset/unlimited
    /// (upstream convention).
    #[must_use]
    pub fn peer_upload_limit(&self, remote: std::net::SocketAddr) -> i64 {
        self.peer_limits.get(&remote).map_or(-1, |l| l.0)
    }

    /// `get_peer_download_limit`.
    #[must_use]
    pub fn peer_download_limit(&self, remote: std::net::SocketAddr) -> i64 {
        self.peer_limits.get(&remote).map_or(-1, |l| l.1)
    }

    /// `torrent_handle::add_url_seed` / `add_http_seed`.
    pub fn add_url_seed(&mut self, url: String) {
        if !self.web_seeds.contains(&url) {
            self.web_seeds.push(url);
            self.need_save_resume = true;
        }
    }

    /// `torrent_handle::remove_url_seed` / `remove_http_seed`.
    pub fn remove_url_seed(&mut self, url: &str) {
        if self.web_seeds.iter().any(|u| u == url) {
            self.web_seeds.retain(|u| u != url);
            self.need_save_resume = true;
        }
    }

    /// `torrent_handle::url_seeds` snapshot.
    #[must_use]
    pub fn url_seeds(&self) -> &[String] {
        &self.web_seeds
    }

    /// `set_ban` (upstream `torrent::set_ban`) — ban the swarm entry
    /// for `remote`; an already-connected peer is dropped by the
    /// session's disconnect pump via `peer_disconnected_addr` —
    /// callers pass the effect through as `SessionOut::Disconnect`.
    pub fn ban_peer(&mut self, remote: std::net::SocketAddr) {
        if let Some(i) = self.peer_index(remote) {
            self.peer_list.ban_peer(i);
        }
    }

    /// `prioritize_pieces` (upstream `torrent_handle::prioritize_pieces`
    /// → `torrent::set_piece_priority` per index) — direct piece
    /// overrides; they sit on top of the file-derived priorities.
    pub fn prioritize_pieces(&mut self, prios: &[bitdaemon_common::types::DownloadPriority]) {
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        for (i, prio) in prios.iter().enumerate().take(self.num_pieces as usize) {
            picker.set_piece_priority(PieceIndex(i as i32), *prio);
        }
        self.need_save_resume = true;
    }

    /// `piece_priority(index)` — current picker priority.
    #[must_use]
    pub fn piece_priority(
        &self,
        index: PieceIndex,
    ) -> Option<bitdaemon_common::types::DownloadPriority> {
        self.picker.as_ref().map(|p| p.piece_priority(index))
    }

    /// `set_upload_limit` / `set_download_limit` — per-torrent
    /// bandwidth throttle (bytes/s, 0 = unlimited).
    pub fn set_upload_limit(&mut self, bytes_per_sec: i64) {
        self.upload_limit = bytes_per_sec.max(0);
    }

    /// `set_download_limit`.
    pub fn set_download_limit(&mut self, bytes_per_sec: i64) {
        self.download_limit = bytes_per_sec.max(0);
    }

    /// `set_sequential_download` (upstream `torrent::set_sequential_download`).
    pub fn set_sequential_download(&mut self, yes: bool) {
        self.sequential = yes;
        // upstream: leaving sequential mode re-opens the picker to
        // the full range (`set_sequential_range(0, last_piece)`).
        if !yes {
            if let Some(p) = self.picker.as_mut() {
                p.set_sequential_range(PieceIndex(0), PieceIndex(self.num_pieces - 1));
            }
        }
    }

    /// `torrent::set_sequential_range(first, last)` — bound the
    /// sequential download window (implies sequential mode).
    pub fn set_sequential_range(&mut self, first_piece: PieceIndex, last_piece: PieceIndex) {
        if first_piece.0 < 0 || last_piece < first_piece {
            return;
        }
        self.sequential = true;
        if let Some(p) = self.picker.as_mut() {
            p.set_sequential_range(first_piece, last_piece);
        }
    }

    /// `start_full_recheck` prep — clear `have`/seed state and arm
    /// `checking_pieces`; the session then queues a `DiskHash`
    /// per piece (`torrent::force_recheck` → per-piece hash jobs).
    /// Returns how many pieces were queued (0 = nothing to check).
    pub fn arm_full_recheck(&mut self) -> i32 {
        if self.picker.is_none() || self.checking || self.num_pieces <= 0 {
            return 0;
        }
        self.have.clear_all();
        self.verified_bytes = 0;
        self.finished = false;
        self.seeding = false;
        self.checking = true;
        self.checking_pieces = self.num_pieces;
        self.num_pieces
    }

    /// one recheck hash came back — `checking` clears when the
    /// armed count drains (upstream `torrent::on_piece_hashed`
    /// ends the checking state at the last piece).
    /// returns `true` on the last checked piece (checking ended).
    pub fn recheck_piece_done(&mut self) -> bool {
        if self.checking_pieces > 0 {
            self.checking_pieces -= 1;
            if self.checking_pieces == 0 {
                self.checking = false;
                return true;
            }
        }
        false
    }

    /// `prioritize_files` (upstream `torrent::prioritize_files`) —
    /// merge `new` into `m_file_priority`, then
    /// `update_piece_priorities`: every piece's priority becomes
    /// the max of the files overlapping it.
    pub fn prioritize_files(&mut self, new: &[bitdaemon_common::types::DownloadPriority]) {
        self.need_save_resume = true;
        let Some(fs) = self.files.clone() else {
            return;
        };
        use bitdaemon_common::types::DownloadPriority;
        let n = fs.num_files() as usize;
        self.file_priorities.resize(n, DownloadPriority::DEFAULT);
        for (i, prio) in new.iter().enumerate().take(n) {
            self.file_priorities[i] = *prio;
        }
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        for p in 0..self.num_pieces {
            let piece = PieceIndex(p);
            let prio = fs
                .map_block(piece, 0, i64::from(fs.piece_size(piece)))
                .iter()
                .map(|sl| {
                    self.file_priorities
                        .get(sl.file_index.0 as usize)
                        .copied()
                        .unwrap_or(DownloadPriority::DEFAULT)
                })
                .max()
                .unwrap_or(DownloadPriority::DEFAULT);
            picker.set_piece_priority(piece, prio);
        }
    }

    /// `file_progress` — per-file bytes done (upstream
    /// `torrent::file_progress`): whole pieces come from `have`,
    /// partial pieces from the picker's finished blocks.
    #[must_use]
    pub fn file_progress(&self) -> Vec<i64> {
        const BLOCK: i64 = 0x4000;
        let Some(fs) = &self.files else {
            return Vec::new();
        };
        let mut out = vec![0i64; fs.num_files() as usize];
        for p in 0..self.num_pieces {
            let piece = PieceIndex(p);
            let psize = i64::from(fs.piece_size(piece));
            if self.have.get_bit(p as usize) {
                for sl in fs.map_block(piece, 0, psize) {
                    if sl.file_index.0 >= 0 {
                        out[sl.file_index.0 as usize] += sl.size;
                    }
                }
            } else if let Some(picker) = &self.picker {
                let nb = picker.blocks_in_piece(piece) as i64;
                for b in 0..nb {
                    let blk = PieceBlock {
                        piece_index: piece,
                        block_index: b as i32,
                    };
                    if picker.is_finished_block(blk) {
                        let off = b * BLOCK;
                        for sl in fs.map_block(piece, off, (psize - off).min(BLOCK)) {
                            if sl.file_index.0 >= 0 {
                                out[sl.file_index.0 as usize] += sl.size;
                            }
                        }
                    }
                }
            }
        }
        out
    }

    /// `set_piece_deadline` (upstream `torrent::set_piece_deadline`):
    /// mark `piece` time-critical — bumped to `top_priority`,
    /// and the first critical piece cancels outstanding
    /// non-critical requests (`cancel_non_critical`).
    /// `alert_when_available` = upstream flag: when the piece
    /// completes, `ReadPiece` reads it back and posts
    /// `read_piece_alert`. Already-haved pieces read back now.
    pub fn set_piece_deadline(
        &mut self,
        piece: PieceIndex,
        deadline_ms: i64,
        alert_when_available: bool,
        out: &mut Vec<TorrentEffect>,
    ) {
        if self.picker.is_none() || piece.0 < 0 || piece.0 >= self.num_pieces {
            if alert_when_available {
                out.push(TorrentEffect::ReadPieceFailed {
                    piece,
                    error: "piece deadline on invalid/missing metadata".into(),
                });
            }
            return;
        }
        if self.is_seed() || self.have.get_bit(piece.0 as usize) {
            if alert_when_available {
                out.push(TorrentEffect::ReadPiece { piece });
            }
            return;
        }
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(
                u64::try_from(deadline_ms.max(0)).unwrap_or(u64::MAX),
            );
        let first = self.time_critical.is_empty();
        self.time_critical
            .insert(piece, (deadline, alert_when_available));
        // upstream bumps to `top_priority` unconditionally
        if let Some(p) = self.picker.as_mut() {
            p.set_piece_priority(piece, bitdaemon_common::types::DownloadPriority::TOP);
        }
        if first {
            self.cancel_non_critical(out);
        }
    }

    /// `reset_piece_deadline`.
    pub fn reset_piece_deadline(&mut self, piece: PieceIndex) {
        self.time_critical.remove(&piece);
    }

    /// `peer_connection::second_tick` — a wire request outstanding
    /// longer than `request_timeout` (RTT mean + 4σ, capped by the
    /// setting) snubs the peer: queue target collapses to 1 and
    /// slow-start ends. Upstream keeps the outstanding requests
    /// (cancelling them causes re-pick churn and sawtooth rates);
    /// they're still refunded if the peer later disconnects.
    pub fn check_request_timeouts(&mut self, out: &mut Vec<TorrentEffect>) {
        let now_ms = self.now_ms;
        if now_ms <= 0 {
            return;
        }
        for (peer_id, c) in self.peers.iter_mut() {
            let _ = peer_id;
            let idle = c.pipeline.request_queue.is_empty() && c.pipeline.download_queue.is_empty();
            if c.pipeline.requested_ms == 0 || idle {
                continue;
            }
            if now_ms - c.pipeline.requested_ms <= c.pipeline.request_timeout() * 1000 {
                continue;
            }
            let remote = c.remote.map_or_else(String::new, |ep| ep.to_string());
            // `peer_connection::snub_peer` — snub (idempotent),
            // wipe unsent requests, and free the last eligible
            // in-flight block so another peer can pick it.
            if !c.pipeline.snubbed {
                c.pipeline.snubbed = true;
                out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerSnubbed {
                    ih: self.info_hash,
                    remote: remote.clone(),
                }));
            }
            c.pipeline.slow_start = false;
            let key = c.peer_key();
            let unsent: Vec<PieceBlock> = c
                .pipeline
                .request_queue
                .drain(..)
                .map(|b| b.block)
                .collect();
            if let Some(p) = self.picker.as_mut() {
                for b in unsent {
                    p.abort_download(b, key);
                }
                // only the *last* eligible request each pass — and
                // only when no free blocks remain in its piece
                // (upstream: no point cancelling while other
                // blocks can still complete the piece).
                if let Some(i) = c
                    .pipeline
                    .download_queue
                    .iter()
                    .rposition(|b| !b.timed_out && !b.not_wanted)
                {
                    let r = c.pipeline.download_queue[i].block;
                    let free = p.blocks_in_piece(r.piece_index)
                        - i32::from(p.piece_info(r.piece_index).finished)
                        - i32::from(p.piece_info(r.piece_index).writing)
                        - i32::from(p.piece_info(r.piece_index).requested);
                    if free <= 0 {
                        c.pipeline.download_queue[i].timed_out = true;
                        p.abort_download(r, key);
                        // `peer_connection::snub_peer` posts
                        // `block_timeout_alert` per freed block.
                        out.push(TorrentEffect::Alert(crate::alerts::Alert::BlockTimeout {
                            ih: self.info_hash,
                            remote: remote.clone(),
                            piece: r.piece_index,
                            block: r.block_index as u32,
                        }));
                    }
                }
            }
        }
    }

    /// `cancel_non_critical` — first time-critical piece cancels
    /// every outstanding request not on a critical piece:
    /// `Cancel` wire message + `abort_download` so the picker can
    /// re-pick the block for a fast peer.
    fn cancel_non_critical(&mut self, out: &mut Vec<TorrentEffect>) {
        let critical: std::collections::BTreeSet<PieceIndex> =
            self.time_critical.keys().copied().collect();
        for (id, c) in self.peers.iter_mut() {
            let mut removed = Vec::new();
            for q in [
                &mut c.pipeline.request_queue,
                &mut c.pipeline.download_queue,
            ] {
                let mut i = 0;
                while i < q.len() {
                    if !critical.contains(&q[i].block.piece_index) {
                        removed.push(q.remove(i).block);
                    } else {
                        i += 1;
                    }
                }
            }
            let key = c.peer_key();
            for b in removed {
                //  doit miroirer la requete emise — meme
                // clamp de longueur que .
                let start = b.block_index * 0x4000;
                let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                    i64::from(f.piece_size(b.piece_index))
                });
                out.push(TorrentEffect::SendTo {
                    peer: *id,
                    msg: PeerMessage::Cancel(PeerRequest {
                        piece: b.piece_index,
                        start,
                        length: (psize - i64::from(start)).min(0x4000) as i32,
                    }),
                });
                if let Some(p) = self.picker.as_mut() {
                    p.abort_download(b, key);
                }
            }
        }
    }

    /// `torrent::seed_rank` — auto-manage priority score; verbatim
    /// flag formula (`seed_ratio_not_met`/`no_seeds`/
    /// `recently_started` + seeds/downloaders ratio).
    #[must_use]
    pub fn seed_rank(&self, s: &crate::settings::SessionSettings) -> i32 {
        const SEED_RATIO_NOT_MET: i32 = 0x4000_0000;
        const NO_SEEDS: i32 = 0x2000_0000;
        const RECENTLY_STARTED: i32 = 0x1000_0000;
        const PRIO_MASK: i32 = 0x0fff_ffff;

        if !self.finished {
            return 0;
        }
        let scale: i64 = if self.is_seed() { 1000 } else { 500 };
        let mut ret = 0i32;
        let now = Instant::now();
        let act = now.duration_since(self.started).as_secs() as i64;
        let fin = self
            .finished_at
            .map_or(0, |t| now.duration_since(t).as_secs() as i64);
        let dl = act - fin;
        let downloaded = std::cmp::max(
            self.all_time_download + self.total_download(),
            self.files.as_ref().map_or(0, |f| f.total_size()),
        );
        if fin < i64::from(s.seed_time_limit)
            && dl > 1
            && fin * 100 / dl < i64::from(s.seed_time_ratio_limit)
            && downloaded > 0
            && (self.all_time_upload + self.total_upload()) * 100 / downloaded
                < i64::from(s.share_ratio_limit)
        {
            ret |= SEED_RATIO_NOT_MET;
        }
        if !self.paused && act < 30 * 60 {
            ret |= RECENTLY_STARTED;
        }
        // scrape data wins, like upstream `m_complete`/`m_incomplete`
        // (0xffffff = none); else fall back to the swarm list.
        let scrape = self
            .trackers
            .trackers
            .iter()
            .filter(|e| e.scrape_complete >= 0)
            .max_by_key(|e| e.scrape_complete);
        let self_seed = i64::from(self.is_seed() && !self.paused);
        let (seeds, downloaders) = scrape.map_or_else(
            || {
                (
                    i64::from(self.peer_list.num_seeds()),
                    self.peer_list.num_peers() as i64 - i64::from(self.peer_list.num_seeds()),
                )
            },
            |e| {
                (
                    (e.scrape_complete - self_seed).max(0),
                    e.scrape_incomplete.max(0),
                )
            },
        );
        if seeds == 0 {
            ret |= NO_SEEDS;
            ret |= (downloaders as i32) & PRIO_MASK;
        } else {
            ret |= (((1 + downloaders) * scale / seeds) as i32) & PRIO_MASK;
        }
        ret
    }

    /// payload totals across conns (`m_stat` upstream).
    pub fn total_download(&self) -> i64 {
        self.peers.values().map(|c| c.stat_down_total).sum()
    }

    /// ditto upload.
    pub fn total_upload(&self) -> i64 {
        self.peers.values().map(|c| c.stat_up_total).sum()
    }

    /// `state()` — the coarse lifecycle state (`status().state`
    /// without building the whole snapshot; the session diffs it
    /// for `state_changed_alert`).
    #[must_use]
    pub fn state(&self) -> crate::status::TorrentState {
        use crate::status::TorrentState;
        if self.checking {
            TorrentState::CheckingFiles
        } else if self.magnet && self.picker.is_none() {
            TorrentState::DownloadingMetadata
        } else if self.finished {
            if self.seeding {
                TorrentState::Seeding
            } else {
                TorrentState::Finished
            }
        } else {
            TorrentState::Downloading
        }
    }

    /// `torrent_status()` — the snapshot upstream's `status()`
    /// fills; `queue_pos` comes from the session (ordering lives
    /// there).
    pub fn status(
        &self,
        s: &crate::settings::SessionSettings,
        queue_pos: i32,
    ) -> crate::status::TorrentStatus {
        use crate::status::TorrentStatus;
        let state = self.state();
        let total_size = self.files.as_ref().map_or_else(
            || i64::from(self.num_pieces) * self.piece_length,
            |f| f.total_size(),
        );
        let wanted_pieces = self.picker.as_ref().map_or_else(
            || self.num_pieces,
            |p| {
                (0..self.num_pieces)
                    .filter(|&i| {
                        p.piece_priority(PieceIndex(i))
                            > bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD
                    })
                    .count() as i32
            },
        );
        let (num_complete, num_incomplete, downloaded, last_scrape) = self
            .trackers
            .trackers
            .iter()
            .filter(|e| e.scrape_complete >= 0)
            .max_by_key(|e| e.scrape_complete)
            .map_or((-1, -1, -1, 0), |e| {
                (
                    e.scrape_complete,
                    e.scrape_incomplete,
                    e.scrape_downloaded,
                    e.scrape_time.map_or(0, |t| t.elapsed().as_secs() as i64),
                )
            });
        let total_download = self.total_download();
        let total_upload = self.total_upload();
        let progress = if total_size > 0 {
            (self.verified_bytes as f64 / total_size as f64).min(1.0)
        } else {
            0.0
        };
        let now = Instant::now();
        let epoch = |t: Option<Instant>| {
            // report epoch seconds for last_* — Instant lacks an
            // epoch; approximate via SystemTime offset.
            t.map(|i| {
                self.added_time
                    .checked_add(i.duration_since(self.started))
                    .and_then(|st| st.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_secs() as i64)
            })
            .unwrap_or(0)
        };
        let (d_full, d_frac) = self
            .picker
            .as_ref()
            .map_or((0, 0), |p| p.distributed_copies());
        TorrentStatus {
            info_hash: self.info_hash,
            info_hashes: self.ti.as_ref().map_or_else(
                || bitdaemon_common::info_hash::InfoHash::from_v1(self.info_hash),
                |ti| *ti.info_hashes(),
            ),
            state,
            paused: self.paused,
            auto_managed: self.auto_managed,
            is_seed: self.is_seed(),
            is_seeding: self.seeding,
            finished: self.finished,
            has_metadata: self.ti.is_some() || self.picker.is_some(),
            has_incoming: self.peers.values().any(|c| !c.outgoing),
            moving_storage: self.moving_storage,
            announcing_to_trackers: self.outstanding_announces > 0,
            announcing_to_dht: self.dht_announcing,
            announcing_to_lsd: self.lsd_announcing,
            need_save_resume: self.need_save_resume,
            ip_filter_applies: self.apply_ip_filter,
            current_tracker: self
                .trackers
                .trackers
                .iter()
                .find(|e| e.verified)
                .or_else(|| self.trackers.trackers.first())
                .map_or_else(String::new, |e| e.url.clone()),
            name: self.name.clone(),
            save_path: self.save_path.clone(),
            error: self.last_error.clone(),
            num_peers: self.peers.len(),
            num_seeds: self.num_seeds,
            num_candidates: self.peer_list.num_peers(),
            num_complete,
            num_incomplete,
            downloaded,
            total_done: self.verified_bytes,
            total_wanted: total_size.min(i64::from(wanted_pieces) * self.piece_length),
            total_upload,
            total_download,
            total_payload_download: total_download,
            total_payload_upload: total_upload,
            total_failed_bytes: self.failed_bytes,
            total_redundant_bytes: self.redundant_bytes,
            total_wanted_done: self.verified_bytes,
            all_time_upload: self.all_time_upload + total_upload,
            all_time_download: self.all_time_download + total_download,
            last_upload: epoch(self.last_payload_up),
            last_download: epoch(self.last_payload_down),
            active_duration: now.duration_since(self.started).as_secs() as i64,
            finished_duration: self
                .finished_at
                .map_or(0, |t| now.duration_since(t).as_secs() as i64),
            seeding_duration: self
                .seeding_since
                .map_or(0, |t| now.duration_since(t).as_secs() as i64),
            download_rate: self.down_rate,
            upload_rate: self.up_rate,
            download_payload_rate: self.down_rate,
            upload_payload_rate: self.up_rate,
            list_seeds: self.peer_list.num_seeds(),
            list_peers: self.peer_list.num_peers() as i32 - self.peer_list.num_seeds(),
            connect_candidates: self.peer_list.num_connect_candidates(),
            num_uploads: self.peers.values().filter(|c| !c.choked).count() as i32,
            num_connections: self.peers.len() as i32,
            uploads_limit: self.max_uploads,
            connections_limit: self.max_connections,
            upload_limit: self
                .upload_limit
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
            download_limit: self
                .download_limit
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
            up_bandwidth_queue: self
                .peers
                .values()
                .map(|c| {
                    c.upload_queue
                        .iter()
                        .map(|r| i64::from(r.length))
                        .sum::<i64>()
                })
                .sum::<i64>() as i32,
            down_bandwidth_queue: self
                .peers
                .values()
                .map(|c| {
                    c.pipeline
                        .request_queue
                        .iter()
                        .map(|b| {
                            // 16 KiB blocks; tail of last piece is
                            // shorter (`files.piece_size` exact).
                            let start = i64::from(b.block.block_index) * 0x4000;
                            let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                                i64::from(f.piece_size(b.block.piece_index))
                            });
                            (psize - start).clamp(0, 0x4000)
                        })
                        .sum::<i64>()
                })
                .sum::<i64>() as i32,
            seed_rank: self.seed_rank(s),
            queue_position: queue_pos,
            distributed_full_copies: d_full,
            distributed_fraction: d_frac,
            progress,
            progress_ppm: (progress * 1e6) as i32,
            pieces: self.have.clone(),
            num_pieces: self.num_pieces,
            piece_length: self.piece_length,
            block_size: 0x4000,
            distributed_copies: f64::from(d_full) + f64::from(d_frac) / 1000.0,
            added_time: self
                .added_time
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64),
            last_scrape,
            next_announce: epoch(self.trackers.next_announce_time()),
            completed_time: epoch(self.finished_at),
            last_seen_complete: self.last_seen_complete,
        }
    }

    /// `stat::second_tick` — refresh `down_rate`/`up_rate` from the
    /// per-peer payload counters (upstream uses `stat` channel
    /// sliding-window rates; a 1-tick delta is the same signal).
    pub fn update_rates(&mut self, now: std::time::Instant) {
        // advance the torrent's monotonic ms clock — peers use it
        // for `m_requested`/RTT timestamps (upstream `time_now()`).
        if let Some(prev) = self.last_rates_instant.replace(now) {
            self.now_ms += now.saturating_duration_since(prev).as_millis() as i64;
        }
        let now_ms = self.now_ms;
        for c in self.peers.values_mut() {
            c.now_ms = now_ms;
        }
        // Rate from per-peer deltas, not lifetime totals: a peer
        // that disconnected between ticks must contribute 0 to this
        // tick, not a negative delta (its total vanishes from the
        // sum → `down - prev` < 0 → bogus negative rate).
        let down: i64 = self
            .peers
            .values()
            .map(|c| c.stat_down_total - c.last_down_total)
            .sum();
        let up: i64 = self
            .peers
            .values()
            .map(|c| c.stat_up_total - c.last_up_total)
            .sum();
        if let Some(prev_t) = self.stat_sample {
            let dt = (now - prev_t).as_secs_f64();
            if dt > 0.0 {
                let current_torrent_down = (down as f64 / dt) as i64;
                let current_torrent_up = (up as f64 / dt) as i64;
                self.down_rate = if self.down_rate == 0 {
                    current_torrent_down
                } else {
                    (self.down_rate * 4 + current_torrent_down) / 5
                };
                self.up_rate = if self.up_rate == 0 {
                    current_torrent_up
                } else {
                    (self.up_rate * 4 + current_torrent_up) / 5
                };
                // per-peer `stat::rate()` — 5-second exponential moving average
                // matching upstream `stat_channel::second_tick` (`m_5_sec_average = m_5_sec_average * 4 / 5 + sample / 5`).
                for c in self.peers.values_mut() {
                    let down_delta = c.stat_down_total - c.last_down_total;
                    let current_down =
                        (down_delta as f64 / dt).clamp(0.0, f64::from(i32::MAX)) as i32;
                    let current_up = ((c.stat_up_total - c.last_up_total) as f64 / dt)
                        .clamp(0.0, f64::from(i32::MAX))
                        as i32;
                    c.rate_down_speed = if c.rate_down_speed == 0 {
                        current_down
                    } else {
                        ((i64::from(c.rate_down_speed) * 4 + i64::from(current_down)) / 5) as i32
                    };
                    c.rate_up_speed = if c.rate_up_speed == 0 {
                        current_up
                    } else {
                        ((i64::from(c.rate_up_speed) * 4 + i64::from(current_up)) / 5) as i32
                    };
                    // upstream `second_tick` slow-start exit: the
                    // rate stopped growing (prev + 5 K slack >=
                    // current) after reaching minimum pipeline depth → hand over to rate-based AIMD.
                    if c.pipeline.slow_start
                        && !c.peer_choked
                        && c.pipeline.desired_queue_size >= 8
                        && c.down_delta_prev > 0
                        && c.down_delta_prev + 5000 >= down_delta
                    {
                        c.pipeline.slow_start = false;
                    }
                    c.down_delta_prev = down_delta;
                    // `peer_connection::second_tick` — AIMD: the
                    // request queue target follows the measured
                    // download rate (queue_time × rate / block).
                    c.pipeline
                        .update_desired_queue_size(i64::from(c.rate_down_speed), 0x4000);
                }
            }
        }
        for c in self.peers.values_mut() {
            c.last_down_total = c.stat_down_total;
            c.last_up_total = c.stat_up_total;
        }
        self.stat_sample = Some(now);
    }

    /// `want_more_peers` — downstream connect decisions.
    #[must_use]
    /// upstream `torrent::want_peers` — true for downloading torrents
    /// *and* seeds (upstream `want_peers_finished`: a seed keeps
    /// dialing to serve upload and feed PEX), unless
    /// `seeding_outgoing_connections` is disabled.
    pub fn want_peers(&self, seeding_outgoing: bool) -> bool {
        // upstream `torrent::want_peers` also checks the
        // per-torrent connection cap (`m_max_connections`, -1 = inf)
        // and gates seeds on `seeding_outgoing_connections`
        // (default true — a finished torrent still dials candidates).
        let conn_ok = self.max_connections < 0 || (self.peers.len() as i32) < self.max_connections;
        self.want_peers
            && !self.paused
            && !self.queued
            && (seeding_outgoing || !self.is_seed())
            && conn_ok
    }

    // ---- remaining `torrent.cpp` surface (upstream method names kept
    // for ledger greppability)

    /// `torrent::abort` — stop everything; the torrent is being
    /// removed (`m_abort`).
    pub fn abort(&mut self, out: &mut Vec<TorrentEffect>) {
        self.aborted = true;
        self.disconnect_all(out);
    }

    /// `is_aborted`.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }

    /// `torrent::disconnect_all` — tear down every peer connection.
    pub fn disconnect_all(&mut self, out: &mut Vec<TorrentEffect>) {
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.remove_peer(id, out);
        }
    }

    /// `torrent::need_picker` — true while metadata is present but the
    /// picker hasn't been allocated (we deallocate it in seed_mode).
    #[must_use]
    pub fn need_picker(&self) -> bool {
        self.ti.is_some() && self.picker.is_none() && !self.seed_mode
    }

    /// `num_known_peers` — the swarm list size (connected or not).
    #[must_use]
    pub fn num_known_peers(&self) -> usize {
        self.peer_list.num_peers()
    }

    /// `want_tick` — upstream gates `second_tick` work; the session
    /// tick already calls into every unpaused torrent, so this is the
    /// same predicate upstream uses (`!paused`).
    #[must_use]
    pub fn want_tick(&self) -> bool {
        !self.paused
    }

    /// `ready_for_connections` — may open new peer connections
    /// (upstream: started, has metadata-or-magnet, not aborted).
    #[must_use]
    pub fn ready_for_connections(&self) -> bool {
        !self.paused && !self.aborted
    }

    /// `peer_is_interesting` — the peer advertises at least one piece
    /// we don't have and want (upstream picker `want` test).
    #[must_use]
    pub fn peer_is_interesting(&self, c: &PeerConn) -> bool {
        if self.is_seed() {
            return false;
        }
        (0..self.num_pieces).any(|i| c.has_piece(PieceIndex(i)) && !self.have.get_bit(i as usize))
    }

    /// `torrent::set_error` — `m_error` (surfaced via `status.error`).
    pub fn set_error(&mut self, e: &str) {
        self.last_error = e.to_string();
    }

    /// `handle_disk_error` — upstream `on_error`: record the failure,
    /// post `torrent_error_alert`, and pause the torrent (disk errors
    /// are fatal until fixed).
    pub fn handle_disk_error(&mut self, e: &str, out: &mut Vec<TorrentEffect>) {
        self.set_error(e);
        out.push(TorrentEffect::Alert(crate::alerts::Alert::TorrentError {
            ih: self.info_hash,
            error: e.to_string(),
            file: String::new(),
        }));
        self.paused = true;
        self.disconnect_all(out);
    }

    /// `on_torrent_paused` — pause bookkeeping: time-critical
    /// deadlines and suggest state are dropped (upstream clears
    /// `m_time_critical_pieces` on pause).
    pub fn on_torrent_paused(&mut self) {
        self.time_critical.clear();
        self.suggest_pieces.clear();
        self.stop_when_ready = false;
    }

    /// `set_announce_to_*` — per-channel announce gates
    /// (`m_announce_to_dht`/`m_announce_to_lsd`/`m_announce_to_trackers`).
    pub fn set_announce_to_dht(&mut self, yes: bool) {
        self.announce_dht = yes;
    }

    /// `set_announce_to_lsd`.
    pub fn set_announce_to_lsd(&mut self, yes: bool) {
        self.announce_lsd = yes;
    }

    /// `set_announce_to_trackers`.
    pub fn set_announce_to_trackers(&mut self, yes: bool) {
        self.announce_trackers = yes;
    }

    /// `start_announcing`/`stop_announcing` — `m_announcing` gates the
    /// tracker timer (`update_tracker_timer` honors it).
    pub fn start_announcing(&mut self) {
        self.announcing = true;
    }

    /// `stop_announcing`.
    pub fn stop_announcing(&mut self) {
        self.announcing = false;
    }

    /// `enable_all_trackers` — mark every tracker entry enabled
    /// (`announce_entry::enabled = true`, `enable_announce`).
    pub fn enable_all_trackers(&mut self) {
        for t in &mut self.trackers.trackers {
            for ep in &mut t.endpoints {
                ep.enabled = true;
            }
        }
    }

    /// `announce_with_tracker` — reannounce one tracker by URL:
    /// `force_tracker_request(url)` upstream resets `next_announce`
    /// so the session's `tracker_announce_due` scan fires it next tick.
    pub fn announce_with_tracker(&mut self, url: &str) {
        let now = Instant::now();
        if let Some(i) = self.trackers.find(url) {
            for ep in &mut self.trackers.trackers[i].endpoints {
                ep.v1.next_announce = Some(now);
                ep.v2.next_announce = Some(now);
            }
        }
    }

    /// `add_suggest_piece` — peer suggested a piece (BEP-6);
    /// upstream keeps them in `m_suggest_pieces` for `want` scoring.
    pub fn add_suggest_piece(&mut self, p: PieceIndex) {
        if self.suggest_pieces.len() < 64 && !self.suggest_pieces.contains(&p) {
            self.suggest_pieces.push_back(p);
        }
    }

    /// `get_suggest_pieces`.
    #[must_use]
    pub fn get_suggest_pieces(&self) -> Vec<PieceIndex> {
        self.suggest_pieces.iter().copied().collect()
    }

    /// `is_upload_only` — `m_upload_only`: all wanted pieces done.
    #[must_use]
    pub fn is_upload_only(&self) -> bool {
        self.upload_only
    }

    /// `set_upload_only` (upstream clears `want_peers` when set).
    pub fn set_upload_only(&mut self, yes: bool) {
        self.upload_only = yes;
        if yes {
            self.want_peers = false;
        }
    }

    /// `is_ssl_torrent` — upstream: the torrent *info* embeds a
    /// `ssl-cert` (`torrent_info::ssl_cert()` non-empty), not our
    /// own keypair.
    #[must_use]
    pub fn is_ssl_torrent(&self) -> bool {
        self.ti.as_ref().is_some_and(|ti| ti.is_ssl_torrent())
    }

    /// `torrent::ssl_ctx` — the PEM keypair this torrent serves on
    /// inbound SSL handshakes, if `set_ssl_certificate` was called.
    #[must_use]
    pub fn ssl_pem_pair(&self) -> Option<(&str, &str)> {
        match (&self.ssl_cert, &self.ssl_key) {
            (Some(c), Some(k)) => Some((c.as_str(), k.as_str())),
            _ => None,
        }
    }

    /// `torrent_info::ssl_cert` — the root CA cert embedded in the
    /// metainfo (peers must present certs chaining to it).
    #[must_use]
    pub fn ssl_root_cert(&self) -> Option<&[u8]> {
        self.ti.as_ref().map(|ti| ti.ssl_cert())
    }

    /// `predicted_have_piece` — `m_predictive_pieces` (predictive
    /// piece-announce mode: pieces we claim to have speculatively).
    pub fn predicted_have_piece(&mut self, p: PieceIndex) {
        if (p.0 as usize) < self.predictive.size() {
            self.predictive.set_bit(p.0 as usize);
        }
    }

    /// `is_predictive_piece`.
    #[must_use]
    pub fn is_predictive_piece(&self, p: PieceIndex) -> bool {
        (p.0 as usize) < self.predictive.size() && self.predictive.get_bit(p.0 as usize)
    }

    /// `get_hash_picker` / `initialize_merkle_trees` — v2 merkle trees
    /// loaded from `torrent_info` hash storage (`load_merkle_trees`).
    pub fn initialize_merkle_trees(&mut self) {
        if self.merkle_trees.is_empty()
            && let Some(ti) = self.ti.clone()
        {
            // `load_merkle_trees` upstream builds lazily per file —
            // `build_merkle_trees` materializes what the torrent_info
            // carries (roots, piece layers).
            self.build_merkle_trees(&ti);
        }
    }

    /// `set_need_save_resume` — flag for the periodic flush
    /// (`m_need_save_resume_data`).
    pub fn set_need_save_resume(&mut self) {
        self.need_save_resume = true;
    }

    /// `set_progress_ppm` — `m_progress_ppm` progress parts-per-
    /// million snapshot used by `status`.
    pub fn set_progress_ppm(&mut self, ppm: i64) {
        self.progress_ppm = ppm;
    }

    /// `update_state_list` — upstream rebuilds the per-state torrent
    /// lists for the queue; ours is derived from `queue_seq` ordering,
    /// so this is a no-op kept for parity.
    pub fn update_state_list(&mut self) {}

    /// `update_auto_sequential` — sequential mode auto-flip when the
    /// download stream demands it; ours is user-driven (`sequential`).
    pub fn update_auto_sequential(&mut self) {}

    /// `update_want_peers` — recompute `m_want_peers` (paused,
    /// finished, abort and connection cap all feed it).
    pub fn update_want_peers(&mut self) {
        // upstream seeds stay in `want_peers_finished` — the
        // `seeding_outgoing_connections` gate lives in `want_peers()`.
        self.want_peers = !self.paused && !self.aborted && !self.upload_only;
    }

    /// `update_tracker_timer` — upstream nudges the next announce
    /// tick; our session tick already scans `tracker_announce_due`,
    /// so this only needs to keep `announcing` honored (no-op).
    pub fn update_tracker_timer(&mut self) {}

    /// `resume`/`do_resume`/`resume_download` — `m_state` transitions
    /// out of paused; `graceful_pause` arms the drain first.
    pub fn resume(&mut self) {
        self.paused = false;
        self.aborted = false;
        self.graceful_pause = false;
    }

    /// `torrent::do_pause` counterpart for the download path.
    pub fn resume_download(&mut self) {
        self.resume();
        self.update_want_peers();
    }

    /// `recalc_share_mode` — share-mode re-prioritization: pieces we
    /// already have get demoted so rarest-first serves the swarm
    /// (upstream walks `m_picker` marking have-pieces `dont_download`).
    pub fn recalc_share_mode(&mut self) {
        if !self.share_mode {
            return;
        }
        if let Some(p) = &mut self.picker {
            for i in 0..self.num_pieces {
                if (i as usize) < self.have.size() && self.have.get_bit(i as usize) {
                    p.set_piece_priority(
                        PieceIndex(i),
                        bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD,
                    );
                }
            }
        }
    }

    /// `request_time_critical_pieces` — re-issue deadlines:
    /// upstream refreshes `m_time_critical_pieces` into peer requests
    /// every tick for deadline pieces; we nudge every connected peer
    /// (the request pipeline prioritizes deadline blocks already).
    pub fn request_time_critical_pieces(&mut self, out: &mut Vec<TorrentEffect>) {
        if self.time_critical.is_empty() {
            return;
        }
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.drive_peer(id, out);
        }
    }

    /// `attach_peer` — register a connection object (the session
    /// allocates the id; identical to `add_peer` plus connecting
    /// bookkeeping).
    pub fn attach_peer(&mut self, id: u64, c: PeerConn) {
        self.add_peer(id, c);
    }

    /// `choke_peer` — send `Choke` + flag (`m_choked`) immediately.
    pub fn choke_peer(&mut self, id: u64, out: &mut Vec<TorrentEffect>) {
        if let Some(c) = self.peers.get_mut(&id) {
            c.choked = true;
            out.push(TorrentEffect::SendTo {
                peer: id,
                msg: PeerMessage::Choke,
            });
        }
    }

    /// `unchoke_peer`.
    pub fn unchoke_peer(&mut self, id: u64, out: &mut Vec<TorrentEffect>) {
        if let Some(c) = self.peers.get_mut(&id) {
            c.choked = false;
            out.push(TorrentEffect::SendTo {
                peer: id,
                msg: PeerMessage::Unchoke,
            });
        }
    }

    /// `inc_num_connecting` / `dec_num_connecting` — dial bookkeeping
    /// (`m_num_connecting` caps concurrent connects).
    pub fn inc_num_connecting(&mut self) {
        self.num_connecting += 1;
    }

    /// `dec_num_connecting`.
    pub fn dec_num_connecting(&mut self) {
        self.num_connecting = self.num_connecting.saturating_sub(1);
    }

    /// `num_connecting`.
    #[must_use]
    pub fn num_connecting(&self) -> i32 {
        self.num_connecting
    }

    /// `retry_web_seed` — re-arm a web seed after backoff
    /// (`web_seed_t::retry` upstream schedules on the tracker timer;
    /// ours just clears the error flag so `next_web_seed_blocks`
    /// picks it up again).
    pub fn retry_web_seed(&mut self, _url: &str) {
        self.web_seed_idx = 0;
    }

    /// `maybe_connect_web_seeds` — nudge the engine to pull blocks
    /// off url seeds (session `next_web_seed_blocks` does the pick).
    pub fn maybe_connect_web_seeds(&self) -> bool {
        !self.web_seeds.is_empty() && !self.paused && !self.is_seed()
    }

    /// `set_ssl_cert_buffer` — PEM buffer form (`set_ssl_cert` takes
    /// paths upstream; we store the resolved cert string either way).
    pub fn set_ssl_cert_buffer(&mut self, cert: &str, key: &str, _dh: &str) {
        self.ssl_cert = Some(cert.to_string());
        self.ssl_key = Some(key.to_string());
    }

    /// `set_apply_ip_filter` — `m_apply_ip_filter` flag.
    pub fn set_apply_ip_filter(&mut self, yes: bool) {
        self.apply_ip_filter = yes;
    }

    /// `torrent::set_file_priority` — `prioritize_files` on one slot
    /// (upstream resizes `m_file_priority` to `num_files` first).
    pub fn set_file_priority(
        &mut self,
        index: bitdaemon_common::types::FileIndex,
        prio: bitdaemon_common::types::DownloadPriority,
    ) {
        let mut v = self.file_priorities.clone();
        let i = index.0 as usize;
        if v.len() <= i {
            v.resize(i + 1, bitdaemon_common::types::DownloadPriority::default());
        }
        v[i] = prio;
        self.prioritize_files(&v);
    }

    /// `torrent::set_state` — upstream `set_state(torrent_status::
    /// state_t)` drives the checking→downloading→finished ladder;
    /// ours is derived (`checking`/`finished`/`seeding` flags), so
    /// this only covers the explicit transitions callers make.
    pub fn set_state(&mut self, s: crate::status::TorrentState) {
        use crate::status::TorrentState as S;
        match s {
            S::CheckingResumeData | S::CheckingFiles | S::QueuedForChecking => {
                self.checking = true;
            }
            S::DownloadingMetadata | S::Downloading => {
                self.checking = false;
                self.finished = false;
                self.seeding = false;
            }
            S::Finished => {
                self.checking = false;
                self.finished = true;
                self.seeding = false;
            }
            S::Seeding => {
                self.checking = false;
                self.finished = true;
                self.seeding = true;
            }
        }
    }

    /// `post_status` — snapshot routed to the alert queue by the
    /// caller (upstream `post(status_alert)`); the session feeds
    /// `settings`/`queue_pos` through [`Self::status`].
    #[must_use]
    pub fn post_status(
        &self,
        s: &crate::settings::SessionSettings,
        queue_pos: i32,
    ) -> crate::status::TorrentStatus {
        self.status(s, queue_pos)
    }

    /// `get_torrent_copy_with_hashes` — `torrent_file_with_hashes`
    /// (`save_resume_data` attaches `piece_layers` + `merkle_trees`).
    #[must_use]
    pub fn get_torrent_copy_with_hashes(&self) -> Option<std::sync::Arc<TorrentInfo>> {
        self.ti.clone()
    }
}
