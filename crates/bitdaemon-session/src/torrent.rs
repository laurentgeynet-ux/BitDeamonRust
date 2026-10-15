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
        /// `Bytes` — the wire buffer rides through refcount clones
        /// (piece dispatch / web-seed slicing); the only copy is the
        /// disk-pool buffer at `async_write`.
        data: bytes::Bytes,
    },
    /// verify a piece's hash (`verify_piece` — resume restore and
    /// post-write verification).
    DiskHash { piece: PieceIndex },
    /// seed-mode lazy verification — a peer requested a promised
    /// piece we haven't hashed yet (`async_hash` →
    /// `on_seed_mode_hashed`).
    SeedModeVerify { peer: u64, piece: PieceIndex },
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
    /// `piece_failed` tail — fence a `clear_piece` disk job on the
    /// piece's storage; when it drains, `on_piece_sync` restores the
    /// (possibly `blocks`-scoped) piece state. Routed through the
    /// session because the disk handle lives there.
    ClearPiece {
        /// the failed piece.
        piece: PieceIndex,
        /// v2 known-bad block indices (empty = whole piece).
        blocks: Vec<i32>,
    },
    /// `async_set_file_priority` — the session dispatches the disk
    /// job; the storage-side vector returns in the completion
    /// (`DiskCompleted::SetFilePriority` → `on_file_priority`).
    SetFilePriority {
        /// `fix_priorities`'d request.
        prios: Vec<bitdaemon_common::types::DownloadPriority>,
    },
    /// file finished — alert upstream `file_completed`.
    FileCompleted(FileIndex),
    /// the torrent just became a seed.
    BecameSeed,
    /// disconnect a peer — `sev` is upstream `disconnect_severity_t`:
    /// `PeerError` posts `peer_error_alert` + `error_peers` (protocol
    /// violations); `Failure`/`Normal` post `peer_disconnected_alert`.
    /// `sev > Normal` counts against the swarm entry's `failcount`.
    DisconnectPeer {
        peer: u64,
        reason: &'static str,
        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity,
    },
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
    /// `choke_this_peer` — an incoming `not_interested` forfeits the
    /// peer's upload slot. Routed through the session because
    /// `choke_peer` touches the shared `Counters` and must schedule
    /// an unchoke-slot recalculation (upstream `trigger_unchoke`).
    ChokePeer { peer: u64 },
    /// `remove_peer` tail — the last conn under `graceful_pause`
    /// just dropped: upstream re-enters `set_paused(true)` which
    /// clears the graceful bit and posts `torrent_paused_alert`.
    /// Session-side the handler completes `do_pause`.
    GracefulPauseDone,
}

/// The `PipelineTorrent` view `send_block_requests` needs — the
/// `t->*` accessors upstream calls on the torrent while pumping a
/// peer's request queue.
struct PipelineView<'a> {
    /// `t->torrent_file().piece_length()`.
    piece_length: i64,
    /// `t->block_size()`.
    block_size: i32,
    /// `t->torrent_file().files()` — present iff `valid_metadata()`.
    files: Option<&'a bitdaemon_core::FileStorage>,
    /// `t->graceful_pause()`.
    graceful_pause: bool,
    /// `t->upload_mode()`.
    upload_mode: bool,
    /// `state() == checking_files || checking_resume_data ||
    /// downloading_metadata` — requests stay queued in these states.
    data_requests_blocked: bool,
    /// `t->has_picker()` — seeds have none; their queued blocks drop
    /// without the `is_downloaded` invariant check.
    has_picker: bool,
}

impl crate::peer_pipeline::PipelineTorrent for PipelineView<'_> {
    fn block_size(&self) -> i32 {
        self.block_size
    }
    fn piece_length(&self) -> i64 {
        self.piece_length
    }
    fn piece_size_for_req(&self, piece: PieceIndex) -> i32 {
        self.files.map_or_else(
            || self.piece_length.min(i64::from(i32::MAX)) as i32,
            |f| f.piece_size(piece),
        )
    }
    fn to_req(&self, b: PieceBlock, piece_size: i32) -> PeerRequest {
        // `torrent::to_req` — clamp to the real piece size so the
        // tail block of a truncated piece stays in bounds.
        let start = b.block_index * self.block_size;
        PeerRequest {
            piece: b.piece_index,
            start,
            length: (piece_size - start).min(self.block_size),
        }
    }
    fn graceful_pause(&self) -> bool {
        self.graceful_pause
    }
    fn upload_mode(&self) -> bool {
        self.upload_mode
    }
    fn data_requests_blocked(&self) -> bool {
        self.data_requests_blocked
    }
    fn has_picker(&self) -> bool {
        self.has_picker
    }
}

/// `aux::sliding_average<int, 30>` — fixed-point recency-weighted
/// mean (`fixed_point` 64, `inverted_gain` 30).
#[derive(Debug, Default)]
struct SlidingAvg30 {
    mean_fp: i64,
    n: i32,
}

impl SlidingAvg30 {
    fn add_sample(&mut self, s: i32) {
        let s = i64::from(s) * 64;
        if self.n < 30 {
            self.n += 1;
        }
        self.mean_fp += (s - self.mean_fp) / i64::from(self.n);
    }
    fn mean(&self) -> i32 {
        if self.n > 0 {
            ((self.mean_fp + 32) / 64) as i32
        } else {
            0
        }
    }
}

/// `aux::suggest_piece` — bounded set of low-availability pieces we
/// recently read from disk (likely still in the read cache); the
/// back of `pieces` is the highest priority.
#[derive(Debug, Default)]
pub struct SuggestSet {
    pieces: Vec<PieceIndex>,
    avail: SlidingAvg30,
}

impl SuggestSet {
    /// `suggest_piece::add_piece` — above-mean availability is
    /// filtered out; a re-add bumps the piece to the back.
    fn add_piece(&mut self, index: PieceIndex, availability: i32, max_queue: i32) {
        let mean = self.avail.mean();
        self.avail.add_sample(availability);
        if availability > mean {
            return;
        }
        if let Some(pos) = self.pieces.iter().position(|&p| p == index) {
            self.pieces.remove(pos);
        }
        if self.pieces.len() as i32 >= max_queue {
            let to_remove = self.pieces.len() as i32 - max_queue + 1;
            self.pieces.drain(..to_remove.max(0) as usize);
        }
        self.pieces.push(index);
    }

    /// `suggest_piece::get_pieces` — pick up to `n` pieces not in
    /// `p` (already suggested to this peer) nor `bits` (peer has
    /// them). Appends to `p` with the best piece last.
    fn get_pieces(&self, p: &mut Vec<PieceIndex>, bits: &Bitfield, mut n: usize) -> usize {
        let mut ret = 0usize;
        for &piece in self.pieces.iter().rev() {
            if (piece.0 as usize) < bits.size() && bits.get_bit(piece.0 as usize) {
                continue;
            }
            if p[..p.len() - ret].contains(&piece) {
                continue;
            }
            p.push(piece);
            ret += 1;
            n -= 1;
            if n == 0 {
                break;
            }
        }
        let start = p.len() - ret;
        p[start..].reverse();
        ret
    }
}

/// `web_seed_t` — per-URL runtime state of a url seed. Upstream
/// keeps one `web_peer_connection` per seed; our pull model
/// spawns one fetch task per block batch, so `busy` doubles as
/// the `peer_info.connection`/`resolving` marker upstream uses
/// to avoid double-connecting a seed.
#[derive(Clone, Debug)]
pub struct WebSeed {
    /// picker downloader key (`peer_key`) this seed's blocks are
    /// marked under — per-seed, so error/timeout release never
    /// refunds another seed's blocks.
    pub key: u64,
    /// `web_seed_t::retry` — don't pick the seed before the
    /// torrent clock (`now_ms`) reaches this.
    pub retry_ms: i64,
    /// `web_seed_t::disabled` — terminal for the session
    /// (banned, SSRF-blocked, unsupported protocol, bad port…).
    pub disabled: bool,
    /// `web_seed_t::removed` — dropped from `web_seeds` while a
    /// fetch was in flight; its late result is discarded.
    pub removed: bool,
    /// `web_seed_t::interesting` — upstream clears it when an
    /// ephemeral seed disconnects `uninteresting_upload_peer`.
    pub interesting: bool,
    /// `web_seed_t::ephemeral` — redirect-added; never persisted.
    pub ephemeral: bool,
    /// `web_seed_t::no_local_ips` — SSRF: a redirect seed learned
    /// from a global server must not resolve to a local address.
    pub no_local_ips: bool,
    /// `peer_info.connection || resolving` — a fetch task is in
    /// flight for this seed.
    pub busy: bool,
    /// the virtual conn's all-pieces refcount — held while the
    /// seed's conn lives (`peer_connection::init` /
    /// `~peer_connection`'s `dec_refcount` upstream); dropped on
    /// error/timeout/`Connection: close`/removal.
    pub refcounted: bool,
    /// `web_seed_t::supports_keepalive` — cleared when a response
    /// carries `Connection: close` (HTTP/1.0 seed), which widens
    /// the next request's preferred size upstream-style.
    pub supports_keepalive: bool,
    /// deadline of the in-flight batch on the `now_ms` clock —
    /// `urlseed_timeout` from dispatch.
    pub deadline_ms: i64,
    /// blocks the in-flight batch covers (`m_requests`).
    pub outstanding: std::collections::BTreeSet<bitdaemon_common::types::PieceBlock>,
    /// observable per-URL counters (`web_seed_statuses`).
    pub status: crate::status::WebSeedStatus,
}

/// How a failed fetch reschedules its seed — the engine-side
/// outcome folded into `Session::on_web_seed_error`, mapped here
/// onto upstream's `web_seed_t::retry`/`disabled`/erase moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebSeedBackoff {
    /// transport/status error — `now + urlseed_wait_retry`
    /// (upstream `on_disconnect(error >= failure)`).
    Retry,
    /// name resolution failed — `now + web_seed_name_lookup_retry`.
    NameLookup,
    /// the server sent `Retry-After` — `retry_web_seed(this, d)`.
    Header(std::time::Duration),
    /// never try again — `web->disabled = true` (SSRF, idna,
    /// unsupported protocol, filtered port, invalid range).
    Disable,
    /// erase the seed — `remove_web_seed_iter` (unparseable URL,
    /// empty hostname, port 0).
    Remove,
}

/// `torrent` — one swarm.
pub struct Torrent {
    /// `m_torrent_file->info_hash()`.
    pub info_hash: Sha1Hash,
    /// `m_info_hash` — the torrent's info-hash set (v1 and/or v2);
    /// `info_hash` stays the map key (v1, or truncated v2 for
    /// v2-only torrents). Tracker announce selection announces each
    /// supported version independently.
    pub info_hashes: bitdaemon_common::InfoHash,
    /// `tracker_key()` — upstream hashes the torrent/session/storage
    /// pointers into a per-torrent announce key, stable for the
    /// object's lifetime. A random nonce drawn at construction is
    /// the equivalent (same stability, same churn on reload).
    pub tracker_key: u32,
    /// `m_trackerid` — client-supplied tracker id
    /// (`AddTorrentParams::trackerid`); an endpoint's own
    /// `trackerid` response value takes precedence.
    pub trackerid: Vec<u8>,
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
    /// `m_merkle_trees` — per-file v2 merkle trees (empty for v1).
    /// `HashPicker` borrows it per verification call.
    pub merkle_trees: Vec<bitdaemon_core::merkle_tree::MerkleTree>,
    /// `m_hash_picker`'s persistent state (upstream keeps the picker
    /// on the torrent — request throttle + per-piece block-hash
    /// retry queue survive across calls).
    pub hash_picker_state: bitdaemon_disk::hash_picker::HashPickerState,
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
    /// blocks in flight toward web seeds — the union of every
    /// seed's `outstanding` set (the `m_requests` of all live
    /// `web_peer_connection`s).
    pub web_seed_outstanding: std::collections::BTreeSet<bitdaemon_common::types::PieceBlock>,
    /// `m_web_seeds` runtime half — per-URL `web_seed_t` state
    /// (retry/disabled/busy/keep-alive), keyed by base URL.
    /// Surfaced via [`Self::web_seed_statuses`].
    pub web_seed_state: std::collections::BTreeMap<String, WebSeed>,
    /// picker downloader keys handed to web seeds — counts down
    /// from `u64::MAX - 1` (`WEB_SEED_PEER` stays the generic
    /// sentinel). Conn keys derive from `peer_id`'s high 64
    /// bits, so descending-from-MAX seeds can't collide in
    /// practice.
    next_web_seed_key: u64,
    /// web-seed payload bytes credited this session — folded into
    /// `total_download()` here because the pull model has no conn
    /// to carry `stat_down_total` (upstream's `m_stat` fold).
    pub web_seed_payload_downloaded: i64,
    /// previous-tick shadow of `web_seed_payload_downloaded` for
    /// `update_rates` deltas.
    last_web_seed_down: i64,
    /// last synced count of conns with a non-empty download queue —
    /// the session applies its delta to `num_peers_down_requests`
    /// (upstream incs/decs at every `m_download_queue` transition).
    pub down_request_peers: i64,
    /// `m_peer_id` — the per-torrent peer-id we announce to
    /// trackers (`aux::generate_peer_id` at the `torrent` ctor).
    /// NOT the conn handshake pid — those are per-connection
    /// (`PeerConn::our_peer_id`).
    pub peer_id: bitdaemon_common::PeerId,
    /// `m_outgoing_pids` — peer-ids we generated for OUTGOING conns
    /// on this torrent; a remote handshake echoing one is a
    /// self-connection (`is_self_connection`). Entries live for the
    /// conn's lifetime (`remove_peer` erases; `peer_connect_failed`
    /// erases a failed dial's).
    pub outgoing_pids: std::collections::HashSet<bitdaemon_common::PeerId>,
    /// local endpoints of our in-flight dials (`remote → local`) —
    /// fills `i->connection->local_endpoint()` for pending dials in
    /// `peer_list::new_connection`'s self-connection legs. The
    /// engine registers an entry once the socket connects; attach
    /// or `peer_connect_failed` drops it.
    pub dial_locals: std::collections::HashMap<std::net::SocketAddr, std::net::SocketAddr>,
    /// `remote → our_pid` for in-flight dials — the pid each dial
    /// registered into `outgoing_pids`, so `peer_connected_outgoing`
    /// can verify the mark is still live and `peer_connect_failed`/
    /// teardown can erase it.
    pub dial_pids: std::collections::HashMap<std::net::SocketAddr, bitdaemon_common::PeerId>,
    /// `m_has_incoming` — sticky: set on the first INCOMING attach
    /// (`st.has_incoming` keeps reporting true after the conn leaves).
    pub has_incoming: bool,
    /// a slot-consuming unchoked conn left since the last choker
    /// pass — `trigger_unchoke` (upstream zeroes the time scaler so
    /// the next tick re-runs instead of waiting out the interval).
    pub triggered_unchoke: bool,
    /// the optimistic-unchoked conn left since the last pass —
    /// `trigger_optimistic_unchoke`.
    pub triggered_optimistic: bool,
    /// `m_peer_list` — known swarm members (tracker/PEX/DHT/LSD/
    /// resume candidates). Upstream stores `torrent_peer`s owned by
    /// the list; ours are `TorrentPeer`s in `PeerList`.
    pub peer_list: bitdaemon_net::peer_list::PeerList,
    /// `torrent_state` snapshot — the session-settings view the peer
    /// list consults (`max_peerlist_size`, `min_reconnect_time`,
    /// `max_failcount`, `allow_multiple_connections_per_ip`, external
    /// ip/port). Refreshed by the connect pump each tick.
    pub peer_list_state: bitdaemon_net::peer_list::PeerListState,
    /// `m_storage` — disk job routing index.
    pub storage: StorageIndex,
    /// `m_num_seeds` — *connected* peers known to be seeds
    /// (peer-list `seed` flag). Tracker scrape `complete` feeds
    /// `complete` below, never this counter.
    pub num_seeds: usize,
    /// `m_num_connecting_seeds` — in-flight dials to seed-flagged
    /// peers (upstream counts them for `num_downloaders`).
    pub num_connecting_seeds: i32,
    /// `m_complete`/`m_incomplete`/`m_downloaded` — torrent-wide
    /// scrape cache: the max over tracker entries' `scrape_*`,
    /// refreshed by `update_scrape_state`. -1 = never reported.
    pub complete: i32,
    /// leechers (max over trackers).
    pub incomplete: i32,
    /// completed downloads (max over trackers).
    pub downloaded: i32,
    /// `m_auto_sequential` — `auto_sequential` setting detected a
    /// well-seeded swarm (≥10 seeds, 10× seeds:leechers): pick
    /// pieces sequentially for performance. OR'd with
    /// `sequential` by `is_sequential_download`.
    pub auto_sequential: bool,
    /// `m_paused` — the user pause flag (`set_paused` /
    /// `flag_paused`).
    pub paused: bool,
    /// `m_session_paused` — the session-level pause layer
    /// (`session_impl::pause` → `set_session_paused`). `is_paused()`
    /// is `paused || session_paused`.
    pub session_paused: bool,
    /// `m_finished` — all pieces verified.
    pub finished: bool,
    /// `m_seeding` — was complete from the start or passed check.
    pub seeding: bool,
    /// `m_complete_sent` — a tracker acked our `completed` event;
    /// suppresses further `completed` announces session-wide.
    pub complete_sent: bool,
    /// bytes verified total (`m_total_downloaded`-ish).
    pub verified_bytes: i64,
    /// `m_file_progress` completion edges — `file_completed_alert`
    /// fires once per file when its covering pieces all become
    /// `have`. Re-derived whenever `have` is rebuilt (picker
    /// reset/recheck) so a re-verified file re-alerts, like
    /// upstream's `file_progress::clear`+`init`. Pad and zero-byte
    /// files are seeded `true` (upstream never fires `completed_cb`
    /// for them).
    pub files_completed: Vec<bool>,
    /// `m_queued_time_critical` aggregate — peers in endgame.
    pub want_peers: bool,
    /// `m_resume_data` fields the picker needs at init.
    pub piece_priority_dirty: bool,
    /// `ut_metadata` download state — magnet-added torrents only.
    pub metadata_tracker: UtMetadataTracker,
    /// raw info dict (the bytes `ut_metadata` serves). Set by
    /// `set_metadata`/`from_params`.
    pub metadata_raw: Option<Vec<u8>>,
    /// `m_magnet_link` — added without metadata.
    pub magnet: bool,
    /// `m_private` (BEP-27) — disables LSD/PEX/DHT peer discovery.
    pub private: bool,
    /// `smart_ban` plugin state (`extensions/smart_ban.cpp`).
    pub smart_ban: SmartBan,
    /// `m_auto_managed` — queue promotion/demotion is the
    /// session's job (`active_downloads`/`active_seeds`).
    pub auto_managed: bool,
    /// `m_state_subscription` (`flag_update_subscribe`) — this
    /// torrent lands in `state_update_alert` reports when it marks
    /// `state_updated()` (not serialized to resume data).
    pub state_subscription: bool,
    /// the `torrent_state_updates` dirty marker — set by
    /// [`Torrent::state_updated`], cleared when
    /// `session::post_torrent_updates` drains it.
    pub needs_state_update: bool,
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
    /// `m_comment` — from the .torrent's top-level `comment` or the
    /// resume data; re-emitted verbatim by `write_resume_data`.
    pub comment: Vec<u8>,
    /// `m_created_by` — the `created by` field, same lifecycle.
    pub created_by: Vec<u8>,
    /// `m_creation_date` — `creation date` posix time, 0 when absent.
    pub creation_date: i64,
    /// a `check_files` job is in flight (`m_checking_piece` coarse).
    pub checking: bool,
    /// upstream `m_state == checking_resume_data` — the
    /// `async_check_files` (fastresume verify) window, before any
    /// per-piece `checking_files` pass. Cleared on `files_checked`
    /// and when a full recheck arms.
    pub checking_resume: bool,
    /// all-channel download rate (`m_stat.download_rate()` — payload
    /// + protocol + ip-overhead EMA; `st.download_rate`).
    pub down_rate: i64,
    /// `m_stat.upload_rate()`.
    pub up_rate: i64,
    /// `m_stat.download_payload_rate()` — payload-channel EMA only
    /// (`st.download_payload_rate`).
    pub down_payload_rate: i64,
    /// `m_stat.upload_payload_rate()`.
    pub up_payload_rate: i64,
    /// last sample instant.
    pub stat_sample: Option<std::time::Instant>,
    /// `m_inactive` — debounced verdict of `is_inactive_internal`;
    /// only counts when `dont_count_slow_torrents` is on
    /// (upstream `is_inactive`). Feeds `want_tick` and
    /// `auto_manage_torrents`.
    pub inactive: bool,
    /// `m_pending_active_change` — the debounce is armed; the
    /// flip lands when `inactivity_deadline` passes.
    pub pending_active_change: bool,
    /// `m_inactivity_timer` deadline — `auto_manage_startup` for
    /// →inactive, immediate for →active.
    pub inactivity_deadline: Option<std::time::Instant>,
    /// `m_file_priority` — per-file `DownloadPriority` (empty =
    /// all default). Drives `piece_priority` upstream-style.
    /// Upstream only mutates this *after* the storage job lands
    /// (`on_file_priority`) — the vector mirrors the storage's
    /// actual priority state.
    pub file_priorities: Vec<bitdaemon_common::types::DownloadPriority>,
    /// `m_outstanding_file_priority` — an `async_set_file_priority`
    /// job is in flight; further updates merge into
    /// `deferred_file_priorities` instead of queueing a second job
    /// (upstream: concurrent jobs can complete out of order and
    /// silently clobber each other).
    pub outstanding_file_priority: bool,
    /// `m_deferred_file_priorities` — per-index overrides applied
    /// by `on_file_priority` once the outstanding job returns.
    pub deferred_file_priorities:
        std::collections::BTreeMap<i32, bitdaemon_common::types::DownloadPriority>,
    /// `torrent_flags::default_dont_download` — kept from
    /// `m_add_torrent_params` until `init` runs (metadata arrives):
    /// sizes `file_priorities` to all-`dont_download` unless a
    /// longer stored list already exists.
    pub default_dont_download: bool,
    /// `m_add_torrent_params->piece_priorities` — piece priorities
    /// passed at add time, applied by `init` once the picker exists
    /// (magnets carry them across the metadata gap).
    pub pending_piece_priorities: Vec<bitdaemon_common::types::DownloadPriority>,
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
    /// `m_error_file` — `file_index` the error touched, or an
    /// `ERROR_FILE_*` sentinel (`status::ERROR_FILE_NONE` = not
    /// file-scoped).
    pub error_file: i32,
    /// `m_peer_class` — the torrent's own peer class, created lazily
    /// by `setup_peer_class` on the first nonzero upload/download
    /// limit (upstream `torrent::set_limit_impl`). `0` = none; pool
    /// index 0 is the global class, so a real torrent class is ≥ 3.
    pub peer_class: bitdaemon_net::peer_class::PeerClassIndex,
    /// `peer_class_set` — the torrent-leg class set. Holds this
    /// torrent's own class once created plus any classes the user
    /// attaches; `ignore_unchoke_slots()` and the connection-limit
    /// factor scan it alongside the conn's set.
    pub peer_classes: bitdaemon_net::peer_class::PeerClassSet,
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
    /// `m_graceful_pause` — pausing without cancelling in-flight
    /// writes; requests stop immediately (`pause_flags_t::
    /// graceful_pause` upstream).
    pub graceful_pause: bool,
    /// `m_super_seeding` (flag) — initial-seed: leak one piece at
    /// a time per peer (`super_seeding()` upstream).
    pub super_seeding: bool,
    /// `upload_mode` (flag) — never request pieces, upload only.
    pub upload_mode: bool,
    /// `m_upload_mode_time` — when upload-mode was last entered
    /// (drives the `optimistic_disk_retry` periodic exit).
    pub upload_mode_time: Option<Instant>,
    /// `share_mode` (flag) — ratio-keeping download throttle.
    pub share_mode: bool,
    /// `m_seed_mode` — resume data promised every piece is present;
    /// while set, `is_seed()` is true and verification is lazy
    /// (`m_verified`/`m_verifying` track slow background checks).
    pub seed_mode: bool,
    /// `m_disable_v1_hashes` — hybrid torrent: skip the SHA-1 piece
    /// hash and treat the storage as v2-only. Locked at construction
    /// (upstream ctor) so the storage layout can't drift mid-life.
    pub disable_v1_hashes: bool,
    /// `m_verified` — pieces confirmed while in seed_mode
    /// (lazy re-check bookkeeping; cleared on leave).
    pub seed_verified: Bitfield,
    /// `m_verifying` — pieces currently being hashed in seed_mode.
    pub seed_verifying: Bitfield,
    /// `m_num_verified`.
    pub num_verified: i32,
    /// `m_picker_generation` — bumped when picker state is discarded
    /// (`force_recheck`, `handle_inconsistent_hashes`); stamped onto
    /// hash jobs so a stale completion can't touch the new state.
    pub picker_generation: u8,
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
    /// `m_ip_filter` — the session's shared access filter
    /// (`shared_ptr<const ip_filter>` upstream). `None` until the
    /// session attaches one via `set_ip_filter`/`add_torrent` —
    /// `add_swarm_peer`/`connect_to_peer` only gate when `Some` AND
    /// `apply_ip_filter` are set, matching upstream's double check.
    pub ip_filter: Option<std::sync::Arc<bitdaemon_net::ip_filter::IpFilter>>,
    /// `m_ses.get_port_filter()` read at `add_peer` — the session
    /// port filter, synced on `add_torrent`/`set_port_filter`.
    /// `None` = allow-all (standalone torrents).
    pub port_filter: Option<std::sync::Arc<bitdaemon_net::ip_filter::PortFilter>>,
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
    /// `m_connections_initialized` — peer conns may initialize their
    /// `m_have_piece`/interest state. Upstream sets it in the ctor
    /// for metadata-present adds and at the end of `files_checked`
    /// for magnets — before that, `do_update_interest` early-returns
    /// (pre-metadata bitfields get replayed, not scanned live).
    pub connections_initialized: bool,
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
    /// session-lifetime payload accumulators for conns already
    /// disconnected — upstream folds each conn's traffic into
    /// `m_stat`'s payload channels continuously (`total_payload_*`
    /// and `m_total_uploaded` are monotonic across disconnects);
    /// we fold at `remove_peer`, the only place a conn's stat
    /// leaves the live sum. `total_upload()`/`total_download()`
    /// read `acc + live`.
    pub payload_uploaded: i64,
    /// ditto download.
    pub payload_downloaded: i64,
    /// `m_stat[upload_protocol]` accumulator — same fold-at-
    /// `remove_peer` semantics as `payload_uploaded`; upstream
    /// `st->total_upload` = payload + protocol (not ip-overhead).
    pub protocol_uploaded: i64,
    /// `m_stat[download_protocol]` accumulator.
    pub protocol_downloaded: i64,
    /// `m_stat::*_ip_protocol` accumulators — kept for per-torrent
    /// wire-traffic visibility (`session_stats`-equivalent totals);
    /// not folded into `status.total_*` upstream-style.
    pub ip_overhead_uploaded: i64,
    /// download-side ip overhead accumulator.
    pub ip_overhead_downloaded: i64,
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
    /// `set_total` on upstream's `m_active_timer`/`m_finished_timer`/
    /// `m_seeding_timer` — resume-restored seconds accumulated in a
    /// previous process; added on top of the live `Instant` deltas.
    pub active_offset: std::time::Duration,
    /// ditto, finished.
    pub finished_offset: std::time::Duration,
    /// ditto, seeding.
    pub seeding_offset: std::time::Duration,
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
    /// `m_connect_boost_counter` — dials to hand out immediately on
    /// the next tracker response / `do_connect_boost` call. Armed to
    /// `settings.torrent_connect_boost` at add + resume (upstream
    /// ctor + `do_resume`).
    pub connect_boost_counter: u8,
    /// `settings_pack::close_redundant_connections` — session
    /// settings live on `Session`; this copy is what
    /// `disconnect_if_redundant` consults (upstream conns read
    /// `m_settings` directly).
    pub close_redundant_connections: bool,
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
    /// `m_enable_dht` — inverse of `torrent_flags::disable_dht`;
    /// settable via `set_flags` (upstream `should_announce_dht`
    /// gate).
    pub enable_dht: bool,
    /// `m_enable_lsd` — inverse of `torrent_flags::disable_lsd`.
    pub enable_lsd: bool,
    /// `m_enable_pex` — inverse of `torrent_flags::disable_pex`.
    pub enable_pex: bool,
    /// `m_suggest_pieces` — `aux::suggest_piece` read-cache set
    /// (low-availability pieces we recently read; BEP-6 SUGGEST
    /// source under `suggest_read_cache` mode).
    pub suggest_pieces: SuggestSet,
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
    /// set of pieces it may request even while choked (called once,
    /// on the peer's first `Interested`). The conn-level builder
    /// records `m_accept_fast`/`m_accept_fast_piece_cnt` itself so
    /// choked peers can actually claim the offered pieces.
    pub fn send_allowed_set(&mut self, peer_id: u64, set_size: i32, out: &mut Vec<TorrentEffect>) {
        // upstream gates: `!valid_metadata`, `super_seeding`,
        // `upload_only()` — OUR upload-only state, not the peer's.
        if self.super_seeding
            || !self.valid_metadata()
            || self.num_pieces <= 0
            || self.is_upload_only()
        {
            return;
        }
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        if !c.supports_fast {
            return;
        }
        let msgs = c.send_allowed_set(
            &self.info_hash,
            self.num_pieces,
            set_size,
            true,
            false,
            false,
        );
        for msg in msgs {
            out.push(TorrentEffect::SendTo { peer: peer_id, msg });
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
            info_hashes: bitdaemon_common::InfoHash::from_v1(info_hash),
            tracker_key: rand::random(),
            trackerid: Vec::new(),
            num_pieces,
            piece_length,
            piece_hashes: None,
            have,
            // upstream ctor: `m_connections_initialized` follows
            // `m_torrent_file->is_valid()` — a sized piece count
            // implies valid metadata; magnets (num_pieces == 0)
            // get the flag at `files_checked`.
            connections_initialized: num_pieces > 0,
            picker: None,
            peers: BTreeMap::new(),
            trackers: crate::tracker_list::TrackerList::default(),
            merkle_trees: Vec::new(),
            hash_picker_state: bitdaemon_disk::hash_picker::HashPickerState::default(),
            picker_generation: 0,
            ti: None,
            web_seeds: Vec::new(),
            files: None,
            web_seed_idx: 0,
            web_seed_outstanding: std::collections::BTreeSet::new(),
            web_seed_state: std::collections::BTreeMap::new(),
            next_web_seed_key: Self::WEB_SEED_PEER - 1,
            web_seed_payload_downloaded: 0,
            last_web_seed_down: 0,
            down_request_peers: 0,
            // the session overwrites this at add time with
            // `generate_peer_id(settings.peer_fingerprint)` —
            // a bare `Torrent::new` (unit tests) gets the zero
            // fingerprint like upstream's default settings.
            peer_id: bitdaemon_common::peer_id::generate_peer_id(&[]),
            outgoing_pids: std::collections::HashSet::new(),
            dial_locals: std::collections::HashMap::new(),
            dial_pids: std::collections::HashMap::new(),
            has_incoming: false,
            triggered_unchoke: false,
            triggered_optimistic: false,
            peer_list: bitdaemon_net::peer_list::PeerList::default(),
            peer_list_state: bitdaemon_net::peer_list::PeerListState::default(),
            storage,
            num_seeds: 0,
            num_connecting_seeds: 0,
            complete: -1,
            incomplete: -1,
            downloaded: -1,
            auto_sequential: false,
            paused: false,
            session_paused: false,
            finished: false,
            seeding: false,
            complete_sent: false,
            verified_bytes: 0,
            files_completed: Vec::new(),
            want_peers: true,
            piece_priority_dirty: false,
            metadata_tracker: UtMetadataTracker::new(),
            metadata_raw: None,
            magnet: false,
            private: false,
            smart_ban: SmartBan::new(),
            auto_managed: true,
            state_subscription: false,
            needs_state_update: false,
            last_pex: None,
            queued: false,
            added_time: std::time::SystemTime::now(),
            save_path: String::new(),
            storage_mode: bitdaemon_core::StorageMode::default(),
            part_file_dir: String::new(),
            name: String::new(),
            comment: Vec::new(),
            created_by: Vec::new(),
            creation_date: 0,
            checking: false,
            checking_resume: false,
            down_rate: 0,
            up_rate: 0,
            down_payload_rate: 0,
            up_payload_rate: 0,
            stat_sample: None,
            inactive: false,
            pending_active_change: false,
            inactivity_deadline: None,
            file_priorities: Vec::new(),
            outstanding_file_priority: false,
            deferred_file_priorities: std::collections::BTreeMap::new(),
            default_dont_download: false,
            pending_piece_priorities: Vec::new(),
            renamed_files: std::collections::BTreeMap::new(),
            userdata: 0,
            last_seen_complete: -1,
            sequential: false,
            checking_pieces: 0,
            last_error: String::new(),
            error_file: crate::status::ERROR_FILE_NONE,
            peer_class: 0,
            peer_classes: bitdaemon_net::peer_class::PeerClassSet::default(),
            need_save_resume: false,
            last_saved_resume: None,
            queue_seq: 0,
            time_critical: std::collections::BTreeMap::new(),
            now_ms: 0,
            last_rates_instant: None,
            disk_back_pressure: false,
            graceful_pause: false,
            super_seeding: false,
            upload_mode: false,
            upload_mode_time: None,
            share_mode: false,
            seed_mode: false,
            disable_v1_hashes: false,
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
            // `add_torrent_params::flags` defaults carry
            // `apply_ip_filter` — the raw ctor mirrors that stance;
            // `from_params` overrides from `atp.flags`.
            apply_ip_filter: true,
            ip_filter: None,
            port_filter: None,
            peer_limits: std::collections::HashMap::new(),
            extensions: Vec::new(),
            files_checked: false,
            i2p_pex_state: crate::i2p_pex::I2pPexState::default(),
            failed_bytes: 0,
            redundant_bytes: 0,
            all_time_upload: 0,
            all_time_download: 0,
            payload_uploaded: 0,
            payload_downloaded: 0,
            protocol_uploaded: 0,
            protocol_downloaded: 0,
            ip_overhead_uploaded: 0,
            ip_overhead_downloaded: 0,
            last_payload_down: None,
            last_payload_up: None,
            started: Instant::now(),
            finished_at: None,
            seeding_since: None,
            active_offset: std::time::Duration::ZERO,
            finished_offset: std::time::Duration::ZERO,
            seeding_offset: std::time::Duration::ZERO,
            moving_storage: false,
            outstanding_announces: 0,
            dht_announcing: false,
            lsd_announcing: false,
            aborted: false,
            num_connecting: 0,
            // armed by `Session::add_torrent` — `Torrent::new` is a
            // harness ctor without settings access.
            connect_boost_counter: 0,

            // upstream `settings_pack::close_redundant_connections`
            // default — `add_torrent`/`apply_settings` sync it.
            close_redundant_connections: true,
            announcing: true,
            announce_dht: true,
            announce_lsd: true,
            enable_dht: true,
            enable_lsd: true,
            enable_pex: true,
            announce_trackers: true,
            suggest_pieces: SuggestSet::default(),
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
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        pool: &mut bitdaemon_net::peer_class::PeerClassPool,
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
                // `m_info_hash = m_torrent_file->info_hashes()` —
                // the metainfo's set wins over atp's (they're
                // consistent by the hash check at add time).
                t.info_hashes = *ti.info_hashes();
                // `m_private = m_torrent_file->priv()` — BEP-27.
                t.private = ti.is_private();
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
                // `apply_pad_files` — all-pad pieces are had on the
                // promise alone (upstream `init` → `we_have`).
                let padded = t.init_picker(false);
                for piece in padded {
                    // upstream `apply_pad_files` → `we_have` directly
                    // (no `piece_passed` machinery for synthesized
                    // padding).
                    t.we_have(piece, false, s, counters, out, &mut |_| None);
                }
                // upstream ctor stores `atp.file_priorities` as
                // `m_file_priority`; `init` then shrinks oversized
                // lists to num_files or, under
                // `default_dont_download`, pads short/empty lists
                // with dont_download — `prioritize_files` layers the
                // stored entries on top (resize keeps them).
                t.file_priorities = atp.file_priorities.clone();
                if t.file_priorities.len() > ti.num_files() as usize {
                    t.file_priorities.truncate(ti.num_files() as usize);
                } else if atp
                    .flags
                    .contains(bitdaemon_common::flags::TorrentFlags::DEFAULT_DONT_DOWNLOAD)
                {
                    t.file_priorities.resize(
                        ti.num_files() as usize,
                        bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD,
                    );
                }
                // upstream `check_resume` order: file priorities
                // first, explicit `piece_priorities` override after
                // (applied inside `apply_resume_data`). `init` runs
                // `update_piece_priorities(m_file_priority)` — the
                // storage itself already got `atp.file_priorities`
                // through `StorageParams::priorities` at
                // `initialize()`, no `set_file_priority` job.
                if !t.file_priorities.is_empty() {
                    let fp = t.file_priorities.clone();
                    t.update_piece_priorities(&fp, s, counters, out);
                }
                t.apply_resume_data(atp, s, counters, out);
                t
            }
            None => {
                // magnet add — metadata pending; upstream keeps
                // `m_add_torrent_params` alive until `init`, so the
                // add-time priorities/flags ride along here.
                let mut t = Self::new(ih, 0, 0, Bitfield::default(), storage);
                // `m_info_hash` from atp — a hybrid magnet (`btih`
                // + `btmh`) announces both hash versions.
                if atp.info_hashes.has_v1() || atp.info_hashes.has_v2() {
                    t.info_hashes = atp.info_hashes;
                }
                t.magnet = true;
                t.default_dont_download = atp
                    .flags
                    .contains(bitdaemon_common::flags::TorrentFlags::DEFAULT_DONT_DOWNLOAD);
                t.pending_piece_priorities = atp.piece_priorities.clone();
                if !atp.file_priorities.is_empty() {
                    t.file_priorities = atp.file_priorities.clone();
                }
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
        // upstream `init` → `m_renamed_files.import_filenames` —
        // add-time renames ride `m_add_torrent_params` until metadata
        // is valid; our map keys by index so both add arms import
        // uniformly here.
        t.renamed_files = atp
            .renamed_files
            .iter()
            .map(|(i, n)| (*i, String::from_utf8_lossy(n).into_owned()))
            .collect();
        // upstream ctor init list: `m_comment(p.comment)`,
        // `m_created_by(p.created_by)`, `m_creation_date(p.creation_date)`
        // — metainfo metadata the torrent carries until the next
        // `write_resume_data` re-emits it.
        t.comment = atp.comment.clone();
        t.created_by = atp.created_by.clone();
        t.creation_date = atp.creation_date;
        // `m_trackerid(p.trackerid)` — the client-supplied tracker
        // id, echoed until a tracker's own `trackerid` lands on the
        // announce entry.
        t.trackerid = atp.trackerid.clone();
        for &ep in &atp.peers {
            // upstream runs `add_peer` for `atp.peers` inside the
            // torrent ctor — `m_ip_filter`/`m_port_filter` aren't
            // attached until session insert, so no gate fires yet.
            t.add_swarm_peer(
                ep,
                bitdaemon_common::flags::PeerSourceFlags::RESUME_DATA,
                &mut Vec::new(),
            );
        }
        for &ep in &atp.banned_peers {
            if let Some((i, _)) = t.peer_list.add_peer(
                ep,
                bitdaemon_common::flags::PeerSourceFlags::RESUME_DATA,
                bitdaemon_common::flags::PexFlags::NONE,
                &t.peer_list_state,
            ) {
                t.peer_list.ban_peer(i);
            }
        }
        t.paused = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::PAUSED);
        t.auto_managed = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::AUTO_MANAGED);
        // `m_apply_ip_filter(p.flags & apply_ip_filter)` — the flag
        // rides `default_flags`, so a fresh `AddTorrentParams` opts
        // the torrent INTO the session filter.
        t.apply_ip_filter = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::APPLY_IP_FILTER);
        t.state_subscription = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::UPDATE_SUBSCRIBE);
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
        // `m_enable_*` = !disable_* upstream — settable per-torrent
        // via `set_flags`, persisted in resume data.
        t.enable_dht = !atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::DISABLE_DHT);
        t.enable_lsd = !atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::DISABLE_LSD);
        t.enable_pex = !atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::DISABLE_PEX);
        // upstream `torrent::torrent`: seed_mode only sticks when the
        // resume data is consistent — no dont_download priorities and
        // every piece claimed present. Otherwise the flag is dropped
        // (upstream warns it "can be very confusing" to seed by
        // accident).
        // upstream: empty `have_pieces` is vacuously "all present"
        // (`std::find(.., false) == end`) — seed mode then
        // lazy-verifies everything on first request.
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
        {
            t.seed_mode = true;
            t.seed_verified = Bitfield::with_size(t.num_pieces.max(0) as usize);
            t.seed_verifying = Bitfield::with_size(t.num_pieces.max(0) as usize);
        }
        // upstream `torrent::torrent` locks `m_disable_v1_hashes` at
        // construction (`atp.flags & disable_v1_hashes && v2`) so a
        // later settings change can't desync the storage layout.
        t.disable_v1_hashes = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::DISABLE_V1_HASHES)
            && t.ti.as_ref().is_some_and(|ti| ti.v2());
        t.save_path = String::from_utf8_lossy(&atp.save_path).into_owned();
        t.storage_mode = atp.storage_mode;
        t.part_file_dir = String::from_utf8_lossy(&atp.part_file_dir).into_owned();
        // upstream ctor restores the lifetime counters, wall-clock
        // stamps, and accumulated timer totals from the atp.
        t.all_time_upload = atp.total_uploaded;
        t.all_time_download = atp.total_downloaded;
        t.last_seen_complete = atp.last_seen_complete;
        if atp.added_time > 0 {
            t.added_time = std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_secs(atp.added_time.max(0) as u64);
        }
        // `m_completed_time`/`m_last_{up,down}load` are wall-clock —
        // map them back onto our conn-clock relative epochs.
        let wall_age = |epoch: i64| -> Option<std::time::Duration> {
            if epoch <= 0 {
                return None;
            }
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .ok()?
                .checked_sub(std::time::Duration::from_secs(epoch as u64))
        };
        // `m_completed_time < m_added_time → m_completed_time =
        // m_added_time` (upstream ctor clamp).
        let completed_time = if atp.completed_time > 0 {
            atp.completed_time.max(atp.added_time)
        } else {
            0
        };
        if let Some(age) = wall_age(completed_time) {
            t.finished_at = Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
        }
        if let Some(age) = wall_age(atp.last_download) {
            t.last_payload_down =
                Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
        }
        if let Some(age) = wall_age(atp.last_upload) {
            t.last_payload_up = Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
        }
        // `set_total(clamp(0..0x3fffffff))` on the three timers.
        t.active_offset =
            std::time::Duration::from_secs(atp.active_time.clamp(0, 0x3fffffff) as u64);
        t.finished_offset =
            std::time::Duration::from_secs(atp.finished_time.clamp(0, 0x3fffffff) as u64);
        t.seeding_offset =
            std::time::Duration::from_secs(atp.seeding_time.clamp(0, 0x3fffffff) as u64);
        if !atp.name.is_empty() {
            t.name = String::from_utf8_lossy(&atp.name).into_owned();
        } else if let Some(ti) = &atp.ti {
            t.name = String::from_utf8_lossy(ti.name()).into_owned();
        }
        // `torrent::torrent` seeds `m_trackers` from the params
        // (`announce-list` + `announce`, tiered); without this the
        // tracker list stayed empty and no announce ever fired.
        // trackers from a real .torrent are `source_torrent`, from
        // a magnet's `tr=` params `source_magnet_link`.
        let trk_source = if atp.ti.is_some() {
            crate::tracker_list::SOURCE_TORRENT
        } else {
            crate::tracker_list::SOURCE_MAGNET
        };
        for (i, url) in atp.trackers.iter().enumerate() {
            let tier = atp.tracker_tiers.get(i).copied().unwrap_or(0);
            t.add_tracker(String::from_utf8_lossy(url).into_owned(), tier, trk_source);
        }
        // upstream ctor seeds the per-torrent caps and class-backed
        // rate limits (`state_update = false` — construction isn't a
        // user change). atp clamps to i32 on write-back.
        t.set_max_uploads(
            atp.max_uploads
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
            false,
        );
        {
            let mut scratch = Vec::new();
            t.set_max_connections(
                atp.max_connections
                    .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
                false,
                &mut scratch,
            );
        }
        t.set_limit_impl(
            atp.upload_limit,
            bitdaemon_net::peer_class::UPLOAD,
            false,
            pool,
        );
        t.set_limit_impl(
            atp.download_limit,
            bitdaemon_net::peer_class::DOWNLOAD,
            false,
            pool,
        );
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
        // are discarded here. Default settings only steer the
        // auto-sequential heuristic inside `set_seed`.
        let mut scratch = Vec::new();
        let scratch_counters = crate::counters::Counters::default();
        matches!(
            self.set_metadata_verdict(
                metadata,
                cfg,
                &crate::settings::SessionSettings::default(),
                &scratch_counters,
                &mut scratch
            ),
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
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
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
        self.files = Some(std::sync::Arc::new(info.files().clone()));
        // `m_info_hash = m_torrent_file->info_hashes()` — upstream
        // replaces the add-time hash set with the metainfo's.
        self.info_hashes = *info.info_hashes();
        self.private = info.is_private();
        self.ti = Some(std::sync::Arc::new(info));
        self.metadata_raw = Some(metadata.to_vec());
        self.magnet = false;
        // `apply_pad_files` — fully-padded pieces get the `we_have`
        // treatment (upstream `init` → `we_have` per pad piece).
        for piece in self.init_picker(true) {
            self.we_have(piece, false, s, counters, out, &mut |_| None);
        }
        // upstream `init` — an oversized `m_file_priority` shrinks
        // to num_files; otherwise `default_dont_download` fills it
        // with dont_download. Stored priorities then drive the fresh
        // picker (`update_piece_priorities`) without dirtying resume
        // data, and add-time `piece_priorities` override last.
        let num_files = self.files.as_ref().map_or(0, |fs| fs.num_files() as usize);
        if self.file_priorities.len() > num_files {
            self.file_priorities.truncate(num_files);
        } else if self.default_dont_download {
            self.file_priorities.resize(
                num_files,
                bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD,
            );
        }
        if !self.file_priorities.is_empty() {
            let ns = self.need_save_resume;
            if let (Some(fs), Some(picker)) = (self.files.clone(), self.picker.as_mut()) {
                for p in 0..self.num_pieces {
                    let piece = PieceIndex(p);
                    let prio = fs
                        .map_block(piece, 0, i64::from(fs.piece_size(piece)))
                        .iter()
                        .map(|sl| {
                            self.file_priorities
                                .get(sl.file_index.0 as usize)
                                .copied()
                                .unwrap_or(bitdaemon_common::types::DownloadPriority::DEFAULT)
                        })
                        .max()
                        .unwrap_or(bitdaemon_common::types::DownloadPriority::DEFAULT);
                    picker.set_piece_priority(piece, prio);
                }
            }
            self.need_save_resume = ns;
        }
        if let Some(picker) = self.picker.as_mut() {
            for (i, prio) in self.pending_piece_priorities.iter().enumerate() {
                if i as i32 >= self.num_pieces {
                    break;
                }
                picker.set_piece_priority(PieceIndex(i as i32), *prio);
            }
        }
        self.pending_piece_priorities.clear();
        if self.seed_mode {
            // upstream `init` → `set_have_all(true)` under
            // `m_seed_mode` — every piece is claimed present and
            // lazily verified on request; the tracked bitfields size
            // up from the zero-piece magnet state.
            self.have.set_all();
            self.seed_verified.resize(self.num_pieces.max(0) as usize);
            self.seed_verifying.resize(self.num_pieces.max(0) as usize);
        }
        self.need_save_resume = true;
        // upstream `torrent::init` sets `m_connections_initialized`
        // before replaying `pc->init()` per conn — magnet interest is
        // evaluated as soon as metadata lands, not deferred to
        // `files_checked`.
        self.connections_initialized = true;
        // peers that connected pre-metadata: their bitfields were
        // stored at whatever size arrived (`RefcountBulk` was a no-op
        // without a picker) — reconcile sizes and replay availability
        // into the fresh picker, then re-evaluate interest.
        let npieces = self.num_pieces.max(0) as usize;
        let mut peer_bits = Vec::new();
        let mut seed_peers = Vec::new();
        for (&pid, c) in self.peers.iter_mut() {
            if c.pre_metadata_all {
                let mut bf = Bitfield::with_size(npieces);
                bf.set_all();
                c.peer_pieces = bf.clone();
                c.pre_metadata_all = false;
            } else if c.peer_pieces.size() > npieces {
                c.peer_pieces.resize_with(npieces, false);
            }
            // upstream `init` — now that the piece count is known,
            // drop out-of-range `allowed_fast`/`suggested_pieces`.
            let limit = npieces as i32;
            c.allowed_fast.retain(|&p| p.0 < limit);
            c.suggested_pieces.retain(|&p| p.0 < limit);
            // `num_pieces` counts only in-range bits — a have-all
            // replay or a truncated bitfield can leave it stale.
            c.num_pieces = c.peer_pieces.iter().filter(|&b| b).count() as i32;
            if !c.peer_pieces.none_set() {
                peer_bits.push(c.peer_pieces.clone());
            }
            if c.is_seed() {
                seed_peers.push(pid);
            }
        }
        if let Some(p) = self.picker.as_mut() {
            for bits in &peer_bits {
                p.inc_refcount_bitfield(bits);
            }
        }
        // upstream `init` re-attaches conns: a resolved have-all is
        // a real seed now (`peer_has_all` → `set_seed(true)`).
        // (`seen_complete` is *not* marked here — upstream only sets
        // it on payload delivery or a payload-proven bitfield.)
        for pid in seed_peers {
            self.set_seed(pid, true, s);
        }
        // `torrent::init`/`start` — interested was never sent (our
        // `have` only materialized now): tell whoever holds pieces
        // we want, else the swarm keeps us choked forever.
        self.update_interest_all(counters, out);
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
    fn on_metadata_packet(
        &mut self,
        peer: u64,
        body: &[u8],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        // upstream `on_extended`: `length > 17*1024` is a
        // `invalid_metadata_message` peer-error disconnect — a
        // 16 KiB piece plus the dict header can never exceed it.
        if body.len() > 17 * 1024 {
            out.push(TorrentEffect::DisconnectPeer {
                peer,
                reason: "invalid metadata message",
                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
            });
            return;
        }
        let parsed = crate::ut_metadata::parse_metadata_packet(body);
        // replies ride the extension id the *peer* advertised for
        // `ut_metadata` (upstream `write_metadata_packet` uses
        // `m_message_index`); a zero id means it never advertised —
        // unreachable here because dispatch already gated on it.
        let ext_id = self.peers.get(&peer).map_or(0, |c| c.ext_ut_metadata);
        match parsed {
            crate::ut_metadata::MetadataParse::Malformed => {
                // bdecode failure / non-dict / missing or non-int
                // `msg_type`|`piece` → `invalid_metadata_message`.
                out.push(TorrentEffect::DisconnectPeer {
                    peer,
                    reason: "invalid metadata message",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                });
                return;
            }
            crate::ut_metadata::MetadataParse::UnknownType => {
                // upstream's switch has no `default` — counted and
                // consumed, nothing else.
            }
            crate::ut_metadata::MetadataParse::Msg(
                crate::ut_metadata::MetadataMsg::Request,
                piece,
                _,
                _,
            ) => {
                // upstream: `!valid_metadata() || piece < 0 ||
                // piece >= npieces` → `dont_have` + `return true` —
                // the early return skips the `num_incoming_metadata`
                // bump at the tail (invalid requests aren't counted).
                // The bounds check also guards the `piece * 16k`
                // slice in `metadata_packet`.
                let npieces = self
                    .metadata_raw
                    .as_ref()
                    .map_or(0i64, |md| md.len().div_ceil(16 * 1024) as i64);
                if self.metadata_raw.is_none() || piece < 0 || piece >= npieces {
                    // `write_metadata_packet` no-ops when the peer
                    // never advertised an id (`m_message_index == 0`).
                    if ext_id != 0 {
                        let resp = crate::ut_metadata::metadata_packet(
                            crate::ut_metadata::MetadataMsg::Reject,
                            piece as i32,
                            false,
                            &[],
                        );
                        out.push(TorrentEffect::SendTo {
                            peer,
                            msg: PeerMessage::Extended {
                                id: ext_id,
                                data: resp.into(),
                            },
                        });
                    }
                    return;
                }
                // `send_buffer_size() < send_buffer_limit` (160 KiB)
                // → serve now; else queue on the conn (cap 1024);
                // overflow → `dont_have` (that leg still counts).
                let send_buffer_size = self.peers.get(&peer).map_or(0, |c| c.send_buffer);
                let queued = self
                    .peers
                    .get(&peer)
                    .is_some_and(|c| c.metadata_incoming_requests.len() < 1024);
                let resp_ty = if send_buffer_size < 0x4000 * 10 {
                    crate::ut_metadata::MetadataMsg::Piece
                } else if queued {
                    if let Some(c) = self.peers.get_mut(&peer) {
                        c.metadata_incoming_requests.push_back(piece as i32);
                    }
                    counters.inc(crate::counters::Counter::NumIncomingMetadata);
                    return;
                } else {
                    crate::ut_metadata::MetadataMsg::Reject
                };
                // `write_metadata_packet` no-ops when the peer never
                // advertised an id (`m_message_index == 0`).
                if ext_id != 0 {
                    let resp = crate::ut_metadata::metadata_packet(
                        resp_ty,
                        piece as i32,
                        self.metadata_raw.is_some(),
                        self.metadata_raw.as_deref().unwrap_or(&[]),
                    );
                    out.push(TorrentEffect::SendTo {
                        peer,
                        msg: PeerMessage::Extended {
                            id: ext_id,
                            data: resp.into(),
                        },
                    });
                }
            }
            crate::ut_metadata::MetadataParse::Msg(
                crate::ut_metadata::MetadataMsg::Piece,
                piece,
                total,
                data,
            ) => {
                // upstream: a `piece` we never asked this peer for is
                // "UNWANTED / TIMED OUT" and dropped before touching
                // the tracker — and before the incoming counter.
                let Some(conn) = self.peers.get_mut(&peer) else {
                    return;
                };
                let Some(pos) = conn
                    .metadata_sent_requests
                    .iter()
                    .position(|&p| i64::from(p) == piece)
                else {
                    return;
                };
                conn.metadata_sent_requests.remove(pos);
                let r =
                    self.on_ut_metadata_data(data, piece as i32, total, peer, s.max_metadata_size);
                // upstream counts every message that reaches the
                // switch — the wanted piece lands here.
                counters.inc(crate::counters::Counter::NumIncomingMetadata);
                if matches!(r, MetadataResult::Redundant) {
                    // `received_metadata` early leg —
                    // `add_redundant_bytes(piece_unknown)` on a
                    // duplicate payload.
                    self.add_redundant_bytes(
                        data.len() as i64,
                        crate::peer_conn::WasteReason::Unknown,
                        counters,
                    );
                }
                if matches!(r, MetadataResult::Complete) {
                    // upstream `set_metadata` → `state_updated()`
                    // (valid_metadata flips → status changes).
                    self.state_updated();
                    let md = self.metadata_tracker.metadata().to_vec();
                    // `metadata_received{,_failed}_alert` — the
                    // info-hash verdict (upstream posts in
                    // `torrent::on_metadata`/`set_metadata`). A
                    // hash-valid but unparseable payload sets
                    // `invalid_swarm_metadata` and pauses upstream.
                    match self.set_metadata_verdict(
                        &md,
                        &LoadTorrentLimits::default(),
                        s,
                        counters,
                        out,
                    ) {
                        MetadataVerdict::Ok => {
                            self.metadata_tracker.clear();
                            out.push(TorrentEffect::MetadataReceived);
                        }
                        MetadataVerdict::HashMismatch => {
                            self.apply_metadata_hash_fail_penalty();
                            out.push(TorrentEffect::MetadataFailed);
                        }
                        MetadataVerdict::ParseError => {
                            self.apply_metadata_hash_fail_penalty();
                            out.push(TorrentEffect::MetadataFailed);
                            // upstream `set_metadata`:
                            // `set_error(invalid_swarm_metadata)` +
                            // `pause()` — hash-correct but
                            // unparseable is fatal until fixed
                            // (no disconnect_all — `pause` only).
                            self.set_error(
                                "invalid swarm metadata",
                                crate::status::ERROR_FILE_METADATA,
                            );
                            out.push(TorrentEffect::Alert(crate::alerts::Alert::TorrentError {
                                ih: self.info_hash,
                                error: "invalid swarm metadata".into(),
                                file: self.resolve_filename(crate::status::ERROR_FILE_METADATA),
                            }));
                            self.paused = true;
                        }
                    }
                }
                // upstream `maybe_send_request()` runs right after a
                // piece lands — the next request goes out on this
                // batch, not on the next 1 Hz tick.
                self.maybe_send_metadata_request(peer, out);
                return;
            }
            crate::ut_metadata::MetadataParse::Msg(
                crate::ut_metadata::MetadataMsg::Reject,
                piece,
                _,
                _,
            ) => {
                // upstream `dont_have`: `m_request_limit` bumps even
                // when the piece was never asked for; the erase and
                // the counter only run for a known request.
                let Some(c) = self.peers.get_mut(&peer) else {
                    return;
                };
                let retry = Instant::now() + Duration::from_secs(60);
                c.metadata_request_limit =
                    Some(c.metadata_request_limit.map_or(retry, |t| t.max(retry)));
                let Some(pos) = c
                    .metadata_sent_requests
                    .iter()
                    .position(|&p| i64::from(p) == piece)
                else {
                    return;
                };
                c.metadata_sent_requests.remove(pos);
            }
        }
        counters.inc(crate::counters::Counter::NumIncomingMetadata);
    }

    /// `ut_metadata_plugin` tick — drains each conn's queued
    /// incoming metadata requests (serving side, runs even when we
    /// hold metadata) and, while metadata is missing, asks peers
    /// that advertised `ut_metadata` for the next piece.
    pub fn metadata_tick(&mut self, out: &mut Vec<TorrentEffect>) {
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.metadata_tick_peer(id, out);
        }
    }

    /// `failed_hash_check` — penalize every peer that served a piece
    /// of the bad metadata: retry limit = `base + 20–70s`, where
    /// `base` is `now + 5min` when the metadata was a single piece
    /// (one source), else `now`.
    fn apply_metadata_hash_fail_penalty(&mut self) {
        let (single_peer, peers) = self.metadata_tracker.reset_on_hash_failure();
        let now = Instant::now();
        for p in peers {
            if let Some(c) = self.peers.get_mut(&p) {
                let base = if single_peer {
                    now + Duration::from_secs(5 * 60)
                } else {
                    now
                };
                c.metadata_request_limit = Some(
                    base + Duration::from_secs(u64::from(
                        20 + bitdaemon_common::random::random(50),
                    )),
                );
            }
        }
    }

    /// `ut_metadata_peer_plugin::tick` for one conn — first drain
    /// its queued incoming requests while the send buffer is under
    /// the 160 KiB serve limit (upstream `tick()`), then
    /// `maybe_send_request`: `!valid_metadata && ext id != 0 &&
    /// sent_requests < 2 && has_metadata` (advertised `metadata_size`
    /// or retry timer expired).
    fn metadata_tick_peer(&mut self, peer: u64, out: &mut Vec<TorrentEffect>) {
        // upstream `tick()` — `maybe_send_request` first, then the
        // incoming-queue drain.
        self.maybe_send_metadata_request(peer, out);
        // serving drain — `while !m_incoming_requests.empty() &&
        // send_buffer_size < send_buffer_limit` upstream.
        while self.peers.get(&peer).is_some_and(|c| {
            c.ext_ut_metadata != 0
                && c.send_buffer < 0x4000 * 10
                && !c.metadata_incoming_requests.is_empty()
                && self.metadata_raw.is_some()
        }) {
            let piece = self
                .peers
                .get_mut(&peer)
                .and_then(|c| c.metadata_incoming_requests.pop_front());
            let Some(piece) = piece else { break };
            let (ext, md) = match (
                self.peers.get(&peer).map(|c| c.ext_ut_metadata),
                self.metadata_raw.as_deref(),
            ) {
                (Some(ext), Some(md)) => (ext, md),
                _ => break,
            };
            let resp = crate::ut_metadata::metadata_packet(
                crate::ut_metadata::MetadataMsg::Piece,
                piece,
                true,
                md,
            );
            let msg = PeerMessage::Extended {
                id: ext,
                data: resp.into(),
            };
            // charge the conn send buffer so the drain loop's
            // watermark reflects what we just queued.
            if let Some(c) = self.peers.get_mut(&peer) {
                c.send_buffer += bitdaemon_net::peer_wire::wire_len(&msg);
            }
            out.push(TorrentEffect::SendTo { peer, msg });
        }
    }

    /// `maybe_send_request` — `is_disconnecting` first, then
    /// `!valid_metadata && m_message_index != 0 &&
    /// sent_requests < 2 && has_metadata()` (advertised
    /// `metadata_size` or expired retry limit).
    fn maybe_send_metadata_request(&mut self, peer: u64, out: &mut Vec<TorrentEffect>) {
        if self.piece_hashes.is_some() {
            return;
        }
        let Some(conn) = self.peers.get(&peer) else {
            return;
        };
        if conn.disconnecting {
            return;
        }
        let ext = conn.ext_ut_metadata;
        if ext == 0 || conn.metadata_sent_requests.len() >= 2 {
            return;
        }
        // upstream `has_metadata()` — the `m_has_metadata` flag
        // (cleared by a size-less ut_metadata handshake, set by any
        // piece-level message) OR the retry limit having expired.
        let peer_has_metadata = conn.has_metadata;
        let can_ask = peer_has_metadata
            || conn
                .metadata_request_limit
                .is_none_or(|t| Instant::now() > t);
        if !can_ask {
            return;
        }
        let piece = self.metadata_request(peer_has_metadata);
        if piece < 0 {
            return;
        }
        if let Some(conn) = self.peers.get_mut(&peer) {
            conn.metadata_sent_requests.push(piece);
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

    /// `ut_pex` receive — `ut_pex_peer_plugin::on_extended`: size
    /// and flood guards disconnect, a malformed body disconnects
    /// (`invalid_pex_message`), `added` is deduped/capped against
    /// the per-peer known set, `dropped` erases from it, and any
    /// real addition triggers `do_connect_boost` (`want_peers`).
    pub fn on_pex_packet(
        &mut self,
        peer_id: u64,
        body: &[u8],
        max_pex_peers: usize,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if !self.enable_pex {
            return;
        }
        // `length > 500*1024` → `pex_message_too_large`.
        if body.len() > 500 * 1024 {
            out.push(TorrentEffect::DisconnectPeer {
                peer: peer_id,
                reason: "pex message too large",
                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
            });
            return;
        }
        let now = Instant::now();
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        // flood guard — upstream keeps the last 6 receive times and
        // disconnects (`too_frequent_pex`) when the oldest is <60s.
        if c.pex_rx_times.len() == 6
            && now.duration_since(*c.pex_rx_times.front().unwrap_or(&now)) < Duration::from_secs(60)
        {
            out.push(TorrentEffect::DisconnectPeer {
                peer: peer_id,
                reason: "too frequent pex",
                // upstream `too_frequent_pex` disconnects at default
                // (normal) severity.
                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
            });
            return;
        }
        if c.pex_rx_times.len() == 6 {
            c.pex_rx_times.pop_front();
        }
        c.pex_rx_times.push_back(now);
        let remote_local = c.remote.is_some_and(|ep| crate::ut_pex::is_local(&ep.ip()));
        let Some((added, dropped)) = crate::ut_pex::parse_pex_message(body, usize::MAX, 0) else {
            // `ret != 0 || !dict` → `invalid_pex_message`.
            out.push(TorrentEffect::DisconnectPeer {
                peer: peer_id,
                reason: "invalid pex message",
                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
            });
            return;
        };
        let c = self.peers.get_mut(&peer_id);
        let Some(c) = c else {
            return;
        };
        for ep in dropped {
            c.pex_known.remove(&ep);
        }
        let mut peers_added = false;
        for p in added {
            if c.pex_known.len() >= max_pex_peers {
                break;
            }
            // ignore local addresses unless the sender is local
            if crate::ut_pex::is_local(&p.addr.ip()) && !remote_local {
                continue;
            }
            if !c.pex_known.insert(p.addr) {
                continue;
            }
            // `added.f` flows through as `pex_flags` (upstream
            // `add_peer` → `insert_peer` maps `pex_encryption` →
            // `pe_support`, `pex_seed` → `maybe_upload_only`, …).
            let pf = bitdaemon_common::flags::PexFlags::from_bits(p.flags);
            if self
                .peer_list
                .add_peer(
                    p.addr,
                    bitdaemon_common::flags::PeerSourceFlags::PEX,
                    pf,
                    &self.peer_list_state,
                )
                .is_some()
            {
                peers_added = true;
            }
        }
        if peers_added {
            // `do_connect_boost` — new dial candidates → upstream
            // `add_peer`/`on_pex` marks `state_updated()` alongside.
            self.want_peers = true;
            self.state_updated();
        }
        // `num_incoming_pex` counts at the handler tail upstream —
        // a parsed-and-processed message only (not the disabled /
        // oversized / flooded / malformed early returns).
        counters.inc(crate::counters::Counter::NumIncomingPex);
    }

    /// `second_tick`'s pex section — every `PEX_INTERVAL` send each
    /// `ut_pex`-capable peer a diff of our connected endpoints.
    pub fn pex_tick(&mut self, now: Instant, out: &mut Vec<TorrentEffect>) {
        const PEX_INTERVAL: Duration = Duration::from_secs(60);
        let disable = !self.enable_pex;
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
        // upstream `send_peer` also carries the uTP/holepunch flags.
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
                if c.is_utp {
                    flags |= crate::ut_pex::flags::PEX_UTP;
                }
                // `supports_holepunch()` = remote advertised a
                // `ut_holepunch` ext id.
                if c.ext_holepunch != 0 {
                    flags |= crate::ut_pex::flags::PEX_HOLEPUNCH;
                }
                Some((ep, flags))
            })
            .collect();
        // upstream peer tick: `num_peers() <= 1 → return` — don't
        // gossip the swarm back to the only peer in it.
        if !disable && self.peers.len() > 1 {
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
                let first_time = c.pex_first_time;
                let (body, sent, in_msg) =
                    crate::ut_pex::build_pex_message(&connected, &c.pex_sent);
                // upstream `send_ut_peer_diff`: a diff with no
                // added/dropped entries is never sent
                // (`peers_in_msg() == 0 → return`). The first
                // (full-list) message always goes out.
                if !first_time && in_msg == 0 {
                    continue;
                }
                c.pex_sent = sent;
                c.pex_first_time = false;
                out.push(TorrentEffect::SendTo {
                    peer: id,
                    msg: PeerMessage::Extended {
                        id: ext,
                        data: body.into(),
                    },
                });
            }
        }
        // `i2p_pex` — the torrent-plugin tick rebuilds the shared diff;
        // upstream skips it entirely with zero connections (keeping
        // `m_old_peers` intact); each i2p conn then sends it (or the
        // full list the first time) — but not to a lone peer.
        if !self.peers.is_empty() {
            self.i2p_pex_state.tick(now, self.peers.values(), disable);
        }
        // prebuilt full list for first-time senders (`send_i2p_peer_list`).
        let full = crate::i2p_pex::build_full_list(self.peers.values());
        let lone = self.peers.len() <= 1;
        for (id, c) in self.peers.iter_mut() {
            if c.ext_i2p_pex == 0 || c.i2p.is_none() || lone {
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
    /// `num_incoming_pex` bumps on the processed path only
    /// (i2p_pex.cpp counts after the add loop).
    pub fn on_i2p_pex_packet(
        &mut self,
        peer_id: u64,
        body: &[u8],
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let now = std::time::Instant::now();
        let disable = !self.enable_pex;
        if disable {
            return;
        }
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        match c.i2p_pex.on_message(now, body, body.len()) {
            crate::i2p_pex::I2pPexVerdict::Added(hashes) => {
                if crate::i2p_pex::apply_added(self, &hashes) {
                    // upstream `on_i2p_resolve` → `update_want_peers`
                    // — new candidates make the torrent dialable.
                    self.update_want_peers();
                }
                counters.inc(crate::counters::Counter::NumIncomingPex);
            }
            crate::i2p_pex::I2pPexVerdict::Disconnect(reason) => {
                // upstream severities (i2p_pex.cpp): `too_frequent_pex`
                // is a normal-severity disconnect; size/format
                // violations are `peer_error`.
                let sev = if reason == "too_frequent_pex" {
                    bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal
                } else {
                    bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
                };
                out.push(TorrentEffect::DisconnectPeer {
                    peer: peer_id,
                    reason,
                    sev,
                });
            }
            crate::i2p_pex::I2pPexVerdict::Nothing => {}
        }
    }

    /// `metadata_request(has_metadata)` — which ut_metadata piece to
    /// ask `peer` for next (-1 = nothing requestable right now).
    /// `peer_has_metadata` is the *requesting peer's* advertised
    /// `metadata_size` (upstream `m_pc.has_metadata()`): peers that
    /// never advertised metadata don't get a 3-second per-block
    /// timeout recorded, so they can't starve out peers that do.
    /// Caller wraps it in a `ut_metadata` `request` packet via
    /// `metadata_packet`.
    pub fn metadata_request(&mut self, peer_has_metadata: bool) -> i32 {
        let peers = &self.peers;
        self.metadata_tracker
            .metadata_request(peer_has_metadata, |src| peers.contains_key(&src))
    }

    /// `init_picker` — create the picker (metadata arrived or
    /// resume check done) and apply `apply_pad_files`: pad files
    /// contribute per-piece pad bytes; pieces that are *entirely*
    /// padding are returned so the caller can run the `we_have` path
    /// (upstream `init` calls `we_have` on them — no data exists on
    /// the wire). `priorities`/`have` seeded from resume.
    pub fn init_picker(&mut self, now: bool) -> Vec<PieceIndex> {
        let mut padded = Vec::new();
        // pads are applied once, at picker creation (upstream `init`
        // creates the picker then runs `apply_pad_files` on it) — a
        // second pass would re-record the same piece's pad bytes.
        let created = self.picker.is_none();
        if created {
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
            // `need_picker` also (re)seeds `m_file_progress` — the
            // completion-edge tracker starts from current `have`.
            self.init_files_completed();
            let _ = now;
        }
        if !created {
            return padded;
        }
        if let (Some(fs), Some(picker)) = (self.files.clone(), self.picker.as_mut()) {
            let piece_length = i64::from(fs.piece_length());
            let num_files = fs.num_files();
            for i in 0..num_files {
                let fi = bitdaemon_common::types::FileIndex(i);
                if !fs.pad_file_at(fi) || fs.file_size(fi) == 0 {
                    continue;
                }
                // `map_file(last_byte)` — walk the pad file's pieces
                // backwards; misaligned pad files (not ending on a
                // piece boundary mid-torrent) are treated as normal
                // files upstream.
                let pr = fs.map_file(fi, fs.file_size(fi) - 1, 0);
                if (i64::from(pr.start) + 1) % piece_length != 0 && i < num_files - 1 {
                    continue;
                }
                let mut piece = pr.piece;
                let mut pad_bytes_left = fs.file_size(fi);
                while pad_bytes_left > 0 && piece.0 >= 0 {
                    let bytes = pad_bytes_left.min(i64::from(fs.piece_size(piece))) as i32;
                    if bytes <= 0 {
                        break;
                    }
                    let full = i64::from(fs.piece_size(piece)) == i64::from(bytes);
                    picker.set_pad_bytes(piece, bytes);
                    if full && !self.have.get_bit(piece.0 as usize) {
                        padded.push(piece);
                    }
                    pad_bytes_left -= i64::from(bytes);
                    piece.0 -= 1;
                }
            }
        }
        padded
    }

    /// `write_resume_data` (upstream `torrent::write_resume_data`) —
    /// snapshot the picker state into `AddTorrentParams` fields.
    /// `verified`/`have` bitfields, `piece_priorities`, and
    /// `unfinished_pieces` (downloading pieces with ≥1
    /// finished/writing block → finished-block bitmask).
    pub fn to_resume_data(
        &self,
        atp: &mut AddTorrentParams,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
    ) {
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
        // upstream `ret.total_uploaded = m_total_uploaded` — the
        // all-time counter (resume seed + session payload).
        atp.total_uploaded = self.all_time_upload + self.total_upload();
        atp.total_downloaded = self.all_time_download + self.total_download();
        atp.upload_limit = self.upload_limit(pool);
        atp.download_limit = self.download_limit(pool);
        atp.max_connections = i64::from(self.max_connections);
        atp.max_uploads = i64::from(self.max_uploads);
        atp.last_seen_complete = self.last_seen_complete;
        // upstream `torrent::write_resume_data`: `ret.comment =
        // m_comment` / `created by` / `creation date` — the
        // metainfo metadata round-trips through the resume blob.
        atp.comment = self.comment.clone();
        atp.created_by = self.created_by.clone();
        atp.creation_date = self.creation_date;
        // `num_complete`/`num_incomplete`/`num_downloaded` — the
        // `update_scrape_state` cache (-1 = never scraped).
        atp.num_complete = i64::from(self.complete);
        atp.num_incomplete = i64::from(self.incomplete);
        atp.num_downloaded = i64::from(self.downloaded);
        // `m_last_{up,down}load` persist as wall-clock `time_t`
        // (`to_time_t`) — anchor our conn-clock Instants via
        // `added_time`/`started` the same way `status()` does.
        let epoch_of = |i: Option<Instant>| {
            i.and_then(|t| {
                self.added_time
                    .checked_add(t.duration_since(self.started))
                    .and_then(|st| st.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
            })
            .unwrap_or(0)
        };
        atp.last_download = epoch_of(self.last_payload_down);
        atp.last_upload = epoch_of(self.last_payload_up);
        atp.completed_time = epoch_of(self.finished_at);
        atp.added_time = self
            .added_time
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        atp.active_time = (self.started.elapsed() + self.active_offset).as_secs() as i64;
        atp.finished_time = self
            .finished_at
            .map_or(self.finished_offset.as_secs() as i64, |t| {
                (t.elapsed() + self.finished_offset).as_secs() as i64
            });
        atp.seeding_time = self
            .seeding_since
            .map_or(self.seeding_offset.as_secs() as i64, |t| {
                (t.elapsed() + self.seeding_offset).as_secs() as i64
            });
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
            if let Ok(mut ti) = TorrentInfo::from_info_node(&node, &LoadTorrentLimits::default()) {
                // `ti` carries the metainfo metadata too — the
                // `info` dict alone doesn't hold it (it lives at
                // the resume dict's top level).
                ti.internal_set_comment(&self.comment);
                ti.internal_set_creator(&self.created_by);
                ti.internal_set_creation_date(self.creation_date);
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
    pub fn apply_resume_data(
        &mut self,
        atp: &AddTorrentParams,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
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
        let padded = self.init_picker(false);
        for piece in padded {
            // upstream `apply_pad_files` → `we_have` — pad-only
            // pieces are complete the moment the picker exists.
            self.we_have(piece, false, s, counters, out, &mut |_| None);
        }
        let picker = self.picker.as_mut();
        let Some(picker) = picker else { return };

        // resume `pieces` — the claimed `have` bitfield (upstream
        // `m_have_pieces` starts optimistic and `check_fastresume`
        // either confirms or clears it). Upstream only touches the
        // picker *after* the verdict (`on_resume_data_checked`
        // `!error` leg) — the claim lives in `self.have` alone until
        // `files_checked(accepted)` runs `piece_flushed`+`we_have`.
        for i in 0..self.num_pieces.min(atp.have_pieces.size() as i32) {
            if atp.have_pieces.get_bit(i as usize) {
                self.have.set_bit(i as usize);
            }
        }
        // resume `verified_pieces` — seed-mode confirmations feed
        // `m_verified` only (upstream loads them in the `m_seed_mode`
        // branch), never `have`. Upstream `set_have_all(true)` marks
        // every piece present on the promise alone.
        if self.seed_mode {
            self.have.resize(self.num_pieces as usize);
            self.have.set_all();
            self.seed_verified.resize(self.num_pieces as usize);
            for i in 0..self.num_pieces.min(atp.verified_pieces.size() as i32) {
                if atp.verified_pieces.get_bit(i as usize) {
                    self.seed_verified.set_bit(i as usize);
                    self.num_verified += 1;
                }
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
            self.state_updated();
            // `maybe_done_flushing` — same as `files_checked`: a
            // confirmed seed drops hash-picker state unless the
            // read-cache suggester still needs it.
            if s.suggest_mode != crate::settings::SuggestMode::SuggestReadCache {
                self.hash_picker_state = bitdaemon_disk::hash_picker::HashPickerState::default();
            }
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
        s: &crate::settings::SessionSettings,
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
        if c.on_parole {
            flags = flags.with(PeerFlags::ON_PAROLE);
        }
        if c.num_pieces >= self.num_pieces {
            flags = flags.with(PeerFlags::SEED);
        }
        // `p.source` = the torrent_peer's source flags (`{}` when the
        // conn isn't tracked in the peer list).
        let tp = c.remote.or(remote).and_then(|ep| self.peer_index(ep));
        Some(PeerInfo {
            flags,
            source: tp.map_or_else(Default::default, |i| self.peer_list.peers()[i].source),
            // `up_speed`/`down_speed` = `stat::*_rate()` — all
            // channels; `payload_*_speed` = `*_payload_rate()`.
            up_speed: c.rate_up_speed,
            down_speed: c.rate_down_speed,
            payload_up_speed: c.rate_up_payload,
            payload_down_speed: c.rate_down_payload,
            // `p.total_*` upstream = `total_payload_*` — payload only.
            total_upload: c.stat_up_total,
            total_download: c.stat_down_total,
            // `p.last_request`/`p.last_active` — conn-clock stamps.
            last_request: std::time::Duration::from_millis(
                (c.now_ms - c.pipeline.last_request_ms).max(0) as u64,
            ),
            last_active: std::time::Duration::from_millis(
                (c.now_ms - c.last_sent_ms.max(c.last_receive_ms)).max(0) as u64,
            ),
            // `p.download_queue_time` — `download_queue_time(0)`.
            download_queue_time: std::time::Duration::from_millis(
                c.download_queue_time(
                    0,
                    self.down_payload_rate,
                    self.down_request_peers,
                    i64::from(self.block_size()),
                )
                .max(0) as u64,
            ),
            pid: c.peer_id.unwrap_or_default(),
            pieces: c.peer_pieces.clone(),
            queue_bytes: c.pipeline.outstanding_bytes.min(i64::from(i32::MAX)) as i32,
            request_timeout: if c.pipeline.download_queue.is_empty() {
                -1
            } else {
                c.pipeline.request_timeout_setting
            },
            // `send_buffer_size` = `m_send_buffer.capacity()` — our
            // effective cap is the configured watermark.
            send_buffer_size: s.send_buffer_watermark.min(i32::MAX as usize) as i32,
            // `used_send_buffer` — payload staged for write.
            used_send_buffer: c.send_buffer.min(i32::MAX as usize) as i32,
            // `m_recv_buffer.capacity()`/`pos()` — live via the conn's
            // shared bandwidth state (the pump updates per iteration).
            receive_buffer_size: c
                .bw
                .recv_capacity
                .load(std::sync::atomic::Ordering::Relaxed)
                .min(i64::from(i32::MAX)) as i32,
            used_receive_buffer: c
                .bw
                .recv_pos
                .load(std::sync::atomic::Ordering::Relaxed)
                .min(i64::from(i32::MAX)) as i32,
            // `m_recv_buffer.watermark()` — the configured cap.
            receive_buffer_watermark: s.max_peer_recv_buffer_size.clamp(0, i64::from(i32::MAX))
                as i32,
            num_hashfails: tp.map_or(0, |i| i32::from(self.peer_list.peers()[i].hashfails)),
            // upstream: `download_queue().size() + m_request_queue
            // .size()` — sent *and* unsent requests.
            download_queue_length: (c.pipeline.download_queue.len()
                + c.pipeline.request_queue.len()) as i32,
            timed_out_requests: c
                .pipeline
                .download_queue
                .iter()
                .filter(|b| b.timed_out)
                .count() as i32,
            busy_requests: c.pipeline.download_queue.iter().filter(|b| b.busy).count() as i32,
            // upstream counts `download_queue` entries whose request
            // message is still sitting in the send buffer.
            requests_in_buffer: c
                .pipeline
                .download_queue
                .iter()
                .filter(|b| b.send_buffer_offset != crate::peer_pipeline::NOT_IN_BUFFER)
                .count() as i32,
            // upstream exposes the `desired_queue_size()` accessor —
            // the endgame/snubbed collapse to 1 is part of the value.
            target_dl_queue_length: c.pipeline.desired_queue_size(),
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
            // `pending_disk_bytes` = `m_outstanding_writing_bytes` —
            // block payload staged for this conn awaiting the wire.
            pending_disk_bytes: c.outstanding_writing_bytes.min(i64::from(i32::MAX)) as i32,
            // `pending_disk_read_bytes` = `m_reading_bytes`.
            pending_disk_read_bytes: c.reading_bytes.min(i32::MAX as usize) as i32,
            // `p.send_quota`/`p.receive_quota` — live per-conn quota.
            send_quota: c.bw.quota[crate::peer_conn::UPLOAD_CHANNEL]
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
            receive_quota: c.bw.quota[crate::peer_conn::DOWNLOAD_CHANNEL]
                .load(std::sync::atomic::Ordering::Relaxed) as i32,
            // `p.rtt` = `m_request_time.mean()` — ms.
            rtt: c.pipeline.request_time.mean().min(i64::from(i32::MAX)) as i32,
            num_pieces: c.num_pieces,
            download_rate_peak: c.download_rate_peak,
            upload_rate_peak: c.upload_rate_peak,
            progress_ppm: if self.num_pieces > 0 {
                (i64::from(c.num_pieces) * 1_000_000 / i64::from(self.num_pieces)) as i32
            } else {
                0
            },
            remote_endpoint: remote,
            local_endpoint: None,
            // `p.read_state`/`p.write_state` — `m_channel_state`
            // bits; `bw_disk` lives on `disk_blocked`/`up_disk_blocked`
            // here and is folded back in.
            read_state: BandwidthState(
                c.bw.channel_state[crate::peer_conn::DOWNLOAD_CHANNEL]
                    .load(std::sync::atomic::Ordering::Relaxed)
                    | if c.disk_blocked {
                        BandwidthState::DISK.0
                    } else {
                        0
                    },
            ),
            write_state: BandwidthState(
                c.bw.channel_state[crate::peer_conn::UPLOAD_CHANNEL]
                    .load(std::sync::atomic::Ordering::Relaxed)
                    | if c.up_disk_blocked {
                        BandwidthState::DISK.0
                    } else {
                        0
                    },
            ),
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
    pub fn on_files_checked(
        &mut self,
        accepted: bool,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        self.checking = false;
        self.need_save_resume = true;
        // `files_checked` posts `torrent_checked_alert` on both
        // verdicts (also reached via `no_verify_files`/`seed_mode`
        // shortcuts upstream).
        out.push(TorrentEffect::Alert(crate::alerts::Alert::TorrentChecked(
            self.info_hash,
        )));
        // `m_files_checked`/`m_connections_initialized` — gates the
        // late-add `on_files_checked` replay in `add_extension`.
        // `notify_initialized` — magnets get conn init replayed here
        // (upstream `files_checked` → `pc->init()` per conn, which
        // re-runs interest evaluation now that metadata is valid).
        let notify_initialized = !self.connections_initialized;
        self.files_checked = true;
        self.connections_initialized = true;
        // upstream `files_checked` → `state_updated()` (also the
        // auto-pause-on-failure path marks it at the same site).
        self.state_updated();
        for ext in &self.extensions {
            ext.on_files_checked();
        }
        if notify_initialized {
            // `pc->init()` per conn — interest/picking re-evaluated
            // for peers that attached before metadata arrived.
            self.update_interest_all(counters, out);
        }
        // upstream `files_checked` — every interesting unchoked conn
        // runs `request_a_block`/`send_block_requests` (unconditional,
        // outside the `notify_initialized` guard).
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for pid in ids {
            let kick = self
                .peers
                .get(&pid)
                .is_some_and(|c| c.interesting && !c.peer_choked && !c.connecting);
            if kick {
                self.drive_peer(pid, s, counters, out);
            }
        }
        if !accepted {
            // the picker's have-tracking only knows pieces that
            // passed `piece_verified` — resume bits live solely in
            // `self.have`, clearing it is sufficient.
            self.have.clear_all();
            self.verified_bytes = 0;
            self.init_files_completed();
            self.finished = false;
            self.seeding = false;
            // the fastresume-verify window closed — a following
            // `arm_full_recheck` is `checking_files`, not resume data.
            self.checking_resume = false;
            return;
        }
        // `on_resume_data_checked` accepted leg — every claimed
        // piece gets `piece_flushed` + `num_piece_passed` +
        // `we_have(i, loading_resume)` (announce/alert/file-progress
        // edges run; resume-dirty marks suppressed).
        {
            let claimed: Vec<PieceIndex> = (0..self.have.size() as i32)
                .filter(|i| self.have.get_bit(*i as usize))
                .map(PieceIndex)
                .collect();
            for piece in claimed {
                if let Some(p) = self.picker.as_mut() {
                    p.piece_flushed(piece);
                }
                counters.add(crate::counters::Counter::NumPiecePassed, 1);
                self.we_have(piece, true, s, counters, out, &mut |_| None);
            }
        }
        // the fastresume-verify window is closed — a following
        // `arm_full_recheck` is `checking_files`, not resume data.
        // (Kept set through the apply loop so `we_have`'s
        // `is_downloading_state` gate held.)
        self.checking_resume = false;
        // `is_finished()` — all *wanted* pieces (upstream
        // `files_checked` → `finished()`); `seeding` needs the
        // full set (`completed()` → `torrent_status::seeding`).
        let all = self.num_pieces > 0 && self.have.count() as i32 == self.num_pieces;
        let want_done = all || self.picker.as_ref().is_some_and(|p| p.is_finished());
        let became_finished = want_done && !self.finished;
        if want_done {
            self.finished = true;
            self.finished_at.get_or_insert_with(Instant::now);
            out.push(TorrentEffect::BecameSeed);
        }
        if all {
            self.seeding = true;
            self.seeding_since.get_or_insert_with(Instant::now);
            // `maybe_done_flushing` — a confirmed seed drops the
            // hash-picker state upstream (`suggest_read_cache` keeps
            // it to answer piece-hash requests).
            if s.suggest_mode != crate::settings::SuggestMode::SuggestReadCache {
                self.hash_picker_state = bitdaemon_disk::hash_picker::HashPickerState::default();
            }
        }
        // upstream `files_checked` — `is_seed()` (load-time seed /
        // seed_mode / force-recheck confirming all pieces) assumes
        // `completed` was already announced: latch it so the
        // `BecameSeed`→`completed()` announce is skipped. Non-seeds
        // lose super-seeding.
        if self.is_seed() {
            self.complete_sent = true;
            self.trackers.set_complete_sent();
        } else if self.super_seeding {
            self.super_seeding = false;
        }
        // upstream `finished()` → `send_upload_only()` on the
        // transition into finished (`is_upload_only` just went true).
        if became_finished {
            self.send_upload_only(out);
        }
    }

    /// `add_tracker_url` — upstream `add_tracker_url`; `source`
    /// is the `tracker_source` bit (`source_torrent`/
    /// `source_magnet_link`/`source_client`).
    pub fn add_tracker(&mut self, url: String, tier: i32, source: u8) {
        let mut e = crate::tracker_list::AnnounceEntry::new(url, tier);
        e.source = source;
        self.trackers.add(e);
    }

    /// `torrent::bytes_left` — payload bytes still wanted:
    /// `total_size - have·piece_length`, corrected for the short
    /// tail piece when we have it (the picker counts pieces, so the
    /// partial last piece is over-subtracted by `piece_length -
    /// piece_size(last)`). `None` before metadata — the announce
    /// falls back to 16 KiB upstream.
    #[must_use]
    pub fn bytes_left(&self) -> Option<i64> {
        let fs = self.files.as_ref()?;
        if self.seed_mode {
            return Some(0);
        }
        let Some(p) = &self.picker else {
            return Some(if self.is_seed() { 0 } else { fs.total_size() });
        };
        let pc = p.have();
        let mut left = fs.total_size() - i64::from(pc.num_pieces) * i64::from(fs.piece_length());
        if pc.last_piece {
            left += i64::from(fs.piece_length()) - i64::from(fs.piece_size(fs.last_piece()));
        }
        Some(left)
    }

    /// `build_tracker_request` — the `AnnounceParams` for a due
    /// tracker. `peer_id`/`port` come from the session; `key` is the
    /// per-torrent `tracker_key` (upstream's pointer hash — our
    /// random nonce, same stability). `uploaded`/`downloaded` are the
    /// real payload counters (`downloaded` minus `failed_bytes`;
    /// `report_true_downloaded == false` subtracts `redundant` and —
    /// on seeds — the payload bytes peers are mid-flight on, which
    /// will land as redundant anyway). `num_want` is 0 on `stopped`
    /// like upstream. `url`/`tracker_id`/`ipv4`/`ipv6` are
    /// per-announce-entry — the caller fills them from the
    /// `AnnounceEntry` (`tracker_id` falls back to the torrent-level
    /// `m_trackerid` when the entry has none).
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn announce_params(
        &self,
        peer_id: bitdaemon_common::PeerId,
        port: u16,
        event: i32,
        apply_ip_filter_to_trackers: bool,
        num_want: i32,
        report_true_downloaded: bool,
        info_hash: [u8; 20],
    ) -> bitdaemon_net::tracker_client::AnnounceParams {
        // `req.downloaded = total_payload_download - failed` /
        // `req.uploaded = total_payload_upload` upstream.
        let mut downloaded = self.total_download() - self.failed_bytes;
        let mut redundant = self.redundant_bytes;
        if !report_true_downloaded {
            downloaded -= redundant;
            // a torrent that just completed still receives payload
            // on in-flight requests — every such byte is redundant,
            // so fold the mid-message progress into `redundant`
            // instead of letting `downloaded` exceed the size.
            if self.is_seed() {
                for c in self.peers.values() {
                    let b =
                        c.bw.piece_recv_staged
                            .load(std::sync::atomic::Ordering::Relaxed);
                    if b > 0 {
                        downloaded -= b;
                        redundant += b;
                    }
                }
            }
        }
        bitdaemon_net::tracker_client::AnnounceParams {
            url: String::new(),
            info_hash,
            peer_id: *peer_id.as_bytes(),
            port,
            uploaded: self.total_upload(),
            downloaded: downloaded.max(0),
            left: self.bytes_left().unwrap_or(16 * 1024),
            corrupt: self.failed_bytes,
            redundant,
            event,
            // `event == stopped → numwant = 0` — we don't want new
            // peers while tearing down.
            num_want: if event == 3 { 0 } else { num_want },
            key: self.tracker_key,
            tracker_id: Vec::new(),
            ipv4: Vec::new(),
            ipv6: Vec::new(),
            private_torrent: self.private,
            high_priority: false,
            i2p: self.ti.as_ref().is_some_and(|ti| ti.is_i2p()),
            // `req.filter = m_ip_filter` only when both flags are set.
            filter: (apply_ip_filter_to_trackers && self.apply_ip_filter)
                .then(|| self.ip_filter.clone())
                .flatten(),
        }
    }

    /// `is_seed`.
    #[must_use]
    pub fn is_seed(&self) -> bool {
        // upstream: `valid_metadata && (seed_mode || have_all ||
        // picker.is_seeding() || state == seeding)` — `finished`
        // (all *wanted* pieces) is not a seed; pieces may be
        // priority-0 and genuinely missing.
        if !self.valid_metadata() {
            return false;
        }
        self.seed_mode || self.seeding || self.picker.as_ref().is_some_and(PiecePicker::is_seeding)
    }

    /// `add_redundant_bytes` — discarded payload accounting:
    /// saturating `m_total_redundant_bytes` + `recv_redundant_bytes`
    /// + the per-reason `waste_piece_*` counter (contiguous from
    ///   `waste_piece_timed_out`, same as upstream's `+ reason`).
    pub fn add_redundant_bytes(
        &mut self,
        n: i64,
        reason: crate::peer_conn::WasteReason,
        counters: &crate::counters::Counters,
    ) {
        self.redundant_bytes = self.redundant_bytes.saturating_add(n);
        counters.add(crate::counters::Counter::RecvRedundantBytes, n);
        let c = match reason {
            crate::peer_conn::WasteReason::TimedOut => crate::counters::Counter::WastePieceTimedOut,
            crate::peer_conn::WasteReason::Cancelled => {
                crate::counters::Counter::WastePieceCancelled
            }
            crate::peer_conn::WasteReason::Unknown => crate::counters::Counter::WastePieceUnknown,
            crate::peer_conn::WasteReason::Seed => crate::counters::Counter::WastePieceSeed,
            crate::peer_conn::WasteReason::EndGame => crate::counters::Counter::WastePieceEndGame,
            crate::peer_conn::WasteReason::Closing => crate::counters::Counter::WastePieceClosing,
        };
        counters.add(c, n);
    }

    /// `t->block_size()` — `min(piece_length, 0x4000)` once metadata
    /// is known (`torrent.hpp:982`), `0x4000` before.
    pub fn block_size(&self) -> i32 {
        if self.files.is_some() {
            self.piece_length.min(0x4000) as i32
        } else {
            0x4000
        }
    }

    /// `add_peer` (swarm entry, not connection) — tracker/PEX/DHT/
    /// LSD/tracker feed — upstream `torrent::add_peer` (`peer_list::
    /// add_peer`); `None` when the entry was dropped (invalid
    /// endpoint or a full peer list).
    pub fn add_swarm_peer(
        &mut self,
        remote: std::net::SocketAddr,
        src: bitdaemon_common::flags::PeerSourceFlags,
        out: &mut Vec<TorrentEffect>,
    ) -> Option<usize> {
        // upstream `torrent::add_peer` gates before the list insert:
        // `ip_filter` (opt-in via `apply_ip_filter`) then
        // `port_filter`, each refusal posting `peer_blocked_alert`.
        if self.apply_ip_filter
            && let Some(f) = &self.ip_filter
            && f.access(remote.ip()) == bitdaemon_net::ip_filter::BLOCKED
        {
            out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerBlocked {
                ih: self.info_hash,
                remote: remote.to_string(),
                reason: "ip_filter".into(),
            }));
            return None;
        }
        if let Some(pf) = &self.port_filter
            && pf.access(remote.port()) == bitdaemon_net::ip_filter::BLOCKED
        {
            out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerBlocked {
                ih: self.info_hash,
                remote: remote.to_string(),
                reason: "port_filter".into(),
            }));
            return None;
        }
        let added = self
            .peer_list
            .add_peer(
                remote,
                src,
                bitdaemon_common::flags::PexFlags::NONE,
                &self.peer_list_state,
            )
            .map(|(i, _)| i);
        // upstream `add_peer`/`connect_to_peer` → `state_updated()`
        // (list_peers changed).
        if added.is_some() {
            self.state_updated();
        }
        added
    }

    /// `connect_to_peer` (upstream `torrent_handle::connect_peer`)
    /// — insert `remote` in the peer list as an immediate
    /// connect candidate (failcount untouched → next pump picks it).
    pub fn connect_peer(
        &mut self,
        remote: std::net::SocketAddr,
        out: &mut Vec<TorrentEffect>,
    ) -> Option<usize> {
        self.add_swarm_peer(remote, bitdaemon_common::flags::PeerSourceFlags::NONE, out)
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

    /// `torrent::want_peers_download` — `want_peers` gated to the
    /// downloading states (upstream `m_state` membership).
    #[must_use]
    pub fn want_peers_download(&self, s: &crate::settings::SessionSettings) -> bool {
        matches!(
            self.state(),
            crate::status::TorrentState::Downloading
                | crate::status::TorrentState::DownloadingMetadata
        ) && self.want_peers(s)
    }

    /// `torrent::want_peers_finished` — `want_peers` gated to
    /// `finished`/`seeding` (a seed keeps dialing to serve upload
    /// and feed PEX).
    #[must_use]
    pub fn want_peers_finished(&self, s: &crate::settings::SessionSettings) -> bool {
        matches!(
            self.state(),
            crate::status::TorrentState::Finished | crate::status::TorrentState::Seeding
        ) && self.want_peers(s)
    }

    /// `torrent::try_connect_peer` — `connect_one_peer` + the dial
    /// bookkeeping; the session hands out one dial per call. The
    /// returned `(endpoint, encrypted)` is what the engine dials;
    /// `None` = no usable candidate (`p->ip()` unresolvable — i2p
    /// — counts as a missed attempt, like upstream's failed
    /// `connect_to_peer`).
    /// `bt_peer_connection` ctor's encryption pick under
    /// `pe_enabled` — the `pe_support` self-toggle: a plain dial
    /// pre-sets it (a failure retries encrypted), an MSE dial
    /// clears it (success re-arms via `peer_connected`) AND flags
    /// the conn `fast_reconnect` so a failed MSE leg isn't cooled
    /// out for the plaintext retry. Returns `(encrypted, fast)`.
    fn pick_dial_encryption(
        p: &mut bitdaemon_net::torrent_peer::TorrentPeer,
        out_policy: bitdaemon_net::pe_crypto::EncPolicy,
    ) -> (bool, bool) {
        match out_policy {
            bitdaemon_net::pe_crypto::EncPolicy::Forced => (true, false),
            bitdaemon_net::pe_crypto::EncPolicy::Disabled => (false, false),
            bitdaemon_net::pe_crypto::EncPolicy::Enabled => {
                if p.pe_support {
                    p.pe_support = false;
                    (true, true)
                } else {
                    p.pe_support = true;
                    (false, false)
                }
            }
        }
    }

    pub fn try_connect_peer(
        &mut self,
        session_time: i32,
        state: &bitdaemon_net::peer_list::PeerListState,
        out_policy: bitdaemon_net::pe_crypto::EncPolicy,
        fingerprint: &[u8],
    ) -> Option<(std::net::SocketAddr, bool, bool, bitdaemon_common::PeerId)> {
        // keep `finished` in sync so finished torrents skip
        // seed/upload-only candidates — upstream feeds
        // `torrent_state::is_finished` (`is_finished()`, which is
        // wanted-done incl. seeds), not `is_seed()`.
        let mut st = state.clone();
        st.is_finished = self.finished || self.seeding;
        // `torrent_state` snapshot for the other peer-list entry
        // points (upstream rebuilds it from settings each call).
        self.peer_list_state = st.clone();
        // upstream `connect_to_peer`: `m_abort` → refuse the dial.
        if self.aborted {
            return None;
        }
        let i = self.peer_list.connect_one_peer(session_time, &st)?;
        let dial = {
            let p = self.peer_list.peers_mut().get_mut(i)?;
            // upstream stamps `last_connected` at `connect_to_peer`
            // entry — a failed instantiate still counts as an
            // attempt for the reconnect cooldown.
            p.last_connected = session_time.max(0) as u16;
            let Some(ep) = p.ip() else {
                // i2p or unresolvable — upstream's `connect_to_peer`
                // fails → `inc_failcount` + `update_want_peers` (the
                // peer was never marked connecting).
                self.peer_list.inc_failcount(i);
                self.update_want_peers();
                return None;
            };
            let (encrypted, fast_reconnect) = Self::pick_dial_encryption(p, out_policy);
            (ep, encrypted, fast_reconnect)
        };
        // `set_connection` at dial start (upstream's peer_connection
        // ctor) — the entry stops being a candidate until
        // `peer_connect_failed`/`peer_disconnected` reports back.
        // `conn_key` 0 = no conn object yet (binds at attach).
        let _ = self.peer_list.set_connected(i, session_time, false, 0);
        // `create_peer_connection` — a per-conn peer-id is generated
        // for the handshake and tracked in `m_outgoing_pids` (the
        // `is_self_connection` probe); `dial_pids` keeps the remote →
        // pid map until the conn attaches or fails.
        let our_pid = bitdaemon_common::peer_id::generate_peer_id(fingerprint);
        self.outgoing_pids.insert(our_pid);
        self.dial_pids.insert(dial.0, our_pid);
        // `fast_reconnect(true)` on the MSE-armed dial — rewind
        // `last_connected` + credit `fast_reconnects` so a failed
        // leg retries immediately (upstream runs it at the pe1
        // write inside `start()`).
        if dial.2 {
            let st2 = self.peer_list_state.clone();
            let _ = self.peer_list.fast_reconnect(i, session_time, &st2);
        }
        // `++m_num_seeds` at conn creation for a seed-flagged entry
        // (upstream bumps it in `create_peer_connection`, before the
        // dial completes).
        if self.peer_list.peers()[i].seed {
            self.num_seeds += 1;
        }
        // `inc_num_connecting` — dial bookkeeping; a
        // seed-flagged candidate counts toward
        // `num_connecting_seeds` too (upstream counts them out
        // of `num_downloaders`).
        self.num_connecting += 1;
        if self.peer_list.peers()[i].seed {
            self.num_connecting_seeds += 1;
        }
        self.update_want_peers();
        Some((dial.0, dial.1, dial.2, our_pid))
    }

    /// `torrent::do_connect_boost` — the first tracker response
    /// dials `m_connect_boost_counter` peers immediately instead of
    /// waiting for the next session tick. `num_connections` is the
    /// session's live count (incl. half-open). Returns the dial
    /// requests — one `inc_boost_connections` each, deducted from
    /// the next tick's `connection_speed` budget.
    #[must_use]
    pub fn do_connect_boost(
        &mut self,
        session_time: i32,
        state: &bitdaemon_net::peer_list::PeerListState,
        out_policy: bitdaemon_net::pe_crypto::EncPolicy,
        num_connections: i64,
        s: &crate::settings::SessionSettings,
        fingerprint: &[u8],
    ) -> Vec<(std::net::SocketAddr, bool, bool, bitdaemon_common::PeerId)> {
        if self.connect_boost_counter == 0 {
            return Vec::new();
        }
        let mut conns = i64::from(self.connect_boost_counter)
            .min(i64::from(s.connections_limit) - num_connections);
        if conns <= 0 {
            return Vec::new();
        }
        let mut dials = Vec::new();
        while self.want_peers(s) && conns > 0 {
            conns -= 1;
            self.connect_boost_counter = self.connect_boost_counter.saturating_sub(1);
            if let Some(d) = self.try_connect_peer(session_time, state, out_policy, fingerprint) {
                dials.push(d);
            }
            self.update_want_peers();
        }
        dials
    }

    /// `peer_connection::connect_failed` — a dial that never
    /// completed. Two legs upstream:
    ///
    /// * **uTP fallback** (`is_utp` && `supports_utp`): clear
    ///   `supports_utp`, `fast_reconnect(true)` (rewind
    ///   `last_connected` + credit `fast_reconnects`), disconnect at
    ///   **normal** severity (no `failcount`) and immediately re-dial
    ///   the SAME entry over TCP (`connect_to_peer(pi, ignore_limit)`).
    ///   The returned `(endpoint, encrypted)` is that retry — the
    ///   caller dials it right away; `num_connecting` nets to zero
    ///   (`dec` on the dead conn, `inc` on the new one).
    /// * **any other failure**: `disconnect(e, connect, failure)` —
    ///   `failcount++` via `connection_closed`.
    ///
    /// `conn_fast` is the failed dial conn's own `m_fast_reconnect`
    /// (an MSE-retry conn skips the `last_connected` restamp).
    /// Returns the immediate TCP retry for the uTP leg.
    pub fn peer_connect_failed(
        &mut self,
        remote: std::net::SocketAddr,
        session_time: i32,
        was_utp: bool,
        conn_fast: bool,
        out_policy: bitdaemon_net::pe_crypto::EncPolicy,
        fingerprint: &[u8],
    ) -> Option<(std::net::SocketAddr, bool, bool, bitdaemon_common::PeerId)> {
        let i = self.peer_index(remote)?;
        // pending-dial bookkeeping — the conn never attached, so its
        // pid leaves `outgoing_pids`/`dial_pids`/`dial_locals` here
        // (upstream: conn dtor → `remove_peer` erases it).
        self.dial_locals.remove(&remote);
        if let Some(pid) = self.dial_pids.remove(&remote) {
            self.outgoing_pids.remove(&pid);
        }
        let st = self.peer_list_state.clone();
        if was_utp && self.peer_list.peers()[i].supports_utp {
            // `fast_reconnect(true)` rewinds before the close so
            // `connection_closed` keeps the rewind (`!c.fast_reconnect()`
            // gates the restamp); `connect_to_peer` restamps to now
            // anyway for the retry.
            let _ = self.peer_list.fast_reconnect(i, session_time, &st);
            self.peer_list.peers_mut()[i].supports_utp = false;
            // `disconnect(e, connect, normal)` — no failcount.
            self.peer_list
                .connection_closed(i, session_time, true, false, &st);
            // `connect_to_peer(pi, true)` — same entry, TCP this
            // time (`supports_utp` clear steers `instantiate` away
            // from uTP); `set_connection` marks + restamps like the
            // normal dial path. `num_connecting`/`num_connecting_seeds`
            // net zero — upstream decs then incs.
            let _ = self.peer_list.set_connected(i, session_time, false, 0);
            let (encrypted, fast_reconnect) =
                Self::pick_dial_encryption(&mut self.peer_list.peers_mut()[i], out_policy);
            // `create_peer_connection` for the retry — fresh pid in
            // `m_outgoing_pids`, MSE arm rewinds again under the
            // `fast_reconnects` cap.
            let our_pid = bitdaemon_common::peer_id::generate_peer_id(fingerprint);
            self.outgoing_pids.insert(our_pid);
            self.dial_pids.insert(remote, our_pid);
            if fast_reconnect {
                let _ = self.peer_list.fast_reconnect(i, session_time, &st);
            }
            self.update_want_peers();
            return Some((remote, encrypted, fast_reconnect, our_pid));
        }
        // `dec_num_connecting` — the dial is over.
        self.num_connecting = self.num_connecting.saturating_sub(1);
        if self.peer_list.peers()[i].seed {
            self.num_connecting_seeds = self.num_connecting_seeds.saturating_sub(1);
            // `num_seeds` was bumped at `try_connect_peer` for the
            // pending dial — the teardown decrements it like
            // upstream's `remove_peer` on a conn that never
            // completed.
            self.num_seeds = self.num_seeds.saturating_sub(1);
        }
        // `disconnect(e, connect, failure)` — `connection_closed`
        // bumps `failcount`; the conn's own `m_fast_reconnect` (an
        // MSE retry) keeps the dial-time stamp so the plaintext
        // retry isn't cooled out.
        self.peer_list
            .connection_closed(i, session_time, conn_fast, true, &st);
        None
    }

    /// `connect_to_peer` for a caller-specified endpoint (the
    /// engine's `connect_peer` API / test dials): upstream resolves
    /// `peerinfo` off the handle path; ours inserts a connectable
    /// entry when missing, marks it pending-dial, and registers the
    /// conn's generated peer-id in `outgoing_pids`/`dial_pids`.
    /// `false` when the peer is already connected/dialing or banned —
    /// upstream's `peerinfo->connection != nullptr` refuse leg.
    pub fn register_manual_dial(
        &mut self,
        remote: std::net::SocketAddr,
        session_time: i32,
        pid: bitdaemon_common::PeerId,
    ) -> bool {
        let st = self.peer_list_state.clone();
        let i = match self.peer_index(remote) {
            Some(i) => i,
            None => match self.peer_list.add_peer(
                remote,
                bitdaemon_common::flags::PeerSourceFlags::NONE,
                bitdaemon_common::flags::PexFlags::NONE,
                &st,
            ) {
                Some((i, _)) => i,
                None => return false,
            },
        };
        if self.peer_list.peers()[i].connection.is_some() || self.peer_list.peers()[i].banned {
            return false;
        }
        let _ = self.peer_list.set_connected(i, session_time, false, 0);
        self.outgoing_pids.insert(pid);
        self.dial_pids.insert(remote, pid);
        self.num_connecting += 1;
        if self.peer_list.peers()[i].seed {
            self.num_connecting_seeds += 1;
            self.num_seeds += 1;
        }
        self.update_want_peers();
        true
    }

    /// `peer_list::new_connection` for an INCOMING conn — the
    /// upstream accept legs (banned / endpoint self-connection /
    /// same-direction drop / cross-direction port rule) wrapped in
    /// one call. `attached` resolves `entry.conn_key` through
    /// `peers`; a `conn_key` of 0 resolves through `dial_locals`
    /// (an in-flight dial's local endpoint for the self-conn check).
    /// `coin` is the equal-port tiebreak bit (upstream `random(1)`).
    #[must_use]
    pub fn peer_connected_incoming(
        &mut self,
        remote: std::net::SocketAddr,
        local: Option<std::net::SocketAddr>,
        coin: bool,
    ) -> bitdaemon_net::peer_list::NewConnVerdict {
        let st = self.peer_list_state.clone();
        let peers = &self.peers;
        let dial_locals = &self.dial_locals;
        self.peer_list.new_connection(
            remote,
            local,
            &st,
            &|p: &bitdaemon_net::torrent_peer::TorrentPeer| {
                if p.conn_key != 0 {
                    peers.get(&p.conn_key).and_then(|c| {
                        c.remote.map(|r| bitdaemon_net::peer_list::AttachedConn {
                            remote: r,
                            local: c.local,
                            outgoing: c.outgoing,
                            failed: c.failed,
                            key: p.conn_key,
                        })
                    })
                } else {
                    // pending outbound dial — its remote is the entry
                    // endpoint, its local was registered by the engine.
                    p.ip().map(|ep| bitdaemon_net::peer_list::AttachedConn {
                        remote: ep,
                        local: dial_locals.get(&ep).copied(),
                        outgoing: true,
                        failed: false,
                        key: 0,
                    })
                }
            },
            coin,
        )
    }

    /// `attach_peer`/`set_connection` tail — bind `conn_key` to the
    /// swarm entry: prev-amount restore (returned for the caller to
    /// fold into the conn's stats), `pe_support` re-arm on MSE
    /// success, `confirmed_supports_utp` on a successful uTP leg,
    /// `++m_num_seeds` for a seed-flagged entry.
    ///
    /// `was_pending` = the entry's mark was a dial placeholder
    /// (`conn_key` 0) — the attach settles the `num_connecting`
    /// bookkeeping upstream runs in `peer_connection::disconnect`'s
    /// `m_connecting` leg / ctor's `inc_num_connecting`.
    #[allow(clippy::too_many_arguments)]
    pub fn peer_connected_attach(
        &mut self,
        index: usize,
        encrypted: bool,
        session_time: i32,
        conn_key: u64,
        conn_fast_reconnect: bool,
        was_utp: bool,
        was_pending: bool,
    ) -> (i64, i64) {
        if was_pending {
            self.num_connecting = self.num_connecting.saturating_sub(1);
            if self.peer_list.peers()[index].seed {
                self.num_connecting_seeds = self.num_connecting_seeds.saturating_sub(1);
            }
        }
        let (dl, ul) =
            self.peer_list
                .set_connected(index, session_time, conn_fast_reconnect, conn_key);
        if let Some(p) = self.peer_list.peers_mut().get_mut(index) {
            p.pe_support = encrypted;
            // a successful uTP dial confirms the transport —
            // upstream `peer_connection` on socket-connect.
            if was_utp {
                p.confirmed_supports_utp = true;
                p.supports_utp = false;
            }
            // upstream `++m_num_seeds` on attach when the peer was
            // already seed-flagged (e.g. a re-dial to a known seed).
            // `++m_num_seeds` on attach — outgoing dials bumped it
            // at `try_connect_peer` (conn-creation time upstream),
            // incoming attaches bump here (`attach_peer` tail).
            if p.seed && !was_pending {
                self.num_seeds += 1;
            }
        }
        (dl, ul)
    }

    /// the OUTGOING half of attach — the entry was marked by
    /// `try_connect_peer` (`conn_key` 0): find it, settle the dial
    /// bookkeeping, bind. Returns the index + restored prev bytes;
    /// `None` when the entry vanished or was stolen by an incoming
    /// conn (`new_connection` killed the pending dial — upstream
    /// `i->connection->disconnect()` tore the conn object down, so
    /// its attach is a no-op).
    #[allow(clippy::too_many_arguments)]
    pub fn peer_connected_outgoing(
        &mut self,
        remote: std::net::SocketAddr,
        encrypted: bool,
        session_time: i32,
        conn_key: u64,
        conn_fast_reconnect: bool,
        was_utp: bool,
        our_pid: Option<bitdaemon_common::PeerId>,
    ) -> Option<(usize, i64, i64)> {
        // `Some(pid)` = a marked dial (`try_connect_peer` /
        // `register_manual_dial` registered `dial_pids[remote]`): the
        // mapping must still be OURS — a missing or foreign mapping
        // means the mark was consumed while the dial was in flight
        // (`new_connection` dup-resolution winner, `peer_connect_failed`,
        // an ip_filter/ban sweep). Upstream destructed that conn
        // object — ours is a zombie, dropped at attach.
        match our_pid {
            Some(pid) => {
                if self.dial_pids.get(&remote) == Some(&pid) {
                    self.dial_pids.remove(&remote);
                } else {
                    return None;
                }
            }
            // unmarked conns (`spawn_dial` without a mark — tests)
            // just consume whatever stale mapping exists.
            None => {
                self.dial_pids.remove(&remote);
            }
        }
        self.dial_locals.remove(&remote);
        let i = match self.peer_index(remote) {
            // `self_connection` banned + cleared the entry while the
            // dial was in flight — the conn was destructed upstream.
            Some(i) if self.peer_list.peers()[i].banned => return None,
            Some(i) => i,
            // unmarked conn with no entry — a direct caller-driven
            // dial never ran `try_connect_peer`; upstream
            // `connect_to_peer` resolves `peerinfo` off the handle
            // path, ours inserts a connectable entry at attach.
            // (Marked dials always have an entry — `None` here is a
            // zombie.)
            None if our_pid.is_none() => {
                let st = self.peer_list_state.clone();
                self.peer_list
                    .add_peer(
                        remote,
                        bitdaemon_common::flags::PeerSourceFlags::NONE,
                        bitdaemon_common::flags::PexFlags::NONE,
                        &st,
                    )?
                    .0
            }
            None => return None,
        };
        let bound = self.peer_list.peers()[i].conn_key;
        // a nonzero key that isn't ours means `new_connection`
        // resolved a duplicate by keeping the OTHER conn — this
        // dial's conn is the one upstream's `disconnect()` killed.
        if bound != 0 && bound != conn_key {
            return None;
        }
        // `was_pending` = the entry carried a dial mark
        // (`connection` set with `conn_key` 0) — a fresh manual-dial
        // entry has neither and must not decrement `num_connecting`.
        let was_pending = bound == 0 && self.peer_list.peers()[i].connection.is_some();
        let (dl, ul) = self.peer_connected_attach(
            i,
            encrypted,
            session_time,
            conn_key,
            conn_fast_reconnect,
            was_utp,
            was_pending,
        );
        Some((i, dl, ul))
    }

    /// `peer_connection::peer_rank` of a live conn — its swarm
    /// entry's BEP-40 rank (0 when the conn has no entry, matching
    /// `m_peer_info == nullptr`).
    #[must_use]
    fn conn_rank(&self, c: &PeerConn) -> u32 {
        c.remote.and_then(|r| self.peer_index(r)).map_or(0, |i| {
            self.peer_list.peers()[i].rank(
                &self.peer_list_state.external_ip,
                self.peer_list_state.external_port,
            )
        })
    }

    /// `peer_rank` for a swarm endpoint (the conn may not be in
    /// `peers` yet — `new_connection` already made its entry).
    #[must_use]
    pub fn rank_of_addr(&self, remote: std::net::SocketAddr) -> u32 {
        self.peer_index(remote).map_or(0, |i| {
            self.peer_list.peers()[i].rank(
                &self.peer_list_state.external_ip,
                self.peer_list_state.external_port,
            )
        })
    }

    /// `find_lowest_ranking_peer` — the non-disconnecting conn with
    /// the lowest `peer_rank`, returned with its rank.
    #[must_use]
    pub fn lowest_ranking_peer(&self) -> Option<(u64, u32)> {
        self.peers
            .iter()
            .filter(|(_, c)| !c.disconnecting)
            .map(|(id, c)| (*id, self.conn_rank(c)))
            .min_by_key(|(_, rank)| *rank)
    }

    /// `connection_closed` — clear the connection flag + stamp
    /// `last_connected` for the dial backoff. `key` is the conn's
    /// `conn_key` binding (upstream `c.peer_info_struct()` — the
    /// entry survives an `update_peer_port` port rewrite, which a
    /// bare endpoint lookup would miss); `0` falls back to the
    /// endpoint match for call sites without a bound conn (dial
    /// marks, user bans).
    pub fn peer_disconnected_addr(
        &mut self,
        remote: std::net::SocketAddr,
        session_time: i32,
        failed: bool,
        conn_fast_reconnect: bool,
        key: u64,
    ) {
        let i = if key != 0 {
            self.peer_list
                .peers()
                .iter()
                .position(|p| p.conn_key == key)
                .or_else(|| self.peer_index(remote))
        } else {
            self.peer_index(remote)
        };
        if let Some(i) = i {
            // a pending dial mark dies here — its dial tables leave
            // too (the conn object owning them is gone upstream).
            if self.peer_list.peers()[i].conn_key == 0
                && self.peer_list.peers()[i].connection.is_some()
            {
                // dial tables key on the dialed endpoint — the
                // entry's own `ip()` (== `remote` while marked, but
                // `update_peer_port` can't have touched a marked
                // entry).
                let ep = self.peer_list.peers()[i].ip().unwrap_or(remote);
                self.dial_locals.remove(&ep);
                if let Some(pid) = self.dial_pids.remove(&ep) {
                    self.outgoing_pids.remove(&pid);
                }
                // `dec_num_connecting` — the pending dial's conn
                // object dies here (upstream `remove_peer` on a
                // conn that never completed). `peer_connect_failed`
                // has its own copy of this leg (it runs before the
                // mark could be re-bound).
                self.num_connecting = self.num_connecting.saturating_sub(1);
                if self.peer_list.peers()[i].seed {
                    self.num_connecting_seeds = self.num_connecting_seeds.saturating_sub(1);
                    self.num_seeds = self.num_seeds.saturating_sub(1);
                }
            }
            // `failed` is the conn's `m_failed` (severity > normal at
            // disconnect); `conn_fast_reconnect` is its
            // `m_fast_reconnect` (MSE-retry conns keep the dial
            // stamp so the plaintext fallback isn't cooled out).
            let st = self.peer_list_state.clone();
            self.peer_list
                .connection_closed(i, session_time, conn_fast_reconnect, failed, &st);
        }
    }

    /// `peer_info_struct()` by `conn_key` — the bound-connection
    /// lookup (`update_peer_port`, disconnect bookkeeping) that
    /// survives endpoint rewrites.
    #[must_use]
    pub fn entry_index_by_conn(&self, key: u64) -> Option<usize> {
        if key == 0 {
            return None;
        }
        self.peer_list
            .peers()
            .iter()
            .position(|p| p.conn_key == key)
    }

    pub(crate) fn peer_index(&self, remote: std::net::SocketAddr) -> Option<usize> {
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
            // upstream `on_parole()` reads the `torrent_peer` flag
            // live — mirror it onto the conn at attach (a peer that
            // failed hash checks on a previous connection comes back
            // on parole).
            if let Some(i) = self.peer_index(remote) {
                conn.on_parole = self.peer_list.peers()[i].on_parole;
            }
            for ext in &self.extensions {
                if let Some(pp) = ext.new_peer_connection(&remote) {
                    conn.plugins.push(pp);
                }
            }
        }
        self.peers.insert(id, conn);
    }

    /// `num_peers_down_requests` bookkeeping — recompute this
    /// torrent's share (conns with a non-empty `download_queue`)
    /// and apply the delta to the session-wide counter. A removed
    /// conn simply drops out of the count — matching upstream's
    /// `disconnect` decrement.
    pub fn sync_down_requests(&mut self, counters: &crate::counters::Counters) {
        let n = self
            .peers
            .values()
            .filter(|c| !c.pipeline.download_queue.is_empty())
            .count() as i64;
        let delta = n - self.down_request_peers;
        if delta != 0 {
            self.down_request_peers = n;
            counters.add(crate::counters::Counter::NumPeersDownRequests, delta);
        }
    }

    /// `peer_disconnected` — refcount down everything it had and
    /// abort its in-flight requests (upstream `peer_lost` +
    /// `clear_peer` semantics).
    pub fn remove_peer(
        &mut self,
        id: u64,
        counters: &crate::counters::Counters,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
        eff: &mut Vec<TorrentEffect>,
    ) {
        let t_ignores_unchoke = self.ignores_unchoke_slots(pool);
        let Some(c) = self.peers.remove(&id) else {
            return;
        };
        // `torrent::remove_peer` — picker refcounts only drop once
        // connections are initialized (a pre-metadata conn's raw
        // bitfield was never inc'd). Seeds drop via
        // `dec_refcount_all`, not per-piece (the picker tracks them
        // in `seeds`, not `peer_count`).
        if self.connections_initialized
            && let Some(p) = self.picker.as_mut()
        {
            if c.is_seed() {
                p.dec_refcount_all();
            } else {
                for i in 0..c.peer_pieces.size() {
                    if c.peer_pieces.get_bit(i) {
                        p.dec_refcount(PieceIndex(i as i32));
                    }
                }
            }
        }
        // abort this conn's in-flight + unsent blocks back into the
        // picker (`remove_peer` runs before the queues are torn down
        // upstream). Only `!timed_out && !not_wanted` entries release
        // picker state — a timed-out or unwanted block already gave
        // its refcount back (the double-abort would free a block
        // another peer re-picked).
        if let Some(p) = self.picker.as_mut() {
            let key = c.peer_key();
            for b in c.outstanding_entries() {
                if !b.timed_out && !b.not_wanted {
                    p.abort_download(b.block, key);
                }
            }
        }
        // `smart_ban::on_erase_peers` — entries recorded against a
        // dead conn handle are dropped with it.
        self.smart_ban.erase_peers(&[id]);
        // `~peer_connection` — transition gauges drop with the conn
        // (upstream decrements each live flag; `num_peers_connected`
        // is recomputed via `set` on our side).
        if c.interesting {
            counters.add(crate::counters::Counter::NumPeersDownInterested, -1);
        }
        if c.peer_interested {
            counters.add(crate::counters::Counter::NumPeersUpInterested, -1);
        }
        if !c.choked {
            counters.add(crate::counters::Counter::NumPeersUpUnchokedAll, -1);
            // `ignore_unchoke_slots` conns (e.g. the `local` class)
            // never occupy an upload slot — the gauge skips them.
            if !c.ignores_unchoke_slots(pool) && !t_ignores_unchoke {
                counters.add(crate::counters::Counter::NumPeersUpUnchoked, -1);
            }
        }
        if !c.peer_choked {
            counters.add(crate::counters::Counter::NumPeersDownUnchoked, -1);
        }
        if c.pipeline.endgame {
            counters.add(crate::counters::Counter::NumPeersEndGame, -1);
        }
        // `peer_connection::~peer_connection` — a queued bandwidth
        // request of a dead conn is dropped and its assigned quota
        // returned on the next `update_quotas` pass.
        c.bw.disconnecting
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if c.disk_blocked {
            // `~peer_connection` clears the `bw_disk` channel bit —
            // a conn dropped while disk-gated leaves the gauge.
            counters.add(crate::counters::Counter::NumPeersDownDisk, -1);
        }
        if c.up_disk_blocked {
            counters.add(crate::counters::Counter::NumPeersUpDisk, -1);
        }
        // an unchoked conn vacates an upload slot — `m_num_uploads`
        // derives per-call here so the removal is enough; upstream
        // `trigger_unchoke`/`trigger_optimistic_unchoke` zero the
        // scalers so the next choker tick re-picks immediately.
        if !c.choked && !c.ignores_unchoke_slots(pool) && !t_ignores_unchoke {
            self.triggered_unchoke = true;
        }
        // `optimistically_unchoked` on the swarm entry drops with the
        // conn and re-arms the optimistic slot rotation.
        if let Some(i) = c.remote.and_then(|r| self.peer_index(r)) {
            let p = &mut self.peer_list.peers_mut()[i];
            if p.optimistically_unchoked {
                p.optimistically_unchoked = false;
                counters.add(crate::counters::Counter::NumPeersUpUnchokedOptimistic, -1);
                self.triggered_optimistic = true;
            }
            if p.seed {
                self.num_seeds = self.num_seeds.saturating_sub(1);
            }
            // `prev_amount_*` — the swarm entry keeps the conn's
            // lifetime KiB totals for `peer_info`/`est_reciprocation`.
            p.prev_amount_download = p
                .prev_amount_download
                .saturating_add((c.stat_down_total / 1024).max(0) as u32);
            p.prev_amount_upload = p
                .prev_amount_upload
                .saturating_add((c.stat_up_total / 1024).max(0) as u32);
        }
        // `~peer_connection` — an outgoing conn's `our_pid` leaves
        // `m_outgoing_pids` (upstream `remove_peer` erases it — a
        // pid never reported means a later remote claiming it is
        // no longer a self-connection).
        if c.outgoing
            && let Some(pid) = c.our_peer_id
        {
            self.outgoing_pids.remove(&pid);
        }
        // `m_stat` payload channels are session-lifetime — the
        // conn's transfer folds into the torrent accumulators so
        // `total_payload_*`/`total_uploaded` stay monotonic after
        // the conn is gone.
        self.payload_downloaded += c.stat_down_total;
        self.payload_uploaded += c.stat_up_total;
        self.protocol_downloaded += c.stat_down_protocol;
        self.protocol_uploaded += c.stat_up_protocol;
        self.ip_overhead_downloaded += c.stat_down_ip;
        self.ip_overhead_uploaded += c.stat_up_ip;
        self.update_want_peers();
        // `update_want_tick` — our `want_tick` is a computed
        // predicate, so removing the conn is the update.
        // `remove_peer` tail — the last conn under graceful pause
        // completes the pause (`set_paused(true)` → `do_pause`
        // posts `torrent_paused_alert` and clears the mode).
        if self.graceful_pause && self.is_paused() && self.peers.is_empty() {
            eff.push(TorrentEffect::GracefulPauseDone);
        }
    }

    /// resolve a picker's downloader key (`block_info.peer`,
    /// `peer_key()`) back to its live connection id.
    /// `smart_ban`'s ban application — upstream
    /// `on_read_{failed,ok}_block` tail: `find_peers(addr)`
    /// re-resolves the swarm entry (survives conn churn), an already
    /// `banned` entry short-circuits, else `ban_peer` marks it +
    /// `update_want_peers` + `num_banned_peers`, and the live conn —
    /// by handle or same-IP — gets `disconnect(peer_banned)` at
    /// normal severity.
    fn apply_smart_ban(
        &mut self,
        key: u64,
        addr: std::net::IpAddr,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let Some(i) = self.peer_list.find_peer(addr) else {
            return;
        };
        if self.peer_list.peers()[i].banned {
            return;
        }
        self.peer_list.ban_peer(i);
        counters.inc(crate::counters::Counter::NumBannedPeers);
        self.update_want_peers();
        let conn = self.conn_by_key(key).or_else(|| {
            self.peers
                .iter()
                .find(|(_, c)| c.remote.is_some_and(|r| r.ip() == addr))
                .map(|(id, _)| *id)
        });
        if let Some(peer) = conn {
            out.push(TorrentEffect::DisconnectPeer {
                peer,
                reason: "peer_banned",
                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
            });
        }
    }

    fn conn_by_key(&self, key: u64) -> Option<u64> {
        self.peers
            .iter()
            .find_map(|(id, c)| (c.peer_key() == Some(key)).then_some(*id))
    }

    /// `piece_finished`'s trust loop — every peer that contributed
    /// blocks to a *passing* piece clears parole and gains a trust
    /// point (cap 8). Upstream's `received_valid_data` only fires
    /// peer-plugin hooks here; ours live on the torrent plugin list
    /// already (`on_piece_pass`).
    fn reward_downloader(&mut self, key: u64) {
        let Some(id) = self.conn_by_key(key) else {
            // downloader disconnected — upstream still updates the
            // `torrent_peer` (it outlives conns); our picker key is
            // conn-derived so the map is lost. Documented gap.
            return;
        };
        if let Some(c) = self.peers.get_mut(&id) {
            c.on_parole = false;
        }
        if let Some(remote) = self.peers.get(&id).and_then(|c| c.remote)
            && let Some(i) = self.peer_index(remote)
        {
            let p = &mut self.peer_list.peers_mut()[i];
            p.on_parole = false;
            p.trust_points = (p.trust_points + 1).min(8);
        }
    }

    /// `penalize_peers` — parole + trust/hashfail bookkeeping on the
    /// culprit set (dedup'd downloader keys; `None` entries are
    /// disconnected peers — untraceable, skipped). `known_bad` is
    /// upstream's certainty flag (`peers.size() == 1 ||
    /// !blocks.empty()`): a certain culprit is banned outright,
    /// repeat offenders (`trust_points <= -7`) always are.
    fn penalize_peers(
        &mut self,
        peers: &std::collections::BTreeSet<Option<u64>>,
        known_bad: bool,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        for key in peers.iter().flatten() {
            let key = *key;
            // `web_peer_connection::received_invalid_data` — a
            // certain-culprit web seed gets banned upstream
            // (`peer_info.banned` → `connect_to_url_seed` disables
            // it); `ban_web_seeds` gates that. A seed that has
            // delivered good data keeps serving — upstream only
            // clears its have-bits for the failed piece's files
            // (our seeds are whole-torrent, so a softer retry is
            // the closest analogue short of per-file bitfields).
            if let Some(url) = self
                .web_seed_state
                .iter()
                .find(|(_, st)| st.key == key)
                .map(|(u, _)| u.clone())
            {
                if known_bad && s.ban_web_seeds {
                    if let Some(st) = self.web_seed_state.get_mut(&url) {
                        st.disabled = true;
                    }
                    // banned conns die upstream — refcount drops.
                    self.dec_web_seed_refcount(&url);
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::UrlSeed {
                        ih: self.info_hash,
                        url,
                        msg: "peer banned".to_string(),
                    }));
                }
                continue;
            }
            if key == Self::WEB_SEED_PEER {
                continue;
            }
            let allow_disconnect = true;
            let Some(id) = self.conn_by_key(key) else {
                continue;
            };
            let Some(remote) = self.peers.get(&id).and_then(|c| c.remote) else {
                continue;
            };
            if let Some(c) = self.peers.get_mut(&id)
                && s.use_parole_mode
            {
                c.on_parole = true;
            }
            let Some(i) = self.peer_index(remote) else {
                continue;
            };
            let ban = {
                let p = &mut self.peer_list.peers_mut()[i];
                if s.use_parole_mode {
                    p.on_parole = true;
                }
                // decrease more than we increase — keeps the allowed
                // fail/pass ratio low (upstream −2/+1).
                p.trust_points = (p.trust_points - 2).max(-7);
                p.hashfails = p.hashfails.saturating_add(1);
                p.trust_points <= -7 || (known_bad && allow_disconnect)
            };
            if ban {
                self.peer_list.ban_peer(i);
                counters.add(crate::counters::Counter::BannedForHashFailure, 1);
                // `update_want_peers` — a banned peer is no longer a
                // connect candidate; upstream re-evaluates here.
                self.update_want_peers();
                out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerBan {
                    ih: self.info_hash,
                    ip: remote,
                }));
                // `peer->disconnect(errors::too_many_corrupt_pieces)`
                // — a peer-error disconnect, counted as a failure
                // (the swarm entry's `failcount` grows).
                out.push(TorrentEffect::DisconnectPeer {
                    peer: id,
                    reason: "too many corrupt pieces",
                    // torrent.cpp:5200 — default (normal) severity;
                    // the ban itself is separate.
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
            }
        }
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
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        // upstream `fill_send_buffer` bails when the torrent is
        // being torn down (`t->is_aborted()`); a deleted-storage
        // torrent drains its queue with rejects — removal here drops
        // the peers table entirely, which is the same effect.
        if self.aborted {
            return;
        }
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        let watermark = (i64::from(c.rate_up_speed) * i64::from(s.send_buffer_watermark_factor)
            / 100)
            .clamp(s.send_buffer_low_watermark, s.send_buffer_watermark as i64)
            .max(0) as usize;
        // upstream iterates the whole queue, not just the front: a
        // seed-mode request blocked on lazy hash verification stays
        // queued and later requests are still serviced.
        let mut sent_a_piece = false;
        let mut i = 0;
        while i < c.upload_queue.len() && c.send_buffer + c.reading_bytes < watermark {
            let r = c.upload_queue[i];
            if self.seed_mode
                && !s.disable_hash_checks
                && (r.piece.0 as usize) < self.seed_verified.size()
                && !self.seed_verified.get_bit(r.piece.0 as usize)
            {
                // unverified promised piece — hash it first;
                // `verifying_piece`/`m_outstanding_piece_verification`
                // cap the fan-out (3 per peer) upstream.
                if self.seed_verifying.get_bit(r.piece.0 as usize)
                    || c.outstanding_piece_verification >= 3
                {
                    i += 1;
                    continue;
                }
                c.outstanding_piece_verification += 1;
                self.seed_verifying.set_bit(r.piece.0 as usize);
                out.push(TorrentEffect::SeedModeVerify {
                    peer: peer_id,
                    piece: r.piece,
                });
                i += 1;
                continue;
            }
            let have =
                (r.piece.0 as usize) < self.have.size() && self.have.get_bit(r.piece.0 as usize);
            if !have
                && !self.seed_mode
                && (r.piece.0 as usize) < self.predictive.size()
                && self.predictive.get_bit(r.piece.0 as usize)
            {
                // announced ahead of the write landing — hold the
                // request until the piece exists (upstream
                // `is_predictive_piece` hold).
                i += 1;
                continue;
            }
            c.upload_queue.remove(i);
            // `num_peers_up_requests` gauges peers holding a nonempty
            // `m_requests` — the counter drops back when the last
            // request leaves the queue.
            if c.upload_queue.is_empty() {
                counters.add(crate::counters::Counter::NumPeersUpRequests, -1);
            }
            if have || self.seed_mode {
                c.reading_bytes += r.length.max(0) as usize;
                sent_a_piece = true;
                out.push(TorrentEffect::DiskRead {
                    peer: peer_id,
                    req: r,
                });
            } else {
                // `write_reject_request` — `piece_rejects` counts the
                // decision; the wire message is BEP-6 only.
                counters.add(crate::counters::Counter::PieceRejects, 1);
                if c.supports_fast {
                    out.push(TorrentEffect::SendTo {
                        peer: peer_id,
                        msg: PeerMessage::RejectRequest(r),
                    });
                }
            }
            // upstream stamps `m_last_sent_payload` per *processed*
            // request — a rejected request still means we produced
            // bytes for this peer.
            c.last_sent_payload_ms = c.now_ms;
        }
        // upstream `fill_send_buffer` tail — a served read in share
        // mode re-runs the swarm rebalance (`recalc_share_mode`).
        if sent_a_piece && self.share_mode {
            self.recalc_share_mode(s, counters, out);
        }
        self.update_up_disk_gate(peer_id, s, counters, out);
    }

    /// `setup_send_buffer`'s upload-channel `bw_disk` leg — a conn
    /// whose send buffer is drained while disk reads are still in
    /// flight counts as disk-stalled (`num_peers_up_disk`); a stall
    /// at a full watermark with queued requests posts
    /// `send_buffer_watermark_too_low` once per crossing. The whole
    /// leg is gated on upload quota being available, like upstream.
    pub fn update_up_disk_gate(
        &mut self,
        peer_id: u64,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        // `setup_send` gates the whole leg on `m_quota[upload] > 0`
        // — a quota-starved conn isn't disk-stalled, it's throttled.
        let quota_up =
            c.bw.quota[crate::peer_conn::UPLOAD_CHANNEL].load(std::sync::atomic::Ordering::Relaxed);
        let stalled = c.send_buffer == 0 && c.reading_bytes > 0 && quota_up > 0;
        if stalled && !c.up_disk_blocked {
            c.up_disk_blocked = true;
            counters.inc(crate::counters::Counter::NumPeersUpDisk);
        } else if !stalled && c.up_disk_blocked {
            c.up_disk_blocked = false;
            counters.add(crate::counters::Counter::NumPeersUpDisk, -1);
        }
        if stalled
            && !c.connecting
            && !c.upload_queue.is_empty()
            && c.reading_bytes as i64 > s.send_buffer_watermark as i64 - 0x4000
        {
            out.push(TorrentEffect::Alert(crate::alerts::Alert::Performance {
                warning: "send_buffer_watermark_too_low".into(),
            }));
        }
    }

    /// `peer_has`/`peer_lost` — apply [`PeerEffect`]s from a peer
    /// dispatch into picker/disk intents.
    pub fn apply_peer_effects(
        &mut self,
        peer_id: u64,
        effects: Vec<PeerEffect>,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let key = self.peers.get(&peer_id).and_then(PeerConn::peer_key);
        let remote = self
            .peers
            .get(&peer_id)
            .and_then(|c| c.remote)
            .map_or_else(String::new, |ep| ep.to_string());
        // `incoming_piece` uses `incoming_redundant_piece_picks` for
        // the post-block pick when the payload was waste — set by the
        // `DiskWrite` arm, consumed by the next `MaybePick`.
        let mut waste_pick = false;
        // `incoming_piece` clears `m_snubbed` only on the write path
        // (the `is_downloaded` arm returns before the check) — the
        // `Unsnub` effect arms this flag, the `DiskWrite` arm applies
        // or drops it.
        let mut pending_unsnub = false;
        // `incoming_piece` returns before `mark_as_writing` when a
        // block is redundant or we're seeding — the `DiskWrite` arm
        // classifies it (it runs first in the batch) and records the
        // block here so the trailing `MarkDownloaded` is dropped.
        let mut skipped_writes: Vec<PieceBlock> = Vec::new();
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
                PeerEffect::DiskWrite(req, data, waste) => {
                    // `incoming_piece` — a seed ignores incoming
                    // payload (`waste_reason::piece_seed`), and a
                    // block the picker already finished is redundant
                    // (the queue-entry flags pick timed_out /
                    // cancelled / end_game / unknown); neither
                    // reaches disk — and neither clears a snub
                    // (upstream returns before the unsnub check).
                    let block = PieceBlock {
                        piece_index: req.piece,
                        block_index: req.start / self.block_size().max(1),
                    };
                    if self.is_seed() {
                        pending_unsnub = false;
                        self.add_redundant_bytes(
                            i64::from(req.length),
                            crate::peer_conn::WasteReason::Seed,
                            counters,
                        );
                        skipped_writes.push(block);
                        continue;
                    }
                    // upstream consults `is_downloaded` *before*
                    // `mark_as_writing` — `DiskWrite` precedes
                    // `MarkDownloaded` in the batch for exactly that
                    // reason.
                    if self.picker.as_ref().is_some_and(|p| p.is_downloaded(block)) {
                        pending_unsnub = false;
                        self.add_redundant_bytes(i64::from(req.length), waste, counters);
                        // upstream re-picks on the freed slot
                        // (`request_a_block` + `send_block_requests`)
                        // and counts it as `incoming_redundant_piece_
                        // picks` — the `MaybePick` next in the batch
                        // picks up this flag.
                        waste_pick = true;
                        skipped_writes.push(block);
                        continue;
                    }
                    // `incoming_piece` — the block is going to disk:
                    // a timely arrival un-snubs the peer here
                    // (upstream's `m_snubbed = false` +
                    // `peer_unsnubbed_alert` on the write path only).
                    if pending_unsnub {
                        pending_unsnub = false;
                        if let Some(c) = self.peers.get_mut(&peer_id) {
                            c.pipeline.snubbed = false;
                        }
                        out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerUnsnubbed {
                            ih: self.info_hash,
                            remote: remote.clone(),
                        }));
                    }
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
                            counters.inc(crate::counters::Counter::InvalidPieceRequests);
                            counters.add(crate::counters::Counter::PieceRejects, 1);
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
                    // only gets BEP-6 `accept_fast` pieces (the set we
                    // offered it — `allowed_fast` is the incoming side);
                    // a request passes the shape gate when we hold the
                    // piece, promised it predictively, or run seed_mode.
                    // Accepted requests post `incoming_request_alert`;
                    // rejected ones post `invalid_request_alert`.
                    let allowed = self.peers.get(&peer_id).is_some_and(|c| {
                        (!c.choked || c.accept_fast.contains(&req.piece))
                            && (((req.piece.0 as usize) < self.have.size()
                                && self.have.get_bit(req.piece.0 as usize))
                                || self.seed_mode
                                || ((req.piece.0 as usize) < self.predictive.size()
                                    && self.predictive.get_bit(req.piece.0 as usize)))
                    });
                    if allowed {
                        if self
                            .peers
                            .get(&peer_id)
                            .is_some_and(|c| c.upload_queue.len() == 1)
                        {
                            counters.add(crate::counters::Counter::NumPeersUpRequests, 1);
                        }
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
                        self.fill_send_buffer(peer_id, s, counters, out);
                    } else {
                        counters.add(crate::counters::Counter::PieceRejects, 1);
                        if let Some(c) = self.peers.get_mut(&peer_id) {
                            c.upload_queue.retain(|u| {
                                !(u.piece == req.piece
                                    && u.start == req.start
                                    && u.length == req.length)
                            });
                            // rejected requests never entered the
                            // `num_peers_up_requests` gauge, so the
                            // removal can't drop it either.
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
                PeerEffect::CancelRequest(_) => {
                    // `incoming_cancel` — the admission already
                    // counted this conn in `num_peers_up_requests`;
                    // cancelling the last queued request drops it.
                    if self
                        .peers
                        .get(&peer_id)
                        .is_some_and(|c| c.upload_queue.is_empty())
                    {
                        counters.add(crate::counters::Counter::NumPeersUpRequests, -1);
                    }
                }
                PeerEffect::SendChoke => {
                    // `write_choke` — bare re-choke reminder; not the
                    // `choke_peer` machinery (no queue drain, no
                    // unchoke-slot bookkeeping — the peer is choked
                    // already), just the wire message + counter.
                    counters.inc(crate::counters::Counter::NumOutgoingChoke);
                    out.push(TorrentEffect::SendTo {
                        peer: peer_id,
                        msg: PeerMessage::Choke,
                    });
                }
                PeerEffect::UnwantedBlockAlert(b) => {
                    // `start_receive_piece` — the alert fires when
                    // the unsolicited block enters the queue, before
                    // `incoming_piece` writes it; waste attribution
                    // stays downstream (the `is_downloaded` consult).
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::UnwantedBlock {
                        ih: self.info_hash,
                        remote: remote.clone(),
                        piece: b.piece_index,
                        block: b.block_index as u32,
                    }));
                }
                PeerEffect::UnwantedBlock(b, len) => {
                    // `incoming_piece` — the payload was never
                    // requested: `unwanted_block_alert` +
                    // `piece_unknown` waste.
                    self.add_redundant_bytes(
                        i64::from(len),
                        crate::peer_conn::WasteReason::Unknown,
                        counters,
                    );
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::UnwantedBlock {
                        ih: self.info_hash,
                        remote: remote.clone(),
                        piece: b.piece_index,
                        block: b.block_index as u32,
                    }));
                }
                PeerEffect::SeedWaste(len) => {
                    // `incoming_piece` is_seed leg — payload received
                    // while seeding counts as `piece_seed` waste; no
                    // `unwanted_block_alert` upstream.
                    self.add_redundant_bytes(
                        i64::from(len),
                        crate::peer_conn::WasteReason::Seed,
                        counters,
                    );
                }
                PeerEffect::BlockFinished(b) => {
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::BlockFinished {
                        ih: self.info_hash,
                        piece: b.piece_index,
                        block: b.block_index as u32,
                    }));
                }
                PeerEffect::Unsnub => {
                    // Deferred — upstream clears `m_snubbed` only on
                    // the write path (`incoming_piece` returns
                    // earlier for `is_downloaded` payloads). The
                    // `DiskWrite` arm next in the batch applies it.
                    pending_unsnub = true;
                }
                PeerEffect::PeerError(msg) => {
                    // `peer_error_alert` without the disconnect —
                    // upstream posts it for non-fatal complaints
                    // (empty piece, hash msg misuse handled elsewhere
                    // does disconnect).
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerError {
                        ih: self.info_hash,
                        remote: remote.clone(),
                        op: "bittorrent".into(),
                        error: msg.into(),
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
                    // `write_reject_request` — `piece_rejects` counts
                    // the decision unconditionally; the wire message
                    // itself is BEP-6 fast-ext only.
                    counters.add(crate::counters::Counter::PieceRejects, 1);
                    if self.peers.get(&peer_id).is_some_and(|c| c.supports_fast) {
                        out.push(TorrentEffect::SendTo {
                            peer: peer_id,
                            msg: PeerMessage::RejectRequest(req),
                        });
                    }
                }
                PeerEffect::MarkDownloaded(b) => {
                    // upstream returns before `mark_as_writing` for
                    // seed/redundant blocks — the `DiskWrite` arm
                    // classified them already.
                    if skipped_writes.contains(&b) {
                        continue;
                    }
                    if let Some(p) = self.picker.as_mut() {
                        p.mark_as_writing(b, key);
                    }
                    // `incoming_piece` — predictive piece announce,
                    // verbatim gates: `get_downloaders` returns one
                    // entry *per block* (not deduped), so the
                    // `len == 1` leg only exists for single-block
                    // pieces — which post-`mark_as_writing` never have
                    // `requested > 0`. Upstream is the same: the
                    // feature is inert but the check must stay
                    // observable-identical.
                    if s.predictive_piece_announce > 0
                        && let Some(p) = self.picker.as_ref()
                    {
                        let st = p.piece_info(b.piece_index);
                        let num_blocks = p.blocks_in_piece(b.piece_index);
                        if st.requested > 0
                            && i64::from(st.writing)
                                + i64::from(st.finished)
                                + i64::from(st.requested)
                                == i64::from(num_blocks)
                        {
                            let d = p.get_downloaders(b.piece_index);
                            if d.len() == 1 {
                                // upstream: `statistics()
                                // .download_payload_rate()` — the
                                // payload channel only.
                                let rate = d[0]
                                    .and_then(|k| {
                                        self.peers.values().find(|c| c.peer_key() == Some(k))
                                    })
                                    .map_or(0, |c| c.rate_down_payload);
                                let bytes_left =
                                    i64::from(st.requested) * i64::from(self.block_size());
                                let eta_ms = if rate > 0 {
                                    bytes_left * 1000 / i64::from(rate)
                                } else {
                                    0
                                };
                                if rate > 1000 && eta_ms < i64::from(s.predictive_piece_announce) {
                                    self.predicted_have_piece(b.piece_index, s, out);
                                }
                            }
                        }
                    }
                }
                PeerEffect::AbortDownload(b) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.abort_download(b, key);
                    }
                }
                PeerEffect::GracefulCheck => {
                    // `check_graceful_pause` — under graceful pause a
                    // conn whose `m_outstanding_bytes` has drained is
                    // disconnected (`errors::torrent_paused`); the
                    // torrent flag itself stays set until resume.
                    // `peer_connection::disconnect` sets
                    // `m_disconnecting` *immediately* upstream, so the
                    // rest of the effect batch (re-pick, request
                    // flush) must observe it too. Upstream calls this
                    // only on `incoming_piece`'s *write* path — a
                    // block the `DiskWrite` arm just classified
                    // redundant takes the early return instead.
                    if skipped_writes.is_empty()
                        && self.graceful_pause
                        && self
                            .peers
                            .get(&peer_id)
                            .is_some_and(|c| !c.disconnecting && c.pipeline.outstanding_bytes <= 0)
                    {
                        if let Some(c) = self.peers.get_mut(&peer_id) {
                            c.disconnecting = true;
                            c.bw.disconnecting
                                .store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        out.push(TorrentEffect::DisconnectPeer {
                            peer: peer_id,
                            reason: "torrent_paused",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                        });
                    }
                }
                PeerEffect::MaybePick(ctr) => {
                    if self.drive_peer(peer_id, s, counters, out)
                        && let Some(ctr) = ctr
                    {
                        let ctr =
                            if waste_pick && ctr == crate::counters::Counter::IncomingPiecePicks {
                                crate::counters::Counter::IncomingRedundantPiecePicks
                            } else {
                                ctr
                            };
                        counters.inc(ctr);
                    }
                    waste_pick = false;
                }
                PeerEffect::FlushRequests => {
                    // `incoming_reject_request` / non-FAST
                    // `incoming_choke` tail — upstream ends both with
                    // an unconditional `send_block_requests` (no
                    // pick): queued entries — including a parole
                    // block just requeued at the head — go on the
                    // wire immediately.
                    self.flush_requests(peer_id, out);
                }
                PeerEffect::ExtHandshake => {
                    // `metadata_size` advertised → pre-size the
                    // download tracker (`metadata_size` upstream —
                    // ignored once valid metadata landed).
                    // upstream `on_extension_handshake`: only a
                    // positive `metadata_size` sizes the tracker; a
                    // size-less peer also clears `m_has_metadata`
                    // (handled in dispatch).
                    if let Some(sz) = self
                        .peers
                        .get(&peer_id)
                        .and_then(|c| c.metadata_size)
                        .filter(|&sz| sz > 0)
                    {
                        self.metadata_tracker
                            .metadata_size(sz, self.valid_metadata());
                    }
                    // the `p` listen port also lands on the swarm
                    // entry — `torrent::update_peer_port` keeps ut_pex
                    // gossiping a dialable endpoint for inbound conns.
                    if let Some(c) = self.peers.get(&peer_id)
                        && c.received_listen_port
                        && let (Some(remote), Some(port)) = (c.remote, c.peer_listen_port)
                        && remote.port() != port
                        && let Some(i) = self.peer_index(remote)
                    {
                        // `peer_list::update_peer_port` — `false` on a
                        // collision with an already-connected entry:
                        // this peer is a duplicate →
                        // `disconnect(errors::duplicate_peer_id)` at the
                        // default (normal) severity upstream.
                        if !self.peer_list.update_peer_port(
                            port,
                            i,
                            bitdaemon_common::flags::PeerSourceFlags::INCOMING,
                            &self.peer_list_state,
                        ) {
                            out.push(TorrentEffect::DisconnectPeer {
                                peer: peer_id,
                                reason: "duplicate_peer_id",
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                            });
                        }
                        // `torrent::update_peer_port` tail —
                        // `peers_erased(st.erased)` + `update_want_peers`.
                        self.update_want_peers();
                    }
                    // peer just advertised ut_metadata — upstream
                    // `on_extension_handshake` runs
                    // `maybe_send_request` (not the full tick).
                    self.maybe_send_metadata_request(peer_id, out);
                    // v2: peers that know `hash_request` can fill
                    // missing merkle layers (`maybe_send_hash_request`).
                    self.maybe_send_hash_request(peer_id, out);
                    // upstream `on_extended` tail — the handshake
                    // carries `upload_only`/`share_mode`, so redundant
                    // upload-only conns are re-evaluated here.
                    self.disconnect_if_redundant(peer_id, out);
                }
                PeerEffect::MetadataPacket(body) => {
                    self.on_metadata_packet(peer_id, &body, s, counters, out);
                }
                PeerEffect::PexPacket(body) => {
                    self.on_pex_packet(peer_id, &body, s.max_pex_peers as usize, counters, out);
                }
                PeerEffect::I2pPexPacket(body) => {
                    self.on_i2p_pex_packet(peer_id, &body, counters, out);
                }
                PeerEffect::SendAllowedSet => {
                    self.send_allowed_set(peer_id, s.allowed_fast_set_size, out);
                }
                PeerEffect::AllowedFast(i) => {
                    // `incoming_allowed_fast` — conn-disconnecting
                    // drop, negative drop; with metadata the index is
                    // bounded by the peer's bitfield and pieces we
                    // already have are skipped; then the offer is
                    // recorded.
                    let drop = self.peers.get(&peer_id).is_none_or(|c| {
                        c.disconnecting
                            || i.0 < 0
                            || (self.valid_metadata()
                                && (i.0 >= c.peer_pieces.size() as i32
                                    || self.have.get_bit(i.0.max(0) as usize)))
                    });
                    if !drop {
                        if let Some(c) = self.peers.get_mut(&peer_id) {
                            c.on_allowed_fast(i);
                        }
                    }
                    // `peer_is_interesting` — the peer offers a piece
                    // we want: send `Interested` (update_interest at
                    // the tail covers it) and pick a block unless
                    // we're choked with an empty allowed-fast set.
                    let interesting = !drop
                        && self.valid_metadata()
                        && self.picker.is_some()
                        && self.piece_priority(i).is_some_and(|p| {
                            p != bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD
                        })
                        && !self.have.get_bit(i.0 as usize)
                        && self.peers.get(&peer_id).is_some_and(|c| c.has_piece(i));
                    let choked_no_fast = self
                        .peers
                        .get(&peer_id)
                        .is_none_or(|c| c.peer_choked && c.allowed_fast.is_empty());
                    if interesting && !choked_no_fast && self.drive_peer(peer_id, s, counters, out)
                    {
                        counters.inc(crate::counters::Counter::InterestingPiecePicks);
                    }
                }
                PeerEffect::RemoteShareMode => {
                    // `set_share_mode` — only when the session
                    // supports share mode; seeds can't be demoted.
                    if s.support_share_mode
                        && let Some(c) = self.peers.get_mut(&peer_id)
                        && !c.is_seed()
                    {
                        c.remote_share_mode = true;
                    }
                }
                PeerEffect::Holepunch(hp) => {
                    self.on_holepunch(peer_id, hp, out);
                }
                PeerEffect::DhtPort(port) => {
                    // upstream `incoming_dht_port` → `add_dht_node`
                    // runs UNCONDITIONALLY (a peer can announce its
                    // DHT port without having advertised the
                    // handshake bit)…
                    let mut addr = None;
                    if let Some(c) = self.peers.get(&peer_id)
                        && let Some(mut a) = c.remote
                    {
                        a.set_port(port);
                        addr = Some(a);
                    }
                    if let Some(a) = addr {
                        out.push(TorrentEffect::DhtNode(a));
                    }
                    // …then `on_dht_port` flips `m_supports_dht_port`
                    // and answers with our own port when the bitfield
                    // already went out (`m_sent_bitfield`).
                    if let Some(c) = self.peers.get_mut(&peer_id)
                        && !c.supports_dht
                    {
                        c.supports_dht = true;
                        if c.sent_bitfield
                            && let Some(p) = c.local_dht_port
                        {
                            let msg = PeerMessage::DhtPort(p);
                            c.send_buffer += bitdaemon_net::peer_wire::wire_len(&msg);
                            out.push(TorrentEffect::SendTo { peer: peer_id, msg });
                        }
                    }
                }
                PeerEffect::YourIp(ip) => {
                    out.push(TorrentEffect::YourIp(ip));
                }
                PeerEffect::ListenPort(port) => {
                    // `peer_connection::on_extended` →
                    // `peer_list::update_peer_port` — resolve the
                    // entry by `conn_key` so a peer whose source port
                    // is being rewritten still matches. `false`
                    // means the new endpoint collides with a live
                    // conn → drop this one.
                    let idx = self
                        .peer_list
                        .peers()
                        .iter()
                        .position(|p| p.conn_key == peer_id)
                        .or_else(|| {
                            self.peers
                                .get(&peer_id)
                                .and_then(|c| c.remote)
                                .and_then(|r| self.peer_index(r))
                        });
                    if let Some(i) = idx {
                        let src = self.peer_list.peers()[i].source;
                        if !self
                            .peer_list
                            .update_peer_port(port, i, src, &self.peer_list_state)
                        {
                            out.push(TorrentEffect::DisconnectPeer {
                                peer: peer_id,
                                reason: "duplicate peer id",
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                            });
                        }
                    }
                }
                PeerEffect::PeerSeed(seed) => {
                    self.set_seed(peer_id, seed, s);
                }
                PeerEffect::PeerNotInterested => {
                    // `choke_this_peer` — needs the session counters;
                    // routed as an effect (`trigger_unchoke` runs on
                    // the next choker pass).
                    out.push(TorrentEffect::ChokePeer { peer: peer_id });
                }
                PeerEffect::SeenComplete => {
                    self.seen_complete();
                }
                PeerEffect::RemoteUploadOnly(u) => {
                    // `peer_connection::set_upload_only` — sync the
                    // conn bit, the persistent peer-list bit, then
                    // re-check redundancy (two upload-only seeds are
                    // useless to each other).
                    if let Some(c) = self.peers.get_mut(&peer_id) {
                        c.remote_upload_only = u;
                    }
                    if let Some(i) = self
                        .peers
                        .get(&peer_id)
                        .and_then(|c| c.remote)
                        .and_then(|r| self.peer_index(r))
                    {
                        self.peer_list.set_upload_only(i, u);
                        self.update_want_peers();
                    }
                    self.disconnect_if_redundant(peer_id, out);
                }
                PeerEffect::SuggestAck(_) => {
                    // `suggest_read_cache` — the peer fetched a
                    // suggestion; send two fresh ones.
                    if s.suggest_mode == crate::settings::SuggestMode::SuggestReadCache {
                        self.send_piece_suggestions(peer_id, 2, s, out);
                    }
                }
                PeerEffect::SuggestPiece(i) => {
                    // `incoming_suggest` — disconnecting conns and
                    // negative indices are ignored; with metadata,
                    // out-of-range indices and pieces we already have
                    // are dropped.
                    let drop = self.peers.get(&peer_id).is_none_or(|c| {
                        c.disconnecting
                            || i.0 < 0
                            || (self.valid_metadata()
                                && (i.0 >= c.peer_pieces.size() as i32
                                    || self.have.get_bit(i.0 as usize)))
                    });
                    if !drop && let Some(c) = self.peers.get_mut(&peer_id) {
                        c.on_suggest(i, s.max_suggest_pieces);
                    }
                }
                PeerEffect::Disconnect(r, sev) => {
                    // `m_failed` stamps on the conn so the
                    // `connection_closed` bookkeeping (failcount)
                    // sees it when the teardown lands.
                    if let Some(c) = self.peers.get_mut(&peer_id) {
                        c.failed |= sev > bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal;
                    }
                    out.push(TorrentEffect::DisconnectPeer {
                        peer: peer_id,
                        reason: r,
                        sev,
                    });
                }
            }
        }
        // upstream re-evaluates `interesting`→`interested` after any
        // incoming peer message (bitfield/have/have_all flip the
        // `m_interesting` bit).
        self.update_interest(peer_id, counters, out);
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
                if !crate::ut_holepunch::connect_allowed(&hp.ep, remote_local) {
                    return;
                }
                // upstream `add_peer` — `nullptr` (full list) or
                // `p->connection` (already connected/dialing) or
                // `p->banned` all abort the connect. Otherwise the
                // entry is marked uTP-capable and force-dialed
                // (`connect_to_peer(p, /*ignore_limit*/ true)`); the
                // `holepunch` entry flag carries `set_holepunch_mode`
                // across our async dial to the attach.
                let added =
                    self.add_swarm_peer(hp.ep, bitdaemon_common::flags::PeerSourceFlags::PEX, out);
                let Some(i) = added else {
                    return;
                };
                match self.peer_list.peers_mut().get_mut(i) {
                    Some(p) if p.connection.is_none() && !p.banned => {
                        p.supports_utp = true;
                        p.holepunch = true;
                    }
                    _ => return,
                }
                // `connect_to_peer(p, true)` + `update_want_peers` —
                // the connect pump picks the candidate up.
                self.update_want_peers();
            }
            HpMessage::Failed => {}
        }
    }

    /// feed one decoded message from `peer` — `dispatch` + effects.
    /// `s` feeds the send-buffer watermark (`m_settings` upstream);
    /// `wire_bytes` is the frame's exact wire length (len-prefix
    /// included) for `stat.received_bytes` protocol accounting.
    pub fn on_peer_message(
        &mut self,
        peer_id: u64,
        msg: &PeerMessage,
        wire_bytes: usize,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        // upstream packet accounting (`bt_peer_connection` dispatch):
        // the counter mapping lives in
        // `counters::incoming_counter_idx` (ids 0..=9 →
        // `num_incoming_choke + id`; 13..=17 → `num_incoming_suggest +
        // id` — the quirk that lands FAST messages on
        // `num_outgoing_have..cancel` offsets is kept verbatim; every
        // other type → `num_incoming_extended`). Keepalives carry no
        // id and are never counted.
        if !matches!(msg, PeerMessage::Keepalive) {
            counters.inc_idx(crate::counters::incoming_counter_idx(msg.wire_id()));
        }
        // `stat.received_bytes(payload, protocol)` — only `on_piece`
        // counts payload upstream; every other message is protocol.
        // `stat_down_total` itself is bumped at the piece dispatch.
        if let Some(c) = self.peers.get_mut(&peer_id) {
            let payload = if let PeerMessage::Piece { data, .. } = msg {
                data.len() as i64
            } else {
                0
            };
            c.stat_down_protocol += (wire_bytes as i64 - payload).max(0);
        }
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
                    self.on_hashes(peer_id, req, hashes, s, counters, out);
                } else {
                    self.peer_error_effect(peer_id, "hashes on non-v2 connection", out);
                }
                return;
            }
            PeerMessage::HashReject(req) => {
                if self.hash_protocol_ok(peer_id) {
                    self.on_hash_reject(peer_id, req, out);
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
        let block_size = self.block_size();
        // `t->is_seed()` — the torrent-level seed state; computed
        // before the per-conn borrow (the `Piece` arm needs it for
        // upstream's `is_seed` leg).
        let is_seed = self.is_seed();
        let mut seen_complete = false;
        if let Some(c) = self.peers.get_mut(&peer_id) {
            // `m_last_receive` — conn-clock of last inbound data;
            // the inactivity timeout measures from it.
            c.last_receive_ms = c.now_ms;
            if let PeerMessage::Piece { data, .. } = msg {
                let n = data.len() as i64;
                c.stat_down_total += n;
                c.stat_down_round += n;
                // `stat::received_payload` → session counter.
                counters.add(crate::counters::Counter::RecvPayloadBytes, n);
            }
            // `seen_complete` — any packet from a seed that has
            // already delivered payload refreshes the timestamp
            // (upstream requires `is_seed() && payload_downloaded >
            // 0` so a lying "seed" can't spoof it without data).
            if c.is_seed() && c.stat_down_total > 0 {
                seen_complete = true;
            }
            // `peer_plugin::on_*` — plugins observe the decoded
            // message before our dispatch consumes it (upstream
            // fires the per-message hooks inside `incoming_*`).
            for p in &c.plugins {
                p.on_message(msg);
            }
            c.dispatch(
                msg,
                self.num_pieces,
                block_size,
                self.piece_length,
                self.files.as_deref(),
                is_seed,
                s.max_piece_count,
                s.max_allowed_in_request_queue,
                counters,
                &mut effects,
            );
        }
        if seen_complete {
            self.seen_complete();
        }
        self.apply_peer_effects(peer_id, effects, s, counters, out);
        // `on_have` tail — a newly-have piece may let this peer serve
        // missing merkle layers (`maybe_send_hash_request`). `piece`
        // messages reach the same call through `MaybePick`→drive_peer.
        if matches!(msg, PeerMessage::Have(_)) {
            self.maybe_send_hash_request(peer_id, out);
        }
    }

    /// `!peer_info_struct()->protocol_v2 || !t->valid_metadata()` —
    /// the upstream guard shared by all three hash handlers.
    fn hash_protocol_ok(&self, peer_id: u64) -> bool {
        self.metadata_raw.is_some() && self.peers.get(&peer_id).is_some_and(|c| c.protocol_v2)
    }

    /// `disconnect(errors::invalid_message, bittorrent, peer_error)`
    /// — the teardown path (`peer_disconnected`) posts the
    /// `peer_error_alert` itself, carrying `msg` as the error name;
    /// posting it here would duplicate the alert.
    fn peer_error_effect(&self, peer_id: u64, msg: &'static str, out: &mut Vec<TorrentEffect>) {
        out.push(TorrentEffect::DisconnectPeer {
            peer: peer_id,
            reason: msg,
            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
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
        peer: u64,
        req: &bitdaemon_net::peer_wire::HashRequestWire,
        hashes: &[bitdaemon_common::Sha256Hash],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        // upstream `on_hashes` — a `hashes` payload that doesn't match
        // an outstanding `m_hash_requests` entry is dropped (the peer
        // can't volunteer hash subtrees).
        let Some(c) = self.peers.get_mut(&peer) else {
            return;
        };
        let Some(pos) = c.hash_requests.iter().position(|r| r == req) else {
            return;
        };
        c.hash_requests.remove(pos);
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
        let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(
            &fs,
            &mut self.merkle_trees,
            &mut self.hash_picker_state,
        );
        let result = picker.add_hashes(&hp_req, hashes);
        let v1_capable = self
            .ti
            .as_ref()
            .is_some_and(|ti| ti.info_hash_v1().is_some());
        for (piece, blocks) in result.hash_failed {
            // a v1-capable torrent that already `have`s the piece has
            // its v1 hash contradict the v2 tree — inconsistent.
            if v1_capable && self.have.get_bit(piece.0 as usize) {
                self.handle_inconsistent_hashes(piece, s, out);
                return;
            }
            // the piece may not have been downloaded this session —
            // only pieces mid-download go through `piece_failed`.
            if !self
                .picker
                .as_ref()
                .is_some_and(|p| p.is_downloading(piece))
            {
                continue;
            }
            self.piece_failed(
                piece,
                &blocks,
                s,
                counters,
                out,
                |_| std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                |_| None,
            );
        }
        for piece in result.hash_passed {
            // v2 says the piece passed but we never verified the v1
            // hash — the trees disagree.
            if v1_capable && !self.disable_v1_hashes && !self.have.get_bit(piece.0 as usize) {
                self.handle_inconsistent_hashes(piece, s, out);
                return;
            }
            // upstream: `is_downloading && is_piece_finished &&
            // !is_hashing && !have_piece` — the piece may already
            // have passed via the whole-piece hash.
            let ready = self.picker.as_ref().is_some_and(|p| {
                p.is_downloading(piece) && p.is_piece_finished(piece) && !p.is_hashing(piece)
            });
            if ready && !self.have.get_bit(piece.0 as usize) {
                self.piece_verified(piece, true, &[], s, counters, out);
            }
        }
        // `on_hashes` tail — a freed request slot picks the next
        // missing hash layer right away (upstream calls
        // `maybe_send_hash_request` here).
        self.maybe_send_hash_request(peer, out);
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
        // protocol violation and gets the connection dropped. And
        // `m_hash_requests.size() > 1` — at most two in flight.
        if !c.protocol_v2 || c.hash_requests.len() > 1 {
            return;
        }
        let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(
            &fs,
            &mut self.merkle_trees,
            &mut self.hash_picker_state,
        );
        // upstream throttles inside the picker (`last_request` /
        // `num_requests` per chunk, `min_request_interval`).
        let req = picker.pick_hashes(&pieces);
        if req.file.0 < 0 {
            return;
        }
        let wire = bitdaemon_net::peer_wire::HashRequestWire {
            file_root: fs.root(req.file),
            base: req.base,
            index: req.index,
            count: req.count,
            proof_layers: req.proof_layers,
        };
        // `m_hash_requests.push_back` — the reply (`hashes` /
        // `hash_reject`) must match an outstanding request.
        if let Some(c) = self.peers.get_mut(&peer) {
            c.hash_requests.push(wire.clone());
        }
        out.push(TorrentEffect::SendTo {
            peer,
            msg: PeerMessage::HashRequest(wire),
        });
    }

    /// `torrent::hashes_rejected` — the remote refused; upstream
    /// un-marks the request so another peer can be asked. A reject
    /// matching nothing in `m_hash_requests` is dropped.
    pub fn on_hash_reject(
        &mut self,
        peer: u64,
        req: &bitdaemon_net::peer_wire::HashRequestWire,
        out: &mut Vec<TorrentEffect>,
    ) {
        let Some(c) = self.peers.get_mut(&peer) else {
            return;
        };
        let Some(pos) = c.hash_requests.iter().position(|r| r == req) else {
            return;
        };
        c.hash_requests.remove(pos);
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
        let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(
            &fs,
            &mut self.merkle_trees,
            &mut self.hash_picker_state,
        );
        picker.hashes_rejected(&hp_req);
        // `maybe_send_hash_request` — the refusal freed a request
        // slot; immediately pick the next layer to fetch.
        self.maybe_send_hash_request(peer, out);
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
        if !self.super_seeding || self.is_paused() || self.now_ms <= 0 {
            return;
        }
        let stale: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| {
                !c.disconnecting
                    && !c.peer_interested
                    && c.became_uninterested_ms + 10_000 < self.now_ms
            })
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
    pub fn update_interest(
        &mut self,
        peer_id: u64,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        // upstream `do_update_interest`: `m_have_piece.empty()`
        // (nothing to scan — an empty bitfield-sized peer was never
        // initialized; magnets store a sized-zero bitfield whose size
        // is reconciled when metadata resolves) or
        // `!ready_for_connections` → `m_connections_initialized`
        // (metadata-present adds set it in the ctor; magnets get it
        // at `files_checked`, which then replays conn init).
        if self
            .peers
            .get(&peer_id)
            .is_none_or(|c| c.peer_pieces.size() == 0)
            || !self.connections_initialized
        {
            return;
        }
        // `is_upload_only()` — upstream skips the scan entirely and
        // reports not-interesting (a finished torrent wants nothing).
        // `piece_priority(j) > dont_download` filters pieces we
        // explicitly don't want.
        let interesting = !self.is_upload_only()
            && self.peers.get(&peer_id).is_some_and(|c| {
                (0..self.num_pieces).any(|i| {
                    c.has_piece(PieceIndex(i))
                        // upstream runs the scan under `need_picker()`
                        // — no picker means nothing was filtered out.
                        && self.piece_priority(PieceIndex(i))
                            != Some(bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD)
                        && !self.have.get_bit(i as usize)
                })
            });
        let seed = self.is_seed();
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        // recompute `m_interesting` — upstream re-evaluates the
        // full predicate each time (sticky-set is a bug: we'd never
        // send NotInterested after gaining the last wanted piece).
        if c.interesting != interesting {
            // `num_peers_down_interested` — transition gauge
            // (`send_interested`/`send_not_interested` upstream).
            counters.add(
                crate::counters::Counter::NumPeersDownInterested,
                if interesting { 1 } else { -1 },
            );
        }
        c.interesting = interesting;
        // seeds are never interested (upstream: interesting only
        // evaluated for downloaders).
        let want = !seed && c.interesting && !c.remote_upload_only;
        let send_msg = if want == c.we_sent_interested {
            None
        } else {
            c.we_sent_interested = want;
            if !want {
                // `m_became_uninteresting` stamps on the NotInterested
                // transition (upstream `send_not_interested`).
                c.became_uninteresting_ms = c.now_ms;
            }
            Some(if want {
                PeerMessage::Interested
            } else {
                PeerMessage::NotInterested
            })
        };
        if let Some(msg) = send_msg {
            out.push(TorrentEffect::SendTo { peer: peer_id, msg });
        }
        // upstream `do_update_interest` tail — drop conns that can
        // offer us nothing once interest has been re-evaluated.
        self.disconnect_if_redundant(peer_id, out);
    }

    /// `peer_connection::disconnect_if_redundant` — under
    /// `close_redundant_connections`, a remote that is upload-only
    /// (BEP-21 flag, seed, or have-all) is redundant when we can't
    /// take anything from it either: both sides upload-only
    /// (`upload_upload_connection`), or it is uninteresting to us
    /// once our files are verified (`uninteresting_upload_peer`).
    /// Skipped while metadata is unresolved on either side — the
    /// peer may still want our metadata — and in share mode.
    pub fn disconnect_if_redundant(&mut self, peer_id: u64, out: &mut Vec<TorrentEffect>) {
        if !self.close_redundant_connections {
            return;
        }
        if !self.valid_metadata() || self.share_mode {
            return;
        }
        let Some(c) = self.peers.get(&peer_id) else {
            return;
        };
        // `!has_metadata()` — the peer advertised ut_metadata
        // without a `metadata_size` and hasn't sent any piece-level
        // message: it may still need ours.
        if !c.has_metadata {
            return;
        }
        let peer_upload_only = c.remote_upload_only || c.is_seed();
        if !peer_upload_only {
            return;
        }
        let reason = if self.is_upload_only() {
            Some("upload-upload connection")
        } else if !c.interesting && c.bitfield_received && self.files_checked {
            Some("uninteresting upload-only peer")
        } else {
            None
        };
        if let Some(r) = reason {
            // `disconnect_if_redundant` runs at default severity
            // (`upload_upload_connection`/`peer_no_support` — normal).
            out.push(TorrentEffect::DisconnectPeer {
                peer: peer_id,
                reason: r,
                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
            });
        }
    }

    /// `peer_connection::update_interest` broadcast — after our own
    /// bitfield changed (`piece_verified`), every peer's interest
    /// may flip.
    pub fn update_interest_all(
        &mut self,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.update_interest(id, counters, out);
        }
    }

    /// `request_a_block` + `send_block_requests` — the production
    /// request drive. Picked blocks queue on the conn (busy-capped,
    /// time-critical front-inserted) then drain to wire `Request`
    /// messages through the shared pipeline — identical ordering to
    /// upstream's `second_tick`/`do_request_blocks` path.
    /// Returns whether `request_a_block` actually picked anything
    /// (upstream callers bump the context `*_piece_picks` counter on
    /// it).
    pub fn drive_peer(
        &mut self,
        peer_id: u64,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) -> bool {
        if self.picker.is_none() {
            return false;
        }
        let block_size = self.block_size();
        let (send_buffer_size, now_ms, rate) = match self.peers.get(&peer_id) {
            // a peer that announced no pieces has nothing pickable —
            // upstream's `m_have_piece` mask filters the same way.
            // A *choked* peer is still driven: `request_a_block`
            // itself restricts the mask to `allowed_fast` (BEP-6).
            Some(c) if c.peer_pieces.size() > 0 => {
                // `request_a_block` takes the payload-channel rate
                // upstream (`download_payload_rate()`).
                (c.send_buffer, c.now_ms, i64::from(c.rate_down_payload))
            }
            _ => return false,
        };
        // `is_sequential_download()` — user `sequential` OR the
        // `auto_sequential` well-seeded heuristic.
        let mut opts = if self.is_sequential_download() {
            PickerOptions::RAREST_FIRST | PickerOptions::SEQUENTIAL
        } else {
            PickerOptions::RAREST_FIRST
        };
        if !self.time_critical.is_empty() {
            opts = opts | PickerOptions::TIME_CRITICAL;
        }
        // v2 hash exchange — request missing tree layers alongside
        // data requests (upstream `maybe_send_hash_request`).
        if !self.merkle_trees.is_empty() {
            self.maybe_send_hash_request(peer_id, out);
        }
        let tctx = crate::request_blocks::TorrentRequestCtx {
            is_seed: self.is_seed(),
            upload_mode: self.upload_mode,
            valid_metadata: self.files.is_some(),
            files_checked: self.files_checked,
            graceful_pause: self.graceful_pause,
            time_critical_mode: !self.time_critical.is_empty(),
            num_peers: self.peers.len() as i32,
            whole_pieces_threshold: s.whole_pieces_threshold,
            strict_end_game_mode: s.strict_end_game_mode,
            piece_length: self.piece_length,
            block_size,
        };
        let view = PipelineView {
            piece_length: self.piece_length,
            block_size,
            files: self.files.as_deref(),
            graceful_pause: self.graceful_pause,
            upload_mode: self.upload_mode,
            // `checking_files || checking_resume_data ||
            // downloading_metadata` — metadata-less torrents never
            // reach this point (no picker), so `checking` is the
            // live case.
            data_requests_blocked: self.checking,
            has_picker: self.picker.is_some(),
        };
        let mut pc_stats = crate::piece_picker::NullPickerStats;
        let (invoked, _endgame, reqs, queued) = {
            let (Some(p), Some(c)) = (self.picker.as_mut(), self.peers.get_mut(&peer_id)) else {
                return false;
            };
            c.drive_requests(
                p,
                &tctx,
                &view,
                opts,
                rate,
                send_buffer_size,
                now_ms,
                counters,
                &mut pc_stats,
            )
        };
        // `request_blocks` upstream posts `picker_log_alert` — the
        // picker's interesting-block set for this decision.
        if invoked && !queued.is_empty() {
            let mut bits = vec![0u8; (self.num_pieces.max(0) as usize).div_ceil(8)];
            for b in &queued {
                let i = b.piece_index.0 as usize;
                if i / 8 < bits.len() {
                    bits[i / 8] |= 1 << (7 - (i % 8));
                }
            }
            out.push(TorrentEffect::Alert(crate::alerts::Alert::PickerLog {
                ih: self.info_hash,
                picker_flags: 0,
                blocks: bits,
            }));
        }
        let remote = self
            .peers
            .get(&peer_id)
            .and_then(|c| c.remote)
            .map_or_else(String::new, |ep| ep.to_string());
        // `peer_connection::add_request` posts
        // `block_downloading_alert` once the block is marked
        // downloading in the picker.
        for b in &queued {
            out.push(TorrentEffect::Alert(
                crate::alerts::Alert::BlockDownloading {
                    ih: self.info_hash,
                    remote: remote.clone(),
                    piece: b.piece_index,
                    block: b.block_index as u32,
                },
            ));
        }
        for req in reqs {
            // `to_req` already clamped the length to the real piece
            // size — the tail block of a truncated piece is never a
            // full block (strict peers drop out-of-bounds requests).
            out.push(TorrentEffect::SendTo {
                peer: peer_id,
                msg: PeerMessage::Request(req),
            });
        }
        invoked
    }

    /// `peer_connection::send_block_requests` standalone — upstream
    /// ends `incoming_reject_request` (and the non-FAST choke
    /// rejection loop) with an unconditional flush: whatever still
    /// sits in `request_queue` goes on the wire without a pick.
    /// Same gates as the `drive_peer` send half — `send_block_requests`
    /// itself early-outs on `disconnecting`/`graceful_pause`/
    /// `data_requests_blocked`.
    pub fn flush_requests(&mut self, peer_id: u64, out: &mut Vec<TorrentEffect>) {
        let block_size = self.block_size();
        let view = PipelineView {
            piece_length: self.piece_length,
            block_size,
            files: self.files.as_deref(),
            graceful_pause: self.graceful_pause,
            upload_mode: self.upload_mode,
            data_requests_blocked: self.checking,
            has_picker: self.picker.is_some(),
        };
        let mut picker = self.picker.as_mut();
        let Some(c) = self.peers.get_mut(&peer_id) else {
            return;
        };
        let disconnecting = c.disconnecting;
        let send_buffer = c.send_buffer;
        let now = c.now_ms;
        let key = c.peer_key();
        let reqs = c
            .pipeline
            .send_block_requests(&view, send_buffer, now, disconnecting, |b| {
                // a block finished meanwhile frees its picker mark
                // and is not re-requested (`is_downloaded` consult).
                match picker.as_deref_mut() {
                    Some(p) if p.is_downloaded(b) => {
                        p.abort_download(b, key);
                        true
                    }
                    _ => false,
                }
            });
        for req in reqs {
            out.push(TorrentEffect::SendTo {
                peer: peer_id,
                msg: PeerMessage::Request(req),
            });
        }
    }

    /// `web_seed_peer` pseudo-identity in the picker — a web seed
    /// counts as a virtual peer upstream (`web_peer_connection`
    /// derives `peer_connection`); blocks it serves are tracked
    /// under this key so real peers never double-request them.
    pub const WEB_SEED_PEER: u64 = u64::MAX;

    /// `maybe_connect_web_seeds` + `web_peer_connection::write_request`
    /// — pick up to `max_blocks` blocks one eligible seed can serve
    /// and mark them `requested` under *that seed's* picker key.
    ///
    /// Upstream gates: `m_abort`, empty seed list, `is_finished()`,
    /// `!m_files_checked`, `num_peers() >= m_max_connections`,
    /// `num_connections() >= connections_limit`; then per seed
    /// `disabled || removed || retry > now || !interesting` skips,
    /// each eligible seed consumes one `max_web_seed_connections`
    /// slot, and `peer_info.connection || resolving` skips. Our
    /// pull model maps the conn slot to `busy` (a fetch task in
    /// flight) and issues at most one batch per call.
    pub fn next_web_seed_blocks(
        &mut self,
        max_blocks: i32,
        s: &crate::settings::SessionSettings,
        session_conns: i64,
    ) -> Option<(String, Vec<PeerRequest>)> {
        if self.aborted
            || self.web_seeds.is_empty()
            || self.finished
            || self.is_seed()
            || self.upload_mode
            || self.is_paused()
            || !self.files_checked
            || self.picker.is_none()
            || self.files.is_none()
        {
            return None;
        }
        // upstream's `num_peers()` counts live web-seed conns too —
        // our in-flight fetch tasks are their equivalent.
        let inflight = self.web_seed_inflight() as i64;
        if self.max_connections >= 0
            && self.peers.len() as i64 + inflight >= i64::from(self.max_connections)
        {
            return None;
        }
        if s.connections_limit > 0 && session_conns >= i64::from(s.connections_limit) {
            return None;
        }
        // `zero_or(max_web_seed_connections, 100)` — unlimited
        // means 100 upstream. Upstream's `--limit` lands on every
        // *eligible* seed — including already-connected ones — so
        // in-flight fetches consume slots even where our
        // round-robin scan would visit them later.
        let limit: i64 = if s.max_web_seed_connections == 0 {
            100
        } else {
            i64::from(s.max_web_seed_connections)
        };
        let now_ms = self.now_ms;
        // ensure every listed seed has its `web_seed_t` entry (and
        // picker key) before accounting.
        for u in self.web_seeds.clone() {
            if let std::collections::btree_map::Entry::Vacant(e) =
                self.web_seed_state.entry(u.clone())
            {
                let key = self.next_web_seed_key;
                self.next_web_seed_key = self.next_web_seed_key.wrapping_sub(1);
                e.insert(WebSeed {
                    key,
                    retry_ms: 0,
                    disabled: false,
                    removed: false,
                    interesting: true,
                    ephemeral: false,
                    no_local_ips: false,
                    busy: false,
                    refcounted: false,
                    supports_keepalive: true,
                    deadline_ms: 0,
                    outstanding: std::collections::BTreeSet::new(),
                    status: crate::status::WebSeedStatus {
                        url: u,
                        ..Default::default()
                    },
                });
            }
        }
        // eligible-and-busy seeds hold conn slots already —
        // upstream's `--limit` lands on every eligible seed
        // (`disabled || removed || retry > now || !interesting`
        // skips don't consume), `peer_info.connection || resolving`
        // included.
        if self
            .web_seeds
            .iter()
            .filter(|u| {
                self.web_seed_state.get(u.as_str()).is_some_and(|st| {
                    st.busy
                        && !st.disabled
                        && !st.removed
                        && st.retry_ms <= now_ms
                        && st.interesting
                })
            })
            .count() as i64
            >= limit
        {
            return None;
        }
        let n = self.web_seeds.len();
        let mut url: Option<String> = None;
        for off in 0..n {
            let idx = (self.web_seed_idx + off) % n;
            let u = self.web_seeds[idx].clone();
            let Some(st) = self.web_seed_state.get(&u) else {
                continue;
            };
            if st.disabled || st.removed || st.retry_ms > now_ms || !st.interesting || st.busy {
                continue;
            }
            url = Some(u);
            self.web_seed_idx = idx + 1;
            break;
        }
        let url = url?;
        let key = self.web_seed_state.get(&url).map(|st| st.key)?;
        // `peer_connection::init` — the virtual conn's all-pieces
        // bitfield joins the picker before the request is written
        // (the seed serves the full payload); `dec_refcount` on
        // teardown mirrors `~peer_connection`.
        if !self
            .web_seed_state
            .get(&url)
            .is_some_and(|st| st.refcounted)
            && let Some(st) = self.web_seed_state.get_mut(&url)
        {
            st.refcounted = true;
            if let Some(p) = self.picker.as_mut() {
                p.inc_refcount_all();
            }
        }
        let keepalive = self
            .web_seed_state
            .get(&url)
            .is_none_or(|st| st.supports_keepalive);
        let all = Bitfield::with_value(self.num_pieces as usize, true);
        let tctx = crate::request_blocks::TorrentRequestCtx {
            is_seed: false,
            upload_mode: false,
            valid_metadata: true,
            files_checked: self.files_checked,
            graceful_pause: self.graceful_pause,
            time_critical_mode: false,
            num_peers: (self.peers.len() + 1) as i32,
            whole_pieces_threshold: 0,
            strict_end_game_mode: false,
            piece_length: self.piece_length,
            block_size: self.block_size(),
        };
        let allowed_fast = Vec::new();
        let suggested = Vec::new();
        let st = self.web_seed_state.get(&url)?;
        let outstanding: Vec<_> = st.outstanding.iter().copied().collect();
        // `web_peer_connection` ctor — `prefer_contiguous_blocks`:
        // keep-alive seeds get `max(1 MiB, piece_length)`, HTTP/1.0
        // seeds `max(4 MiB, piece_length)`, both raised to
        // `urlseed_max_request_bytes`.
        let min_size =
            (i64::from(if keepalive { 1 } else { 4 }) * 1024 * 1024).max(self.piece_length);
        let preferred_size = min_size.max(s.urlseed_max_request_bytes);
        let block_size_i64 = i64::from(self.block_size());
        let pctx = crate::request_blocks::PeerRequestCtx {
            no_download: false,
            disconnecting: false,
            desired_queue_size: max_blocks,
            outstanding: &outstanding,
            prefer_contiguous_blocks: (preferred_size / block_size_i64.max(1)) as i32,
            download_payload_rate: 0,
            peer_choked: false,
            allowed_fast: &allowed_fast,
            peer_pieces: &all,
            peer_key: Some(key),
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
        // `request_large_blocks` — upstream's `web_peer_connection`
        // coalesces the contiguous run into one byte-range request
        // per file span; a run may cross a piece boundary and is
        // capped at `urlseed_max_request_bytes` (`request_merger`
        // tracks torrent-space offsets and `map_block` resolves
        // them).
        let block_size = self.block_size();
        let max_req = s.urlseed_max_request_bytes.max(i64::from(block_size));
        let mut inserted = 0i32;
        let mut reqs: Vec<PeerRequest> = Vec::with_capacity(picked.len());
        for (b, _busy) in picked {
            if !self
                .picker
                .as_mut()
                .is_some_and(|p| p.mark_as_downloading(b, Some(key), PickerOptions::RAREST_FIRST))
            {
                continue;
            }
            if self.web_seed_outstanding.insert(b) {
                inserted += 1;
            }
            if let Some(st) = self.web_seed_state.get_mut(&url) {
                st.outstanding.insert(b);
            }
            if !(0..self.num_pieces).contains(&b.piece_index.0) {
                continue;
            }
            let start = b.block_index * block_size;
            let piece_size = self.files.as_ref().map_or(self.piece_length, |f| {
                i64::from(f.piece_size(b.piece_index))
            });
            let length = (piece_size - i64::from(start)).min(i64::from(block_size)) as i32;
            let off = i64::from(b.piece_index.0) * self.piece_length + i64::from(start);
            if let Some(last) = reqs.last_mut() {
                let last_end = i64::from(last.piece.0) * self.piece_length
                    + i64::from(last.start)
                    + i64::from(last.length);
                if last_end == off && i64::from(last.length) + i64::from(length) <= max_req {
                    last.length += length;
                    continue;
                }
            }
            reqs.push(PeerRequest {
                piece: b.piece_index,
                start,
                length,
            });
        }
        if reqs.is_empty() {
            // defensive: picked blocks that produced no request
            // must not stay `downloading` under the seed's key.
            if let Some(st) = self.web_seed_state.get_mut(&url) {
                let blocks: Vec<_> = st.outstanding.iter().copied().collect();
                st.outstanding.clear();
                for b in &blocks {
                    self.web_seed_outstanding.remove(b);
                    if let Some(p) = self.picker.as_mut() {
                        p.abort_download(*b, Some(key));
                    }
                }
            }
            return None;
        }
        // the batch rides this seed — mark it busy (the
        // `peer_info.connection` slot) and arm its
        // `urlseed_timeout` deadline.
        if let Some(st) = self.web_seed_state.get_mut(&url) {
            st.busy = true;
            st.deadline_ms = now_ms + s.urlseed_timeout.as_millis() as i64;
            st.status.outstanding += inserted;
        }
        Some((url, reqs))
    }

    /// Expand a (possibly merged — `request_large_blocks`) web-seed
    /// request into the `PieceBlock`s it covers. A range may cross
    /// a piece boundary.
    fn web_seed_blocks(&self, req: &PeerRequest) -> Vec<PieceBlock> {
        let piece_length = self.piece_length;
        let block_size = i64::from(self.block_size());
        let mut out = Vec::new();
        let mut off = i64::from(req.piece.0) * piece_length + i64::from(req.start);
        let mut rem = i64::from(req.length);
        while rem > 0 && piece_length > 0 && block_size > 0 {
            let piece_index = (off / piece_length) as i32;
            let block_index = ((off % piece_length) / block_size) as i32;
            let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                i64::from(f.piece_size(PieceIndex(piece_index)))
            });
            let bs = (psize - i64::from(block_index) * block_size)
                .min(block_size)
                .min(rem);
            if bs <= 0 {
                break;
            }
            out.push(bitdaemon_common::types::PieceBlock {
                piece_index: PieceIndex(piece_index),
                block_index,
            });
            off += bs;
            rem -= bs;
        }
        out
    }

    /// one web-seed request's data arrived — slice it back into the
    /// covered blocks (upstream `web_peer_connection` consumes its
    /// `m_requests` per pending_block), then `mark_as_writing` +
    /// emit a disk write each — the same path a wire `Piece` takes.
    /// `url` attributes the delivered payload to the seed's per-URL
    /// counters and picker key (upstream's `web_seed_t` conn lives
    /// per seed).
    ///
    /// A block that is no longer `outstanding` for this seed —
    /// released by a timeout or a `remove_url_seed` while the fetch
    /// was in flight — counts as redundant instead of writing
    /// (upstream's conn teardown discards the same bytes).
    #[allow(clippy::too_many_arguments)]
    pub fn on_web_seed_block(
        &mut self,
        url: &str,
        req: PeerRequest,
        data: Vec<u8>,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        // one `Vec → Bytes` conversion, then refcounted slices per
        // covered block (`request_large_blocks` replies span many).
        let data = bytes::Bytes::from(data);
        let block_size = i64::from(self.block_size());
        let key = self.web_seed_state.get(url).map(|st| st.key);
        let blocks = self.web_seed_blocks(&req);
        let mut data_off = 0usize;
        for b in blocks {
            let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                i64::from(f.piece_size(b.piece_index))
            });
            let start = i64::from(b.block_index) * block_size;
            let bs =
                ((psize - start).min(block_size) as usize).min(data.len().saturating_sub(data_off));
            if bs == 0 {
                break;
            }
            let still_mine = self
                .web_seed_state
                .get_mut(url)
                .is_some_and(|st| st.outstanding.remove(&b));
            self.web_seed_outstanding.remove(&b);
            if !still_mine {
                // released underneath the in-flight task —
                // `waste_reason::piece_closing` bookkeeping.
                self.redundant_bytes += bs as i64;
                data_off += bs;
                if data_off >= data.len() {
                    break;
                }
                continue;
            }
            if let Some(st) = self.web_seed_state.get_mut(url) {
                st.status.outstanding -= 1;
                st.status.downloaded += bs as u64;
                st.status.last_error = None;
                // `web_connection_base` re-arms its deadline per
                // received payload — a serving batch isn't "stuck".
                st.deadline_ms = self.now_ms + s.urlseed_timeout.as_millis() as i64;
            }
            // `report_web_seed_downloads` — upstream skips
            // `m_stat` accounting when off (`ignore_stats(true)`).
            if s.report_web_seed_downloads {
                self.web_seed_payload_downloaded += bs as i64;
                counters.add(crate::counters::Counter::RecvPayloadBytes, bs as i64);
            }
            if let Some(p) = self.picker.as_mut() {
                p.mark_as_writing(b, key);
            }
            out.push(TorrentEffect::DiskWrite {
                peer: key.unwrap_or(Self::WEB_SEED_PEER),
                req: PeerRequest {
                    piece: b.piece_index,
                    start: (i64::from(b.block_index) * block_size) as i32,
                    length: bs as i32,
                },
                data: data.slice(data_off..data_off + bs),
            });
            data_off += bs;
            if data_off >= data.len() {
                break;
            }
        }
    }

    /// `web_seed_t` keep-alive bookkeeping — a `Connection: close`
    /// response (or dead conn) clears `supports_keepalive`,
    /// widening the next preferred request size upstream-style.
    /// Also frees the seed's conn slot (`busy`) once a fetch
    /// resolved — the batch is over.
    pub fn on_web_seed_done(&mut self, url: &str, saw_close: bool) {
        if let Some(st) = self.web_seed_state.get_mut(url) {
            st.busy = false;
            st.deadline_ms = 0;
            if saw_close {
                // upstream's conn dropped on `m_received_disconnect`
                // — its picker refcount goes with it.
                st.supports_keepalive = false;
                self.dec_web_seed_refcount(url);
            }
            // GC a `removed` seed whose last fetch just resolved —
            // upstream's `on_name_lookup`/`on_disconnect` does the
            // deferred `remove_web_seed_iter` here.
            if self
                .web_seed_state
                .get(url)
                .is_some_and(|st| st.removed && st.outstanding.is_empty())
            {
                self.web_seed_state.remove(url);
            }
        }
    }

    /// `~web_peer_connection` — the virtual conn's all-pieces
    /// refcount leaves the picker. Idempotent.
    fn dec_web_seed_refcount(&mut self, url: &str) {
        let Some(st) = self.web_seed_state.get_mut(url) else {
            return;
        };
        if !st.refcounted {
            return;
        }
        st.refcounted = false;
        if let Some(p) = self.picker.as_mut() {
            p.dec_refcount_all();
        }
    }

    /// a web-seed fetch failed — release the blocks so they can be
    /// re-requested from a real peer or another seed (`web_seed`
    /// `fail_count`→`abort_download` upstream path), then apply
    /// `backoff` (`retry_web_seed` / `disable()` /
    /// `remove_web_seed_iter`). `url`/`msg` record the per-URL
    /// error — upstream's `url_seed_alert` carries the failing
    /// seed + its message.
    pub fn on_web_seed_error(
        &mut self,
        url: &str,
        reqs: &[PeerRequest],
        msg: &str,
        backoff: WebSeedBackoff,
        s: &crate::settings::SessionSettings,
    ) {
        let (key, mut release) = match self.web_seed_state.get_mut(url) {
            Some(st) => {
                let v: Vec<_> = std::mem::take(&mut st.outstanding).into_iter().collect();
                (Some(st.key), v)
            }
            None => (None, Vec::new()),
        };
        // the fetch died — upstream's conn teardown
        // (`~peer_connection`) drops its picker refcount here.
        self.dec_web_seed_refcount(url);
        // safety net — blocks named by `reqs` still outstanding but
        // missing from the seed's set get released too.
        for r in reqs {
            for b in self.web_seed_blocks(r) {
                if self.web_seed_outstanding.contains(&b) && !release.contains(&b) {
                    release.push(b);
                }
            }
        }
        for b in &release {
            self.web_seed_outstanding.remove(b);
            if let Some(p) = self.picker.as_mut() {
                p.abort_download(*b, key);
            }
        }
        if let Some(st) = self.web_seed_state.get_mut(url) {
            st.status.outstanding = 0;
            st.status.last_error = Some(msg.to_string());
        }
        match backoff {
            WebSeedBackoff::Disable => {
                if let Some(st) = self.web_seed_state.get_mut(url) {
                    st.disabled = true;
                }
            }
            WebSeedBackoff::Remove => {
                self.web_seeds.retain(|u| u != url);
                self.web_seed_state.remove(url);
                self.need_save_resume = true;
            }
            WebSeedBackoff::Retry | WebSeedBackoff::NameLookup | WebSeedBackoff::Header(_) => {
                self.retry_web_seed(url, backoff, s);
            }
        }
    }

    /// `maybe_connect_web_seeds`'s conn-count leg — in-flight
    /// fetch tasks count toward `max_web_seed_connections` /
    /// `max_connections` upstream-style (`peer_info.connection`).
    #[must_use]
    pub fn web_seed_inflight(&self) -> usize {
        self.web_seed_state.values().filter(|st| st.busy).count()
    }

    /// `web_seed_t` deadline sweep — upstream bounds a web-seed
    /// request by `web_connection_base::timeout()`
    /// (`urlseed_timeout`); an expired in-flight batch releases
    /// its blocks and reschedules the seed with
    /// `urlseed_wait_retry` backoff.
    pub fn check_web_seed_timeouts(
        &mut self,
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) {
        let now = self.now_ms;
        if now <= 0 {
            return;
        }
        let wait = s.urlseed_wait_retry.as_millis() as i64;
        let mut expired: Vec<(String, u64, bool, Vec<_>)> = Vec::new();
        for (url, st) in &mut self.web_seed_state {
            if !st.busy || st.deadline_ms <= 0 || st.deadline_ms > now {
                continue;
            }
            st.busy = false;
            st.deadline_ms = 0;
            st.status.outstanding = 0;
            st.status.last_error = Some("timed out".to_string());
            st.retry_ms = st.retry_ms.max(now + wait);
            let was_refcounted = std::mem::replace(&mut st.refcounted, false);
            expired.push((
                url.clone(),
                st.key,
                was_refcounted,
                std::mem::take(&mut st.outstanding).into_iter().collect(),
            ));
        }
        for (url, key, was_refcounted, blocks) in expired {
            if was_refcounted && let Some(p) = self.picker.as_mut() {
                // `~web_peer_connection` — the timed-out conn's
                // bitfield leaves the picker.
                p.dec_refcount_all();
            }
            for b in &blocks {
                self.web_seed_outstanding.remove(b);
                if let Some(p) = self.picker.as_mut() {
                    p.abort_download(*b, Some(key));
                }
            }
            out.push(TorrentEffect::Alert(crate::alerts::Alert::UrlSeed {
                ih: self.info_hash,
                url,
                msg: "web seed timed out".to_string(),
            }));
        }
    }

    /// `web_seed_statuses` — the live per-URL snapshot over
    /// `web_seed_state` for every `m_web_seeds` entry (upstream
    /// surfaces the same observability through `web_seed_t` conns).
    #[must_use]
    pub fn web_seed_statuses(&self) -> Vec<crate::status::WebSeedStatus> {
        self.web_seeds
            .iter()
            .map(|u| {
                self.web_seed_state.get(u).map_or_else(
                    || crate::status::WebSeedStatus {
                        url: u.clone(),
                        ..Default::default()
                    },
                    |st| st.status.clone(),
                )
            })
            .collect()
    }

    /// `is_seed`-side helper — all blocks of `piece` have been
    /// written (`picker` tracks open pieces; a piece leaves the
    /// open queue when every block is finished).
    /// `on_disk_write_complete` — the block landed on disk;
    /// upstream `picker().mark_as_finished(block, nullptr)`.
    pub fn mark_block_finished(&mut self, piece: PieceIndex, start: i32, peer: Option<u64>) {
        // `p.start / t->block_size()` upstream — `block_size()` is
        // `min(piece_length, 0x4000)`, not a fixed 16 KiB.
        let block_index = start / self.block_size().max(1);
        if let Some(p) = self.picker.as_mut() {
            p.mark_as_finished(
                bitdaemon_common::types::PieceBlock {
                    piece_index: piece,
                    block_index,
                },
                peer,
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

    /// `piece_finished`/`on_piece_verified` — verification result.
    /// `passed` → have-bit + broadcast; failed → restore want +
    /// smart_ban hook point. Thin wrapper without per-conn
    /// smart-ban bookkeeping (internal callers like
    /// `verify_v2_blocks`/`on_hashes` don't carry address/digest
    /// resolvers). `blocks` carries the v2 known-bad block indices
    /// when a `hashes` message pinpointed them (empty = the whole
    /// piece failed).
    pub fn piece_verified(
        &mut self,
        piece: PieceIndex,
        passed: bool,
        blocks: &[i32],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        self.piece_verified_impl(
            piece,
            passed,
            blocks,
            s,
            counters,
            out,
            |_| std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            |_| None,
        );
    }

    /// `piece_verified` + the `smart_ban` bookkeeping upstream does
    /// inside `piece_passed`/`piece_failed`: on failure the
    /// per-block CRC of every downloader is recorded, on success the
    /// stored CRCs are re-checked. `addr` resolves a peer id, and
    /// `read_digest` re-reads a block's SHA-1 from storage (the
    /// session performs the disk read; `None` = skip, like upstream
    /// ignoring read errors). Bans become `DisconnectPeer` effects.
    /// `blocks` = v2 known-bad block indices (empty = whole piece).
    #[allow(clippy::too_many_arguments)]
    pub fn piece_verified_ban(
        &mut self,
        piece: PieceIndex,
        passed: bool,
        blocks: &[i32],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
        addr: impl FnMut(u64) -> std::net::IpAddr,
        read_digest: impl FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) {
        self.piece_verified_impl(piece, passed, blocks, s, counters, out, addr, read_digest);
    }

    /// `on_piece_verified` → `piece_passed`/`piece_failed` — the
    /// full post-hash state machine.
    #[allow(clippy::too_many_arguments)]
    fn piece_verified_impl(
        &mut self,
        piece: PieceIndex,
        passed: bool,
        blocks: &[i32],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
        mut addr: impl FnMut(u64) -> std::net::IpAddr,
        mut read_digest: impl FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) {
        // `m_abort`/`m_deleted` upstream — a torn-down torrent drops
        // the completion before `completed_hash_job` even runs.
        if self.aborted {
            return;
        }
        // the hash job landed either way — clear `dp.hashing`
        // (`picker.completed_hash_job` upstream).
        self.hashing_done(piece);
        // `on_piece_hashed` — the *checking* pass shares this funnel:
        // a pass is `piece_flushed` + `we_have` directly (no
        // `piece_passed` rewards/`num_piece_passed`, no suggest, no
        // smart_ban ledger); a miss just leaves the piece not-have —
        // upstream does not route check misses through
        // `piece_failed` (no restore, no penalize, no alert).
        if self.checking {
            if passed {
                if self.picker.is_some() || !self.is_seed() {
                    if let Some(p) = self.picker.as_mut() {
                        p.piece_flushed(piece);
                    }
                    self.need_save_resume = true;
                }
                self.we_have(piece, false, s, counters, out, &mut read_digest);
            }
            return;
        }
        // upstream `on_piece_verified` gates the download path:
        // `!has_picker() && m_have_all` → nothing to do;
        // `!is_piece_finished(piece)` → a `write_failed` raced the
        // hash job, the piece was already restored — ignore it.
        match self.picker.as_ref() {
            None if self.seeding => return,
            _ => {}
        }
        // `state_updated()` — upstream marks the torrent dirty
        // before the `is_piece_finished` gate.
        self.state_updated();
        if self
            .picker
            .as_ref()
            .is_some_and(|p| !p.is_piece_finished(piece))
        {
            return;
        }
        if passed {
            // `piece_passed` — `set_need_save_resume` (download
            // progress) + `num_piece_passed` before anything else.
            self.need_save_resume = true;
            counters.add(crate::counters::Counter::NumPiecePassed, 1);
            // `suggest_read_cache` — a just-verified piece is likely
            // the rarest we hold; upstream inserts it into the
            // read-cache suggest set inside `piece_passed` (before
            // the downloader rewards).
            self.add_suggest_piece(piece, s);
            // upstream `piece_finished` — trust/parole update on
            // every peer that contributed blocks, collected from
            // the picker *before* `piece_passed` clears them.
            let downloaders = self
                .picker
                .as_ref()
                .map(|p| p.get_downloaders(piece))
                .unwrap_or_default();
            let mut seen = std::collections::BTreeSet::new();
            for key in downloaders.into_iter().flatten() {
                if key == Self::WEB_SEED_PEER || !seen.insert(key) {
                    continue;
                }
                self.reward_downloader(key);
            }
            // `picker.piece_passed` — passed_hash_check +
            // `account_have` + flush; without it the piece stays in
            // the download queue and every in-flight write
            // re-triggers a hash job.
            if let Some(p) = self.picker.as_mut() {
                p.piece_passed(piece);
            }
            // seed_mode — `verified(piece)` marks the lazy-recheck
            // bookkeeping (`m_verified`/`m_num_verified`), right
            // after `piece_passed` upstream.
            if self.seed_mode
                && (piece.0 as usize) < self.seed_verified.size()
                && !self.seed_verified.get_bit(piece.0 as usize)
            {
                self.seed_verified_piece(piece);
            }
            self.we_have(piece, false, s, counters, out, &mut read_digest);
            return;
        }
        // `piece_failed` — the restore waits for the
        // `clear_piece` disk fence (`on_piece_sync`).
        self.piece_failed(piece, blocks, s, counters, out, &mut addr, &mut read_digest);
    }

    /// `torrent::piece_failed` — penalize the piece's senders
    /// (`blocks` pinpoints the bad ones when v2 identified them),
    /// then `lock_piece` + `async_clear_piece` so `on_piece_sync`
    /// restores the piece only after in-flight writes drained.
    #[allow(clippy::too_many_arguments)]
    fn piece_failed(
        &mut self,
        piece: PieceIndex,
        blocks: &[i32],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
        mut addr: impl FnMut(u64) -> std::net::IpAddr,
        mut read_digest: impl FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) {
        // `num_piece_failed` first, then the predictive retraction.
        counters.add(crate::counters::Counter::NumPieceFailed, 1);
        if self.is_predictive_piece(piece) {
            self.predictive.clear_bit(piece.0 as usize);
            for (id, c) in self.peers.iter_mut() {
                let mut i = 0;
                while i < c.upload_queue.len() {
                    if c.upload_queue[i].piece == piece {
                        let r = c.upload_queue.remove(i);
                        // `write_reject_request` counts the
                        // decision even when BEP-6 suppresses
                        // the wire message.
                        counters.add(crate::counters::Counter::PieceRejects, 1);
                        if c.supports_fast {
                            out.push(TorrentEffect::SendTo {
                                peer: *id,
                                msg: PeerMessage::RejectRequest(r),
                            });
                        }
                    } else {
                        i += 1;
                    }
                }
                // `reject_piece` — the `num_peers_up_requests`
                // gauge drops when the drain empties the queue.
                if c.upload_queue.is_empty() {
                    counters.add(crate::counters::Counter::NumPeersUpRequests, -1);
                }
                // `write_dont_have` — BEP-10 `lt_donthave`, NOT
                // BEP-6: suppressed unless the peer advertised a
                // `lt_donthave` extension id.
                if c.ext_dont_have != 0 {
                    out.push(TorrentEffect::SendTo {
                        peer: *id,
                        msg: PeerMessage::DontHave {
                            piece,
                            ext_id: c.ext_dont_have,
                        },
                    });
                }
            }
        }
        // upstream `piece_failed` — the culprit set is the
        // bad `blocks`' senders when v2 identified them,
        // else the piece's payload-block downloaders (pad
        // bytes are synthesized locally, never blamed).
        let downloaders = self
            .picker
            .as_ref()
            .map(|p| p.get_downloaders(piece))
            .unwrap_or_default();
        let payload_blocks = self
            .picker
            .as_ref()
            .map_or(0, |p| p.payload_blocks_in_piece(piece));
        let set: std::collections::BTreeSet<Option<u64>> = if blocks.is_empty() {
            downloaders
                .iter()
                .take(payload_blocks.max(0) as usize)
                .copied()
                .collect()
        } else {
            blocks
                .iter()
                .filter_map(|b| downloaders.get(*b as usize).copied())
                .collect()
        };
        // `found_on_disk` — the piece wasn't fetched from the
        // swarm (its only downloader entry disconnected):
        // no failed-byte accounting, no plugin call, no
        // peer penalty, no v2 block-hash isolation.
        let found_on_disk = set.len() == 1 && set.contains(&None);
        if !found_on_disk {
            // for v2 pieces without a known bad-block list,
            // request the block hash layer to isolate the
            // bad block.
            if blocks.is_empty()
                && self.ti.as_ref().is_some_and(|ti| ti.v2())
                && !self.merkle_trees.is_empty()
            {
                if let Some(fs) = self.files.clone() {
                    let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(
                        &fs,
                        &mut self.merkle_trees,
                        &mut self.hash_picker_state,
                    );
                    picker.verify_block_hashes(piece);
                }
            }
            // `add_failed_bytes` — the whole piece, or just
            // the identified bad blocks.
            let psize = self
                .files
                .as_ref()
                .map_or(self.piece_length, |f| i64::from(f.piece_size(piece)));
            self.failed_bytes += if blocks.is_empty() {
                psize
            } else {
                blocks.len() as i64 * 16384
            };
            // `torrent_plugin::on_piece_failed` — inside the
            // `!found_on_disk` leg upstream.
            for ext in &self.extensions {
                ext.on_piece_failed(piece);
            }
            // `smart_ban`'s `on_piece_failed` records the
            // per-block CRC of every downloader — skipped on
            // aborted torrents (`is_aborted` gate upstream) and on
            // v2 torrents, where the plugin is never created
            // (`create_smart_ban_plugin` returns null: merkle block
            // hashes already isolate corrupt senders).
            let smart_ban_on = !self.aborted && !self.ti.as_ref().is_some_and(|ti| ti.v2());
            if smart_ban_on {
                // upstream calls `ban_peer` + `disconnect` inside the
                // read callback *independently* of the trust-points
                // penalize below — the downloader set is not narrowed.
                let decision = self.smart_ban.on_piece_failed(
                    piece,
                    psize,
                    &downloaders,
                    &mut addr,
                    &mut read_digest,
                );
                if let BanDecision::Ban(key, ip) = decision {
                    self.apply_smart_ban(key, ip, counters, out);
                }
            }
            // `known_bad_peer` — lone downloader or a
            // block-precise culprit list.
            let known_bad = set.len() == 1 || !blocks.is_empty();
            self.penalize_peers(&set, known_bad, s, counters, out);
        }
        // `lock_piece` + `async_clear_piece` — the piece is restored
        // in `on_piece_sync` once in-flight writes have drained;
        // restoring early lets a delayed stale write re-mark a clean
        // block. Upstream fences through `m_storage`; the session
        // routes `ClearPiece` to `async_clear_piece` (or runs
        // `on_piece_sync` inline when there's no storage).
        if let Some(p) = self.picker.as_mut() {
            p.lock_piece(piece);
        }
        out.push(TorrentEffect::ClearPiece {
            piece,
            blocks: blocks.to_vec(),
        });
    }

    /// `torrent::we_have` — the piece is guaranteed on disk:
    /// `num_have_pieces`, clear predictive-announce state, run
    /// `received_piece`/`announce_piece`/`fill_send_buffer` per conn,
    /// extension `on_piece_pass` + smart_ban digest re-check, the
    /// filtered interest re-check, `piece_finished_alert`,
    /// file-progress edges, time-critical drain, and (downloading
    /// state only) the finished/seed transition + `m_last_download`.
    /// `loading_resume` suppresses the resume-dirty marks
    /// (`we_have(index, true)` upstream — the resume load *is* the
    /// state being persisted).
    #[allow(clippy::too_many_arguments)]
    fn we_have(
        &mut self,
        piece: PieceIndex,
        loading_resume: bool,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
        read_digest: &mut dyn FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) {
        // `num_have_pieces` — unconditional, first thing upstream.
        counters.add(crate::counters::Counter::NumHavePieces, 1);
        if (piece.0 as usize) < self.have.size() && !self.have.get_bit(piece.0 as usize) {
            self.have.set_bit(piece.0 as usize);
            // `total_done` — last piece counts its tail size, not a
            // full `piece_length` (`files` gives the exact size).
            let psize = self
                .files
                .as_ref()
                .map_or(self.piece_length, |f| i64::from(f.piece_size(piece)));
            self.verified_bytes += psize;
        }
        // `m_predictive_pieces` — a predicted piece was already
        // announced; it just needs its queued requests filled.
        let was_predictive = self.is_predictive_piece(piece);
        if was_predictive {
            self.predictive.clear_bit(piece.0 as usize);
        }
        for id in self.peers.keys().copied().collect::<Vec<_>>() {
            let disconnected = |out: &[TorrentEffect], at: usize| {
                out[at..]
                    .iter()
                    .any(|e| matches!(e, TorrentEffect::DisconnectPeer { peer, .. } if *peer == id))
            };
            let (skip, has_it) = {
                let c = match self.peers.get_mut(&id) {
                    Some(c) => c,
                    None => continue,
                };
                // `received_piece` — drop the piece from its
                // suggested/allowed-fast sets.
                c.suggested_pieces.retain(|&p| p != piece);
                c.allowed_fast.retain(|&p| p != piece);
                (c.connecting || !c.handshake_done, c.has_piece(piece))
            };
            if skip {
                continue;
            }
            // `received_piece` — gaining a piece the peer already has
            // may have removed its last interesting one.
            if has_it {
                let at = out.len();
                self.update_interest(id, counters, out);
                if disconnected(out, at) {
                    continue;
                }
            }
            let at = out.len();
            self.disconnect_if_redundant(id, out);
            if disconnected(out, at) {
                continue;
            }
            if !was_predictive {
                // `announce_piece` — `send_redundant_have` gate.
                if !s.send_redundant_have && has_it {
                    continue;
                }
                out.push(TorrentEffect::SendTo {
                    peer: id,
                    msg: PeerMessage::Have(piece),
                });
            } else {
                // already announced — a queued request for the piece
                // can now be served.
                self.fill_send_buffer(id, s, counters, out);
            }
        }
        // `m_extensions` `on_piece_pass` — fires after the announce
        // loop upstream; `smart_ban` rides here (its `on_piece_pass`
        // re-checks the stored per-block CRCs and may ban).
        for ext in &self.extensions {
            ext.on_piece_pass(piece);
        }
        let psize = self
            .files
            .as_ref()
            .map_or(self.piece_length, |f| i64::from(f.piece_size(piece)));
        // the smart_ban plugin is never created on v2 torrents
        // (`has_v2 → nullptr`) — no CRC ledger to re-check.
        if !self.ti.as_ref().is_some_and(|ti| ti.v2()) {
            for d in self.smart_ban.on_piece_passed(piece, psize, read_digest) {
                if let BanDecision::Ban(key, ip) = d {
                    self.apply_smart_ban(key, ip, counters, out);
                }
            }
        }
        // upstream `on_piece_pass` tail — once seeding, the whole
        // CRC ledger is dropped (no more downloads to blame).
        if self.is_seed() {
            self.smart_ban.clear();
        }
        // interest re-check — upstream filters `is_interesting &&
        // has_piece(index)`: gaining a piece can only shrink our
        // interest in peers that actually hold it.
        let ids: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| c.interesting && c.has_piece(piece))
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.update_interest(id, counters, out);
        }
        if !loading_resume {
            // `set_need_save_resume(if_download_progress)` +
            // `state_updated` — download progress changed.
            self.need_save_resume = true;
            self.state_updated();
        }
        // `piece_finished_alert` — after the announce + interest
        // work upstream.
        out.push(TorrentEffect::PiecePassed(piece));
        // `m_file_progress.update` — a file whose last piece just
        // landed gets `file_completed_alert`.
        self.file_completed_edges(piece, out);
        // `remove_time_critical_piece` — the piece just verified;
        // `alert_when_available` → read it back.
        if self
            .time_critical
            .remove(&piece)
            .is_some_and(|(_, alert)| alert)
        {
            out.push(TorrentEffect::ReadPiece { piece });
        }
        // `is_downloading_state(m_state)` gate upstream — the
        // finished transition, `m_last_download` and share-mode
        // rebalance don't run in `checking_files` /
        // `checking_resume_data`.
        if self.checking || self.checking_resume {
            return;
        }
        // upstream `torrent::finished()` fires on `is_finished()` —
        // all *wanted* pieces, not necessarily all pieces (a file
        // priority of 0 makes its pieces unwanted).
        let all = self.have.count() as i32 == self.num_pieces;
        let want_done = all || self.picker.as_ref().is_some_and(|p| p.is_finished());
        if !self.finished && want_done {
            self.finished = true;
            self.finished_at.get_or_insert_with(Instant::now);
            self.state_updated();
            out.push(TorrentEffect::BecameSeed);
            // upstream `finished()` drops upload-only peers — nothing
            // more to exchange either way.
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
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
            }
            // `stop_when_ready` — pause as soon as the download
            // completes (upstream pauses the torrent in
            // `files_checked`/piece-finish path).
            if self.stop_when_ready && !self.is_paused() {
                self.paused = true;
            }
        }
        if all {
            self.seeding = true;
            self.seeding_since.get_or_insert_with(Instant::now);
            // `maybe_done_flushing` — once seeding, upstream drops
            // `m_hash_picker`/`m_file_progress` (we keep the piece
            // picker for bookkeeping; its request/throttle state is
            // what's actually reset). Under `suggest_read_cache` the
            // picker must survive to keep answering hash requests.
            if s.suggest_mode != crate::settings::SuggestMode::SuggestReadCache {
                self.hash_picker_state = bitdaemon_disk::hash_picker::HashPickerState::default();
            }
        }
        // `m_last_download = now` — the verify edge, not block
        // arrival (`time_since_download`/`st.last_download`).
        self.last_payload_down = Some(Instant::now());
        // share mode re-runs the swarm rebalance on every piece we
        // gain.
        if self.share_mode {
            self.recalc_share_mode(s, counters, out);
        }
    }

    /// `torrent::on_piece_sync` — deferred continuation of
    /// `piece_failed`, runs after the `clear_piece` disk fence has
    /// drained every in-flight write of the piece: restore the
    /// blocks (`blocks` empty = whole piece), post
    /// `hash_failed_alert`, re-mark in-flight duplicate blocks
    /// (filtered to the bad set when `blocks` is precise).
    pub fn on_piece_sync(
        &mut self,
        piece: PieceIndex,
        blocks: &[i32],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if self.aborted {
            return;
        }
        // `!has_picker()` upstream — a `force_recheck` cleared the
        // picker mid-flight.
        let Some(p) = self.picker.as_mut() else {
            return;
        };
        p.restore_piece(piece, blocks);
        out.push(TorrentEffect::PieceFailed(piece));
        // re-mark in-flight duplicate blocks still queued on conns —
        // they are still expected to arrive (upstream re-marks only
        // the blocks in `blocks` when the set is precise).
        let mut inflight: Vec<(u64, bitdaemon_common::types::PieceBlock)> = Vec::new();
        for (id, c) in &self.peers {
            for b in c
                .pipeline
                .download_queue
                .iter()
                .filter(|b| !b.timed_out && !b.not_wanted)
                .chain(&c.pipeline.request_queue)
            {
                if b.block.piece_index == piece
                    && (blocks.is_empty() || blocks.contains(&b.block.block_index))
                {
                    inflight.push((*id, b.block));
                }
            }
        }
        for (id, block) in inflight {
            if let (Some(p), Some(c)) = (self.picker.as_mut(), self.peers.get(&id)) {
                // `picker_options()` — parole/snubbed peers pick
                // differently (same rule as the request path).
                let mut opts = crate::piece_picker::PickerOptions::RAREST_FIRST;
                if c.on_parole {
                    opts = opts
                        | crate::piece_picker::PickerOptions::ON_PAROLE
                        | crate::piece_picker::PickerOptions::PRIORITIZE_PARTIALS;
                }
                if c.pipeline.snubbed {
                    opts = opts | crate::piece_picker::PickerOptions::REVERSE;
                }
                p.mark_as_downloading(block, c.peer_key().or(Some(id)), opts);
            }
        }
        let _ = s;
        let _ = counters;
    }

    /// Build the per-file v2 merkle trees from `piece_layers`
    /// (upstream materializes them from `atp.merkle_trees` at
    /// `add_torrent`; same result — verified sparse vector or
    /// full layer).
    fn build_merkle_trees(&mut self, ti: &TorrentInfo) {
        let fs = ti.files();
        self.merkle_trees.clear();
        // upstream `m_hash_picker` is re-created on new metadata —
        // stale request/throttle state must not leak across trees.
        self.hash_picker_state = bitdaemon_disk::hash_picker::HashPickerState::default();
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

    /// `info_hashes().has_v1() && !m_disable_v1_hashes` — whether a
    /// hash job should also produce the SHA-1 piece hash (upstream
    /// `torrent::verify_piece`/`start_checking` flag condition).
    #[must_use]
    pub fn wants_v1_hash(&self) -> bool {
        !self.disable_v1_hashes
            && self
                .ti
                .as_ref()
                .is_some_and(|ti| ti.info_hash_v1().is_some())
    }

    /// The `disk_job_flags` for `async_hash`, mirroring upstream:
    /// `verify_piece` — `flush_piece` under write_through, `flush_piece
    /// | volatile_read` under disable_os_cache (+ `v1_hash` when v1 is
    /// wanted); `start_checking` — `sequential_access | volatile_read`
    /// (+ `v1_hash`). `checking` selects which upstream path applies.
    #[must_use]
    pub fn hash_job_flags(&self, checking: bool, write_mode: crate::settings::DiskIoMode) -> u8 {
        use bitdaemon_disk::disk_interface::flags as f;
        let mut flags = if checking {
            f::SEQUENTIAL_ACCESS | f::VOLATILE_READ
        } else {
            match write_mode {
                crate::settings::DiskIoMode::WriteThrough => f::FLUSH_PIECE,
                crate::settings::DiskIoMode::DisableOsCache => f::FLUSH_PIECE | f::VOLATILE_READ,
                _ => 0,
            }
        };
        if self.wants_v1_hash() {
            flags |= f::V1_HASH;
        }
        flags
    }

    /// The `block_hashes` span to pass to `async_hash`: sized to
    /// `blocks_in_piece2` for every v2-capable torrent — hybrid
    /// included (upstream `verify_piece` resizes on `has_v2()`), so
    /// `on_blocks_hashed` feeds the v2 tree on every verified piece.
    #[must_use]
    pub fn hash_job_v2_span(&self, piece: PieceIndex) -> Vec<bitdaemon_common::Sha256Hash> {
        if !self.ti.as_ref().is_some_and(|ti| ti.v2()) {
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
    /// file's merkle tree. `Some(true)` = fully verified,
    /// `Some(false)` = failed, `None` = indeterminate (upstream
    /// `boost::indeterminate` — the data exists but cannot be
    /// verified yet; the piece stays finished-but-unchecked).
    /// Verified ranges may span *adjacent* pieces (`first_verified_
    /// block`/`num_verified` may reach past this piece): those get
    /// `we_have` (or `piece_failed` when the last block proves the
    /// range bad), like upstream.
    pub fn verify_v2_blocks(
        &mut self,
        piece: PieceIndex,
        block_hashes: &[bitdaemon_common::Sha256Hash],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) -> Option<bool> {
        let Some(fs) = self.files.clone() else {
            return Some(false);
        };
        let blocks_in_piece = fs.blocks_in_piece2(piece);
        let blocks_per_piece = fs.blocks_per_piece();
        // upstream bails on truncated hash arrays (enoent/eof).
        let mut block_passed = vec![false; blocks_in_piece as usize];
        let mut last_result = bitdaemon_disk::hash_picker::SetBlockHashResult {
            status: bitdaemon_disk::hash_picker::SetBlockHashStatus::Unknown,
            first_verified_block: 0,
            num_verified: 0,
        };
        // pieces other than `piece` fully covered by a verified range
        // — deferred until we know the last block didn't fail.
        let mut adjacent_verified: Vec<PieceIndex> = Vec::new();
        let mut ret: Option<bool> = None;
        {
            let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(
                &fs,
                &mut self.merkle_trees,
                &mut self.hash_picker_state,
            );
            for (i, h) in block_hashes.iter().enumerate() {
                if h.is_all_zeros() {
                    return Some(false);
                }
                let r = picker.set_block_hash(piece, (i * 16384) as i32, h);
                last_result = r;
                match r.status {
                    bitdaemon_disk::hash_picker::SetBlockHashStatus::Success => {
                        let first = r.first_verified_block.max(0) as usize;
                        let count = (blocks_in_piece as usize - first).min(r.num_verified as usize);
                        for bp in &mut block_passed[first..first + count] {
                            *bp = true;
                        }
                        // every piece the verified range touches, if
                        // it was already finished-but-unverified.
                        let start = piece.0 + r.first_verified_block / blocks_per_piece;
                        let end = start + r.num_verified / blocks_per_piece;
                        for vp in start..end {
                            let vp = PieceIndex(vp);
                            if vp != piece
                                && self
                                    .picker
                                    .as_ref()
                                    .is_some_and(|p| p.is_piece_finished(vp) && !p.have_piece(vp))
                            {
                                // upstream asserts the hash-picker
                                // verifies the piece here.
                                debug_assert!(picker.piece_verified(vp));
                                adjacent_verified.push(vp);
                            }
                        }
                    }
                    bitdaemon_disk::hash_picker::SetBlockHashStatus::BlockHashFailed => {
                        ret = Some(false);
                    }
                    bitdaemon_disk::hash_picker::SetBlockHashStatus::PieceHashFailed
                    | bitdaemon_disk::hash_picker::SetBlockHashStatus::Unknown => {}
                }
            }
            let _ = picker;
        }
        if last_result.status == bitdaemon_disk::hash_picker::SetBlockHashStatus::PieceHashFailed {
            // only the last block failing proves *this* piece bad
            // (earlier failures could come from stale siblings).
            ret = Some(false);
            let start = piece.0 + last_result.first_verified_block / blocks_per_piece;
            let end = start + last_result.num_verified / blocks_per_piece;
            for vp in start..end {
                let vp = PieceIndex(vp);
                if vp == piece {
                    continue;
                }
                // `we_dont_have` + `piece_failed` — the piece had
                // verified earlier via a stale sibling hash; pull it
                // back out of `have` and run the full failure
                // machinery (penalize → lock → clear fence → restore).
                if let Some(p) = self.picker.as_mut() {
                    p.we_dont_have(vp);
                }
                self.mark_lost_piece(vp);
                self.piece_failed(
                    vp,
                    &[],
                    s,
                    counters,
                    out,
                    |_| std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    |_| None,
                );
            }
        } else {
            for vp in adjacent_verified {
                if self.picker.as_ref().is_some_and(|p| p.have_piece(vp)) {
                    continue;
                }
                // adjacent pieces pass via `piece_flushed` +
                // `we_have` upstream — the sibling hash proved them,
                // they weren't downloaded (no `piece_passed`
                // counters/rewards).
                if let Some(p) = self.picker.as_mut() {
                    p.piece_flushed(vp);
                }
                self.we_have(vp, false, s, counters, out, &mut |_| None);
            }
        }
        if ret.is_none() && block_passed.iter().all(|b| *b) {
            ret = Some(true);
        }
        ret
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
        if self.disable_v1_hashes {
            f |= F::DISABLE_V1_HASHES;
        }
        if self.state_subscription {
            f |= F::UPDATE_SUBSCRIBE;
        }
        // `m_enable_*` serialize as their inverse `disable_*` bits.
        if !self.enable_dht {
            f |= F::DISABLE_DHT;
        }
        if !self.enable_lsd {
            f |= F::DISABLE_LSD;
        }
        if !self.enable_pex {
            f |= F::DISABLE_PEX;
        }
        f
    }

    /// `torrent::state_updated` — mark this torrent for the next
    /// `state_update_alert`. A no-op unless the
    /// `flag_update_subscribe` subscription bit is set (upstream
    /// early-returns on `!m_state_subscription`).
    pub fn state_updated(&mut self) {
        if self.state_subscription {
            self.needs_state_update = true;
        }
    }

    /// `set_flags`/`unset_flags` — apply `flags` under `mask`
    /// (upstream `torrent::set_flags`): only the masked bits are
    /// touched. `PAUSED` is *not* written here — upstream delegates
    /// it to `pause(graceful_pause)`/`resume()`; the session runs
    /// that machinery. Returns `Some(requested)` when the mask
    /// carried `PAUSED`, `None` otherwise.
    pub fn set_flags(
        &mut self,
        flags: bitdaemon_common::flags::TorrentFlags,
        mask: bitdaemon_common::flags::TorrentFlags,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) -> Option<bool> {
        use bitdaemon_common::flags::TorrentFlags as F;
        let pause_req = mask.contains(F::PAUSED).then(|| flags.contains(F::PAUSED));
        // upstream delegates to per-flag setters which early-return on
        // no-change and otherwise mark `state_updated()`.
        let mut changed = false;
        if mask.contains(F::AUTO_MANAGED) {
            let v = flags.contains(F::AUTO_MANAGED);
            changed |= v != self.auto_managed;
            self.auto_managed = v;
        }
        if mask.contains(F::SEQUENTIAL_DOWNLOAD) {
            let v = flags.contains(F::SEQUENTIAL_DOWNLOAD);
            if v != self.sequential {
                self.set_sequential_download(v);
                changed = true;
            }
        }
        if mask.contains(F::UPLOAD_MODE) {
            self.set_upload_mode(flags.contains(F::UPLOAD_MODE), s, counters, out);
        }
        if mask.contains(F::SHARE_MODE) {
            self.set_share_mode(flags.contains(F::SHARE_MODE), s, counters, out);
        }
        if mask.contains(F::SUPER_SEEDING) {
            self.set_super_seeding(flags.contains(F::SUPER_SEEDING));
        }
        if mask.contains(F::NEED_SAVE_RESUME) {
            self.need_save_resume = flags.contains(F::NEED_SAVE_RESUME);
        }
        if mask.contains(F::UPDATE_SUBSCRIBE) {
            // upstream `set_flags` flips `m_state_subscription`
            // directly (it isn't a saved flag).
            self.state_subscription = flags.contains(F::UPDATE_SUBSCRIBE);
        }
        // `disable_*` masks write the inverse `m_enable_*` —
        // upstream marks `need_save_resume` on each flip.
        for (bit, field) in [
            (F::DISABLE_DHT, &mut self.enable_dht),
            (F::DISABLE_LSD, &mut self.enable_lsd),
            (F::DISABLE_PEX, &mut self.enable_pex),
        ] {
            if mask.contains(bit) {
                let v = !flags.contains(bit);
                if *field != v {
                    *field = v;
                    self.need_save_resume = true;
                    changed = true;
                }
            }
        }
        // upstream: clearing seed_mode via flags leaves seed mode the
        // "check files" way — promise was wrong → recheck.
        if mask.contains(F::SEED_MODE) && self.seed_mode && !flags.contains(F::SEED_MODE) {
            self.leave_seed_mode(true, out);
            changed = true;
        }
        if changed {
            self.state_updated();
        }
        pause_req
    }

    /// `leave_seed_mode` — `check_files` mirrors upstream
    /// `seed_mode_t::check_files`: the promise was false → drop
    /// have_all, re-enter `downloading` and force a recheck.
    /// `seed_mode_t::skip_checking` (`check_files = false`) keeps
    /// the data as-is (used by the fastresume-ok path).
    pub fn leave_seed_mode(&mut self, check_files: bool, out: &mut Vec<TorrentEffect>) {
        if !self.seed_mode {
            return;
        }
        self.seed_mode = false;
        // upstream rechecks unless the torrent is still in
        // `checking_resume_data` (a fastresume verdict is pending).
        if check_files && !self.checking_resume {
            self.seeding = false;
            self.finished = false;
            // `set_have_all(false)` — forget all pieces; the armed
            // `force_recheck` pass re-verifies them from disk.
            self.have.clear_all();
            self.verified_bytes = 0;
            self.init_files_completed();
            self.checking = false;
            if self.arm_full_recheck() > 0 {
                for p in 0..self.num_pieces {
                    out.push(TorrentEffect::DiskHash {
                        piece: PieceIndex(p),
                    });
                }
            }
        }
        self.num_verified = 0;
        self.seed_verified = Bitfield::default();
        self.seed_verifying = Bitfield::default();
        self.need_save_resume = true;
    }

    /// `peer_connection::on_seed_mode_hashed`'s torrent-side verdict
    /// (the disk error leg is handled by the caller). Feeds the
    /// block hashes into the merkle tree, then:
    ///
    /// - v1/v2 disagree → `set_error(torrent_inconsistent_hashes)` +
    ///   `pause()`; returns `false` (upstream skips the
    ///   `fill_send_buffer` tail).
    /// - either side failed → `hash_failed_alert` +
    ///   `leave_seed_mode(check_files)`; returns `true`.
    /// - pass → `verified(piece)`; `all_verified()` →
    ///   `leave_seed_mode(skip_checking)`; returns `true`.
    pub fn seed_mode_hashed(
        &mut self,
        piece: PieceIndex,
        hash: &bitdaemon_common::Sha1Hash,
        block_hashes: &[bitdaemon_common::Sha256Hash],
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) -> bool {
        // `hash_failed[V1/V2]` as tribools — None = indeterminate.
        let mut failed = [None, None];
        if !s.disable_hash_checks && self.wants_v1_hash() {
            failed[0] = self.expected_hash(piece).map(|want| *hash != want);
        }
        if !s.disable_hash_checks && self.ti.as_ref().is_some_and(|ti| ti.v2()) {
            if let Some(fs) = self.files.clone() {
                failed[1] = Some(false);
                let mut picker = bitdaemon_disk::hash_picker::HashPicker::new(
                    &fs,
                    &mut self.merkle_trees,
                    &mut self.hash_picker_state,
                );
                let mut last = bitdaemon_disk::hash_picker::SetBlockHashStatus::Unknown;
                for i in 0..fs.blocks_in_piece2(piece) {
                    let h = block_hashes.get(i as usize).copied().unwrap_or_default();
                    let r = picker.set_block_hash(piece, i * 16384, &h);
                    last = r.status;
                    if matches!(
                        r.status,
                        bitdaemon_disk::hash_picker::SetBlockHashStatus::BlockHashFailed
                            | bitdaemon_disk::hash_picker::SetBlockHashStatus::PieceHashFailed
                    ) {
                        failed[1] = Some(true);
                    }
                }
                // the last block still unknown → the piece's root
                // hash can't be verified → must leave seed mode.
                if last == bitdaemon_disk::hash_picker::SetBlockHashStatus::Unknown {
                    failed = [Some(true), Some(true)];
                }
            }
        }
        if matches!((failed[0], failed[1]), (Some(a), Some(b)) if a != b) {
            // `torrent_inconsistent_hashes` — one protocol passed,
            // the other failed.
            self.set_error("inconsistent hashes", crate::status::ERROR_FILE_NONE);
            out.push(TorrentEffect::Alert(crate::alerts::Alert::TorrentError {
                ih: self.info_hash,
                error: self.last_error.clone(),
                file: self.resolve_filename(crate::status::ERROR_FILE_NONE),
            }));
            self.state_updated();
            self.paused = true;
            return false;
        }
        if failed[0] == Some(true) || failed[1] == Some(true) {
            out.push(TorrentEffect::Alert(crate::alerts::Alert::HashFailed {
                ih: self.info_hash,
                piece,
            }));
            self.leave_seed_mode(true, out);
        } else {
            if self.seed_mode {
                debug_assert!(self.seed_verifying.get_bit(piece.0 as usize));
                if (piece.0 as usize) < self.seed_verified.size()
                    && !self.seed_verified.get_bit(piece.0 as usize)
                {
                    self.seed_verified_piece(piece);
                }
            }
            if self.seed_mode && i64::from(self.num_verified) == i64::from(self.num_pieces) {
                // `all_verified()` — every promised piece now proven.
                self.leave_seed_mode(false, out);
            }
        }
        true
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
            self.add_tracker(url.clone(), *tier, crate::tracker_list::SOURCE_CLIENT);
        }
        self.need_save_resume = true;
    }

    /// `torrent::write_resume_data` — serialize this torrent's state
    /// through `to_resume_data` + `write_resume_data_buf`
    /// (byte-compatible with upstream `write_resume_data`).
    #[must_use]
    pub fn write_resume_data(&self, pool: &bitdaemon_net::peer_class::PeerClassPool) -> Vec<u8> {
        let mut atp = AddTorrentParams::default();
        self.to_resume_data(&mut atp, pool);
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
            // upstream deletes the `web_seed_t` entry — its conn
            // teardown frees the picked blocks, so ours release
            // here. A fetch still in flight marks the entry
            // `removed` (deferred delete — upstream does the same
            // while a resolve is outstanding); the late result is
            // discarded by `on_web_seed_done`'s GC.
            let (key, blocks) = self
                .web_seed_state
                .get(url)
                .map_or((None, Vec::new()), |st| {
                    (Some(st.key), st.outstanding.iter().copied().collect())
                });
            for b in &blocks {
                self.web_seed_outstanding.remove(b);
                if let Some(p) = self.picker.as_mut() {
                    p.abort_download(*b, key);
                }
            }
            // `~peer_connection` — the seed's refcount leaves the
            // picker whether or not a fetch is still resolving.
            self.dec_web_seed_refcount(url);
            if self.web_seed_state.get(url).is_some_and(|st| st.busy) {
                if let Some(st) = self.web_seed_state.get_mut(url) {
                    st.removed = true;
                    st.outstanding.clear();
                    st.status.outstanding = 0;
                }
            } else {
                self.web_seed_state.remove(url);
            }
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
    /// Upstream follows with `update_peer_interest(was_finished)` —
    /// interest may flip everywhere and the torrent can cross the
    /// finished boundary in either direction.
    pub fn prioritize_pieces(
        &mut self,
        prios: &[bitdaemon_common::types::DownloadPriority],
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let was_finished = self.is_finished();
        if let Some(picker) = self.picker.as_mut() {
            for (i, prio) in prios.iter().enumerate().take(self.num_pieces as usize) {
                picker.set_piece_priority(PieceIndex(i as i32), *prio);
            }
        }
        self.need_save_resume = true;
        self.update_peer_interest(was_finished, counters, out);
    }

    /// `torrent::update_peer_interest` — recompute every conn's
    /// interest, then run the finished/resume-download transition
    /// when the wanted-set predicate moved (`is_downloading_state`
    /// gate: checking_* torrents skip transitions).
    fn update_peer_interest(
        &mut self,
        was_finished: bool,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for pid in ids {
            self.update_interest(pid, counters, out);
        }
        if self.checking || self.checking_resume {
            return;
        }
        let finished = self.is_finished();
        if !was_finished && finished {
            // upstream `finished()` — queue leaves the download list
            // (our position derives from `self.finished`), state
            // timers roll over, upload-only is broadcast, and
            // `m_completed_time` stamps.
            self.finished = true;
            self.finished_at.get_or_insert_with(Instant::now);
            out.push(TorrentEffect::BecameSeed);
            self.send_upload_only(out);
            self.state_updated();
        } else if was_finished && !finished {
            // upstream `resume_download()` — back on the download
            // queue, `m_completed_time` cleared, upload-only
            // rebroadcast (now false), tick re-evaluated.
            self.finished = false;
            self.seeding = self.is_seed();
            self.finished_at = None;
            self.send_upload_only(out);
            self.state_updated();
        }
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
    /// bandwidth throttle (bytes/s, 0/-1 = unlimited). Upstream
    /// routes these through `set_limit_impl` → the torrent's
    /// lazily-created `peer_class` channel.
    pub fn set_upload_limit(
        &mut self,
        bytes_per_sec: i64,
        pool: &mut bitdaemon_net::peer_class::PeerClassPool,
    ) {
        self.set_limit_impl(bytes_per_sec, bitdaemon_net::peer_class::UPLOAD, true, pool);
    }

    /// `set_download_limit`.
    pub fn set_download_limit(
        &mut self,
        bytes_per_sec: i64,
        pool: &mut bitdaemon_net::peer_class::PeerClassPool,
    ) {
        self.set_limit_impl(
            bytes_per_sec,
            bitdaemon_net::peer_class::DOWNLOAD,
            true,
            pool,
        );
    }

    /// `torrent::set_limit_impl` — `limit <= 0` or `inf` normalizes
    /// to 0 (unlimited); a zero limit with no class yet is a no-op,
    /// otherwise `setup_peer_class` runs first. `state_update` maps
    /// the caller's `state_update` flag (ctor seeding passes false).
    fn set_limit_impl(
        &mut self,
        limit: i64,
        channel: usize,
        state_update: bool,
        pool: &mut bitdaemon_net::peer_class::PeerClassPool,
    ) {
        let limit = if limit <= 0 || limit == i64::from(i32::MAX) {
            0
        } else {
            limit
        };
        if self.peer_class == 0 {
            if limit == 0 {
                return;
            }
            self.setup_peer_class(pool);
        }
        let Some(tpc) = pool.at_mut(self.peer_class) else {
            return;
        };
        if tpc.channel[channel].throttle_value() == limit {
            return;
        }
        if state_update {
            self.state_updated();
            // `set_need_save_resume(if_config_changed)`.
            self.need_save_resume = true;
        }
        tpc.channel[channel].throttle(limit);
    }

    /// `torrent::setup_peer_class` — allocate the torrent's class in
    /// the session pool (labeled with the torrent name) and attach
    /// it to the torrent's class set.
    fn setup_peer_class(&mut self, pool: &mut bitdaemon_net::peer_class::PeerClassPool) {
        debug_assert_eq!(self.peer_class, 0);
        self.peer_class = pool.new_peer_class(self.name.clone());
        self.peer_classes.add_class(pool, self.peer_class);
    }

    /// `torrent::limit_impl` — `-1` when the torrent has no class or
    /// the channel is unlimited. Our `BandwidthChannel` repr stores
    /// `0` for infinite (upstream stores `inf`/`int::max`), so the
    /// unlimited check is `== 0`.
    fn limit_impl(&self, channel: usize, pool: &bitdaemon_net::peer_class::PeerClassPool) -> i64 {
        if self.peer_class == 0 {
            return -1;
        }
        let Some(tpc) = pool.at(self.peer_class) else {
            return -1;
        };
        let limit = tpc.channel[channel].throttle_value();
        if limit == 0 { -1 } else { limit }
    }

    /// `torrent::upload_limit` — bytes/s (`-1` = unset/unlimited).
    #[must_use]
    pub fn upload_limit(&self, pool: &bitdaemon_net::peer_class::PeerClassPool) -> i64 {
        self.limit_impl(bitdaemon_net::peer_class::UPLOAD, pool)
    }

    /// `torrent::download_limit`.
    #[must_use]
    pub fn download_limit(&self, pool: &bitdaemon_net::peer_class::PeerClassPool) -> i64 {
        self.limit_impl(bitdaemon_net::peer_class::DOWNLOAD, pool)
    }

    /// `session_impl::ignore_unchoke_slots_set(*this)` — the torrent
    /// leg of `peer_connection::ignore_unchoke_slots`: any class in
    /// the torrent's set flagged `ignore_unchoke_slots`. Live pool
    /// eval so `set_peer_class` flag changes apply immediately.
    #[must_use]
    pub fn ignores_unchoke_slots(&self, pool: &bitdaemon_net::peer_class::PeerClassPool) -> bool {
        let mut ignore = false;
        self.peer_classes.for_each(|c| {
            if let Some(pc) = pool.at(c) {
                ignore |= pc.ignore_unchoke_slots;
            }
        });
        ignore
    }

    /// `~torrent`/`abort`'s `m_peer_class` release — `remove_class`
    /// drops the set's reference, then `decref` drops the creator's.
    pub fn release_peer_class(&mut self, pool: &mut bitdaemon_net::peer_class::PeerClassPool) {
        if self.peer_class > 0 {
            self.peer_classes.remove_class(pool, self.peer_class);
            pool.decref(self.peer_class);
            self.peer_class = 0;
        }
    }

    /// `torrent::set_max_uploads` — `limit <= 0` normalizes to the
    /// `(1<<24)-1` "unlimited" sentinel upstream stores.
    pub fn set_max_uploads(&mut self, limit: i32, state_update: bool) {
        let limit = if limit <= 0 { (1 << 24) - 1 } else { limit };
        if self.max_uploads == limit {
            return;
        }
        if state_update {
            self.state_updated();
        }
        self.max_uploads = limit;
        if state_update {
            // `set_need_save_resume(if_config_changed)`.
            self.need_save_resume = true;
        }
    }

    /// `torrent::set_max_connections` — same normalization, plus
    /// `update_want_peers` and an immediate `disconnect_peers` trim
    /// when the pool shrank below the live conn count.
    pub fn set_max_connections(
        &mut self,
        limit: i32,
        state_update: bool,
        out: &mut Vec<TorrentEffect>,
    ) {
        let limit = if limit <= 0 { (1 << 24) - 1 } else { limit };
        if self.max_connections == limit {
            return;
        }
        if state_update {
            self.state_updated();
        }
        self.max_connections = limit;
        self.update_want_peers();
        if self.peers.len() as i64 > i64::from(self.max_connections) {
            let excess = self.peers.len() - self.max_connections.max(0) as usize;
            self.disconnect_peers(excess, "too_many_connections", out);
        }
        if state_update {
            self.need_save_resume = true;
        }
    }

    /// `torrent::set_share_mode` — entering share mode reprioritizes
    /// every file to `dont_download` (the picker demotes have-pieces
    /// so rarest-first serves the swarm).
    pub fn set_share_mode(
        &mut self,
        on: bool,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if on == self.share_mode {
            return;
        }
        self.share_mode = on;
        // upstream `set_need_save_resume(if_config_changed)`.
        self.need_save_resume = true;
        self.state_updated();
        if on {
            // upstream sizes the vector from `file_priority`'s
            // length when metadata is absent.
            let n = self
                .files
                .as_ref()
                .map_or(self.file_priorities.len(), |fs| fs.num_files() as usize);
            self.prioritize_files(
                &vec![bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD; n],
                s,
                counters,
                out,
            );
        }
    }

    /// `torrent::set_super_seeding` — leaving super-seed mode clears
    /// each conn's leaked-piece slots (`superseed_piece(-1, -1)`).
    pub fn set_super_seeding(&mut self, on: bool) {
        if on == self.super_seeding {
            return;
        }
        self.super_seeding = on;
        self.need_save_resume = true;
        self.state_updated();
        if on {
            return;
        }
        for c in self.peers.values_mut() {
            c.superseed_pieces = [None, None];
        }
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
        // any disk job dispatched before this point is now stale; its
        // completion must not touch the new checking pass
        // (upstream `++m_picker_generation` in `force_recheck`).
        self.picker_generation = self.picker_generation.wrapping_add(1);
        self.have.clear_all();
        self.verified_bytes = 0;
        // upstream `picker.resize` → `file_progress.clear`+`init` —
        // a recheck re-fires `file_completed` edges.
        self.init_files_completed();
        self.finished = false;
        self.seeding = false;
        self.checking = true;
        self.checking_resume = false;
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
                // recheck completion → `state_updated()` upstream.
                self.state_updated();
                return true;
            }
        }
        false
    }

    /// `prioritize_files` (upstream `torrent::prioritize_files`) —
    /// `fix_priorities` — size the vector to `num_files` (default
    /// fill), force pad files to `dont_download`, clamp at
    /// `top_priority`. Without metadata upstream only clamps.
    fn fix_priorities(
        &self,
        new: &[bitdaemon_common::types::DownloadPriority],
    ) -> Vec<bitdaemon_common::types::DownloadPriority> {
        use bitdaemon_common::types::DownloadPriority;
        let mut v: Vec<DownloadPriority> = if let Some(fs) = &self.files {
            let n = fs.num_files().max(0) as usize;
            let mut v = vec![DownloadPriority::DEFAULT; n];
            for (i, p) in new.iter().enumerate().take(n) {
                v[i] = *p;
            }
            v
        } else {
            new.to_vec()
        };
        for (i, p) in v.iter_mut().enumerate() {
            let is_pad = self.files.as_ref().is_some_and(|fs| {
                i32::try_from(i)
                    .ok()
                    .is_some_and(|fi| fs.pad_file_at(FileIndex(fi)))
            });
            if *p > DownloadPriority::DONT_DOWNLOAD && is_pad {
                *p = DownloadPriority::DONT_DOWNLOAD;
            } else if *p > DownloadPriority::TOP {
                *p = DownloadPriority::TOP;
            }
        }
        v
    }

    /// `prioritize_files` — upstream defers to `on_file_priority`
    /// when an `async_set_file_priority` job is in flight (out-of-
    /// order completions would silently clobber), dispatches the
    /// disk job otherwise; `m_file_priority` is updated only by the
    /// completion. Without storage the vector is stored directly.
    pub fn prioritize_files(
        &mut self,
        new: &[bitdaemon_common::types::DownloadPriority],
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        let fixed = self.fix_priorities(new);
        self.deferred_file_priorities.clear();
        if self.outstanding_file_priority {
            for (i, p) in fixed.iter().enumerate() {
                self.deferred_file_priorities
                    .insert(i32::try_from(i).unwrap_or(i32::MAX), *p);
            }
            return;
        }
        if self.storage != StorageIndex::INVALID {
            self.outstanding_file_priority = true;
            out.push(TorrentEffect::SetFilePriority { prios: fixed });
        } else {
            // `!m_storage` upstream (shutdown leg): store, no disk
            // job, no picker touch.
            if self.file_priorities != fixed {
                self.file_priorities = fixed;
                self.need_save_resume = true;
            }
        }
        let _ = (s, counters);
    }

    /// `on_file_priority` — the `async_set_file_priority`
    /// completion: `prios` is the storage's *adjusted* vector
    /// (partial failure writes back the on-disk truth). Applies
    /// piece priorities + `m_file_priority`, reports
    /// `file_prio`/`file_error`, then drains deferred updates.
    pub fn on_file_priority(
        &mut self,
        prios: Vec<bitdaemon_common::types::DownloadPriority>,
        err: Option<&bitdaemon_disk::storage_error::StorageError>,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        self.outstanding_file_priority = false;
        if self.file_priorities != prios {
            self.update_piece_priorities(&prios, s, counters, out);
            self.file_priorities = prios;
            // `need_save_resume(if_config_changed)`.
            self.need_save_resume = true;
            if self.share_mode {
                self.recalc_share_mode(s, counters, out);
            }
        }
        if let Some(e) = err.filter(|e| e.is_err()) {
            // `file_error_alert` + `set_error` + `pause()` — the
            // storage could not apply the priorities.
            let file = e.file;
            out.push(TorrentEffect::Alert(crate::alerts::Alert::FileError {
                ih: self.info_hash,
                path: self.resolve_filename(file.0),
                error: e.to_string(),
            }));
            self.set_error(&e.to_string(), file.0);
            // `pause()` — hard pause (upstream calls it on any
            // file-priority failure).
            self.paused = true;
            self.state_updated();
        } else {
            out.push(TorrentEffect::Alert(crate::alerts::Alert::FilePrio {
                ih: self.info_hash,
                op: "file_priority".into(),
                error: String::new(),
            }));
        }
        // apply queued priorities even on failure — `m_file_priority`
        // reflects the storage's actual state.
        if !self.deferred_file_priorities.is_empty() && !self.aborted {
            let deferred = std::mem::take(&mut self.deferred_file_priorities);
            let mut new_priority = self.file_priorities.clone();
            let max_idx = deferred.keys().next_back().copied().unwrap_or(-1);
            if max_idx >= 0 {
                let need = max_idx as usize + 1;
                if new_priority.len() < need {
                    new_priority.resize(need, bitdaemon_common::types::DownloadPriority::DEFAULT);
                }
                for (idx, prio) in deferred {
                    if let Some(slot) = new_priority.get_mut(idx as usize) {
                        *slot = prio;
                    }
                }
            }
            self.prioritize_files(&new_priority, s, counters, out);
        }
    }

    /// `update_piece_priorities` — every piece's priority becomes
    /// the max of the files overlapping it (`file_to_piece_prio`
    /// → `prioritize_pieces`), then `update_peer_interest`.
    fn update_piece_priorities(
        &mut self,
        prios: &[bitdaemon_common::types::DownloadPriority],
        _s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        use bitdaemon_common::types::DownloadPriority;
        let was_finished = self.is_finished();
        // `update_piece_priority` → `state_updated()` — the wanted
        // set may have changed.
        self.state_updated();
        if let (Some(fs), Some(picker)) = (self.files.clone(), self.picker.as_mut()) {
            for p in 0..self.num_pieces {
                let piece = PieceIndex(p);
                let prio = fs
                    .map_block(piece, 0, i64::from(fs.piece_size(piece)))
                    .iter()
                    .map(|sl| {
                        prios
                            .get(sl.file_index.0 as usize)
                            .copied()
                            .unwrap_or(DownloadPriority::DEFAULT)
                    })
                    .max()
                    .unwrap_or(DownloadPriority::DEFAULT);
                picker.set_piece_priority(piece, prio);
            }
        }
        self.update_peer_interest(was_finished, counters, out);
    }

    /// `file_progress::init` — (re)seed the completion-edge tracker
    /// from current `have` state. Files already covered (or never
    /// alertable: pad/zero-size) start marked so `file_completed_edges`
    /// can't re-fire for them — upstream's `init` loads the same
    /// coverage into `m_file_progress` and only `update` edges alert.
    pub fn init_files_completed(&mut self) {
        let Some(fs) = self.files.clone() else {
            self.files_completed.clear();
            return;
        };
        let n = (fs.num_files().max(0)) as usize;
        self.files_completed = (0..n)
            .map(|f| {
                let fi = bitdaemon_common::types::FileIndex(f as i32);
                fs.pad_file_at(fi) || fs.file_size(fi) <= 0 || self.file_covered(&fs, fi)
            })
            .collect();
    }

    /// every piece overlapping `file`'s byte range is `have` —
    /// byte-complete ⟺ piece-complete (a `have` piece is fully on
    /// disk, covering any file bytes it spans).
    fn file_covered(&self, fs: &bitdaemon_core::FileStorage, file: FileIndex) -> bool {
        let size = fs.file_size(file);
        if size <= 0 {
            return true;
        }
        let first = fs.map_file(file, 0, 1).piece;
        let last = fs.map_file(file, size - 1, 1).piece;
        (first.0..=last.0).all(|p| self.have.get_bit(p as usize))
    }

    /// `file_progress::update` + `file_completed_alert` — `piece`
    /// just became `have`; alert each real file whose covering
    /// pieces are now all had (the first transition only —
    /// `we_have`'s not-had-before invariant makes coverage now
    /// imply the edge).
    fn file_completed_edges(&mut self, piece: PieceIndex, out: &mut Vec<TorrentEffect>) {
        let Some(fs) = self.files.clone() else {
            return;
        };
        if self.files_completed.is_empty() {
            return;
        }
        let off = i64::from(piece.0) * i64::from(fs.piece_length());
        let end = off + i64::from(fs.piece_size(piece)) - 1;
        let first = fs.file_index_at_offset(off).0;
        let last = fs.file_index_at_offset(end.max(off)).0;
        for f in first..=last {
            let idx = f as usize;
            if idx >= self.files_completed.len() || self.files_completed[idx] {
                continue;
            }
            let fi = FileIndex(f);
            if self.file_covered(&fs, fi) {
                self.files_completed[idx] = true;
                out.push(TorrentEffect::FileCompleted(fi));
            }
        }
    }

    /// `we_dont_have` — pull the piece out of `have`, rewind
    /// `verified_bytes`, and re-arm the completion edge of every
    /// real file it touches (upstream `file_progress::update`
    /// re-fires its `>= size` check on the re-download).
    fn mark_lost_piece(&mut self, piece: PieceIndex) {
        if (piece.0 as usize) >= self.have.size() || !self.have.get_bit(piece.0 as usize) {
            return;
        }
        self.have.clear_bit(piece.0 as usize);
        if let Some(fs) = self.files.clone() {
            self.verified_bytes = (self.verified_bytes - i64::from(fs.piece_size(piece))).max(0);
            let off = i64::from(piece.0) * i64::from(fs.piece_length());
            let end = off + i64::from(fs.piece_size(piece)) - 1;
            let first = fs.file_index_at_offset(off).0;
            let last = fs.file_index_at_offset(end.max(off)).0;
            for f in first..=last {
                let idx = f as usize;
                let fi = FileIndex(f);
                if idx < self.files_completed.len() && !fs.pad_file_at(fi) {
                    self.files_completed[idx] = false;
                }
            }
        } else {
            self.verified_bytes = (self.verified_bytes - self.piece_length).max(0);
        }
    }

    /// `file_progress` — per-file bytes done (upstream
    /// `torrent::file_progress`): whole pieces come from `have`,
    /// partial pieces from the picker's finished blocks.
    #[must_use]
    /// `torrent::file_progress` — per-file downloaded bytes.
    /// `piece_granularity` (upstream `file_progress_flags_t`) skips
    /// the partial-piece pass entirely. Without it, `Finished` and
    /// `Writing` blocks count whole (upstream additionally counts
    /// `Requested` blocks' in-flight bytes via the socket receive
    /// buffer — our transports deliver whole messages, so those
    /// sub-block bytes aren't visible to the session).
    pub fn file_progress(&self, piece_granularity: bool) -> Vec<i64> {
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
            } else if !piece_granularity {
                if let Some(picker) = &self.picker {
                    let nb = picker.blocks_in_piece(piece) as i64;
                    for b in 0..nb {
                        let blk = PieceBlock {
                            piece_index: piece,
                            block_index: b as i32,
                        };
                        if picker.is_downloaded(blk) {
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
        counters: &crate::counters::Counters,
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
        let existed = self
            .time_critical
            .insert(piece, (deadline, alert_when_available))
            .is_some();
        // upstream bumps to `top_priority` unconditionally; a piece
        // that was `dont_download` re-enters the wanted set →
        // interest + finished↔downloading re-eval.
        let was_dont_download = self.picker.as_ref().is_some_and(|p| {
            p.piece_priority(piece) == bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD
        });
        let was_finished = self.is_finished();
        if let Some(p) = self.picker.as_mut() {
            p.set_piece_priority(piece, bitdaemon_common::types::DownloadPriority::TOP);
        }
        if was_dont_download {
            self.state_updated();
            self.update_peer_interest(was_finished, counters, out);
        }
        if existed {
            // re-deadline of a known critical piece stops here
            // upstream (update + resort only).
            return;
        }
        if first {
            self.cancel_non_critical(out);
        }
        // upstream promotes outstanding blocks of the piece into the
        // conns' critical prefixes (`make_time_critical` per
        // downloader).
        let downloaders: Vec<Option<u64>> = self
            .picker
            .as_ref()
            .map(|p| p.get_downloaders(piece))
            .unwrap_or_default();
        // upstream promotes the downloader's own block index —
        // `downloaders[i]` is the peer holding block `i`.
        for (b, key) in downloaders.into_iter().enumerate() {
            let Some(key) = key else { continue };
            for c in self.peers.values_mut() {
                if c.peer_key() == Some(key) {
                    c.make_time_critical(bitdaemon_common::types::PieceBlock {
                        piece_index: piece,
                        block_index: b as i32,
                    });
                }
            }
        }
    }

    /// `reset_piece_deadline` → `remove_time_critical_piece` —
    /// a canceled deadline restores `low_priority` and posts a
    /// canceled `read_piece_alert` when `alert_when_available` was
    /// set (upstream posts `operation_canceled`).
    pub fn reset_piece_deadline(&mut self, piece: PieceIndex, out: &mut Vec<TorrentEffect>) {
        if let Some((_, alert)) = self.time_critical.remove(&piece) {
            if alert {
                out.push(TorrentEffect::ReadPieceFailed {
                    piece,
                    error: "operation canceled".into(),
                });
            }
            if let Some(p) = self.picker.as_mut() {
                p.set_piece_priority(piece, bitdaemon_common::types::DownloadPriority::LOW);
            }
        }
    }

    /// `peer_connection::second_tick` — a wire request outstanding
    /// longer than `request_timeout` (RTT mean + 4σ, capped by the
    /// setting) snubs the peer: queue target collapses to 1 and
    /// slow-start ends. Upstream keeps the outstanding requests
    /// (cancelling them causes re-pick churn and sawtooth rates);
    /// they're still refunded if the peer later disconnects.
    /// `peer_connection::second_tick` → `snub_peer` — a peer whose
    /// in-flight requests outlive `request_timeout` (or whose piece
    /// progress stalls past `piece_timeout`) gets snubbed: queue
    /// target collapses to 1, unsent requests return to the picker,
    /// and the last eligible in-flight block times out so another
    /// peer can pick it. Parole peers keep their pipeline.
    ///
    /// The `m_quota[download_channel] > 0` snub gate reads the conn's
    /// own bandwidth quota (`c.bw`) — a throttled peer can't be
    /// blamed for missing deadlines.
    ///
    /// The endgame branch mirrors `second_tick`'s 5 s retry: once a
    /// strict-endgame peer drains both queues it re-picks (a busy
    /// block may have freed) and re-stamps `m_last_request`.
    pub fn check_request_timeouts(
        &mut self,
        s: &crate::settings::SessionSettings,
        session_conns: i64,
        counters: &crate::counters::Counters,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
        alert_mask: crate::alerts::AlertCategory,
        out: &mut Vec<TorrentEffect>,
    ) {
        let now_ms = self.now_ms;
        if now_ms <= 0 {
            return;
        }
        let piece_timeout_ms = s.piece_timeout.as_millis() as i64;
        let inactivity_ms = s.inactivity_timeout.as_millis() as i64;
        // `t->is_upload_only()` — `is_finished() || upload_mode()`.
        let upload_only = self.finished || self.upload_mode;
        // `t->num_peers()` / `t->max_connections()` for the
        // `timed_out_no_interest` capacity check (-1 = unlimited,
        // which upstream's unsigned field also expresses).
        let num_peers = self.peers.len() as i32;
        let max_connections = self.max_connections;
        // ctx for the (usually vacuous) replacement pick inside
        // snub_peer — same settings-driven values as `drive_peer`.
        let block_size = self.block_size();
        let tctx = crate::request_blocks::TorrentRequestCtx {
            is_seed: self.is_seed(),
            upload_mode: self.upload_mode,
            valid_metadata: self.files.is_some(),
            files_checked: self.files_checked,
            graceful_pause: self.graceful_pause,
            time_critical_mode: !self.time_critical.is_empty(),
            num_peers: self.peers.len() as i32,
            whole_pieces_threshold: s.whole_pieces_threshold,
            strict_end_game_mode: s.strict_end_game_mode,
            piece_length: self.piece_length,
            block_size,
        };
        let opts = PickerOptions::RAREST_FIRST;
        let view = PipelineView {
            piece_length: self.piece_length,
            block_size,
            files: self.files.as_deref(),
            graceful_pause: self.graceful_pause,
            upload_mode: self.upload_mode,
            data_requests_blocked: self.checking,
            has_picker: self.picker.is_some(),
        };
        // `peer_connection::second_tick` head — the per-conn IP/TCP
        // overhead drain (`use_quota_overhead` on the conn's class
        // set AND the torrent's) runs before the disconnecting
        // early-out upstream.
        let rate_limit_overhead = s.rate_limit_ip_overhead;
        // `m_stat.download_ip_overhead()` at torrent scope — the
        // per-tick sum feeding `torrent::second_tick`'s
        // `overhead >= download_limit()` alert check.
        let mut tick_oh_down = 0i64;
        let mut tick_oh_up = 0i64;
        for (peer_id, c) in self.peers.iter_mut() {
            let _ = peer_id;
            // `trancieve_ip_packet` hits `m_stat` *unconditionally*
            // upstream — only the `use_quota_overhead` channel debit
            // and its alert are `rate_limit_ip_overhead`-gated.
            use std::sync::atomic::Ordering::Relaxed;
            let amount_down = c.bw.ip_overhead[crate::peer_conn::DOWNLOAD_CHANNEL].swap(0, Relaxed);
            let amount_up = c.bw.ip_overhead[crate::peer_conn::UPLOAD_CHANNEL].swap(0, Relaxed);
            if amount_down != 0 || amount_up != 0 {
                c.stat_down_ip += amount_down;
                c.stat_up_ip += amount_up;
                tick_oh_down += amount_down;
                tick_oh_up += amount_up;
                counters.add(crate::counters::Counter::RecvIpOverheadBytes, amount_down);
                counters.add(crate::counters::Counter::SentIpOverheadBytes, amount_up);
            }
            let mut warning = 0u8;
            if rate_limit_overhead && (amount_down != 0 || amount_up != 0) {
                warning |= crate::session::Session::quota_overhead(
                    pool,
                    &c.peer_classes,
                    amount_down,
                    amount_up,
                );
                warning |= crate::session::Session::quota_overhead(
                    pool,
                    &self.peer_classes,
                    amount_down,
                    amount_up,
                );
            }
            // upstream posts `performance_alert::{download,
            // upload}_limit_too_low` when a class throttle is lower
            // than the overhead it must absorb.
            if warning != 0 && (alert_mask.0 & crate::alerts::AlertCategory::PERFORMANCE.0) != 0 {
                for channel in 0..2 {
                    if (warning >> channel) & 1 == 0 {
                        continue;
                    }
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::Performance {
                        warning: if channel == crate::peer_conn::DOWNLOAD_CHANNEL {
                            "download_limit_too_low".into()
                        } else {
                            "upload_limit_too_low".into()
                        },
                    }));
                }
            }
            // `peer_connection::second_tick` early-outs a conn mid-
            // teardown — `m_disconnecting` → `disconnect()` + return
            // before any timeout/keepalive/snub eval.
            if c.disconnecting {
                continue;
            }
            // `peer_connection::second_tick` disconnect ladder,
            // evaluated in upstream order; each hit emits
            // `DisconnectPeer` and skips the rest of the tick for
            // this peer (upstream `disconnect(); return;`). The
            // `m_connecting` leg (`peer_connect_timeout`) lives in
            // the transport — a conn only registers once its socket
            // is up. `d` = `now - m_last_receive.get(m_connect)`.
            let d_ms = now_ms - c.last_receive_ms;
            // `timeout()` — i2p peers get 4× the peer_timeout.
            let i2p_mult: i64 = if c.i2p.is_some() { 4 } else { 1 };
            let peer_timeout_ms = s.peer_timeout.as_millis() as i64 * i2p_mult;
            // `connect_timeout` — `peer_connect_timeout` + 3 s per
            // prior failed dial (`m_peer_info->failcount`), +20 s on
            // i2p (SSL's +10 has no Rust-side socket type).
            let failcount = c.remote.map_or(0, |r| {
                self.peer_list
                    .peers()
                    .iter()
                    .position(|p| p.ip() == Some(r))
                    .map_or(0, |i| i64::from(self.peer_list.peers()[i].failcount))
            });
            let connect_budget_ms = s.peer_connect_timeout.as_millis() as i64
                + 3_000 * failcount
                + if c.i2p.is_some() { 20_000 } else { 0 };
            let disconnect_reason: Option<&'static str> = if c.connecting
                && d_ms > connect_budget_ms
            {
                // `errors::timed_out` → `connect_failed` — a conn
                // still marked connecting past its budget.
                Some("timed out: connect")
            } else if !c.connecting && d_ms > peer_timeout_ms {
                // `errors::timed_out_inactivity`
                Some("timed out: inactivity")
            } else if !c.connecting
                && !c.handshake_done
                && d_ms
                    > s.handshake_timeout.as_millis() as i64 * if c.i2p.is_some() { 4 } else { 1 }
            {
                // `errors::timed_out_no_handshake`
                Some("timed out: no handshake")
            } else if !c.connecting
                && c.upload_queue.is_empty()
                && !c.choked
                && c.peer_interested
                && upload_only
                && now_ms
                    - c.last_unchoke_ms
                        .max(c.last_incoming_request_ms)
                        .max(c.last_sent_payload_ms)
                    > 60_000
            {
                // `errors::timed_out_no_request` — an unchoked,
                // interested peer on an upload-only torrent that
                // has asked for nothing for >60 s.
                Some("timed out: no request")
            } else if !c.interesting
                && !c.peer_interested
                && now_ms - c.became_uninterested_ms > inactivity_ms
                && now_ms - c.became_uninteresting_ms > inactivity_ms
                && (session_conns >= i64::from(s.connections_limit) - 5
                    || (max_connections >= 0 && num_peers >= max_connections - 5))
            {
                // `errors::timed_out_no_interest` — mutual
                // disinterest while connection slots run short.
                Some("timed out: no interest")
            } else {
                None
            };
            if let Some(reason) = disconnect_reason {
                // timeouts run at default severity upstream —
                // `m_failed` stays clear, no failcount bump.
                out.push(TorrentEffect::DisconnectPeer {
                    peer: *peer_id,
                    reason,
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
                continue;
            }
            // `setup_send`'s upload `bw_disk` transition — evaluated
            // per tick so a conn that drains its buffer or gets its
            // reads back while idle still leaves/joins the gauge (the
            // alert leg only fires inside `fill_send_buffer`,
            // matching upstream's eval site). Upstream gates on
            // `m_quota[upload] > 0` too — throttled ≠ disk-stalled.
            let quota_up = c.bw.quota[crate::peer_conn::UPLOAD_CHANNEL]
                .load(std::sync::atomic::Ordering::Relaxed);
            let stalled = c.send_buffer == 0 && c.reading_bytes > 0 && quota_up > 0;
            if stalled && !c.up_disk_blocked {
                c.up_disk_blocked = true;
                counters.inc(crate::counters::Counter::NumPeersUpDisk);
            } else if !stalled && c.up_disk_blocked {
                c.up_disk_blocked = false;
                counters.add(crate::counters::Counter::NumPeersUpDisk, -1);
            }
            // `keep_alive` — a bare keepalive goes out when nothing
            // was written for `peer_timeout / 2` seconds (upstream
            // also skips when the upload channel is network-blocked;
            // our writer pump never leaves that flag set at tick
            // time).
            if !c.connecting
                && c.handshake_done
                && (now_ms - c.last_sent_ms) / 1000 >= peer_timeout_ms / 1000 / 2
            {
                c.last_sent_ms = now_ms;
                out.push(TorrentEffect::SendTo {
                    peer: *peer_id,
                    msg: PeerMessage::Keepalive,
                });
            }
            // `peer_connection::second_tick` — strict endgame with
            // both queues drained retries the pick every 5 s; a
            // busy block may have been released meanwhile. Runs
            // before the timeout checks (upstream order) and is not
            // quota-gated.
            if c.pipeline.endgame
                && c.interesting
                && c.pipeline.download_queue.is_empty()
                && c.pipeline.request_queue.is_empty()
                && now_ms - c.pipeline.last_request_ms >= 5_000
            {
                c.pipeline.last_request_ms = now_ms;
                if let Some(p) = self.picker.as_mut() {
                    let (send_buffer, rate) = (c.send_buffer, i64::from(c.rate_down_payload));
                    let mut pc_stats = crate::piece_picker::NullPickerStats;
                    let (invoked, _eg, reqs, _queued) = c.drive_requests(
                        p,
                        &tctx,
                        &view,
                        opts,
                        rate,
                        send_buffer,
                        now_ms,
                        counters,
                        &mut pc_stats,
                    );
                    // `second_tick` endgame retry — bump
                    // `end_game_piece_picks` on a successful pick.
                    if invoked {
                        counters.inc(crate::counters::Counter::EndGamePiecePicks);
                    }
                    for req in reqs {
                        out.push(TorrentEffect::SendTo {
                            peer: *peer_id,
                            msg: PeerMessage::Request(req),
                        });
                    }
                }
            }
            // `m_quota[download_channel] > 0` — upstream skips both
            // timeout checks for a conn the bandwidth manager gave
            // no download budget to this tick (a throttled peer
            // can't be blamed for missing deadlines).
            if c.bw.quota[crate::peer_conn::DOWNLOAD_CHANNEL]
                .load(std::sync::atomic::Ordering::Relaxed)
                <= 0
                || c.pipeline.download_queue.is_empty()
            {
                continue;
            }
            // `second_tick` — the *request* timeout only applies
            // while a socket read is in flight (`reading_socket` =
            // `m_channel_state[download_channel] & bw_network`): a
            // conn blocked on disk (`bw_disk`) or on quota
            // (`bw_limit`) isn't reading and can't be blamed for
            // the silence. The *piece* timeout leg carries no such
            // gate upstream.
            let reading_socket = c.bw.channel_state[crate::peer_conn::DOWNLOAD_CHANNEL]
                .load(std::sync::atomic::Ordering::Relaxed)
                & crate::peer_info::BandwidthState::NETWORK.0
                != 0;
            let request_timed_out = reading_socket
                && c.pipeline.requested_ms != 0
                && now_ms - c.pipeline.requested_ms > c.pipeline.request_timeout() * 1000;
            let piece_timed_out = now_ms - c.pipeline.last_piece_ms > piece_timeout_ms;
            if !request_timed_out && !piece_timed_out {
                continue;
            }
            // `snub_peer` — the flag flips once (alert included);
            // the collapse to a single outstanding request happens
            // every pass.
            if !c.pipeline.snubbed {
                c.pipeline.snubbed = true;
                c.pipeline.slow_start = false;
                out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerSnubbed {
                    ih: self.info_hash,
                    remote: c.remote.map_or_else(String::new, |ep| ep.to_string()),
                }));
            }
            c.pipeline.desired_queue_size = 1;
            // upstream keeps a parole peer's pipeline untouched.
            if c.on_parole {
                continue;
            }
            let Some(p) = self.picker.as_mut() else {
                continue;
            };
            // wipe unsent requests — upstream pops back-to-front.
            c.pipeline.queued_time_critical = 0;
            while let Some(pb) = c.pipeline.request_queue.pop() {
                p.abort_download(pb.block, c.peer_key());
            }
            // time out the *last* eligible in-flight block — but
            // only when its piece has no free blocks left for other
            // peers; while free blocks remain there's nothing to
            // unblock and the request stands.
            if let Some(i) = c
                .pipeline
                .download_queue
                .iter()
                .rposition(|b| !b.timed_out && !b.not_wanted)
            {
                let r = c.pipeline.download_queue[i].block;
                let info = p.piece_info(r.piece_index);
                let free = p.blocks_in_piece(r.piece_index)
                    - i32::from(info.finished)
                    - i32::from(info.writing)
                    - i32::from(info.requested);
                if free <= 0 {
                    out.push(TorrentEffect::Alert(crate::alerts::Alert::BlockTimeout {
                        ih: self.info_hash,
                        remote: c.remote.map_or_else(String::new, |ep| ep.to_string()),
                        piece: r.piece_index,
                        block: r.block_index as u32,
                    }));
                    // pick a replacement *before* freeing the stalled
                    // block so the picker can't just re-pick it.
                    // Note: `desired_queue_size()` still reports 1
                    // while snubbed, so with an in-flight request
                    // this pick typically finds nothing — verbatim
                    // upstream behavior.
                    c.pipeline.desired_queue_size = 2;
                    let key = c.peer_key();
                    let outstanding = c.outstanding();
                    let disconnecting = c.disconnecting;
                    let rate = i64::from(c.rate_down_payload);
                    let mut ctx = c.request_ctx(false, disconnecting, rate);
                    ctx.outstanding = &outstanding;
                    ctx.picker_options = opts;
                    let mut picked: Vec<(PieceBlock, bool)> = Vec::new();
                    let mut pc_stats = crate::piece_picker::NullPickerStats;
                    let (invoked, eg) = crate::request_blocks::request_a_block_out(
                        p,
                        &tctx,
                        &ctx,
                        &mut pc_stats,
                        |b, busy| {
                            picked.push((b, busy));
                            true
                        },
                    );
                    // upstream `snub_peer`: an invoked replacement
                    // pick bumps `snubbed_piece_picks` (even when it
                    // finds nothing).
                    if invoked {
                        counters.inc(crate::counters::Counter::SnubbedPiecePicks);
                    }
                    if let Some(e) = eg {
                        c.set_endgame(e, counters);
                    }
                    for (b, busy) in picked {
                        if p.mark_as_downloading(b, key, opts) && !c.add_request(b, busy, false) {
                            p.abort_download(b, key);
                        }
                    }
                    c.pipeline.desired_queue_size = 1;
                    c.pipeline.download_queue[i].timed_out = true;
                    p.abort_download(r, key);
                }
            }
            // upstream ends `snub_peer` with send_block_requests —
            // drains whatever the queues still allow and wires any
            // replacement that was picked.
            let disconnecting = c.disconnecting;
            let send_buffer = c.send_buffer;
            let now = c.now_ms;
            let key = c.peer_key();
            let reqs =
                c.pipeline
                    .send_block_requests(&view, send_buffer, now, disconnecting, |b| {
                        if p.is_downloaded(b) {
                            p.abort_download(b, key);
                            true
                        } else {
                            false
                        }
                    });
            for req in reqs {
                out.push(TorrentEffect::SendTo {
                    peer: *peer_id,
                    msg: PeerMessage::Request(req),
                });
            }
        }
        // `torrent::second_tick` — the *torrent-level* overhead check
        // (distinct from the per-conn class debit above): this tick's
        // torrent stat overhead vs the torrent's own rate limits.
        if rate_limit_overhead && (alert_mask.0 & crate::alerts::AlertCategory::PERFORMANCE.0) != 0
        {
            let down_limit = self.download_limit(pool);
            let up_limit = self.upload_limit(pool);
            if down_limit > 0 && tick_oh_down >= down_limit {
                out.push(TorrentEffect::Alert(crate::alerts::Alert::Performance {
                    warning: "download_limit_too_low".into(),
                }));
            }
            if up_limit > 0 && tick_oh_up >= up_limit {
                out.push(TorrentEffect::Alert(crate::alerts::Alert::Performance {
                    warning: "upload_limit_too_low".into(),
                }));
            }
        }
    }

    /// `cancel_non_critical` — first time-critical piece cancels
    /// every outstanding request not on a critical piece via
    /// `cancel_request(block, force)`: unsent request-queue entries
    /// drop silently (the peer never saw them — no wire `Cancel`),
    /// in-flight blocks are *kept* and marked `not_wanted` so a
    /// racing payload still lands as `piece_cancelled` waste, and
    /// only those get a wire `Cancel`. The picker is told via
    /// `abort_download` so it can re-pick the block for a fast peer.
    fn cancel_non_critical(&mut self, out: &mut Vec<TorrentEffect>) {
        if self.picker.is_none() {
            return;
        }
        let critical: std::collections::BTreeSet<PieceIndex> =
            self.time_critical.keys().copied().collect();
        let block_size = i64::from(self.block_size());
        let piece_len = self.piece_length;
        let files = self.files.clone();
        for (id, c) in self.peers.iter_mut() {
            let mut wire = Vec::new();
            let mut aborted = Vec::new();
            // in-flight first (upstream iterates `m_download_queue`
            // then `m_request_queue`): entries already not_wanted or
            // timed_out were cancelled/timed-out once — skip them.
            for b in &mut c.pipeline.download_queue {
                if critical.contains(&b.block.piece_index) || b.not_wanted || b.timed_out {
                    continue;
                }
                b.not_wanted = true;
                aborted.push(b.block);
                let start = i64::from(b.block.block_index) * block_size;
                let psize = files
                    .as_ref()
                    .map_or(piece_len, |f| i64::from(f.piece_size(b.block.piece_index)));
                let len = (psize - start).min(block_size);
                // upstream sends the cancel only when the peer's
                // outstanding bytes still cover the request.
                if c.pipeline.outstanding_bytes >= len {
                    wire.push((b.block, start, len));
                }
            }
            // unsent requests never hit the wire — erase + shrink
            // the critical prefix, no `Cancel` message.
            let mut i = 0;
            while i < c.pipeline.request_queue.len() {
                if critical.contains(&c.pipeline.request_queue[i].block.piece_index) {
                    i += 1;
                    continue;
                }
                let b = c.pipeline.request_queue.remove(i).block;
                if i < c.pipeline.queued_time_critical as usize {
                    c.pipeline.queued_time_critical -= 1;
                }
                aborted.push(b);
            }
            let key = c.peer_key();
            for b in aborted {
                if let Some(p) = self.picker.as_mut() {
                    p.abort_download(b, key);
                }
            }
            for (b, start, len) in wire {
                out.push(TorrentEffect::SendTo {
                    peer: *id,
                    msg: PeerMessage::Cancel(PeerRequest {
                        piece: b.piece_index,
                        start: start as i32,
                        length: len as i32,
                    }),
                });
            }
        }
    }

    /// `torrent::cancel_block` — cancel this block on every
    /// connection (`peer_connection::cancel_request`): unsent
    /// request-queue entries drop silently, in-flight blocks get a
    /// wire `Cancel`; the picker is told via `abort_download`.
    /// No-op when the block isn't requested anymore.
    pub fn cancel_block(&mut self, block: PieceBlock, out: &mut Vec<TorrentEffect>) {
        if self.picker.as_ref().is_some_and(|p| !p.is_requested(block)) {
            return;
        }
        let block_size = i64::from(self.block_size());
        for (id, c) in self.peers.iter_mut() {
            let in_flight = c.pipeline.download_queue.iter().any(|b| b.block == block);
            if !c.cancel_request(block) {
                continue;
            }
            if let Some(p) = self.picker.as_mut() {
                p.abort_download(block, c.peer_key());
            }
            if in_flight {
                let start = i64::from(block.block_index) * block_size;
                let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                    i64::from(f.piece_size(block.piece_index))
                });
                out.push(TorrentEffect::SendTo {
                    peer: *id,
                    msg: PeerMessage::Cancel(PeerRequest {
                        piece: block.piece_index,
                        start: start as i32,
                        length: (psize - start).min(block_size) as i32,
                    }),
                });
            }
        }
    }

    /// `torrent::on_piece_fail_sync` — runs after the `clear_piece`
    /// fence: restore the piece (as if no block was ever written)
    /// and re-drive peers that became interesting again now that we
    /// lack the piece.
    pub fn on_piece_fail_sync(
        &mut self,
        piece: PieceIndex,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if self.aborted {
            return;
        }
        if let Some(p) = self.picker.as_mut() {
            p.restore_piece(piece, &[]);
        }
        self.state_updated();
        // `update_interest` may disconnect; only peers that were not
        // already interesting gain interest from our losing a piece.
        let targets: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| !c.interesting)
            .map(|(id, _)| *id)
            .collect();
        for id in targets {
            self.update_interest(id, counters, out);
            // upstream `request_a_block` return →
            // `hash_fail_piece_picks`.
            if self.drive_peer(id, s, counters, out) {
                counters.add(crate::counters::Counter::HashFailPiecePicks, 1);
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
        if !self.is_paused() && act < 30 * 60 {
            ret |= RECENTLY_STARTED;
        }
        // scrape data wins, like upstream `m_complete`/`m_incomplete`
        // (-1 = none); else fall back to the swarm list.
        let self_seed = i64::from(self.is_seed() && !self.is_paused());
        let (seeds, downloaders) = (
            if self.complete >= 0 {
                Some((self.complete - self_seed as i32).max(0) as i64)
            } else {
                None
            },
            if self.incomplete >= 0 {
                Some(i64::from(self.incomplete))
            } else {
                None
            },
        );
        let seeds = seeds.unwrap_or_else(|| i64::from(self.peer_list.num_seeds()));
        let downloaders = downloaders.unwrap_or_else(|| {
            self.peer_list.num_peers() as i64 - i64::from(self.peer_list.num_seeds())
        });
        if seeds == 0 {
            ret |= NO_SEEDS;
            ret |= (downloaders as i32) & PRIO_MASK;
        } else {
            ret |= (((1 + downloaders) * scale / seeds) as i32) & PRIO_MASK;
        }
        ret
    }

    /// session payload totals — `m_stat.total_payload_download()`:
    /// live conns plus the folded totals of conns already gone
    /// (monotonic across disconnects, unlike a live-only sum).
    /// `st->total_download` adds the protocol channel on top —
    /// `status()` folds `protocol_downloaded` separately.
    pub fn total_download(&self) -> i64 {
        self.payload_downloaded
            + self.web_seed_payload_downloaded
            + self.peers.values().map(|c| c.stat_down_total).sum::<i64>()
    }

    /// ditto upload.
    pub fn total_upload(&self) -> i64 {
        self.payload_uploaded + self.peers.values().map(|c| c.stat_up_total).sum::<i64>()
    }

    /// `state()` — the coarse lifecycle state (`status().state`
    /// without building the whole snapshot; the session diffs it
    /// for `state_changed_alert`).
    #[must_use]
    pub fn state(&self) -> crate::status::TorrentState {
        use crate::status::TorrentState;
        if self.checking {
            if self.checking_resume {
                TorrentState::CheckingResumeData
            } else {
                TorrentState::CheckingFiles
            }
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
        flags: crate::status::StatusFlags,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
    ) -> crate::status::TorrentStatus {
        use crate::status::{StatusFlags, TorrentStatus};
        let state = self.state();
        // `bytes_done` — piece-granular byte accounting over the
        // picker's want/have_want/have/all_pieces piece counts
        // (`calc_bytes` subtracts the last piece's short tail and
        // v2 pad bytes). `m_size_on_disk` (payload minus pad files)
        // is the pre-metadata/seed-mode baseline.
        // `m_size_on_disk` — payload size minus pad files. Without
        // `files` (magnet pre-metadata) `num_pieces` is 0 anyway, so
        // the `num_pieces * piece_length` fallback only serves
        // synthetic picker-only states upstream can't represent.
        let size_on_disk = self.files.as_ref().map_or_else(
            || i64::from(self.num_pieces) * self.piece_length,
            |f| f.size_on_disk(),
        );
        let block_bytes = i64::from(self.block_size());
        let calc_bytes = |pc: crate::piece_picker::PieceCount| {
            let last_tail = if pc.last_piece {
                self.files.as_ref().map_or(0, |f| {
                    self.piece_length - i64::from(f.piece_size(PieceIndex(self.num_pieces - 1)))
                })
            } else {
                0
            };
            i64::from(pc.num_pieces) * self.piece_length - last_tail - pc.pad_bytes
        };
        let (mut total_done, mut total_wanted_done) = (0i64, 0i64);
        let mut total_wanted = size_on_disk;
        let mut total = size_on_disk;
        if self.files.is_some() {
            if self.seed_mode || self.is_seed() {
                total_done = size_on_disk;
                total_wanted_done = size_on_disk;
            } else if let Some(p) = &self.picker {
                total_wanted = size_on_disk.min(calc_bytes(p.want()));
                // upstream clamps `total_wanted_done` by
                // `m_file_progress.total_on_disk()` — a per-file
                // on-disk byte counter we don't keep; `size_on_disk`
                // is the same upper bound.
                total_wanted_done = size_on_disk.min(calc_bytes(p.have_want()));
                total_done = calc_bytes(p.have());
                total = calc_bytes(p.all_pieces());
                if flags.contains(StatusFlags::QUERY_ACCURATE_DOWNLOAD_COUNTERS) {
                    // count finished/writing blocks of in-flight
                    // pieces too (upstream `get_download_queue`
                    // pass; pad blocks can't arrive over the wire).
                    for dp in p.get_download_queue() {
                        if p.have_piece(dp.index) {
                            continue;
                        }
                        let add = (i64::from(dp.finished + dp.writing)
                            - i64::from(p.pad_bytes_in_piece(dp.index)) / block_bytes)
                            * block_bytes;
                        total_done += add;
                        if dp.index.0 >= 0
                            && p.piece_priority(dp.index)
                                > bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD
                        {
                            total_wanted_done += add;
                        }
                    }
                }
            }
        }
        // `update_scrape_state` cache — upstream reports `m_complete`/
        // `m_incomplete`/`m_downloaded` (-1 until a scrape lands).
        let (num_complete, num_incomplete, downloaded) = (
            i64::from(self.complete),
            i64::from(self.incomplete),
            i64::from(self.downloaded),
        );
        let last_scrape = self
            .trackers
            .trackers
            .iter()
            .filter_map(|e| e.scrape_time)
            .map(|t| t.elapsed().as_secs() as i64)
            .min()
            .unwrap_or(0);
        let total_download = self.total_download();
        let total_upload = self.total_upload();
        // `st->total_*` = payload + *protocol* channels only (upstream
        // does not fold ip-overhead into the totals).
        let proto_down: i64 = self.protocol_downloaded
            + self
                .peers
                .values()
                .map(|c| c.stat_down_protocol)
                .sum::<i64>();
        let proto_up: i64 =
            self.protocol_uploaded + self.peers.values().map(|c| c.stat_up_protocol).sum::<i64>();
        // `torrent::status` progress rules: the checking pass
        // reports pieces-checked progress (upstream `m_progress_ppm`
        // = num_checked/num_pieces — the fastresume-verify window
        // reports the same 0 it did before the check), `total_wanted
        // == 0` reads 100 %, otherwise `wanted_done / total_wanted`.
        // Metadata-download progress stays 0 like upstream (its
        // `m_progress_ppm` isn't updated per piece).
        let progress_ppm = if self.files.is_none() && self.magnet {
            // `!valid_metadata()` — upstream early-returns
            // `downloading_metadata` with `m_progress_ppm`
            // untouched (0 until a check ran).
            0
        } else if self.checking && !self.checking_resume && self.num_pieces > 0 {
            ((i64::from(self.num_pieces - self.checking_pieces) * 1_000_000)
                / i64::from(self.num_pieces)) as i32
        } else if total_wanted == 0 {
            1_000_000
        } else {
            (total_wanted_done * 1_000_000 / total_wanted) as i32
        };
        let progress = f64::from(progress_ppm) / 1e6;
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
        // `query_distributed_copies` gates the (expensive) swarm
        // availability estimate; the upstream default is -1s.
        let (d_full, d_frac) = if flags.contains(StatusFlags::QUERY_DISTRIBUTED_COPIES) {
            self.picker
                .as_ref()
                .map_or((-1, -1), |p| p.distributed_copies())
        } else {
            (-1, -1)
        };
        // upstream `active_time(now)` — live elapsed + the
        // resume-restored `set_total` offset.
        let active_duration =
            (now.duration_since(self.started) + self.active_offset).as_secs() as i64;
        let finished_duration = self
            .finished_at
            .map_or(self.finished_offset.as_secs() as i64, |t| {
                (now.duration_since(t) + self.finished_offset).as_secs() as i64
            });
        let seeding_duration = self
            .seeding_since
            .map_or(self.seeding_offset.as_secs() as i64, |t| {
                (now.duration_since(t) + self.seeding_offset).as_secs() as i64
            });
        // `m_swarm_last_seen_complete` — our own `seen_complete`
        // maxed over conns' BEP-21 `complete_ago` reports.
        let last_seen_complete = self
            .peers
            .values()
            .map(|c| c.last_seen_complete)
            .fold(self.last_seen_complete, i64::max);
        TorrentStatus {
            info_hash: self.info_hash,
            info_hashes: self.ti.as_ref().map_or_else(
                || bitdaemon_common::info_hash::InfoHash::from_v1(self.info_hash),
                |ti| *ti.info_hashes(),
            ),
            state,
            paused: self.is_paused(),
            auto_managed: self.auto_managed,
            // upstream fills `is_seeding = is_seed()` — the
            // have-all-or-seed-mode predicate, not just `seeding`.
            is_seed: self.is_seed(),
            is_seeding: self.is_seed(),
            finished: self.finished,
            has_metadata: self.ti.is_some() || self.picker.is_some(),
            has_incoming: self.peers.values().any(|c| !c.outgoing),
            moving_storage: self.moving_storage,
            announcing_to_trackers: self.outstanding_announces > 0,
            announcing_to_dht: self.dht_announcing,
            announcing_to_lsd: self.lsd_announcing,
            need_save_resume: self.need_save_resume,
            ip_filter_applies: self.apply_ip_filter,
            flags: self.flags(),
            sequential_download: self.sequential,
            seed_mode: self.seed_mode,
            upload_mode: self.upload_mode,
            share_mode: self.share_mode,
            super_seeding: self.super_seeding,
            stop_when_ready: self.stop_when_ready,
            storage_mode: self.storage_mode,
            // `last_working_url()` — the most recent tracker to
            // answer; empty until an announce succeeds.
            current_tracker: self.trackers.last_working_url.clone(),
            name: if flags.contains(StatusFlags::QUERY_NAME) {
                self.name.clone()
            } else {
                String::new()
            },
            save_path: if flags.contains(StatusFlags::QUERY_SAVE_PATH) {
                self.save_path.clone()
            } else {
                String::new()
            },
            error: self.last_error.clone(),
            error_file: self.error_file,
            num_peers: self.peers.len(),
            num_seeds: self.num_seeds,
            num_candidates: self.peer_list.num_peers(),
            num_complete,
            num_incomplete,
            downloaded,
            total_done,
            total_wanted,
            total_upload: total_upload + proto_up,
            total_download: total_download + proto_down,
            total_payload_download: total_download,
            total_payload_upload: total_upload,
            total_failed_bytes: self.failed_bytes,
            total_redundant_bytes: self.redundant_bytes,
            total_wanted_done,
            all_time_upload: self.all_time_upload + total_upload,
            all_time_download: self.all_time_download + total_download,
            last_upload: epoch(self.last_payload_up),
            last_download: epoch(self.last_payload_down),
            active_duration,
            finished_duration,
            seeding_duration,
            active_time: active_duration,
            finished_time: finished_duration,
            seeding_time: seeding_duration,
            // `time_since_*` — -1 until the first payload transfer.
            time_since_upload: self
                .last_payload_up
                .map_or(-1, |t| now.duration_since(t).as_secs() as i64),
            time_since_download: self
                .last_payload_down
                .map_or(-1, |t| now.duration_since(t).as_secs() as i64),
            download_rate: self.down_rate,
            upload_rate: self.up_rate,
            download_payload_rate: self.down_payload_rate,
            upload_payload_rate: self.up_payload_rate,
            list_seeds: self.peer_list.num_seeds(),
            list_peers: self.peer_list.num_peers() as i32 - self.peer_list.num_seeds(),
            connect_candidates: self.peer_list.num_connect_candidates(),
            // upstream `num_uploads` skips `ignore_unchoke_slots`
            // conns — they don't occupy upload slots (conn classes
            // or the torrent's own).
            num_uploads: {
                let t_ign = self.ignores_unchoke_slots(pool);
                self.peers
                    .values()
                    .filter(|c| !c.choked && !c.ignores_unchoke_slots(pool) && !t_ign)
                    .count() as i32
            },
            num_connections: self.peers.len() as i32,
            uploads_limit: self.max_uploads,
            connections_limit: self.max_connections,
            upload_limit: self
                .upload_limit(pool)
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32,
            download_limit: self
                .download_limit(pool)
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
            down_bandwidth_queue: {
                // block indices are in `block_size()` units —
                // `min(piece_length, 0x4000)`, not a fixed 16 KiB;
                // tail of last piece is shorter (`files.piece_size`
                // exact).
                let bs = i64::from(self.block_size());
                self.peers
                    .values()
                    .map(|c| {
                        c.pipeline
                            .request_queue
                            .iter()
                            .map(|b| {
                                let start = i64::from(b.block.block_index) * bs;
                                let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                                    i64::from(f.piece_size(b.block.piece_index))
                                });
                                (psize - start).clamp(0, bs)
                            })
                            .sum::<i64>()
                    })
                    .sum::<i64>() as i32
            },
            seed_rank: self.seed_rank(s),
            queue_position: queue_pos,
            distributed_full_copies: d_full,
            distributed_fraction: d_frac,
            progress,
            progress_ppm,
            // `query_pieces` gates the have-bitfield copy.
            pieces: if flags.contains(StatusFlags::QUERY_PIECES) {
                self.have.clone()
            } else {
                Bitfield::default()
            },
            num_pieces: self.num_pieces,
            piece_length: self.piece_length,
            // `st->block_size` — 0 until metadata lands, else
            // `min(piece_length, 0x4000)` (`t->block_size()`).
            block_size: if self.ti.is_some() {
                self.block_size()
            } else {
                0
            },
            distributed_copies: f64::from(d_full) + f64::from(d_frac) / 1000.0,
            total,
            added_time: self
                .added_time
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64),
            last_scrape,
            next_announce: epoch(self.trackers.next_announce_time()),
            completed_time: epoch(self.finished_at),
            last_seen_complete,
            torrent_file: if flags.contains(StatusFlags::QUERY_TORRENT_FILE) {
                self.ti.clone()
            } else {
                None
            },
            // `verified_pieces` — the seed-mode `m_verified` set.
            verified_pieces: if flags.contains(StatusFlags::QUERY_VERIFIED_PIECES) {
                self.seed_verified.clone()
            } else {
                Bitfield::default()
            },
            renamed_files: if flags.contains(StatusFlags::QUERY_RENAMED_FILES) {
                self.renamed_files.clone()
            } else {
                std::collections::BTreeMap::new()
            },
        }
    }

    /// `stat::second_tick` — refresh `down_rate`/`up_rate` from the
    /// per-peer payload counters (upstream uses `stat` channel
    /// sliding-window rates; a 1-tick delta is the same signal).
    pub fn update_rates(&mut self, now: std::time::Instant, out: &mut Vec<TorrentEffect>) {
        // upstream `second_tick` marks `state_updated()` whenever the
        // low-pass rate is still nonzero — checked before the stat
        // tick (pre-fade values), so a subscriber sees the last
        // active update.
        if self.up_rate > 0 || self.down_rate > 0 {
            self.state_updated();
        }
        // advance the torrent's monotonic ms clock — peers use it
        // for `m_requested`/RTT timestamps (upstream `time_now()`).
        if let Some(prev) = self.last_rates_instant.replace(now) {
            self.now_ms += now.saturating_duration_since(prev).as_millis() as i64;
        }
        let now_ms = self.now_ms;
        let block_size = self.block_size();
        for c in self.peers.values_mut() {
            c.now_ms = now_ms;
        }
        // Rate from per-peer deltas, not lifetime totals: a peer
        // that disconnected between ticks must contribute 0 to this
        // tick, not a negative delta (its total vanishes from the
        // sum → `down - prev` < 0 → bogus negative rate).
        // web-seed payload isn't carried by a conn — fold its
        // folded accumulator's delta into both legs (upstream's
        // `web_peer_connection::received_bytes` feeds the same
        // `m_stat` channels unless `report_web_seed_downloads`
        // turned stats off).
        let web_down = self.web_seed_payload_downloaded - self.last_web_seed_down;
        self.last_web_seed_down = self.web_seed_payload_downloaded;
        let down: i64 = self
            .peers
            .values()
            .map(|c| c.stat_down_total - c.last_down_total)
            .sum::<i64>()
            + web_down;
        let up: i64 = self
            .peers
            .values()
            .map(|c| c.stat_up_total - c.last_up_total)
            .sum();
        // `m_stat` all-channel deltas — payload + protocol +
        // ip-overhead (upstream `m_stat.download_rate()` feeds
        // `st.download_rate`, `want_tick`'s rate leg, `is_inactive`).
        let down_wire: i64 = self
            .peers
            .values()
            .map(|c| c.stat_down_total + c.stat_down_protocol + c.stat_down_ip - c.last_down_wire)
            .sum::<i64>()
            + web_down;
        let up_wire: i64 = self
            .peers
            .values()
            .map(|c| c.stat_up_total + c.stat_up_protocol + c.stat_up_ip - c.last_up_wire)
            .sum();
        // `set_need_save_resume(if_counters_changed)` — upstream
        // marks the resume dirty each tick the stat counters moved
        // (`total_uploaded`/`total_downloaded` are resume fields).
        if down + up > 0 {
            self.need_save_resume = true;
        }
        if let Some(prev_t) = self.stat_sample {
            let dt = (now - prev_t).as_secs_f64();
            if dt > 0.0 {
                let current_torrent_down = (down_wire as f64 / dt) as i64;
                let current_torrent_up = (up_wire as f64 / dt) as i64;
                let current_torrent_down_p = (down as f64 / dt) as i64;
                let current_torrent_up_p = (up as f64 / dt) as i64;
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
                self.down_payload_rate = if self.down_payload_rate == 0 {
                    current_torrent_down_p
                } else {
                    (self.down_payload_rate * 4 + current_torrent_down_p) / 5
                };
                self.up_payload_rate = if self.up_payload_rate == 0 {
                    current_torrent_up_p
                } else {
                    (self.up_payload_rate * 4 + current_torrent_up_p) / 5
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
                    // all-channel rate (`stat::download_rate()` —
                    // upstream's `up_speed`/`m_download_rate`).
                    let wire_down_delta = c.stat_down_total + c.stat_down_protocol + c.stat_down_ip
                        - c.last_down_wire;
                    let wire_up_delta =
                        c.stat_up_total + c.stat_up_protocol + c.stat_up_ip - c.last_up_wire;
                    let current_down_w =
                        (wire_down_delta as f64 / dt).clamp(0.0, f64::from(i32::MAX)) as i32;
                    let current_up_w =
                        (wire_up_delta as f64 / dt).clamp(0.0, f64::from(i32::MAX)) as i32;
                    c.rate_down_speed = if c.rate_down_speed == 0 {
                        current_down_w
                    } else {
                        ((i64::from(c.rate_down_speed) * 4 + i64::from(current_down_w)) / 5) as i32
                    };
                    c.rate_up_speed = if c.rate_up_speed == 0 {
                        current_up_w
                    } else {
                        ((i64::from(c.rate_up_speed) * 4 + i64::from(current_up_w)) / 5) as i32
                    };
                    // payload-channel rate (`stat::*_payload_rate()`).
                    c.rate_down_payload = if c.rate_down_payload == 0 {
                        current_down
                    } else {
                        ((i64::from(c.rate_down_payload) * 4 + i64::from(current_down)) / 5) as i32
                    };
                    c.rate_up_payload = if c.rate_up_payload == 0 {
                        current_up
                    } else {
                        ((i64::from(c.rate_up_payload) * 4 + i64::from(current_up)) / 5) as i32
                    };
                    // upstream `second_tick` slow-start exit: the
                    // rate stopped growing (prev + 5 KiB slack >=
                    // current) → hand over to rate-based AIMD. No
                    // minimum pipeline depth — upstream has none.
                    if c.pipeline.slow_start
                        && !c.peer_choked
                        && c.down_delta_prev > 0
                        && c.down_delta_prev + 5000 >= down_delta
                    {
                        c.pipeline.slow_start = false;
                    }
                    c.down_delta_prev = down_delta;
                    // `m_upload_rate_peak`/`m_download_rate_peak` —
                    // second_tick records the best payload rate seen
                    // (`peer_info` reads these).
                    if c.rate_up_payload > c.upload_rate_peak {
                        c.upload_rate_peak = c.rate_up_payload;
                    }
                    if c.rate_down_payload > c.download_rate_peak {
                        c.download_rate_peak = c.rate_down_payload;
                    }
                    // `peer_connection::second_tick` — AIMD: the
                    // request queue target follows the measured
                    // download rate (queue_time × rate / block).
                    // Upstream's `m_download_rate` = all-channel rate.
                    c.pipeline
                        .update_desired_queue_size(i64::from(c.rate_down_speed), block_size);
                    // `performance_alert::
                    // outstanding_request_limit_reached` — the
                    // pipeline target saturating `reqq` signals the
                    // peer can't drain requests fast enough.
                    if i64::from(c.pipeline.desired_queue_size())
                        >= i64::from(c.pipeline.max_out_request_queue)
                    {
                        out.push(TorrentEffect::Alert(crate::alerts::Alert::Performance {
                            warning: "outstanding_request_limit_reached".into(),
                        }));
                    }
                }
            }
        }
        for c in self.peers.values_mut() {
            c.last_down_total = c.stat_down_total;
            c.last_up_total = c.stat_up_total;
            c.last_down_wire = c.stat_down_total + c.stat_down_protocol + c.stat_down_ip;
            c.last_up_wire = c.stat_up_total + c.stat_up_protocol + c.stat_up_ip;
        }
        self.stat_sample = Some(now);
    }

    /// `want_more_peers` — downstream connect decisions.
    #[must_use]
    /// upstream `torrent::want_peers` — true for downloading torrents
    /// *and* seeds (upstream `want_peers_finished`: a seed keeps
    /// dialing to serve upload and feed PEX). The full predicate:
    /// enable-flag + paused/queued/graceful/checking gates +
    /// per-torrent conn cap + swarm candidates +
    /// `seeding_outgoing_connections` + at least one outgoing
    /// transport.
    pub fn want_peers(&self, s: &crate::settings::SessionSettings) -> bool {
        let conn_ok = self.max_connections < 0 || (self.peers.len() as i32) < self.max_connections;
        let checking = self.checking && self.valid_metadata();
        self.want_peers
            && !self.is_paused()
            && !self.queued
            && !self.graceful_pause
            && !checking
            && self.peer_list.num_connect_candidates() > 0
            // upstream gates on `m_state == seeding || finished`
            // (wanted-pieces done), not `is_seed()`.
            && (s.seeding_outgoing_connections || !(self.finished || self.seeding))
            && (s.enable_outgoing_tcp || s.enable_outgoing_utp)
            && conn_ok
    }

    // ---- remaining `torrent.cpp` surface (upstream method names kept
    // for ledger greppability)

    /// `torrent::abort` — stop everything; the torrent is being
    /// removed (`m_abort`). Releases the peer class (upstream frees
    /// `m_peer_class` here) and tears down every conn.
    pub fn abort(
        &mut self,
        counters: &crate::counters::Counters,
        pool: &mut bitdaemon_net::peer_class::PeerClassPool,
        out: &mut Vec<TorrentEffect>,
    ) {
        self.aborted = true;
        self.release_peer_class(pool);
        self.disconnect_all(counters, pool, out);
    }

    /// `is_aborted`.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }

    /// `torrent::disconnect_all` — tear down every peer connection.
    pub fn disconnect_all(
        &mut self,
        counters: &crate::counters::Counters,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
        out: &mut Vec<TorrentEffect>,
    ) {
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.remove_peer(id, counters, pool, out);
        }
        // `on_clear_peers` — the whole CRC ledger dies with the
        // peer set (upstream `smart_ban_notify_clear_peers`).
        self.smart_ban.clear();
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

    /// `is_paused` — the effective pause (`m_paused ||
    /// m_session_paused`): upstream's torrent is quiet when either
    /// the user or the session paused it.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.paused || self.session_paused
    }

    /// `want_tick` — upstream's `second_tick` list predicate:
    /// any live conn keeps the torrent ticking (idle peers still
    /// need their disconnect ladder), unfinished torrents with
    /// checked files tick for web seeds, nonzero low-pass rates
    /// keep it alive, and an unpaused, non-inactive torrent ticks
    /// until it can prove it doesn't need to. (Our session tick
    /// still refreshes rates for every torrent — recovery is a
    /// superset of upstream's state-list churn wakeups.)
    #[must_use]
    pub fn want_tick(&self) -> bool {
        if self.aborted {
            return false;
        }
        if !self.peers.is_empty() {
            return true;
        }
        if !self.finished && !self.web_seeds.is_empty() && self.files_checked {
            return true;
        }
        if self.down_rate > 0 || self.up_rate > 0 {
            return true;
        }
        // upstream tests `m_paused`, not `is_paused()` — a
        // session-paused torrent stays on the tick list (its
        // `second_tick` early-return gates the heavy work instead).
        !self.paused && !self.inactive
    }

    /// `is_inactive` — the debounced verdict, honored only under
    /// `dont_count_slow_torrents` (upstream `torrent::is_inactive`).
    #[must_use]
    pub fn is_inactive(&self, s: &crate::settings::SessionSettings) -> bool {
        s.dont_count_slow_torrents && self.inactive
    }

    /// `is_inactive_internal` — finished torrents compare the
    /// upload low-pass to `inactive_up_rate`, downloaders compare
    /// the download low-pass to `inactive_down_rate` (upstream).
    #[must_use]
    pub fn is_inactive_internal(&self, s: &crate::settings::SessionSettings) -> bool {
        if self.finished {
            self.up_rate < s.inactive_up_rate
        } else {
            self.down_rate < s.inactive_down_rate
        }
    }

    /// `ready_for_connections` — may open new peer connections
    /// (upstream: started, has metadata-or-magnet, not aborted).
    #[must_use]
    pub fn ready_for_connections(&self) -> bool {
        !self.is_paused() && !self.aborted
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

    /// `torrent::set_error` — `m_error` + `m_error_file` (surfaced
    /// via `status.error`/`status.error_file`). `file` is the
    /// `file_index` the error touched or an `ERROR_FILE_*` sentinel.
    pub fn set_error(&mut self, e: &str, file: i32) {
        self.last_error = e.to_string();
        self.error_file = file;
    }

    /// `torrent::handle_inconsistent_hashes` — v1 and v2 verdicts
    /// disagree: the torrent's hash state is untrustworthy, so every
    /// "have" bit is dropped and the torrent pauses in error.
    pub fn handle_inconsistent_hashes(
        &mut self,
        piece: PieceIndex,
        _s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) {
        let file = self
            .files
            .as_ref()
            .and_then(|fs| fs.map_block(piece, 0, 0).first().map(|s| s.file_index.0))
            .unwrap_or(crate::status::ERROR_FILE_NONE);
        self.set_error("inconsistent hashes (v1 vs v2 disagree)", file);
        // upstream `set_error` posts `torrent_error_alert`.
        out.push(TorrentEffect::Alert(crate::alerts::Alert::TorrentError {
            ih: self.info_hash,
            error: self.last_error.clone(),
            file: self.resolve_filename(file),
        }));
        // upstream drops the picker, hash picker and file progress;
        // pieces marked "have" may be unvalidated → assume none.
        self.picker_generation = self.picker_generation.wrapping_add(1);
        self.picker = None;
        self.hash_picker_state = bitdaemon_disk::hash_picker::HashPickerState::default();
        self.have.clear_all();
        self.verified_bytes = 0;
        self.files_completed.clear();
        self.finished = false;
        self.seeding = false;
        self.state_updated();
        self.paused = true;
    }

    /// `torrent::on_piece_verified`'s both-indeterminate leg — the
    /// data is on disk but cannot be verified yet (missing hashes):
    /// leave the piece finished-but-unchecked so it is re-verified
    /// once the hashes arrive, rather than re-downloaded.
    pub fn mark_piece_finished_unchecked(&mut self, piece: PieceIndex) {
        if let Some(p) = self.picker.as_mut() {
            for i in 0..p.blocks_in_piece(piece) {
                p.mark_as_finished(
                    bitdaemon_common::types::PieceBlock {
                        piece_index: piece,
                        block_index: i,
                    },
                    None,
                );
            }
        }
    }

    /// `torrent::resolve_filename` — map an `error_file` index (or
    /// sentinel) to the path/name string carried by alerts.
    /// Renamed files win over `file_storage` names, like upstream's
    /// `m_renamed_files.file_path`.
    #[must_use]
    pub fn resolve_filename(&self, file: i32) -> String {
        use crate::status::*;
        match file {
            ERROR_FILE_NONE => return String::new(),
            ERROR_FILE_SSL_CTX => return "SSL Context".into(),
            ERROR_FILE_EXCEPTION => return "exception".into(),
            ERROR_FILE_PARTFILE => return "partfile".into(),
            ERROR_FILE_METADATA => return "metadata".into(),
            _ => {}
        }
        if file >= 0
            && let Some(fs) = self.files.as_ref()
        {
            if let Some(renamed) = self.renamed_files.get(&file) {
                return renamed.clone();
            }
            if file < fs.num_files() {
                return String::from_utf8_lossy(&fs.file_path(
                    bitdaemon_common::types::FileIndex(file),
                    self.save_path.as_bytes(),
                ))
                .into_owned();
            }
        }
        self.save_path.clone()
    }

    /// `on_torrent_paused` — pause bookkeeping: time-critical
    /// deadlines are dropped (upstream clears
    /// `m_time_critical_pieces` on pause). Per-conn suggest state
    /// dies with the conns; the read-cache set persists.
    pub fn on_torrent_paused(&mut self) {
        self.time_critical.clear();
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

    /// `torrent::add_suggest_piece` (`suggest_read_cache` mode) —
    /// a block we just read from disk is a good suggestion: low
    /// availability, likely still cached. The picker must exist
    /// for the availability estimate (`need_picker` upstream — ours
    /// is always present under metadata).
    pub fn add_suggest_piece(&mut self, index: PieceIndex, s: &crate::settings::SessionSettings) {
        if s.suggest_mode != crate::settings::SuggestMode::SuggestReadCache {
            return;
        }
        let Some(p) = self.picker.as_ref() else {
            return;
        };
        let peers = (self.peers.len() as i32).max(1);
        let availability = p.get_availability(index) * 100 / peers;
        self.suggest_pieces
            .add_piece(index, availability, s.max_suggest_pieces);
    }

    /// `torrent::get_suggest_pieces` — pick up to `n` suggestions
    /// for a conn: not already in its `sent` list, not in its
    /// have-bitfield. Appends to `sent`, returns how many.
    pub fn get_suggest_pieces(
        &self,
        sent: &mut Vec<PieceIndex>,
        bits: &Bitfield,
        n: usize,
    ) -> usize {
        self.suggest_pieces.get_pieces(sent, bits, n)
    }

    /// `peer_connection::send_piece_suggestions` — emit `num`
    /// SUGGEST msgs to `pid` (highest priority last, upstream
    /// sends the vector tail). The conn's sent list is trimmed to
    /// `max_suggest_pieces` after picking.
    fn send_piece_suggestions(
        &mut self,
        pid: u64,
        num: usize,
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) {
        let Some(c) = self.peers.get_mut(&pid) else {
            return;
        };
        let new = self
            .suggest_pieces
            .get_pieces(&mut c.suggest_sent, &c.peer_pieces, num);
        let tail = c.suggest_sent.len() - new;
        let queue: Vec<PieceIndex> = c.suggest_sent[tail..].to_vec();
        if c.suggest_sent.len() as i32 > s.max_suggest_pieces {
            let over = c.suggest_sent.len() as i32 - s.max_suggest_pieces;
            c.suggest_sent.drain(..over.max(0) as usize);
        }
        for piece in queue {
            // `send_suggest` — skip conns still handshaking and
            // pieces the peer now has.
            let ok = self
                .peers
                .get(&pid)
                .is_some_and(|c| !c.connecting && !c.has_piece(piece));
            if ok {
                out.push(TorrentEffect::SendTo {
                    peer: pid,
                    msg: PeerMessage::SuggestPiece(piece),
                });
            }
        }
    }

    /// `valid_metadata` — the info dict is known. `m_torrent_file`
    /// upstream; our marker is `ti` or an initialized picker
    /// (picker-only torrents are a synthetic state upstream can't
    /// have, but they behave as metadata-known everywhere else).
    #[must_use]
    pub fn valid_metadata(&self) -> bool {
        self.ti.is_some() || self.picker.is_some()
    }

    /// `is_finished` — all *wanted* pieces verified. Upstream
    /// recomputes: `is_seed() || (valid_metadata() && has_picker()
    /// && picker.is_finished())`. The `finished` flag is `m_state`,
    /// not this predicate — priorities flipping the wanted set move
    /// the predicate before the state follows.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.is_seed()
            || (self.valid_metadata() && self.picker.as_ref().is_some_and(PiecePicker::is_finished))
    }

    /// `is_upload_only` — upstream `is_finished() || upload_mode()`:
    /// the user flag, or everything wanted has arrived.
    #[must_use]
    pub fn is_upload_only(&self) -> bool {
        self.is_finished() || self.upload_mode
    }

    /// `torrent::set_upload_mode` — entering: `send_upload_only`
    /// (BEP-21 flag to peers that negotiated it), every conn's
    /// download queues cancelled (picker `abort_download` each),
    /// interest re-evaluated for upload-only peers, and the
    /// `m_upload_mode_time` stamp the periodic retry keys off.
    /// Leaving: `last_connected` zeroed on every swarm entry
    /// (upstream forces a fast reconnect), `update_interest` +
    /// `send_block_requests` on all conns.
    pub fn set_upload_mode(
        &mut self,
        b: bool,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if b == self.upload_mode {
            return;
        }
        self.upload_mode = b;
        // upstream `set_need_save_resume(if_state_changed)`.
        self.need_save_resume = true;
        self.state_updated();
        self.send_upload_only(out);
        if b {
            let ids: Vec<u64> = self.peers.keys().copied().collect();
            for pid in ids {
                // upstream: upload-only peers re-evaluate interest
                // first (we may want to keep them as uploaders),
                // then every conn's request queues clear.
                if self.peers.get(&pid).is_some_and(|c| c.remote_upload_only) {
                    self.update_interest(pid, counters, out);
                }
                let key = self.peers.get(&pid).and_then(PeerConn::peer_key);
                if let Some(c) = self.peers.get_mut(&pid) {
                    // `cancel_all_requests` — queued blocks abort in
                    // the picker without a wire message; in-flight
                    // ones get `cancel` but stay registered (the
                    // peer may still deliver them).
                    let queued = c.pipeline.request_queue.drain(..).map(|b| b.block);
                    for blk in queued.collect::<Vec<_>>() {
                        if let Some(p) = self.picker.as_mut() {
                            p.abort_download(blk, key);
                        }
                    }
                    c.pipeline.queued_time_critical = 0;
                }
                let block_size = i64::from(self.block_size());
                let sent: Vec<bitdaemon_common::types::PieceBlock> = self
                    .peers
                    .get(&pid)
                    .map(|c| c.pipeline.download_queue.iter().map(|b| b.block).collect())
                    .unwrap_or_default();
                for b in sent {
                    let start = i64::from(b.block_index) * block_size;
                    let psize = self.files.as_ref().map_or(self.piece_length, |f| {
                        i64::from(f.piece_size(b.piece_index))
                    });
                    out.push(TorrentEffect::SendTo {
                        peer: pid,
                        msg: PeerMessage::Cancel(PeerRequest {
                            piece: b.piece_index,
                            start: start as i32,
                            length: (psize - start).min(block_size) as i32,
                        }),
                    });
                }
            }
            self.upload_mode_time = Some(Instant::now());
        } else {
            // reset `last_connected` — the swarm becomes dialable
            // again immediately (upstream forces fast reconnect).
            for p in self.peer_list.peers_mut() {
                p.last_connected = 0;
            }
            let ids: Vec<u64> = self.peers.keys().copied().collect();
            for pid in ids {
                self.update_interest(pid, counters, out);
                // `send_block_requests` — re-open the download pump.
                self.drive_peer(pid, s, counters, out);
            }
        }
    }

    /// `torrent::send_upload_only` — broadcast the BEP-21 flag to
    /// every conn. Skipped under share/super-seed mode and when
    /// `close_redundant_connections` is off. The advertised flag is
    /// `is_upload_only() && !super_seeding` (a finished torrent
    /// leaving upload mode still reports upload-only). Each conn
    /// gets `send_not_interested` first, then the ext message.
    fn send_upload_only(&mut self, out: &mut Vec<TorrentEffect>) {
        if self.share_mode || self.super_seeding || !self.close_redundant_connections {
            return;
        }
        let enabled = self.is_upload_only();
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for pid in ids {
            // `send_not_interested`: an uninterested conn runs
            // `disconnect_if_redundant` instead of emitting a msg.
            if self.peers.get(&pid).is_some_and(|c| c.we_sent_interested) {
                if let Some(c) = self.peers.get_mut(&pid) {
                    c.we_sent_interested = false;
                    c.became_uninteresting_ms = c.now_ms;
                }
                out.push(TorrentEffect::SendTo {
                    peer: pid,
                    msg: PeerMessage::NotInterested,
                });
            } else {
                self.disconnect_if_redundant(pid, out);
            }
            // `peer_connection::send_upload_only` skips conns still
            // in handshake; `write_upload_only` no-ops when the peer
            // lacks the extension.
            let ext_id = self.peers.get(&pid).and_then(|c| {
                (!c.connecting && c.ext_upload_only != 0).then_some(c.ext_upload_only)
            });
            if let Some(id) = ext_id {
                out.push(TorrentEffect::SendTo {
                    peer: pid,
                    msg: PeerMessage::Extended {
                        id,
                        data: bytes::Bytes::copy_from_slice(&[u8::from(enabled)]),
                    },
                });
            }
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
    /// `torrent::predicted_have_piece` — predictive piece announce:
    /// claim `piece` in `m_predictive_pieces` and send a speculative
    /// `have` to every conn (`announce_piece` — skips handshaking
    /// conns, redundant haves unless `send_redundant_have`, and may
    /// drop seed↔seed conns via `disconnect_if_redundant`). A
    /// piece already claimed is a no-op.
    pub fn predicted_have_piece(
        &mut self,
        index: PieceIndex,
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) {
        if self.is_predictive_piece(index) {
            return;
        }
        let n = self.num_pieces.max(0) as usize;
        if self.predictive.size() < n {
            self.predictive.resize(n);
        }
        self.predictive.set_bit(index.0 as usize);
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            if self
                .peers
                .get(&id)
                .is_none_or(|c| c.connecting || !c.handshake_done)
            {
                continue;
            }
            // `announce_piece` — a seed↔seed conn drops before the
            // speculative have goes out.
            let at = out.len();
            self.disconnect_if_redundant(id, out);
            if out[at..]
                .iter()
                .any(|e| matches!(e, TorrentEffect::DisconnectPeer { peer, .. } if *peer == id))
            {
                continue;
            }
            let has_it = self.peers.get(&id).is_some_and(|c| c.has_piece(index));
            if !s.send_redundant_have && has_it {
                continue;
            }
            out.push(TorrentEffect::SendTo {
                peer: id,
                msg: PeerMessage::Have(index),
            });
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

    /// `update_auto_sequential` — with `auto_sequential` enabled,
    /// enter sequential picking when the swarm is healthy enough to
    /// sustain it: ≥10 connected (non-connecting) peers, ≥10 seeds,
    /// and seeds ≥10× downloaders.
    pub fn update_auto_sequential(&mut self, s: &crate::settings::SessionSettings) {
        if !s.auto_sequential {
            self.auto_sequential = false;
            return;
        }
        // upstream `num_peers() - m_num_connecting < 10` — our
        // `peers` holds established conns only, which is the same
        // value (upstream counts still-connecting conns too, then
        // subtracts them back off).
        if self.peers.len() < 10 {
            self.auto_sequential = false;
            return;
        }
        // `num_downloaders` = connected peers that aren't seeds —
        // upstream's full formula (`num_peers - num_seeds -
        // num_connecting + num_connecting_seeds`) reduces to this
        // once connecting conns are accounted out of `peers`.
        let seeds = self.num_seeds as i64;
        let downloaders = self.peers.len() as i64 - seeds;
        self.auto_sequential = downloaders * 10 <= seeds && seeds > 9;
    }

    /// `is_sequential_download` — user `sequential` OR the
    /// `auto_sequential` heuristic.
    #[must_use]
    pub fn is_sequential_download(&self) -> bool {
        self.sequential || self.auto_sequential
    }

    /// `set_seed` — mark a *connected* peer as (not) a seed:
    /// flips the peer-list `seed` flag and re-counts
    /// `m_num_seeds`, then re-runs the auto-sequential heuristic
    /// (upstream's `update_want_peers` isn't needed here — our
    /// connect pump reads live peer state each tick).
    pub fn set_seed(&mut self, peer_id: u64, s: bool, settings: &crate::settings::SessionSettings) {
        let Some(remote) = self.peers.get(&peer_id).and_then(|c| c.remote) else {
            return;
        };
        let Some(i) = self.peer_index(remote) else {
            return;
        };
        if self.peer_list.peers()[i].seed == s {
            return;
        }
        if s {
            self.num_seeds += 1;
        } else {
            self.num_seeds = self.num_seeds.saturating_sub(1);
        }
        self.peer_list.set_seed(i, s);
        self.update_auto_sequential(settings);
        self.update_want_peers();
    }

    /// `seen_complete` — mark that we just saw a complete copy
    /// (seed peer delivered payload, or a tracker scrape came back).
    /// `m_last_seen_complete` upstream.
    pub fn seen_complete(&mut self) {
        self.last_seen_complete = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
    }

    /// `time_since_complete` — seconds since `last_seen_complete`
    /// (the `complete_ago` value our BEP-21 handshake advertises).
    #[must_use]
    pub fn time_since_complete(&self) -> i64 {
        if self.last_seen_complete <= 0 {
            return -1;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        (now - self.last_seen_complete).max(0)
    }

    /// `update_scrape_state` — refresh the torrent-wide scrape cache
    /// (`m_complete`/`m_incomplete`/`m_downloaded`) from the max over
    /// tracker entries. Posts `state_updated` when a reported value
    /// moves, and marks resume data dirty (upstream's
    /// `if_counters_changed` — our `need_save_resume` covers it).
    pub fn update_scrape_state(&mut self, s: &crate::settings::SessionSettings) {
        let (mut complete, mut incomplete, mut downloaded) = (-1i32, -1i32, -1i32);
        for e in &self.trackers.trackers {
            complete = complete.max(e.scrape_complete as i32);
            incomplete = incomplete.max(e.scrape_incomplete as i32);
            downloaded = downloaded.max(e.scrape_downloaded as i32);
        }
        if (complete >= 0 && self.complete != complete)
            || (incomplete >= 0 && self.incomplete != incomplete)
            || (downloaded >= 0 && self.downloaded != downloaded)
        {
            self.state_updated();
        }
        if self.complete != complete
            || self.incomplete != incomplete
            || self.downloaded != downloaded
        {
            self.complete = complete;
            self.incomplete = incomplete;
            self.downloaded = downloaded;
            self.update_auto_sequential(s);
            self.need_save_resume = true;
        }
    }

    /// `update_want_peers` — recompute the cached enable flag
    /// (upstream marks the torrent dirty in its `want_peers_*`
    /// state lists; our flag only holds the durable gates —
    /// volatile ones stay live in [`Torrent::want_peers`]).
    pub fn update_want_peers(&mut self) {
        self.want_peers = !self.is_paused() && !self.aborted && !self.graceful_pause;
    }

    /// `update_tracker_timer` — upstream nudges the next announce
    /// tick; our session tick already scans `tracker_announce_due`,
    /// so this only needs to keep `announcing` honored (no-op).
    pub fn update_tracker_timer(&mut self) {}

    /// `torrent::resume` — clears `m_paused` unconditionally and
    /// `graceful_pause` unless still session-paused; the announce
    /// flags and machinery live in `Session::do_resume`.
    pub fn resume(&mut self) {
        if !self.paused && self.announce_dht && self.announce_trackers && self.announce_lsd {
            return;
        }
        if self.is_paused() || self.aborted || self.graceful_pause {
            self.state_updated();
        }
        self.paused = false;
        // `m_abort` is terminal — upstream `resume` never clears it
        // (an aborted torrent is mid-removal).
        if !self.session_paused {
            self.graceful_pause = false;
        }
        self.need_save_resume = true;
    }

    /// `recalc_share_mode` — share-mode re-prioritization: pieces we
    /// already have get demoted so rarest-first serves the swarm
    /// (upstream walks `m_picker` marking have-pieces `dont_download`).
    pub fn recalc_share_mode(
        &mut self,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if !self.share_mode || self.is_seed() {
            return;
        }
        // swarm census (upstream `recalc_share_mode`) — connecting
        // and disconnecting conns don't count; seeds/share-mode/
        // upload-only peers aren't downloaders.
        let mut num_peers = 0;
        let mut num_seeds = 0;
        let mut num_downloaders = 0;
        let mut missing_pieces = 0i64;
        for c in self.peers.values() {
            if c.connecting || c.disconnecting {
                continue;
            }
            num_peers += 1;
            if c.is_seed() {
                num_seeds += 1;
                continue;
            }
            if c.remote_share_mode || c.remote_upload_only {
                continue;
            }
            num_downloaders += 1;
            missing_pieces += i64::from(self.num_pieces - c.num_pieces);
        }
        if num_peers == 0 {
            return;
        }
        // >50% seeds near the conn cap limits our upload ability —
        // disconnect seeds down to half the swarm (upstream
        // `upload_upload_connection`).
        let max_conn = if self.max_connections <= 0 {
            s.connections_limit
        } else {
            self.max_connections
        }
        .max(1);
        if num_seeds * 100 / num_peers > 50 && (num_peers * 100 / max_conn > 90 || num_peers > 20) {
            let to_disconnect = (num_seeds - num_peers / 2) as usize;
            let mut seeds: Vec<u64> = self
                .peers
                .iter()
                .filter(|(_, c)| c.is_seed())
                .map(|(id, _)| *id)
                .collect();
            use rand::seq::SliceRandom as _;
            seeds.shuffle(&mut rand::rng());
            for id in seeds.into_iter().take(to_disconnect) {
                out.push(TorrentEffect::DisconnectPeer {
                    peer: id,
                    reason: "upload-upload",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
            }
        }
        if num_downloaders == 0 {
            return;
        }
        // seeds can re-upload in the time we fetch one piece —
        // discount their share from the gap.
        missing_pieces -= 2 * i64::from(num_seeds);
        if missing_pieces <= 0 {
            return;
        }
        // download at least one piece; stay under ~50% share ratio
        // (`share_mode_target`) and cap parallel downloads at 5% of
        // downloaded pieces.
        let (num_downloaded, dl_queue) = self
            .picker
            .as_ref()
            .map(|p| {
                (
                    p.num_have().max(p.num_pieces() - p.num_filtered()),
                    p.get_download_queue_size() as i32,
                )
            })
            .unwrap_or((0, 0));
        // upstream compares against `m_total_uploaded` — the
        // all-time counter (resume seed + session payload).
        if i64::from(num_downloaded) * self.piece_length * i64::from(s.share_mode_target)
            > self.all_time_upload + self.total_upload()
            && num_downloaded > 0
        {
            return;
        }
        if num_downloaded <= 0 || dl_queue > num_downloaded / 20 {
            return;
        }
        // collect the rarest pieces among connected peers;
        // have/downloading pieces first get re-promoted from
        // `dont_download`.
        let mut rarest: Vec<PieceIndex> = Vec::new();
        let mut rarest_rarity = i32::MAX;
        if let Some(p) = self.picker.as_mut() {
            for i in 0..self.num_pieces {
                let ps = p.piece_stats(PieceIndex(i));
                if ps.peer_count == 0 {
                    continue;
                }
                if ps.priority == 0 && (ps.flushed || ps.downloading) {
                    p.set_piece_priority(
                        PieceIndex(i),
                        bitdaemon_common::types::DownloadPriority::DEFAULT,
                    );
                    continue;
                }
                if ps.priority > 0 || ps.flushed {
                    continue;
                }
                if ps.peer_count > rarest_rarity {
                    continue;
                }
                if ps.peer_count < rarest_rarity {
                    rarest.clear();
                    rarest_rarity = ps.peer_count;
                }
                rarest.push(PieceIndex(i));
            }
        }
        self.update_want_peers();
        // not enough interested peers to re-upload a piece —
        // share ratio can't go positive.
        if num_peers - rarest_rarity < s.share_mode_target || rarest.is_empty() {
            return;
        }
        let pick = bitdaemon_common::random::random(rarest.len() as u32) as usize;
        let was_finished = self.is_finished();
        if let Some(p) = self.picker.as_mut() {
            p.set_piece_priority(
                rarest[pick],
                bitdaemon_common::types::DownloadPriority::DEFAULT,
            );
        }
        self.update_peer_interest(was_finished, counters, out);
        self.update_want_peers();
    }

    /// `request_time_critical_pieces` — re-issue deadlines:
    /// upstream refreshes `m_time_critical_pieces` into peer requests
    /// every tick for deadline pieces; we nudge every connected peer
    /// (the request pipeline prioritizes deadline blocks already).
    pub fn request_time_critical_pieces(
        &mut self,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if self.time_critical.is_empty() {
            return;
        }
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.drive_peer(id, s, counters, out);
        }
    }

    /// `attach_peer` — register a connection object (the session
    /// allocates the id; identical to `add_peer` plus connecting
    /// bookkeeping).
    pub fn attach_peer(
        &mut self,
        id: u64,
        c: PeerConn,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        self.add_peer(id, c);
        // upstream `attach_peer` tail — a new swarm member changes
        // the share-mode rebalance inputs.
        if self.share_mode {
            self.recalc_share_mode(s, counters, out);
        }
    }

    /// `send_choke` — send `Choke` + flag (`m_choked`), reject every
    /// queued upload request that isn't an accept-fast piece, drop
    /// the optimistic bit and clear suggestions. Returns whether a
    /// `Choke` was queued.
    pub fn choke_peer(
        &mut self,
        id: u64,
        counters: &crate::counters::Counters,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
        out: &mut Vec<TorrentEffect>,
    ) -> bool {
        let t_ignores_unchoke = self.ignores_unchoke_slots(pool);
        let Some(c) = self.peers.get_mut(&id) else {
            return false;
        };
        if c.choked {
            return false;
        }
        let remote = c.remote;
        if let Some(i) = remote.and_then(|r| self.peer_index(r)) {
            let p = &mut self.peer_list.peers_mut()[i];
            if p.optimistically_unchoked {
                p.optimistically_unchoked = false;
                counters.add(crate::counters::Counter::NumPeersUpUnchokedOptimistic, -1);
            }
        }
        let Some(c) = self.peers.get_mut(&id) else {
            return false;
        };
        // upstream `send_choke` — suggestions go stale while the
        // peer is choked, so the sent-list resets.
        c.suggest_sent.clear();
        c.suggest_sent.shrink_to_fit();
        c.choked = true;
        c.num_invalid_requests = 0;
        // `m_last_choke` — requests from a choked peer are only
        // tolerated for 2 s after our last Choke.
        c.last_choke_ms = c.now_ms;
        let mut keep = Vec::new();
        for r in c.upload_queue.drain(..) {
            if c.accept_fast.contains(&r.piece) {
                keep.push(r);
                continue;
            }
            counters.add(crate::counters::Counter::ChokedPieceRequests, 1);
            // `write_reject_request` counts the reject even when the
            // peer can't receive it (`!supports_fast` → no message).
            counters.add(crate::counters::Counter::PieceRejects, 1);
            if c.supports_fast {
                out.push(TorrentEffect::SendTo {
                    peer: id,
                    msg: PeerMessage::RejectRequest(r),
                });
            }
        }
        c.upload_queue = keep;
        if c.upload_queue.is_empty() {
            counters.add(crate::counters::Counter::NumPeersUpRequests, -1);
        }
        counters.add(crate::counters::Counter::NumPeersUpUnchokedAll, -1);
        if !c.ignores_unchoke_slots(pool) && !t_ignores_unchoke {
            counters.add(crate::counters::Counter::NumPeersUpUnchoked, -1);
        }
        // `choke_peer` → `state_updated()` upstream (num_uploads).
        self.state_updated();
        out.push(TorrentEffect::SendTo {
            peer: id,
            msg: PeerMessage::Choke,
        });
        true
    }

    /// `send_unchoke` gated by `unchoke_peer`'s per-torrent
    /// `max_uploads` veto (`optimistic` unchokes bypass the veto).
    /// Returns whether a `Unchoke` was queued.
    #[allow(clippy::too_many_arguments)]
    pub fn unchoke_peer(
        &mut self,
        id: u64,
        optimistic: bool,
        counters: &crate::counters::Counters,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
        now: std::time::Instant,
        s: &crate::settings::SessionSettings,
        out: &mut Vec<TorrentEffect>,
    ) -> bool {
        // `m_num_uploads` upstream counts only slot-consuming conns
        // (`!ignore_unchoke_slots` — exempt conns unchoke for free).
        let t_ignores_unchoke = self.ignores_unchoke_slots(pool);
        let num_uploads = self
            .peers
            .values()
            .filter(|c| !c.choked && !c.ignores_unchoke_slots(pool) && !t_ignores_unchoke)
            .count() as i64;
        let max_uploads = if self.max_uploads <= 0 {
            i64::MAX
        } else {
            i64::from(self.max_uploads)
        };
        if num_uploads >= max_uploads && !optimistic {
            return false;
        }
        let Some(c) = self.peers.get_mut(&id) else {
            return false;
        };
        if !c.choked {
            return false;
        }
        c.choked = false;
        c.last_unchoke = now;
        c.last_unchoke_ms = c.now_ms;
        c.uploaded_at_last_unchoke = c.stat_up_total;
        counters.add(crate::counters::Counter::NumPeersUpUnchokedAll, 1);
        if !c.ignores_unchoke_slots(pool) && !t_ignores_unchoke {
            counters.add(crate::counters::Counter::NumPeersUpUnchoked, 1);
        }
        self.state_updated();
        // upstream `send_unchoke` — under `suggest_read_cache`,
        // fresh suggestions go out immediately before the unchoke.
        if s.suggest_mode == crate::settings::SuggestMode::SuggestReadCache {
            self.send_piece_suggestions(id, 2, s, out);
        }
        out.push(TorrentEffect::SendTo {
            peer: id,
            msg: PeerMessage::Unchoke,
        });
        true
    }

    /// `peer_connection::maybe_unchoke_this_peer` — once the
    /// handshake+bitfield went out (and only when we have pieces),
    /// unchoke on the spot to save a round-trip: `ignore_unchoke_slots`
    /// conns always (bypassing `max_uploads`, like upstream's direct
    /// `send_unchoke`), others only under `preemptive_unchoke`
    /// (fixed-slots choker with free slots, or a negative
    /// `unchoke_slots_limit`).
    pub fn maybe_unchoke_this_peer(
        &mut self,
        id: u64,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
        now: std::time::Instant,
        out: &mut Vec<TorrentEffect>,
    ) {
        // upstream: `t->num_have() > 0` gates the whole leg.
        if self.have.count() == 0 {
            return;
        }
        let exempt = self
            .peers
            .get(&id)
            .is_some_and(|c| c.ignores_unchoke_slots(pool))
            || self.ignores_unchoke_slots(pool);
        if exempt {
            self.unchoke_peer(id, true, counters, pool, now, s, out);
            return;
        }
        // `session_impl::preemptive_unchoke`.
        let preemptive = s.choking_algorithm == crate::choker::ChokingAlgorithm::FixedSlots
            && (counters.value(crate::counters::Counter::NumPeersUpUnchoked)
                < counters.value(crate::counters::Counter::NumUnchokeSlots)
                || s.unchoke_slots_limit < 0);
        if preemptive {
            self.unchoke_peer(id, false, counters, pool, now, s, out);
        }
    }

    /// `free_upload_slots` — under the per-torrent upload cap.
    pub fn free_upload_slots(&self) -> bool {
        self.max_uploads <= 0
            || (self.peers.values().filter(|c| !c.choked).count() as i64)
                < i64::from(self.max_uploads)
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

    /// `retry_web_seed` — reschedule a seed after a failure:
    /// upstream sets `web->retry = time_now() +
    /// retry.value_or(urlseed_wait_retry)`, skipping
    /// removed/disabled seeds. `backoff` carries the engine's
    /// classification (`Retry-After` header, name-lookup failure…).
    pub fn retry_web_seed(
        &mut self,
        url: &str,
        backoff: WebSeedBackoff,
        s: &crate::settings::SessionSettings,
    ) {
        let wait = match backoff {
            WebSeedBackoff::Header(d) => d,
            WebSeedBackoff::NameLookup => s.web_seed_name_lookup_retry,
            _ => s.urlseed_wait_retry,
        };
        if let Some(st) = self.web_seed_state.get_mut(url) {
            if st.removed || st.disabled {
                return;
            }
            st.retry_ms = self.now_ms + wait.as_millis() as i64;
        }
    }

    /// `maybe_connect_web_seeds` — nudge the engine to pull blocks
    /// off url seeds (session `next_web_seed_blocks` does the pick).
    /// Mirrors the upstream early-outs that don't need the picker:
    /// aborted, no seeds, finished, unchecked files.
    pub fn maybe_connect_web_seeds(&self) -> bool {
        !self.aborted
            && !self.web_seeds.is_empty()
            && !self.finished
            && self.files_checked
            && !self.is_paused()
            && !self.is_seed()
            && !self.upload_mode
    }

    /// `set_ssl_cert_buffer` — PEM buffer form (`set_ssl_cert` takes
    /// paths upstream; we store the resolved cert string either way).
    pub fn set_ssl_cert_buffer(&mut self, cert: &str, key: &str, _dh: &str) {
        self.ssl_cert = Some(cert.to_string());
        self.ssl_key = Some(key.to_string());
    }

    /// `torrent::disconnect_peers(num, ec)` — pick `num` conns by
    /// upstream's `compare_disconnect_peer` (disconnecting first,
    /// then uninteresting, non-seed, parole, lowest download rate,
    /// choking us, stalest `last_received`) and emit `DisconnectPeer`
    /// effects — the session route runs `peer_disconnected`
    /// bookkeeping and drops the engine-side writer. Returns how
    /// many disconnects were issued.
    pub fn disconnect_peers(
        &mut self,
        num: usize,
        reason: &'static str,
        out: &mut Vec<TorrentEffect>,
    ) -> usize {
        let now = self.now_ms;
        let mut ranked: Vec<u64> = self.peers.keys().copied().collect();
        ranked.sort_by(|&a, &b| {
            let (x, y) = (&self.peers[&a], &self.peers[&b]);
            // every `true` means "disconnect before the other" — the
            // upstream comparator returns `lhs < rhs` for
            // *preferred disconnects*, so prefer-flags negate.
            let key = |c: &PeerConn| {
                let secs = (now - c.connect_ms).max(0) / 1000 + 1;
                let rate = c.stat_down_total / secs;
                (
                    // disconnecting peers first
                    !c.disconnecting,
                    // uninteresting next
                    c.interesting,
                    // non-seeds before seeds
                    c.is_seed(),
                    // parole next
                    !c.on_parole,
                    // lowest download rate
                    rate,
                    // peers choking us
                    !c.peer_choked,
                    // stalest last-received
                    c.last_receive_ms,
                )
            };
            key(x).cmp(&key(y))
        });
        ranked.truncate(num);
        let n = ranked.len();
        for id in ranked {
            if let Some(c) = self.peers.get_mut(&id) {
                c.disconnecting = true;
                // `peer_connection::disconnect` — `m_disconnecting`
                // purges the conn's queued bandwidth requests on the
                // next `update_quotas` pass.
                c.bw.disconnecting
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            out.push(TorrentEffect::DisconnectPeer {
                peer: id,
                reason,
                // `too_many_connections`-class teardowns are
                // normal-severity upstream (no failcount bump).
                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
            });
        }
        n
    }

    /// `torrent::privileged_port_updated` — `no_connect_privileged
    /// _ports` turned on: erase peer-list entries with remote port
    /// <1024 and disconnect live conns to privileged ports
    /// (upstream's `peer_list::apply_port_filter` sweep).
    pub fn privileged_port_updated(
        &mut self,
        _session_time: i32,
        _counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        const PRIVILEGED: u16 = 1024;
        // upstream `apply_port_filter` disconnects a filtered conn
        // *before* erasing its swarm entry — `connection_closed`
        // bookkeeping needs the entry. `banned_by_port_filter` is
        // default-severity (no failcount bump); the sweep also
        // reports each endpoint for `peer_blocked_alert`.
        let ids: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| {
                c.remote
                    .is_some_and(|r| r.port() < PRIVILEGED && r.port() > 0)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some(c) = self.peers.get(&id)
                && let Some(remote) = c.remote
            {
                out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerBlocked {
                    ih: self.info_hash,
                    remote: remote.to_string(),
                    reason: "port_filter".into(),
                }));
                out.push(TorrentEffect::DisconnectPeer {
                    peer: id,
                    reason: "banned_by_port_filter",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
            }
        }
        let mut i = 0;
        while i < self.peer_list.peers().len() {
            let p = &self.peer_list.peers()[i];
            let banned = p.port < PRIVILEGED && p.port > 0 && p.addr.address().is_some();
            if !banned || self.peer_list.locked_peer() == Some(i) {
                i += 1;
                continue;
            }
            self.peer_list.erase_peer(i);
        }
    }

    /// `set_apply_ip_filter` — `m_apply_ip_filter` flag (upstream
    /// `apply_ip_filter` marks `state_updated` + resume dirty and
    /// runs `ip_filter_updated` — the torrent-level sweep of
    /// now-blocked live conns rides `out`).
    /// `non_filter_torrents` delta: `false → +1`, `true → -1`.
    pub fn set_apply_ip_filter(
        &mut self,
        yes: bool,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        if yes == self.apply_ip_filter {
            return;
        }
        counters.add(
            crate::counters::Counter::NonFilterTorrents,
            if yes { -1 } else { 1 },
        );
        self.apply_ip_filter = yes;
        self.need_save_resume = true;
        self.state_updated();
        // `ip_filter_updated` — turning the flag ON applies the
        // current filter to live conns and swarm entries right now
        // (same shape as `privileged_port_updated`): conn teardown
        // rides `DisconnectPeer`, blocked list entries drop via
        // `apply_ip_filter`, each endpoint is reported.
        if !yes {
            return;
        }
        let Some(f) = self.ip_filter.clone() else {
            return;
        };
        let ids: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| {
                c.remote
                    .is_some_and(|r| f.access(r.ip()) == bitdaemon_net::ip_filter::BLOCKED)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some(c) = self.peers.get(&id)
                && let Some(remote) = c.remote
            {
                out.push(TorrentEffect::Alert(crate::alerts::Alert::PeerBlocked {
                    ih: self.info_hash,
                    remote: remote.to_string(),
                    reason: "ip_filter".into(),
                }));
                out.push(TorrentEffect::DisconnectPeer {
                    peer: id,
                    reason: "banned_by_ip_filter",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
            }
        }
        self.peer_list
            .apply_ip_filter(|a| f.access(a) == bitdaemon_net::ip_filter::BLOCKED);
    }

    /// `torrent::set_file_priority` — bounds-checked single-slot
    /// update: rejected out-of-range (metadata present) or negative
    /// indexes, clamped to `dont_download..=top_priority`; defers to
    /// `m_deferred_file_priorities` while a disk job is outstanding.
    pub fn set_file_priority(
        &mut self,
        index: bitdaemon_common::types::FileIndex,
        prio: bitdaemon_common::types::DownloadPriority,
        s: &crate::settings::SessionSettings,
        counters: &crate::counters::Counters,
        out: &mut Vec<TorrentEffect>,
    ) {
        use bitdaemon_common::types::DownloadPriority;
        // upstream precondition: `index >= 0 && (!valid_metadata() ||
        // index < end_file())`.
        if index.0 < 0
            || self
                .files
                .as_ref()
                .is_some_and(|fs| index.0 >= fs.num_files())
        {
            return;
        }
        let prio = prio.clamp(DownloadPriority::DONT_DOWNLOAD, DownloadPriority::TOP);
        if self.outstanding_file_priority {
            self.deferred_file_priorities.insert(index.0, prio);
            return;
        }
        let mut v = self.file_priorities.clone();
        let i = index.0 as usize;
        if v.len() <= i {
            v.resize(i + 1, DownloadPriority::DEFAULT);
        }
        v[i] = prio;
        if self.storage != StorageIndex::INVALID {
            self.outstanding_file_priority = true;
            out.push(TorrentEffect::SetFilePriority { prios: v });
        } else {
            if self.file_priorities != v {
                self.file_priorities = v;
                self.need_save_resume = true;
            }
        }
        let _ = (s, counters);
    }

    /// `torrent::set_state` — upstream `set_state(torrent_status::
    /// state_t)` drives the checking→downloading→finished ladder;
    /// ours is derived (`checking`/`finished`/`seeding` flags), so
    /// this only covers the explicit transitions callers make.
    pub fn set_state(&mut self, s: crate::status::TorrentState) {
        use crate::status::TorrentState as S;
        // upstream `set_state`: no-op on no-change; otherwise the
        // transition marks `state_updated()`.
        if self.state() == s {
            return;
        }
        match s {
            // `checking_resume_data` — and ABI-1's
            // `queued_for_checking`, which upstream reports through
            // the same window — is the fastresume-verify pass.
            S::CheckingResumeData | S::QueuedForChecking => {
                self.checking = true;
                self.checking_resume = true;
            }
            S::CheckingFiles => {
                self.checking = true;
                self.checking_resume = false;
            }
            S::DownloadingMetadata | S::Downloading => {
                self.checking = false;
                self.checking_resume = false;
                self.finished = false;
                self.seeding = false;
            }
            S::Finished => {
                self.checking = false;
                self.checking_resume = false;
                self.finished = true;
                self.seeding = false;
            }
            S::Seeding => {
                self.checking = false;
                self.checking_resume = false;
                self.finished = true;
                self.seeding = true;
            }
        }
        self.state_updated();
    }

    /// `post_status` — snapshot routed to the alert queue by the
    /// caller (upstream `post(status_alert)`); the session feeds
    /// `settings`/`queue_pos`/`flags` through [`Self::status`].
    #[must_use]
    pub fn post_status(
        &self,
        s: &crate::settings::SessionSettings,
        queue_pos: i32,
        flags: crate::status::StatusFlags,
        pool: &bitdaemon_net::peer_class::PeerClassPool,
    ) -> crate::status::TorrentStatus {
        self.status(s, queue_pos, flags, pool)
    }

    /// `get_torrent_copy_with_hashes` — `torrent_file_with_hashes`
    /// (`save_resume_data` attaches `piece_layers` + `merkle_trees`).
    #[must_use]
    pub fn get_torrent_copy_with_hashes(&self) -> Option<std::sync::Arc<TorrentInfo>> {
        self.ti.clone()
    }
}
