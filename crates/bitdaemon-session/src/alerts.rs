// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Cognition — BitDaemon port
//
//! `alert` + `alert_manager` — the event bus (ADR-0023).
//!
//! Upstream `alert_types.hpp` defines 102 `alert_type` ids (3–107
//! with gaps; `mmap_cache_alert` 69 is `TORRENT_ABI_VERSION == 1`-only
//! → skipped); this ports the 101 concrete ABI-2 types behind one
//! `Alert` enum (class hierarchy →
//! variants), with `category()`/`alert_type()`/`priority()` tables
//! verbatim from `test_alert_types.cpp` — the conformance spec.
//! `AlertCategory` uses upstream `alert_category_t` bit positions
//! (bits 0–24): `alert_mask` is persisted in `.session`/`session_params`,
//! so bit values are a wire-format concern, not style.
//!
//! `AlertManager` parity notes (upstream `aux_/alert_manager.hpp`):
//! - `emplace_alert` is *unconditional* — `should_post<T>()` gates the
//!   call site. Category-0 alerts (`SessionStats`,
//!   `SessionStatsHeader`, `DhtStats`) therefore always land; our
//!   `post` short-circuits the mask check for them.
//! - The queue limit is priority-weighted:
//!   `queue.size() / (1 + priority) >= limit` — high/critical/meta
//!   alerts get 2×/3×/4× headroom.
//! - `m_dropped` is a per-type bitset keyed by `alert_type`; a
//!   synthesized `alerts_dropped_alert` rides the next `get_all`.

use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::types::PieceIndex;

/// `alert_category_t` — upstream bit positions verbatim
/// (`alert.hpp` bits 0–24).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct AlertCategory(pub u32);

impl AlertCategory {
    /// `error_notification` (bit 0).
    pub const ERROR: Self = Self(1 << 0);
    /// `peer_notification` (bit 1).
    pub const PEER: Self = Self(1 << 1);
    /// `port_mapping_notification` (bit 2).
    pub const PORT_MAPPING: Self = Self(1 << 2);
    /// `storage_notification` (bit 3).
    pub const STORAGE: Self = Self(1 << 3);
    /// `tracker_notification` (bit 4).
    pub const TRACKER: Self = Self(1 << 4);
    /// `connect_notification` (bit 5).
    pub const CONNECT: Self = Self(1 << 5);
    /// `status_notification` (bit 6).
    pub const STATUS: Self = Self(1 << 6);
    /// `progress_notification` (bit 7; deprecated upstream, retained
    /// for mask compatibility).
    pub const PROGRESS: Self = Self(1 << 7);
    /// `ip_block_notification` (bit 8).
    pub const IP_BLOCK: Self = Self(1 << 8);
    /// `performance_warning` (bit 9).
    pub const PERFORMANCE: Self = Self(1 << 9);
    /// `dht_notification` (bit 10).
    pub const DHT: Self = Self(1 << 10);
    /// `stats_notification` (bit 11).
    pub const STATS: Self = Self(1 << 11);
    /// `session_log_notification` (bit 13).
    pub const SESSION_LOG: Self = Self(1 << 13);
    /// `torrent_log_notification` (bit 14).
    pub const TORRENT_LOG: Self = Self(1 << 14);
    /// `peer_log_notification` (bit 15).
    pub const PEER_LOG: Self = Self(1 << 15);
    /// `incoming_request_notification` (bit 16).
    pub const INCOMING_REQUEST: Self = Self(1 << 16);
    /// `dht_log_notification` (bit 17).
    pub const DHT_LOG: Self = Self(1 << 17);
    /// `dht_operation_notification` (bit 18).
    pub const DHT_OPERATION: Self = Self(1 << 18);
    /// `port_mapping_log_notification` (bit 19).
    pub const PORT_MAPPING_LOG: Self = Self(1 << 19);
    /// `picker_log_notification` (bit 20).
    pub const PICKER_LOG: Self = Self(1 << 20);
    /// `file_progress_notification` (bit 21).
    pub const FILE_PROGRESS: Self = Self(1 << 21);
    /// `piece_progress_notification` (bit 22).
    pub const PIECE_PROGRESS: Self = Self(1 << 22);
    /// `upload_notification` (bit 23).
    pub const UPLOAD: Self = Self(1 << 23);
    /// `block_progress_notification` (bit 24).
    pub const BLOCK_PROGRESS: Self = Self(1 << 24);
    /// `debug_notification` — upstream alias for `connect`.
    pub const DEBUG: Self = Self::CONNECT;
    /// `all_categories`.
    pub const ALL: Self = Self(u32::MAX);
    /// raw bits (session_params round-trip).
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }
    /// from raw bits.
    #[must_use]
    pub const fn from_bits(b: u32) -> Self {
        Self(b)
    }
}

/// `alert_priority` (`alert_types.hpp`) — feeds the priority-weighted
/// queue limit `len / (1 + priority) >= limit`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum AlertPriority {
    /// `alert_priority::normal` — factor 1.
    #[default]
    Normal = 0,
    /// `alert_priority::high` — 2× queue headroom.
    High = 1,
    /// `alert_priority::critical` — 3× queue headroom.
    Critical = 2,
    /// `alert_priority::meta` — 4× queue headroom.
    Meta = 3,
}

/// `alert` — the full upstream alert set as one enum. Each variant
/// documents its `*_alert` type and carries upstream's `(alert_type,
/// priority, static_category)` via the tables below. Variants whose
/// machinery isn't reached yet are simply never posted — declaring
/// them keeps `type()`/`category()` conformance complete.
#[derive(Clone, Debug)]
pub enum Alert {
    /// `torrent_added_alert` (3).
    TorrentAdded(Sha1Hash),
    /// `torrent_removed_alert` (4).
    TorrentRemoved(Sha1Hash),
    /// `read_piece_alert` — the piece payload (5).
    ReadPiece {
        /// torrent.
        ih: Sha1Hash,
        /// piece index.
        piece: i32,
        /// piece bytes.
        buffer: Vec<u8>,
    },
    /// `read_piece_alert` failure form (`ec` set upstream — same
    /// alert type 5; we split success/failure into two variants).
    ReadPieceFailed {
        /// torrent.
        ih: Sha1Hash,
        /// piece index.
        piece: i32,
        /// error.
        error: String,
    },
    /// `file_completed_alert` (6).
    FileCompleted {
        /// torrent.
        ih: Sha1Hash,
        /// `file_index`.
        index: i32,
    },
    /// `file_renamed_alert` (7).
    FileRenamed {
        /// torrent.
        ih: Sha1Hash,
        /// `index`.
        index: i32,
        /// new name.
        name: String,
    },
    /// `file_rename_failed_alert` (8).
    FileRenameFailed {
        /// torrent.
        ih: Sha1Hash,
        /// `index`.
        index: i32,
        /// error.
        error: String,
    },
    /// `performance_alert` — disk queue / buffer pressure (9).
    Performance {
        /// warning code/message.
        warning: String,
    },
    /// `state_changed_alert` — paused→downloading etc. (10).
    StateChanged {
        /// torrent.
        ih: Sha1Hash,
        /// previous state name.
        prev: String,
        /// new state name.
        next: String,
    },
    /// `tracker_error_alert` (11).
    TrackerError {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url.
        url: String,
        /// failure message.
        error: String,
    },
    /// `tracker_warning_alert` (12).
    TrackerWarning {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url.
        url: String,
        /// warning message.
        msg: String,
        /// `times_in_row`.
        times_in_row: i32,
    },
    /// `scrape_reply_alert` (13).
    ScrapeReply {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url.
        url: String,
        /// `complete`.
        complete: i64,
        /// `incomplete`.
        incomplete: i64,
    },
    /// `scrape_failed_alert` (14).
    ScrapeFailed {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url.
        url: String,
        /// reason.
        reason: String,
    },
    /// `tracker_reply_alert` (15).
    TrackerReply {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url.
        url: String,
        /// `num_peers`.
        num_peers: usize,
    },
    /// `dht_reply_alert` — DHT announce result counted as a tracker
    /// reply upstream (16).
    DhtReply {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url ("DHT").
        url: String,
        /// `num_peers`.
        num_peers: i32,
    },
    /// `tracker_announce_alert` — announce sent (17).
    TrackerAnnounce {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url.
        url: String,
        /// `event` (announce event code).
        event: u8,
    },
    /// `hash_failed_alert` — a piece failed verification (18).
    HashFailed {
        /// torrent.
        ih: Sha1Hash,
        /// piece.
        piece: PieceIndex,
    },
    /// `peer_ban_alert` — `ban_peer` / `ip_filter` dropped a peer (19).
    PeerBan {
        /// torrent.
        ih: Sha1Hash,
        /// `ip` — the banned endpoint.
        ip: SocketAddr,
    },
    /// `peer_unsnubbed_alert` (20).
    PeerUnsnubbed {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
    },
    /// `peer_snubbed_alert` (21).
    PeerSnubbed {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
    },
    /// `peer_error_alert` — transport-level peer failure (22).
    PeerError {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// `op` — failing operation.
        op: String,
        /// error message.
        error: String,
    },
    /// `peer_connect_alert` (23).
    PeerConnect {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
    },
    /// `peer_disconnected_alert` (24).
    PeerDisconnected {
        /// torrent.
        ih: Sha1Hash,
        /// disconnect reason.
        reason: String,
    },
    /// `invalid_request_alert` — malformed/unsolicited request (25).
    InvalidRequest {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// offending request description.
        msg: String,
    },
    /// `torrent_finished_alert` (26).
    TorrentFinished(Sha1Hash),
    /// `piece_finished_alert` (27).
    PieceFinished {
        /// torrent.
        ih: Sha1Hash,
        /// piece.
        piece: PieceIndex,
    },
    /// `request_dropped_alert` (28).
    RequestDropped {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// piece.
        piece: PieceIndex,
        /// block.
        block: u32,
    },
    /// `block_timeout_alert` (29).
    BlockTimeout {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// piece.
        piece: PieceIndex,
        /// block.
        block: u32,
    },
    /// `block_finished_alert` (30).
    BlockFinished {
        /// torrent.
        ih: Sha1Hash,
        /// piece.
        piece: PieceIndex,
        /// block.
        block: u32,
    },
    /// `block_downloading_alert` (31).
    BlockDownloading {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// piece.
        piece: PieceIndex,
        /// block.
        block: u32,
    },
    /// `unwanted_block_alert` (32).
    UnwantedBlock {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// piece.
        piece: PieceIndex,
        /// block.
        block: u32,
    },
    /// `storage_moved_alert` (33).
    StorageMoved {
        /// torrent.
        ih: Sha1Hash,
        /// new save path.
        path: String,
    },
    /// `storage_moved_failed_alert` (34).
    StorageMovedFailed {
        /// torrent.
        ih: Sha1Hash,
        /// target path.
        path: String,
        /// error.
        error: String,
    },
    /// `torrent_deleted_alert` (35).
    TorrentDeleted(Sha1Hash),
    /// `torrent_delete_failed_alert` (36).
    TorrentDeleteFailed {
        /// torrent.
        ih: Sha1Hash,
        /// error.
        error: String,
    },
    /// `save_resume_data_alert` — blob ready to persist (37).
    SaveResumeData {
        /// torrent.
        ih: Sha1Hash,
        /// bencoded resume blob.
        data: Vec<u8>,
    },
    /// `save_resume_data_failed_alert` (38).
    SaveResumeDataFailed {
        /// torrent.
        ih: Sha1Hash,
        /// error.
        error: String,
    },
    /// `torrent_paused_alert` (39).
    TorrentPaused {
        /// torrent.
        ih: Sha1Hash,
    },
    /// `torrent_resumed_alert` (40).
    TorrentResumed {
        /// torrent.
        ih: Sha1Hash,
    },
    /// `torrent_checked_alert` — files_checked/recheck done (41).
    TorrentChecked(Sha1Hash),
    /// `url_seed_alert` — web seed connect/EOF/failure (42).
    UrlSeed {
        /// torrent.
        ih: Sha1Hash,
        /// seed url.
        url: String,
        /// event message.
        msg: String,
    },
    /// `file_error_alert` — disk error on a file (43).
    FileError {
        /// torrent.
        ih: Sha1Hash,
        /// file path.
        path: String,
        /// error.
        error: String,
    },
    /// `metadata_failed_alert` — ut_metadata hash failure (44).
    MetadataFailed {
        /// torrent.
        ih: Sha1Hash,
        /// error.
        error: String,
    },
    /// `metadata_received_alert` — magnet resolved (45).
    MetadataReceived {
        /// torrent.
        ih: Sha1Hash,
    },
    /// `udp_error_alert` — UDP socket failure (46).
    UdpError {
        /// remote endpoint.
        remote: String,
        /// failing operation.
        op: String,
        /// error.
        error: String,
    },
    /// `external_ip_alert` — `ip_voter` elected a new WAN address (47).
    ExternalIp {
        /// elected external address.
        addr: IpAddr,
    },
    /// `listen_failed_alert` (48).
    ListenFailed {
        /// interface endpoint attempted.
        remote: String,
        /// failing operation.
        op: String,
        /// error.
        error: String,
        /// `socket_type` kind.
        kind: u8,
    },
    /// `listen_succeeded_alert` (49).
    ListenSucceeded {
        /// bound endpoint.
        remote: String,
        /// `socket_type` kind.
        kind: u8,
    },
    /// `portmap_error_alert` (50).
    PortmapError {
        /// `mapping` handle the failure refers to.
        mapping: i32,
        /// `map_transport`: 0 = NAT-PMP, 1 = UPnP.
        transport: u8,
        /// `local_address` — interface the mapper runs on.
        local_addr: String,
        /// error.
        error: String,
    },
    /// `portmap_alert` (51).
    Portmap {
        /// `mapping` handle (`port_mapping_t` upstream).
        mapping: i32,
        /// `map_transport`: 0 = NAT-PMP, 1 = UPnP.
        transport: u8,
        /// `protocol`: 0 = none, 1 = TCP, 2 = UDP.
        protocol: u8,
        /// `external_port` allocated on the router.
        ext_port: u16,
        /// `local_address` — interface the mapper runs on.
        local_addr: String,
    },
    /// `portmap_log_alert` (52).
    PortmapLog {
        /// `map_transport`: 0 = NAT-PMP, 1 = UPnP.
        transport: u8,
        /// `local_address` — interface the mapper runs on.
        local_addr: String,
        /// log message.
        msg: String,
    },
    /// `fastresume_rejected_alert` (53).
    FastresumeRejected {
        /// torrent.
        ih: Sha1Hash,
        /// error.
        error: String,
        /// `op` (failing operation).
        op: String,
        /// `file` involved.
        file: String,
    },
    /// `peer_blocked_alert` — peer connection refused (54).
    PeerBlocked {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// block reason (`ip_filter` etc.).
        reason: String,
    },
    /// `dht_announce_alert` — we announced ourselves (55).
    DhtAnnounce {
        /// node endpoint.
        remote: String,
        /// port announced.
        port: u16,
        /// info-hash.
        ih: Sha1Hash,
        /// message.
        msg: String,
    },
    /// `dht_get_peers_alert` — incoming get_peers on our node (56).
    DhtGetPeers {
        /// info-hash.
        ih: Sha1Hash,
        /// message.
        msg: String,
    },
    /// `stats_alert` — periodic bandwidth counters (57).
    Stats {
        /// torrent (upstream carries `torrent_handle`).
        ih: Sha1Hash,
        /// `interval` in ms.
        interval_ms: u32,
        /// transferred counters snapshot.
        counters: Vec<i64>,
    },
    /// `cache_flushed_alert` (58).
    CacheFlushed(Sha1Hash),
    /// `anonymous_mode_alert` — anonymous-mode violation avoided (59).
    AnonymousMode {
        /// torrent.
        ih: Sha1Hash,
        /// `kind` (0 = tracker, 1 = peer).
        kind: u8,
        /// message.
        msg: String,
    },
    /// `lsd_peer_alert` — LSD found a local peer (60).
    LsdPeer {
        /// torrent.
        ih: Sha1Hash,
        /// peer endpoint.
        remote: String,
    },
    /// `trackerid_alert` — tracker returned a `trackerid` (61).
    Trackerid {
        /// torrent.
        ih: Sha1Hash,
        /// tracker url.
        url: String,
        /// `trackerid` value.
        trackerid: String,
    },
    /// `dht_bootstrap_alert` — first routing-table bootstrap done (62).
    DhtBootstrap {},
    /// `torrent_error_alert` (64).
    TorrentError {
        /// torrent.
        ih: Sha1Hash,
        /// error.
        error: String,
        /// `file` involved.
        file: String,
    },
    /// `torrent_need_cert_alert` — SSL torrent without cert (65).
    TorrentNeedCert {
        /// torrent.
        ih: Sha1Hash,
        /// error.
        error: String,
        /// cert file hint.
        cert_file: String,
    },
    /// `incoming_connection_alert` — inbound peer accepted (66).
    IncomingConnection {
        /// remote endpoint.
        remote: String,
        /// `socket_type` kind.
        kind: u8,
    },
    /// `add_torrent_alert` — `add_torrent`/`async_add_torrent` (67).
    AddTorrent {
        /// torrent (zeroed on hard failure).
        ih: Sha1Hash,
        /// `params` summary / error.
        error: String,
    },
    /// `state_update_alert` — `post_torrent_updates` payload (68).
    StateUpdate {
        /// one `torrent_status` per torrent.
        statuses: Vec<crate::status::TorrentStatus>,
    },
    /// `session_stats_alert` — `post_session_stats` snapshot (70).
    /// Category-0: posts unconditionally like upstream.
    SessionStats {
        /// `values[num_counters]` snapshot.
        stats: crate::counters::SessionStats,
    },
    /// `dht_error_alert` (73).
    DhtError {
        /// `operation` (0 = hostname lookup, 1 = routing lookup).
        op: u8,
        /// error.
        error: String,
    },
    /// `dht_immutable_item_alert` — BEP-44 immutable result (74).
    DhtImmutableItem {
        /// item target (`sha1(value)`).
        target: Sha1Hash,
        /// bencoded item value.
        value: Vec<u8>,
    },
    /// `dht_mutable_item_alert` — BEP-44 mutable result (75).
    /// `Box`ed: 170+ B payload keeps `size_of::<Alert>()` small.
    DhtMutableItem(Box<DhtMutableItemAlert>),
    /// `dht_put_alert` — BEP-44 put completed (76). `Box`ed, same
    /// reason.
    DhtPut(Box<DhtPutAlert>),
    /// `i2p_alert` — I2P failure (77).
    I2p {
        /// error.
        error: String,
    },
    /// `dht_outgoing_get_peers_alert` — `get_peers` query sent (78).
    DhtOutgoingGetPeers {
        /// info-hash.
        ih: Sha1Hash,
        /// `obfuscated_info_hash`.
        obfuscated: Sha1Hash,
        /// `endpoint` the query went to.
        ep: SocketAddr,
    },
    /// `log_alert` — session log (79).
    Log {
        /// message.
        msg: String,
    },
    /// `torrent_log_alert` (80).
    TorrentLog {
        /// torrent.
        ih: Sha1Hash,
        /// message.
        msg: String,
    },
    /// `peer_log_alert` (81).
    PeerLog {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// message.
        msg: String,
    },
    /// `lsd_error_alert` (82).
    LsdError {
        /// error.
        error: String,
    },
    /// `dht_stats_alert` — routing snapshot on `post_dht_stats` (83).
    /// Category-0: unconditional.
    DhtStats {
        /// v4 table stats (`Box`ed — two inline stats would make
        /// `Alert` ~120 B).
        v4: Box<DhtNodeStats>,
        /// v6 table stats.
        v6: Box<DhtNodeStats>,
    },
    /// `incoming_request_alert` — peer requested a block (84).
    IncomingRequest {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// piece.
        piece: PieceIndex,
        /// `start` offset.
        start: i32,
        /// `length`.
        length: i32,
    },
    /// `dht_log_alert` (85).
    DhtLog {
        /// `dht_module_t` (0 = tracker, 1 = node, 2 = routing_table,
        /// 3 = rpc_manager, 4 = traversal).
        module: u8,
        /// message.
        msg: String,
    },
    /// `dht_pkt_alert` — a DHT datagram (86).
    DhtPacket {
        /// `direction` (0 = incoming, 1 = outgoing).
        direction: u8,
        /// node endpoint.
        remote: String,
        /// datagram payload.
        msg: Vec<u8>,
    },
    /// `dht_get_peers_reply_alert` — user get_peers result (87).
    DhtGetPeersReply {
        /// info-hash.
        ih: Sha1Hash,
        /// `num_peers`.
        num_peers: u32,
        /// peer endpoints.
        peers: Vec<SocketAddr>,
    },
    /// `dht_direct_response_alert` — direct_request reply (88).
    DhtDirectResponse {
        /// node endpoint.
        remote: String,
        /// `userdata` tag echo.
        user_data: u64,
        /// `response` (bencoded).
        response: Vec<u8>,
    },
    /// `picker_log_alert` — piece-picker state dump (89).
    PickerLog {
        /// torrent.
        ih: Sha1Hash,
        /// `picker_flags`.
        picker_flags: u8,
        /// `blocks` (bitfield).
        blocks: Vec<u8>,
    },
    /// `session_error_alert` (90).
    SessionError(String),
    /// `dht_live_nodes_alert` — live nodes for an info-hash (91).
    DhtLiveNodes {
        /// info-hash.
        ih: Sha1Hash,
        /// `(node_id, endpoint)` pairs.
        nodes: Vec<(bitdaemon_dht::node_id::NodeId, SocketAddr)>,
    },
    /// `session_stats_header_alert` — header line precedes the first
    /// `SessionStats` (92). Category-0: unconditional.
    SessionStatsHeader {},
    /// `dht_sample_infohashes_alert` — BEP-51 result (93).
    /// `Box`ed: ~120 B payload keeps `size_of::<Alert>()` small.
    DhtSampleInfohashes(Box<DhtSampleInfohashesAlert>),
    /// `block_uploaded_alert` — block sent to a peer (94).
    BlockUploaded {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        remote: String,
        /// piece.
        piece: PieceIndex,
        /// block.
        block: u32,
    },
    /// `alerts_dropped_alert` — synthesized when the queue overflowed;
    /// `types` is the sorted set of dropped `alert_type` ids (95).
    AlertsDropped {
        /// `dropped_alerts` — sorted `alert_type` ids.
        types: Vec<u32>,
    },
    /// `socks5_alert` — SOCKS proxy failure (96).
    Socks5 {
        /// proxy endpoint.
        remote: String,
        /// failing operation.
        op: String,
        /// error.
        error: String,
    },
    /// `file_prio_alert` — file-priority change applied (97).
    FilePrio {
        /// torrent.
        ih: Sha1Hash,
        /// `op`.
        op: String,
        /// error (empty = success).
        error: String,
    },
    /// `oversized_file_alert` — file exceeds fs limits (98).
    OversizedFile {
        /// torrent.
        ih: Sha1Hash,
        /// file name.
        file: String,
    },
    /// `torrent_conflict_alert` — duplicate info-hash (99).
    TorrentConflict {
        /// existing torrent's info-hash.
        ih: Sha1Hash,
        /// conflicting info-hash (when known distinct).
        other: Option<Sha1Hash>,
        /// source url/magnet.
        url: String,
    },
    /// `peer_info_alert` — `torrent_handle::post_peer_info` (100).
    PeerInfo {
        /// torrent.
        ih: Sha1Hash,
        /// one `peer_info` per live connection.
        peers: Vec<crate::peer_info::PeerInfo>,
    },
    /// `file_progress_alert` — `post_file_progress` (101).
    FileProgress {
        /// torrent.
        ih: Sha1Hash,
        /// bytes downloaded per file.
        files: Vec<i64>,
    },
    /// `piece_info_alert` — `post_download_queue` (102).
    PieceInfo {
        /// torrent.
        ih: Sha1Hash,
        /// `partial_piece_info` equivalents.
        queue: Vec<crate::piece_picker::DownloadingPiece>,
    },
    /// `piece_availability_alert` — `post_piece_availability` (103).
    PieceAvailability {
        /// torrent.
        ih: Sha1Hash,
        /// swarm availability per piece.
        availability: Vec<i32>,
    },
    /// `tracker_list_alert` — `post_trackers` (104).
    TrackerList {
        /// torrent.
        ih: Sha1Hash,
        /// `(url, tier)` announce entries.
        trackers: Vec<(String, i32)>,
    },
    /// `file_priorities_alert` — `post_file_priorities` (105).
    FilePriorities {
        /// torrent.
        ih: Sha1Hash,
        /// `download_priority_t` per file.
        priorities: Vec<bitdaemon_common::types::DownloadPriority>,
    },
    /// `file_status_alert` — `post_file_status` (106).
    FileStatus {
        /// torrent.
        ih: Sha1Hash,
        /// `(file_index, open_mode flags)` per open file.
        state: Vec<(i32, u8)>,
    },
    /// `ip_ban_alert` — IP banned by `ip_filter` (107).
    IpBan {
        /// banned endpoint.
        remote: String,
        /// `reason` (`address_filter`/`port_filter`).
        reason: String,
    },
}

/// `dht_mutable_item_alert` payload (kept boxed — ~170 B).
#[derive(Clone, Debug)]
pub struct DhtMutableItemAlert {
    /// item target.
    pub target: Sha1Hash,
    /// `key` — ed25519 public key.
    pub key: [u8; 32],
    /// `signature`.
    pub signature: [u8; 64],
    /// `seq`.
    pub seq: i64,
    /// `salt`.
    pub salt: Vec<u8>,
    /// `item` (bencoded).
    pub value: Vec<u8>,
    /// `authoritative` — signature verified.
    pub authoritative: bool,
}

/// per-family `dht_stats_alert` payload — `routing_table` summary +
/// `nid` + `active_requests` count (upstream carries the full bucket
/// vector; the counters cover the same observability surface).
#[derive(Clone, Debug, Default)]
pub struct DhtNodeStats {
    /// this node's id (`nid` upstream).
    pub nid: bitdaemon_dht::node_id::NodeId,
    /// live nodes in the routing table.
    pub nodes: usize,
    /// replacement-cache entries.
    pub replacements: usize,
    /// bucket-verified nodes.
    pub confirmed: usize,
    /// running traversals (`active_requests` upstream).
    pub active_lookups: usize,
}

/// `dht_sample_infohashes_alert` payload (kept boxed — ~120 B).
#[derive(Clone, Debug)]
pub struct DhtSampleInfohashesAlert {
    /// `node_id` — responder's id.
    pub node_id: Sha1Hash,
    /// `endpoint` the response came from.
    pub remote: String,
    /// `interval` (seconds) to wait before sampling again.
    pub interval_secs: u32,
    /// `num` — infohashes the node claims to store.
    pub num: i64,
    /// sampled info-hashes.
    pub samples: Vec<Sha1Hash>,
    /// `(node id, endpoint)` pairs the reply carried.
    pub nodes: Vec<(bitdaemon_dht::node_id::NodeId, SocketAddr)>,
}

/// `dht_put_alert` payload (kept boxed — ~150 B).
#[derive(Clone, Debug)]
pub struct DhtPutAlert {
    /// item target.
    pub target: Sha1Hash,
    /// `num_success` nodes that stored it.
    pub num_success: u32,
    /// mutable `public_key` (zeroes for immutable).
    pub public_key: [u8; 32],
    /// mutable `signature` (zeroes for immutable).
    pub signature: [u8; 64],
    /// mutable `seq` (0 for immutable).
    pub seq: i64,
    /// mutable `salt`.
    pub salt: Vec<u8>,
}

const fn or(a: AlertCategory, b: AlertCategory) -> AlertCategory {
    AlertCategory(a.0 | b.0)
}

impl Alert {
    /// `alert::type()` → upstream `*_alert::alert_type` id. Ids
    /// 1, 2, 63, 71, 72 are upstream gaps (alert types removed in
    /// ABI v2); 69 is `mmap_cache_alert`, which only exists under
    /// `TORRENT_ABI_VERSION == 1` upstream — both intentionally
    /// absent here.
    #[must_use]
    pub const fn alert_type(&self) -> u32 {
        match self {
            Self::TorrentAdded(_) => 3,
            Self::TorrentRemoved(_) => 4,
            Self::ReadPiece { .. } | Self::ReadPieceFailed { .. } => 5,
            Self::FileCompleted { .. } => 6,
            Self::FileRenamed { .. } => 7,
            Self::FileRenameFailed { .. } => 8,
            Self::Performance { .. } => 9,
            Self::StateChanged { .. } => 10,
            Self::TrackerError { .. } => 11,
            Self::TrackerWarning { .. } => 12,
            Self::ScrapeReply { .. } => 13,
            Self::ScrapeFailed { .. } => 14,
            Self::TrackerReply { .. } => 15,
            Self::DhtReply { .. } => 16,
            Self::TrackerAnnounce { .. } => 17,
            Self::HashFailed { .. } => 18,
            Self::PeerBan { .. } => 19,
            Self::PeerUnsnubbed { .. } => 20,
            Self::PeerSnubbed { .. } => 21,
            Self::PeerError { .. } => 22,
            Self::PeerConnect { .. } => 23,
            Self::PeerDisconnected { .. } => 24,
            Self::InvalidRequest { .. } => 25,
            Self::TorrentFinished(_) => 26,
            Self::PieceFinished { .. } => 27,
            Self::RequestDropped { .. } => 28,
            Self::BlockTimeout { .. } => 29,
            Self::BlockFinished { .. } => 30,
            Self::BlockDownloading { .. } => 31,
            Self::UnwantedBlock { .. } => 32,
            Self::StorageMoved { .. } => 33,
            Self::StorageMovedFailed { .. } => 34,
            Self::TorrentDeleted(_) => 35,
            Self::TorrentDeleteFailed { .. } => 36,
            Self::SaveResumeData { .. } => 37,
            Self::SaveResumeDataFailed { .. } => 38,
            Self::TorrentPaused { .. } => 39,
            Self::TorrentResumed { .. } => 40,
            Self::TorrentChecked(_) => 41,
            Self::UrlSeed { .. } => 42,
            Self::FileError { .. } => 43,
            Self::MetadataFailed { .. } => 44,
            Self::MetadataReceived { .. } => 45,
            Self::UdpError { .. } => 46,
            Self::ExternalIp { .. } => 47,
            Self::ListenFailed { .. } => 48,
            Self::ListenSucceeded { .. } => 49,
            Self::PortmapError { .. } => 50,
            Self::Portmap { .. } => 51,
            Self::PortmapLog { .. } => 52,
            Self::FastresumeRejected { .. } => 53,
            Self::PeerBlocked { .. } => 54,
            Self::DhtAnnounce { .. } => 55,
            Self::DhtGetPeers { .. } => 56,
            Self::Stats { .. } => 57,
            Self::CacheFlushed(_) => 58,
            Self::AnonymousMode { .. } => 59,
            Self::LsdPeer { .. } => 60,
            Self::Trackerid { .. } => 61,
            Self::DhtBootstrap {} => 62,
            Self::TorrentError { .. } => 64,
            Self::TorrentNeedCert { .. } => 65,
            Self::IncomingConnection { .. } => 66,
            Self::AddTorrent { .. } => 67,
            Self::StateUpdate { .. } => 68,
            Self::SessionStats { .. } => 70,
            Self::DhtError { .. } => 73,
            Self::DhtImmutableItem { .. } => 74,
            Self::DhtMutableItem { .. } => 75,
            Self::DhtPut { .. } => 76,
            Self::I2p { .. } => 77,
            Self::DhtOutgoingGetPeers { .. } => 78,
            Self::Log { .. } => 79,
            Self::TorrentLog { .. } => 80,
            Self::PeerLog { .. } => 81,
            Self::LsdError { .. } => 82,
            Self::DhtStats { .. } => 83,
            Self::IncomingRequest { .. } => 84,
            Self::DhtLog { .. } => 85,
            Self::DhtPacket { .. } => 86,
            Self::DhtGetPeersReply { .. } => 87,
            Self::DhtDirectResponse { .. } => 88,
            Self::PickerLog { .. } => 89,
            Self::SessionError(_) => 90,
            Self::DhtLiveNodes { .. } => 91,
            Self::SessionStatsHeader {} => 92,
            Self::DhtSampleInfohashes { .. } => 93,
            Self::BlockUploaded { .. } => 94,
            Self::AlertsDropped { .. } => 95,
            Self::Socks5 { .. } => 96,
            Self::FilePrio { .. } => 97,
            Self::OversizedFile { .. } => 98,
            Self::TorrentConflict { .. } => 99,
            Self::PeerInfo { .. } => 100,
            Self::FileProgress { .. } => 101,
            Self::PieceInfo { .. } => 102,
            Self::PieceAvailability { .. } => 103,
            Self::TrackerList { .. } => 104,
            Self::FilePriorities { .. } => 105,
            Self::FileStatus { .. } => 106,
            Self::IpBan { .. } => 107,
        }
    }

    /// `alert_priority` — upstream `*_alert::priority`.
    #[must_use]
    pub const fn priority(&self) -> AlertPriority {
        match self {
            Self::TorrentRemoved(_)
            | Self::ReadPiece { .. }
            | Self::ReadPieceFailed { .. }
            | Self::FileRenamed { .. }
            | Self::FileRenameFailed { .. }
            | Self::ScrapeReply { .. }
            | Self::ScrapeFailed { .. }
            | Self::StorageMoved { .. }
            | Self::StorageMovedFailed { .. }
            | Self::TorrentDeleted(_)
            | Self::TorrentDeleteFailed { .. }
            | Self::SaveResumeData { .. }
            | Self::SaveResumeDataFailed { .. }
            | Self::ListenFailed { .. }
            | Self::ListenSucceeded { .. }
            | Self::FastresumeRejected { .. }
            | Self::TorrentNeedCert { .. }
            | Self::AddTorrent { .. }
            | Self::SessionStats { .. }
            | Self::DhtImmutableItem { .. }
            | Self::DhtMutableItem { .. }
            | Self::DhtDirectResponse { .. }
            | Self::PeerInfo { .. }
            | Self::FileProgress { .. }
            | Self::PieceInfo { .. }
            | Self::PieceAvailability { .. }
            | Self::TrackerList { .. }
            | Self::FilePriorities { .. }
            | Self::FileStatus { .. } => AlertPriority::Critical,
            Self::StateChanged { .. }
            | Self::TrackerError { .. }
            | Self::TorrentFinished(_)
            | Self::TorrentPaused { .. }
            | Self::TorrentResumed { .. }
            | Self::TorrentChecked(_)
            | Self::FileError { .. }
            | Self::CacheFlushed(_)
            | Self::TorrentError { .. }
            | Self::StateUpdate { .. }
            | Self::TorrentConflict { .. } => AlertPriority::High,
            Self::AlertsDropped { .. } => AlertPriority::Meta,
            _ => AlertPriority::Normal,
        }
    }

    /// `alert::category()` — upstream `static_category` verbatim.
    #[must_use]
    pub fn category(&self) -> AlertCategory {
        match self {
            Self::TorrentAdded(_)
            | Self::TorrentRemoved(_)
            | Self::StateChanged { .. }
            | Self::HashFailed { .. }
            | Self::TorrentFinished(_)
            | Self::TorrentPaused { .. }
            | Self::TorrentResumed { .. }
            | Self::TorrentChecked(_)
            | Self::MetadataReceived { .. }
            | Self::ExternalIp { .. }
            | Self::ListenSucceeded { .. }
            | Self::Trackerid { .. }
            | Self::TorrentNeedCert { .. }
            | Self::AddTorrent { .. }
            | Self::StateUpdate { .. }
            | Self::PieceAvailability { .. }
            | Self::TrackerList { .. }
            | Self::FilePriorities { .. }
            | Self::FileStatus { .. }
            | Self::PeerInfo { .. } => AlertCategory::STATUS,
            Self::ReadPiece { .. }
            | Self::ReadPieceFailed { .. }
            | Self::FileRenamed { .. }
            | Self::FileRenameFailed { .. }
            | Self::StorageMoved { .. }
            | Self::StorageMovedFailed { .. }
            | Self::TorrentDeleted(_)
            | Self::SaveResumeData { .. }
            | Self::CacheFlushed(_)
            | Self::FilePrio { .. }
            | Self::OversizedFile { .. } => AlertCategory::STORAGE,
            Self::FileCompleted { .. } | Self::FileProgress { .. } => AlertCategory::FILE_PROGRESS,
            Self::Performance { .. } => AlertCategory::PERFORMANCE,
            Self::TrackerError { .. } | Self::TrackerWarning { .. } => {
                or(AlertCategory::TRACKER, AlertCategory::ERROR)
            }
            Self::ScrapeReply { .. } | Self::TrackerReply { .. } | Self::TrackerAnnounce { .. } => {
                AlertCategory::TRACKER
            }
            Self::ScrapeFailed { .. } => or(AlertCategory::TRACKER, AlertCategory::ERROR),
            Self::DhtReply { .. } => or(AlertCategory::DHT, AlertCategory::TRACKER),
            Self::PeerBan { .. }
            | Self::PeerUnsnubbed { .. }
            | Self::PeerSnubbed { .. }
            | Self::PeerError { .. }
            | Self::InvalidRequest { .. }
            | Self::UnwantedBlock { .. }
            | Self::IncomingConnection { .. }
            | Self::LsdPeer { .. } => AlertCategory::PEER,
            Self::PeerConnect { .. } | Self::PeerDisconnected { .. } => AlertCategory::CONNECT,
            Self::PieceFinished { .. } | Self::PieceInfo { .. } => AlertCategory::PIECE_PROGRESS,
            Self::RequestDropped { .. } | Self::BlockTimeout { .. } => {
                or(AlertCategory::BLOCK_PROGRESS, AlertCategory::PEER)
            }
            Self::BlockFinished { .. } | Self::BlockDownloading { .. } => {
                AlertCategory::BLOCK_PROGRESS
            }
            Self::TorrentDeleteFailed { .. } | Self::SaveResumeDataFailed { .. } => {
                or(AlertCategory::STORAGE, AlertCategory::ERROR)
            }
            Self::UrlSeed { .. } => or(AlertCategory::PEER, AlertCategory::ERROR),
            Self::FileError { .. } => or(
                or(AlertCategory::STATUS, AlertCategory::ERROR),
                AlertCategory::STORAGE,
            ),
            Self::MetadataFailed { .. }
            | Self::UdpError { .. }
            | Self::AnonymousMode { .. }
            | Self::I2p { .. }
            | Self::LsdError { .. }
            | Self::SessionError(_)
            | Self::AlertsDropped { .. }
            | Self::Socks5 { .. } => AlertCategory::ERROR,
            Self::ListenFailed { .. } | Self::FastresumeRejected { .. } => {
                or(AlertCategory::STATUS, AlertCategory::ERROR)
            }
            Self::PortmapError { .. } => or(AlertCategory::PORT_MAPPING, AlertCategory::ERROR),
            Self::Portmap { .. } => AlertCategory::PORT_MAPPING,
            Self::PortmapLog { .. } => AlertCategory::PORT_MAPPING_LOG,
            Self::PeerBlocked { .. } | Self::IpBan { .. } => AlertCategory::IP_BLOCK,
            Self::DhtAnnounce { .. }
            | Self::DhtGetPeers { .. }
            | Self::DhtBootstrap {}
            | Self::DhtImmutableItem { .. }
            | Self::DhtMutableItem { .. }
            | Self::DhtPut { .. }
            | Self::DhtOutgoingGetPeers { .. }
            | Self::DhtDirectResponse { .. }
            | Self::DhtLiveNodes { .. } => AlertCategory::DHT,
            Self::Stats { .. } => AlertCategory::STATS,
            Self::TorrentError { .. } => or(AlertCategory::ERROR, AlertCategory::STATUS),
            Self::TorrentConflict { .. } => AlertCategory::ERROR,
            Self::DhtError { .. } => or(AlertCategory::ERROR, AlertCategory::DHT),
            Self::Log { .. } => AlertCategory::SESSION_LOG,
            Self::TorrentLog { .. } => AlertCategory::TORRENT_LOG,
            Self::PeerLog { .. } => AlertCategory::PEER_LOG,
            Self::IncomingRequest { .. } => AlertCategory::INCOMING_REQUEST,
            Self::DhtLog { .. } | Self::DhtPacket { .. } => AlertCategory::DHT_LOG,
            Self::DhtGetPeersReply { .. } | Self::DhtSampleInfohashes { .. } => {
                AlertCategory::DHT_OPERATION
            }
            Self::PickerLog { .. } => AlertCategory::PICKER_LOG,
            Self::BlockUploaded { .. } => AlertCategory::UPLOAD,
            // category-0 alerts — post unconditionally (D7)
            Self::SessionStats { .. } | Self::SessionStatsHeader {} | Self::DhtStats { .. } => {
                AlertCategory(0)
            }
        }
    }
}

impl AlertCategory {
    /// `category & category` bit-or (upstream categories compose).
    #[must_use]
    pub const fn or(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
    /// `should_notify` — mask test.
    #[must_use]
    pub const fn matches(self, mask: Self) -> bool {
        self.0 & mask.0 != 0
    }
}

/// `alert_manager` — the double-buffered queue + mask + notify
/// callback + extension fan-out (upstream uses a condition variable;
/// the `notify` hook wakes the owning actor instead).
pub struct AlertManager {
    /// `m_alert_mask` — upstream default `alert_category::error`.
    mask: AlertCategory,
    /// `m_queue_size_limit`.
    queue_limit: usize,
    /// `m_alerts[m_generation & 1]` — the pending queue.
    pending: Vec<Alert>,
    /// `m_notify` — invoked when an empty queue becomes non-empty.
    notify: Option<Box<dyn Fn() + Send>>,
    /// `m_dropped` — per-type set of dropped `alert_type` ids
    /// (upstream bitfield indexed by `alert::type()`).
    dropped: BTreeSet<u32>,
    /// `m_ses_extensions` — `plugin::on_alert` fan-out targets.
    extensions: Vec<std::sync::Arc<dyn SessionPlugin>>,
}

impl Default for AlertManager {
    /// Upstream defaults: `m_alert_mask = alert_category::error`,
    /// `alert_queue_size = 2000`.
    fn default() -> Self {
        Self {
            mask: AlertCategory::ERROR,
            queue_limit: 2000,
            pending: Vec::new(),
            notify: None,
            dropped: BTreeSet::new(),
            extensions: Vec::new(),
        }
    }
}

impl AlertManager {
    /// `alert_manager(queue_limit, mask)`.
    #[must_use]
    pub fn new(queue_limit: usize, mask: AlertCategory) -> Self {
        Self {
            mask,
            queue_limit,
            ..Default::default()
        }
    }

    /// `settings_pack::alert_queue_size` — hot-applied by
    /// `apply_settings` (upstream `update_alert_queue_size`).
    pub fn set_queue_limit(&mut self, limit: usize) {
        self.queue_limit = limit;
    }

    /// `set_alert_mask` — which categories get queued (upstream
    /// `update_alert_mask` also persists it in `settings_pack`).
    pub fn set_mask(&mut self, mask: AlertCategory) {
        self.mask = mask;
    }

    /// `alert_manager::alert_mask`.
    #[must_use]
    pub fn mask(&self) -> AlertCategory {
        self.mask
    }

    /// `set_notify_callback`.
    pub fn set_notify(&mut self, cb: impl Fn() + Send + 'static) {
        self.notify = Some(Box::new(cb));
    }

    /// `alert_manager::add_extension` — every posted alert fans out
    /// to `plugin::on_alert`.
    pub fn add_extension(&mut self, ext: std::sync::Arc<dyn SessionPlugin>) {
        self.extensions.push(ext);
    }

    /// `should_post` — upstream's call-site mask gate. Equivalent to
    /// `post`'s internal check; exposed for sites that want to skip
    /// building a payload when it's masked off (upstream idiom).
    #[must_use]
    pub fn should_post(&self, cat: AlertCategory) -> bool {
        cat.matches(self.mask)
    }

    /// `emplace_alert` — unconditional enqueue like upstream (the
    /// mask is a call-site concern). Category-0 alerts
    /// (`SessionStats`, `SessionStatsHeader`, `DhtStats`) always land.
    /// Queue limit is priority-weighted:
    /// `queue.size() / (1 + priority) >= limit`.
    pub fn post(&mut self, a: Alert) {
        let cat = a.category();
        if cat.0 != 0 && !cat.matches(self.mask) {
            return;
        }
        self.post_unconditional(a);
    }

    /// `emplace_alert` without the mask gate — upstream calls
    /// `should_post<T>() || req.triggered_manually` for
    /// `tracker_reply`/`scrape_reply` so a client-initiated
    /// announce always reports its result.
    pub fn post_unconditional(&mut self, a: Alert) {
        if self.pending.len() / (1 + a.priority() as usize) >= self.queue_limit {
            self.dropped.insert(a.alert_type());
            return;
        }
        let was_empty = self.pending.is_empty();
        for ext in &self.extensions {
            ext.on_alert(&a);
        }
        self.pending.push(a);
        if was_empty {
            if let Some(cb) = &self.notify {
                cb();
            }
        }
    }

    /// `get_all` — swap the queue out for the caller; a synthesized
    /// `AlertsDropped` rides the tail when types were dropped since
    /// the last drain.
    pub fn get_all(&mut self) -> Vec<Alert> {
        let mut out = std::mem::take(&mut self.pending);
        if !self.dropped.is_empty() {
            let types: Vec<u32> = self.dropped.iter().copied().collect();
            self.dropped.clear();
            out.push(Alert::AlertsDropped { types });
        }
        out
    }

    /// `alerts_dropped` count (diagnostic).
    #[must_use]
    pub fn num_dropped(&self) -> usize {
        self.dropped.len()
    }

    /// `pending()`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// `plugin`/`ses_extension` — session-level extension point.
/// OnionBit attaches IPv8/peer-class plugins here.
///
/// When stored on a `Torrent` (via
/// `TorrentHandle::add_extension`) the plugin plays the
/// `torrent_plugin` role: the lifecycle hooks below are driven
/// from the torrent's tick/peer paths (upstream
/// `torrent_plugin.hpp`).
pub trait SessionPlugin: Send + Sync {
    /// `plugin::on_alert` — every posted alert fans out here.
    fn on_alert(&self, _a: &Alert) {}

    /// `torrent_plugin::new_connection` — a peer was added to the
    /// torrent; returning a `PeerPlugin` attaches it to that
    /// connection (upstream `peer_connection::add_extension`).
    fn new_peer_connection(
        &self,
        _remote: &std::net::SocketAddr,
    ) -> Option<std::sync::Arc<dyn PeerPlugin>> {
        None
    }

    /// `torrent_plugin::on_piece_pass` — piece `index` verified.
    fn on_piece_pass(&self, _index: PieceIndex) {}

    /// `torrent_plugin::on_piece_failed` — piece `index` failed
    /// hashing.
    fn on_piece_failed(&self, _index: PieceIndex) {}

    /// `torrent_plugin::on_files_checked` — storage check
    /// completed (also invoked on `add_extension` when the torrent
    /// was already checked).
    fn on_files_checked(&self) {}

    /// `torrent_plugin::on_pause` — torrent is pausing; returning
    /// `true` vetoes the pause (first true wins, upstream iterates
    /// and stops).
    fn on_pause(&self) -> bool {
        false
    }

    /// `torrent_plugin::on_resume` — torrent is resuming;
    /// returning `true` vetoes the resume machinery (upstream
    /// `do_resume` iterates and stops on the first true).
    fn on_resume(&self) -> bool {
        false
    }

    /// `torrent_plugin::tick` — driven once per session tick.
    fn tick(&self) {}
}

/// `peer_plugin` — per-connection extension produced by
/// `SessionPlugin::new_peer_connection` (upstream
/// `peer_plugin.hpp` — scoped port: `on_message`/`on_tick`
/// cover the consumer-facing hooks; the upstream per-message
/// `on_*` family collapses into `on_message`). `Debug` is a
/// supertrait so `PeerConn` can stay `#[derive(Debug)]`.
pub trait PeerPlugin: Send + Sync + std::fmt::Debug {
    /// `peer_plugin::on_*` message family — invoked for every
    /// incoming wire message after it has been decoded
    /// (`on_have`, `on_request`, `on_piece`, `on_bitfield`, …
    /// upstream). The plugin observes, it cannot alter dispatch.
    fn on_message(&self, _msg: &bitdaemon_net::peer_wire::PeerMessage) {}

    /// `peer_plugin::tick` — driven once per session tick.
    fn on_tick(&self) {}
}

#[cfg(test)]
mod size_guard {
    use super::Alert;
    /// Keep the enum small enough to queue cheaply — `Box` any
    /// variant payload that would exceed this (ADR-0023 D1). The
    /// floor is set by 3-`String`/`Vec` variants (~92 B).
    #[test]
    fn alert_stays_small() {
        assert!(
            size_of::<Alert>() <= 96,
            "Alert grew to {}",
            size_of::<Alert>()
        );
    }
}
