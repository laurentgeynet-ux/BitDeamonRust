// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `performance_counters.cpp`/`session_stats.cpp` +
//! `session_stats.hpp`/`performance_counters.hpp`.
//!
//! `performance_counters`/`session_stats` — session metrics.
//! Upstream keeps one `std::array<std::atomic<int64>>` indexed by
//! `stats_counter_t`; we port the counter enum subset the ported
//! machinery emits + a typed `SessionStats` snapshot.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bitdaemon_net::utp_socket::{UtpCounterSink, UtpStat};

/// `bt_peer_connection::dispatch_message`'s packet counter mapping —
/// the upstream quirk verbatim: wire ids 0..=9 land on
/// `num_incoming_choke + id`; the FAST ids 13..=17 land on
/// `num_incoming_suggest + id` (offsets `num_incoming_suggest+13`
/// overflows into the `num_outgoing_*` range — kept as-is); every
/// other decoded type lands on `num_incoming_extended`. Keepalives
/// carry no id and are never counted; a *complete* frame that fails
/// size validation still counts (the bump runs after the handler
/// breaks out), an unknown id does not (the `default` leg returns
/// early).
#[must_use]
pub fn incoming_counter_idx(id: u8) -> usize {
    if id <= 9 {
        Counter::NumIncomingChoke as usize + id as usize
    } else if (13..=17).contains(&id) {
        Counter::NumIncomingSuggest as usize + id as usize
    } else {
        Counter::NumIncomingExtended as usize
    }
}

/// `peer_connection::disconnect`'s per-reason counter — the
/// `ec == errors::*` legs upstream runs on every teardown (in
/// addition to the transport-kind legs). `reason` is the upstream
/// error name our disconnect sites emit (`"self_connection"`,
/// `"too_many_connections"`, …).
#[must_use]
pub fn disconnect_reason_counter(reason: &str) -> Option<Counter> {
    Some(match reason {
        // `upload_upload_connection || uninteresting_upload_peer ||
        // torrent_aborted || self_connection || torrent_paused`
        // upstream — closes that carry no blame.
        "self_connection"
        | "upload-upload"
        | "torrent_paused"
        | "session_closing"
        | "torrent_aborted"
        | "uninteresting_upload_peer" => Counter::UninterestingPeers,
        "too_many_connections" => Counter::TooManyPeers,
        "out of memory" => Counter::NoMemoryPeers,
        "timed_out" | "transport timed out" => Counter::TransportTimeoutPeers,
        "timed_out_inactivity" | "timed_out_no_request" | "timed_out_no_interest" => {
            Counter::TimeoutPeers
        }
        "timed_out_no_handshake" => Counter::ConnectTimeouts,
        _ => return None,
    })
}

/// `stats_counter_t` + `stats_gauge_t` — the full upstream index set
/// (`num_stats_counters = 235` splits stats from gauges; the bank is
/// one flat array like upstream's `counters`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Counter {
    /// `error_peers`.
    ErrorPeers,
    /// `disconnected_peers`.
    DisconnectedPeers,
    /// `eof_peers`.
    EofPeers,
    /// `connreset_peers`.
    ConnresetPeers,
    /// `connrefused_peers`.
    ConnrefusedPeers,
    /// `connaborted_peers`.
    ConnabortedPeers,
    /// `notconnected_peers`.
    NotconnectedPeers,
    /// `perm_peers`.
    PermPeers,
    /// `buffer_peers`.
    BufferPeers,
    /// `unreachable_peers`.
    UnreachablePeers,
    /// `broken_pipe_peers`.
    BrokenPipePeers,
    /// `addrinuse_peers`.
    AddrinusePeers,
    /// `no_access_peers`.
    NoAccessPeers,
    /// `invalid_arg_peers`.
    InvalidArgPeers,
    /// `aborted_peers`.
    AbortedPeers,
    /// `piece_requests`.
    PieceRequests,
    /// `max_piece_requests`.
    MaxPieceRequests,
    /// `invalid_piece_requests`.
    InvalidPieceRequests,
    /// `choked_piece_requests`.
    ChokedPieceRequests,
    /// `cancelled_piece_requests`.
    CancelledPieceRequests,
    /// `piece_rejects`.
    PieceRejects,
    /// `error_incoming_peers`.
    ErrorIncomingPeers,
    /// `error_outgoing_peers`.
    ErrorOutgoingPeers,
    /// `error_rc4_peers`.
    ErrorRc4Peers,
    /// `error_encrypted_peers`.
    ErrorEncryptedPeers,
    /// `error_tcp_peers`.
    ErrorTcpPeers,
    /// `error_utp_peers`.
    ErrorUtpPeers,
    /// `reject_piece_picks`.
    RejectPiecePicks,
    /// `unchoke_piece_picks`.
    UnchokePiecePicks,
    /// `incoming_redundant_piece_picks`.
    IncomingRedundantPiecePicks,
    /// `incoming_piece_picks`.
    IncomingPiecePicks,
    /// `end_game_piece_picks`.
    EndGamePiecePicks,
    /// `snubbed_piece_picks`.
    SnubbedPiecePicks,
    /// `interesting_piece_picks`.
    InterestingPiecePicks,
    /// `hash_fail_piece_picks`.
    HashFailPiecePicks,
    /// `piece_picker_partial_loops`.
    PiecePickerPartialLoops,
    /// `piece_picker_suggest_loops`.
    PiecePickerSuggestLoops,
    /// `piece_picker_sequential_loops`.
    PiecePickerSequentialLoops,
    /// `piece_picker_reverse_rare_loops`.
    PiecePickerReverseRareLoops,
    /// `piece_picker_rare_loops`.
    PiecePickerRareLoops,
    /// `piece_picker_rand_start_loops`.
    PiecePickerRandStartLoops,
    /// `piece_picker_rand_loops`.
    PiecePickerRandLoops,
    /// `piece_picker_busy_loops`.
    PiecePickerBusyLoops,
    /// `connect_timeouts`.
    ConnectTimeouts,
    /// `uninteresting_peers`.
    UninterestingPeers,
    /// `timeout_peers`.
    TimeoutPeers,
    /// `no_memory_peers`.
    NoMemoryPeers,
    /// `too_many_peers`.
    TooManyPeers,
    /// `transport_timeout_peers`.
    TransportTimeoutPeers,
    /// `num_banned_peers`.
    NumBannedPeers,
    /// `banned_for_hash_failure`.
    BannedForHashFailure,
    /// `connection_attempts`.
    ConnectionAttempts,
    /// `connection_attempt_loops`.
    ConnectionAttemptLoops,
    /// `boost_connection_attempts`.
    BoostConnectionAttempts,
    /// `missed_connection_attempts`.
    MissedConnectionAttempts,
    /// `no_peer_connection_attempts`.
    NoPeerConnectionAttempts,
    /// `incoming_connections`.
    IncomingConnections,
    /// `on_read_counter`.
    OnReadCounter,
    /// `on_write_counter`.
    OnWriteCounter,
    /// `on_tick_counter`.
    OnTickCounter,
    /// `on_lsd_counter`.
    OnLsdCounter,
    /// `on_lsd_peer_counter`.
    OnLsdPeerCounter,
    /// `on_udp_counter`.
    OnUdpCounter,
    /// `on_accept_counter`.
    OnAcceptCounter,
    /// `on_disk_queue_counter`.
    OnDiskQueueCounter,
    /// `on_disk_counter`.
    OnDiskCounter,
    /// `num_incoming_choke`.
    NumIncomingChoke,
    /// `num_incoming_unchoke`.
    NumIncomingUnchoke,
    /// `num_incoming_interested`.
    NumIncomingInterested,
    /// `num_incoming_not_interested`.
    NumIncomingNotInterested,
    /// `num_incoming_have`.
    NumIncomingHave,
    /// `num_incoming_bitfield`.
    NumIncomingBitfield,
    /// `num_incoming_request`.
    NumIncomingRequest,
    /// `num_incoming_piece`.
    NumIncomingPiece,
    /// `num_incoming_cancel`.
    NumIncomingCancel,
    /// `num_incoming_dht_port`.
    NumIncomingDhtPort,
    /// `num_incoming_suggest`.
    NumIncomingSuggest,
    /// `num_incoming_have_all`.
    NumIncomingHaveAll,
    /// `num_incoming_have_none`.
    NumIncomingHaveNone,
    /// `num_incoming_reject`.
    NumIncomingReject,
    /// `num_incoming_allowed_fast`.
    NumIncomingAllowedFast,
    /// `num_incoming_ext_handshake`.
    NumIncomingExtHandshake,
    /// `num_incoming_pex`.
    NumIncomingPex,
    /// `num_incoming_metadata`.
    NumIncomingMetadata,
    /// `num_incoming_extended`.
    NumIncomingExtended,
    /// `num_outgoing_choke`.
    NumOutgoingChoke,
    /// `num_outgoing_unchoke`.
    NumOutgoingUnchoke,
    /// `num_outgoing_interested`.
    NumOutgoingInterested,
    /// `num_outgoing_not_interested`.
    NumOutgoingNotInterested,
    /// `num_outgoing_have`.
    NumOutgoingHave,
    /// `num_outgoing_bitfield`.
    NumOutgoingBitfield,
    /// `num_outgoing_request`.
    NumOutgoingRequest,
    /// `num_outgoing_piece`.
    NumOutgoingPiece,
    /// `num_outgoing_cancel`.
    NumOutgoingCancel,
    /// `num_outgoing_dht_port`.
    NumOutgoingDhtPort,
    /// `num_outgoing_suggest`.
    NumOutgoingSuggest,
    /// `num_outgoing_have_all`.
    NumOutgoingHaveAll,
    /// `num_outgoing_have_none`.
    NumOutgoingHaveNone,
    /// `num_outgoing_reject`.
    NumOutgoingReject,
    /// `num_outgoing_allowed_fast`.
    NumOutgoingAllowedFast,
    /// `num_outgoing_ext_handshake`.
    NumOutgoingExtHandshake,
    /// `num_outgoing_pex`.
    NumOutgoingPex,
    /// `num_outgoing_metadata`.
    NumOutgoingMetadata,
    /// `num_outgoing_extended`.
    NumOutgoingExtended,
    /// `num_outgoing_hash_request`.
    NumOutgoingHashRequest,
    /// `num_outgoing_hashes`.
    NumOutgoingHashes,
    /// `num_outgoing_hash_reject`.
    NumOutgoingHashReject,
    /// `num_piece_passed`.
    NumPiecePassed,
    /// `num_piece_failed`.
    NumPieceFailed,
    /// `num_have_pieces`.
    NumHavePieces,
    /// `num_total_pieces_added`.
    NumTotalPiecesAdded,
    /// `num_blocks_written`.
    NumBlocksWritten,
    /// `num_blocks_read`.
    NumBlocksRead,
    /// `num_blocks_hashed`.
    NumBlocksHashed,
    /// `num_write_ops`.
    NumWriteOps,
    /// `num_read_ops`.
    NumReadOps,
    /// `num_read_back`.
    NumReadBack,
    /// `file_pool_hits`.
    FilePoolHits,
    /// `file_pool_misses`.
    FilePoolMisses,
    /// `file_pool_thread_stall`.
    FilePoolThreadStall,
    /// `file_pool_race`.
    FilePoolRace,
    /// `disk_read_time`.
    DiskReadTime,
    /// `disk_write_time`.
    DiskWriteTime,
    /// `disk_hash_time`.
    DiskHashTime,
    /// `disk_job_time`.
    DiskJobTime,
    /// `waste_piece_timed_out`.
    WastePieceTimedOut,
    /// `waste_piece_cancelled`.
    WastePieceCancelled,
    /// `waste_piece_unknown`.
    WastePieceUnknown,
    /// `waste_piece_seed`.
    WastePieceSeed,
    /// `waste_piece_end_game`.
    WastePieceEndGame,
    /// `waste_piece_closing`.
    WastePieceClosing,
    /// `sent_payload_bytes`.
    SentPayloadBytes,
    /// `sent_bytes`.
    SentBytes,
    /// `sent_ip_overhead_bytes`.
    SentIpOverheadBytes,
    /// `sent_tracker_bytes`.
    SentTrackerBytes,
    /// `recv_payload_bytes`.
    RecvPayloadBytes,
    /// `recv_bytes`.
    RecvBytes,
    /// `recv_ip_overhead_bytes`.
    RecvIpOverheadBytes,
    /// `recv_tracker_bytes`.
    RecvTrackerBytes,
    /// `recv_failed_bytes`.
    RecvFailedBytes,
    /// `recv_redundant_bytes`.
    RecvRedundantBytes,
    /// `dht_messages_in`.
    DhtMessagesIn,
    /// `dht_messages_in_dropped`.
    DhtMessagesInDropped,
    /// `dht_messages_out`.
    DhtMessagesOut,
    /// `dht_messages_out_dropped`.
    DhtMessagesOutDropped,
    /// `dht_bytes_in`.
    DhtBytesIn,
    /// `dht_bytes_out`.
    DhtBytesOut,
    /// `dht_ping_in`.
    DhtPingIn,
    /// `dht_ping_out`.
    DhtPingOut,
    /// `dht_find_node_in`.
    DhtFindNodeIn,
    /// `dht_find_node_out`.
    DhtFindNodeOut,
    /// `dht_get_peers_in`.
    DhtGetPeersIn,
    /// `dht_get_peers_out`.
    DhtGetPeersOut,
    /// `dht_announce_peer_in`.
    DhtAnnouncePeerIn,
    /// `dht_announce_peer_out`.
    DhtAnnouncePeerOut,
    /// `dht_get_in`.
    DhtGetIn,
    /// `dht_get_out`.
    DhtGetOut,
    /// `dht_put_in`.
    DhtPutIn,
    /// `dht_put_out`.
    DhtPutOut,
    /// `dht_sample_infohashes_in`.
    DhtSampleInfohashesIn,
    /// `dht_sample_infohashes_out`.
    DhtSampleInfohashesOut,
    /// `dht_invalid_announce`.
    DhtInvalidAnnounce,
    /// `dht_invalid_get_peers`.
    DhtInvalidGetPeers,
    /// `dht_invalid_find_node`.
    DhtInvalidFindNode,
    /// `dht_invalid_put`.
    DhtInvalidPut,
    /// `dht_invalid_get`.
    DhtInvalidGet,
    /// `dht_invalid_sample_infohashes`.
    DhtInvalidSampleInfohashes,
    /// `utp_packet_loss`.
    UtpPacketLoss,
    /// `utp_timeout`.
    UtpTimeout,
    /// `utp_packets_in`.
    UtpPacketsIn,
    /// `utp_packets_out`.
    UtpPacketsOut,
    /// `utp_fast_retransmit`.
    UtpFastRetransmit,
    /// `utp_packet_resend`.
    UtpPacketResend,
    /// `utp_samples_above_target`.
    UtpSamplesAboveTarget,
    /// `utp_samples_below_target`.
    UtpSamplesBelowTarget,
    /// `utp_payload_pkts_in`.
    UtpPayloadPktsIn,
    /// `utp_payload_pkts_out`.
    UtpPayloadPktsOut,
    /// `utp_invalid_pkts_in`.
    UtpInvalidPktsIn,
    /// `utp_redundant_pkts_in`.
    UtpRedundantPktsIn,
    /// `socket_send_size3`.
    SocketSendSize3,
    /// `socket_send_size4`.
    SocketSendSize4,
    /// `socket_send_size5`.
    SocketSendSize5,
    /// `socket_send_size6`.
    SocketSendSize6,
    /// `socket_send_size7`.
    SocketSendSize7,
    /// `socket_send_size8`.
    SocketSendSize8,
    /// `socket_send_size9`.
    SocketSendSize9,
    /// `socket_send_size10`.
    SocketSendSize10,
    /// `socket_send_size11`.
    SocketSendSize11,
    /// `socket_send_size12`.
    SocketSendSize12,
    /// `socket_send_size13`.
    SocketSendSize13,
    /// `socket_send_size14`.
    SocketSendSize14,
    /// `socket_send_size15`.
    SocketSendSize15,
    /// `socket_send_size16`.
    SocketSendSize16,
    /// `socket_send_size17`.
    SocketSendSize17,
    /// `socket_send_size18`.
    SocketSendSize18,
    /// `socket_send_size19`.
    SocketSendSize19,
    /// `socket_send_size20`.
    SocketSendSize20,
    /// `socket_recv_size3`.
    SocketRecvSize3,
    /// `socket_recv_size4`.
    SocketRecvSize4,
    /// `socket_recv_size5`.
    SocketRecvSize5,
    /// `socket_recv_size6`.
    SocketRecvSize6,
    /// `socket_recv_size7`.
    SocketRecvSize7,
    /// `socket_recv_size8`.
    SocketRecvSize8,
    /// `socket_recv_size9`.
    SocketRecvSize9,
    /// `socket_recv_size10`.
    SocketRecvSize10,
    /// `socket_recv_size11`.
    SocketRecvSize11,
    /// `socket_recv_size12`.
    SocketRecvSize12,
    /// `socket_recv_size13`.
    SocketRecvSize13,
    /// `socket_recv_size14`.
    SocketRecvSize14,
    /// `socket_recv_size15`.
    SocketRecvSize15,
    /// `socket_recv_size16`.
    SocketRecvSize16,
    /// `socket_recv_size17`.
    SocketRecvSize17,
    /// `socket_recv_size18`.
    SocketRecvSize18,
    /// `socket_recv_size19`.
    SocketRecvSize19,
    /// `socket_recv_size20`.
    SocketRecvSize20,
    /// `disk_read_latency1`.
    DiskReadLatency1,
    /// `disk_read_latency2`.
    DiskReadLatency2,
    /// `disk_read_latency3`.
    DiskReadLatency3,
    /// `disk_read_latency4`.
    DiskReadLatency4,
    /// `disk_read_latency5`.
    DiskReadLatency5,
    /// `disk_read_latency6`.
    DiskReadLatency6,
    /// `disk_read_latency7`.
    DiskReadLatency7,
    /// `disk_read_latency8`.
    DiskReadLatency8,
    /// `disk_read_latency9`.
    DiskReadLatency9,
    /// `disk_read_latency10`.
    DiskReadLatency10,
    /// `disk_read_latency11`.
    DiskReadLatency11,
    /// `disk_read_latency12`.
    DiskReadLatency12,
    /// `disk_read_latency13`.
    DiskReadLatency13,
    /// `disk_read_latency14`.
    DiskReadLatency14,
    /// `disk_read_latency15`.
    DiskReadLatency15,
    /// `disk_read_latency16`.
    DiskReadLatency16,
    /// `disk_read_latency17`.
    DiskReadLatency17,
    /// `disk_read_latency18`.
    DiskReadLatency18,
    /// `disk_read_latency19`.
    DiskReadLatency19,
    /// `disk_read_latency20`.
    DiskReadLatency20,
    /// `num_checking_torrents`.
    NumCheckingTorrents,
    /// `num_stopped_torrents`.
    NumStoppedTorrents,
    /// `num_upload_only_torrents`.
    NumUploadOnlyTorrents,
    /// `num_downloading_torrents`.
    NumDownloadingTorrents,
    /// `num_seeding_torrents`.
    NumSeedingTorrents,
    /// `num_queued_seeding_torrents`.
    NumQueuedSeedingTorrents,
    /// `num_queued_download_torrents`.
    NumQueuedDownloadTorrents,
    /// `num_error_torrents`.
    NumErrorTorrents,
    /// `non_filter_torrents`.
    NonFilterTorrents,
    /// `num_tcp_peers`.
    NumTcpPeers,
    /// `num_socks5_peers`.
    NumSocks5Peers,
    /// `num_http_proxy_peers`.
    NumHttpProxyPeers,
    /// `num_utp_peers`.
    NumUtpPeers,
    /// `num_i2p_peers`.
    NumI2pPeers,
    /// `num_ssl_peers`.
    NumSslPeers,
    /// `num_ssl_socks5_peers`.
    NumSslSocks5Peers,
    /// `num_ssl_http_proxy_peers`.
    NumSslHttpProxyPeers,
    /// `num_ssl_utp_peers`.
    NumSslUtpPeers,
    /// `num_peers_half_open`.
    NumPeersHalfOpen,
    /// `num_peers_connected`.
    NumPeersConnected,
    /// `num_peers_up_interested`.
    NumPeersUpInterested,
    /// `num_peers_down_interested`.
    NumPeersDownInterested,
    /// `num_peers_up_unchoked_all`.
    NumPeersUpUnchokedAll,
    /// `num_peers_up_unchoked_optimistic`.
    NumPeersUpUnchokedOptimistic,
    /// `num_peers_up_unchoked`.
    NumPeersUpUnchoked,
    /// `num_peers_down_unchoked`.
    NumPeersDownUnchoked,
    /// `num_peers_up_requests`.
    NumPeersUpRequests,
    /// `num_peers_down_requests`.
    NumPeersDownRequests,
    /// `num_peers_up_disk`.
    NumPeersUpDisk,
    /// `num_peers_down_disk`.
    NumPeersDownDisk,
    /// `num_peers_end_game`.
    NumPeersEndGame,
    /// `request_latency`.
    RequestLatency,
    /// `disk_blocks_in_use`.
    DiskBlocksInUse,
    /// `cached_blocks`.
    CachedBlocks,
    /// `num_unhashed`.
    NumUnhashed,
    /// `queued_disk_jobs`.
    QueuedDiskJobs,
    /// `num_running_disk_jobs`.
    NumRunningDiskJobs,
    /// `num_read_jobs`.
    NumReadJobs,
    /// `num_write_jobs`.
    NumWriteJobs,
    /// `num_jobs`.
    NumJobs,
    /// `num_writing_threads`.
    NumWritingThreads,
    /// `num_running_threads`.
    NumRunningThreads,
    /// `blocked_disk_jobs`.
    BlockedDiskJobs,
    /// `file_pool_size`.
    FilePoolSize,
    /// `queued_write_bytes`.
    QueuedWriteBytes,
    /// `num_unchoke_slots`.
    NumUnchokeSlots,
    /// `num_fenced_read`.
    NumFencedRead,
    /// `num_fenced_write`.
    NumFencedWrite,
    /// `num_fenced_hash`.
    NumFencedHash,
    /// `num_fenced_move_storage`.
    NumFencedMoveStorage,
    /// `num_fenced_release_files`.
    NumFencedReleaseFiles,
    /// `num_fenced_delete_files`.
    NumFencedDeleteFiles,
    /// `num_fenced_check_fastresume`.
    NumFencedCheckFastresume,
    /// `num_fenced_save_resume_data`.
    NumFencedSaveResumeData,
    /// `num_fenced_rename_file`.
    NumFencedRenameFile,
    /// `num_fenced_stop_torrent`.
    NumFencedStopTorrent,
    /// `num_fenced_flush_piece`.
    NumFencedFlushPiece,
    /// `num_fenced_flush_hashed`.
    NumFencedFlushHashed,
    /// `num_fenced_flush_storage`.
    NumFencedFlushStorage,
    /// `num_fenced_file_priority`.
    NumFencedFilePriority,
    /// `num_fenced_load_torrent`.
    NumFencedLoadTorrent,
    /// `num_fenced_clear_piece`.
    NumFencedClearPiece,
    /// `num_fenced_tick_storage`.
    NumFencedTickStorage,
    /// `dht_nodes`.
    DhtNodes,
    /// `dht_node_cache`.
    DhtNodeCache,
    /// `dht_torrents`.
    DhtTorrents,
    /// `dht_peers`.
    DhtPeers,
    /// `dht_immutable_data`.
    DhtImmutableData,
    /// `dht_mutable_data`.
    DhtMutableData,
    /// `dht_allocated_observers`.
    DhtAllocatedObservers,
    /// `has_incoming_connections`.
    HasIncomingConnections,
    /// `limiter_up_queue`.
    LimiterUpQueue,
    /// `limiter_down_queue`.
    LimiterDownQueue,
    /// `limiter_up_bytes`.
    LimiterUpBytes,
    /// `limiter_down_bytes`.
    LimiterDownBytes,
    /// `num_utp_idle`.
    NumUtpIdle,
    /// `num_utp_syn_sent`.
    NumUtpSynSent,
    /// `num_utp_connected`.
    NumUtpConnected,
    /// `num_utp_fin_sent`.
    NumUtpFinSent,
    /// `num_utp_close_wait`.
    NumUtpCloseWait,
    /// `num_utp_deleted`.
    NumUtpDeleted,
    /// `num_outstanding_accept`.
    NumOutstandingAccept,
    /// `num_queued_tracker_announces`.
    NumQueuedTrackerAnnounces,
}

/// `num_stats_counters` � boundary between stats and gauges.
pub const NUM_STATS_COUNTERS: usize = 235;

/// `num_counters`.
pub const NUM_COUNTERS: usize = 318;

pub const COUNTER_NAMES: &[&str] = &[
    "error_peers",
    "disconnected_peers",
    "eof_peers",
    "connreset_peers",
    "connrefused_peers",
    "connaborted_peers",
    "notconnected_peers",
    "perm_peers",
    "buffer_peers",
    "unreachable_peers",
    "broken_pipe_peers",
    "addrinuse_peers",
    "no_access_peers",
    "invalid_arg_peers",
    "aborted_peers",
    "piece_requests",
    "max_piece_requests",
    "invalid_piece_requests",
    "choked_piece_requests",
    "cancelled_piece_requests",
    "piece_rejects",
    "error_incoming_peers",
    "error_outgoing_peers",
    "error_rc4_peers",
    "error_encrypted_peers",
    "error_tcp_peers",
    "error_utp_peers",
    "reject_piece_picks",
    "unchoke_piece_picks",
    "incoming_redundant_piece_picks",
    "incoming_piece_picks",
    "end_game_piece_picks",
    "snubbed_piece_picks",
    "interesting_piece_picks",
    "hash_fail_piece_picks",
    "piece_picker_partial_loops",
    "piece_picker_suggest_loops",
    "piece_picker_sequential_loops",
    "piece_picker_reverse_rare_loops",
    "piece_picker_rare_loops",
    "piece_picker_rand_start_loops",
    "piece_picker_rand_loops",
    "piece_picker_busy_loops",
    "connect_timeouts",
    "uninteresting_peers",
    "timeout_peers",
    "no_memory_peers",
    "too_many_peers",
    "transport_timeout_peers",
    "num_banned_peers",
    "banned_for_hash_failure",
    "connection_attempts",
    "connection_attempt_loops",
    "boost_connection_attempts",
    "missed_connection_attempts",
    "no_peer_connection_attempts",
    "incoming_connections",
    "on_read_counter",
    "on_write_counter",
    "on_tick_counter",
    "on_lsd_counter",
    "on_lsd_peer_counter",
    "on_udp_counter",
    "on_accept_counter",
    "on_disk_queue_counter",
    "on_disk_counter",
    "num_incoming_choke",
    "num_incoming_unchoke",
    "num_incoming_interested",
    "num_incoming_not_interested",
    "num_incoming_have",
    "num_incoming_bitfield",
    "num_incoming_request",
    "num_incoming_piece",
    "num_incoming_cancel",
    "num_incoming_dht_port",
    "num_incoming_suggest",
    "num_incoming_have_all",
    "num_incoming_have_none",
    "num_incoming_reject",
    "num_incoming_allowed_fast",
    "num_incoming_ext_handshake",
    "num_incoming_pex",
    "num_incoming_metadata",
    "num_incoming_extended",
    "num_outgoing_choke",
    "num_outgoing_unchoke",
    "num_outgoing_interested",
    "num_outgoing_not_interested",
    "num_outgoing_have",
    "num_outgoing_bitfield",
    "num_outgoing_request",
    "num_outgoing_piece",
    "num_outgoing_cancel",
    "num_outgoing_dht_port",
    "num_outgoing_suggest",
    "num_outgoing_have_all",
    "num_outgoing_have_none",
    "num_outgoing_reject",
    "num_outgoing_allowed_fast",
    "num_outgoing_ext_handshake",
    "num_outgoing_pex",
    "num_outgoing_metadata",
    "num_outgoing_extended",
    "num_outgoing_hash_request",
    "num_outgoing_hashes",
    "num_outgoing_hash_reject",
    "num_piece_passed",
    "num_piece_failed",
    "num_have_pieces",
    "num_total_pieces_added",
    "num_blocks_written",
    "num_blocks_read",
    "num_blocks_hashed",
    "num_write_ops",
    "num_read_ops",
    "num_read_back",
    "file_pool_hits",
    "file_pool_misses",
    "file_pool_thread_stall",
    "file_pool_race",
    "disk_read_time",
    "disk_write_time",
    "disk_hash_time",
    "disk_job_time",
    "waste_piece_timed_out",
    "waste_piece_cancelled",
    "waste_piece_unknown",
    "waste_piece_seed",
    "waste_piece_end_game",
    "waste_piece_closing",
    "sent_payload_bytes",
    "sent_bytes",
    "sent_ip_overhead_bytes",
    "sent_tracker_bytes",
    "recv_payload_bytes",
    "recv_bytes",
    "recv_ip_overhead_bytes",
    "recv_tracker_bytes",
    "recv_failed_bytes",
    "recv_redundant_bytes",
    "dht_messages_in",
    "dht_messages_in_dropped",
    "dht_messages_out",
    "dht_messages_out_dropped",
    "dht_bytes_in",
    "dht_bytes_out",
    "dht_ping_in",
    "dht_ping_out",
    "dht_find_node_in",
    "dht_find_node_out",
    "dht_get_peers_in",
    "dht_get_peers_out",
    "dht_announce_peer_in",
    "dht_announce_peer_out",
    "dht_get_in",
    "dht_get_out",
    "dht_put_in",
    "dht_put_out",
    "dht_sample_infohashes_in",
    "dht_sample_infohashes_out",
    "dht_invalid_announce",
    "dht_invalid_get_peers",
    "dht_invalid_find_node",
    "dht_invalid_put",
    "dht_invalid_get",
    "dht_invalid_sample_infohashes",
    "utp_packet_loss",
    "utp_timeout",
    "utp_packets_in",
    "utp_packets_out",
    "utp_fast_retransmit",
    "utp_packet_resend",
    "utp_samples_above_target",
    "utp_samples_below_target",
    "utp_payload_pkts_in",
    "utp_payload_pkts_out",
    "utp_invalid_pkts_in",
    "utp_redundant_pkts_in",
    "socket_send_size3",
    "socket_send_size4",
    "socket_send_size5",
    "socket_send_size6",
    "socket_send_size7",
    "socket_send_size8",
    "socket_send_size9",
    "socket_send_size10",
    "socket_send_size11",
    "socket_send_size12",
    "socket_send_size13",
    "socket_send_size14",
    "socket_send_size15",
    "socket_send_size16",
    "socket_send_size17",
    "socket_send_size18",
    "socket_send_size19",
    "socket_send_size20",
    "socket_recv_size3",
    "socket_recv_size4",
    "socket_recv_size5",
    "socket_recv_size6",
    "socket_recv_size7",
    "socket_recv_size8",
    "socket_recv_size9",
    "socket_recv_size10",
    "socket_recv_size11",
    "socket_recv_size12",
    "socket_recv_size13",
    "socket_recv_size14",
    "socket_recv_size15",
    "socket_recv_size16",
    "socket_recv_size17",
    "socket_recv_size18",
    "socket_recv_size19",
    "socket_recv_size20",
    "disk_read_latency1",
    "disk_read_latency2",
    "disk_read_latency3",
    "disk_read_latency4",
    "disk_read_latency5",
    "disk_read_latency6",
    "disk_read_latency7",
    "disk_read_latency8",
    "disk_read_latency9",
    "disk_read_latency10",
    "disk_read_latency11",
    "disk_read_latency12",
    "disk_read_latency13",
    "disk_read_latency14",
    "disk_read_latency15",
    "disk_read_latency16",
    "disk_read_latency17",
    "disk_read_latency18",
    "disk_read_latency19",
    "disk_read_latency20",
    "num_checking_torrents",
    "num_stopped_torrents",
    "num_upload_only_torrents",
    "num_downloading_torrents",
    "num_seeding_torrents",
    "num_queued_seeding_torrents",
    "num_queued_download_torrents",
    "num_error_torrents",
    "non_filter_torrents",
    "num_tcp_peers",
    "num_socks5_peers",
    "num_http_proxy_peers",
    "num_utp_peers",
    "num_i2p_peers",
    "num_ssl_peers",
    "num_ssl_socks5_peers",
    "num_ssl_http_proxy_peers",
    "num_ssl_utp_peers",
    "num_peers_half_open",
    "num_peers_connected",
    "num_peers_up_interested",
    "num_peers_down_interested",
    "num_peers_up_unchoked_all",
    "num_peers_up_unchoked_optimistic",
    "num_peers_up_unchoked",
    "num_peers_down_unchoked",
    "num_peers_up_requests",
    "num_peers_down_requests",
    "num_peers_up_disk",
    "num_peers_down_disk",
    "num_peers_end_game",
    "request_latency",
    "disk_blocks_in_use",
    "cached_blocks",
    "num_unhashed",
    "queued_disk_jobs",
    "num_running_disk_jobs",
    "num_read_jobs",
    "num_write_jobs",
    "num_jobs",
    "num_writing_threads",
    "num_running_threads",
    "blocked_disk_jobs",
    "file_pool_size",
    "queued_write_bytes",
    "num_unchoke_slots",
    "num_fenced_read",
    "num_fenced_write",
    "num_fenced_hash",
    "num_fenced_move_storage",
    "num_fenced_release_files",
    "num_fenced_delete_files",
    "num_fenced_check_fastresume",
    "num_fenced_save_resume_data",
    "num_fenced_rename_file",
    "num_fenced_stop_torrent",
    "num_fenced_flush_piece",
    "num_fenced_flush_hashed",
    "num_fenced_flush_storage",
    "num_fenced_file_priority",
    "num_fenced_load_torrent",
    "num_fenced_clear_piece",
    "num_fenced_tick_storage",
    "dht_nodes",
    "dht_node_cache",
    "dht_torrents",
    "dht_peers",
    "dht_immutable_data",
    "dht_mutable_data",
    "dht_allocated_observers",
    "has_incoming_connections",
    "limiter_up_queue",
    "limiter_down_queue",
    "limiter_up_bytes",
    "limiter_down_bytes",
    "num_utp_idle",
    "num_utp_syn_sent",
    "num_utp_connected",
    "num_utp_fin_sent",
    "num_utp_close_wait",
    "num_utp_deleted",
    "num_outstanding_accept",
    "num_queued_tracker_announces",
];

/// `counters` — the atomic bank the session owns.
pub struct Counters {
    v: [AtomicI64; NUM_COUNTERS],
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            v: std::array::from_fn(|_| AtomicI64::new(0)),
        }
    }
}

impl Counters {
    /// `inc_stats_counter(c)` (+1).
    pub fn inc(&self, c: Counter) {
        self.add(c, 1);
    }
    /// `inc_stats_counter(c, n)`.
    pub fn add(&self, c: Counter, n: i64) {
        self.v[c as usize].fetch_add(n, Ordering::Relaxed);
    }
    /// `set_value(c, n)` — gauge counters (queue sizes).
    pub fn set(&self, c: Counter, n: i64) {
        self.v[c as usize].store(n, Ordering::Relaxed);
    }
    /// `set_value` by raw index — for upstream's
    /// `num_fenced_read + get_type()` counter arithmetic where the
    /// base plus a `job_action_t` ordinal selects the slot.
    pub fn set_idx(&self, idx: usize, n: i64) {
        if let Some(c) = self.v.get(idx) {
            c.store(n, Ordering::Relaxed);
        }
    }
    /// `inc_stats_counter` by raw index — same use as [`Self::set_idx`].
    pub fn inc_idx(&self, idx: usize) {
        if let Some(c) = self.v.get(idx) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// `inc_stats_counter(idx, n)` by raw index — upstream's
    /// `num_utp_idle + m_state` arithmetic uses this form.
    pub fn add_idx(&self, idx: usize, n: i64) {
        if let Some(c) = self.v.get(idx) {
            c.fetch_add(n, Ordering::Relaxed);
        }
    }
    /// `blend_stats_counter(c, value, ratio)` — move the gauge toward
    /// `value` by `ratio` percent: `v = (v·(100−r) + x·r)/100`.
    pub fn blend(&self, c: Counter, value: i64, ratio: i64) {
        let cur = self.v[c as usize].load(Ordering::Relaxed);
        self.v[c as usize].store(
            (cur * (100 - ratio) + value * ratio) / 100,
            Ordering::Relaxed,
        );
    }
    /// `counters[c]`.
    #[must_use]
    pub fn value(&self, c: Counter) -> i64 {
        self.v[c as usize].load(Ordering::Relaxed)
    }
}

/// `utp_socket_manager::inc_stats_counter` bridge — builds the
/// `UtpConfig::counter_sink` that lands uTP stat increments in this
/// bank. `UtpStat::StateGauge(s)` resolves to `num_utp_idle + s`,
/// upstream's exact counter arithmetic.
#[must_use]
pub fn utp_counter_sink(counters: &Arc<Counters>) -> UtpCounterSink {
    let c = counters.clone();
    Arc::new(move |stat, delta| match stat {
        UtpStat::PacketLoss => c.add(Counter::UtpPacketLoss, delta),
        UtpStat::Timeout => c.add(Counter::UtpTimeout, delta),
        UtpStat::PacketsIn => c.add(Counter::UtpPacketsIn, delta),
        UtpStat::PacketsOut => c.add(Counter::UtpPacketsOut, delta),
        UtpStat::FastRetransmit => c.add(Counter::UtpFastRetransmit, delta),
        UtpStat::PacketResend => c.add(Counter::UtpPacketResend, delta),
        UtpStat::SamplesAboveTarget => c.add(Counter::UtpSamplesAboveTarget, delta),
        UtpStat::SamplesBelowTarget => c.add(Counter::UtpSamplesBelowTarget, delta),
        UtpStat::PayloadPktsIn => c.add(Counter::UtpPayloadPktsIn, delta),
        UtpStat::PayloadPktsOut => c.add(Counter::UtpPayloadPktsOut, delta),
        UtpStat::InvalidPktsIn => c.add(Counter::UtpInvalidPktsIn, delta),
        UtpStat::RedundantPktsIn => c.add(Counter::UtpRedundantPktsIn, delta),
        UtpStat::StateGauge(state) => c.add_idx(Counter::NumUtpIdle as usize + state, delta),
    })
}

/// `session_stats` — the snapshot posted by `post_session_stats`.
#[derive(Clone, Debug, Default)]
pub struct SessionStats {
    /// `values[num_counters]` snapshot, index = `Counter as usize`.
    pub values: Vec<i64>,
}

impl SessionStats {
    /// `session_stats` — snapshot the bank.
    #[must_use]
    pub fn snapshot(c: &Counters) -> Self {
        Self {
            values: (0..NUM_COUNTERS)
                .map(|i| c.v[i].load(Ordering::Relaxed))
                .collect(),
        }
    }

    /// `stats_metric` lookup.
    #[must_use]
    pub fn get(&self, c: Counter) -> i64 {
        self.values.get(c as usize).copied().unwrap_or(0)
    }
}
/// `session_stats_metrics()` — upstream `name` per counter index.
pub const METRIC_NAMES: &[&str] = &[
    "peer.error_peers",
    "peer.disconnected_peers",
    "peer.eof_peers",
    "peer.connreset_peers",
    "peer.connrefused_peers",
    "peer.connaborted_peers",
    "peer.notconnected_peers",
    "peer.perm_peers",
    "peer.buffer_peers",
    "peer.unreachable_peers",
    "peer.broken_pipe_peers",
    "peer.addrinuse_peers",
    "peer.no_access_peers",
    "peer.invalid_arg_peers",
    "peer.aborted_peers",
    "peer.piece_requests",
    "peer.max_piece_requests",
    "peer.invalid_piece_requests",
    "peer.choked_piece_requests",
    "peer.cancelled_piece_requests",
    "peer.piece_rejects",
    "peer.error_incoming_peers",
    "peer.error_outgoing_peers",
    "peer.error_rc4_peers",
    "peer.error_encrypted_peers",
    "peer.error_tcp_peers",
    "peer.error_utp_peers",
    "peer.connect_timeouts",
    "peer.uninteresting_peers",
    "peer.timeout_peers",
    "peer.no_memory_peers",
    "peer.too_many_peers",
    "peer.transport_timeout_peers",
    "peer.num_banned_peers",
    "peer.banned_for_hash_failure",
    "peer.connection_attempts",
    "peer.connection_attempt_loops",
    "peer.boost_connection_attempts",
    "peer.missed_connection_attempts",
    "peer.no_peer_connection_attempts",
    "peer.incoming_connections",
    "peer.num_tcp_peers",
    "peer.num_socks5_peers",
    "peer.num_http_proxy_peers",
    "peer.num_utp_peers",
    "peer.num_i2p_peers",
    "peer.num_ssl_peers",
    "peer.num_ssl_socks5_peers",
    "peer.num_ssl_http_proxy_peers",
    "peer.num_ssl_utp_peers",
    "peer.num_peers_half_open",
    "peer.num_peers_connected",
    "peer.num_peers_up_interested",
    "peer.num_peers_down_interested",
    "peer.num_peers_up_unchoked_all",
    "peer.num_peers_up_unchoked_optimistic",
    "peer.num_peers_up_unchoked",
    "peer.num_peers_down_unchoked",
    "peer.num_peers_up_requests",
    "peer.num_peers_down_requests",
    "peer.num_peers_end_game",
    "peer.num_peers_up_disk",
    "peer.num_peers_down_disk",
    "net.on_read_counter",
    "net.on_write_counter",
    "net.on_tick_counter",
    "net.on_lsd_counter",
    "net.on_lsd_peer_counter",
    "net.on_udp_counter",
    "net.on_accept_counter",
    "net.on_disk_queue_counter",
    "net.on_disk_counter",
    "net.sent_payload_bytes",
    "net.sent_bytes",
    "net.sent_ip_overhead_bytes",
    "net.sent_tracker_bytes",
    "net.recv_payload_bytes",
    "net.recv_bytes",
    "net.recv_ip_overhead_bytes",
    "net.recv_tracker_bytes",
    "net.limiter_up_queue",
    "net.limiter_down_queue",
    "net.limiter_up_bytes",
    "net.limiter_down_bytes",
    "net.recv_failed_bytes",
    "net.recv_redundant_bytes",
    "net.has_incoming_connections",
    "ses.num_checking_torrents",
    "ses.num_stopped_torrents",
    "ses.num_upload_only_torrents",
    "ses.num_downloading_torrents",
    "ses.num_seeding_torrents",
    "ses.num_queued_seeding_torrents",
    "ses.num_queued_download_torrents",
    "ses.num_error_torrents",
    "ses.non_filter_torrents",
    "ses.num_piece_passed",
    "ses.num_piece_failed",
    "ses.num_have_pieces",
    "ses.num_total_pieces_added",
    "ses.num_unchoke_slots",
    "ses.num_outstanding_accept",
    "ses.num_incoming_choke",
    "ses.num_incoming_unchoke",
    "ses.num_incoming_interested",
    "ses.num_incoming_not_interested",
    "ses.num_incoming_have",
    "ses.num_incoming_bitfield",
    "ses.num_incoming_request",
    "ses.num_incoming_piece",
    "ses.num_incoming_cancel",
    "ses.num_incoming_dht_port",
    "ses.num_incoming_suggest",
    "ses.num_incoming_have_all",
    "ses.num_incoming_have_none",
    "ses.num_incoming_reject",
    "ses.num_incoming_allowed_fast",
    "ses.num_incoming_ext_handshake",
    "ses.num_incoming_pex",
    "ses.num_incoming_metadata",
    "ses.num_incoming_extended",
    "ses.num_outgoing_choke",
    "ses.num_outgoing_unchoke",
    "ses.num_outgoing_interested",
    "ses.num_outgoing_not_interested",
    "ses.num_outgoing_have",
    "ses.num_outgoing_bitfield",
    "ses.num_outgoing_request",
    "ses.num_outgoing_piece",
    "ses.num_outgoing_cancel",
    "ses.num_outgoing_dht_port",
    "ses.num_outgoing_suggest",
    "ses.num_outgoing_have_all",
    "ses.num_outgoing_have_none",
    "ses.num_outgoing_reject",
    "ses.num_outgoing_allowed_fast",
    "ses.num_outgoing_ext_handshake",
    "ses.num_outgoing_pex",
    "ses.num_outgoing_metadata",
    "ses.num_outgoing_extended",
    "ses.num_outgoing_hash_request",
    "ses.num_outgoing_hashes",
    "ses.num_outgoing_hash_reject",
    "ses.waste_piece_timed_out",
    "ses.waste_piece_cancelled",
    "ses.waste_piece_unknown",
    "ses.waste_piece_seed",
    "ses.waste_piece_end_game",
    "ses.waste_piece_closing",
    "picker.piece_picker_partial_loops",
    "picker.piece_picker_suggest_loops",
    "picker.piece_picker_sequential_loops",
    "picker.piece_picker_reverse_rare_loops",
    "picker.piece_picker_rare_loops",
    "picker.piece_picker_rand_start_loops",
    "picker.piece_picker_rand_loops",
    "picker.piece_picker_busy_loops",
    "picker.reject_piece_picks",
    "picker.unchoke_piece_picks",
    "picker.incoming_redundant_piece_picks",
    "picker.incoming_piece_picks",
    "picker.end_game_piece_picks",
    "picker.snubbed_piece_picks",
    "picker.interesting_piece_picks",
    "picker.hash_fail_piece_picks",
    "disk.request_latency",
    "disk.disk_blocks_in_use",
    "disk.cached_blocks",
    "disk.num_unhashed",
    "disk.queued_disk_jobs",
    "disk.num_running_disk_jobs",
    "disk.num_read_jobs",
    "disk.num_write_jobs",
    "disk.num_jobs",
    "disk.blocked_disk_jobs",
    "disk.file_pool_size",
    "disk.num_writing_threads",
    "disk.num_running_threads",
    "disk.queued_write_bytes",
    "disk.num_blocks_written",
    "disk.num_blocks_read",
    "disk.num_blocks_hashed",
    "disk.num_write_ops",
    "disk.num_read_ops",
    "disk.num_read_back",
    "disk.file_pool_hits",
    "disk.file_pool_misses",
    "disk.file_pool_thread_stall",
    "disk.file_pool_race",
    "disk.disk_read_time",
    "disk.disk_write_time",
    "disk.disk_hash_time",
    "disk.disk_job_time",
    "disk.disk_read_latency1",
    "disk.disk_read_latency2",
    "disk.disk_read_latency3",
    "disk.disk_read_latency4",
    "disk.disk_read_latency5",
    "disk.disk_read_latency6",
    "disk.disk_read_latency7",
    "disk.disk_read_latency8",
    "disk.disk_read_latency9",
    "disk.disk_read_latency10",
    "disk.disk_read_latency11",
    "disk.disk_read_latency12",
    "disk.disk_read_latency13",
    "disk.disk_read_latency14",
    "disk.disk_read_latency15",
    "disk.disk_read_latency16",
    "disk.disk_read_latency17",
    "disk.disk_read_latency18",
    "disk.disk_read_latency19",
    "disk.disk_read_latency20",
    "disk.num_fenced_read",
    "disk.num_fenced_write",
    "disk.num_fenced_hash",
    "disk.num_fenced_move_storage",
    "disk.num_fenced_release_files",
    "disk.num_fenced_delete_files",
    "disk.num_fenced_check_fastresume",
    "disk.num_fenced_save_resume_data",
    "disk.num_fenced_rename_file",
    "disk.num_fenced_stop_torrent",
    "disk.num_fenced_flush_piece",
    "disk.num_fenced_flush_hashed",
    "disk.num_fenced_flush_storage",
    "disk.num_fenced_file_priority",
    "disk.num_fenced_load_torrent",
    "disk.num_fenced_clear_piece",
    "disk.num_fenced_tick_storage",
    "dht.dht_nodes",
    "dht.dht_node_cache",
    "dht.dht_torrents",
    "dht.dht_peers",
    "dht.dht_immutable_data",
    "dht.dht_mutable_data",
    "dht.dht_allocated_observers",
    "dht.dht_messages_in",
    "dht.dht_messages_out",
    "dht.dht_messages_in_dropped",
    "dht.dht_messages_out_dropped",
    "dht.dht_bytes_in",
    "dht.dht_bytes_out",
    "dht.dht_ping_in",
    "dht.dht_ping_out",
    "dht.dht_find_node_in",
    "dht.dht_find_node_out",
    "dht.dht_get_peers_in",
    "dht.dht_get_peers_out",
    "dht.dht_announce_peer_in",
    "dht.dht_announce_peer_out",
    "dht.dht_get_in",
    "dht.dht_get_out",
    "dht.dht_put_in",
    "dht.dht_put_out",
    "dht.dht_sample_infohashes_in",
    "dht.dht_sample_infohashes_out",
    "dht.dht_invalid_announce",
    "dht.dht_invalid_get_peers",
    "dht.dht_invalid_find_node",
    "dht.dht_invalid_put",
    "dht.dht_invalid_get",
    "dht.dht_invalid_sample_infohashes",
    "utp.utp_packet_loss",
    "utp.utp_timeout",
    "utp.utp_packets_in",
    "utp.utp_packets_out",
    "utp.utp_fast_retransmit",
    "utp.utp_packet_resend",
    "utp.utp_samples_above_target",
    "utp.utp_samples_below_target",
    "utp.utp_payload_pkts_in",
    "utp.utp_payload_pkts_out",
    "utp.utp_invalid_pkts_in",
    "utp.utp_redundant_pkts_in",
    "utp.num_utp_idle",
    "utp.num_utp_syn_sent",
    "utp.num_utp_connected",
    "utp.num_utp_fin_sent",
    "utp.num_utp_close_wait",
    "utp.num_utp_deleted",
    "sock_bufs.socket_send_size3",
    "sock_bufs.socket_send_size4",
    "sock_bufs.socket_send_size5",
    "sock_bufs.socket_send_size6",
    "sock_bufs.socket_send_size7",
    "sock_bufs.socket_send_size8",
    "sock_bufs.socket_send_size9",
    "sock_bufs.socket_send_size10",
    "sock_bufs.socket_send_size11",
    "sock_bufs.socket_send_size12",
    "sock_bufs.socket_send_size13",
    "sock_bufs.socket_send_size14",
    "sock_bufs.socket_send_size15",
    "sock_bufs.socket_send_size16",
    "sock_bufs.socket_send_size17",
    "sock_bufs.socket_send_size18",
    "sock_bufs.socket_send_size19",
    "sock_bufs.socket_send_size20",
    "sock_bufs.socket_recv_size3",
    "sock_bufs.socket_recv_size4",
    "sock_bufs.socket_recv_size5",
    "sock_bufs.socket_recv_size6",
    "sock_bufs.socket_recv_size7",
    "sock_bufs.socket_recv_size8",
    "sock_bufs.socket_recv_size9",
    "sock_bufs.socket_recv_size10",
    "sock_bufs.socket_recv_size11",
    "sock_bufs.socket_recv_size12",
    "sock_bufs.socket_recv_size13",
    "sock_bufs.socket_recv_size14",
    "sock_bufs.socket_recv_size15",
    "sock_bufs.socket_recv_size16",
    "sock_bufs.socket_recv_size17",
    "sock_bufs.socket_recv_size18",
    "sock_bufs.socket_recv_size19",
    "sock_bufs.socket_recv_size20",
    "tracker.num_queued_tracker_announces",
];

/// `stats_metric` — one row of `session_stats_metrics()`.
#[derive(Clone, Copy, Debug)]
pub struct StatsMetric {
    /// upstream `name` (`category.counter`).
    pub name: &'static str,
    /// `value_index` into the `values[]` snapshot.
    pub value_index: usize,
    /// `stats_type`: `metric_type_gauge` vs `metric_type_counter`.
    pub is_gauge: bool,
}

/// `session_stats_metrics` — the metric table, in upstream's
/// `session_stats.cpp` presentation order (category-grouped, not
/// enum order). `value_index` resolves each `category.name` back to
/// its `stats_counter_t`/`stats_gauge_t` index like upstream's
/// `METRIC(category, name)` → `counters::name`.
#[must_use]
pub fn metrics() -> Vec<StatsMetric> {
    METRIC_NAMES
        .iter()
        .map(|name| {
            let short = name.split('.').nth(1).unwrap_or(name);
            let value_index = COUNTER_NAMES.iter().position(|c| *c == short).unwrap_or(0);
            StatsMetric {
                name,
                value_index,
                is_gauge: value_index >= NUM_STATS_COUNTERS,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_cover_every_counter_once() {
        // upstream session_stats.cpp covers all num_counters slots,
        // each METRIC() resolving to its enum index.
        let m = metrics();
        assert_eq!(m.len(), NUM_COUNTERS);
        let mut seen = vec![false; NUM_COUNTERS];
        for met in &m {
            let short = met.name.split('.').nth(1).expect("category.name");
            assert_eq!(COUNTER_NAMES[met.value_index], short, "{}", met.name);
            assert!(!seen[met.value_index], "dup index {}", met.name);
            seen[met.value_index] = true;
            assert_eq!(met.is_gauge, met.value_index >= NUM_STATS_COUNTERS);
        }
        assert!(seen.iter().all(|s| *s));
    }

    #[test]
    fn metric_order_matches_upstream() {
        // upstream session_stats.cpp groups by category: peer.* first
        // (incl. gauges), picker.* later — not enum order.
        let m = metrics();
        assert_eq!(m[0].name, "peer.error_peers");
        assert_eq!(m[0].value_index, 0);
        let first_gauge = m.iter().position(|x| x.is_gauge).unwrap();
        assert_eq!(m[first_gauge].name, "peer.num_tcp_peers");
        assert_eq!(
            m[first_gauge].value_index,
            COUNTER_NAMES
                .iter()
                .position(|n| *n == "num_tcp_peers")
                .unwrap()
        );
    }
}
