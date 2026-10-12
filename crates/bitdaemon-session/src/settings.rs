// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `settings_pack` + `session_params` — typed session
//! configuration. Upstream packs ~230 scalar keys into three
//! `std::array<int>` indexed by enum; here it's a typed struct —
//! impossible to index the wrong category.
//!
//! Field names are the upstream `settings_pack` key names verbatim
//! (greppable against `settings_pack.cpp`), in upstream declaration
//! order: string settings, then bool settings, then int settings.
//! Every default is `default_settings()` upstream. Fields marked
//! *deprecated* are accepted and round-trip through `.session`
//! files like upstream but have no effect.
//!
//! Time-valued knobs are `Duration`; doc comments give the upstream
//! unit (seconds unless noted). `i64` is used where upstream's value
//! can exceed `i32` or is byte-sized.

use std::sync::Arc;
use std::time::Duration;

use bitdaemon_disk::disk_interface::DiskIo;
use bitdaemon_net::pe_crypto::{EncPolicy, enc_level};
use bitdaemon_net::proxy::ProxyType;

use crate::choker::{ChokingAlgorithm, SeedChokingAlgorithm};

/// `settings_pack::suggest_mode` (`suggest_mode_t`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SuggestMode {
    /// `no_piece_suggestions` = 0.
    #[default]
    NoPieceSuggestions,
    /// `suggest_read_cache` = 1.
    SuggestReadCache,
}

/// `settings_pack::disk_io_write_mode` / `disk_io_read_mode`
/// (`disk_io_mode_t`). Whether the OS cache is used for disk I/O.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DiskIoMode {
    /// `enable_os_cache` = 0 — normal buffered I/O.
    #[default]
    EnableOsCache,
    /// `disable_os_cache_for_aligned_files` = 1 — deprecated upstream.
    DisableOsCacheForAlignedFiles,
    /// `disable_os_cache` = 2 — qBittorrent's "OS cache" toggle off.
    DisableOsCache,
    /// `write_through` = 3 — write-through (upstream Windows default
    /// for `disk_io_write_mode`).
    WriteThrough,
}

impl DiskIoMode {
    /// Whether this mode bypasses the OS cache. Anything but
    /// `EnableOsCache` maps to our `no_cache`/`write_through` open
    /// flag (upstream maps `disable_os_cache*`/`write_through` to
    /// uncached or synchronous I/O).
    #[must_use]
    pub fn bypasses_os_cache(self) -> bool {
        self != DiskIoMode::EnableOsCache
    }
}

/// `settings_pack::mixed_mode_algorithm` (`bandwidth_mixed_algo_t`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BandwidthMixedAlgo {
    /// `prefer_tcp` = 0 — no uTP/TCP balancing.
    PreferTcp,
    /// `peer_proportional` = 1 (upstream default).
    #[default]
    PeerProportional,
}

/// `settings_pack::disk_write_mode` (`mmap_write_mode_t`) — selects
/// pwrite vs mmap for disk writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MmapWriteMode {
    /// `always_pwrite` = 0.
    AlwaysPwrite,
    /// `always_mmap_write` = 1 (upstream default).
    #[default]
    AlwaysMmapWrite,
    /// `auto_mmap_write` = 2 — pick by storage kind.
    AutoMmapWrite,
}

/// `settings_pack` — every scalar knob, upstream names and defaults.
#[derive(Clone, Debug)]
pub struct SessionSettings {
    // ── string settings ──────────────────────────────────────────
    /// `user_agent` — client string in the extended handshake.
    /// (Upstream default is `"libtorrent/<ver>"`; ours identifies
    /// BitDaemon — same field, own identity.)
    pub user_agent: Vec<u8>,
    /// `announce_ip` — IP sent to trackers instead of the real one.
    pub announce_ip: Vec<u8>,
    /// `mmap_cache` — deprecated upstream.
    pub mmap_cache: Vec<u8>,
    /// `handshake_client_version` — `client_version` in the legacy
    /// handshake (debug aid).
    pub handshake_client_version: Vec<u8>,
    /// `outgoing_interfaces` — bind device/IP list for outgoing
    /// connections.
    pub outgoing_interfaces: Vec<u8>,
    /// `listen_interfaces` — `iface:port` list; `…s` suffix = SSL.
    /// Default `"0.0.0.0:6881,[::]:6881"`.
    pub listen_interfaces: Vec<u8>,
    /// `proxy_hostname`.
    pub proxy_hostname: Vec<u8>,
    /// `proxy_username`.
    pub proxy_username: Vec<u8>,
    /// `proxy_password`.
    pub proxy_password: Vec<u8>,
    /// `i2p_hostname` — SAM bridge host.
    pub i2p_hostname: Vec<u8>,
    /// `peer_fingerprint` — peer-id prefix (`"-XX1234-"`).
    /// Upstream ships `"-LT2120-"`; ours is `"-BD0001-"`.
    pub peer_fingerprint: Vec<u8>,
    /// `dht_bootstrap_nodes` — `host:port` list.
    pub dht_bootstrap_nodes: Vec<u8>,
    /// `natpmp_gateway` — override the NAT-PMP gateway address.
    pub natpmp_gateway: Vec<u8>,
    /// `webtorrent_stun_server` — STUN for WebRTC signaling.
    pub webtorrent_stun_server: Vec<u8>,

    // ── bool settings ────────────────────────────────────────────
    /// `allow_multiple_connections_per_ip`.
    pub allow_multiple_connections_per_ip: bool,
    /// `ignore_limits_on_local_network` — deprecated upstream.
    pub ignore_limits_on_local_network: bool,
    /// `send_redundant_have` — re-send HAVE for new metadata.
    pub send_redundant_have: bool,
    /// `lazy_bitfields` — deprecated upstream.
    pub lazy_bitfields: bool,
    /// `use_dht_as_fallback` — only use DHT when trackers fail.
    pub use_dht_as_fallback: bool,
    /// `upnp_ignore_nonrouters` — ignore UPnP devices that are not
    /// routers.
    pub upnp_ignore_nonrouters: bool,
    /// `use_parole_mode` — one-time unchoke for new peers.
    pub use_parole_mode: bool,
    /// `use_read_cache` — deprecated upstream.
    pub use_read_cache: bool,
    /// `use_write_cache` — deprecated upstream.
    pub use_write_cache: bool,
    /// `dont_flush_write_cache` — deprecated upstream.
    pub dont_flush_write_cache: bool,
    /// `coalesce_reads` — deprecated upstream.
    pub coalesce_reads: bool,
    /// `coalesce_writes` — deprecated upstream.
    pub coalesce_writes: bool,
    /// `auto_manage_prefer_seeds` — prefer seeding torrents when
    /// activating.
    pub auto_manage_prefer_seeds: bool,
    /// `dont_count_slow_torrents` — don't count stalled torrents
    /// against the active limits.
    pub dont_count_slow_torrents: bool,
    /// `close_redundant_connections` — close peers offering no rare
    /// pieces.
    pub close_redundant_connections: bool,
    /// `prioritize_partial_pieces` — finish started pieces first.
    pub prioritize_partial_pieces: bool,
    /// `rate_limit_ip_overhead` — count IP/TCP headers in rates.
    pub rate_limit_ip_overhead: bool,
    /// `announce_to_all_trackers` — announce to every tracker, not
    /// just one per tier.
    pub announce_to_all_trackers: bool,
    /// `announce_to_all_tiers` — announce to one tracker per tier.
    pub announce_to_all_tiers: bool,
    /// `prefer_udp_trackers` — prefer UDP over HTTP trackers.
    pub prefer_udp_trackers: bool,
    /// `strict_super_seeding` — deprecated upstream.
    pub strict_super_seeding: bool,
    /// `lock_disk_cache` — deprecated upstream.
    pub lock_disk_cache: bool,
    /// `disable_hash_checks` — accept pieces unverified (dangerous).
    pub disable_hash_checks: bool,
    /// `allow_i2p_mixed` — allow mixed I2P/clearnet swarms.
    pub allow_i2p_mixed: bool,
    /// `low_prio_disk` — deprecated upstream.
    pub low_prio_disk: bool,
    /// `volatile_read_cache` — deprecated upstream.
    pub volatile_read_cache: bool,
    /// `guided_read_cache` — deprecated upstream.
    pub guided_read_cache: bool,
    /// `no_atime_storage` — `O_NOATIME`-equivalent on data files.
    pub no_atime_storage: bool,
    /// `incoming_starts_queued_torrents` — inbound connections start
    /// queued torrents.
    pub incoming_starts_queued_torrents: bool,
    /// `report_true_downloaded` — report payload bytes, not piece
    /// bytes, to trackers.
    pub report_true_downloaded: bool,
    /// `strict_end_game_mode` — deduplicate end-game requests.
    pub strict_end_game_mode: bool,
    /// `broadcast_lsd` — deprecated upstream.
    pub broadcast_lsd: bool,
    /// `enable_outgoing_utp`.
    pub enable_outgoing_utp: bool,
    /// `enable_incoming_utp`.
    pub enable_incoming_utp: bool,
    /// `enable_outgoing_tcp`.
    pub enable_outgoing_tcp: bool,
    /// `enable_incoming_tcp`.
    pub enable_incoming_tcp: bool,
    /// `ignore_resume_timestamps` — deprecated upstream.
    pub ignore_resume_timestamps: bool,
    /// `no_recheck_incomplete_resume` — skip recheck of incomplete
    /// resume data.
    pub no_recheck_incomplete_resume: bool,
    /// `anonymous_mode` — hide identity on the wire; **also**
    /// suppresses LSD multicasts (OnionBit deviation: upstream only
    /// strips announce `ip`/user-agent — announcing info-hashes to
    /// the LAN would defeat the purpose for Tribler).
    pub anonymous_mode: bool,
    /// `report_web_seed_downloads` — report web-seed traffic in the
    /// session counters.
    pub report_web_seed_downloads: bool,
    /// `rate_limit_utp` — deprecated upstream.
    pub rate_limit_utp: bool,
    /// `announce_double_nat` — deprecated upstream.
    pub announce_double_nat: bool,
    /// `seeding_outgoing_connections` — dial out while seeding.
    pub seeding_outgoing_connections: bool,
    /// `no_connect_privileged_ports` — refuse peers on ports < 1024.
    pub no_connect_privileged_ports: bool,
    /// `smooth_connects` — spread outgoing connection attempts.
    pub smooth_connects: bool,
    /// `always_send_user_agent` — send user_agent even when the
    /// peer didn't ask.
    pub always_send_user_agent: bool,
    /// `apply_ip_filter_to_trackers` — filter tracker IPs too.
    pub apply_ip_filter_to_trackers: bool,
    /// `use_disk_read_ahead` — deprecated upstream.
    pub use_disk_read_ahead: bool,
    /// `lock_files` — deprecated upstream.
    pub lock_files: bool,
    /// `contiguous_recv_buffer` — deprecated upstream.
    pub contiguous_recv_buffer: bool,
    /// `ban_web_seeds` — ban web seeds on hash fails.
    pub ban_web_seeds: bool,
    /// `allow_partial_disk_writes` — deprecated upstream.
    pub allow_partial_disk_writes: bool,
    /// `force_proxy` — deprecated upstream.
    pub force_proxy: bool,
    /// `support_share_mode` — BEP-21 upload-only mode.
    pub support_share_mode: bool,
    /// `support_merkle_torrents` — deprecated upstream.
    pub support_merkle_torrents: bool,
    /// `report_redundant_bytes` — count wasted bytes in status.
    pub report_redundant_bytes: bool,
    /// `listen_system_port_fallback` — retry with port 0 when the
    /// configured port is taken.
    pub listen_system_port_fallback: bool,
    /// `use_disk_cache_pool` — deprecated upstream.
    pub use_disk_cache_pool: bool,
    /// `announce_crypto_support` — advertise MSE in announces.
    pub announce_crypto_support: bool,
    /// `enable_upnp`.
    pub enable_upnp: bool,
    /// `enable_natpmp`.
    pub enable_natpmp: bool,
    /// `enable_lsd` — local service discovery.
    pub enable_lsd: bool,
    /// `enable_dht`.
    pub enable_dht: bool,
    /// `prefer_rc4` — prefer RC4 when both crypto levels allowed.
    pub prefer_rc4: bool,
    /// `proxy_hostnames` — resolve peer hostnames via the proxy.
    pub proxy_hostnames: bool,
    /// `proxy_peer_connections`.
    pub proxy_peer_connections: bool,
    /// `auto_sequential` — sequential download for media streams.
    pub auto_sequential: bool,
    /// `proxy_tracker_connections`.
    pub proxy_tracker_connections: bool,
    /// `enable_ip_notifier` — react to interface changes.
    pub enable_ip_notifier: bool,
    /// `dht_prefer_verified_node_ids` — BEP-42 enforcement.
    pub dht_prefer_verified_node_ids: bool,
    /// `dht_restrict_routing_ips` — only route-table nodes from
    /// observed addresses.
    pub dht_restrict_routing_ips: bool,
    /// `dht_restrict_search_ips`.
    pub dht_restrict_search_ips: bool,
    /// `dht_extended_routing_table` — 8-bucket-per-level table.
    pub dht_extended_routing_table: bool,
    /// `dht_aggressive_lookups` — fire queries at more nodes.
    pub dht_aggressive_lookups: bool,
    /// `dht_privacy_lookups` — distribute lookups across nodes.
    pub dht_privacy_lookups: bool,
    /// `dht_enforce_node_id` — verify node-id in responses.
    pub dht_enforce_node_id: bool,
    /// `dht_ignore_dark_internet` — reject unroutable peers in
    /// replies.
    pub dht_ignore_dark_internet: bool,
    /// `dht_read_only` — no state writes / announces.
    pub dht_read_only: bool,
    /// `piece_extent_affinity` — download contiguous extents per
    /// peer.
    pub piece_extent_affinity: bool,
    /// `validate_https_trackers` — verify tracker TLS certs.
    pub validate_https_trackers: bool,
    /// `ssrf_mitigation` — refuse trackers resolving to private
    /// ranges.
    pub ssrf_mitigation: bool,
    /// `allow_idna` — allow internationalized tracker hostnames.
    pub allow_idna: bool,
    /// `enable_set_file_valid_data` — Windows `SetFileValidData` on
    /// non-sparse extension (upstream default false; qBittorrent
    /// sets true — ADR-0022).
    pub enable_set_file_valid_data: bool,
    /// `socks5_udp_send_local_ep` — include local endpoint in
    /// SOCKS5 UDP datagrams.
    pub socks5_udp_send_local_ep: bool,
    /// `proxy_send_host_in_connect` — send hostname (not IP) in the
    /// SOCKS5 CONNECT.
    pub proxy_send_host_in_connect: bool,
    /// `disk_disable_copy_on_write` — `ioctl FICLONE`-class flags
    /// off.
    pub disk_disable_copy_on_write: bool,
    /// `allow_multiple_connections_per_pid` — multiple conns per
    /// peer-id.
    pub allow_multiple_connections_per_pid: bool,
    /// `apply_filter_to_dht` — IP-filter DHT nodes too.
    pub apply_filter_to_dht: bool,

    // ── int settings ─────────────────────────────────────────────
    /// `tracker_completion_timeout` — s before an announce is
    /// considered failed.
    pub tracker_completion_timeout: Duration,
    /// `tracker_receive_timeout` — s waiting for a tracker response.
    pub tracker_receive_timeout: Duration,
    /// `stop_tracker_timeout` — s waiting for a stopped-event reply.
    pub stop_tracker_timeout: Duration,
    /// `tracker_maximum_response_length` — bytes.
    pub tracker_maximum_response_length: i64,
    /// `piece_timeout` — a piece must complete or its blocks abort.
    pub piece_timeout: Duration,
    /// `request_timeout` — block request timeout.
    pub request_timeout: Duration,
    /// `request_queue_time` — AIMD target for `desired_queue_size`.
    pub request_queue_time: Duration,
    /// `max_allowed_in_request_queue`.
    pub max_allowed_in_request_queue: i32,
    /// `max_out_request_queue` — cap on our requests per peer.
    pub max_out_request_queue: i32,
    /// `whole_pieces_threshold` — blocks in flight before finishing
    /// a whole piece is preferred.
    pub whole_pieces_threshold: i32,
    /// `peer_timeout` — inactivity before disconnect.
    pub peer_timeout: Duration,
    /// `urlseed_timeout` — HTTP seed request timeout.
    pub urlseed_timeout: Duration,
    /// `urlseed_pipeline_size` — HTTP requests per url-seed conn.
    pub urlseed_pipeline_size: i32,
    /// `urlseed_wait_retry` — s before retrying a failed url seed.
    pub urlseed_wait_retry: Duration,
    /// `file_pool_size` — max open files (`file_pool`).
    pub file_pool_size: i32,
    /// `max_failcount` — dial failures before a peer is dropped.
    pub max_failcount: i32,
    /// `min_reconnect_time` — peer re-dial backoff (seconds).
    pub min_reconnect_time: i32,
    /// `peer_connect_timeout` — TCP/uTP connect timeout.
    pub peer_connect_timeout: Duration,
    /// `connection_speed` — peer connect attempts per second.
    pub connection_speed: i32,
    /// `inactivity_timeout` — session-idle alert threshold.
    pub inactivity_timeout: Duration,
    /// `unchoke_interval` — s between unchoke sort passes.
    pub unchoke_interval: Duration,
    /// `optimistic_unchoke_interval` — s between optimistic
    /// rotations.
    pub optimistic_unchoke_interval: Duration,
    /// `num_want` — peers asked for per announce.
    pub num_want: i32,
    /// `initial_picker_threshold` — blocks to request before going
    /// rarest-first.
    pub initial_picker_threshold: i32,
    /// `allowed_fast_set_size` — pieces offered via BEP-6
    /// `allowed_fast` to choked peers.
    pub allowed_fast_set_size: i32,
    /// `suggest_mode` — BEP-21 SUGGEST messages policy.
    pub suggest_mode: SuggestMode,
    /// `max_queued_disk_bytes` — back-pressure cap on the disk
    /// buffer pool.
    pub max_queued_disk_bytes: i64,
    /// `handshake_timeout` — peer handshake timeout.
    pub handshake_timeout: Duration,
    /// `send_buffer_low_watermark` — bytes.
    pub send_buffer_low_watermark: i64,
    /// `send_buffer_watermark` — pause socket reads above this.
    pub send_buffer_watermark: usize,
    /// `send_buffer_watermark_factor` — % of watermark that is the
    /// low watermark.
    pub send_buffer_watermark_factor: i32,
    /// `choking_algorithm` — unchoke policy while downloading.
    pub choking_algorithm: ChokingAlgorithm,
    /// `seed_choking_algorithm` — unchoke policy while seeding.
    pub seed_choking_algorithm: SeedChokingAlgorithm,
    /// `cache_size` — deprecated upstream.
    pub cache_size: i32,
    /// `cache_buffer_chunk_size` — deprecated upstream.
    pub cache_buffer_chunk_size: i32,
    /// `cache_expiry` — deprecated upstream (seconds a dirty block
    /// may sit in cache).
    pub cache_expiry: Duration,
    /// `disk_io_write_mode` — upstream default `write_through` on
    /// Windows, `enable_os_cache` elsewhere.
    pub disk_io_write_mode: DiskIoMode,
    /// `disk_io_read_mode`.
    pub disk_io_read_mode: DiskIoMode,
    /// `outgoing_port` — first port for outgoing conns (0 = any).
    pub outgoing_port: i32,
    /// `num_outgoing_ports` — bind-port range for outgoing conns.
    pub num_outgoing_ports: i32,
    /// `peer_dscp` — DSCP/TOS byte for peer sockets.
    pub peer_dscp: i32,
    /// `active_downloads`.
    pub active_downloads: i32,
    /// `active_seeds`.
    pub active_seeds: i32,
    /// `active_checking` — max torrents checking files at once.
    pub active_checking: i32,
    /// `active_dht_limit` — max torrents announcing to the DHT.
    pub active_dht_limit: i32,
    /// `active_tracker_limit` — max torrents announcing to trackers.
    pub active_tracker_limit: i32,
    /// `active_lsd_limit` — max torrents doing LSD announces.
    pub active_lsd_limit: i32,
    /// `active_limit` — hard cap on auto-managed torrents.
    pub active_limit: i32,
    /// `active_loaded_limit` — deprecated upstream.
    pub active_loaded_limit: i32,
    /// `auto_manage_interval` — seconds between auto-manage passes.
    pub auto_manage_interval: i32,
    /// `seed_time_limit` — max seconds a torrent may seed
    /// (used by `seed_rank`).
    pub seed_time_limit: i32,
    /// `auto_scrape_interval` — s between tracker scrapes.
    pub auto_scrape_interval: Duration,
    /// `auto_scrape_min_interval` — min s between scrapes.
    pub auto_scrape_min_interval: Duration,
    /// `max_peerlist_size` — swarm entries per torrent (0 = ∞).
    pub max_peerlist_size: i32,
    /// `max_paused_peerlist_size` — peer-list cap while paused.
    pub max_paused_peerlist_size: i32,
    /// `min_announce_interval` — tracker re-announce floor.
    pub min_announce_interval: Duration,
    /// `auto_manage_startup` — s before auto-manage kicks in.
    pub auto_manage_startup: Duration,
    /// `seeding_piece_quota` — pieces per unchoked seeding peer.
    pub seeding_piece_quota: i32,
    /// `max_rejects` — consecutive rejected requests before snub.
    pub max_rejects: i32,
    /// `recv_socket_buffer_size` — bytes (0 = OS default).
    pub recv_socket_buffer_size: i64,
    /// `send_socket_buffer_size` — bytes (0 = OS default).
    pub send_socket_buffer_size: i64,
    /// `max_peer_recv_buffer_size` — bytes per peer socket.
    pub max_peer_recv_buffer_size: i64,
    /// `file_checks_delay_per_block` — deprecated upstream.
    pub file_checks_delay_per_block: i32,
    /// `read_cache_line_size` — deprecated upstream.
    pub read_cache_line_size: i32,
    /// `write_cache_line_size` — deprecated upstream.
    pub write_cache_line_size: i32,
    /// `optimistic_disk_retry` — s before retrying a busy piece.
    pub optimistic_disk_retry: Duration,
    /// `max_suggest_pieces` — SUGGEST entries remembered per peer.
    pub max_suggest_pieces: i32,
    /// `local_service_announce_interval` — LSD announce period.
    pub local_service_announce_interval: Duration,
    /// `dht_announce_interval` — DHT re-announce period.
    pub dht_announce_interval: Duration,
    /// `udp_tracker_token_expiry` — s a UDP tracker token lives.
    pub udp_tracker_token_expiry: Duration,
    /// `default_cache_min_age` — deprecated upstream.
    pub default_cache_min_age: i32,
    /// `num_optimistic_unchoke_slots`.
    pub num_optimistic_unchoke_slots: i32,
    /// `default_est_reciprocation_rate` — deprecated upstream.
    pub default_est_reciprocation_rate: i32,
    /// `increase_est_reciprocation_rate` — deprecated upstream.
    pub increase_est_reciprocation_rate: i32,
    /// `decrease_est_reciprocation_rate` — deprecated upstream.
    pub decrease_est_reciprocation_rate: i32,
    /// `max_pex_peers` — peers remembered per PEX message.
    pub max_pex_peers: i32,
    /// `tick_interval` — session tick cadence (ms).
    pub tick_interval: Duration,
    /// `share_mode_target` — % of payload share-mode aims for.
    pub share_mode_target: i32,
    /// `upload_rate_limit` bytes/s (0 = unlimited).
    pub upload_rate_limit: i64,
    /// `download_rate_limit` bytes/s.
    pub download_rate_limit: i64,
    /// `local_upload_rate_limit` — deprecated upstream.
    pub local_upload_rate_limit: i64,
    /// `local_download_rate_limit` — deprecated upstream.
    pub local_download_rate_limit: i64,
    /// `dht_upload_rate_limit` — datagrams/s the DHT may send.
    pub dht_upload_rate_limit: i64,
    /// `unchoke_slots_limit`.
    pub unchoke_slots_limit: i32,
    /// `half_open_limit` — deprecated upstream.
    pub half_open_limit: i32,
    /// `connections_limit` — global max open connections.
    pub connections_limit: i32,
    /// `connections_slack` — slack below the limit before dropping.
    pub connections_slack: i32,
    /// `utp_target_delay` — ms (LEDBAT target).
    pub utp_target_delay: Duration,
    /// `utp_gain_factor` — cwnd gain per ms below target.
    pub utp_gain_factor: i32,
    /// `utp_min_timeout` — ms.
    pub utp_min_timeout: Duration,
    /// `utp_syn_resends`.
    pub utp_syn_resends: i32,
    /// `utp_fin_resends`.
    pub utp_fin_resends: i32,
    /// `utp_num_resends`.
    pub utp_num_resends: i32,
    /// `utp_connect_timeout` — ms.
    pub utp_connect_timeout: Duration,
    /// `utp_delayed_ack` — deprecated upstream.
    pub utp_delayed_ack: i32,
    /// `utp_loss_multiplier` — % of cwnd cut on loss.
    pub utp_loss_multiplier: i32,
    /// `mixed_mode_algorithm` — uTP/TCP balance policy.
    pub mixed_mode_algorithm: BandwidthMixedAlgo,
    /// `listen_queue_size` — listen() backlog.
    pub listen_queue_size: i32,
    /// `torrent_connect_boost` — peers dialed per tick at startup.
    pub torrent_connect_boost: i32,
    /// `alert_queue_size` — alert_manager limit.
    pub alert_queue_size: usize,
    /// `max_metadata_size` — bytes accepted for magnet metadata.
    pub max_metadata_size: i64,
    /// `hashing_threads` — dedicated hash pool size (ADR-0022).
    pub hashing_threads: i64,
    /// `checking_mem_usage` — MiB budgeted while checking.
    pub checking_mem_usage: i32,
    /// `predictive_piece_announce` — s ahead a piece-complete is
    /// announced.
    pub predictive_piece_announce: i32,
    /// `aio_threads` — generic disk pool size.
    pub aio_threads: i64,
    /// `aio_max` — deprecated upstream.
    pub aio_max: i32,
    /// `network_threads` — deprecated upstream.
    pub network_threads: i32,
    /// `ssl_listen` — deprecated upstream.
    pub ssl_listen: i32,
    /// `tracker_backoff` — announce-delay growth per failure (×ms).
    pub tracker_backoff: i32,
    /// `share_ratio_limit` — percent (200 = 2.0) for `seed_rank`.
    pub share_ratio_limit: i32,
    /// `seed_time_ratio_limit` — percent (700) of seed-vs-download
    /// time for `seed_rank`.
    pub seed_time_ratio_limit: i32,
    /// `peer_turnover` — % of peers recycled per interval.
    pub peer_turnover: i32,
    /// `peer_turnover_cutoff` — % of peer-list eligible for recycle.
    pub peer_turnover_cutoff: i32,
    /// `peer_turnover_interval` — s between recycle passes.
    pub peer_turnover_interval: Duration,
    /// `connect_seed_every_n_download` — dial a seed per N peers.
    pub connect_seed_every_n_download: i32,
    /// `max_http_recv_buffer_size` — bytes (upstream default
    /// 4*1024*204).
    pub max_http_recv_buffer_size: i64,
    /// `max_retry_port_bind` — retries binding the listen port.
    pub max_retry_port_bind: i32,
    /// `alert_mask` — queued alert categories (upstream default
    /// `alert_category::error`); bit values are upstream
    /// `alert_category_t` (ADR-0023 D2).
    pub alert_mask: crate::alerts::AlertCategory,
    /// `out_enc_policy` — outgoing MSE policy.
    pub out_enc_policy: EncPolicy,
    /// `in_enc_policy` — incoming MSE policy.
    pub in_enc_policy: EncPolicy,
    /// `allowed_enc_level` — bitmask of `enc_level` (RC4=2,
    /// plaintext=1).
    pub allowed_enc_level: u8,
    /// `inactive_down_rate` — bytes/s below which a torrent counts
    /// as inactive.
    pub inactive_down_rate: i64,
    /// `inactive_up_rate`.
    pub inactive_up_rate: i64,
    /// `proxy_type`.
    pub proxy_type: ProxyType,
    /// `proxy_port`.
    pub proxy_port: i32,
    /// `i2p_port` — SAM bridge port.
    pub i2p_port: i32,
    /// `cache_size_volatile` — deprecated upstream.
    pub cache_size_volatile: i32,
    /// `urlseed_max_request_bytes` — bytes a url seed may serve per
    /// request.
    pub urlseed_max_request_bytes: i64,
    /// `web_seed_name_lookup_retry` — s before re-resolving a failed
    /// web-seed host.
    pub web_seed_name_lookup_retry: Duration,
    /// `close_file_interval` — s an idle file stays open (upstream:
    /// 240 on Windows, 0 elsewhere).
    pub close_file_interval: Duration,
    /// `utp_cwnd_reduce_timer` — ms between cwnd reductions.
    pub utp_cwnd_reduce_timer: Duration,
    /// `max_web_seed_connections` — per-torrent web-seed conn cap.
    pub max_web_seed_connections: i32,
    /// `resolver_cache_timeout` — s a DNS answer is cached.
    pub resolver_cache_timeout: Duration,
    /// `send_not_sent_low_watermark` — bytes queued below which the
    /// socket stays writable.
    pub send_not_sent_low_watermark: i64,
    /// `rate_choker_initial_threshold` — bytes/s first rate-choker
    /// slot.
    pub rate_choker_initial_threshold: i64,
    /// `upnp_lease_duration` — s of the UPnP mapping lease.
    pub upnp_lease_duration: Duration,
    /// `max_concurrent_http_announces` — in-flight HTTP announces.
    pub max_concurrent_http_announces: i32,
    /// `dht_max_peers_reply` — peers returned per get_peers.
    pub dht_max_peers_reply: i32,
    /// `dht_search_branching` — lookup concurrency.
    pub dht_search_branching: i32,
    /// `dht_max_fail_count` — fails before a node is dropped.
    pub dht_max_fail_count: i32,
    /// `dht_max_torrents` — tracked torrents for announce.
    pub dht_max_torrents: i32,
    /// `dht_max_dht_items` — stored items per DHT node.
    pub dht_max_dht_items: i32,
    /// `dht_max_peers` — peers stored per item.
    pub dht_max_peers: i32,
    /// `dht_max_torrent_search_reply`.
    pub dht_max_torrent_search_reply: i32,
    /// `dht_block_timeout` — s a node stays blocked.
    pub dht_block_timeout: Duration,
    /// `dht_block_ratelimit` — messages/s before blocking.
    pub dht_block_ratelimit: i32,
    /// `dht_item_lifetime` — s items live (0 = upstream default
    /// behavior).
    pub dht_item_lifetime: Duration,
    /// `dht_sample_infohashes_interval` — s between BEP-51 samples.
    pub dht_sample_infohashes_interval: Duration,
    /// `dht_max_infohashes_sample_count` — info-hashes per sample.
    pub dht_max_infohashes_sample_count: i32,
    /// `max_piece_count` — absolute cap on pieces per torrent.
    pub max_piece_count: i32,
    /// `metadata_token_limit` — x?o token bucket for metadata
    /// requests.
    pub metadata_token_limit: i64,
    /// `disk_write_mode` — pwrite vs mmap policy (ADR-0010 §3).
    pub disk_write_mode: MmapWriteMode,
    /// `mmap_file_size_cutoff` — KiB below which mmap is skipped.
    pub mmap_file_size_cutoff: i32,
    /// `i2p_inbound_quantity` — inbound tunnels.
    pub i2p_inbound_quantity: i32,
    /// `i2p_outbound_quantity` — outbound tunnels.
    pub i2p_outbound_quantity: i32,
    /// `i2p_inbound_length` — inbound tunnel length.
    pub i2p_inbound_length: i32,
    /// `i2p_outbound_length` — outbound tunnel length.
    pub i2p_outbound_length: i32,
    /// `announce_port` — override port sent in announces.
    pub announce_port: i32,
    /// `i2p_inbound_length_variance`.
    pub i2p_inbound_length_variance: i32,
    /// `i2p_outbound_length_variance`.
    pub i2p_outbound_length_variance: i32,
    /// `natpmp_lease_duration` — s of the NAT-PMP mapping lease.
    pub natpmp_lease_duration: Duration,
    /// `min_websocket_announce_interval` — s between websocket
    /// tracker announces.
    pub min_websocket_announce_interval: Duration,
    /// `webtorrent_connection_timeout` — s before an un-negotiated
    /// RTC connection is dropped.
    pub webtorrent_connection_timeout: i32,
    /// `max_webtorrent_offers` — cap on incoming RTC offers.
    pub max_webtorrent_offers: i32,

    // ── BitDaemon extensions (not upstream settings_pack keys) ───
    /// Internal: max blocks picked from url seeds per tick — closest
    /// upstream knob is `urlseed_pipeline_size`.
    pub web_seed_blocks_per_tick: i32,
    /// Internal: periodic resume-data flush interval. Serializes as
    /// `save_resume_data_interval` (a key upstream ≤ 1.2 understood;
    /// 2.x removed it — harmless on load).
    pub save_resume_data_interval: Duration,
}

impl Default for SessionSettings {
    /// `default_settings()`.
    fn default() -> Self {
        Self {
            // string settings
            user_agent: b"bitdaemon/0.1".to_vec(),
            announce_ip: Vec::new(),
            mmap_cache: Vec::new(),
            handshake_client_version: Vec::new(),
            outgoing_interfaces: Vec::new(),
            listen_interfaces: b"0.0.0.0:6881,[::]:6881".to_vec(),
            proxy_hostname: Vec::new(),
            proxy_username: Vec::new(),
            proxy_password: Vec::new(),
            i2p_hostname: Vec::new(),
            peer_fingerprint: b"-BD0001-".to_vec(),
            dht_bootstrap_nodes: b"dht.libtorrent.org:25401".to_vec(),
            natpmp_gateway: Vec::new(),
            webtorrent_stun_server: b"stun.l.google.com:19302".to_vec(),

            // bool settings
            allow_multiple_connections_per_ip: false,
            ignore_limits_on_local_network: true,
            send_redundant_have: true,
            lazy_bitfields: false,
            use_dht_as_fallback: false,
            upnp_ignore_nonrouters: false,
            use_parole_mode: true,
            use_read_cache: true,
            use_write_cache: true,
            dont_flush_write_cache: false,
            coalesce_reads: false,
            coalesce_writes: false,
            auto_manage_prefer_seeds: false,
            dont_count_slow_torrents: true,
            close_redundant_connections: true,
            prioritize_partial_pieces: false,
            rate_limit_ip_overhead: true,
            announce_to_all_trackers: false,
            announce_to_all_tiers: false,
            prefer_udp_trackers: true,
            strict_super_seeding: false,
            lock_disk_cache: false,
            disable_hash_checks: false,
            allow_i2p_mixed: false,
            low_prio_disk: true,
            volatile_read_cache: false,
            guided_read_cache: false,
            no_atime_storage: true,
            incoming_starts_queued_torrents: false,
            report_true_downloaded: false,
            strict_end_game_mode: true,
            broadcast_lsd: true,
            enable_outgoing_utp: true,
            enable_incoming_utp: true,
            enable_outgoing_tcp: true,
            enable_incoming_tcp: true,
            ignore_resume_timestamps: false,
            no_recheck_incomplete_resume: false,
            anonymous_mode: false,
            report_web_seed_downloads: true,
            rate_limit_utp: true,
            announce_double_nat: false,
            seeding_outgoing_connections: true,
            no_connect_privileged_ports: false,
            smooth_connects: true,
            always_send_user_agent: false,
            apply_ip_filter_to_trackers: true,
            use_disk_read_ahead: true,
            lock_files: false,
            contiguous_recv_buffer: true,
            ban_web_seeds: true,
            allow_partial_disk_writes: true,
            force_proxy: false,
            support_share_mode: true,
            support_merkle_torrents: false,
            report_redundant_bytes: true,
            listen_system_port_fallback: true,
            use_disk_cache_pool: false,
            announce_crypto_support: true,
            enable_upnp: true,
            enable_natpmp: true,
            enable_lsd: true,
            enable_dht: true,
            prefer_rc4: false,
            proxy_hostnames: true,
            proxy_peer_connections: true,
            auto_sequential: true,
            proxy_tracker_connections: true,
            enable_ip_notifier: true,
            dht_prefer_verified_node_ids: true,
            dht_restrict_routing_ips: true,
            dht_restrict_search_ips: true,
            dht_extended_routing_table: true,
            dht_aggressive_lookups: true,
            dht_privacy_lookups: false,
            dht_enforce_node_id: false,
            dht_ignore_dark_internet: true,
            dht_read_only: false,
            piece_extent_affinity: false,
            validate_https_trackers: true,
            ssrf_mitigation: true,
            allow_idna: false,
            enable_set_file_valid_data: false,
            socks5_udp_send_local_ep: false,
            proxy_send_host_in_connect: false,
            disk_disable_copy_on_write: false,
            allow_multiple_connections_per_pid: false,
            apply_filter_to_dht: true,

            // int settings
            tracker_completion_timeout: Duration::from_secs(30),
            tracker_receive_timeout: Duration::from_secs(10),
            stop_tracker_timeout: Duration::from_secs(5),
            tracker_maximum_response_length: 1024 * 1024,
            piece_timeout: Duration::from_secs(20),
            request_timeout: Duration::from_secs(60),
            request_queue_time: Duration::from_secs(3),
            max_allowed_in_request_queue: 2000,
            max_out_request_queue: 500,
            whole_pieces_threshold: 20,
            peer_timeout: Duration::from_secs(120),
            urlseed_timeout: Duration::from_secs(20),
            urlseed_pipeline_size: 5,
            urlseed_wait_retry: Duration::from_secs(30),
            file_pool_size: 40,
            max_failcount: 3,
            min_reconnect_time: 60,
            peer_connect_timeout: Duration::from_secs(15),
            connection_speed: 30,
            inactivity_timeout: Duration::from_secs(600),
            unchoke_interval: Duration::from_secs(15),
            optimistic_unchoke_interval: Duration::from_secs(30),
            num_want: 200,
            initial_picker_threshold: 4,
            allowed_fast_set_size: 5,
            suggest_mode: SuggestMode::NoPieceSuggestions,
            max_queued_disk_bytes: 100 << 20,
            handshake_timeout: Duration::from_secs(10),
            send_buffer_low_watermark: 10 * 1024,
            send_buffer_watermark: 500 << 10,
            send_buffer_watermark_factor: 50,
            choking_algorithm: ChokingAlgorithm::FixedSlots,
            seed_choking_algorithm: SeedChokingAlgorithm::RoundRobin,
            cache_size: 2048,
            cache_buffer_chunk_size: 0,
            cache_expiry: Duration::from_secs(300),
            // upstream: write_through on Windows, enable_os_cache else
            disk_io_write_mode: if cfg!(windows) {
                DiskIoMode::WriteThrough
            } else {
                DiskIoMode::EnableOsCache
            },
            disk_io_read_mode: DiskIoMode::EnableOsCache,
            outgoing_port: 0,
            num_outgoing_ports: 0,
            peer_dscp: 0x01,
            active_downloads: 3,
            active_seeds: 5,
            active_checking: 1,
            active_dht_limit: 88,
            active_tracker_limit: 1600,
            active_lsd_limit: 60,
            active_limit: 500,
            active_loaded_limit: 0,
            auto_manage_interval: 30,
            seed_time_limit: 24 * 60 * 60,
            auto_scrape_interval: Duration::from_secs(1800),
            auto_scrape_min_interval: Duration::from_secs(300),
            max_peerlist_size: 3000,
            max_paused_peerlist_size: 1000,
            min_announce_interval: Duration::from_secs(5 * 60),
            auto_manage_startup: Duration::from_secs(60),
            seeding_piece_quota: 20,
            max_rejects: 50,
            recv_socket_buffer_size: 0,
            send_socket_buffer_size: 0,
            max_peer_recv_buffer_size: 2 * 1024 * 1024,
            file_checks_delay_per_block: 0,
            read_cache_line_size: 32,
            write_cache_line_size: 16,
            optimistic_disk_retry: Duration::from_secs(10 * 60),
            max_suggest_pieces: 16,
            local_service_announce_interval: Duration::from_secs(5 * 60),
            dht_announce_interval: Duration::from_secs(15 * 60),
            udp_tracker_token_expiry: Duration::from_secs(60),
            default_cache_min_age: 1,
            num_optimistic_unchoke_slots: 0,
            default_est_reciprocation_rate: 16000,
            increase_est_reciprocation_rate: 20,
            decrease_est_reciprocation_rate: 3,
            max_pex_peers: 50,
            tick_interval: Duration::from_millis(500),
            share_mode_target: 3,
            upload_rate_limit: 0,
            download_rate_limit: 0,
            local_upload_rate_limit: 0,
            local_download_rate_limit: 0,
            dht_upload_rate_limit: 8000,
            unchoke_slots_limit: 8,
            half_open_limit: 0,
            connections_limit: 200,
            connections_slack: 10,
            utp_target_delay: Duration::from_millis(100),
            utp_gain_factor: 3000,
            utp_min_timeout: Duration::from_millis(500),
            utp_syn_resends: 2,
            utp_fin_resends: 2,
            utp_num_resends: 3,
            utp_connect_timeout: Duration::from_millis(3000),
            utp_delayed_ack: 0,
            utp_loss_multiplier: 50,
            mixed_mode_algorithm: BandwidthMixedAlgo::PeerProportional,
            listen_queue_size: 5,
            torrent_connect_boost: 30,
            alert_queue_size: 2000,
            max_metadata_size: 30 * 1024 * 1024,
            hashing_threads: 1,
            checking_mem_usage: 256,
            predictive_piece_announce: 0,
            aio_threads: 10,
            aio_max: 300,
            network_threads: 0,
            ssl_listen: 0,
            tracker_backoff: 250,
            share_ratio_limit: 200,
            seed_time_ratio_limit: 700,
            peer_turnover: 4,
            peer_turnover_cutoff: 90,
            peer_turnover_interval: Duration::from_secs(300),
            connect_seed_every_n_download: 10,
            max_http_recv_buffer_size: 4 * 1024 * 204,
            max_retry_port_bind: 10,
            alert_mask: crate::alerts::AlertCategory::ERROR,
            out_enc_policy: EncPolicy::Enabled,
            in_enc_policy: EncPolicy::Enabled,
            allowed_enc_level: enc_level::BOTH,
            inactive_down_rate: 2048,
            inactive_up_rate: 2048,
            proxy_type: ProxyType::None,
            proxy_port: 0,
            i2p_port: 0,
            cache_size_volatile: 256,
            urlseed_max_request_bytes: 16 * 1024 * 1024,
            web_seed_name_lookup_retry: Duration::from_secs(1800),
            // upstream: 240 on Windows, 0 elsewhere
            close_file_interval: Duration::from_secs(if cfg!(windows) { 240 } else { 0 }),
            utp_cwnd_reduce_timer: Duration::from_millis(100),
            max_web_seed_connections: 3,
            resolver_cache_timeout: Duration::from_secs(1200),
            send_not_sent_low_watermark: 16384,
            rate_choker_initial_threshold: 1024,
            upnp_lease_duration: Duration::from_secs(3600),
            max_concurrent_http_announces: 50,
            dht_max_peers_reply: 100,
            dht_search_branching: 5,
            dht_max_fail_count: 20,
            dht_max_torrents: 2000,
            dht_max_dht_items: 700,
            dht_max_peers: 500,
            dht_max_torrent_search_reply: 20,
            dht_block_timeout: Duration::from_secs(5 * 60),
            dht_block_ratelimit: 5,
            dht_item_lifetime: Duration::ZERO,
            dht_sample_infohashes_interval: Duration::from_secs(21600),
            dht_max_infohashes_sample_count: 20,
            max_piece_count: 0x200000,
            metadata_token_limit: 2_500_000,
            disk_write_mode: MmapWriteMode::AlwaysMmapWrite,
            mmap_file_size_cutoff: 40,
            i2p_inbound_quantity: 3,
            i2p_outbound_quantity: 3,
            i2p_inbound_length: 3,
            i2p_outbound_length: 3,
            announce_port: 0,
            i2p_inbound_length_variance: 0,
            i2p_outbound_length_variance: 0,
            natpmp_lease_duration: Duration::from_secs(3600),
            min_websocket_announce_interval: Duration::from_secs(60),
            webtorrent_connection_timeout: 120,
            max_webtorrent_offers: 10,

            // BitDaemon extensions
            web_seed_blocks_per_tick: 4,
            save_resume_data_interval: Duration::from_secs(60),
        }
    }
}

/// `session_params` — what `session` is constructed with. In Rust
/// the storage/disk *constructor* is an injected trait object
/// (`StorageFactory` seam — the OnionBit private-storage hook).
#[derive(Default)]
pub struct SessionParams {
    /// `settings` — typed `settings_pack`.
    pub settings: SessionSettings,
    /// `dht_settings`.
    pub dht_settings: bitdaemon_dht::dht_settings::DhtSettings,
    /// `dht_state` — persisted DHT node-id / bootstrap nodes.
    pub dht_state: bitdaemon_dht::dht_state::DhtState,
    /// `ip_filter` — persisted peer-IP ranges.
    pub ip_filter: bitdaemon_net::ip_filter::IpFilter,
    /// `ext_state` — opaque per-extension state blobs (upstream
    /// `session_params::ext_state`).
    pub ext_state: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    /// `disk_io_constructor` — `None` → `PreadDiskIo` default.
    pub disk_io: Option<Arc<dyn DiskIo>>,
    /// `extensions` — plugin hooks (alert/session). Each is a
    /// callback invoked on alert post; kept opaque so OnionBit can
    /// attach IPv8 extensions without recompiling the core.
    pub plugins: Vec<Arc<dyn crate::alerts::SessionPlugin>>,
}

impl Clone for SessionParams {
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            dht_settings: self.dht_settings.clone(),
            dht_state: self.dht_state.clone(),
            ip_filter: self.ip_filter.clone(),
            ext_state: self.ext_state.clone(),
            disk_io: self.disk_io.clone(),
            plugins: self.plugins.clone(),
        }
    }
}
