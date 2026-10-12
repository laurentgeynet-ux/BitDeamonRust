// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `settings_pack` + `session_params` — typed session
//! configuration. Upstream packs ~230 scalar keys into three
//! `std::array<int>` indexed by enum; here it's a typed struct —
//! impossible to index the wrong category, and every default is
//! the documented upstream one.

use std::sync::Arc;
use std::time::Duration;

use bitdaemon_disk::disk_interface::DiskIo;

/// `settings_pack` — the scalar knobs a session reads every tick.
/// Defaults are upstream's (`default_settings()`).
#[derive(Clone, Debug)]
pub struct SessionSettings {
    /// `connections_limit` — global max open connections.
    pub connections_limit: i32,
    /// `active_limit` — hard cap on auto-managed torrents.
    pub active_limit: i32,
    /// `active_downloads`.
    pub active_downloads: i32,
    /// `active_seeds`.
    pub active_seeds: i32,
    /// `unchoke_slots_limit`.
    pub unchoke_slots: i32,
    /// `num_optimistic_unchoke_slots`.
    pub optimistic_unchoke_slots: i32,
    /// `request_timeout` — block request timeout.
    pub request_timeout: Duration,
    /// `peer_timeout` — inactivity before disconnect.
    pub peer_timeout: Duration,
    /// `request_queue_time` — AIMD target for `desired_queue_size`.
    pub request_queue_time: Duration,
    /// `max_allowed_in_request_queue`.
    pub max_allowed_in_queue: i32,
    /// `upload_rate_limit` bytes/s (0 = unlimited).
    pub upload_rate_limit: i64,
    /// `download_rate_limit` bytes/s.
    pub download_rate_limit: i64,
    /// `piece_timeout` — piece must complete or blocks abort.
    pub piece_timeout: Duration,
    /// `max_disk_write_queue` — back-pressure cap.
    pub max_disk_write_queue: i64,
    /// `settings_pack::disk_io_write_mode` — `disable_os_cache`:
    /// write-through data files (qBittorrent's "OS cache" toggle).
    pub disable_os_write_cache: bool,
    /// `settings_pack::cache_expiry` — seconds a dirty block may sit
    /// in the write cache before the periodic flush.
    pub cache_expiry: Duration,
    /// `settings_pack::disk_io_read_mode` — `disable_os_cache`:
    /// `no_cache` open flags on read/hash jobs (ADR-0015).
    pub disable_os_read_cache: bool,
    /// `settings_pack::aio_threads` — generic disk pool size
    /// (upstream default 10).
    pub aio_threads: i64,
    /// `settings_pack::hashing_threads` — dedicated hash pool size
    /// (upstream default 1, ADR-0022).
    pub hashing_threads: i64,
    /// `settings_pack::enable_set_file_valid_data` — Windows
    /// `SetFileValidData` on non-sparse extension (upstream default
    /// false; qBittorrent sets true — ADR-0022).
    pub enable_set_file_valid_data: bool,
    /// `send_buffer_watermark` — pause reads above this.
    pub send_buffer_watermark: usize,
    /// `alert_queue_size` — alert_manager limit.
    pub alert_queue_size: usize,
    /// `settings_pack::alert_mask` — which alert categories get
    /// queued. Upstream default = `alert_category::error`
    /// (`settings_pack.cpp`); persisted in `session_params` so the
    /// bit values are upstream `alert_category_t` (ADR-0023 D2).
    pub alert_mask: crate::alerts::AlertCategory,
    /// `dht_upload_rate_limit` — datagrams/s the DHT may send.
    pub dht_enabled: bool,
    /// `enable_outgoing_utp` — dial uTP to peers that support it.
    pub enable_outgoing_utp: bool,
    /// `enable_incoming_utp` — accept inbound uTP SYNs.
    pub enable_incoming_utp: bool,
    /// `prefer_rc4` — MSE crypto preference.
    pub prefer_rc4: bool,
    /// `connection_speed` — peer connect attempt rate.
    pub connection_speed: i32,
    /// `tick_interval` — session tick cadence.
    pub tick_interval: Duration,
    /// `save_resume_data_interval` — periodic resume-data flush
    /// (upstream default 60 s).
    pub save_resume_interval: Duration,
    /// `web_seed_request_blocks`-equivalent — max blocks picked
    /// from url seeds per tick.
    pub web_seed_blocks_per_tick: i32,
    /// `min_reconnect_time` — peer re-dial backoff (s).
    pub min_reconnect_time: i32,
    /// `max_peerlist_size` — swarm entries per torrent (0 = ∞).
    pub max_peerlist_size: i32,
    /// `max_failcount` — dial failures before a peer is dropped.
    pub max_failcount: i32,
    /// `allow_multiple_connections_per_ip`.
    pub allow_multiple_connections_per_ip: bool,
    /// `auto_manage_interval` — seconds between auto-manage queue
    /// passes (upstream default 30; adds also trigger a pass).
    pub auto_manage_interval: i32,
    /// `anonymous_mode` — hide identity on the wire; **also**
    /// suppresses LSD multicasts (OnionBit deviation: upstream only
    /// strips announce `ip`/user-agent — announcing info-hashes to
    /// the LAN would defeat the purpose for Tribler).
    pub anonymous_mode: bool,
    /// `seed_time_limit` — max seconds a torrent may seed
    /// (upstream default 24 h); used by `seed_rank`.
    pub seed_time_limit: i32,
    /// `share_ratio_limit` — percent (200 = 2.0 ratio) feeding
    /// `seed_rank`'s `seed_ratio_not_met` flag.
    pub share_ratio_limit: i32,
    /// `seed_time_ratio_limit` — percent (700) of seed-vs-download
    /// time feeding `seed_rank`.
    pub seed_time_ratio_limit: i32,
    /// `allowed_fast_set_size` — upstream default 5; pieces we offer
    /// via BEP-6 `allowed_fast` to choked peers.
    pub allowed_fast_set_size: i32,
    /// `user_agent` — client string sent in the extended handshake
    /// (`settings_pack::user_agent`).
    pub user_agent: Vec<u8>,
    /// `webtorrent_stun_server` — STUN for WebRTC signaling
    /// (upstream default `stun.l.google.com:19302`, empty = none).
    pub webtorrent_stun_server: Vec<u8>,
    /// `webtorrent_connection_timeout` — seconds before an
    /// un-negotiated RTC connection is dropped (upstream 120).
    pub webtorrent_connection_timeout: i32,
    /// `max_webtorrent_offers` — cap on incoming RTC offers
    /// (upstream 10).
    pub max_webtorrent_offers: i32,
}

impl Default for SessionSettings {
    /// `default_settings()`.
    fn default() -> Self {
        Self {
            connections_limit: 200,
            active_limit: 500,
            active_downloads: 3,
            active_seeds: 5,
            unchoke_slots: 8,
            optimistic_unchoke_slots: 0,
            request_timeout: Duration::from_secs(60),
            peer_timeout: Duration::from_secs(120),
            request_queue_time: Duration::from_secs(3),
            max_allowed_in_queue: 2000,
            upload_rate_limit: 0,
            download_rate_limit: 0,
            piece_timeout: Duration::from_secs(20),
            max_disk_write_queue: 100 << 20,
            disable_os_write_cache: false,
            cache_expiry: Duration::from_secs(60),
            disable_os_read_cache: false,
            // upstream `settings_pack` defaults (settings_pack.cpp:331-334)
            aio_threads: 10,
            hashing_threads: 1,
            enable_set_file_valid_data: false,
            send_buffer_watermark: 500 << 10,
            alert_queue_size: 2000,
            alert_mask: crate::alerts::AlertCategory::ERROR,
            dht_enabled: true,
            // upstream defaults: both true
            enable_outgoing_utp: true,
            enable_incoming_utp: true,
            prefer_rc4: false,
            connection_speed: 30,
            tick_interval: Duration::from_secs(1),
            save_resume_interval: Duration::from_secs(60),
            web_seed_blocks_per_tick: 4,
            min_reconnect_time: 60,
            max_peerlist_size: 3000,
            max_failcount: 3,
            allow_multiple_connections_per_ip: false,
            auto_manage_interval: 30,
            anonymous_mode: false,
            seed_time_limit: 24 * 60 * 60,
            share_ratio_limit: 200,
            seed_time_ratio_limit: 700,
            allowed_fast_set_size: 5,
            user_agent: b"bitdaemon/0.1".to_vec(),
            webtorrent_stun_server: b"stun.l.google.com:19302".to_vec(),
            webtorrent_connection_timeout: 120,
            max_webtorrent_offers: 10,
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
