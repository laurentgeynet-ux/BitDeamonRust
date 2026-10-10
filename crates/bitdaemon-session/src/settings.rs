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
    /// `send_buffer_watermark` — pause reads above this.
    pub send_buffer_watermark: usize,
    /// `alert_queue_size` — alert_manager limit.
    pub alert_queue_size: usize,
    /// `dht_upload_rate_limit` — datagrams/s the DHT may send.
    pub dht_enabled: bool,
    /// `prefer_rc4` — MSE crypto preference.
    pub prefer_rc4: bool,
    /// `connection_speed` — peer connect attempt rate.
    pub connection_speed: i32,
    /// `tick_interval` — session tick cadence.
    pub tick_interval: Duration,
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
    /// `anonymous_mode` — hide identity on the wire; **also**
    /// suppresses LSD multicasts (OnionBit deviation: upstream only
    /// strips announce `ip`/user-agent — announcing info-hashes to
    /// the LAN would defeat the purpose for Tribler).
    pub anonymous_mode: bool,
}

impl Default for SessionSettings {
    /// `default_settings()`.
    fn default() -> Self {
        Self {
            connections_limit: 500,
            active_limit: 15,
            active_downloads: 3,
            active_seeds: 5,
            unchoke_slots: 8,
            optimistic_unchoke_slots: 0,
            request_timeout: Duration::from_secs(60),
            peer_timeout: Duration::from_secs(120),
            request_queue_time: Duration::from_secs(3),
            max_allowed_in_queue: 500,
            upload_rate_limit: 0,
            download_rate_limit: 0,
            piece_timeout: Duration::from_secs(20),
            max_disk_write_queue: 32 << 20,
            send_buffer_watermark: 500 << 10,
            alert_queue_size: 100,
            dht_enabled: true,
            prefer_rc4: false,
            connection_speed: 30,
            tick_interval: Duration::from_secs(1),
            web_seed_blocks_per_tick: 4,
            min_reconnect_time: 60,
            max_peerlist_size: 4000,
            max_failcount: 3,
            allow_multiple_connections_per_ip: false,
            anonymous_mode: false,
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
            disk_io: self.disk_io.clone(),
            plugins: self.plugins.clone(),
        }
    }
}
