// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `session_handle` + `torrent_handle` — the thread-safe public
//! API. Upstream posts every call through `session_call` to the io
//! thread; our `Session` core is `Send`, so the handle wraps it in
//! a poison-tolerant `Mutex` — identical semantics (callers never
//! touch swarm state directly), zero lock-free complexity.

use std::sync::{Arc, Mutex, MutexGuard};

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::types::PieceIndex;

use crate::alerts::Alert;
use crate::session::{PeerPacket, Session, SessionOut};

/// effects vector alias for handle signatures.
pub type SessionOutVec = Vec<SessionOut>;
use crate::settings::SessionParams;
use crate::torrent::Torrent;

/// `torrent_status` — re-export of the ported status snapshot.
pub use crate::status::TorrentStatus;

/// `peer_list_entry` — the value snapshot upstream returns from
/// `get_full_peer_list` (endpoint + source + flags).
#[derive(Clone, Debug)]
pub struct PeerListEntry {
    /// `ip` — remote endpoint (None for I2p destinations).
    pub addr: Option<std::net::SocketAddr>,
    /// `source` — `peer_source_flags_t` (tracker/dht/pex/lsd…).
    pub source: bitdaemon_common::flags::PeerSourceFlags,
    /// `peer_list_flags_t` bitfield.
    pub flags: PeerListFlags,
}

bitdaemon_common::bitfield_flag! {
    /// `peer_list_flags_t` — `peer_list_entry::flags`.
    PeerListFlags, u32
}

impl PeerListFlags {
    /// `connecting` — a connection attempt is in flight.
    pub const CONNECTING: Self = Self::from_bits(0x1);
    /// `banned`.
    pub const BANNED: Self = Self::from_bits(0x2);
    /// `rc4` — encrypted connection.
    pub const RC4: Self = Self::from_bits(0x8);
    /// `holepunch`.
    pub const HOLEPUNCH: Self = Self::from_bits(0x10);
    /// `i2p` — I2P destination.
    pub const I2P: Self = Self::from_bits(0x20);
    /// `utp` — uTP transport.
    pub const UTP: Self = Self::from_bits(0x40);
    /// `ssl` — TLS transport.
    pub const SSL: Self = Self::from_bits(0x80);
}

impl From<&bitdaemon_net::torrent_peer::TorrentPeer> for PeerListEntry {
    fn from(p: &bitdaemon_net::torrent_peer::TorrentPeer) -> Self {
        let mut flags = PeerListFlags::NONE;
        if p.banned {
            flags |= PeerListFlags::BANNED;
        }
        if p.supports_holepunch {
            flags |= PeerListFlags::HOLEPUNCH;
        }
        if p.supports_utp {
            flags |= PeerListFlags::UTP;
        }
        let (addr, i2p) = match &p.addr {
            bitdaemon_net::torrent_peer::PeerAddr::V4(a) => {
                (Some(std::net::SocketAddr::new((*a).into(), p.port)), false)
            }
            bitdaemon_net::torrent_peer::PeerAddr::V6(a) => {
                (Some(std::net::SocketAddr::new((*a).into(), p.port)), false)
            }
            bitdaemon_net::torrent_peer::PeerAddr::I2p(_) => (None, true),
        };
        if i2p {
            flags |= PeerListFlags::I2P;
        }
        Self {
            addr,
            source: p.source,
            flags,
        }
    }
}

/// lock with poison recovery — upstream has no poison concept; a
/// panicked caller must not wedge the session actor.
pub fn lock(s: &Mutex<Session>) -> MutexGuard<'_, Session> {
    s.lock().unwrap_or_else(|e| e.into_inner())
}

/// `session_handle` — cloneable, thread-safe entry point.
#[derive(Clone)]
pub struct SessionHandle {
    inner: Arc<Mutex<Session>>,
}

impl SessionHandle {
    /// `from_shared` — wrap an existing shared `Session` (e.g. the
    /// one an `Engine` drives) so handle ops act on the same actor.
    #[must_use]
    pub fn from_shared(inner: Arc<Mutex<Session>>) -> Self {
        Self { inner }
    }

    /// `session` ctor — build the core from params.
    #[must_use]
    pub fn new(params: SessionParams) -> Self {
        let mut params = params;
        // upstream `session` ctor: `params.dht_settings` fills flat
        // `dht_*` keys that weren't explicitly set — the "settings"
        // pack wins per-key. We approximate "unset" with "still at
        // the settings default".
        {
            let def = crate::settings::SessionSettings::default();
            let dht_def = bitdaemon_dht::dht_settings::DhtSettings::default();
            let (s, d) = (&mut params.settings, &params.dht_settings);
            macro_rules! merge {
                ($f:ident, $df:ident) => {
                    if s.$f == def.$f && d.$df != dht_def.$df {
                        s.$f = d.$df.into();
                    }
                };
                ($f:ident, $df:ident, dur) => {
                    if s.$f == def.$f && d.$df != dht_def.$df {
                        s.$f = std::time::Duration::from_secs(d.$df.max(0) as u64);
                    }
                };
                ($f:ident, $df:ident, clamp) => {
                    if s.$f == def.$f && d.$df != dht_def.$df {
                        s.$f = i64::from(d.$df);
                    }
                };
            }
            merge!(dht_max_peers_reply, max_peers_reply);
            merge!(dht_search_branching, search_branching);
            merge!(dht_max_fail_count, max_fail_count);
            merge!(dht_max_torrents, max_torrents);
            merge!(dht_max_dht_items, max_dht_items);
            merge!(dht_max_peers, max_peers);
            merge!(dht_max_torrent_search_reply, max_torrent_search_reply);
            merge!(dht_restrict_routing_ips, restrict_routing_ips);
            merge!(dht_restrict_search_ips, restrict_search_ips);
            merge!(dht_extended_routing_table, extended_routing_table);
            merge!(dht_aggressive_lookups, aggressive_lookups);
            merge!(dht_privacy_lookups, privacy_lookups);
            merge!(dht_enforce_node_id, enforce_node_id);
            merge!(dht_ignore_dark_internet, ignore_dark_internet);
            merge!(dht_block_timeout, block_timeout, dur);
            merge!(dht_block_ratelimit, block_ratelimit);
            merge!(dht_read_only, read_only);
            merge!(dht_item_lifetime, item_lifetime, dur);
            merge!(dht_upload_rate_limit, upload_rate_limit, clamp);
            merge!(
                dht_sample_infohashes_interval,
                sample_infohashes_interval,
                dur
            );
            merge!(dht_max_infohashes_sample_count, max_infohashes_sample_count);
            merge!(dht_prefer_verified_node_ids, prefer_verified_node_ids);
        }
        let mut s = Session::new();
        if let Some(disk) = params.disk_io {
            s.set_disk(disk);
        }
        // upstream `session_impl` applies `params.settings` at
        // construction — this also seeds the alert mask.
        s.apply_settings(params.settings);
        // `alert_manager::add_extension` — plugins see every posted
        // alert via `on_alert`.
        for p in params.plugins {
            s.alerts.add_extension(p);
        }
        Self {
            inner: Arc::new(Mutex::new(s)),
        }
    }

    /// wrap an existing core (tests drive it directly too).
    #[must_use]
    pub fn wrap(s: Arc<Mutex<Session>>) -> Self {
        Self { inner: s }
    }

    /// borrow the core (single-owner sections like tick pumps).
    #[must_use]
    pub fn session(&self) -> Arc<Mutex<Session>> {
        self.inner.clone()
    }

    /// `add_torrent` — registers the swarm, returns a handle.
    pub fn add_torrent(&self, t: Torrent) -> TorrentHandle {
        let ih = t.info_hash;
        lock(&self.inner).add_torrent(t);
        TorrentHandle {
            session: self.inner.clone(),
            info_hash: ih,
        }
    }

    /// `remove_torrent` — `remove_flags_t` (`delete_files`/`delete_partfile`)
    /// routes through the disk delete job; a plain remove only unregisters.
    pub fn remove_torrent(&self, ih: &Sha1Hash, options: u8) {
        // the handle drains `stopped` announces — a real engine
        // routes the session's out-effects itself
        let mut s = lock(&self.inner);
        if options != 0 {
            s.delete_files(ih, options, &mut Vec::new());
        } else {
            s.remove_torrent(ih, &mut Vec::new());
        }
    }

    /// `session_impl::add_torrent` — the real entry point:
    /// `AddTorrentParams` carries resume data, metadata or magnet
    /// state, url seeds and swarm peers.
    pub fn add_torrent_params(
        &self,
        atp: &bitdaemon_core::AddTorrentParams,
    ) -> Option<TorrentHandle> {
        let mut out = Vec::new();
        // INVALID = "session builds the storage from the atp"
        // (`torrent::init`); magnet adds stay INVALID until metadata.
        let ih = lock(&self.inner).add_torrent_params(
            atp,
            bitdaemon_common::types::StorageIndex::INVALID,
            &mut out,
        )?;
        Some(TorrentHandle {
            session: self.inner.clone(),
            info_hash: ih,
        })
    }

    /// `find_torrent` → handle.
    #[must_use]
    pub fn find_torrent(&self, ih: &Sha1Hash) -> Option<TorrentHandle> {
        if lock(&self.inner).torrents.contains_key(ih) {
            Some(TorrentHandle {
                session: self.inner.clone(),
                info_hash: *ih,
            })
        } else {
            None
        }
    }

    /// `get_torrents` — one status per swarm. `flags` gates which
    /// expensive fields fill (upstream `status_flags_t`).
    #[must_use]
    pub fn torrent_statuses(&self, flags: crate::status::StatusFlags) -> Vec<TorrentStatus> {
        let g = lock(&self.inner);
        g.torrents
            .iter()
            .map(|(ih, t)| t.status(&g.settings, g.queue_position(ih), flags, &g.peer_class_pool))
            .collect()
    }

    /// `post_torrent_updates` — posts `state_update_alert`.
    /// `flags` gates which fields fill.
    pub fn post_torrent_updates(&self, flags: crate::status::StatusFlags) {
        lock(&self.inner).post_torrent_updates(flags);
    }

    /// `pop_alerts` / `wait_for_alert` — drain the alert queue.
    #[must_use]
    pub fn pop_alerts(&self) -> Vec<Alert> {
        lock(&self.inner).pop_alerts()
    }

    /// `session::on_peer_message` — feed a decoded wire message.
    pub fn on_peer_message(&self, p: &PeerPacket, out: &mut Vec<SessionOut>) {
        lock(&self.inner).on_peer_message(p, out);
    }

    /// `session_tick` — drive the periodic pump; returns wait.
    pub fn tick(&self, out: &mut Vec<SessionOut>) -> std::time::Duration {
        lock(&self.inner).tick(out)
    }

    /// `session::save_state` + `save_session_state_buf` — the
    /// `.session` blob (per-torrent resume data + DHT state).
    #[must_use]
    pub fn save_state_buf(&self) -> Vec<u8> {
        crate::session_state::save_session_state_buf(&lock(&self.inner).save_state())
    }

    /// `session::load_state` — restore torrents + DHT state from a
    /// `.session` blob; returns how many torrents were added.
    pub fn load_state(&self, buf: &[u8]) -> usize {
        lock(&self.inner).load_state(buf, &mut Vec::new())
    }

    /// `post_session_stats` — snapshot the session counters.
    #[must_use]
    pub fn session_stats(&self) -> crate::counters::SessionStats {
        lock(&self.inner).session_stats()
    }

    /// `session::post_session_stats` — posts
    /// `session_stats_header_alert` (once) + `session_stats_alert`.
    pub fn post_session_stats(&self) {
        lock(&self.inner).post_session_stats();
    }

    /// `torrent::pause`/`resume` by info-hash.
    /// `apply_settings_pack` on the shared session.
    pub fn apply_settings(&self, s: crate::settings::SessionSettings) {
        lock(&self.inner).apply_settings(s);
    }

    pub fn set_paused(&self, ih: &Sha1Hash, paused: bool) {
        lock(&self.inner).set_paused(ih, paused, &mut Vec::new());
    }

    /// `session_handle::pause`/`resume` — session-wide pause.
    pub fn pause(&self) {
        lock(&self.inner).set_session_paused(true, &mut Vec::new());
    }

    /// `session_handle::resume` — unpauses the whole session.
    pub fn resume(&self) {
        lock(&self.inner).set_session_paused(false, &mut Vec::new());
    }

    /// `is_paused` — session-wide flag.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        lock(&self.inner).is_paused()
    }

    /// `is_listening` — at least one listen socket exists.
    #[must_use]
    pub fn is_listening(&self) -> bool {
        !lock(&self.inner).listen_sockets.is_empty()
    }

    /// `is_dht_running`.
    #[must_use]
    pub fn is_dht_running(&self) -> bool {
        lock(&self.inner).is_dht_running()
    }

    /// `get_torrents` — handles for every torrent in the session.
    #[must_use]
    pub fn get_torrents(&self) -> Vec<TorrentHandle> {
        lock(&self.inner)
            .torrents
            .keys()
            .map(|ih| TorrentHandle {
                session: self.inner.clone(),
                info_hash: *ih,
            })
            .collect()
    }

    /// `get_settings` — copy of the current `settings_pack`.
    #[must_use]
    pub fn get_settings(&self) -> crate::settings::SessionSettings {
        lock(&self.inner).settings.clone()
    }

    /// `session_state` — the opaque `.session` blob for `load_state`.
    #[must_use]
    pub fn session_state(&self) -> Vec<u8> {
        self.save_state_buf()
    }

    /// `write_session_params` — same serialized form (upstream
    /// `write_session_params` → bencoded `session_state`).
    #[must_use]
    pub fn write_session_params(&self) -> Vec<u8> {
        self.save_state_buf()
    }

    /// `set_alert_mask` / `get_alert_mask`.
    pub fn set_alert_mask(&self, mask: crate::alerts::AlertCategory) {
        let mut s = lock(&self.inner);
        // upstream `update_alert_mask` — persist to settings AND the
        // manager so `session_params` round-trips it.
        s.settings.alert_mask = mask;
        s.alerts.set_mask(mask);
    }

    /// `get_alert_mask`.
    #[must_use]
    pub fn get_alert_mask(&self) -> crate::alerts::AlertCategory {
        lock(&self.inner).alerts.mask()
    }

    /// `set_alert_queue_size_limit`.
    pub fn set_alert_queue_size_limit(&self, limit: usize) {
        lock(&self.inner).alerts.set_queue_limit(limit);
    }

    /// `set_alert_notify` — `alert_manager::set_notify_callback`.
    pub fn set_alert_notify(&self, cb: impl Fn() + Send + 'static) {
        lock(&self.inner).alerts.set_notify(cb);
    }

    /// `wait_for_alert` — upstream blocks on the alert condvar. Our
    /// actor is sync: poll for up to `max_wait` (callers wanting true
    /// blocking should use [`set_alert_notify`](Self::set_alert_notify)
    /// with their own waker).
    #[must_use]
    pub fn wait_for_alert(&self, max_wait: std::time::Duration) -> Option<crate::alerts::Alert> {
        let deadline = std::time::Instant::now() + max_wait;
        loop {
            {
                let mut s = lock(&self.inner);
                let mut v = s.alerts.get_all();
                if let Some(a) = v.pop() {
                    return Some(a);
                }
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// `set_ip_filter` — swap and apply (disconnects banned peers).
    pub fn set_ip_filter(&self, f: bitdaemon_net::ip_filter::IpFilter) {
        lock(&self.inner).set_ip_filter(f, &mut Vec::new());
    }

    /// `get_ip_filter` — clone of the current filter.
    #[must_use]
    pub fn get_ip_filter(&self) -> bitdaemon_net::ip_filter::IpFilter {
        lock(&self.inner).ip_filter().clone()
    }

    /// `session_handle` peer-class ops — `create_peer_class`.
    pub fn create_peer_class(&self, label: &str) -> bitdaemon_net::peer_class::PeerClassIndex {
        lock(&self.inner).create_peer_class(label)
    }

    /// `delete_peer_class`.
    pub fn delete_peer_class(&self, c: bitdaemon_net::peer_class::PeerClassIndex) {
        lock(&self.inner).delete_peer_class(c);
    }

    /// `set_peer_class_filter` — `ip_filter` mapping ranges to classes.
    pub fn set_peer_class_filter(&self, f: bitdaemon_net::ip_filter::IpFilter) {
        lock(&self.inner).set_peer_class_filter(f);
    }

    /// `get_peer_class_filter`.
    #[must_use]
    pub fn get_peer_class_filter(&self) -> bitdaemon_net::ip_filter::IpFilter {
        lock(&self.inner).peer_class_filter().clone()
    }

    /// `set_peer_class_type_filter`.
    pub fn set_peer_class_type_filter(&self, f: bitdaemon_net::peer_class::PeerClassTypeFilter) {
        lock(&self.inner).set_peer_class_type_filter(f);
    }

    /// `get_peer_class_type_filter`.
    #[must_use]
    pub fn get_peer_class_type_filter(&self) -> bitdaemon_net::peer_class::PeerClassTypeFilter {
        lock(&self.inner).peer_class_type_filter().clone()
    }

    /// `set_peer_id` — session announce/handshake fingerprint.
    pub fn set_peer_id(&self, id: bitdaemon_common::PeerId) {
        lock(&self.inner).set_peer_id(id);
    }

    /// `id()` upstream — current peer id.
    #[must_use]
    pub fn peer_id(&self) -> bitdaemon_common::PeerId {
        lock(&self.inner).peer_id
    }

    /// `set_key` — tracker `key` token.
    pub fn set_key(&self, key: i32) {
        lock(&self.inner).set_key(key);
    }

    // ---- proxies (`set_proxy` covers `peer`; upstream splits per use)

    /// `set_proxy` — the peer-connection proxy.
    pub fn set_proxy(&self, p: bitdaemon_net::proxy::ProxySettings) {
        lock(&self.inner).proxies.peer = p;
    }

    /// `proxy()`.
    #[must_use]
    pub fn proxy(&self) -> bitdaemon_net::proxy::ProxySettings {
        lock(&self.inner).proxies.peer.clone()
    }

    /// `set_peer_proxy`.
    pub fn set_peer_proxy(&self, p: bitdaemon_net::proxy::ProxySettings) {
        lock(&self.inner).proxies.peer = p;
    }

    /// `peer_proxy`.
    #[must_use]
    pub fn peer_proxy(&self) -> bitdaemon_net::proxy::ProxySettings {
        lock(&self.inner).proxies.peer.clone()
    }

    /// `set_tracker_proxy`.
    pub fn set_tracker_proxy(&self, p: bitdaemon_net::proxy::ProxySettings) {
        lock(&self.inner).proxies.tracker = p;
    }

    /// `tracker_proxy`.
    #[must_use]
    pub fn tracker_proxy(&self) -> bitdaemon_net::proxy::ProxySettings {
        lock(&self.inner).proxies.tracker.clone()
    }

    /// `set_web_seed_proxy`.
    pub fn set_web_seed_proxy(&self, p: bitdaemon_net::proxy::ProxySettings) {
        lock(&self.inner).proxies.web_seed = p;
    }

    /// `web_seed_proxy`.
    #[must_use]
    pub fn web_seed_proxy(&self) -> bitdaemon_net::proxy::ProxySettings {
        lock(&self.inner).proxies.web_seed.clone()
    }

    /// `set_dht_proxy`.
    pub fn set_dht_proxy(&self, p: bitdaemon_net::proxy::ProxySettings) {
        lock(&self.inner).proxies.dht = p;
    }

    /// `dht_proxy`.
    #[must_use]
    pub fn dht_proxy(&self) -> bitdaemon_net::proxy::ProxySettings {
        lock(&self.inner).proxies.dht.clone()
    }

    /// `set_i2p_proxy` — SAM bridge endpoint.
    pub fn set_i2p_proxy(&self, p: bitdaemon_net::proxy::ProxySettings) {
        lock(&self.inner).proxies.i2p = p;
    }

    /// `i2p_proxy`.
    #[must_use]
    pub fn i2p_proxy(&self) -> bitdaemon_net::proxy::ProxySettings {
        lock(&self.inner).proxies.i2p.clone()
    }

    /// `set_pe_settings` — MSE/PE crypto policy.
    pub fn set_pe_settings(&self, s: bitdaemon_net::pe_crypto::PeSettings) {
        lock(&self.inner).pe_settings = s;
    }

    /// `get_pe_settings`.
    #[must_use]
    pub fn get_pe_settings(&self) -> bitdaemon_net::pe_crypto::PeSettings {
        lock(&self.inner).pe_settings
    }

    // ---- rate limits (`set_*_rate_limit` write the settings_pack fields)

    /// `set_upload_rate_limit` (bytes/s, 0 = unlimited).
    pub fn set_upload_rate_limit(&self, bytes_per_sec: i64) {
        lock(&self.inner).settings.upload_rate_limit = bytes_per_sec;
    }

    /// `upload_rate_limit`.
    #[must_use]
    pub fn upload_rate_limit(&self) -> i64 {
        lock(&self.inner).settings.upload_rate_limit
    }

    /// `set_download_rate_limit`.
    pub fn set_download_rate_limit(&self, bytes_per_sec: i64) {
        lock(&self.inner).settings.download_rate_limit = bytes_per_sec;
    }

    /// `download_rate_limit`.
    #[must_use]
    pub fn download_rate_limit(&self) -> i64 {
        lock(&self.inner).settings.download_rate_limit
    }

    /// `set_max_connections` (`connections_limit`).
    pub fn set_max_connections(&self, limit: i32) {
        lock(&self.inner).settings.connections_limit = limit;
    }

    /// `max_connections`.
    #[must_use]
    pub fn max_connections(&self) -> i32 {
        lock(&self.inner).settings.connections_limit
    }

    /// `set_max_uploads` (`unchoke_slots_limit`).
    pub fn set_max_uploads(&self, limit: i32) {
        lock(&self.inner).settings.unchoke_slots_limit = limit;
    }

    /// `max_uploads`.
    #[must_use]
    pub fn max_uploads(&self) -> i32 {
        lock(&self.inner).settings.unchoke_slots_limit
    }

    // ---- service switches (`start_*`/`stop_*`)

    /// `start_dht`.
    pub fn start_dht(&self) {
        lock(&self.inner).services.dht = true;
    }

    /// `stop_dht`.
    pub fn stop_dht(&self) {
        lock(&self.inner).services.dht = false;
    }

    /// `start_lsd`.
    pub fn start_lsd(&self) {
        lock(&self.inner).services.lsd = true;
    }

    /// `stop_lsd`.
    pub fn stop_lsd(&self) {
        lock(&self.inner).services.lsd = false;
    }

    /// `start_natpmp`.
    pub fn start_natpmp(&self) {
        lock(&self.inner).services.natpmp = true;
    }

    /// `stop_natpmp`.
    pub fn stop_natpmp(&self) {
        lock(&self.inner).services.natpmp = false;
    }

    /// `start_upnp`.
    pub fn start_upnp(&self) {
        lock(&self.inner).services.upnp = true;
    }

    /// `stop_upnp`.
    pub fn stop_upnp(&self) {
        lock(&self.inner).services.upnp = false;
    }

    // ---- DHT handle ops

    /// `add_dht_node` / `add_dht_router` — seed the routing table.
    pub fn add_dht_node(&self, addr: std::net::SocketAddr) {
        lock(&self.inner).dht_add_node(addr);
    }

    /// `add_dht_router` — routers land directly in the table and
    /// persist across `start_dht`/`stop_dht` cycles
    /// (`m_dht_router_nodes` upstream).
    pub fn add_dht_router(&self, addr: std::net::SocketAddr) {
        lock(&self.inner).dht_add_router(addr);
    }

    /// `dht_get_peers` — start a `get_peers` traversal; replies
    /// post `dht_get_peers_reply_alert`.
    pub fn dht_get_peers(&self, ih: Sha1Hash) {
        lock(&self.inner).dht_get_peers(ih);
    }

    /// `dht_announce` — user-level announce on all bound families;
    /// found peers post `dht_get_peers_reply_alert`, the write
    /// phase runs `announce_peer` to token-holders. `flags` is
    /// upstream's `dht::announce_flags_t` (seed/implied_port).
    pub fn dht_announce(&self, ih: Sha1Hash, port: u16, flags: crate::session::DhtAnnounceFlags) {
        lock(&self.inner).dht_announce(ih, port, flags);
    }

    /// `dht_get_item` (immutable) — BEP-44 `get`; the first `r.v`
    /// reply posts `dht_immutable_item_alert`.
    pub fn dht_get_item(&self, target: Sha1Hash) {
        lock(&self.inner).dht_get_item(target);
    }

    /// `dht_get_item` (mutable) — `get` on `sha1(pk ‖ salt)`;
    /// replies post `dht_mutable_item_alert` (`authoritative` =
    /// signature verified against `pk`).
    pub fn dht_get_item_mut(&self, pk: [u8; 32], salt: Vec<u8>) {
        lock(&self.inner).dht_get_item_mut(pk, salt);
    }

    /// `dht_sample_infohashes` — BEP-51 *direct* request to `ep`
    /// (upstream `session_handle::dht_sample_infohashes(ep,
    /// target)` — single node, not a traversal). The reply posts
    /// `dht_sample_infohashes_alert`.
    pub fn dht_sample_infohashes(&self, ep: std::net::SocketAddr, target: Sha1Hash) {
        lock(&self.inner).dht_sample_infohashes(ep, target);
    }

    /// `dht_direct_request` — arbitrary query to `ep`; the reply
    /// posts `dht_direct_response_alert` echoing `user_data`
    /// (upstream `userdata`/`client_data_t`).
    pub fn dht_direct_request(
        &self,
        ep: std::net::SocketAddr,
        e: bitdaemon_bencode::Entry,
        user_data: u64,
    ) {
        lock(&self.inner).dht_direct_request(ep, e, user_data);
    }

    /// `dht_live_nodes` — endpoints that answered the `get_peers`
    /// lookup for `ih` (also posts `dht_live_nodes_alert`).
    #[must_use]
    pub fn dht_live_nodes(&self, ih: &Sha1Hash) -> Vec<std::net::SocketAddr> {
        lock(&self.inner).dht_live_nodes(ih)
    }

    /// `dht_state` — serialized live DHT state.
    #[must_use]
    pub fn dht_state(&self) -> bitdaemon_dht::dht_state::DhtState {
        lock(&self.inner).dht_state()
    }

    /// `set_dht_state` — feed saved state into the routing table;
    /// each bound family seeds its bootstrap from `concat(own,
    /// cross)` like upstream `dht_tracker::start`. Also records
    /// `nids` so a later bind resolves its stored node id.
    pub fn set_dht_state(&self, st: bitdaemon_dht::dht_state::DhtState) {
        let mut s = lock(&self.inner);
        s.dht_state_seed = st.clone();
        for v6 in [false, true] {
            if let Some(n) = s.dht.get_node_mut(v6) {
                n.load_state(&st);
            }
        }
    }

    /// `session_handle::get_dht_settings` — the `dht_*` settings
    /// keys packed as a `dht_settings` struct.
    #[must_use]
    pub fn get_dht_settings(&self) -> bitdaemon_dht::dht_settings::DhtSettings {
        lock(&self.inner).dht_settings()
    }

    /// `session_handle::set_dht_settings` — writes the struct into
    /// the flat `dht_*` keys and re-pushes the live nodes.
    pub fn set_dht_settings(&self, st: bitdaemon_dht::dht_settings::DhtSettings) {
        lock(&self.inner).set_dht_settings(st);
    }

    /// `post_dht_stats` — routing-table snapshot routed as an alert
    /// (upstream `dht_stats_alert`). Category-0: unconditional.
    pub fn post_dht_stats(&self) {
        let mut s = lock(&self.inner);
        let stat = |v6: bool| {
            s.dht
                .get_node(v6)
                .map_or_else(crate::alerts::DhtNodeStats::default, |n| {
                    let (nid, nodes, repl, confirmed, lookups) = n.stats();
                    crate::alerts::DhtNodeStats {
                        nid,
                        nodes,
                        replacements: repl,
                        confirmed,
                        active_lookups: lookups,
                    }
                })
        };
        let v4 = Box::new(stat(false));
        let v6 = Box::new(stat(true));
        s.alerts.post(crate::alerts::Alert::DhtStats { v4, v6 });
    }

    /// `reopen_network_sockets` — flags a listener rebind; emitted
    /// as `SessionOut::InterfacesChanged` on the next `tick`
    /// (upstream `on_ip_change` → `reopen_listen_sockets`).
    pub fn reopen_network_sockets(&self) {
        lock(&self.inner).reopen_sockets = true;
    }

    /// `set_dht_storage` — upstream takes a `dht_storage_interface`
    /// (custom DHT item/peer store); here the concrete
    /// [`bitdaemon_dht::dht_storage::DhtStorage`] is swappable on the
    /// bound node (same seam, concrete type since behavior is data).
    pub fn set_dht_storage(&self, storage: bitdaemon_dht::dht_storage::DhtStorage) {
        let mut s = lock(&self.inner);
        for v6 in [false, true] {
            if let Some(n) = s.dht.get_node_mut(v6) {
                n.storage = storage.clone();
            }
        }
    }

    /// `listen_on` — replace the listen set with a single
    /// `port_range` on `iface` (`use_interfaces` with one entry).
    /// `port_range` is `(first, last)`; the first free port wins at
    /// bind time — the engine owns the actual sockets.
    pub fn listen_on(&self, port_range: (u16, u16), iface: &str) {
        let mut s = lock(&self.inner);
        s.listen_sockets = (port_range.0..=port_range.1.max(port_range.0))
            .map(|p| {
                crate::listen::ListenSocket::new(
                    std::net::SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), p),
                    std::net::Ipv4Addr::UNSPECIFIED.into(),
                    iface.to_string(),
                )
            })
            .collect();
        s.listen_port = port_range.0;
        s.reopen_sockets = true;
    }

    /// `use_interfaces` — replace the whole listen set
    /// (`session_impl::update_listen_interfaces`).
    pub fn use_interfaces(&self, sockets: Vec<crate::listen::ListenSocket>) {
        let mut s = lock(&self.inner);
        s.listen_sockets = sockets;
        s.reopen_sockets = true;
    }

    /// `listen_port` — the announce `port` field.
    #[must_use]
    pub fn listen_port(&self) -> u16 {
        lock(&self.inner).listen_port
    }

    /// `ssl_listen_port` — SSL-torrent listen port (upstream
    /// `m_ssl_listen_sockets`; we multiplex on the same set and keep
    /// the advertised port separate).
    #[must_use]
    pub fn ssl_listen_port(&self) -> u16 {
        lock(&self.inner).ssl_listen_port
    }

    /// `num_connections` — live peer connections across torrents.
    #[must_use]
    pub fn num_connections(&self) -> usize {
        lock(&self.inner)
            .torrents
            .values()
            .map(|t| t.peers.len())
            .sum()
    }

    /// `num_uploads` — peers currently unchoked by us.
    #[must_use]
    pub fn num_uploads(&self) -> usize {
        lock(&self.inner)
            .torrents
            .values()
            .flat_map(|t| t.peers.values())
            .filter(|p| !p.choked)
            .count()
    }

    /// `delete_port_mapping` — resolve `handle` against the
    /// session's mapping table and queue the unmap for the engine
    /// (`SessionOut::PortmapDelete` — NAT-PMP unmap / UPnP
    /// `DeletePortMapping`). No-op on an unknown handle.
    pub fn delete_port_mapping(&self, handle: i32) {
        lock(&self.inner).delete_port_mapping(handle);
    }

    /// `add_port_mapping` — register an arbitrary mapping with the
    /// NAT-PMP/UPnP drivers; returns one `port_mapping_t` handle per
    /// (listen socket × active mapper), matching upstream's
    /// `std::vector<port_mapping_t>` (empty when no mapper runs).
    /// The actual map work runs on the engine's next tick.
    pub fn add_port_mapping(&self, protocol: u8, external_port: u16, local_port: u16) -> Vec<i32> {
        lock(&self.inner).add_port_mapping(protocol, external_port, local_port)
    }

    /// `async_add_torrent` — same as [`Self::add_torrent_params`];
    /// upstream's async split is an artifact of
    /// its thread hop, our actor is already serialized.
    #[must_use]
    pub fn async_add_torrent(
        &self,
        atp: &bitdaemon_core::AddTorrentParams,
    ) -> Option<TorrentHandle> {
        self.add_torrent_params(atp)
    }

    /// `set_peer_class` — set a class' `peer_class_info` (limits,
    /// `ignore_unchoke_slots`…).
    pub fn set_peer_class(
        &self,
        c: bitdaemon_net::peer_class::PeerClassIndex,
        info: bitdaemon_net::peer_class::PeerClassInfo,
    ) {
        let mut s = lock(&self.inner);
        if let Some(pc) = s.peer_class_pool.at_mut(c) {
            pc.set_info(&info);
        }
    }

    /// `get_peer_class` — the class' current `peer_class_info`.
    #[must_use]
    pub fn get_peer_class(
        &self,
        c: bitdaemon_net::peer_class::PeerClassIndex,
    ) -> Option<bitdaemon_net::peer_class::PeerClassInfo> {
        lock(&self.inner)
            .peer_class_pool
            .at(c)
            .map(|pc| pc.get_info())
    }

    /// `set_port_filter` — port access filter for peers: stores the
    /// filter and applies it (`port_filter_updated` sweeps every
    /// torrent's peer list, disconnecting + erasing blocked ports).
    pub fn set_port_filter(&self, f: bitdaemon_net::ip_filter::PortFilter) {
        lock(&self.inner).set_port_filter(f, &mut Vec::new());
    }

    /// `session_stats_metrics` — the counter metric table
    /// (upstream `stats_metric[]`).
    #[must_use]
    pub fn session_stats_metrics(&self) -> Vec<crate::counters::StatsMetric> {
        crate::counters::metrics()
    }

    /// `dht_put_item` — BEP-44 immutable put; returns the target
    /// key (`sha1(value)`). The `put` is issued when the `get`
    /// traversal completes; `dht_put_alert` posts with the count.
    pub fn dht_put_item(&self, value: &bitdaemon_bencode::Entry) -> Sha1Hash {
        let bytes = bitdaemon_bencode::bencode(value);
        lock(&self.inner).dht_put_item(&bytes)
    }

    /// `dht_put_item` (mutable) — BEP-44 signed put: signs `value`
    /// under `sk` (`seed ‖ pk`, 64 B) with `seq`/`salt`, then the
    /// `get`→`put` sequence posts `dht_put_alert` carrying the key,
    /// signature, seq and salt. Returns the target on success.
    pub fn dht_put_item_mut(
        &self,
        pk: [u8; 32],
        sk: &[u8; 64],
        salt: Vec<u8>,
        seq: i64,
        value: &bitdaemon_bencode::Entry,
    ) -> Option<Sha1Hash> {
        let bytes = bitdaemon_bencode::bencode(value);
        lock(&self.inner).dht_put_item_mut(pk, sk, salt, seq, &bytes)
    }

    /// `set_max_half_open_connections` / `max_half_open_connections`
    /// (upstream `settings_pack::half_open_limit` — deprecated but
    /// still honored as the connect-attempt cap).
    pub fn set_max_half_open_connections(&self, limit: i32) {
        lock(&self.inner).half_open_limit = limit;
    }

    /// `max_half_open_connections`.
    #[must_use]
    pub fn max_half_open_connections(&self) -> i32 {
        lock(&self.inner).half_open_limit
    }
}

/// `torrent_handle` — per-swarm commands routed to `Session`.
#[derive(Clone)]
pub struct TorrentHandle {
    session: Arc<Mutex<Session>>,
    /// `info_hash`.
    pub info_hash: Sha1Hash,
}

impl TorrentHandle {
    /// `torrent::pause` / `resume`.
    pub fn set_paused(&self, paused: bool) {
        lock(&self.session).set_paused(&self.info_hash, paused, &mut Vec::new());
    }

    /// `torrent::status(flags)` — `flags` gates which expensive
    /// fields fill (upstream `status_flags_t`, default empty).
    #[must_use]
    pub fn status(&self, flags: crate::status::StatusFlags) -> Option<TorrentStatus> {
        let g = lock(&self.session);
        g.torrents.get(&self.info_hash).map(|t| {
            t.status(
                &g.settings,
                g.queue_position(&self.info_hash),
                flags,
                &g.peer_class_pool,
            )
        })
    }

    /// `prioritize_files` — upstream `torrent_handle::prioritize_files`.
    pub fn prioritize_files(&self, prios: &[bitdaemon_common::types::DownloadPriority]) {
        let mut guard = lock(&self.session);
        let s = &mut *guard;
        let mut eff = Vec::new();
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.prioritize_files(prios, &s.settings, &s.counters, &mut eff);
        }
        s.route_into_pending(self.info_hash, eff);
    }

    /// `file_priorities` — current per-file priorities.
    #[must_use]
    pub fn file_priorities(&self) -> Vec<bitdaemon_common::types::DownloadPriority> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, |t| t.file_priorities.clone())
    }

    /// `prioritize_pieces` — direct per-piece overrides.
    pub fn prioritize_pieces(&self, prios: &[bitdaemon_common::types::DownloadPriority]) {
        let mut s = lock(&self.session);
        let mut eff = Vec::new();
        {
            let crate::session::Session {
                torrents, counters, ..
            } = &mut *s;
            if let Some(t) = torrents.get_mut(&self.info_hash) {
                t.prioritize_pieces(prios, counters, &mut eff);
            }
        }
        s.route_into_pending(self.info_hash, eff);
    }

    /// `piece_priority`.
    #[must_use]
    pub fn piece_priority(
        &self,
        index: PieceIndex,
    ) -> Option<bitdaemon_common::types::DownloadPriority> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .and_then(|t| t.piece_priority(index))
    }

    /// `set_sequential_download`.
    pub fn set_sequential_download(&self, yes: bool) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.set_sequential_download(yes);
        }
    }

    /// `force_recheck` — effects (disk hash jobs) are returned for
    /// the caller to route (a real engine drains `SessionOut`).
    pub fn force_recheck(&self, out: &mut SessionOutVec) {
        lock(&self.session).force_recheck(&self.info_hash, out);
    }

    /// `move_storage`.
    pub fn move_storage(
        &self,
        path: std::path::PathBuf,
        flags: bitdaemon_disk::storage::MoveFlags,
    ) {
        lock(&self.session).move_storage(&self.info_hash, path, flags);
    }

    /// `rename_file`.
    pub fn rename_file(&self, index: bitdaemon_common::types::FileIndex, name: std::path::PathBuf) {
        lock(&self.session).rename_file(&self.info_hash, index, name);
    }

    /// `delete_files` — removes the torrent and schedules payload
    /// deletion; `torrent_deleted{,_failed}` arrives via alerts.
    /// Upstream's `session_handle::delete_files` (`remove_flags_t`
    /// bit 0) — the bit actually reaches `storage_utils::delete_files`.
    pub fn delete_files(&self, out: &mut SessionOutVec) {
        lock(&self.session).delete_files(
            &self.info_hash,
            bitdaemon_disk::storage::remove_flags::DELETE_FILES,
            out,
        );
    }

    /// `file_progress` — per-file downloaded bytes.
    /// `piece_granularity` = upstream `file_progress_flags_t::piece_granularity`.
    #[must_use]
    pub fn file_progress(&self, piece_granularity: bool) -> Vec<i64> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, |t| t.file_progress(piece_granularity))
    }

    /// `set_piece_deadline` — time-critical pick + optional
    /// `read_piece_alert` readback (`alert_when_available`).
    /// Deadline effects are routed via `out` (engine drains it).
    pub fn set_piece_deadline(
        &self,
        piece: PieceIndex,
        deadline_ms: i64,
        alert_when_available: bool,
        out: &mut SessionOutVec,
    ) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            let mut eff = Vec::new();
            t.set_piece_deadline(
                piece,
                deadline_ms,
                alert_when_available,
                &lock(&self.session).counters,
                &mut eff,
            );
            out.extend(eff.into_iter().map(crate::session::SessionOut::Torrent));
        }
    }

    /// `reset_piece_deadline`.
    pub fn reset_piece_deadline(&self, piece: PieceIndex, out: &mut SessionOutVec) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            let mut eff = Vec::new();
            t.reset_piece_deadline(piece, &mut eff);
            out.extend(eff.into_iter().map(crate::session::SessionOut::Torrent));
        }
    }

    /// `read_piece` — read a verified piece back (posts
    /// `read_piece_alert` via the disk pipeline).
    pub fn read_piece(&self, piece: PieceIndex, out: &mut SessionOutVec) {
        out.push(crate::session::SessionOut::Torrent(
            crate::torrent::TorrentEffect::ReadPiece { piece },
        ));
    }

    /// `queue_position` — index within the auto-managed queue.
    #[must_use]
    pub fn queue_position(&self) -> i32 {
        lock(&self.session).queue_position(&self.info_hash)
    }

    /// `queue_position_up/down/top/bottom`.
    pub fn queue_position_set(&self, p: crate::session::QueueMove) {
        lock(&self.session).queue_position_set(&self.info_hash, p);
    }

    /// `force_reannounce` — announce now (all trackers or one).
    /// Upstream schedules rather than dispatches; `0` seconds lands
    /// on the next announce pass.
    pub fn force_announce(&self, tracker_idx: Option<usize>, out: &mut SessionOutVec) {
        lock(&self.session).force_announce(
            &self.info_hash,
            tracker_idx,
            0,
            crate::session::ReannounceFlags::default(),
            out,
        );
    }

    /// `force_dht_announce` — `get_peers` traversal now.
    pub fn force_dht_announce(&self) {
        lock(&self.session).force_dht_announce(&self.info_hash);
    }

    /// `flush_cache` — `async_release_files`; `cache_flushed`
    /// arrives via alerts.
    pub fn flush_cache(&self) {
        lock(&self.session).flush_cache(&self.info_hash);
    }

    /// `save_resume_data` — posts `save_resume_data_alert` with
    /// the bencoded resume blob (`only_if_modified` gate).
    pub fn save_resume_data(&self, only_if_modified: bool) {
        lock(&self.session).save_resume_data(&self.info_hash, only_if_modified);
    }

    /// `set_upload_limit` / `set_download_limit` — bytes/s, 0 = ∞.
    pub fn set_upload_limit(&self, bytes_per_sec: i64) {
        let s = &mut *lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.set_upload_limit(bytes_per_sec, &mut s.peer_class_pool);
        }
    }

    /// `set_download_limit`.
    pub fn set_download_limit(&self, bytes_per_sec: i64) {
        let s = &mut *lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.set_download_limit(bytes_per_sec, &mut s.peer_class_pool);
        }
    }

    /// `scrape_tracker` — immediate scrape of tracker entry `idx`;
    /// `-1` (or out-of-range) picks `last_working` → `first`
    /// upstream-style.
    pub fn scrape_tracker(&self, tracker_idx: i32, out: &mut SessionOutVec) {
        lock(&self.session).scrape_tracker(&self.info_hash, tracker_idx, out);
    }

    /// `get_peer_info` — `peer_info` snapshot of one connection.
    #[must_use]
    pub fn get_peer_info(
        &self,
        peer: u64,
        remote: Option<std::net::SocketAddr>,
    ) -> Option<crate::peer_info::PeerInfo> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .and_then(|t| t.peer_info(peer, remote))
    }

    /// `piece_availability` — swarm availability per piece.
    #[must_use]
    pub fn piece_availability(&self) -> Vec<i32> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, Torrent::piece_availability)
    }

    /// `get_download_queue` — partial pieces in flight.
    #[must_use]
    pub fn get_download_queue(&self) -> Vec<crate::piece_picker::DownloadingPiece> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, Torrent::get_download_queue)
    }

    /// `connect_peer` — add `remote` as an immediate dial candidate.
    pub fn connect_peer(&self, remote: std::net::SocketAddr) {
        let s = &mut *lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            // only `Alert` effects can arise here (filter gates) —
            // route them so `peer_blocked_alert` posts.
            let mut eff = Vec::new();
            t.connect_peer(remote, &mut eff);
            for e in eff {
                if let crate::torrent::TorrentEffect::Alert(a) = e {
                    s.alerts.post(a);
                }
            }
        }
    }

    /// `have_piece`.
    #[must_use]
    pub fn have_piece(&self, p: PieceIndex) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.have.get_bit(p.0 as usize))
    }

    /// `is_valid` — the torrent still exists.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        lock(&self.session).torrents.contains_key(&self.info_hash)
    }

    /// `torrent_handle::flags` — the `torrent_flags` bitset.
    #[must_use]
    pub fn flags(&self) -> bitdaemon_common::flags::TorrentFlags {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map(crate::torrent::Torrent::flags)
            .unwrap_or_default()
    }

    /// `torrent_handle::set_flags(flags, mask)` — masked flag apply;
    /// toggling `PAUSED` routes through `Session::set_paused` so the
    /// announce side-effects run.
    pub fn set_flags(
        &self,
        flags: bitdaemon_common::flags::TorrentFlags,
        mask: bitdaemon_common::flags::TorrentFlags,
    ) {
        let mut s = lock(&self.session);
        let mut eff = Vec::new();
        let pause_req = {
            let Session {
                settings,
                torrents,
                counters,
                ..
            } = &mut *s;
            torrents
                .get_mut(&self.info_hash)
                .and_then(|t| t.set_flags(flags, mask, settings, counters, &mut eff))
        };
        s.route_into_pending(self.info_hash, eff);
        // upstream `set_flags(paused)` delegates to
        // `pause(graceful_pause)` / `resume()` — `m_paused` is never
        // written by the flag apply itself.
        let mut out = Vec::new();
        if let Some(paused) = pause_req {
            if paused {
                s.set_paused_graceful(&self.info_hash, &mut out);
            } else {
                s.resume_torrent(&self.info_hash, &mut out);
            }
        }
        // `pause`/`resume` effects — `pending_out` drains on the
        // next `tick`.
        s.pending_out.append(&mut out);
    }

    /// `torrent_handle::unset_flags(flags)` shorthand.
    pub fn unset_flags(&self, flags: bitdaemon_common::flags::TorrentFlags) {
        self.set_flags(bitdaemon_common::flags::TorrentFlags::NONE, flags);
    }

    /// `torrent_handle::trackers` — the announce list snapshot.
    #[must_use]
    pub fn trackers(&self) -> Vec<(String, i32)> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map(|t| {
                t.trackers
                    .trackers
                    .iter()
                    .map(|e| (e.url.clone(), e.tier))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `torrent_handle::replace_trackers`.
    pub fn replace_trackers(&self, trackers: &[(String, i32)]) {
        {
            let mut s = lock(&self.session);
            if let Some(t) = s.torrents.get_mut(&self.info_hash) {
                t.replace_trackers(trackers);
            }
        }
    }

    /// `torrent_handle::add_tracker(url, tier)`.
    pub fn add_tracker(&self, url: String, tier: i32) {
        {
            let mut s = lock(&self.session);
            if let Some(t) = s.torrents.get_mut(&self.info_hash) {
                t.add_tracker(url, tier, crate::tracker_list::SOURCE_CLIENT);
            }
        }
    }

    /// `torrent_handle::ban_peer` — ban + drop the live connection.
    pub fn ban_peer(&self, remote: std::net::SocketAddr, out: &mut Vec<SessionOut>) {
        {
            let mut s = lock(&self.session);
            s.ban_peer(&self.info_hash, remote, out);
        }
    }

    /// `torrent_handle::get_torrent_info` — the shared metainfo
    /// (upstream `torrent_file()`); `None` while a magnet is
    /// unresolved.
    #[must_use]
    pub fn torrent_file(
        &self,
    ) -> Option<std::sync::Arc<bitdaemon_core::torrent_info::TorrentInfo>> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .and_then(|t| t.ti.clone())
    }

    /// `torrent_handle::need_save_resume_data`.
    #[must_use]
    pub fn need_save_resume_data(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.need_save_resume)
    }

    /// `torrent_handle::clear_error` — clears `m_error` /
    /// `error_file`.
    pub fn clear_error(&self) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.last_error.clear();
        }
    }

    /// `torrent_handle::clear_piece_deadlines` — drop every
    /// `set_piece_deadline` entry (upstream iterates the map).
    pub fn clear_piece_deadlines(&self) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.time_critical.clear();
        }
    }

    /// `torrent_handle::name` — torrent name from metainfo
    /// (`m_name` fallback for magnets).
    #[must_use]
    pub fn name(&self) -> String {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map(|t| {
                t.ti.as_ref().map_or_else(
                    || t.name.clone(),
                    |ti| String::from_utf8_lossy(ti.name()).into_owned(),
                )
            })
            .unwrap_or_default()
    }

    /// `torrent_handle::force_lsd_announce` — one-shot local
    /// service discovery broadcast for this torrent.
    pub fn force_lsd_announce(&self, out: &mut Vec<SessionOut>) {
        let mut s = lock(&self.session);
        s.force_lsd_announce(&self.info_hash, out);
    }

    /// `torrent_handle::add_piece` — write + verify a user-supplied
    /// piece (data must be the full piece length).
    pub fn add_piece(&self, piece: PieceIndex, data: &[u8], flags: u8) {
        let mut s = lock(&self.session);
        s.add_piece(&self.info_hash, piece, data, flags);
    }

    /// `torrent_handle::set_max_connections` (per-torrent).
    pub fn set_max_connections(&self, max: i32) {
        {
            let mut s = lock(&self.session);
            if let Some(t) = s.torrents.get_mut(&self.info_hash) {
                t.max_connections = max;
            }
        }
    }

    /// `torrent_handle::add_url_seed` — BEP-17 web seed endpoint.
    pub fn add_url_seed(&self, url: String) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.add_url_seed(url);
        }
    }

    /// `torrent_handle::remove_url_seed`.
    pub fn remove_url_seed(&self, url: &str) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.remove_url_seed(url);
        }
    }

    /// `torrent_handle::url_seeds` snapshot.
    #[must_use]
    pub fn url_seeds(&self) -> Vec<String> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map(|t| t.url_seeds().to_vec())
            .unwrap_or_default()
    }

    /// `torrent_handle::set_max_uploads` (per-torrent).
    pub fn set_max_uploads(&self, max: i32) {
        {
            let mut s = lock(&self.session);
            if let Some(t) = s.torrents.get_mut(&self.info_hash) {
                t.max_uploads = max;
            }
        }
    }

    /// `torrent_handle::pause` / `resume` (upstream flag-carried
    /// variants collapse to `set_paused`).
    pub fn pause(&self) {
        self.set_paused(true);
    }

    /// `torrent_handle::resume` — upstream `torrent::resume`
    /// re-arms the announce flags even when the user flag was
    /// already clear.
    pub fn resume(&self) {
        let mut s = lock(&self.session);
        s.resume_torrent(&self.info_hash, &mut Vec::new());
    }

    /// `torrent_handle::queue_position_up`.
    pub fn queue_position_up(&self) {
        self.queue_position_set(crate::session::QueueMove::Up);
    }

    /// `torrent_handle::queue_position_down`.
    pub fn queue_position_down(&self) {
        self.queue_position_set(crate::session::QueueMove::Down);
    }

    /// `torrent_handle::queue_position_top`.
    pub fn queue_position_top(&self) {
        self.queue_position_set(crate::session::QueueMove::Top);
    }

    /// `torrent_handle::queue_position_bottom`.
    pub fn queue_position_bottom(&self) {
        self.queue_position_set(crate::session::QueueMove::Bottom);
    }

    /// `torrent_handle::upload_limit` — current bytes/s (-1 = ∞,
    /// upstream `limit_impl` semantics; 0 on invalid handle).
    #[must_use]
    pub fn upload_limit(&self) -> i64 {
        let g = lock(&self.session);
        g.torrents
            .get(&self.info_hash)
            .map_or(0, |t| t.upload_limit(&g.peer_class_pool))
    }

    /// `torrent_handle::download_limit`.
    #[must_use]
    pub fn download_limit(&self) -> i64 {
        let g = lock(&self.session);
        g.torrents
            .get(&self.info_hash)
            .map_or(0, |t| t.download_limit(&g.peer_class_pool))
    }

    /// `torrent_handle::is_seed` — every piece verified.
    #[must_use]
    pub fn is_seed(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(Torrent::is_seed)
    }

    /// `torrent_handle::is_finished` — download complete (may still
    /// be hashing/checking in upstream terms; here = `finished`).
    #[must_use]
    pub fn is_finished(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.finished)
    }

    /// `torrent_handle::is_paused`.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.paused)
    }

    /// `torrent_handle::is_auto_managed`.
    #[must_use]
    pub fn is_auto_managed(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.auto_managed)
    }

    /// `torrent_handle::is_sequential_download`.
    #[must_use]
    pub fn is_sequential_download(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.sequential)
    }

    /// `torrent_handle::has_metadata` — `ti` resolved.
    #[must_use]
    pub fn has_metadata(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.ti.is_some())
    }

    /// `torrent_handle::super_seeding` — getter.
    #[must_use]
    pub fn super_seeding(&self) -> bool {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .is_some_and(|t| t.super_seeding)
    }

    /// `torrent_handle::super_seeding(bool)` — setter. Switching off
    /// a super-seed re-broadcasts the real bitfield (upstream sends
    /// HaveNone + Bitfield via the engine drain path).
    pub fn set_super_seeding(&self, on: bool) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.super_seeding = on;
        }
    }

    /// `torrent_handle::set_upload_mode` — toggling pushes the BEP-21
    /// `upload_only` extension message to every peer that negotiated
    /// it (upstream `torrent::send_upload_only`: skipped under
    /// super-seeding/share-mode, gated on `close_redundant_connections`).
    pub fn set_upload_mode(&self, on: bool) {
        let mut s = lock(&self.session);
        let close_redundant = s.settings.close_redundant_connections;
        let mut eff = Vec::new();
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            t.upload_mode = on;
            if close_redundant && !t.super_seeding && !t.share_mode {
                eff = t
                    .peers
                    .iter()
                    .filter(|(_, c)| c.ext_upload_only != 0)
                    .map(|(pid, c)| crate::torrent::TorrentEffect::SendTo {
                        peer: *pid,
                        msg: bitdaemon_net::peer_wire::PeerMessage::Extended {
                            id: c.ext_upload_only,
                            data: bytes::Bytes::from_static(if on { &[1] } else { &[0] }),
                        },
                    })
                    .collect();
            }
        }
        if !eff.is_empty() {
            s.route_into_pending(self.info_hash, eff);
        }
    }

    /// `torrent_handle::set_share_mode`.
    pub fn set_share_mode(&self, on: bool) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.share_mode = on;
        }
    }

    /// `torrent_handle::auto_managed`.
    pub fn set_auto_managed(&self, on: bool) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.auto_managed = on;
        }
    }

    /// `torrent_handle::set_pinned`.
    pub fn set_pinned(&self, on: bool) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.pinned = on;
        }
    }

    /// `torrent_handle::stop_when_ready`.
    pub fn stop_when_ready(&self, on: bool) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.stop_when_ready = on;
        }
    }

    /// `torrent_handle::apply_ip_filter`.
    pub fn apply_ip_filter(&self, on: bool) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.apply_ip_filter = on;
        }
    }

    /// `torrent_handle::save_path` — the save path this torrent was
    /// added with (`m_save_path`).
    #[must_use]
    pub fn save_path(&self) -> String {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(String::new, |t| t.save_path.clone())
    }

    /// `torrent_handle::set_ratio`.
    pub fn set_ratio(&self, ratio: f32) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.ratio = ratio;
        }
    }

    /// `torrent_handle::set_tracker_login` — user/pass sent to
    /// trackers (`m_tracker_login`).
    pub fn set_tracker_login(&self, user: String, pass: String) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.tracker_login = Some((user, pass));
        }
    }

    /// `torrent_handle::use_interface` — bind outbound connects to a
    /// named interface (`m_net_interface`).
    pub fn use_interface(&self, name: String) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.net_interface = Some(name);
        }
    }

    /// `torrent_handle::set_ssl_certificate` — SSL torrents: the
    /// PEM keypair this torrent serves on inbound SSL handshakes
    /// (`dh` kept for signature parity; rustls is ECDHE-only).
    /// Upstream's `set_ssl_cert_buffer` ends by posting
    /// `torrent_need_cert_alert` — the cert requirement is now met.
    pub fn set_ssl_certificate(&self, cert: String, key: String, dh: String) {
        let mut ses = lock(&self.session);
        if let Some(t) = ses.torrents.get_mut(&self.info_hash) {
            let ssl = t.is_ssl_torrent();
            t.set_ssl_cert_buffer(&cert, &key, &dh);
            if ssl {
                ses.alerts.post(crate::alerts::Alert::TorrentNeedCert {
                    ih: self.info_hash,
                    error: String::new(),
                    cert_file: String::new(),
                });
            }
        }
    }

    /// `torrent_handle::set_ssl_certificate_buffer` — PEM buffer
    /// form (same store; the path/buffer distinction is upstream's).
    pub fn set_ssl_certificate_buffer(&self, cert: String, key: String, dh: String) {
        self.set_ssl_certificate(cert, key, dh);
    }

    /// `torrent_handle::add_http_seed` — **deprecated upstream
    /// no-op**: libtorrent 2.x removed BEP-17 fetching; upstream's
    /// method body is literally `add_http_seed(std::string const&)
    /// {}`. Kept for API parity; use `add_url_seed` for working
    /// web seeds (`atp.http_seeds` from torrent_info still merges
    /// into the BEP-19 fetch path at add time).
    pub fn add_http_seed(&self, _url: String) {}

    /// `torrent_handle::remove_http_seed` — deprecated upstream
    /// no-op.
    pub fn remove_http_seed(&self, _url: &str) {}

    /// `torrent_handle::http_seeds` — upstream returns an empty
    /// set unconditionally.
    #[must_use]
    pub fn http_seeds(&self) -> Vec<String> {
        Vec::new()
    }

    /// `torrent_handle::set_peer_upload_limit`.
    pub fn set_peer_upload_limit(&self, remote: std::net::SocketAddr, limit: i64) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.set_peer_upload_limit(remote, limit);
        }
    }

    /// `torrent_handle::set_peer_download_limit`.
    pub fn set_peer_download_limit(&self, remote: std::net::SocketAddr, limit: i64) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.set_peer_download_limit(remote, limit);
        }
    }

    /// `torrent_handle::get_peer_upload_limit` (-1 = unset).
    #[must_use]
    pub fn get_peer_upload_limit(&self, remote: std::net::SocketAddr) -> i64 {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or(-1, |t| t.peer_upload_limit(remote))
    }

    /// `torrent_handle::get_peer_download_limit`.
    #[must_use]
    pub fn get_peer_download_limit(&self, remote: std::net::SocketAddr) -> i64 {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or(-1, |t| t.peer_download_limit(remote))
    }

    /// `torrent_handle::clear_peers` — drop every known swarm peer
    /// (connected or not; upstream `torrent::clear_peers` disconnects
    /// and clears the peer list).
    pub fn clear_peers(&self, out: &mut SessionOutVec) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get_mut(&self.info_hash) {
            for id in t.peers.keys().copied().collect::<Vec<_>>() {
                out.push(SessionOut::Torrent(
                    crate::torrent::TorrentEffect::DisconnectPeer {
                        peer: id,
                        reason: "clear_peers",
                        // `torrent::clear_peers` → `abort` — normal
                        // teardown, no failcount.
                        failed: false,
                    },
                ));
            }
            t.peers.clear();
        }
    }

    /// `torrent_handle::max_uploads` / `max_connections` getters.
    #[must_use]
    pub fn max_uploads(&self) -> i32 {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or(-1, |t| t.max_uploads)
    }

    /// `torrent_handle::max_connections`.
    #[must_use]
    pub fn max_connections(&self) -> i32 {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or(-1, |t| t.max_connections)
    }

    /// `torrent_handle::get_piece_priorities` — per-piece priority
    /// snapshot.
    #[must_use]
    pub fn get_piece_priorities(&self) -> Vec<bitdaemon_common::types::DownloadPriority> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, |t| {
                (0..t.num_pieces)
                    .map(|i| t.piece_priority(PieceIndex(i)).unwrap_or_default())
                    .collect()
            })
    }

    /// `torrent_handle::get_file_priorities` — alias of
    /// [`Self::file_priorities`] matching the upstream getter name.
    #[must_use]
    pub fn get_file_priorities(&self) -> Vec<bitdaemon_common::types::DownloadPriority> {
        self.file_priorities()
    }

    /// `torrent_handle::file_priority` — set one file's priority.
    /// `file_prio_alert` reports the apply verdict (empty error =
    /// success, like upstream's async `on_file_priority`).
    pub fn file_priority(
        &self,
        index: bitdaemon_common::types::FileIndex,
        prio: bitdaemon_common::types::DownloadPriority,
    ) {
        let mut guard = lock(&self.session);
        let ses = &mut *guard;
        let mut eff = Vec::new();
        if let Some(t) = ses.torrents.get_mut(&self.info_hash) {
            let mut prios = t.file_priorities.clone();
            let err = if let Some(p) = prios.get_mut(index.0 as usize) {
                *p = prio;
                t.prioritize_files(&prios, &ses.settings, &ses.counters, &mut eff);
                String::new()
            } else {
                format!("file index {} out of range", index.0)
            };
            ses.alerts.post(crate::alerts::Alert::FilePrio {
                ih: self.info_hash,
                op: "file_priority".into(),
                error: err,
            });
        }
        ses.route_into_pending(self.info_hash, eff);
    }

    /// `torrent_handle::write_resume_data` — bencoded resume blob.
    #[must_use]
    pub fn write_resume_data(&self) -> Vec<u8> {
        let g = lock(&self.session);
        g.torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, |t| t.write_resume_data(&g.peer_class_pool))
    }

    /// `torrent_handle::info_hashes` — v1/v2 pair when metadata is
    /// present; v1 id otherwise.
    #[must_use]
    pub fn info_hashes(&self) -> bitdaemon_common::info_hash::InfoHash {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(
                || bitdaemon_common::info_hash::InfoHash::from_v1(self.info_hash),
                |t| {
                    t.ti.as_ref().map_or_else(
                        || bitdaemon_common::info_hash::InfoHash::from_v1(self.info_hash),
                        |ti| *ti.info_hashes(),
                    )
                },
            )
    }

    /// `torrent_handle::torrent_file_with_hashes` — same `ti` object;
    /// upstream returns the non-const shared_ptr because it may carry
    /// later-fetched merkle trees — ours already does.
    #[must_use]
    pub fn torrent_file_with_hashes(
        &self,
    ) -> Option<std::sync::Arc<bitdaemon_core::torrent_info::TorrentInfo>> {
        self.torrent_file()
    }

    /// `torrent_handle::piece_layers` — per-file v2 piece-layer
    /// hashes (empty for v1/unresolved torrents).
    #[must_use]
    pub fn piece_layers(&self) -> Vec<Vec<bitdaemon_common::Sha256Hash>> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, |t| {
                t.merkle_trees.iter().map(|m| m.get_piece_layer()).collect()
            })
    }

    /// `torrent_handle::get_full_peer_list` — every swarm peer,
    /// connected or not (upstream `peer_list_entry` snapshots).
    #[must_use]
    pub fn get_full_peer_list(&self) -> Vec<PeerListEntry> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, |t| {
                t.full_peer_list().iter().map(PeerListEntry::from).collect()
            })
    }

    /// `torrent_handle::add_extension` — invoke the factory
    /// immediately (upstream `add_extension_fun`: the plugin is
    /// constructed now, then `torrent::add_extension` walks every
    /// existing connection through `new_connection` and replays
    /// `on_files_checked` when the storage check already ran).
    pub fn add_extension(&self, ext: crate::torrent::ExtensionFactory) {
        let plugin = ext(self);
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.add_extension_plugin(plugin);
        }
    }

    /// `torrent_handle::set_metadata` — feed raw `info` dict bytes
    /// (magnet resolution path; upstream validates against info-hash).
    #[must_use]
    pub fn set_metadata(&self, metadata: &[u8]) -> bool {
        let mut s = lock(&self.session);
        // reborrow through the guard so `torrents`/`settings` split.
        let s = &mut *s;
        let Some(t) = s.torrents.get_mut(&self.info_hash) else {
            return false;
        };
        let mut eff = Vec::new();
        let ok = matches!(
            t.set_metadata_verdict(
                metadata,
                &bitdaemon_core::torrent_info::LoadTorrentLimits::default(),
                &s.settings,
                &s.counters,
                &mut eff,
            ),
            crate::torrent::MetadataVerdict::Ok
        );
        if ok {
            // deferred storage + Interested effects — same as the
            // ut_metadata path
            eff.push(crate::torrent::TorrentEffect::MetadataReceived);
            s.route_into_pending(self.info_hash, eff);
        }
        ok
    }

    /// `torrent_handle::scrape_tracker(url)` — scrape by URL;
    /// an unknown url falls back to `last_working`/`first` upstream-style.
    pub fn scrape_tracker_url(&self, url: &str, out: &mut SessionOutVec) {
        lock(&self.session).scrape_tracker_url(&self.info_hash, url, out);
    }

    /// `torrent_handle::force_reannounce(seconds, idx, flags)` —
    /// schedule a manual reannounce on tracker `idx` at
    /// `now + seconds` (`min_announce` still applies unless
    /// `IGNORE_MIN_INTERVAL`).
    pub fn force_reannounce(
        &self,
        seconds: i32,
        idx: usize,
        flags: crate::session::ReannounceFlags,
        out: &mut SessionOutVec,
    ) {
        lock(&self.session).force_announce(
            &self.info_hash,
            Some(idx),
            i64::from(seconds),
            flags,
            out,
        );
    }

    /// `torrent_handle::force_reannounce(seconds, url, flags)` —
    /// upstream resolves `url` to the tracker entry and schedules
    /// the same manual announce; unknown URLs are ignored.
    pub fn force_reannounce_url(
        &self,
        seconds: i32,
        url: &str,
        flags: crate::session::ReannounceFlags,
        out: &mut SessionOutVec,
    ) {
        let mut s = lock(&self.session);
        let idx = s
            .torrents
            .get(&self.info_hash)
            .and_then(|t| t.trackers.trackers.iter().position(|e| e.url == url));
        if let Some(i) = idx {
            s.force_announce(&self.info_hash, Some(i), i64::from(seconds), flags, out);
        }
    }

    // ---- `post_*` async getters (upstream async_call → alert) ----

    /// `torrent_handle::post_status` — upstream posts a
    /// `state_update_alert` containing just this torrent.
    /// `flags` gates which fields fill.
    pub fn post_status(&self, flags: crate::status::StatusFlags) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get(&self.info_hash) {
            let st = t.status(
                &s.settings,
                s.queue_position(&self.info_hash),
                flags,
                &s.peer_class_pool,
            );
            s.alerts.post(Alert::StateUpdate { statuses: vec![st] });
        }
    }

    /// `torrent_handle::post_peer_info` → `peer_info_alert`.
    pub fn post_peer_info(&self) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get(&self.info_hash) {
            let peers = t
                .peers
                .keys()
                .copied()
                .filter_map(|id| t.peer_info(id, None))
                .collect();
            s.alerts.post(Alert::PeerInfo {
                ih: self.info_hash,
                peers,
            });
        }
    }

    /// `torrent_handle::post_download_queue` → `piece_info_alert`.
    pub fn post_download_queue(&self) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get(&self.info_hash) {
            let queue = t.get_download_queue();
            s.alerts.post(Alert::PieceInfo {
                ih: self.info_hash,
                queue,
            });
        }
    }

    /// `torrent_handle::post_piece_availability` →
    /// `piece_availability_alert`.
    pub fn post_piece_availability(&self) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get(&self.info_hash) {
            let availability = t.piece_availability();
            s.alerts.post(Alert::PieceAvailability {
                ih: self.info_hash,
                availability,
            });
        }
    }

    /// `torrent_handle::post_trackers` → `tracker_list_alert`.
    pub fn post_trackers(&self) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get(&self.info_hash) {
            let trackers = t
                .trackers
                .trackers
                .iter()
                .map(|e| (e.url.clone(), e.tier))
                .collect();
            s.alerts.post(Alert::TrackerList {
                ih: self.info_hash,
                trackers,
            });
        }
    }

    /// `torrent_handle::post_file_priorities` →
    /// `file_priorities_alert`.
    pub fn post_file_priorities(&self) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get(&self.info_hash) {
            let priorities = t.file_priorities.clone();
            s.alerts.post(Alert::FilePriorities {
                ih: self.info_hash,
                priorities,
            });
        }
    }

    /// `torrent_handle::post_file_progress` → `file_progress_alert`.
    /// `piece_granularity` = upstream `file_progress_flags_t::piece_granularity`:
    /// report only completed pieces, skip partial-block accounting.
    pub fn post_file_progress(&self, piece_granularity: bool) {
        let mut s = lock(&self.session);
        if let Some(t) = s.torrents.get(&self.info_hash) {
            let files = t.file_progress(piece_granularity);
            s.alerts.post(Alert::FileProgress {
                ih: self.info_hash,
                files,
            });
        }
    }

    /// `torrent_handle::post_file_status` → `file_status_alert`.
    /// Upstream returns the disk thread's open-file list; our disk
    /// pool doesn't expose per-file open state yet, so `state` is
    /// posted empty for now.
    pub fn post_file_status(&self) {
        let mut s = lock(&self.session);
        if s.torrents.contains_key(&self.info_hash) {
            s.alerts.post(Alert::FileStatus {
                ih: self.info_hash,
                state: Vec::new(),
            });
        }
    }

    /// `torrent_handle::get_resume_data` — an `add_torrent_params`
    /// reconstructed from `write_resume_data` (round-trips through
    /// the same parser a real `.resume` file would take).
    #[must_use]
    pub fn get_resume_data(&self) -> Option<bitdaemon_core::AddTorrentParams> {
        let buf = self.write_resume_data();
        if buf.is_empty() {
            return None;
        }
        bitdaemon_core::resume_data::read_resume_data(
            &buf,
            &bitdaemon_core::torrent_info::LoadTorrentLimits::default(),
        )
        .ok()
    }

    /// `torrent_handle::write_resume_data_buf` — the bencoded blob
    /// (alias matching the upstream buffer-returning overload).
    #[must_use]
    pub fn write_resume_data_buf(&self) -> Vec<u8> {
        self.write_resume_data()
    }

    /// `torrent_handle::set_sequential_range(first, last)`.
    pub fn set_sequential_range(&self, first_piece: PieceIndex, last_piece: PieceIndex) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.set_sequential_range(first_piece, last_piece);
        }
    }

    /// `torrent_handle::get_renamed_files` — `file_index → name`
    /// user renames (upstream `renamed_files`).
    #[must_use]
    pub fn get_renamed_files(&self) -> Vec<(i32, String)> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, |t| {
                t.renamed_files
                    .iter()
                    .map(|(&k, v)| (k, v.clone()))
                    .collect()
            })
    }

    /// `torrent_handle::piece_length`.
    #[must_use]
    pub fn piece_length(&self) -> i64 {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or(0, |t| t.piece_length)
    }

    /// `torrent_handle::userdata` — opaque client data slot
    /// (upstream `client_data_t`; ours is a `u64` key).
    pub fn set_userdata(&self, data: u64) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.userdata = data;
        }
    }

    /// `userdata`.
    #[must_use]
    pub fn userdata(&self) -> u64 {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or(0, |t| t.userdata)
    }

    /// `torrent_handle::write_torrent_file` — rebuild the `.torrent`
    /// dict: verbatim `info` section + the live announce list +
    /// `url-list`. Top-level metadata upstream keeps verbatim
    /// (`comment`, `created by`, `creation date`, `nodes`) isn't
    /// retained by our `TorrentInfo`, so it isn't re-emitted.
    #[must_use]
    pub fn write_torrent_file(&self) -> Option<Vec<u8>> {
        use bitdaemon_bencode::Entry;
        let s = lock(&self.session);
        let t = s.torrents.get(&self.info_hash)?;
        let ti = t.ti.as_ref()?;
        let mut d = bitdaemon_bencode::Dictionary::new();
        // group trackers into announce-list tiers like upstream
        // `torrent::write_torrent_file`.
        let mut tiers: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut per_tier: std::collections::BTreeMap<i32, Vec<Vec<u8>>> =
            std::collections::BTreeMap::new();
        for e in &t.trackers.trackers {
            per_tier
                .entry(e.tier)
                .or_default()
                .push(e.url.as_bytes().to_vec());
        }
        for list in per_tier.into_values() {
            tiers.push(list);
        }
        if let Some(first) = tiers.first().and_then(|t| t.first()) {
            d.insert(b"announce".to_vec(), Entry::String(first.clone()));
        }
        if !tiers.is_empty() {
            d.insert(
                b"announce-list".to_vec(),
                Entry::List(
                    tiers
                        .into_iter()
                        .map(|tier| Entry::List(tier.into_iter().map(Entry::String).collect()))
                        .collect(),
                ),
            );
        }
        if !t.url_seeds().is_empty() {
            d.insert(
                b"url-list".to_vec(),
                Entry::List(
                    t.url_seeds()
                        .iter()
                        .map(|u| Entry::String(u.as_bytes().to_vec()))
                        .collect(),
                ),
            );
        }
        // verbatim info section — `Entry::Preformatted` emits the
        // original bytes, preserving the info-hash exactly.
        d.insert(
            b"info".to_vec(),
            Entry::Preformatted(ti.info_section().to_vec()),
        );
        Some(bitdaemon_bencode::bencode(&Entry::Dict(d)))
    }
}
