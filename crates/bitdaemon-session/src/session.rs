// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `session_impl.cpp`/`session.cpp` (synchronous core) +
//! `session_call.cpp`/`session_interface.hpp`.
//!
//! `session` — the synchronous core of `session_impl`. One owner
//! for all torrents, the disk backend and the DHT tracker; a thin
//! async actor (mpsc commands) wraps this in the daemon.
//!
//! `SessionImpl` upstream runs on the io thread; [`Session`] is
//! transport-agnostic — sockets/streams are fed by the caller and
//! effects drained after each call, identical to the rest of the
//! port (ADR-0008 injected transport).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bitdaemon_common::types::{FileIndex, PeerRequest, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_core::file_storage::RenamedFiles;
use bitdaemon_dht::dht_tracker::DhtTracker;
use bitdaemon_disk::back_pressure::PeerWriteGates;
use bitdaemon_disk::disk_buffer_pool::DiskBufferHolder;
use bitdaemon_disk::disk_interface::DiskIo;
use bitdaemon_disk::storage::StorageParams;
use bitdaemon_disk::storage_error::StorageError;
use bitdaemon_net::peer_wire::PeerMessage;

use crate::alerts::{Alert, AlertCategory, AlertManager};
use crate::counters::{Counter, Counters};
use crate::torrent::{Torrent, TorrentEffect};

/// A decoded wire message arriving for a torrent.
#[derive(Debug)]
pub struct PeerPacket {
    /// which torrent (`info_hash`).
    pub info_hash: Sha1Hash,
    /// caller's peer id within the torrent.
    pub peer: u64,
    /// the peer's endpoint.
    pub remote: SocketAddr,
    /// the message.
    pub msg: PeerMessage,
}

/// `socket_type_idx` → `peer_class_type_filter::socket_type_t` —
/// upstream's `mapping` array (`set_peer_classes`): plain/socks5/http
/// sockets are `tcp`, their SSL flavors `ssl_tcp`, uTP is `utp`,
/// SSL-uTP is `ssl_utp`, then `i2p` and `rtc`.
fn socket_type_to_filter(kind: u8) -> bitdaemon_net::peer_class::FilterSocketType {
    use bitdaemon_net::peer_class::FilterSocketType;
    match kind {
        3 => FilterSocketType::Utp,
        4 => FilterSocketType::I2p,
        5 => FilterSocketType::Rtc,
        6..=8 => FilterSocketType::SslTcp,
        9 => FilterSocketType::SslUtp,
        // tcp / socks5 / http (0..=2) and unknown indices → tcp.
        _ => FilterSocketType::Tcp,
    }
}

/// `peer_connection`'s socket type — `socket_type_idx(m_socket)`
/// recorded on the conn at creation (`PeerConn::sock_type`).
fn conn_socket_type(
    conn: &crate::peer_conn::PeerConn,
) -> bitdaemon_net::peer_class::FilterSocketType {
    socket_type_to_filter(conn.sock_type)
}

/// `session_impl::on_incoming_connection` verdict — the engine owns
/// the accepted socket, so the session reports whether to keep it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IncomingVerdict {
    /// under the session conn cap — proceed to handshake.
    Accept,
    /// past `connections_limit` but inside `connections_slack` —
    /// `peer_exceeds_limit`: the conn must evict a lower-ranking
    /// conn (or drop) once its handshake lands.
    AcceptExceeded,
    /// past `limit + slack`, or no active torrent while
    /// `incoming_starts_queued_torrents` is off — drop the socket.
    Reject,
}

/// Session-wide output — wire sends, disk jobs, alerts.
#[derive(Debug)]
pub enum SessionOut {
    /// send bytes/msg to a peer.
    ToPeer {
        /// torrent.
        ih: Sha1Hash,
        /// peer id.
        peer: u64,
        /// the wire message.
        msg: PeerMessage,
    },
    /// UDP datagram from the DHT (injected transport).
    UdpPacket(Vec<u8>, SocketAddr),
    /// torrent-level effect routed upward (disk/alerts).
    Torrent(TorrentEffect),
    /// `alert` — surfaced to the API.
    Alert(String),
    /// announce to a tracker (the async driver performs the
    /// HTTP/UDP/WebSocket exchange, then reports back via
    /// `Session::on_announce_response`).
    TrackerAnnounce {
        /// torrent.
        ih: Sha1Hash,
        /// tracker URL.
        url: String,
        /// `trackerid` from the previous response, if any.
        tracker_id: String,
        /// `r.info_hash`'s version — which `announce_infohash`
        /// slot the reply/failure lands on (upstream runs one
        /// announce per (endpoint, info-hash version)).
        v: bitdaemon_common::info_hash::ProtocolVersion,
        /// announce parameters (event, counters, key).
        params: bitdaemon_net::tracker_client::AnnounceParams,
    },
    /// `network_config_change_alert` — interfaces changed; the
    /// driver re-runs listen setup (upstream `on_ip_change` →
    /// `reopen_listen_sockets`).
    InterfacesChanged,
    /// `session_impl::connect_to_peer` — a session-initiated dial
    /// (connect-pump hand-out, torrent connect-boost, tracker
    /// response). The engine spawns the async connect and reports
    /// the outcome via `peer_connected`/`peer_connect_failed`.
    ConnectPeer {
        /// torrent.
        ih: Sha1Hash,
        /// remote endpoint.
        ep: SocketAddr,
        /// the peer-id generated for this conn's handshake
        /// (`create_peer_connection` — per-conn `generate_peer_id`,
        /// tracked in the torrent's `outgoing_pids`).
        our_pid: bitdaemon_common::PeerId,
    },
    /// `scrape_tracker` — user-triggered scrape of one tracker
    /// entry; the driver runs the scrape exchange and reports via
    /// `Session::on_scrape_response`.
    TrackerScrape {
        /// torrent.
        ih: Sha1Hash,
        /// tracker URL (announce form — the driver derives the
        /// scrape URL / udp action).
        url: String,
        /// the info-hash version this scrape covers — upstream
        /// sends one `scrape_request` per supported version.
        v: bitdaemon_common::info_hash::ProtocolVersion,
        /// `sreq.scrape_ih` — the info-hash (truncated to 20
        /// bytes) the scrape query carries.
        info_hash: [u8; 20],
    },
    /// fetch one batch of blocks from a url seed — the async
    /// driver dials the host via the injected connector and
    /// reports through `Session::on_web_seed_block` /
    /// `Session::on_web_seed_error`.
    WebSeedFetch {
        /// torrent.
        ih: Sha1Hash,
        /// seed base url.
        url: String,
        /// file layout for range mapping.
        files: std::sync::Arc<bitdaemon_core::FileStorage>,
        /// piece size for request building.
        piece_length: i64,
        /// single-file torrent → BEP-19 absolute path.
        single_file: bool,
        /// blocks to fetch.
        reqs: Vec<bitdaemon_common::types::PeerRequest>,
    },
    /// `dht->m_add_node` — a `.torrent` `nodes` entry (host, port)
    /// to resolve and seed into `router_nodes`. Resolution goes
    /// through the injected resolver (anonymous deployments must
    /// not leak a direct DNS query).
    DhtAddNode {
        /// hostname.
        host: String,
        /// port.
        port: i32,
    },
    /// `upnp::delete_mapping`/`natpmp` unmap — the engine owns the
    /// drivers; the session resolves the handle to this target.
    PortmapDelete {
        /// `map_transport`: 0 = NAT-PMP, 1 = UPnP.
        transport: u8,
        /// `portmap_protocol`: 1 = TCP, 2 = UDP.
        protocol: u8,
        /// external port to release.
        external_port: u16,
    },
    /// `session_impl::add_port_mapping` — map an arbitrary port
    /// (upstream API; the engine forwards it to both drivers).
    PortmapAdd {
        /// `portmap_protocol`: 1 = TCP, 2 = UDP.
        protocol: u8,
        /// external port requested.
        external_port: u16,
        /// local port to expose.
        local_port: u16,
    },
}

/// What a DHT traversal's events feed — one entry per in-flight
/// lookup (ADR-0023 D4). Upstream binds a `std::function` callback
/// per call site; we keep the typed sink and translate to alerts.
#[derive(Debug)]
enum DhtResult {
    /// torrent announce `get_peers` — replies feed `peer_list`
    /// (source DHT) + `dht_reply_alert`; `Done` runs the
    /// `announce_peer` write phase with upstream's flag set
    /// (`announce_port`, `seed`, `implied_port`).
    Announce {
        /// announced info-hash.
        ih: Sha1Hash,
        /// `settings::announce_port` (0 → the real listen port).
        port: u16,
        /// `dht::announce::seed` — better scrape stats upstream.
        seed: bool,
        /// `dht::announce::implied_port` — `!announce_port &&
        /// enable_incoming_utp` (not set for ssl torrents).
        implied_port: bool,
    },
    /// user `dht_get_peers`/`dht_announce` — replies with peers
    /// post `dht_get_peers_reply_alert` (upstream
    /// `on_dht_get_peers`).
    GetPeers(Sha1Hash),
    /// user `dht_announce` — like `GetPeers` plus `announce_peer`
    /// to token-holders on `Done` (upstream `dht_announce`).
    UserAnnounce {
        /// announced info-hash.
        ih: Sha1Hash,
        /// port to announce.
        port: u16,
        /// `dht::announce::seed` — tell nodes we're seeding (they
        /// skip sending us seeds back; scrape stats upstream).
        seed: bool,
        /// `dht::announce::implied_port` — nodes record the source
        /// port instead of the `port` arg.
        implied_port: bool,
    },
    /// `dht_get_item` (immutable) — `r.v` → `dht_immutable_item_alert`.
    /// The shared `ctx` dedupes across v4/v6 families like
    /// upstream `get_immutable_item_ctx` (first item wins, posted
    /// once; all-empty still posts once on the last `Done`).
    GetItem {
        /// `sha1(v)` lookup target.
        target: Sha1Hash,
        /// shared `DhtItemCtx` id.
        ctx: u64,
    },
    /// `dht_get_item` (mutable) — `r.{v,k,seq,sig}` →
    /// `dht_mutable_item_alert`; ctx tracks best `seq` and fires
    /// `authoritative` only on the last family's `Done`.
    GetItemMutable {
        /// `sha1(pk ‖ salt)` lookup target.
        target: Sha1Hash,
        /// expected ed25519 public key.
        pk: [u8; 32],
        /// BEP-44 salt.
        salt: Vec<u8>,
        /// shared `DhtItemCtx` id.
        ctx: u64,
        /// this family's best `(seq, v, sig)` so far — re-posted
        /// as `authoritative` at `Done` like upstream's
        /// `get_item::done` re-delivering `m_data`.
        best: Option<(i64, Vec<u8>, [u8; 64])>,
    },
    /// `dht_put_item`/`dht_put_item_mut` — the `get` traversal
    /// collects write tokens; `Done` issues the `put` storm whose
    /// `PutDone`s aggregate into the shared `ctx` before a single
    /// `dht_put_alert` (upstream `put_item_ctx`).
    Put {
        /// staged item to write.
        pending: PendingPut,
        /// shared `DhtPutCtx` id.
        ctx: u64,
    },
    /// routing-table bootstrap — `Done` → `dht_bootstrap_alert`.
    Bootstrap,
}

/// `dht_tracker`'s `get_immutable_item_ctx`/`get_mutable_item_ctx`
/// — one shared ctx per `dht_get_item*` call across every bound
/// node family.
#[derive(Debug)]
struct DhtItemCtx {
    /// families still traversing.
    active: usize,
    /// an alert was already posted for this call.
    posted: bool,
    /// highest mutable `seq` posted so far (`-1` = none).
    best_seq: i64,
}

/// `dht_tracker`'s `put_item_ctx` — sums `num_success` across
/// families and fires the alert once at `active == 0`.
#[derive(Debug)]
struct DhtPutCtx {
    /// put batches still awaiting `PutDone`.
    active: usize,
    /// summed successful put replies.
    responses: u32,
    /// carried fields for the final `dht_put_alert`.
    pending: DhtPutAlertFields,
}

/// The alert payload a `DhtPutCtx` keeps until all batches finish.
#[derive(Debug)]
enum DhtPutAlertFields {
    /// `dht_put_item` — target + num only.
    Immutable {
        /// `sha1(v)` target.
        target: Sha1Hash,
    },
    /// `dht_put_item_mut` — full signed fields.
    Mutable {
        /// `sha1(pk ‖ salt)` target.
        target: Sha1Hash,
        /// public key.
        pk: [u8; 32],
        /// signature.
        sig: [u8; 64],
        /// sequence number.
        seq: i64,
        /// salt.
        salt: Vec<u8>,
    },
}

/// BEP-44 item staged for its `get`-then-`put` sequence.
#[derive(Debug)]
enum PendingPut {
    /// `dht_put_item(value)` — `target = sha1(v)`.
    Immutable {
        /// bencoded value.
        value: Vec<u8>,
    },
    /// `dht_put_item_mut` — signed mutable item.
    Mutable {
        /// bencoded value.
        value: Vec<u8>,
        /// ed25519 public key.
        pk: [u8; 32],
        /// signature over `canonical_string(v, seq, salt)`.
        sig: [u8; 64],
        /// sequence number.
        seq: i64,
        /// salt.
        salt: Vec<u8>,
    },
}

/// What a `direct_request` reply feeds (the `userdata` slot
/// upstream hands to the caller's callback).
#[derive(Debug)]
enum DhtDirectSink {
    /// `dht_direct_request` — `dht_direct_response_alert`.
    User(u64),
    /// `dht_sample_infohashes` — `dht_sample_infohashes_alert`.
    Sample,
}

/// One `port_mapping_t` handle's target (upstream keeps the same
/// triple in `mapping_t`).
#[derive(Debug, Clone)]
struct PortmapTarget {
    /// `map_transport`: 0 = NAT-PMP, 1 = UPnP.
    transport: u8,
    /// `portmap_protocol`: 1 = TCP, 2 = UDP.
    protocol: u8,
    /// local (internal) port.
    local_port: u16,
    /// external port the router assigned.
    external_port: u16,
    /// `local_address` — interface the mapper runs on.
    local_addr: String,
}

/// `session_impl` — all state owned on one thread.
/// Per-purpose proxy settings (`m_peer_proxy`/`m_tracker_proxy`/
/// `m_web_seed_proxy`/`m_dht_proxy`/`m_i2p_proxy` upstream).
#[derive(Clone, Debug, Default)]
pub struct SessionProxies {
    /// `settings_pack::proxy_*` — peer connections.
    pub peer: bitdaemon_net::proxy::ProxySettings,
    /// `tracker_proxy` — tracker announces.
    pub tracker: bitdaemon_net::proxy::ProxySettings,
    /// `web_seed_proxy`.
    pub web_seed: bitdaemon_net::proxy::ProxySettings,
    /// `dht_proxy`.
    pub dht: bitdaemon_net::proxy::ProxySettings,
    /// `i2p_proxy` — SAM bridge address.
    pub i2p: bitdaemon_net::proxy::ProxySettings,
}

/// `start_*`/`stop_*` service switches (upstream tears down and
/// rebuilds `m_natpmp`/`m_upnp`/`m_lsd` objects; we keep the objects
/// and gate their ticks — same observable behavior).
#[derive(Clone, Debug)]
pub struct ServiceSwitches {
    /// `start_dht`/`stop_dht`.
    pub dht: bool,
    /// `start_lsd`/`stop_lsd` — LSD multicast announces gated.
    pub lsd: bool,
    /// `start_natpmp`/`stop_natpmp`.
    pub natpmp: bool,
    /// `start_upnp`/`stop_upnp`.
    pub upnp: bool,
}

impl Default for ServiceSwitches {
    fn default() -> Self {
        Self {
            dht: true,
            lsd: true,
            natpmp: true,
            upnp: true,
        }
    }
}

pub struct Session {
    /// `m_torrents` by info_hash.
    pub torrents: BTreeMap<Sha1Hash, Torrent>,
    /// `m_dht` — v4/v6 tracker nodes.
    pub dht: DhtTracker,
    /// `m_disk_thread` — injected backend (`StorageFactory` seam).
    pub disk: Option<Arc<dyn DiskIo>>,
    /// `peer_info::bw_disk` registry shared with the engine's socket
    /// pumps — a per-`(info_hash, peer)` read gate, set while that
    /// peer's own disk write is in flight over the queue limit and
    /// cleared the moment it completes (ADR: replaces the old global
    /// freeze-until-low-watermark gate that stalled every peer for as
    /// long as the slowest flush in a burst took).
    pub write_gates: Option<Arc<PeerWriteGates<(Sha1Hash, u64)>>>,
    /// `m_alert_manager`.
    pub alerts: AlertManager,
    /// `m_stats_counters` — `Arc` so transport sinks
    /// (`utp_counter_sink`) can post increments off-thread like
    /// upstream's manager→counters calls.
    pub counters: Arc<Counters>,
    /// `m_peer_id` — generated with the default fingerprint
    /// (`generate_peer_id`); used in announces and handshakes.
    pub peer_id: bitdaemon_common::PeerId,
    /// `m_listen_port` — our listen port (`announce request.port`).
    pub listen_port: u16,
    /// `m_ssl_listen_port` — advertised SSL listen port
    /// (`ssl_listen_port` upstream; SSL sockets share the listen set).
    pub ssl_listen_port: u16,
    /// `m_cached_interfaces` — lazily filled interface enumeration
    /// for `verify_bound_address` (`outgoing_interfaces` device-name
    /// matching); refreshed when a lookup misses.
    pub cached_interfaces: Vec<bitdaemon_net::enum_net::IpInterface>,
    /// `m_listen_sockets` — one per listen interface; the session's
    /// single-socket default holds the bound port. Handles resolve
    /// against this vec (`weak_ptr<listen_socket_t>` upstream).
    pub listen_sockets: Vec<crate::listen::ListenSocket>,
    /// `m_udp_sockets` — `session_udp_socket` per listen socket.
    pub udp_sockets: Vec<crate::listen::SessionUdpSocket>,
    /// `m_ip_notifier` — interface-change detector polled in tick.
    pub ip_notifier: bitdaemon_net::enum_net::IpNotifier,
    /// `reopen_network_sockets` pending flag — emitted as
    /// `SessionOut::InterfacesChanged` on the next `tick`.
    pub reopen_sockets: bool,
    /// `settings_pack::half_open_limit` — cap on half-open TCP
    /// connects (-1 = unlimited, upstream default).
    pub half_open_limit: i32,
    /// `m_key` — announce key (per-session random, BEP-7).
    pub key: i32,
    /// completions pushed by disk-job callbacks (any thread).
    disk_tx: std::sync::mpsc::Sender<DiskCompleted>,
    disk_rx: std::sync::mpsc::Receiver<DiskCompleted>,
    /// `m_cookie` — filters our own multicast loopback (LSD).
    pub lsd_cookie: u32,
    /// queue sequence counter — each torrent's `queue_seq`
    /// (upstream `torrent::m_sequence_number`) orders the
    /// auto-managed queues.
    pub next_queue_seq: i64,
    /// `m_lsd_torrents` — `prioritize_lsd` queue; drained ahead of
    /// the round-robin in `lsd_tick` (upstream caps their wait at
    /// 4 s; our 1 s tick is inside that bound).
    lsd_queue: std::collections::VecDeque<Sha1Hash>,
    /// `m_next_lsd_torrent` — round-robin cursor into the sorted
    /// torrent set for LSD announces.
    lsd_rr_cursor: usize,
    /// millisecond budget pacing for `lsd_tick` — the announce
    /// timer upstream fires every `local_service_announce_interval/
    /// N` ms; our 1 s tick accrues 1000 ms and spends it in
    /// `cost`-sized slots.
    lsd_budget_ms: i64,
    /// last disk-pool idle reaping — upstream gives every
    /// `disk_io_thread_pool` a 60 s `deadline_timer`
    /// (`reap_idle_threads`); the session tick drives ours.
    last_disk_reap: Option<Instant>,
    /// `m_session_time` — seconds since session start, bumped once
    /// per `tick` (peer_list reconnect backoff epoch).
    pub session_time: i32,
    /// `m_last_choke` — timestamp of the last
    /// `recalculate_unchoke_slots`; the rate-based choker measures
    /// the real interval between rounds (`unchoke_sort` divides
    /// `uploaded_in_last_round` by it).
    last_choke: Option<Instant>,
    /// `m_unchoke_time_scaler = 0` sites that fire mid-recalc —
    /// `recalculate_optimistic_unchoke_slots` re-arms the regular
    /// choker when optimistic unchokes push
    /// `num_peers_up_unchoked_all` past `num_unchoke_slots`
    /// (upstream `session_impl.cpp:4478`). Consumed by `unchoke_tick`.
    pending_unchoke: bool,
    /// `m_ip_filter` — access filter applied to new + existing peers.
    /// Shared (`Arc`) so each torrent can hold the same instance —
    /// upstream passes `shared_ptr<ip_filter>` into `torrent::
    /// set_ip_filter`.
    ip_filter: std::sync::Arc<bitdaemon_net::ip_filter::IpFilter>,
    /// `m_paused` — session-wide pause (upstream pauses via
    /// `session_handle::pause()` → every torrent).
    pub paused: bool,
    /// `m_classes` — the session's `peer_class_pool`.
    pub peer_class_pool: bitdaemon_net::peer_class::PeerClassPool,
    /// `m_peer_class_filter` — upstream types it as `ip_filter`
    /// (IP ranges map to peer classes).
    pub peer_class_filter: bitdaemon_net::ip_filter::IpFilter,
    /// `m_peer_class_type_filter` — socket-type → class mapping.
    pub peer_class_type_filter: bitdaemon_net::peer_class::PeerClassTypeFilter,
    /// `m_port_filter` — `session_handle::set_port_filter`; applied
    /// to incoming + outgoing peers (`port_filter_updated` upstream).
    /// `Arc` so torrents share one instance (`get_port_filter()`).
    pub port_filter: std::sync::Arc<bitdaemon_net::ip_filter::PortFilter>,
    /// `m_global_class` — session-wide rate limits live on this
    /// class's channels (`upload_rate_limit`/`download_rate_limit`
    /// settings map here). Upstream asserts id 0.
    pub global_peer_class: bitdaemon_net::peer_class::PeerClassIndex,
    /// `m_tcp_peer_class` — added to TCP/SSL-TCP/I2P conns by the
    /// type filter; carries the `mixed_mode_algorithm` throttle.
    pub tcp_peer_class: bitdaemon_net::peer_class::PeerClassIndex,
    /// `m_local_class` — `ignore_unchoke_slots` + 150% connection
    /// factor; `local_*_rate_limit` settings map here.
    pub local_peer_class: bitdaemon_net::peer_class::PeerClassIndex,
    /// `m_download_rate` — the download-channel `BandwidthManager`
    /// (queued quota requests + per-tick distribution).
    pub download_rate: bitdaemon_net::bandwidth::BandwidthManager,
    /// `m_upload_rate` — the upload-channel manager.
    pub upload_rate: bitdaemon_net::bandwidth::BandwidthManager,
    /// per-purpose proxies (`m_peer_proxy`, `m_tracker_proxy`,
    /// `m_web_seed_proxy`, `m_dht_proxy`, `m_i2p_proxy` upstream).
    pub proxies: SessionProxies,
    /// `m_pe_settings` — MSE/PE crypto policy (`pe_settings`).
    pub pe_settings: bitdaemon_net::pe_crypto::PeSettings,
    /// `m_natpmp`/`m_upnp`/`m_lsd`/`m_dht` service switches.
    pub services: ServiceSwitches,
    /// `settings_pack` — typed session knobs (`m_settings` upstream).
    pub settings: crate::settings::SessionSettings,
    /// `m_ip_voter` — external-address voting (DHT `ip`, tracker
    /// `external ip`, peer `yourip`).
    pub ip_voter: bitdaemon_net::ip_voter::IpVoter,
    /// `(v6, lookup idx)` → what the traversal's events feed —
    /// replaces the `dht_lookup_map`/`dht_pending_put` pair
    /// (ADR-0023 D4): every lookup registers its sink so replies
    /// are never silently dropped.
    dht_result_map: std::collections::BTreeMap<(bool, usize), DhtResult>,
    /// `direct_request` tag → what the reply feeds (upstream's
    /// `userdata` callback slot).
    dht_direct_map: std::collections::BTreeMap<u64, DhtDirectSink>,
    /// `dht_get_item*` cross-family ctxs (upstream
    /// `get_immutable_item_ctx`/`get_mutable_item_ctx`).
    dht_item_ctxs: std::collections::BTreeMap<u64, DhtItemCtx>,
    /// `dht_put_item*` cross-family ctxs (`put_item_ctx`).
    dht_put_ctxs: std::collections::BTreeMap<u64, DhtPutCtx>,
    /// `(v6, put batch tag)` → `dht_put_ctxs` key — a `PutDone`
    /// resolves into its call's shared ctx.
    dht_put_tags: std::collections::BTreeMap<(bool, u64), u64>,
    /// ctx id allocator for `dht_item_ctxs`/`dht_put_ctxs`.
    next_dht_ctx: u64,
    /// `port_mapping_t` handle → the mapping it refers to
    /// (`m_portmap_handles` upstream).
    portmap_handles: std::collections::BTreeMap<i32, PortmapTarget>,
    /// next `port_mapping_t` value.
    next_portmap_handle: i32,
    /// effects queued by `SessionHandle` calls that can't run in
    /// sync context (port-map deletes) — drained into `tick`'s out.
    pub pending_out: Vec<SessionOut>,
    /// `session_stats_header_alert` posted once (upstream posts it
    /// the first time `post_session_stats` runs).
    stats_header_posted: bool,
    /// `external_udp_port()` — the port our DHT socket listens on,
    /// advertised to peers via the `port` message (BEP-5 §port).
    pub dht_port: Option<u16>,
    /// last `session_time` a DHT announce ran per torrent
    /// (`m_dht_announce_timer` upstream).
    dht_last_announce: std::collections::BTreeMap<Sha1Hash, i32>,
    /// `m_dht_torrents` — `prioritize_dht` queue; drained ahead of
    /// the round-robin every tick.
    dht_queue: std::collections::VecDeque<Sha1Hash>,
    /// `m_next_dht_torrent` — round-robin cursor for the paced
    /// announce (one torrent per `dht_announce_interval/N` ms).
    dht_rr_cursor: usize,
    /// millisecond budget for the paced announce — each tick adds
    /// 1000, each announce costs `dht_announce_interval/N` ms.
    dht_announce_budget_ms: i64,
    /// `m_boost_connections` — dials handed out by torrent
    /// connect-boosts (tracker responses) since the last connect
    /// pump; deducted from `connection_speed` per
    /// `try_connect_more_peers`.
    boost_connections: i32,
    /// `m_prio_torrents` — `(torrent, remaining connect tickets)`
    /// queue from `prioritize_connections`; drained ahead of the
    /// round-robin each pump pass.
    prio_torrents: std::collections::VecDeque<(Sha1Hash, i32)>,
    /// `m_next_downloading_connect_torrent` — round-robin cursor
    /// into the per-tick `want_peers_download` partition.
    next_downloading_connect_torrent: usize,
    /// `m_next_finished_connect_torrent` — same for
    /// `want_peers_finished`.
    next_finished_connect_torrent: usize,
    /// `m_disconnect_time_scaler` — seconds until the next
    /// `peer_turnover` disconnect sweep (init 90 upstream).
    disconnect_time_scaler: i32,
    /// `m_auto_scrape_time_scaler` — seconds until the next
    /// auto-scrape of a paused auto-managed torrent (init 180).
    auto_scrape_time_scaler: i32,
    /// `m_next_scrape_torrent` — round-robin cursor into the
    /// per-tick `want_scrape` partition.
    next_scrape_torrent: usize,
    /// `m_download_connect_attempts` — enforces the
    /// `connect_seed_every_n_download` finished:download ratio.
    download_connect_attempts: i32,
    /// last tick's disk back-pressure state — `performance_alert`
    /// fires on the rising edge only.
    prev_disk_bp: bool,
    /// last seen failed-flush count — `file_error_alert` fires per
    /// increment (upstream `handle_disk_error`).
    prev_flush_errors: i64,
    /// `m_auto_manage_time_scaler` — ticks until the next
    /// auto-manage pass; `add_torrent` forces 0 (immediate).
    auto_manage_scaler: i32,
    /// `m_next_port` — the `outgoing_port`/`num_outgoing_ports`
    /// round-robin cursor for `bind_outgoing_socket`.
    next_outgoing_port: i32,
    /// last tick's session-wide IP-overhead counters — the
    /// `download_ip_overhead() >= down_limit` alert compares the
    /// *per-tick* amount (upstream `stat_channel::counter()` resets
    /// each `second_tick`).
    prev_ip_overhead: [i64; 2],
    /// `m_dht_nodes` — `add_dht_node` calls that arrive while no
    /// DHT node is bound queue here (cap 200, random replace) and
    /// drain into `add_node` on the next `start_dht`.
    dht_nodes: Vec<SocketAddr>,
    /// `m_dht_router_nodes` — routers persist across DHT restarts;
    /// every `start_dht` re-adds them.
    dht_router_nodes: Vec<SocketAddr>,
    /// `session_params::dht_state` — saved `(local addr, nid)`
    /// pairs consulted when a node binds (`new_socket` resolves
    /// its id from `m_state.nids` upstream); refreshed by
    /// `load_state` so a late bind still picks the stored id.
    pub(crate) dht_state_seed: bitdaemon_dht::dht_state::DhtState,
}

/// `reannounce_flags_t` — per-request flags on
/// `force_reannounce`/`force_tracker_request` (bitfield, ABI
/// values preserved).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReannounceFlags(pub u8);

impl ReannounceFlags {
    /// `torrent_handle::ignore_min_interval` — bypass the
    /// tracker's `min interval` so the announce may fire now.
    pub const IGNORE_MIN_INTERVAL: Self = Self(1);
    /// `torrent_handle::high_priority` — queue-jump the request
    /// at the tracker manager (dispatch immediately).
    pub const HIGH_PRIORITY: Self = Self(2);
    /// `flags & f`.
    #[must_use]
    pub const fn contains(self, f: Self) -> bool {
        self.0 & f.0 != 0
    }
}

impl std::ops::BitOr for ReannounceFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// `dht::announce_flags_t` — flags on `session_handle::dht_announce`
/// (bitfield, ABI values preserved).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DhtAnnounceFlags(pub u8);

impl DhtAnnounceFlags {
    /// `dht::announce::seed` — we're seeding; nodes treat us as a
    /// seed for scrape stats.
    pub const SEED: Self = Self(1);
    /// `dht::announce::implied_port` — nodes record the packet's
    /// source port as our listen port (NAT-friendly).
    pub const IMPLIED_PORT: Self = Self(2);
    /// `dht::announce::ssl_torrent` — the announce is for an SSL
    /// torrent (the port resolves on the SSL listen socket
    /// upstream; single-socket sessions carry the flag for
    /// wire-parity only).
    pub const SSL_TORRENT: Self = Self(4);
    /// `flags & f`.
    #[must_use]
    pub const fn contains(self, f: Self) -> bool {
        self.0 & f.0 != 0
    }
}

impl std::ops::BitOr for DhtAnnounceFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// `move_queue_position` — upstream `queue_position_set` ops.
#[derive(Clone, Copy, Debug)]
pub enum QueueMove {
    /// `queue_position_top`.
    Top,
    /// `queue_position_bottom`.
    Bottom,
    /// `queue_position_up` — swap with the previous entry.
    Up,
    /// `queue_position_down` — swap with the next entry.
    Down,
}

/// `aux::torrent::disk_class` — only `write` qualifies a disk error
/// for the upload-mode soft-fail path in `on_disk_error`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskJobClass {
    /// `disk_class::none` — reads, hashes, check jobs.
    None,
    /// `disk_class::write` — peer block writes.
    Write,
}

/// A disk job's result, delivered back onto the session thread.
/// (`disk_io_job` completion → `torrent::on_disk_*`.)
pub enum DiskCompleted {
    /// `async_read` — block ready to send as `Piece`.
    Read {
        /// torrent.
        ih: Sha1Hash,
        /// requesting peer.
        peer: u64,
        /// the request it answers.
        req: PeerRequest,
        /// payload (`None` bytes when the job errored).
        buf: DiskBufferHolder,
        /// job result.
        err: StorageError,
        /// when the job was issued — `on_disk_read_complete`
        /// blends `now - issue_time` into `request_latency`.
        issue_time: std::time::Instant,
    },
    /// `async_write` — block persisted; schedule piece hash.
    Write {
        /// torrent.
        ih: Sha1Hash,
        /// the peer that sent this block (`on_disk_write_complete`'s
        /// `this` upstream) — clears its `bw_disk` read gate once its
        /// own `outstanding_writing_bytes` drains to zero.
        peer: u64,
        /// block written.
        req: PeerRequest,
        /// `picker_gen` — picker generation at dispatch; upstream
        /// drops the completion entirely (success or failure) when
        /// a `force_recheck` reset the picker in between.
        picker_gen: u8,
        /// job result.
        err: StorageError,
    },
    /// `disk_observer::on_disk` — the global write queue drained
    /// below the low watermark; a `bw_disk`-gated conn releases its
    /// read gate here even with writes still outstanding (upstream
    /// clears `bw_disk` unconditionally and re-runs `setup_receive`).
    PeerDrain {
        /// torrent.
        ih: Sha1Hash,
        /// gated conn.
        peer: u64,
    },
    /// `async_check_files` — fastresume validation verdict; `ret` is
    /// upstream's pure `status_t` (`disk_status` bits:
    /// `need_full_check`, `fatal_disk_error`, `oversized_file`,
    /// plus the `initialize()` flags).
    CheckFiles {
        /// torrent.
        ih: Sha1Hash,
        /// resume accepted — neither `need_full_check` nor
        /// `fatal_disk_error` set on the returned `status_t`.
        accepted: bool,
        /// the job's `status_t` (`disk_status` bits) verbatim.
        status: i32,
        /// job result.
        err: StorageError,
    },
    /// `async_move_storage` verdict.
    MoveStorage {
        /// torrent.
        ih: Sha1Hash,
        /// new `save_path`.
        path: std::path::PathBuf,
        /// the job's `status_t` (`disk_status` bits) — `file_exist`
        /// and `fatal_disk_error` mark failure; `need_full_check`
        /// triggers a recheck on success.
        status: i32,
        /// job result.
        err: StorageError,
    },
    /// `async_set_file_priority` verdict — `prios` is the storage's
    /// adjusted vector (upstream writes it back into the job).
    SetFilePriority {
        /// torrent.
        ih: Sha1Hash,
        /// storage-adjusted priorities.
        prios: Vec<bitdaemon_common::types::DownloadPriority>,
        /// job result.
        err: StorageError,
    },
    /// `async_rename_file` verdict.
    RenameFile {
        /// torrent.
        ih: Sha1Hash,
        /// the file.
        index: bitdaemon_common::types::FileIndex,
        /// requested name.
        name: std::path::PathBuf,
        /// job result.
        err: StorageError,
    },
    /// `read_piece` — whole-piece read for `read_piece_alert`.
    ReadPiece {
        /// torrent.
        ih: Sha1Hash,
        /// the piece.
        piece: PieceIndex,
        /// payload (empty on error).
        data: Vec<u8>,
        /// job result.
        err: StorageError,
    },
    /// `async_release_files` done (upstream `cache_flushed`).
    ReleaseFiles {
        /// torrent.
        ih: Sha1Hash,
    },
    /// `async_stop_torrent` drained — upstream `on_torrent_paused`:
    /// the storage is closed/flushed, so the deferred
    /// `torrent_paused_alert` may now post.
    Stopped {
        /// torrent.
        ih: Sha1Hash,
    },
    /// `async_clear_piece` fence drained — safe to restore the piece
    /// (upstream's `on_piece_fail_sync` continuation).
    ClearPiece {
        /// torrent.
        ih: Sha1Hash,
        /// the piece whose writes were fenced.
        piece: PieceIndex,
    },
    /// `async_clear_piece` fence drained after a *hash* failure —
    /// upstream's `on_piece_sync`: restore only the bad `blocks`
    /// (whole piece when empty), post `hash_failed_alert`, re-mark
    /// in-flight duplicate blocks.
    PieceSync {
        /// torrent.
        ih: Sha1Hash,
        /// the piece whose writes were fenced.
        piece: PieceIndex,
        /// v2 known-bad block indices (empty = whole piece failed).
        blocks: Vec<i32>,
    },
    /// `async_delete_files` verdict.
    DeleteFiles {
        /// torrent.
        ih: Sha1Hash,
        /// job result.
        err: StorageError,
    },
    /// `async_hash` — SHA-1 of `piece` is `hash` (compare pending
    /// — Torrent does not yet hold `TorrentInfo` piece hashes).
    Hash {
        /// torrent.
        ih: Sha1Hash,
        /// the piece.
        piece: PieceIndex,
        /// computed SHA-1.
        hash: bitdaemon_common::Sha1Hash,
        /// computed per-block SHA-256s (v2 pieces; empty for v1).
        block_hashes: Vec<bitdaemon_common::Sha256Hash>,
        /// `picker_gen` — the torrent's picker generation at dispatch;
        /// a reset between dispatch and completion makes the result
        /// stale (upstream drops it before touching picker state).
        picker_gen: u8,
        /// job result.
        err: StorageError,
    },
    /// seed-mode lazy verification (`peer_connection::
    /// on_seed_mode_hashed`) — a peer requested a promised piece we
    /// hadn't hashed yet; `peer` drives the per-peer outstanding
    /// counter and the `fill_send_buffer` re-run.
    SeedModeHash {
        /// torrent.
        ih: Sha1Hash,
        /// requesting peer.
        peer: u64,
        /// the piece.
        piece: PieceIndex,
        /// computed SHA-1.
        hash: bitdaemon_common::Sha1Hash,
        /// computed per-block SHA-256s (v2; empty for v1).
        block_hashes: Vec<bitdaemon_common::Sha256Hash>,
        /// job result.
        err: StorageError,
    },
}

/// `disk_observer` — upstream hands `async_write` the `peer_connection`
/// itself (`self()`); the back-pressure layer holds it weakly and posts
/// `on_disk()` onto the io_context once the write queue drains below
/// the low watermark. Ours reports through the same channel as disk-job
/// completions so the `bw_disk`/`num_peers_down_disk` bookkeeping stays
/// on the session task.
struct PeerDiskObserver {
    /// completion channel (same sender the disk-job handlers clone).
    tx: std::sync::mpsc::Sender<DiskCompleted>,
    /// torrent.
    ih: Sha1Hash,
    /// gated conn.
    peer: u64,
}

impl bitdaemon_disk::back_pressure::DiskObserver for PeerDiskObserver {
    /// `peer_connection::on_disk` — runs on a disk worker thread
    /// (the default poster); the actual state flip happens when the
    /// session drains `PeerDrain`.
    fn on_disk(&self) {
        let _ = self.tx.send(DiskCompleted::PeerDrain {
            ih: self.ih,
            peer: self.peer,
        });
    }
}

impl Default for Session {
    fn default() -> Self {
        let (disk_tx, disk_rx) = std::sync::mpsc::channel();
        Self {
            torrents: BTreeMap::new(),
            dht: DhtTracker::default(),
            disk: None,
            write_gates: None,
            alerts: AlertManager::default(),
            counters: Arc::new(Counters::default()),

            peer_id: bitdaemon_common::peer_id::generate_peer_id(b"-OB0001-"),
            listen_port: 6881,
            ssl_listen_port: 4433,
            listen_sockets: {
                // default single IPv4-unspecified listen socket —
                // `setup_listener` expands `listen_interfaces` into
                // one per interface; transports bind externally.
                let s = crate::listen::ListenSocket::new(
                    std::net::SocketAddr::new(
                        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                        6881,
                    ),
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    String::new(),
                );
                vec![s]
            },
            cached_interfaces: Vec::new(),
            udp_sockets: Vec::new(),
            paused: false,
            peer_class_pool: bitdaemon_net::peer_class::PeerClassPool::new(),
            peer_class_filter: bitdaemon_net::ip_filter::IpFilter::new(),
            peer_class_type_filter: bitdaemon_net::peer_class::PeerClassTypeFilter::default(),
            port_filter: std::sync::Arc::new(bitdaemon_net::ip_filter::PortFilter::new()),
            // `PeerClassPool::new` pre-creates `global` (0) and `tcp`
            // (1); `local` (2) is made in `Session::new`.
            global_peer_class: 0,
            tcp_peer_class: 1,
            local_peer_class: 2,
            // `get_bandwidth_manager(channel)` — one manager per
            // direction, keyed by the `peer_connection` channel id.
            download_rate: bitdaemon_net::bandwidth::BandwidthManager::new(
                crate::peer_conn::DOWNLOAD_CHANNEL as i32,
            ),
            upload_rate: bitdaemon_net::bandwidth::BandwidthManager::new(
                crate::peer_conn::UPLOAD_CHANNEL as i32,
            ),
            proxies: SessionProxies::default(),
            pe_settings: bitdaemon_net::pe_crypto::PeSettings::default(),
            services: ServiceSwitches::default(),
            ip_notifier: bitdaemon_net::enum_net::IpNotifier::new(),
            reopen_sockets: false,
            half_open_limit: -1,
            key: {
                use rand::Rng as _;
                rand::rng().random::<i32>()
            },
            lsd_cookie: rand::random::<u32>(),
            lsd_queue: std::collections::VecDeque::new(),
            lsd_rr_cursor: 0,
            lsd_budget_ms: 0,
            next_queue_seq: 0,
            last_disk_reap: None,
            session_time: 0,
            last_choke: None,
            pending_unchoke: false,
            ip_filter: std::sync::Arc::new(bitdaemon_net::ip_filter::IpFilter::new()),
            settings: crate::settings::SessionSettings::default(),
            ip_voter: bitdaemon_net::ip_voter::IpVoter::new(),
            dht_result_map: std::collections::BTreeMap::new(),
            dht_direct_map: std::collections::BTreeMap::new(),
            dht_item_ctxs: std::collections::BTreeMap::new(),
            dht_put_ctxs: std::collections::BTreeMap::new(),
            dht_put_tags: std::collections::BTreeMap::new(),
            next_dht_ctx: 0,
            portmap_handles: std::collections::BTreeMap::new(),
            next_portmap_handle: 0,
            pending_out: Vec::new(),
            stats_header_posted: false,

            boost_connections: 0,
            prio_torrents: std::collections::VecDeque::new(),
            next_downloading_connect_torrent: 0,
            next_finished_connect_torrent: 0,
            disconnect_time_scaler: 90,
            auto_scrape_time_scaler: 180,
            next_scrape_torrent: 0,
            download_connect_attempts: 0,
            prev_disk_bp: false,
            prev_flush_errors: 0,
            dht_port: None,
            dht_last_announce: std::collections::BTreeMap::new(),
            dht_queue: std::collections::VecDeque::new(),
            dht_rr_cursor: 0,
            dht_announce_budget_ms: 0,
            auto_manage_scaler: 0,
            next_outgoing_port: -1,
            prev_ip_overhead: [0; 2],
            dht_nodes: Vec::new(),
            dht_router_nodes: Vec::new(),
            dht_state_seed: bitdaemon_dht::dht_state::DhtState::default(),
            disk_tx,
            disk_rx,
        }
    }
}

impl Session {
    /// `session_impl::session_impl`.
    #[must_use]
    pub fn new() -> Self {
        let mut s = Self::default();
        // `session_impl` ctor — the `local` class (id 2) is unchoke-
        // exempt and gets a 150% connection-limit factor.
        let local = s.peer_class_pool.new_peer_class("local");
        debug_assert_eq!(local, s.local_peer_class);
        if let Some(c) = s.peer_class_pool.at_mut(local) {
            c.ignore_unchoke_slots = true;
            c.connection_limit_factor = 150;
        }
        s.init_peer_class_filter(true);
        // TCP / SSL-TCP / I2P conns join the `tcp` class (id 1).
        s.peer_class_type_filter
            .add(bitdaemon_net::peer_class::FilterSocketType::Tcp, 1);
        s.peer_class_type_filter
            .add(bitdaemon_net::peer_class::FilterSocketType::SslTcp, 1);
        s.peer_class_type_filter
            .add(bitdaemon_net::peer_class::FilterSocketType::I2p, 1);
        // upstream `session_impl` starts with `session_log(
        // "start session")` → `log_alert` under session_log.
        s.alerts.post(Alert::Log {
            msg: "start session".into(),
        });
        s
    }

    /// `session_impl::init_peer_class_filter` — default class
    /// assignment table: everything → `global` (id 0), private /
    /// link-local / loop-back nets → `local` (id 2).
    /// `unlimited_local = false` keeps only the catch-all rules
    /// (used when local traffic isn't free — e.g. all listen
    /// sockets failed upstream re-inits with `false`).
    fn init_peer_class_filter(&mut self, unlimited_local: bool) {
        const GLOBAL: u32 = 1 << 0; // global_peer_class_id
        const LOCAL: u32 = 1 << 2; // local_peer_class_id
        // (first, last, mask) — verbatim from `init_peer_class_filter`.
        const V4: &[(&str, &str, u32)] = &[
            ("0.0.0.0", "255.255.255.255", GLOBAL),
            ("10.0.0.0", "10.255.255.255", LOCAL),
            ("172.16.0.0", "172.31.255.255", LOCAL),
            ("192.168.0.0", "192.168.255.255", LOCAL),
            ("169.254.0.0", "169.254.255.255", LOCAL),
            ("127.0.0.0", "127.255.255.255", LOCAL),
        ];
        const V6: &[(&str, &str, u32)] = &[
            ("::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", GLOBAL),
            ("fc00::", "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", LOCAL),
            ("fe80::", "febf::ffff:ffff:ffff:ffff:ffff:ffff:ffff", LOCAL),
            ("::1", "::1", LOCAL),
        ];
        let n = if unlimited_local { usize::MAX } else { 1 };
        for (first, last, mask) in V4
            .iter()
            .take(n.min(V4.len()))
            .chain(V6.iter().take(n.min(V6.len())))
        {
            if let (Ok(a), Ok(b)) = (
                first.parse::<std::net::IpAddr>(),
                last.parse::<std::net::IpAddr>(),
            ) {
                self.peer_class_filter.add_rule(a, b, *mask);
            }
        }
    }

    /// `peer_class_filter.access` ∩ `peer_class_type_filter.apply` —
    /// the raw class bitmask for an (address, socket-type) pair
    /// (`session_impl::set_peer_classes`, first half).
    #[must_use]
    fn peer_class_mask(
        &self,
        addr: std::net::IpAddr,
        st: bitdaemon_net::peer_class::FilterSocketType,
    ) -> u32 {
        self.peer_class_type_filter
            .apply(st, self.peer_class_filter.access(addr))
    }

    /// the `connection_limit_factor` roll-up: max factor across the
    /// classes named by `mask`; `0` (empty/unknown mask) → `100`
    /// (upstream default). Used by both the session conn cap and the
    /// per-torrent `connect_to_peer` limit.
    #[must_use]
    fn connection_limit_factor(&self, mask: u32) -> i32 {
        let mut factor = 0i32;
        let (mut m, mut i) = (mask, 0u32);
        while m != 0 {
            if m & 1 != 0
                && let Some(pc) = self.peer_class_pool.at(i)
            {
                factor = factor.max(pc.connection_limit_factor);
            }
            m >>= 1;
            i += 1;
        }
        if factor == 0 { 100 } else { factor }
    }

    /// `session_impl::set_peer_classes` — assigns `conn`'s
    /// `peer_class_set` from the ip/type filters (pool refcounts bump
    /// via `add_class`; upstream never releases conn-side refs — the
    /// builtin classes live for the session) and snapshots
    /// `ignore_unchoke_slots()` (`num_classes()==0` or any flagged
    /// member class; the torrent's own class can't flag it — its
    /// `ignore_unchoke_slots` is never set).
    fn set_peer_classes(
        &mut self,
        conn: &mut crate::peer_conn::PeerConn,
        addr: std::net::IpAddr,
        st: bitdaemon_net::peer_class::FilterSocketType,
    ) {
        let mask = self.peer_class_mask(addr, st);
        let mut set = bitdaemon_net::peer_class::PeerClassSet::default();
        let (mut m, mut i) = (mask, 0u32);
        while m != 0 {
            if m & 1 != 0 && self.peer_class_pool.at(i).is_some() {
                set.add_class(&mut self.peer_class_pool, i);
            }
            m >>= 1;
            i += 1;
        }
        conn.peer_classes = set;
    }

    /// `session_impl::num_connections` — live conns: attached peers
    /// plus half-open (dialing + handshaking) ones.
    #[must_use]
    pub fn num_connections(&self) -> i64 {
        let attached: i64 = self.torrents.values().map(|t| t.peers.len() as i64).sum();
        attached
            + self
                .counters
                .value(crate::counters::Counter::NumPeersHalfOpen)
    }

    /// `session_impl::update_peer_connections`'s `torrent_state`
    /// snapshot — settings + the `external_ip` aggregate (elected
    /// WAN address by family) + listen port.
    #[must_use]
    pub fn peer_list_state(&self) -> bitdaemon_net::peer_list::PeerListState {
        let s = &self.settings;
        let mut ext = bitdaemon_net::ip_voter::ExternalIp::default();
        if let Some(ip) = self.ip_voter.external_address() {
            ext = match ip {
                std::net::IpAddr::V4(v4) => bitdaemon_net::ip_voter::ExternalIp::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    std::net::IpAddr::V4(v4),
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                ),
                std::net::IpAddr::V6(v6) => bitdaemon_net::ip_voter::ExternalIp::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    std::net::IpAddr::V6(v6),
                ),
            };
        }
        bitdaemon_net::peer_list::PeerListState {
            is_finished: false,
            external_ip: ext,
            external_port: self.listen_port,
            min_reconnect_time: s.min_reconnect_time,
            max_peerlist_size: s.max_peerlist_size,
            max_failcount: s.max_failcount,
            allow_multiple_connections_per_ip: s.allow_multiple_connections_per_ip,
        }
    }

    /// `session_impl::prioritize_connections` — push a torrent to
    /// the front of the connect pump with 10 dial tickets.
    pub fn prioritize_connections(&mut self, ih: Sha1Hash) {
        self.prio_torrents.push_back((ih, 10));
    }

    /// `session_impl::inc_boost_connections` — one boost dial took
    /// a connect slot outside the per-tick budget; deducted next
    /// pump pass.
    pub fn inc_boost_connections(&mut self) {
        self.boost_connections += 1;
    }

    /// `session_impl::try_connect_more_peers` — hand out up to
    /// `connection_speed` dials this pass: boost deductions first,
    /// `smooth_connects` cap, then prioritized torrents ahead of the
    /// downloading/finished round-robin at the
    /// `connect_seed_every_n_download` ratio. `speed_override`
    /// replaces `connection_speed` (harness knob — upstream has no
    /// equivalent). Returns `(ih, endpoint)` dial requests — the
    /// wire crypto is decided per-dial by [`Self::peer_link_up`]
    /// once the stream exists (upstream `on_connected`).
    pub fn try_connect_more_peers(
        &mut self,
        session_time: i32,
        speed_override: Option<i32>,
    ) -> Vec<(Sha1Hash, SocketAddr, bitdaemon_common::PeerId)> {
        let mut dials = Vec::new();
        // `peer_fingerprint` — per-conn `generate_peer_id` seed for
        // the dial pid (`create_peer_connection`).
        let fp = self.settings.peer_fingerprint.clone();
        let connections_limit = i64::from(self.settings.connections_limit);
        let live = self.num_connections();
        if live >= connections_limit {
            return dials;
        }

        let mut max_connections = speed_override.unwrap_or(self.settings.connection_speed);
        if self.boost_connections > 0 {
            if self.boost_connections > max_connections {
                self.boost_connections -= max_connections;
                max_connections = 0;
            } else {
                max_connections -= self.boost_connections;
                self.boost_connections = 0;
            }
        }
        if max_connections <= 0 {
            return dials;
        }

        // `smooth_connects` — cap the batch at half the free slots
        // so attempts spread over ticks instead of bursting.
        let limit = (connections_limit - live).min(i64::from(i32::MAX) - 1);
        if self.settings.smooth_connects && i64::from(max_connections) > (limit + 1) / 2 {
            max_connections = ((limit + 1) / 2) as i32;
        }

        // upstream keeps intrusive `want_peers_*` lists; ours is a
        // per-tick partition of the torrents map (order stable
        // enough for the round-robin cursors).
        let st = self.peer_list_state();
        let mut want_download: Vec<Sha1Hash> = Vec::new();
        let mut want_finished: Vec<Sha1Hash> = Vec::new();
        for (ih, t) in &self.torrents {
            if t.want_peers_download(&self.settings) {
                want_download.push(*ih);
            } else if t.want_peers_finished(&self.settings) {
                want_finished.push(*ih);
            }
        }
        if want_download.is_empty() && want_finished.is_empty() {
            return dials;
        }

        let num_torrents = (want_download.len() + want_finished.len()) as i32;
        let mut steps_since_last_connect = 0i32;
        loop {
            if self.next_downloading_connect_torrent >= want_download.len() {
                self.next_downloading_connect_torrent = 0;
            }
            if self.next_finished_connect_torrent >= want_finished.len() {
                self.next_finished_connect_torrent = 0;
            }

            // prioritized torrents hand out first — verbatim
            // `--tickets; tickets > 0 && want_peers → use else pop`.
            let mut picked: Option<Sha1Hash> = None;
            while let Some((ih, tk)) = self.prio_torrents.front_mut() {
                let ih = *ih;
                *tk -= 1;
                if *tk > 0
                    && self
                        .torrents
                        .get(&ih)
                        .is_some_and(|t| t.want_peers(&self.settings))
                {
                    picked = Some(ih);
                    break;
                }
                self.prio_torrents.pop_front();
            }
            let ih = match picked {
                Some(ih) => ih,
                None => {
                    if (self.download_connect_attempts
                        >= self.settings.connect_seed_every_n_download
                        && !want_finished.is_empty())
                        || want_download.is_empty()
                    {
                        let ih = want_finished[self.next_finished_connect_torrent];
                        self.download_connect_attempts = 0;
                        self.next_finished_connect_torrent += 1;
                        ih
                    } else {
                        let ih = want_download[self.next_downloading_connect_torrent];
                        self.download_connect_attempts += 1;
                        self.next_downloading_connect_torrent += 1;
                        ih
                    }
                }
            };

            steps_since_last_connect += 1;
            if max_connections == 0 {
                break;
            }
            if want_download.is_empty() && want_finished.is_empty() {
                break;
            }
            if steps_since_last_connect > num_torrents + 1 {
                break;
            }
            // upstream re-reads `num_connections()` per hand-out —
            // the `peer_connection` ctor registers half-open dials.
            // Ours bumps `num_connecting` at `try_connect_peer`, so
            // the live count grows by each dial this pass returns.
            if live + dials.len() as i64 >= connections_limit {
                break;
            }

            // a torrent picked from the want lists may have been
            // drained by an earlier hand-out — upstream's intrusive
            // lists drop it via `update_want_peers`; ours is a
            // snapshot, so re-check live and drop when stale.
            let Some(t) = self.torrents.get_mut(&ih) else {
                want_download.retain(|k| *k != ih);
                want_finished.retain(|k| *k != ih);
                continue;
            };
            if !t.want_peers(&self.settings) {
                want_download.retain(|k| *k != ih);
                want_finished.retain(|k| *k != ih);
                if want_download.is_empty() && want_finished.is_empty() {
                    break;
                }
                continue;
            }
            match t.try_connect_peer(session_time, &st, &fp) {
                Some((ep, pid)) => {
                    self.counters
                        .inc(crate::counters::Counter::ConnectionAttempts);
                    dials.push((ih, ep, pid));
                    max_connections -= 1;
                    steps_since_last_connect = 0;
                }
                None => {
                    // `no_peer_connection_attempts` when the cache
                    // was empty, `missed_connection_attempts` when
                    // `connect_to_peer` failed — both collapse into
                    // the want-list drop + `update_want_peers`.
                    self.counters
                        .inc(crate::counters::Counter::NoPeerConnectionAttempts);
                    t.update_want_peers();
                    if !t.want_peers(&self.settings) {
                        want_download.retain(|k| *k != ih);
                        want_finished.retain(|k| *k != ih);
                        if want_download.is_empty() && want_finished.is_empty() {
                            break;
                        }
                    }
                }
            }
        }
        dials
    }

    /// `torrent::do_connect_boost` + the session bookkeeping around
    /// it — each returned dial counts as `inc_boost_connections`
    /// (deducted from the next pump's `connection_speed`), emitted
    /// as `ConnectPeer`; `want_peers` still true →
    /// `prioritize_connections`.
    fn do_connect_boost(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        let st = self.peer_list_state();
        let nconn = self.num_connections();
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        // upstream: `do_connect_boost` early-returns before the
        // `prioritize_connections` tail when the counter is spent.
        if t.connect_boost_counter == 0 {
            return;
        }
        let dials = t.do_connect_boost(
            self.session_time,
            &st,
            nconn,
            &self.settings,
            &self.settings.peer_fingerprint,
        );
        for _ in &dials {
            self.inc_boost_connections();
        }
        for (ep, pid) in dials {
            // `peer_connect_alert` — upstream posts it at dial for
            // outgoing conns (`peer_connection::start`).
            self.alerts.post(Alert::PeerConnect {
                ih: *ih,
                remote: ep.to_string(),
            });
            out.push(SessionOut::ConnectPeer {
                ih: *ih,
                ep,
                our_pid: pid,
            });
        }
        if self
            .torrents
            .get(ih)
            .is_some_and(|t| t.want_peers(&self.settings))
        {
            self.prioritize_connections(*ih);
        }
    }

    /// `find_disconnect_candidate_torrent` — the torrent to steal a
    /// conn slot from: must have peers, finished torrents first, then
    /// the largest swarm.
    fn find_disconnect_candidate_torrent(&self) -> Option<Sha1Hash> {
        self.torrents
            .iter()
            .filter(|(_, t)| !t.peers.is_empty())
            .max_by_key(|(_, t)| (t.is_finished(), t.peers.len()))
            .map(|(ih, _)| *ih)
    }

    /// `bind_outgoing_socket`'s bind endpoint for an outgoing conn —
    /// `None` when `outgoing_port == 0` (the OS picks the port, the
    /// common case). Otherwise the `m_next_port` round-robin over
    /// `[outgoing_port, outgoing_port + num_outgoing_ports]`; the
    /// family follows the remote (upstream `address_v6::any()` for a
    /// v6 target).
    pub fn outgoing_bind_addr(
        &mut self,
        remote: std::net::SocketAddr,
    ) -> Option<std::net::SocketAddr> {
        if self.settings.outgoing_port <= 0 {
            return None;
        }
        let start = self.settings.outgoing_port;
        let end = start.saturating_add(self.settings.num_outgoing_ports);
        if self.next_outgoing_port < start || self.next_outgoing_port > end {
            self.next_outgoing_port = start;
        }
        let port = self.next_outgoing_port;
        self.next_outgoing_port += 1;
        if self.next_outgoing_port > end {
            self.next_outgoing_port = start;
        }
        let ip = if remote.is_ipv6() {
            std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
        } else {
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        };
        Some(std::net::SocketAddr::new(ip, port as u16))
    }

    /// accept-side `too_many_files_open` handling — upstream posts
    /// `performance_alert::too_few_file_descriptors`, disconnects one
    /// conn of the largest torrent (plain `num_peers` max — the
    /// in-torrent ladder ranks which), and clamps
    /// `connections_limit` to the live count (floor 10).
    pub fn on_accept_fd_exhausted(&mut self, out: &mut Vec<SessionOut>) {
        if self.settings.connections_limit <= 10 {
            return;
        }
        self.alerts.post(Alert::Performance {
            warning: "too_few_file_descriptors".into(),
        });
        if let Some(t) = self.torrents.values_mut().max_by_key(|t| t.peers.len()) {
            let mut eff = Vec::new();
            t.disconnect_peers(1, "too_many_files_open", &mut eff);
            for e in eff {
                out.push(SessionOut::Torrent(e));
            }
        }
        self.settings.connections_limit =
            i32::try_from(self.num_connections()).unwrap_or(10).max(10);
    }

    /// the sender disk-job callbacks clone to report completions.
    #[must_use]
    pub fn disk_completions(&self) -> std::sync::mpsc::Sender<DiskCompleted> {
        self.disk_tx.clone()
    }

    /// `session_impl::set_listen_port` — keep `listen_port` and the
    /// primary `listen_socket_t` in lockstep (upstream derives the
    /// port from the sockets; we keep the flat field too).
    pub fn set_listen_port(&mut self, port: u16) {
        self.listen_port = port;
        if let Some(s) = self.listen_sockets.first_mut() {
            s.local_endpoint.set_port(port);
            s.original_port = port;
        }
    }

    /// `m_listen_sockets` handles — one per interface.
    #[must_use]
    pub fn listen_socket_handles(&self) -> Vec<crate::listen::ListenSocketHandle> {
        self.listen_sockets
            .iter()
            .map(crate::listen::ListenSocketHandle::new)
            .collect()
    }

    /// `on_disk_*` — drain completed disk jobs into swarm effects.
    /// Read → `Piece` to the peer. Write → `async_hash` the piece.
    /// Hash → `piece_verified` (passed = no error; hash comparison
    /// lands with `TorrentInfo` on `Torrent`).
    pub fn drain_disk_completed(&mut self, out: &mut Vec<SessionOut>) {
        // `disk_completed_queue::call_job_handlers` — upstream posts
        // this on the io_context on each 0→1 queue transition. Our
        // handlers push `DiskCompleted` into `disk_rx`, so run them
        // first or nothing below ever fires (writes/hashes wedged).
        if let Some(d) = &self.disk {
            let n = d.call_job_handlers();
            if n > 0 {
                tracing::debug!(n, "disk completions drained");
            }
            d.flush_expired();
        }
        let mut drained = 0usize;
        while let Ok(c) = self.disk_rx.try_recv() {
            drained += 1;
            match c {
                DiskCompleted::Read {
                    ih,
                    peer,
                    req,
                    buf,
                    err,
                    issue_time,
                } => {
                    // `on_disk_read_complete` — the in-flight read is
                    // over regardless of outcome (`m_reading_bytes`).
                    if let Some(t) = self.torrents.get_mut(&ih)
                        && let Some(c) = t.peers.get_mut(&peer)
                    {
                        c.reading_bytes =
                            c.reading_bytes.saturating_sub(req.length.max(0) as usize);
                    }
                    if err.is_err() {
                        // `peer_connection::on_disk_read_complete` —
                        // read failures are NOT torrent errors: the
                        // peer gets `dont_have` + `reject_request`,
                        // a `file_error_alert`, and its consecutive
                        // failure count bumped; >100 disconnects.
                        // Success resets the count (below).
                        // `write_dont_have` — BEP-10 `lt_donthave`
                        // frame under the peer's negotiated id;
                        // suppressed entirely when unadvertised.
                        // `write_reject_request` — BEP-6 only: a
                        // non-FAST peer must not see msg 0x10.
                        let (dh, fast) = self
                            .torrents
                            .get(&ih)
                            .and_then(|t| t.peers.get(&peer))
                            .map_or((0, false), |c| (c.ext_dont_have, c.supports_fast));
                        if dh != 0 {
                            self.push_to_peer(
                                &ih,
                                peer,
                                PeerMessage::DontHave {
                                    piece: req.piece,
                                    ext_id: dh,
                                },
                                out,
                            );
                        }
                        if fast {
                            self.push_to_peer(&ih, peer, PeerMessage::RejectRequest(req), out);
                        }
                        // `write_reject_request` counts the reject
                        // even when the peer can't receive it.
                        self.counters.inc(crate::counters::Counter::PieceRejects);
                        let path = self
                            .torrents
                            .get(&ih)
                            .map_or_else(String::new, |t| t.resolve_filename(err.file.0));
                        self.alerts.post(Alert::FileError {
                            ih,
                            path,
                            error: err.to_string(),
                        });
                        let failures = self
                            .torrents
                            .get_mut(&ih)
                            .and_then(|t| t.peers.get_mut(&peer))
                            .map(|c| {
                                c.disk_read_failures += 1;
                                c.disk_read_failures
                            })
                            .unwrap_or(0);
                        if failures > 100 {
                            out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                                peer,
                                reason: "repeated disk read failures",
                                // a storage failure is a local fault,
                                // not a peer protocol violation.
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                            }));
                        }
                        continue;
                    }
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        if let Some(c) = t.peers.get_mut(&peer) {
                            c.disk_read_failures = 0;
                        }
                        // `request_latency` — EWMA(5%) of the disk
                        // round-trip, upstream blends `disk_rtt` in
                        // µs on every successful read.
                        self.counters.blend(
                            crate::counters::Counter::RequestLatency,
                            issue_time.elapsed().as_micros().min(i64::MAX as u128) as i64,
                            5,
                        );
                        // `suggest_read_cache` — a block we just
                        // served is likely still in the read cache
                        // → a good suggestion candidate.
                        t.add_suggest_piece(req.piece, &self.settings);
                    }
                    // upstream `on_disk_read_complete` returns before
                    // `write_piece` when `m_disconnecting` — a conn in
                    // teardown gets neither the block nor the alert.
                    let conn_gone = self
                        .torrents
                        .get(&ih)
                        .and_then(|t| t.peers.get(&peer))
                        .is_none_or(|c| c.disconnecting);
                    if !conn_gone && let Some(data) = buf.data().map(|d| d.to_vec()) {
                        self.push_to_peer(
                            &ih,
                            peer,
                            PeerMessage::Piece {
                                piece: req.piece,
                                offset: req.start,
                                data: data.into(),
                            },
                            out,
                        );
                        // `block_uploaded_alert` — the block went
                        // out to the peer (upstream posts it in
                        // `peer_connection::on_disk_read_complete`)
                        // with the block *index*, `r.start /
                        // t->block_size()`, not the byte offset.
                        let (remote, block) = self
                            .torrents
                            .get(&ih)
                            .map(|t| {
                                let remote = t
                                    .peers
                                    .get(&peer)
                                    .and_then(|c| c.remote)
                                    .map_or_else(String::new, |ep| ep.to_string());
                                let blk = (req.start.max(0) / t.block_size().max(1)).max(0) as u32;
                                (remote, blk)
                            })
                            .unwrap_or_else(|| (String::new(), 0));
                        self.alerts.post(Alert::BlockUploaded {
                            ih,
                            remote,
                            piece: req.piece,
                            block,
                        });
                    }
                }
                DiskCompleted::Write {
                    ih,
                    peer,
                    req,
                    picker_gen,
                    err,
                } => {
                    // `on_disk_write_complete` — `queued_write_bytes`
                    // and `m_outstanding_writing_bytes` drain (the
                    // `bw_disk` read gate + `num_peers_down_disk`
                    // clear once they hit zero) unconditionally —
                    // success or error, the write is no longer in
                    // flight for this peer.
                    self.counters.add(
                        crate::counters::Counter::QueuedWriteBytes,
                        -i64::from(req.length),
                    );
                    if let Some(t) = self.torrents.get_mut(&ih)
                        && let Some(p) = t.peers.get_mut(&peer)
                    {
                        p.outstanding_writing_bytes =
                            (p.outstanding_writing_bytes - i64::from(req.length)).max(0);
                        if p.outstanding_writing_bytes == 0 {
                            if p.disk_blocked {
                                p.disk_blocked = false;
                                self.counters
                                    .add(crate::counters::Counter::NumPeersDownDisk, -1);
                            }
                            if let Some(gates) = &self.write_gates {
                                gates.clear_blocked((ih, peer));
                            }
                        }
                    }
                    // `picker_gen` gate — a `force_recheck` reset the
                    // picker after this write was issued; upstream
                    // drops the completion (success *and* failure)
                    // rather than touching freshly-reset state.
                    if self
                        .torrents
                        .get(&ih)
                        .is_none_or(|t| t.picker_generation != picker_gen)
                    {
                        continue;
                    }
                    if err.is_err() {
                        // `peer_connection::on_disk_write_complete`
                        // error path: aborted writes only un-mark the
                        // block; real failures cancel the block on
                        // every conn, mark `write_failed` (blocks the
                        // piece against re-requests), then a
                        // `clear_piece` fence runs `on_piece_fail_sync`
                        // (restore + re-drive) — then
                        // `handle_disk_error("write", e, peer, write)`.
                        let aborted = err.io.kind() == std::io::ErrorKind::Interrupted;
                        let mut eff = Vec::new();
                        let mut fenced = false;
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            let bs = t.block_size().max(1);
                            let block = bitdaemon_common::types::PieceBlock {
                                piece_index: req.piece,
                                block_index: req.start / bs,
                            };
                            if aborted {
                                if let Some(p) = t.picker.as_mut() {
                                    p.mark_as_canceled(block, None);
                                }
                            } else if t.picker.is_some() {
                                t.cancel_block(block, &mut eff);
                                if let Some(p) = t.picker.as_mut() {
                                    p.write_failed(block);
                                }
                                if let Some(disk) = self.disk.clone() {
                                    let storage = t.storage;
                                    let tx = self.disk_tx.clone();
                                    let piece = req.piece;
                                    fenced = true;
                                    disk.async_clear_piece(
                                        storage,
                                        piece,
                                        Box::new(move |piece| {
                                            let _ =
                                                tx.send(DiskCompleted::ClearPiece { ih, piece });
                                        }),
                                    );
                                }
                            }
                        }
                        self.route(ih, eff, out);
                        if !fenced && !aborted {
                            // no storage to fence on — run the restore
                            // inline (`!has_storage()` leg upstream).
                            let mut eff = Vec::new();
                            if let Some(t) = self.torrents.get_mut(&ih) {
                                t.on_piece_fail_sync(
                                    req.piece,
                                    &self.settings,
                                    &self.counters,
                                    &mut eff,
                                );
                            }
                            self.route(ih, eff, out);
                        }
                        // `t->update_gauge()` upstream — a failed
                        // write (aborted or real) still mutates the
                        // pieces gauge; subscribed torrents must
                        // observe it on the next `state_update_alert`.
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            t.state_updated();
                        }
                        self.on_disk_error(ih, &err, DiskJobClass::Write, Some(peer), out);
                        continue;
                    }
                    // `on_disk_write_complete` → mark_as_finished
                    // + `block_finished_alert` + `disconnect_if_
                    // redundant` (a seed↔seed conn drops here too).
                    let mut eff = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        // upstream `mark_as_finished(block,
                        // peer_info_struct())` — the completing
                        // conn's key attributes the downloader.
                        let key = t.peers.get(&peer).and_then(|c| c.peer_key());
                        t.mark_block_finished(req.piece, req.start, key);
                        // `on_disk_write_complete` tail —
                        // `check_graceful_pause`: under graceful pause
                        // a conn disconnects once its own outstanding
                        // bytes drain; `graceful_pause` stays set
                        // until resume (upstream `m_graceful_pause_
                        // mode` clears only via `set_paused`).
                        if t.graceful_pause
                            && t.peers
                                .get(&peer)
                                .is_some_and(|c| c.pipeline.outstanding_bytes <= 0)
                        {
                            eff.push(TorrentEffect::DisconnectPeer {
                                peer,
                                reason: "torrent_paused",
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                            });
                        }
                        t.disconnect_if_redundant(peer, &mut eff);
                        self.alerts.post(Alert::BlockFinished {
                            ih,
                            piece: req.piece,
                            block: (req.start / t.block_size()).max(0) as u32,
                        });
                    }
                    self.route(ih, eff, out);
                    // whole piece complete and no hash in flight?
                    // (`picker.started_hash_job` upstream)
                    // `piece_finished` → hash dispatch decision; the
                    // torrent borrow must end before
                    // `on_piece_verified` re-borrows the map.
                    let needs_hash = self.torrents.get(&ih).is_some_and(|t| {
                        !t.have_piece(req.piece)
                            && t.piece_complete(req.piece)
                            && !t.is_hashing(req.piece)
                    });
                    if needs_hash {
                        if self.settings.disable_hash_checks {
                            // upstream `verify_piece` short-circuit:
                            // `started_hash_job` + synchronous
                            // `on_piece_verified(passed=true)`, no
                            // disk job dispatched.
                            if let Some(t) = self.torrents.get_mut(&ih) {
                                t.hashing_started(req.piece);
                            }
                            self.on_piece_verified(&ih, req.piece, true, out);
                        } else if let (Some(disk), Some(t)) =
                            (self.disk.clone(), self.torrents.get_mut(&ih))
                        {
                            t.hashing_started(req.piece);
                            let tx = self.disk_tx.clone();
                            let storage = t.storage;
                            let v2 = t.hash_job_v2_span(req.piece);
                            let picker_gen = t.picker_generation;
                            // `verify_piece` flags: write_mode-derived
                            // flush/volatile + `v1_hash`.
                            let hflags = t.hash_job_flags(false, self.settings.disk_io_write_mode);
                            disk.async_hash(
                                storage,
                                req.piece,
                                v2,
                                hflags,
                                Box::new(move |piece, hash, block_hashes, err| {
                                    let _ = tx.send(DiskCompleted::Hash {
                                        ih,
                                        piece,
                                        hash,
                                        block_hashes,
                                        picker_gen,
                                        err,
                                    });
                                }),
                            );
                        }
                    }
                }
                DiskCompleted::PeerDrain { ih, peer } => {
                    // `peer_connection::on_disk` — the write queue
                    // drained below the low watermark; clear the
                    // `bw_disk` gate even while writes are still
                    // outstanding and let the pump resume reading
                    // (upstream re-runs `setup_receive` here). A
                    // not-blocked or gone conn is a no-op.
                    if let Some(t) = self.torrents.get_mut(&ih)
                        && let Some(p) = t.peers.get_mut(&peer)
                        && p.disk_blocked
                    {
                        p.disk_blocked = false;
                        self.counters
                            .add(crate::counters::Counter::NumPeersDownDisk, -1);
                        if let Some(gates) = &self.write_gates {
                            gates.clear_blocked((ih, peer));
                        }
                    }
                }
                DiskCompleted::MoveStorage {
                    ih,
                    path,
                    status,
                    err,
                } => {
                    // `on_storage_moved` — `m_moving_storage` clears
                    // first; `fatal_disk_error`/`file_exist` status
                    // bits mark failure.
                    use bitdaemon_disk::storage::disk_status;
                    let has_error = status & i32::from(disk_status::FATAL_DISK_ERROR) != 0
                        || status & i32::from(disk_status::FILE_EXIST) != 0;
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.moving_storage = false;
                    }
                    if !has_error {
                        let old = self
                            .torrents
                            .get_mut(&ih)
                            .map(|t| {
                                let old = std::mem::replace(
                                    &mut t.save_path,
                                    path.to_string_lossy().into_owned(),
                                );
                                // `need_save_resume(if_config_changed)`
                                t.need_save_resume |=
                                    bitdaemon_common::flags::ResumeDataFlags::IF_CONFIG_CHANGED;
                                old
                            })
                            .unwrap_or_default();
                        self.alerts.post(Alert::StorageMoved {
                            ih,
                            path: path.to_string_lossy().into_owned(),
                            old_path: old,
                        });
                        // `need_full_check` — `dont_replace` skipped
                        // existing files / `reset_save_path` adopted
                        // unverified data: upstream `force_recheck`es.
                        if status & i32::from(disk_status::NEED_FULL_CHECK) != 0 {
                            self.force_recheck(&ih, out);
                        }
                    } else {
                        self.alerts.post(Alert::StorageMovedFailed {
                            ih,
                            file: self
                                .torrents
                                .get(&ih)
                                .map(|t| t.resolve_filename(err.file.0))
                                .unwrap_or_default(),
                            op: err.operation.name().to_string(),
                            error: err.to_string(),
                        });
                    }
                }
                DiskCompleted::RenameFile {
                    ih,
                    index,
                    name,
                    err,
                } => {
                    // `file_renamed{,_failed}_alert`
                    if err.is_ok() {
                        // upstream `file_renamed_alert(handle,
                        // new_name, old_name, index)` — old resolves
                        // from `m_renamed_files`/layout *before* the
                        // rename lands; then `m_renamed_files` records
                        // the mapping and resume/state are dirtied in
                        // the completion.
                        let old_name = self
                            .torrents
                            .get_mut(&ih)
                            .map(|t| {
                                let old = t
                                    .renamed_files
                                    .get(&index.0)
                                    .cloned()
                                    .unwrap_or_else(|| t.resolve_filename(index.0));
                                t.renamed_files
                                    .insert(index.0, name.to_string_lossy().into_owned());
                                t.need_save_resume |=
                                    bitdaemon_common::flags::ResumeDataFlags::IF_CONFIG_CHANGED;
                                t.state_updated();
                                old
                            })
                            .unwrap_or_default();
                        self.alerts.post(Alert::FileRenamed {
                            ih,
                            index: index.0,
                            name: name.to_string_lossy().into_owned(),
                            old_name,
                        });
                    } else {
                        self.alerts.post(Alert::FileRenameFailed {
                            ih,
                            index: index.0,
                            error: err.to_string(),
                        });
                    }
                }
                DiskCompleted::ReadPiece {
                    ih,
                    piece,
                    data,
                    err,
                } => {
                    // `read_piece_alert` — payload or the error.
                    if err.is_ok() {
                        self.alerts.post(Alert::ReadPiece {
                            ih,
                            piece: piece.0,
                            buffer: data,
                        });
                    } else {
                        self.alerts.post(Alert::ReadPieceFailed {
                            ih,
                            piece: piece.0,
                            error: err.to_string(),
                        });
                        // upstream `on_read_piece` also runs the
                        // block through `handle_disk_error("read")`
                        // — the failure still sets torrent error
                        // state and pauses.
                        self.on_disk_error(ih, &err, DiskJobClass::None, None, out);
                    }
                }
                DiskCompleted::ReleaseFiles { ih } => {
                    // `cache_flushed_alert`
                    self.alerts.post(Alert::CacheFlushed(ih));
                }
                DiskCompleted::Stopped { ih } => {
                    // `on_torrent_paused` — the deferred
                    // `torrent_paused_alert`. A torrent removed or
                    // un-flagged between dispatch and completion
                    // drops it (upstream's shared_ptr guard).
                    if let Some(t) = self.torrents.get_mut(&ih)
                        && t.paused_alert_pending
                    {
                        t.paused_alert_pending = false;
                        self.alerts.post(Alert::TorrentPaused { ih });
                    }
                }
                DiskCompleted::DeleteFiles { ih, err } => {
                    // `torrent_deleted{,_failed}_alert`
                    if err.is_ok() {
                        self.alerts.post(Alert::TorrentDeleted(ih));
                    } else {
                        self.alerts.post(Alert::TorrentDeleteFailed {
                            ih,
                            error: err.to_string(),
                        });
                    }
                }
                DiskCompleted::SetFilePriority { ih, prios, err } => {
                    // `on_file_priority` — clears
                    // `outstanding_file_priority`, applies the
                    // storage-adjusted vector (piece priorities +
                    // `m_file_priority` + save-resume), posts
                    // `file_prio`/`file_error`, drains deferred
                    // updates.
                    let mut eff = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.on_file_priority(
                            prios,
                            Some(&err).filter(|e| e.is_err()),
                            &self.settings,
                            &self.counters,
                            &mut eff,
                        );
                    }
                    self.route(ih, eff, out);
                }
                DiskCompleted::CheckFiles {
                    ih,
                    accepted,
                    status,
                    err,
                } => {
                    // `torrent::files_checked`: resume accepted → the
                    // optimistic `have` bits stand; rejected →
                    // `need_full_check`: per-piece `disk_hash` jobs
                    // re-verify everything (the `TorrentChecked`
                    // alert fires when `checking_pieces` drains).
                    // `disk_status::oversized_file` →
                    // `oversized_file_alert` (upstream posts it in
                    // `on_check_files`).
                    if status & i32::from(bitdaemon_disk::storage::disk_status::OVERSIZED_FILE) != 0
                    {
                        self.alerts.post(Alert::OversizedFile {
                            ih,
                            file: String::new(),
                        });
                    }
                    let mut eff = Vec::new();
                    let ok = err.is_ok() && accepted;
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.on_files_checked(ok, &self.settings, &self.counters, &mut eff);
                        // upstream `files_checked` — an auto-managed
                        // torrent forces a queue recalculation.
                        if t.auto_managed {
                            self.auto_manage_scaler = 0;
                        }
                        if !ok && t.arm_full_recheck(&mut eff) > 0 {
                            for p in 0..t.num_pieces {
                                eff.push(TorrentEffect::DiskHash {
                                    piece: bitdaemon_common::types::PieceIndex(p),
                                });
                            }
                        }
                    }
                    if err.is_err() {
                        // `on_check_files`'s `fatal_disk_error` leg
                        // — `handle_disk_error("check_resume_data")`
                        // (set_error + pause) and the torrent drops
                        // out of auto-management so the queue does
                        // not restart it.
                        self.on_disk_error(ih, &err, DiskJobClass::None, None, out);
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            t.auto_managed = false;
                        }
                    }
                    // `fastresume_rejected_alert` — the resume
                    // verdict came back rejected (upstream posts it
                    // on every !accepted check; `op`/`file` from the
                    // failing job when known).
                    if err.is_ok() && !accepted {
                        self.alerts.post(Alert::FastresumeRejected {
                            ih,
                            error: "resume data rejected".into(),
                            op: "check_fastresume".into(),
                            file: String::new(),
                        });
                    }
                    self.route(ih, eff, out);
                    // `files_checked` tail — `start_announcing`
                    // (gated: a just-armed re-check early-returns).
                    self.start_announcing(&ih, out);
                }
                DiskCompleted::Hash {
                    ih,
                    piece,
                    hash,
                    block_hashes,
                    picker_gen,
                    err,
                } => {
                    // a `force_recheck`/`handle_inconsistent_hashes`
                    // reset the picker after dispatch — the piece may
                    // no longer correspond to any known job (upstream
                    // `picker_gen != m_picker_generation` → drop).
                    if self
                        .torrents
                        .get(&ih)
                        .is_none_or(|t| t.picker_generation != picker_gen)
                    {
                        continue;
                    }
                    // upstream `on_piece_verified`: v1 and v2 verdicts
                    // are independent tribools — `None` is
                    // `boost::indeterminate` (no data to decide).
                    let mut eff = Vec::new();
                    let (v1, v2) = {
                        let Some(t) = self.torrents.get_mut(&ih) else {
                            continue;
                        };
                        // `m_picker->completed_hash_job` runs after
                        // the generation gate, before the verdicts —
                        // `verify_v2_blocks` must see the hashing flag
                        // already cleared.
                        t.hashing_done(piece);
                        let v1 = if self.settings.disable_hash_checks {
                            Some(true)
                        } else if err.is_ok() && t.wants_v1_hash() {
                            t.expected_hash(piece).map(|want| want == hash)
                        } else if err.is_err() {
                            Some(false)
                        } else {
                            None
                        };
                        // upstream skips the v2 tree update when v1
                        // already failed (those hashes are likely bad);
                        // `block_hashes` is only populated for
                        // v2-capable torrents (`verify_piece` sizes it
                        // on `has_v2()`).
                        let v2 = if self.settings.disable_hash_checks {
                            Some(true)
                        } else if err.is_err() {
                            Some(false)
                        } else if v1 == Some(false) || block_hashes.is_empty() {
                            None
                        } else {
                            t.verify_v2_blocks(
                                piece,
                                &block_hashes,
                                &self.settings,
                                &self.counters,
                                &mut eff,
                            )
                        };
                        (v1, v2)
                    };
                    if err.is_err() {
                        // `on_piece_hashed` (the *checking* pass)
                        // treats absent/short files as skip-file, not
                        // a torrent error: upstream `eof`,
                        // `file_too_short`, `no_such_file_or_directory`,
                        // `partfile_missing_piece` and Windows'
                        // `ERROR_HANDLE_EOF` all fold into
                        // `NotFound`/`UnexpectedEof` here. A sparse
                        // file's never-written tail reads past the
                        // physical end — the pieces just stay
                        // not-have and the check continues (upstream
                        // also jumps `m_checking_piece` past the
                        // file; ours were all dispatched up front so
                        // they simply fail the same way). The fatal
                        // leg instead drops auto-management, sets the
                        // error and pauses (`auto_managed(false)` +
                        // `trigger_auto_manage` + `set_error`).
                        let checking = self
                            .torrents
                            .get(&ih)
                            .is_some_and(|t| t.checking && !t.checking_resume);
                        let eof_class = matches!(
                            err.io.kind(),
                            std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::NotFound
                        );
                        if !(checking && eof_class) {
                            // `piece_finished`'s `disk_error` leg —
                            // `handle_disk_error("piece_verified", e)`
                            // (class none → set_error + pause), then
                            // the verify path still runs below.
                            self.on_disk_error(ih, &err, DiskJobClass::None, None, out);
                            if checking {
                                if let Some(t) = self.torrents.get_mut(&ih) {
                                    t.auto_managed = false;
                                }
                                self.auto_manage_scaler = 0;
                            }
                        }
                    }
                    // `(v1 && !v2) || (!v1 && v2)` →
                    // `handle_inconsistent_hashes`; v1||v2 true →
                    // piece_passed; both indeterminate → keep the
                    // piece finished-but-unchecked
                    // (`mark_as_finished` on every block).
                    let inconsistent = matches!((v1, v2), (Some(a), Some(b)) if a != b);
                    if inconsistent {
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            t.handle_inconsistent_hashes(piece, &self.settings, &mut eff);
                        }
                    } else {
                        match v1.or(v2) {
                            Some(true) => self.on_piece_verified(&ih, piece, true, out),
                            Some(false) => self.on_piece_verified(&ih, piece, false, out),
                            None => {
                                if let Some(t) = self.torrents.get_mut(&ih) {
                                    t.mark_piece_finished_unchecked(piece);
                                }
                            }
                        }
                    }
                    self.route(ih, eff, out);
                }
                DiskCompleted::ClearPiece { ih, piece } => {
                    // fence drained → `on_piece_fail_sync`: restore
                    // the piece and re-drive newly-interesting peers.
                    let mut eff = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.on_piece_fail_sync(piece, &self.settings, &self.counters, &mut eff);
                    }
                    self.route(ih, eff, out);
                }
                DiskCompleted::PieceSync { ih, piece, blocks } => {
                    // hash-fail fence drained → `on_piece_sync`:
                    // restore the (blocks-scoped) piece, post
                    // `hash_failed_alert`, re-mark in-flight dupes.
                    let mut eff = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.on_piece_sync(piece, &blocks, &self.settings, &self.counters, &mut eff);
                    }
                    self.route(ih, eff, out);
                }
                DiskCompleted::SeedModeHash {
                    ih,
                    peer,
                    piece,
                    hash,
                    block_hashes,
                    err,
                } => {
                    // `peer_connection::on_seed_mode_hashed` — the
                    // per-peer outstanding counter always drops.
                    let mut eff = Vec::new();
                    let mut refill = false;
                    let mut disk_err = false;
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        if let Some(c) = t.peers.get_mut(&peer) {
                            c.outstanding_piece_verification -= 1;
                        }
                        if t.aborted {
                            continue;
                        }
                        if err.is_err() {
                            disk_err = true;
                        } else {
                            refill = t.seed_mode_hashed(
                                piece,
                                &hash,
                                &block_hashes,
                                &self.settings,
                                &mut eff,
                            );
                        }
                        if refill {
                            // requests blocked on this piece can now
                            // be serviced.
                            t.fill_send_buffer(peer, &self.settings, &self.counters, &mut eff);
                        }
                    }
                    if disk_err {
                        // `handle_disk_error("hash", e, peer)` — the
                        // peer arg lets OOM disconnect just this conn.
                        self.on_disk_error(ih, &err, DiskJobClass::None, Some(peer), out);
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            t.leave_seed_mode(true, &mut eff);
                        }
                    }
                    self.route(ih, eff, out);
                }
            }
        }
        if drained > 0 {
            tracing::debug!(drained, "disk_rx drained");
        }
    }

    /// translate `TorrentEffect::DiskRead/DiskWrite` into real
    /// jobs on the injected `DiskIo` (the `m_disk_thread` glue).
    fn dispatch_disk(&mut self, ih: Sha1Hash, effects: &[TorrentEffect]) {
        let Some(disk) = self.disk.clone() else {
            return;
        };
        for e in effects {
            match e {
                TorrentEffect::DiskRead { peer, req } => {
                    let Some(t) = self.torrents.get(&ih) else {
                        continue;
                    };
                    let storage = t.storage;
                    let (peer, req, tx) = (*peer, *req, self.disk_tx.clone());
                    // `on_disk_read_complete` blends the disk RTT
                    // into `request_latency` — the issue time rides
                    // with the job like upstream's `issue_time`.
                    let issue_time = std::time::Instant::now();
                    // upstream `peer_connection::on_piece_request`
                    // (peer_connection.cpp:5403): `volatile_read`
                    // under `disk_io_read_mode == disable_os_cache`.
                    disk.async_read(
                        storage,
                        req,
                        Box::new(move |buf, err| {
                            let _ = tx.send(DiskCompleted::Read {
                                ih,
                                peer,
                                req,
                                buf,
                                err,
                                issue_time,
                            });
                        }),
                        if self.settings.disk_io_read_mode
                            == crate::settings::DiskIoMode::DisableOsCache
                        {
                            bitdaemon_disk::disk_interface::flags::VOLATILE_READ
                        } else {
                            0
                        },
                    );
                }
                TorrentEffect::DiskWrite { peer, req, data } => {
                    let Some(t) = self.torrents.get(&ih) else {
                        continue;
                    };
                    let storage = t.storage;
                    let (peer, req) = (*peer, *req);
                    // `picker_gen` stamp — `on_disk_write_complete`
                    // drops completions issued before a
                    // `force_recheck` reset the picker.
                    let picker_gen = t.picker_generation;
                    let tx = self.disk_tx.clone();
                    // `async_write`'s `o` — upstream passes the conn
                    // itself (`self()`); the observer lives on the
                    // `PeerConn` so the back-pressure layer's `Weak`
                    // stays valid for the conn's lifetime, and its
                    // `on_disk` reports through `disk_tx` like a job
                    // completion.
                    let observer = self.torrents.get_mut(&ih).and_then(|t| {
                        t.peers.get_mut(&peer).map(|p| {
                            std::sync::Arc::clone(p.disk_observer.get_or_insert_with(|| {
                                std::sync::Arc::new(PeerDiskObserver {
                                    tx: tx.clone(),
                                    ih,
                                    peer,
                                })
                            }))
                        })
                    });
                    // `data` is `Bytes` (refcounted up to here) —
                    // this is the one copy, into the disk buffer.
                    let holder = DiskBufferHolder::from_owned(data.to_vec());
                    // `incoming_piece`'s `exceeded` — the disk queue
                    // was already over its limit when this block was
                    // handed off; unlike the global cache watermark,
                    // only THIS peer gets held back, and only until
                    // its own write (tracked below) lands.
                    let exceeded = disk.async_write(
                        storage,
                        req,
                        holder,
                        observer,
                        Box::new(move |err| {
                            let _ = tx.send(DiskCompleted::Write {
                                ih,
                                peer,
                                req,
                                picker_gen,
                                err,
                            });
                        }),
                        // upstream `torrent::add_piece`: writes get
                        // `flush_piece | volatile_read` only under
                        // `disk_io_write_mode == disable_os_cache`.
                        if self.settings.disk_io_write_mode
                            == crate::settings::DiskIoMode::DisableOsCache
                        {
                            bitdaemon_disk::disk_interface::flags::FLUSH_PIECE
                                | bitdaemon_disk::disk_interface::flags::VOLATILE_READ
                        } else {
                            0
                        },
                    );
                    // `incoming_piece` — `queued_write_bytes` grows
                    // by the payload; crossing
                    // `max_queued_disk_bytes` posts the
                    // `too_high_disk_queue_limit` performance alert
                    // once (edge-triggered).
                    let wq = {
                        self.counters.add(
                            crate::counters::Counter::QueuedWriteBytes,
                            i64::from(req.length),
                        );
                        self.counters
                            .value(crate::counters::Counter::QueuedWriteBytes)
                    };
                    let max_q = self.settings.max_queued_disk_bytes;
                    if wq > max_q && wq - i64::from(req.length) < max_q {
                        self.alerts.post(Alert::Performance {
                            warning: "too_high_disk_queue_limit".into(),
                        });
                    }
                    if let Some(t) = self.torrents.get_mut(&ih)
                        && let Some(p) = t.peers.get_mut(&peer)
                    {
                        // upstream gates + counts `bw_disk` only when
                        // an earlier write is still outstanding
                        // (`m_outstanding_writing_bytes > 0` before
                        // this block's bytes land).
                        if exceeded && p.outstanding_writing_bytes > 0 {
                            if !p.disk_blocked {
                                p.disk_blocked = true;
                                self.counters
                                    .inc(crate::counters::Counter::NumPeersDownDisk);
                            }
                            if let Some(gates) = &self.write_gates {
                                gates.set_blocked((ih, peer));
                            }
                        }
                        p.outstanding_writing_bytes += i64::from(req.length);
                    }
                }
                TorrentEffect::ReadPiece { piece } => {
                    // `torrent::read_piece` — whole piece buffer
                    let Some(t) = self.torrents.get(&ih) else {
                        continue;
                    };
                    let storage = t.storage;
                    let size = t
                        .files
                        .as_ref()
                        .map_or(t.piece_length, |f| i64::from(f.piece_size(*piece)));
                    let req = PeerRequest {
                        piece: *piece,
                        start: 0,
                        length: size as i32,
                    };
                    let piece = *piece;
                    let tx = self.disk_tx.clone();
                    // upstream `torrent::read_piece` (torrent.cpp:861):
                    // `volatile_read` under `disable_os_cache`.
                    disk.async_read(
                        storage,
                        req,
                        Box::new(move |buf, err| {
                            let _ = tx.send(DiskCompleted::ReadPiece {
                                ih,
                                piece,
                                data: buf.data().map_or_else(Vec::new, |d| d.to_vec()),
                                err,
                            });
                        }),
                        if self.settings.disk_io_read_mode
                            == crate::settings::DiskIoMode::DisableOsCache
                        {
                            bitdaemon_disk::disk_interface::flags::VOLATILE_READ
                        } else {
                            0
                        },
                    );
                }
                TorrentEffect::SeedModeVerify { peer, piece } => {
                    // `fill_send_buffer`'s seed-mode leg — lazy hash
                    // check before a promised piece can be served.
                    let (Some(disk), Some(t)) = (self.disk.clone(), self.torrents.get(&ih)) else {
                        continue;
                    };
                    let storage = t.storage;
                    let v2 = t.hash_job_v2_span(*piece);
                    // upstream flags: `v1_hash` when wanted;
                    // `volatile` when OS cache is off.
                    let hflags = t.hash_job_flags(false, self.settings.disk_io_write_mode);
                    let (piece, peer, tx) = (*piece, *peer, self.disk_tx.clone());
                    disk.async_hash(
                        storage,
                        piece,
                        v2,
                        hflags,
                        Box::new(move |piece, hash, block_hashes, err| {
                            let _ = tx.send(DiskCompleted::SeedModeHash {
                                ih,
                                peer,
                                piece,
                                hash,
                                block_hashes,
                                err,
                            });
                        }),
                    );
                }
                TorrentEffect::DiskHash { piece } => {
                    // `verify_piece` — resume restore or explicit
                    // re-check of a completed piece.
                    let Some(t) = self.torrents.get(&ih) else {
                        continue;
                    };
                    let storage = t.storage;
                    let v2 = t.hash_job_v2_span(*piece);
                    let picker_gen = t.picker_generation;
                    // `start_checking` flags: sequential + volatile +
                    // `v1_hash` — a full recheck shouldn't pollute the
                    // OS page cache (upstream `advise_dont_need`).
                    let hflags = t.hash_job_flags(true, self.settings.disk_io_write_mode);
                    let (piece, tx) = (*piece, self.disk_tx.clone());
                    disk.async_hash(
                        storage,
                        piece,
                        v2,
                        hflags,
                        Box::new(move |piece, hash, block_hashes, err| {
                            let _ = tx.send(DiskCompleted::Hash {
                                ih,
                                piece,
                                hash,
                                block_hashes,
                                picker_gen,
                                err,
                            });
                        }),
                    );
                }
                _ => {}
            }
        }
    }

    /// attach the disk backend (`session_params` wiring).
    pub fn set_disk(&mut self, io: Arc<dyn DiskIo>) {
        io.set_max_queued_bytes(self.settings.max_queued_disk_bytes);
        // `settings_updated` — upstream's hook also resizes
        // `m_file_pool` and both thread pools; push the same set so a
        // backend handed in cold picks up the live configuration.
        io.set_file_pool_size(self.settings.file_pool_size);
        io.set_max_threads(self.settings.aio_threads);
        io.set_hash_threads(self.settings.hashing_threads);
        io.set_write_no_cache(self.settings.disk_io_write_mode.bypasses_os_cache());
        io.set_write_through(
            self.settings.disk_io_write_mode == crate::settings::DiskIoMode::WriteThrough,
        );
        io.set_cache_expiry(self.settings.cache_expiry.as_secs());
        io.set_no_recheck_incomplete_resume(self.settings.no_recheck_incomplete_resume);
        io.set_open_mode_extra(open_mode_extra(&self.settings));
        io.set_mmap_file_size_cutoff(i64::from(self.settings.mmap_file_size_cutoff));
        io.set_disk_write_mode(self.settings.disk_write_mode as i32);
        io.set_no_cow(self.settings.disk_disable_copy_on_write);
        io.set_close_file_interval(
            i64::try_from(self.settings.close_file_interval.as_secs()).unwrap_or(i64::MAX),
        );
        self.disk = Some(io);
    }

    /// Share the per-peer `bw_disk` gate table with the engine — the
    /// same `Arc` must be handed to both sides (the engine's socket
    /// pumps poll it, this actor flips it from write submit/complete).
    pub fn set_write_gates(&mut self, gates: Arc<PeerWriteGates<(Sha1Hash, u64)>>) {
        self.write_gates = Some(gates);
    }

    /// `add_torrent` — insert (or replace) a swarm.
    pub fn add_torrent(&mut self, mut t: Torrent) -> Sha1Hash {
        let ih = t.info_hash;
        // a nonzero `peer_class` was minted against a different
        // pool — its index would silently alias a live session
        // class. Upstream torrents are always built inside the
        // session, so this path (harness-only `Torrent::new`)
        // must arrive classless; limits go through the session
        // pool after insert.
        debug_assert_eq!(t.peer_class, 0);
        t.queue_seq = self.next_queue_seq;
        self.next_queue_seq += 1;
        // session settings the torrent caches (`m_settings` upstream
        // is a live ref — ours is a synced copy).
        t.close_redundant_connections = self.settings.close_redundant_connections;
        // upstream `session_impl::add_torrent` hands each torrent the
        // live filters (`torrent_ptr->set_ip_filter(m_ip_filter)`);
        // `add_peer`/`connect_to_peer` consult them from then on.
        t.ip_filter = Some(std::sync::Arc::clone(&self.ip_filter));
        t.port_filter = Some(std::sync::Arc::clone(&self.port_filter));
        // upstream ctor arms `m_connect_boost_counter` from
        // `torrent_connect_boost`; `do_connect_boost` runs when the
        // params brought peers (`!p.peers.empty()` — ours: peers
        // already in the list, e.g. from resume data).
        t.connect_boost_counter =
            self.settings.torrent_connect_boost.clamp(0, u8::MAX as i32) as u8;
        let has_peers = t.peer_list.num_peers() > 0;
        // `torrent::init` → `start_announcing` — a peer-starved
        // torrent on a live DHT jumps the announce queue; `has_lsd`
        // also pushes a prioritized LSD announce.
        if !t.is_paused() && t.peer_list.num_peers() < 50 {
            self.dht_queue.push_back(ih);
        }
        if self.services.lsd && !t.is_paused() {
            self.lsd_queue.push_back(ih);
        }
        // `non_filter_torrents` — upstream incs the gauge at
        // torrent construction when `!m_apply_ip_filter`.
        if !t.apply_ip_filter {
            self.counters.inc(Counter::NonFilterTorrents);
        }
        self.torrents.insert(ih, t);
        if has_peers {
            let mut tmp = std::mem::take(&mut self.pending_out);
            self.do_connect_boost(&ih, &mut tmp);
            self.pending_out = tmp;
        }
        // `add_torrent_alert`
        self.alerts.post(Alert::TorrentAdded(ih));
        ih
    }

    /// `check_files` — upstream `torrent::start_checking`: run
    /// `async_check_files` on the injected `DiskIo`; the verdict
    /// arrives as `DiskCompleted::CheckFiles`.
    pub fn check_files(
        &mut self,
        ih: Sha1Hash,
        resume_data: Option<Box<bitdaemon_core::add_torrent_params::AddTorrentParams>>,
    ) {
        let Some(disk) = self.disk.clone() else {
            return;
        };
        // upstream `torrent::init` → `resolve_links`: mutable torrents
        // (`similar_torrents`/`collections`) reuse files already on
        // disk in seeding torrents — the links go into
        // `async_check_files` which hard-links them.
        let links = self.collect_links(&ih);
        let Some(t) = self.torrents.get_mut(&ih) else {
            return;
        };
        // `m_state = checking_resume_data` — the fastresume-verify
        // window; a `need_full_check` verdict re-arms as
        // `checking_files` via `arm_full_recheck`.
        t.checking = true;
        t.checking_resume = true;
        let storage = t.storage;
        let tx = self.disk_tx.clone();
        disk.async_check_files(
            storage,
            resume_data,
            links,
            Box::new(move |ret, err| {
                // `ret` is upstream's `status_t` (`disk_status`) —
                // the resume is accepted when neither
                // `need_full_check` nor `fatal_disk_error` is set.
                let rejected = ret
                    & (i32::from(bitdaemon_disk::storage::disk_status::NEED_FULL_CHECK)
                        | i32::from(bitdaemon_disk::storage::disk_status::FATAL_DISK_ERROR))
                    != 0;
                let _ = tx.send(DiskCompleted::CheckFiles {
                    ih,
                    accepted: !rejected,
                    status: ret,
                    err,
                });
            }),
        );
    }

    /// `torrent::init`'s `resolve_links` pass — match this torrent's
    /// files against seeding torrents listed in `similar_torrents` or
    /// sharing a `collections` tag. Returns per-file link targets
    /// (empty strings = no link).
    fn collect_links(&self, ih: &Sha1Hash) -> Vec<String> {
        let Some(t) = self.torrents.get(ih) else {
            return Vec::new();
        };
        let Some(ti) = t.ti.as_ref() else {
            return Vec::new();
        };
        if ti.similar_torrents().is_empty() && ti.collections().is_empty() {
            return Vec::new();
        }
        let mut res = bitdaemon_core::resolve_links::ResolveLinks::new(ti.clone());
        // `similar_torrents` — explicit info-hashes.
        for other_ih in ti.similar_torrents().to_vec() {
            self.match_link(&mut res, &other_ih, None);
        }
        // `collections` — every torrent tagged with the same label.
        for coll in ti.collections() {
            for (other_ih, other) in &self.torrents {
                if other_ih == ih {
                    continue;
                }
                let tagged = other
                    .ti
                    .as_ref()
                    .is_some_and(|o| o.collections().iter().any(|c| c == coll));
                if tagged {
                    self.match_link(&mut res, other_ih, Some(other));
                }
            }
        }
        res.get_links()
            .iter()
            .map(|l| String::from_utf8_lossy(l).into_owned())
            .collect()
    }

    /// one `resolve_links::match` — seeders only, like upstream.
    fn match_link(
        &self,
        res: &mut bitdaemon_core::resolve_links::ResolveLinks,
        other_ih: &Sha1Hash,
        torrent: Option<&crate::torrent::Torrent>,
    ) {
        let other = torrent.or_else(|| self.torrents.get(other_ih));
        let Some(other) = other else { return };
        // upstream only reuses files from torrents that are seeding.
        if !other.is_seed() {
            return;
        }
        let Some(oti) = other.ti.as_ref() else { return };
        let mut renamed = bitdaemon_core::file_storage::RenamedFiles::default();
        for (&idx, path) in &other.renamed_files {
            renamed.rename_file(
                oti.files(),
                bitdaemon_common::types::FileIndex(idx),
                path.as_bytes(),
            );
        }
        res.matches(oti, oti.files(), &renamed, other.save_path.as_bytes());
    }

    /// `torrent::init`'s storage creation — build a `StorageParams`
    /// from the add-torrent params and register it with the disk.
    /// Callers may pass `StorageIndex::INVALID` to `add_torrent_params`
    /// to defer this here (SessionHandle API, `.session` restores).
    fn create_storage_atp(
        &self,
        atp: &bitdaemon_core::add_torrent_params::AddTorrentParams,
    ) -> Option<StorageIndex> {
        let disk = self.disk.as_ref()?;
        let ti = atp.ti.as_ref()?;
        let mut renamed = RenamedFiles::default();
        for (&idx, path) in &atp.renamed_files {
            renamed.rename_file(ti.files(), FileIndex(idx), path);
        }
        // `info_hash` names the `.parts` file — use the swarm id
        // (v1, or truncated v2 for v2-only torrents).
        let info_hash = if atp.info_hashes.has_v1() {
            *atp.info_hashes.v1.as_bytes()
        } else {
            *atp.info_hashes
                .get(bitdaemon_common::info_hash::ProtocolVersion::V2)
                .as_bytes()
        };
        let params = StorageParams {
            files: ti.files(),
            renamed: &renamed,
            path: PathBuf::from(String::from_utf8_lossy(&atp.save_path).into_owned()),
            part_file_dir: PathBuf::from(String::from_utf8_lossy(&atp.part_file_dir).into_owned()),
            mode: match atp.storage_mode {
                bitdaemon_core::StorageMode::Allocate => {
                    bitdaemon_disk::storage::StorageMode::Allocate
                }
                bitdaemon_core::StorageMode::Sparse => bitdaemon_disk::storage::StorageMode::Sparse,
            },
            priorities: &atp.file_priorities,
            info_hash,
            v1: atp.info_hashes.has_v1(),
            v2: atp.info_hashes.has_v2(),
        };
        // `torrent` lifetime token — Torrent lives in `self.torrents`,
        // PreadDiskIo ignores it; a unit Arc satisfies the trait.
        Some(disk.new_torrent(&params, Arc::new(())))
    }

    /// Same as `create_storage_atp` but sourced from the `Torrent`
    /// itself — used when a magnet resolves (`MetadataReceived`).
    /// Returns the new index, `None` when storage already exists,
    /// metadata is still missing, or no disk is set.
    fn create_storage_torrent(&mut self, ih: &Sha1Hash) -> Option<StorageIndex> {
        let disk = self.disk.clone()?;
        let t = self.torrents.get(ih)?;
        if t.storage.is_valid() || t.ti.is_none() {
            return None;
        }
        let ti = t.ti.clone()?;
        let mut renamed = RenamedFiles::default();
        for (&idx, path) in &t.renamed_files {
            renamed.rename_file(ti.files(), FileIndex(idx), path.as_bytes());
        }
        let params = StorageParams {
            files: ti.files(),
            renamed: &renamed,
            path: PathBuf::from(&t.save_path),
            part_file_dir: PathBuf::from(&t.part_file_dir),
            mode: match t.storage_mode {
                bitdaemon_core::StorageMode::Allocate => {
                    bitdaemon_disk::storage::StorageMode::Allocate
                }
                bitdaemon_core::StorageMode::Sparse => bitdaemon_disk::storage::StorageMode::Sparse,
            },
            priorities: &t.file_priorities,
            info_hash: *t.info_hash.as_bytes(),
            v1: ti.v1(),
            v2: ti.v2(),
        };
        let idx = disk.new_torrent(&params, Arc::new(()));
        self.torrents.get_mut(ih)?.storage = idx;
        Some(idx)
    }

    /// `add_torrent` — the real entry point: `AddTorrentParams` →
    /// `Torrent::from_params`, resume effects are dispatched
    /// (disk verify jobs for restored unfinished pieces).
    pub fn add_torrent_params(
        &mut self,
        atp: &bitdaemon_core::add_torrent_params::AddTorrentParams,
        storage: StorageIndex,
        out: &mut Vec<SessionOut>,
    ) -> Option<Sha1Hash> {
        // `torrent::init` creates the storage from the atp — a caller
        // passing INVALID means "session, do it" (public handle API,
        // session restores). Unresolved magnets keep INVALID until
        // `MetadataReceived`.
        let storage = if storage == StorageIndex::INVALID {
            self.create_storage_atp(atp).unwrap_or(storage)
        } else {
            storage
        };
        let mut eff = Vec::new();
        let mut t = Torrent::from_params(
            atp,
            storage,
            &self.settings,
            &self.counters,
            &mut self.peer_class_pool,
            &mut eff,
        )?;
        let ih = t.info_hash;
        // `torrent_duplicate` — the info-hash is already in the
        // swarm table → `torrent_conflict_alert` upstream.
        if self.torrents.contains_key(&ih) {
            self.alerts.post(Alert::TorrentConflict {
                ih,
                other: None,
                url: atp
                    .trackers
                    .first()
                    .map(|u| String::from_utf8_lossy(u).into_owned())
                    .unwrap_or_default(),
            });
        }
        // `session_impl::add_torrent` — attach the live filters
        // (`torrent_ptr->set_ip_filter(m_ip_filter)`).
        t.ip_filter = Some(std::sync::Arc::clone(&self.ip_filter));
        t.port_filter = Some(std::sync::Arc::clone(&self.port_filter));
        // `non_filter_torrents` — incs at construction when
        // `!m_apply_ip_filter`.
        if !t.apply_ip_filter {
            self.counters.inc(Counter::NonFilterTorrents);
        }
        self.torrents.insert(ih, t);
        // upstream `trigger_auto_manage` — an add forces the next
        // tick into a queue pass (newly added default-flagged
        // torrents start paused+auto-managed).
        self.auto_manage_scaler = 0;
        // `add_torrent_alert` — upstream carries name + params +
        // error; our variant keeps the error slot ("" = ok).
        self.alerts.post(Alert::AddTorrent {
            ih,
            error: String::new(),
        });
        self.alerts.post(Alert::TorrentAdded(ih));
        self.route(ih, eff, out);
        // `.torrent` `nodes` — `dht->m_add_node` upstream: seed
        // router nodes through the async resolution path.
        if !self.settings.anonymous_mode {
            for (host, port) in &atp.dht_nodes {
                out.push(SessionOut::DhtAddNode {
                    host: String::from_utf8_lossy(host).into_owned(),
                    port: *port,
                });
            }
        }
        // upstream `torrent::init` → `start_checking`: every add
        // goes through `check_fastresume` — including seed_mode (the
        // pieces stay lazy-verified, but the storage check still runs
        // and `files_checked` must fire for conn-init/auto-manage
        // gating). A rejected verdict falls through to the per-piece
        // full recheck in `DiskCompleted::CheckFiles`.
        // `no_verify_files` skips the disk pass entirely — upstream
        // feeds `on_resume_data_checked({}, {})` (accepted, clean).
        use bitdaemon_common::flags::TorrentFlags as TF;
        if atp.ti.is_some() {
            if atp.flags.contains(TF::NO_VERIFY_FILES) {
                let _ = self.disk_tx.send(DiskCompleted::CheckFiles {
                    ih,
                    accepted: true,
                    status: 0,
                    err: StorageError::default(),
                });
            } else {
                self.check_files(ih, Some(Box::new(atp.clone())));
            }
        }
        Some(ih)
    }

    /// `remove_torrent` — `~torrent`: emit a `stopped` announce to
    /// every tracker that saw a `started` (upstream sends it on
    /// graceful shutdown/removal), then drop the swarm.
    pub fn remove_torrent(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) -> Option<Torrent> {
        let mut sent = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            t.trackers.stop_announcing(Instant::now());
            Self::announce_stopped_collect(
                t,
                self.peer_id,
                self.listen_port,
                &self.settings,
                out,
                &mut sent,
            );
        }
        self.post_tracker_announces(sent);
        self.lsd_queue.retain(|k| k != ih);
        let mut removed = self.torrents.remove(ih);
        if let Some(t) = &mut removed {
            // `~torrent` — `non_filter_torrents` drops back out.
            if !t.apply_ip_filter {
                self.counters.add(Counter::NonFilterTorrents, -1);
            }
            // `~torrent`/`abort` — release the torrent's peer class
            // (`remove_class` + `decref` drop both references).
            t.release_peer_class(&mut self.peer_class_pool);
            // upstream `~torrent` → `m_disk_thread.remove_torrent` —
            // drop the storage so queued jobs short-circuit behind
            // the fence.
            if let Some(disk) = &self.disk {
                disk.remove_torrent(t.storage);
            }
            self.alerts.post(Alert::TorrentRemoved(*ih));
        }
        removed
    }

    /// `pause`/`resume` (user pause — the auto-managed queue
    /// uses `queued` instead, like upstream). Pausing announces
    /// `stopped`; resuming re-arms `started` on the next tick.
    /// `torrent::set_paused(b, flags)` — user-layer pause/resume.
    /// `graceful` mirrors `pause_flags_t::graceful_pause`. The
    /// pause machinery only runs when the *effective* pause
    /// (`m_paused || m_session_paused`) actually flips.
    /// `mark_save` is the `pause()`/auto-manage distinction:
    /// upstream `torrent::pause()` marks `need_save_resume(
    /// if_state_changed)`, but `set_paused` itself does not — the
    /// queue demotion and session-pause paths call it unmarked.
    fn set_paused_mode(
        &mut self,
        ih: &Sha1Hash,
        paused: bool,
        graceful: bool,
        mark_save: bool,
        out: &mut Vec<SessionOut>,
    ) {
        enum Act {
            None,
            Pause,
            Resume,
        }
        let act = if let Some(t) = self.torrents.get_mut(ih) {
            // upstream `pause()`: `if (!m_paused)` → `set_need_save_resume`
            // (`set_paused` — the demotion path — does not mark).
            if mark_save && paused && !t.paused {
                t.need_save_resume |= bitdaemon_common::flags::ResumeDataFlags::IF_STATE_CHANGED;
            }
            // upstream strips graceful_pause when there are no peers
            // to drain — the paused alert posts immediately.
            let graceful = graceful && !t.peers.is_empty();
            if t.paused == paused {
                // graceful→hard: already flagged paused; dropping
                // the graceful bit makes the pause effective now.
                if t.paused && t.graceful_pause && !graceful {
                    t.graceful_pause = false;
                    Act::Pause
                } else {
                    Act::None
                }
            } else {
                let before = t.is_paused();
                t.paused = paused;
                if before == t.is_paused() {
                    Act::None
                } else {
                    t.graceful_pause = paused && graceful;
                    if paused { Act::Pause } else { Act::Resume }
                }
            }
        } else {
            Act::None
        };
        match act {
            Act::Pause => self.do_pause(ih, out),
            Act::Resume => self.do_resume(ih, out),
            Act::None => {}
        }
    }

    /// `torrent::pause` — `set_paused(true, 0)`; the API pause
    /// marks `need_save_resume(if_state_changed)` like upstream
    /// `torrent::pause()`.
    pub fn set_paused(&mut self, ih: &Sha1Hash, paused: bool, out: &mut Vec<SessionOut>) {
        self.set_paused_mode(ih, paused, false, true, out);
    }

    /// `torrent::pause(graceful_pause)` — stop issuing requests but
    /// let in-flight writes finish; idle peers disconnect, busy ones
    /// are choked, and the alert fires when the last write drains.
    pub fn set_paused_graceful(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        self.set_paused_mode(ih, true, true, true, out);
    }

    /// `torrent::resume` — clears `m_paused` unconditionally and
    /// re-arms the announce flags (distinct from `set_paused(false)`:
    /// it also lifts `graceful_pause` and runs `do_resume` even when
    /// the flag was already clear but announcing was off).
    pub fn resume_torrent(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        {
            let Some(t) = self.torrents.get_mut(ih) else {
                return;
            };
            if !t.paused && t.announce_dht && t.announce_trackers && t.announce_lsd {
                return;
            }
            t.paused = false;
            if !t.session_paused {
                t.graceful_pause = false;
            }
            t.need_save_resume |= bitdaemon_common::flags::ResumeDataFlags::IF_STATE_CHANGED;
        }
        self.do_resume(ih, out);
    }

    /// `torrent::do_pause` — runs when the effective pause flips on:
    /// extension veto, `announce_to_*` off, disconnect-all
    /// (`checking_files`/non-graceful) or the graceful drain
    /// (choke busy peers, disconnect idle ones), `stop_announcing`
    /// + the `stopped` event, `TorrentPaused` alert.
    fn do_pause(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        let now = Instant::now();
        let mut sent = Vec::new();
        let mut choke_ids = Vec::new();
        let mut post_alert = true;
        {
            let Some(t) = self.torrents.get_mut(ih) else {
                return;
            };
            // `torrent_plugin::on_pause` — a `true` reply vetoes the
            // pause machinery (the paused flag itself stays set).
            if t.extensions.iter().any(|e| e.on_pause()) {
                return;
            }
            t.announce_dht = false;
            t.announce_trackers = false;
            t.announce_lsd = false;
            // upstream `do_pause`: `m_inactive = false` + the
            // inactivity debounce is cancelled outright.
            t.inactive = false;
            t.pending_active_change = false;
            t.inactivity_deadline = None;
            t.state_updated();
            t.update_want_peers();
            let checking = t.checking && !t.checking_resume;
            if !t.graceful_pause || checking {
                // `disconnect_all(errors::torrent_paused)` — while
                // checking, upstream also skips `stop_announcing`
                // (the stopped announce went out when the check
                // started) and defers the alert to the last
                // checked piece.
                for (pid, c) in &mut t.peers {
                    if c.disconnecting {
                        continue;
                    }
                    c.disconnecting = true;
                    // `peer_connection::disconnect` sets
                    // `m_disconnecting` — `update_quotas` drops the
                    // conn's queued bandwidth requests next pass.
                    c.bw.disconnecting
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer: *pid,
                        reason: "torrent_paused",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                    }));
                }
                if checking {
                    post_alert = t.checking_pieces <= 0;
                    t.state_updated();
                    if post_alert {
                        self.alerts.post(Alert::TorrentPaused { ih: *ih });
                    }
                    return;
                }
            } else {
                // graceful: peers with data in flight get their
                // unsent requests cancelled (`clear_request_queue`)
                // and are choked; idle peers disconnect. Upstream
                // posts NO alert here — the last peer to drain runs
                // `do_pause` again (mode cleared) and posts it
                // (`GracefulPauseDone` below).
                post_alert = false;
                for (pid, c) in &mut t.peers {
                    if c.disconnecting {
                        continue;
                    }
                    if c.pipeline.outstanding_bytes > 0 {
                        choke_ids.push(*pid);
                    } else {
                        c.disconnecting = true;
                        c.bw.disconnecting
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                            peer: *pid,
                            reason: "torrent_paused",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                        }));
                    }
                }
            }
            // `stop_announcing` — re-arm slots for the `stopped`
            // event and drop in-flight requests (upstream
            // `abort_all_requests` on session pause).
            t.announcing = false;
            t.outstanding_announces = 0;
            t.trackers.stop_announcing(now);
            Self::announce_stopped_collect(
                t,
                self.peer_id,
                self.listen_port,
                &self.settings,
                out,
                &mut sent,
            );
        }
        for pid in choke_ids {
            let mut te = Vec::new();
            if let Some(t) = self.torrents.get_mut(ih) {
                // `clear_request_queue` — unsent requests are
                // aborted back into the picker (parole peers keep
                // theirs, upstream's `peer_info_struct` exemption).
                if let Some(c) = t.peers.get_mut(&pid) {
                    let aborted: Vec<_> = if c.on_parole {
                        Vec::new()
                    } else {
                        c.pipeline
                            .request_queue
                            .drain(..)
                            .map(|b| b.block)
                            .collect()
                    };
                    if let Some(p) = t.picker.as_mut() {
                        for b in aborted {
                            p.abort_download(b, Some(pid));
                        }
                    }
                }
                t.choke_peer(pid, &self.counters, &self.peer_class_pool, &mut te);
            }
            for e in te {
                out.push(SessionOut::Torrent(e));
            }
        }
        self.post_tracker_announces(sent);
        if post_alert {
            // upstream `on_torrent_paused` — with storage the alert
            // rides the `async_stop_torrent` completion (files
            // closed, cache flushed); `!m_storage` posts inline.
            let storage = self.torrents.get(ih).map(|t| t.storage);
            match (self.disk.clone(), storage) {
                (Some(disk), Some(storage)) if storage.is_valid() => {
                    if let Some(t) = self.torrents.get_mut(ih) {
                        t.paused_alert_pending = true;
                    }
                    let tx = self.disk_tx.clone();
                    let ih = *ih;
                    disk.async_stop_torrent(
                        storage,
                        Box::new(move || {
                            let _ = tx.send(DiskCompleted::Stopped { ih });
                        }),
                    );
                }
                _ => self.alerts.post(Alert::TorrentPaused { ih: *ih }),
            }
        }
    }

    /// `torrent::do_resume` — extension veto, `TorrentResumed`
    /// alert, announce flags back on, `clear_error`,
    /// `start_announcing` (`announcing`, trackers reset, immediate
    /// announce), `update_want_peers`.
    fn do_resume(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        {
            let Some(t) = self.torrents.get_mut(ih) else {
                return;
            };
            if t.is_paused() {
                // still paused via the other layer
                return;
            }
            if t.extensions.iter().any(|e| e.on_resume()) {
                return;
            }
            t.announce_dht = true;
            t.announce_trackers = true;
            t.announce_lsd = true;
            // upstream `unpause`: the connect boost re-arms so the
            // next tracker response dials a burst immediately.
            t.connect_boost_counter =
                self.settings.torrent_connect_boost.clamp(0, u8::MAX as i32) as u8;
            t.last_error.clear();
            t.state_updated();
            t.update_want_peers();
            // upstream `do_resume`: `checking_files && auto_managed`
            // re-arms the auto-manage pass (a resume can free a
            // queue slot the checker was holding).
            if t.checking && !t.checking_resume && t.auto_managed {
                self.auto_manage_scaler = 0;
            }
        }
        self.alerts.post(Alert::TorrentResumed { ih: *ih });
        // upstream `do_resume` — `checking_files` returns before
        // `start_announcing`/`do_connect_boost`; the check posts
        // its own announce once `files_checked` lands.
        if self
            .torrents
            .get(ih)
            .is_some_and(|t| t.checking && !t.checking_resume)
        {
            return;
        }
        // `start_announcing` — `m_announcing`, `m_trackers.reset()`
        // (fresh session → `started` again), the per-session stat
        // counters cleared, then announce now.
        self.start_announcing(ih, out);
        // `start_announcing` tail — `do_connect_boost` dials any
        // pre-known peers (resume data, params) without waiting a
        // full tick.
        if self.torrents.contains_key(ih) {
            self.do_connect_boost(ih, out);
        }
    }

    /// `torrent::start_announcing` — gates upstream applies, in
    /// order: `is_paused()`, the `checking_files` state,
    /// `!m_files_checked && valid_metadata()` (metadata is on disk
    /// but unchecked — announce after `files_checked`), and the
    /// `m_announcing` latch. Announcing arms the tracker slots
    /// (`m_trackers.reset()` → `started` again), clears the
    /// per-session stat counters (`m_total_failed_bytes`,
    /// `m_total_redundant_bytes`, `m_stat.clear()` — fresh session
    /// from the tracker's point of view) and fires an immediate
    /// announce, `high_priority` while the connect boost is armed.
    /// The tail reprioritizes DHT (peer-starved) and LSD.
    fn start_announcing(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        let now = Instant::now();
        let (pid, port) = (self.peer_id, self.listen_port);
        let mut sent = Vec::new();
        let (to_all_tiers, to_all_trackers) = (
            self.settings.announce_to_all_tiers,
            self.settings.announce_to_all_trackers,
        );
        let (num_want, report_true, apply_filter) = (
            self.settings.num_want,
            self.settings.report_true_downloaded,
            self.settings.apply_ip_filter_to_trackers,
        );
        let prioritize = self
            .torrents
            .get(ih)
            .is_some_and(|t| t.peer_list.num_peers() < 50)
            && self.dht.get_node(false).is_some();
        let announced = if let Some(t) = self.torrents.get_mut(ih) {
            if t.is_paused()
                || (t.checking && !t.checking_resume)
                || (!t.files_checked && t.ti.is_some())
                || t.announcing
            {
                false
            } else {
                t.announcing = true;
                // `m_trackers.reset()` + the stat reset.
                t.trackers.reset();
                t.failed_bytes = 0;
                t.redundant_bytes = 0;
                t.payload_uploaded = 0;
                t.payload_downloaded = 0;
                t.protocol_uploaded = 0;
                t.protocol_downloaded = 0;
                t.ip_overhead_uploaded = 0;
                t.ip_overhead_downloaded = 0;
                // `high_priority = m_connect_boost_counter > 0` —
                // queue-jumps the request at the tracker manager.
                let high_priority = t.connect_boost_counter > 0;
                let is_seed = t.is_seed();
                let eff_event = if t.finished && !t.seeding && !t.seed_mode {
                    4
                } else {
                    0
                };
                let supports = [t.info_hashes.has_v1(), t.info_hashes.has_v2()];
                let picks =
                    t.trackers
                        .due(now, is_seed, to_all_tiers, to_all_trackers, supports, false);
                for (idx, v) in picks {
                    Self::dispatch_announce(
                        t,
                        *ih,
                        idx,
                        v,
                        eff_event,
                        is_seed,
                        pid,
                        port,
                        apply_filter,
                        num_want,
                        report_true,
                        high_priority,
                        out,
                        &mut sent,
                    );
                }
                true
            }
        } else {
            false
        };
        if !announced {
            return;
        }
        if prioritize {
            self.prioritize_dht(*ih);
        }
        // `has_lsd()` → `prioritize_lsd`: the torrent jumps the LSD
        // broadcast queue.
        if self.services.lsd && self.torrents.contains_key(ih) {
            self.lsd_queue.push_back(*ih);
        }
        self.post_tracker_announces(sent);
    }

    /// `torrent::do_pause`/`~torrent` — `stopped` for trackers that
    /// already announced `started`. Upstream routes this through
    /// `announce_with_tracker(event_t::stopped)` — the regular
    /// tier/announce-state selection with
    /// `!a.start_sent && event == stopped → skip`, so stopped only
    /// reaches the slots that actually announced (per info-hash
    /// version). `sent` collects `(ih,url)` for the caller's
    /// `tracker_announce_alert` posts.
    fn announce_stopped_collect(
        t: &mut Torrent,
        peer_id: bitdaemon_common::PeerId,
        port: u16,
        settings: &crate::settings::SessionSettings,
        out: &mut Vec<SessionOut>,
        sent: &mut Vec<(Sha1Hash, String, i32)>,
    ) {
        let now = Instant::now();
        let supports = [t.info_hashes.has_v1(), t.info_hashes.has_v2()];
        let is_seed = t.is_seed();
        let picks = t.trackers.due(
            now,
            is_seed,
            settings.announce_to_all_tiers,
            settings.announce_to_all_trackers,
            supports,
            true,
        );
        for (idx, v) in picks {
            Self::dispatch_announce(
                t,
                t.info_hash,
                idx,
                v,
                3,
                is_seed,
                peer_id,
                port,
                settings.apply_ip_filter_to_trackers,
                settings.num_want,
                settings.report_true_downloaded,
                false,
                out,
                sent,
            );
        }
    }

    /// `tracker_announce_alert` — one post per `TrackerAnnounce`
    /// just queued (upstream fires it inside `announce_to_tracker`
    /// when the request goes on the wire; `event`: BEP-3 code).
    fn post_tracker_announces(&mut self, list: Vec<(Sha1Hash, String, i32)>) {
        for (ih, url, ev) in list {
            self.alerts.post(Alert::TrackerAnnounce {
                ih,
                url,
                event: ev as u8,
            });
        }
    }

    /// `session_impl` auto-managed queue — promote/demotion
    /// against `active_downloads`/`active_seeds`. A torrent
    /// `recalculate_auto_managed_torrents` — the queue scheduler.
    /// Builds upstream's three auto-managed lists (checking /
    /// downloading / seeding; `auto_managed && !has_error`), then:
    ///
    /// - `auto_manage_checking_torrents`: within `active_checking`,
    ///   `resume()`; beyond it, `pause()` (hard — not graceful).
    /// - `auto_manage_torrents` (downloaders then seeds, swapped by
    ///   `auto_manage_prefer_seeds`): inactive torrents stay active
    ///   without counting `type_limit` but still spend
    ///   dht/tracker/lsd/hard slots; within `type_limit && hard_limit`
    ///   → `set_paused(false)`; beyond →
    ///   `set_paused(true, graceful|clear_disk_cache)` + announce off.
    ///   Each active torrent consumes one `active_dht_limit`/
    ///   `active_tracker_limit`/`active_lsd_limit`/`active_limit`
    ///   slot — the announce flags go off once a channel exhausts,
    ///   even on torrents that remain unpaused.
    ///
    /// Ordering: checking/downloaders ascend `queue_seq`
    /// (`sequence_number`), seeds descend `seed_rank`. A `-1` limit
    /// means unlimited (`get_int_setting` → `INT_MAX` upstream).
    pub fn manage_queue(&mut self, out: &mut Vec<SessionOut>) {
        if self.paused {
            return;
        }
        let lim = |v: i32| -> i32 { if v < 0 { i32::MAX } else { v } };
        let mut checking_limit = lim(self.settings.active_checking);
        let downloading_limit = lim(self.settings.active_downloads);
        let seeding_limit = lim(self.settings.active_seeds);
        let mut dht_limit = lim(self.settings.active_dht_limit);
        let mut tracker_limit = lim(self.settings.active_tracker_limit);
        let mut lsd_limit = lim(self.settings.active_lsd_limit);
        let mut hard_limit = lim(self.settings.active_limit);
        let prefer_seeds = self.settings.auto_manage_prefer_seeds;
        let dont_count_slow = self.settings.dont_count_slow_torrents;
        let sort = hard_limit > 0;

        // list membership — upstream `update_state_list`:
        // `auto_managed && !has_error()`; `checking_files` state →
        // checking; `downloading*`/`finished`/`seeding` states →
        // seeds iff `is_finished()`. `checking_resume_data` (the
        // fastresume-verify window) belongs to no list.
        let mut checking: Vec<(i64, Sha1Hash)> = Vec::new();
        let mut downloaders: Vec<(i64, Sha1Hash)> = Vec::new();
        // seeds carry negated `seed_rank` so one ascending sort
        // gives upstream's descending-rank order.
        let mut seeds: Vec<(i64, Sha1Hash)> = Vec::new();
        let mut left_lists: Vec<Sha1Hash> = Vec::new();
        for (ih, t) in &self.torrents {
            if !t.auto_managed || !t.last_error.is_empty() {
                left_lists.push(*ih);
                continue;
            }
            match t.state() {
                crate::status::TorrentState::CheckingFiles => {
                    checking.push((t.queue_seq, *ih));
                }
                crate::status::TorrentState::CheckingResumeData => {
                    left_lists.push(*ih);
                }
                _ => {
                    if t.finished {
                        seeds.push((-i64::from(t.seed_rank(&self.settings)), *ih));
                    } else {
                        downloaders.push((t.queue_seq, *ih));
                    }
                }
            }
        }
        if sort {
            // upstream `partial_sort` — `sequence_number` asc for
            // checking+downloaders, `seed_rank` desc for seeds.
            checking.sort_unstable();
            downloaders.sort_unstable();
            seeds.sort_unstable();
        }

        // torrents that fell out of every list clear `queued` —
        // upstream recomputes status on `update_state_list` churn.
        for ih in left_lists {
            if let Some(t) = self.torrents.get_mut(&ih)
                && t.queued
            {
                t.queued = false;
                t.state_updated();
            }
        }

        // `auto_manage_checking_torrents` — `pause()` (hard) beyond
        // the limit; `resume()` + `start_checking` within. Our
        // `checking` flag already arms the in-flight pass, so
        // `should_check_files` only consumes the limit here.
        for (_, ih) in checking {
            if checking_limit <= 0 {
                self.set_paused_mode(&ih, true, false, false, out);
                continue;
            }
            self.set_paused_mode(&ih, false, false, false, out);
            let should_check = self.torrents.get(&ih).is_some_and(|t| {
                t.checking
                    && !t.checking_resume
                    && !t.is_paused()
                    && t.last_error.is_empty()
                    && !t.aborted
                    && !t.session_paused
            });
            if should_check {
                checking_limit -= 1;
            }
        }

        // `auto_manage_torrents` over one ordered list pair — the
        // shared dht/tracker/lsd/hard counters decrement across
        // *both* classes in visitation order.
        let classes: [&[(i64, Sha1Hash)]; 2] = if prefer_seeds {
            [&seeds[..], &downloaders[..]]
        } else {
            [&downloaders[..], &seeds[..]]
        };
        // the seed list is sorted on rank — normalize to the same
        // (key, ih) shape for a uniform loop.
        let ordered: Vec<Vec<Sha1Hash>> = classes
            .iter()
            .map(|c| c.iter().map(|(_, ih)| *ih).collect())
            .collect();
        for (class_idx, class) in ordered.iter().enumerate() {
            // prefer_seeds decides which type_limit a slot spends:
            // class 0 is "first visited", not a fixed kind.
            let seeds_class = (class_idx == 0) == prefer_seeds;
            let mut type_limit = if seeds_class {
                seeding_limit
            } else {
                downloading_limit
            };
            for ih in class {
                let inactive = self
                    .torrents
                    .get(ih)
                    .is_some_and(|t| dont_count_slow && t.inactive);
                if hard_limit > 0 && inactive {
                    // upstream: inactive torrents stay active but
                    // consume the channel/hard budgets.
                    dht_limit -= 1;
                    tracker_limit -= 1;
                    lsd_limit -= 1;
                    hard_limit -= 1;
                    self.set_announce_limits(ih, dht_limit, tracker_limit, lsd_limit);
                    self.set_paused_mode(ih, false, false, false, out);
                    continue;
                }
                if type_limit > 0 && hard_limit > 0 {
                    dht_limit -= 1;
                    tracker_limit -= 1;
                    lsd_limit -= 1;
                    hard_limit -= 1;
                    type_limit -= 1;
                    self.set_announce_limits(ih, dht_limit, tracker_limit, lsd_limit);
                    self.set_paused_mode(ih, false, false, false, out);
                    if let Some(t) = self.torrents.get_mut(ih) {
                        if t.queued {
                            t.queued = false;
                            t.state_updated();
                        }
                    }
                    continue;
                }
                // beyond the limits → graceful pause + all announce
                // flags off (upstream demotes with
                // `graceful_pause|clear_disk_cache`).
                if let Some(t) = self.torrents.get_mut(ih) {
                    if !t.queued {
                        t.queued = true;
                        t.state_updated();
                    }
                }
                self.set_paused_mode(ih, true, true, false, out);
                if let Some(t) = self.torrents.get_mut(ih) {
                    t.announce_dht = false;
                    t.announce_trackers = false;
                    t.announce_lsd = false;
                }
            }
        }
    }

    /// `set_announce_to_dht/trackers/lsd` — the per-channel budget
    /// verdicts from `auto_manage_torrents` (`--limit >= 0` each).
    fn set_announce_limits(
        &mut self,
        ih: &Sha1Hash,
        dht_limit: i32,
        tracker_limit: i32,
        lsd_limit: i32,
    ) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        t.announce_dht = dht_limit >= 0;
        t.announce_trackers = tracker_limit >= 0;
        t.announce_lsd = lsd_limit >= 0;
    }

    /// `queue_position` — upstream's `m_download_queue` index: one
    /// flat queue over all *non-finished* torrents ordered by
    /// `queue_seq` (`m_sequence_number`). Paused and non-auto-managed
    /// downloaders keep their slot; finished and aborted torrents
    /// report `-1` (`no_pos`).
    #[must_use]
    pub fn queue_position(&self, ih: &Sha1Hash) -> i32 {
        let Some(t) = self.torrents.get(ih) else {
            return -1;
        };
        if t.finished || t.aborted {
            return -1;
        }
        self.torrents
            .values()
            .filter(|o| !o.finished && !o.aborted)
            .map(|o| (o.queue_seq, o.info_hash))
            .collect::<std::collections::BTreeSet<_>>()
            .iter()
            .position(|(_, i)| i == ih)
            .map_or(-1, |p| p as i32)
    }

    /// `queue_position_up/down/top/bottom` — reorder inside the flat
    /// `m_download_queue` by rewriting `queue_seq` (upstream
    /// `queue_position_set` → `session_impl::set_queue_position`).
    /// Finished/aborted torrents reject moves upstream; a move also
    /// re-arms the auto-manage pass.
    pub fn queue_position_set(&mut self, ih: &Sha1Hash, p: QueueMove) {
        let Some(t) = self.torrents.get(ih) else {
            return;
        };
        if t.finished || t.aborted {
            return;
        }
        let me = t.queue_seq;
        // ordered seq list over the whole download queue
        let mut seqs: Vec<(i64, Sha1Hash)> = self
            .torrents
            .values()
            .filter(|o| o.info_hash != *ih && !o.finished && !o.aborted)
            .map(|o| (o.queue_seq, o.info_hash))
            .collect();
        seqs.sort_unstable();
        // Up/Down = swap with neighbor; Top/Bottom = leap-frog
        match p {
            QueueMove::Up | QueueMove::Down => {
                if let Some((s0, oih)) = match p {
                    QueueMove::Up => seqs.iter().rev().find(|(s0, _)| *s0 < me),
                    _ => seqs.iter().find(|(s0, _)| *s0 > me),
                } {
                    let (s0, oih) = (*s0, *oih);
                    if let Some(other) = self.torrents.get_mut(&oih) {
                        other.queue_seq = me;
                        other.state_updated();
                    }
                    if let Some(t) = self.torrents.get_mut(ih) {
                        t.queue_seq = s0;
                        t.state_updated();
                    }
                }
            }
            QueueMove::Top | QueueMove::Bottom => {
                let new_seq = match p {
                    QueueMove::Top => seqs.first().map_or(me, |(s0, _)| s0 - 1),
                    _ => seqs.last().map_or(me, |(s0, _)| s0 + 1),
                };
                if let Some(t) = self.torrents.get_mut(ih) {
                    t.queue_seq = new_seq;
                    t.state_updated();
                }
            }
        }
        self.auto_manage_scaler = 0;
    }

    /// feed one decoded peer message → effects.
    pub fn on_peer_message(
        &mut self,
        p: &PeerPacket,
        wire_bytes: usize,
        out: &mut Vec<SessionOut>,
    ) {
        // `counters::recv_bytes` — raw socket bytes (`peer_connection::
        // account_received_bytes`' accounting leg; IP/TCP overhead is
        // tracked separately via `trancieve_ip_packet`).
        if wire_bytes > 0 {
            self.counters
                .add(crate::counters::Counter::RecvBytes, wire_bytes as i64);
        }
        // `inc_stats_counter` per incoming message class
        match p.msg {
            PeerMessage::Bitfield(_) => self.counters.inc(Counter::NumIncomingBitfield),
            PeerMessage::Have(_) => self.counters.inc(Counter::NumIncomingHave),
            PeerMessage::Request(_) => self.counters.inc(Counter::NumIncomingRequest),
            PeerMessage::Piece { .. } => self.counters.inc(Counter::NumIncomingPiece),
            PeerMessage::Cancel(_) => self.counters.inc(Counter::NumIncomingCancel),
            PeerMessage::Choke => self.counters.inc(Counter::NumIncomingChoke),
            PeerMessage::Unchoke => self.counters.inc(Counter::NumIncomingUnchoke),
            PeerMessage::Interested => self.counters.inc(Counter::NumIncomingInterested),
            PeerMessage::NotInterested => self.counters.inc(Counter::NumIncomingNotInterested),
            _ => {}
        }
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(&p.info_hash) {
            // `incoming_request_alert`/`invalid_request_alert` are
            // posted by `Torrent`'s ReadRequest handler — upstream
            // only posts the former for *accepted* requests.
            t.on_peer_message(
                p.peer,
                &p.msg,
                wire_bytes,
                &self.settings,
                &self.counters,
                &mut eff,
            );
        }
        self.route(p.info_hash, eff, out);
    }

    /// `route` for call sites that don't own a `SessionOut` vec
    /// (handle APIs): effects land in `pending_out` and drain on the
    /// next tick.
    pub fn route_into_pending(&mut self, ih: Sha1Hash, eff: Vec<TorrentEffect>) {
        let mut tmp = std::mem::take(&mut self.pending_out);
        self.route(ih, eff, &mut tmp);
        self.pending_out = tmp;
    }

    /// a peer connected — register it on the torrent.
    ///
    /// Upstream order by direction:
    ///
    /// * **INCOMING** — `peer_connection::attach_to_torrent`
    ///   (`is_aborted` → `invalid_info_hash`@failure; auto-resume of
    ///   auto-managed queued torrents under
    ///   `incoming_starts_queued_torrents`; `paused||aborted||graceful`
    ///   → `torrent_paused`@`peer_error`), then `torrent::attach_peer`
    ///   (ssl gate → `m_has_incoming` → `ip_filter` →
    ///   `torrent_not_ready` → conn-cap ladder →
    ///   `peer_list::new_connection` legs → `maybe_replace`), then
    ///   `peer_connect_alert`, `protocol_v2`, the `m_exceeded_limit`
    ///   re-eval + global eviction.
    /// * **OUTGOING** — attached at `connect_to_peer` (the dial
    ///   mark); post-handshake it just binds the entry
    ///   (`peer_connected_outgoing`, restoring `prev_amount`) — a
    ///   stolen mark means the conn died upstream.
    ///
    /// Both then run the `read_peer_id` legs — pid dedup
    /// (`(pid < our_pid) == is_outgoing`, `duplicate_peer_id`),
    /// `is_self_connection` via `m_outgoing_pids` (kill + ban) — and
    /// `init()` (upload_only, bitfield, allowed-fast, unchoke).
    #[allow(clippy::too_many_lines)]
    pub fn peer_connected(
        &mut self,
        ih: &Sha1Hash,
        peer: u64,
        conn: crate::peer_conn::PeerConn,
        remote: SocketAddr,
        out: &mut Vec<SessionOut>,
    ) {
        if !self.torrents.contains_key(ih) {
            return;
        }
        let mut conn = conn;
        conn.remote = Some(remote);
        // `peer_connection` ctor → `session_impl::set_peer_classes`
        // — class membership is fixed for the conn's lifetime.
        let st = conn_socket_type(&conn);
        self.set_peer_classes(&mut conn, remote.ip(), st);
        let session_time = self.session_time;
        // `sent_syn`/`received_synack` IP overhead — outgoing TCP
        // conns carry the handshake overhead on their stat channels
        // and the session counters; accepts aren't charged upstream.
        if conn.outgoing && !conn.is_utp {
            let oh = i64::from(if remote.is_ipv6() { 60 } else { 40 });
            conn.stat_up_ip += oh; // sent_syn
            conn.stat_up_ip += oh; // received_synack's ACK
            conn.stat_down_ip += oh; // the SYN-ACK itself
            self.counters
                .add(crate::counters::Counter::SentIpOverheadBytes, oh * 2);
            self.counters
                .add(crate::counters::Counter::RecvIpOverheadBytes, oh);
        }
        let was_utp = matches!(conn.sock_type, 3 | 9);
        if conn.outgoing {
            // OUTGOING — `connect_to_peer` marked the entry at dial;
            // bind it. `prev_amount` folds into the conn's stats
            // (`add_stat(prev*1024)` in `create_peer_connection`).
            let t = self.torrents.get_mut(ih);
            let Some(t) = t else {
                return;
            };
            // the `pe_support` write-backs are `is_outgoing() &&
            // out_enc_policy == pe_enabled`-gated upstream.
            let out_enabled =
                self.settings.out_enc_policy == bitdaemon_net::pe_crypto::EncPolicy::Enabled;
            match t.peer_connected_outgoing(
                remote,
                crate::torrent::PeerAttach {
                    outgoing: true,
                    encrypted: conn.supports_encryption,
                    protocol_v2: conn.protocol_v2,
                    was_utp,
                    out_enabled,
                },
                session_time,
                peer,
                conn.fast_reconnect,
                conn.our_peer_id,
            ) {
                Some((_, dl, ul)) => {
                    conn.stat_down_total += dl;
                    conn.stat_up_total += ul;
                }
                None => {
                    // the dial mark was stolen by `new_connection`
                    // (the incoming side won the dup resolution) or
                    // already failed — upstream's conn object was
                    // destructed; ours dies here.
                    self.alerts.post(Alert::PeerDisconnected {
                        ih: *ih,
                        reason: format!("peer {peer}: duplicate_peer_id"),
                    });
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "duplicate_peer_id",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                    }));
                    // `num_connecting`/`num_seeds` bookkeeping — only
                    // when the dial mark is still open (an entry
                    // erased out from under the dial). A mark consumed
                    // by `new_connection`/`peer_connect_failed`
                    // already settled these counters.
                    t.dial_locals.remove(&remote);
                    if let Some(pid) = t.dial_pids.remove(&remote) {
                        t.outgoing_pids.remove(&pid);
                        t.num_connecting = t.num_connecting.saturating_sub(1);
                        if let Some(i) = t.peer_index(remote)
                            && t.peer_list.peers()[i].seed
                        {
                            t.num_connecting_seeds = t.num_connecting_seeds.saturating_sub(1);
                            t.num_seeds = t.num_seeds.saturating_sub(1);
                        }
                    }
                    return;
                }
            }
        } else {
            // INCOMING — `attach_to_torrent` session-level legs.
            let (aborted, auto_resume) = {
                let t = self.torrents.get_mut(ih);
                match t {
                    Some(t) => (
                        t.is_aborted(),
                        t.is_paused()
                            && t.auto_managed
                            && self.settings.incoming_starts_queued_torrents,
                    ),
                    None => return,
                }
            };
            if aborted {
                // upstream resets the torrent pointer on abort →
                // the lookup misses → `invalid_info_hash` at
                // **failure** (the DHT secret-id `ban_ip` leg is the
                // engine's unknown-hash path).
                self.alerts.post(Alert::PeerDisconnected {
                    ih: *ih,
                    reason: format!("peer {peer}: invalid_info_hash"),
                });
                out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                    peer,
                    reason: "invalid_info_hash",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Failure,
                }));
                return;
            }
            if auto_resume {
                self.resume_torrent(ih, out);
            }
            {
                let Some(t) = self.torrents.get_mut(ih) else {
                    return;
                };
                if t.is_paused() || t.is_aborted() || t.graceful_pause {
                    // `errors::torrent_paused` at **peer_error** —
                    // alert + `error_peers`; the severity feeds
                    // `m_failed` through `peer_disconnected`.
                    self.counters.inc(crate::counters::Counter::ErrorPeers);
                    self.alerts.post(Alert::PeerError {
                        ih: *ih,
                        remote: remote.to_string(),
                        op: "bittorrent".into(),
                        error: "torrent_paused".into(),
                    });
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "torrent_paused",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError,
                    }));
                    return;
                }
                // `attach_peer` — SSL torrents refuse non-SSL conns
                // (`requires_ssl_connection`; `invalid_ssl_cert`
                // when we hold no cert — same refusal path).
                if t.is_ssl_torrent() && !matches!(conn.sock_type, 6..=9) {
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "requires_ssl_connection",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                    }));
                    return;
                }
                // `m_has_incoming` — sticky flag.
                t.has_incoming = true;
                // `m_apply_ip_filter` gate → `peer_blocked_alert` +
                // `banned_by_ip_filter` at default severity.
                if t.apply_ip_filter
                    && t.ip_filter
                        .as_ref()
                        .is_some_and(|f| f.access(remote.ip()) == bitdaemon_net::ip_filter::BLOCKED)
                {
                    self.alerts.post(Alert::PeerBlocked {
                        ih: *ih,
                        remote: remote.to_string(),
                        reason: "ip_filter".into(),
                    });
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "banned_by_ip_filter",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                    }));
                    return;
                }
                // `!is_downloading_state(m_state) && valid_metadata`
                // — checking torrents refuse conns (`torrent_not_ready`).
                if t.checking && t.valid_metadata() {
                    self.alerts.post(Alert::PeerDisconnected {
                        ih: *ih,
                        reason: format!("peer {peer}: torrent_not_ready"),
                    });
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "torrent_not_ready",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                    }));
                    return;
                }
                // `has_connection`/`session_closing` are structural
                // non-bugs here (actor ownership; session teardown
                // kills the accepts wholesale).
                // conn-cap ladder — `limit = m_max_connections * 100 /
                // connection_limit_factor` (max factor across the
                // conn's classes; `0` → 100 upstream).
                let max_conn = if t.max_connections <= 0 {
                    0xff_ffff_i64
                } else {
                    i64::from(t.max_connections)
                };
                let mut factor = 0i32;
                conn.peer_classes.for_each(|pc| {
                    if let Some(p) = self.peer_class_pool.at(pc) {
                        factor = factor.max(p.connection_limit_factor);
                    }
                });
                let factor = i64::from(if factor == 0 { 100 } else { factor });
                let limit = max_conn * 100 / factor;
                let mut maybe_replace = false;
                if t.peers.len() as i64 >= limit {
                    // >10% still-connecting → the longest-waiting
                    // half-open conn yields its slot instead.
                    if i64::from(t.num_connecting) > max_conn / 10 {
                        let victim = t
                            .peers
                            .iter()
                            .filter(|(_, c)| c.connecting && !c.disconnecting)
                            .min_by_key(|(_, c)| c.connect_ms)
                            .map(|(id, _)| *id);
                        match victim {
                            Some(v) => {
                                if let Some(r) = t.peers.get(&v).and_then(|c| c.remote) {
                                    t.peer_disconnected_addr(r, self.session_time, false, false, v);
                                }
                                let mut eff = Vec::new();
                                t.remove_peer(v, &self.counters, &self.peer_class_pool, &mut eff);
                                debug_assert!(eff.is_empty());
                                self.alerts.post(Alert::PeerDisconnected {
                                    ih: *ih,
                                    reason: format!("peer {v}: too_many_connections"),
                                });
                                out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                                    peer: v,
                                    reason: "too_many_connections",
                                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                                }));
                                // `peer_disconnected_other` — the
                                // slack debt is settled.
                                conn.exceeded_limit = false;
                            }
                            None => {
                                self.alerts.post(Alert::PeerDisconnected {
                                    ih: *ih,
                                    reason: format!("peer {peer}: too_many_connections"),
                                });
                                out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                                    peer,
                                    reason: "too_many_connections",
                                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                                }));
                                return;
                            }
                        }
                    } else {
                        maybe_replace = true;
                    }
                }
                // `peer_list::new_connection` — banned / endpoint
                // self-conn / same-direction / cross-direction
                // port-rule legs; `coin` is the equal-port tiebreak.
                let coin = rand::random::<bool>();
                match t.peer_connected_incoming(remote, conn.local, coin) {
                    bitdaemon_net::peer_list::NewConnVerdict::Attach(i) => {
                        let (dl, ul) = t.peer_connected_attach(
                            i,
                            crate::torrent::PeerAttach {
                                outgoing: false,
                                encrypted: conn.supports_encryption,
                                protocol_v2: conn.protocol_v2,
                                was_utp,
                                out_enabled: false,
                            },
                            session_time,
                            peer,
                            conn.fast_reconnect,
                            false,
                        );
                        conn.stat_down_total += dl;
                        conn.stat_up_total += ul;
                    }
                    bitdaemon_net::peer_list::NewConnVerdict::Banned => {
                        self.alerts.post(Alert::PeerDisconnected {
                            ih: *ih,
                            reason: format!("peer {peer}: peer_banned"),
                        });
                        out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                            peer,
                            reason: "peer_banned",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                        }));
                        return;
                    }
                    bitdaemon_net::peer_list::NewConnVerdict::SelfConnect {
                        index,
                        other_key,
                        other_remote,
                    } => {
                        // `self_connection` at **failure** on both
                        // conns; `disconnect` bans the entry
                        // (`ec == self_connection` → `ban_peer`) —
                        // by index, not endpoint (`update_peer_port`
                        // may have rewritten the stored port).
                        t.peer_list.ban_peer(index);
                        if other_key != 0 {
                            t.peer_disconnected_addr(
                                other_remote,
                                session_time,
                                true,
                                false,
                                other_key,
                            );
                            let mut eff = Vec::new();
                            t.remove_peer(
                                other_key,
                                &self.counters,
                                &self.peer_class_pool,
                                &mut eff,
                            );
                            debug_assert!(eff.is_empty());
                            self.alerts.post(Alert::PeerDisconnected {
                                ih: *ih,
                                reason: format!("peer {other_key}: self_connection"),
                            });
                            out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                                peer: other_key,
                                reason: "self_connection",
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Failure,
                            }));
                        } else {
                            // pending outbound dial — clear its mark
                            // (failcount bump like upstream's conn
                            // teardown); its `DialDone` lands on the
                            // zombie path.
                            t.peer_disconnected_addr(other_remote, session_time, true, false, 0);
                        }
                        self.alerts.post(Alert::PeerDisconnected {
                            ih: *ih,
                            reason: format!("peer {peer}: self_connection"),
                        });
                        out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                            peer,
                            reason: "self_connection",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Failure,
                        }));
                        return;
                    }
                    bitdaemon_net::peer_list::NewConnVerdict::KillSelf => {
                        self.alerts.post(Alert::PeerDisconnected {
                            ih: *ih,
                            reason: format!("peer {peer}: duplicate_peer_id"),
                        });
                        out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                            peer,
                            reason: "duplicate_peer_id",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                        }));
                        return;
                    }
                    bitdaemon_net::peer_list::NewConnVerdict::KillOther {
                        index,
                        other_key,
                        other_remote,
                    } => {
                        // `m_locked_peer` — the synchronous teardown
                        // can't erase/shift the entry we rebind to.
                        t.peer_list.lock_peer(index);
                        if other_key != 0 {
                            t.peer_disconnected_addr(
                                other_remote,
                                session_time,
                                false,
                                false,
                                other_key,
                            );
                            let mut eff = Vec::new();
                            t.remove_peer(
                                other_key,
                                &self.counters,
                                &self.peer_class_pool,
                                &mut eff,
                            );
                            debug_assert!(eff.is_empty());
                            self.alerts.post(Alert::PeerDisconnected {
                                ih: *ih,
                                reason: format!("peer {other_key}: duplicate_peer_id"),
                            });
                            out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                                peer: other_key,
                                reason: "duplicate_peer_id",
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                            }));
                        } else {
                            // pending dial — clear the mark; its
                            // `DialDone` lands on the zombie path.
                            t.peer_disconnected_addr(other_remote, session_time, false, false, 0);
                        }
                        t.peer_list.unlock_peer();
                        let (dl, ul) = t.peer_connected_attach(
                            index,
                            crate::torrent::PeerAttach {
                                outgoing: false,
                                encrypted: conn.supports_encryption,
                                protocol_v2: conn.protocol_v2,
                                was_utp,
                                out_enabled: false,
                            },
                            session_time,
                            peer,
                            conn.fast_reconnect,
                            false,
                        );
                        conn.stat_down_total += dl;
                        conn.stat_up_total += ul;
                    }
                    bitdaemon_net::peer_list::NewConnVerdict::Full => {
                        self.alerts.post(Alert::PeerDisconnected {
                            ih: *ih,
                            reason: format!("peer {peer}: too_many_connections"),
                        });
                        out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                            peer,
                            reason: "too_many_connections",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                        }));
                        return;
                    }
                }
                // `maybe_replace_peer` — at-cap attaches evict the
                // lowest-ranking conn only if they outrank it;
                // otherwise the newcomer drops (its fresh swarm
                // entry still needs `connection_closed` bookkeeping).
                if maybe_replace {
                    let new_rank = t.rank_of_addr(remote);
                    let victim = t.lowest_ranking_peer();
                    match victim {
                        Some((v, rank)) if rank < new_rank => {
                            if let Some(r) = t.peers.get(&v).and_then(|c| c.remote) {
                                t.peer_disconnected_addr(r, self.session_time, false, false, v);
                            }
                            let mut eff = Vec::new();
                            t.remove_peer(v, &self.counters, &self.peer_class_pool, &mut eff);
                            debug_assert!(eff.is_empty());
                            self.alerts.post(Alert::PeerDisconnected {
                                ih: *ih,
                                reason: format!("peer {v}: too_many_connections"),
                            });
                            out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                                peer: v,
                                reason: "too_many_connections",
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                            }));
                            conn.exceeded_limit = false;
                        }
                        _ => {
                            t.peer_disconnected_addr(remote, self.session_time, false, false, peer);
                            self.alerts.post(Alert::PeerDisconnected {
                                ih: *ih,
                                reason: format!("peer {peer}: too_many_connections"),
                            });
                            out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                                peer,
                                reason: "too_many_connections",
                                sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                            }));
                            return;
                        }
                    }
                }
            }
            // `peer_connect_alert` — post-attach for incoming
            // conns (outgoing conns posted it at dial).
            self.alerts.post(Alert::PeerConnect {
                ih: *ih,
                remote: remote.to_string(),
            });
            // `peer_info::protocol_v2` — resolved at handshake parse
            // (skey version / reserved bit / v2 echo); the engine
            // already wrote it on the conn.
            // `m_exceeded_limit` re-eval — the session may have
            // dipped back under the cap between accept and attach.
            if conn.exceeded_limit
                && self
                    .counters
                    .value(crate::counters::Counter::NumPeersConnected)
                    + self
                        .counters
                        .value(crate::counters::Counter::NumPeersHalfOpen)
                    <= i64::from(self.settings.connections_limit)
            {
                conn.exceeded_limit = false;
            }
            if conn.exceeded_limit {
                // `find_disconnect_candidate_torrent` — prefer a
                // torrent with peers, finished first, largest swarm;
                // it must hold strictly more conns than ours, else
                // we drop ourselves.
                let cand = self.find_disconnect_candidate_torrent();
                let this_n = self.torrents.get(ih).map_or(0, |t| t.peers.len());
                let cand_n = cand
                    .and_then(|cih| self.torrents.get(&cih))
                    .map_or(0, |t| t.peers.len());
                let victim = if let Some(cih) = cand
                    && cand_n > this_n
                {
                    self.torrents[&cih].lowest_ranking_peer().map(|v| (cih, v))
                } else {
                    None
                };
                match victim {
                    Some((cih, (v, _rank))) if self.torrents.contains_key(&cih) => {
                        // evict the candidate's lowest-ranking conn —
                        // the `DisconnectPeer` effect runs the full
                        // `peer_connection::disconnect` path through
                        // `peer_disconnected`.
                        out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                            peer: v,
                            reason: "too_many_connections",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                        }));
                        conn.exceeded_limit = false;
                    }
                    _ => {
                        self.alerts.post(Alert::PeerDisconnected {
                            ih: *ih,
                            reason: format!("peer {peer}: too_many_connections"),
                        });
                        out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                            peer,
                            reason: "too_many_connections",
                            sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                        }));
                        return;
                    }
                }
            }
        }
        // `read_peer_id` legs — both directions.
        if let Some(rpid) = conn.peer_id {
            let t = self.torrents.get_mut(ih);
            let Some(t) = t else { return };
            // `find_peer` + `allow_multiple_connections_per_pid` —
            // a second conn carrying a peer-id we already serve is
            // a duplex loopback: the conn initiated by the greater
            // pid wins (`errors::duplicate_peer_id`). `our` is THIS
            // conn's generated pid (`m_our_peer_id`).
            let our = conn.our_peer_id.unwrap_or(self.peer_id);
            if !self.settings.allow_multiple_connections_per_pid
                && let Some(other) = t
                    .peers
                    .iter()
                    .find_map(|(id, c)| (c.peer_id == Some(rpid)).then_some(*id))
            {
                if (rpid < our) == conn.outgoing {
                    // kill the existing conn — this one survives.
                    // `duplicate_peer_id` at default severity — no
                    // failcount bump.
                    if let Some(r) = t.peers.get(&other).and_then(|c| c.remote) {
                        t.peer_disconnected_addr(r, self.session_time, false, false, other);
                    }
                    let mut eff = Vec::new();
                    t.remove_peer(other, &self.counters, &self.peer_class_pool, &mut eff);
                    debug_assert!(eff.is_empty());
                    self.alerts.post(Alert::PeerDisconnected {
                        ih: *ih,
                        reason: format!("peer {other}: duplicate_peer_id"),
                    });
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer: other,
                        reason: "duplicate_peer_id",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                    }));
                } else {
                    // kill this conn — its attach already bound the
                    // swarm entry, so `connection_closed` runs to
                    // clear the mark.
                    t.peer_disconnected_addr(remote, self.session_time, false, false, peer);
                    self.alerts.post(Alert::PeerDisconnected {
                        ih: *ih,
                        reason: format!("peer {peer}: duplicate_peer_id"),
                    });
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "duplicate_peer_id",
                        sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                    }));
                    return;
                }
            }
            // BitComet (`-BC…-`) pids cap the request queue at 50
            // (upstream `on_receive` peer_id).
            {
                let b = rpid.as_bytes();
                if b[0] == b'-'
                    && b[1] == b'B'
                    && b[2] == b'C'
                    && b[7] == b'-'
                    && conn.pipeline.max_out_request_queue > 50
                {
                    conn.pipeline.max_out_request_queue = 50;
                }
            }
            // `is_self_connection` — the remote claims a peer-id we
            // generated for an OUTGOING conn (`m_outgoing_pids`);
            // mark-less manual dials still shake hands with the
            // session pid, so a literal echo counts too.
            // `disconnect` → `ban_peer(m_peer_info)` upstream: the
            // entry stops being a candidate and rejects re-attach.
            if t.outgoing_pids.contains(&rpid) || rpid == self.peer_id {
                t.ban_peer(remote);
                t.peer_disconnected_addr(remote, self.session_time, false, false, peer);
                self.alerts.post(Alert::PeerDisconnected {
                    ih: *ih,
                    reason: format!("peer {peer}: self_connection"),
                });
                out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                    peer,
                    reason: "self_connection",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                }));
                return;
            }
        }
        // `init()` — handshake sends + attach tail.
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        // `write_handshake` — `send_upload_only(is_upload_only &&
        // !super_seeding)` precedes the bitfield (skipped under
        // `share_mode`); the conn-side ext id is only known once
        // the peer's own BEP-10 handshake arrived, so this is a
        // no-op for connections still mid-handshake.
        if !t.share_mode && !t.super_seeding && t.is_upload_only() && conn.ext_upload_only != 0 {
            let msg = PeerMessage::Extended {
                id: conn.ext_upload_only,
                data: bytes::Bytes::copy_from_slice(&[1]),
            };
            conn.send_buffer += bitdaemon_net::peer_wire::wire_len(&msg);
            out.push(SessionOut::ToPeer { ih: *ih, peer, msg });
        }
        // upstream `write_bitfield` on connect — super-seeding
        // hides the real bitfield (`have_none` under FAST) and
        // bootstraps by leaking *two* pieces via `have`.
        let msgs: Vec<PeerMessage> = if t.super_seeding {
            let mut v = Vec::new();
            if conn.supports_fast {
                v.push(PeerMessage::HaveNone);
            }
            for _ in 0..2 {
                // the conn isn't in `t.peers` yet — its own
                // leaked slots still bias the second pick
                // (`get_piece_to_super_seed_ex`).
                if let Some(p) = t.get_piece_to_super_seed_ex(&conn.peer_pieces, Some(&conn)) {
                    conn.superseed_pieces[1] = conn.superseed_pieces[0];
                    conn.superseed_pieces[0] = Some(p);
                    v.push(PeerMessage::Have(p));
                }
            }
            v
        } else if conn.supports_fast && t.is_seed() {
            // BEP-6: `have_all` replaces a full bitfield.
            vec![PeerMessage::HaveAll]
        } else if conn.supports_fast && t.have.count() == 0 {
            vec![PeerMessage::HaveNone]
        } else {
            let bits: Vec<u8> = t.have.bytes().into();
            if bits.is_empty() || t.have.count() == 0 {
                Vec::new()
            } else {
                vec![PeerMessage::Bitfield(bits.into())]
            }
        };
        for msg in msgs {
            // the conn isn't registered yet — charge its
            // send_buffer directly (fill_send_buffer's watermark
            // counts it).
            conn.send_buffer += bitdaemon_net::peer_wire::wire_len(&msg);
            out.push(SessionOut::ToPeer { ih: *ih, peer, msg });
        }
        // `write_bitfield` sets `m_sent_bitfield` in every branch
        // — including the have-none/super-seed legs that send no
        // bitfield bytes — whenever it ran (i.e. `valid_metadata`).
        // It gates the `DhtPort` reply leg of `on_dht_port`.
        conn.sent_bitfield = t.valid_metadata();
        conn.local_dht_port = self.dht_port;
        // `bt_peer_connection` ctor — the local plugin ids are
        // `find_plugin`-assigned: a missing plugin leaves its
        // index at 0 so `on_extended`'s fixed-id dispatch can't
        // be claimed by a peer that guesses the numeric id.
        // `ut_metadata`: absent only for a private torrent that
        // already has metadata — a private *magnet* still needs
        // the plugin to fetch it (`valid_metadata && priv`).
        // `ut_pex`: `!priv` and never on a pure-i2p torrent.
        // `i2p_pex`: torrent `!priv && i2p`, and upstream only
        // attaches the peer plugin on i2p sockets (the SAM
        // transport isn't implemented yet — `conn.i2p` is never
        // set in production).
        let is_priv = t.ti.as_ref().is_some_and(|ti| ti.is_private());
        let torrent_i2p = t.ti.as_ref().is_some_and(|ti| ti.is_i2p());
        conn.local_ut_metadata = u8::from(!(is_priv && t.valid_metadata())) * 2;
        conn.local_ut_pex = u8::from(!is_priv && !torrent_i2p);
        conn.local_i2p_pex =
            u8::from((!is_priv && torrent_i2p) || conn.i2p.is_some()) * crate::i2p_pex::I2P_PEX_ID;
        // `set_holepunch_mode` — upstream flags the conn right
        // after `connect_to_peer`; our dial is async so the
        // swarm entry carried the flag to this attach.
        if let Some(i) = t.peer_index(remote) {
            if let Some(p) = t.peer_list.peers_mut().get_mut(i)
                && std::mem::take(&mut p.holepunch)
            {
                conn.holepunch_mode = true;
            }
        }
        // upstream `peer_connection::init` sends `port` when
        // the peer negotiated DHT and our dht is bound.
        if conn.supports_dht
            && let Some(port) = self.dht_port
        {
            let msg = PeerMessage::DhtPort(port);
            conn.send_buffer += bitdaemon_net::peer_wire::wire_len(&msg);
            out.push(SessionOut::ToPeer { ih: *ih, peer, msg });
        }
        self.alerts.post(Alert::PeerLog {
            ih: *ih,
            remote: remote.to_string(),
            msg: "peer connected".into(),
        });
        t.add_peer(peer, conn);
        {
            let mut eff = Vec::new();
            // upstream `attach_peer` tail — a new swarm member
            // changes the share-mode rebalance inputs.
            if t.share_mode {
                t.recalc_share_mode(&self.settings, &self.counters, &mut eff);
            }
            // upstream does NOT `send_allowed_set` at attach —
            // it fires once on the first inbound `Interested`
            // (`m_sent_allowed_fast` in `on_interested`).
            // `write_handshake` tail — `maybe_send_hash_request`
            // on a v2 conn starts fetching missing merkle layers
            // immediately.
            t.maybe_send_hash_request(peer, &mut eff);
            // `maybe_unchoke_this_peer` — exempt conns unchoke
            // on the spot; normal ones do when the fixed-slots
            // choker has free slots (`preemptive_unchoke`). Upstream
            // gates the handshake leg on `num_have > 0`
            // (`bt_peer_connection.cpp` — the `incoming_interested`
            // re-run carries no such gate).
            if t.have.count() > 0 {
                t.maybe_unchoke_this_peer(
                    peer,
                    &self.settings,
                    &self.counters,
                    &self.peer_class_pool,
                    Instant::now(),
                    &mut eff,
                );
            }
            self.route(*ih, eff, out);
        }
        self.sync_connected_peers();
    }

    /// `num_peers_connected` gauge — recomputed over all torrents
    /// after any conn attach/drop (upstream inc/decs on
    /// `peer_connection` init/disconnect; a recompute keeps it
    /// exact when several conns leave in one batch).
    fn sync_connected_peers(&self) {
        let n = self.torrents.values().map(|t| t.peers.len() as i64).sum();
        self.counters.set(Counter::NumPeersConnected, n);
    }

    /// a peer disconnected — refcount down + abort in-flight.
    ///
    /// `gone` carries the disconnect context: session-initiated
    /// `disconnect()` severity + reason (`PeerGone::Disconnected`),
    /// a transport/codec failure from the read pump
    /// (`PeerGone::Transport`), or a clean/socket close (`None`).
    ///
    /// Severity semantics are upstream `peer_connection::disconnect`'s:
    /// `peer_error` → `peer_error_alert` + `error_peers` (and no
    /// `peer_disconnected_alert` — they are mutually exclusive);
    /// `failure`/`normal` → `peer_disconnected_alert`. Anything
    /// `> normal` sets `m_failed` (failcount bump in
    /// `connection_closed`) — OR-ed with `PeerConn::failed` so a
    /// dispatch-time severity cannot be downgraded by a later benign
    /// teardown.
    pub fn peer_disconnected(
        &mut self,
        ih: &Sha1Hash,
        peer: u64,
        gone: Option<bitdaemon_net::bt_peer_conn::PeerGone>,
        out: &mut Vec<SessionOut>,
    ) {
        use bitdaemon_net::bt_peer_conn::{DisconnectSeverity, PeerConnError, PeerGone};
        use bitdaemon_net::peer_wire::WireError;
        let (sev, reason, incoming_id) = match &gone {
            Some(PeerGone::Disconnected { severity, reason }) => {
                (*severity, Some((*reason).to_string()), None)
            }
            Some(PeerGone::Transport(e)) => match e {
                // socket EOF/reset — `sock_read`/`sock_write` run at
                // normal severity upstream.
                PeerConnError::Io(_) => (DisconnectSeverity::Normal, Some(e.to_string()), None),
                // a complete frame that failed size validation —
                // upstream counts it in `num_incoming_*` before the
                // `invalid_*` disconnect (the counter block runs when
                // the packet finishes; the disconnect is inside the
                // handler).
                PeerConnError::Wire(WireError::BadLength { id, .. }) => (
                    DisconnectSeverity::PeerError,
                    Some(e.to_string()),
                    Some(*id),
                ),
                // unknown wire id — upstream's dispatch `default` leg
                // returns *before* the counter block: an
                // `invalid_message` kill with no `num_incoming_*`.
                PeerConnError::Wire(WireError::UnknownId(_)) => {
                    (DisconnectSeverity::PeerError, Some(e.to_string()), None)
                }
                // `packet_too_large` (frame > 1 MiB) and friends —
                // `peer_error` upstream.
                _ => (DisconnectSeverity::PeerError, Some(e.to_string()), None),
            },
            None => (DisconnectSeverity::Normal, None, None),
        };
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            if let Some(id) = incoming_id {
                // same quirky mapping as the dispatch counter block:
                // ids 0..=9 → `num_incoming_choke + id`, 13..=17 →
                // `num_incoming_suggest + id`, else `num_incoming_extended`.
                self.counters
                    .inc_idx(crate::counters::incoming_counter_idx(id));
            }
            let (remote, outgoing, is_utp, conn_failed, conn_fast) = t
                .peers
                .get(&peer)
                .map_or((None, false, false, false, false), |c| {
                    (c.remote, c.outgoing, c.is_utp, c.failed, c.fast_reconnect)
                });
            if let Some(r) = remote {
                // `peer` IS the conn's `conn_key` — the bound-entry
                // lookup survives `update_peer_port` rewrites.
                t.peer_disconnected_addr(
                    r,
                    self.session_time,
                    conn_failed || sev > DisconnectSeverity::Normal,
                    conn_fast,
                    peer,
                );
            }
            let had = t.peers.contains_key(&peer);
            t.remove_peer(peer, &self.counters, &self.peer_class_pool, &mut eff);
            if had {
                // `peer_connection::disconnect` counter legs —
                // `disconnected_peers` on every close, then the
                // per-error legs. Transport kinds come off the io
                // error; upstream error names off the disconnect
                // reason.
                self.counters
                    .inc(crate::counters::Counter::DisconnectedPeers);
                let kind_ctr = match &gone {
                    Some(PeerGone::Transport(PeerConnError::Io(e))) => {
                        use crate::counters::Counter as C;
                        use std::io::ErrorKind as K;
                        match e.kind() {
                            K::ConnectionReset => Some(C::ConnresetPeers),
                            K::UnexpectedEof => Some(C::EofPeers),
                            K::ConnectionRefused => Some(C::ConnrefusedPeers),
                            K::ConnectionAborted => Some(C::ConnabortedPeers),
                            K::NotConnected => Some(C::NotconnectedPeers),
                            K::PermissionDenied => Some(C::PermPeers),
                            K::BrokenPipe => Some(C::BrokenPipePeers),
                            K::AddrInUse => Some(C::AddrinusePeers),
                            K::TimedOut => Some(C::TransportTimeoutPeers),
                            _ => None,
                        }
                    }
                    Some(PeerGone::Disconnected { reason, .. }) => {
                        crate::counters::disconnect_reason_counter(reason)
                    }
                    _ => None,
                };
                if let Some(c) = kind_ctr {
                    self.counters.inc(c);
                }
                if sev > DisconnectSeverity::Normal {
                    // `error > normal` → transport + direction legs.
                    self.counters.inc(if is_utp {
                        crate::counters::Counter::ErrorUtpPeers
                    } else {
                        crate::counters::Counter::ErrorTcpPeers
                    });
                    self.counters.inc(if outgoing {
                        crate::counters::Counter::ErrorOutgoingPeers
                    } else {
                        crate::counters::Counter::ErrorIncomingPeers
                    });
                }
                if sev == DisconnectSeverity::PeerError {
                    // upstream posts `peer_error_alert` for severity
                    // `peer_error` (`error_peers` counter), never
                    // `peer_disconnected_alert`.
                    self.counters.inc(crate::counters::Counter::ErrorPeers);
                    self.alerts.post(Alert::PeerError {
                        ih: *ih,
                        remote: remote.map_or_else(String::new, |a| a.to_string()),
                        op: "bittorrent".into(),
                        error: reason.unwrap_or_else(|| "peer error".into()),
                    });
                } else {
                    // `peer_disconnected_alert` — normal/failure.
                    self.alerts.post(Alert::PeerDisconnected {
                        ih: *ih,
                        reason: format!(
                            "peer {peer}: {}",
                            reason.as_deref().unwrap_or("disconnected")
                        ),
                    });
                }
                self.alerts.post(Alert::PeerLog {
                    ih: *ih,
                    remote: String::new(),
                    msg: "peer disconnected".into(),
                });
            }
        }
        self.sync_connected_peers();
        self.route(*ih, eff, out);
    }

    /// `torrent_handle::ban_peer` — ban the swarm entry and drop the
    /// live connection if `remote` is connected right now (upstream
    /// `torrent::set_ban` + `disconnect` semantics).
    pub fn ban_peer(&mut self, ih: &Sha1Hash, remote: SocketAddr, out: &mut Vec<SessionOut>) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            t.ban_peer(remote);
            let conn_id = t
                .peers
                .iter()
                .find(|(_, c)| c.remote == Some(remote))
                .map(|(id, _)| *id);
            if let Some(id) = conn_id {
                // `peer_banned` is `peer_error` upstream → failed.
                let fast = t.peers.get(&id).is_some_and(|c| c.fast_reconnect);
                t.peer_disconnected_addr(remote, self.session_time, true, fast, id);
                t.remove_peer(id, &self.counters, &self.peer_class_pool, &mut eff);
                self.alerts.post(Alert::PeerDisconnected {
                    ih: *ih,
                    reason: format!("peer {id}: banned"),
                });
            }
            self.alerts.post(Alert::PeerBan {
                ih: *ih,
                ip: remote,
                // `p->connection ? pid() : peer_id()` upstream.
                pid: conn_id
                    .and_then(|id| t.peers.get(&id))
                    .and_then(|c| c.peer_id),
            });
        }
        self.sync_connected_peers();
        self.route(*ih, eff, out);
    }

    /// `torrent::lsd_announce` — emit a one-shot LSD broadcast for
    /// `ih` (same gates and payload as the periodic tick).
    pub fn force_lsd_announce(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        if self.settings.anonymous_mode || !self.services.lsd {
            return;
        }
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        let i2p = t.ti.as_ref().is_some_and(|ti| ti.is_i2p());
        if t.private
            || !t.announce_lsd
            || !t.enable_lsd
            || t.is_paused()
            || (!t.files_checked && t.ti.is_some())
            || (i2p && !self.settings.allow_i2p_mixed)
        {
            return;
        }
        let hex = bitdaemon_common::hex::to_hex(t.info_hash.as_bytes());
        for v6 in [false, true] {
            let pkt =
                bitdaemon_net::lsd::render_lsd_packet(self.listen_port, &hex, self.lsd_cookie, v6);
            let to = if v6 {
                SocketAddr::new(bitdaemon_net::lsd::LSD_ADDR6.into(), 6771)
            } else {
                SocketAddr::new(bitdaemon_net::lsd::LSD_ADDR4.into(), 6771)
            };
            out.push(SessionOut::UdpPacket(pkt, to));
        }
        t.lsd_announcing = true;
    }

    /// `torrent_handle::add_piece` — inject user-supplied piece
    /// data: split into blocks, queue one disk write per block; the
    /// normal write→`piece_complete`→hash path verifies it
    /// (upstream `torrent::add_piece` semantics).
    pub fn add_piece(&mut self, ih: &Sha1Hash, piece: PieceIndex, data: &[u8], flags: u8) {
        let _ = flags;
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        // upstream `torrent::add_piece`: each block goes through
        // mark_as_downloading + mark_as_writing so the picker tracks
        // it; already-finished blocks are skipped without
        // `overwrite_existing`.
        const OVERWRITE: u8 = 1;
        let mut eff = Vec::new();
        if let Some(p) = t.picker.as_mut() {
            let blocks = p.blocks_in_piece(piece);
            for i in 0..blocks {
                let b = bitdaemon_common::types::PieceBlock {
                    piece_index: piece,
                    block_index: i,
                };
                if flags & OVERWRITE == 0 && p.is_finished_block(b) {
                    continue;
                }
                if p.is_downloaded(b) && !p.is_finished_block(b) {
                    continue;
                }
                p.mark_as_downloading(b, None, crate::piece_picker::PickerOptions::NONE);
                p.mark_as_writing(b, None);
            }
        }
        let piece_len = t.piece_length;
        let mut off = 0i32;
        for chunk in data.chunks(16384) {
            eff.push(TorrentEffect::DiskWrite {
                // `add_piece` injects data out-of-band (no peer sent
                // it) — the same sentinel web-seed writes use, since
                // neither has a socket pump to gate.
                peer: crate::torrent::Torrent::WEB_SEED_PEER,
                req: bitdaemon_common::types::PeerRequest {
                    piece,
                    start: off,
                    length: chunk.len() as i32,
                },
                data: bytes::Bytes::copy_from_slice(chunk),
            });
            off += chunk.len() as i32;
        }
        let _ = piece_len;
        let mut out = Vec::new();
        self.route(*ih, eff, &mut out);
    }

    /// `peer_connection::connect_failed` + `disconnect(e, op, sev)`
    /// — an outbound dial died before attach (transport refusal or
    /// handshake failure). Applies the upstream counter block and
    /// alert (`peer_disconnected` for severity ≤ failure,
    /// `peer_error` otherwise — posted even when the torrent is
    /// gone, like upstream's detached handles), then the swarm-entry
    /// bookkeeping. A failed uTP leg on a `supports_utp` peer posts
    /// upstream's deferred TCP retry (`connect_to_peer(pi,
    /// ignore_limit)`) as a `ConnectPeer` on `pending_out`.
    pub fn peer_dial_failed(
        &mut self,
        ih: &Sha1Hash,
        remote: SocketAddr,
        fail: &bitdaemon_net::bt_peer_conn::DialFail,
    ) {
        use bitdaemon_net::pe_crypto::EncPolicy;
        self.disconnect_alert_counters(*ih, remote, fail, true);
        // `init_bt_handshake` re-arms `pe_support` the instant PE4
        // completes — before the inner handshake — so a conn whose
        // encrypted portion finished retries encrypted even when the
        // BT leg failed. Gated `is_outgoing && pe_enabled` upstream.
        if fail.mse_done && self.settings.out_enc_policy == EncPolicy::Enabled {
            if let Some(t) = self.torrents.get_mut(ih) {
                t.rearm_pe_support(remote);
            }
        }
        if let Some(t) = self.torrents.get_mut(ih) {
            if let Some(pid) = t.peer_dial_failed(
                remote,
                self.session_time,
                fail,
                &self.settings.peer_fingerprint,
            ) {
                // `connect_to_peer(pi, ignore_limit)` — the retry
                // bypasses the connect-pump budget (the dial mark
                // was re-armed, not refunded).
                self.alerts.post(Alert::PeerConnect {
                    ih: *ih,
                    remote: remote.to_string(),
                });
                self.pending_out.push(SessionOut::ConnectPeer {
                    ih: *ih,
                    ep: remote,
                    our_pid: pid,
                });
            }
        }
    }

    /// `disconnect()` on an *inbound* conn that died during the
    /// accept-side handshake — upstream runs the same counter block
    /// and posts the same alert even for an unattached conn (the
    /// alert's torrent handle is simply invalid). The peer-list legs
    /// are a no-op: `new_connection` registers the entry only after
    /// the full handshake, so `connection_closed` sees a null
    /// `peer_info_struct`.
    pub fn peer_conn_failed_inbound(
        &mut self,
        ih: Option<Sha1Hash>,
        remote: SocketAddr,
        fail: &bitdaemon_net::bt_peer_conn::DialFail,
    ) {
        use crate::counters::Counter as C;
        self.counters.add(C::NumPeersHalfOpen, -1);
        self.disconnect_alert_counters(ih.unwrap_or_default(), remote, fail, false);
    }

    /// the `disconnect()` counter block + alert — shared by the
    /// outgoing-dial and inbound-accept failure paths. `outgoing`
    /// selects `error_outgoing_peers`/`error_incoming_peers`.
    fn disconnect_alert_counters(
        &mut self,
        ih: Sha1Hash,
        remote: SocketAddr,
        fail: &bitdaemon_net::bt_peer_conn::DialFail,
        outgoing: bool,
    ) {
        use crate::counters::Counter as C;
        use bitdaemon_net::bt_peer_conn::{DisconnectSeverity, FailOp};
        // `connect_failed` bumps `connect_timeouts` for the transport
        // refusal itself; `timed_out_no_handshake` inside
        // `disconnect()` carries it for the handshake deadline.
        if fail.op == FailOp::Connect {
            self.counters.inc(C::ConnectTimeouts);
        }
        // `disconnected_peers` — every `ec != 0` close.
        self.counters.inc(C::DisconnectedPeers);
        // per-`ec` transport legs.
        if let Some(k) = fail.io_kind {
            use std::io::ErrorKind as K;
            let c = match k {
                K::ConnectionReset => Some(C::ConnresetPeers),
                K::UnexpectedEof => Some(C::EofPeers),
                K::ConnectionRefused => Some(C::ConnrefusedPeers),
                K::ConnectionAborted => Some(C::ConnabortedPeers),
                K::NotConnected => Some(C::NotconnectedPeers),
                K::PermissionDenied => Some(C::PermPeers),
                K::BrokenPipe => Some(C::BrokenPipePeers),
                K::AddrInUse => Some(C::AddrinusePeers),
                K::TimedOut => Some(C::TransportTimeoutPeers),
                _ => None,
            };
            if let Some(c) = c {
                self.counters.inc(c);
            }
        }
        // named lt-error legs (`timed_out_no_handshake` & co.).
        if let Some(c) = crate::counters::disconnect_reason_counter(fail.reason) {
            self.counters.inc(c);
        }
        if fail.severity > DisconnectSeverity::Normal {
            self.counters.inc(if fail.utp {
                C::ErrorUtpPeers
            } else {
                C::ErrorTcpPeers
            });
            self.counters.inc(if outgoing {
                C::ErrorOutgoingPeers
            } else {
                C::ErrorIncomingPeers
            });
            // crypto legs — `type()==bittorrent && op != connect`:
            // `supports_encryption` → `error_encrypted_peers`, plus
            // `rc4_encrypted` → `error_rc4_peers`.
            if fail.op != FailOp::Connect && fail.encrypted {
                self.counters.inc(C::ErrorEncryptedPeers);
                if fail.rc4 {
                    self.counters.inc(C::ErrorRc4Peers);
                }
            }
        }
        if fail.severity == DisconnectSeverity::PeerError {
            self.counters.inc(C::ErrorPeers);
            self.alerts.post(Alert::PeerError {
                ih,
                remote: remote.to_string(),
                op: fail.op.name().into(),
                error: fail.reason.into(),
            });
        } else {
            self.alerts.post(Alert::PeerDisconnected {
                ih,
                reason: format!("peer {remote}: {}", fail.reason),
            });
        }
    }

    /// `peer_connect_failed` — a transport-level dial failure
    /// (`connect()` refused/errored). Thin wrapper over
    /// [`Self::peer_dial_failed`]; returns `true` when the deferred
    /// TCP retry is owed (failed uTP leg).
    pub fn peer_connect_failed(&mut self, ih: &Sha1Hash, remote: SocketAddr, was_utp: bool) {
        let fail = bitdaemon_net::bt_peer_conn::DialFail::connect_failed(
            std::io::ErrorKind::Other,
            was_utp,
        );
        self.peer_dial_failed(ih, remote, &fail);
    }

    /// `bt_peer_connection::on_connected` — runs on a freshly
    /// *connected* outbound socket, before the handshake is written:
    /// the wire crypto is decided there (the `pe_support` toggle and
    /// the `fast_reconnect` arm both live at this point upstream).
    /// `is_ssl` forces `pe_disabled` (SSL streams never run MSE).
    ///
    /// Returns `(wire_ih, protocol_v2, has_v2, encrypted)` — the
    /// `associated_info_hash` for the wire + SKEY, the conn's
    /// protocol version, the v2 reserved-bit advertise, and whether
    /// to run MSE. `None` when the peer entry vanished mid-dial.
    pub fn peer_link_up(
        &mut self,
        ih: &Sha1Hash,
        remote: SocketAddr,
        is_ssl: bool,
    ) -> Option<(Sha1Hash, bool, bool, bool)> {
        let policy = self.settings.out_enc_policy;
        let stime = self.session_time;
        self.torrents
            .get_mut(ih)?
            .peer_link_up(remote, policy, is_ssl, stime)
    }

    /// Resolve an inbound wire info-hash (plaintext handshake or MSE
    /// skey) to a torrent — upstream `torrent_list::find`/`find_
    /// obfuscated` semantics: every torrent is indexed under *both*
    /// its v1 hash and its truncated v2 hash.
    #[must_use]
    pub fn known_torrent(
        &self,
        wire_ih: &Sha1Hash,
    ) -> Option<bitdaemon_net::bt_peer_conn::KnownTorrent> {
        use bitdaemon_common::info_hash::ProtocolVersion as PV;
        self.torrents.iter().find_map(|(kih, t)| {
            let mut hit = None;
            t.info_hashes.for_each(|h, v| {
                if hit.is_none() && h == *wire_ih {
                    hit = Some(Self::known_torrent_of(kih, t, h, v == PV::V2));
                }
            });
            hit
        })
    }

    /// `torrent_list::find_obfuscated` — resolve an MSE `req2`
    /// stream-key hash (`SHA1("req2" + ih)`) to a torrent; both the
    /// v1 and the truncated v2 hash of every torrent are probed
    /// (upstream indexes each under its own obfuscated key).
    #[must_use]
    pub fn known_torrent_by_skey(
        &self,
        sk: &Sha1Hash,
    ) -> Option<bitdaemon_net::bt_peer_conn::KnownTorrent> {
        use bitdaemon_common::info_hash::ProtocolVersion as PV;
        self.torrents.iter().find_map(|(kih, t)| {
            let mut hit = None;
            t.info_hashes.for_each(|h, v| {
                if hit.is_none() && bitdaemon_net::pe_crypto::stream_key_hash(&h) == *sk {
                    hit = Some(Self::known_torrent_of(kih, t, h, v == PV::V2));
                }
            });
            hit
        })
    }

    fn known_torrent_of(
        kih: &Sha1Hash,
        t: &Torrent,
        wire_ih: Sha1Hash,
        protocol_v2: bool,
    ) -> bitdaemon_net::bt_peer_conn::KnownTorrent {
        bitdaemon_net::bt_peer_conn::KnownTorrent {
            ih: *kih,
            wire_ih,
            protocol_v2,
            has_v2: t.info_hashes.has_v2(),
            v2_ih: t.info_hashes.has_v2().then(|| {
                t.info_hashes
                    .get(bitdaemon_common::info_hash::ProtocolVersion::V2)
            }),
            valid_metadata: t.valid_metadata(),
        }
    }

    /// `connect_to_peer` mark for a caller-specified endpoint —
    /// the engine's manual `connect_peer`: insert + pending mark +
    /// `outgoing_pids`/`dial_pids` registration for the conn's
    /// generated peer-id. `false` when already connected/dialing.
    pub fn register_dial(
        &mut self,
        ih: &Sha1Hash,
        remote: SocketAddr,
        pid: bitdaemon_common::PeerId,
    ) -> bool {
        self.torrents
            .get_mut(ih)
            .is_some_and(|t| t.register_manual_dial(remote, self.session_time, pid))
    }

    /// pending-dial local endpoint — the engine reports the bound
    /// socket address the moment a dial's connect lands (upstream's
    /// half-open conn object carries `local_endpoint()`); the
    /// `new_connection` self-connection check resolves pending
    /// dials through it.
    pub fn register_dial_local(&mut self, ih: &Sha1Hash, remote: SocketAddr, local: SocketAddr) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.dial_locals.insert(remote, local);
        }
    }

    /// a disk write/read/hash completed for `ih`.
    /// `piece_verified` when a hash job reports.
    pub fn on_piece_verified(
        &mut self,
        ih: &Sha1Hash,
        piece: bitdaemon_common::types::PieceIndex,
        passed: bool,
        out: &mut Vec<SessionOut>,
    ) {
        let mut eff = Vec::new();
        let recheck_drained = if let Some(t) = self.torrents.get_mut(ih) {
            // `smart_ban` bookkeeping rides the same path as upstream
            // `on_piece_finished`/`on_piece_failed`: `addr` resolves a
            // downloader's IP from its live connection; `read_digest`
            // would re-read the block hash from disk — the disk job is
            // async here, so we pass `None` (upstream treats a failed
            // re-read identically: the suspicious peer goes unbanned
            // until a later piece exposes it).
            // downloader keys are `peer_key()` values (first 8 bytes
            // of the remote peer-id), not conn ids — resolve through
            // the conn's key, not its slot in `peers`.
            let addrs: std::collections::HashMap<u64, std::net::IpAddr> = t
                .peers
                .values()
                .filter_map(|c| Some((c.peer_key()?, c.remote?.ip())))
                .collect();
            t.piece_verified_ban(
                piece,
                passed,
                &[],
                &self.settings,
                &self.counters,
                &mut eff,
                |peer| {
                    addrs
                        .get(&peer)
                        .copied()
                        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
                },
                |_| None,
            );
            t.recheck_piece_done(&self.settings, &mut eff)
        } else {
            false
        };
        self.route(*ih, eff, out);
        if recheck_drained {
            // `files_checked` tail — `start_announcing` (state
            // transitions and `torrent_checked` already routed;
            // upstream calls `start_announcing` last).
            self.start_announcing(ih, out);
        }
    }

    /// `move_storage` — upstream `torrent_handle::move_storage`
    /// → `async_move_storage` (verdict arrives as
    /// `DiskCompleted::MoveStorage` → `storage_moved{,_failed}`).
    pub fn move_storage(
        &mut self,
        ih: &Sha1Hash,
        path: std::path::PathBuf,
        flags: bitdaemon_disk::storage::MoveFlags,
    ) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        // upstream `m_abort` — a torn-down torrent rejects with
        // `storage_moved_failed(operation_aborted)`.
        if t.aborted {
            self.alerts.post(Alert::StorageMovedFailed {
                ih: *ih,
                file: String::new(),
                op: "unknown".into(),
                error: "operation aborted".into(),
            });
            return;
        }
        // `!valid_metadata()` — no file structure known; upstream
        // adopts the path and posts `storage_moved` without a job.
        if t.files.is_none() {
            let newp = bitdaemon_disk::fs::complete(&path);
            let old = t.save_path.clone();
            t.save_path = newp.to_string_lossy().into_owned();
            t.need_save_resume |= bitdaemon_common::flags::ResumeDataFlags::IF_CONFIG_CHANGED;
            self.alerts.post(Alert::StorageMoved {
                ih: *ih,
                path: newp.to_string_lossy().into_owned(),
                old_path: old,
            });
            return;
        }
        // `!m_storage` — adopt the path + `need_save_resume` +
        // `storage_moved`, no disk work.
        let Some(disk) = self.disk.clone() else {
            let old = t.save_path.clone();
            t.save_path = path.to_string_lossy().into_owned();
            t.need_save_resume |= bitdaemon_common::flags::ResumeDataFlags::IF_CONFIG_CHANGED;
            self.alerts.post(Alert::StorageMoved {
                ih: *ih,
                path: path.to_string_lossy().into_owned(),
                old_path: old,
            });
            return;
        };
        let storage = t.storage;
        // `m_moving_storage` — cleared in `on_storage_moved`.
        t.moving_storage = true;
        let tx = self.disk_tx.clone();
        let ih = *ih;
        let path2 = path.clone();
        disk.async_move_storage(
            storage,
            path,
            flags,
            Box::new(move |status, path, err| {
                let _ = tx.send(DiskCompleted::MoveStorage {
                    ih,
                    path: if path.as_os_str().is_empty() {
                        path2.clone()
                    } else {
                        path
                    },
                    status,
                    err,
                });
            }),
        );
    }

    /// `rename_file` — `async_rename_file` → `file_renamed{,_failed}`.
    /// Upstream marks `need_save_resume(if_state_changed)` +
    /// `state_updated` in the *completion* on success — a failed
    /// rename leaves resume state clean.
    pub fn rename_file(
        &mut self,
        ih: &Sha1Hash,
        index: bitdaemon_common::types::FileIndex,
        name: std::path::PathBuf,
    ) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        // upstream precondition — `index` out of bounds returns
        // silently; a metadata-less torrent's empty layout bounds
        // every index the same way.
        if index.0 < 0 || t.files.as_ref().is_none_or(|fs| index.0 >= fs.num_files()) {
            return;
        }
        let Some(disk) = self.disk.clone() else {
            // `!m_storage` upstream → `file_rename_failed_alert`
            // (`session_is_closing`).
            self.alerts.post(Alert::FileRenameFailed {
                ih: *ih,
                index: index.0,
                error: "session is closing".into(),
            });
            return;
        };
        let storage = t.storage;
        let tx = self.disk_tx.clone();
        let ih = *ih;
        disk.async_rename_file(
            storage,
            index,
            name,
            Box::new(move |name, index, err| {
                let _ = tx.send(DiskCompleted::RenameFile {
                    ih,
                    index,
                    name,
                    err,
                });
            }),
        );
    }

    /// `delete_files` — upstream `torrent_handle::delete_files`:
    /// remove the torrent then `async_delete_files` the payload
    /// (`torrent_deleted{,_failed}` on completion).
    pub fn delete_files(&mut self, ih: &Sha1Hash, options: u8, out: &mut Vec<SessionOut>) {
        let Some(t) = self.torrents.get(ih) else {
            return;
        };
        let storage = t.storage;
        let Some(disk) = self.disk.clone() else {
            // upstream `torrent::delete_files` returns false without
            // storage → `remove_torrent_impl` posts
            // `torrent_delete_failed_alert` (empty ec) *before* the
            // removal alert.
            self.alerts.post(Alert::TorrentDeleteFailed {
                ih: *ih,
                error: String::new(),
            });
            self.remove_torrent(ih, out);
            return;
        };
        let tx = self.disk_tx.clone();
        let ih2 = *ih;
        // upstream `disk_io_thread` ordering: the delete job is
        // queued *before* the storage teardown — `remove_torrent`
        // is itself a fence job now, so enqueueing delete first
        // keeps `async_delete_files` ahead of it.
        disk.async_delete_files(
            storage,
            options,
            Box::new(move |err| {
                let _ = tx.send(DiskCompleted::DeleteFiles { ih: ih2, err });
            }),
        );
        self.remove_torrent(ih, out);
    }

    /// `flush_cache` — upstream `torrent_handle::flush_cache`
    /// → `async_release_files` (handles released; the OS page
    /// cache writeback is the storage layer's job).
    pub fn flush_cache(&mut self, ih: &Sha1Hash) {
        let (Some(disk), Some(t)) = (self.disk.clone(), self.torrents.get(ih)) else {
            return;
        };
        let storage = t.storage;
        let tx = self.disk_tx.clone();
        let ih = *ih;
        disk.async_release_files(
            storage,
            Box::new(move || {
                let _ = tx.send(DiskCompleted::ReleaseFiles { ih });
            }),
        );
    }

    /// `force_recheck` — clear `have` and re-hash every piece
    /// (upstream `torrent_handle::force_recheck` → per-piece
    /// `disk_hash` jobs; `piece_verified` re-populates).
    pub fn force_recheck(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih)
            && t.arm_full_recheck(&mut eff) > 0
        {
            for p in 0..t.num_pieces {
                eff.push(TorrentEffect::DiskHash {
                    piece: bitdaemon_common::types::PieceIndex(p),
                });
            }
        }
        self.route(*ih, eff, out);
    }

    /// `session_impl::set_ip_filter` — swap the filter and apply it:
    /// disconnected banned peers are erased, live ones disconnected
    /// (`ip_filter_updated` → `peer_list::apply_ip_filter`).
    pub fn set_ip_filter(
        &mut self,
        filter: bitdaemon_net::ip_filter::IpFilter,
        out: &mut Vec<SessionOut>,
    ) {
        self.ip_filter = std::sync::Arc::new(filter);
        let f = std::sync::Arc::clone(&self.ip_filter);
        let mut pending: Vec<(Sha1Hash, Vec<TorrentEffect>)> = Vec::new();
        for t in self.torrents.values_mut() {
            // `torrent::set_ip_filter` — the torrent holds the same
            // shared filter (`m_ip_filter`) for `add_peer` gates.
            t.ip_filter = Some(std::sync::Arc::clone(&f));
            // `torrent::ip_filter_updated` early-returns when
            // `!m_apply_ip_filter` — opted-out torrents
            // (`non_filter_torrents`) are exempt from the sweep.
            if !t.apply_ip_filter {
                continue;
            }
            // upstream `apply_ip_filter` sweeps blocked entries and
            // disconnects their linked conns first — `disconnect`
            // runs `remove_peer`/`connection_closed` bookkeeping
            // while the swarm entry is still there, then the entry
            // is erased. Do conn teardown before the list sweep so
            // `peer_disconnected_addr` still resolves the index.
            let mut eff = Vec::new();
            let ids: Vec<u64> = t
                .peers
                .iter()
                .filter(|(_, c)| {
                    c.remote
                        .is_some_and(|r| f.access(r.ip()) == bitdaemon_net::ip_filter::BLOCKED)
                })
                .map(|(id, _)| *id)
                .collect();
            let mut banned: Vec<std::net::IpAddr> = Vec::new();
            for id in &ids {
                if let Some(c) = t.peers.get(id)
                    && let Some(remote) = c.remote
                {
                    banned.push(remote.ip());
                    // `banned_by_ip_filter` runs at default severity
                    // upstream — no failcount bump.
                    t.peer_disconnected_addr(
                        remote,
                        self.session_time,
                        false,
                        c.fast_reconnect,
                        *id,
                    );
                }
                t.remove_peer(*id, &self.counters, &self.peer_class_pool, &mut eff);
                eff.push(TorrentEffect::DisconnectPeer {
                    peer: *id,
                    reason: "banned_by_ip_filter",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
            }
            // erase blocked entries with no live conn (the sweep
            // returns only *connected* banned addrs — already in
            // `banned` above).
            t.peer_list
                .apply_ip_filter(|a| f.access(a) == bitdaemon_net::ip_filter::BLOCKED);
            // `peer_blocked_alert(ip_filter)` per disconnected addr.
            for ip in banned {
                self.alerts.post(Alert::PeerBlocked {
                    ih: t.info_hash,
                    remote: ip.to_string(),
                    reason: "ip_filter".into(),
                });
            }
            if eff.is_empty() {
                continue;
            }
            pending.push((t.info_hash, eff));
        }
        self.sync_connected_peers();
        for (ih, eff) in pending {
            self.route(ih, eff, out);
        }
    }

    /// `torrent_handle::apply_ip_filter` routed through the
    /// session — the torrent setter emits conn teardown + alerts
    /// via `TorrentEffect` like `set_ip_filter` does.
    pub(crate) fn apply_ip_filter(&mut self, ih: &Sha1Hash, on: bool, out: &mut Vec<SessionOut>) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            t.set_apply_ip_filter(on, &self.counters, &mut eff);
        }
        self.route(*ih, eff, out);
    }

    /// current access rule for `addr` (`ip_filter::access`).
    #[must_use]
    pub fn ip_access(&self, addr: std::net::IpAddr) -> u32 {
        self.ip_filter.access(addr)
    }

    /// `session_impl::set_port_filter` — swap and apply: every torrent
    /// runs `port_filter_updated` → `peer_list::apply_port_filter`,
    /// which disconnects filtered conns (default severity — no
    /// failcount), erases blocked swarm entries, and reports each
    /// endpoint via `peer_blocked_alert(port_filter)`.
    pub fn set_port_filter(
        &mut self,
        filter: bitdaemon_net::ip_filter::PortFilter,
        out: &mut Vec<SessionOut>,
    ) {
        self.port_filter = std::sync::Arc::new(filter);
        let f = std::sync::Arc::clone(&self.port_filter);
        let mut pending: Vec<(Sha1Hash, Vec<TorrentEffect>)> = Vec::new();
        for t in self.torrents.values_mut() {
            t.port_filter = Some(std::sync::Arc::clone(&f));
            let mut eff = Vec::new();
            // conn teardown first — the swarm entry must still be
            // present for `connection_closed` bookkeeping.
            let ids: Vec<u64> = t
                .peers
                .iter()
                .filter(|(_, c)| {
                    c.remote
                        .is_some_and(|r| f.access(r.port()) == bitdaemon_net::ip_filter::BLOCKED)
                })
                .map(|(id, _)| *id)
                .collect();
            for id in &ids {
                if let Some(c) = t.peers.get(id)
                    && let Some(remote) = c.remote
                {
                    t.peer_disconnected_addr(
                        remote,
                        self.session_time,
                        false,
                        c.fast_reconnect,
                        *id,
                    );
                    self.alerts.post(Alert::PeerBlocked {
                        ih: t.info_hash,
                        remote: remote.to_string(),
                        reason: "port_filter".into(),
                    });
                }
                t.remove_peer(*id, &self.counters, &self.peer_class_pool, &mut eff);
                eff.push(TorrentEffect::DisconnectPeer {
                    peer: *id,
                    reason: "banned_by_port_filter",
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                });
            }
            // erase filtered swarm entries (unconnected leftovers).
            let mut i = 0;
            while i < t.peer_list.peers().len() {
                let blocked = t.peer_list.peers()[i].port != 0
                    && f.access(t.peer_list.peers()[i].port) == bitdaemon_net::ip_filter::BLOCKED;
                if !blocked || t.peer_list.locked_peer() == Some(i) {
                    i += 1;
                    continue;
                }
                t.peer_list.erase_peer(i);
            }
            if eff.is_empty() {
                continue;
            }
            pending.push((t.info_hash, eff));
        }
        self.sync_connected_peers();
        for (ih, eff) in pending {
            self.route(ih, eff, out);
        }
    }

    /// `session_impl::get_ip_filter`.
    #[must_use]
    pub fn ip_filter(&self) -> &bitdaemon_net::ip_filter::IpFilter {
        &self.ip_filter
    }

    /// `session_handle::pause`/`resume` — upstream pauses the whole
    /// session: every torrent is paused/resumed and the flag is
    /// remembered for torrents added later.
    /// `session_impl::pause`/`resume` — flips the *session* pause
    /// layer (`m_session_paused`) on every torrent; a torrent's
    /// effective state only changes when `is_paused()` flips, so
    /// a user-resumed torrent stays paused while the session is
    /// paused (upstream `torrent::set_session_paused`).
    pub fn set_session_paused(&mut self, paused: bool, out: &mut Vec<SessionOut>) {
        if self.paused == paused {
            return;
        }
        self.paused = paused;
        let ihs: Vec<Sha1Hash> = self.torrents.keys().copied().collect();
        for ih in ihs {
            let changed = if let Some(t) = self.torrents.get_mut(&ih) {
                if t.session_paused == paused {
                    false
                } else {
                    let before = t.is_paused();
                    t.session_paused = paused;
                    before != t.is_paused()
                }
            } else {
                false
            };
            if !changed {
                continue;
            }
            if paused {
                self.do_pause(&ih, out);
            } else {
                self.do_resume(&ih, out);
            }
        }
    }

    /// `session_impl::is_paused`.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// `session_impl::create_peer_class` — allocate a class in
    /// `m_classes` (upstream `peer_class_pool::new_peer_class`).
    pub fn create_peer_class(&mut self, label: &str) -> bitdaemon_net::peer_class::PeerClassIndex {
        self.peer_class_pool.new_peer_class(label)
    }

    /// `session_impl::delete_peer_class` — `peer_class_pool::decref`.
    pub fn delete_peer_class(&mut self, c: bitdaemon_net::peer_class::PeerClassIndex) {
        self.peer_class_pool.decref(c);
    }

    /// `session_impl::set_peer_class` — apply `PeerClassInfo` to an
    /// existing class (upstream `peer_class::set_info` clamps limits
    /// and priorities). No-op for an invalid id (`TORRENT_ASSERT_PRECOND`).
    pub fn set_peer_class(
        &mut self,
        cid: bitdaemon_net::peer_class::PeerClassIndex,
        pci: &bitdaemon_net::peer_class::PeerClassInfo,
    ) {
        if let Some(pc) = self.peer_class_pool.at_mut(cid) {
            pc.set_info(pci);
        }
    }

    /// `session_impl::get_peer_class` — snapshot of the class'
    /// current `PeerClassInfo` (`None` for an invalid id, where
    /// upstream asserts and returns a default info).
    #[must_use]
    pub fn get_peer_class(
        &self,
        cid: bitdaemon_net::peer_class::PeerClassIndex,
    ) -> Option<bitdaemon_net::peer_class::PeerClassInfo> {
        self.peer_class_pool.at(cid).map(|pc| pc.get_info())
    }

    /// `session_impl::set_peer_class_filter` (`ip_filter` of ranges
    /// mapping to classes).
    pub fn set_peer_class_filter(&mut self, f: bitdaemon_net::ip_filter::IpFilter) {
        self.peer_class_filter = f;
    }

    /// `session_impl::get_peer_class_filter`.
    #[must_use]
    pub fn peer_class_filter(&self) -> &bitdaemon_net::ip_filter::IpFilter {
        &self.peer_class_filter
    }

    /// `session_impl::set_peer_class_type_filter`.
    pub fn set_peer_class_type_filter(
        &mut self,
        f: bitdaemon_net::peer_class::PeerClassTypeFilter,
    ) {
        self.peer_class_type_filter = f;
    }

    /// `session_impl::get_peer_class_type_filter`.
    #[must_use]
    pub fn peer_class_type_filter(&self) -> &bitdaemon_net::peer_class::PeerClassTypeFilter {
        &self.peer_class_type_filter
    }

    /// `session_impl::set_peer_id` — replaces the announce/handshake
    /// fingerprint (`m_peer_id`).
    pub fn set_peer_id(&mut self, id: bitdaemon_common::PeerId) {
        self.peer_id = id;
    }

    /// `session_impl::set_key` — the tracker `key` token (upstream
    /// `m_key`, regenerated when `key` setting changes).
    pub fn set_key(&mut self, key: i32) {
        self.key = key;
    }

    /// `session_impl::is_dht_running`.
    #[must_use]
    pub fn is_dht_running(&self) -> bool {
        self.dht.get_node(false).is_some() || self.dht.get_node(true).is_some()
    }

    /// `session_handle::dht_state` — serialize live DHT state.
    #[must_use]
    pub fn dht_state(&self) -> bitdaemon_dht::dht_state::DhtState {
        let mut st = bitdaemon_dht::dht_state::DhtState::default();
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node(v6) {
                let s = n.dht_state();
                st.nodes.extend(s.nodes);
                st.nodes6.extend(s.nodes6);
                st.nids.extend(s.nids);
            }
        }
        st
    }

    /// `session_handle::dht_live_nodes` — nodes that answered us
    /// recently on the given info-hash lookup. Upstream posts
    /// `dht_live_nodes_alert`; we do the same *and* return the set
    /// (the handle keeps its sync accessor).
    #[must_use]
    pub fn dht_live_nodes(&mut self, ih: &Sha1Hash) -> Vec<std::net::SocketAddr> {
        // `dht_tracker::live_nodes` — concat across node families.
        let mut nodes = Vec::new();
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node(v6) {
                nodes.extend(n.live_nodes_for(ih));
            }
        }
        self.alerts.post(Alert::DhtLiveNodes {
            ih: *ih,
            nodes: nodes.clone(),
        });
        nodes.into_iter().map(|(_, ep)| ep).collect()
    }

    /// `session_impl::add_port_mapping` — upstream returns one
    /// `port_mapping_t` per (listen socket × active mapper): NAT-PMP
    /// (transport 0) and UPnP (transport 1) each register their own
    /// handle per socket. `SessionOut::PortmapAdd` fans the request
    /// out to the engine's drivers on `tick`.
    pub fn add_port_mapping(
        &mut self,
        protocol: u8,
        external_port: u16,
        local_port: u16,
    ) -> Vec<i32> {
        let mut handles = Vec::new();
        for s in self.listen_sockets.iter() {
            for transport in [0u8, 1] {
                // transport 0 = NAT-PMP, 1 = UPnP — only active
                // mappers get a handle upstream.
                let active = (transport == 0 && self.services.natpmp)
                    || (transport == 1 && self.services.upnp);
                if !active {
                    continue;
                }
                let handle = self.next_portmap_handle;
                self.next_portmap_handle += 1;
                self.portmap_handles.insert(
                    handle,
                    PortmapTarget {
                        transport,
                        protocol,
                        local_port,
                        external_port,
                        local_addr: s.local_endpoint.ip().to_string(),
                    },
                );
                handles.push(handle);
            }
        }
        if !handles.is_empty() {
            self.pending_out.push(SessionOut::PortmapAdd {
                protocol,
                external_port,
                local_port,
            });
        }
        handles
    }

    /// `session_impl::delete_port_mapping` — resolve the handle and
    /// queue the unmap (`PortmapDelete` runs on the engine's tick).
    pub fn delete_port_mapping(&mut self, handle: i32) {
        let Some(t) = self.portmap_handles.remove(&handle) else {
            return;
        };
        self.pending_out.push(SessionOut::PortmapDelete {
            transport: t.transport,
            protocol: t.protocol,
            external_port: t.external_port,
        });
    }

    /// `on_port_mapping` — a driver confirmed a mapping
    /// (`portmap_alert` + handle registration). Called by the
    /// engine when NAT-PMP/UPnP report success.
    pub fn on_portmap_success(
        &mut self,
        transport: u8,
        protocol: u8,
        local_port: u16,
        external_port: u16,
        local_addr: String,
        // UPnP device index inside the mapper — engine bookkeeping,
        // upstream's `mapping_t` doesn't carry it.
        _device: usize,
    ) {
        // reuse an existing pending handle for this (proto, ports,
        // interface) tuple when the user registered one — upstream
        // binds the driver result back to the mapping_t it came
        // from. The reported `device` is the UPnP device index
        // inside the mapper, not the handle's socket — it can't
        // gate the match. `local_addr` narrows when known.
        let handle = self
            .portmap_handles
            .iter()
            .find(|(_, t)| {
                // an unspecified socket addr (0.0.0.0) matches any
                // reported interface — it's the "any" socket.
                let unspecified = t
                    .local_addr
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|a| a.is_unspecified());
                t.protocol == protocol
                    && t.local_port == local_port
                    && t.transport == transport
                    && (unspecified || t.local_addr.is_empty() || t.local_addr == local_addr)
            })
            .map(|(h, _)| *h)
            .unwrap_or_else(|| {
                let h = self.next_portmap_handle;
                self.next_portmap_handle += 1;
                h
            });
        self.portmap_handles.insert(
            handle,
            PortmapTarget {
                transport,
                protocol,
                local_port,
                external_port,
                local_addr: local_addr.clone(),
            },
        );
        self.alerts.post(Alert::Portmap {
            mapping: handle,
            transport,
            protocol,
            ext_port: external_port,
            local_addr,
        });
    }

    /// `portmap_error_alert` — a driver reported a mapping failure.
    pub fn on_portmap_error(
        &mut self,
        transport: u8,
        mapping: i32,
        local_addr: String,
        error: String,
    ) {
        self.alerts.post(Alert::PortmapError {
            mapping,
            transport,
            local_addr,
            error,
        });
    }

    /// `portmap_log_alert` — driver-side diagnostic line.
    pub fn portmap_log(&mut self, transport: u8, local_addr: String, msg: String) {
        self.alerts.post(Alert::PortmapLog {
            transport,
            local_addr,
            msg,
        });
    }

    /// `post_session_stats` — `session_stats_header_alert` once,
    /// then `session_stats_alert` (both category-0: unconditional).
    pub fn post_session_stats(&mut self) {
        if !self.stats_header_posted {
            self.stats_header_posted = true;
            self.alerts.post(Alert::SessionStatsHeader {});
        }
        let stats = self.session_stats();
        self.alerts.post(Alert::SessionStats { stats });
    }

    /// `listen_succeeded_alert` — the engine reports a bound
    /// listener (`socket_type`: upstream `lt::socket_type_t`).
    pub fn on_listen_succeeded(&mut self, remote: String, kind: u8) {
        self.alerts.post(Alert::ListenSucceeded { remote, kind });
    }

    /// `listen_failed_alert` — a bind/accept failure on an
    /// interface (upstream posts `op`, `error`, `socket_type`).
    pub fn on_listen_failed(&mut self, remote: String, op: String, error: String, kind: u8) {
        self.alerts.post(Alert::ListenFailed {
            remote,
            op,
            error,
            kind,
        });
    }

    /// `incoming_connection_alert` — an inbound peer stream was
    /// accepted (`socket_type` kind).
    /// `session_impl::incoming_connection` — the pre-attach gates,
    /// in upstream order: `m_paused` drop → transport enablement
    /// (`utp_disabled`/`tcp_disabled`) → `outgoing_interfaces`
    /// binding (`verify_incoming_interface` + `verify_bound_address`)
    /// → `has_incoming_connections` (counter, non-local remotes)
    /// → session `ip_filter` (skipped while `non_filter_torrents`
    /// is nonzero) → empty-torrents drop → conn-cap
    /// (`connection_limit_factor`-scaled + `connections_slack`) →
    /// no-active-torrents drop → `incoming_connections` +
    /// `incoming_connection_alert`. The engine owns the socket, so
    /// the verdict travels back: `Reject` drops it, `AcceptExceeded`
    /// sets `m_exceeded_limit` (re-evaluated at attach).
    ///
    /// `local` is the acceptor's bound endpoint (upstream reads
    /// `s.local_endpoint()`); `kind` is the listener's
    /// `socket_type` index.
    #[must_use]
    pub fn on_incoming_connection(
        &mut self,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        kind: u8,
    ) -> IncomingVerdict {
        // `m_paused` — a paused session refuses every accept before
        // any gate runs (`incoming_connection` upstream).
        if self.paused {
            return IncomingVerdict::Reject;
        }
        let is_utp = matches!(kind, 3 | 9);
        if is_utp && !self.settings.enable_incoming_utp {
            self.alerts.post(Alert::PeerBlocked {
                ih: Sha1Hash::default(),
                remote: remote.to_string(),
                reason: "utp_disabled".into(),
            });
            return IncomingVerdict::Reject;
        }
        // `enable_incoming_tcp` — plain `tcp::socket` only upstream;
        // ssl-listener/uTP kinds are their own socket types.
        if kind == 0 && !self.settings.enable_incoming_tcp {
            self.alerts.post(Alert::PeerBlocked {
                ih: Sha1Hash::default(),
                remote: remote.to_string(),
                reason: "tcp_disabled".into(),
            });
            return IncomingVerdict::Reject;
        }
        // `outgoing_interfaces` gates — the socket's local endpoint
        // must sit on an `accept_incoming` listen socket AND on an
        // allowed outgoing interface.
        if !self.settings.outgoing_interfaces.is_empty() {
            let Some(l) = local else {
                return IncomingVerdict::Reject;
            };
            let accept = self.listen_sockets.iter().any(|s| {
                s.local_endpoint.ip() == l.ip()
                    && s.flags
                        .contains(crate::listen::ListenSocketFlags::ACCEPT_INCOMING)
            });
            if !accept || !self.verify_bound_address(l.ip()) {
                self.alerts.post(Alert::PeerBlocked {
                    ih: Sha1Hash::default(),
                    remote: remote.to_string(),
                    reason: "invalid_local_interface".into(),
                });
                return IncomingVerdict::Reject;
            }
        }
        // `has_incoming_connections` — a non-local inbound proves the
        // listen port is reachable; local conns don't count upstream.
        if !bitdaemon_net::ip_helpers::is_local(&remote.ip()) {
            self.counters
                .set(crate::counters::Counter::HasIncomingConnections, 1);
        }
        // session-level `ip_filter` — skipped while any torrent opts
        // out (`non_filter_torrents` gauge; the peer may belong to it).
        if self
            .counters
            .value(crate::counters::Counter::NonFilterTorrents)
            == 0
            && self.ip_filter.access(remote.ip()) == bitdaemon_net::ip_filter::BLOCKED
        {
            self.alerts.post(Alert::PeerBlocked {
                ih: Sha1Hash::default(),
                remote: remote.to_string(),
                reason: "ip_filter".into(),
            });
            return IncomingVerdict::Reject;
        }
        // `m_torrents.empty()` — no want-on-unknown-torrent extension
        // slot exists on our side.
        if self.torrents.is_empty() {
            return IncomingVerdict::Reject;
        }
        // the conn object doesn't exist yet — upstream rolls up the
        // limit factor over a throwaway `peer_class_set`.
        let mask = self.peer_class_mask(remote.ip(), socket_type_to_filter(kind));
        let factor = self.connection_limit_factor(mask);
        let limit = i64::from(self.settings.connections_limit) * 100 / i64::from(factor);
        let n = self.num_connections();
        if n >= limit + i64::from(self.settings.connections_slack) {
            // `peer_disconnected_alert` on an invalid handle
            // (no torrent is known yet) — a zero info-hash mirrors it.
            self.alerts.post(Alert::PeerDisconnected {
                ih: Sha1Hash::default(),
                reason: format!("peer {remote}: too_many_connections"),
            });
            return IncomingVerdict::Reject;
        }
        // upstream drops silently unless a non-paused torrent exists
        // (the `want_on_unknown_torrent` extension escape doesn't
        // exist on our side — no extension advertises it).
        if !self.settings.incoming_starts_queued_torrents
            && !self.torrents.values().any(|t| !t.is_paused())
        {
            return IncomingVerdict::Reject;
        }
        self.counters
            .inc(crate::counters::Counter::IncomingConnections);
        self.alerts.post(Alert::IncomingConnection {
            remote: remote.to_string(),
            kind,
        });
        // `peer_exceeds_limit` — inside `slack` but past `limit`:
        // the conn owes an eviction once it's usable.
        if n >= limit {
            IncomingVerdict::AcceptExceeded
        } else {
            IncomingVerdict::Accept
        }
    }

    /// `session_impl::verify_bound_address` — `addr` must be one of
    /// the configured `outgoing_interfaces` (by literal IP) or sit on
    /// a device named in the list (resolved through the cached
    /// interface enumeration).
    fn verify_bound_address(&mut self, addr: std::net::IpAddr) -> bool {
        let list = String::from_utf8_lossy(&self.settings.outgoing_interfaces);
        for entry in list.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            if entry.parse::<std::net::IpAddr>().is_ok_and(|ip| ip == addr) {
                return true;
            }
        }
        // the addr isn't a listed literal — map it to a device name
        // via the interface cache and match on that.
        if self.cached_interfaces.is_empty() {
            self.cached_interfaces = bitdaemon_net::enum_net::enum_interfaces();
        }
        let Some(iface) = self
            .cached_interfaces
            .iter()
            .find(|i| i.interface_address == addr)
        else {
            return false;
        };
        let device = iface.name.clone();
        list.split(',').any(|s| s.trim() == device)
    }

    /// `session_impl::ban_ip` — insert a host rule into
    /// `m_ip_filter`, post `ip_ban_alert`, propagate to every
    /// torrent's peer list (upstream site: the DHT secret-id
    /// fishing check on unknown inbound info-hashes).
    pub fn ban_ip(&mut self, ip: std::net::IpAddr) {
        // upstream mutates the shared `m_ip_filter` then re-shares
        // it (`i->set_ip_filter(m_ip_filter)` → `ip_filter_updated`
        // sweep). An `Arc` can't mutate in place, so rebuild the
        // filter with the rule and re-share — same observable result.
        let mut f = (*self.ip_filter).clone();
        f.add_rule(ip, ip, bitdaemon_net::ip_filter::BLOCKED);
        self.alerts.post(Alert::IpBan {
            remote: ip.to_string(),
            reason: "address_filter".into(),
        });
        // `set_ip_filter` runs the full per-torrent sweep; this
        // entry point has no `out` sink (engine mid-handshake), so
        // the teardown effects drain via `pending_out` on the next
        // tick — sockets close then, conn state is already gone.
        let mut tmp = Vec::new();
        self.set_ip_filter(f, &mut tmp);
        self.pending_out.append(&mut tmp);
    }

    /// `ip_voter::cast_vote` + `external_ip_alert` when the elected
    /// address changes (upstream `session_impl` posts the alert at
    /// every cast-vote site).
    pub fn vote_external(
        &mut self,
        ip: std::net::IpAddr,
        source: bitdaemon_net::ip_voter::IpSourceFlags,
        source_ip: &std::net::IpAddr,
    ) {
        if self.ip_voter.cast_vote(ip, source, source_ip)
            && let Some(addr) = self.ip_voter.external_address()
        {
            self.alerts.post(Alert::ExternalIp { addr });
        }
    }

    /// `apply_settings_pack` — hot-swap the settings bag. Upstream
    /// re-reads everything lazily; we mirror that by replacing the
    /// struct (all reads go through `self.settings`).
    pub fn apply_settings(&mut self, s: crate::settings::SessionSettings) {
        // hot-applied knobs upstream pushes eagerly
        // (`update_alert_queue_size`, `update_disk_cache_limit`):
        // the rest is read lazily through `self.settings`.
        if let Some(d) = &self.disk {
            d.set_max_queued_bytes(s.max_queued_disk_bytes);
            // `settings_updated` — `m_file_pool.resize` lands here
            // upstream, next to the cache-size and thread pushes.
            d.set_file_pool_size(s.file_pool_size);
            d.set_write_no_cache(s.disk_io_write_mode.bypasses_os_cache());
            d.set_write_through(s.disk_io_write_mode == crate::settings::DiskIoMode::WriteThrough);
            d.set_cache_expiry(s.cache_expiry.as_secs());
            d.set_no_recheck_incomplete_resume(s.no_recheck_incomplete_resume);
            d.set_open_mode_extra(open_mode_extra(&s));
            d.set_mmap_file_size_cutoff(i64::from(s.mmap_file_size_cutoff));
            d.set_disk_write_mode(s.disk_write_mode as i32);
            d.set_no_cow(s.disk_disable_copy_on_write);
            d.set_close_file_interval(
                i64::try_from(s.close_file_interval.as_secs()).unwrap_or(i64::MAX),
            );
            // `session_impl::update_disk_threads` — `aio_threads` and
            // `hashing_threads` resize their pools live upstream.
            d.set_max_threads(s.aio_threads);
            d.set_hash_threads(s.hashing_threads);
        }
        self.alerts.set_queue_limit(s.alert_queue_size);
        // `update_alert_mask` — upstream hot-applies the persisted
        // mask to the alert manager.
        self.alerts.set_mask(s.alert_mask);
        // upstream 2.x keeps the deprecated `pe_settings` in sync
        // with the `settings_pack` encryption knobs — mirror that so
        // `m_pe_settings` readers always see the live policy.
        self.pe_settings = bitdaemon_net::pe_crypto::PeSettings {
            out_enc_policy: s.out_enc_policy,
            in_enc_policy: s.in_enc_policy,
            // `sanitize_settings`: `allowed_enc_level` outside
            // {plaintext, rc4, both} → `pe_both` upstream.
            allowed_enc_level: if matches!(s.allowed_enc_level, 1..=3) {
                s.allowed_enc_level
            } else {
                bitdaemon_net::pe_crypto::enc_level::BOTH
            },
            prefer_rc4: s.prefer_rc4,
        };
        // `update_dht`/`update_lsd`/`update_natpmp`/`update_upnp`
        // — each service starts or stops with its `enable_*` knob.
        let conns_limit_changed = s.connections_limit != self.settings.connections_limit;
        let unchoke_limit_changed = s.unchoke_slots_limit != self.settings.unchoke_slots_limit;
        // upstream `apply_pack` fires each `update_*` only when its
        // setting changed — gating keeps user-applied `set_peer_class`
        // / `set_peer_class_*_filter` overrides intact.
        let upload_rate_changed = s.upload_rate_limit != self.settings.upload_rate_limit;
        let download_rate_changed = s.download_rate_limit != self.settings.download_rate_limit;
        let local_up_changed = s.local_upload_rate_limit != self.settings.local_upload_rate_limit;
        let local_down_changed =
            s.local_download_rate_limit != self.settings.local_download_rate_limit;
        let rate_limit_utp_changed = s.rate_limit_utp != self.settings.rate_limit_utp;
        let ignore_local_changed =
            s.ignore_limits_on_local_network != self.settings.ignore_limits_on_local_network;
        let mixed_mode_changed = s.mixed_mode_algorithm != self.settings.mixed_mode_algorithm;
        self.services.dht = s.enable_dht;
        self.services.lsd = s.enable_lsd;
        self.services.natpmp = s.enable_natpmp;
        self.services.upnp = s.enable_upnp;
        // `update_listen_interfaces`/`update_outgoing_interfaces`/
        // `update_ssl_listen` — a changed bind spec re-opens sockets.
        if s.listen_interfaces != self.settings.listen_interfaces
            || s.outgoing_interfaces != self.settings.outgoing_interfaces
            || s.ssl_listen != self.settings.ssl_listen
        {
            self.reopen_sockets = true;
        }
        let privileged_now_on =
            s.no_connect_privileged_ports && !self.settings.no_connect_privileged_ports;
        self.settings = s;
        // upstream nodes read `m_settings` live — our nodes carry a
        // `DhtSettings` copy, so re-push on every apply. This is also
        // `update_dht_upload_rate_limit`'s clamp.
        self.sync_dht_settings();
        if conns_limit_changed {
            self.update_connections_limit();
        }
        if unchoke_limit_changed {
            self.update_unchoke_limit();
        }
        if upload_rate_changed {
            self.update_upload_rate();
        }
        if download_rate_changed {
            self.update_download_rate();
        }
        if local_up_changed {
            self.update_local_upload_rate();
        }
        if local_down_changed {
            self.update_local_download_rate();
        }
        if rate_limit_utp_changed {
            self.update_rate_limit_utp();
        }
        if ignore_local_changed {
            self.update_ignore_rate_limits_on_local_network();
        }
        if mixed_mode_changed || upload_rate_changed || download_rate_changed {
            self.update_mixed_mode();
        }
        if privileged_now_on {
            // `update_privileged_ports` — disconnect peers whose
            // remote port is <1024, across every torrent.
            let mut pending = Vec::new();
            for t in self.torrents.values_mut() {
                let mut eff = Vec::new();
                t.privileged_port_updated(self.session_time, &self.counters, &mut eff);
                pending.push((t.info_hash, eff));
            }
            for (ih, eff) in pending {
                self.route_into_pending(ih, eff);
            }
        }
        // cached settings each torrent consults (`m_settings` upstream).
        for t in self.torrents.values_mut() {
            t.close_redundant_connections = self.settings.close_redundant_connections;
        }
    }

    /// `session_impl::update_connections_limit` — when the cap
    /// drops below the live conn count, disconnect the excess spread
    /// evenly across torrents: `limit/n` slots each, remainder
    /// redistributed to torrents above the quotient, 4 iterations.
    /// `limit <= 0` maps to `max_open_files` upstream (effectively
    /// unbounded here — no disconnect sweep runs).
    fn update_connections_limit(&mut self) {
        let limit = self.settings.connections_limit.max(1) as usize;
        let num_conns: usize = self.torrents.values().map(|t| t.peers.len()).sum();
        if num_conns <= limit || self.torrents.is_empty() {
            return;
        }
        let n = self.torrents.len();
        let mut to_disconnect = num_conns - limit;
        let mut last_average = 0usize;
        let mut average = limit / n;
        let mut extra = limit % n;
        // 4 iterations to converge the quotient like upstream.
        for _ in 0..4 {
            let mut num_above = 0usize;
            for t in self.torrents.values() {
                let num = t.peers.len();
                if num <= last_average {
                    continue;
                }
                if num > average {
                    num_above += 1;
                }
                if num < average {
                    extra += average - num;
                }
            }
            let num_above = num_above.max(1);
            last_average = average;
            average += extra / num_above;
            if extra == 0 {
                break;
            }
            extra %= num_above;
        }
        let mut pending = Vec::new();
        for t in self.torrents.values_mut() {
            let num = t.peers.len();
            if num <= average {
                continue;
            }
            // distribute the remainder one slot at a time.
            let mut my_average = average;
            if extra > 0 {
                my_average += 1;
                extra -= 1;
            }
            let disconnect = to_disconnect.min(num - my_average);
            to_disconnect -= disconnect;
            let mut eff = Vec::new();
            t.disconnect_peers(disconnect, "too_many_connections", &mut eff);
            pending.push((t.info_hash, eff));
        }
        for (ih, eff) in pending {
            self.route_into_pending(ih, eff);
        }
    }

    /// `session_impl::set_rate_limit` — `limit <= 0` throttles to 0
    /// (our repr: 0 = unlimited; upstream stores `inf`), capped at
    /// `i32::MAX - 1`. No-op on an invalid class (`at()` → null).
    fn set_rate_limit(
        &mut self,
        c: bitdaemon_net::peer_class::PeerClassIndex,
        channel: usize,
        limit: i64,
    ) {
        let Some(pc) = self.peer_class_pool.at_mut(c) else {
            return;
        };
        let limit = if limit <= 0 {
            0
        } else {
            limit.min(i64::from(i32::MAX) - 1)
        };
        pc.channel[channel].throttle(limit);
    }

    /// `session_impl::update_upload_rate`/`update_download_rate` —
    /// the global class channels carry `upload_rate_limit` /
    /// `download_rate_limit` (negatives sanitize to 0 first).
    fn update_upload_rate(&mut self) {
        if self.settings.upload_rate_limit < 0 {
            self.settings.upload_rate_limit = 0;
        }
        let l = self.settings.upload_rate_limit;
        self.set_rate_limit(self.global_peer_class, bitdaemon_net::peer_class::UPLOAD, l);
    }

    /// `session_impl::update_download_rate`.
    fn update_download_rate(&mut self) {
        if self.settings.download_rate_limit < 0 {
            self.settings.download_rate_limit = 0;
        }
        let l = self.settings.download_rate_limit;
        self.set_rate_limit(
            self.global_peer_class,
            bitdaemon_net::peer_class::DOWNLOAD,
            l,
        );
    }

    /// `session_impl::update_local_upload_rate`/`update_local_download_rate`
    /// (deprecated upstream knobs — they still drive the `local`
    /// class channels).
    fn update_local_upload_rate(&mut self) {
        if self.settings.local_upload_rate_limit < 0 {
            self.settings.local_upload_rate_limit = 0;
        }
        let l = self.settings.local_upload_rate_limit;
        self.set_rate_limit(self.local_peer_class, bitdaemon_net::peer_class::UPLOAD, l);
    }

    /// `session_impl::update_local_download_rate`.
    fn update_local_download_rate(&mut self) {
        if self.settings.local_download_rate_limit < 0 {
            self.settings.local_download_rate_limit = 0;
        }
        let l = self.settings.local_download_rate_limit;
        self.set_rate_limit(
            self.local_peer_class,
            bitdaemon_net::peer_class::DOWNLOAD,
            l,
        );
    }

    /// `session_impl::update_rate_limit_utp` — `rate_limit_utp` off
    /// strips the `global` class from (SSL-)uTP conns so the global
    /// caps don't apply to them.
    fn update_rate_limit_utp(&mut self) {
        use bitdaemon_net::peer_class::FilterSocketType;
        let g = self.global_peer_class;
        if self.settings.rate_limit_utp {
            self.peer_class_type_filter.allow(FilterSocketType::Utp, g);
            self.peer_class_type_filter
                .allow(FilterSocketType::SslUtp, g);
        } else {
            self.peer_class_type_filter
                .disallow(FilterSocketType::Utp, g);
            self.peer_class_type_filter
                .disallow(FilterSocketType::SslUtp, g);
        }
    }

    /// `session_impl::update_ignore_rate_limits_on_local_network` —
    /// rebuilds the address→class filter with/without the `local`
    /// class for private ranges.
    fn update_ignore_rate_limits_on_local_network(&mut self) {
        let unlimited = self.settings.ignore_limits_on_local_network;
        self.init_peer_class_filter(unlimited);
    }

    /// `session_impl::copy_pertinent_channels` — append the
    /// *throttled* `BandwidthChannel`s of every class in `set`
    /// (unlimited channels don't constrain anything and are left
    /// out upstream too).
    fn copy_pertinent_channels(
        &self,
        set: &bitdaemon_net::peer_class::PeerClassSet,
        channel: usize,
        dst: &mut Vec<std::sync::Arc<bitdaemon_net::bandwidth::BandwidthChannel>>,
        cap: usize,
    ) {
        for i in 0..set.num_classes() {
            if dst.len() >= cap {
                break;
            }
            let Some(pc) = self.peer_class_pool.at(set.class_at(i)) else {
                continue;
            };
            let ch = &pc.channel[channel];
            if ch.throttle_value() == 0 {
                continue;
            }
            dst.push(std::sync::Arc::clone(ch));
        }
    }

    /// `peer_connection::get_priority` — the request's weight is the
    /// *max* `priority[channel]` over the conn's class set and the
    /// torrent's class set.
    fn request_priority(
        &self,
        conn_set: &bitdaemon_net::peer_class::PeerClassSet,
        torrent_set: &bitdaemon_net::peer_class::PeerClassSet,
        channel: usize,
    ) -> i64 {
        let mut prio = 1i64;
        for set in [conn_set, torrent_set] {
            for i in 0..set.num_classes() {
                if let Some(pc) = self.peer_class_pool.at(set.class_at(i))
                    && prio < i64::from(pc.priority[channel])
                {
                    prio = i64::from(pc.priority[channel]);
                }
            }
        }
        prio
    }

    /// `peer_connection::request_bandwidth` — asks the channel
    /// manager for `bytes` (raised to `wanted_transfer`) of quota on
    /// `channel`. Returns `None` when the conn is gone (the socket
    /// task should tear down); `Some(0)` when the request is queued
    /// (`bw_limit` set — the grant lands via
    /// `PeerQuota::assign_bandwidth`) *or* when existing quota
    /// already covers the wanted transfer; `Some(n)` = `n` bytes
    /// granted into `quota` immediately.
    pub fn request_bandwidth(
        &mut self,
        ih: &Sha1Hash,
        peer: u64,
        channel: usize,
        mut bytes: i64,
    ) -> Option<i64> {
        use crate::peer_info::BandwidthState as Bw;
        use std::sync::atomic::Ordering::Relaxed;

        let tick_ms = self.settings.tick_interval.as_millis() as i64;
        let t = self.torrents.get(ih)?;
        let c = t.peers.get(&peer)?;

        // one outstanding request per channel upstream
        if c.bw.channel_state[channel].load(Relaxed) & Bw::LIMIT.0 != 0 {
            return Some(0);
        }

        bytes = bytes.max(c.wanted_transfer(channel, bytes, tick_ms));
        let quota = c.bw.quota[channel].load(Relaxed);
        if quota >= bytes {
            return Some(0);
        }
        bytes -= quota;

        let priority = self.request_priority(&c.peer_classes, &t.peer_classes, channel);
        let handle = std::sync::Arc::clone(&c.bw);
        let mut channels = Vec::new();
        let cap = bitdaemon_net::bandwidth::MAX_BANDWIDTH_CHANNELS;
        self.copy_pertinent_channels(&c.peer_classes, channel, &mut channels, cap);
        self.copy_pertinent_channels(&t.peer_classes, channel, &mut channels, cap);

        let mgr = if channel == crate::peer_conn::DOWNLOAD_CHANNEL {
            &mut self.download_rate
        } else {
            &mut self.upload_rate
        };
        let ret = mgr.request_bandwidth(handle.clone(), bytes, priority, &channels);
        if ret == 0 {
            handle.channel_state[channel].fetch_or(Bw::LIMIT.0, Relaxed);
        } else {
            handle.quota[channel].fetch_add(ret, Relaxed);
        }
        Some(ret)
    }

    /// `session_impl::use_quota_overhead` — debit `amount_down`/
    /// `amount_up` from every channel of every class in `set`
    /// (IP/TCP overhead consumes quota too). Returns a bitmask of
    /// directions whose limit is *lower than* the overhead —
    /// upstream's `performance_alert` trigger bits. Associated fn:
    /// the per-conn tick drains while a torrent is mut-borrowed.
    pub(crate) fn quota_overhead(
        pool: &bitdaemon_net::peer_class::PeerClassPool,
        set: &bitdaemon_net::peer_class::PeerClassSet,
        amount_down: i64,
        amount_up: i64,
    ) -> u8 {
        let mut ret = 0u8;
        for i in 0..set.num_classes() {
            let Some(pc) = pool.at(set.class_at(i)) else {
                continue;
            };
            let ch = &pc.channel[crate::peer_conn::DOWNLOAD_CHANNEL];
            ch.use_quota(amount_down);
            if ch.throttle_value() > 0 && ch.throttle_value() < amount_down {
                ret |= 1 << crate::peer_conn::DOWNLOAD_CHANNEL;
            }
            let ch = &pc.channel[crate::peer_conn::UPLOAD_CHANNEL];
            ch.use_quota(amount_up);
            if ch.throttle_value() > 0 && ch.throttle_value() < amount_up {
                ret |= 1 << crate::peer_conn::UPLOAD_CHANNEL;
            }
        }
        ret
    }

    /// `session_impl::update_quotas` — one round of quota
    /// distribution on both direction managers (`dt` = elapsed
    /// milliseconds since the last pass).
    pub fn update_quotas(&mut self, dt_milliseconds: i64) {
        self.download_rate.update_quotas(dt_milliseconds);
        self.upload_rate.update_quotas(dt_milliseconds);
    }

    /// The conn's shared bandwidth handle — the engine's socket task
    /// consumes quota through it without the session lock.
    #[must_use]
    pub fn peer_quota(
        &self,
        ih: &Sha1Hash,
        peer: u64,
    ) -> Option<std::sync::Arc<crate::peer_conn::PeerQuota>> {
        self.torrents
            .get(ih)
            .and_then(|t| t.peers.get(&peer))
            .map(|c| std::sync::Arc::clone(&c.bw))
    }

    /// `session_impl`'s mixed-mode block at the top of `on_tick` —
    /// `prefer_tcp` leaves the `tcp` class unthrottled;
    /// `peer_proportional` splits the global rate between TCP and
    /// uTP conns proportional to each side's active-peer count —
    /// upstream counts conns whose `bw_network` flag is set (quota
    /// held, waiting on a socket op).
    fn update_mixed_mode(&mut self) {
        match self.settings.mixed_mode_algorithm {
            crate::settings::BandwidthMixedAlgo::PreferTcp => {
                let tcp = self.tcp_peer_class;
                self.set_rate_limit(tcp, bitdaemon_net::peer_class::UPLOAD, 0);
                self.set_rate_limit(tcp, bitdaemon_net::peer_class::DOWNLOAD, 0);
            }
            crate::settings::BandwidthMixedAlgo::PeerProportional => {
                // num_peers[protocol][channel] — protocol 0 = tcp,
                // 1 = utp; channel 0 = upload, 1 = download.
                let mut num_peers = [[0i64; 2]; 2];
                for t in self.torrents.values() {
                    for c in t.peers.values() {
                        if !c.handshake_done {
                            continue;
                        }
                        let proto = usize::from(matches!(
                            conn_socket_type(c),
                            bitdaemon_net::peer_class::FilterSocketType::Utp
                                | bitdaemon_net::peer_class::FilterSocketType::SslUtp
                        ));
                        let ch_state = |d: usize| {
                            c.bw.channel_state[d].load(std::sync::atomic::Ordering::Relaxed)
                                & crate::peer_info::BandwidthState::NETWORK.0
                                != 0
                        };
                        if c.pipeline.download_queue.len() > 1
                            && ch_state(bitdaemon_net::peer_class::DOWNLOAD)
                        {
                            num_peers[proto][bitdaemon_net::peer_class::DOWNLOAD] += 1;
                        }
                        if !c.upload_queue.is_empty() && ch_state(bitdaemon_net::peer_class::UPLOAD)
                        {
                            num_peers[proto][bitdaemon_net::peer_class::UPLOAD] += 1;
                        }
                    }
                }
                // `m_stat.upload_rate()`/`download_rate()` — the
                // session's rolling rates (sum over torrents).
                let stat_rate = [
                    self.torrents.values().map(|t| t.up_rate).sum::<i64>(),
                    self.torrents.values().map(|t| t.down_rate).sum::<i64>(),
                ];
                // never throttle below this (upstream `{5000, 30000}`).
                let lower_limit = [5000i64, 30000];
                let tcp = self.tcp_peer_class;
                for i in 0..2 {
                    let total_peers = num_peers[0][i] + num_peers[1][i];
                    if num_peers[1][i] == 0 || total_peers < 5 {
                        self.set_rate_limit(tcp, i, 0);
                    } else {
                        let tcp_peers = num_peers[0][i].max(1);
                        let limit =
                            (stat_rate[i] * tcp_peers * 4 / total_peers).max(lower_limit[i]);
                        self.set_rate_limit(tcp, i, limit);
                    }
                }
            }
        }
    }

    /// `session_impl::update_unchoke_limit` — refresh the
    /// `num_unchoke_slots` gauge, warn when the optimistic quota
    /// eats half the slots, and (fixed-slots choker only) unchoke
    /// every eligible conn when the limit is `INT_MAX`/unlimited.
    fn update_unchoke_limit(&mut self) {
        let limit = self.settings.unchoke_slots_limit;
        // upstream stores the raw value — `-1` (unlimited) shows
        // as-is on the gauge.
        self.counters
            .set(crate::counters::Counter::NumUnchokeSlots, i64::from(limit));
        // upstream has no `limit > 0` guard — an unlimited (-1)
        // setting still fires when `num_optimistic >= -1/2`.
        if i64::from(self.settings.num_optimistic_unchoke_slots) >= i64::from(limit) / 2 {
            self.alerts.post(Alert::Performance {
                warning: "too_many_optimistic_unchoke_slots".into(),
            });
        }
        if self.settings.choking_algorithm != crate::choker::ChokingAlgorithm::FixedSlots {
            return;
        }
        if limit != i32::MAX && limit >= 0 {
            return;
        }
        // unlimited — unchoke everyone eligible (upstream's sweep
        // skips disconnecting/connecting/handshake/ignore-slots
        // conns; a `choked` conn that shouldn't be choked gets an
        // immediate `unchoke_peer`).
        let now = Instant::now();
        let counters = &self.counters;
        let pool = &self.peer_class_pool;
        let mut pending = Vec::new();
        for t in self.torrents.values_mut() {
            let mut eff = Vec::new();
            let t_ign = t.ignores_unchoke_slots(pool);
            let ids: Vec<u64> = t
                .peers
                .iter()
                .filter(|(_, c)| {
                    // upstream skips `ignore_unchoke_slots()` conns
                    // (conn classes or the torrent's own).
                    !c.disconnecting
                        && !c.connecting
                        && c.choked
                        && c.handshake_done
                        && !c.ignores_unchoke_slots(pool)
                        && !t_ign
                })
                .map(|(id, _)| *id)
                .collect();
            for id in ids {
                t.unchoke_peer(id, false, counters, pool, now, &self.settings, &mut eff);
            }
            pending.push((t.info_hash, eff));
        }
        for (ih, eff) in pending {
            self.route_into_pending(ih, eff);
        }
    }

    /// `start_dht` — bind the DHT node for `bind_ip`'s family.
    /// `id`/`None` → generate (BEP-42 enforced by settings).
    /// Upstream re-seeds `m_dht_router_nodes` (persistent) then
    /// drains the queued `m_dht_nodes` through `add_node`.
    pub fn enable_dht(
        &mut self,
        bind_ip: std::net::IpAddr,
        id: Option<bitdaemon_dht::node_id::NodeId>,
        settings: bitdaemon_dht::dht_settings::DhtSettings,
    ) {
        let seed = self.dht_state_seed.clone();
        self.dht.bind(bind_ip, id, settings, &seed);
        let v6 = bind_ip.is_ipv6();
        if let Some(n) = self.dht.get_node_mut(v6) {
            for &r in &self.dht_router_nodes {
                n.table.add_router_node(r);
            }
            for &ep in &self.dht_nodes {
                n.add_node(ep);
            }
            if !self.dht_router_nodes.is_empty() && n.table.size().0 != 0 {
                let li = n.bootstrap();
                self.dht_result_map.insert((v6, li), DhtResult::Bootstrap);
            }
        }
        self.dht_nodes.clear();
    }

    /// `session_impl::get_dht_settings` — the flat `dht_*` keys
    /// packed as a `DhtSettings` struct.
    #[must_use]
    pub fn dht_settings(&self) -> bitdaemon_dht::dht_settings::DhtSettings {
        self.settings.dht_settings()
    }

    /// `session_impl::set_dht_settings` — writes the struct back
    /// into the flat `dht_*` keys and re-pushes to the live nodes
    /// (upstream's `update_dht_upload_rate_limit` clamp included).
    pub fn set_dht_settings(&mut self, s: bitdaemon_dht::dht_settings::DhtSettings) {
        self.settings.set_dht_settings(&s);
        self.sync_dht_settings();
    }

    /// Push `settings.dht_settings()` into the live DHT nodes and
    /// clamp `dht_upload_rate_limit` to `i32::MAX/3` (upstream
    /// `update_dht_upload_rate_limit`).
    fn sync_dht_settings(&mut self) {
        if self.settings.dht_upload_rate_limit > i64::from(i32::MAX) / 3 {
            self.settings.dht_upload_rate_limit = i64::from(i32::MAX) / 3;
        }
        let s = self.settings.dht_settings();
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                n.settings = s.clone();
            }
        }
    }

    /// `session_impl::external_udp_port` — set once the injected
    /// datagram socket is bound so `peer_connected` can advertise
    /// it to DHT-capable peers.
    pub fn set_dht_port(&mut self, port: u16) {
        self.dht_port = Some(port);
    }

    /// UDP datagram → dht tracker (any family) — and LSD announces
    /// (BT-SEARCH isn't a bencoded dict, so the dht drops it first).
    pub fn on_udp(&mut self, from: SocketAddr, buf: &[u8], out: &mut Vec<SessionOut>) {
        if let Some((pkt, to)) = self.dht.incoming(from, buf) {
            tracing::trace!(%from, len = buf.len(), "dht incoming handled");
            out.push(SessionOut::UdpPacket(pkt, to));
        }
        // `ip_voter::cast_vote` — the `ip` key a node echoed back is
        // its view of our external address (upstream `dht.cpp`).
        // A new consensus posts `external_ip_alert`.
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                for ip in n.drain_ip_votes() {
                    self.vote_external(
                        ip,
                        bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_DHT,
                        &from.ip(),
                    );
                }
            }
        }
        if let Some(a) = bitdaemon_net::lsd::process_packet(buf, from, self.lsd_cookie) {
            let mut lsd_found = Vec::new();
            for ih_raw in a.info_hashes {
                let Some(ih) = Sha1Hash::from_bytes(&ih_raw) else {
                    continue;
                };
                if let Some(t) = self.torrents.get_mut(&ih) {
                    if t.private {
                        continue;
                    }
                    let ep = SocketAddr::new(from.ip(), a.port);
                    let mut eff = Vec::new();
                    t.add_swarm_peer(ep, bitdaemon_common::flags::PeerSourceFlags::LSD, &mut eff);
                    for e in eff {
                        if let TorrentEffect::Alert(a) = e {
                            self.alerts.post(a);
                        }
                    }
                    lsd_found.push(ih);
                }
            }
            // `lsd_peer_alert` — one per torrent the broadcast
            // named (upstream `lsd::on_announce` → `alerts`).
            for ih in lsd_found {
                self.alerts.post(Alert::LsdPeer {
                    ih,
                    remote: SocketAddr::new(from.ip(), a.port).to_string(),
                });
            }
        }
        let _ = out;
    }

    /// `lsd::announce` cadence — every 5 min per non-private
    /// torrent, one datagram per multicast group.
    /// `session_impl::on_lsd_announce` — upstream's dedicated
    /// announce timer fires one broadcast per
    /// `local_service_announce_interval / num_torrents` ms (≥1 ms),
    /// prioritized torrents (`m_lsd_torrents`, from
    /// `prioritize_lsd` at `start_announcing`) first, else
    /// round-robin `m_next_lsd_torrent`. Our 1 s tick models the
    /// timer with a millisecond budget.
    pub fn lsd_tick(&mut self, _now: Instant, out: &mut Vec<SessionOut>) {
        // anonymous sessions must not broadcast info-hashes to the LAN
        if self.settings.anonymous_mode || !self.services.lsd {
            self.lsd_queue.clear();
            return;
        }
        // prioritized announces drain first.
        while let Some(ih) = self.lsd_queue.pop_front() {
            self.lsd_announce_one(ih, out);
        }
        let n_torrents = self.torrents.len().max(1) as i64;
        let cost =
            (self.settings.local_service_announce_interval.as_millis() as i64 / n_torrents).max(1);
        self.lsd_budget_ms += 1000;
        if self.lsd_budget_ms < cost || self.torrents.is_empty() {
            return;
        }
        let mut ihs: Vec<Sha1Hash> = self.torrents.keys().copied().collect();
        ihs.sort_unstable_by_key(|h| *h.as_bytes());
        let len = ihs.len();
        let mut steps = 0;
        while self.lsd_budget_ms >= cost && steps < len {
            self.lsd_budget_ms -= cost;
            let ih = ihs[self.lsd_rr_cursor % len];
            self.lsd_rr_cursor = (self.lsd_rr_cursor + 1) % len;
            steps += 1;
            self.lsd_announce_one(ih, out);
        }
        self.lsd_rr_cursor %= len;
    }

    /// `torrent::lsd_announce` — eligibility + one broadcast per
    /// address family. Ineligible torrents still consume their
    /// pacing slot upstream (the round-robin cursor advances past
    /// them).
    fn lsd_announce_one(&mut self, ih: Sha1Hash, out: &mut Vec<SessionOut>) {
        let Some(t) = self.torrents.get_mut(&ih) else {
            return;
        };
        let i2p = t.ti.as_ref().is_some_and(|ti| ti.is_i2p());
        if t.private
            || !t.announce_lsd
            || !t.enable_lsd
            || t.is_paused()
            || (!t.files_checked && t.ti.is_some())
            || (i2p && !self.settings.allow_i2p_mixed)
        {
            return;
        }
        t.lsd_announcing = true;
        let hex = bitdaemon_common::hex::to_hex(ih.as_bytes());
        for v6 in [false, true] {
            let pkt =
                bitdaemon_net::lsd::render_lsd_packet(self.listen_port, &hex, self.lsd_cookie, v6);
            let to = if v6 {
                SocketAddr::new(bitdaemon_net::lsd::LSD_ADDR6.into(), 6771)
            } else {
                SocketAddr::new(bitdaemon_net::lsd::LSD_ADDR4.into(), 6771)
            };
            out.push(SessionOut::UdpPacket(pkt, to));
        }
    }

    /// `tick` — drive every torrent's peers + dht timeouts.
    pub fn tick(&mut self, out: &mut Vec<SessionOut>) -> std::time::Duration {
        self.session_time += 1;
        // upstream `session_time()` is a u16-seconds epoch — near
        // the 65536 s wrap (18.2 h) `m_created` steps forward 4 h
        // and every peer's `last_connected`/`last_optimistically_
        // unchoked` clamps back the same amount; stepping our
        // counter is the identical transform.
        if self.session_time > 65000 {
            self.session_time -= 4 * 60 * 60;
            for t in self.torrents.values_mut() {
                t.peer_list.step_session_time(4 * 60 * 60);
            }
        }
        // upstream `on_tick`'s top block — `mixed_mode_algorithm`
        // rebalances the `tcp` class throttle against live uTP share.
        self.update_mixed_mode();
        // `session_impl::on_tick` — `m_auto_manage_time_scaler--`
        // (skipped while the session is paused); at <0 the queue
        // runs and the scaler re-arms to `auto_manage_interval`.
        if !self.paused {
            self.auto_manage_scaler -= 1;
        }
        if self.auto_manage_scaler < 0 {
            self.auto_manage_scaler = self.settings.auto_manage_interval;
            self.manage_queue(out);
        }
        self.drain_disk_completed(out);
        // `disk_io_thread_pool::m_idle_timer` — reap threads idle for
        // the whole 60 s interval (verbatim cadence). Upstream's timer
        // starts with the first pool thread and cancels on empty; a
        // tick-driven call is a no-op on an empty pool.
        if let Some(d) = &self.disk {
            let now = Instant::now();
            if self
                .last_disk_reap
                .is_none_or(|t| now.duration_since(t) >= Duration::from_secs(60))
            {
                self.last_disk_reap = Some(now);
                d.reap_idle_threads();
            }
        }
        // `session_impl` ip notifier — interface changes re-open
        // listen sockets upstream; here the driver owns sockets so
        // we surface the event. `update_ip_notifier` gates polling
        // on `enable_ip_notifier` (upstream stops the worker).
        if (self.settings.enable_ip_notifier && self.ip_notifier.poll())
            || std::mem::take(&mut self.reopen_sockets)
        {
            out.push(SessionOut::InterfacesChanged);
        }
        let wait = self.dht.tick();
        self.dht_announce_tick();
        self.dht_drain_events();
        // `dht_logger` outgoing direction — every datagram we send
        // is a `dht_pkt_alert` when the category is enabled.
        let post_dht_pkt = self.alerts.should_post(AlertCategory::DHT_LOG);
        while let Some((pkt, to)) = self.dht.poll() {
            if post_dht_pkt {
                self.alerts.post(Alert::DhtPacket {
                    direction: 1,
                    remote: to.to_string(),
                    msg: pkt.clone(),
                });
            }
            out.push(SessionOut::UdpPacket(pkt, to));
        }
        let now = Instant::now();
        // `m_stat.second_tick` for every torrent — upstream ticks
        // stats only for `want_tick()` members, but our `want_tick`
        // is a computed predicate that itself reads the faded
        // rates, so the stat tick runs unconditionally (a no-op
        // for unlisted torrents: no conns, rates already zero).
        // `update_rates` also carries the upstream `state_updated()`
        // on nonzero low-pass rate + `need_save_resume` on counter
        // movement — both gated on nonzero values inside.
        for t in self.torrents.values_mut() {
            let mut eff = Vec::new();
            t.update_rates(now, &mut eff);
            for e in eff {
                if let crate::torrent::TorrentEffect::Alert(a) = e {
                    self.alerts.post(a);
                }
            }
        }
        self.lsd_tick(now, out);
        // upstream `torrent_want_tick` list — the heavy per-torrent
        // work (pex, requests, timeouts, web seeds) runs for every
        // torrent `want_tick()` admits; `want_peers` is a *dialing*
        // predicate and does not gate ticking.
        let ihs: Vec<Sha1Hash> = self
            .torrents
            .iter()
            .filter(|(_, t)| t.want_tick())
            .map(|(ih, _)| *ih)
            .collect();
        let web_seed_blocks = self.settings.web_seed_blocks_per_tick;
        // `m_ses.num_connections()` — feeds `timed_out_no_interest`
        // and the web-seed `connections_limit` gate; in-flight web
        // fetches count as connections upstream
        // (`peer_info.connection`).
        let session_conns: i64 = self
            .torrents
            .values()
            .map(|t| t.peers.len() as i64 + t.web_seed_inflight() as i64)
            .sum();
        // `disk_observer` gate — stop issuing block requests while
        // the write queue sits above `max_queued_disk_bytes`.
        let disk_bp = self.disk.as_ref().is_some_and(|d| d.write_queue_exceeded());
        // `performance_alert` — entering disk back-pressure (upstream
        // posts `send_buffer_watermark`/`write_discard` sites; the
        // queue-overrun transition is the observable equivalent).
        if disk_bp && !self.prev_disk_bp {
            self.alerts.post(Alert::Performance {
                warning: "disk write queue exceeded max_queued_disk_bytes".into(),
            });
        }
        self.prev_disk_bp = disk_bp;
        // upstream `handle_disk_error` — a failed cache flush is a
        // file_error_alert per occurrence; the buffers were restored
        // to the cache and retry at the next flush opportunity.
        let flush_errs = self.disk.as_ref().map_or(0, |d| d.flush_error_count());
        if flush_errs > self.prev_flush_errors {
            self.alerts.post(Alert::FileError {
                ih: Sha1Hash::default(),
                path: String::new(),
                error: format!(
                    "disk flush failed (x{})",
                    flush_errs - self.prev_flush_errors
                ),
            });
        }
        self.prev_flush_errors = flush_errs;
        let post_stats = self.alerts.should_post(AlertCategory::STATS);
        let dont_count_slow = self.settings.dont_count_slow_torrents;
        let startup_delay = self.settings.auto_manage_startup;
        for ih in ihs {
            let mut eff = Vec::new();
            if let Some(t) = self.torrents.get_mut(&ih) {
                // `second_tick` head — `torrent_plugin::tick` for
                // listed torrents only (the `m_abort` return that
                // follows upstream is unreachable here — `want_tick`
                // already excludes aborted torrents).
                for ext in &t.extensions {
                    ext.tick();
                }
                // an auto-managed torrent in upload mode retries the
                // disk after `optimistic_disk_retry` hoping the error
                // cleared — upstream runs this before the paused
                // early-return.
                if t.upload_mode
                    && t.auto_managed
                    && t.upload_mode_time.is_some_and(|t0| {
                        now.duration_since(t0) >= self.settings.optimistic_disk_retry
                    })
                {
                    t.set_upload_mode(false, &self.settings, &self.counters, &mut eff);
                }
                // upstream `second_tick` early return:
                // `is_paused() && !m_graceful_pause_mode` → stats-only
                // tick (`update_rates` above already did it) — no
                // conn work, web seeds, time-critical picks, stats
                // alerts or inactivity debounce.
                if !(t.is_paused() && !t.graceful_pause) {
                    t.disk_back_pressure = disk_bp;
                    t.metadata_tick(&mut eff);
                    t.pex_tick(Instant::now(), &mut eff);
                    // `request_time_critical_pieces` — deadline pieces
                    // re-drive every tick while not in upload mode.
                    if !t.upload_mode {
                        t.request_time_critical_pieces(&self.settings, &self.counters, &mut eff);
                    }
                    for id in t.peers.keys().copied().collect::<Vec<_>>() {
                        // `peer_connection::second_tick` early-outs a
                        // conn mid-teardown (`m_disconnecting` →
                        // disconnect, return).
                        if t.peers.get(&id).is_some_and(|c| c.disconnecting) {
                            continue;
                        }
                        // `peer_plugin::tick` fires inside the conn's
                        // `second_tick` upstream.
                        if let Some(c) = t.peers.get_mut(&id) {
                            for p in &c.plugins {
                                p.on_tick();
                            }
                        }
                        t.drive_peer(id, &self.settings, &self.counters, &mut eff);
                        // `peer_connection::second_tick` tail —
                        // `fill_send_buffer` admits queued requests once
                        // the send buffer drained (covers stalls where
                        // no `on_sent` fired, e.g. slow sockets).
                        t.fill_send_buffer(id, &self.settings, &self.counters, &mut eff);
                    }
                    // `peer_connection::second_tick` — the disconnect
                    // ladder (inactivity / no-handshake / no-request /
                    // no-interest), keepalives, then snub + refund of
                    // blocks stuck past `request_timeout`.
                    t.check_request_timeouts(
                        &self.settings,
                        session_conns,
                        &self.counters,
                        &self.peer_class_pool,
                        self.alerts.mask(),
                        &mut eff,
                    );
                    // `web_connection_base::timeout()` — expired
                    // web-seed batches release their blocks and
                    // reschedule the seed (`urlseed_wait_retry`).
                    t.check_web_seed_timeouts(&self.settings, &mut eff);
                    // `peer_connection::second_tick` super-seed
                    // recovery — peers uninterested >10 s get a fresh
                    // leaked piece.
                    t.super_seed_second_tick(&mut eff);
                    // `second_tick` tail — `is_inactive` debounce:
                    // →inactive waits `auto_manage_startup`, →active
                    // flips immediately; a flip re-arms auto-manage.
                    if dont_count_slow {
                        let inactive = t.is_inactive_internal(&self.settings);
                        if inactive == t.inactive {
                            t.pending_active_change = false;
                            t.inactivity_deadline = None;
                        } else if !t.pending_active_change {
                            t.inactivity_deadline = if inactive {
                                Some(now + startup_delay)
                            } else {
                                Some(now)
                            };
                            t.pending_active_change = true;
                        }
                        if t.pending_active_change
                            && t.inactivity_deadline.is_some_and(|d| now >= d)
                        {
                            t.pending_active_change = false;
                            t.inactivity_deadline = None;
                            if inactive != t.inactive {
                                t.inactive = inactive;
                                t.state_updated();
                                self.auto_manage_scaler = 0;
                            }
                        }
                    }
                    // `stats_alert` rides the per-second counters
                    // refresh (category-gated upstream the same way).
                    if post_stats {
                        self.alerts.post(Alert::Stats {
                            ih,
                            interval_ms: 1000,
                            counters: vec![
                                t.up_rate,
                                t.down_rate,
                                t.total_upload(),
                                t.total_download(),
                            ],
                        });
                    }
                    // `maybe_connect_web_seeds` — pull-model: the
                    // block pick hands work to the engine's fetch
                    // task instead of keeping a persistent conn.
                    if let Some((url, reqs)) = t.next_web_seed_blocks(
                        web_seed_blocks,
                        &self.settings,
                        session_conns,
                        &self.counters,
                    ) && let Some(fs) = t.files.clone()
                    {
                        let single_file = fs.num_files() == 1;
                        out.push(SessionOut::WebSeedFetch {
                            ih,
                            url,
                            files: fs,
                            piece_length: t.piece_length,
                            single_file,
                            reqs,
                        });
                    }
                }
            }
            self.route(ih, eff, out);
        }
        // `session_impl::on_tick` — session-wide IP-overhead check:
        // the per-tick `stat_channel::counter()` (delta since last
        // tick) meeting the *global class* limit posts
        // `download`/`upload_limit_too_low`. The per-conn/torrent
        // legs run inside `check_request_timeouts`; this one is the
        // global channel the class channels can't see per-conn.
        if self.settings.rate_limit_ip_overhead {
            // indexed by `UPLOAD_CHANNEL`/`DOWNLOAD_CHANNEL` (0/1).
            let down_now = self
                .counters
                .value(crate::counters::Counter::RecvIpOverheadBytes);
            let up_now = self
                .counters
                .value(crate::counters::Counter::SentIpOverheadBytes);
            let down_oh = down_now - self.prev_ip_overhead[crate::peer_conn::DOWNLOAD_CHANNEL];
            let up_oh = up_now - self.prev_ip_overhead[crate::peer_conn::UPLOAD_CHANNEL];
            self.prev_ip_overhead[crate::peer_conn::UPLOAD_CHANNEL] = up_now;
            self.prev_ip_overhead[crate::peer_conn::DOWNLOAD_CHANNEL] = down_now;
            if self.alerts.should_post(AlertCategory::PERFORMANCE)
                && let Some(pc) = self.peer_class_pool.at(self.global_peer_class)
            {
                let down_limit = pc.channel[crate::peer_conn::DOWNLOAD_CHANNEL].throttle_value();
                let up_limit = pc.channel[crate::peer_conn::UPLOAD_CHANNEL].throttle_value();
                if down_limit > 0 && down_oh >= down_limit {
                    self.alerts.post(Alert::Performance {
                        warning: "download_limit_too_low".into(),
                    });
                }
                if up_limit > 0 && up_oh >= up_limit {
                    self.alerts.post(Alert::Performance {
                        warning: "upload_limit_too_low".into(),
                    });
                }
            }
        }
        // `session_impl::on_tick` — auto-scrape paused auto-managed
        // torrents (skipped while the session is paused): the scaler
        // re-arms to `auto_scrape_interval / want_scrape.len()` with
        // a floor of `auto_scrape_min_interval`, then scrapes the
        // next round-robin member.
        if !self.paused {
            self.auto_scrape_time_scaler -= 1;
            if self.auto_scrape_time_scaler <= 0 {
                let want_scrape: Vec<Sha1Hash> = self
                    .torrents
                    .iter()
                    .filter(|(_, t)| t.is_paused() && t.auto_managed && !t.aborted)
                    .map(|(ih, _)| *ih)
                    .collect();
                self.auto_scrape_time_scaler = (self.settings.auto_scrape_interval.as_secs()
                    as i32)
                    / want_scrape.len().max(1) as i32;
                let min = self.settings.auto_scrape_min_interval.as_secs() as i32;
                if self.auto_scrape_time_scaler < min {
                    self.auto_scrape_time_scaler = min;
                }
                if !want_scrape.is_empty() {
                    if self.next_scrape_torrent >= want_scrape.len() {
                        self.next_scrape_torrent = 0;
                    }
                    let ih = want_scrape[self.next_scrape_torrent];
                    // `scrape_tracker(-1, false)` — automatic, not
                    // user-triggered.
                    self.scrape_tracker(&ih, -1, out);
                    self.next_scrape_torrent = (self.next_scrape_torrent + 1) % want_scrape.len();
                }
            }
        }
        // `session_impl::on_tick` — `try_connect_more_peers` sits
        // here in upstream order (after scrape, before the unchoke
        // scalers); `ConnectPeer` effects ride `out` and the engine
        // dispatches the dials.
        let stime = self.session_time;
        for (ih, ep, pid) in self.try_connect_more_peers(stime, None) {
            // `peer_connect_alert` — upstream posts it at dial for
            // outgoing conns.
            self.alerts.post(Alert::PeerConnect {
                ih,
                remote: ep.to_string(),
            });
            out.push(SessionOut::ConnectPeer {
                ih,
                ep,
                our_pid: pid,
            });
        }
        self.unchoke_tick(out);
        // `m_disconnect_time_scaler` → `peer_turnover`: at
        // `connections_limit > 5`, either the swarm is at
        // `peer_turnover_cutoff`% of the global limit (disconnect
        // `peer_turnover`% of the largest torrent's peers) or any
        // torrent sits above its own `max_connections` cutoff.
        self.disconnect_time_scaler -= 1;
        if self.disconnect_time_scaler <= 0 {
            self.disconnect_time_scaler =
                self.settings.peer_turnover_interval.as_secs().max(1) as i32;
            if self.settings.connections_limit > 5 {
                let limit = i64::from(self.settings.connections_limit.min(i32::MAX / 100));
                let cutoff = i64::from(self.settings.peer_turnover_cutoff.min(100));
                if self.num_connections() >= limit * cutoff / 100 && !self.torrents.is_empty() {
                    // disconnect the worst peers of the largest
                    // swarm every `peer_turnover_interval` —
                    // `num_peers()` is the *peer-list* size upstream,
                    // not the live-conn count.
                    if let Some(t) = self
                        .torrents
                        .values_mut()
                        .max_by_key(|t| t.peer_list.num_peers())
                    {
                        let n = (t.peer_list.num_peers() as i64
                            * i64::from(self.settings.peer_turnover)
                            / 100)
                            .max(1)
                            .min(t.peer_list.num_connect_candidates() as i64)
                            as usize;
                        let mut eff = Vec::new();
                        t.disconnect_peers(n, "optimistic_disconnect", &mut eff);
                        let ih = t.info_hash;
                        self.route(ih, eff, out);
                    }
                } else {
                    // below the global cap — enforce per-torrent
                    // `max_connections` cutoffs instead (same
                    // peer-list sizing as the global leg).
                    let ihs: Vec<Sha1Hash> = self.torrents.keys().copied().collect();
                    for ih in ihs {
                        let Some(t) = self.torrents.get_mut(&ih) else {
                            continue;
                        };
                        let max = i64::from(t.max_connections.min(i32::MAX / 100));
                        if (t.peer_list.num_peers() as i64) < max * cutoff / 100 || max < 6 {
                            continue;
                        }
                        let n = (t.peer_list.num_peers() as i64
                            * i64::from(self.settings.peer_turnover)
                            / 100)
                            .max(1)
                            .min(t.peer_list.num_connect_candidates() as i64)
                            as usize;
                        let mut eff = Vec::new();
                        t.disconnect_peers(n, "optimistic_disconnect", &mut eff);
                        self.route(ih, eff, out);
                    }
                }
            }
        }
        self.resume_save_tick(now);
        // `SessionOut` produced outside the actor loop (portmap
        // registrations) is emitted here — `effects` already went
        // out via `route`.
        out.append(&mut self.pending_out);
        // `state_changed_alert` posts at the transition point now
        // (`TorrentEffect::StateChangedEdge` — upstream `set_state`
        // emits inside the call, one alert per transition); the
        // end-of-tick diff coalesced intermediate steps and is gone.
        wait
    }

    /// `torrent::dht_announce` pacing — upstream's
    /// `on_dht_announce` timer announces ONE torrent per
    /// `dht_announce_interval / num_torrents` ms (round-robin via
    /// `m_next_dht_torrent`), except `prioritize_dht`'s queue which
    /// caps the wait at 4s. Our 1s tick models the same pacing with
    /// a millisecond budget.
    fn dht_announce_tick(&mut self) {
        if !self.settings.enable_dht || !self.services.dht || self.settings.anonymous_mode {
            self.dht_queue.clear();
            return;
        }
        // upstream announces on a *running* DHT — the traversal
        // seeds itself from `router_nodes` when the table is empty
        // (`find_data::start` mixes both), so an empty routing table
        // must not gate the announce. What *does* gate it is having
        // nothing to seed from at all: a node with neither live
        // entries nor router nodes would finish the traversal
        // instantly, so we hold the announce until the first router
        // node lands (upstream resolves `dht_bootstrap_nodes`
        // before torrents are added, so its announces always see
        // seeded traversals).
        let seeded = [false, true].iter().any(|v6| {
            self.dht
                .get_node(*v6)
                .is_some_and(|n| n.table.size().0 > 0 || n.table.router_nodes().next().is_some())
        });
        if !seeded {
            return;
        }
        // prioritized torrents first — upstream caps their wait at
        // 4s; our 1s tick is already inside that bound.
        while let Some(ih) = self.dht_queue.pop_front() {
            if self.dht_eligible(&ih) {
                self.dht_announce_one(ih);
            }
        }
        // pacing budget: `dht_announce_interval / N` ms per
        // announce — each 1s tick buys that many slots.
        let n_torrents = self.torrents.len().max(1) as i64;
        let interval_ms = self.settings.dht_announce_interval.as_millis() as i64;
        let cost = (interval_ms / n_torrents).max(1);
        self.dht_announce_budget_ms += 1000;
        if self.dht_announce_budget_ms < cost || self.torrents.is_empty() {
            return;
        }
        // round-robin cursor over insertion-sorted info-hashes —
        // upstream walks `m_torrents` by index with wrap.
        let mut ihs: Vec<Sha1Hash> = self.torrents.keys().copied().collect();
        ihs.sort_unstable_by_key(|h| *h.as_bytes());
        let len = ihs.len();
        // a skipped (ineligible) torrent still consumes its slot
        // upstream — `dht_announce` gates inside.
        let mut steps = 0;
        while self.dht_announce_budget_ms >= cost && steps < len {
            self.dht_announce_budget_ms -= cost;
            let ih = ihs[self.dht_rr_cursor % len];
            self.dht_rr_cursor = (self.dht_rr_cursor + 1) % len;
            steps += 1;
            if self.dht_eligible(&ih) {
                self.dht_announce_one(ih);
            }
        }
        self.dht_rr_cursor %= len;
    }

    /// `torrent::should_announce_dht` — the eligibility gate the
    /// announce timer applies per torrent.
    fn dht_eligible(&self, ih: &Sha1Hash) -> bool {
        let Some(t) = self.torrents.get(ih) else {
            return false;
        };
        let i2p = t.ti.as_ref().is_some_and(|ti| ti.is_i2p());
        let fallback_blocked = self.settings.use_dht_as_fallback
            && !t.trackers.trackers.is_empty()
            && t.trackers.trackers.iter().any(|e| e.verified);
        !t.is_paused()
            && t.enable_dht
            && t.files_checked
            && !t.private
            && t.announce_dht
            && !(i2p && !self.settings.allow_i2p_mixed)
            && !fallback_blocked
    }

    /// `session_impl::prioritize_dht` — a torrent that wants peers
    /// (`num_peers < 50` in `start_announcing`) jumps the announce
    /// queue; the tick drains it ahead of the round-robin.
    fn prioritize_dht(&mut self, ih: Sha1Hash) {
        self.dht_queue.push_back(ih);
    }

    /// `dht::announce` for one torrent — starts the `get_peers`
    /// traversal on every bound node family and registers the
    /// `Announce` sink for reply routing. Upstream announces
    /// `settings::announce_port` (0 → implied/listen port), marks
    /// `seed` for scrape stats, and `implied_port` when the port
    /// isn't forced and incoming uTP is allowed.
    fn dht_announce_one(&mut self, ih: Sha1Hash) {
        self.dht_last_announce.insert(ih, self.session_time);
        let (seed, ssl) = self
            .torrents
            .get_mut(&ih)
            .map(|t| {
                t.dht_announcing = true;
                (t.is_seed(), t.is_ssl_torrent())
            })
            .unwrap_or((false, false));
        let port = u16::try_from(self.settings.announce_port).unwrap_or(0);
        // ssl torrents can't use implied_port (DHT is non-ssl).
        let implied_port = port == 0 && !ssl && self.settings.enable_incoming_utp;
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                tracing::debug!(%ih, v6, nodes = n.table.size().0, "dht get_peers announce");
                let li = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetPeers, ih);
                self.dht_result_map.insert(
                    (v6, li),
                    DhtResult::Announce {
                        ih,
                        port,
                        seed,
                        implied_port,
                    },
                );
            }
        }
    }

    /// `session_impl::dht_get_peers` — user query: replies with
    /// peers post `dht_get_peers_reply_alert` (upstream binds
    /// `on_dht_get_peers` — no `peer_list` feeding). Runs on every
    /// bound node family (`dht_tracker` iterates `m_nodes`).
    pub fn dht_get_peers(&mut self, ih: Sha1Hash) {
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                let li = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetPeers, ih);
                self.dht_result_map
                    .insert((v6, li), DhtResult::GetPeers(ih));
            }
        }
    }

    /// `session_impl::dht_announce` — user-level announce: same
    /// traversal as `dht_get_peers`, plus `announce_peer` to every
    /// token-holder on completion (upstream `dht_announce` binds
    /// `on_dht_get_peers` too — replies post `DhtGetPeersReply`).
    pub fn dht_announce(&mut self, ih: Sha1Hash, port: u16, flags: DhtAnnounceFlags) {
        self.dht_last_announce.insert(ih, self.session_time);
        let (seed, implied_port) = (
            flags.contains(DhtAnnounceFlags::SEED),
            flags.contains(DhtAnnounceFlags::IMPLIED_PORT),
        );
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                let li = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetPeers, ih);
                self.dht_result_map.insert(
                    (v6, li),
                    DhtResult::UserAnnounce {
                        ih,
                        port,
                        seed,
                        implied_port,
                    },
                );
            }
        }
    }

    /// `session_impl::dht_get_immutable_item` — `get` traversal on
    /// every bound family; the first `r.v` reply posts
    /// `dht_immutable_item_alert` once (upstream
    /// `get_immutable_item_ctx` — empty gets still post once on
    /// the last `Done`).
    pub fn dht_get_item(&mut self, target: Sha1Hash) {
        let ctx = self.next_dht_ctx;
        self.next_dht_ctx += 1;
        let mut active = 0usize;
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                let li = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetItem, target);
                self.dht_result_map
                    .insert((v6, li), DhtResult::GetItem { target, ctx });
                active += 1;
            }
        }
        if active > 0 {
            self.dht_item_ctxs.insert(
                ctx,
                DhtItemCtx {
                    active,
                    posted: false,
                    best_seq: -1,
                },
            );
        }
    }

    /// `session_impl::dht_get_mutable_item` — `get` on
    /// `sha1(pk ‖ salt)` across bound families; replies carrying
    /// `k`/`sig`/`seq`/`v` post `dht_mutable_item_alert` on every
    /// better `seq`, `authoritative` only on the last `Done`
    /// (upstream `get_mutable_item_ctx`).
    pub fn dht_get_item_mut(&mut self, pk: [u8; 32], salt: Vec<u8>) {
        let target = bitdaemon_dht::item::item_target_id_mutable(&salt, &pk);
        let ctx = self.next_dht_ctx;
        self.next_dht_ctx += 1;
        let mut active = 0usize;
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                let li = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetItem, target);
                self.dht_result_map.insert(
                    (v6, li),
                    DhtResult::GetItemMutable {
                        target,
                        pk,
                        salt: salt.clone(),
                        ctx,
                        best: None,
                    },
                );
                active += 1;
            }
        }
        if active > 0 {
            self.dht_item_ctxs.insert(
                ctx,
                DhtItemCtx {
                    active,
                    posted: false,
                    best_seq: -1,
                },
            );
        }
    }

    /// `session_impl::dht_sample_infohashes` — BEP-51 *direct*
    /// request to one node (upstream `dht_tracker::sample_infohashes`
    /// is not a traversal). The reply lands via `DhtEvent::Direct`.
    pub fn dht_sample_infohashes(&mut self, ep: SocketAddr, target: Sha1Hash) {
        let v6 = ep.is_ipv6();
        if let Some(n) = self.dht.get_node_mut(v6) {
            let tag = n.sample_infohashes(ep, target);
            self.dht_direct_map.insert(tag, DhtDirectSink::Sample);
        }
    }

    /// `session_impl::dht_direct_request` — send an arbitrary query
    /// to `ep`; the reply posts `dht_direct_response_alert` with
    /// `user_data` echoed (upstream `userdata`/`client_data_t`).
    pub fn dht_direct_request(
        &mut self,
        ep: SocketAddr,
        e: bitdaemon_bencode::Entry,
        user_data: u64,
    ) {
        let v6 = ep.is_ipv6();
        if let Some(n) = self.dht.get_node_mut(v6) {
            let tag = n.direct_request(e, ep);
            self.dht_direct_map
                .insert(tag, DhtDirectSink::User(user_data));
        }
    }

    /// `session_handle::dht_put_item` (immutable overload) — BEP-44:
    /// `target = sha1(value)`, run a `get` lookup per family, then
    /// `put` to the token-holding responders when each completes
    /// (`put_data`). `dht_put_alert` fires once with the summed
    /// `num_success` (upstream `put_item_ctx`). Returns the item
    /// target key.
    pub fn dht_put_item(&mut self, value: &[u8]) -> Sha1Hash {
        let target = bitdaemon_dht::item::item_target_id_immutable(value);
        let ctx = self.next_dht_ctx;
        self.next_dht_ctx += 1;
        let mut families = 0usize;
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                let idx = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetItem, target);
                self.dht_result_map.insert(
                    (v6, idx),
                    DhtResult::Put {
                        pending: PendingPut::Immutable {
                            value: value.to_vec(),
                        },
                        ctx,
                    },
                );
                families += 1;
            }
        }
        if families > 0 {
            self.dht_put_ctxs.insert(
                ctx,
                DhtPutCtx {
                    active: families,
                    responses: 0,
                    pending: DhtPutAlertFields::Immutable { target },
                },
            );
        }
        target
    }

    /// `session_impl::dht_put_mutable_item` — BEP-44 signed put:
    /// signs `value` under `sk` with `seq`/`salt`, runs the `get`
    /// for tokens, then `put`s `{v,k,seq,sig[,salt]}` and posts
    /// `dht_put_alert` once (summed `num_success`). Returns the
    /// item target.
    pub fn dht_put_item_mut(
        &mut self,
        pk: [u8; 32],
        sk: &[u8; 64],
        salt: Vec<u8>,
        seq: i64,
        value: &[u8],
    ) -> Option<Sha1Hash> {
        let sig = bitdaemon_dht::item::sign_mutable_item(value, &salt, seq, sk)?;
        let target = bitdaemon_dht::item::item_target_id_mutable(&salt, &pk);
        let ctx = self.next_dht_ctx;
        self.next_dht_ctx += 1;
        let mut families = 0usize;
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                let idx = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetItem, target);
                self.dht_result_map.insert(
                    (v6, idx),
                    DhtResult::Put {
                        pending: PendingPut::Mutable {
                            value: value.to_vec(),
                            pk,
                            sig,
                            seq,
                            salt: salt.clone(),
                        },
                        ctx,
                    },
                );
                families += 1;
            }
        }
        if families > 0 {
            self.dht_put_ctxs.insert(
                ctx,
                DhtPutCtx {
                    active: families,
                    responses: 0,
                    pending: DhtPutAlertFields::Mutable {
                        target,
                        pk,
                        sig,
                        seq,
                        salt,
                    },
                },
            );
        }
        Some(target)
    }

    /// `force_dht_announce` — user-triggered, ignores the interval
    /// (private torrents still never touch the DHT).
    pub fn force_dht_announce(&mut self, ih: &Sha1Hash) {
        if !self.settings.enable_dht
            || !self.services.dht
            || self.settings.anonymous_mode
            || self.torrents.get(ih).is_none_or(|t| t.private)
        {
            return;
        }
        self.dht_announce_one(*ih);
    }

    /// `force_reannounce` — emit `TrackerAnnounce` now (event
    /// `none` = periodic refresh, like upstream's reannounce).
    /// `tracker_idx` selects one entry; `None` = all trackers.
    /// `force_tracker_request` — upstream *schedules* a manual
    /// announce (`trigger_announce`: `next_announce =
    /// max(t, min_announce) + 1s`, `triggered_manually`) and lets
    /// the tracker timer fire it; a paused torrent ignores the
    /// request entirely. `seconds` is the `force_reannounce`
    /// delay; the dispatch runs through `tracker_announce_due`
    /// like every other announce so the manual flag rides the
    /// request into its reply/error alerts.
    pub fn force_announce(
        &mut self,
        ih: &Sha1Hash,
        tracker_idx: Option<usize>,
        seconds: i64,
        flags: ReannounceFlags,
        out: &mut Vec<SessionOut>,
    ) {
        let now = Instant::now();
        let at = now + Duration::from_secs(seconds.max(0) as u64);
        let ignore_min = flags.contains(ReannounceFlags::IGNORE_MIN_INTERVAL);
        let (pid, port) = (self.peer_id, self.listen_port);
        let (num_want, report_true, apply_filter) = (
            self.settings.num_want,
            self.settings.report_true_downloaded,
            self.settings.apply_ip_filter_to_trackers,
        );
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        if t.is_paused() {
            return;
        }
        match tracker_idx {
            Some(i) => {
                // `force_tracker_request(idx)` — forcing a specific
                // tracker must reach *it*, bypassing tier failover
                // (upstream `announce_tracker_now`): trigger the
                // entry, then dispatch each supported info-hash
                // version whose new deadline is already due
                // (`min_announce` still clamps unless
                // `ignore_min_interval`).
                let is_seed = t.is_seed();
                t.trackers.trigger(Some(i), at, ignore_min);
                let supports = [t.info_hashes.has_v1(), t.info_hashes.has_v2()];
                let mut sent = Vec::new();
                for v in bitdaemon_common::info_hash::ALL_VERSIONS {
                    if !supports[v as usize] {
                        continue;
                    }
                    let due = t.trackers.trackers.get(i).is_some_and(|e| {
                        // `announce_with_tracker` skips undispatchable
                        // schemes even when forced (`e0716cf24`).
                        crate::tracker_list::tracker_protocol_supported(&e.url)
                            && e.endpoints.iter().any(|ep| {
                                let a = if v == bitdaemon_common::info_hash::ProtocolVersion::V1 {
                                    &ep.v1
                                } else {
                                    &ep.v2
                                };
                                ep.enabled && a.can_announce(now, is_seed, e.fail_limit)
                            })
                    });
                    if !due {
                        continue;
                    }
                    Self::dispatch_announce(
                        t,
                        *ih,
                        i,
                        v,
                        0,
                        is_seed,
                        pid,
                        port,
                        apply_filter,
                        num_want,
                        report_true,
                        flags.contains(ReannounceFlags::HIGH_PRIORITY),
                        out,
                        &mut sent,
                    );
                }
                self.post_tracker_announces(sent);
            }
            None => {
                // `force_tracker_request(-1)` marks every tracker
                // due; the normal announce_state selection still
                // applies (failover), so route through the pump.
                // `high_priority` runs the scan immediately
                // (`announce_with_tracker`); otherwise upstream only
                // rearms `update_tracker_timer` — the next tick's
                // pump dispatch is our equivalent (the +1s slack in
                // `can_announce` makes the deadline due at `at`).
                let high = flags.contains(ReannounceFlags::HIGH_PRIORITY);
                t.trackers.trigger(None, at, ignore_min);
                if high {
                    self.tracker_announce_due(now, 0, out);
                }
            }
        }
    }

    /// drain `DhtEvent`s — every lookup registered a
    /// [`DhtResult`] sink: `get_peers` replies feed `peer_list`
    /// (source DHT), finished lookups trigger `announce_peer`/`put`
    /// second phases, and user-facing results land as alerts
    /// (ADR-0023 D4 — nothing is silently dropped).
    fn dht_drain_events(&mut self) {
        use bitdaemon_dht::dht_node::DhtEvent;
        for v6 in [false, true] {
            let Some(n) = self.dht.get_node_mut(v6) else {
                continue;
            };
            for ev in n.drain_events() {
                match ev {
                    DhtEvent::Data { lookup, data } => {
                        self.dht_on_data(v6, lookup, *data);
                    }
                    DhtEvent::Done { lookup, tokens } => {
                        self.dht_on_done(v6, lookup, tokens);
                    }
                    DhtEvent::Direct {
                        tag,
                        id,
                        ep,
                        response,
                        failed,
                    } => {
                        self.dht_on_direct(tag, id, ep, response, failed);
                    }
                    DhtEvent::PutDone { tag, num_success } => {
                        // `put_immutable_item_callback`/`put_mutable`
                        // — resolve into the call's shared ctx.
                        if let Some(ctx) = self.dht_put_tags.remove(&(v6, tag)) {
                            self.dht_put_ctx_finish(ctx, num_success);
                        }
                    }
                    DhtEvent::IncomingGetPeers { ih } => {
                        // `dht_get_peers_alert` — an inbound query
                        // hit our node.
                        self.alerts.post(Alert::DhtGetPeers {
                            ih,
                            msg: format!("incoming get_peers for {ih}"),
                        });
                    }
                    DhtEvent::IncomingAnnounce { ih, ip, port } => {
                        // `dht_announce_alert` — a remote peer
                        // announced itself to our node.
                        self.alerts.post(Alert::DhtAnnounce {
                            remote: ip.to_string(),
                            port,
                            ih,
                            msg: format!("incoming announce_peer for {ih}"),
                        });
                    }
                    DhtEvent::OutgoingGetPeers {
                        target,
                        sent_target,
                        ep,
                    } => {
                        self.alerts.post(Alert::DhtOutgoingGetPeers {
                            ih: target,
                            obfuscated: sent_target,
                            ep,
                        });
                    }
                    DhtEvent::Pkt { ep, bytes } => {
                        self.alerts.post(Alert::DhtPacket {
                            direction: 0,
                            remote: ep.to_string(),
                            msg: bytes,
                        });
                    }
                    DhtEvent::Log { module, msg } => {
                        self.alerts.post(Alert::DhtLog { module, msg });
                    }
                }
            }
        }
    }

    /// one lookup reply — route by the registered [`DhtResult`]
    /// sink (upstream `find_data_observer::reply` → per-kind
    /// callback).
    fn dht_on_data(&mut self, v6: bool, lookup: usize, data: bitdaemon_dht::lookups::ReplyData) {
        let Some(sink) = self.dht_result_map.get_mut(&(v6, lookup)) else {
            return;
        };
        match sink {
            DhtResult::Announce { ih, .. } => {
                let ih = *ih;
                if data.peers.is_empty() {
                    return;
                }
                // `dht_reply_alert` — a DHT announce that found
                // peers counts as a tracker reply upstream
                // (torrent::on_dht_announce_response).
                self.alerts.post(Alert::DhtReply {
                    ih,
                    url: "DHT".into(),
                    num_peers: data.peers.len() as i32,
                });
                // `dht_get_peers_reply_alert` — upstream binds
                // `on_dht_get_peers` on announce traversals too.
                self.alerts.post(Alert::DhtGetPeersReply {
                    ih,
                    num_peers: data.peers.len() as u32,
                    peers: data.peers.clone(),
                });
                if let Some(t) = self.torrents.get_mut(&ih) {
                    if !data.peers.is_empty() {
                        tracing::info!(%ih, peers = data.peers.len(), "dht get_peers found peers");
                    }
                    for ep in data.peers {
                        let mut eff = Vec::new();
                        t.add_swarm_peer(
                            ep,
                            bitdaemon_common::flags::PeerSourceFlags::DHT,
                            &mut eff,
                        );
                        for e in eff {
                            if let TorrentEffect::Alert(a) = e {
                                self.alerts.post(a);
                            }
                        }
                    }
                }
            }
            DhtResult::GetPeers(ih) | DhtResult::UserAnnounce { ih, .. } => {
                if !data.peers.is_empty() {
                    self.alerts.post(Alert::DhtGetPeersReply {
                        ih: *ih,
                        num_peers: data.peers.len() as u32,
                        peers: data.peers.clone(),
                    });
                }
            }
            DhtResult::GetItem { target, ctx } => {
                let (target, ctx_id) = (*target, *ctx);
                let Some(v) = &data.item else {
                    return;
                };
                let Some(c) = self.dht_item_ctxs.get_mut(&ctx_id) else {
                    return;
                };
                if c.posted {
                    return;
                }
                c.posted = true;
                // upstream `get_item::got_data` — the first matching
                // `v` ends the traversal (`done()`); other families
                // keep going but can no longer post.
                self.alerts.post(Alert::DhtImmutableItem {
                    target,
                    value: v.clone(),
                });
                if let Some(n) = self.dht.get_node_mut(v6) {
                    n.abort_lookup(lookup);
                }
            }
            DhtResult::GetItemMutable {
                target,
                pk,
                salt,
                ctx,
                best,
            } => {
                let (Some(v), Some(k), Some(sig)) = (&data.item, &data.key, &data.sig) else {
                    return;
                };
                let (Ok(pk_b), Ok(sig_b)) = (
                    <[u8; 32]>::try_from(k.as_slice()),
                    <[u8; 64]>::try_from(sig.as_slice()),
                ) else {
                    return;
                };
                if pk_b != *pk {
                    return; // not our key — upstream only reports its own item
                }
                let seq = data.seq.unwrap_or(-1);
                let (target, salt, pk, ctx_id) = (*target, salt.clone(), *pk, *ctx);
                // `item::assign` — a bad signature rejects the item
                // outright (no callback at all upstream).
                if !bitdaemon_dht::item::verify_mutable_item(v, &salt, seq, &pk, &sig_b) {
                    return;
                }
                // family-local best for the `Done` re-post.
                let this_best = Some((seq, v.clone(), sig_b))
                    .filter(|b| best.as_ref().is_none_or(|(s, ..)| *s < b.0));
                if let Some(b) = this_best {
                    *best = Some(b);
                }
                // `get_mutable_item_ctx` — post each better-seq item
                // non-authoritative; `authoritative` comes at `Done`.
                let post = self.dht_item_ctxs.get_mut(&ctx_id).is_some_and(|c| {
                    seq > c.best_seq && {
                        c.best_seq = seq;
                        true
                    }
                });
                if !post {
                    return;
                }
                self.alerts.post(Alert::DhtMutableItem(Box::new(
                    crate::alerts::DhtMutableItemAlert {
                        target,
                        key: pk_b,
                        signature: sig_b,
                        seq,
                        salt,
                        value: v.clone(),
                        authoritative: false,
                    },
                )));
            }
            DhtResult::Put { .. } | DhtResult::Bootstrap => {}
        }
    }

    /// lookup completion — run the write phase (`announce_peer` /
    /// `put`) or post the completion alert, per sink.
    fn dht_on_done(
        &mut self,
        v6: bool,
        lookup: usize,
        tokens: Vec<(bitdaemon_dht::node_id::NodeId, SocketAddr, Vec<u8>)>,
    ) {
        let Some(sink) = self.dht_result_map.remove(&(v6, lookup)) else {
            return;
        };
        match sink {
            DhtResult::Put { pending, ctx } => {
                let targets: Vec<(SocketAddr, Vec<u8>)> =
                    tokens.iter().map(|(_, ep, t)| (*ep, t.clone())).collect();
                // run this family's `put` storm; its `PutDone` feeds
                // the shared `put_item_ctx`. An empty token set
                // resolves the ctx immediately (upstream still
                // counts the traversal, just writes nowhere).
                let mut batch_tag = None;
                if !targets.is_empty()
                    && let Some(n) = self.dht.get_node_mut(v6)
                {
                    let tag = match &pending {
                        PendingPut::Immutable { value, .. } => n.put(value, None, targets),
                        PendingPut::Mutable {
                            value,
                            pk,
                            sig,
                            seq,
                            salt,
                            ..
                        } => n.put(value, Some((pk, *seq, sig, salt)), targets),
                    };
                    batch_tag = Some(tag);
                }
                if let Some(tag) = batch_tag {
                    self.dht_put_tags.insert((v6, tag), ctx);
                } else {
                    self.dht_put_ctx_finish(ctx, 0);
                }
            }
            DhtResult::Announce {
                ih,
                port,
                seed,
                implied_port,
            } => {
                if tokens.is_empty() {
                    self.finish_announce(ih);
                    return;
                }
                // `announce` — tell token-holding nodes we are a
                // peer (`announce_peer` second phase).
                let addrs: std::collections::BTreeMap<_, _> =
                    tokens.iter().map(|(id, ep, _)| (*id, *ep)).collect();
                let tok_map: std::collections::BTreeMap<_, _> =
                    tokens.iter().map(|(id, _, t)| (*id, t.clone())).collect();
                // `listen_port == 0` → the socket's real port
                // (upstream resolves it through the observer).
                let port = if port == 0 { self.listen_port } else { port };
                if let Some(n) = self.dht.get_node_mut(v6) {
                    n.announce(&ih, port, seed, implied_port, &addrs, |id| {
                        tok_map.get(id).cloned()
                    });
                }
                self.finish_announce(ih);
            }
            DhtResult::UserAnnounce {
                ih,
                port,
                seed,
                implied_port,
            } => {
                if tokens.is_empty() {
                    return;
                }
                let addrs: std::collections::BTreeMap<_, _> =
                    tokens.iter().map(|(id, ep, _)| (*id, *ep)).collect();
                let tok_map: std::collections::BTreeMap<_, _> =
                    tokens.iter().map(|(id, _, t)| (*id, t.clone())).collect();
                let port = if port == 0 { self.listen_port } else { port };
                if let Some(n) = self.dht.get_node_mut(v6) {
                    n.announce(&ih, port, seed, implied_port, &addrs, |id| {
                        tok_map.get(id).cloned()
                    });
                }
            }
            DhtResult::GetPeers(_) => {}
            DhtResult::GetItem { target, ctx } => {
                // `get_immutable_item_ctx` — when the last family
                // finishes unposted, upstream still fires the
                // callback once (empty item → empty alert value).
                let Some(c) = self.dht_item_ctxs.get_mut(&ctx) else {
                    return;
                };
                c.active = c.active.saturating_sub(1);
                if c.active == 0 && !c.posted {
                    c.posted = true;
                    self.alerts.post(Alert::DhtImmutableItem {
                        target,
                        value: Vec::new(),
                    });
                }
                if c.active == 0 {
                    self.dht_item_ctxs.remove(&ctx);
                }
            }
            DhtResult::GetItemMutable {
                target,
                pk,
                salt,
                ctx,
                best,
            } => {
                // `get_mutable_item_ctx` — the family's best item is
                // re-posted `authoritative` when it finishes (upstream
                // `get_item::done` re-delivers `m_data, true`).
                let Some(c) = self.dht_item_ctxs.get_mut(&ctx) else {
                    return;
                };
                c.active = c.active.saturating_sub(1);
                let last = c.active == 0;
                if let Some((seq, v, sig)) = best {
                    self.alerts.post(Alert::DhtMutableItem(Box::new(
                        crate::alerts::DhtMutableItemAlert {
                            target,
                            key: pk,
                            signature: sig,
                            seq,
                            salt,
                            value: v,
                            authoritative: true,
                        },
                    )));
                    c.posted = true;
                } else if last && !c.posted {
                    // upstream posts `m_data` (empty) once the last
                    // traversal completes — a get that found
                    // nothing still alerts (empty value).
                    self.alerts.post(Alert::DhtMutableItem(Box::new(
                        crate::alerts::DhtMutableItemAlert {
                            target,
                            key: pk,
                            signature: [0; 64],
                            seq: -1,
                            salt,
                            value: Vec::new(),
                            authoritative: true,
                        },
                    )));
                }
                if c.active == 0 {
                    self.dht_item_ctxs.remove(&ctx);
                }
            }
            DhtResult::Bootstrap => {
                self.alerts.post(Alert::DhtBootstrap {});
            }
        }
    }

    /// `put_item_ctx` accounting — a finished `put` batch (or a
    /// family that had nowhere to write) adds `ok` replies; the
    /// last one posts `dht_put_alert` once with the summed count.
    fn dht_put_ctx_finish(&mut self, ctx: u64, ok: u32) {
        let Some(c) = self.dht_put_ctxs.get_mut(&ctx) else {
            return;
        };
        c.active = c.active.saturating_sub(1);
        c.responses += ok;
        if c.active > 0 {
            return;
        }
        let Some(c) = self.dht_put_ctxs.remove(&ctx) else {
            return;
        };
        match c.pending {
            DhtPutAlertFields::Immutable { target } => {
                self.alerts
                    .post(Alert::DhtPut(Box::new(crate::alerts::DhtPutAlert {
                        target,
                        num_success: c.responses,
                        public_key: [0; 32],
                        signature: [0; 64],
                        seq: 0,
                        salt: Vec::new(),
                    })));
            }
            DhtPutAlertFields::Mutable {
                target,
                pk,
                sig,
                seq,
                salt,
            } => {
                self.alerts
                    .post(Alert::DhtPut(Box::new(crate::alerts::DhtPutAlert {
                        target,
                        num_success: c.responses,
                        public_key: pk,
                        signature: sig,
                        seq,
                        salt,
                    })));
            }
        }
    }

    /// a torrent's announce round ends when no `Announce` sink for
    /// `ih` remains — clears `dht_announcing`.
    fn finish_announce(&mut self, ih: Sha1Hash) {
        let still = self
            .dht_result_map
            .values()
            .any(|s| matches!(s, DhtResult::Announce { ih: h, .. } if *h == ih));
        if !still && let Some(t) = self.torrents.get_mut(&ih) {
            t.dht_announcing = false;
        }
    }

    /// `direct_request` reply/timeout — route by the registered
    /// [`DhtDirectSink`] (upstream `direct_observer` → caller cb).
    fn dht_on_direct(
        &mut self,
        tag: u64,
        id: Option<bitdaemon_dht::node_id::NodeId>,
        ep: SocketAddr,
        response: Vec<u8>,
        failed: bool,
    ) {
        let Some(sink) = self.dht_direct_map.remove(&tag) else {
            return;
        };
        match sink {
            DhtDirectSink::User(user_data) => {
                self.alerts.post(Alert::DhtDirectResponse {
                    remote: ep.to_string(),
                    user_data,
                    response,
                });
            }
            DhtDirectSink::Sample => {
                if failed {
                    return;
                }
                // parse `r.{id,interval,num,samples,nodes}` — same
                // fields `sample_infohashes` traversals extract.
                let Ok(node) = bitdaemon_bencode::bdecode_checked(&response) else {
                    return;
                };
                let interval = node.dict_find_int_value(b"interval", 0).max(0) as u32;
                let num = node.dict_find_int_value(b"num", 0);
                let mut samples = Vec::new();
                let s = node.dict_find_string(b"samples");
                if s.is_valid() {
                    for chunk in s.string_value().chunks_exact(20) {
                        if let Some(h) = Sha1Hash::from_bytes(chunk) {
                            samples.push(h);
                        }
                    }
                }
                // `r.nodes` — compact 26 B entries (id ‖ ip ‖ port,
                // v4) like `get_peers` replies.
                let mut nodes = Vec::new();
                let nd = node.dict_find_string(b"nodes");
                if nd.is_valid() {
                    for chunk in nd.string_value().chunks_exact(26) {
                        let (Some(nid), Ok(ip), port) = (
                            Sha1Hash::from_bytes(&chunk[..20]),
                            <[u8; 4]>::try_from(&chunk[20..24]),
                            u16::from_be_bytes([chunk[24], chunk[25]]),
                        ) else {
                            continue;
                        };
                        nodes.push((nid, SocketAddr::new(ip.into(), port)));
                    }
                }
                let nid = id.unwrap_or_default();
                self.alerts.post(Alert::DhtSampleInfohashes(Box::new(
                    crate::alerts::DhtSampleInfohashesAlert {
                        node_id: nid,
                        remote: ep.to_string(),
                        interval_secs: interval,
                        num,
                        samples,
                        nodes,
                    },
                )));
            }
        }
    }

    /// `session_impl::recalculate_unchoke_slots` +
    /// `recalculate_optimistic_unchoke_slots` — the choker runs
    /// session-wide over every connection (upstream iterates
    /// `m_connections`, not per-torrent). The unchoke set is
    /// `allowed_slots - optimistic`, where optimistic slots rotate
    /// every `optimistic_unchoke_interval`.
    fn unchoke_tick(&mut self, out: &mut Vec<SessionOut>) {
        let unchoke_every = self.settings.unchoke_interval.as_secs().max(1) as i32;
        let opt_every = self.settings.optimistic_unchoke_interval.as_secs().max(1) as i32;
        // `trigger_unchoke`/`trigger_optimistic_unchoke` — upstream
        // zeroes the time scaler when a vacated slot should refill
        // immediately (conn removed while unchoked / optimistic).
        // The flag aggregates over torrents and re-arms the tick.
        let mut trigger_unchoke = false;
        let mut trigger_optimistic = false;
        for t in self.torrents.values_mut() {
            trigger_unchoke |= t.triggered_unchoke;
            trigger_optimistic |= t.triggered_optimistic;
            t.triggered_unchoke = false;
            t.triggered_optimistic = false;
        }
        let run_unchoke = self.session_time % unchoke_every == 0
            || trigger_unchoke
            || std::mem::take(&mut self.pending_unchoke);
        let run_optimistic = self.session_time % opt_every == 0 || trigger_optimistic;
        if !run_unchoke && !run_optimistic {
            return;
        }
        let now = Instant::now();
        let fixed = self.settings.choking_algorithm == crate::choker::ChokingAlgorithm::FixedSlots;
        let everyone_unchoked = fixed && self.settings.unchoke_slots_limit < 0;

        if run_unchoke {
            if everyone_unchoked {
                // `unchoke_slots_limit < 0` — unlimited upstream
                self.counters.set(
                    crate::counters::Counter::NumUnchokeSlots,
                    i64::from(i32::MAX),
                );
            } else {
                self.recalculate_unchoke_slots(fixed, now, out);
            }
        }

        if run_optimistic && !everyone_unchoked {
            self.recalculate_optimistic_unchoke_slots(now, out);
        }
    }

    /// `recalculate_unchoke_slots` — eligibility sweep, `unchoke_sort`
    /// once over the whole swarm, then choke/unchoke the tail.
    fn recalculate_unchoke_slots(&mut self, fixed: bool, now: Instant, out: &mut Vec<SessionOut>) {
        // upstream measures the real interval since the last round
        // (`now - m_last_choke`) — the rate-based choker divides
        // `uploaded_in_last_round` by it; a triggered early round
        // must not pretend a full interval elapsed.
        let unchoke_interval = self
            .last_choke
            .map_or(self.settings.unchoke_interval, |t0| now.duration_since(t0));
        self.last_choke = Some(now);
        let mut list: Vec<crate::choker::ChokePeer> = Vec::new();
        let mut ih_of: std::collections::HashMap<u64, Sha1Hash> = std::collections::HashMap::new();
        let ihs: Vec<Sha1Hash> = self.torrents.keys().copied().collect();
        for ih in ihs {
            let Some(t) = self.torrents.get_mut(&ih) else {
                continue;
            };
            let pool = &self.peer_class_pool;
            let t_ign = t.ignores_unchoke_slots(pool);
            let t_prio = t.upload_priority(pool);
            let paused = t.is_paused();
            let seeding = t.is_seed();
            let total_size = t.files.as_ref().map_or(0, |f| f.total_size());
            let piece_length = t.piece_length;
            let mut eff = Vec::new();
            // `(id, was_optimistic)` — the optimistic bit is read
            // before `choke_peer` clears it.
            let mut to_choke: Vec<(u64, bool)> = Vec::new();
            // peer-list index resolution needs `&t` — do it in an
            // immutable pass before the mutable one below.
            let pi_of: Vec<(u64, Option<usize>)> = t
                .peers
                .iter()
                .map(|(id, c)| (*id, c.remote.and_then(|r| t.peer_index(r))))
                .collect();
            for (id, c) in &mut t.peers {
                // upstream skips `ignore_unchoke_slots` conns,
                // `pi == nullptr`, web seeds and paused torrents
                // outright (round counters reset); uninterested/
                // disconnecting/connecting peers get choked if they
                // aren't already.
                let pi = pi_of.iter().find(|(i, _)| i == id).and_then(|(_, p)| *p);
                let web_seed = pi.is_some_and(|i| t.peer_list.peers()[i].web_seed);
                if c.ignores_unchoke_slots(pool) || t_ign || paused || web_seed || pi.is_none() {
                    c.stat_up_round = 0;
                    c.stat_down_round = 0;
                    continue;
                }
                if !c.peer_interested || c.disconnecting || c.connecting {
                    if !c.choked {
                        let was_opt =
                            pi.is_some_and(|i| t.peer_list.peers()[i].optimistically_unchoked);
                        to_choke.push((*id, was_opt));
                    }
                    c.stat_up_round = 0;
                    c.stat_down_round = 0;
                    continue;
                }
                ih_of.insert(*id, ih);
                list.push(crate::choker::ChokePeer {
                    id: *id,
                    choked: c.choked,
                    interested: c.peer_interested,
                    uploaded_last: c.stat_up_round,
                    downloaded_last: c.stat_down_round,
                    uploaded_since_unchoked: c.stat_up_total - c.uploaded_at_last_unchoke,
                    last_unchoke: c.last_unchoke,
                    seeding,
                    num_pieces: c.num_pieces,
                    uploaded_total: c.stat_up_total,
                    total_size,
                    piece_length,
                    priority: c.upload_priority(pool).max(t_prio),
                });
            }
            for (id, was_opt) in to_choke {
                if t.choke_peer(id, &self.counters, pool, &mut eff) && was_opt {
                    // upstream zeroes the optimistic scaler when it
                    // chokes an optimistic peer out of the eligible
                    // set — the slot refills next tick.
                    t.triggered_optimistic = true;
                }
            }
            self.route(ih, eff, out);
        }

        let allowed = crate::choker::unchoke_sort(
            &mut list,
            unchoke_interval,
            &crate::choker::ChokerSettings {
                slots_limit: self.settings.unchoke_slots_limit,
                algorithm: self.settings.choking_algorithm,
                seed_algorithm: self.settings.seed_choking_algorithm,
                seeding_piece_quota: i64::from(self.settings.seeding_piece_quota),
                rate_threshold: self.settings.rate_choker_initial_threshold,
            },
            now,
        );
        self.counters.set(
            crate::counters::Counter::NumUnchokeSlots,
            if fixed {
                i64::from(self.settings.unchoke_slots_limit)
            } else {
                allowed as i64
            },
        );

        let optimistic = self
            .counters
            .value(crate::counters::Counter::NumPeersUpUnchokedOptimistic);
        let num_opt = if optimistic == 0 {
            (allowed as i64 / 5).max(1)
        } else {
            optimistic
        };
        let mut unchoke_set = allowed as i64 - num_opt;
        for p in &list {
            let Some(&ih) = ih_of.get(&p.id) else {
                continue;
            };
            let Some(t) = self.torrents.get_mut(&ih) else {
                continue;
            };
            let mut eff = Vec::new();
            // `reset_choke_counters` — new round baseline
            if let Some(c) = t.peers.get_mut(&p.id) {
                c.stat_up_round = 0;
                c.stat_down_round = 0;
            }
            if unchoke_set > 0 {
                if t.peers.get(&p.id).is_some_and(|c| c.choked)
                    && !t.unchoke_peer(
                        p.id,
                        false,
                        &self.counters,
                        &self.peer_class_pool,
                        now,
                        &self.settings,
                        &mut eff,
                    )
                {
                    self.route(ih, eff, out);
                    continue;
                }
                unchoke_set -= 1;
                // promoted into the real set — drop the optimistic bit
                let remote = t.peers.get(&p.id).and_then(|c| c.remote);
                if let Some(i) = remote.and_then(|r| t.peer_index(r)) {
                    let pi = &mut t.peer_list.peers_mut()[i];
                    if pi.optimistically_unchoked {
                        pi.optimistically_unchoked = false;
                        self.counters
                            .add(crate::counters::Counter::NumPeersUpUnchokedOptimistic, -1);
                        // upstream `m_optimistic_unchoke_time_scaler = 0`
                        // — the vacated optimistic slot refills next tick.
                        t.triggered_optimistic = true;
                    }
                }
            } else {
                // past the unchoke set: choke unless optimistically held
                let optimistic = t
                    .peers
                    .get(&p.id)
                    .and_then(|c| c.remote)
                    .and_then(|r| t.peer_index(r))
                    .is_some_and(|i| t.peer_list.peers()[i].optimistically_unchoked);
                if !optimistic {
                    t.choke_peer(p.id, &self.counters, &self.peer_class_pool, &mut eff);
                }
            }
            self.route(ih, eff, out);
        }
    }

    /// `recalculate_optimistic_unchoke_slots` — every
    /// `optimistic_unchoke_interval`, pick `num_optimistic_unchoke_slots`
    /// (or `max(1, slots/5)`) longest-waiting choked peers to unchoke.
    fn recalculate_optimistic_unchoke_slots(&mut self, now: Instant, out: &mut Vec<SessionOut>) {
        let num_unchoke_slots = self
            .counters
            .value(crate::counters::Counter::NumUnchokeSlots);
        if num_unchoke_slots == 0 {
            return;
        }
        // candidates: choked (or already optimistic), interested,
        // connected, on an unpaused torrent with metadata and a free
        // upload slot. `(id, ih, last_optimistically_unchoked)`.
        let mut cands: Vec<(u64, Sha1Hash, u16)> = Vec::new();
        let mut prev_opt: Vec<(u64, Sha1Hash)> = Vec::new();
        for (ih, t) in &self.torrents {
            if t.is_paused() || t.ti.is_none() || !t.free_upload_slots(&self.peer_class_pool) {
                continue;
            }
            let t_ign = t.ignores_unchoke_slots(&self.peer_class_pool);
            for (id, c) in &t.peers {
                if c.connecting
                    || c.disconnecting
                    || !c.peer_interested
                    // upstream `!p->ignore_unchoke_slots()` — exempt
                    // conns never take optimistic slots either.
                    || c.ignores_unchoke_slots(&self.peer_class_pool)
                    || t_ign
                {
                    continue;
                }
                let Some(i) = c.remote.and_then(|r| t.peer_index(r)) else {
                    continue;
                };
                let p = &t.peer_list.peers()[i];
                if p.web_seed {
                    continue;
                }
                if p.optimistically_unchoked {
                    prev_opt.push((*id, *ih));
                }
                if c.choked || p.optimistically_unchoked {
                    cands.push((*id, *ih, p.last_optimistically_unchoked));
                }
            }
        }
        let mut num_opt = i64::from(self.settings.num_optimistic_unchoke_slots);
        if num_opt == 0 {
            num_opt = (num_unchoke_slots / 5).max(1);
        }
        let num_opt = (num_opt as usize).min(cands.len());
        // `partial_sort` by `last_optimistically_unchoked` ascending —
        // the peers that waited longest come first.
        if !cands.is_empty() {
            let nth = num_opt.min(cands.len() - 1);
            cands.select_nth_unstable_by(nth, |a, b| a.2.cmp(&b.2));
        }
        let top = &cands[..num_opt];
        let keep: std::collections::BTreeSet<(u64, Sha1Hash)> =
            top.iter().map(|&(id, ih, _)| (id, ih)).collect();
        // choke every previous optimistic peer that didn't make the
        // new set (`choke_peer` clears the flag + counter)
        for (id, ih) in prev_opt {
            if keep.contains(&(id, ih)) {
                continue;
            }
            if let Some(t) = self.torrents.get_mut(&ih) {
                let mut eff = Vec::new();
                t.choke_peer(id, &self.counters, &self.peer_class_pool, &mut eff);
                self.route(ih, eff, out);
            }
        }
        for &(id, ih, _) in top {
            let Some(t) = self.torrents.get_mut(&ih) else {
                continue;
            };
            let already = t
                .peers
                .get(&id)
                .and_then(|c| c.remote)
                .and_then(|r| t.peer_index(r))
                .is_some_and(|i| t.peer_list.peers()[i].optimistically_unchoked);
            if already {
                continue;
            }
            let mut eff = Vec::new();
            if t.unchoke_peer(
                id,
                true,
                &self.counters,
                &self.peer_class_pool,
                now,
                &self.settings,
                &mut eff,
            ) {
                if let Some(c) = t.peers.get(&id) {
                    if let Some(i) = c.remote.and_then(|r| t.peer_index(r)) {
                        let p = &mut t.peer_list.peers_mut()[i];
                        p.optimistically_unchoked = true;
                        p.last_optimistically_unchoked =
                            self.session_time.clamp(0, i32::from(u16::MAX)) as u16;
                        self.counters
                            .add(crate::counters::Counter::NumPeersUpUnchokedOptimistic, 1);
                    }
                }
            }
            self.route(ih, eff, out);
        }
        // upstream tail — optimistic unchokes can push the unchoked
        // total past the slot budget; zero the unchoke scaler so the
        // regular choker trims the surplus on the next tick.
        if self
            .counters
            .value(crate::counters::Counter::NumUnchokeSlots)
            < self
                .counters
                .value(crate::counters::Counter::NumPeersUpUnchokedAll)
        {
            self.pending_unchoke = true;
        }
    }

    /// `send_message`'s per-type accounting — `num_outgoing_*` for
    /// every message handed to the transport layer. Upstream bumps
    /// these at the `write_*` builders; our single funnel is the
    /// `SessionOut::ToPeer` drain in the engine, which calls this
    /// before each `tx.send`. Extended sub-messages (`ut_pex`,
    /// `ut_metadata`) get their own counter on top of
    /// `num_outgoing_extended`, like the plugins upstream.
    pub fn count_outgoing(&mut self, ih: &Sha1Hash, peer: u64, msg: &PeerMessage) {
        use crate::counters::Counter as C;
        use bitdaemon_net::peer_wire::PeerMessage as M;
        let idx = match msg {
            M::Keepalive => return, // no counter upstream
            M::Extended { id, .. } => {
                if *id == 0 {
                    C::NumOutgoingExtHandshake as usize
                } else {
                    // plugins count their own subtype too — resolve
                    // against this conn's negotiated remote ids.
                    if let Some(c) = self.torrents.get(ih).and_then(|t| t.peers.get(&peer)) {
                        if c.ext_ut_pex == *id || c.ext_i2p_pex == *id {
                            self.counters.inc(C::NumOutgoingPex);
                        } else if c.ext_ut_metadata == *id {
                            self.counters.inc(C::NumOutgoingMetadata);
                        }
                    }
                    C::NumOutgoingExtended as usize
                }
            }
            // `write_dont_have` bumps `num_outgoing_extended`.
            M::DontHave { .. } => C::NumOutgoingExtended as usize,
            M::HashRequest(_) => C::NumOutgoingHashRequest as usize,
            M::Hashes { .. } => C::NumOutgoingHashes as usize,
            M::HashReject(_) => C::NumOutgoingHashReject as usize,
            m => {
                let id = m.wire_id();
                if id <= 9 {
                    C::NumOutgoingChoke as usize + id as usize
                } else if (13..=17).contains(&id) {
                    // contiguous `num_outgoing_suggest..allowed_fast`
                    // — the outgoing map is *not* quirked upstream.
                    C::NumOutgoingSuggest as usize + (id as usize - 13)
                } else {
                    C::NumOutgoingExtended as usize
                }
            }
        };
        self.counters.inc_idx(idx);
    }

    /// Queue `msg` to `peer` — charges its wire size to the peer's
    /// `send_buffer` so `fill_send_buffer`'s watermark sees queued
    /// bytes (upstream `append_to_send_buffer`).
    fn push_to_peer(
        &mut self,
        ih: &Sha1Hash,
        peer: u64,
        msg: PeerMessage,
        out: &mut Vec<SessionOut>,
    ) {
        if let Some(t) = self.torrents.get_mut(ih)
            && let Some(c) = t.peers.get_mut(&peer)
        {
            c.send_buffer += bitdaemon_net::peer_wire::wire_len(&msg);
        }
        out.push(SessionOut::ToPeer { ih: *ih, peer, msg });
    }

    /// `torrent::handle_disk_error` — the shared disk-failure
    /// discrimination every job completion runs:
    ///
    /// * `OutOfMemory` → `file_error_alert` + disconnect the
    ///   requesting `peer` (no torrent error state);
    /// * otherwise → `file_error_alert`;
    /// * `rw == Write` on a recoverable write code
    ///   (`read_only_file_system`, `permission_denied`,
    ///   `operation_not_permitted`, `no_space_on_device`,
    ///   `file_too_large`) → `set_upload_mode(true)` and keep the
    ///   torrent running;
    /// * everything else → `set_error(ec, file)` + `pause()`.
    fn on_disk_error(
        &mut self,
        ih: Sha1Hash,
        err: &StorageError,
        rw: DiskJobClass,
        peer: Option<u64>,
        out: &mut Vec<SessionOut>,
    ) {
        use std::io::ErrorKind as K;
        let path = self
            .torrents
            .get(&ih)
            .map_or_else(String::new, |t| t.resolve_filename(err.file.0));
        self.alerts.post(Alert::FileError {
            ih,
            path,
            error: err.to_string(),
        });
        if err.io.kind() == K::OutOfMemory {
            if let Some(p) = peer {
                out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                    peer: p,
                    reason: "out of memory",
                    // `errors::no_memory` — default (normal) severity
                    // upstream; a local alloc failure, not a peer fault.
                    sev: bitdaemon_net::bt_peer_conn::DisconnectSeverity::Normal,
                }));
            }
            return;
        }
        // `operation_aborted` never reaches us — our disk jobs are
        // not cancelled mid-flight.
        // upstream's soft-write list: `read_only_file_system`,
        // `permission_denied`, `operation_not_permitted` (EPERM maps
        // to `PermissionDenied` in Rust), `no_space_on_device`
        // (`StorageFull`), `file_too_large`.
        if rw == DiskJobClass::Write
            && matches!(
                err.io.kind(),
                K::ReadOnlyFilesystem | K::PermissionDenied | K::StorageFull | K::FileTooLarge
            )
        {
            // a failed write may keep failing while reads still
            // work — degrade to upload-only rather than pausing.
            let mut eff = Vec::new();
            {
                let Session {
                    settings,
                    torrents,
                    counters,
                    ..
                } = &mut *self;
                if let Some(t) = torrents.get_mut(&ih) {
                    t.set_upload_mode(true, settings, counters, &mut eff);
                }
            }
            self.route(ih, eff, out);
            return;
        }
        // put the torrent in error-state and pause it — upstream
        // `set_error` itself posts `torrent_error_alert` with the
        // resolved filename.
        let (error, file) = if let Some(t) = self.torrents.get_mut(&ih) {
            t.set_error(&err.to_string(), err.file.0);
            (err.to_string(), t.resolve_filename(err.file.0))
        } else {
            (err.to_string(), String::new())
        };
        self.alerts.post(Alert::TorrentError { ih, error, file });
        self.set_paused(&ih, true, out);
    }

    /// `on_sent` — the writer pump reports `bytes` actually written
    /// to `peer`'s socket: the send buffer shrinks and
    /// `fill_send_buffer` runs to admit queued requests.
    pub fn on_peer_sent(
        &mut self,
        ih: &Sha1Hash,
        peer: u64,
        bytes: usize,
        payload: usize,
        out: &mut Vec<SessionOut>,
    ) {
        let mut eff = Vec::new();
        // `on_send_data` — `counters::sent_bytes` tracks socket-level
        // writes (payload + protocol, excluding IP overhead).
        self.counters
            .add(crate::counters::Counter::SentBytes, bytes as i64);
        if let Some(t) = self.torrents.get_mut(ih) {
            if let Some(c) = t.peers.get_mut(&peer) {
                c.send_buffer = c.send_buffer.saturating_sub(bytes);
                // `on_send_data` — shift the in-flight requests'
                // `send_buffer_offset`s down by the drained bytes
                // (`requests_in_buffer` tracks what's still queued).
                c.pipeline.on_send_data(bytes);
                // `sent_bytes(payload, protocol)` — accounted when
                // the bytes leave the socket, like upstream
                // `on_sent`/`m_payloads` range accounting.
                let payload = i64::try_from(payload).unwrap_or(i64::MAX);
                c.stat_up_total += payload;
                c.stat_up_round += payload;
                c.stat_up_protocol += bytes as i64 - payload;
                if payload > 0 {
                    c.last_sent_payload_ms = c.now_ms;
                    t.last_payload_up = Some(std::time::Instant::now());
                    // `stat::sent_payload` → session counter.
                    self.counters
                        .add(crate::counters::Counter::SentPayloadBytes, payload);
                }
            }
            t.fill_send_buffer(peer, &self.settings, &self.counters, &mut eff);
        }
        self.route(*ih, eff, out);
    }

    fn route(&mut self, ih: Sha1Hash, effects: Vec<TorrentEffect>, out: &mut Vec<SessionOut>) {
        // `num_peers_down_requests` — conns with a non-empty
        // download_queue; synced once per event batch (upstream
        // incs/decs at each queue transition site).
        if let Some(t) = self.torrents.get_mut(&ih) {
            t.sync_down_requests(&self.counters);
        }
        self.dispatch_disk(ih, &effects);
        for e in effects {
            match e {
                TorrentEffect::SendTo { peer, msg } => {
                    // `m_last_sent` stamps at `send_packet` enqueue
                    // upstream; `sent_bytes`/`sent_payload` stat
                    // accounting happens at write completion in
                    // `on_peer_sent`.
                    if let Some(t) = self.torrents.get_mut(&ih)
                        && let Some(c) = t.peers.get_mut(&peer)
                    {
                        c.last_sent_ms = t.now_ms;
                    }
                    self.push_to_peer(&ih, peer, msg, out);
                }
                TorrentEffect::DhtNode(addr) => {
                    self.dht_add_peer_node(addr);
                }
                TorrentEffect::ChokePeer { peer } => {
                    // `choke_this_peer` — free the upload slot; the
                    // SendTo::Choke it emits re-enters this route.
                    // Upstream re-arms both scalers: `trigger_unchoke`
                    // refills the freed slot next tick, and a dropped
                    // optimistic peer re-arms the optimistic rotation.
                    let mut te = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        let was_opt = t
                            .peers
                            .get(&peer)
                            .and_then(|c| c.remote)
                            .and_then(|r| t.peer_index(r))
                            .is_some_and(|i| t.peer_list.peers()[i].optimistically_unchoked);
                        let slot_consumer = t.peers.get(&peer).is_some_and(|c| {
                            !c.choked
                                && !c.ignores_unchoke_slots(&self.peer_class_pool)
                                && !t.ignores_unchoke_slots(&self.peer_class_pool)
                        });
                        if t.choke_peer(peer, &self.counters, &self.peer_class_pool, &mut te) {
                            if was_opt {
                                t.triggered_optimistic = true;
                            }
                            if slot_consumer {
                                t.triggered_unchoke = true;
                            }
                        }
                    }
                    self.route(ih, te, out);
                }
                TorrentEffect::MaybeUnchoke { peer } => {
                    // `incoming_interested` tail — `graceful_pause`
                    // skips; already-unchoked conns get the redundant
                    // `Unchoke` re-send upstream emits for clients
                    // that missed the handshake unchoke.
                    let mut te = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih)
                        && !t.graceful_pause
                    {
                        match t.peers.get(&peer).map(|c| c.choked) {
                            Some(true) => {
                                t.maybe_unchoke_this_peer(
                                    peer,
                                    &self.settings,
                                    &self.counters,
                                    &self.peer_class_pool,
                                    Instant::now(),
                                    &mut te,
                                );
                            }
                            Some(false) => {
                                te.push(TorrentEffect::SendTo {
                                    peer,
                                    msg: PeerMessage::Unchoke,
                                });
                            }
                            None => {}
                        }
                    }
                    self.route(ih, te, out);
                }
                TorrentEffect::SetStopWhenReady { on } => {
                    // `torrent::stop_when_ready(b)` — the setter's
                    // immediate trigger (already in a downloading
                    // state) pushes `StopWhenReadyCheck`, which the
                    // handler below evaluates.
                    let mut te = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.set_stop_when_ready(on, &mut te);
                    }
                    self.route(ih, te, out);
                }
                TorrentEffect::StopWhenReadyCheck => {
                    // `set_state`'s `trigger_stop` — the armed flag
                    // saw a non-downloading→downloading edge:
                    // `auto_managed(false)` (`if_config_changed`
                    // dirty + state update + auto-manage re-arm),
                    // `pause()` (the mark lives in `set_paused_mode`),
                    // then the flag clears.
                    let fire = self
                        .torrents
                        .get(&ih)
                        .is_some_and(|t| t.stop_when_ready && t.is_downloading_state());
                    if fire {
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            t.stop_when_ready = false;
                            if t.auto_managed {
                                t.auto_managed = false;
                                t.need_save_resume |=
                                    bitdaemon_common::flags::ResumeDataFlags::IF_CONFIG_CHANGED;
                                t.state_updated();
                                self.auto_manage_scaler = 0;
                            }
                        }
                        self.set_paused(&ih, true, out);
                    }
                }
                TorrentEffect::StateChangedEdge { prev, next } => {
                    // `state_changed_alert` — one post per
                    // transition, in transition order.
                    self.alerts.post(Alert::StateChanged {
                        ih,
                        prev: prev.name().to_string(),
                        next: next.name().to_string(),
                    });
                }
                TorrentEffect::QueueTail => {
                    // `resume_download` → `set_queue_position(
                    // last_pos)` — re-stamp `queue_seq` at the tail
                    // of the download queue.
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.queue_seq = self.next_queue_seq;
                        self.next_queue_seq += 1;
                        t.state_updated();
                    }
                }
                TorrentEffect::GracefulPauseDone => {
                    // `set_paused(true)` on a graceful-paused torrent
                    // — `m_graceful_pause_mode` clears and `do_pause`
                    // runs (posts `TorrentPaused`, `stop_announcing`).
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.graceful_pause = false;
                    }
                    self.do_pause(&ih, out);
                }
                TorrentEffect::DisconnectPeer { peer, reason, sev } => {
                    // `peer_connection::disconnect` — teardown +
                    // severity-correct alert happen inside
                    // `peer_disconnected` (one site, same for
                    // transport deaths).
                    self.peer_disconnected(
                        &ih,
                        peer,
                        Some(bitdaemon_net::bt_peer_conn::PeerGone::Disconnected {
                            severity: sev,
                            reason,
                        }),
                        out,
                    );
                    // forward — the engine drops the writer task so
                    // the socket actually closes (`close_connection`).
                    out.push(SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                        peer,
                        reason,
                        sev,
                    }));
                }
                TorrentEffect::PiecePassed(piece) => {
                    self.alerts.post(Alert::PieceFinished { ih, piece });
                    out.push(SessionOut::Torrent(TorrentEffect::PiecePassed(piece)));
                }
                TorrentEffect::PieceFailed(piece) => {
                    self.alerts.post(Alert::HashFailed { ih, piece });
                    out.push(SessionOut::Torrent(TorrentEffect::PieceFailed(piece)));
                }
                TorrentEffect::ClearPiece { piece, blocks } => {
                    // `piece_failed` tail — the picker already
                    // `lock_piece`d; fence a `clear_piece` job so the
                    // restore happens only after every in-flight write
                    // to the piece has drained (`has_storage` leg).
                    let storage = self.torrents.get(&ih).map(|t| t.storage);
                    match (self.disk.clone(), storage) {
                        (Some(disk), Some(storage)) if storage.is_valid() => {
                            let tx = self.disk_tx.clone();
                            disk.async_clear_piece(
                                storage,
                                piece,
                                Box::new(move |piece| {
                                    let _ = tx.send(DiskCompleted::PieceSync { ih, piece, blocks });
                                }),
                            );
                        }
                        // `!has_storage()` — upstream runs
                        // `on_piece_sync` inline; nothing to fence.
                        _ => {
                            let mut eff = Vec::new();
                            if let Some(t) = self.torrents.get_mut(&ih) {
                                t.on_piece_sync(
                                    piece,
                                    &blocks,
                                    &self.settings,
                                    &self.counters,
                                    &mut eff,
                                );
                            }
                            self.route(ih, eff, out);
                        }
                    }
                }
                TorrentEffect::SetFilePriority { prios } => {
                    // upstream `async_set_file_priority` — fenced
                    // storage job; the completion returns the
                    // storage's adjusted vector to
                    // `on_file_priority`. No storage → apply
                    // directly, like the `!m_storage` leg.
                    let storage = self.torrents.get(&ih).map(|t| t.storage);
                    match (self.disk.clone(), storage) {
                        (Some(disk), Some(storage)) if storage.is_valid() => {
                            let tx = self.disk_tx.clone();
                            disk.async_set_file_priority(
                                storage,
                                prios,
                                Box::new(move |err, prios| {
                                    let _ =
                                        tx.send(DiskCompleted::SetFilePriority { ih, prios, err });
                                }),
                            );
                        }
                        _ => {
                            // `!m_storage` upstream — assign
                            // `m_file_priority` + save-resume only;
                            // no picker work, no `file_prio` alert.
                            // The torrent set `outstanding` on
                            // dispatch — release it and drain any
                            // deferred updates through the same leg.
                            if let Some(t) = self.torrents.get_mut(&ih) {
                                t.outstanding_file_priority = false;
                                if t.file_priorities != prios {
                                    t.file_priorities = prios;
                                    t.need_save_resume |=
                                        bitdaemon_common::flags::ResumeDataFlags::IF_CONFIG_CHANGED;
                                }
                                if !t.deferred_file_priorities.is_empty() {
                                    let deferred = std::mem::take(&mut t.deferred_file_priorities);
                                    for (idx, prio) in deferred {
                                        if let Some(slot) = t.file_priorities.get_mut(idx as usize)
                                        {
                                            *slot = prio;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                TorrentEffect::FileCompleted(index) => {
                    self.alerts
                        .post(Alert::FileCompleted { ih, index: index.0 });
                    out.push(SessionOut::Torrent(TorrentEffect::FileCompleted(index)));
                }
                TorrentEffect::MetadataReceived => {
                    // upstream `set_metadata` — before `init()`, the
                    // resolved info-hash set is checked against every
                    // other torrent in the session (`find_torrent`
                    // loop, `torrent.cpp:8363-8395`): two metadata-less
                    // magnets resolving to the same swarm error +
                    // pause BOTH and post `torrent_conflict_alert`
                    // (no `metadata_received_alert`, no storage —
                    // upstream returns before the `m_torrent_file`
                    // swap; ours keeps the resolved metadata but the
                    // torrent is paused-errored and cannot progress).
                    use bitdaemon_common::info_hash::ProtocolVersion as PV;
                    let clash = self
                        .torrents
                        .get(&ih)
                        .map(|t| t.info_hashes)
                        .and_then(|hashes| {
                            self.torrents.iter().find_map(|(&k, o)| {
                                if k == ih {
                                    return None;
                                }
                                let mut hit = false;
                                o.info_hashes.for_each(|oh, _| {
                                    hit |= oh == hashes.get(PV::V1) || oh == hashes.get(PV::V2);
                                });
                                hit.then_some(k)
                            })
                        });
                    if let Some(other_ih) = clash {
                        for eih in [other_ih, ih] {
                            if let Some(t) = self.torrents.get_mut(&eih) {
                                // `set_error(errors::duplicate_torrent,
                                // error_file_metadata)` + `pause()`.
                                t.set_error(
                                    "duplicate torrent",
                                    crate::status::ERROR_FILE_METADATA,
                                );
                                t.paused = true;
                                t.state_updated();
                            }
                            self.alerts.post(Alert::TorrentError {
                                ih: eih,
                                error: "duplicate torrent".into(),
                                file: String::new(),
                            });
                        }
                        self.alerts.post(Alert::TorrentConflict {
                            ih,
                            other: None,
                            url: String::new(),
                        });
                    } else {
                        // `torrent::init` — magnets defer storage
                        // creation until the info dict arrives (file
                        // layout was unknown). Without this the picker
                        // runs but every disk job routes to a
                        // nonexistent index and the download stalls
                        // at 0%.
                        if let Some(idx) = self.create_storage_torrent(&ih) {
                            self.check_files(ih, None);
                            tracing::debug!(storage = idx.0, "magnet storage initialized");
                        }
                        self.alerts.post(Alert::MetadataReceived { ih });
                    }
                }
                TorrentEffect::MetadataFailed => {
                    self.alerts.post(Alert::MetadataFailed {
                        ih,
                        error: "metadata hash check failed".into(),
                    });
                }
                TorrentEffect::Alert(a) => {
                    self.alerts.post(a);
                }
                TorrentEffect::YourIp(ip) => {
                    // BEP-10 `yourip` → `ip_voter` (source_peer) —
                    // a new consensus posts `external_ip_alert`.
                    self.vote_external(
                        ip,
                        bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_PEER,
                        &ip,
                    );
                }
                TorrentEffect::ReadPieceFailed { piece, error } => {
                    self.alerts.post(Alert::ReadPieceFailed {
                        ih,
                        piece: piece.0,
                        error,
                    });
                }
                TorrentEffect::BecameSeed => {
                    // upstream `torrent::finished()` →
                    // `torrent_finished_alert` + `upload_only`
                    // re-announce to every peer that negotiated
                    // BEP-21, plus a `completed` announce to
                    // trackers that saw `started`.
                    self.alerts.post(Alert::TorrentFinished(ih));
                    // upstream `finished()` calls `completed()`
                    // (event=completed announce) only on `is_seed`
                    // — a torrent finished-by-priorities posts the
                    // alert but no completed announce.
                    let is_full_seed = self
                        .torrents
                        .get(&ih)
                        .is_some_and(|t| t.have.count() as i32 == t.num_pieces);
                    let mut sent = Vec::new();
                    if is_full_seed && let Some(t) = self.torrents.get_mut(&ih) {
                        // `torrent::completed` — `!m_announcing`
                        // returns early upstream; otherwise
                        // `tracker_list::completed` arms every
                        // non-complete slot and the burst goes
                        // through the regular announce selection —
                        // slots latch `updating`/`sent_event` so the
                        // ack marks `complete_sent` (and
                        // `m_complete_sent`) and the event is never
                        // re-sent.
                        if t.announcing {
                            let now = Instant::now();
                            t.trackers.completed(now);
                            let supports = [t.info_hashes.has_v1(), t.info_hashes.has_v2()];
                            let picks = t.trackers.due(
                                now,
                                true,
                                self.settings.announce_to_all_tiers,
                                self.settings.announce_to_all_trackers,
                                supports,
                                false,
                            );
                            for (idx, v) in picks {
                                Self::dispatch_announce(
                                    t,
                                    ih,
                                    idx,
                                    v,
                                    0,
                                    true,
                                    self.peer_id,
                                    self.listen_port,
                                    self.settings.apply_ip_filter_to_trackers,
                                    self.settings.num_want,
                                    self.settings.report_true_downloaded,
                                    false,
                                    out,
                                    &mut sent,
                                );
                            }
                        }
                    }
                    self.post_tracker_announces(sent);
                    // `write_upload_only` — skipped under
                    // super-seeding/share-mode and gated on
                    // `close_redundant_connections`; payload is the
                    // `enabled` byte (1 on becoming upload-only).
                    let uo_peers: Vec<(u64, u8)> = self
                        .torrents
                        .get(&ih)
                        .filter(|t| {
                            self.settings.close_redundant_connections
                                && !t.super_seeding
                                && !t.share_mode
                        })
                        .map(|t| {
                            t.peers
                                .iter()
                                .filter(|(_, c)| c.ext_upload_only != 0)
                                .map(|(pid, c)| (*pid, c.ext_upload_only))
                                .collect()
                        })
                        .unwrap_or_default();
                    for (pid, ext_id) in uo_peers {
                        self.push_to_peer(
                            &ih,
                            pid,
                            PeerMessage::Extended {
                                id: ext_id,
                                data: bytes::Bytes::from_static(&[1]),
                            },
                            out,
                        );
                    }
                    // upstream `finished()` tail — `async_release_
                    // files(m_storage)` (the `!m_storage` leg
                    // skips) and `trigger_auto_manage` for
                    // auto-managed torrents: a finished torrent
                    // moved to the seeding class and the queue
                    // must rebalance now, not next interval.
                    if let (Some(disk), Some(storage)) =
                        (self.disk.clone(), self.torrents.get(&ih).map(|t| t.storage))
                        && storage.is_valid()
                    {
                        let tx = self.disk_tx.clone();
                        disk.async_release_files(
                            storage,
                            Box::new(move || {
                                let _ = tx.send(DiskCompleted::ReleaseFiles { ih });
                            }),
                        );
                    }
                    if self.torrents.get(&ih).is_some_and(|t| t.auto_managed) {
                        self.auto_manage_scaler = 0;
                    }
                    out.push(SessionOut::Torrent(TorrentEffect::BecameSeed));
                }
                other => out.push(SessionOut::Torrent(other)),
            }
        }
    }

    /// `tracker_announce_due` — `torrent::on_tracker_announce`
    /// pump: collect due tracker entries (`tracker_list::due`) and
    /// emit `TrackerAnnounce` effects. `event`: 0 none / 1
    /// completed / 2 started / 3 stopped (BEP-3), caller chooses.
    pub fn tracker_announce_due(
        &mut self,
        now: std::time::Instant,
        event: i32,
        out: &mut Vec<SessionOut>,
    ) {
        let (pid, port) = (self.peer_id, self.listen_port);
        let (to_all_tiers, to_all_trackers) = (
            self.settings.announce_to_all_tiers,
            self.settings.announce_to_all_trackers,
        );
        let (num_want, report_true, apply_filter) = (
            self.settings.num_want,
            self.settings.report_true_downloaded,
            self.settings.apply_ip_filter_to_trackers,
        );
        let mut sent = Vec::new();
        for (ih, t) in &mut self.torrents {
            if t.is_paused() || !t.announcing || !t.announce_trackers {
                continue;
            }
            let is_seed = t.is_seed();
            // upstream `announce_with_tracker`: `none` becomes
            // `paused` (4) for finished-but-not-seed torrents.
            let eff_event = if event == 0 && t.finished && !t.seeding && !t.seed_mode {
                4
            } else {
                event
            };
            let supports = [t.info_hashes.has_v1(), t.info_hashes.has_v2()];
            let picks = t.trackers.due(
                now,
                is_seed,
                to_all_tiers,
                to_all_trackers,
                supports,
                eff_event == 3,
            );
            for (idx, v) in picks {
                Self::dispatch_announce(
                    t,
                    *ih,
                    idx,
                    v,
                    eff_event,
                    is_seed,
                    pid,
                    port,
                    apply_filter,
                    num_want,
                    report_true,
                    false,
                    out,
                    &mut sent,
                );
            }
        }
        self.post_tracker_announces(sent);
    }

    /// `send_tracker_request` — dispatch one (entry, info-hash
    /// version) announce: auto-picks `started`/`completed`/`none`
    /// when `event` is 0 (upstream `tracker_request` event
    /// selection on the announced slot), marks the `v` slot
    /// in-flight on every enabled endpoint and moves
    /// `triggered_manually` onto the request. Skips `stopped`
    /// announces to slots that never got `started`. The
    /// `trackerid` falls back to the torrent-level `m_trackerid`
    /// (add-torrent param) when the entry has none.
    #[allow(clippy::too_many_arguments)]
    fn dispatch_announce(
        t: &mut Torrent,
        ih: Sha1Hash,
        idx: usize,
        v: bitdaemon_common::info_hash::ProtocolVersion,
        eff_event: i32,
        is_seed: bool,
        pid: bitdaemon_common::PeerId,
        port: u16,
        apply_ip_filter_to_trackers: bool,
        num_want: i32,
        report_true_downloaded: bool,
        high_priority: bool,
        out: &mut Vec<SessionOut>,
        sent: &mut Vec<(Sha1Hash, String, i32)>,
    ) {
        use bitdaemon_common::info_hash::ProtocolVersion as PV;
        let m_complete_sent = t.complete_sent;
        let (ev, url, tracker_id, skip) = {
            let e = &mut t.trackers.trackers[idx];
            let mut ev = eff_event;
            let skip = {
                let slot = if v == PV::V1 {
                    &e.endpoints[0].v1
                } else {
                    &e.endpoints[0].v2
                };
                if ev == 0 {
                    // upstream `send_tracker_request` — `started`
                    // on first contact, `completed` once after
                    // seeding, `none` otherwise.
                    ev = if !slot.start_sent {
                        2
                    } else if is_seed && !m_complete_sent && !slot.complete_sent {
                        1
                    } else {
                        0
                    };
                }
                // `event == stopped` only reaches slots that
                // announced `started` (upstream `send_tracker_request`
                // drops it when `!a.start_sent`).
                ev == 3 && !slot.start_sent
            };
            if !skip {
                for ep in e.endpoints.iter_mut().filter(|ep| ep.enabled) {
                    let a = if v == PV::V1 { &mut ep.v1 } else { &mut ep.v2 };
                    a.updating = true;
                    a.sent_event = ev;
                    // upstream moves `a.triggered_manually` onto the
                    // request at build time — the reply/error alerts
                    // then post unconditionally.
                    a.sent_manual = a.triggered_manually;
                    a.triggered_manually = false;
                }
            }
            (ev, e.url.clone(), e.tracker_id.clone(), skip)
        };
        if skip {
            return;
        }
        // `req.trackerid = ae->trackerid` — falling back to the
        // client-supplied `m_trackerid` when the tracker never
        // assigned one.
        let tracker_id = if tracker_id.is_empty() {
            String::from_utf8_lossy(&t.trackerid).into_owned()
        } else {
            tracker_id
        };
        let info_hash = t.info_hashes.get(v).to_bytes();
        let mut params = t.announce_params(
            pid,
            port,
            ev,
            apply_ip_filter_to_trackers,
            num_want,
            report_true_downloaded,
            info_hash,
        );
        // `tracker_request::high_priority` — queue-jump at the
        // tracker manager (`m_queued.push_front`).
        params.high_priority = high_priority;
        out.push(SessionOut::TrackerAnnounce {
            ih,
            url,
            tracker_id,
            v,
            params,
        });
        t.outstanding_announces += 1;
        sent.push((ih, t.trackers.trackers[idx].url.clone(), ev));
    }

    /// `torrent::scrape_tracker` — request an immediate scrape of
    /// tracker entry `idx` (upstream: user-triggered, outside the
    /// announce schedule).
    /// `scrape_tracker` — upstream resolves the entry: `idx` valid →
    /// that tracker; `idx < 0` or out-of-range → `last_working` →
    /// `first`. `scrape_tracker_url` resolves by url with the same
    /// fallback chain.
    pub fn scrape_tracker(&mut self, ih: &Sha1Hash, tracker_idx: i32, out: &mut Vec<SessionOut>) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        let Some(i) = scrape_pick(&t.trackers, tracker_idx) else {
            return;
        };
        if let Some(e) = t.trackers.trackers.get_mut(i) {
            // `tracker_supported` — upstream skips protocols the
            // tracker manager can't service (`e0716cf24`).
            if !crate::tracker_list::tracker_protocol_supported(&e.url) {
                return;
            }
            // `m_last_scrape` stamps at request time upstream —
            // `status.last_scrape` reports the elapsed.
            e.scrape_time = Some(Instant::now());
            let url = e.url.clone();
            // upstream `scrape_tracker` — one `scrape_request` per
            // supported info-hash version (`sreq.scrape_ih`); each
            // is client-initiated (`user_triggered`) so its
            // reply/failed alert posts unconditionally — the flag
            // lives on the matching version slot.
            for v in bitdaemon_common::info_hash::ALL_VERSIONS {
                if !t.info_hashes.has(v) {
                    continue;
                }
                if let Some(ep) = e.endpoints.first_mut() {
                    if v == bitdaemon_common::info_hash::ProtocolVersion::V1 {
                        ep.v1.sent_manual = true;
                    } else {
                        ep.v2.sent_manual = true;
                    }
                }
                out.push(SessionOut::TrackerScrape {
                    ih: *ih,
                    url: url.clone(),
                    v,
                    info_hash: t.info_hashes.get(v).to_bytes(),
                });
            }
        }
    }

    /// `scrape_tracker_url` — resolve by url, falling back to
    /// `last_working`/`first` like upstream (an unknown url does
    /// not abort the scrape).
    pub fn scrape_tracker_url(&mut self, ih: &Sha1Hash, url: &str, out: &mut Vec<SessionOut>) {
        let idx = self
            .torrents
            .get(ih)
            .and_then(|t| t.trackers.find(url))
            .map_or(-1, |i| i as i32);
        self.scrape_tracker(ih, idx, out);
    }

    /// `on_scrape_response` — update seed/leech counts from a
    /// scrape (`resp.complete`/`incomplete`), post `scrape_reply`;
    /// failure → `scrape_failed` (410 additionally latches
    /// `dont_try_again`, like upstream's scrape branch of
    /// `tracker_request_error`). `v` is the info-hash version the
    /// scrape covered — its `sent_manual` flag is the one consumed.
    pub fn on_scrape_response(
        &mut self,
        ih: &Sha1Hash,
        url: &str,
        v: bitdaemon_common::info_hash::ProtocolVersion,
        resp: Result<
            bitdaemon_net::http_tracker::TrackerResponse,
            bitdaemon_net::tracker_client::AnnounceError,
        >,
    ) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        // `triggered_manually` — a client-initiated scrape posts its
        // reply regardless of the alert mask.
        let manual = t
            .trackers
            .find(url)
            .is_some_and(|i| announce_slot(t, i, v).sent_manual);
        let post = |s: &mut Self, a: Alert| {
            if manual {
                s.alerts.post_unconditional(a);
            } else {
                s.alerts.post(a);
            }
        };
        match resp {
            Ok(r) => {
                if let Some(i) = t.trackers.find(url) {
                    let e = &mut t.trackers.trackers[i];
                    e.verified = true;
                    // `aep->scrape_*` — written on every
                    // reply that carries the counts.
                    if r.downloaded >= 0 {
                        e.scrape_downloaded = r.downloaded;
                    }
                    if r.incomplete >= 0 {
                        e.scrape_incomplete = r.incomplete;
                    }
                    if r.complete >= 0 {
                        e.scrape_complete = r.complete;
                    }
                }
                // `update_scrape_state` — refresh the torrent-wide
                // `complete`/`incomplete`/`downloaded` cache (max
                // over entries), marking `state_updated` +
                // `need_save_resume` on change and re-running the
                // auto-sequential heuristic.
                t.update_scrape_state(&self.settings);
                post(
                    self,
                    Alert::ScrapeReply {
                        ih: *ih,
                        url: url.to_string(),
                        complete: r.complete,
                        incomplete: r.incomplete,
                    },
                );
            }
            Err(e) => {
                // `never talk to this tracker again` — HTTP 410,
                // same rule as the announce branch upstream.
                if e.http_status() == Some(410)
                    && let Some(i) = t.trackers.find(url)
                {
                    t.trackers.dont_try_again(i);
                }
                post(
                    self,
                    Alert::ScrapeFailed {
                        ih: *ih,
                        url: url.to_string(),
                        reason: e.to_string(),
                    },
                );
            }
        }
        // consume the manual flag with the response.
        if let Some(t2) = self.torrents.get_mut(ih)
            && let Some(i) = t2.trackers.find(url)
        {
            let ep = &mut t2.trackers.trackers[i].endpoints[0];
            if v == bitdaemon_common::info_hash::ProtocolVersion::V1 {
                ep.v1.sent_manual = false;
            } else {
                ep.v2.sent_manual = false;
            }
        }
    }

    /// `dht->add_router_node` — a resolved `.torrent` `nodes` host.
    /// Routes on the address family like upstream (`add_node`
    /// picks the v4/v6 node by the resolved address). When the
    /// routing table holds no live nodes yet, the first router node
    /// `session_impl::add_dht_node` — while a DHT node is bound the
    /// endpoint is *ping-verified* (`node::add_node`, upstream) and
    /// only enters the table on reply; unbound it queues in
    /// `m_dht_nodes` (cap 200, random replace) until `start_dht`.
    pub fn dht_add_node(&mut self, addr: SocketAddr) {
        let v6 = addr.is_ipv6();
        tracing::trace!(%addr, v6, bound = self.dht.get_node(v6).is_some(), "dht_add_node");
        if let Some(n) = self.dht.get_node_mut(v6) {
            n.add_node(addr);
        } else if self.dht_nodes.len() >= 200 {
            use rand::Rng as _;
            let i = rand::rng().random_range(0..self.dht_nodes.len());
            self.dht_nodes[i] = addr;
        } else {
            self.dht_nodes.push(addr);
        }
    }

    /// `session_impl::add_dht_router` — routers land in the table
    /// directly (`node::add_router_node`) *and* persist in
    /// `m_dht_router_nodes` so a `start_dht` re-seeds them. A first
    /// table entry kicks off `bootstrap` (upstream arms the
    /// traversal in `dht_tracker::start`).
    pub fn dht_add_router(&mut self, addr: SocketAddr) {
        let v6 = addr.is_ipv6();
        self.dht_router_nodes.push(addr);
        if let Some(n) = self.dht.get_node_mut(v6) {
            let needs_bootstrap = n.table.size().0 == 0;
            n.table.add_router_node(addr);
            if needs_bootstrap {
                tracing::debug!(%addr, "dht bootstrap on first router node");
                let li = n.bootstrap();
                // `dht_bootstrap_alert` on completion (upstream
                // posts it when the first refresh finishes).
                self.dht_result_map.insert((v6, li), DhtResult::Bootstrap);
            }
        }
    }

    /// `dht_tracker::add_node` (peer-discovered) — a `DhtPort`
    /// message's target gets a `ping_only` node entry, not a
    /// router entry (upstream keeps router nodes distinct).
    pub fn dht_add_peer_node(&mut self, addr: SocketAddr) {
        let v6 = addr.is_ipv6();
        if let Some(n) = self.dht.get_node_mut(v6) {
            n.table
                .add_node(bitdaemon_dht::node_entry::NodeEntry::ping_only(addr));
        }
    }

    /// `web_peer_connection::on_body_data` — one web-seed block
    /// arrived; routes to the disk-write path (marks the picker
    /// block `writing` under the seed's own picker key). `url`
    /// attributes the payload to the seed's per-URL counters.
    pub fn on_web_seed_block(
        &mut self,
        ih: &Sha1Hash,
        url: &str,
        req: bitdaemon_common::types::PeerRequest,
        data: Vec<u8>,
        out: &mut Vec<SessionOut>,
    ) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            t.on_web_seed_block(url, req, data, &self.settings, &self.counters, &mut eff);
        }
        self.route(*ih, eff, out);
    }

    /// end of a fetch batch — frees the seed's conn slot and folds
    /// the keep-alive verdict into `web_seed_t::supports_keepalive`
    /// (also the deferred-delete point for `removed` seeds).
    pub fn on_web_seed_done(&mut self, ih: &Sha1Hash, url: &str, saw_close: bool) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.on_web_seed_done(url, saw_close);
        }
    }

    /// `web_peer_connection` failure — release the picked blocks
    /// and reschedule the seed per `backoff` (`retry_web_seed`,
    /// `disabled`, or `remove_web_seed_iter`). Upstream posts
    /// `url_seed_alert` on web-seed errors, carrying the failing
    /// seed's URL and the error message.
    pub fn on_web_seed_error(
        &mut self,
        ih: &Sha1Hash,
        url: &str,
        reqs: &[bitdaemon_common::types::PeerRequest],
        msg: &str,
        backoff: crate::torrent::WebSeedBackoff,
    ) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.on_web_seed_error(url, reqs, msg, backoff, &self.settings);
        }
        self.alerts.post(Alert::UrlSeed {
            ih: *ih,
            url: url.to_string(),
            msg: msg.to_string(),
        });
    }

    /// `on_announce_response` — a tracker exchange completed:
    /// success → peers into `peer_list` (source TRACKER),
    /// `trackerid` saved, scrape counts updated, the announced
    /// info-hash slot marked `sent`; failure → `announce_infohash::
    /// failed` (quadratic backoff + tracker `retry_interval`
    /// hints), `410 → dont_try_again`, all-endpoints-failed →
    /// `deprioritize_tracker`, then the next tracker is tried like
    /// upstream's `announce_with_tracker` tail. `v` is the info-hash
    /// version the exchange announced; `source_ip` is the resolved
    /// tracker endpoint (`tracker_ip` upstream) — the `ip_voter`
    /// reporter key for `external ip`.
    pub fn on_announce_response(
        &mut self,
        ih: &Sha1Hash,
        url: &str,
        v: bitdaemon_common::info_hash::ProtocolVersion,
        resp: Result<
            bitdaemon_net::http_tracker::TrackerResponse,
            bitdaemon_net::tracker_client::AnnounceError,
        >,
        source_ip: Option<std::net::IpAddr>,
        out: &mut Vec<SessionOut>,
    ) {
        use bitdaemon_common::info_hash::ProtocolVersion as PV;
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        t.outstanding_announces = (t.outstanding_announces - 1).max(0);
        let now = std::time::Instant::now();
        // `triggered_manually` rode the request into `sent_manual` —
        // reply/error alerts then post unconditionally.
        let manual = t
            .trackers
            .find(url)
            .is_some_and(|i| announce_slot(t, i, v).sent_manual);
        let mut ext_ip = None;
        // `do_connect_boost` fires once the response is fully
        // folded in (upstream calls it inside `tracker_response`
        // after the peers + endpoint bookkeeping).
        let mut need_boost = false;
        match resp {
            Ok(r) => {
                ext_ip = r.external_ip;
                t.last_error.clear();
                // `tracker_reply_alert::num_peers` counts peers in
                // *this response*, not the swarm total.
                let resp_peers = r.peers.len();
                if let Some(i) = t.trackers.find(url) {
                    t.trackers.trackers[i].set_message("");
                }
                for ep in r.peers {
                    // `torrent::add_peer` — the ip/port-filter gates
                    // run inside; a blocked ep yields `PeerBlocked`.
                    let mut eff = Vec::new();
                    t.add_swarm_peer(
                        ep,
                        bitdaemon_common::flags::PeerSourceFlags::TRACKER,
                        &mut eff,
                    );
                    for e in eff {
                        if let TorrentEffect::Alert(a) = e {
                            self.alerts.post(a);
                        }
                    }
                }
                // `tracker_response` tail — new dial candidates
                // refresh `want_peers` so the connect pump fires.
                t.update_want_peers();
                need_boost = true;
                let mut trackerid = None;
                let mut completed_acked = false;
                if let Some(i) = t.trackers.find(url) {
                    let ev = announce_slot(t, i, v).sent_event;
                    let e = &mut t.trackers.trackers[i];
                    e.verified = true;
                    // `aep->scrape_*` — announce replies carry the
                    // swarm counts too (entry-level fields upstream,
                    // max-ed in `update_scrape_state`).
                    if r.downloaded >= 0 {
                        e.scrape_downloaded = r.downloaded;
                    }
                    if r.incomplete >= 0 {
                        e.scrape_incomplete = r.incomplete;
                    }
                    if r.complete >= 0 {
                        e.scrape_complete = r.complete;
                    }
                    // `announce_infohash::sent` — re-arms
                    // `next_announce` at the tracker interval and
                    // records `started`/`completed` delivery on the
                    // announced hash slot only. An acked `completed`
                    // marks `m_complete_sent` — no other tracker
                    // gets the event again.
                    let iv = std::time::Duration::from_secs(r.interval.max(0) as u64);
                    let min_iv = std::time::Duration::from_secs(r.min_interval.max(0) as u64);
                    e.sent_version(v, iv, min_iv, now);
                    // `record_working` — the last tracker to answer
                    // is `status::current_tracker` and the scrape
                    // fallback target.
                    t.trackers.last_working_url.clone_from(&e.url);
                    // `m_last_scrape` — an announce reply carrying
                    // both counts also marks the scrape timestamp.
                    if r.complete >= 0 && r.incomplete >= 0 {
                        e.scrape_time = Some(Instant::now());
                    }
                    completed_acked = ev == 1;
                    if let Some(tid) = r.tracker_id {
                        e.tracker_id = String::from_utf8_lossy(&tid).into_owned();
                        trackerid = Some(e.tracker_id.clone());
                    }
                }
                if completed_acked {
                    t.complete_sent = true;
                }
                // `update_scrape_state` — refresh the torrent-wide
                // scrape cache from the entries just written.
                t.update_scrape_state(&self.settings);
                // `trackerid_alert` — the tracker gave us a
                // `trackerid` to echo back on later announces.
                if let Some(tid) = trackerid {
                    self.alerts.post(Alert::Trackerid {
                        ih: *ih,
                        url: url.to_string(),
                        trackerid: tid,
                    });
                }
                // `tracker_warning_alert` — `warning message` in
                // the response (non-fatal).
                if let Some(w) = r.warning_message {
                    self.alerts.post(Alert::TrackerWarning {
                        ih: *ih,
                        url: url.to_string(),
                        msg: w,
                    });
                }
                let reply = Alert::TrackerReply {
                    ih: *ih,
                    url: url.to_string(),
                    num_peers: resp_peers,
                };
                if manual {
                    self.alerts.post_unconditional(reply);
                } else {
                    self.alerts.post(reply);
                }
            }
            Err(e) => {
                // `tracker_request_error` — `a.failed(tracker_backoff,
                // retry_interval)` where retry_interval is the
                // failure response's `interval` (or `min interval`);
                // 0 on transport errors → pure quadratic backoff.
                let backoff = self.settings.tracker_backoff;
                let retry = e.retry_interval();
                let status = e.http_status();
                let op = crate::alerts::TrackerOp::from_name(e.operation());
                let msg = match &e {
                    bitdaemon_net::tracker_client::AnnounceError::Failure { message, .. } => {
                        message.clone()
                    }
                    other => other.to_string(),
                };
                // `r.event` rides the cascade — a failed `stopped`
                // still triggers `announce_with_tracker`.
                let mut failed_event = 0;
                let mut fails = 0;
                let supports = [t.info_hashes.has_v1(), t.info_hashes.has_v2()];
                if let Some(i) = t.trackers.find(url) {
                    failed_event = announce_slot(t, i, v).sent_event;
                    {
                        let e = &mut t.trackers.trackers[i];
                        e.failed_version(v, backoff, retry, now);
                        fails = if v == bitdaemon_common::info_hash::ProtocolVersion::V1 {
                            e.endpoints[0].v1.fails
                        } else {
                            e.endpoints[0].v2.fails
                        };
                        // `a.last_error`/`a.message` live on the
                        // endpoint — `torrent_status::error` is for
                        // fatal (disk) errors only.
                        e.set_message(&msg);
                    }
                    // `never talk to this tracker again` — HTTP 410.
                    if status == Some(410) {
                        t.trackers.dont_try_again(i);
                    }
                    // `if all endpoints fail → deprioritize_tracker`
                    // — only when *every* supported (endpoint, hash)
                    // slot is non-working.
                    let any_working = t.trackers.trackers[i].endpoints.iter().any(|ep| {
                        ep.enabled
                            && ((supports[0] && ep.v1.is_working())
                                || (supports[1] && ep.v2.is_working()))
                    });
                    if !any_working {
                        t.trackers.deprioritize(i);
                    }
                }
                let err = Alert::TrackerError {
                    ih: *ih,
                    url: url.to_string(),
                    error: msg,
                    times_in_row: i32::from(fails),
                    status_code: status.unwrap_or(-1),
                    op,
                };
                if manual {
                    self.alerts.post_unconditional(err);
                } else {
                    self.alerts.post(err);
                }
                // `announce to the next working tracker` — upstream
                // tails `tracker_request_error` with
                // `announce_with_tracker(r.event)` unless
                // paused/checking/aborting (stopped events always
                // cascade). Each failure drives failover, so a dead
                // tier-0 tracker hands off to tier-1 without waiting
                // for its own backoff to expire.
                if (!t.is_paused() && !t.checking) || failed_event == 3 {
                    let is_seed = t.is_seed();
                    let (to_all_tiers, to_all_trackers) = (
                        self.settings.announce_to_all_tiers,
                        self.settings.announce_to_all_trackers,
                    );
                    let (num_want, report_true, apply_filter) = (
                        self.settings.num_want,
                        self.settings.report_true_downloaded,
                        self.settings.apply_ip_filter_to_trackers,
                    );
                    let pid = self.peer_id;
                    let port = self.listen_port;
                    let mut sent = Vec::new();
                    let picks = t.trackers.due(
                        now,
                        is_seed,
                        to_all_tiers,
                        to_all_trackers,
                        supports,
                        failed_event == 3,
                    );
                    for (idx, v2) in picks {
                        Self::dispatch_announce(
                            t,
                            *ih,
                            idx,
                            v2,
                            failed_event,
                            is_seed,
                            pid,
                            port,
                            apply_filter,
                            num_want,
                            report_true,
                            false,
                            out,
                            &mut sent,
                        );
                    }
                    self.post_tracker_announces(sent);
                }
            }
        }
        // consume the manual flag with the response.
        if let Some(t2) = self.torrents.get_mut(ih)
            && let Some(i) = t2.trackers.find(url)
        {
            let ep = &mut t2.trackers.trackers[i].endpoints[0];
            if v == PV::V1 {
                ep.v1.sent_manual = false;
            } else {
                ep.v2.sent_manual = false;
            }
        }
        // `tracker response "external ip"` → ip_voter
        // (`source_tracker` — upstream keys the vote by
        // `tracker_ip`, the resolved tracker endpoint).
        if let (Some(ip), Some(src)) = (ext_ip, source_ip) {
            self.vote_external(
                ip,
                bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_TRACKER,
                &src,
            );
        }
        // `do_connect_boost` — the first tracker response hands out
        // `torrent_connect_boost` dials now instead of waiting a
        // whole tick for the connect pump.
        if need_boost {
            self.do_connect_boost(ih, out);
        }
    }

    /// `save_resume_data` — upstream `torrent::save_resume_data`
    /// → `save_resume_data_alert` carrying the bencoded resume
    /// blob. `flags` carries the `resume_data_flags_t` conditions:
    /// when any `if_*` bit is requested but none of those
    /// categories changed since the last dump, upstream posts
    /// `save_resume_data_failed_alert(resume_data_not_modified)`.
    pub fn save_resume_data(
        &mut self,
        ih: &Sha1Hash,
        flags: bitdaemon_common::flags::ResumeDataFlags,
    ) {
        use bitdaemon_common::flags::ResumeDataFlags as RF;
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        let conditions = flags & RF::ONLY_IF_MODIFIED;
        if conditions.is_set() && !t.need_save_resume.contains(conditions) {
            self.alerts.post(Alert::SaveResumeDataFailed {
                ih: *ih,
                error: "resume data not modified".into(),
            });
            return;
        }
        t.need_save_resume = RF::NONE;
        t.state_updated();
        if flags.contains(RF::FLUSH_DISK_CACHE)
            && t.storage.is_valid()
            && let Some(disk) = self.disk.clone()
        {
            let storage = t.storage;
            let tx = self.disk_tx.clone();
            let ih = *ih;
            disk.async_release_files(
                storage,
                Box::new(move || {
                    let _ = tx.send(DiskCompleted::ReleaseFiles { ih });
                }),
            );
        }
        let mut atp = bitdaemon_core::AddTorrentParams::default();
        t.to_resume_data(&mut atp, &self.peer_class_pool);
        t.last_saved_resume = Some(Instant::now());
        let data = bitdaemon_core::resume_data::write_resume_data_buf(&atp);
        self.alerts.post(Alert::SaveResumeData { ih: *ih, data });
    }

    /// `auto_save_resume` — periodic flush of dirty resume data
    /// (upstream saves every `save_resume_data_interval`).
    fn resume_save_tick(&mut self, now: Instant) {
        let interval = self.settings.save_resume_data_interval;
        let due: Vec<Sha1Hash> = self
            .torrents
            .values()
            .filter(|t| {
                t.need_save_resume.is_set()
                    && t.last_saved_resume
                        .is_none_or(|t0| now.duration_since(t0) >= interval)
            })
            .map(|t| t.info_hash)
            .collect();
        for ih in due {
            self.save_resume_data(&ih, bitdaemon_common::flags::ResumeDataFlags::NONE);
        }
    }

    /// `post_torrent_updates` — upstream posts a
    /// `state_update_alert` covering the `torrent_state_updates`
    /// list: torrents carrying `flag_update_subscribe` that marked
    /// `state_updated()` since the last drain. The list clears on
    /// drain.
    pub fn post_torrent_updates(&mut self, flags: crate::status::StatusFlags) {
        // drain the dirty marks first (upstream clears the list as it
        // iterates), then snapshot statuses.
        let flagged: Vec<Sha1Hash> = self
            .torrents
            .iter_mut()
            .filter_map(|(ih, t)| {
                t.needs_state_update.then(|| {
                    t.needs_state_update = false;
                    *ih
                })
            })
            .collect();
        let statuses = flagged
            .iter()
            .filter_map(|ih| {
                self.torrents.get(ih).map(|t| {
                    t.status(
                        &self.settings,
                        self.queue_position(ih),
                        flags,
                        &self.peer_class_pool,
                    )
                })
            })
            .collect::<Vec<_>>();
        // upstream posts the alert unconditionally — an empty
        // `status` vector is a valid (and observable) update.
        self.alerts.post(Alert::StateUpdate { statuses });
    }

    /// `get_all` — drain posted alerts.
    pub fn pop_alerts(&mut self) -> Vec<Alert> {
        self.alerts.get_all()
    }

    /// `session_status`-shaped summary.
    #[must_use]
    pub fn num_torrents(&self) -> usize {
        self.torrents.len()
    }
    /// total connected peers.
    #[must_use]
    pub fn num_peers(&self) -> usize {
        self.torrents.values().map(|t| t.peers.len()).sum()
    }
    /// `post_session_stats` — `session_stats_alert` payload:
    /// a `SessionStats` snapshot plus the `session_status` summary.
    #[must_use]
    pub fn session_stats(&self) -> crate::counters::SessionStats {
        let downloading = self
            .torrents
            .values()
            .filter(|t| !t.is_seed() && !t.is_paused())
            .count() as i64;
        let seeding = self.torrents.values().filter(|t| t.is_seed()).count() as i64;
        self.counters.set(
            crate::counters::Counter::NumDownloadingTorrents,
            downloading,
        );
        self.counters
            .set(crate::counters::Counter::NumSeedingTorrents, seeding);
        self.counters.set(
            crate::counters::Counter::NumPeersConnected,
            self.num_peers() as i64,
        );
        // `update_stats_counters` — the `limiter_*` gauges read the
        // live bandwidth-manager queues.
        self.counters.set(
            crate::counters::Counter::LimiterUpQueue,
            self.upload_rate.queue_size() as i64,
        );
        self.counters.set(
            crate::counters::Counter::LimiterDownQueue,
            self.download_rate.queue_size() as i64,
        );
        self.counters.set(
            crate::counters::Counter::LimiterUpBytes,
            self.upload_rate.queued_bytes(),
        );
        self.counters.set(
            crate::counters::Counter::LimiterDownBytes,
            self.download_rate.queued_bytes(),
        );
        // `update_stats_counters` — the backend's live disk gauges
        // land in the stats bank on each snapshot (upstream calls
        // `m_disk_thread.update_stats_counters(m_stats_counters)`
        // from `update_stats_counters` on the same cadence).
        if let Some(d) = &self.disk {
            let q = d.queue_stats();
            self.counters.set(
                crate::counters::Counter::QueuedDiskJobs,
                q.queued_disk_jobs as i64,
            );
            self.counters.set(
                crate::counters::Counter::BlockedDiskJobs,
                q.blocked_disk_jobs as i64,
            );
            self.counters.set(
                crate::counters::Counter::DiskBlocksInUse,
                q.disk_blocks_in_use,
            );
            self.counters
                .set(crate::counters::Counter::CachedBlocks, q.cached_blocks);
            self.counters
                .set(crate::counters::Counter::NumUnhashed, q.num_unhashed);
            self.counters
                .set(crate::counters::Counter::FilePoolSize, q.file_pool_size);
            self.counters
                .set(crate::counters::Counter::FilePoolHits, q.file_pool_hits);
            self.counters
                .set(crate::counters::Counter::FilePoolMisses, q.file_pool_misses);
            self.counters.set(
                crate::counters::Counter::FilePoolThreadStall,
                q.file_pool_thread_stall,
            );
            self.counters
                .set(crate::counters::Counter::FilePoolRace, q.file_pool_race);
            self.counters.set(
                crate::counters::Counter::NumBlocksWritten,
                q.num_blocks_written,
            );
            self.counters
                .set(crate::counters::Counter::NumBlocksRead, q.num_blocks_read);
            self.counters.set(
                crate::counters::Counter::NumBlocksHashed,
                q.num_blocks_hashed,
            );
            self.counters
                .set(crate::counters::Counter::NumWriteOps, q.num_write_ops);
            self.counters
                .set(crate::counters::Counter::NumReadOps, q.num_read_ops);
            // `num_fenced_read + get_type()` — verbatim upstream
            // arithmetic: the name table predates `hash2` (type 3),
            // so every kind ≥ hash2 reports under the next-shifted
            // metric name upstream; we replicate the same indexing
            // (stale names included). `RemoveTorrent` (type 13) is a
            // Rust-only job kind — it lands on the
            // `num_fenced_file_priority` slot by raw index; upstream
            // never produces a type-13 job so no real counter is
            // displaced.
            for (i, &n) in q.fenced_jobs.iter().enumerate() {
                self.counters
                    .set_idx(crate::counters::Counter::NumFencedRead as usize + i, n);
            }
        }
        crate::counters::SessionStats::snapshot(&self.counters)
    }
    /// `save_state` — the `.session` blob: resume data per torrent
    /// + merged DHT routing state — v4+v6 live in one dict, like
    ///   upstream's `session_state::dht` blob.
    #[must_use]
    pub fn save_state(&self) -> crate::session_state::SessionState {
        let mut st = crate::session_state::SessionState::default();
        for (ih, t) in &self.torrents {
            let mut atp = bitdaemon_core::AddTorrentParams::default();
            t.to_resume_data(&mut atp, &self.peer_class_pool);
            st.torrents.insert(*ih, atp);
        }
        let mut ds = bitdaemon_dht::dht_state::DhtState::default();
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node(v6) {
                let s = n.dht_state();
                ds.nodes.extend(s.nodes);
                ds.nodes6.extend(s.nodes6);
                ds.nids.extend(s.nids);
            }
        }
        if !ds.nodes.is_empty() || !ds.nodes6.is_empty() {
            st.dht_state =
                bitdaemon_bencode::bencode(&bitdaemon_dht::dht_state::save_dht_state(&ds));
        }
        st.settings = Some(self.settings.clone());
        st
    }

    /// `load_state` — parse a `.session` blob: every torrent becomes
    /// an `add_torrent_params` (resume semantics), DHT endpoints
    /// seed `router_nodes`. Returns torrents restored.
    pub fn load_state(&mut self, buf: &[u8], out: &mut Vec<SessionOut>) -> usize {
        let root = bitdaemon_bencode::bdecode(buf);
        let Some(st) = crate::session_state::load_session_state_node(&root) else {
            // upstream posts `session_error_alert` on session-level
            // failures (corrupt `.session` is one).
            self.alerts
                .post(Alert::SessionError("invalid session state".into()));
            return 0;
        };
        // upstream `load_state`: `apply_settings_pack_impl` overlays
        // only the keys the file carries, and `pack.clear`s
        // `user_agent`/`peer_fingerprint` — the client identity is
        // set by the client software, never loaded from state.
        let set_node = root.dict_find_dict(b"settings");
        if set_node.node_type() == bitdaemon_bencode::NodeType::Dict {
            let (ua, fp) = (
                self.settings.user_agent.clone(),
                self.settings.peer_fingerprint.clone(),
            );
            crate::session_params::apply_settings_dict(&mut self.settings, &set_node);
            self.settings.user_agent = ua;
            self.settings.peer_fingerprint = fp;
            // `dht_*` keys reached the live nodes like
            // `apply_settings` would push them.
            self.sync_dht_settings();
        }
        if let Some(ds) = bitdaemon_dht::dht_state::read_dht_state_bytes(&st.dht_state) {
            // `session_params::dht_state` — keep the parsed blob so
            // a later `enable_dht`/`bind` can resolve `nids`.
            self.dht_state_seed = ds.clone();
            for v6 in [false, true] {
                if let Some(n) = self.dht.get_node_mut(v6) {
                    n.load_state(&ds);
                }
            }
        }
        let mut n = 0;
        for (_, atp) in st.torrents {
            if self
                .add_torrent_params(&atp, StorageIndex::INVALID, out)
                .is_some()
            {
                n += 1;
            }
        }
        n
    }

    /// `Bitfield` re-export used by tests/callers.
    #[doc(hidden)]
    pub fn _bitfield_marker(_: &Bitfield) {}
}

/// `aep->info_hashes[ih]` — the announce slot of tracker entry `i`
/// for info-hash version `v` (single synthetic endpoint).
fn announce_slot(
    t: &Torrent,
    i: usize,
    v: bitdaemon_common::info_hash::ProtocolVersion,
) -> &crate::tracker_list::AnnounceInfohash {
    let ep = &t.trackers.trackers[i].endpoints[0];
    if v == bitdaemon_common::info_hash::ProtocolVersion::V1 {
        &ep.v1
    } else {
        &ep.v2
    }
}

/// Upstream `scrape_tracker` entry selection: a valid `idx` wins;
/// `idx < 0`/out-of-range → `last_working` → `first`.
fn scrape_pick(list: &crate::tracker_list::TrackerList, idx: i32) -> Option<usize> {
    if let Ok(i) = usize::try_from(idx) {
        if i < list.trackers.len() {
            return Some(i);
        }
    }
    if !list.last_working_url.is_empty()
        && let Some(i) = list.find(&list.last_working_url)
    {
        return Some(i);
    }
    (!list.trackers.is_empty()).then_some(0)
}

/// `settings_pack` → extra `OpenMode` bits pushed into every storage's
/// file opens (upstream `pread_storage::open_file` reads them live
/// from `settings_interface`; we snapshot them per storage).
fn open_mode_extra(s: &crate::settings::SessionSettings) -> bitdaemon_disk::open_mode::OpenMode {
    use bitdaemon_disk::open_mode::OpenMode;
    let mut m = OpenMode::NONE;
    if s.no_atime_storage {
        m |= OpenMode::NO_ATIME;
    }
    // upstream `open_file` (pread_storage.cpp:780-784 /
    // mmap_storage.cpp:955-959): `disable_os_cache`/`write_through`
    // add `no_cache` on *every* open, not just job-driven paths.
    if matches!(
        s.disk_io_write_mode,
        crate::settings::DiskIoMode::DisableOsCache | crate::settings::DiskIoMode::WriteThrough
    ) {
        m |= OpenMode::NO_CACHE;
    }
    // upstream pread_storage.cpp:730-735 — `#ifdef _WIN32` only.
    #[cfg(windows)]
    if s.enable_set_file_valid_data {
        m |= OpenMode::ALLOW_SET_FILE_VALID_DATA;
    }
    #[cfg(not(windows))]
    let _ = s.enable_set_file_valid_data;
    m
}
