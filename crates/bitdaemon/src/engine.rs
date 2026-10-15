// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of the async side of `session_impl.cpp` (network thread loop,
//! socket demux) — the synchronous core is `session::session`.
//!
//! `Engine` — the async `session_impl` analogue: drives `Session`,
//! fans `SessionOut` effects out to the injected transports
//! (`PeerConnector` for TCP-like streams, `DatagramSocket` for
//! UDP/DHT — both replaceable by OnionBit anonymous circuits).
//!
//! Per connected peer, two tasks run: a reader decoding
//! `PeerMessage`s into the engine's inbound queue and a writer
//! draining the peer's outbound channel. Announce exchanges spawn
//! per-request and report through the same queue.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bitdaemon_common::types::PeerRequest;
use bitdaemon_common::{PeerId, Sha1Hash};
use bitdaemon_net::bt_peer_conn::{run_handshake, write_messages};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_net::resolver::{BoxedResolver, ResolveFlags, resolve_host};
use bitdaemon_net::tracker_client::{AnnounceError, announce_http, announce_udp};
use bitdaemon_net::transport::{BoxedPeerStream, DatagramSocket, PeerConnector};
use bitdaemon_session::alerts::Alert;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{PeerPacket, Session, SessionOut};
use bitdaemon_session::session_handle::lock as slock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn};

/// `ssl_context::m_ssl_ctx` — tracker-facing rustls config built once
/// from the OS root store (upstream loads the system trust DB via
/// OpenSSL defaults). Lazily initialized so non-https setups never
/// pay for it. `None` when the crypto provider can't build a config.
fn tracker_tls_config() -> Option<Arc<rustls::ClientConfig>> {
    static CFG: OnceLock<Option<Arc<rustls::ClientConfig>>> = OnceLock::new();
    CFG.get_or_init(|| {
        let mut roots = rustls::RootCertStore::empty();
        let loaded = rustls_native_certs::load_native_certs();
        for e in &loaded.errors {
            warn!("skipped a native root cert: {e}");
        }
        for c in loaded.certs {
            let _ = roots.add(c);
        }
        match bitdaemon_net::ssl_stream::tracker_client_config(Arc::new(roots)) {
            Ok(c) => Some(c),
            Err(e) => {
                warn!("tracker TLS config unavailable: {e}");
                None
            }
        }
    })
    .clone()
}

/// One completed tracker exchange, reported back to the session.
struct AnnounceDone {
    ih: Sha1Hash,
    url: String,
    result: Result<bitdaemon_net::http_tracker::TrackerResponse, String>,
    /// `scrape_request` — routes to `on_scrape_response`.
    scrape: bool,
}

/// An inbound peer message, tagged with its stream's peer id.
struct InboundPeerMsg {
    packet: PeerPacket,
    /// raw socket bytes consumed by this read chunk — attributed to
    /// the first message decoded from it (`counters::recv_bytes`
    /// counts wire bytes, including partial/deferred frames).
    wire_bytes: usize,
}

/// result of an async dial job (`session_impl::on_connection_attempt`).
struct DialDone {
    ih: Sha1Hash,
    /// `None` for inbound streams on non-IP transports (I2P).
    remote: Option<SocketAddr>,
    stream: Result<BoxedPeerStream, String>,
    /// `false` — an outbound dial failure feeds the reconnect
    /// backoff; an inbound accept failure is just dropped.
    outbound: bool,
    /// reserved-bit caps parsed from the peer's handshake.
    fast: bool,
    dht: bool,
    /// `m_encrypted` — the conn went through the MSE handshake
    /// (`supports_encryption()`, feeds `pex_encryption`).
    encrypted: bool,
    /// remote's peer_id from the handshake (`set_pid`).
    pid: Option<bitdaemon_common::PeerId>,
    /// negotiated RC4 stream state when `crypto_select == RC4` —
    /// moved into the io pump to wrap all wire traffic.
    rc4: Option<bitdaemon_net::pe_crypto::Rc4Handler>,
    /// `peer_exceeds_limit` — the accept admitted us past
    /// `connections_limit` inside `connections_slack`; eviction
    /// re-evaluates at attach (`m_exceeded_limit` upstream).
    exceeded: bool,
    /// `socket_type_idx` — the transport's `socket_type_t` index
    /// (feeds `PeerConn::sock_type` → `set_peer_classes`).
    kind: u8,
}

/// `socket_type_idx` — `TransportKind` → `socket_type_t` index
/// (upstream enum order: tcp 0, socks5 1, http 2, utp 3, i2p 4,
/// rtc 5, tcp_ssl 6, socks5_ssl 7, http_ssl 8, utp_ssl 9).
fn transport_socket_type(k: &bitdaemon_net::instantiate::TransportKind) -> u8 {
    use bitdaemon_net::instantiate::TransportKind as TK;
    match k {
        TK::Tcp => 0,
        TK::Socks => 1,
        TK::HttpStream => 2,
        TK::Utp => 3,
        TK::I2p => 4,
        TK::SslTcp => 6,
        TK::SslSocks => 7,
        TK::SslHttpStream => 8,
        TK::SslUtp => 9,
    }
}

/// A web-seed fetch completion: per-block data or the failed
/// request list (to release the picked blocks).
struct WebSeedDone {
    ih: Sha1Hash,
    result: Result<Vec<(PeerRequest, Vec<u8>)>, Vec<PeerRequest>>,
}

/// `natpmp` per-protocol mapping state — the 12-byte request +
/// the expiry of the last successful map.
#[derive(Debug)]
pub struct NatpmpState {
    /// gateway address the map datagrams go to (`:5351`).
    pub gateway: SocketAddr,
    /// `(proto, local_port, external_port, expires)` per mapping.
    pub mappings: Vec<(bitdaemon_net::natpmp::PortmapProtocol, u16, u16, Instant)>,
    /// lease TTL we asked for (`settings_pack::natpmp_lease_time`-like).
    pub ttl: u32,
}

/// A UPnP SOAP response back to the engine loop — `kind` routes to
/// the upstream reply handler (`on_upnp_get_ip_address_response`,
/// `on_upnp_map_response`, `on_upnp_unmap_response`).
struct UpnpReply {
    dev: usize,
    mapping: usize,
    kind: bitdaemon_net::upnp::SoapKind,
    /// Ok(status, content-type, body, header_finished) —
    /// Err on transport/connect failure (`e && e != eof` upstream).
    reply: Result<(i32, Option<String>, String, bool), String>,
}

/// `Engine` — everything outside `Session` that upstream does in
/// `session_impl`'s io-context loop.
pub struct Engine {
    /// the synchronous core.
    /// The session actor — shared so a `SessionHandle` hands the
    /// same state to downstream callers (`SessionHandle::from_shared`).
    pub session: Arc<std::sync::Mutex<Session>>,
    /// outbound dialer (TCP default; tunnels plug in here).
    connector: Arc<dyn PeerConnector>,
    /// datagram transport for DHT + UDP trackers.
    datagram: Option<Arc<dyn DatagramSocket>>,
    /// dedicated LSD socket (`lsd` upstream binds its own socket on
    /// the well-known port 6771 joined to the BEP-14 group — LSD
    /// announces both arrive and leave from it). Injected by the
    /// caller; `None` disables the multicast lanes.
    lsd_datagram: Option<Arc<dyn DatagramSocket>>,
    /// hostname resolution (anonymous: inject a circuit resolver —
    /// without one only IP-literal tracker hosts work).
    resolver: Option<BoxedResolver>,
    /// next peer id (per-torrent in upstream; engine-unique here).
    next_peer: u64,
    /// peer writers: `(ih, peer)` → outbound message channel.
    writers: HashMap<(Sha1Hash, u64), mpsc::UnboundedSender<PeerMessage>>,
    /// inbound message queue from reader tasks.
    inbound_rx: mpsc::UnboundedReceiver<InboundPeerMsg>,
    inbound_tx: mpsc::UnboundedSender<InboundPeerMsg>,
    /// announce completions.
    announce_rx: mpsc::UnboundedReceiver<AnnounceDone>,
    announce_tx: mpsc::UnboundedSender<AnnounceDone>,
    /// disconnect events from dead streams.
    gone_rx: mpsc::UnboundedReceiver<(Sha1Hash, u64)>,
    gone_tx: mpsc::UnboundedSender<(Sha1Hash, u64)>,
    /// socket-write completions from peer pumps — upstream's
    /// `on_sent` handler: shrink the peer's send buffer and
    /// re-run `fill_send_buffer`.
    sent_rx: mpsc::UnboundedReceiver<(Sha1Hash, u64, usize, usize)>,
    /// `(ih, peer, wire_bytes, payload_bytes)` — upstream
    /// `on_sent` reports socket bytes; `payload` is the piece-data
    /// share (`m_payloads` range accounting).
    sent_tx: mpsc::UnboundedSender<(Sha1Hash, u64, usize, usize)>,
    /// inbound datagrams (DHT, UDP tracker replies).
    udp_rx: mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>,
    /// tracker-reply demux on the shared datagram — upstream gives
    /// each `udp_tracker_connection` its own socket; with one injected
    /// socket the pump must route replies by `transaction_id` or
    /// announces never see their answers.
    udp_tracker_demux: Arc<bitdaemon_net::tracker_client::UdpTrackerDemux>,
    udp_tx: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    /// outbound dial completions (`connection_attempt` jobs).
    connect_rx: mpsc::UnboundedReceiver<DialDone>,
    connect_tx: mpsc::UnboundedSender<DialDone>,
    /// accept-loop `too_many_files_open` events — the accept task
    /// can't disconnect peers itself (the writers map lives in the
    /// main loop); upstream runs `disconnect_peers(1)` +
    /// `connections_limit` clamp inline on the network thread.
    fd_exhausted_rx: mpsc::UnboundedReceiver<()>,
    fd_exhausted_tx: mpsc::UnboundedSender<()>,
    /// web-seed fetch completions.
    webseed_rx: mpsc::UnboundedReceiver<WebSeedDone>,
    webseed_tx: mpsc::UnboundedSender<WebSeedDone>,
    /// resolved `.torrent` `nodes` hosts → router seeds.
    dhtnode_rx: mpsc::UnboundedReceiver<SocketAddr>,
    dhtnode_tx: mpsc::UnboundedSender<SocketAddr>,
    /// `natpmp` mapping state — `None` until `start_natpmp` (or
    /// under `anonymous_mode`, where upstream never maps).
    pub natpmp: Option<NatpmpState>,
    /// UPnP per-device drivers (SSDP discovery is transport-side;
    /// OnionBit feeds `RootDevice`s through `portmap_add_device`).
    pub upnp_devices: Vec<bitdaemon_net::upnp::UpnpDriver>,
    /// UPnP SOAP response completions.
    upnp_rx: mpsc::UnboundedReceiver<UpnpReply>,
    /// fetched SSDP device descriptions → `portmap_add_device`
    /// (ADR-0018); the fetch task runs on the injected connector.
    upnp_dev_tx: mpsc::UnboundedSender<bitdaemon_net::upnp::RootDevice>,
    upnp_dev_rx: mpsc::UnboundedReceiver<bitdaemon_net::upnp::RootDevice>,
    /// SSDP `LOCATION`s already fetched (dedup across M-SEARCH bursts).
    ssdp_seen: std::collections::HashSet<String>,
    /// `m_retry_count` — M-SEARCH resend counter; upstream resends
    /// every `2*retry_count` s while `retry < 12 && (no devices ||
    /// retry < 4)`.
    ssdp_retry_count: u32,
    /// `m_broadcast_timer` — next resend deadline.
    ssdp_next: Option<Instant>,
    upnp_tx: mpsc::UnboundedSender<UpnpReply>,
    /// spawned task handles (kept for abort on drop).
    tasks: Vec<JoinHandle<()>>,
    /// info-hash set shared with acceptor tasks (`m_torrents` check
    /// in upstream's listen_socket on_accept).
    known_ihs: Arc<std::sync::Mutex<std::collections::BTreeSet<Sha1Hash>>>,
    /// pipeline settings handed to every new `PeerConn`
    /// (`queue_time`, `request_timeout`, `max_out_request_queue` —
    /// `settings_pack` values upstream).
    pub queue_time: i32,
    /// `settings_pack::request_timeout` (s).
    pub request_timeout: i32,
    /// `settings_pack::max_out_request_queue`.
    pub max_out_request_queue: u16,
    /// `m_last_tick` for `update_quotas` — the quota distribution
    /// needs the real elapsed time between ticks, not the 1 Hz
    /// interval (this loop also fires on inbound/disk events).
    last_quota_tick: Instant,
    /// `m_last_second_tick` — upstream gates the heavy
    /// `second_tick` work (session/torrent/conn) to once a second
    /// while `update_quotas` runs every `tick_interval`.
    last_second_tick: Instant,
    /// uTP socket manager (`utp_socket_manager`) — demuxes uTP
    /// datagrams on the shared UDP socket when enabled.
    pub utp_manager: Option<Arc<bitdaemon_net::utp_stream::UtpManager>>,
    /// uTP dialer (`enable_outgoing_utp`).
    utp_connector: Option<Arc<bitdaemon_net::utp_stream::UtpConnector>>,
    /// when true, `spawn_dial` prefers uTP and falls back to TCP —
    /// opt-in for now: upstream defaults `enable_*_utp` to true but
    /// our uTP path is newer than the TCP one.
    utp_enabled: bool,
    /// non-peer `SessionOut`s queued by [`Self::dispatch_out`] —
    /// dispatched (async) at the start of the next `tick`.
    pending_out: Vec<SessionOut>,
    /// upstream `post(ioc, call_job_handlers)` — the disk
    /// completion queue's drain poster notifies this; `run` wakes
    /// and calls `drain_disk_completed` instead of batching to the
    /// 1 Hz tick.
    disk_notify: Arc<tokio::sync::Notify>,
    /// disk backend handle.
    disk: Option<Arc<dyn bitdaemon_disk::disk_interface::DiskIo>>,
    /// `peer_info::bw_disk` gate table, shared with the session —
    /// each pump polls its own `(ih, peer)` entry to stop reading the
    /// socket while *its* block is queued on an over-limit disk
    /// (upstream `peer_connection::can_read` returning false closes
    /// the TCP window for that peer only; the gate clears the moment
    /// that peer's own write lands, not when the whole queue drains).
    write_gates: Option<
        Arc<bitdaemon_disk::back_pressure::PeerWriteGates<(bitdaemon_common::Sha1Hash, u64)>>,
    >,
    /// `m_peer_ssl_ctx` SNI dispatch — per-info-hash `CertifiedKey`
    /// map for SSL-torrent listen sockets (`ssl_server_name_callback`
    /// upstream). Fed by [`Self::refresh_ssl_certs`].
    ssl_resolver: Arc<bitdaemon_net::ssl_stream::SniResolver>,
    /// per-torrent trust roots for mutual TLS on SSL-torrent peers —
    /// the acceptor's `ClientCertVerifier` checks the peer chain +
    /// leaf name against these (`torrent::verify_peer_cert`).
    ssl_roots: Arc<bitdaemon_net::ssl_stream::SslTorrentRoots>,
}

impl Engine {
    /// `session_impl` ctor — inject transports.
    pub fn new(
        connector: Arc<dyn PeerConnector>,
        datagram: Option<Arc<dyn DatagramSocket>>,
    ) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (announce_tx, announce_rx) = mpsc::unbounded_channel();
        let (gone_tx, gone_rx) = mpsc::unbounded_channel();
        let (sent_tx, sent_rx) = mpsc::unbounded_channel();
        let (udp_tx, udp_rx) = mpsc::unbounded_channel();
        let (connect_tx, connect_rx) = mpsc::unbounded_channel();
        let (fd_exhausted_tx, fd_exhausted_rx) = mpsc::unbounded_channel();
        let (webseed_tx, webseed_rx) = mpsc::unbounded_channel();
        let (dhtnode_tx, dhtnode_rx) = mpsc::unbounded_channel();
        let (upnp_tx, upnp_rx) = mpsc::unbounded_channel();
        let (upnp_dev_tx, upnp_dev_rx) = mpsc::unbounded_channel();
        Self {
            session: Arc::new(std::sync::Mutex::new(Session::new())),
            connector,
            datagram,
            resolver: None,
            next_peer: 1,
            writers: HashMap::new(),
            inbound_rx,
            inbound_tx,
            announce_rx,
            announce_tx,
            gone_rx,
            gone_tx,
            sent_rx,
            sent_tx,
            udp_rx,
            udp_tx,
            lsd_datagram: None,
            udp_tracker_demux: Arc::new(bitdaemon_net::tracker_client::UdpTrackerDemux::default()),
            connect_rx,
            connect_tx,
            fd_exhausted_rx,
            fd_exhausted_tx,
            webseed_rx,
            webseed_tx,
            dhtnode_rx,
            dhtnode_tx,
            natpmp: None,
            upnp_devices: Vec::new(),
            upnp_rx,
            upnp_dev_tx,
            upnp_dev_rx,
            ssdp_seen: std::collections::HashSet::new(),
            ssdp_retry_count: 0,
            ssdp_next: None,
            upnp_tx,
            tasks: Vec::new(),
            known_ihs: Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new())),
            queue_time: 3,
            request_timeout: 60,
            // `settings_pack::max_out_request_queue` default.
            max_out_request_queue: 500,
            last_quota_tick: Instant::now(),
            last_second_tick: Instant::now(),
            utp_manager: None,
            utp_connector: None,
            utp_enabled: false,
            ssl_resolver: Arc::new(bitdaemon_net::ssl_stream::SniResolver::new(None)),
            ssl_roots: Arc::new(bitdaemon_net::ssl_stream::SslTorrentRoots::new()),
            pending_out: Vec::new(),
            disk_notify: Arc::new(tokio::sync::Notify::new()),
            disk: None,
            write_gates: None,
        }
    }

    /// `session_impl::set_disk_io` — the CLI hands the same backend
    /// it puts in `SessionParams` so peer pumps can consult the
    /// write-queue back-pressure level.
    pub fn set_disk(&mut self, disk: Arc<dyn bitdaemon_disk::disk_interface::DiskIo>) {
        self.disk = Some(disk);
    }

    /// Share the per-peer `bw_disk` gate table — the CLI hands the
    /// same `Arc` to the session (`Session::set_write_gates`) so both
    /// sides read/write the same entries.
    pub fn set_write_gates(
        &mut self,
        gates: Arc<
            bitdaemon_disk::back_pressure::PeerWriteGates<(bitdaemon_common::Sha1Hash, u64)>,
        >,
    ) {
        self.write_gates = Some(gates);
    }

    /// handle for the disk completion queue's drain poster —
    /// `PreadDiskIo::new(threads, poster)` where
    /// `poster = move || notify.notify_one()`.
    pub fn disk_notify(&self) -> Arc<tokio::sync::Notify> {
        self.disk_notify.clone()
    }

    /// `enable_outgoing_utp`/`enable_incoming_utp` wiring — build the
    /// uTP manager on the injected datagram transport, route uTP
    /// datagrams in `tick`, and (incoming) accept SYNs through
    /// [`Self::start_acceptor`]. Call once, before `start_udp_pump`.
    pub async fn enable_utp(&mut self) {
        let Some(dg) = self.datagram.clone() else {
            return;
        };
        let cfg = bitdaemon_net::utp_socket::UtpConfig::default();
        let manager = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
        if slock(&self.session).settings.enable_outgoing_utp {
            self.utp_connector = Some(Arc::new(bitdaemon_net::utp_stream::UtpConnector {
                manager: manager.clone(),
                datagram: dg.clone(),
                cfg: cfg.clone(),
            }));
        }
        if slock(&self.session).settings.enable_incoming_utp {
            let acc = bitdaemon_net::utp_stream::UtpAcceptor::new(manager.clone(), dg, cfg).await;
            // `socket_type_t::utp` = 3 — the tag feeds alerts and
            // `peer_class_type_filter` (`set_peer_classes`).
            self.start_acceptor_at(Arc::new(acc), None, 3);
        }
        self.utp_manager = Some(manager);
        self.utp_enabled = true;
    }

    /// SSL-over-uTP lane — upstream's `…s` listen interfaces create a
    /// separate `ssl_stream<utp_stream>` accept socket on the
    /// interface's own UDP port (`utp_socket_manager` per socket).
    /// `datagram` is that dedicated socket: every datagram is uTP,
    /// every accepted stream is TLS-wrapped like `start_ssl_acceptor`.
    pub async fn start_ssl_utp(
        &mut self,
        dg: Arc<dyn bitdaemon_net::transport::DatagramSocket>,
        local: SocketAddr,
        kind: u8,
    ) {
        let cfg = bitdaemon_net::utp_socket::UtpConfig::default();
        let manager = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
        let acc =
            bitdaemon_net::utp_stream::UtpAcceptor::new(manager.clone(), dg.clone(), cfg).await;
        self.start_ssl_acceptor_at(Arc::new(acc), Some(local), kind);
        self.tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; 65_535];
            while let Ok((n, from)) = dg.recv_from(&mut buf).await {
                manager.route(from, &buf[..n]).await;
            }
        }));
    }

    /// start the datagram pump — every received packet is queued
    /// for `session.on_udp` at the next `tick` (upstream's
    /// `udp_socket` → `on_receive` fan-out; the injected socket
    /// decides what arrives — an anonymous circuit mux demuxes here).
    pub fn start_udp_pump(&mut self) {
        let Some(dg) = self.datagram.clone() else {
            return;
        };
        let tx = self.udp_tx.clone();
        // uTP datagrams must be routed *immediately* — the session
        // tick runs at 1 Hz and would add ~1 s of delay per ACK
        // (upstream's `utp_socket_manager` is fed straight from the
        // socket's read handler for the same reason).
        let utp = self.utp_manager.clone();
        self.tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; 65_535];
            loop {
                match dg.recv_from(&mut buf).await {
                    Ok((n, from)) => {
                        if let Some(mgr) = &utp {
                            if is_utp_datagram(&buf[..n]) {
                                mgr.route(from, &buf[..n]).await;
                                continue;
                            }
                        }
                        if tx.send((from, buf[..n].to_vec())).is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        // `udp_socket.cpp` re-arms the read on
                        // receive errors — a UDP socket that sent
                        // to a dead port gets a transient ICMP
                        // `WSAECONNRESET` on Windows; it must not
                        // kill the pump.
                        warn!(error = %e, "udp recv error — pump continues");
                        tokio::task::yield_now().await;
                    }
                }
            }
        }));
    }

    /// inject the dedicated LSD socket (`lsd` upstream binds its
    /// own socket on port 6771 — separate from the listen socket).
    /// Call before `start_lsd_pump`.
    pub fn set_lsd_datagram(&mut self, dg: Arc<dyn DatagramSocket>) {
        self.lsd_datagram = Some(dg);
    }

    /// LSD receive pump — the dedicated 6771 socket feeds the same
    /// `udp_tx` demux; BT-SEARCH datagrams fall through to
    /// `Session::on_udp` → `lsd::process_packet` (DHT bdecode fails
    /// on them harmlessly first, like upstream's udp_socket
    /// fan-out order).
    pub fn start_lsd_pump(&mut self) {
        let Some(dg) = self.lsd_datagram.clone() else {
            return;
        };
        let tx = self.udp_tx.clone();
        self.tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; 65_535];
            loop {
                match dg.recv_from(&mut buf).await {
                    Ok((n, from)) => {
                        if tx.send((from, buf[..n].to_vec())).is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "lsd recv error — pump continues");
                        tokio::task::yield_now().await;
                    }
                }
            }
        }));
    }

    /// `listen_port` passthrough (announce `port=`).
    pub fn set_listen_port(&mut self, port: u16) {
        slock(&self.session).listen_port = port;
    }

    /// `peer_id` passthrough.
    pub fn set_peer_id(&mut self, pid: PeerId) {
        slock(&self.session).peer_id = pid;
    }

    /// inject the hostname resolver (`resolver_interface` — use an
    /// anonymous/circuit resolver under OnionBit).
    pub fn set_resolver(&mut self, r: BoxedResolver) {
        self.resolver = Some(r);
    }

    /// ADR-0020 — `settings.proxy.dht` = socks5/5pw: rebuild the
    /// session `DatagramSocket` through a SOCKS5 UDP-associate so
    /// DHT, uTP and UDP trackers ride the proxy. Call after
    /// `set_resolver`, before `enable_dht`/`enable_utp` start their
    /// pumps. No silent direct fallback: on failure the previous
    /// socket stays and the error is logged.
    pub async fn apply_udp_proxy(&mut self) {
        let p = slock(&self.session).proxies.dht.clone();
        use bitdaemon_net::proxy::ProxyType;
        let Some(dg) = self.datagram.as_ref().map(Arc::clone) else {
            return;
        };
        if !matches!(p.proxy_type, ProxyType::Socks5 | ProxyType::Socks5Password)
            || p.hostname.is_empty()
        {
            return;
        }
        let v4 = DatagramSocket::local_addr(dg.as_ref()).is_ipv4();
        match resolve_for_socket(&self.resolver, &p.hostname, p.port, v4).await {
            Ok(proxy_addr) => {
                match bitdaemon_net::socks_udp::Socks5UdpSocket::associate(
                    Arc::clone(&self.connector),
                    dg,
                    proxy_addr,
                    &p.username,
                    &p.password,
                )
                .await
                {
                    Ok(s) => {
                        tracing::info!(%proxy_addr, "udp lane via socks5 relay");
                        self.datagram = Some(Arc::new(s));
                    }
                    Err(e) => {
                        // `socks5_alert` — upstream posts it on every
                        // socks handshake/relay failure.
                        slock(&self.session).alerts.post(Alert::Socks5 {
                            remote: proxy_addr.to_string(),
                            op: "udp_associate".into(),
                            error: e.to_string(),
                        });
                        tracing::error!(%proxy_addr, error = %e, "socks5 udp-associate failed")
                    }
                }
            }
            Err(e) => {
                slock(&self.session).alerts.post(Alert::Socks5 {
                    remote: format!("{}:{}", p.hostname, p.port),
                    op: "hostname_lookup".into(),
                    error: e.to_string(),
                });
                tracing::error!(host = %p.hostname, error = %e, "proxy resolve failed")
            }
        }
    }

    /// `on_tick` — pump inbound messages, announce completions,
    /// disconnects, then the session's own `tick` +
    /// `tracker_announce_due`, and fan every effect out.
    /// Returns surfaced `Alert`s.
    pub async fn tick(&mut self, now: Instant) -> Vec<Alert> {
        let mut out = Vec::new();
        // effects queued by `dispatch_out` from sync contexts
        if !self.pending_out.is_empty() {
            let pending = std::mem::take(&mut self.pending_out);
            self.dispatch(pending).await;
        }

        while let Ok(m) = self.inbound_rx.try_recv() {
            slock(&self.session).on_peer_message(&m.packet, m.wire_bytes, &mut out);
        }
        while let Ok((ih, peer)) = self.gone_rx.try_recv() {
            self.writers.remove(&(ih, peer));
            // transport-initiated close: no severity of its own —
            // `peer_disconnected` ORs in the conn's `failed` flag.
            slock(&self.session).peer_disconnected(&ih, peer, false, &mut out);
        }
        while let Ok((ih, peer, bytes, payload)) = self.sent_rx.try_recv() {
            slock(&self.session).on_peer_sent(&ih, peer, bytes, payload, &mut out);
        }
        while let Ok(a) = self.announce_rx.try_recv() {
            if a.scrape {
                slock(&self.session).on_scrape_response(&a.ih, &a.url, a.result);
            } else {
                slock(&self.session).on_announce_response(&a.ih, &a.url, a.result, &mut out);
            }
        }
        while let Ok((from, buf)) = self.udp_rx.try_recv() {
            // demux: NAT-PMP replies come from the gateway :5351 —
            // upstream binds a separate socket; injected transports
            // multiplex on one, so dispatch by sender.
            if self.udp_tracker_demux.route(from, &buf) {
                // reponse tracker consommee par l'annonce en attente
            } else if self.natpmp.as_ref().is_some_and(|n| from == n.gateway) {
                self.on_natpmp_response(&buf);
            } else if is_utp_datagram(&buf) {
                // uTP is normally routed in the UDP pump (immediate);
                // this path only fires if the pump ran before
                // `enable_utp` installed the manager.
                if let Some(mgr) = self.utp_manager.clone() {
                    tokio::spawn(async move { mgr.route(from, &buf).await });
                }
            } else if let Some(loc) = bitdaemon_net::upnp::parse_ssdp_location(&buf) {
                // ADR-0018 — SSDP reply: fetch the device description
                // once, then run the driver chain.
                if slock(&self.session).services.upnp && self.ssdp_seen.insert(loc.clone()) {
                    self.spawn_ssdp_fetch(loc);
                }
            } else {
                trace!(%from, len = buf.len(), "udp recv -> session");
                slock(&self.session).on_udp(from, &buf, &mut out);
            }
        }
        while let Ok(d) = self.connect_rx.try_recv() {
            self.on_dial_done(d);
        }
        while let Ok(w) = self.webseed_rx.try_recv() {
            match w.result {
                Ok(blocks) => {
                    for (req, data) in blocks {
                        slock(&self.session).on_web_seed_block(&w.ih, req, data, &mut out);
                    }
                }
                Err(reqs) => slock(&self.session).on_web_seed_error(&w.ih, &reqs),
            }
        }
        while let Ok(a) = self.dhtnode_rx.try_recv() {
            // upstream resolves `dht_bootstrap_nodes` through the
            // *router* path (`m_dht_router_nodes`) — they seed the
            // table directly and persist across DHT restarts.
            trace!(%a, "dht_add_router drain");
            slock(&self.session).dht_add_router(a);
        }
        while let Ok(r) = self.upnp_rx.try_recv() {
            self.on_upnp_reply(r);
        }
        while let Ok(dev) = self.upnp_dev_rx.try_recv() {
            let port = slock(&self.session).listen_port;
            self.portmap_add_device(dev, port, "bitdaemon".to_string());
        }

        // `session_impl::update_quotas` — distribute quota to queued
        // bandwidth requests with the elapsed time (upstream runs it
        // every `tick_interval` = 500 ms, independent of the 1 s
        // `second_tick` work).
        {
            let dt = now
                .saturating_duration_since(self.last_quota_tick)
                .as_millis() as i64;
            slock(&self.session).update_quotas(dt);
            self.last_quota_tick = now;
        }
        // refresh the acceptor's known info-hash set
        *self.known_ihs.lock().unwrap_or_else(|e| e.into_inner()) =
            slock(&self.session).torrents.keys().copied().collect();
        // `on_tick` gate — the per-second `second_tick` aggregate only
        // runs once ≥1 s elapsed (`m_last_second_tick`); quota
        // distribution above still rides the fast tick.
        if now.saturating_duration_since(self.last_second_tick) >= std::time::Duration::from_secs(1)
        {
            self.last_second_tick = now;
            slock(&self.session).tick(&mut out);
        }
        slock(&self.session).tracker_announce_due(now, 0, &mut out);
        self.dispatch(out).await;
        self.natpmp_renew().await;
        self.upnp_ssdp_tick(now).await;
        slock(&self.session).pop_alerts()
    }

    /// `ToPeer`-only fan-out for sync call sites (connect paths —
    /// bitfield/first-have emission after `attach_stream`).
    /// `session_impl` effect fan-out — public so callers that
    /// mutate `Session` directly (e.g. `add_torrent_params`) can
    /// push the produced `SessionOut`s through the transport layer.
    /// Non-peer effects (DhtAddNode, TrackerAnnounce, UdpPacket…)
    /// can't run in a sync context — they queue onto
    /// `pending_out` and are dispatched on the next [`Self::tick`].
    pub fn dispatch_out(&mut self, o: SessionOut) {
        if matches!(o, SessionOut::ToPeer { .. }) {
            self.send_msgs_to_peers(vec![o]);
        } else {
            self.pending_out.push(o);
        }
    }

    /// Single funnel for peer-bound messages — the engine-side end
    /// of `send_message`. `count_outgoing` keeps the session's
    /// `num_outgoing_*` counters in sync with what actually enters
    /// a writer pump.
    fn send_to_peer(
        &mut self,
        ih: Sha1Hash,
        peer: u64,
        msg: bitdaemon_net::peer_wire::PeerMessage,
    ) {
        slock(&self.session).count_outgoing(&ih, peer, &msg);
        if let Some(tx) = self.writers.get(&(ih, peer)) {
            if tx.send(msg).is_err() {
                self.writers.remove(&(ih, peer));
            }
        }
    }

    fn send_msgs_to_peers(&mut self, out: Vec<SessionOut>) {
        for e in out {
            match e {
                SessionOut::ToPeer { ih, peer, msg } => self.send_to_peer(ih, peer, msg),
                SessionOut::Torrent(
                    bitdaemon_session::torrent::TorrentEffect::DisconnectPeer { peer, .. },
                ) => self.disconnect_writer(peer),
                _ => {}
            }
        }
    }

    /// `peer_connection::disconnect` — drop the conn's write channel;
    /// the pump's `rx.recv()` then returns `None`, the task exits and
    /// the socket closes (`gone_tx` reaps whatever session state
    /// remains — usually none, since the emitting torrent already ran
    /// `remove_peer`).
    fn disconnect_writer(&mut self, peer: u64) {
        if let Some(key) = self.writers.keys().find(|(_, p)| *p == peer).copied() {
            self.writers.remove(&key);
        }
    }

    /// fan `SessionOut` effects to transports/peers.
    async fn dispatch(&mut self, out: Vec<SessionOut>) {
        for e in out {
            match e {
                SessionOut::ToPeer { ih, peer, msg } => self.send_to_peer(ih, peer, msg),
                SessionOut::UdpPacket(buf, to) => {
                    // upstream sends BT-SEARCH *from* its dedicated
                    // 6771 socket — route multicast-group traffic
                    // there when one was injected.
                    let lsd = to.port() == 6771;
                    let sock = if lsd {
                        self.lsd_datagram.as_ref().or(self.datagram.as_ref())
                    } else {
                        self.datagram.as_ref()
                    };
                    // upstream only has an lsd socket for families it
                    // can bind — a v4 socket never sees the v6 send.
                    let skip = lsd
                        && to.is_ipv6()
                        && sock.is_some_and(|s| DatagramSocket::local_addr(s.as_ref()).is_ipv4());
                    if let Some(d) = sock.filter(|_| !skip) {
                        trace!(%to, len = buf.len(), "udp send");
                        // `udp_error_alert` — a datagram send
                        // failed (upstream posts it from the
                        // socket's send handler); multicast targets
                        // also map to `lsd_error_alert`
                        // (`session_impl` posts it on lsd socket
                        // send failures).
                        if let Err(e) = d.send_to(&buf, to).await {
                            let mut ses = slock(&self.session);
                            ses.alerts.post(Alert::UdpError {
                                remote: to.to_string(),
                                op: "send_to".into(),
                                error: e.to_string(),
                            });
                            if lsd {
                                ses.alerts.post(Alert::LsdError {
                                    error: e.to_string(),
                                });
                            }
                        }
                    } else {
                        trace!(%to, "udp send dropped — no datagram socket");
                    }
                }
                SessionOut::TrackerAnnounce {
                    ih,
                    url,
                    tracker_id,
                    params,
                } => {
                    self.spawn_announce(ih, url, tracker_id, params);
                }
                SessionOut::WebSeedFetch {
                    ih,
                    url,
                    files,
                    piece_length,
                    single_file,
                    reqs,
                } => {
                    self.spawn_web_seed(ih, url, files, piece_length, single_file, reqs);
                }
                SessionOut::DhtAddNode { host, port } => {
                    let resolver = self.resolver.clone();
                    self.tasks_forget();
                    let dht_tx = self.dhtnode_tx.clone();
                    let session = Arc::clone(&self.session);
                    self.tasks.push(tokio::spawn(async move {
                        let Ok(port) = u16::try_from(port) else {
                            return;
                        };
                        // all resolved addresses become router nodes —
                        // upstream feeds every endpoint from
                        // `udp::resolver` into `dht.add_node`.
                        let Some(res) = resolver else { return };
                        match bitdaemon_net::resolver::resolve_host(
                            res.as_ref(),
                            &host,
                            port,
                            bitdaemon_net::resolver::ResolveFlags::default(),
                        )
                        .await
                        {
                            Ok(addrs) => {
                                for addr in addrs {
                                    debug!(%addr, "dht bootstrap node resolved");
                                    let _ = dht_tx.send(addr);
                                }
                            }
                            Err(e) => {
                                // `on_dht_name_lookup` upstream →
                                // `dht_error_alert` (hostname_lookup).
                                slock(&session).alerts.post(Alert::DhtError {
                                    op: 0,
                                    error: e.to_string(),
                                });
                                debug!(%host, err = %e, "dht bootstrap resolve failed");
                            }
                        }
                    }));
                }
                SessionOut::TrackerScrape { ih, url } => {
                    self.spawn_scrape(ih, url);
                }
                SessionOut::Torrent(e) => {
                    if let bitdaemon_session::torrent::TorrentEffect::DisconnectPeer {
                        peer,
                        reason,
                        ..
                    } = &e
                    {
                        warn!(peer = peer, reason = reason, "peer disconnect");
                        // `m_ses.close_connection` — actually close the
                        // socket (the torrent already ran remove_peer).
                        self.disconnect_writer(*peer);
                    }
                }
                SessionOut::Alert(_) => {}
                SessionOut::PortmapAdd {
                    protocol,
                    external_port,
                    local_port,
                } => {
                    self.portmap_add(protocol, external_port, local_port).await;
                }
                SessionOut::PortmapDelete {
                    transport,
                    protocol,
                    external_port,
                } => {
                    self.portmap_delete(transport, protocol, external_port)
                        .await;
                }
                SessionOut::InterfacesChanged => {
                    // upstream `on_ip_change` → reopen listeners;
                    // the engine re-binds at the transport level.
                    info!("network interfaces changed");
                }
                SessionOut::ConnectPeer { ih, ep, encrypted } => {
                    // `session_impl::connect_to_peer` — the session
                    // handed out a dial (connect-boost/tracker
                    // response outside the pump cadence).
                    self.spawn_dial(ih, ep, encrypted);
                }
            }
        }
    }

    /// spawn the announce exchange for one tracker entry.
    fn spawn_announce(
        &mut self,
        ih: Sha1Hash,
        url: String,
        tracker_id: String,
        params: bitdaemon_net::tracker_client::AnnounceParams,
    ) {
        let tx = self.announce_tx.clone();
        let connector = self.connector.clone();
        let datagram = self.datagram.clone();
        let demux = self.udp_tracker_demux.clone();
        let resolver = self.resolver.clone();
        let session = Arc::clone(&self.session);
        self.tasks_forget();
        let handle = tokio::spawn(async move {
            let mut params = params;
            params.url = url.clone();
            params.tracker_id = tracker_id.clone().into_bytes();
            let opts = bitdaemon_net::http_tracker::AnnounceOptions::default();
            let result = match bitdaemon_net::parse_url::parse_url_components(&url) {
                Ok(c) if c.protocol == "udp" => {
                    let Some(dg) = datagram else {
                        let _ = tx.send(AnnounceDone {
                            ih,
                            url,
                            result: Err("no datagram transport injected".into()),
                            scrape: false,
                        });
                        return;
                    };
                    let port = u16::try_from(c.port).unwrap_or(80);
                    let v4 = DatagramSocket::local_addr(dg.as_ref()).is_ipv4();
                    match resolve_for_socket(&resolver, &c.hostname, port, v4)
                        .await
                        .and_then(|a| filter_tracker_addr(&params.filter, a))
                    {
                        Ok(addr) => announce_udp(
                            dg.as_ref(),
                            &demux,
                            addr,
                            &params,
                            &opts,
                            std::time::Duration::from_secs(15),
                        )
                        .await
                        .map_err(|e: AnnounceError| e.to_string()),
                        Err(e) => Err(e),
                    }
                }
                Ok(c) if c.protocol == "ws" || c.protocol == "wss" => {
                    // websocket tracker — WebTorrent JSON signaling;
                    // upstream never delivers peer lists here, only
                    // RTC offers + interval/complete stats
                    let port = u16::try_from(c.port).unwrap_or(80);
                    match resolve(&resolver, &c.hostname, port)
                        .await
                        .and_then(|a| filter_tracker_addr(&params.filter, a))
                    {
                        Ok(addr) => match connector.clone().connect(addr).await {
                            Ok(mut s) => {
                                let host = if c.port < 0 {
                                    c.hostname.clone()
                                } else {
                                    format!("{}:{}", c.hostname, port)
                                };
                                let key: String = (0..16)
                                    .map(|_| format!("{:02x}", rand::random::<u8>()))
                                    .collect();
                                let payload =
                                    bitdaemon_net::ws_tracker::build_announce(&params, &[]);
                                let min_interval = slock(&session)
                                    .settings
                                    .min_websocket_announce_interval
                                    .as_secs()
                                    .min(i32::MAX as u64)
                                    as i32;
                                bitdaemon_net::ws_tracker::announce_ws(
                                    &mut s, &host, &c.path, &key, &payload,
                                )
                                .await
                                .map_err(|e| e.to_string())
                                .and_then(|r| {
                                    // upstream `on_read`: a `failure
                                    // reason` reports the announce as
                                    // failed — `tracker_request_error`
                                    // (`0ee23cc89`). Frames without
                                    // info_hash are filtered inside
                                    // `announce_ws` (`73f4832de`).
                                    if let Some(reason) = r.failure_reason {
                                        return Err(reason);
                                    }
                                    Ok(bitdaemon_net::http_tracker::TrackerResponse {
                                        // upstream clamps the reply's
                                        // interval to
                                        // `min_websocket_announce_interval`
                                        interval: (r.interval as i32).max(min_interval),
                                        min_interval: r.min_interval as i32,
                                        complete: r.complete,
                                        incomplete: r.incomplete,
                                        downloaded: r.downloaded,
                                        ..Default::default()
                                    })
                                })
                            }
                            Err(e) => Err(format!("connect: {e}")),
                        },
                        Err(e) => Err(e),
                    }
                }
                Ok(c) => {
                    // http/https — dial a stream; https wraps it in
                    // rustls with the OS root store (upstream's
                    // `m_ssl_ctx` built from the system trust DB).
                    let https = c.protocol == "https";
                    let default_port = if https { 443 } else { 80 };
                    let port = u16::try_from(c.port).unwrap_or(default_port);
                    match resolve(&resolver, &c.hostname, port)
                        .await
                        .and_then(|a| filter_tracker_addr(&params.filter, a))
                    {
                        Ok(addr) => match connector.clone().connect(addr).await {
                            Ok(s) => {
                                let mut s: BoxedPeerStream = if https {
                                    let cfg = match tracker_tls_config() {
                                        Some(c) => c,
                                        None => {
                                            let _ = tx.send(AnnounceDone {
                                                ih,
                                                url,
                                                result: Err(
                                                    "tls: crypto provider unavailable".to_string()
                                                ),
                                                scrape: false,
                                            });
                                            return;
                                        }
                                    };
                                    match bitdaemon_net::ssl_stream::ssl_connect(
                                        s,
                                        &c.hostname,
                                        addr,
                                        cfg,
                                    )
                                    .await
                                    {
                                        Ok(t) => Box::new(t),
                                        Err(e) => {
                                            let _ = tx.send(AnnounceDone {
                                                ih,
                                                url,
                                                result: Err(format!("tls: {e}")),
                                                scrape: false,
                                            });
                                            return;
                                        }
                                    }
                                } else {
                                    s
                                };
                                announce_http(&mut s, &params, &opts, &[], 1024 * 1024)
                                    .await
                                    .map_err(|e| e.to_string())
                            }
                            Err(e) => Err(format!("connect: {e}")),
                        },
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(format!("bad tracker url {url}: {e}")),
            };
            // dict-style `peers` may carry hostnames — upstream
            // `on_peer_name_lookup` resolves them and feeds
            // `add_peer` (the ip_filter gate runs downstream in
            // `add_swarm_peer`, same as upstream).
            let mut result = result;
            if let Ok(r) = &mut result {
                for hp in r.hostname_peers.clone() {
                    if let Ok(a) = resolve(&resolver, &hp.ip, hp.port).await {
                        r.peers.push(a);
                    }
                }
            }
            let _ = tx.send(AnnounceDone {
                ih,
                url,
                result,
                scrape: false,
            });
        });
        self.tasks.push(handle);
    }

    /// `scrape_request` — one scrape exchange (http scrape-url or
    /// udp action 2), result routed to `on_scrape_response`.
    fn spawn_scrape(&mut self, ih: Sha1Hash, url: String) {
        let tx = self.announce_tx.clone();
        let connector = self.connector.clone();
        let datagram = self.datagram.clone();
        let demux = self.udp_tracker_demux.clone();
        let resolver = self.resolver.clone();
        self.tasks_forget();
        let handle = tokio::spawn(async move {
            let result = match bitdaemon_net::parse_url::parse_url_components(&url) {
                Ok(c) if c.protocol == "udp" => {
                    let Some(dg) = datagram else {
                        let _ = tx.send(AnnounceDone {
                            ih,
                            url,
                            result: Err("no datagram transport injected".into()),
                            scrape: true,
                        });
                        return;
                    };
                    let port = u16::try_from(c.port).unwrap_or(80);
                    let v4 = DatagramSocket::local_addr(dg.as_ref()).is_ipv4();
                    match resolve_for_socket(&resolver, &c.hostname, port, v4).await {
                        Ok(addr) => bitdaemon_net::tracker_client::scrape_udp(
                            dg.as_ref(),
                            &demux,
                            addr,
                            ih.as_bytes(),
                            std::time::Duration::from_secs(15),
                        )
                        .await
                        .map(|r| bitdaemon_net::http_tracker::TrackerResponse {
                            complete: i64::from(r.complete),
                            incomplete: i64::from(r.incomplete),
                            downloaded: i64::from(r.downloaded),
                            ..Default::default()
                        })
                        .map_err(|e: AnnounceError| e.to_string()),
                        Err(e) => Err(e),
                    }
                }
                Ok(_) => {
                    // http — derive scrape url (`scrape_not_available`
                    // upstream → hard fail)
                    let Some(scrape) = bitdaemon_net::http_tracker::scrape_url(&url) else {
                        let _ = tx.send(AnnounceDone {
                            ih,
                            url,
                            result: Err("scrape not available".into()),
                            scrape: true,
                        });
                        return;
                    };
                    let Ok(c) = bitdaemon_net::parse_url::parse_url_components(&scrape) else {
                        let _ = tx.send(AnnounceDone {
                            ih,
                            url,
                            result: Err("bad scrape url".into()),
                            scrape: true,
                        });
                        return;
                    };
                    let port = u16::try_from(c.port).unwrap_or(80);
                    match resolve(&resolver, &c.hostname, port).await {
                        Ok(addr) => match connector.clone().connect(addr).await {
                            Ok(mut s) => bitdaemon_net::tracker_client::scrape_http(
                                &mut s,
                                &scrape,
                                ih.as_bytes(),
                                &[],
                                1024 * 1024,
                            )
                            .await
                            .map_err(|e| e.to_string()),
                            Err(e) => Err(format!("connect: {e}")),
                        },
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(format!("bad tracker url {url}: {e}")),
            };
            let _ = tx.send(AnnounceDone {
                ih,
                url,
                result,
                scrape: true,
            });
        });
        self.tasks.push(handle);
    }

    /// `web_peer_connection` driver — dial the seed host through
    /// the injected connector, issue one GET per block via
    /// `build_piece_request`/`fetch_block`, then feed every block
    /// back into `Session::on_web_seed_block` (same disk path a
    /// wire `Piece` takes).
    fn spawn_web_seed(
        &mut self,
        ih: Sha1Hash,
        url: String,
        files: std::sync::Arc<bitdaemon_core::FileStorage>,
        piece_length: i64,
        single_file: bool,
        reqs: Vec<PeerRequest>,
    ) {
        use bitdaemon_net::web_seed::{WebSeedHeaders, fetch_block};
        let tx = self.webseed_tx.clone();
        let connector = self.connector.clone();
        let resolver = self.resolver.clone();
        // upstream `add_headers` sends `User-Agent` on the first
        // request (or always with `always_send_user_agent`) unless
        // anonymous_mode is on.
        let session = Arc::clone(&self.session);
        let (user_agent, anonymous, always_ua, ip_filter) = {
            let s = slock(&self.session);
            (
                String::from_utf8_lossy(&s.settings.user_agent).into_owned(),
                s.settings.anonymous_mode,
                s.settings.always_send_user_agent,
                // `connect_web_seed`'s resolved-endpoint filter —
                // `m_ip_filter` applies to web seeds unconditionally
                // (no `m_apply_ip_filter` gate upstream).
                s.torrents.get(&ih).and_then(|t| t.ip_filter.clone()),
            )
        };
        self.tasks_forget();
        let handle = tokio::spawn(async move {
            // report the completed blocks, then release the rest —
            // mirrors `web_peer_connection` keeping finished file
            // requests while retrying the failed tail on a fresh
            // connection (`maybe_reconnect`).
            macro_rules! report {
                ($done:expr, $rest:expr) => {{
                    if !$done.is_empty() {
                        let _ = tx.send(WebSeedDone {
                            ih,
                            result: Ok($done),
                        });
                    }
                    if !$rest.is_empty() {
                        let _ = tx.send(WebSeedDone {
                            ih,
                            result: Err($rest),
                        });
                    }
                }};
            }
            let Ok(c) = bitdaemon_net::parse_url::parse_url_components(&url) else {
                report!(Vec::<(PeerRequest, Vec<u8>)>::new(), reqs);
                return;
            };
            let mut done: Vec<(PeerRequest, Vec<u8>)> = Vec::with_capacity(reqs.len());
            let port = u16::try_from(c.port).unwrap_or(80);
            let addr = match resolve(&resolver, &c.hostname, port).await {
                Ok(a) => a,
                Err(_) => {
                    report!(Vec::<(PeerRequest, Vec<u8>)>::new(), reqs);
                    return;
                }
            };
            // upstream `on_name_lookup`/`connect_web_seed` — a
            // filtered resolved address posts `peer_blocked_alert`
            // and aborts the connect.
            if ip_filter
                .as_ref()
                .is_some_and(|f| f.access(addr.ip()) == bitdaemon_net::ip_filter::BLOCKED)
            {
                slock(&session).alerts.post(Alert::PeerBlocked {
                    ih,
                    remote: addr.to_string(),
                    reason: "ip_filter".into(),
                });
                report!(Vec::<(PeerRequest, Vec<u8>)>::new(), reqs);
                return;
            }
            // `format_host_header` — omit the default http port
            let host =
                bitdaemon_net::string_util::format_host_header(c.hostname.clone(), c.port, 80);
            let mut headers = WebSeedHeaders {
                host,
                user_agent,
                always_send_user_agent: always_ua,
                anonymous_mode: anonymous,
                ..Default::default()
            };
            // URL-embedded credentials → `Authorization: Basic
            // <b64>` (upstream `m_basic_auth`); an explicit
            // web_seed_entry `auth` would take precedence.
            if !c.auth.is_empty() {
                headers.authorization = format!(
                    "Basic {}",
                    String::from_utf8_lossy(&bitdaemon_core::escape_string::base64encode(
                        c.auth.as_bytes()
                    ))
                );
            }
            // conn state per base — `web_seed_t` upstream is one
            // connection per seed; 3xx redirects add ephemeral seeds
            // (`web_seed_t::redirects` routes per file).
            let default_base = bitdaemon_net::parse_url::split_url(&url)
                .map(|(b, _)| b)
                .unwrap_or_else(|_| url.clone());
            let mut bases: std::collections::HashMap<String, SocketAddr> =
                [(default_base.clone(), addr)].into_iter().collect();
            let mut conns: std::collections::HashMap<String, BoxedPeerStream> =
                std::collections::HashMap::new();
            let mut redirects: std::collections::HashMap<
                usize,
                bitdaemon_net::web_seed::RedirectTarget,
            > = std::collections::HashMap::new();
            let mut num_redirects = 0usize;
            let mut i = 0usize;
            'outer: while i < reqs.len() {
                let r = reqs[i];
                let groups = bitdaemon_net::web_seed::plan_piece_request(
                    &files,
                    piece_length,
                    r.piece,
                    i64::from(r.start),
                    i64::from(r.length),
                    &url,
                    &c.path,
                    single_file,
                    &redirects,
                );
                let mut data = Vec::with_capacity(r.length as usize);
                for g in &groups {
                    // `Connection: close` (incl. implicit HTTP/1.0)
                    // invalidates the stream — upstream reconnects
                    // per request batch; a fresh conn restarts
                    // `m_first_request`.
                    let first = !conns.contains_key(&g.base);
                    if first {
                        let Some(&baddr) = bases.get(&g.base) else {
                            continue;
                        };
                        match connector.clone().connect(baddr).await {
                            Ok(s) => {
                                conns.insert(g.base.clone(), s);
                            }
                            Err(_) => {
                                report!(done, reqs[i..].to_vec());
                                return;
                            }
                        }
                    }
                    let wreq = bitdaemon_net::web_seed::render_web_request(
                        &files, g, &headers, false, first,
                    );
                    let Some(s) = conns.get_mut(&g.base) else {
                        report!(done, reqs[i..].to_vec());
                        return;
                    };
                    match fetch_block(s, &wreq, 4 * 1024 * 1024).await {
                        Ok((segs, close)) => {
                            if close {
                                conns.remove(&g.base);
                            }
                            // reassemble in `m_file_requests` order —
                            // pad slices contribute zeroes
                            let mut it = segs.into_iter();
                            for f in &g.files {
                                if f.got_get {
                                    if let Some((_, _, b)) = it.next() {
                                        data.extend_from_slice(&b);
                                    }
                                } else {
                                    data.resize(data.len() + f.length as usize, 0);
                                }
                            }
                        }
                        Err(bitdaemon_net::web_seed::WebSeedError::Redirect {
                            file_index,
                            location,
                        }) => {
                            conns.remove(&g.base);
                            num_redirects += 1;
                            if num_redirects > 64 {
                                report!(done, reqs[i..].to_vec());
                                return;
                            }
                            // upstream `handle_redirect`:
                            // resolve → maybe_url_encode → split_url
                            // → new ephemeral seed carrying
                            // `redirects[file_index]`; auth only on
                            // same origin.
                            let loc = bitdaemon_net::parse_url::maybe_url_encode(
                                &bitdaemon_net::http_parser::resolve_redirect_location(
                                    &url, &location,
                                ),
                            );
                            let Ok((rbase, rpath)) = bitdaemon_net::parse_url::split_url(&loc)
                            else {
                                report!(done, reqs[i..].to_vec());
                                return;
                            };
                            let Ok(rc) = bitdaemon_net::parse_url::parse_url_components(&loc)
                            else {
                                report!(done, reqs[i..].to_vec());
                                return;
                            };
                            let rport = u16::try_from(rc.port).unwrap_or(80);
                            let Ok(raddr) = resolve(&resolver, &rc.hostname, rport).await else {
                                report!(done, reqs[i..].to_vec());
                                return;
                            };
                            let auth = if bitdaemon_net::parse_url::same_origin(&url, &loc) {
                                headers.authorization.clone()
                            } else {
                                String::new()
                            };
                            // single-file: the location is the seed
                            // URL verbatim; multi-file: the seed is
                            // the base, the path lives in redirects.
                            let target_url = if single_file {
                                loc.clone()
                            } else {
                                rbase.clone()
                            };
                            redirects.insert(
                                file_index,
                                bitdaemon_net::web_seed::RedirectTarget {
                                    url: target_url,
                                    path: rpath,
                                    base: rbase.clone(),
                                    authorization: auth,
                                },
                            );
                            bases.entry(rbase).or_insert(raddr);
                            continue 'outer;
                        }
                        Err(_) => {
                            report!(done, reqs[i..].to_vec());
                            return;
                        }
                    }
                }
                done.push((r, data));
                i += 1;
            }
            report!(done, Vec::<PeerRequest>::new());
        });
        self.tasks.push(handle);
    }

    /// `natpmp` start — map `local` (the listen port) UDP+TCP
    /// through the gateway over the injected datagram transport.
    /// Upstream queries the default gateway from the routing
    /// table; injected transports pass it in.
    pub async fn start_natpmp(&mut self, gateway: SocketAddr, local_port: u16, ttl: u32) {
        use bitdaemon_net::natpmp::{PortmapProtocol, build_map_request};
        if slock(&self.session).settings.anonymous_mode {
            return; // upstream: no port mapping under anonymity
        }
        let Some(dg) = self.datagram.clone() else {
            return;
        };
        self.natpmp = Some(NatpmpState {
            gateway,
            mappings: vec![
                (PortmapProtocol::Tcp, local_port, 0, Instant::now()),
                (PortmapProtocol::Udp, local_port, 0, Instant::now()),
            ],
            ttl,
        });
        for proto in [PortmapProtocol::Tcp, PortmapProtocol::Udp] {
            let _ = dg
                .send_to(&build_map_request(proto, local_port, 0, ttl), gateway)
                .await;
        }
        let _ = dg
            .send_to(&bitdaemon_net::natpmp::build_ip_request(), gateway)
            .await;
    }

    /// `natpmp` auto-start (ADR-0019): discover the default IPv4
    /// gateway via `enum_net` and map the listen port TCP+UDP.
    /// No-op when the service is off, anonymous mode is on, no
    /// gateway exists, or the state is already running.
    pub async fn start_natpmp_auto(&mut self, local_port: u16, ttl: u32) {
        if self.natpmp.is_some()
            || !slock(&self.session).services.natpmp
            || slock(&self.session).settings.anonymous_mode
        {
            return;
        }
        let Some(gw) = bitdaemon_net::enum_net::default_gateway_v4() else {
            return;
        };
        self.start_natpmp(SocketAddr::new(gw, 5351), local_port, ttl)
            .await;
    }

    /// `natpmp::on_reply` — update mapping state, feed `ip_voter`
    /// with the router's view of our public IP.
    fn on_natpmp_response(&mut self, buf: &[u8]) {
        use bitdaemon_net::natpmp::NatPmpResponse;
        let Some(r) = bitdaemon_net::natpmp::parse_response(buf) else {
            return;
        };
        let now = Instant::now();
        match r {
            NatPmpResponse::PublicIp(ip) => {
                slock(&self.session).vote_external(
                    std::net::IpAddr::V4(ip),
                    bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_ROUTER,
                    &std::net::IpAddr::V4(ip),
                );
            }
            NatPmpResponse::UnsupportedVersion => {
                // `pcp_unsupp_version` → upstream resends the
                // outstanding requests as NAT-PMP on a v4 socket.
                // We only ever send NAT-PMP, so the fallback is a
                // plain re-send of the pending mappings.
                if let Some(n) = &self.natpmp
                    && let Some(dg) = self.datagram.clone()
                {
                    let gw = n.gateway;
                    let ttl = n.ttl;
                    let resends: Vec<_> = n.mappings.iter().map(|m| (m.0, m.1, m.2)).collect();
                    for (proto, local, ext) in resends {
                        let req = bitdaemon_net::natpmp::build_map_request(proto, local, ext, ttl);
                        let dg = dg.clone();
                        self.tasks.push(tokio::spawn(async move {
                            let _ = dg.send_to(&req, gw).await;
                        }));
                    }
                }
            }
            NatPmpResponse::Map {
                protocol,
                private_port,
                public_port,
                lifetime,
                result,
                ..
            } => {
                if let Some(n) = &mut self.natpmp {
                    // upstream matches the reply to the outstanding
                    // request by opcode-derived proto + private port
                    // (PCP adds a nonce match — we only send NAT-PMP)
                    if let Some(m) = n
                        .mappings
                        .iter_mut()
                        .find(|m| m.0 == protocol && m.1 == private_port)
                    {
                        if result == 0 {
                            m.2 = public_port;
                            // renew at half the lease (upstream
                            // `mapping.expires` semantics)
                            m.3 = now + std::time::Duration::from_secs(u64::from(lifetime / 2));
                        }
                    }
                    let local_addr = n.gateway.ip().to_string();
                    // `on_port_mapping`/`return_error` upstream —
                    // surface the result as portmap alerts + a
                    // `port_mapping_t` handle.
                    let proto_u8 = match protocol {
                        bitdaemon_net::natpmp::PortmapProtocol::Tcp => 1,
                        bitdaemon_net::natpmp::PortmapProtocol::Udp => 2,
                    };
                    if result == 0 {
                        slock(&self.session).on_portmap_success(
                            0,
                            proto_u8,
                            private_port,
                            public_port,
                            local_addr,
                            0,
                        );
                    } else {
                        slock(&self.session).on_portmap_error(
                            0,
                            -1,
                            local_addr,
                            format!("nat-pmp map failed (code {result})"),
                        );
                    }
                }
            }
        }
    }

    /// `start_upnp` (ADR-0018) — emit the SSDP M-SEARCH burst on the
    /// session datagram socket. Unicast replies arrive on the demux;
    /// `LOCATION` fetches run through the injected connector.
    /// No-op when the service is off or anonymous mode is on.
    pub async fn start_upnp(&mut self) {
        if !slock(&self.session).services.upnp || slock(&self.session).settings.anonymous_mode {
            return;
        }
        if self.datagram.is_none() {
            return;
        }
        self.send_msearch().await;
        // `resend_request` upstream: retry at `2*retry_count` s while
        // `retry < 12 && (no devices || retry < 4)`.
        self.ssdp_retry_count = 1;
        self.ssdp_next = Some(Instant::now() + std::time::Duration::from_secs(2));
    }

    /// emit one M-SEARCH (upstream sends on the multicast *and*
    /// unicast sockets; we have the single injected datagram).
    async fn send_msearch(&self) {
        let Some(dg) = self.datagram.clone() else {
            return;
        };
        let target = SocketAddr::new(
            std::net::IpAddr::V4(bitdaemon_net::upnp::SSDP_ADDR),
            bitdaemon_net::upnp::SSDP_PORT,
        );
        let _ = dg
            .send_to(bitdaemon_net::upnp::msearch_request().as_bytes(), target)
            .await;
    }

    /// `resend_request` — the broadcast-timer arm of discovery:
    /// re-send M-SEARCH on the growing backoff until a device is
    /// found (≤4 sends once one is) or 12 tries pass with none
    /// (upstream then `disable`s — nothing to kill here beyond the
    /// retry timer itself). Called from `tick`.
    async fn upnp_ssdp_tick(&mut self, now: Instant) {
        let Some(due) = self.ssdp_next else {
            return;
        };
        if now < due {
            return;
        }
        let devices_empty = self.ssdp_seen.is_empty();
        if self.ssdp_retry_count < 12 && (devices_empty || self.ssdp_retry_count < 4) {
            if slock(&self.session).services.upnp && !slock(&self.session).settings.anonymous_mode {
                self.send_msearch().await;
            }
            self.ssdp_retry_count += 1;
            self.ssdp_next =
                Some(now + std::time::Duration::from_secs(2 * u64::from(self.ssdp_retry_count)));
        } else {
            // upstream `disable(errors::no_router)` when nothing
            // ever answered
            self.ssdp_next = None;
        }
    }

    /// fetch the SSDP `LOCATION` device description over the injected
    /// connector, then hand the parsed `RootDevice` to the tick.
    fn spawn_ssdp_fetch(&mut self, location: String) {
        let tx = self.upnp_dev_tx.clone();
        let connector = self.connector.clone();
        let resolver = self.resolver.clone();
        self.tasks_forget();
        self.tasks.push(tokio::spawn(async move {
            // `on_reply` upstream: reject non-http schemes, port 0
            // and invalid/unroutable hosts before fetching.
            let Some((host, port, path)) = bitdaemon_net::upnp::validate_location(&location) else {
                return;
            };
            let Ok(addr) = resolve(&resolver, &host, port).await else {
                return;
            };
            // upstream reads `c.socket().local_endpoint()` on the
            // control connection for `NewInternalClient` — the
            // connect-probe yields the same source address without
            // exposing stream internals.
            let local_addr = bitdaemon_net::os_transports::local_addr_to(addr)
                .ok()
                .map(|a| a.ip());
            let Ok(mut s) = connector.connect(addr).await else {
                return;
            };
            let r = async {
                bitdaemon_net::http_conn::send_request(&mut s, "GET", &path, &host, &[], &[])
                    .await?;
                bitdaemon_net::http_conn::recv_response(&mut s, 256 * 1024).await
            }
            .await;
            let Ok(resp) = r else { return };
            // upstream: `p.status_code() != 200` → `d.disabled`
            if resp.status() != 200 {
                return;
            }
            let body = String::from_utf8_lossy(&resp.body()).into_owned();
            let mut dev = bitdaemon_net::upnp::parse_device_description(&body);
            dev.url = location.clone();
            dev.local_addr = local_addr;
            // `resolve_control_url` also fills hostname/port/path and
            // rejects empty/unusable/unsafe control URLs (`disabled`).
            if !bitdaemon_net::upnp::resolve_control_url(&mut dev, &location) {
                return;
            }
            tracing::info!(model = %dev.model, control = %dev.control_url, "upnp: IGD discovered");
            let _ = tx.send(dev);
        }));
    }

    /// `stop_upnp` — `upnp::close()`: queue `DeletePortMapping` for
    /// mapped slots (pending adds die silently), then drop the
    /// device list.
    pub fn stop_upnp(&mut self) {
        for idx in 0..self.upnp_devices.len() {
            let outs = self.upnp_devices[idx].close();
            self.drive_upnp_outs(idx, outs);
        }
        self.upnp_devices.clear();
        self.ssdp_seen.clear();
        self.ssdp_next = None;
    }

    /// `upnp` device registration — a discovered `RootDevice`
    /// (control_url/service namespace known). Maps the listen port
    /// TCP+UDP like `portmap` upstream. Skipped under
    /// `anonymous_mode`.
    pub fn portmap_add_device(
        &mut self,
        dev: bitdaemon_net::upnp::RootDevice,
        local_port: u16,
        description: String,
    ) {
        use bitdaemon_net::upnp::{PortmapProtocol, UpnpDriver};
        if slock(&self.session).settings.anonymous_mode {
            return;
        }
        let local_addr = dev.local_addr.map(|a| a.to_string()).unwrap_or_default();
        let d = UpnpDriver::new(dev, 3600, description);
        self.upnp_devices.push(d);
        let idx = self.upnp_devices.len() - 1;
        // ask for the router's external address first (upstream
        // chains discovery → GetExternalIPAddress → AddPortMapping).
        // `discover()` sets `pending` — the `set_mappings` posts stay
        // queued until the reply lands (one connection per device).
        let get_ip = self.upnp_devices[idx].discover();
        let queued = self.upnp_devices[idx].set_mappings(vec![
            (
                PortmapProtocol::Tcp,
                local_port,
                local_port,
                local_addr.clone(),
            ),
            (PortmapProtocol::Udp, local_port, local_port, local_addr),
        ]);
        let outs = std::iter::once(get_ip).chain(queued).collect::<Vec<_>>();
        self.drive_upnp_outs(idx, outs);
    }

    /// execute `UpnpOut` actions — SOAP posts ride the injected
    /// connector + resolver (no direct sockets).
    fn drive_upnp_outs(&mut self, dev: usize, outs: Vec<bitdaemon_net::upnp::UpnpOut>) {
        use bitdaemon_net::upnp::UpnpOut;
        for o in outs {
            match o {
                UpnpOut::Post {
                    mapping,
                    kind,
                    request,
                } => {
                    debug!(dev, mapping, ?kind, "upnp soap post");
                    self.spawn_upnp_post(dev, mapping, kind, request);
                }
                UpnpOut::Mapped {
                    mapping,
                    external_ip,
                    external_port,
                    proto,
                } => {
                    if let Some(ip) = external_ip {
                        slock(&self.session).vote_external(
                            ip,
                            bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_ROUTER,
                            &ip,
                        );
                    }
                    // `on_port_mapping` — `portmap_alert` + handle.
                    let m = self
                        .upnp_devices
                        .get(dev)
                        .and_then(|d| d.mappings.get(mapping));
                    if let Some(m) = m {
                        let proto_u8 = match proto {
                            bitdaemon_net::upnp::PortmapProtocol::Tcp => 1,
                            bitdaemon_net::upnp::PortmapProtocol::Udp => 2,
                        };
                        slock(&self.session).on_portmap_success(
                            1,
                            proto_u8,
                            m.local_port,
                            external_port,
                            m.local_addr.clone(),
                            dev,
                        );
                    }
                }
                UpnpOut::MapError { mapping, code } => {
                    // `return_error` → `portmap_error_alert` +
                    // `portmap_log_alert` (upstream logs both).
                    let local_addr = self
                        .upnp_devices
                        .get(dev)
                        .map(|d| d.dev.model.clone())
                        .unwrap_or_default();
                    slock(&self.session).on_portmap_error(
                        1,
                        mapping as i32,
                        local_addr.clone(),
                        format!("upnp map failed (code {code})"),
                    );
                    slock(&self.session).portmap_log(
                        1,
                        local_addr,
                        format!("AddPortMapping failed: UPnP error {code}"),
                    );
                }
                UpnpOut::Unmapped {
                    mapping: _,
                    proto: _,
                    code,
                } => {
                    // `on_upnp_unmap_response` fires the callback with
                    // an error code *always* upstream (a clean delete
                    // reports `error_code(-1, upnp_category())` — the
                    // session treats it as informational).
                    let model = self
                        .upnp_devices
                        .get(dev)
                        .map(|d| d.dev.model.clone())
                        .unwrap_or_default();
                    slock(&self.session).portmap_log(
                        1,
                        model,
                        format!("DeletePortMapping finished (code {code})"),
                    );
                }
            }
        }
    }

    /// one SOAP POST through the injected transport — `request` is
    /// sent verbatim (upstream writes `post()`'s exact bytes into
    /// `m_sendbuffer`), the reply (status/content-type/body/
    /// header-finished) comes back through `upnp_tx`.
    fn spawn_upnp_post(
        &mut self,
        dev: usize,
        mapping: usize,
        kind: bitdaemon_net::upnp::SoapKind,
        request: String,
    ) {
        let Some(d) = self.upnp_devices.get(dev) else {
            return;
        };
        let host = d.dev.hostname.clone();
        let port = u16::try_from(d.dev.port).unwrap_or(80);
        let tx = self.upnp_tx.clone();
        let connector = self.connector.clone();
        let resolver = self.resolver.clone();
        self.tasks_forget();
        self.tasks.push(tokio::spawn(async move {
            let send = |b: Result<(i32, Option<String>, String, bool), String>| async move {
                let _ = tx.send(UpnpReply {
                    dev,
                    mapping,
                    kind,
                    reply: b,
                });
            };
            let Ok(addr) = resolve(&resolver, &host, port).await else {
                send(Err("resolve".into())).await;
                return;
            };
            let Ok(mut s) = connector.connect(addr).await else {
                send(Err("connect".into())).await;
                return;
            };
            let r = async {
                use tokio::io::AsyncWriteExt;
                s.write_all(request.as_bytes()).await?;
                bitdaemon_net::http_conn::recv_response(&mut s, 64 * 1024).await
            }
            .await;
            match r {
                Ok(resp) => {
                    send(Ok((
                        resp.status(),
                        resp.header("content-type").map(str::to_string),
                        String::from_utf8_lossy(&resp.body()).into_owned(),
                        resp.parser.header_finished(),
                    )))
                    .await;
                }
                Err(e) => send(Err(e.to_string())).await,
            }
        }));
    }

    /// feed a SOAP response back into the driver's state machine —
    /// the reply handler is the one upstream bound when the request
    /// went out (`kind`), not a guess from the payload.
    fn on_upnp_reply(&mut self, r: UpnpReply) {
        use bitdaemon_net::upnp::SoapReply;
        let reply = match &r.reply {
            Err(e) => {
                debug!(err = %e, "upnp soap post failed");
                SoapReply::TransportError
            }
            Ok((status, ct, body, header_finished)) => {
                debug!(dev = r.dev, kind = ?r.kind, body = %body.get(..160).unwrap_or(body), "upnp soap reply");
                if *header_finished {
                    SoapReply::Full {
                        status: *status,
                        content_type: ct.as_deref(),
                        body,
                    }
                } else {
                    SoapReply::Incomplete
                }
            }
        };
        let outs = self.upnp_devices.get_mut(r.dev).map(|d| match r.kind {
            bitdaemon_net::upnp::SoapKind::GetIp => d.on_ip_response(match reply {
                SoapReply::Full {
                    status: 200, body, ..
                } => Some(body),
                _ => None,
            }),
            bitdaemon_net::upnp::SoapKind::Add => {
                d.on_map_response(r.mapping, reply, rand::random::<u16>())
            }
            bitdaemon_net::upnp::SoapKind::Delete => d.on_unmap_response(r.mapping, reply),
        });
        if let Some(outs) = outs {
            self.drive_upnp_outs(r.dev, outs);
        }
    }

    /// `session_impl::add_port_mapping` — forward a user mapping
    /// to both drivers. NAT-PMP gets a new `(proto,local,ext)`
    /// slot; UPnP appends a mapping per discovered device.
    /// Results land via `on_portmap_success`/`on_portmap_error`.
    pub async fn portmap_add(&mut self, protocol: u8, external_port: u16, local_port: u16) {
        use bitdaemon_net::natpmp::PortmapProtocol as Np;
        let proto_np = match protocol {
            2 => Np::Udp,
            _ => Np::Tcp,
        };
        if let Some(n) = &mut self.natpmp
            && let Some(dg) = self.datagram.clone()
        {
            n.mappings
                .push((proto_np, local_port, external_port, Instant::now()));
            let _ = dg
                .send_to(
                    &bitdaemon_net::natpmp::build_map_request(
                        proto_np,
                        local_port,
                        external_port,
                        n.ttl,
                    ),
                    n.gateway,
                )
                .await;
        }
        for dev in 0..self.upnp_devices.len() {
            let proto_up = match protocol {
                2 => bitdaemon_net::upnp::PortmapProtocol::Udp,
                _ => bitdaemon_net::upnp::PortmapProtocol::Tcp,
            };
            // `add_mapping` → Option: None when the slot table is
            // full (upstream returns -1) or the device is disabled
            if let Some((_idx, outs)) = self.upnp_devices[dev].add_mapping(
                proto_up,
                external_port,
                local_port,
                String::new(),
            ) {
                self.drive_upnp_outs(dev, outs);
            }
        }
    }

    /// `session_impl::delete_port_mapping` — NAT-PMP: ttl=0
    /// request (the RFC unmap); UPnP: `DeletePortMapping` on the
    /// slot matching `(proto, ext)`.
    pub async fn portmap_delete(&mut self, transport: u8, protocol: u8, external_port: u16) {
        match transport {
            0 => {
                use bitdaemon_net::natpmp::PortmapProtocol as Np;
                let proto = match protocol {
                    2 => Np::Udp,
                    _ => Np::Tcp,
                };
                if let Some(n) = &mut self.natpmp
                    && let Some(dg) = self.datagram.clone()
                    && let Some(pos) = n
                        .mappings
                        .iter()
                        .position(|m| m.0 == proto && m.2 == external_port)
                {
                    let local = n.mappings[pos].1;
                    let _ = dg
                        .send_to(
                            &bitdaemon_net::natpmp::build_map_request(proto, local, 0, 0),
                            n.gateway,
                        )
                        .await;
                    n.mappings.remove(pos);
                }
            }
            _ => {
                let proto = match protocol {
                    2 => bitdaemon_net::upnp::PortmapProtocol::Udp,
                    _ => bitdaemon_net::upnp::PortmapProtocol::Tcp,
                };
                for dev in 0..self.upnp_devices.len() {
                    let idx = self.upnp_devices[dev].mappings.iter().position(|m| {
                        m.protocol == Some(proto) && m.external_port == external_port
                    });
                    if let Some(i) = idx {
                        let outs = self.upnp_devices[dev].delete_mapping(i);
                        self.drive_upnp_outs(dev, outs);
                    }
                }
            }
        }
    }

    /// `natpmp` renew — upstream remaps at half the lease. Called
    /// from `tick`; resends the 12-byte map request per proto.
    async fn natpmp_renew(&mut self) {
        use bitdaemon_net::natpmp::build_map_request;
        let Some(n) = &self.natpmp else {
            return;
        };
        let Some(dg) = self.datagram.clone() else {
            return;
        };
        let now = Instant::now();
        let gw = n.gateway;
        let ttl = n.ttl;
        // upstream renews at half the lease — the stored deadline
        // already encodes it (set to `lifetime/2` on response).
        let due: Vec<_> = n.mappings.iter().filter(|m| m.3 <= now).copied().collect();
        for (proto, local, _ext, _exp) in due {
            let _ = dg
                .send_to(&build_map_request(proto, local, 0, ttl), gw)
                .await;
        }
    }

    /// drop finished handles.
    fn tasks_forget(&mut self) {
        self.tasks.retain(|t| !t.is_finished());
    }

    /// `torrent::connect_to_peer` — dial one candidate, run the BT
    /// handshake, register the `PeerConn` and spawn its io tasks.
    pub async fn connect_peer(
        &mut self,
        ih: &Sha1Hash,
        remote: SocketAddr,
        encrypted: bool,
    ) -> std::io::Result<u64> {
        // `num_peers_half_open` — conn exists (upstream ctor) from
        // dial start until the handshake completes or fails.
        slock(&self.session)
            .counters
            .inc(bitdaemon_session::counters::Counter::NumPeersHalfOpen);
        let s_res = self.connector.clone().connect(remote).await;
        let mut s = match s_res {
            Ok(s) => s,
            Err(e) => {
                let mut ses = slock(&self.session);
                ses.counters
                    .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                ses.peer_connect_failed(ih, remote, false);
                return Err(std::io::Error::other(e));
            }
        };
        let (pid, dht, enc_level) = {
            let ses = slock(&self.session);
            (
                ses.peer_id,
                ses.settings.enable_dht,
                u32::from(ses.pe_settings.allowed_enc_level),
            )
        };
        // upstream `peer_connection` always advertises the FAST
        // extension (BEP-6); DHT bit follows `enable_dht`.
        let fast = true;
        // `bt_peer_connection::start` — the encrypted flag selects
        // the MSE path; the negotiated RC4 state follows the conn
        // into the io pump.
        let mut mse_rc4 = None;
        // `set_pid` — the remote's peer_id from its handshake rides
        // into the conn (pid dedup + self-connection checks).
        let remote_pid;
        if encrypted {
            match bitdaemon_net::bt_peer_conn::run_handshake_encrypted_initiator(
                &mut s, ih, &pid, dht, fast, enc_level, 0,
            )
            .await
            {
                Ok((hs, mse)) => {
                    remote_pid = Some(hs.peer_id);
                    if mse.rc4_encrypted {
                        mse_rc4 = Some(mse.rc4);
                    }
                }
                Err(e) => {
                    let mut ses = slock(&self.session);
                    ses.counters
                        .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                    ses.peer_connect_failed(ih, remote, false);
                    return Err(std::io::Error::other(e));
                }
            }
        } else {
            match run_handshake(&mut s, ih, &pid, dht, fast).await {
                Ok(hs) => remote_pid = Some(hs.peer_id),
                Err(e) => {
                    let mut ses = slock(&self.session);
                    ses.counters
                        .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                    ses.peer_connect_failed(ih, remote, false);
                    return Err(std::io::Error::other(e));
                }
            }
        };
        // handshake done — the conn leaves half-open state
        // (`peer_connection::on_connection_complete`).
        slock(&self.session)
            .counters
            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);

        let num_pieces = slock(&self.session)
            .torrents
            .get(ih)
            .map_or(0, |t| t.num_pieces as usize);
        // BEP-10: advertise our extension ids right after the handshake
        let _ = bitdaemon_net::bt_peer_conn::send_extended_handshake(
            &mut s,
            &self.ext_handshake(ih, true, Some(remote.ip())),
            mse_rc4.as_mut(),
        )
        .await;
        let peer = self.next_peer;
        self.next_peer += 1;
        let mut conn = PeerConn::new(
            num_pieces,
            PeerPipeline::new(
                self.queue_time,
                self.request_timeout,
                self.max_out_request_queue,
            ),
            0,
        );
        conn.outgoing = true;
        conn.connecting = false;
        conn.handshake_done = true;
        conn.supports_encryption = encrypted;
        conn.peer_id = remote_pid;
        let mut out = Vec::new();
        slock(&self.session).peer_connected(ih, peer, conn, remote, &mut out);
        self.attach_stream(*ih, peer, remote, s, mse_rc4);
        self.send_msgs_to_peers(out);
        Ok(peer)
    }

    /// `session_impl::apply_settings_pack` — swap settings and
    /// re-arm the engine-side timeouts immediately (rate limits live
    /// on the peer-class channels, updated inside `apply_settings`).
    pub fn apply_settings(&mut self, s: bitdaemon_session::settings::SessionSettings) {
        self.queue_time = s.request_queue_time.as_secs() as i32;
        self.request_timeout = s.request_timeout.as_secs() as i32;
        // `m_max_out_request_queue` reads `max_out_request_queue`,
        // not `max_allowed_in_request_queue` (different knob —
        // upstream keeps them separate).
        self.max_out_request_queue = s.max_out_request_queue.clamp(0, i32::from(u16::MAX)) as u16;
        slock(&self.session).apply_settings(s);
    }

    /// `session_handle` for downstream — shares this engine's
    /// `Session` actor (`SessionHandle::from_shared`).
    #[must_use]
    pub fn handle(&self) -> bitdaemon_session::session_handle::SessionHandle {
        bitdaemon_session::session_handle::SessionHandle::from_shared(self.session.clone())
    }

    /// `session_impl::on_connection_attempt` — a dial job finished:
    /// register the peer + attach io tasks, or report the failure
    /// for the reconnect backoff.
    fn on_dial_done(&mut self, d: DialDone) {
        // the handshake window closed either way — `m_half_open`
        // clears on attach or failure.
        slock(&self.session)
            .counters
            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
        let s = match d.stream {
            Ok(s) => s,
            Err(e) => {
                warn!(remote = ?d.remote, err = e, "dial failed");
                if d.outbound {
                    if let Some(r) = d.remote {
                        slock(&self.session).peer_connect_failed(&d.ih, r, matches!(d.kind, 3 | 9));
                    }
                }
                return;
            }
        };
        let num_pieces = slock(&self.session)
            .torrents
            .get(&d.ih)
            .map_or(0, |t| t.num_pieces as usize);
        let peer = self.next_peer;
        self.next_peer += 1;
        let conn = PeerConn::new(
            num_pieces,
            PeerPipeline::new(
                self.queue_time,
                self.request_timeout,
                self.max_out_request_queue,
            ),
            0,
        );
        let remote = d
            .remote
            .unwrap_or_else(|| SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), 0));
        let mut conn = conn;
        conn.supports_fast = d.fast;
        conn.supports_dht = d.dht;
        // DialDone only fires after the BT handshake — the conn is
        // established; direction drives ut_pex send_peer semantics.
        conn.outgoing = d.outbound;
        conn.connecting = false;
        conn.handshake_done = true;
        // `m_encrypted` → `supports_encryption()` (drives the
        // `pex_encryption` flag + `pi->pe_support` re-arm).
        conn.supports_encryption = d.encrypted;
        conn.peer_id = d.pid;
        // `peer_exceeds_limit` — slack-admitted conns owe an eviction
        // at attach; `sock_type` feeds `set_peer_classes`.
        conn.exceeded_limit = d.exceeded;
        conn.sock_type = d.kind;
        let mut out = Vec::new();
        slock(&self.session).peer_connected(&d.ih, peer, conn, remote, &mut out);
        self.attach_stream(d.ih, peer, remote, s, d.rc4);
        self.send_msgs_to_peers(out);
    }

    /// `torrent::connect_to_peer` (async form) — spawn the dial +
    /// handshake job; the result lands in `connect_rx` and is
    /// applied by the next `tick`. `max_out_request_queue`+ext ids
    /// are snapshotted now.
    pub fn spawn_dial(&mut self, ih: Sha1Hash, remote: SocketAddr, encrypted: bool) {
        // `ip_filter` gate on outbound too (upstream filters at
        // `connect_to_peer` → `peer_blocked_alert`). A refused dial
        // reports `peer_connect_failed` so the candidate mark +
        // `num_connecting` bookkeeping don't leak.
        if slock(&self.session).ip_access(remote.ip()) == bitdaemon_net::ip_filter::BLOCKED {
            slock(&self.session).alerts.post(Alert::PeerBlocked {
                ih,
                remote: remote.to_string(),
                reason: "ip_filter".into(),
            });
            slock(&self.session).peer_connect_failed(&ih, remote, false);
            return;
        }
        let connector = self.connector.clone();
        let tx = self.connect_tx.clone();
        let (pid, dht, ssl_params, peer_proxy, enc_level, utp_wanted, tcp_ok, hs_timeout) = {
            let ses = slock(&self.session);
            let t = ses.torrents.get(&ih);
            let ssl = t.and_then(|t| {
                if !t.is_ssl_torrent() {
                    return None;
                }
                let ti = t.ti.as_ref()?;
                let root: Vec<u8> = ti.ssl_cert().to_vec();
                let name: String = String::from_utf8_lossy(ti.name()).into_owned();
                let cert: Option<String> = t.ssl_pem_pair().map(|(c, _)| c.to_string());
                let key: Option<String> = t.ssl_pem_pair().map(|(_, k)| k.to_string());
                Some((root, name, cert, key))
            });
            // upstream `connect_to_peer`: uTP iff `enable_outgoing_utp`
            // && (`!enable_outgoing_tcp` || the peer flagged
            // `supports_utp`/`confirmed_supports_utp`) — a plain peer
            // gets TCP when both transports are on.
            let peer_utp = t
                .and_then(|t| {
                    t.peer_list
                        .find_peer(remote.ip())
                        .map(|i| &t.peer_list.peers()[i])
                })
                .is_some_and(|p| p.supports_utp || p.confirmed_supports_utp);
            let utp_wanted = self.utp_enabled && (!ses.settings.enable_outgoing_tcp || peer_utp);
            (
                ses.peer_id,
                ses.settings.enable_dht,
                ssl,
                ses.proxies.peer.clone(),
                u32::from(ses.pe_settings.allowed_enc_level),
                utp_wanted,
                ses.settings.enable_outgoing_tcp,
                ses.settings.handshake_timeout,
            )
        };
        let utp = if self.utp_enabled {
            self.utp_connector
                .clone()
                .map(|c| c as Arc<dyn PeerConnector>)
        } else {
            None
        };
        // upstream refuses the dial when TCP is disabled and the
        // peer didn't get a uTP socket.
        if !utp_wanted && !tcp_ok {
            slock(&self.session).peer_connect_failed(&ih, remote, false);
            return;
        }
        // `instantiate_connection` — upstream's transport decision
        // table: uTP first, then I2P, then the proxy-wrapped streams
        // when `proxy_peer_connections`, plain TCP otherwise.
        let is_ssl = ssl_params.is_some();
        // SSL sockets never run MSE (upstream: `is_ssl_torrent()`
        // streams are TLS-wrapped — encryption is redundant).
        let encrypted = encrypted && !is_ssl;
        let kind = match bitdaemon_net::instantiate::instantiate(
            &peer_proxy,
            bitdaemon_net::instantiate::ConnKind::Peer,
            is_ssl,
            utp_wanted,
        ) {
            Some(k) => k,
            None => {
                slock(&self.session).peer_connect_failed(&ih, remote, false);
                return;
            }
        };
        let resolver = self.resolver.clone();
        let fast = true;
        let ext = self.ext_handshake(&ih, true, Some(remote.ip()));
        let session = Arc::clone(&self.session);
        // `num_peers_half_open` — the dial task owns the conn until
        // `on_dial_done` attaches or drops it.
        // `bind_outgoing_socket` — the `outgoing_port` rotation
        // resolves to a bind endpoint before dial (None = OS picks).
        let bind = slock(&session).outgoing_bind_addr(remote);
        slock(&session)
            .counters
            .inc(bitdaemon_session::counters::Counter::NumPeersHalfOpen);
        debug!(%remote, ih = %crate::common::hex::to_hex(&ih.as_bytes()[..8]), "dial spawned");
        let h = tokio::spawn(async move {
            // `too_few_outgoing_ports` — a connect failing with
            // `address_in_use` while `outgoing_port` is configured
            // means the port range is exhausted (upstream posts it
            // from `peer_connection`'s error path).
            let check_ports = |e: &bitdaemon_net::NetError| {
                if bind.is_none() {
                    return;
                }
                let bitdaemon_net::NetError::Transport(io) = e else {
                    return;
                };
                if io.kind() == std::io::ErrorKind::AddrInUse {
                    slock(&session)
                        .alerts
                        .post(bitdaemon_session::alerts::Alert::Performance {
                            warning: "too_few_outgoing_ports".into(),
                        });
                }
            };
            let send_err = |e: String| {
                let _ = tx.send(DialDone {
                    ih,
                    remote: Some(remote),
                    stream: Err(e),
                    outbound: true,
                    fast: false,
                    dht: false,
                    encrypted: false,
                    pid: None,
                    rc4: None,
                    exceeded: false,
                    kind: transport_socket_type(&kind),
                });
            };
            // `instantiate` resolved the socket type — map it back to
            // a connector. Proxied kinds resolve the proxy hostname
            // first (anonymous lanes inject their own resolver).
            use bitdaemon_net::instantiate::TransportKind as TK;
            let conn: Arc<dyn PeerConnector> = match kind {
                TK::Utp | TK::SslUtp => match utp.clone() {
                    Some(u) => u,
                    None => connector.clone(),
                },
                TK::Tcp | TK::SslTcp => connector.clone(),
                TK::Socks | TK::SslSocks => {
                    match resolve_for_socket(
                        &resolver,
                        &peer_proxy.hostname,
                        peer_proxy.port,
                        false,
                    )
                    .await
                    {
                        Ok(addr) => Arc::new(bitdaemon_net::proxy::SocksConnector::from_settings(
                            connector.clone(),
                            addr,
                            &peer_proxy,
                        )),
                        Err(e) => {
                            send_err(format!("proxy resolve: {e}"));
                            return;
                        }
                    }
                }
                TK::HttpStream | TK::SslHttpStream => {
                    match resolve_for_socket(
                        &resolver,
                        &peer_proxy.hostname,
                        peer_proxy.port,
                        false,
                    )
                    .await
                    {
                        Ok(addr) => Arc::new(bitdaemon_net::proxy::HttpConnectConnector::new(
                            connector.clone(),
                            addr,
                            peer_proxy.username.clone(),
                            peer_proxy.password.clone(),
                        )),
                        Err(e) => {
                            send_err(format!("proxy resolve: {e}"));
                            return;
                        }
                    }
                }
                TK::I2p => {
                    // i2p peers are reached by destination, not
                    // SocketAddr — the session never puts them in the
                    // dial queue. `i2p_alert` — the subsystem can't
                    // route this dial (upstream's `no_i2p_router`/
                    // transport error class).
                    slock(&session).alerts.post(Alert::I2p {
                        error: "i2p transport requested for ip peer".into(),
                    });
                    send_err("i2p transport requested for ip peer".to_string());
                    return;
                }
            };
            // `enable_outgoing_utp` — upstream prefers uTP when both
            // transports are available; on dial failure fall back to
            // the TCP connector (with the same bind — upstream runs
            // `bind_outgoing_socket` for every socket_type).
            let s = match if matches!(kind, TK::Utp | TK::SslUtp) {
                match conn.connect(remote).await {
                    Ok(s) => Ok(s),
                    Err(_) => connector.connect_with_bind(remote, bind).await,
                }
            } else {
                conn.connect_with_bind(remote, bind).await
            } {
                Ok(s) => s,
                Err(e) => {
                    check_ports(&e);
                    send_err(e.to_string());
                    return;
                }
            };
            // SSL torrent: every peer connection is TLS-wrapped
            // (upstream `is_ssl_torrent()` → `ssl_stream<T>`) — SNI
            // carries the hex info-hash for the server-side dispatch.
            // The peer's chain is verified against the torrent's
            // embedded root and its leaf SAN/CN must equal the
            // torrent name or "*" (`torrent::verify_peer_cert`);
            // mutual TLS — we present our `set_ssl_certificate`
            // cert/key when configured.
            let mut s = if let Some((root, name, cert, key)) = ssl_params {
                let sni = crate::common::hex::to_hex(ih.as_bytes());
                let cfg = match bitdaemon_net::ssl_stream::peer_ssl_client_config(
                    &root,
                    &name,
                    cert.as_deref().map(str::as_bytes),
                    key.as_deref().map(str::as_bytes),
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        send_err(format!("ssl config: {e}"));
                        return;
                    }
                };
                match bitdaemon_net::ssl_stream::ssl_connect(s, &sni, remote, cfg).await {
                    Ok(tls) => Box::new(tls) as bitdaemon_net::transport::BoxedPeerStream,
                    Err(e) => {
                        send_err(format!("ssl handshake: {e}"));
                        return;
                    }
                }
            } else {
                s
            };
            // `handshake_timeout` — upstream's on_tick disconnects
            // torrent-less conns past it (and conn second_tick does
            // the same post-attach); the outbound handshake gets the
            // same bound as the accept side.
            let (hs, mut mse_rc4) = if encrypted {
                match tokio::time::timeout(
                    hs_timeout,
                    bitdaemon_net::bt_peer_conn::run_handshake_encrypted_initiator(
                        &mut s, &ih, &pid, dht, fast, enc_level, 0,
                    ),
                )
                .await
                {
                    Ok(Ok((hs, mse))) => {
                        let rc4 = if mse.rc4_encrypted {
                            Some(mse.rc4)
                        } else {
                            None
                        };
                        (Ok(hs), rc4)
                    }
                    Ok(Err(e)) => (Err(e), None),
                    Err(_) => (
                        Err(bitdaemon_net::bt_peer_conn::PeerConnError::Protocol(
                            "handshake timeout",
                        )),
                        None,
                    ),
                }
            } else {
                (
                    tokio::time::timeout(hs_timeout, run_handshake(&mut s, &ih, &pid, dht, fast))
                        .await
                        .unwrap_or(Err(bitdaemon_net::bt_peer_conn::PeerConnError::Protocol(
                            "handshake timeout",
                        ))),
                    None,
                )
            };
            let hs = match hs {
                Ok(h) => h,
                Err(e) => {
                    send_err(e.to_string());
                    return;
                }
            };
            if let Err(e) =
                bitdaemon_net::bt_peer_conn::send_extended_handshake(&mut s, &ext, mse_rc4.as_mut())
                    .await
            {
                send_err(e.to_string());
                return;
            }
            let _ = tx.send(DialDone {
                ih,
                remote: Some(remote),
                stream: Ok(s),
                outbound: true,
                fast: hs.supports_fast(),
                dht: hs.supports_dht(),
                encrypted,
                pid: Some(hs.peer_id),
                rc4: mse_rc4,
                exceeded: false,
                kind: transport_socket_type(&kind),
            });
        });
        self.tasks.push(h);
    }

    /// `listen_sockets` accept pump — one task per acceptor: each
    /// inbound stream runs the responder handshake (info-hash gated
    /// by `known`, refreshed every tick) and lands in `connect_rx`
    /// like an outbound dial. Posts `listen_succeeded` when `local`
    /// is the bound endpoint, `listen_failed` on accept errors,
    /// `incoming_connection` per accepted stream.
    pub fn start_acceptor_at(
        &mut self,
        acceptor: Arc<dyn bitdaemon_net::transport::PeerAcceptor>,
        local: Option<SocketAddr>,
        kind: u8,
    ) {
        if let Some(a) = local {
            slock(&self.session).on_listen_succeeded(a.to_string(), kind);
        }
        let tx = self.connect_tx.clone();
        let fd_tx = self.fd_exhausted_tx.clone();
        let known = self.known_ihs.clone();
        let session = self.session.clone();
        let (pid, dht) = {
            let ses = slock(&self.session);
            (ses.peer_id, ses.settings.enable_dht)
        };
        let h = tokio::spawn(async move {
            loop {
                let acc = acceptor.clone().accept().await;
                let (s0, remote) = match acc {
                    Ok(pair) => pair,
                    Err(e) => {
                        // `too_many_files_open` upstream: post
                        // `too_few_file_descriptors`, drop a peer and
                        // clamp `connections_limit` — then KEEP
                        // accepting (the error is transient pressure).
                        if is_fd_exhausted(&e) {
                            let _ = fd_tx.send(());
                            continue;
                        }
                        warn!(err = %e, "accept failed");
                        slock(&session).on_listen_failed(
                            local.map_or_else(String::new, |a| a.to_string()),
                            "accept".into(),
                            e.to_string(),
                            kind,
                        );
                        return;
                    }
                };
                let mut s = s0;
                // `num_peers_half_open` — the accepted conn exists
                // until attach/drop (`peer_connection` ctor→complete).
                slock(&session)
                    .counters
                    .inc(bitdaemon_session::counters::Counter::NumPeersHalfOpen);
                // `on_incoming_connection` — session conn-cap gates:
                // past `limit + slack` the socket drops, inside slack
                // it attaches flagged `m_exceeded_limit`.
                let verdict = slock(&session).on_incoming_connection(
                    remote.unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0))),
                    kind,
                );
                let exceeded = match verdict {
                    bitdaemon_session::session::IncomingVerdict::Accept => false,
                    bitdaemon_session::session::IncomingVerdict::AcceptExceeded => true,
                    bitdaemon_session::session::IncomingVerdict::Reject => {
                        slock(&session)
                            .counters
                            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                        continue;
                    }
                };
                // upstream `on_receive`: probe as plaintext, fall back
                // to MSE per `in_enc_policy` (the 20 probe bytes are
                // re-interpreted as the DH key head).
                let (pe, hs_timeout, skeys) = {
                    let ses = slock(&session);
                    (
                        ses.pe_settings,
                        ses.settings.handshake_timeout,
                        ses.torrents
                            .keys()
                            .map(|ih| (bitdaemon_net::pe_crypto::stream_key_hash(ih), *ih))
                            .collect::<std::collections::HashMap<_, _>>(),
                    )
                };
                // upstream bounds the accept-side handshake by
                // `settings_pack::handshake_timeout` — a silent peer
                // must not wedge the accept loop for everyone else.
                let hs = tokio::time::timeout(
                    hs_timeout,
                    bitdaemon_net::bt_peer_conn::run_handshake_responder_auto(
                        &mut s,
                        &pid,
                        dht,
                        true,
                        |ih| known.lock().unwrap_or_else(|e| e.into_inner()).contains(ih),
                        pe.in_enc_policy,
                        |sk| skeys.get(sk).copied(),
                        |provide| crypto_select(provide, pe),
                    ),
                )
                .await
                .unwrap_or(Err(
                    bitdaemon_net::bt_peer_conn::PeerConnError::Protocol("handshake timeout"),
                ));
                match hs {
                    Ok((hs, rc4, encrypted)) => {
                        let _ = tx.send(DialDone {
                            ih: hs.info_hash,
                            // the accepted source endpoint — needed for
                            // ut_pex advertising and alert accuracy
                            remote,
                            stream: Ok(s),
                            outbound: false,
                            fast: hs.supports_fast(),
                            dht: hs.supports_dht(),
                            encrypted,
                            pid: Some(hs.peer_id),
                            rc4,
                            exceeded,
                            kind,
                        });
                    }
                    Err(e) => {
                        warn!(err = %e, "inbound handshake failed");
                        slock(&session)
                            .counters
                            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                    }
                }
            }
        });
        self.tasks.push(h);
    }

    /// `start_acceptor` without endpoint info — `listen_succeeded`
    /// can't be attributed; alerts still post with `""` remote.
    pub fn start_acceptor(&mut self, acceptor: Arc<dyn bitdaemon_net::transport::PeerAcceptor>) {
        self.start_acceptor_at(acceptor, None, 0);
    }

    /// SSL-torrent accept pump — same as [`Self::start_acceptor`]
    /// but every inbound stream is TLS-accepted first
    /// (`ssl_listen_port` sockets upstream). Cert selection uses the
    /// per-info-hash SNI resolver fed by [`Self::refresh_ssl_certs`].
    pub fn start_ssl_acceptor(
        &mut self,
        acceptor: Arc<dyn bitdaemon_net::transport::PeerAcceptor>,
    ) {
        self.start_ssl_acceptor_at(acceptor, None, 6);
    }

    /// `start_ssl_acceptor` variant carrying the bound endpoint +
    /// `socket_type` kind — posts `listen_succeeded`/`listen_failed`
    ////`incoming_connection` like the plain acceptor.
    pub fn start_ssl_acceptor_at(
        &mut self,
        acceptor: Arc<dyn bitdaemon_net::transport::PeerAcceptor>,
        local: Option<SocketAddr>,
        kind: u8,
    ) {
        if let Some(a) = local {
            slock(&self.session).on_listen_succeeded(a.to_string(), kind);
        }
        let cfg = match rustls::ServerConfig::builder_with_provider(
            bitdaemon_net::ssl_stream::crypto_provider(),
        )
        .with_safe_default_protocol_versions()
        {
            Ok(b) => Arc::new(
                b
                    // SSL torrents are mutual-TLS upstream
                    // (`verify_peer | verify_fail_if_no_peer_cert`).
                    .with_client_cert_verifier(Arc::new(
                        bitdaemon_net::ssl_stream::TorrentClientVerifier::new(
                            self.ssl_roots.clone(),
                        ),
                    ))
                    .with_cert_resolver(self.ssl_resolver.clone()),
            ),
            Err(e) => {
                slock(&self.session).on_listen_failed(
                    local.map_or_else(String::new, |a| a.to_string()),
                    "ssl".into(),
                    e.to_string(),
                    kind,
                );
                return;
            }
        };
        let tx = self.connect_tx.clone();
        let fd_tx = self.fd_exhausted_tx.clone();
        let known = self.known_ihs.clone();
        let session = self.session.clone();
        let (pid, dht) = {
            let ses = slock(&self.session);
            (ses.peer_id, ses.settings.enable_dht)
        };
        let h = tokio::spawn(async move {
            loop {
                let acc = acceptor.clone().accept().await;
                let (s0, remote) = match acc {
                    Ok(pair) => pair,
                    Err(e) => {
                        if is_fd_exhausted(&e) {
                            let _ = fd_tx.send(());
                            continue;
                        }
                        warn!(err = %e, "ssl accept failed");
                        slock(&session).on_listen_failed(
                            local.map_or_else(String::new, |a| a.to_string()),
                            "accept".into(),
                            e.to_string(),
                            kind,
                        );
                        return;
                    }
                };
                let remote = remote.unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
                // `num_peers_half_open` — accepted conn until
                // attach/drop (`on_dial_done` clears it).
                slock(&session)
                    .counters
                    .inc(bitdaemon_session::counters::Counter::NumPeersHalfOpen);
                let exceeded = match slock(&session).on_incoming_connection(remote, kind) {
                    bitdaemon_session::session::IncomingVerdict::Accept => false,
                    bitdaemon_session::session::IncomingVerdict::AcceptExceeded => true,
                    bitdaemon_session::session::IncomingVerdict::Reject => {
                        slock(&session)
                            .counters
                            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                        continue;
                    }
                };
                let tls = match bitdaemon_net::ssl_stream::ssl_accept(s0, remote, cfg.clone()).await
                {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(err = %e, "ssl handshake failed");
                        slock(&session)
                            .counters
                            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                        continue;
                    }
                };
                let mut s = Box::new(tls) as bitdaemon_net::transport::BoxedPeerStream;
                let hs = bitdaemon_net::bt_peer_conn::run_handshake_responder(
                    &mut s,
                    &pid,
                    dht,
                    true,
                    |ih| known.lock().unwrap_or_else(|e| e.into_inner()).contains(ih),
                )
                .await;
                match hs {
                    Ok(hs) => {
                        let _ = tx.send(DialDone {
                            ih: hs.info_hash,
                            remote: Some(remote),
                            stream: Ok(s),
                            outbound: false,
                            fast: hs.supports_fast(),
                            dht: hs.supports_dht(),
                            // upstream: SSL sockets never run MSE.
                            encrypted: false,
                            pid: Some(hs.peer_id),
                            rc4: None,
                            exceeded,
                            kind,
                        });
                    }
                    Err(e) => {
                        warn!(err = %e, "inbound ssl handshake failed");
                        slock(&session)
                            .counters
                            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                    }
                }
            }
        });
        self.tasks.push(h);
    }

    /// `ssl_server_name_callback` setup — register every SSL
    /// torrent's `CertifiedKey` under its hex info-hash SNI name
    /// (upstream picks the torrent's `ssl_ctx` by info-hash).
    /// Call after `set_ssl_certificate` on any SSL torrent.
    pub fn refresh_ssl_certs(&self) {
        let ses = slock(&self.session);
        let res = &*self.ssl_resolver;
        for (ih, t) in &ses.torrents {
            if !t.is_ssl_torrent() {
                continue;
            }
            if let Some((cert, key)) = t.ssl_pem_pair() {
                let sni = crate::common::hex::to_hex(ih.as_bytes());
                // the trust root comes from the torrent file's
                // `ssl-cert` field (`init_ssl` upstream)
                let (name, root) = t.ti.as_ref().map_or((Vec::new(), Vec::new()), |ti| {
                    (ti.name().to_vec(), ti.ssl_cert().to_vec())
                });
                let name = String::from_utf8_lossy(&name).into_owned();
                match bitdaemon_net::ssl_stream::register_ssl_torrent(
                    res,
                    &self.ssl_roots,
                    &sni,
                    &name,
                    &root,
                    cert,
                    key,
                ) {
                    Ok(()) => {}
                    Err(e) => warn!(err = %e, "ssl cert load failed"),
                }
            }
        }
    }

    /// `PeerListState` snapshot — delegates to
    /// [`Session::peer_list_state`] (settings + external-IP
    /// aggregate + listen port).
    #[must_use]
    pub fn peer_list_state(&self) -> bitdaemon_net::peer_list::PeerListState {
        slock(&self.session).peer_list_state()
    }

    /// `session_impl::run` — the canonical driver: 1 Hz tick
    /// (announces, peer drive, disk completions, DHT, LSD); the
    /// connect pump runs inside `Session::tick` at upstream's
    /// `on_tick` position, paced by `settings.connection_speed`.
    /// `max_dials_per_tick` is retained for API compatibility and
    /// ignored — upstream has no per-tick dial override.
    /// Runs until `stop` resolves.
    pub async fn run<F: std::future::Future<Output = ()>>(
        &mut self,
        stop: F,
        max_dials_per_tick: usize,
    ) {
        let _ = max_dials_per_tick;
        let disk_notify = self.disk_notify.clone();
        tokio::pin!(stop);
        // upstream's tick is an absolute `deadline_timer`: it fires on
        // schedule no matter how busy the io_context is. A `sleep()`
        // created inside `select!` is re-armed on every iteration, so
        // any branch that stays continuously ready (inbound pieces at
        // full rate, sent_rx, disk_notify) starves the tick forever —
        // observed as multi-tens-of-seconds freezes mid-download.
        // Pinning one `Sleep` outside the loop preserves the deadline
        // across rounds; it is re-armed each time it fires.
        let tick_timer = tokio::time::sleep(slock(&self.session).settings.tick_interval);
        tokio::pin!(tick_timer);
        loop {
            tokio::select! {
                _ = &mut tick_timer => {
                    // `m_tick_interval` — upstream reschedules its tick
                    // timer from `settings_pack::tick_interval` each
                    // round (default 500 ms); read it live so
                    // `apply_settings` retunes.
                    let t = slock(&self.session).settings.tick_interval;
                    tick_timer.as_mut().reset(tokio::time::Instant::now() + t);
                }
                m = self.inbound_rx.recv() => {
                    // upstream's io_context services peer messages as they
                    // arrive — batching them until the 1 Hz tick would let
                    // the unbounded queue grow by the whole download rate
                    // per second (observed: ~360 MiB bursts + system
                    // stalls at ~100 MiB/s). Drain what is already pending
                    // (bounded batch keeps `stop` responsive), dispatch
                    // the resulting effects, and loop back to select.
                    let mut out = Vec::new();
                    if let Some(m) = m {
                        slock(&self.session)
                            .on_peer_message(&m.packet, m.wire_bytes, &mut out);
                    }
                    for _ in 0..1024 {
                        match self.inbound_rx.try_recv() {
                            Ok(m) => {
                                slock(&self.session)
                                    .on_peer_message(&m.packet, m.wire_bytes, &mut out);
                            }
                            Err(_) => break,
                        }
                    }
                    // `call_job_handlers` — upstream posts this on the
                    // io_context on each 0→1 completion transition, so
                    // piece responses are written as disk jobs land,
                    // not on the next 1 Hz tick. Draining per inbound
                    // batch keeps upload latency bounded under load.
                    slock(&self.session).drain_disk_completed(&mut out);
                    // `on_sent` events pile up under sustained upload —
                    // drain them here too (the dedicated select branch
                    // covers the quiet case).
                    while let Ok((ih, peer, bytes, payload)) = self.sent_rx.try_recv() {
                        slock(&self.session).on_peer_sent(&ih, peer, bytes, payload, &mut out);
                    }
                    self.dispatch(out).await;
                    continue;
                }
                _ = disk_notify.notified() => {
                    // `disk_completed_queue` poster → run the handlers
                    // and push the resulting effects now.
                    let mut out = Vec::new();
                    slock(&self.session).drain_disk_completed(&mut out);
                    self.dispatch(out).await;
                    continue;
                }
                _ = self.fd_exhausted_rx.recv() => {
                    // accept hit EMFILE — upstream `on_accept`'s
                    // too_many_files_open leg: alert + drop a peer
                    // + clamp connections_limit, then keep accepting.
                    let mut out = Vec::new();
                    slock(&self.session).on_accept_fd_exhausted(&mut out);
                    self.dispatch(out).await;
                    continue;
                }
                first = self.sent_rx.recv() => {
                    // `on_sent` — a peer socket drained some of its
                    // send buffer; re-run `fill_send_buffer` so reads
                    // resume under the watermark without waiting for
                    // the 1 Hz tick.
                    let mut out = Vec::new();
                    if let Some((ih, peer, bytes, payload)) = first {
                        slock(&self.session).on_peer_sent(&ih, peer, bytes, payload, &mut out);
                    }
                    for _ in 0..1024 {
                        match self.sent_rx.try_recv() {
                            Ok((ih, peer, bytes, payload)) => {
                                slock(&self.session)
                                    .on_peer_sent(&ih, peer, bytes, payload, &mut out);
                            }
                            Err(_) => break,
                        }
                    }
                    self.dispatch(out).await;
                    continue;
                }
                _ = &mut stop => break,
            }
            // the connect pump lives inside `Session::tick` at
            // upstream's `on_tick` position (~1 Hz cadence capped
            // by `connection_speed`); its `ConnectPeer` effects ride
            // `out` → `dispatch` → `spawn_dial`.
            self.tick(Instant::now()).await;
        }
    }

    /// our BEP-10 offer — `ut_metadata`/`ut_pex` ids (matching
    /// `PeerConn::local_ut_*` defaults) + listen port and the
    /// torrent's metadata size when known.
    fn ext_handshake(
        &self,
        ih: &Sha1Hash,
        outgoing: bool,
        remote: Option<std::net::IpAddr>,
    ) -> bitdaemon_net::bep10::ExtendedHandshake {
        let ses = slock(&self.session);
        let mut hs = bitdaemon_net::bep10::ExtendedHandshake {
            // upstream sends `p` only on outgoing connections — the
            // other end of an incoming one already knows our port —
            // and only when non-zero.
            listen_port: (outgoing && ses.listen_port > 0).then(|| i64::from(ses.listen_port)),
            // `yourip` — the peer's external address as we see it
            // (skipped on i2p sockets upstream — `None` here covers
            // that since i2p peers have no IP remote).
            your_ip: remote.map(|ip| match ip {
                std::net::IpAddr::V4(v4) => v4.octets().to_vec(),
                std::net::IpAddr::V6(v6) => v6.octets().to_vec(),
            }),
            // `reqq` is `max_allowed_in_request_queue` upstream
            // (how many requests *they* may queue on us).
            reqq: i64::from(ses.settings.max_allowed_in_request_queue),
            ..Default::default()
        };
        if !ses.settings.anonymous_mode {
            hs.client_version = Some(if ses.settings.handshake_client_version.is_empty() {
                ses.settings.user_agent.clone()
            } else {
                ses.settings.handshake_client_version.clone()
            });
        }
        // upstream fixed extension ids (`bt_peer_connection.hpp`):
        // ut_pex=1, ut_metadata=2, upload_only=3, ut_holepunch=4,
        // lt_donthave=7, share_mode=8, i2p_pex=9.
        hs.messages.insert("upload_only".into(), 3);
        hs.messages.insert("ut_holepunch".into(), 4);
        if ses.settings.support_share_mode {
            hs.messages.insert("share_mode".into(), 8);
        }
        hs.messages.insert("lt_donthave".into(), 7);
        if let Some(t) = ses.torrents.get(ih) {
            let is_priv = t.ti.as_ref().is_some_and(|ti| ti.is_private());
            let is_i2p = t.ti.as_ref().is_some_and(|ti| ti.is_i2p());
            // `create_ut_pex_plugin` — skipped for private torrents
            // and pure-i2p torrents without `allow_i2p_mixed`.
            if !is_priv && !(is_i2p && !ses.settings.allow_i2p_mixed) {
                hs.messages.insert("ut_pex".into(), 1);
            }
            // `i2p_pex` only exists on i2p sockets upstream; the
            // torrent-level approximation here is the i2p flag.
            if is_i2p {
                hs.messages.insert("i2p_pex".into(), 9);
            }
            hs.messages.insert("ut_metadata".into(), 2);
            if let Some(md) = &t.metadata_raw {
                hs.metadata_size = Some(md.len() as i64);
            }
            hs.complete_ago = t.time_since_complete();
            // upstream `write_extensions`: suppress `upload_only`
            // while super-seeding, in share mode, and before
            // metadata is valid.
            hs.upload_only =
                t.is_upload_only() && !t.share_mode && t.valid_metadata() && !t.super_seeding;
            hs.share_mode = ses.settings.support_share_mode && t.share_mode;
        }
        hs
    }

    /// `on_incoming` — register an accepted stream: responder
    /// handshake (needs `their` first 68 bytes to learn `info_hash`),
    /// then attach. Returns the peer id, or `None` for unknown ih.
    pub async fn accept_stream(
        &mut self,
        remote: SocketAddr,
        mut s: BoxedPeerStream,
    ) -> std::io::Result<Option<u64>> {
        // `session_impl::incoming_connection` → `m_ip_filter` gate
        // before the handshake even runs (block at TCP level;
        // upstream posts `peer_blocked_alert` with an invalid
        // handle — the info-hash isn't known yet).
        if slock(&self.session).ip_access(remote.ip()) == bitdaemon_net::ip_filter::BLOCKED {
            slock(&self.session).alerts.post(Alert::PeerBlocked {
                ih: Sha1Hash::default(),
                remote: remote.to_string(),
                reason: "ip_filter".into(),
            });
            return Ok(None);
        }
        let (pid, dht) = {
            let ses = slock(&self.session);
            (ses.peer_id, ses.settings.enable_dht)
        };
        // `num_peers_half_open` — the accepted conn exists from here
        // until attach or handshake failure (`peer_connection` ctor).
        slock(&self.session)
            .counters
            .inc(bitdaemon_session::counters::Counter::NumPeersHalfOpen);
        // `on_incoming_connection` gates — conn cap + slack; the
        // injected-transport entry point has no socket type (tcp = 0).
        let exceeded = match slock(&self.session).on_incoming_connection(remote, 0) {
            bitdaemon_session::session::IncomingVerdict::Accept => false,
            bitdaemon_session::session::IncomingVerdict::AcceptExceeded => true,
            bitdaemon_session::session::IncomingVerdict::Reject => {
                slock(&self.session)
                    .counters
                    .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                return Ok(None);
            }
        };
        let (ih_known, in_policy, skeys, pe) = {
            let ses = slock(&self.session);
            let set: Vec<Sha1Hash> = ses.torrents.keys().copied().collect();
            let skeys = ses
                .torrents
                .keys()
                .map(|ih| (bitdaemon_net::pe_crypto::stream_key_hash(ih), *ih))
                .collect::<std::collections::HashMap<_, _>>();
            (set, ses.pe_settings.in_enc_policy, skeys, ses.pe_settings)
        };
        let hs = match bitdaemon_net::bt_peer_conn::run_handshake_responder_auto(
            &mut s,
            &pid,
            dht,
            true,
            |ih| ih_known.contains(ih),
            in_policy,
            |sk| skeys.get(sk).copied(),
            |provide| crypto_select(provide, pe),
        )
        .await
        {
            Ok(r) => r,
            // `peer_connection::on_connected` — an inbound ih that
            // matches our DHT secret-id means the remote fished it
            // out of DHT chatter: suspicious → `ban_ip`.
            Err(bitdaemon_net::bt_peer_conn::PeerConnError::WrongInfoHash(ih))
                if bitdaemon_dht::node_id::verify_secret_id(&ih) =>
            {
                let mut ses = slock(&self.session);
                ses.counters
                    .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                ses.ban_ip(remote.ip());
                return Ok(None);
            }
            Err(e) => {
                slock(&self.session)
                    .counters
                    .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
                return Err(std::io::Error::other(e));
            }
        };
        slock(&self.session)
            .counters
            .add(bitdaemon_session::counters::Counter::NumPeersHalfOpen, -1);
        let (hs, mut rc4, encrypted) = hs;
        let num_pieces = slock(&self.session)
            .torrents
            .get(&hs.info_hash)
            .map_or(0, |t| t.num_pieces as usize);
        let peer = self.next_peer;
        self.next_peer += 1;
        let mut conn = PeerConn::new(
            num_pieces,
            PeerPipeline::new(
                self.queue_time,
                self.request_timeout,
                self.max_out_request_queue,
            ),
            0,
        );
        conn.outgoing = false;
        conn.connecting = false;
        conn.handshake_done = true;
        conn.supports_encryption = encrypted;
        conn.peer_id = Some(hs.peer_id);
        conn.exceeded_limit = exceeded;
        conn.sock_type = 0;
        // upstream `write_handshake` sends our BEP-10 handshake on
        // both directions when the peer set the extension bit —
        // without it the remote never learns our `ut_metadata`/
        // `ut_pex`/`lt_donthave` ids and can't send them to us.
        if hs.supports_extension() {
            let ext = self.ext_handshake(&hs.info_hash, false, Some(remote.ip()));
            let _ =
                bitdaemon_net::bt_peer_conn::send_extended_handshake(&mut s, &ext, rc4.as_mut())
                    .await;
        }
        let mut out = Vec::new();
        slock(&self.session).peer_connected(&hs.info_hash, peer, conn, remote, &mut out);
        self.attach_stream(hs.info_hash, peer, remote, s, rc4);
        self.send_msgs_to_peers(out);
        Ok(Some(peer))
    }

    /// split a live peer stream into reader/writer tasks feeding the
    /// engine queues. `rc4` is the negotiated MSE keystream — all
    /// wire traffic goes through it when present
    /// (`m_rc4_encrypted`).
    fn attach_stream(
        &mut self,
        ih: Sha1Hash,
        peer: u64,
        remote: SocketAddr,
        mut s: BoxedPeerStream,
        rc4: Option<bitdaemon_net::pe_crypto::Rc4Handler>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<PeerMessage>();
        self.writers.insert((ih, peer), tx);

        let inbound_tx = self.inbound_tx.clone();
        let gone_tx = self.gone_tx.clone();
        let sent_tx = self.sent_tx.clone();
        let session = Arc::clone(&self.session);
        // `m_quota[]`/`m_channel_state[]` — the conn's bandwidth
        // handle (shared with the session's `PeerConn`; a conn that
        // died during `peer_connected` gets a detached handle — the
        // `gone`/disconnect path tears the task down).
        let bw = slock(&session)
            .peer_quota(&ih, peer)
            .unwrap_or_else(bitdaemon_session::peer_conn::PeerQuota::new);
        let write_gates = self.write_gates.clone();
        let ipv6 = remote.is_ipv6();
        // one task owns the stream: select between inbound decode and
        // outbound encode (upstream's per-connection pump). Reads go
        // through a persistent buffer — `read_message`'s two-phase
        // `read_exact` is NOT cancellation-safe inside a `select!`:
        // a losing read leaves the length bytes consumed and desyncs
        // the frame stream.
        let h = tokio::spawn(async move {
            use bitdaemon_session::peer_conn::{DOWNLOAD_CHANNEL, UPLOAD_CHANNEL};
            use std::sync::atomic::Ordering::Relaxed;
            let mut buf = bytes::BytesMut::with_capacity(256 * 1024);
            // capped read staging — `async_read_some(reserve(
            // min(max_receive, quota)))` upstream: the socket only
            // pulls what the quota allows, so the wire never runs
            // ahead of the limiter.
            let mut tmp = vec![0u8; 64 * 1024];
            // `m_send_buffer` — the task-local tail of queued
            // messages that outran the upload quota (drained first,
            // before new `rx` traffic, like upstream's chained_buffer
            // holding what one `async_write_some` couldn't cover).
            let mut pending: std::collections::VecDeque<PeerMessage> =
                std::collections::VecDeque::new();
            let mut rc4 = rc4;
            let gate = write_gates.as_ref().map(|g| g.gate((ih, peer)));
            'pump: loop {
                tokio::select! {
                    // `peer_connection::can_read` — while *this* peer
                    // has a block queued on an over-limit disk, its
                    // socket is not read at all, closing the TCP
                    // window so it stops sending more; the gate
                    // clears the instant that peer's own write lands
                    // (upstream `on_disk_write_complete`), not when
                    // the whole cache drains — a slow/seek-bound disk
                    // no longer freezes every unrelated peer for the
                    // duration of the burst.
                    r = async {
                        while gate.as_ref().is_some_and(|g| {
                            g.load(std::sync::atomic::Ordering::Relaxed)
                        }) {
                            tokio::time::sleep(
                                std::time::Duration::from_millis(10),
                            )
                            .await;
                        }
                        // `setup_receive` — `m_quota[download_channel]
                        // == 0` stalls the read; `request_bandwidth`
                        // runs *unconditionally* at its head upstream,
                        // topping up to `wanted_transfer` so a queued
                        // request's grant lands before the current
                        // quota drains (prefetch). A pending `bw_limit`
                        // or sufficient quota makes it a no-op.
                        let want = loop {
                            // `m_recv_buffer.max_receive()` upstream —
                            // the staged room the next read wants.
                            let spare = (buf.capacity() - buf.len()) as i64;
                            if slock(&session)
                                .request_bandwidth(
                                    &ih,
                                    peer,
                                    DOWNLOAD_CHANNEL,
                                    spare.max(1),
                                )
                                .is_none()
                            {
                                // conn gone — tear down
                                return Err(std::io::ErrorKind::BrokenPipe.into());
                            }
                            let q = bw.quota[DOWNLOAD_CHANNEL].load(Relaxed);
                            if q > 0 {
                                break usize::try_from(q)
                                    .unwrap_or(tmp.len())
                                    .min(tmp.len());
                            }
                            if bw.disconnecting.load(Relaxed) {
                                return Err(std::io::ErrorKind::BrokenPipe.into());
                            }
                            // quota exhausted and a request is queued
                            // (`bw_limit` set) → wait for the grant;
                            // `bw_limit` unset means the manager granted
                            // nothing yet but the request isn't queued
                            // — loop back and re-check/re-request.
                            if bw.channel_state[DOWNLOAD_CHANNEL]
                                .load(Relaxed)
                                & bitdaemon_session::peer_info::BandwidthState::LIMIT.0
                                != 0
                            {
                                bw.wait_grant(DOWNLOAD_CHANNEL).await;
                            } else {
                                tokio::task::yield_now().await;
                            }
                        };
                        // `bw_network` — a socket op in flight
                        bw.channel_state[DOWNLOAD_CHANNEL].fetch_or(
                            bitdaemon_session::peer_info::BandwidthState::NETWORK.0,
                            Relaxed,
                        );
                        let r = tokio::io::AsyncReadExt::read(
                            &mut s, &mut tmp[..want],
                        ).await;
                        bw.channel_state[DOWNLOAD_CHANNEL].fetch_and(
                            !bitdaemon_session::peer_info::BandwidthState::NETWORK.0,
                            Relaxed,
                        );
                        r
                    } => {
                        match r {
                            Ok(0) => {
                                debug!(peer = peer, remote = ?remote, err = %"eof", "peer read error, dropping");
                                break;
                            }
                            Ok(n) => {
                                // `on_receive_data` — consume quota
                                // by *socket* bytes, then charge the
                                // IP/TCP overhead (`trancieve_ip_packet`
                                // hits both directions — the packet
                                // we got plus the ACK we send).
                                bw.quota[DOWNLOAD_CHANNEL].fetch_sub(n as i64, Relaxed);
                                let oh = bitdaemon_session::peer_conn::PeerConn::ip_overhead_bytes(
                                    n as i64,
                                    ipv6,
                                );
                                bw.ip_overhead[DOWNLOAD_CHANNEL].fetch_add(oh, Relaxed);
                                bw.ip_overhead[UPLOAD_CHANNEL].fetch_add(oh, Relaxed);
                                buf.extend_from_slice(&tmp[..n]);
                                // RC4 stream decryption happens in
                                // place before frame decode.
                                if let Some(r) = &mut rc4 {
                                    let tail = buf.len() - n;
                                    r.decrypt(&mut buf[tail..]);
                                }
                                loop {
                                // per-message wire bytes = the frame's
                                // exact consumed length (`m_recv_buffer`
                                // accounting upstream) — a frame
                                // straddling reads attributes all of its
                                // bytes to the decode that completes it.
                                let before = buf.len();
                                match bitdaemon_net::bt_peer_conn::decode_next(&mut buf) {
                                    Ok(Some(msg)) => {
                                        let wb = before - buf.len();
                                        if inbound_tx
                                            .send(InboundPeerMsg {
                                                packet: PeerPacket {
                                                    info_hash: ih,
                                                    peer,
                                                    remote,
                                                    msg,
                                                },
                                                wire_bytes: wb,
                                            })
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(e) => {
                                        debug!(peer = peer, remote = ?remote, err = %e, "peer read error, dropping");
                                        break 'pump;
                                    }
                                }
                                }
                                // `m_recv_buffer` visibility for
                                // `peer_info::{used_,}receive_buffer_size`.
                                bw.recv_pos.store(buf.len() as i64, Relaxed);
                                bw.recv_capacity.store(buf.capacity() as i64, Relaxed);
                            },
                            Err(e) => {
                                debug!(peer = peer, remote = ?remote, err = %e, "peer read error, dropping");
                                break;
                            }
                        }
                    }
                    // `setup_send` — the whole quota dance lives in
                    // the select future so a parked `wait_grant` does
                    // NOT stall the read arm (upstream's reads keep
                    // running while the upload channel is starved).
                    // Resolves to the quota-capped batch to write.
                    w = async {
                        // `append_to_send_buffer` — new work lands on
                        // `pending`; a 60 s idle conn gets a keepalive.
                        if pending.is_empty() {
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(60),
                                rx.recv(),
                            ).await {
                                Ok(Some(m)) => pending.push_back(m),
                                Ok(None) => return None,
                                Err(_) => pending.push_back(PeerMessage::Keepalive),
                            }
                        }
                        while pending.len() < 64 {
                            match rx.try_recv() {
                                Ok(m) => pending.push_back(m),
                                Err(_) => break,
                            }
                        }
                        let queued_bytes: i64 = pending
                            .iter()
                            .map(|m| bitdaemon_net::peer_wire::wire_len(m) as i64)
                            .sum();
                        // `setup_send` head — `request_bandwidth` runs
                        // unconditionally upstream, topping up to
                        // `wanted_transfer` so a queued grant is in
                        // flight before the current quota drains
                        // (prefetch); a pending `bw_limit` or
                        // sufficient quota makes it a no-op.
                        slock(&session)
                            .request_bandwidth(&ih, peer, UPLOAD_CHANNEL, queued_bytes.max(1))?;
                        // `can_write` — need ≥ the first message's
                        // quota before issuing the write.
                        loop {
                            let q = bw.quota[UPLOAD_CHANNEL].load(Relaxed);
                            let first = bitdaemon_net::peer_wire::wire_len(&pending[0]) as i64;
                            if q >= first {
                                break;
                            }
                            if bw.disconnecting.load(Relaxed) {
                                return None;
                            }
                            if bw.channel_state[UPLOAD_CHANNEL]
                                .load(Relaxed)
                                & bitdaemon_session::peer_info::BandwidthState::LIMIT.0
                                != 0
                            {
                                bw.wait_grant(UPLOAD_CHANNEL).await;
                            } else {
                                // no queued request and not enough
                                // quota — re-request (grant may have
                                // landed between checks)
                                slock(&session).request_bandwidth(
                                    &ih,
                                    peer,
                                    UPLOAD_CHANNEL,
                                    queued_bytes.max(1),
                                )?;
                                tokio::task::yield_now().await;
                            }
                        }
                        // `min(send_buffer.size(), quota)` — upstream
                        // writes a quota-truncated byte run; ours is
                        // message-granular (same wire stream).
                        let q = bw.quota[UPLOAD_CHANNEL].load(Relaxed);
                        let mut bytes = 0i64;
                        let mut n = 0usize;
                        for m in &pending {
                            let wl = bitdaemon_net::peer_wire::wire_len(m) as i64;
                            if n > 0 && bytes + wl > q {
                                break;
                            }
                            bytes += wl;
                            n += 1;
                        }
                        // `m_payloads` accounting — the piece-data
                        // share of the batch is payload, the rest
                        // (headers + non-piece messages) is protocol.
                        let payload: i64 = pending
                            .iter()
                            .take(n)
                            .map(|m| {
                                if let PeerMessage::Piece { data, .. } = m {
                                    data.len() as i64
                                } else {
                                    0
                                }
                            })
                            .sum();
                        let batch: Vec<PeerMessage> =
                            pending.drain(..n).collect();
                        Some((batch, bytes, payload))
                    } => {
                        let Some((batch, sent_bytes, sent_payload)) = w else {
                            break 'pump;
                        };
                        // `bw_network` for the duration of the write
                        bw.channel_state[UPLOAD_CHANNEL].fetch_or(
                            bitdaemon_session::peer_info::BandwidthState::NETWORK.0,
                            Relaxed,
                        );
                        let wres = if let Some(r) = &mut rc4 {
                            bitdaemon_net::bt_peer_conn::write_messages_rc4(
                                &mut s, r, &batch,
                            )
                            .await
                        } else {
                            write_messages(&mut s, &batch).await
                        };
                        bw.channel_state[UPLOAD_CHANNEL].fetch_and(
                            !bitdaemon_session::peer_info::BandwidthState::NETWORK.0,
                            Relaxed,
                        );
                        match wres {
                            Err(e) => {
                                debug!(peer = peer, remote = ?remote, err = %e, "peer write error, dropping");
                                break;
                            }
                            Ok(()) => {
                                // `on_send_data` — debit the quota by
                                // socket bytes + account the IP/TCP
                                // overhead (header per packet + the
                                // ACK going back).
                                bw.quota[UPLOAD_CHANNEL].fetch_sub(sent_bytes, Relaxed);
                                let oh = bitdaemon_session::peer_conn::PeerConn::ip_overhead_bytes(
                                    sent_bytes,
                                    ipv6,
                                );
                                bw.ip_overhead[UPLOAD_CHANNEL].fetch_add(oh, Relaxed);
                                bw.ip_overhead[DOWNLOAD_CHANNEL].fetch_add(oh, Relaxed);
                                // `on_sent` — the bytes left the send
                                // buffer; session re-runs
                                // `fill_send_buffer` and accounts
                                // `sent_bytes(payload, protocol)`.
                                let _ = sent_tx.send((
                                    ih,
                                    peer,
                                    sent_bytes as usize,
                                    sent_payload as usize,
                                ));
                            }
                        }
                    }
                }
            }
            bw.disconnecting.store(true, Relaxed);
            if let Some(g) = &write_gates {
                g.remove(&(ih, peer));
            }
            let _ = gone_tx.send((ih, peer));
        });
        self.tasks.push(h);
    }

    /// `session_impl::try_connect_more_peers` pump — the session
    /// hands out up to `connection_speed` dials per pass
    /// (prioritized torrents first, then the
    /// downloading/finished round-robin at
    /// `connect_seed_every_n_download`, `smooth_connects` cap,
    /// boost deductions). `max_peers` is a harness override —
    /// `usize::MAX` (or `0` → the setting) uses upstream pacing.
    /// Returns endpoints being dialed (for tests/metrics).
    pub fn connect_pump(
        &mut self,
        session_time: i32,
        max_peers: usize,
    ) -> Vec<(Sha1Hash, SocketAddr, bool)> {
        let mut guard = slock(&self.session);
        let ses = &mut *guard;
        let out_policy = ses.pe_settings.out_enc_policy;
        let speed_override = (max_peers > 0)
            .then(|| i32::try_from(max_peers.min(i32::MAX as usize)).unwrap_or(i32::MAX));
        ses.try_connect_more_peers(session_time, out_policy, speed_override)
    }

    /// number of live peer writers.
    #[must_use]
    pub fn num_peers(&self) -> usize {
        self.writers.len()
    }
}

/// resolve `hostname:port` — IP literal short-circuit, else the
/// injected `Resolver` (anonymous deployment MUST inject one; without
/// it hostname trackers fail instead of leaking a DNS query).
/// `http_tracker_connection::on_filter` — a resolved tracker
/// endpoint blocked by `req.filter` is dropped; with a single
/// resolved addr that means the announce fails
/// `banned_by_ip_filter` (upstream fails the whole request the
/// same way once every endpoint is filtered).
fn filter_tracker_addr(
    f: &Option<std::sync::Arc<bitdaemon_net::ip_filter::IpFilter>>,
    addr: SocketAddr,
) -> Result<SocketAddr, String> {
    match f {
        Some(f) if f.access(addr.ip()) == bitdaemon_net::ip_filter::BLOCKED => {
            Err("banned_by_ip_filter".into())
        }
        _ => Ok(addr),
    }
}

async fn resolve(
    r: &Option<BoxedResolver>,
    hostname: &str,
    port: u16,
) -> Result<SocketAddr, String> {
    if let Ok(addr) = format!("{hostname}:{port}").parse::<SocketAddr>() {
        return Ok(addr);
    }
    match r {
        Some(r) => resolve_host(r.as_ref(), hostname, port, ResolveFlags::default())
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| format!("no address for {hostname}")),
        None => Err(format!("no resolver injected for {hostname}")),
    }
}

/// `resolve` + filtre famille : la socket datagramme v4 ne peut pas
/// envoyer a une cible v6 (`WSAEAFNOSUPPORT`) — upstream garde des
/// sockets par famille ; on selectionne une adresse compatible.
async fn resolve_for_socket(
    r: &Option<BoxedResolver>,
    hostname: &str,
    port: u16,
    v4_only: bool,
) -> Result<SocketAddr, String> {
    if !v4_only {
        return resolve(r, hostname, port).await;
    }
    if let Ok(addr) = format!("{hostname}:{port}").parse::<SocketAddr>() {
        return if addr.is_ipv4() {
            Ok(addr)
        } else {
            Err(format!("{hostname} is v6-only, datagram is v4"))
        };
    }
    match r {
        Some(r) => resolve_host(r.as_ref(), hostname, port, ResolveFlags::default())
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .find(|a| a.is_ipv4())
            .ok_or_else(|| format!("no v4 address for {hostname}")),
        None => Err(format!("no resolver injected for {hostname}")),
    }
}

/// uTP datagram detection — `udp_socket::incoming_packet` does the
/// same check upstream: 20-byte header, `version == 1`
/// (low nibble) and `type <= ST_RESET` (high nibble).
/// `boost::system::errc::too_many_files_open` — EMFILE (24) /
/// ENFILE (23) on unix, WSAEMFILE (10024) on Windows. Upstream
/// retries the accept on this (transient fd pressure) after
/// disconnecting a peer + clamping `connections_limit`.
fn is_fd_exhausted(e: &bitdaemon_net::NetError) -> bool {
    let bitdaemon_net::NetError::Transport(io) = e else {
        return false;
    };
    matches!(io.raw_os_error(), Some(24 | 23) | Some(10024))
}

fn is_utp_datagram(buf: &[u8]) -> bool {
    buf.len() >= 20 && buf[0] & 0x0f == 1 && (buf[0] >> 4) <= 4
}

/// upstream `read_pe_cryptofield`'s `crypto_select` — intersect the
/// peer's `crypto_provide` with `allowed_enc_level`, keep the most
/// significant bit when `prefer_rc4`, the least significant
/// otherwise (bt_peer_connection.cpp:3153).
fn crypto_select(provide: u32, pe: bitdaemon_net::pe_crypto::PeSettings) -> u32 {
    use bitdaemon_net::mse::{CRYPTO_PLAINTEXT, CRYPTO_RC4};
    let allowed = provide & u32::from(pe.allowed_enc_level);
    if pe.prefer_rc4 {
        if allowed & CRYPTO_RC4 != 0 {
            CRYPTO_RC4
        } else {
            allowed & CRYPTO_PLAINTEXT
        }
    } else if allowed & CRYPTO_PLAINTEXT != 0 {
        CRYPTO_PLAINTEXT
    } else {
        allowed & CRYPTO_RC4
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // `session_impl::abort_stage2` — closing the bandwidth
        // managers drains queued requests (each peer gets its
        // assigned quota dispatched) before the tasks abort.
        if let Ok(s) = self.session.lock() {
            let mut s = s;
            s.download_rate.close();
            s.upload_rate.close();
        }
        for t in &self.tasks {
            t.abort();
        }
        debug!("engine drop: {} tasks aborted", self.tasks.len());
    }
}
