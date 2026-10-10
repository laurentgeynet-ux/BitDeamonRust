// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

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
use std::sync::Arc;
use std::time::Instant;

use bitdaemon_common::types::PeerRequest;
use bitdaemon_common::{PeerId, Sha1Hash};
use bitdaemon_net::bt_peer_conn::{read_message, run_handshake, write_message};
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
use tracing::{debug, warn};

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
}

/// token-bucket rate limiter shared by every peer writer task —
/// async counterpart of upstream's `bandwidth_channel` grant loop
/// (`throttle` sets bytes/s; `0` = unlimited, refill each tick,
/// burst capped at one second of quota).
pub struct RateLimiter {
    tokens: std::sync::atomic::AtomicI64,
    limit: std::sync::atomic::AtomicI64,
    notify: tokio::sync::Notify,
}

impl RateLimiter {
    /// `bandwidth_channel` — `0` means unlimited.
    #[must_use]
    pub fn new(bytes_per_sec: i64) -> Self {
        Self {
            tokens: std::sync::atomic::AtomicI64::new(bytes_per_sec),
            limit: std::sync::atomic::AtomicI64::new(bytes_per_sec),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// `throttle`.
    pub fn set_limit(&self, bytes_per_sec: i64) {
        self.limit
            .store(bytes_per_sec, std::sync::atomic::Ordering::Relaxed);
    }

    /// `update_quota` — grant one `dt_ms` worth of tokens (cap = 1s).
    pub fn refill(&self, dt_ms: i64) {
        let limit = self.limit.load(std::sync::atomic::Ordering::Relaxed);
        if limit <= 0 {
            return;
        }
        let grant = limit * dt_ms / 1000;
        let mut cur = self.tokens.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            let next = (cur + grant).min(limit);
            match self.tokens.compare_exchange_weak(
                cur,
                next,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(t) => cur = t,
            }
        }
        self.notify.notify_waiters();
    }

    /// `assign_bandwidth` — wait until `n` bytes may be sent.
    pub async fn acquire(&self, n: i64) {
        if self.limit.load(std::sync::atomic::Ordering::Relaxed) <= 0 {
            return;
        }
        loop {
            let cur = self.tokens.load(std::sync::atomic::Ordering::Relaxed);
            if cur >= n {
                if self
                    .tokens
                    .compare_exchange_weak(
                        cur,
                        cur - n,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
                continue;
            }
            self.notify.notified().await;
        }
    }
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

/// A UPnP SOAP response back to the engine loop — `Ip` answers
/// `GetExternalIPAddress`, `Map` answers add/delete posts.
struct UpnpReply {
    dev: usize,
    mapping: usize,
    is_ip_query: bool,
    body: Result<String, String>,
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
    /// inbound datagrams (DHT, UDP tracker replies).
    udp_rx: mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>,
    udp_tx: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    /// outbound dial completions (`connection_attempt` jobs).
    connect_rx: mpsc::UnboundedReceiver<DialDone>,
    connect_tx: mpsc::UnboundedSender<DialDone>,
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
    /// `settings_pack::upload_rate_limit` — shared upload token
    /// bucket for every peer writer (payload bytes only, like
    /// upstream's channel-0 data accounting).
    pub upload_limiter: Arc<RateLimiter>,
    /// `settings_pack::download_rate_limit` — shared download
    /// bucket (upstream throttles incoming payload the same way).
    pub download_limiter: Arc<RateLimiter>,
    /// per-torrent limiters (`torrent::set_upload_limit`/
    /// `set_download_limit` → `peer_class` channels upstream).
    /// Created lazily in `attach_stream`, re-throttled each tick.
    pub torrent_up_limiters: std::collections::HashMap<Sha1Hash, Arc<RateLimiter>>,
    /// per-torrent download limiters.
    pub torrent_down_limiters: std::collections::HashMap<Sha1Hash, Arc<RateLimiter>>,
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
        let (udp_tx, udp_rx) = mpsc::unbounded_channel();
        let (connect_tx, connect_rx) = mpsc::unbounded_channel();
        let (webseed_tx, webseed_rx) = mpsc::unbounded_channel();
        let (dhtnode_tx, dhtnode_rx) = mpsc::unbounded_channel();
        let (upnp_tx, upnp_rx) = mpsc::unbounded_channel();
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
            udp_rx,
            udp_tx,
            connect_rx,
            connect_tx,
            webseed_rx,
            webseed_tx,
            dhtnode_rx,
            dhtnode_tx,
            natpmp: None,
            upnp_devices: Vec::new(),
            upnp_rx,
            upnp_tx,
            tasks: Vec::new(),
            known_ihs: Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new())),
            queue_time: 3,
            request_timeout: 60,
            max_out_request_queue: 500,
            upload_limiter: Arc::new(RateLimiter::new(0)),
            download_limiter: Arc::new(RateLimiter::new(0)),
            torrent_up_limiters: std::collections::HashMap::new(),
            torrent_down_limiters: std::collections::HashMap::new(),
        }
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
        self.tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; 65_535];
            while let Ok((n, from)) = dg.recv_from(&mut buf).await {
                if tx.send((from, buf[..n].to_vec())).is_err() {
                    return;
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

    /// `on_tick` — pump inbound messages, announce completions,
    /// disconnects, then the session's own `tick` +
    /// `tracker_announce_due`, and fan every effect out.
    /// Returns surfaced `Alert`s.
    pub async fn tick(&mut self, now: Instant) -> Vec<Alert> {
        let mut out = Vec::new();

        while let Ok(m) = self.inbound_rx.try_recv() {
            slock(&self.session).on_peer_message(&m.packet, &mut out);
        }
        while let Ok((ih, peer)) = self.gone_rx.try_recv() {
            self.writers.remove(&(ih, peer));
            slock(&self.session).peer_disconnected(&ih, peer, &mut out);
        }
        while let Ok(a) = self.announce_rx.try_recv() {
            if a.scrape {
                slock(&self.session).on_scrape_response(&a.ih, &a.url, a.result);
            } else {
                slock(&self.session).on_announce_response(&a.ih, &a.url, a.result);
            }
        }
        while let Ok((from, buf)) = self.udp_rx.try_recv() {
            // demux: NAT-PMP replies come from the gateway :5351 —
            // upstream binds a separate socket; injected transports
            // multiplex on one, so dispatch by sender.
            if self.natpmp.as_ref().is_some_and(|n| from == n.gateway) {
                self.on_natpmp_response(&buf);
            } else {
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
            slock(&self.session).dht_add_node(a);
        }
        while let Ok(r) = self.upnp_rx.try_recv() {
            self.on_upnp_reply(r);
        }

        // `update_quota` — refill the upload bucket each tick
        self.upload_limiter
            .set_limit(slock(&self.session).settings.upload_rate_limit);
        self.upload_limiter.refill(1000);
        self.download_limiter
            .set_limit(slock(&self.session).settings.download_rate_limit);
        self.download_limiter.refill(1000);
        // per-torrent channels — `torrent::set_{up,down}load_limit`
        for (ih, t) in &slock(&self.session).torrents {
            self.torrent_up_limiters
                .entry(*ih)
                .or_insert_with(|| Arc::new(RateLimiter::new(t.upload_limit)))
                .set_limit(t.upload_limit);
            self.torrent_down_limiters
                .entry(*ih)
                .or_insert_with(|| Arc::new(RateLimiter::new(t.download_limit)))
                .set_limit(t.download_limit);
        }
        self.torrent_up_limiters
            .retain(|ih, _| slock(&self.session).torrents.contains_key(ih));
        self.torrent_down_limiters
            .retain(|ih, _| slock(&self.session).torrents.contains_key(ih));
        for l in self
            .torrent_up_limiters
            .values()
            .chain(self.torrent_down_limiters.values())
        {
            l.refill(1000);
        }
        // refresh the acceptor's known info-hash set
        *self.known_ihs.lock().unwrap_or_else(|e| e.into_inner()) =
            slock(&self.session).torrents.keys().copied().collect();
        slock(&self.session).tick(&mut out);
        slock(&self.session).tracker_announce_due(now, 0, &mut out);
        self.dispatch(out).await;
        self.natpmp_renew().await;
        slock(&self.session).pop_alerts()
    }

    /// `ToPeer`-only fan-out for sync call sites (connect paths —
    /// bitfield/first-have emission after `attach_stream`).
    fn send_msgs_to_peers(&mut self, out: Vec<SessionOut>) {
        for e in out {
            if let SessionOut::ToPeer { ih, peer, msg } = e {
                if let Some(tx) = self.writers.get(&(ih, peer)) {
                    if tx.send(msg).is_err() {
                        self.writers.remove(&(ih, peer));
                    }
                }
            }
        }
    }

    /// fan `SessionOut` effects to transports/peers.
    async fn dispatch(&mut self, out: Vec<SessionOut>) {
        for e in out {
            match e {
                SessionOut::ToPeer { ih, peer, msg } => {
                    if let Some(tx) = self.writers.get(&(ih, peer)) {
                        if tx.send(msg).is_err() {
                            self.writers.remove(&(ih, peer));
                        }
                    }
                }
                SessionOut::UdpPacket(buf, to) => {
                    if let Some(d) = &self.datagram {
                        let _ = d.send_to(&buf, to).await;
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
                    self.tasks.push(tokio::spawn(async move {
                        let Ok(port) = u16::try_from(port) else {
                            return;
                        };
                        if let Ok(addr) = resolve(&resolver, &host, port).await {
                            let _ = dht_tx.send(addr);
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
                    } = &e
                    {
                        warn!(peer = peer, reason = reason, "peer disconnect");
                    }
                    // routed upstream via alerts already
                }
                SessionOut::Alert(_) => {}
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
                            scrape: false,
                        });
                        return;
                    };
                    let port = u16::try_from(c.port).unwrap_or(80);
                    match resolve(&resolver, &c.hostname, port).await {
                        Ok(addr) => announce_udp(
                            dg.as_ref(),
                            addr,
                            &params,
                            std::time::Duration::from_secs(15),
                            2,
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
                    match resolve(&resolver, &c.hostname, port).await {
                        Ok(addr) => match connector.connect(addr).await {
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
                                    bitdaemon_net::ws_tracker::build_announce(&params, &[], 0);
                                bitdaemon_net::ws_tracker::announce_ws(
                                    &mut s, &host, &c.path, &key, &payload,
                                )
                                .await
                                .map(|r| bitdaemon_net::http_tracker::TrackerResponse {
                                    interval: r.interval as i32,
                                    min_interval: r.min_interval as i32,
                                    complete: r.complete,
                                    incomplete: r.incomplete,
                                    downloaded: r.downloaded,
                                    ..Default::default()
                                })
                                .map_err(|e| e.to_string())
                            }
                            Err(e) => Err(format!("connect: {e}")),
                        },
                        Err(e) => Err(e),
                    }
                }
                Ok(c) => {
                    // http/https — dial a stream and GET
                    let default_port = if c.protocol == "https" { 443 } else { 80 };
                    let port = u16::try_from(c.port).unwrap_or(default_port);
                    match resolve(&resolver, &c.hostname, port).await {
                        Ok(addr) => match connector.connect(addr).await {
                            Ok(mut s) => {
                                let host = if c.port < 0 {
                                    c.hostname.clone()
                                } else {
                                    format!("{}:{}", c.hostname, port)
                                };
                                announce_http(
                                    &mut s,
                                    &host,
                                    &c.path,
                                    &params,
                                    if tracker_id.is_empty() {
                                        None
                                    } else {
                                        Some(tracker_id.as_bytes())
                                    },
                                    &[],
                                    1024 * 1024,
                                )
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
                    match resolve(&resolver, &c.hostname, port).await {
                        Ok(addr) => bitdaemon_net::tracker_client::scrape_udp(
                            dg.as_ref(),
                            addr,
                            ih.as_bytes(),
                            std::time::Duration::from_secs(15),
                            2,
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
                        Ok(addr) => match connector.connect(addr).await {
                            Ok(mut s) => {
                                let host = if c.port < 0 {
                                    c.hostname.clone()
                                } else {
                                    format!("{}:{}", c.hostname, port)
                                };
                                bitdaemon_net::tracker_client::scrape_http(
                                    &mut s,
                                    &host,
                                    &c.path,
                                    ih.as_bytes(),
                                    &[],
                                    1024 * 1024,
                                )
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
        use bitdaemon_net::web_seed::{WebSeedHeaders, build_piece_request, fetch_block};
        let tx = self.webseed_tx.clone();
        let connector = self.connector.clone();
        let resolver = self.resolver.clone();
        self.tasks_forget();
        let handle = tokio::spawn(async move {
            macro_rules! fail {
                () => {
                    let _ = tx.send(WebSeedDone {
                        ih,
                        result: Err(reqs),
                    });
                };
            }
            let Ok(c) = bitdaemon_net::parse_url::parse_url_components(&url) else {
                fail!();
                return;
            };
            let port = u16::try_from(c.port).unwrap_or(80);
            let Ok(addr) = resolve(&resolver, &c.hostname, port).await else {
                fail!();
                return;
            };
            let Ok(mut s) = connector.connect(addr).await else {
                fail!();
                return;
            };
            let host = if c.port < 0 {
                c.hostname.clone()
            } else {
                format!("{}:{}", c.hostname, port)
            };
            let headers = WebSeedHeaders {
                host,
                send_user_agent: false,
                ..Default::default()
            };
            let mut done = Vec::with_capacity(reqs.len());
            for i in 0..reqs.len() {
                let r = reqs[i];
                let wreq = build_piece_request(
                    &files,
                    piece_length,
                    r.piece,
                    i64::from(r.start),
                    i64::from(r.length),
                    &url,
                    &c.path,
                    single_file,
                    false,
                    &headers,
                );
                match fetch_block(&mut s, &wreq, 4 * 1024 * 1024).await {
                    Ok(segs) => {
                        // reassemble in `m_file_requests` order —
                        // pad slices contribute zeroes
                        let mut data = Vec::with_capacity(r.length as usize);
                        let mut it = segs.into_iter();
                        for f in &wreq.files {
                            if f.got_get {
                                if let Some((_, _, b)) = it.next() {
                                    data.extend_from_slice(&b);
                                }
                            } else {
                                data.resize(data.len() + f.length as usize, 0);
                            }
                        }
                        done.push((r, data));
                    }
                    Err(_) => {
                        fail!();
                        return;
                    }
                }
            }
            let _ = tx.send(WebSeedDone {
                ih,
                result: Ok(done),
            });
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
                slock(&self.session).ip_voter.cast_vote(
                    std::net::IpAddr::V4(ip),
                    bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_ROUTER,
                    &std::net::IpAddr::V4(ip),
                );
            }
            NatPmpResponse::Map {
                private_port,
                public_port,
                lifetime,
                result,
            } => {
                if let Some(n) = &mut self.natpmp {
                    if let Some(m) = n.mappings.iter_mut().find(|m| m.1 == private_port) {
                        if result == 0 {
                            m.2 = public_port;
                            // renew at half the lease (upstream
                            // `mapping.expires` semantics)
                            m.3 = now + std::time::Duration::from_secs(u64::from(lifetime / 2));
                        }
                    }
                }
            }
        }
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
        let mut d = UpnpDriver::new(dev, 3600, description);
        let outs = d.set_mappings(vec![
            (PortmapProtocol::Tcp, local_port, local_port, String::new()),
            (PortmapProtocol::Udp, local_port, local_port, String::new()),
        ]);
        self.upnp_devices.push(d);
        let idx = self.upnp_devices.len() - 1;
        // ask for the router's external address first (upstream
        // chains discovery → GetExternalIPAddress → AddPortMapping)
        let outs = std::iter::once(self.upnp_devices[idx].discover())
            .chain(outs)
            .collect::<Vec<_>>();
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
                    action,
                    body,
                } => self.spawn_upnp_post(dev, mapping, action, body),
                UpnpOut::Mapped {
                    external_ip: Some(ip),
                    ..
                } => {
                    slock(&self.session).ip_voter.cast_vote(
                        ip,
                        bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_ROUTER,
                        &ip,
                    );
                }
                _ => {}
            }
        }
    }

    /// one SOAP POST through the injected transport, result back
    /// through `upnp_tx`.
    fn spawn_upnp_post(&mut self, dev: usize, mapping: usize, action: String, body: String) {
        let Some(d) = self.upnp_devices.get(dev) else {
            return;
        };
        let is_ip_query = action.contains("GetExternalIPAddress");
        let url = d.dev.control_url.clone();
        let tx = self.upnp_tx.clone();
        let connector = self.connector.clone();
        let resolver = self.resolver.clone();
        self.tasks_forget();
        self.tasks.push(tokio::spawn(async move {
            let send = |b: Result<String, String>| async move {
                let _ = tx.send(UpnpReply {
                    dev,
                    mapping,
                    is_ip_query,
                    body: b,
                });
            };
            let Ok(c) = bitdaemon_net::parse_url::parse_url_components(&url) else {
                send(Err("bad control url".into())).await;
                return;
            };
            let port = u16::try_from(c.port).unwrap_or(80);
            let Ok(addr) = resolve(&resolver, &c.hostname, port).await else {
                send(Err("resolve".into())).await;
                return;
            };
            let Ok(mut s) = connector.connect(addr).await else {
                send(Err("connect".into())).await;
                return;
            };
            let host = if c.port < 0 {
                c.hostname.clone()
            } else {
                format!("{}:{}", c.hostname, port)
            };
            let headers = [
                ("Content-Type".to_string(), "text/xml".to_string()),
                ("SOAPAction".to_string(), action),
            ];
            let r = async {
                bitdaemon_net::http_conn::send_request(
                    &mut s,
                    "POST",
                    &c.path,
                    &host,
                    &headers,
                    body.as_bytes(),
                )
                .await?;
                bitdaemon_net::http_conn::recv_response(&mut s, 64 * 1024).await
            }
            .await;
            match r {
                Ok(resp) => {
                    send(Ok(String::from_utf8_lossy(&resp.body()).into_owned())).await;
                }
                Err(e) => send(Err(e.to_string())).await,
            }
        }));
    }

    /// feed a SOAP response back into the driver's state machine.
    fn on_upnp_reply(&mut self, r: UpnpReply) {
        let Ok(body) = r.body else {
            return; // transport errors drop like upstream timeouts
        };
        let outs = if r.is_ip_query {
            self.upnp_devices
                .get_mut(r.dev)
                .map(|d| d.on_ip_response(&body))
        } else {
            self.upnp_devices
                .get_mut(r.dev)
                .map(|d| d.on_map_response(r.mapping, &body, rand::random::<u16>()))
        };
        if let Some(outs) = outs {
            self.drive_upnp_outs(r.dev, outs);
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
        let s_res = self.connector.clone().connect(remote).await;
        let mut s = match s_res {
            Ok(s) => s,
            Err(e) => {
                slock(&self.session).peer_connect_failed(ih, remote);
                return Err(std::io::Error::other(e));
            }
        };
        let pid = slock(&self.session).peer_id;
        // encrypted flag selects the MSE path upstream-style;
        // plaintext default for now (`run_handshake_encrypted_*`
        // available when the caller wants PE)
        let _hs = if encrypted {
            match bitdaemon_net::bt_peer_conn::run_handshake_encrypted_initiator(
                &mut s,
                ih,
                &pid,
                false,
                false,
                bitdaemon_net::mse::CRYPTO_PLAINTEXT | bitdaemon_net::mse::CRYPTO_RC4,
                0,
            )
            .await
            {
                Ok((hs, _)) => hs,
                Err(e) => {
                    slock(&self.session).peer_connect_failed(ih, remote);
                    return Err(std::io::Error::other(e));
                }
            }
        } else {
            match run_handshake(&mut s, ih, &pid, false, false).await {
                Ok(hs) => hs,
                Err(e) => {
                    slock(&self.session).peer_connect_failed(ih, remote);
                    return Err(std::io::Error::other(e));
                }
            }
        };

        let num_pieces = slock(&self.session)
            .torrents
            .get(ih)
            .map_or(0, |t| t.num_pieces as usize);
        // BEP-10: advertise our extension ids right after the handshake
        let _ =
            bitdaemon_net::bt_peer_conn::send_extended_handshake(&mut s, &self.ext_handshake(ih))
                .await;
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
        let mut out = Vec::new();
        slock(&self.session).peer_connected(ih, peer, conn, remote, &mut out);
        self.attach_stream(*ih, peer, remote, s);
        self.send_msgs_to_peers(out);
        Ok(peer)
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
        let s = match d.stream {
            Ok(s) => s,
            Err(e) => {
                warn!(remote = ?d.remote, err = e, "dial failed");
                if d.outbound {
                    if let Some(r) = d.remote {
                        slock(&self.session).peer_connect_failed(&d.ih, r);
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
        let mut out = Vec::new();
        slock(&self.session).peer_connected(&d.ih, peer, conn, remote, &mut out);
        self.attach_stream(d.ih, peer, remote, s);
        self.send_msgs_to_peers(out);
    }

    /// `torrent::connect_to_peer` (async form) — spawn the dial +
    /// handshake job; the result lands in `connect_rx` and is
    /// applied by the next `tick`. `max_out_request_queue`+ext ids
    /// are snapshotted now.
    pub fn spawn_dial(&mut self, ih: Sha1Hash, remote: SocketAddr, encrypted: bool) {
        let connector = self.connector.clone();
        let tx = self.connect_tx.clone();
        let pid = slock(&self.session).peer_id;
        let ext = self.ext_handshake(&ih);
        let h = tokio::spawn(async move {
            let send_err = |e: String| {
                let _ = tx.send(DialDone {
                    ih,
                    remote: Some(remote),
                    stream: Err(e),
                    outbound: true,
                    fast: false,
                    dht: false,
                });
            };
            let mut s = match connector.connect(remote).await {
                Ok(s) => s,
                Err(e) => {
                    send_err(e.to_string());
                    return;
                }
            };
            let hs = if encrypted {
                bitdaemon_net::bt_peer_conn::run_handshake_encrypted_initiator(
                    &mut s,
                    &ih,
                    &pid,
                    false,
                    false,
                    bitdaemon_net::mse::CRYPTO_PLAINTEXT | bitdaemon_net::mse::CRYPTO_RC4,
                    0,
                )
                .await
                .map(|(hs, _)| hs)
            } else {
                run_handshake(&mut s, &ih, &pid, false, false).await
            };
            if let Err(e) = hs {
                send_err(e.to_string());
                return;
            }
            if let Err(e) = bitdaemon_net::bt_peer_conn::send_extended_handshake(&mut s, &ext).await
            {
                send_err(e.to_string());
                return;
            }
            let _ = tx.send(DialDone {
                ih,
                remote: Some(remote),
                stream: Ok(s),
                outbound: true,
                fast: hs.as_ref().is_ok_and(|h| h.supports_fast()),
                dht: hs.as_ref().is_ok_and(|h| h.supports_dht()),
            });
        });
        self.tasks.push(h);
    }

    /// `listen_sockets` accept pump — one task per acceptor: each
    /// inbound stream runs the responder handshake (info-hash gated
    /// by `known`, refreshed every tick) and lands in `connect_rx`
    /// like an outbound dial.
    pub fn start_acceptor(&mut self, acceptor: Arc<dyn bitdaemon_net::transport::PeerAcceptor>) {
        let tx = self.connect_tx.clone();
        let known = self.known_ihs.clone();
        let pid = slock(&self.session).peer_id;
        let h = tokio::spawn(async move {
            loop {
                let acc = acceptor.clone().accept().await;
                let mut s = match acc {
                    Ok((s, _remote)) => s,
                    Err(e) => {
                        warn!(err = %e, "accept failed");
                        return;
                    }
                };
                let hs = bitdaemon_net::bt_peer_conn::run_handshake_responder(
                    &mut s,
                    &pid,
                    false,
                    false,
                    |ih| known.lock().unwrap_or_else(|e| e.into_inner()).contains(ih),
                )
                .await;
                match hs {
                    Ok(hs) => {
                        let _ = tx.send(DialDone {
                            ih: hs.info_hash,
                            remote: None,
                            stream: Ok(s),
                            outbound: false,
                            fast: hs.supports_fast(),
                            dht: hs.supports_dht(),
                        });
                    }
                    Err(e) => {
                        warn!(err = %e, "inbound handshake failed");
                    }
                }
            }
        });
        self.tasks.push(h);
    }

    /// `PeerListState` snapshot for `connect_pump` from session
    /// settings (`session_impl::update_peer_connections` upstream
    /// builds the same from `settings_pack`).
    #[must_use]
    pub fn peer_list_state(&self) -> bitdaemon_net::peer_list::PeerListState {
        let s = &slock(&self.session).settings;
        bitdaemon_net::peer_list::PeerListState {
            is_finished: false,
            external_ip: None,
            external_port: slock(&self.session).listen_port,
            min_reconnect_time: s.min_reconnect_time,
            max_peerlist_size: s.max_peerlist_size,
            max_failcount: s.max_failcount,
            allow_multiple_connections_per_ip: s.allow_multiple_connections_per_ip,
        }
    }

    /// `session_impl::run` — the canonical driver: 1 Hz tick
    /// (announces, peer drive, disk completions, DHT, LSD) plus the
    /// connect pump dialing up to `settings.connections_limit`
    /// candidates. Runs until `stop` resolves.
    pub async fn run<F: std::future::Future<Output = ()>>(
        &mut self,
        stop: F,
        max_dials_per_tick: usize,
    ) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        tokio::pin!(stop);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = &mut stop => break,
            }
            self.tick(Instant::now()).await;
            let st = self.peer_list_state();
            let stime = slock(&self.session).session_time;
            let candidates = self.connect_pump(stime, &st, max_dials_per_tick);
            for (ih, ep) in candidates {
                self.spawn_dial(ih, ep, false);
            }
        }
    }

    /// our BEP-10 offer — `ut_metadata`/`ut_pex` ids (matching
    /// `PeerConn::local_ut_*` defaults) + listen port and the
    /// torrent's metadata size when known.
    fn ext_handshake(&self, ih: &Sha1Hash) -> bitdaemon_net::bep10::ExtendedHandshake {
        let mut hs = bitdaemon_net::bep10::ExtendedHandshake {
            listen_port: Some(i64::from(slock(&self.session).listen_port)),
            reqq: i64::from(self.max_out_request_queue),
            ..Default::default()
        };
        hs.messages.insert("ut_metadata".into(), 1);
        hs.messages.insert("ut_pex".into(), 2);
        hs.messages.insert("lt_donthave".into(), 3);
        if let Some(t) = slock(&self.session).torrents.get(ih) {
            if let Some(md) = &t.metadata_raw {
                hs.metadata_size = Some(md.len() as i64);
            }
            // BEP-21 — seeds advertise `upload_only`; likewise
            // `share_mode` when the swarm flag is set.
            hs.upload_only = t.is_seed();
            hs.share_mode = t.share_mode;
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
        let pid = slock(&self.session).peer_id;
        let ih_known = {
            // snapshot of known info-hashes for the callback
            let set: Vec<Sha1Hash> = slock(&self.session).torrents.keys().copied().collect();
            move |ih: &Sha1Hash| set.contains(ih)
        };
        let hs = bitdaemon_net::bt_peer_conn::run_handshake_responder(
            &mut s, &pid, false, false, ih_known,
        )
        .await
        .map_err(std::io::Error::other)?;
        let num_pieces = slock(&self.session)
            .torrents
            .get(&hs.info_hash)
            .map_or(0, |t| t.num_pieces as usize);
        let peer = self.next_peer;
        self.next_peer += 1;
        let mut out = Vec::new();
        slock(&self.session).peer_connected(
            &hs.info_hash,
            peer,
            PeerConn::new(
                num_pieces,
                PeerPipeline::new(
                    self.queue_time,
                    self.request_timeout,
                    self.max_out_request_queue,
                ),
                0,
            ),
            remote,
            &mut out,
        );
        self.attach_stream(hs.info_hash, peer, remote, s);
        self.send_msgs_to_peers(out);
        Ok(Some(peer))
    }

    /// split a live peer stream into reader/writer tasks feeding the
    /// engine queues.
    fn attach_stream(
        &mut self,
        ih: Sha1Hash,
        peer: u64,
        remote: SocketAddr,
        mut s: BoxedPeerStream,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<PeerMessage>();
        self.writers.insert((ih, peer), tx);

        let inbound_tx = self.inbound_tx.clone();
        let gone_tx = self.gone_tx.clone();
        let ul = self.upload_limiter.clone();
        let dl = self.download_limiter.clone();
        let tul = self.torrent_up_limiters.get(&ih).cloned();
        let tdl = self.torrent_down_limiters.get(&ih).cloned();
        // one task owns the stream: select between inbound decode and
        // outbound encode (upstream's per-connection pump).
        let h = tokio::spawn(async move {
            loop {
                tokio::select! {
                    m = read_message(&mut s) => {
                        match m {
                            Ok(msg) => {
                                // download throttle — hold the
                                // pump, TCP back-pressure does the
                                // rest (upstream rate-limits reads)
                                if let PeerMessage::Piece { data, .. } = &msg {
                                    dl.acquire(data.len() as i64).await;
                                    if let Some(l) = &tdl {
                                        l.acquire(data.len() as i64).await;
                                    }
                                }
                                if inbound_tx
                                    .send(InboundPeerMsg {
                                        packet: PeerPacket {
                                            info_hash: ih,
                                            peer,
                                            remote,
                                            msg,
                                        },
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    w = tokio::time::timeout(
                        std::time::Duration::from_secs(60),
                        rx.recv(),
                    ) => {
                        // upstream sends a keepalive when the conn
                        // has been silent ~2min; 60s keeps buffers
                        // warm under NAT without wire chatter.
                        let w = match w {
                            Err(_) => Some(PeerMessage::Keepalive),
                            Ok(v) => v,
                        };
                        match w {
                            Some(msg) => {
                                // throttle bulk payload (upstream
                                // throttles on the send path)
                                if let PeerMessage::Piece { data, .. } = &msg {
                                    ul.acquire(data.len() as i64).await;
                                    if let Some(l) = &tul {
                                        l.acquire(data.len() as i64).await;
                                    }
                                }
                                if write_message(&mut s, &msg).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
            let _ = gone_tx.send((ih, peer));
        });
        self.tasks.push(h);
    }

    /// `torrent::connect_to_peer` pump — ask every torrent for
    /// connect candidates (`want_peers` gate like upstream) and
    /// spawn dial tasks; each success registers via
    /// [`Engine::connect_peer`]. `max_peers` caps new dials.
    /// Returns endpoints being dialed (for tests/metrics).
    pub fn connect_pump(
        &mut self,
        session_time: i32,
        state: &bitdaemon_net::peer_list::PeerListState,
        max_peers: usize,
    ) -> Vec<(Sha1Hash, SocketAddr)> {
        let mut dialing = Vec::new();
        for (ih, t) in &mut slock(&self.session).torrents {
            if dialing.len() >= max_peers || !t.want_peers() {
                continue;
            }
            for ep in t.connect_candidates(session_time, state) {
                if dialing.len() >= max_peers {
                    break;
                }
                dialing.push((*ih, ep));
            }
        }
        dialing
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

impl Drop for Engine {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        debug!("engine drop: {} tasks aborted", self.tasks.len());
    }
}
