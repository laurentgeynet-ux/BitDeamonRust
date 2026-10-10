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
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// One completed tracker exchange, reported back to the session.
struct AnnounceDone {
    ih: Sha1Hash,
    url: String,
    result: Result<bitdaemon_net::http_tracker::TrackerResponse, String>,
}

/// An inbound peer message, tagged with its stream's peer id.
struct InboundPeerMsg {
    packet: PeerPacket,
}

/// `Engine` — everything outside `Session` that upstream does in
/// `session_impl`'s io-context loop.
pub struct Engine {
    /// the synchronous core.
    pub session: Session,
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
    /// spawned task handles (kept for abort on drop).
    tasks: Vec<JoinHandle<()>>,
    /// pipeline settings handed to every new `PeerConn`
    /// (`queue_time`, `request_timeout`, `max_out_request_queue` —
    /// `settings_pack` values upstream).
    pub queue_time: i32,
    /// `settings_pack::request_timeout` (s).
    pub request_timeout: i32,
    /// `settings_pack::max_out_request_queue`.
    pub max_out_request_queue: u16,
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
        Self {
            session: Session::new(),
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
            tasks: Vec::new(),
            queue_time: 3,
            request_timeout: 60,
            max_out_request_queue: 500,
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
        self.session.listen_port = port;
    }

    /// `peer_id` passthrough.
    pub fn set_peer_id(&mut self, pid: PeerId) {
        self.session.peer_id = pid;
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
            self.session.on_peer_message(&m.packet, &mut out);
        }
        while let Ok((ih, peer)) = self.gone_rx.try_recv() {
            self.writers.remove(&(ih, peer));
            self.session.peer_disconnected(&ih, peer, &mut out);
        }
        while let Ok(a) = self.announce_rx.try_recv() {
            self.session.on_announce_response(&a.ih, &a.url, a.result);
        }
        while let Ok((from, buf)) = self.udp_rx.try_recv() {
            self.session.on_udp(from, &buf, &mut out);
        }

        self.session.tick(&mut out);
        self.session.tracker_announce_due(now, 0, &mut out);
        self.dispatch(out).await;
        self.session.pop_alerts()
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
            let _ = tx.send(AnnounceDone { ih, url, result });
        });
        self.tasks.push(handle);
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
        let mut s = self
            .connector
            .clone()
            .connect(remote)
            .await
            .map_err(std::io::Error::other)?;
        let pid = self.session.peer_id;
        // encrypted flag selects the MSE path upstream-style;
        // plaintext default for now (`run_handshake_encrypted_*`
        // available when the caller wants PE)
        let _hs = if encrypted {
            bitdaemon_net::bt_peer_conn::run_handshake_encrypted_initiator(
                &mut s,
                ih,
                &pid,
                false,
                false,
                bitdaemon_net::mse::CRYPTO_PLAINTEXT | bitdaemon_net::mse::CRYPTO_RC4,
                0,
            )
            .await
            .map_err(std::io::Error::other)?
            .0
        } else {
            run_handshake(&mut s, ih, &pid, false, false)
                .await
                .map_err(std::io::Error::other)?
        };

        let num_pieces = self
            .session
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
        self.session.peer_connected(ih, peer, conn, remote);
        self.attach_stream(*ih, peer, remote, s);
        Ok(peer)
    }

    /// our BEP-10 offer — `ut_metadata`/`ut_pex` ids (matching
    /// `PeerConn::local_ut_*` defaults) + listen port and the
    /// torrent's metadata size when known.
    fn ext_handshake(&self, ih: &Sha1Hash) -> bitdaemon_net::bep10::ExtendedHandshake {
        let mut hs = bitdaemon_net::bep10::ExtendedHandshake {
            listen_port: Some(i64::from(self.session.listen_port)),
            reqq: i64::from(self.max_out_request_queue),
            ..Default::default()
        };
        hs.messages.insert("ut_metadata".into(), 1);
        hs.messages.insert("ut_pex".into(), 2);
        if let Some(t) = self.session.torrents.get(ih) {
            if let Some(md) = &t.metadata_raw {
                hs.metadata_size = Some(md.len() as i64);
            }
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
        let pid = self.session.peer_id;
        let ih_known = {
            // snapshot of known info-hashes for the callback
            let set: Vec<Sha1Hash> = self.session.torrents.keys().copied().collect();
            move |ih: &Sha1Hash| set.contains(ih)
        };
        let hs = bitdaemon_net::bt_peer_conn::run_handshake_responder(
            &mut s, &pid, false, false, ih_known,
        )
        .await
        .map_err(std::io::Error::other)?;
        let num_pieces = self
            .session
            .torrents
            .get(&hs.info_hash)
            .map_or(0, |t| t.num_pieces as usize);
        let peer = self.next_peer;
        self.next_peer += 1;
        self.session.peer_connected(
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
        );
        self.attach_stream(hs.info_hash, peer, remote, s);
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
        // one task owns the stream: select between inbound decode and
        // outbound encode (upstream's per-connection pump).
        let h = tokio::spawn(async move {
            loop {
                tokio::select! {
                    m = read_message(&mut s) => {
                        match m {
                            Ok(msg) => {
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
                    w = rx.recv() => {
                        match w {
                            Some(msg) => {
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
        for (ih, t) in &mut self.session.torrents {
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
