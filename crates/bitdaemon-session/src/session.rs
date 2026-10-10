// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

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
use std::sync::Arc;
use std::time::{Duration, Instant};

use bitdaemon_common::types::{PeerRequest, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_dht::dht_tracker::DhtTracker;
use bitdaemon_disk::disk_buffer_pool::DiskBufferHolder;
use bitdaemon_disk::disk_interface::DiskIo;
use bitdaemon_disk::storage_error::StorageError;
use bitdaemon_net::peer_wire::PeerMessage;

use crate::alerts::{Alert, AlertManager};
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
        /// announce parameters (event, counters, key).
        params: bitdaemon_net::tracker_client::AnnounceParams,
    },
}

/// `session_impl` — all state owned on one thread.
pub struct Session {
    /// `m_torrents` by info_hash.
    pub torrents: BTreeMap<Sha1Hash, Torrent>,
    /// `m_dht` — v4/v6 tracker nodes.
    pub dht: DhtTracker,
    /// `m_disk_thread` — injected backend (`StorageFactory` seam).
    pub disk: Option<Arc<dyn DiskIo>>,
    /// `m_alert_manager`.
    pub alerts: AlertManager,
    /// `m_stats_counters`.
    pub counters: Counters,
    /// `m_peer_id` — generated with the default fingerprint
    /// (`generate_peer_id`); used in announces and handshakes.
    pub peer_id: bitdaemon_common::PeerId,
    /// `m_listen_port` — our listen port (`announce request.port`).
    pub listen_port: u16,
    /// `m_key` — announce key (per-session random, BEP-7).
    pub key: i32,
    /// completions pushed by disk-job callbacks (any thread).
    disk_tx: std::sync::mpsc::Sender<DiskCompleted>,
    disk_rx: std::sync::mpsc::Receiver<DiskCompleted>,
    /// `m_cookie` — filters our own multicast loopback (LSD).
    pub lsd_cookie: u32,
    /// last LSD broadcast.
    last_lsd: Option<Instant>,
    /// `m_session_time` — seconds since session start, bumped once
    /// per `tick` (peer_list reconnect backoff epoch).
    pub session_time: i32,
    /// `m_ip_filter` — access filter applied to new + existing peers.
    ip_filter: bitdaemon_net::ip_filter::IpFilter,
    /// `settings_pack` — typed session knobs (`m_settings` upstream).
    pub settings: crate::settings::SessionSettings,
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
    },
    /// `async_write` — block persisted; schedule piece hash.
    Write {
        /// torrent.
        ih: Sha1Hash,
        /// block written.
        req: PeerRequest,
        /// job result.
        err: StorageError,
    },
    /// `async_check_files` — fastresume validation verdict
    /// (`ret` = accepted).
    CheckFiles {
        /// torrent.
        ih: Sha1Hash,
        /// resume accepted (`verify_resume_data` verdict).
        accepted: bool,
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
        /// job result.
        err: StorageError,
    },
}

impl Default for Session {
    fn default() -> Self {
        let (disk_tx, disk_rx) = std::sync::mpsc::channel();
        Self {
            torrents: BTreeMap::new(),
            dht: DhtTracker::default(),
            disk: None,
            alerts: AlertManager::default(),
            counters: Counters::default(),
            peer_id: bitdaemon_common::peer_id::generate_peer_id(b"-OB0001-"),
            listen_port: 6881,
            key: {
                use rand::Rng as _;
                rand::rng().random::<i32>()
            },
            lsd_cookie: rand::random::<u32>(),
            last_lsd: None,
            session_time: 0,
            ip_filter: bitdaemon_net::ip_filter::IpFilter::new(),
            settings: crate::settings::SessionSettings::default(),
            disk_tx,
            disk_rx,
        }
    }
}

impl Session {
    /// `session_impl::session_impl`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// the sender disk-job callbacks clone to report completions.
    #[must_use]
    pub fn disk_completions(&self) -> std::sync::mpsc::Sender<DiskCompleted> {
        self.disk_tx.clone()
    }

    /// `on_disk_*` — drain completed disk jobs into swarm effects.
    /// Read → `Piece` to the peer. Write → `async_hash` the piece.
    /// Hash → `piece_verified` (passed = no error; hash comparison
    /// lands with `TorrentInfo` on `Torrent`).
    pub fn drain_disk_completed(&mut self, out: &mut Vec<SessionOut>) {
        while let Ok(c) = self.disk_rx.try_recv() {
            match c {
                DiskCompleted::Read {
                    ih,
                    peer,
                    req,
                    buf,
                    err,
                } => {
                    if err.is_err() {
                        self.alerts.post(Alert::FileError {
                            ih,
                            path: String::new(),
                            error: err.to_string(),
                        });
                        continue;
                    }
                    if let Some(data) = buf.data().map(|d| d.to_vec()) {
                        out.push(SessionOut::ToPeer {
                            ih,
                            peer,
                            msg: PeerMessage::Piece {
                                piece: req.piece,
                                offset: req.start,
                                data: data.into(),
                            },
                        });
                    }
                }
                DiskCompleted::Write { ih, req, err } => {
                    if err.is_err() {
                        self.alerts.post(Alert::FileError {
                            ih,
                            path: String::new(),
                            error: err.to_string(),
                        });
                        continue;
                    }
                    // whole piece complete? then hash it
                    if let (Some(disk), Some(t)) = (self.disk.clone(), self.torrents.get(&ih)) {
                        if t.piece_complete(req.piece) {
                            let tx = self.disk_tx.clone();
                            let storage = t.storage;
                            disk.async_hash(
                                storage,
                                req.piece,
                                Vec::new(),
                                0,
                                Box::new(move |piece, hash, err| {
                                    let _ = tx.send(DiskCompleted::Hash {
                                        ih,
                                        piece,
                                        hash,
                                        err,
                                    });
                                }),
                            );
                        }
                    }
                }
                DiskCompleted::CheckFiles { ih, accepted, err } => {
                    // `torrent::files_checked`: resume accepted → the
                    // optimistic `have` bits stand; rejected → drop
                    // them (upstream `need_full_check` → per-piece
                    // re-hash, driven by the caller scheduling
                    // `DiskHash` jobs or `check_files` again).
                    let mut eff = Vec::new();
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.on_files_checked(err.is_ok() && accepted, &mut eff);
                    }
                    if err.is_err() {
                        self.alerts.post(Alert::FileError {
                            ih,
                            path: String::new(),
                            error: err.to_string(),
                        });
                    }
                    self.route(ih, eff, out);
                }
                DiskCompleted::Hash {
                    ih,
                    piece,
                    hash,
                    err,
                } => {
                    // upstream `torrent::piece_finished` compares
                    // `j.d.h.hash` vs `hash_for_piece` — mismatch is
                    // a `hash_failed_alert` + re-download.
                    let passed = err.is_ok()
                        && self
                            .torrents
                            .get(&ih)
                            .and_then(|t| t.expected_hash(piece))
                            .is_none_or(|want| want == hash);
                    if err.is_err() {
                        self.alerts.post(Alert::FileError {
                            ih,
                            path: String::new(),
                            error: err.to_string(),
                        });
                    }
                    self.on_piece_verified(&ih, piece, passed, out);
                }
            }
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
                            });
                        }),
                        0,
                    );
                }
                TorrentEffect::DiskWrite { req, data } => {
                    let Some(t) = self.torrents.get(&ih) else {
                        continue;
                    };
                    let storage = t.storage;
                    let req = *req;
                    let tx = self.disk_tx.clone();
                    let holder = DiskBufferHolder::from_owned(data.clone());
                    disk.async_write(
                        storage,
                        req,
                        holder,
                        None,
                        Box::new(move |err| {
                            let _ = tx.send(DiskCompleted::Write { ih, req, err });
                        }),
                        0,
                    );
                }
                TorrentEffect::DiskHash { piece } => {
                    // `verify_piece` — resume restore or explicit
                    // re-check of a completed piece.
                    let Some(t) = self.torrents.get(&ih) else {
                        continue;
                    };
                    let storage = t.storage;
                    let (piece, tx) = (*piece, self.disk_tx.clone());
                    disk.async_hash(
                        storage,
                        piece,
                        Vec::new(),
                        0,
                        Box::new(move |piece, hash, err| {
                            let _ = tx.send(DiskCompleted::Hash {
                                ih,
                                piece,
                                hash,
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
        self.disk = Some(io);
    }

    /// `add_torrent` — insert (or replace) a swarm.
    pub fn add_torrent(&mut self, t: Torrent) -> Sha1Hash {
        let ih = t.info_hash;
        self.torrents.insert(ih, t);
        ih
    }

    /// `check_files` — upstream `torrent::start_checking`: run
    /// `async_check_files` on the injected `DiskIo`; the verdict
    /// arrives as `DiskCompleted::CheckFiles`.
    pub fn check_files(
        &self,
        ih: Sha1Hash,
        resume_data: Option<Box<bitdaemon_core::add_torrent_params::AddTorrentParams>>,
    ) {
        let Some(disk) = self.disk.clone() else {
            return;
        };
        let Some(t) = self.torrents.get(&ih) else {
            return;
        };
        let storage = t.storage;
        let tx = self.disk_tx.clone();
        disk.async_check_files(
            storage,
            resume_data,
            Vec::new(),
            Box::new(move |ret, err| {
                let _ = tx.send(DiskCompleted::CheckFiles {
                    ih,
                    accepted: ret != 0,
                    err,
                });
            }),
        );
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
        let mut eff = Vec::new();
        let t = Torrent::from_params(atp, storage, &mut eff)?;
        let ih = t.info_hash;
        self.torrents.insert(ih, t);
        self.route(ih, eff, out);
        Some(ih)
    }

    /// `remove_torrent` — drop the swarm (peers + picker).
    pub fn remove_torrent(&mut self, ih: &Sha1Hash) -> Option<Torrent> {
        self.torrents.remove(ih)
    }

    /// `pause`/`resume` (user pause — the auto-managed queue
    /// uses `queued` instead, like upstream).
    pub fn set_paused(&mut self, ih: &Sha1Hash, paused: bool) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.paused = paused;
        }
    }

    /// `session_impl` auto-managed queue — promote/demotion
    /// against `active_downloads`/`active_seeds`. A torrent
    /// counts toward `active_seeds` once `is_seed`, else
    /// `active_downloads`. Non-auto-managed and user-paused
    /// torrents are left alone (upstream `auto_managed` flag).
    /// `-1` limits mean unlimited.
    pub fn manage_queue(&mut self, active_downloads: i32, active_seeds: i32) {
        let mut dl_active = 0i32;
        let mut seed_active = 0i32;
        // BTreeMap order → deterministic promotion, oldest-ih first
        for t in self.torrents.values_mut() {
            if !t.auto_managed || t.paused {
                t.queued = false;
                continue;
            }
            let seed = t.is_seed();
            let over = if seed {
                seed_active >= active_seeds && active_seeds >= 0
            } else {
                dl_active >= active_downloads && active_downloads >= 0
            };
            t.queued = over;
            if !over {
                if seed {
                    seed_active += 1;
                } else {
                    dl_active += 1;
                }
            }
        }
    }

    /// feed one decoded peer message → effects.
    pub fn on_peer_message(&mut self, p: &PeerPacket, out: &mut Vec<SessionOut>) {
        // `inc_stats_counter` per incoming message class
        match p.msg {
            PeerMessage::Bitfield(_) => self.counters.inc(Counter::IncomingBitfield),
            PeerMessage::Have(_) => self.counters.inc(Counter::IncomingHave),
            PeerMessage::Request(_) => self.counters.inc(Counter::IncomingRequest),
            PeerMessage::Piece { .. } => self.counters.inc(Counter::IncomingPiece),
            PeerMessage::Cancel(_) => self.counters.inc(Counter::IncomingCancel),
            PeerMessage::Choke => self.counters.inc(Counter::IncomingChoke),
            PeerMessage::Unchoke => self.counters.inc(Counter::IncomingUnchoke),
            PeerMessage::Interested => self.counters.inc(Counter::IncomingInterested),
            PeerMessage::NotInterested => self.counters.inc(Counter::IncomingNotInterested),
            _ => {}
        }
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(&p.info_hash) {
            t.on_peer_message(p.peer, &p.msg, &mut eff);
        }
        self.route(p.info_hash, eff, out);
    }

    /// a peer connected — register it on the torrent.
    pub fn peer_connected(
        &mut self,
        ih: &Sha1Hash,
        peer: u64,
        conn: crate::peer_conn::PeerConn,
        remote: SocketAddr,
    ) {
        if let Some(t) = self.torrents.get_mut(ih) {
            let mut conn = conn;
            conn.remote = Some(remote);
            t.peer_connected_addr(remote);
            t.add_peer(peer, conn);
            self.counters.set(
                Counter::PeersConnected,
                self.torrents.values().map(|t| t.peers.len() as i64).sum(),
            );
        }
    }

    /// a peer disconnected — refcount down + abort in-flight.
    pub fn peer_disconnected(&mut self, ih: &Sha1Hash, peer: u64, out: &mut Vec<SessionOut>) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            if let Some(remote) = t.peers.get(&peer).and_then(|c| c.remote) {
                t.peer_disconnected_addr(remote, self.session_time);
            }
            t.remove_peer(peer, &mut eff);
        }
        self.route(*ih, eff, out);
    }

    /// `peer_connect_failed` — an outbound dial never completed the
    /// handshake; report it so the swarm entry backs off.
    pub fn peer_connect_failed(&mut self, ih: &Sha1Hash, remote: SocketAddr) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.peer_connect_failed(remote, self.session_time);
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
        if let Some(t) = self.torrents.get_mut(ih) {
            t.piece_verified(piece, passed, &mut eff);
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
        self.ip_filter = filter;
        let f = self.ip_filter.clone();
        let mut pending: Vec<(Sha1Hash, Vec<TorrentEffect>)> = Vec::new();
        for t in self.torrents.values_mut() {
            let banned = t
                .peer_list
                .apply_ip_filter(|a| f.access(a) == bitdaemon_net::ip_filter::BLOCKED);
            if banned.is_empty() {
                continue;
            }
            // disconnect live connections to banned endpoints
            let mut eff = Vec::new();
            let ids: Vec<u64> = t
                .peers
                .iter()
                .filter(|(_, c)| c.remote.is_some_and(|r| banned.contains(&r.ip())))
                .map(|(id, _)| *id)
                .collect();
            for id in ids {
                t.remove_peer(id, &mut eff);
            }
            pending.push((t.info_hash, eff));
        }
        for (ih, eff) in pending {
            self.route(ih, eff, out);
        }
    }

    /// current access rule for `addr` (`ip_filter::access`).
    #[must_use]
    pub fn ip_access(&self, addr: std::net::IpAddr) -> u32 {
        self.ip_filter.access(addr)
    }

    /// `start_dht` — bind the DHT node for `bind_ip`'s family.
    /// `id`/`None` → generate (BEP-42 enforced by settings).
    pub fn enable_dht(
        &mut self,
        bind_ip: std::net::IpAddr,
        id: Option<bitdaemon_dht::node_id::NodeId>,
        settings: bitdaemon_dht::dht_settings::DhtSettings,
    ) {
        self.dht.bind(bind_ip, id, settings);
    }

    /// UDP datagram → dht tracker (any family) — and LSD announces
    /// (BT-SEARCH isn't a bencoded dict, so the dht drops it first).
    pub fn on_udp(&mut self, from: SocketAddr, buf: &[u8], out: &mut Vec<SessionOut>) {
        if let Some((pkt, to)) = self.dht.incoming(from, buf) {
            out.push(SessionOut::UdpPacket(pkt, to));
            return;
        }
        if let Some(a) = bitdaemon_net::lsd::process_packet(buf, from, self.lsd_cookie) {
            for ih_raw in a.info_hashes {
                let Some(ih) = Sha1Hash::from_bytes(&ih_raw) else {
                    continue;
                };
                if let Some(t) = self.torrents.get_mut(&ih) {
                    if t.private {
                        continue;
                    }
                    let ep = SocketAddr::new(from.ip(), a.port);
                    t.peer_list
                        .add_peer(ep, bitdaemon_common::flags::PeerSourceFlags::LSD, true);
                }
            }
        }
        let _ = out;
    }

    /// `lsd::announce` cadence — every 5 min per non-private
    /// torrent, one datagram per multicast group.
    pub fn lsd_tick(&mut self, now: Instant, out: &mut Vec<SessionOut>) {
        const LSD_INTERVAL: Duration = Duration::from_secs(300);
        if self
            .last_lsd
            .is_some_and(|t| now.duration_since(t) < LSD_INTERVAL)
        {
            return;
        }
        self.last_lsd = Some(now);
        for t in self.torrents.values() {
            if t.private {
                continue;
            }
            let hex = bitdaemon_common::hex::to_hex(t.info_hash.as_bytes());
            for v6 in [false, true] {
                let pkt = bitdaemon_net::lsd::render_lsd_packet(
                    self.listen_port,
                    &hex,
                    self.lsd_cookie,
                    v6,
                );
                let to = if v6 {
                    SocketAddr::new(bitdaemon_net::lsd::LSD_ADDR6.into(), 6771)
                } else {
                    SocketAddr::new(bitdaemon_net::lsd::LSD_ADDR4.into(), 6771)
                };
                out.push(SessionOut::UdpPacket(pkt, to));
            }
        }
    }

    /// `tick` — drive every torrent's peers + dht timeouts.
    pub fn tick(&mut self, out: &mut Vec<SessionOut>) -> std::time::Duration {
        self.session_time += 1;
        self.drain_disk_completed(out);
        let wait = self.dht.tick();
        while let Some((pkt, to)) = self.dht.poll() {
            out.push(SessionOut::UdpPacket(pkt, to));
        }
        self.lsd_tick(Instant::now(), out);
        let ihs: Vec<Sha1Hash> = self
            .torrents
            .iter()
            .filter(|(_, t)| !t.paused && t.want_peers())
            .map(|(ih, _)| *ih)
            .collect();
        for ih in ihs {
            let mut eff = Vec::new();
            if let Some(t) = self.torrents.get_mut(&ih) {
                t.metadata_tick(&mut eff);
                t.pex_tick(Instant::now(), &mut eff);
                for id in t.peers.keys().copied().collect::<Vec<_>>() {
                    t.drive_peer(id, &mut eff);
                }
            }
            self.route(ih, eff, out);
        }
        wait
    }

    fn route(&mut self, ih: Sha1Hash, effects: Vec<TorrentEffect>, out: &mut Vec<SessionOut>) {
        self.dispatch_disk(ih, &effects);
        for e in effects {
            match e {
                TorrentEffect::SendTo { peer, msg } => {
                    out.push(SessionOut::ToPeer { ih, peer, msg });
                }
                TorrentEffect::DisconnectPeer { peer, reason } => {
                    self.peer_disconnected(&ih, peer, out);
                    self.alerts.post(Alert::PeerDisconnected {
                        ih,
                        reason: format!("peer {peer}: {reason}"),
                    });
                }
                TorrentEffect::PiecePassed(piece) => {
                    self.alerts.post(Alert::PieceFinished { ih, piece });
                    out.push(SessionOut::Torrent(TorrentEffect::PiecePassed(piece)));
                }
                TorrentEffect::PieceFailed(piece) => {
                    self.alerts.post(Alert::HashFailed { ih, piece });
                    out.push(SessionOut::Torrent(TorrentEffect::PieceFailed(piece)));
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
        let (pid, port, key) = (self.peer_id, self.listen_port, self.key);
        for (ih, t) in &mut self.torrents {
            if t.paused {
                continue;
            }
            let is_seed = t.is_seed();
            for idx in t.trackers.due(now, is_seed) {
                let e = &t.trackers.trackers[idx];
                out.push(SessionOut::TrackerAnnounce {
                    ih: *ih,
                    url: e.url.clone(),
                    tracker_id: e.tracker_id.clone(),
                    params: t.announce_params(pid, port, event, key),
                });
            }
        }
    }

    /// `on_announce_response` — a tracker exchange completed:
    /// success → peers into `peer_list` (source TRACKER),
    /// `trackerid` saved, `complete`→`num_seeds`; failure →
    /// `announce_entry::failed` (quadratic backoff).
    pub fn on_announce_response(
        &mut self,
        ih: &Sha1Hash,
        url: &str,
        resp: Result<bitdaemon_net::http_tracker::TrackerResponse, String>,
    ) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        let now = std::time::Instant::now();
        match resp {
            Ok(r) => {
                for ep in r.peers {
                    t.add_swarm_peer(ep, bitdaemon_common::flags::PeerSourceFlags::TRACKER, true);
                }
                if r.complete >= 0 {
                    t.num_seeds = r.complete as usize;
                }
                if let Some(i) = t.trackers.find(url) {
                    let e = &mut t.trackers.trackers[i];
                    e.verified = true;
                    if let Some(tid) = r.tracker_id {
                        e.tracker_id = String::from_utf8_lossy(&tid).into_owned();
                    }
                }
                self.alerts.post(Alert::TrackerReply {
                    ih: *ih,
                    url: url.to_string(),
                    num_peers: t.peer_list.num_peers(),
                });
            }
            Err(_) => {
                // settings: `tracker_backoff`/`retry_tracker_interval`
                // live in the caller's settings — use the upstream
                // defaults (250% backoff, base interval handled by
                // the entry's own next_announce).
                if let Some(i) = t.trackers.find(url) {
                    t.trackers.trackers[i].failed(250, std::time::Duration::from_secs(60), now);
                }
                self.alerts.post(Alert::TrackerError {
                    ih: *ih,
                    url: url.to_string(),
                    error: "announce failed".to_string(),
                });
            }
        }
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
    /// `Bitfield` re-export used by tests/callers.
    #[doc(hidden)]
    pub fn _bitfield_marker(_: &Bitfield) {}
}
