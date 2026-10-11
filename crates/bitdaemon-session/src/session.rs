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
    /// `network_config_change_alert` — interfaces changed; the
    /// driver re-runs listen setup (upstream `on_ip_change` →
    /// `reopen_listen_sockets`).
    InterfacesChanged,
    /// `scrape_tracker` — user-triggered scrape of one tracker
    /// entry; the driver runs the scrape exchange and reports via
    /// `Session::on_scrape_response`.
    TrackerScrape {
        /// torrent.
        ih: Sha1Hash,
        /// tracker URL (announce form — the driver derives the
        /// scrape URL / udp action).
        url: String,
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
    /// `m_alert_manager`.
    pub alerts: AlertManager,
    /// `m_stats_counters`.
    pub counters: Counters,
    /// `m_peer_id` — generated with the default fingerprint
    /// (`generate_peer_id`); used in announces and handshakes.
    pub peer_id: bitdaemon_common::PeerId,
    /// `m_listen_port` — our listen port (`announce request.port`).
    pub listen_port: u16,
    /// `m_ssl_listen_port` — advertised SSL listen port
    /// (`ssl_listen_port` upstream; SSL sockets share the listen set).
    pub ssl_listen_port: u16,
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
    /// last LSD broadcast.
    last_lsd: Option<Instant>,
    /// `m_session_time` — seconds since session start, bumped once
    /// per `tick` (peer_list reconnect backoff epoch).
    pub session_time: i32,
    /// `m_ip_filter` — access filter applied to new + existing peers.
    ip_filter: bitdaemon_net::ip_filter::IpFilter,
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
    pub port_filter: bitdaemon_net::ip_filter::PortFilter,
    /// session-level peer class (`peer_class_t` seeding
    /// `setup_peer_class`) — the class new connections inherit.
    pub session_class: bitdaemon_net::peer_class::PeerClassIndex,
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
    /// `(v6, lookup idx)` → info_hash — maps DHT traversal events
    /// back to the torrent they serve.
    dht_lookup_map: std::collections::BTreeMap<(bool, usize), Sha1Hash>,
    /// `(v6, lookup idx)` → pending BEP-44 immutable `put` value —
    /// consumed when the `get` traversal completes (`put_data`).
    dht_pending_put: std::collections::BTreeMap<(bool, usize), Vec<u8>>,
    /// `external_udp_port()` — the port our DHT socket listens on,
    /// advertised to peers via the `port` message (BEP-5 §port).
    pub dht_port: Option<u16>,
    /// last `session_time` a DHT announce ran per torrent
    /// (`m_dht_announce_timer` upstream).
    dht_last_announce: std::collections::BTreeMap<Sha1Hash, i32>,
    /// `m_auto_manage_time_scaler` — ticks until the next
    /// auto-manage pass; `add_torrent` forces 0 (immediate).
    auto_manage_scaler: i32,
}

/// A disk job's result, delivered back onto the session thread.
/// (`disk_io_job` completion → `torrent::on_disk_*`.)
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
    /// `async_move_storage` verdict.
    MoveStorage {
        /// torrent.
        ih: Sha1Hash,
        /// new `save_path`.
        path: std::path::PathBuf,
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
            udp_sockets: Vec::new(),
            paused: false,
            peer_class_pool: bitdaemon_net::peer_class::PeerClassPool::new(),
            peer_class_filter: bitdaemon_net::ip_filter::IpFilter::new(),
            peer_class_type_filter: bitdaemon_net::peer_class::PeerClassTypeFilter::default(),
            port_filter: bitdaemon_net::ip_filter::PortFilter::new(),
            session_class: 0,
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
            next_queue_seq: 0,
            last_lsd: None,
            session_time: 0,
            ip_filter: bitdaemon_net::ip_filter::IpFilter::new(),
            settings: crate::settings::SessionSettings::default(),
            ip_voter: bitdaemon_net::ip_voter::IpVoter::new(),
            dht_lookup_map: std::collections::BTreeMap::new(),
            dht_pending_put: std::collections::BTreeMap::new(),
            dht_port: None,
            dht_last_announce: std::collections::BTreeMap::new(),
            auto_manage_scaler: 0,
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
            d.call_job_handlers();
        }
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
                    // `on_disk_write_complete` → mark_as_finished
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.mark_block_finished(req.piece, req.start);
                        // upstream lifts graceful_pause once the last
                        // in-flight block request has been consumed.
                        if t.graceful_pause && !t.requests_outstanding() {
                            t.graceful_pause = false;
                        }
                    }
                    // whole piece complete and no hash in flight?
                    // (`picker.started_hash_job` upstream)
                    if let (Some(disk), Some(t)) = (self.disk.clone(), self.torrents.get_mut(&ih)) {
                        if !t.have_piece(req.piece)
                            && t.piece_complete(req.piece)
                            && !t.is_hashing(req.piece)
                        {
                            t.hashing_started(req.piece);
                            let tx = self.disk_tx.clone();
                            let storage = t.storage;
                            let v2 = t.hash_job_v2_span(req.piece);
                            disk.async_hash(
                                storage,
                                req.piece,
                                v2,
                                0,
                                Box::new(move |piece, hash, block_hashes, err| {
                                    let _ = tx.send(DiskCompleted::Hash {
                                        ih,
                                        piece,
                                        hash,
                                        block_hashes,
                                        err,
                                    });
                                }),
                            );
                        }
                    }
                }
                DiskCompleted::MoveStorage { ih, path, err } => {
                    // `storage_moved{,_failed}_alert`
                    if err.is_ok() {
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            t.save_path = path.to_string_lossy().into_owned();
                        }
                        self.alerts.post(Alert::StorageMoved {
                            ih,
                            path: path.to_string_lossy().into_owned(),
                        });
                    } else {
                        self.alerts.post(Alert::StorageMovedFailed {
                            ih,
                            path: path.to_string_lossy().into_owned(),
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
                        self.alerts.post(Alert::FileRenamed {
                            ih,
                            index: index.0,
                            name: name.to_string_lossy().into_owned(),
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
                    // `read_piece_alert` — payload or the error
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
                    }
                }
                DiskCompleted::ReleaseFiles { ih } => {
                    // `cache_flushed_alert`
                    self.alerts.post(Alert::CacheFlushed(ih));
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
                DiskCompleted::CheckFiles { ih, accepted, err } => {
                    // `torrent::files_checked`: resume accepted → the
                    // optimistic `have` bits stand; rejected →
                    // `need_full_check`: per-piece `disk_hash` jobs
                    // re-verify everything (the `TorrentChecked`
                    // alert fires when `checking_pieces` drains).
                    let mut eff = Vec::new();
                    let ok = err.is_ok() && accepted;
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        t.on_files_checked(ok, &mut eff);
                        if !ok && t.arm_full_recheck() > 0 {
                            for p in 0..t.num_pieces {
                                eff.push(TorrentEffect::DiskHash {
                                    piece: bitdaemon_common::types::PieceIndex(p),
                                });
                            }
                        }
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
                    block_hashes,
                    err,
                } => {
                    // upstream `torrent::piece_finished` compares
                    // `j.d.h.hash` vs `hash_for_piece` — mismatch is
                    // a `hash_failed_alert` + re-download.
                    let passed = err.is_ok() && {
                        let Some(t) = self.torrents.get_mut(&ih) else {
                            continue;
                        };
                        if let Some(want) = t.expected_hash(piece) {
                            want == hash
                        } else if t.is_v2_only() {
                            // v2: compare per-block SHA-256s against
                            // the file's merkle tree (`on_blocks_hashed`).
                            block_hashes.iter().any(|h| !h.is_all_zeros())
                                && t.verify_v2_blocks(piece, &block_hashes)
                        } else {
                            true
                        }
                    };
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
                    let v2 = t.hash_job_v2_span(*piece);
                    let (piece, tx) = (*piece, self.disk_tx.clone());
                    disk.async_hash(
                        storage,
                        piece,
                        v2,
                        0,
                        Box::new(move |piece, hash, block_hashes, err| {
                            let _ = tx.send(DiskCompleted::Hash {
                                ih,
                                piece,
                                hash,
                                block_hashes,
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
        io.set_max_queued_bytes(self.settings.max_disk_write_queue);
        self.disk = Some(io);
    }

    /// `add_torrent` — insert (or replace) a swarm.
    pub fn add_torrent(&mut self, mut t: Torrent) -> Sha1Hash {
        let ih = t.info_hash;
        t.queue_seq = self.next_queue_seq;
        self.next_queue_seq += 1;
        self.torrents.insert(ih, t);
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
        let Some(t) = self.torrents.get_mut(&ih) else {
            return;
        };
        t.checking = true;
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
        // upstream `trigger_auto_manage` — an add forces the next
        // tick into a queue pass (newly added default-flagged
        // torrents start paused+auto-managed).
        self.auto_manage_scaler = 0;
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
        // goes through `check_fastresume`; seed_mode trusts the
        // resume bits blind, a rejected verdict falls through to
        // the per-piece full recheck in `DiskCompleted::CheckFiles`.
        if atp.ti.is_some()
            && !atp
                .flags
                .contains(bitdaemon_common::flags::TorrentFlags::SEED_MODE)
        {
            self.check_files(ih, Some(Box::new(atp.clone())));
        }
        Some(ih)
    }

    /// `remove_torrent` — `~torrent`: emit a `stopped` announce to
    /// every tracker that saw a `started` (upstream sends it on
    /// graceful shutdown/removal), then drop the swarm.
    pub fn remove_torrent(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) -> Option<Torrent> {
        if let Some(t) = self.torrents.get_mut(ih) {
            Self::announce_stopped(t, self.peer_id, self.listen_port, self.key, out);
        }
        let removed = self.torrents.remove(ih);
        if let Some(t) = &removed {
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
    pub fn set_paused(&mut self, ih: &Sha1Hash, paused: bool, out: &mut Vec<SessionOut>) {
        if let Some(t) = self.torrents.get_mut(ih) {
            if paused && !t.paused {
                // borrow dance: build stopped announces first
            }
            t.paused = paused;
        }
        if paused {
            if let Some(t) = self.torrents.get_mut(ih) {
                Self::announce_stopped(t, self.peer_id, self.listen_port, self.key, out);
            }
        }
    }

    /// `pause_flags_t::graceful_pause` — stop issuing requests but
    /// let in-flight writes finish; the `stopped` announce runs
    /// immediately (upstream auto-manage defers it via
    /// `m_graceful_pause_mode`, an explicit graceful pause does not).
    pub fn set_paused_graceful(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        if let Some(t) = self.torrents.get_mut(ih) {
            if !t.paused {
                t.graceful_pause = true;
            }
        }
        self.set_paused(ih, true, out);
    }

    /// `torrent::do_pause`/`~torrent` — `stopped` for trackers that
    /// already announced `started`.
    fn announce_stopped(
        t: &mut Torrent,
        peer_id: bitdaemon_common::PeerId,
        port: u16,
        key: i32,
        out: &mut Vec<SessionOut>,
    ) {
        for e in &t.trackers.trackers {
            let started = e
                .endpoints
                .iter()
                .any(|ep| ep.v1.start_sent || ep.v2.start_sent);
            if started {
                out.push(SessionOut::TrackerAnnounce {
                    ih: t.info_hash,
                    url: e.url.clone(),
                    tracker_id: e.tracker_id.clone(),
                    params: t.announce_params(peer_id, port, 3, key),
                });
                t.outstanding_announces += 1;
            }
        }
    }

    /// `session_impl` auto-managed queue — promote/demotion
    /// against `active_downloads`/`active_seeds`. A torrent
    /// counts toward `active_seeds` once `is_seed`, else
    /// `active_downloads`. Non-auto-managed and user-paused
    /// torrents are left alone (upstream `auto_managed` flag).
    /// `-1` limits mean unlimited.
    ///
    /// Upstream `auto_manage_torrents` iterates the
    /// `*_auto_managed` lists — which contain *all* auto-managed
    /// torrents, paused included — and calls `set_paused(false)`
    /// within the limits / `set_paused(true)` beyond. `paused` on
    /// an auto-managed torrent is scheduler state, not a user
    /// request (user pause on an auto-managed torrent is
    /// overridden by the queue — `flags::auto_managed` must be
    /// cleared for a sticky pause).
    pub fn manage_queue(&mut self, active_downloads: i32, active_seeds: i32) {
        let mut dl_active = 0i32;
        let mut seed_active = 0i32;
        // BTreeMap order → deterministic promotion, oldest-ih first
        for t in self.torrents.values_mut() {
            if !t.auto_managed {
                t.queued = false;
                continue;
            }
            let seed = t.is_seed();
            let over = if seed {
                seed_active >= active_seeds && active_seeds >= 0
            } else {
                dl_active >= active_downloads && active_downloads >= 0
            };
            if over {
                // upstream `set_paused(true, graceful_pause)` —
                // queued beyond the slot limit.
                t.paused = true;
                t.queued = true;
            } else {
                // upstream `set_paused(false)` — promote into a
                // free slot, unpausing scheduler-paused torrents.
                t.paused = false;
                t.queued = false;
                if seed {
                    seed_active += 1;
                } else {
                    dl_active += 1;
                }
            }
        }
    }

    /// `queue_position` — index of `ih` within its queue class
    /// (downloaders vs seeds, upstream's two queue lists) ordered
    /// by `queue_seq`. `-1` for paused or non-auto-managed torrents.
    #[must_use]
    pub fn queue_position(&self, ih: &Sha1Hash) -> i32 {
        let Some(t) = self.torrents.get(ih) else {
            return -1;
        };
        if t.paused || !t.auto_managed {
            return -1;
        }
        let seed = t.is_seed();
        self.torrents
            .values()
            .filter(|o| !o.paused && o.auto_managed && o.is_seed() == seed)
            .map(|o| (o.queue_seq, o.info_hash))
            .collect::<std::collections::BTreeSet<_>>()
            .iter()
            .position(|(_, i)| i == ih)
            .map_or(-1, |p| p as i32)
    }

    /// `queue_position_up/down/top/bottom` — reorder within the
    /// queue class by rewriting `queue_seq` (upstream
    /// `queue_position_set`). Returns the torrents whose order
    /// changed conceptually (for alerts/tests).
    pub fn queue_position_set(&mut self, ih: &Sha1Hash, p: QueueMove) {
        let Some(t) = self.torrents.get(ih) else {
            return;
        };
        let seed = t.is_seed();
        let me = t.queue_seq;
        // ordered seq list of the same queue class
        let mut seqs: Vec<(i64, Sha1Hash)> = self
            .torrents
            .values()
            .filter(|o| o.info_hash != *ih && o.is_seed() == seed)
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
                    }
                    if let Some(t) = self.torrents.get_mut(ih) {
                        t.queue_seq = s0;
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
                }
            }
        }
    }

    /// feed one decoded peer message → effects.
    pub fn on_peer_message(&mut self, p: &PeerPacket, out: &mut Vec<SessionOut>) {
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
            t.on_peer_message(p.peer, &p.msg, self.session_time, &mut eff);
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
        out: &mut Vec<SessionOut>,
    ) {
        if let Some(t) = self.torrents.get_mut(ih) {
            let mut conn = conn;
            conn.remote = Some(remote);
            // `peer_info::protocol_v2` — the handshake came in keyed
            // by the torrent's v2 (SHA-256, truncated) info-hash.
            conn.protocol_v2 =
                t.ti.as_ref()
                    .and_then(|ti| ti.info_hash_v2())
                    .is_some_and(|v2| v2.as_bytes()[..20] == ih.as_bytes()[..]);
            t.peer_connected_addr(remote);
            // upstream `send_bitfield` on connect — super-seeding
            // hides the real bitfield and `have`s only the leaked
            // piece instead.
            let msg = if t.super_seeding {
                t.get_piece_to_super_seed(&conn.peer_pieces).map(|p| {
                    conn.superseed_piece = Some(p);
                    PeerMessage::Have(p)
                })
            } else if conn.supports_fast && t.is_seed() {
                // BEP-6: `have_all` replaces a full bitfield.
                Some(PeerMessage::HaveAll)
            } else if conn.supports_fast && t.have.count() == 0 {
                Some(PeerMessage::HaveNone)
            } else {
                let bits: Vec<u8> = t.have.bytes().into();
                (!bits.is_empty() && t.have.count() > 0)
                    .then_some(PeerMessage::Bitfield(bits.into()))
            };
            if let Some(msg) = msg {
                out.push(SessionOut::ToPeer { ih: *ih, peer, msg });
            }
            // upstream `peer_connection::init` sends `port` when
            // the peer negotiated DHT and our dht is bound.
            if conn.supports_dht
                && let Some(port) = self.dht_port
            {
                out.push(SessionOut::ToPeer {
                    ih: *ih,
                    peer,
                    msg: PeerMessage::DhtPort(port),
                });
            }
            // `peer_connect_alert` on successful registration.
            self.alerts.post(Alert::PeerConnect {
                ih: *ih,
                remote: remote.to_string(),
            });
            t.add_peer(peer, conn);
            // BEP-6: FAST peers get their allowed-fast set right
            // after connect (upstream `send_allowed_set`).
            {
                let mut eff = Vec::new();
                t.send_allowed_set(peer, &mut eff);
                self.route(*ih, eff, out);
            }
            let n = self.torrents.values().map(|t| t.peers.len() as i64).sum();
            self.counters.set(Counter::NumPeersConnected, n);
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
                t.peer_disconnected_addr(remote, self.session_time);
                t.remove_peer(id, &mut eff);
            }
            self.alerts.post(Alert::PeerBan {
                ih: *ih,
                ip: remote,
            });
        }
        self.route(*ih, eff, out);
    }

    /// `torrent::lsd_announce` — emit a one-shot LSD broadcast for
    /// `ih` (same payload as the periodic tick).
    pub fn force_lsd_announce(&mut self, ih: &Sha1Hash, out: &mut Vec<SessionOut>) {
        if self.settings.anonymous_mode {
            return;
        }
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        if t.private {
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
                req: bitdaemon_common::types::PeerRequest {
                    piece,
                    start: off,
                    length: chunk.len() as i32,
                },
                data: chunk.to_vec(),
            });
            off += chunk.len() as i32;
        }
        let _ = piece_len;
        let mut out = Vec::new();
        self.route(*ih, eff, &mut out);
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
            // `smart_ban` bookkeeping rides the same path as upstream
            // `on_piece_finished`/`on_piece_failed`: `addr` resolves a
            // downloader's IP from its live connection; `read_digest`
            // would re-read the block hash from disk — the disk job is
            // async here, so we pass `None` (upstream treats a failed
            // re-read identically: the suspicious peer goes unbanned
            // until a later piece exposes it).
            let piece_size = t
                .files
                .as_ref()
                .map_or(t.piece_length, |f| i64::from(f.piece_size(piece)));
            let addrs: std::collections::HashMap<u64, std::net::IpAddr> = t
                .peers
                .iter()
                .filter_map(|(id, c)| c.remote.map(|a| (*id, a.ip())))
                .collect();
            t.piece_verified_ban(
                piece,
                piece_size,
                passed,
                &mut eff,
                |peer| {
                    addrs
                        .get(&peer)
                        .copied()
                        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
                },
                |_| None,
            );
            if t.recheck_piece_done() {
                // `torrent_checked_alert` — last check piece landed.
                self.alerts.post(Alert::TorrentChecked(*ih));
            }
        }
        self.route(*ih, eff, out);
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
        let (Some(disk), Some(t)) = (self.disk.clone(), self.torrents.get(ih)) else {
            return;
        };
        let storage = t.storage;
        let tx = self.disk_tx.clone();
        let ih = *ih;
        let path2 = path.clone();
        disk.async_move_storage(
            storage,
            path,
            flags,
            Box::new(move |_status, path, err| {
                let _ = tx.send(DiskCompleted::MoveStorage {
                    ih,
                    path: if path.as_os_str().is_empty() {
                        path2.clone()
                    } else {
                        path
                    },
                    err,
                });
            }),
        );
    }

    /// `rename_file` — `async_rename_file` → `file_renamed{,_failed}`.
    pub fn rename_file(
        &mut self,
        ih: &Sha1Hash,
        index: bitdaemon_common::types::FileIndex,
        name: std::path::PathBuf,
    ) {
        let (Some(disk), Some(t)) = (self.disk.clone(), self.torrents.get(ih)) else {
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
        if self.disk.is_none() {
            // no disk → just remove (upstream only schedules the job
            // when a disk thread exists)
            self.remove_torrent(ih, out);
            return;
        }
        let Some(disk) = self.disk.clone() else {
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
            && t.arm_full_recheck() > 0
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

    /// `session_impl::get_ip_filter`.
    #[must_use]
    pub fn ip_filter(&self) -> &bitdaemon_net::ip_filter::IpFilter {
        &self.ip_filter
    }

    /// `session_handle::pause`/`resume` — upstream pauses the whole
    /// session: every torrent is paused/resumed and the flag is
    /// remembered for torrents added later.
    pub fn set_session_paused(&mut self, paused: bool, out: &mut Vec<SessionOut>) {
        if self.paused == paused {
            return;
        }
        self.paused = paused;
        let ihs: Vec<Sha1Hash> = self.torrents.keys().copied().collect();
        for ih in ihs {
            self.set_paused(&ih, paused, out);
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

    /// `session_handle::dht_put_item` (immutable overload) — BEP-44:
    /// `target = sha1(value)`, run a `get` lookup, then `put` to the
    /// token-holding responders when it completes (`put_data`).
    /// Returns the item target key.
    pub fn dht_put_item(&mut self, value: &[u8]) -> Sha1Hash {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(value);
        let target = Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default();
        if let Some(n) = self.dht.get_node_mut(false) {
            let idx = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetItem, target);
            self.dht_pending_put.insert((false, idx), value.to_vec());
        }
        target
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
    /// recently on the given info-hash lookup.
    #[must_use]
    pub fn dht_live_nodes(&self, ih: &Sha1Hash) -> Vec<std::net::SocketAddr> {
        self.dht
            .get_node(false)
            .map(|n| n.live_nodes_for(ih))
            .unwrap_or_default()
    }

    /// `apply_settings_pack` — hot-swap the settings bag. Upstream
    /// re-reads everything lazily; we mirror that by replacing the
    /// struct (all reads go through `self.settings`).
    pub fn apply_settings(&mut self, s: crate::settings::SessionSettings) {
        // hot-applied knobs upstream pushes eagerly
        // (`update_alert_queue_size`, `update_disk_cache_limit`):
        // the rest is read lazily through `self.settings`.
        if let Some(d) = &self.disk {
            d.set_max_queued_bytes(s.max_disk_write_queue);
        }
        self.alerts.set_queue_limit(s.alert_queue_size);
        self.settings = s;
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
            out.push(SessionOut::UdpPacket(pkt, to));
        }
        // `ip_voter::cast_vote` — the `ip` key a node echoed back is
        // its view of our external address (upstream `dht.cpp`).
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                for ip in n.drain_ip_votes() {
                    self.ip_voter.cast_vote(
                        ip,
                        bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_DHT,
                        &from.ip(),
                    );
                }
            }
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
        // anonymous sessions must not broadcast info-hashes to the LAN
        if self.settings.anonymous_mode || !self.services.lsd {
            return;
        }
        const LSD_INTERVAL: Duration = Duration::from_secs(300);
        if self
            .last_lsd
            .is_some_and(|t| now.duration_since(t) < LSD_INTERVAL)
        {
            return;
        }
        self.last_lsd = Some(now);
        for t in self.torrents.values_mut() {
            if t.private || !t.announce_lsd || t.paused {
                continue;
            }
            t.lsd_announcing = true;
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
        // `session_impl::on_tick` — `m_auto_manage_time_scaler--`;
        // when it hits 0 the queue runs and the scaler re-arms to
        // `auto_manage_interval` (upstream 30 s default).
        self.auto_manage_scaler -= 1;
        if self.auto_manage_scaler <= 0 {
            self.auto_manage_scaler = self.settings.auto_manage_interval;
            let (dl, sd) = (self.settings.active_downloads, self.settings.active_seeds);
            self.manage_queue(dl, sd);
        }
        self.drain_disk_completed(out);
        // `session_impl` ip notifier — interface changes re-open
        // listen sockets upstream; here the driver owns sockets so
        // we surface the event.
        if self.ip_notifier.poll() || std::mem::take(&mut self.reopen_sockets) {
            out.push(SessionOut::InterfacesChanged);
        }
        let wait = self.dht.tick();
        self.dht_announce_tick();
        self.dht_drain_events();
        while let Some((pkt, to)) = self.dht.poll() {
            out.push(SessionOut::UdpPacket(pkt, to));
        }
        let now = Instant::now();
        for t in self.torrents.values_mut() {
            t.update_rates(now);
        }
        self.lsd_tick(now, out);
        let ihs: Vec<Sha1Hash> = self
            .torrents
            .iter()
            .filter(|(_, t)| !t.paused && t.want_peers())
            .map(|(ih, _)| *ih)
            .collect();
        let peer_timeout = self.settings.peer_timeout.as_secs() as i32;
        let web_seed_blocks = self.settings.web_seed_blocks_per_tick;
        // `disk_observer` gate — stop issuing block requests while
        // the write queue sits above `max_queued_disk_bytes`.
        let disk_bp = self.disk.as_ref().is_some_and(|d| d.write_queue_exceeded());
        for ih in ihs {
            let mut eff = Vec::new();
            if let Some(t) = self.torrents.get_mut(&ih) {
                t.disk_back_pressure = disk_bp;
                t.metadata_tick(&mut eff);
                t.pex_tick(Instant::now(), &mut eff);
                for id in t.peers.keys().copied().collect::<Vec<_>>() {
                    t.drive_peer(id, &mut eff);
                }
                // `peer_connection::second_tick` — snub + refund
                // blocks stuck past `request_timeout`.
                t.check_request_timeouts(&mut eff);
                // `web_connection_base` — feed url seeds too
                if let Some((url, reqs)) = t.next_web_seed_blocks(web_seed_blocks)
                    && let Some(fs) = t.files.clone()
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
                // `peer_timeout` — disconnect peers silent for
                // `peer_timeout` seconds (upstream second_tick)
                let stale: Vec<u64> = t
                    .peers
                    .iter()
                    .filter(|(_, c)| self.session_time - c.last_active > peer_timeout)
                    .map(|(id, _)| *id)
                    .collect();
                for id in stale {
                    t.remove_peer(id, &mut eff);
                    self.alerts.post(Alert::PeerDisconnected {
                        ih,
                        reason: format!("peer {id}: timed out"),
                    });
                }
            }
            self.route(ih, eff, out);
        }
        self.unchoke_tick(out);
        self.resume_save_tick(now);
        wait
    }

    /// `torrent::dht_announce` — every `dht_announce_interval`
    /// (15 min upstream) a non-private torrent wanting peers runs a
    /// `get_peers` traversal on each bound node family.
    fn dht_announce_tick(&mut self) {
        const DHT_ANNOUNCE_INTERVAL: i32 = 15 * 60;
        if !self.settings.dht_enabled || !self.services.dht || self.settings.anonymous_mode {
            return;
        }
        let ihs: Vec<Sha1Hash> = self
            .torrents
            .iter()
            .filter(|(ih, t)| {
                t.want_peers()
                    && !t.private
                    && t.announce_dht
                    && self
                        .dht_last_announce
                        .get(ih)
                        .is_none_or(|last| self.session_time - last >= DHT_ANNOUNCE_INTERVAL)
            })
            .map(|(ih, _)| *ih)
            .collect();
        for ih in ihs {
            self.dht_announce_one(ih);
        }
    }

    /// `dht::announce` for one torrent — starts the `get_peers`
    /// traversal on every bound node family and registers the
    /// lookup id for reply routing.
    fn dht_announce_one(&mut self, ih: Sha1Hash) {
        self.dht_last_announce.insert(ih, self.session_time);
        if let Some(t) = self.torrents.get_mut(&ih) {
            t.dht_announcing = true;
        }
        for v6 in [false, true] {
            if let Some(n) = self.dht.get_node_mut(v6) {
                let li = n.new_lookup(bitdaemon_dht::lookups::LookupKind::GetPeers, ih);
                self.dht_lookup_map.insert((v6, li), ih);
            }
        }
    }

    /// `force_dht_announce` — user-triggered, ignores the interval
    /// (private torrents still never touch the DHT).
    pub fn force_dht_announce(&mut self, ih: &Sha1Hash) {
        if !self.settings.dht_enabled
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
    pub fn force_announce(
        &mut self,
        ih: &Sha1Hash,
        tracker_idx: Option<usize>,
        out: &mut Vec<SessionOut>,
    ) {
        let (pid, port, key) = (self.peer_id, self.listen_port, self.key);
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        let idxs: Vec<usize> =
            tracker_idx.map_or_else(|| (0..t.trackers.trackers.len()).collect(), |i| vec![i]);
        for i in idxs {
            let Some(e) = t.trackers.trackers.get(i) else {
                continue;
            };
            out.push(SessionOut::TrackerAnnounce {
                ih: *ih,
                url: e.url.clone(),
                tracker_id: e.tracker_id.clone(),
                params: t.announce_params(pid, port, 0, key),
            });
            t.outstanding_announces += 1;
        }
    }

    /// drain `DhtEvent`s — `get_peers` replies feed `peer_list`
    /// (source DHT), finished lookups trigger `announce_peer` to
    /// every node that gave a write token (upstream `dht::announce`
    /// second phase).
    fn dht_drain_events(&mut self) {
        use bitdaemon_dht::dht_node::DhtEvent;
        for v6 in [false, true] {
            let Some(n) = self.dht.get_node_mut(v6) else {
                continue;
            };
            for ev in n.drain_events() {
                match ev {
                    DhtEvent::Data { lookup, data } => {
                        let Some(ih) = self.dht_lookup_map.get(&(v6, lookup)).copied() else {
                            continue;
                        };
                        if let Some(t) = self.torrents.get_mut(&ih) {
                            for ep in data.peers {
                                t.peer_list.add_peer(
                                    ep,
                                    bitdaemon_common::flags::PeerSourceFlags::DHT,
                                    true,
                                );
                            }
                        }
                    }
                    DhtEvent::Done { lookup, tokens } => {
                        // `put_data` second phase — a `dht_put_item`
                        // GetItem finished: write to token-holders.
                        if let Some(value) = self.dht_pending_put.remove(&(v6, lookup)) {
                            let targets: Vec<(SocketAddr, Vec<u8>)> =
                                tokens.iter().map(|(_, ep, t)| (*ep, t.clone())).collect();
                            if !targets.is_empty() {
                                n.put(&value, None, targets);
                            }
                            continue;
                        }
                        let Some(ih) = self.dht_lookup_map.remove(&(v6, lookup)) else {
                            continue;
                        };
                        if tokens.is_empty() {
                            continue;
                        }
                        // `announce` — tell token-holding nodes we
                        // are a peer (`announce_peer` second phase)
                        let addrs: std::collections::BTreeMap<_, _> =
                            tokens.iter().map(|(id, ep, _)| (*id, *ep)).collect();
                        let tok_map: std::collections::BTreeMap<_, _> =
                            tokens.iter().map(|(id, _, t)| (*id, t.clone())).collect();
                        let seed = self.torrents.get(&ih).is_some_and(|t| t.is_seed());
                        let port = self.listen_port;
                        n.announce(&ih, port, seed, false, &addrs, |id| {
                            tok_map.get(id).cloned()
                        });
                        // no more get_peers lookups for `ih` → the
                        // announce round is over.
                        if !self.dht_lookup_map.values().any(|v| *v == ih) {
                            if let Some(t) = self.torrents.get_mut(&ih) {
                                t.dht_announcing = false;
                            }
                        }
                    }
                    // `direct_request` replies surface to the
                    // session owner — nothing consumes them yet.
                    DhtEvent::Direct { .. } => {}
                }
            }
        }
    }

    /// `session_impl` choker round — every ~10 s upstream picks the
    /// top `unchoke_slots` peers (`unchoke_sort`) and sends
    /// choke/unchoke; round counters reset each round.
    fn unchoke_tick(&mut self, out: &mut Vec<SessionOut>) {
        const UNCHOKE_INTERVAL_S: i32 = 10;
        if self.session_time % UNCHOKE_INTERVAL_S != 0 {
            return;
        }
        let now = Instant::now();
        let slots = self.settings.unchoke_slots;
        // upstream `optimistic_unchoke_interval` default 30 s.
        let rotate_optimistic = self.session_time % 30 == 0;
        let ihs: Vec<Sha1Hash> = self.torrents.keys().copied().collect();
        for ih in ihs {
            let Some(t) = self.torrents.get_mut(&ih) else {
                continue;
            };
            if t.peers.is_empty() {
                continue;
            }
            let seeding = t.is_seed();
            let total_size = t.files.as_ref().map_or(0, |f| f.total_size());
            let mut list: Vec<crate::choker::ChokePeer> = t
                .peers
                .iter()
                .map(|(id, c)| crate::choker::ChokePeer {
                    id: *id,
                    choked: c.choked,
                    interested: c.peer_interested,
                    uploaded_last: c.stat_up_round,
                    downloaded_last: c.stat_down_round,
                    last_unchoke: c.last_unchoke,
                    seeding,
                    num_pieces: c.peer_pieces.count() as i32,
                    uploaded_total: c.stat_up_total,
                    total_size,
                    piece_length: t.piece_length,
                    priority: 1.0,
                })
                .collect();
            let n = crate::choker::unchoke_sort(
                &mut list,
                slots,
                crate::choker::ChokingAlgorithm::FixedSlots,
                crate::choker::SeedChokingAlgorithm::RoundRobin,
                std::time::Duration::from_secs(UNCHOKE_INTERVAL_S as u64),
                crate::choker::DEFAULT_SEEDING_PIECE_QUOTA,
                now,
            );
            let mut keep: std::collections::BTreeSet<u64> =
                list[..n].iter().map(|p| p.id).collect();
            if rotate_optimistic {
                // round-robin over choked interested peers not in
                // the rate-sorted keep-set (upstream bumps the slot
                // for the same peer when re-picked).
                let cands: Vec<u64> = t
                    .peers
                    .iter()
                    .filter(|(id, c)| c.choked && c.peer_interested && !keep.contains(id))
                    .map(|(id, _)| *id)
                    .collect();
                if !cands.is_empty() {
                    let pick = cands[t.optimistic_cursor % cands.len()];
                    t.optimistic_cursor += 1;
                    keep.insert(pick);
                }
            }
            let mut eff = Vec::new();
            for (id, c) in &mut t.peers {
                let want_unchoke = keep.contains(id) && c.peer_interested;
                if want_unchoke && c.choked {
                    c.choked = false;
                    c.last_unchoke = now;
                    eff.push(TorrentEffect::SendTo {
                        peer: *id,
                        msg: PeerMessage::Unchoke,
                    });
                } else if !want_unchoke && !c.choked {
                    c.choked = true;
                    eff.push(TorrentEffect::SendTo {
                        peer: *id,
                        msg: PeerMessage::Choke,
                    });
                }
                // reset the round counters
                c.stat_up_round = 0;
                c.stat_down_round = 0;
            }
            self.route(ih, eff, out);
        }
    }

    fn route(&mut self, ih: Sha1Hash, effects: Vec<TorrentEffect>, out: &mut Vec<SessionOut>) {
        self.dispatch_disk(ih, &effects);
        for e in effects {
            match e {
                TorrentEffect::SendTo { peer, msg } => {
                    // `m_stat` upload accounting — payload bytes
                    // feed the choker's round counters.
                    if let PeerMessage::Piece { data, .. } = &msg
                        && let Some(t) = self.torrents.get_mut(&ih)
                        && let Some(c) = t.peers.get_mut(&peer)
                    {
                        let n = data.len() as i64;
                        c.stat_up_total += n;
                        c.stat_up_round += n;
                        t.last_payload_up = Some(std::time::Instant::now());
                    }
                    out.push(SessionOut::ToPeer { ih, peer, msg });
                }
                TorrentEffect::DhtNode(addr) => {
                    self.dht_add_peer_node(addr);
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
                TorrentEffect::YourIp(ip) => {
                    // BEP-10 `yourip` → `ip_voter` (source_peer)
                    self.ip_voter.cast_vote(
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
                    if let Some(t) = self.torrents.get_mut(&ih) {
                        for e in &t.trackers.trackers {
                            let started = e
                                .endpoints
                                .iter()
                                .any(|ep| ep.v1.start_sent || ep.v2.start_sent);
                            if started {
                                out.push(SessionOut::TrackerAnnounce {
                                    ih,
                                    url: e.url.clone(),
                                    tracker_id: e.tracker_id.clone(),
                                    params: t.announce_params(
                                        self.peer_id,
                                        self.listen_port,
                                        1, // completed
                                        self.key,
                                    ),
                                });
                                t.outstanding_announces += 1;
                            }
                        }
                    }
                    if let Some(t) = self.torrents.get(&ih) {
                        for (pid, c) in &t.peers {
                            if c.ext_upload_only != 0 {
                                out.push(SessionOut::ToPeer {
                                    ih,
                                    peer: *pid,
                                    msg: PeerMessage::Extended {
                                        id: c.ext_upload_only,
                                        data: bytes::Bytes::new(),
                                    },
                                });
                            }
                        }
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
        let (pid, port, key) = (self.peer_id, self.listen_port, self.key);
        for (ih, t) in &mut self.torrents {
            if t.paused || !t.announcing || !t.announce_trackers {
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
                t.outstanding_announces += 1;
            }
        }
    }

    /// `torrent::scrape_tracker` — request an immediate scrape of
    /// tracker entry `idx` (upstream: user-triggered, outside the
    /// announce schedule).
    pub fn scrape_tracker(&mut self, ih: &Sha1Hash, tracker_idx: usize, out: &mut Vec<SessionOut>) {
        let Some(t) = self.torrents.get(ih) else {
            return;
        };
        if let Some(e) = t.trackers.trackers.get(tracker_idx) {
            out.push(SessionOut::TrackerScrape {
                ih: *ih,
                url: e.url.clone(),
            });
        }
    }

    /// `on_scrape_response` — update seed/leech counts from a
    /// scrape (`resp.complete`/`incomplete`), post `scrape_reply`.
    pub fn on_scrape_response(
        &mut self,
        ih: &Sha1Hash,
        url: &str,
        resp: Result<bitdaemon_net::http_tracker::TrackerResponse, String>,
    ) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        match resp {
            Ok(r) => {
                if r.complete >= 0 {
                    t.num_seeds = r.complete as usize;
                }
                if let Some(i) = t.trackers.find(url) {
                    t.trackers.trackers[i].verified = true;
                    t.trackers.trackers[i].scrape_downloaded = r.downloaded;
                    t.trackers.trackers[i].scrape_incomplete = r.incomplete;
                    t.trackers.trackers[i].scrape_complete = r.complete;
                    t.trackers.trackers[i].scrape_time = Some(Instant::now());
                }
                self.alerts.post(Alert::ScrapeReply {
                    ih: *ih,
                    url: url.to_string(),
                    complete: r.complete,
                    incomplete: r.incomplete,
                });
            }
            Err(e) => {
                self.alerts.post(Alert::ScrapeFailed {
                    ih: *ih,
                    url: url.to_string(),
                    reason: e,
                });
            }
        }
    }

    /// `dht->add_router_node` — a resolved `.torrent` `nodes` host.
    /// Routes on the address family like upstream (`add_node`
    /// picks the v4/v6 node by the resolved address).
    pub fn dht_add_node(&mut self, addr: SocketAddr) {
        let v6 = addr.is_ipv6();
        if let Some(n) = self.dht.get_node_mut(v6) {
            n.table.add_router_node(addr);
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
    /// block `writing` under `Torrent::WEB_SEED_PEER`).
    pub fn on_web_seed_block(
        &mut self,
        ih: &Sha1Hash,
        req: bitdaemon_common::types::PeerRequest,
        data: Vec<u8>,
        out: &mut Vec<SessionOut>,
    ) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            t.on_web_seed_block(req, data, &mut eff);
        }
        self.route(*ih, eff, out);
    }

    /// `web_peer_connection` failure — release the picked blocks.
    pub fn on_web_seed_error(
        &mut self,
        ih: &Sha1Hash,
        reqs: &[bitdaemon_common::types::PeerRequest],
    ) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.on_web_seed_error(reqs);
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
        t.outstanding_announces = (t.outstanding_announces - 1).max(0);
        let now = std::time::Instant::now();
        match resp {
            Ok(r) => {
                t.last_error.clear();
                // `tracker response "external ip"` → ip_voter
                // (`source_tracker` — upstream `on_announce`).
                if let Some(ip) = r.external_ip {
                    self.ip_voter.cast_vote(
                        ip,
                        bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_TRACKER,
                        &ip,
                    );
                }
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
            Err(e) => {
                // settings: `tracker_backoff`/`retry_tracker_interval`
                // live in the caller's settings — use the upstream
                // defaults (250% backoff, base interval handled by
                // the entry's own next_announce).
                if let Some(i) = t.trackers.find(url) {
                    t.trackers.trackers[i].failed(250, std::time::Duration::from_secs(60), now);
                }
                // `torrent_status::error` — the tracker failure
                // message is upstream's `m_error` source
                t.last_error = e.clone();
                self.alerts.post(Alert::TrackerError {
                    ih: *ih,
                    url: url.to_string(),
                    error: e,
                });
            }
        }
    }

    /// `save_resume_data` — upstream `torrent::save_resume_data`
    /// → `save_resume_data_alert` carrying the bencoded resume
    /// blob. `only_if_modified` honors `need_save_resume`
    /// (upstream `flags & only_if_modified`).
    pub fn save_resume_data(&mut self, ih: &Sha1Hash, only_if_modified: bool) {
        let Some(t) = self.torrents.get_mut(ih) else {
            return;
        };
        if only_if_modified && !t.need_save_resume {
            return;
        }
        let mut atp = bitdaemon_core::AddTorrentParams::default();
        t.to_resume_data(&mut atp);
        t.need_save_resume = false;
        t.last_saved_resume = Some(Instant::now());
        let data = bitdaemon_core::resume_data::write_resume_data_buf(&atp);
        self.alerts.post(Alert::SaveResumeData { ih: *ih, data });
    }

    /// `auto_save_resume` — periodic flush of dirty resume data
    /// (upstream saves every `save_resume_data_interval`).
    fn resume_save_tick(&mut self, now: Instant) {
        let interval = self.settings.save_resume_interval;
        let due: Vec<Sha1Hash> = self
            .torrents
            .values()
            .filter(|t| {
                t.need_save_resume
                    && t.last_saved_resume
                        .is_none_or(|t0| now.duration_since(t0) >= interval)
            })
            .map(|t| t.info_hash)
            .collect();
        for ih in due {
            self.save_resume_data(&ih, false);
        }
    }

    /// `post_torrent_updates` — upstream posts a
    /// `state_update_alert` with one `torrent_status` per torrent
    /// (the `post_torrent_updates` call sites all want the full set).
    pub fn post_torrent_updates(&mut self) {
        let statuses = self
            .torrents
            .iter()
            .map(|(ih, t)| t.status(&self.settings, self.queue_position(ih)))
            .collect();
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
            .filter(|t| !t.is_seed() && !t.paused)
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
            t.to_resume_data(&mut atp);
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
        let Some(st) = crate::session_state::load_session_state(buf) else {
            return 0;
        };
        if let Some(set) = st.settings {
            self.settings = set;
        }
        if let Some(ds) = bitdaemon_dht::dht_state::read_dht_state_bytes(&st.dht_state) {
            for v6 in [false, true] {
                if let Some(n) = self.dht.get_node_mut(v6) {
                    n.load_state(&ds);
                }
            }
        }
        let mut n = 0;
        for (_, atp) in st.torrents {
            if self
                .add_torrent_params(&atp, StorageIndex(0), out)
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
