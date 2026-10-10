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

use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_dht::dht_tracker::DhtTracker;
use bitdaemon_disk::disk_interface::DiskIo;
use bitdaemon_net::peer_wire::PeerMessage;

use crate::alerts::{Alert, AlertManager};
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
}

/// `session_impl` — all state owned on one thread.
#[derive(Default)]
pub struct Session {
    /// `m_torrents` by info_hash.
    pub torrents: BTreeMap<Sha1Hash, Torrent>,
    /// `m_dht` — v4/v6 tracker nodes.
    pub dht: DhtTracker,
    /// `m_disk_thread` — injected backend (`StorageFactory` seam).
    pub disk: Option<Arc<dyn DiskIo>>,
    /// `m_alert_manager`.
    pub alerts: AlertManager,
}

impl Session {
    /// `session_impl::session_impl`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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

    /// `remove_torrent` — drop the swarm (peers + picker).
    pub fn remove_torrent(&mut self, ih: &Sha1Hash) -> Option<Torrent> {
        self.torrents.remove(ih)
    }

    /// `pause`/`resume`.
    pub fn set_paused(&mut self, ih: &Sha1Hash, paused: bool) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.paused = paused;
        }
    }

    /// feed one decoded peer message → effects.
    pub fn on_peer_message(&mut self, p: &PeerPacket, out: &mut Vec<SessionOut>) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(&p.info_hash) {
            t.on_peer_message(p.peer, &p.msg, &mut eff);
        }
        self.route(p.info_hash, eff, out);
    }

    /// a peer connected — register it on the torrent.
    pub fn peer_connected(&mut self, ih: &Sha1Hash, peer: u64, conn: crate::peer_conn::PeerConn) {
        if let Some(t) = self.torrents.get_mut(ih) {
            t.add_peer(peer, conn);
        }
    }

    /// a peer disconnected — refcount down + abort in-flight.
    pub fn peer_disconnected(&mut self, ih: &Sha1Hash, peer: u64, out: &mut Vec<SessionOut>) {
        let mut eff = Vec::new();
        if let Some(t) = self.torrents.get_mut(ih) {
            t.remove_peer(peer, &mut eff);
        }
        self.route(*ih, eff, out);
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

    /// UDP datagram → dht tracker (any family).
    pub fn on_udp(&mut self, from: SocketAddr, buf: &[u8], out: &mut Vec<SessionOut>) {
        if let Some((pkt, to)) = self.dht.incoming(from, buf) {
            out.push(SessionOut::UdpPacket(pkt, to));
        }
    }

    /// `tick` — drive every torrent's peers + dht timeouts.
    pub fn tick(&mut self, out: &mut Vec<SessionOut>) -> std::time::Duration {
        let wait = self.dht.tick();
        while let Some((pkt, to)) = self.dht.poll() {
            out.push(SessionOut::UdpPacket(pkt, to));
        }
        let ihs: Vec<Sha1Hash> = self
            .torrents
            .iter()
            .filter(|(_, t)| !t.paused && t.want_peers())
            .map(|(ih, _)| *ih)
            .collect();
        for ih in ihs {
            let mut eff = Vec::new();
            if let Some(t) = self.torrents.get_mut(&ih) {
                for id in t.peers.keys().copied().collect::<Vec<_>>() {
                    t.drive_peer(id, &mut eff);
                }
            }
            self.route(ih, eff, out);
        }
        wait
    }

    fn route(&mut self, ih: Sha1Hash, effects: Vec<TorrentEffect>, out: &mut Vec<SessionOut>) {
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
