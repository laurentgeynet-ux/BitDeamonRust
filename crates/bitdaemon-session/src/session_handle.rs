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
use crate::settings::SessionParams;
use crate::torrent::Torrent;

/// `torrent_status` — a snapshot of one swarm.
#[derive(Clone, Debug)]
pub struct TorrentStatus {
    /// `handle`.
    pub info_hash: Sha1Hash,
    /// `state` — paused / downloading / seeding.
    pub paused: bool,
    /// `is_seed`.
    pub is_seed: bool,
    /// `is_finished`.
    pub finished: bool,
    /// `num_peers`.
    pub num_peers: usize,
    /// `num_pieces` verified.
    pub num_have: usize,
    /// `num_pieces` total.
    pub num_pieces: i32,
    /// `progress` 0..1.
    pub progress: f64,
}

fn lock(s: &Mutex<Session>) -> MutexGuard<'_, Session> {
    s.lock().unwrap_or_else(|e| e.into_inner())
}

/// `session_handle` — cloneable, thread-safe entry point.
#[derive(Clone)]
pub struct SessionHandle {
    inner: Arc<Mutex<Session>>,
}

impl SessionHandle {
    /// `session` ctor — build the core from params.
    #[must_use]
    pub fn new(params: SessionParams) -> Self {
        let mut s = Session::new();
        if let Some(disk) = params.disk_io {
            s.set_disk(disk);
        }
        let _plugins = params.plugins; // fan-out wired on alert post
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

    /// `remove_torrent`.
    pub fn remove_torrent(&self, ih: &Sha1Hash) {
        // the handle drains `stopped` announces — a real engine
        // routes `Session::remove_torrent`'s out-effects itself
        lock(&self.inner).remove_torrent(ih, &mut Vec::new());
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

    /// `get_torrents` — one status per swarm.
    #[must_use]
    pub fn torrent_statuses(&self) -> Vec<TorrentStatus> {
        lock(&self.inner)
            .torrents
            .values()
            .map(|t| TorrentStatus {
                info_hash: t.info_hash,
                paused: t.paused,
                is_seed: t.is_seed(),
                finished: t.finished,
                num_peers: t.peers.len(),
                num_have: t.have.count(),
                num_pieces: t.num_pieces,
                progress: if t.num_pieces > 0 {
                    t.have.count() as f64 / f64::from(t.num_pieces)
                } else {
                    0.0
                },
            })
            .collect()
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

    /// `torrent::status`.
    #[must_use]
    pub fn status(&self) -> Option<TorrentStatus> {
        let g = lock(&self.session);
        g.torrents.get(&self.info_hash).map(|t| TorrentStatus {
            info_hash: t.info_hash,
            paused: t.paused,
            is_seed: t.is_seed(),
            finished: t.finished,
            num_peers: t.peers.len(),
            num_have: t.have.count(),
            num_pieces: t.num_pieces,
            progress: if t.num_pieces > 0 {
                t.have.count() as f64 / f64::from(t.num_pieces)
            } else {
                0.0
            },
        })
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
}
