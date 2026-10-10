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

    /// `session_impl::add_torrent` — the real entry point:
    /// `AddTorrentParams` carries resume data, metadata or magnet
    /// state, url seeds and swarm peers.
    pub fn add_torrent_params(
        &self,
        atp: &bitdaemon_core::AddTorrentParams,
    ) -> Option<TorrentHandle> {
        let mut out = Vec::new();
        let ih = lock(&self.inner).add_torrent_params(
            atp,
            bitdaemon_common::types::StorageIndex(0),
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

    /// `get_torrents` — one status per swarm.
    #[must_use]
    pub fn torrent_statuses(&self) -> Vec<TorrentStatus> {
        lock(&self.inner)
            .torrents
            .values()
            .map(Torrent::status)
            .collect()
    }

    /// `post_torrent_updates` — posts `state_update_alert`.
    pub fn post_torrent_updates(&self) {
        lock(&self.inner).post_torrent_updates();
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

    /// `torrent::pause`/`resume` by info-hash.
    pub fn set_paused(&self, ih: &Sha1Hash, paused: bool) {
        lock(&self.inner).set_paused(ih, paused, &mut Vec::new());
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
        g.torrents.get(&self.info_hash).map(Torrent::status)
    }

    /// `prioritize_files` — upstream `torrent_handle::prioritize_files`.
    pub fn prioritize_files(&self, prios: &[bitdaemon_common::types::DownloadPriority]) {
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.prioritize_files(prios);
        }
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
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.prioritize_pieces(prios);
        }
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
    pub fn delete_files(&self, out: &mut SessionOutVec) {
        lock(&self.session).delete_files(&self.info_hash, 0, out);
    }

    /// `file_progress` — per-file downloaded bytes.
    #[must_use]
    pub fn file_progress(&self) -> Vec<i64> {
        lock(&self.session)
            .torrents
            .get(&self.info_hash)
            .map_or_else(Vec::new, Torrent::file_progress)
    }

    /// `scrape_tracker` — immediate scrape of tracker entry `idx`.
    pub fn scrape_tracker(&self, tracker_idx: usize, out: &mut SessionOutVec) {
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
        if let Some(t) = lock(&self.session).torrents.get_mut(&self.info_hash) {
            t.connect_peer(remote);
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
}
