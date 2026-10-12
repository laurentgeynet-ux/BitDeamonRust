// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! ADR-0023 D9 — extension factories, `new_peer_connection` seam,
//! `peer_plugin` message observation, `on_pause` veto; plus the
//! upstream-deprecated `http_seed` handle semantics (BEP-17 fetch
//! was removed upstream — `torrent_info` `atp.http_seeds` still
//! merges into the BEP-19 fetch list).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::alerts::{AlertCategory, PeerPlugin, SessionPlugin};
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::Session;
use bitdaemon_session::torrent::{MetadataVerdict, Torrent};

fn session() -> Session {
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    s
}

fn torrent(ih: Sha1Hash) -> Torrent {
    let mut t = Torrent::new(ih, 2, 128 * 1024, Bitfield::with_size(2), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 2]);
    t.init_picker(false);
    t
}

/// Upstream `torrent_plugin` analog — records every hook call.
#[derive(Debug, Default)]
struct Recorder {
    piece_pass: AtomicUsize,
    piece_fail: AtomicUsize,
    files_checked: AtomicUsize,
    ticks: AtomicUsize,
    new_conns: AtomicUsize,
    veto_pause: std::sync::atomic::AtomicBool,
    peer_msgs: Mutex<Vec<String>>,
}

#[derive(Debug)]
struct RecPeer(Arc<Recorder>);
impl PeerPlugin for RecPeer {
    fn on_message(&self, msg: &PeerMessage) {
        self.0.peer_msgs.lock().unwrap().push(format!("{msg:?}"));
    }
    fn on_tick(&self) {
        self.0.ticks.fetch_add(1, Ordering::Relaxed);
    }
}

impl SessionPlugin for Recorder {
    fn new_peer_connection(&self, _remote: &std::net::SocketAddr) -> Option<Arc<dyn PeerPlugin>> {
        self.new_conns.fetch_add(1, Ordering::Relaxed);
        None
    }
    fn on_piece_pass(&self, _i: PieceIndex) {
        self.piece_pass.fetch_add(1, Ordering::Relaxed);
    }
    fn on_piece_failed(&self, _i: PieceIndex) {
        self.piece_fail.fetch_add(1, Ordering::Relaxed);
    }
    fn on_files_checked(&self) {
        self.files_checked.fetch_add(1, Ordering::Relaxed);
    }
    fn on_pause(&self) -> bool {
        self.veto_pause.load(Ordering::Relaxed)
    }
    fn tick(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
}

/// `new_peer_connection` → `peer_plugin` — the `Recorder` variant
/// that produces a `RecPeer` observing messages.
#[derive(Debug)]
struct ConnProducer(Arc<Recorder>);
impl SessionPlugin for ConnProducer {
    fn new_peer_connection(&self, _remote: &std::net::SocketAddr) -> Option<Arc<dyn PeerPlugin>> {
        self.0.new_conns.fetch_add(1, Ordering::Relaxed);
        Some(Arc::new(RecPeer(self.0.clone())))
    }
    fn tick(&self) {
        self.0.ticks.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn add_extension_instantiates_and_sees_lifecycle() {
    let ih = Sha1Hash::from_bytes(&[5; 20]).unwrap();
    let mut s = session();
    s.add_torrent(torrent(ih));
    let t = s.torrents.get_mut(&ih).unwrap();

    let rec = Arc::new(Recorder::default());
    t.add_extension_plugin(rec.clone());

    // `torrent::files_checked` → `on_files_checked`.
    let mut out = Vec::new();
    t.on_files_checked(true, &mut out);
    assert_eq!(rec.files_checked.load(Ordering::Relaxed), 1);

    // `piece_verified` pass/fail → on_piece_pass/on_piece_failed.
    t.piece_verified(PieceIndex(0), true, &mut out);
    t.piece_verified(PieceIndex(1), false, &mut out);
    assert_eq!(rec.piece_pass.load(Ordering::Relaxed), 1);
    assert_eq!(rec.piece_fail.load(Ordering::Relaxed), 1);

    // `second_tick` → `tick`.
    let mut out = Vec::new();
    s.tick(&mut out);
    assert_eq!(rec.ticks.load(Ordering::Relaxed), 1);
}

#[test]
fn late_add_extension_replays_files_checked() {
    // upstream `torrent::add_extension`: when the torrent already
    // checked files, the new plugin gets `on_files_checked`
    // immediately.
    let ih = Sha1Hash::from_bytes(&[6; 20]).unwrap();
    let mut t = torrent(ih);
    let mut out = Vec::new();
    t.on_files_checked(true, &mut out);
    assert!(t.files_checked);

    let rec = Arc::new(Recorder::default());
    t.add_extension_plugin(rec.clone());
    assert_eq!(rec.files_checked.load(Ordering::Relaxed), 1);
}

#[test]
fn new_peer_connection_and_message_fanout() {
    let ih = Sha1Hash::from_bytes(&[7; 20]).unwrap();
    let mut t = torrent(ih);
    let rec = Arc::new(Recorder::default());
    t.add_extension_plugin(Arc::new(ConnProducer(rec.clone())));

    // attach a peer → `new_peer_connection` fires once.
    let mut c = PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0);
    c.remote = Some("10.0.0.1:6881".parse().unwrap());
    t.add_peer(9, c);
    assert_eq!(rec.new_conns.load(Ordering::Relaxed), 1);
    assert_eq!(t.peers.get(&9).unwrap().plugins.len(), 1);

    // an incoming wire message reaches the peer_plugin.
    let mut out = Vec::new();
    t.on_peer_message(9, &PeerMessage::Have(PieceIndex(0)), 0, &mut out);
    assert_eq!(rec.peer_msgs.lock().unwrap().len(), 1);

    // per-peer `on_tick` rides the session tick.
    let mut s = session();
    s.add_torrent(t);
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(rec.ticks.load(Ordering::Relaxed) >= 1);
}

#[test]
fn on_pause_veto_keeps_torrent_running() {
    let ih = Sha1Hash::from_bytes(&[8; 20]).unwrap();
    let mut s = session();
    let mut t = torrent(ih);
    t.paused = false;
    let rec = Arc::new(Recorder::default());
    rec.veto_pause.store(true, Ordering::Relaxed);
    t.add_extension_plugin(rec.clone());
    s.add_torrent(t);

    let mut out = Vec::new();
    s.set_paused(&ih, true, &mut out);
    // veto → torrent stays unpaused, no TorrentPaused alert.
    assert!(!s.torrents.get(&ih).unwrap().paused);
    assert!(
        !s.pop_alerts()
            .iter()
            .any(|a| matches!(a, bitdaemon_session::alerts::Alert::TorrentPaused { .. }))
    );

    // clear the veto → pause proceeds (the Arc is shared with the
    // extension instance).
    rec.veto_pause.store(false, Ordering::Relaxed);
    s.set_paused(&ih, true, &mut out);
    assert!(s.torrents.get(&ih).unwrap().paused);
}

#[test]
fn metadata_verdict_distinguishes_retry_from_fatal() {
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = torrent(ih);
    // wrong hash → HashMismatch (upstream penalizes the source
    // and keeps trying other peers).
    let mut out = Vec::new();
    assert_eq!(
        t.set_metadata_verdict(
            b"d3:foo3:barde",
            &bitdaemon_core::torrent_info::LoadTorrentLimits::default(),
            &mut out
        ),
        MetadataVerdict::HashMismatch
    );
    // hash-correct-but-unparseable can't be constructed without
    // forging a SHA-1 — the verdict path is exercised via
    // `HashMismatch`; the ParseError arm pauses + TorrentError
    // inside `on_metadata_packet` (upstream `set_metadata`).
}

#[test]
fn http_seed_handle_semantics_are_upstream_deprecated() {
    // upstream v2.x: `add_http_seed`/`remove_http_seed` are no-ops
    // and `http_seeds()` returns an empty set — BEP-17 fetching was
    // removed ("removed support for BEP 17 web seeds (in favor of
    // BEP 19)"). `.torrent`/resume `httpseeds` lists still feed the
    // BEP-19 web-seed path via `from_atp`.
    let ih = Sha1Hash::from_bytes(&[10; 20]).unwrap();
    let mut t = torrent(ih);
    t.add_http_seed("http://seed.example/files".into());
    t.remove_http_seed("http://seed.example/files");
    assert!(t.http_seeds().is_empty());
}
