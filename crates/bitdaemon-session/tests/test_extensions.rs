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
    t.on_files_checked(
        true,
        &Default::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert_eq!(rec.files_checked.load(Ordering::Relaxed), 1);

    // `piece_verified` pass/fail → on_piece_pass/on_piece_failed.
    // upstream `on_piece_verified` ignores pieces the picker doesn't
    // consider finished — mark the blocks downloaded first. Piece 1's
    // blocks carry a downloader key: without one the piece counts as
    // `found_on_disk` and upstream skips the `on_piece_failed` plugin
    // call (nothing to blame on the swarm).
    for i in [0, 1] {
        let dl = if i == 1 { Some(7) } else { None };
        if let Some(p) = t.picker.as_mut() {
            for b in 0..p.blocks_in_piece(PieceIndex(i)) {
                p.mark_as_finished(
                    bitdaemon_common::types::PieceBlock {
                        piece_index: PieceIndex(i),
                        block_index: b,
                    },
                    dl,
                );
            }
        }
    }
    t.piece_verified(
        PieceIndex(0),
        true,
        &[],
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    t.piece_verified(
        PieceIndex(1),
        false,
        &[],
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
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
    t.on_files_checked(
        true,
        &Default::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
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
    t.on_peer_message(
        9,
        &PeerMessage::Have(PieceIndex(0)),
        0,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
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
    // upstream `set_paused` sets `m_paused` first — `on_pause`
    // vetoes only the do_pause machinery (disconnects, stopped
    // announce, TorrentPaused alert).
    assert!(s.torrents.get(&ih).unwrap().paused);
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
            &bitdaemon_session::settings::SessionSettings::default(),
            &bitdaemon_session::counters::Counters::default(),
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

// ---------- A02: extension-protocol wire fidelity ----------

use bitdaemon_session::counters::{Counter, Counters};
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::torrent::TorrentEffect;

fn set() -> SessionSettings {
    SessionSettings::default()
}

/// a conn that finished the handshake and announced `ut_metadata`.
fn announced_conn() -> PeerConn {
    let mut c = PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0);
    c.remote = Some("10.0.0.2:6881".parse().unwrap());
    c.handshake_done = true;
    c.ext_ut_metadata = 2;
    c
}

#[test]
fn ut_metadata_requires_announced_extension() {
    // upstream `on_extended` → plugin `m_message_index == 0` → falls
    // through to `invalid_message` (peer_error).
    let ih = Sha1Hash::from_bytes(&[11; 20]).unwrap();
    let mut t = torrent(ih);
    let mut c = announced_conn();
    c.ext_ut_metadata = 0;
    t.add_peer(9, c);
    let mut out = Vec::new();
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 2,
            data: b"d8:msg_typei0e5:piecei0ee".to_vec().into(),
        },
        0,
        &set(),
        &Counters::default(),
        &mut out,
    );
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::DisconnectPeer { peer: 9, sev, .. }
            if *sev == bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
    )));
}

#[test]
fn ut_metadata_malformed_disconnects_peer_error() {
    let ih = Sha1Hash::from_bytes(&[12; 20]).unwrap();
    let mut t = torrent(ih);
    t.add_peer(9, announced_conn());
    let s = set();
    let counters = Counters::default();
    for body in [
        // oversized — `length > 17*1024` upstream
        {
            let mut v = b"d8:msg_typei1e5:piecei0ee".to_vec();
            v.extend(std::iter::repeat_n(0u8, 17 * 1024));
            v
        },
        // non-dict
        b"i42e".to_vec(),
        // dict missing `piece`
        b"d8:msg_typei1ee".to_vec(),
    ] {
        let mut out = Vec::new();
        t.on_peer_message(
            9,
            &PeerMessage::Extended {
                id: 2,
                data: body.into(),
            },
            0,
            &s,
            &counters,
            &mut out,
        );
        assert!(
            out.iter().any(|e| matches!(
                e,
                TorrentEffect::DisconnectPeer { peer: 9, sev, .. }
                    if *sev == bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
            )),
            "malformed packet must disconnect"
        );
    }
}

#[test]
fn ut_metadata_request_serving_and_counter_timing() {
    let ih = Sha1Hash::from_bytes(&[13; 20]).unwrap();
    let mut t = torrent(ih);
    t.metadata_raw = Some(vec![7u8; 32 * 1024]);
    t.add_peer(9, announced_conn());
    let s = set();
    let counters = Counters::default();
    let mut out = Vec::new();

    // invalid piece index → `dont_have` reply, NO counter bump
    // (upstream `return true` before the tail counter).
    let req = bitdaemon_session::ut_metadata::metadata_packet(
        bitdaemon_session::ut_metadata::MetadataMsg::Request,
        99,
        false,
        b"",
    );
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 2,
            data: req.into(),
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    let reject = out.iter().any(|e| {
        matches!(
            e,
            TorrentEffect::SendTo {
                peer: 9,
                msg: PeerMessage::Extended { id: 2, data }
            } if data.windows(13).any(|w| w == b"8:msg_typei2e")
        )
    });
    assert!(reject, "expected a dont_have reply");
    assert_eq!(counters.value(Counter::NumIncomingMetadata), 0);

    // valid request → `piece` reply and the counter bumps.
    out.clear();
    let req = bitdaemon_session::ut_metadata::metadata_packet(
        bitdaemon_session::ut_metadata::MetadataMsg::Request,
        0,
        false,
        b"",
    );
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 2,
            data: req.into(),
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    let piece = out.iter().any(|e| {
        matches!(
            e,
            TorrentEffect::SendTo {
                peer: 9,
                msg: PeerMessage::Extended { id: 2, data }
            } if data.windows(13).any(|w| w == b"8:msg_typei1e")
                && data.len() > 16 * 1024
        )
    });
    assert!(piece, "expected a piece reply carrying metadata");
    assert_eq!(counters.value(Counter::NumIncomingMetadata), 1);
}

#[test]
fn ut_metadata_piece_chains_next_request() {
    // a magnet without metadata: the tracker asks piece-by-piece;
    // upstream runs `maybe_send_request` right after a `piece` lands.
    let ih = Sha1Hash::from_bytes(&[14; 20]).unwrap();
    let mut t = Torrent::new(ih, 0, 128 * 1024, Bitfield::default(), StorageIndex(0));
    let mut c = announced_conn();
    c.ext_ut_metadata = 2;
    c.metadata_size = Some(32 * 1024);
    t.add_peer(9, c);
    let s = set();
    let counters = Counters::default();
    let mut out = Vec::new();

    // handshake arm: `metadata_size` sizes the tracker → the first
    // request goes out immediately.
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 0,
            data: b"d1:md11:ut_metadatai2ee13:metadata_sizei32768ee"
                .to_vec()
                .into(),
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    let ask = |out: &[TorrentEffect]| -> Vec<i64> {
        out.iter()
            .filter_map(|e| match e {
                TorrentEffect::SendTo {
                    msg: PeerMessage::Extended { id: 2, data },
                    ..
                } => Some(data.clone()),
                _ => None,
            })
            .filter(|d| d.windows(13).any(|w| w == b"8:msg_typei0e"))
            .filter_map(|d| {
                // piece index from the dict
                let pos = d.windows(7).position(|w| w == b"5:piece")?;
                let rest = &d[pos + 7..];
                let end = rest.iter().position(|&b| b == b'e')?;
                std::str::from_utf8(&rest[1..end]).ok()?.parse().ok()
            })
            .collect()
    };
    let asked = ask(&out);
    assert_eq!(asked.len(), 1, "one request after the handshake");

    // answer piece 0 → the next request is emitted in the SAME batch.
    out.clear();
    let metadata = vec![5u8; 32 * 1024];
    let body = bitdaemon_session::ut_metadata::metadata_packet(
        bitdaemon_session::ut_metadata::MetadataMsg::Piece,
        asked[0] as i32,
        true,
        &metadata,
    );
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 2,
            data: body.into(),
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    let asked2 = ask(&out);
    assert_eq!(asked2.len(), 1, "the next piece request is chained");
    assert_ne!(asked2[0], asked[0]);
}

#[test]
fn allowed_fast_deferred_to_first_interested() {
    // upstream `send_allowed_set` fires once inside `on_interested`,
    // never at attach; the conn records `accept_fast` so choked
    // peers can actually claim the offered pieces.
    let ih = Sha1Hash::from_bytes(&[15; 20]).unwrap();
    let mut t = torrent(ih);
    let mut c = announced_conn();
    c.supports_fast = true;
    t.add_peer(9, c);
    let s = set();
    let counters = Counters::default();
    let mut out = Vec::new();

    t.on_peer_message(9, &PeerMessage::Interested, 0, &s, &counters, &mut out);
    let n = out
        .iter()
        .filter(|e| {
            matches!(
                e,
                TorrentEffect::SendTo {
                    peer: 9,
                    msg: PeerMessage::AllowedFast(_)
                }
            )
        })
        .count();
    assert!(n > 0, "first Interested must emit the allowed-fast set");
    assert!(!t.peers.get(&9).unwrap().accept_fast.is_empty());

    // a second Interested must NOT resend (m_sent_allowed_fast).
    out.clear();
    t.on_peer_message(9, &PeerMessage::Interested, 0, &s, &counters, &mut out);
    assert!(!out.iter().any(|e| matches!(
        e,
        TorrentEffect::SendTo {
            msg: PeerMessage::AllowedFast(_),
            ..
        }
    )));
}

#[test]
fn suggest_negative_index_dropped() {
    let ih = Sha1Hash::from_bytes(&[16; 20]).unwrap();
    let mut t = torrent(ih);
    let mut c = announced_conn();
    c.supports_fast = true;
    t.add_peer(9, c);
    let mut out = Vec::new();
    t.on_peer_message(
        9,
        &PeerMessage::SuggestPiece(PieceIndex(-1)),
        0,
        &set(),
        &Counters::default(),
        &mut out,
    );
    assert!(t.peers.get(&9).unwrap().suggested_pieces.is_empty());
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. }))
    );
}

#[test]
fn ut_pex_severities() {
    let ih = Sha1Hash::from_bytes(&[17; 20]).unwrap();
    let mut t = torrent(ih);
    let mut c = announced_conn();
    c.ext_ut_pex = 1;
    t.add_peer(9, c);
    let s = set();
    let counters = Counters::default();

    // >500 KiB → `pex_message_too_large` (peer_error)
    let mut out = Vec::new();
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 1,
            data: vec![0u8; 501 * 1024].into(),
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::DisconnectPeer { peer: 9, sev, .. }
            if *sev == bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
    )));

    // malformed body → `invalid_pex_message` (peer_error)
    let mut out = Vec::new();
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 1,
            data: b"i7e".to_vec().into(),
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::DisconnectPeer { peer: 9, sev, .. }
            if *sev == bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
    )));
    // neither leg bumps `num_incoming_pex` (counter is at the tail).
    assert_eq!(counters.value(Counter::NumIncomingPex), 0);
}
