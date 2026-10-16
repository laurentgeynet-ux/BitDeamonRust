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
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
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
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
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
    t.add_peer(
        9,
        announced_conn(),
        &bitdaemon_session::counters::Counters::default(),
    );
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
    t.add_peer(
        9,
        announced_conn(),
        &bitdaemon_session::counters::Counters::default(),
    );
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
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
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
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
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
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
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
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
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

// ---------- A02 apply regressions ----------

/// A02/N14 — `ut_pex` cadence is per conn (`m_last_msg`, init
/// `min_time()` upstream): a fresh conn's first full list goes out
/// at its next tick once `num_peers() > 1`; a third conn attaching
/// later is not throttled by the others' 60 s window.
#[test]
fn pex_tick_per_conn_first_list() {
    use std::time::{Duration, Instant};
    let ih = Sha1Hash::from_bytes(&[19; 20]).unwrap();
    let mut t = torrent(ih);
    let s = set();
    let _ = s;
    let mk = |ip: &str| {
        let mut c = announced_conn();
        c.ext_ut_pex = 1;
        c.outgoing = true;
        c.connecting = false;
        c.remote = Some(format!("{ip}:6881").parse().unwrap());
        c
    };
    t.add_peer(
        9,
        mk("10.0.0.9"),
        &bitdaemon_session::counters::Counters::default(),
    );
    let now = Instant::now();
    let mut out = Vec::new();

    // lone peer — upstream `num_peers() <= 1 → return`; the lone
    // phase must not consume the conn's 60 s window.
    t.pex_tick(now, &mut out);
    assert!(out.is_empty());
    assert!(t.peers[&9].pex_last_msg.is_none());

    // second conn attaches — the very next tick sends both peers a
    // full list, no torrent-global 60 s delay.
    t.add_peer(
        10,
        mk("10.0.0.10"),
        &bitdaemon_session::counters::Counters::default(),
    );
    t.pex_tick(now + Duration::from_secs(5), &mut out);
    let pex_to = |out: &[TorrentEffect], id: u64| {
        out.iter()
            .filter(|e| {
                matches!(
                    e,
                    TorrentEffect::SendTo {
                        peer,
                        msg: PeerMessage::Extended { id: 1, .. }
                    } if *peer == id
                )
            })
            .count()
    };
    assert_eq!(pex_to(&out, 9), 1);
    assert_eq!(pex_to(&out, 10), 1);

    // inside the 60 s window → nothing for either.
    out.clear();
    t.pex_tick(now + Duration::from_secs(6), &mut out);
    assert_eq!(pex_to(&out, 9), 0);
    assert_eq!(pex_to(&out, 10), 0);

    // a third conn attaching now gets its own first list on the
    // next tick — its `m_last_msg` starts at `min_time()`, it does
    // not wait out the others' window (this is the N14 regression).
    t.add_peer(
        11,
        mk("10.0.0.11"),
        &bitdaemon_session::counters::Counters::default(),
    );
    t.pex_tick(now + Duration::from_secs(7), &mut out);
    assert_eq!(pex_to(&out, 11), 1, "fresh conn must get its first list");
    assert_eq!(pex_to(&out, 9), 0);
    assert_eq!(pex_to(&out, 10), 0);
}

/// A02/N1 — `pex_seed` is `t->valid_metadata() && p->is_seed()`:
/// a conn whose bitfield covers the advertised piece count carries
/// the seed flag; a metadata-less torrent never flags seeds.
#[test]
fn pex_seed_flag_uses_valid_metadata() {
    use bitdaemon_session::ut_pex::parse_pex_message;
    use std::time::Instant;
    let ih = Sha1Hash::from_bytes(&[20; 20]).unwrap();
    let mut t = torrent(ih);
    // seeder conn (advertised + advertises pex): full bitfield.
    let mut seed = announced_conn();
    seed.ext_ut_pex = 1;
    seed.outgoing = true;
    seed.connecting = false;
    seed.peer_pieces = Bitfield::with_size(2);
    seed.peer_pieces.set_all();
    seed.num_pieces = 2;
    // receiver conn.
    let mut recv = announced_conn();
    recv.ext_ut_pex = 1;
    t.add_peer(9, seed, &bitdaemon_session::counters::Counters::default());
    t.add_peer(10, recv, &bitdaemon_session::counters::Counters::default());
    let mut out = Vec::new();
    t.pex_tick(Instant::now(), &mut out);
    let body = out
        .iter()
        .find_map(|e| match e {
            TorrentEffect::SendTo {
                peer: 10,
                msg: PeerMessage::Extended { id: 1, data },
            } => Some(data.clone()),
            _ => None,
        })
        .expect("pex msg to 10");
    let (added, _dropped) = parse_pex_message(&body, usize::MAX, 0).expect("parse");
    let seed_ep = added
        .iter()
        .find(|p| p.addr.ip().to_string() == "10.0.0.2")
        .expect("seed conn advertised");
    assert_eq!(
        seed_ep.flags & bitdaemon_session::ut_pex::flags::PEX_SEED,
        bitdaemon_session::ut_pex::flags::PEX_SEED
    );
}

/// A02/N8 — upstream keeps `m_peers` and `m_peers6` capped
/// *separately* at `max_pex_peers` (ut_pex.cpp separate `added` /
/// `added6` loops): a v4 flood can't starve the v6 ledger.
#[test]
fn pex_known_capped_per_family() {
    let ih = Sha1Hash::from_bytes(&[21; 20]).unwrap();
    let mut t = torrent(ih);
    let mut c = announced_conn();
    c.ext_ut_pex = 1;
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
    // 60 v4 + 30 v6, remote is local so the is_local filter lets
    // private-space addrs through.
    let peers: Vec<(std::net::SocketAddr, u8)> = (0..60u16)
        .map(|i| (format!("10.0.1.{i}:6881").parse().unwrap(), 0u8))
        .chain((0..30u16).map(|i| (format!("[2001:db8::{i:x}]:6881").parse().unwrap(), 0u8)))
        .collect();
    let (body, _sent, _n) =
        bitdaemon_session::ut_pex::build_pex_message(&peers, &Default::default());
    let mut out = Vec::new();
    t.on_peer_message(
        9,
        &PeerMessage::Extended {
            id: 1,
            data: body.into(),
        },
        0,
        &set(),
        &Counters::default(),
        &mut out,
    );
    let c = &t.peers[&9];
    assert_eq!(c.pex_known.len(), 50, "v4 capped at max_pex_peers");
    assert_eq!(c.pex_known6.len(), 30, "v6 ledger unaffected by the v4 cap");
}

/// A02/N13 — `send_allowed_set` gates on the *remote's*
/// `upload_only()` (`m_upload_only || is_seed() || m_have_all`),
/// not ours: seeds and BEP-21 upload-only peers get no set, but a
/// leecher still gets one even when *we* are upload-only.
#[test]
fn allowed_set_gates_on_remote_upload_only() {
    let ih = Sha1Hash::from_bytes(&[22; 20]).unwrap();
    let mut t = torrent(ih);
    let s = set();
    let counters = Counters::default();
    let mk = |upload_only: bool, seed: bool| {
        let mut c = announced_conn();
        c.supports_fast = true;
        c.remote_upload_only = upload_only;
        if seed {
            c.peer_pieces = Bitfield::with_size(2);
            c.peer_pieces.set_all();
            c.num_pieces = 2;
        }
        c
    };
    t.add_peer(
        9,
        mk(true, false),
        &bitdaemon_session::counters::Counters::default(),
    ); // BEP-21 upload-only
    t.add_peer(
        10,
        mk(false, true),
        &bitdaemon_session::counters::Counters::default(),
    ); // seed
    t.add_peer(
        11,
        mk(false, false),
        &bitdaemon_session::counters::Counters::default(),
    ); // leecher
    let mut out = Vec::new();
    for id in [9u64, 10, 11] {
        t.on_peer_message(id, &PeerMessage::Interested, 0, &s, &counters, &mut out);
    }
    let fast_for = |id: u64| {
        out.iter()
            .filter(|e| {
                matches!(
                    e,
                    TorrentEffect::SendTo {
                        peer,
                        msg: PeerMessage::AllowedFast(_)
                    } if *peer == id
                )
            })
            .count()
    };
    assert_eq!(fast_for(9), 0, "upload-only remote: no allowed set");
    assert_eq!(fast_for(10), 0, "seed remote: no allowed set");
    assert!(fast_for(11) > 0, "leecher remote gets the set");

    // and OUR upload-only state does not gate it (the round-3 bug):
    // a leecher attaching while we're in upload mode still gets a
    // set — it can download our missing fast pieces.
    t.upload_mode = true;
    let mut c = mk(false, false);
    c.remote = Some("10.0.0.12:6881".parse().unwrap());
    t.add_peer(12, c, &bitdaemon_session::counters::Counters::default());
    out.clear();
    t.on_peer_message(12, &PeerMessage::Interested, 0, &s, &counters, &mut out);
    assert!(
        out.iter().any(|e| matches!(
            e,
            TorrentEffect::SendTo {
                peer: 12,
                msg: PeerMessage::AllowedFast(_)
            }
        )),
        "local upload-only must not gate the remote's set"
    );
}

/// A02/N16 — upstream sets `m_has_metadata` in
/// `incoming_interested` (a peer ready to download has metadata)
/// but NOT in `incoming_dont_have`. The flag feeds
/// `disconnect_if_redundant`'s keep-alive gate.
#[test]
fn interested_sets_has_metadata_donthave_doesnt() {
    let ih = Sha1Hash::from_bytes(&[23; 20]).unwrap();
    let mut t = torrent(ih);
    let s = set();
    let counters = Counters::default();
    t.add_peer(
        9,
        announced_conn(),
        &bitdaemon_session::counters::Counters::default(),
    );
    t.add_peer(
        10,
        announced_conn(),
        &bitdaemon_session::counters::Counters::default(),
    );
    let mut out = Vec::new();
    // `m_has_metadata` ctor is `true` upstream — only a peer that
    // advertised `ut_metadata` without `metadata_size` starts out
    // flagged false (`set_has_metadata(false)` in the handshake).
    let ut_md_handshake = || PeerMessage::Extended {
        id: 0,
        data: b"d1:md11:ut_metadatai2eee".to_vec().into(),
    };
    for id in [9u64, 10] {
        t.on_peer_message(id, &ut_md_handshake(), 0, &s, &counters, &mut out);
        assert!(!t.peers[&id].has_metadata);
    }
    t.on_peer_message(
        9,
        &PeerMessage::DontHave {
            piece: PieceIndex(0),
            ext_id: 0,
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    assert!(
        !t.peers[&9].has_metadata,
        "dont_have must not set m_has_metadata"
    );
    t.on_peer_message(10, &PeerMessage::Interested, 0, &s, &counters, &mut out);
    assert!(
        t.peers[&10].has_metadata,
        "incoming_interested sets m_has_metadata"
    );
}

/// A02/N17 — upstream `incoming_dont_have` early-outs on
/// `is_disconnecting()` before the `invalid_dont_have` bound check:
/// a conn mid-teardown never earns a failcount bump.
#[test]
fn dont_have_on_disconnecting_conn_is_silent() {
    let ih = Sha1Hash::from_bytes(&[24; 20]).unwrap();
    let mut t = torrent(ih);
    let s = set();
    let counters = Counters::default();

    // BEP-6 DontHave arm.
    let mut c = announced_conn();
    c.disconnecting = true;
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
    let mut out = Vec::new();
    t.on_peer_message(
        9,
        &PeerMessage::DontHave {
            piece: PieceIndex(99),
            ext_id: 0,
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. })),
        "disconnecting conn must not get invalid_dont_have"
    );

    // lt_donthave extended arm — same gate.
    let mut c = announced_conn();
    c.ext_dont_have = 7;
    c.local_dont_have = 7;
    c.disconnecting = true;
    t.add_peer(10, c, &bitdaemon_session::counters::Counters::default());
    out.clear();
    t.on_peer_message(
        10,
        &PeerMessage::Extended {
            id: 7,
            data: vec![0, 0, 0, 99].into(),
        },
        0,
        &s,
        &counters,
        &mut out,
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. })),
        "lt_donthave on a disconnecting conn must be silent"
    );
}

/// A02/N7 — `send_upload_only` runs the full `send_not_interested`
/// legs: interested conns clear `m_interesting`, drop the
/// `num_peers_down_interested` gauge, leave slow start, re-check
/// redundancy — and only then emit `NotInterested`; uninterested
/// upload-only conns go straight to `disconnect_if_redundant`.
#[test]
fn send_upload_only_full_not_interested_legs() {
    let ih = Sha1Hash::from_bytes(&[25; 20]).unwrap();
    let mut t = torrent(ih);
    let s = set();
    let counters = Counters::default();
    t.connections_initialized = true;

    // conn 9 — interested leecher: full legs + wire message.
    let mut c9 = announced_conn();
    c9.we_sent_interested = true;
    c9.interesting = true;
    c9.has_metadata = true;
    c9.pipeline.slow_start = true;
    counters.add(Counter::NumPeersDownInterested, 1);
    t.add_peer(9, c9, &bitdaemon_session::counters::Counters::default());
    // conn 10 — uninterested upload-only seed: `disconnect_if_redundant`
    // fires instead of a wire message.
    let mut c10 = announced_conn();
    c10.remote_upload_only = true;
    c10.has_metadata = true;
    c10.bitfield_received = true;
    c10.peer_pieces = Bitfield::with_size(2);
    c10.peer_pieces.set_all();
    c10.num_pieces = 2;
    c10.remote = Some("10.0.0.10:6881".parse().unwrap());
    t.add_peer(10, c10, &bitdaemon_session::counters::Counters::default());

    let mut out = Vec::new();
    t.set_upload_mode(true, &s, &counters, &mut out);

    let c9 = &t.peers[&9];
    assert!(!c9.we_sent_interested);
    assert!(!c9.interesting);
    assert!(
        !c9.pipeline.slow_start,
        "send_not_interested leaves slow start"
    );
    assert_eq!(counters.value(Counter::NumPeersDownInterested), 0);
    assert!(
        out.iter().any(|e| matches!(
            e,
            TorrentEffect::SendTo {
                peer: 9,
                msg: PeerMessage::NotInterested
            }
        )),
        "surviving conn gets NotInterested on the wire"
    );
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { peer: 10, .. })),
        "uninterested upload-only conn hits disconnect_if_redundant"
    );
}

/// A02/N18 — `disconnect_if_redundant` consults `can_disconnect`:
/// a `peer_plugin` veto keeps the conn (upstream extension hook).
#[derive(Debug)]
struct VetoDisconnect;
impl PeerPlugin for VetoDisconnect {
    fn can_disconnect(&self) -> bool {
        false
    }
}

#[test]
fn disconnect_if_redundant_honors_plugin_veto() {
    let ih = Sha1Hash::from_bytes(&[26; 20]).unwrap();
    let mut t = torrent(ih);
    let mut c = announced_conn();
    c.remote_upload_only = true;
    c.has_metadata = true;
    c.peer_pieces = Bitfield::with_size(2);
    c.peer_pieces.set_all();
    c.num_pieces = 2;
    c.plugins.push(Arc::new(VetoDisconnect));
    t.add_peer(9, c, &bitdaemon_session::counters::Counters::default());
    t.upload_mode = true;
    let mut out = Vec::new();
    assert!(!t.disconnect_if_redundant(9, &mut out));
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. })),
        "a can_disconnect veto keeps the conn"
    );

    // and a conn mid-teardown never earns a second disconnect —
    // upstream `m_disconnecting` early-out.
    t.peers.get_mut(&9).unwrap().plugins.clear();
    t.peers.get_mut(&9).unwrap().disconnecting = true;
    assert!(!t.disconnect_if_redundant(9, &mut out));
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. }))
    );
}

/// A02/N15 — `smart_ban` is fed by the hash job's per-block SHA-1
/// digests via `Session::on_piece_verified` (upstream re-reads each
/// block off disk): two failed downloads of the same piece carrying
/// different digests for the same block ban the inconsistent
/// sender. Two downloaders keep the culprit ambiguous so
/// `penalize_peers` doesn't fire `known_bad` first.
#[test]
fn smart_ban_fed_by_hash_job_digests() {
    use bitdaemon_common::types::PieceBlock;
    let ih = Sha1Hash::from_bytes(&[27; 20]).unwrap();
    // one piece, two 16 KiB blocks.
    let mut t = Torrent::new(ih, 1, 32768, Bitfield::with_size(1), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap()]);
    t.init_picker(true);
    let mk = |pid: u8, ip: &str| {
        let mut c = PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0);
        c.peer_id = Some(bitdaemon_common::PeerId::from_bytes(&[pid; 20]).unwrap());
        c.remote = Some(format!("{ip}:6881").parse().unwrap());
        c.handshake_done = true;
        c
    };
    let c9 = mk(7, "10.0.0.9");
    let c10 = mk(8, "10.0.0.10");
    let key9 = c9.peer_key().unwrap();
    let key10 = c10.peer_key().unwrap();
    t.add_peer(9, c9, &bitdaemon_session::counters::Counters::default());
    t.add_peer(10, c10, &bitdaemon_session::counters::Counters::default());
    // swarm entries `find_peers`/`peer_index` resolve for the ban
    // and the trust-points legs.
    for ip in ["10.0.0.9:6881", "10.0.0.10:6881"] {
        t.peer_list.add_peer(
            ip.parse().unwrap(),
            bitdaemon_common::flags::PeerSourceFlags::TRACKER,
            Default::default(),
            &t.peer_list_state.clone(),
        );
    }
    let mut s = session();
    s.add_torrent(t);

    let b0 = PieceBlock {
        piece_index: PieceIndex(0),
        block_index: 0,
    };
    let b1 = PieceBlock {
        piece_index: PieceIndex(0),
        block_index: 1,
    };
    let finish = |s: &mut Session| {
        let t = s.torrents.get_mut(&ih).unwrap();
        let p = t.picker.as_mut().unwrap();
        p.mark_as_finished(b0, Some(key9));
        p.mark_as_finished(b1, Some(key10));
    };
    let mut out = Vec::new();
    finish(&mut s);
    let d_a = Sha1Hash::from_bytes(&[0xAA; 20]).unwrap();
    let d_b = Sha1Hash::from_bytes(&[0xBB; 20]).unwrap();
    s.on_piece_verified(&ih, PieceIndex(0), false, &[d_a, d_b], &mut out);
    // first failure only records the CRCs — an ambiguous culprit
    // set means trust points, not a ban.
    assert!(!out.iter().any(|e| matches!(
        e,
        bitdaemon_session::session::SessionOut::Torrent(TorrentEffect::DisconnectPeer { .. })
    )));

    // second failure — block 0's digest differs while block 1's is
    // stable: peer 9 sent inconsistent data → `ban_peer` +
    // disconnect (upstream `on_read_failed_block` → `Ban`).
    finish(&mut s);
    let d_a2 = Sha1Hash::from_bytes(&[0xCC; 20]).unwrap();
    s.on_piece_verified(&ih, PieceIndex(0), false, &[d_a2, d_b], &mut out);
    assert!(
        out.iter().any(|e| matches!(
            e,
            bitdaemon_session::session::SessionOut::Torrent(TorrentEffect::DisconnectPeer {
                peer: 9,
                ..
            })
        )),
        "differing block digest must ban the downloader"
    );
    // conn 10's block stayed consistent — no ban for it.
    assert!(!out.iter().any(|e| matches!(
        e,
        bitdaemon_session::session::SessionOut::Torrent(TorrentEffect::DisconnectPeer {
            peer: 10,
            ..
        })
    )));
}
