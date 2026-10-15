// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `incoming_piece`/`second_tick` download-path legs (audit A08):
//! the `reading_socket` gate on the request-timeout leg, the
//! write-path-only un-snub + `check_graceful_pause`, and the
//! `num_peers_down_requests` snapshot reconciliation.

use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, PieceBlock, Sha1Hash};
use bitdaemon_net::peer_class::PeerClassPool;
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::alerts::{Alert, AlertCategory};
use bitdaemon_session::counters::{Counter, Counters};
use bitdaemon_session::peer_conn::{DOWNLOAD_CHANNEL, PeerConn, PeerEffect};
use bitdaemon_session::peer_info::BandwidthState;
use bitdaemon_session::peer_pipeline::{PeerPipeline, PendingBlock};
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::torrent::{Torrent, TorrentEffect};
use std::sync::atomic::Ordering::Relaxed;

fn ih(b: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[b; 20]).unwrap()
}

fn mk_torrent(pieces: i32) -> Torrent {
    let mut have = Bitfield::new();
    have.resize_with(pieces as usize, false);
    Torrent::new(ih(1), pieces, 0x40000, have, StorageIndex(0))
}

fn conn(pieces: usize) -> PeerConn {
    PeerConn::new(pieces, PeerPipeline::new(1000, 30, 250), 0)
}

fn full_piece(piece: i32) -> PeerMessage {
    PeerMessage::Piece {
        piece: PieceIndex(piece),
        offset: 0,
        data: bytes::Bytes::from(vec![1u8; 0x4000]),
    }
}

/// `second_tick` — the *request* timeout only fires while a socket
/// read is in flight (`m_channel_state[download_channel] &
/// bw_network` upstream): a conn stalled on disk or quota can't be
/// blamed for missing replies. The piece-timeout leg carries no
/// such gate.
#[test]
fn request_timeout_requires_reading_socket() {
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let mut c = conn(4);
    c.connecting = false;
    c.handshake_done = true;
    c.interesting = true;
    let b = PieceBlock {
        piece_index: PieceIndex(1),
        block_index: 0,
    };
    c.pipeline.download_queue.push(PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    // stale request timer (request_timeout setting = 30 s), fresh
    // piece/activity stamps so only the request leg can fire.
    c.pipeline.requested_ms = 1;
    c.pipeline.last_piece_ms = 40_000;
    c.last_receive_ms = 40_000;
    c.last_sent_ms = 40_000;
    c.now_ms = 40_000;
    c.bw.quota[DOWNLOAD_CHANNEL].store(1, Relaxed);
    t.now_ms = 40_000;
    t.add_peer(1, c);

    let s = SessionSettings::default();
    let counters = Counters::default();
    let pool = PeerClassPool::new();
    let mut out = Vec::new();
    // channel_state = bw_idle → not reading → no snub.
    t.check_request_timeouts(&s, 1, &counters, &pool, AlertCategory::ALL, &mut out);
    assert!(
        !t.peers[&1].pipeline.snubbed,
        "a conn that isn't reading can't be request-timed-out"
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::Alert(Alert::PeerSnubbed { .. }))),
        "no peer_snubbed alert while idle: {out:?}"
    );

    // a disk-blocked conn isn't reading either — same exemption.
    t.peers.get_mut(&1).unwrap().bw.channel_state[DOWNLOAD_CHANNEL]
        .store(BandwidthState::DISK.0, Relaxed);
    out.clear();
    t.check_request_timeouts(&s, 1, &counters, &pool, AlertCategory::ALL, &mut out);
    assert!(!t.peers[&1].pipeline.snubbed);

    // socket read in flight → the same staleness snubs the peer.
    t.peers.get_mut(&1).unwrap().bw.channel_state[DOWNLOAD_CHANNEL]
        .store(BandwidthState::NETWORK.0, Relaxed);
    out.clear();
    t.check_request_timeouts(&s, 1, &counters, &pool, AlertCategory::ALL, &mut out);
    assert!(t.peers[&1].pipeline.snubbed);
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::Alert(Alert::PeerSnubbed { .. }))),
        "peer_snubbed alert once the read-side times out: {out:?}"
    );
}

/// `incoming_piece` — the `is_downloaded` arm returns *before* the
/// un-snub check and before `check_graceful_pause`: a redundant block
/// under graceful pause neither clears the snub nor disconnects.
#[test]
fn redundant_piece_keeps_snub_and_skips_graceful() {
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.graceful_pause = true;
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    if let Some(p) = t.picker.as_mut() {
        p.mark_as_finished(b, None);
    }
    let mut c = conn(4);
    c.pipeline.snubbed = true;
    c.pipeline.download_queue.push(PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    c.dispatch(
        &full_piece(3),
        4,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &Counters::default(),
        &mut eff,
    );
    // conn-side the block looked timely → the deferred Unsnub marker
    // is in the batch; the apply side must drop it on the redundant
    // path.
    assert!(
        eff.iter().any(|e| matches!(e, PeerEffect::Unsnub)),
        "window evaluated at receipt: {eff:?}"
    );
    t.add_peer(1, c);
    let mut out = Vec::new();
    t.apply_peer_effects(
        1,
        eff,
        &SessionSettings::default(),
        &Counters::default(),
        &mut out,
    );
    assert!(
        t.peers[&1].pipeline.snubbed,
        "redundant blocks don't clear the snub upstream"
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::Alert(Alert::PeerUnsnubbed { .. }))),
        "no peer_unsnubbed alert on the redundant path: {out:?}"
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. })),
        "upstream skips check_graceful_pause on the redundant path: {out:?}"
    );
    assert_eq!(
        t.redundant_bytes, 0x4000,
        "redundant payload is waste-counted"
    );
}

/// Companion: the same timely arrival on a *live* write path clears
/// the snub and runs `check_graceful_pause` (drained outstanding +
/// graceful pause → `torrent_paused` disconnect).
#[test]
fn live_piece_unsnubs_and_runs_graceful_check() {
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.graceful_pause = true;
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    let mut c = conn(4);
    c.pipeline.snubbed = true;
    c.pipeline.download_queue.push(PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    c.dispatch(
        &full_piece(3),
        4,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &Counters::default(),
        &mut eff,
    );
    t.add_peer(1, c);
    let mut out = Vec::new();
    t.apply_peer_effects(
        1,
        eff,
        &SessionSettings::default(),
        &Counters::default(),
        &mut out,
    );
    assert!(
        !t.peers[&1].pipeline.snubbed,
        "a timely block on the write path un-snubs the peer"
    );
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::Alert(Alert::PeerUnsnubbed { .. }))),
        "peer_unsnubbed alert on the write path: {out:?}"
    );
    assert!(
        out.iter().any(|e| matches!(
            e,
            TorrentEffect::DisconnectPeer {
                reason: "torrent_paused",
                ..
            }
        )),
        "drained outstanding under graceful pause disconnects: {out:?}"
    );
}

/// `num_peers_down_requests` moves only through
/// `sync_down_requests` — the seed arm's dlq pop leaves the gauge
/// untouched until the route boundary reconciles it once.
#[test]
fn down_requests_gauge_reconciles_once() {
    let mut t = mk_torrent(4);
    let counters = Counters::default();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    let mut c = conn(4);
    c.pipeline.download_queue.push(PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    t.add_peer(1, c);
    t.sync_down_requests(&counters);
    assert_eq!(counters.value(Counter::NumPeersDownRequests), 1);

    // a `piece` arriving while we seed pops the stale dlq front —
    // the gauge is NOT touched inline.
    let mut eff = Vec::new();
    t.peers.get_mut(&1).unwrap().dispatch(
        &full_piece(3),
        4,
        0x4000,
        0x40000,
        None,
        true,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert!(t.peers[&1].pipeline.download_queue.is_empty());
    assert_eq!(
        counters.value(Counter::NumPeersDownRequests),
        1,
        "no inline decrement in the seed arm"
    );
    t.sync_down_requests(&counters);
    assert_eq!(counters.value(Counter::NumPeersDownRequests), 0);
}
