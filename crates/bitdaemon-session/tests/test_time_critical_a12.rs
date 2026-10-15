// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Time-critical pipeline legs (audit A12): deferred
//! `cancel_non_critical`, deadline-sorted `request_time_critical_pieces`
//! with stall re-picks and peer filtering, canceled-deadline alerts,
//! pause survival, and the priority-0 sweep.

use bitdaemon_common::types::{DownloadPriority, PieceBlock, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::counters::Counters;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::{PeerPipeline, PendingBlock};
use bitdaemon_session::piece_picker::PickerOptions;
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::torrent::{Torrent, TorrentEffect};
use std::time::{Duration, Instant};

fn ih(b: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[b; 20]).unwrap()
}

fn mk_torrent(pieces: i32) -> Torrent {
    let mut have = Bitfield::new();
    have.resize_with(pieces as usize, false);
    Torrent::new(ih(1), pieces, 0x40000, have, StorageIndex(0))
}

/// A connected, unchoked, interesting peer holding every piece.
fn conn(pieces: usize) -> PeerConn {
    let mut c = PeerConn::new(pieces, PeerPipeline::new(1000, 30, 250), 0);
    c.connecting = false;
    c.handshake_done = true;
    c.interesting = true;
    c.peer_choked = false;
    for i in 0..pieces {
        c.peer_pieces.set_bit(i);
    }
    c
}

fn blk(piece: i32, block: i32) -> PieceBlock {
    PieceBlock {
        piece_index: PieceIndex(piece),
        block_index: block,
    }
}

fn settings() -> SessionSettings {
    SessionSettings::default()
}

fn is_cancel_for(e: &TorrentEffect, piece: i32) -> bool {
    matches!(
        e,
        TorrentEffect::SendTo {
            msg: PeerMessage::Cancel(r),
            ..
        } if r.piece == PieceIndex(piece)
    )
}

fn is_request_for(e: &TorrentEffect, peer: u64, piece: i32) -> bool {
    matches!(
        e,
        TorrentEffect::SendTo {
            peer: p,
            msg: PeerMessage::Request(r),
        } if *p == peer && r.piece == PieceIndex(piece)
    )
}

/// `set_piece_deadline` defers `cancel_non_critical` (upstream
/// `post()`): a second deadline declared before the sweep keeps its
/// in-flight blocks alive — only pieces outside the *final* critical
/// set are cancelled.
#[test]
fn cancel_non_critical_is_deferred() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let mut c = conn(4);
    // in-flight blocks on piece 2 (will become critical) and piece 3
    // (non-critical) — both tracked by the picker.
    for piece in [2, 3] {
        let b = blk(piece, 0);
        t.picker.as_mut().unwrap().mark_as_downloading(
            b,
            c.peer_key(),
            PickerOptions::RAREST_FIRST,
        );
        c.pipeline.download_queue.push(PendingBlock::new(b));
        c.pipeline.outstanding_bytes += 0x4000;
    }
    t.add_peer(1, c);

    let counters = Counters::default();
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, false, &counters, &mut eff);
    // first deadline → the sweep is armed, not run: no Cancel yet.
    assert!(t.pending_cancel_non_critical);
    assert!(!eff.iter().any(|e| is_cancel_for(e, 3)));

    t.set_piece_deadline(PieceIndex(2), 5000, false, &counters, &mut eff);
    assert!(
        !eff.iter()
            .any(|e| is_cancel_for(e, 2) || is_cancel_for(e, 3))
    );

    // the deferred sweep runs at the next tick.
    t.request_time_critical_pieces(&settings(), &counters, &mut eff);
    assert!(!t.pending_cancel_non_critical);
    assert!(eff.iter().any(|e| is_cancel_for(e, 3)));
    // piece 2 became critical before the sweep → its in-flight block
    // is left alone (upstream cancels only non-critical pieces).
    assert!(!eff.iter().any(|e| is_cancel_for(e, 2)));
    let c = &t.peers[&1];
    let p3 = c
        .pipeline
        .download_queue
        .iter()
        .find(|b| b.block.piece_index == PieceIndex(3))
        .unwrap();
    assert!(p3.not_wanted);
}

/// A stalled critical piece (every block requested, none arriving)
/// re-picks busy blocks from the fastest peer — upstream
/// `pick_time_critical_block`'s `busy` mode.
#[test]
fn stalled_critical_piece_repicks_busy_blocks() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let counters = Counters::default();

    // peer 1 holds every in-flight block of piece 1; peer 2 is idle.
    let mut c1 = conn(4);
    let p = t.picker.as_mut().unwrap();
    let nblocks = p.blocks_in_piece(PieceIndex(1));
    for i in 0..nblocks {
        let b = blk(1, i);
        p.mark_as_downloading(b, c1.peer_key(), PickerOptions::RAREST_FIRST);
        c1.pipeline.download_queue.push(PendingBlock::new(b));
        c1.pipeline.outstanding_bytes += 0x4000;
    }
    t.add_peer(1, c1);
    t.add_peer(2, conn(4));

    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, false, &counters, &mut eff);

    // simulate a stall: last pick long ago vs a short average piece
    // time → timed_out > 0 → busy re-pick allowed.
    t.average_piece_time_ms = 100;
    t.piece_time_deviation_ms = 0;
    t.time_critical
        .get_mut(&PieceIndex(1))
        .unwrap()
        .last_requested = Some(Instant::now() - Duration::from_secs(60));

    t.request_time_critical_pieces(&settings(), &counters, &mut eff);
    assert!(
        eff.iter().any(|e| is_request_for(e, 2, 1)),
        "expected a busy re-pick wired to peer 2, got {eff:?}"
    );
    // the busy re-pick was front-inserted then drained to the wire —
    // post-commit it sits in the download queue flagged `busy`.
    assert_eq!(t.peers[&2].pipeline.queued_time_critical, 0);
    assert_eq!(t.peers[&2].pipeline.download_queue.len(), 1);
    assert!(t.peers[&2].pipeline.download_queue.iter().all(|b| b.busy));
}

/// Choked peers are not candidates for time-critical requests
/// (`can_request_time_critical` — `has_peer_choked` leg).
#[test]
fn choked_peer_is_not_a_critical_candidate() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let mut c = conn(4);
    c.peer_choked = true;
    t.add_peer(1, c);
    let counters = Counters::default();
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, false, &counters, &mut eff);
    t.request_time_critical_pieces(&settings(), &counters, &mut eff);
    assert!(!eff.iter().any(|e| matches!(
        e,
        TorrentEffect::SendTo {
            msg: PeerMessage::Request(_),
            ..
        }
    )));
}

/// `prioritize_pieces` → `dont_download` drops the deadline as
/// canceled (`remove_time_critical_pieces`): alert posted, entry
/// gone, no priority restore.
#[test]
fn filtered_piece_loses_its_deadline() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let counters = Counters::default();
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(2), 5000, true, &counters, &mut eff);
    assert!(t.time_critical.contains_key(&PieceIndex(2)));

    let prios = vec![DownloadPriority::DONT_DOWNLOAD; 4];
    let mut eff = Vec::new();
    t.prioritize_pieces(&prios, &counters, &mut eff);
    assert!(!t.time_critical.contains_key(&PieceIndex(2)));
    assert!(eff.iter().any(|e| matches!(
        e,
        TorrentEffect::ReadPieceFailed {
            piece: PieceIndex(2),
            ..
        }
    )));
    assert_eq!(
        t.piece_priority(PieceIndex(2)),
        Some(DownloadPriority::DONT_DOWNLOAD)
    );
}

/// `clear_time_critical` posts `operation_canceled` for flagged
/// entries and drops priorities back to `low_priority`
/// (`torrent::clear_time_critical`).
#[test]
fn clear_deadlines_posts_canceled_alerts() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let counters = Counters::default();
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, true, &counters, &mut eff);
    t.set_piece_deadline(PieceIndex(2), 5000, false, &counters, &mut eff);

    let mut eff = Vec::new();
    t.clear_time_critical(&mut eff);
    assert!(t.time_critical.is_empty());
    assert!(eff.iter().any(|e| matches!(
        e,
        TorrentEffect::ReadPieceFailed {
            piece: PieceIndex(1),
            ..
        }
    )));
    // unflagged entries post nothing.
    assert!(!eff.iter().any(|e| matches!(
        e,
        TorrentEffect::ReadPieceFailed {
            piece: PieceIndex(2),
            ..
        }
    )));
    assert_eq!(t.piece_priority(PieceIndex(1)), Some(DownloadPriority::LOW));
    assert_eq!(t.piece_priority(PieceIndex(2)), Some(DownloadPriority::LOW));
}

/// Upstream `do_pause` never touches `m_time_critical_pieces` —
/// deadlines survive a pause.
#[test]
fn pause_keeps_deadlines() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let counters = Counters::default();
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, true, &counters, &mut eff);
    t.on_torrent_paused();
    assert!(t.time_critical.contains_key(&PieceIndex(1)));
}

/// `cancel_request`'s head gate: a block the picker no longer tracks
/// (`!is_requested` — e.g. it arrived via another peer) is skipped by
/// the sweep — no `not_wanted`, no Cancel.
#[test]
fn cancel_skips_untracked_blocks() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let mut c = conn(4);
    // in-flight block on piece 3 never marked in the picker.
    c.pipeline.download_queue.push(PendingBlock::new(blk(3, 0)));
    c.pipeline.outstanding_bytes = 0x4000;
    t.add_peer(1, c);

    let counters = Counters::default();
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, false, &counters, &mut eff);
    t.request_time_critical_pieces(&settings(), &counters, &mut eff);
    assert!(!eff.iter().any(|e| is_cancel_for(e, 3)));
    assert!(!t.peers[&1].pipeline.download_queue[0].not_wanted);
}

/// Completing a critical piece feeds `m_average_piece_time` /
/// `m_piece_time_deviation` (upstream updates the EWMA inside
/// `remove_time_critical_piece` when `first_requested` is set).
#[test]
fn finished_critical_piece_updates_stats() {
    let mut t = mk_torrent(4);
    t.init_picker(false);
    let counters = Counters::default();
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, false, &counters, &mut eff);
    t.time_critical
        .get_mut(&PieceIndex(1))
        .unwrap()
        .first_requested = Some(Instant::now() - Duration::from_millis(500));

    let mut eff = Vec::new();
    t.remove_time_critical_piece(PieceIndex(1), true, &mut eff);
    assert!(t.average_piece_time_ms >= 500);
}
