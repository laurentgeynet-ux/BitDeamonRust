// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `peer_connection::dispatch` — incoming wire messages →
//! picker/disk effects.

use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::{PeerRequest, PieceBlock};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::peer_conn::{PeerConn, PeerEffect};
use bitdaemon_session::peer_pipeline::PeerPipeline;

fn conn() -> PeerConn {
    PeerConn::new(10, PeerPipeline::new(1000, 30, 250), 0)
}

#[test]
fn bitfield_then_have() {
    let mut c = conn();
    let mut eff = Vec::new();
    // bitfield of 10 pieces, peer has piece 0 and 1
    let bits = bytes::Bytes::from(vec![0b1100_0000u8, 0]);
    c.dispatch(
        &PeerMessage::Bitfield(bits),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(c.bitfield_received);
    assert_eq!(c.num_pieces, 2);
    assert!(matches!(eff[0], PeerEffect::RefcountBulk(_, true)));

    eff.clear();
    c.dispatch(
        &PeerMessage::Have(PieceIndex(2)),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(c.num_pieces, 3);
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::RefcountInc(i) if *i == PieceIndex(2)))
    );
}

#[test]
fn choke_aborts_unsent_requests() {
    let mut c = conn();
    c.add_request(
        PieceBlock {
            piece_index: PieceIndex(0),
            block_index: 0,
        },
        false,
        false,
    );
    c.add_request(
        PieceBlock {
            piece_index: PieceIndex(1),
            block_index: 0,
        },
        false,
        false,
    );
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Choke,
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(c.peer_choked);
    assert_eq!(eff.len(), 2, "both queued requests aborted: {eff:?}");
}

#[test]
fn piece_marks_download_and_requests_more() {
    let mut c = conn();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.add_request(b, false, false);
    // simulate the block was sent (moved to download queue by
    // send_block_requests) — push it directly
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from_static(&[1u8; 16]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::MarkDownloaded(_)))
    );
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::DiskWrite(..))));
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::MaybePick(_))));
}

#[test]
fn invalid_request_rejected_and_alerted() {
    let mut c = conn();
    let mut eff = Vec::new();
    let req = PeerRequest {
        piece: PieceIndex(99),
        start: 0,
        length: 0x4000,
    };
    c.dispatch(
        &PeerMessage::Request(req),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    // upstream `incoming_request`: a request from a not-interested
    // peer posts `invalid_request_alert` then fakes `interested`;
    // the out-of-range piece then gets `write_reject_request` +
    // another alert. num_invalid_requests += 1 — no disconnect
    // until >300 invalid requests while choked.
    assert!(matches!(eff[0], PeerEffect::InvalidRequest(_)));
    assert!(matches!(eff[1], PeerEffect::RejectRequest(_)));
    assert!(matches!(eff[2], PeerEffect::InvalidRequest(_)));
    assert!(c.peer_interested, "request fakes incoming_interested");
    assert_eq!(c.num_invalid_requests, 1);
    assert_eq!(eff.len(), 3);
}

#[test]
fn valid_request_queues_for_upload() {
    let mut c = conn();
    let mut eff = Vec::new();
    let r = PeerRequest {
        piece: PieceIndex(4),
        start: 0x4000,
        length: 0x4000,
    };
    // interested + unchoked peers take the accept path directly
    // (choked peers only get `accept_fast` pieces upstream).
    c.peer_interested = true;
    c.choked = false;
    c.dispatch(
        &PeerMessage::Request(r),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(c.upload_queue.len(), 1);
    assert_eq!(eff.len(), 1);
    assert!(matches!(eff[0], PeerEffect::ReadRequest(_)));
    // cancel removes it (+ the upstream `write_reject_request`)
    eff.clear();
    c.dispatch(
        &PeerMessage::Cancel(r),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(c.upload_queue.is_empty());
    assert!(matches!(eff[0], PeerEffect::CancelRequest(_)));
}

#[test]
fn choked_request_rejected_then_disconnects() {
    // upstream `incoming_request`: a choked peer may only request
    // `accept_fast` pieces; anything else is rejected, and past 2 s
    // since our last Choke the peer is dropped.
    let mut c = conn();
    c.peer_interested = true;
    let r = PeerRequest {
        piece: PieceIndex(4),
        start: 0,
        length: 0x4000,
    };
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Request(r),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(c.upload_queue.is_empty());
    assert!(matches!(eff[0], PeerEffect::RejectRequest(_)));
    assert!(!eff.iter().any(|e| matches!(e, PeerEffect::Disconnect(..))));

    // past the 2 s grace window the same request disconnects
    eff.clear();
    c.now_ms += 2500;
    c.dispatch(
        &PeerMessage::Request(r),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::Disconnect(..))));
}

#[test]
fn choked_accept_fast_request_served_with_budget() {
    // `accept_fast` pieces are servicable while choked — up to
    // `3 * blocks_per_piece` requests; beyond that upstream
    // disconnects the peer (piece_length 0x40000 / block 0x4000
    // → 16 blocks → budget 48).
    let mut c = conn();
    c.peer_interested = true;
    c.accept_fast.push(PieceIndex(4));
    c.accept_fast_piece_cnt.push(0);
    let r = PeerRequest {
        piece: PieceIndex(4),
        start: 0,
        length: 0x4000,
    };
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Request(r),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(c.upload_queue.len(), 1);
    assert_eq!(c.accept_fast_piece_cnt[0], 1);
    assert!(matches!(eff[0], PeerEffect::ReadRequest(_)));

    // exhaust the budget → disconnect
    c.accept_fast_piece_cnt[0] = 48;
    eff.clear();
    c.upload_queue.clear();
    c.dispatch(
        &PeerMessage::Request(r),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::Disconnect(..))));
    assert!(c.upload_queue.is_empty());
}

#[test]
fn request_queue_cap_rejects() {
    // `max_allowed_in_request_queue` — beyond it upstream rejects
    // without enqueueing.
    let mut c = conn();
    c.peer_interested = true;
    c.choked = false;
    let r = PeerRequest {
        piece: PieceIndex(4),
        start: 0,
        length: 0x4000,
    };
    c.upload_queue.push(r);
    c.upload_queue.push(r);
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Request(r),
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        1,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(c.upload_queue.len(), 2);
    assert!(matches!(eff[0], PeerEffect::RejectRequest(_)));
}

#[test]
fn request_before_metadata_rejected() {
    // `!valid_metadata` — plain reject, no interested fake-up.
    let mut c = conn();
    let r = PeerRequest {
        piece: PieceIndex(0),
        start: 0,
        length: 0x4000,
    };
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Request(r),
        0,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(eff.len(), 1);
    assert!(matches!(eff[0], PeerEffect::RejectRequest(_)));
    assert!(!c.peer_interested);
}

#[test]
fn oversized_piece_disconnects() {
    // `bt_peer_connection::on_piece` — a payload larger than
    // `t->block_size()` is `packet_too_large`: disconnect before any
    // `incoming_piece` state moves (no slow-start bump, no queue
    // bookkeeping, no bitfield inference).
    let mut c = conn();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from(vec![0u8; 0x4001]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(eff.len(), 1, "expected only the disconnect: {eff:?}");
    assert!(matches!(
        eff[0],
        PeerEffect::Disconnect("packet_too_large", true)
    ));
    assert_eq!(c.pipeline.download_queue.len(), 1);
    assert_eq!(c.pipeline.desired_queue_size, 4, "no slow-start bump");
    assert!(!c.bitfield_received);
}

#[test]
fn piece_matching_only_request_queue_is_unwanted() {
    // `incoming_piece` looks up `m_download_queue` only — a block
    // that matches a still-unsent `request_queue` entry is dropped
    // as `piece_unknown`; the request stays queued and goes out on
    // the wire later.
    let mut c = conn();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    assert!(c.add_request(b, false, false));
    assert_eq!(c.pipeline.request_queue.len(), 1);
    assert!(c.pipeline.download_queue.is_empty());
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from_static(&[1u8; 16]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(eff.len(), 1, "expected only UnwantedBlock: {eff:?}");
    assert!(matches!(eff[0], PeerEffect::UnwantedBlock(b2, 16) if b2 == b));
    // the unsent request survives — it will hit the wire later
    assert_eq!(c.pipeline.request_queue.len(), 1);
}

#[test]
fn seed_drops_piece_as_seed_waste() {
    // `incoming_piece` is_seed leg — payload received while we hold
    // every piece is `piece_seed` waste; one stale download-queue
    // front entry drops per message and nothing reaches disk, the
    // picker or the alert stream.
    let mut c = conn();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from_static(&[1u8; 16]),
        },
        10,
        0x4000,
        0x40000,
        None,
        true,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(eff.len(), 1, "expected only SeedWaste: {eff:?}");
    assert!(matches!(eff[0], PeerEffect::SeedWaste(16)));
    assert!(c.pipeline.download_queue.is_empty());
}

#[test]
fn empty_piece_still_bumps_slow_start() {
    // upstream order: the slow-start bump + `update_desired_queue_
    // size` run *before* the zero-length piece is treated as a
    // reject — a BitComet-style reject still grows the queue target.
    let mut c = conn();
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(0),
            offset: 0,
            data: bytes::Bytes::new(),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(c.pipeline.desired_queue_size, 5, "4 + 1 slow-start");
    assert!(matches!(eff[0], PeerEffect::PeerError(_)));
}

#[test]
fn piece_before_bitfield_runs_have_none() {
    // `incoming_piece` calls `incoming_have_none()` when the peer
    // omitted the bitfield — `m_has_metadata` is set through it
    // (a peer delivering torrent data provably holds metadata).
    let mut c = conn();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from_static(&[1u8; 16]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(c.bitfield_received);
    assert!(c.has_metadata);
    assert_eq!(c.num_pieces, 0);
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::DiskWrite(..))));
}
