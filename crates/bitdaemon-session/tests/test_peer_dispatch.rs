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
    // simulate the block was sent (moved to download queue by
    // send_block_requests) — push it directly. The payload is a
    // full block: `validate_piece_request` requires an exact
    // `to_req` length match upstream.
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from(vec![1u8; 0x4000]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
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
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(eff.len(), 1, "expected only the disconnect: {eff:?}");
    assert!(matches!(
        eff[0],
        PeerEffect::Disconnect(
            "packet_too_large",
            bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
        )
    ));
    assert_eq!(c.pipeline.download_queue.len(), 1);
    assert_eq!(c.pipeline.desired_queue_size, 4, "no slow-start bump");
    assert!(!c.bitfield_received);
}

#[test]
fn piece_matching_request_queue_migrates() {
    // `start_receive_piece` — a block still sitting in `request_queue`
    // (its wire request never went out) migrates into
    // `download_queue` when the payload arrives first: no
    // `unwanted_block_alert`, the entry is not `not_wanted`, and the
    // block follows the normal write path.
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
            data: bytes::Bytes::from(vec![1u8; 0x4000]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        !eff.iter().any(|e| matches!(
            e,
            PeerEffect::UnwantedBlock(..) | PeerEffect::UnwantedBlockAlert(_)
        )),
        "a migrated block is wanted: {eff:?}"
    );
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::DiskWrite(..))));
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::MarkDownloaded(_)))
    );
    // the migrated entry was consumed by `incoming_piece` — both
    // queues drain and the byte ledger nets to zero.
    assert!(c.pipeline.request_queue.is_empty());
    assert!(c.pipeline.download_queue.is_empty());
    assert_eq!(c.pipeline.outstanding_bytes, 0);
}

#[test]
fn unsolicited_piece_inserts_not_wanted_and_writes() {
    // `start_receive_piece` — a block unknown to *both* queues is
    // inserted `not_wanted` at the head of `download_queue`: the
    // `unwanted_block_alert` fires at insert time and the payload
    // still goes through the normal write path (upstream writes it —
    // only a picker-side `is_downloaded` consult skips the write).
    let mut c = conn();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from(vec![1u8; 0x4000]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        matches!(eff[0], PeerEffect::UnwantedBlockAlert(b2) if b2 == b),
        "alert at insert time: {eff:?}"
    );
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::DiskWrite(..))));
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::MarkDownloaded(_)))
    );
    // the transient not_wanted entry was consumed by `incoming_piece`.
    assert!(c.pipeline.download_queue.is_empty());
    assert_eq!(c.pipeline.outstanding_bytes, 0);
}

#[test]
fn seed_drops_piece_as_seed_waste() {
    // `incoming_piece` is_seed leg — payload received while we hold
    // every piece is `piece_seed` waste; one stale download-queue
    // front entry drops per message and nothing reaches disk, the
    // picker or the alert stream. `num_peers_down_requests` is NOT
    // touched here: the gauge reconciles once per batch through
    // `sync_down_requests`.
    use bitdaemon_session::counters::{Counter, Counters};
    let counters = Counters::default();
    let mut c = conn();
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Piece {
            piece: PieceIndex(3),
            offset: 0,
            data: bytes::Bytes::from(vec![1u8; 0x4000]),
        },
        10,
        0x4000,
        0x40000,
        None,
        true,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert_eq!(eff.len(), 1, "expected only SeedWaste: {eff:?}");
    assert!(matches!(eff[0], PeerEffect::SeedWaste(0x4000)));
    assert!(c.pipeline.download_queue.is_empty());
    assert_eq!(c.pipeline.outstanding_bytes, 0);
    assert_eq!(
        counters.value(Counter::NumPeersDownRequests),
        0,
        "no inline decrement — `sync_down_requests` reconciles"
    );
}

#[test]
fn empty_piece_disconnects_invalid_piece() {
    // `validate_piece_request` runs inside `start_receive_piece`
    // upstream — a zero-length `piece` fails the `to_req` match for
    // a BitTorrent peer and disconnects `invalid_piece` BEFORE
    // `incoming_piece` runs (no bitfield inference, no slow-start
    // bump). The `length == 0` BitComet reject quirk inside
    // `incoming_piece` is only reachable from `web_peer_connection`,
    // which bypasses `start_receive_piece`.
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
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(eff.len(), 1, "expected only the disconnect: {eff:?}");
    assert!(matches!(
        eff[0],
        PeerEffect::Disconnect(
            "invalid_piece",
            bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
        )
    ));
    assert_eq!(c.pipeline.desired_queue_size, 4, "no slow-start bump");
    assert!(!c.bitfield_received);
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
            data: bytes::Bytes::from(vec![1u8; 0x4000]),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
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

/// `incoming_request`/`incoming_cancel` session counters —
/// `piece_requests` counts every request wire message;
/// `invalid_piece_requests`/`max_piece_requests` count the reject
/// reasons; `cancelled_piece_requests` only when the request was
/// actually queued.
#[test]
fn request_counters() {
    use bitdaemon_session::counters::{Counter, Counters};
    let counters = Counters::default();
    let mut c = conn();
    // a fresh conn is choked — unchoke so requests actually queue.
    c.choked = false;
    let mut eff = Vec::new();
    let good = PeerRequest {
        piece: PieceIndex(0),
        start: 0,
        length: 0x4000,
    };
    // accepted request → queue + ReadRequest.
    c.dispatch(
        &PeerMessage::Request(good),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert_eq!(counters.value(Counter::PieceRequests), 1);
    assert_eq!(c.upload_queue.len(), 1);
    assert!(matches!(eff.last(), Some(PeerEffect::ReadRequest(_))));

    // out-of-range piece → invalid.
    eff.clear();
    c.dispatch(
        &PeerMessage::Request(PeerRequest {
            piece: PieceIndex(99),
            start: 0,
            length: 0x4000,
        }),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert_eq!(counters.value(Counter::PieceRequests), 2);
    assert_eq!(counters.value(Counter::InvalidPieceRequests), 1);
    assert!(matches!(eff.first(), Some(PeerEffect::RejectRequest(_))));

    // queue-full reject → max_piece_requests, no invalid bump.
    eff.clear();
    c.dispatch(
        &PeerMessage::Request(PeerRequest {
            piece: PieceIndex(1),
            start: 0,
            length: 0x4000,
        }),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        0, // max_in_queue = 0 → any queued request overflows
        &counters,
        &mut eff,
    );
    assert_eq!(counters.value(Counter::MaxPieceRequests), 1);
    assert_eq!(counters.value(Counter::InvalidPieceRequests), 1);
    assert!(matches!(eff.first(), Some(PeerEffect::RejectRequest(_))));

    // cancel the queued request → cancelled_piece_requests.
    eff.clear();
    c.dispatch(
        &PeerMessage::Cancel(good),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert_eq!(counters.value(Counter::CancelledPieceRequests), 1);
    assert!(c.upload_queue.is_empty());
    // a cancel that misses doesn't count.
    c.dispatch(
        &PeerMessage::Cancel(good),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert_eq!(counters.value(Counter::CancelledPieceRequests), 1);
}

#[test]
fn reject_aborts_and_frees_inflight_block() {
    let mut c = conn();
    // reject/allowed-fast are FAST-extension messages.
    c.supports_fast = true;
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::RejectRequest(PeerRequest {
            piece: PieceIndex(3),
            start: 0,
            length: 0x4000,
        }),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::AbortDownload(bb) if *bb == b)),
        "valid reject frees the picker mark: {eff:?}"
    );
    assert!(c.pipeline.download_queue.is_empty());
    assert_eq!(c.pipeline.outstanding_bytes, 0);
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::GracefulCheck)));
    // `incoming_reject_request` ends with an unconditional
    // `send_block_requests` — queued entries go on the wire now.
    assert!(
        eff.iter().any(|e| matches!(e, PeerEffect::FlushRequests)),
        "valid reject flushes the request queue: {eff:?}"
    );
}

/// `incoming_reject_request` validates the reject shape first — a
/// misaligned start / wrong length is a log line upstream, not a
/// state change (no queue mutation, no picker abort, no re-pick).
#[test]
fn malformed_reject_is_a_noop() {
    let mut c = conn();
    // reject/allowed-fast are FAST-extension messages.
    c.supports_fast = true;
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    // sub-block length — `min(piece_sz - start, block_size)` is
    // 0x4000 here, so 0x2000 fails validation.
    c.dispatch(
        &PeerMessage::RejectRequest(PeerRequest {
            piece: PieceIndex(3),
            start: 0,
            length: 0x2000,
        }),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(eff.is_empty(), "malformed reject touches nothing: {eff:?}");
    assert_eq!(c.pipeline.download_queue.len(), 1);
    assert_eq!(c.pipeline.outstanding_bytes, 0x4000);

    // out-of-range piece — same early return.
    c.dispatch(
        &PeerMessage::RejectRequest(PeerRequest {
            piece: PieceIndex(99),
            start: 0,
            length: 0x4000,
        }),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(eff.is_empty());
    assert_eq!(c.pipeline.download_queue.len(), 1);
}

/// `validate_piece_request` (inside `start_receive_piece` upstream):
/// out-of-range piece, misaligned start, or a length that isn't the
/// exact `to_req` tail-clamp all disconnect `invalid_piece` before
/// any queue/picker/disk state moves. Zero-length included — the
/// BitComet reject quirk inside `incoming_piece` is only reachable
/// from `web_peer_connection`, which bypasses the validator.
#[test]
fn invalid_piece_shapes_disconnect() {
    use bitdaemon_net::bt_peer_conn::DisconnectSeverity;
    for (piece, offset, len) in [
        (PieceIndex(99), 0, 0x4000usize), // out-of-range piece index
        (PieceIndex(3), 1, 0x4000),       // start not block-aligned
        (PieceIndex(3), 0x40000, 0x4000), // start past end of piece
        (PieceIndex(3), 0, 0x2000),       // sub-block length
        (PieceIndex(3), 0x4000, 0x2000),  // tail-clamped mismatch
        (PieceIndex(3), 0, 0),            // empty payload
    ] {
        let mut c = conn();
        c.supports_fast = true;
        let b = PieceBlock {
            piece_index: PieceIndex(3),
            block_index: 0,
        };
        c.pipeline
            .download_queue
            .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
        c.pipeline.outstanding_bytes = 0x4000;
        let mut eff = Vec::new();
        c.dispatch(
            &PeerMessage::Piece {
                piece,
                offset,
                data: bytes::Bytes::from(vec![1u8; len]),
            },
            10,
            0x4000,
            0x40000,
            None,
            false,
            false,
            0x200000,
            2000,
            &bitdaemon_session::counters::Counters::default(),
            &mut eff,
        );
        assert_eq!(
            eff.len(),
            1,
            "piece={piece:?} offset={offset:#x} len={len:#x}: {eff:?}"
        );
        assert!(
            matches!(
                eff[0],
                PeerEffect::Disconnect("invalid_piece", DisconnectSeverity::PeerError)
            ),
            "piece={piece:?} offset={offset:#x} len={len:#x}: {eff:?}"
        );
        assert_eq!(
            c.pipeline.download_queue.len(),
            1,
            "queue untouched on invalid piece"
        );
    }
}

/// `remove_from_picker = !timed_out && !not_wanted` — a block that
/// already timed out keeps its picker state (it was already freed
/// when it timed out); the reject just drops the queue entry.
#[test]
fn reject_of_timed_out_block_does_not_reabort() {
    let mut c = conn();
    // reject/allowed-fast are FAST-extension messages.
    c.supports_fast = true;
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    let mut pb = bitdaemon_session::peer_pipeline::PendingBlock::new(b);
    pb.timed_out = true;
    c.pipeline.download_queue.push(pb);
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::RejectRequest(PeerRequest {
            piece: PieceIndex(3),
            start: 0,
            length: 0x4000,
        }),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        !eff.iter()
            .any(|e| matches!(e, PeerEffect::AbortDownload(_))),
        "timed-out entry is not re-aborted upstream: {eff:?}"
    );
    assert!(c.pipeline.download_queue.is_empty());
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::GracefulCheck)));
}

/// Parole peers keep the request on reject — upstream re-inserts the
/// pending block at the head of `m_request_queue` instead of
/// aborting the picker mark.
#[test]
fn parole_reject_requeues_request() {
    let mut c = conn();
    // reject/allowed-fast are FAST-extension messages.
    c.supports_fast = true;
    c.on_parole = true;
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::RejectRequest(PeerRequest {
            piece: PieceIndex(3),
            start: 0,
            length: 0x4000,
        }),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        !eff.iter()
            .any(|e| matches!(e, PeerEffect::AbortDownload(_)))
    );
    assert!(c.pipeline.download_queue.is_empty());
    assert_eq!(
        c.pipeline.request_queue.first().map(|e| e.block),
        Some(b),
        "parole reject requeues at the front"
    );
}

/// BEP-6 gate — every FAST-extension message on a conn that never
/// negotiated it is `invalid_*` → `peer_error` disconnect upstream
/// (`on_reject_request`/`on_have_all`/`on_have_none`/`on_allowed_fast`/
/// `on_suggest_piece` all check `m_supports_fast` first).
#[test]
fn fast_messages_require_fast_negotiation() {
    use bitdaemon_net::bt_peer_conn::DisconnectSeverity;
    for msg in [
        PeerMessage::HaveAll,
        PeerMessage::HaveNone,
        PeerMessage::AllowedFast(PieceIndex(0)),
        PeerMessage::SuggestPiece(PieceIndex(0)),
        PeerMessage::RejectRequest(PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 0x4000,
        }),
    ] {
        let mut c = conn();
        let mut eff = Vec::new();
        c.dispatch(
            &msg,
            10,
            0x4000,
            0x40000,
            None,
            false,
            false,
            0x200000,
            2000,
            &bitdaemon_session::counters::Counters::default(),
            &mut eff,
        );
        assert!(
            matches!(
                eff.as_slice(),
                [PeerEffect::Disconnect(_, DisconnectSeverity::PeerError)]
            ),
            "{msg:?} without FAST must disconnect with peer_error: {eff:?}"
        );
    }
}

/// The same messages are legal once `supports_fast` is set (the
/// handshake reserved bit) — none of them disconnect.
#[test]
fn fast_messages_accepted_when_negotiated() {
    let mut c = conn();
    c.supports_fast = true;
    for msg in [
        PeerMessage::HaveAll,
        PeerMessage::HaveNone,
        PeerMessage::AllowedFast(PieceIndex(0)),
        PeerMessage::SuggestPiece(PieceIndex(0)),
    ] {
        let mut eff = Vec::new();
        c.dispatch(
            &msg,
            10,
            0x4000,
            0x40000,
            None,
            false,
            false,
            0x200000,
            2000,
            &bitdaemon_session::counters::Counters::default(),
            &mut eff,
        );
        assert!(
            !eff.iter().any(|e| matches!(e, PeerEffect::Disconnect(..))),
            "{msg:?} on a FAST conn must not disconnect: {eff:?}"
        );
    }
}

/// `on_choke` upstream: without FAST, a choke is an implicit reject of
/// every outstanding block — each download_queue entry goes through
/// `incoming_reject_request` (picker mark freed, bytes returned).
/// With FAST the queue just stalls.
#[test]
fn choke_without_fast_rejects_download_queue() {
    let mut c = conn();
    // FAST negotiated → queue stalls, no aborts.
    c.supports_fast = true;
    let b = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Choke,
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(
        c.pipeline.download_queue.len(),
        1,
        "FAST choke keeps the dlq entry"
    );
    // without FAST the same choke runs `incoming_reject_request` per
    // dlq entry — the block is aborted back to the picker.
    let mut c = conn();
    let b2 = PieceBlock {
        piece_index: PieceIndex(3),
        block_index: 0,
    };
    c.pipeline
        .download_queue
        .push(bitdaemon_session::peer_pipeline::PendingBlock::new(b2));
    c.pipeline.outstanding_bytes = 0x4000;
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Choke,
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        c.pipeline.download_queue.is_empty(),
        "non-FAST choke rejects the whole dlq"
    );
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::AbortDownload(bb) if *bb == b2)),
        "the implicit reject frees the picker mark: {eff:?}"
    );
    // each implicit `incoming_reject_request` carries its own tail:
    // graceful check, the gated re-pick (`dlq < 2` — true once the
    // single entry drained), and the batch ends with the
    // unconditional `send_block_requests` flush.
    assert!(
        eff.iter().any(|e| matches!(e, PeerEffect::GracefulCheck)),
        "implicit reject runs check_graceful_pause: {eff:?}"
    );
    assert!(
        eff.iter().any(|e| matches!(
            e,
            PeerEffect::MaybePick(Some(bitdaemon_session::counters::Counter::RejectPiecePicks))
        )),
        "implicit reject re-picks when nearly drained: {eff:?}"
    );
    assert!(
        eff.iter().any(|e| matches!(e, PeerEffect::FlushRequests)),
        "implicit reject flushes the request queue: {eff:?}"
    );
    assert_eq!(c.pipeline.outstanding_bytes, 0);
}

/// A09 — super-seeding: a request outside the leaked slots is
/// rejected *before* `incoming_have_none` runs (upstream order,
/// peer_connection.cpp:2408-2447) — `m_has_metadata` stays cleared
/// and the bitfield-omission leg does not fire.
#[test]
fn superseed_rejects_before_have_none() {
    let counters = bitdaemon_session::counters::Counters::default();
    let mut c = conn();
    c.supports_fast = true;
    // peer advertised ut_metadata with no size → has_metadata cleared.
    c.has_metadata = false;
    let r = bitdaemon_common::PeerRequest {
        piece: PieceIndex(1),
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
        true, // super_seeding — piece 1 not in superseed_pieces
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert!(c.upload_queue.is_empty());
    assert!(!c.bitfield_received, "have_none must not run");
    assert!(!c.has_metadata, "m_has_metadata must stay cleared");
    assert_eq!(c.num_invalid_requests, 1);
    assert_eq!(
        counters.value(bitdaemon_session::counters::Counter::InvalidPieceRequests),
        1
    );
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::RejectRequest(_)))
    );
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::InvalidRequest(_)))
    );
}

#[test]
fn oversized_bitfield_disconnects() {
    // upstream `on_bitfield` — with metadata the frame must be
    // exactly `ceil(num_pieces/8)` payload bytes; a longer bitfield
    // is `invalid_bitfield_size` (peer_error), not a truncation.
    let mut c = conn();
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Bitfield(bytes::Bytes::from(vec![0xff; 3])),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        eff.iter().any(|e| matches!(
            e,
            PeerEffect::Disconnect(
                "invalid bitfield size",
                bitdaemon_net::bt_peer_conn::DisconnectSeverity::PeerError
            )
        )),
        "3-byte bitfield for 10 pieces must disconnect: {eff:?}"
    );
    assert!(!c.bitfield_received);
}

#[test]
fn undersized_bitfield_disconnects() {
    // 1 byte < ceil(10/8) = 2 — `invalid_bitfield_size` upstream too.
    let mut c = conn();
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Bitfield(bytes::Bytes::from(vec![0xffu8])),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(
        eff.iter().any(|e| matches!(e, PeerEffect::Disconnect(..))),
        "short bitfield must disconnect: {eff:?}"
    );
}

#[test]
fn multiple_bitfields_recount_share() {
    // `test_fast_extension.cpp::multiple_bitfields` — a repeat
    // bitfield drops the peer's previous share (`peer_lost`) before
    // the new one is counted.
    let mut c = conn();
    let counters = bitdaemon_session::counters::Counters::default();
    let mut eff = Vec::new();
    let first = bytes::Bytes::from(vec![0b1100_0000u8, 0]);
    c.dispatch(
        &PeerMessage::Bitfield(first),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    eff.clear();
    let second = bytes::Bytes::from(vec![0b0011_0000u8, 0]);
    c.dispatch(
        &PeerMessage::Bitfield(second),
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    let dec = eff
        .iter()
        .position(|e| matches!(e, PeerEffect::RefcountBulk(_, false)));
    let inc = eff
        .iter()
        .position(|e| matches!(e, PeerEffect::RefcountBulk(_, true)));
    assert!(
        dec.is_some() && inc.is_some() && dec < inc,
        "old share leaves the refcount before the new one lands: {eff:?}"
    );
    assert_eq!(c.num_pieces, 2, "pieces 2 and 3");
}

#[test]
fn ext_handshake_counts_only_after_parse() {
    // upstream bumps `num_incoming_ext_handshake` at the tail of
    // `on_extended_handshake` — a bdecode-invalid payload on ext id 0
    // is swallowed without counting.
    let mut c = conn();
    let counters = bitdaemon_session::counters::Counters::default();
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Extended {
            id: 0,
            data: bytes::Bytes::from_static(b"not-bencode"),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert_eq!(
        counters.value(bitdaemon_session::counters::Counter::NumIncomingExtHandshake),
        0,
        "unparseable handshake is not counted"
    );
    // `ExtendedHandshake::encode` emits the full wire frame —
    // dispatch sees only the bencoded body (bytes after `[0x14][0x00]`).
    let hs = bitdaemon_net::bep10::ExtendedHandshake::default();
    let frame = hs.encode();
    c.dispatch(
        &PeerMessage::Extended {
            id: 0,
            data: bytes::Bytes::from(frame[6..].to_vec()),
        },
        10,
        0x4000,
        0x40000,
        None,
        false,
        false,
        0x200000,
        2000,
        &counters,
        &mut eff,
    );
    assert_eq!(
        counters.value(bitdaemon_session::counters::Counter::NumIncomingExtHandshake),
        1,
        "a parsed handshake counts once"
    );
}
