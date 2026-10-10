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
    c.dispatch(&PeerMessage::Bitfield(bits), 10, &mut eff);
    assert!(c.bitfield_received);
    assert_eq!(c.num_pieces, 2);
    assert!(matches!(eff[0], PeerEffect::RefcountBulk(_, true)));

    eff.clear();
    c.dispatch(&PeerMessage::Have(PieceIndex(2)), 10, &mut eff);
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
    c.dispatch(&PeerMessage::Choke, 10, &mut eff);
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
        &mut eff,
    );
    assert!(
        eff.iter()
            .any(|e| matches!(e, PeerEffect::MarkDownloaded(_)))
    );
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::DiskWrite(..))));
    assert!(eff.iter().any(|e| matches!(e, PeerEffect::MaybePick)));
}

#[test]
fn invalid_request_disconnects() {
    let mut c = conn();
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Request(PeerRequest {
            piece: PieceIndex(99),
            start: 0,
            length: 0x4000,
        }),
        10,
        &mut eff,
    );
    assert!(matches!(eff[0], PeerEffect::Disconnect(_)));
    assert_eq!(c.num_invalid_requests, 1);
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
    c.dispatch(&PeerMessage::Request(r), 10, &mut eff);
    assert_eq!(c.upload_queue.len(), 1);
    assert!(matches!(eff[0], PeerEffect::ReadRequest(_)));
    // cancel removes it
    eff.clear();
    c.dispatch(&PeerMessage::Cancel(r), 10, &mut eff);
    assert!(c.upload_queue.is_empty());
    assert!(matches!(eff[0], PeerEffect::CancelRequest(_)));
}
