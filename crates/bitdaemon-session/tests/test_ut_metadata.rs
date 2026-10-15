// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `test_ut_metadata.cpp` — BEP-9 wire format + tracker state.

use bitdaemon_session::ut_metadata::{
    MetadataMsg, MetadataParse, MetadataResult, UtMetadataTracker, metadata_packet,
    parse_metadata_packet,
};

/// unwrap the `Msg` variant — `(type, piece, total_size, payload)`.
fn parse_msg(body: &[u8]) -> (MetadataMsg, i64, i64, &[u8]) {
    match parse_metadata_packet(body) {
        MetadataParse::Msg(ty, piece, ts, payload) => (ty, piece, ts, payload),
        _ => panic!("expected a parsed message"),
    }
}

#[test]
fn packet_round_trip() {
    // request
    let body = metadata_packet(MetadataMsg::Request, 2, false, b"");
    let (ty, piece, _ts, payload) = parse_msg(&body);
    assert_eq!(ty, MetadataMsg::Request);
    assert_eq!(piece, 2);
    assert!(payload.is_empty());

    // reject
    let body = metadata_packet(MetadataMsg::Reject, 0, false, b"");
    let (ty, piece, _, _) = parse_msg(&body);
    assert_eq!(ty, MetadataMsg::Reject);
    assert_eq!(piece, 0);
}

#[test]
fn parse_verdicts() {
    // non-dict → Malformed (upstream `invalid_metadata_message`)
    assert!(matches!(
        parse_metadata_packet(b"i42e"),
        MetadataParse::Malformed
    ));
    // dict missing `piece` → Malformed
    assert!(matches!(
        parse_metadata_packet(b"d8:msg_typei1ee"),
        MetadataParse::Malformed
    ));
    // out-of-range msg_type → UnknownType (consumed, counted)
    assert!(matches!(
        parse_metadata_packet(b"d8:msg_typei9e5:piecei0ee"),
        MetadataParse::UnknownType
    ));
    // a `piece` packet's payload starts right after the dict
    let body = metadata_packet(MetadataMsg::Piece, 0, true, &[9u8; 100]);
    match parse_metadata_packet(&body) {
        MetadataParse::Msg(ty, piece, _, payload) => {
            assert_eq!(ty, MetadataMsg::Piece);
            assert_eq!(piece, 0);
            assert!(payload.iter().all(|&b| b == 9));
        }
        _ => panic!("expected piece"),
    }
}

#[test]
fn piece_packet_carries_payload_and_total_size() {
    // 20 KiB metadata — piece 0 carries the first 16 KiB, piece 1 the
    // remaining 4 KiB (upstream clips at metadata end).
    let metadata = vec![7u8; 20 * 1024];
    let body = metadata_packet(MetadataMsg::Piece, 0, true, &metadata);
    let (ty, piece, ts, payload) = parse_msg(&body);
    assert_eq!(ty, MetadataMsg::Piece);
    assert_eq!(piece, 0);
    assert_eq!(ts, 20 * 1024);
    assert_eq!(payload.len(), 16 * 1024);
    assert_eq!(payload, &metadata[..16 * 1024]);

    let body = metadata_packet(MetadataMsg::Piece, 1, true, &metadata);
    let (_, _, _, payload) = parse_msg(&body);
    assert_eq!(payload.len(), 4 * 1024);
}

#[test]
fn tracker_full_download() {
    let mut tr = UtMetadataTracker::new();
    let alive = |_| true;
    // no size known → asks piece 0
    assert_eq!(tr.metadata_request(true, alive), 0);
    let metadata: Vec<u8> = (0..20 * 1024).map(|i| (i % 256) as u8).collect();
    assert_eq!(
        tr.received_metadata(
            &metadata[..16 * 1024],
            0,
            20 * 1024,
            30 * 1024 * 1024,
            false,
        ),
        MetadataResult::Stored
    );
    // now two pieces known; piece 0 is had → asks piece 1
    assert_eq!(tr.metadata_request(true, |_| true), 1);
    assert_eq!(
        tr.received_metadata(
            &metadata[16 * 1024..],
            1,
            20 * 1024,
            30 * 1024 * 1024,
            false,
        ),
        MetadataResult::Complete
    );
    assert_eq!(tr.metadata(), &metadata[..]);
}

#[test]
fn tracker_rejects_bad_input() {
    let mut tr = UtMetadataTracker::new();
    // oversized
    assert_eq!(
        tr.received_metadata(b"x", 0, 40 * 1024 * 1024, 30 * 1024 * 1024, false),
        MetadataResult::TooBig
    );
    // inconsistent total_size across pieces (40 KiB → 3 pieces)
    assert_eq!(
        tr.received_metadata(b"ab", 0, 40 * 1024, 30 * 1024 * 1024, false),
        MetadataResult::Stored
    );
    assert_eq!(
        tr.received_metadata(b"cd", 1, 99, 30 * 1024 * 1024, false),
        MetadataResult::InconsistentSize
    );
    // payload overflowing total_size (piece 2 starts at 32 KiB)
    assert_eq!(
        tr.received_metadata(&vec![0u8; 20 * 1024], 2, 40 * 1024, 30 * 1024 * 1024, false),
        MetadataResult::InvalidPiece
    );
    // already have metadata → redundant
    assert_eq!(
        tr.received_metadata(b"ab", 0, 2, 1024, true),
        MetadataResult::Redundant
    );
}

#[test]
fn tracker_hash_failure_reset() {
    let mut tr = UtMetadataTracker::new();
    tr.received_metadata(b"ab", 0, 2, 1024, false);
    tr.set_source(0, 42);
    let (single, penalized) = tr.reset_on_hash_failure();
    assert!(single);
    assert_eq!(penalized, vec![42]);
    // buffer kept (upstream only resets num_requests), piece 0 can be
    // re-requested from a different peer
    assert!(!tr.metadata().is_empty());
    assert_eq!(tr.metadata_request(true, |_| false), 0);
    // successful completion frees the buffers
    tr.received_metadata(b"ab", 0, 2, 1024, false);
    tr.clear();
    assert!(tr.metadata().is_empty());
}

#[test]
fn request_throttling() {
    let mut tr = UtMetadataTracker::new();
    // a recorded live source throttles re-requests for 3 s
    // (upstream: last_request set + source not disconnecting)
    assert_eq!(tr.metadata_request(true, |_| true), 0);
    tr.set_source(0, 42);
    assert_eq!(tr.metadata_request(true, |_| true), -1);
    // source disconnected → throttle lifted
    assert_eq!(tr.metadata_request(true, |_| false), 0);
}
