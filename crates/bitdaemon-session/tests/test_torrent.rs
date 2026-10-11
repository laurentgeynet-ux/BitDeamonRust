// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `torrent` — peer glue: refcount, dispatch effects, verified
//! piece broadcast, disconnect cleanup.

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::torrent::{Torrent, TorrentEffect};

fn torrent(pieces: i32) -> Torrent {
    let mut have = Bitfield::new();
    have.resize_with(pieces as usize, false);
    Torrent::new(
        Sha1Hash::from_bytes(&[7u8; 20]).unwrap(),
        pieces,
        0x40000,
        have,
        StorageIndex(0),
    )
}

#[test]
fn peer_bitfield_refcounts_picker() {
    let mut t = torrent(8);
    t.init_picker(true);
    let mut c = PeerConn::new(8, PeerPipeline::new(1000, 30, 250), 0);
    let mut eff = Vec::new();
    c.dispatch(
        &PeerMessage::Bitfield(bytes::Bytes::from(vec![0b1100_0000u8])),
        8,
        &mut eff,
    );
    t.add_peer(1, c);
    let mut out = Vec::new();
    t.apply_peer_effects(1, eff, &mut out);
    // pieces 0,1 now have refcount 1
    let p = t.picker.as_ref().unwrap();
    assert_eq!(p.piece_stats(PieceIndex(0)).peer_count, 1);
    assert_eq!(p.piece_stats(PieceIndex(5)).peer_count, 0);

    // disconnect → refcount back to 0
    let mut eff2 = Vec::new();
    t.remove_peer(1, &mut eff2);
    assert_eq!(
        t.picker
            .as_ref()
            .unwrap()
            .piece_stats(PieceIndex(0))
            .peer_count,
        0
    );
}

#[test]
fn piece_verified_broadcasts_have() {
    let mut t = torrent(4);
    t.init_picker(true);
    t.add_peer(1, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));
    t.add_peer(2, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));
    let mut out = Vec::new();
    t.piece_verified(PieceIndex(0), true, &mut out);
    // PiecePassed + have to both peers
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::PiecePassed(_)))
    );
    let haves = out
        .iter()
        .filter(|e| {
            matches!(
                e,
                TorrentEffect::SendTo {
                    msg: PeerMessage::Have(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(haves, 2);
}

#[test]
fn all_pieces_makes_seed() {
    let mut t = torrent(2);
    t.init_picker(true);
    let mut out = Vec::new();
    t.piece_verified(PieceIndex(0), true, &mut out);
    assert!(!t.finished);
    t.piece_verified(PieceIndex(1), true, &mut out);
    assert!(t.finished && t.is_seed());
    assert!(out.iter().any(|e| matches!(e, TorrentEffect::BecameSeed)));
}

/// regression: rates must never go negative when a peer with
/// accumulated totals disconnects between ticks (aggregate
/// subtraction used to produce a negative delta).
#[test]
fn rates_stay_nonnegative_on_disconnect() {
    let mut t = torrent(4);
    t.init_picker(true);
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.stat_down_total = 1_000_000;
    c.stat_up_total = 500_000;
    t.add_peer(1, c);
    let t0 = std::time::Instant::now();
    t.update_rates(t0);
    // peer leaves between ticks — its totals vanish from the sum
    let mut eff = Vec::new();
    t.remove_peer(1, &mut eff);
    t.update_rates(t0 + std::time::Duration::from_secs(1));
    assert!(t.down_rate >= 0 && t.up_rate >= 0, "rates went negative");
    assert_eq!(t.down_rate, 0);
    assert_eq!(t.up_rate, 0);
}

#[test]
fn failed_piece_restores() {
    let mut t = torrent(3);
    t.init_picker(true);
    let mut out = Vec::new();
    t.piece_verified(PieceIndex(1), false, &mut out);
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::PieceFailed(i) if *i == PieceIndex(1)))
    );
    // piece back in open queue — pickable again
    assert!(!t.finished);
}
