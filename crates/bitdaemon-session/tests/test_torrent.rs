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

/// upstream `on_piece_verified` ignores pieces the picker doesn't
/// consider finished — simulate the download completing.
fn finish_piece(t: &mut Torrent, piece: PieceIndex) {
    if let Some(p) = t.picker.as_mut() {
        for b in 0..p.blocks_in_piece(piece) {
            p.mark_as_finished(
                bitdaemon_common::types::PieceBlock {
                    piece_index: piece,
                    block_index: b,
                },
                None,
            );
        }
    }
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
        0x4000,
        0x4000 * 8,
        None,
        false,
        0x200000,
        2000,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    t.add_peer(1, c);
    let mut out = Vec::new();
    t.apply_peer_effects(
        1,
        eff,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    // pieces 0,1 now have refcount 1
    let p = t.picker.as_ref().unwrap();
    assert_eq!(p.piece_stats(PieceIndex(0)).peer_count, 1);
    assert_eq!(p.piece_stats(PieceIndex(5)).peer_count, 0);

    // disconnect → refcount back to 0
    let mut eff2 = Vec::new();
    t.remove_peer(
        1,
        &bitdaemon_session::counters::Counters::default(),
        &bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut eff2,
    );
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
    finish_piece(&mut t, PieceIndex(0));
    // `announce_piece` skips conns still in handshake.
    for c in t.peers.values_mut() {
        c.connecting = false;
        c.handshake_done = true;
    }
    let mut out = Vec::new();
    t.piece_verified(
        PieceIndex(0),
        true,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
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
    finish_piece(&mut t, PieceIndex(0));
    finish_piece(&mut t, PieceIndex(1));
    let mut out = Vec::new();
    t.piece_verified(
        PieceIndex(0),
        true,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert!(!t.finished);
    t.piece_verified(
        PieceIndex(1),
        true,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
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
    t.update_rates(t0, &mut Vec::new());
    // peer leaves between ticks — its totals vanish from the sum
    let mut eff = Vec::new();
    t.remove_peer(
        1,
        &bitdaemon_session::counters::Counters::default(),
        &bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut eff,
    );
    t.update_rates(t0 + std::time::Duration::from_secs(1), &mut Vec::new());
    assert!(t.down_rate >= 0 && t.up_rate >= 0, "rates went negative");
    assert_eq!(t.down_rate, 0);
    assert_eq!(t.up_rate, 0);
}

/// upstream `m_total_uploaded`/`m_stat` payload channels are
/// monotonic across disconnects — a departing conn folds its totals
/// into the torrent accumulators (`remove_peer`), and resume data
/// writes `all_time` seed + session payload.
#[test]
fn payload_totals_survive_disconnect() {
    let mut t = torrent(4);
    t.init_picker(true);
    t.all_time_upload = 777; // resume seed (atp.total_uploaded)
    t.all_time_download = 333;
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.stat_down_total = 1_000_000;
    c.stat_up_total = 500_000;
    t.add_peer(1, c);
    assert_eq!(t.total_download(), 1_000_000);
    assert_eq!(t.total_upload(), 500_000);
    let mut eff = Vec::new();
    t.remove_peer(
        1,
        &bitdaemon_session::counters::Counters::default(),
        &bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut eff,
    );
    // totals stay — upstream `m_stat`/`m_total_uploaded` never shrink
    assert_eq!(t.total_download(), 1_000_000);
    assert_eq!(t.total_upload(), 500_000);
    // resume writes `m_total_uploaded` — seed + session payload
    let mut atp = bitdaemon_core::AddTorrentParams::default();
    let pool = bitdaemon_net::peer_class::PeerClassPool::new();
    t.to_resume_data(&mut atp, &pool);
    assert_eq!(atp.total_uploaded, 777 + 500_000);
    assert_eq!(atp.total_downloaded, 333 + 1_000_000);
}

#[test]
fn failed_piece_restores() {
    let mut t = torrent(3);
    t.init_picker(true);
    finish_piece(&mut t, PieceIndex(1));
    let mut out = Vec::new();
    t.piece_verified(
        PieceIndex(1),
        false,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::PieceFailed(i) if *i == PieceIndex(1)))
    );
    // piece back in open queue — pickable again
    assert!(!t.finished);
}

/// `fill_send_buffer` (upstream `peer_connection::fill_send_buffer`)
/// — a peer's accepted requests issue disk reads only while
/// `send_buffer + reading_bytes` stays under the dynamic watermark;
/// the rest queue in `upload_queue` (`m_requests`) until the socket
/// drains (`on_peer_sent` / the second tick).
#[test]
fn fill_send_buffer_gates_reads_on_watermark() {
    use bitdaemon_common::PeerRequest;
    use bitdaemon_session::settings::SessionSettings;

    let s = SessionSettings::default();
    // seed: every piece present
    let mut have = Bitfield::new();
    have.resize_with(4, true);
    let mut t = Torrent::new(
        Sha1Hash::from_bytes(&[9u8; 20]).unwrap(),
        4,
        0x40000,
        have,
        StorageIndex(0),
    );
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.choked = false;
    // rate 0 → watermark = send_buffer_low_watermark (10 KiB)
    c.upload_queue.push(PeerRequest {
        piece: PieceIndex(0),
        start: 0,
        length: 0x4000,
    });
    c.upload_queue.push(PeerRequest {
        piece: PieceIndex(1),
        start: 0,
        length: 0x4000,
    });
    t.add_peer(1, c);

    let mut out = Vec::new();
    t.fill_send_buffer(
        1,
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    // first read admitted, second held (16 KiB in flight >= 10 KiB)
    let reads: Vec<_> = out
        .iter()
        .filter(|e| matches!(e, TorrentEffect::DiskRead { .. }))
        .collect();
    assert_eq!(reads.len(), 1);
    assert_eq!(t.peers[&1].upload_queue.len(), 1);
    assert_eq!(t.peers[&1].reading_bytes, 0x4000);

    // read completes → reading_bytes frees, but the payload moves to
    // the send buffer: still over the watermark, nothing new issues.
    {
        let c = t.peers.get_mut(&1).unwrap();
        c.reading_bytes = 0;
        c.send_buffer = 0x4000;
    }
    let mut out2 = Vec::new();
    t.fill_send_buffer(
        1,
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out2,
    );
    assert_eq!(
        out2.iter()
            .filter(|e| matches!(e, TorrentEffect::DiskRead { .. }))
            .count(),
        0
    );

    // socket drains (`on_sent`) → buffer below watermark → next
    // queued request issues.
    {
        let c = t.peers.get_mut(&1).unwrap();
        c.send_buffer = 0;
    }
    let mut out3 = Vec::new();
    t.fill_send_buffer(
        1,
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out3,
    );
    assert_eq!(
        out3.iter()
            .filter(|e| matches!(e, TorrentEffect::DiskRead { .. }))
            .count(),
        1
    );
    assert!(t.peers[&1].upload_queue.is_empty());

    // upstream's watermark scales with the measured upload rate:
    // rate_up_speed × factor/100, clamped to [low, high].
    let mut c2 = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c2.choked = false;
    c2.rate_up_speed = 1_000_000; // 1 MB/s × 50 % → 500 KiB cap
    for i in 0..3 {
        c2.upload_queue.push(PeerRequest {
            piece: PieceIndex(i),
            start: 0,
            length: 0x4000,
        });
    }
    t.add_peer(2, c2);
    let mut out4 = Vec::new();
    t.fill_send_buffer(
        2,
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out4,
    );
    assert_eq!(
        out4.iter()
            .filter(|e| matches!(e, TorrentEffect::DiskRead { .. }))
            .count(),
        3
    );
}
