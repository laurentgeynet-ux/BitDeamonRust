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
        &[],
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
        &[],
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert!(!t.finished);
    t.piece_verified(
        PieceIndex(1),
        true,
        &[],
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
        &[],
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    // `piece_failed` fences `clear_piece`; `PieceFailed` (the
    // `hash_failed` alert) only posts once the drain lands in
    // `on_piece_sync` — the session routes ClearPiece there, the
    // test drains it inline.
    t.on_piece_sync(
        PieceIndex(1),
        &[],
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

    // upstream's watermark scales with `m_uploaded_last_second` —
    // the payload bytes moved during the last tick
    // (`last_payload_uploaded()`), not the EMA rate:
    // uploaded_last_second × factor/100, clamped to [low, high].
    // A high EMA with zero payload last tick must NOT raise it.
    let mut c2 = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c2.choked = false;
    c2.rate_up_speed = 1_000_000; // EMA alone must not inflate the cap
    c2.uploaded_last_second = 0;
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
    // 10 KiB low watermark → only the first 16 KiB read fits.
    assert_eq!(
        out4.iter()
            .filter(|e| matches!(e, TorrentEffect::DiskRead { .. }))
            .count(),
        1
    );
    // last-second payload sampled at 1 MiB → 500 KiB cap admits all.
    {
        let c = t.peers.get_mut(&2).unwrap();
        c.uploaded_last_second = 1_000_000;
        c.reading_bytes = 0;
        c.send_buffer = 0;
    }
    let mut out5 = Vec::new();
    t.fill_send_buffer(
        2,
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out5,
    );
    assert_eq!(
        out5.iter()
            .filter(|e| matches!(e, TorrentEffect::DiskRead { .. }))
            .count(),
        2
    );
}

/// `piece_failed` fences `clear_piece` (upstream `piece_failed` →
/// `async_clear_piece` → `on_piece_sync`): the lock lands now, the
/// restore + `hash_failed` alert only arrive when the fence drains.
#[test]
fn hash_fail_fences_restore_until_piece_sync() {
    use bitdaemon_session::counters::{Counter, Counters};
    use bitdaemon_session::settings::SessionSettings;
    let s = SessionSettings::default();
    let counters = Counters::default();
    let mut t = torrent(3);
    t.init_picker(true);
    finish_piece(&mut t, PieceIndex(1));

    let mut out = Vec::new();
    t.piece_verified(PieceIndex(1), false, &[], &s, &counters, &mut out);
    assert_eq!(counters.value(Counter::NumPieceFailed), 1);
    // fenced — ClearPiece emitted, restore/alert still pending
    assert!(
        out.iter().any(
            |e| matches!(e, TorrentEffect::ClearPiece { piece, .. } if *piece == PieceIndex(1))
        )
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::PieceFailed(_)))
    );

    // the `clear_piece` job drained → `on_piece_sync` restores and
    // posts the deferred `hash_failed` alert.
    t.on_piece_sync(PieceIndex(1), &[], &s, &counters, &mut out);
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::PieceFailed(i) if *i == PieceIndex(1)))
    );
    assert!(!t.picker.as_ref().unwrap().is_piece_finished(PieceIndex(1)));
}

/// v2 `hash_failed` pinpointing bad blocks: the restore only drops
/// those blocks (upstream `on_piece_sync` block filtering) — good
/// blocks from honest peers stay finished.
#[test]
fn v2_bad_blocks_restore_only_bad() {
    use bitdaemon_common::types::PieceBlock;
    use bitdaemon_session::settings::SessionSettings;
    let s = SessionSettings::default();
    let mut t = torrent(3);
    t.init_picker(true);
    finish_piece(&mut t, PieceIndex(1));

    let mut out = Vec::new();
    t.piece_verified(
        PieceIndex(1),
        false,
        &[3],
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    // the bad-block list is carried on the fence
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::ClearPiece { piece, blocks }
            if *piece == PieceIndex(1) && blocks.as_slice() == [3]))
    );
    t.on_piece_sync(
        PieceIndex(1),
        &[3],
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    let p = t.picker.as_ref().unwrap();
    assert!(!p.is_piece_finished(PieceIndex(1)));
    assert!(!p.is_finished_block(PieceBlock {
        piece_index: PieceIndex(1),
        block_index: 3,
    }));
    // neighbours survived the restore
    assert!(p.is_finished_block(PieceBlock {
        piece_index: PieceIndex(1),
        block_index: 0,
    }));
}

/// `found_on_disk` — every block arrived without a downloader (only
/// `None` entries): upstream still fences+restores, but skips
/// `add_failed_bytes`, plugin `on_piece_failed` and peer penalties.
#[test]
fn found_on_disk_skips_blame_but_restores() {
    use bitdaemon_session::settings::SessionSettings;
    let s = SessionSettings::default();
    let mut t = torrent(3);
    t.init_picker(true);
    finish_piece(&mut t, PieceIndex(1));

    let mut out = Vec::new();
    t.piece_verified(
        PieceIndex(1),
        false,
        &[],
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert_eq!(t.failed_bytes, 0);
    // still fenced → restore path identical to a swarm-blamed fail
    assert!(
        out.iter().any(
            |e| matches!(e, TorrentEffect::ClearPiece { piece, .. } if *piece == PieceIndex(1))
        )
    );
}

/// `force_recheck` bumps `picker_generation` (upstream
/// `++m_picker_generation`) — that's what makes in-flight
/// write/hash completions stale for the session gates.
#[test]
fn recheck_bumps_picker_generation() {
    let mut t = torrent(4);
    t.init_picker(true);
    let g0 = t.picker_generation;
    assert!(t.arm_full_recheck(&mut Vec::new()) > 0);
    assert_ne!(t.picker_generation, g0);
}

/// A10 — a downloader whose conn died before the hash verdict is
/// still penalized on its swarm entry (`penalize_peers` upstream
/// iterates `torrent_peer*`, which outlives connections): lone
/// culprit → `known_bad` → banned, `pid` empty (no live conn), no
/// `DisconnectPeer` (nothing to drop — `if (p->connection)`).
#[test]
fn hash_fail_penalizes_dead_conn_culprit() {
    use bitdaemon_common::PeerSourceFlags;
    use bitdaemon_common::types::PieceBlock;
    use bitdaemon_session::counters::{Counter, Counters};
    use bitdaemon_session::piece_picker::PickerOptions;
    use bitdaemon_session::settings::SessionSettings;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let s = SessionSettings::default();
    let counters = Counters::default();
    let mut t = torrent(3);
    t.init_picker(true);
    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, 0, 9)), 6881);
    t.add_swarm_peer(remote, PeerSourceFlags::TRACKER, &mut Vec::new());
    let mut c = PeerConn::new(3, PeerPipeline::new(1000, 30, 250), 0);
    c.remote = Some(remote);
    c.peer_id = Some(bitdaemon_common::PeerId::from_bytes(&[0x42; 20]).unwrap());
    let key = c.peer_key().unwrap();
    t.add_peer(1, c);
    // attribute every block of piece 1 to that downloader, all
    // written (finished blocks keep their `block_info.peer`).
    if let Some(p) = t.picker.as_mut() {
        for b in 0..p.blocks_in_piece(PieceIndex(1)) {
            let blk = PieceBlock {
                piece_index: PieceIndex(1),
                block_index: b,
            };
            p.mark_as_downloading(blk, Some(key), PickerOptions::RAREST_FIRST);
            p.mark_as_finished(blk, Some(key));
        }
    }
    t.remove_peer(
        1,
        &counters,
        &bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut Vec::new(),
    );

    let mut out = Vec::new();
    t.piece_verified(PieceIndex(1), false, &[], &s, &counters, &mut out);

    let i = t.peer_list.find_peer(remote.ip()).unwrap();
    let p = &t.peer_list.peers()[i];
    assert!(p.banned);
    assert_eq!(p.trust_points, -2);
    assert_eq!(p.hashfails, 1);
    assert_eq!(counters.value(Counter::BannedForHashFailure), 1);
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::Alert(bitdaemon_session::alerts::Alert::PeerBan { ip, pid, .. })
            if *ip == remote && pid.is_none()
    )));
    // `if (p->connection)` — a dead conn has nothing to disconnect.
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. }))
    );
}

/// A10 — same resolution for the pass leg: a passing piece clears
/// parole and raises trust on the swarm entry even when the
/// downloader's conn is already gone.
#[test]
fn piece_pass_rewards_dead_conn_downloader() {
    use bitdaemon_common::PeerSourceFlags;
    use bitdaemon_common::types::PieceBlock;
    use bitdaemon_session::counters::Counters;
    use bitdaemon_session::piece_picker::PickerOptions;
    use bitdaemon_session::settings::SessionSettings;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let s = SessionSettings::default();
    let counters = Counters::default();
    let mut t = torrent(3);
    t.init_picker(true);
    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, 0, 9)), 6881);
    t.add_swarm_peer(remote, PeerSourceFlags::TRACKER, &mut Vec::new());
    let i = t.peer_list.find_peer(remote.ip()).unwrap();
    t.peer_list.peers_mut()[i].on_parole = true;
    t.peer_list.peers_mut()[i].trust_points = -4;
    let mut c = PeerConn::new(3, PeerPipeline::new(1000, 30, 250), 0);
    c.remote = Some(remote);
    c.peer_id = Some(bitdaemon_common::PeerId::from_bytes(&[0x42; 20]).unwrap());
    let key = c.peer_key().unwrap();
    t.add_peer(1, c);
    if let Some(p) = t.picker.as_mut() {
        for b in 0..p.blocks_in_piece(PieceIndex(1)) {
            let blk = PieceBlock {
                piece_index: PieceIndex(1),
                block_index: b,
            };
            p.mark_as_downloading(blk, Some(key), PickerOptions::RAREST_FIRST);
            p.mark_as_finished(blk, Some(key));
        }
    }
    t.remove_peer(
        1,
        &counters,
        &bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut Vec::new(),
    );

    let mut out = Vec::new();
    t.piece_verified(PieceIndex(1), true, &[], &s, &counters, &mut out);

    let p = &t.peer_list.peers()[i];
    assert!(!p.on_parole);
    assert_eq!(p.trust_points, -3);
}

/// A09 — `send_choke` emits the `Choke` wire message *before* draining
/// the queued upload requests with rejects
/// (`peer_connection::send_choke`, peer_connection.cpp:3848→3859).
#[test]
fn choke_emits_choke_before_rejects() {
    use bitdaemon_common::PeerRequest;
    use bitdaemon_session::counters::Counters;

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
    c.supports_fast = true;
    for i in 0..2 {
        c.upload_queue.push(PeerRequest {
            piece: PieceIndex(i),
            start: 0,
            length: 0x4000,
        });
    }
    t.add_peer(1, c);

    let counters = Counters::default();
    let mut out = Vec::new();
    assert!(t.choke_peer(
        1,
        &counters,
        &bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut out,
    ));

    let wire: Vec<u8> = out
        .iter()
        .filter_map(|e| match e {
            TorrentEffect::SendTo { msg, .. } => Some(msg.wire_id()),
            _ => None,
        })
        .collect();
    // Choke (0) first, then the two RejectRequest (0x10).
    assert_eq!(wire, vec![0, 0x10, 0x10]);
}

/// A09 — a shape-valid request for a piece we don't have (nor
/// predictive, nor seed mode) goes through upstream's *invalid*
/// branch: `invalid_piece_requests` + `m_num_invalid_requests`, not
/// just the reject (`peer_connection.cpp:2519-2582`).
#[test]
fn missing_piece_request_counts_invalid() {
    use bitdaemon_common::PeerRequest;
    use bitdaemon_session::counters::{Counter, Counters};
    use bitdaemon_session::peer_conn::PeerEffect;

    let mut t = torrent(4); // have: all false, not seed, no predictive
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.choked = false;
    c.peer_interested = true;
    c.supports_fast = true;
    // peer_conn already queued the shape-valid request.
    c.upload_queue.push(PeerRequest {
        piece: PieceIndex(0),
        start: 0,
        length: 0x4000,
    });
    t.add_peer(1, c);

    let counters = Counters::default();
    let mut out = Vec::new();
    t.apply_peer_effects(
        1,
        vec![PeerEffect::ReadRequest(PeerRequest {
            piece: PieceIndex(0),
            start: 0,
            length: 0x4000,
        })],
        &bitdaemon_session::settings::SessionSettings::default(),
        &counters,
        &mut out,
    );

    assert_eq!(counters.value(Counter::InvalidPieceRequests), 1);
    assert_eq!(t.peers[&1].num_invalid_requests, 1);
    // request dropped from `m_requests` + BEP-6 reject + alert.
    assert!(t.peers[&1].upload_queue.is_empty());
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::SendTo {
            msg: PeerMessage::RejectRequest(_),
            ..
        }
    )));
    assert!(out.iter().any(|e| matches!(
        e,
        TorrentEffect::Alert(bitdaemon_session::alerts::Alert::InvalidRequest { .. })
    )));
}
