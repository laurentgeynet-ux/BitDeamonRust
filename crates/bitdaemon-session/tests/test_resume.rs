// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! resume glue — `Torrent::to_resume_data`/`apply_resume_data`
//! (upstream `write_resume_data`/`check_resume`).

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, DownloadPriority, Sha1Hash};
use bitdaemon_core::add_torrent_params::AddTorrentParams;
use bitdaemon_session::torrent::{Torrent, TorrentEffect};

fn t() -> Torrent {
    let mut t = Torrent::new(
        Sha1Hash::from_bytes(&[1; 20]).unwrap(),
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    );
    t.init_picker(false);
    t
}

#[test]
fn save_includes_unfinished_blocks() {
    let mut t = t();
    // piece 1 partially finished (blocks 0 and 3 of 8)
    use bitdaemon_common::types::PieceBlock;
    {
        let p = t.picker.as_mut().unwrap();
        p.mark_as_finished(
            PieceBlock {
                piece_index: PieceIndex(1),
                block_index: 0,
            },
            None,
        );
        p.mark_as_finished(
            PieceBlock {
                piece_index: PieceIndex(1),
                block_index: 3,
            },
            None,
        );
    }
    let mut atp = AddTorrentParams::default();
    t.to_resume_data(&mut atp, &bitdaemon_net::peer_class::PeerClassPool::new());
    let mask = atp.unfinished_pieces.get(&1).expect("piece 1 saved");
    assert!(mask.get_bit(0) && mask.get_bit(3));
    assert!(!mask.get_bit(1) && !mask.get_bit(7));
    assert_eq!(atp.piece_priorities.len(), 4);
}

#[test]
fn load_restores_verified_and_unfinished() {
    // upstream: resume `pieces` → `have`; `verified_pieces` only load
    // under `seed_mode` into `m_verified` (tested separately below).
    let mut have = Bitfield::with_size(4);
    have.set_bit(0);
    have.set_bit(2);
    let mut verified = Bitfield::with_size(4);
    verified.set_bit(0);
    verified.set_bit(2);
    let mut atp = AddTorrentParams {
        have_pieces: have,
        verified_pieces: verified,
        ..Default::default()
    };
    let mut bf = Bitfield::with_size(8);
    bf.set_bit(0);
    bf.set_bit(1);
    atp.unfinished_pieces.insert(3, bf);
    atp.piece_priorities = vec![DownloadPriority::DONT_DOWNLOAD; 4];

    let mut t = t();
    let mut eff = Vec::new();
    t.apply_resume_data(&atp, &Default::default(), &Default::default(), &mut eff);
    assert!(t.have.get_bit(0) && t.have.get_bit(2));
    // not seed mode → `verified_pieces` is ignored upstream
    assert_eq!(t.num_verified, 0);
    let p = t.picker.as_ref().unwrap();
    assert_eq!(
        p.piece_priority(PieceIndex(0)),
        DownloadPriority::DONT_DOWNLOAD
    );
    // the two restored blocks show in the download queue
    let q = p.get_download_queue();
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].index, PieceIndex(3));
    assert_eq!(q[0].finished, 2);
    assert!(
        !eff.iter()
            .any(|e| matches!(e, TorrentEffect::DiskHash { .. })),
        "piece not fully restored → no hash"
    );
}

/// upstream `on_resume_data_checked` `m_seed_mode` branch:
/// `set_have_all(true)` marks every piece present and resume
/// `verified_pieces` feed `m_verified` (lazy-verify ledger).
#[test]
fn load_seed_mode_sets_have_and_verified() {
    use bitdaemon_common::flags::TorrentFlags;
    let mut verified = Bitfield::with_size(4);
    verified.set_bit(1);
    verified.set_bit(3);
    let atp = AddTorrentParams {
        flags: TorrentFlags::SEED_MODE,
        verified_pieces: verified,
        ..Default::default()
    };
    let mut t = t();
    t.seed_mode = true;
    let mut eff = Vec::new();
    t.apply_resume_data(&atp, &Default::default(), &Default::default(), &mut eff);
    // `set_have_all` — every piece present on the seed promise
    for i in 0..4 {
        assert!(t.have.get_bit(i));
    }
    assert!(t.seed_verified.get_bit(1) && t.seed_verified.get_bit(3));
    assert!(!t.seed_verified.get_bit(0));
    assert_eq!(t.num_verified, 2);
}

#[test]
fn load_full_unfinished_triggers_verify() {
    let mut atp = AddTorrentParams::default();
    let mut bf = Bitfield::with_size(8);
    for i in 0..8 {
        bf.set_bit(i);
    }
    atp.unfinished_pieces.insert(1, bf);
    let mut t = t();
    let mut eff = Vec::new();
    t.apply_resume_data(&atp, &Default::default(), &Default::default(), &mut eff);
    assert!(
        eff.iter()
            .any(|e| matches!(e, TorrentEffect::DiskHash { piece } if *piece == PieceIndex(1))),
        "complete piece → verify_piece"
    );
}
