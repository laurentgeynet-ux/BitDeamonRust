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
    t.to_resume_data(&mut atp);
    let mask = atp.unfinished_pieces.get(&1).expect("piece 1 saved");
    assert!(mask.get_bit(0) && mask.get_bit(3));
    assert!(!mask.get_bit(1) && !mask.get_bit(7));
    assert_eq!(atp.piece_priorities.len(), 4);
}

#[test]
fn load_restores_verified_and_unfinished() {
    let mut verified = Bitfield::with_size(4);
    verified.set_bit(0);
    verified.set_bit(2);
    let mut atp = AddTorrentParams {
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
    t.apply_resume_data(&atp, &mut eff);
    assert!(t.have.get_bit(0) && t.have.get_bit(2));
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
    t.apply_resume_data(&atp, &mut eff);
    assert!(
        eff.iter()
            .any(|e| matches!(e, TorrentEffect::DiskHash { piece } if *piece == PieceIndex(1))),
        "complete piece → verify_piece"
    );
}
