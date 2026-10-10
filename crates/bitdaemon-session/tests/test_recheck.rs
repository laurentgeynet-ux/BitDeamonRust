// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `force_recheck` / `prioritize_pieces` / sequential flag —
//! handle-level torrent ops.

use bitdaemon_common::types::{DownloadPriority, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::torrent::{Torrent, TorrentEffect};

#[test]
fn force_recheck_queues_hash_per_piece() {
    let ih = Sha1Hash::from_bytes(&[8; 20]).unwrap();
    let mut t = Torrent::new(ih, 3, 128 * 1024, Bitfield::with_size(3), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 3]);
    t.init_picker(false);
    // pretend we had a verified piece — recheck must re-hash it
    t.have.set_bit(0);
    let mut s = Session::new();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.force_recheck(&ih, &mut out);
    let hashes = out
        .iter()
        .filter(|e| matches!(e, SessionOut::Torrent(TorrentEffect::DiskHash { .. })))
        .count();
    assert_eq!(hashes, 3);
    let t = s.torrents.get(&ih).unwrap();
    assert!(t.checking);
    assert_eq!(t.checking_pieces, 3);
    assert_eq!(t.have.count(), 0);

    // all 3 hashes come back good → checking clears, piece back
    for p in 0..3 {
        s.on_piece_verified(&ih, PieceIndex(p), true, &mut Vec::new());
    }
    let t = s.torrents.get(&ih).unwrap();
    assert!(!t.checking);
    assert_eq!(t.checking_pieces, 0);
    assert!(t.have.get_bit(0));
}

#[test]
fn piece_priorities_and_sequential() {
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 4]);
    t.init_picker(false);

    t.prioritize_pieces(&[
        DownloadPriority::TOP,
        DownloadPriority::DONT_DOWNLOAD,
        DownloadPriority::DEFAULT,
        DownloadPriority::DEFAULT,
    ]);
    assert_eq!(t.piece_priority(PieceIndex(0)), Some(DownloadPriority::TOP));
    assert_eq!(
        t.piece_priority(PieceIndex(1)),
        Some(DownloadPriority::DONT_DOWNLOAD)
    );

    t.set_sequential_download(true);
    assert!(t.sequential);
}
