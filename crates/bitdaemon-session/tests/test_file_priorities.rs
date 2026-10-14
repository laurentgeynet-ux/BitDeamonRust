// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `prioritize_files` / `file_progress` — file priorities map to
//! piece priorities (max over overlapping files) and progress is
//! attributed per file from `have` + finished blocks.

use bitdaemon_common::types::{DownloadPriority, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_core::FileStorage;
use bitdaemon_session::torrent::Torrent;

/// 2 files: `a` = 96 KiB (pieces 0-1), `b` = 160 KiB (pieces 1-3);
/// 4 pieces of 64 KiB.
fn mk() -> Torrent {
    let ih = Sha1Hash::from_bytes(&[5; 20]).unwrap();
    let mut fs = FileStorage::new();
    fs.add_file(
        Some(b"a"),
        b"root/a",
        0x18000,
        bitdaemon_core::FileFlags::NONE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.add_file(
        Some(b"b"),
        b"root/b",
        0x28000,
        bitdaemon_core::FileFlags::NONE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.set_piece_length(0x10000);
    fs.set_num_pieces(4);
    let mut t = Torrent::new(ih, 4, 0x10000, Bitfield::with_size(4), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 4]);
    t.files = Some(std::sync::Arc::new(fs));
    t.init_picker(false);
    t
}

#[test]
fn file_priorities_drive_piece_priorities() {
    let mut t = mk();
    // file `a` → dont_download; piece 1 overlaps `b` → stays default
    t.prioritize_files(
        &[DownloadPriority::DONT_DOWNLOAD, DownloadPriority::DEFAULT],
        &Default::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut Vec::new(),
    );
    let p = t.picker.as_ref().unwrap();
    assert_eq!(
        p.piece_priority(PieceIndex(0)),
        DownloadPriority::DONT_DOWNLOAD
    );
    assert_eq!(p.piece_priority(PieceIndex(1)), DownloadPriority::DEFAULT);
    assert_eq!(p.piece_priority(PieceIndex(3)), DownloadPriority::DEFAULT);
    assert_eq!(t.file_priorities.len(), 2);

    // raise `a` to TOP → its pieces (0,1) take TOP; 1 keeps max
    t.prioritize_files(
        &[DownloadPriority::TOP, DownloadPriority::DEFAULT],
        &Default::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut Vec::new(),
    );
    let p = t.picker.as_ref().unwrap();
    assert_eq!(p.piece_priority(PieceIndex(0)), DownloadPriority::TOP);
    assert_eq!(p.piece_priority(PieceIndex(1)), DownloadPriority::TOP);
    assert_eq!(p.piece_priority(PieceIndex(2)), DownloadPriority::DEFAULT);
}

#[test]
fn file_progress_counts_have_and_blocks() {
    let mut t = mk();
    // piece 0 fully verified — entirely inside file `a`
    t.have.set_bit(0);
    let prog = t.file_progress(false);
    assert_eq!(prog[0], 0x10000);
    assert_eq!(prog[1], 0);

    // piece 1 finished in the picker (not yet `have`) — spans
    // a's tail (0x8000) and b's head (0x8000)
    let p = t.picker.as_mut().unwrap();
    let nb = p.blocks_in_piece(PieceIndex(1));
    for b in 0..nb {
        p.mark_as_finished(
            bitdaemon_common::types::PieceBlock {
                piece_index: PieceIndex(1),
                block_index: b,
            },
            None,
        );
    }
    let prog = t.file_progress(false);
    assert_eq!(prog[0], 0x18000); // piece0 (0x10000) + a's part of piece1
    assert_eq!(prog[1], 0x8000); // b's part of piece1
}
