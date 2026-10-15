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
    // upstream `write_resume_data` omits `piece_priority` when every
    // piece sits at `default_priority`.
    assert!(atp.piece_priorities.is_empty());
}

/// upstream `write_resume_data` (`torrent.cpp:7624-7650`): before
/// `m_files_checked` the `pieces` bitfield is *not written at all* —
/// an unverified torrent saving optimistic bits would make the next
/// load treat the resume as valid and skip the required full check.
#[test]
fn save_omits_have_before_files_checked() {
    let mut t = t();
    // optimistic claims exist (e.g. apply_resume_data ran) but the
    // storage was never verified.
    t.have.set_bit(0);
    t.have.set_bit(2);
    assert!(!t.files_checked);
    let mut atp = AddTorrentParams::default();
    t.to_resume_data(&mut atp, &bitdaemon_net::peer_class::PeerClassPool::new());
    assert!(
        atp.have_pieces.is_empty(),
        "no `pieces` before files_checked"
    );
}

/// mid-`checking_files` snapshot: upstream writes only the
/// `m_num_checked_pieces` prefix so the loader resumes the check at
/// `have_pieces.end_index()`.
#[test]
fn save_writes_checked_prefix_while_checking() {
    let mut t = t();
    t.files_checked = true;
    // 4 pieces, tail of 2 still being hashed → prefix = 2.
    t.checking = true;
    t.checking_resume = false;
    t.checking_pieces = 2;
    t.have.set_bit(0);
    t.have.set_bit(1);
    t.have.set_bit(3); // not yet proven — beyond the checked prefix
    let mut atp = AddTorrentParams::default();
    t.to_resume_data(&mut atp, &bitdaemon_net::peer_class::PeerClassPool::new());
    assert_eq!(atp.have_pieces.size(), 2, "prefix only");
    assert!(atp.have_pieces.get_bit(0) && atp.have_pieces.get_bit(1));
    // while *resume* verification runs, nothing is persisted.
    t.checking = true;
    t.checking_resume = true;
    t.files_checked = false;
    let mut atp = AddTorrentParams::default();
    t.to_resume_data(&mut atp, &bitdaemon_net::peer_class::PeerClassPool::new());
    assert!(atp.have_pieces.is_empty());
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
    // upstream applies `unfinished` only in the *accepted* leg of
    // `on_resume_data_checked` — before the verdict the picker
    // carries no phantom finished blocks.
    assert!(p.get_download_queue().is_empty());
    assert!(
        !eff.iter()
            .any(|e| matches!(e, TorrentEffect::DiskHash { .. })),
        "no verify before the verdict"
    );
    // accepted → the two restored blocks show in the download queue
    let mut eff = Vec::new();
    t.on_files_checked(true, &Default::default(), &Default::default(), &mut eff);
    let q = t.picker.as_ref().unwrap().get_download_queue();
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].index, PieceIndex(3));
    assert_eq!(q[0].finished, 2);
    assert!(
        !eff.iter()
            .any(|e| matches!(e, TorrentEffect::DiskHash { .. })),
        "piece not fully restored → no hash"
    );
}

/// upstream `on_resume_data_checked` reject leg: claimed `have`
/// bits, seed mode and deferred `unfinished` all unwind; the dirty
/// flag (`m_need_save_resume_data`) is *restored* on both legs.
#[test]
fn rejected_fastresume_unwinds_claims() {
    let mut have = Bitfield::with_size(4);
    have.set_bit(0);
    have.set_bit(1);
    let mut bf = Bitfield::with_size(8);
    bf.set_bit(0);
    let mut atp = AddTorrentParams {
        have_pieces: have,
        ..Default::default()
    };
    atp.unfinished_pieces.insert(2, bf);
    let mut t = t();
    t.seed_mode = true; // ctor-level SEED_MODE flag applied
    let mut eff = Vec::new();
    t.apply_resume_data(&atp, &Default::default(), &Default::default(), &mut eff);
    assert!(t.pending_unfinished.contains_key(&2));
    assert!(t.have.count() > 0);
    let dirty = t.need_save_resume;
    t.on_files_checked(false, &Default::default(), &Default::default(), &mut eff);
    assert_eq!(t.have.count(), 0);
    assert_eq!(t.verified_bytes, 0);
    assert!(!t.seed_mode, "rejected resume leaves seed mode");
    assert!(!t.finished && !t.seeding);
    assert!(t.pending_unfinished.is_empty());
    assert!(t.picker.as_ref().unwrap().get_download_queue().is_empty());
    assert_eq!(t.need_save_resume, dirty, "load must not dirty the flag");
}

/// `on_resume_data_checked` preserves `m_need_save_resume_data`
/// across the apply — a clean fastresume load stays clean.
#[test]
fn accepted_load_preserves_need_save_resume() {
    let mut have = Bitfield::with_size(4);
    have.set_bit(0);
    let atp = AddTorrentParams {
        have_pieces: have,
        ..Default::default()
    };
    let mut t = t();
    assert!(!t.need_save_resume.is_set());
    let mut eff = Vec::new();
    t.apply_resume_data(&atp, &Default::default(), &Default::default(), &mut eff);
    t.on_files_checked(true, &Default::default(), &Default::default(), &mut eff);
    assert!(!t.need_save_resume.is_set(), "clean load must not dirty");
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
    // deferred until the fastresume verdict
    assert!(
        !eff.iter()
            .any(|e| matches!(e, TorrentEffect::DiskHash { .. })),
        "no verify before acceptance"
    );
    let mut eff = Vec::new();
    t.on_files_checked(true, &Default::default(), &Default::default(), &mut eff);
    assert!(
        eff.iter()
            .any(|e| matches!(e, TorrentEffect::DiskHash { piece } if *piece == PieceIndex(1))),
        "complete piece → verify_piece"
    );
}

/// upstream partial-resume leg (`torrent.cpp:2335-2350`): an
/// accepted fastresume whose `pieces` bitfield stops short of
/// `num_pieces` was saved mid-`checking_files` — claims stand and
/// the hash pass resumes at `end_index()`, not from scratch.
#[test]
fn partial_resume_continues_check_from_prefix() {
    let mut have = Bitfield::with_size(2); // checked prefix only
    have.set_bit(0);
    have.set_bit(1);
    let atp = AddTorrentParams {
        have_pieces: have,
        ..Default::default()
    };
    let mut t = t();
    let mut eff = Vec::new();
    t.apply_resume_data(&atp, &Default::default(), &Default::default(), &mut eff);
    let tail = t.on_resume_partial(2, &Default::default(), &Default::default(), &mut eff);
    assert_eq!(tail, 2, "tail = pieces 2..4");
    assert!(t.checking && !t.checking_resume);
    assert_eq!(t.checking_pieces, 2);
    assert!(t.have.get_bit(0) && t.have.get_bit(1));
    assert!(!t.files_checked, "real check still running");
    // drain → files_checked + TorrentChecked (routed via `out`)
    assert!(!t.recheck_piece_done(&Default::default(), &mut eff));
    assert!(t.recheck_piece_done(&Default::default(), &mut eff));
    assert!(t.files_checked);
}
