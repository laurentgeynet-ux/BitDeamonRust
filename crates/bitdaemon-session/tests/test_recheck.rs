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

#[test]
fn save_resume_data_alert_and_interval() {
    use bitdaemon_session::alerts::Alert;
    let ih = Sha1Hash::from_bytes(&[7; 20]).unwrap();
    let mut t = Torrent::new(ih, 2, 128 * 1024, Bitfield::with_size(2), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 2]);
    t.init_picker(false);
    let mut s = Session::new();
    s.add_torrent(t);
    s.pop_alerts();

    // not dirty → only_if_modified suppresses
    s.save_resume_data(&ih, true);
    assert!(s.pop_alerts().is_empty());

    // verify a piece → dirty → save posts the blob alert
    s.on_piece_verified(&ih, PieceIndex(0), true, &mut Vec::new());
    s.pop_alerts();
    s.save_resume_data(&ih, true);
    let alerts = s.pop_alerts();
    let blob = alerts.iter().find_map(|a| {
        if let Alert::SaveResumeData { data, .. } = a {
            Some(data.clone())
        } else {
            None
        }
    });
    assert!(blob.is_some(), "expected SaveResumeData alert");
    // blob round-trips through upstream's resume reader
    let rd = bitdaemon_core::resume_data::read_resume_data(
        &blob.unwrap(),
        &bitdaemon_core::torrent_info::LoadTorrentLimits::default(),
    );
    assert!(rd.is_ok());
    // flag cleared — a second save is suppressed
    s.save_resume_data(&ih, true);
    assert!(s.pop_alerts().is_empty());
}

#[test]
fn force_announce_emits_effects() {
    use bitdaemon_session::session::SessionOut;
    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let mut t = Torrent::new(ih, 2, 128 * 1024, Bitfield::with_size(2), StorageIndex(0));
    t.add_tracker("http://a/announce".into(), 0);
    t.add_tracker("http://b/announce".into(), 1);
    let mut s = Session::new();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.force_announce(&ih, None, &mut out);
    let urls: Vec<_> = out
        .iter()
        .filter_map(|e| {
            if let SessionOut::TrackerAnnounce { url, .. } = e {
                Some(url.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(urls, vec!["http://a/announce", "http://b/announce"]);

    // single index
    out.clear();
    s.force_announce(&ih, Some(1), &mut out);
    assert!(matches!(
        &out[0],
        SessionOut::TrackerAnnounce { url, .. } if url == "http://b/announce"
    ));

    // flush_cache posts cache_flushed once the job completes
    use bitdaemon_session::alerts::Alert;
    use bitdaemon_session::session::DiskCompleted;
    s.pop_alerts();
    s.disk_completions()
        .send(DiskCompleted::ReleaseFiles { ih })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::CacheFlushed(i) if *i == ih))
    );
}

#[test]
fn queue_position_moves() {
    use bitdaemon_session::session::QueueMove;
    let mut s = Session::new();
    let mut ihs = Vec::new();
    for b in [10u8, 11, 12] {
        let ih = Sha1Hash::from_bytes(&[b; 20]).unwrap();
        let mut t = Torrent::new(ih, 2, 128 * 1024, Bitfield::with_size(2), StorageIndex(0));
        t.finished = false;
        s.add_torrent(t);
        ihs.push(ih);
    }
    // insertion order → positions 0,1,2 in add order
    macro_rules! pos {
        ($i:expr) => {
            s.queue_position(&ihs[$i])
        };
    }
    assert_eq!((pos!(0), pos!(1), pos!(2)), (0, 1, 2));

    s.queue_position_set(&ihs[2], QueueMove::Up);
    assert_eq!((pos!(0), pos!(1), pos!(2)), (0, 2, 1));

    s.queue_position_set(&ihs[0], QueueMove::Bottom);
    // ih2(1) < ih1(2) < ih0(3)
    assert_eq!((pos!(0), pos!(1), pos!(2)), (2, 1, 0));

    s.queue_position_set(&ihs[0], QueueMove::Top);
    // ih0(-1) < ih2(1) < ih1(2)
    assert_eq!((pos!(0), pos!(1), pos!(2)), (0, 2, 1));

    // paused → -1
    s.torrents.get_mut(&ihs[1]).unwrap().paused = true;
    assert_eq!(s.queue_position(&ihs[1]), -1);
}

#[test]
fn piece_deadline_and_read_piece() {
    use bitdaemon_session::alerts::Alert;
    use bitdaemon_session::session::DiskCompleted;
    use bitdaemon_session::torrent::TorrentEffect;

    let ih = Sha1Hash::from_bytes(&[11; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 4]);
    t.init_picker(false);

    // already-haved piece + alert_when_available → ReadPiece now
    t.have.set_bit(1);
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(1), 5000, true, &mut eff);
    assert!(matches!(
        eff[0],
        TorrentEffect::ReadPiece {
            piece: PieceIndex(1)
        }
    ));

    // missing piece → top priority + time_critical + cancel non-critical
    let mut eff = Vec::new();
    t.set_piece_deadline(PieceIndex(2), 5000, true, &mut eff);
    assert_eq!(t.piece_priority(PieceIndex(2)), Some(DownloadPriority::TOP));
    assert!(t.time_critical.contains_key(&PieceIndex(2)));

    // verify piece 2 → removed from critical + ReadPiece effect
    let mut eff = Vec::new();
    t.piece_verified(PieceIndex(2), true, &mut eff);
    assert!(!t.time_critical.contains_key(&PieceIndex(2)));
    assert!(eff.iter().any(|e| matches!(
        e,
        TorrentEffect::ReadPiece {
            piece: PieceIndex(2)
        }
    )));

    // disk readback → read_piece_alert with payload
    let mut s = Session::new();
    s.add_torrent(t);
    s.pop_alerts();
    s.disk_completions()
        .send(DiskCompleted::ReadPiece {
            ih,
            piece: PieceIndex(2),
            data: vec![0xAB; 0x4000],
            err: bitdaemon_disk::storage_error::StorageError::default(),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    assert!(s.pop_alerts().iter().any(|a| matches!(
        a,
        Alert::ReadPiece {
            piece: 2,
            buffer,
            ..
        } if buffer.len() == 0x4000
    )));
}
