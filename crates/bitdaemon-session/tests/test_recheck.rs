// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `force_recheck` / `prioritize_pieces` / sequential flag —
//! handle-level torrent ops.

use bitdaemon_common::types::{DownloadPriority, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_session::alerts::AlertCategory;
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::torrent::{Torrent, TorrentEffect};

/// upstream tests opt into the alert categories they assert on —
/// the default `alert_mask` is `error` only.
fn session() -> Session {
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    s
}

#[test]
fn force_recheck_queues_hash_per_piece() {
    let ih = Sha1Hash::from_bytes(&[8; 20]).unwrap();
    let mut t = Torrent::new(ih, 3, 128 * 1024, Bitfield::with_size(3), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 3]);
    t.init_picker(false);
    // pretend we had a verified piece — recheck must re-hash it
    t.have.set_bit(0);
    let mut s = session();
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
fn recheck_eof_skips_file_without_error() {
    use bitdaemon_common::types::FileIndex;
    use bitdaemon_disk::storage_error::{Operation, StorageError};
    use bitdaemon_session::session::DiskCompleted;

    let ih = Sha1Hash::from_bytes(&[8; 20]).unwrap();
    let mut t = Torrent::new(ih, 3, 128 * 1024, Bitfield::with_size(3), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 3]);
    t.init_picker(false);
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.force_recheck(&ih, &mut out);
    let pgen = s.torrents[&ih].picker_generation;
    assert!(s.torrents[&ih].checking);

    // the file is physically shorter than its declared span (e.g. a
    // sparse tail that was never written) — upstream `on_piece_hashed`
    // treats `eof`/`file_too_short`/`ENOENT`/`partfile_missing_piece`
    // as skip-file: the piece stays not-have, checking continues, no
    // error state, no pause.
    let eof = StorageError::new(
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"),
        FileIndex(0),
        Operation::FileRead,
    );
    s.disk_completions()
        .send(DiskCompleted::Hash {
            ih,
            piece: PieceIndex(1),
            hash: Sha1Hash::default(),
            block_hashes: Vec::new(),
            picker_gen: pgen,
            err: eof,
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    let t = s.torrents.get(&ih).unwrap();
    assert!(
        t.last_error.is_empty(),
        "eof during check must not set_error"
    );
    assert!(!t.is_paused(), "eof during check must not pause");
    assert!(t.checking);
    assert_eq!(t.checking_pieces, 2);
    assert!(!t.have.get_bit(1));

    // the other pieces still complete the pass normally
    s.on_piece_verified(&ih, PieceIndex(0), true, &mut Vec::new());
    s.on_piece_verified(&ih, PieceIndex(2), true, &mut Vec::new());
    let t = s.torrents.get(&ih).unwrap();
    assert!(!t.checking);
    assert!(t.have.get_bit(0) && t.have.get_bit(2));
}

#[test]
fn recheck_fatal_error_pauses_and_drops_automanage() {
    use bitdaemon_common::types::FileIndex;
    use bitdaemon_disk::storage_error::{Operation, StorageError};
    use bitdaemon_session::session::DiskCompleted;

    let ih = Sha1Hash::from_bytes(&[8; 20]).unwrap();
    let mut t = Torrent::new(ih, 2, 128 * 1024, Bitfield::with_size(2), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 2]);
    t.init_picker(false);
    t.auto_managed = true;
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.force_recheck(&ih, &mut out);
    let pgen = s.torrents[&ih].picker_generation;

    // a non-eof-class disk error mid-check is the fatal leg:
    // upstream `on_piece_hashed` → `auto_managed(false)` + `pause()`
    // + `set_error` + `trigger_auto_manage`.
    let perm = StorageError::new(
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
        FileIndex(0),
        Operation::FileRead,
    );
    s.disk_completions()
        .send(DiskCompleted::Hash {
            ih,
            piece: PieceIndex(0),
            hash: Sha1Hash::default(),
            block_hashes: Vec::new(),
            picker_gen: pgen,
            err: perm,
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    let t = s.torrents.get(&ih).unwrap();
    assert!(!t.last_error.is_empty());
    assert!(t.is_paused());
    assert!(!t.auto_managed);
}

#[test]
fn piece_priorities_and_sequential() {
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 4]);
    t.init_picker(false);

    t.prioritize_pieces(
        &[
            DownloadPriority::TOP,
            DownloadPriority::DONT_DOWNLOAD,
            DownloadPriority::DEFAULT,
            DownloadPriority::DEFAULT,
        ],
        &bitdaemon_session::counters::Counters::default(),
        &mut Vec::new(),
    );
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
    let mut s = session();
    s.add_torrent(t);
    s.pop_alerts();

    // not dirty → only_if_modified posts
    // `SaveResumeDataFailed(resume_data_not_modified)` — upstream
    // `torrent::save_resume_data` emits the failure alert rather
    // than staying silent.
    s.save_resume_data(
        &ih,
        bitdaemon_common::flags::ResumeDataFlags::ONLY_IF_MODIFIED,
    );
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::SaveResumeDataFailed { .. })),
        "expected SaveResumeDataFailed alert"
    );

    // verify a piece → dirty → save posts the blob alert
    // (picker must consider the piece finished first — upstream
    // `on_piece_verified` ignores unfinished pieces).
    if let Some(t) = s.torrents.get_mut(&ih)
        && let Some(p) = t.picker.as_mut()
    {
        for b in 0..p.blocks_in_piece(PieceIndex(0)) {
            p.mark_as_finished(
                bitdaemon_common::types::PieceBlock {
                    piece_index: PieceIndex(0),
                    block_index: b,
                },
                None,
            );
        }
    }
    s.on_piece_verified(&ih, PieceIndex(0), true, &mut Vec::new());
    s.pop_alerts();
    s.save_resume_data(
        &ih,
        bitdaemon_common::flags::ResumeDataFlags::ONLY_IF_MODIFIED,
    );
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
    // flag cleared — a second save posts not-modified again
    s.save_resume_data(
        &ih,
        bitdaemon_common::flags::ResumeDataFlags::ONLY_IF_MODIFIED,
    );
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::SaveResumeDataFailed { .. })),
        "expected SaveResumeDataFailed alert"
    );
}

#[test]
fn force_announce_emits_effects() {
    use bitdaemon_session::session::SessionOut;
    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let mut t = Torrent::new(ih, 2, 128 * 1024, Bitfield::with_size(2), StorageIndex(0));
    t.add_tracker(
        "http://a/announce".into(),
        0,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    t.add_tracker(
        "http://b/announce".into(),
        1,
        bitdaemon_session::tracker_list::SOURCE_CLIENT,
    );
    let mut s = session();
    s.add_torrent(t);

    let mut out = Vec::new();
    s.force_announce(
        &ih,
        None,
        0,
        bitdaemon_session::session::ReannounceFlags::HIGH_PRIORITY,
        &mut out,
    );
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
    // upstream `force_reannounce(-1)` marks every tracker due but the
    // announce_state failover still applies: with the default
    // announce_to_all_tiers/trackers=false, only the first working
    // tracker announces.
    assert_eq!(urls, vec!["http://a/announce"]);

    // single index — `force_tracker_request(idx)` bypasses the tier
    // failover and reaches entry 1 directly.
    out.clear();
    s.force_announce(
        &ih,
        Some(1),
        0,
        bitdaemon_session::session::ReannounceFlags::default(),
        &mut out,
    );
    assert!(matches!(
        &out[0],
        SessionOut::TrackerAnnounce { url, .. } if url == "http://b/announce"
    ));

    // with announce_to_all_tiers the scan reaches every due tracker.
    // both are in-flight from the announces above — upstream
    // `can_announce` blocks on `updating`, so complete them first.
    use bitdaemon_net::http_tracker::TrackerResponse;
    let reply = TrackerResponse {
        interval: 0,
        min_interval: 0,
        ..TrackerResponse::default()
    };
    s.on_announce_response(
        &ih,
        "http://a/announce",
        bitdaemon_common::info_hash::ProtocolVersion::V1,
        Ok(reply.clone()),
        None,
        &mut out,
    );
    s.on_announce_response(
        &ih,
        "http://b/announce",
        bitdaemon_common::info_hash::ProtocolVersion::V1,
        Ok(reply),
        None,
        &mut out,
    );
    s.settings.announce_to_all_tiers = true;
    out.clear();
    s.force_announce(
        &ih,
        None,
        0,
        bitdaemon_session::session::ReannounceFlags::HIGH_PRIORITY,
        &mut out,
    );
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
    let mut s = session();
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

    // upstream `m_download_queue` — paused downloaders keep their
    // slot; only `finished`/`aborted` drop out (`no_pos` = -1).
    s.torrents.get_mut(&ihs[1]).unwrap().paused = true;
    assert_eq!(s.queue_position(&ihs[1]), 2);
    s.torrents.get_mut(&ihs[1]).unwrap().finished = true;
    assert_eq!(s.queue_position(&ihs[1]), -1);
    // …which also re-packs the survivors' indices.
    assert_eq!((pos!(0), pos!(2)), (0, 1));
    // a finished torrent rejects re-queueing upstream.
    s.queue_position_set(&ihs[1], QueueMove::Top);
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
    t.set_piece_deadline(
        PieceIndex(1),
        5000,
        true,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(matches!(
        eff[0],
        TorrentEffect::ReadPiece {
            piece: PieceIndex(1)
        }
    ));

    // missing piece → top priority + time_critical + cancel non-critical
    let mut eff = Vec::new();
    t.set_piece_deadline(
        PieceIndex(2),
        5000,
        true,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(t.piece_priority(PieceIndex(2)), Some(DownloadPriority::TOP));
    assert!(t.time_critical.contains_key(&PieceIndex(2)));

    // verify piece 2 → removed from critical + ReadPiece effect
    // (upstream `on_piece_verified` ignores pieces the picker doesn't
    // consider finished — mark its blocks downloaded first).
    if let Some(p) = t.picker.as_mut() {
        for b in 0..p.blocks_in_piece(PieceIndex(2)) {
            p.mark_as_finished(
                bitdaemon_common::types::PieceBlock {
                    piece_index: PieceIndex(2),
                    block_index: b,
                },
                None,
            );
        }
    }
    let mut eff = Vec::new();
    t.piece_verified(
        PieceIndex(2),
        true,
        &[],
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(!t.time_critical.contains_key(&PieceIndex(2)));
    assert!(eff.iter().any(|e| matches!(
        e,
        TorrentEffect::ReadPiece {
            piece: PieceIndex(2)
        }
    )));

    // disk readback → read_piece_alert with payload
    let mut s = session();
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

#[test]
fn flags_ban_and_tracker_ops() {
    use bitdaemon_common::flags::TorrentFlags as F;
    use bitdaemon_common::types::StorageIndex;
    use bitdaemon_common::{Bitfield, Sha1Hash};
    use bitdaemon_session::session_handle::SessionHandle;
    use bitdaemon_session::settings::SessionParams;
    use bitdaemon_session::torrent::Torrent;

    let h = SessionHandle::new(SessionParams::default());
    let ih = Sha1Hash::from_bytes(&[7u8; 20]).unwrap();
    let t = Torrent::new(ih, 1, 128 * 1024, Bitfield::with_size(1), StorageIndex(0));
    let th = h.add_torrent(t);

    // flags
    th.set_flags(
        F::UPLOAD_MODE | F::AUTO_MANAGED,
        F::UPLOAD_MODE | F::AUTO_MANAGED,
    );
    let f = th.flags();
    assert!(f.contains(F::UPLOAD_MODE) && f.contains(F::AUTO_MANAGED));
    th.unset_flags(F::UPLOAD_MODE);
    assert!(!th.flags().contains(F::UPLOAD_MODE));

    // trackers
    th.add_tracker("http://t1/announce".into(), 1);
    th.add_tracker("http://t0/announce".into(), 0);
    th.replace_trackers(&[("http://x/announce".into(), 0)]);
    let trackers = th.trackers();
    assert_eq!(trackers.len(), 1);
    assert_eq!(trackers[0].url, "http://x/announce");
    assert_eq!(trackers[0].tier, 0);

    // ban: peer not connected → just flagged, no disconnect effect
    let mut out = Vec::new();
    th.ban_peer("1.2.3.4:6881".parse().unwrap(), &mut out);
    assert!(out.is_empty());

    th.set_max_connections(10);
    th.set_max_uploads(4);
}

#[test]
fn v2_piece_verifies_via_merkle_blocks() {
    // real v2 torrent: 1 file of exactly 4×16 KiB blocks = 1 piece.
    use bitdaemon_common::Sha256Hash;
    use bitdaemon_common::types::{FileIndex, PieceIndex, StorageIndex};
    use bitdaemon_core::create_torrent::{CreateFileEntry, CreateFlags, CreateTorrent};
    use bitdaemon_core::merkle::merkle_root;
    use bitdaemon_core::torrent_info::{LoadTorrentLimits, load_torrent_buffer};
    use bitdaemon_session::torrent::Torrent;
    use sha2::{Digest as _, Sha256};

    let data: Vec<u8> = (0..4 * 16384).map(|i| (i % 251) as u8).collect();
    let blocks: Vec<Sha256Hash> = data
        .chunks(16384)
        .map(|c| Sha256Hash::from_bytes(&Sha256::digest(c)).unwrap())
        .collect();
    let root = merkle_root(&blocks, Sha256Hash::default());

    let mut ct = CreateTorrent::new(
        vec![CreateFileEntry {
            filename: b"v2f".to_vec(),
            size: 4 * 16384,
            flags: bitdaemon_core::file_storage::FileFlags::NONE,
            mtime: 0,
            symlink: Vec::new(),
        }],
        4 * 16384,
        CreateFlags::V2_ONLY,
    )
    .unwrap();
    ct.set_hash2(FileIndex(0), 0, root).unwrap();
    let buf = ct.generate_buf().unwrap();
    let atp = load_torrent_buffer(&buf, &LoadTorrentLimits::default()).unwrap();
    let mut t = Torrent::from_params(
        &atp,
        StorageIndex(0),
        &Default::default(),
        &Default::default(),
        &mut bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut Vec::new(),
    )
    .unwrap();
    assert!(t.is_v2_only());
    let s = bitdaemon_session::settings::SessionSettings::default();
    let mut eff = Vec::new();
    assert_eq!(
        t.verify_v2_blocks(
            PieceIndex(0),
            &blocks,
            &s,
            &bitdaemon_session::counters::Counters::default(),
            &mut eff
        ),
        Some(true)
    );

    // corrupt one block hash → piece must fail
    let mut bad = blocks.clone();
    bad[2] = Sha256Hash::from_bytes(&[0xff; 32]).unwrap();
    assert_eq!(
        t.verify_v2_blocks(
            PieceIndex(0),
            &bad,
            &s,
            &bitdaemon_session::counters::Counters::default(),
            &mut eff
        ),
        Some(false)
    );

    // resume round-trip exports + restores the merkle state
    let _ = t.verify_v2_blocks(
        PieceIndex(0),
        &blocks,
        &s,
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    let mut atp2 = Default::default();
    t.to_resume_data(&mut atp2, &bitdaemon_net::peer_class::PeerClassPool::new());
    assert_eq!(atp2.merkle_trees.len(), 1);
    assert!(!atp2.merkle_trees[0].is_empty());
    let blob = bitdaemon_core::resume_data::write_resume_data_buf(&atp2);
    let atp3 = bitdaemon_core::resume_data::read_resume_data(&blob, &LoadTorrentLimits::default())
        .unwrap();
    let mut t2 = Torrent::from_params(
        &atp,
        StorageIndex(0),
        &Default::default(),
        &Default::default(),
        &mut bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut Vec::new(),
    )
    .unwrap();
    t2.apply_resume_data(
        &atp3,
        &Default::default(),
        &Default::default(),
        &mut Vec::new(),
    );
    // the block leaf verified before save must still be in the tree
    assert!(t2.merkle_trees[0].has_node(0));
}

#[test]
fn v2_hash_request_serve_and_add() {
    use bitdaemon_common::Sha256Hash;
    use bitdaemon_common::types::{FileIndex, StorageIndex};
    use bitdaemon_core::create_torrent::{CreateFileEntry, CreateFlags, CreateTorrent};
    use bitdaemon_core::merkle::merkle_root;
    use bitdaemon_core::torrent_info::{LoadTorrentLimits, load_torrent_buffer};
    use bitdaemon_net::peer_wire::{HashRequestWire, PeerMessage};
    use bitdaemon_session::torrent::{Torrent, TorrentEffect};
    use sha2::{Digest as _, Sha256};

    // 2-piece v2 file (8 blocks) — the piece layer (2 hashes) is a
    // real servable layer below the root.
    let data: Vec<u8> = (0..8 * 16384).map(|i| (i % 251) as u8).collect();
    let blocks: Vec<Sha256Hash> = data
        .chunks(16384)
        .map(|c| Sha256Hash::from_bytes(&Sha256::digest(c)).unwrap())
        .collect();
    let piece_roots: Vec<Sha256Hash> = (0..2)
        .map(|p| merkle_root(&blocks[p * 4..p * 4 + 4], Sha256Hash::default()))
        .collect();
    let root = merkle_root(&piece_roots, Sha256Hash::default());
    let mk = || {
        let mut ct = CreateTorrent::new(
            vec![CreateFileEntry {
                filename: b"v2f".to_vec(),
                size: 8 * 16384,
                flags: bitdaemon_core::file_storage::FileFlags::NONE,
                mtime: 0,
                symlink: Vec::new(),
            }],
            4 * 16384,
            CreateFlags::V2_ONLY,
        )
        .unwrap();
        ct.set_hash2(FileIndex(0), 0, piece_roots[0]).unwrap();
        ct.set_hash2(FileIndex(0), 1, piece_roots[1]).unwrap();
        ct.generate_buf().unwrap()
    };
    let buf = mk();
    let atp = load_torrent_buffer(&buf, &LoadTorrentLimits::default()).unwrap();
    let mut t = Torrent::from_params(
        &atp,
        StorageIndex(0),
        &Default::default(),
        &Default::default(),
        &mut bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut Vec::new(),
    )
    .unwrap();

    // a peer asks for the 4 leaf hashes → we serve them from the tree
    let req = HashRequestWire {
        file_root: root,
        base: 2,
        index: 0,
        count: 2,
        proof_layers: 0,
    };
    let mut out = Vec::new();
    t.on_hash_request(7, &req, &mut out);
    let TorrentEffect::SendTo { msg, .. } = &out[0] else {
        panic!("expected SendTo");
    };
    let PeerMessage::Hashes { hashes, .. } = msg else {
        panic!("expected Hashes");
    };
    assert_eq!(hashes, &piece_roots[..]);

    // unknown root → HashReject
    let mut out = Vec::new();
    let bad_req = HashRequestWire {
        file_root: Sha256Hash::from_bytes(&[9; 32]).unwrap(),
        ..req.clone()
    };
    t.on_hash_request(7, &bad_req, &mut out);
    assert!(matches!(
        out[0],
        TorrentEffect::SendTo {
            msg: PeerMessage::HashReject(_),
            ..
        }
    ));

    // receive the same hashes → trees accept → piece verifies.
    // Upstream `on_hashes` drops payloads that match no outstanding
    // `m_hash_requests` — model the conn + the request having been
    // sent.
    t.peers.insert(
        7,
        bitdaemon_session::peer_conn::PeerConn::new(
            10,
            bitdaemon_session::peer_pipeline::PeerPipeline::new(1000, 30, 250),
            0,
        ),
    );
    t.peers.get_mut(&7).unwrap().hash_requests.push(req.clone());
    let mut out = Vec::new();
    t.on_hashes(
        7,
        &req,
        &piece_roots,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    let _ = out;
}

#[test]
fn hybrid_multifile_pad_files_dont_panic() {
    // real-swarm catch: multi-file hybrid torrents carry `.pad/N`
    // entries (canonical BEP-52 layout) — build_merkle_trees iterated
    // file_range() into file_num_blocks()/root() which assert on pads.
    use bitdaemon_common::Sha256Hash;
    use bitdaemon_common::types::{FileIndex, PieceIndex, StorageIndex};
    use bitdaemon_core::create_torrent::{CreateFileEntry, CreateFlags, CreateTorrent};
    use bitdaemon_core::file_storage::FileFlags;
    use bitdaemon_core::merkle::merkle_root;
    use bitdaemon_core::torrent_info::{LoadTorrentLimits, load_torrent_buffer};
    use bitdaemon_session::torrent::Torrent;
    use sha2::{Digest as _, Sha256};

    let leaf =
        |i: usize| Sha256Hash::from_bytes(&Sha256::digest([(i % 251) as u8; 16384])).unwrap();

    // f0 = 20_000 B (2 leafs, 1 piece), f1 = 70_000 B (5 leafs, 2
    // pieces), empty = 0 B; canonicalize() pads each non-empty file to
    // the 64 KiB piece boundary. Empty files get no `pieces root`
    // upstream — build_merkle_trees must skip them like pads
    // (merkle_num_leafs(0) asserts).
    let entries = [
        (b"t/f0.bin" as &[u8], 20_000i64),
        (b"t/f1.bin" as &[u8], 70_000i64),
        (b"t/empty.bin" as &[u8], 0i64),
    ]
    .iter()
    .map(|(name, size)| CreateFileEntry {
        filename: name.to_vec(),
        size: *size,
        flags: FileFlags::NONE,
        mtime: 0,
        symlink: Vec::new(),
    })
    .collect::<Vec<_>>();
    let mut ct = CreateTorrent::new(entries, 4 * 16384, CreateFlags::default()).unwrap();
    for p in 0..3 {
        // nonzero v1 hashes — all-zero is rejected by validate
        ct.set_hash(
            PieceIndex(p),
            Sha1Hash::from_bytes(&[p as u8 + 1; 20]).unwrap(),
        )
        .unwrap();
    }
    // canonicalize sorts by path: t/empty.bin < t/f0.bin < t/f1.bin →
    // file indices 0=empty, 1=f0, 2=pad, 3=f1, 4=pad
    ct.set_hash2(
        FileIndex(1),
        0,
        merkle_root(&[leaf(0), leaf(1)], Sha256Hash::default()),
    )
    .unwrap();
    ct.set_hash2(
        FileIndex(3),
        0,
        merkle_root(&[leaf(0), leaf(1), leaf(2), leaf(3)], Sha256Hash::default()),
    )
    .unwrap();
    ct.set_hash2(
        FileIndex(3),
        1,
        merkle_root(&[leaf(4)], Sha256Hash::default()),
    )
    .unwrap();
    let buf = ct.generate_buf().unwrap();
    let atp = load_torrent_buffer(&buf, &LoadTorrentLimits::default()).unwrap();
    let num_files = atp.ti.as_ref().unwrap().files().num_files();

    // previously: assertion failed !f.pad_file at file_num_blocks,
    // then blocks > 0 at merkle_num_leafs for the empty file
    let t = Torrent::from_params(
        &atp,
        StorageIndex(0),
        &Default::default(),
        &Default::default(),
        &mut bitdaemon_net::peer_class::PeerClassPool::new(),
        &mut Vec::new(),
    )
    .unwrap();
    assert_eq!(t.merkle_trees.len(), num_files as usize);
    assert!(!t.is_v2_only());
}

/// upstream `on_check_files` alert gating (`torrent.cpp:2314-2323`):
/// `fastresume_rejected_alert` only posts when the params actually
/// carried resume data (`contains_resume_data`); a bare
/// `need_full_check` with no resume is silent. A *verification*
/// error rejects the resume but is not a fatal disk error —
/// `handle_disk_error` only runs for `fatal_disk_error`.
#[test]
fn check_files_rejected_gates_fastresume_alert() {
    use bitdaemon_common::types::FileIndex;
    use bitdaemon_disk::storage::disk_status;
    use bitdaemon_disk::storage_error::{Operation, StorageError};
    use bitdaemon_session::alerts::Alert;
    use bitdaemon_session::session::DiskCompleted;

    let need_full = i32::from(disk_status::NEED_FULL_CHECK);
    let fatal = i32::from(disk_status::FATAL_DISK_ERROR);
    let mk = |b: u8| -> (Sha1Hash, Session) {
        let ih = Sha1Hash::from_bytes(&[b; 20]).unwrap();
        let mut t = Torrent::new(ih, 2, 128 * 1024, Bitfield::with_size(2), StorageIndex(0));
        t.piece_hashes = Some(vec![Sha1Hash::from_bytes(&[0; 20]).unwrap(); 2]);
        t.init_picker(false);
        let mut s = session();
        s.add_torrent(t);
        s.pop_alerts();
        (ih, s)
    };

    // 1) no resume data + need_full_check → full recheck, no alert
    let (ih, mut s) = mk(11);
    s.disk_completions()
        .send(DiskCompleted::CheckFiles {
            ih,
            accepted: false,
            status: need_full,
            had_resume: false,
            have_bits: 0,
            err: StorageError::default(),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    assert!(s.torrents[&ih].checking, "rejected → real check armed");
    assert!(
        !s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::FastresumeRejected { .. })),
        "no resume data → no rejection alert"
    );

    // 2) resume data + need_full_check → alert posts
    let (ih, mut s) = mk(12);
    s.disk_completions()
        .send(DiskCompleted::CheckFiles {
            ih,
            accepted: false,
            status: need_full,
            had_resume: true,
            have_bits: 2,
            err: StorageError::default(),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::FastresumeRejected { .. })),
        "rejected resume → alert"
    );

    // 3) nonfatal verify error + resume → rejected + alert, but the
    //    torrent is NOT paused / not dropped from auto-manage
    let (ih, mut s) = mk(13);
    s.torrents.get_mut(&ih).unwrap().auto_managed = true;
    let mismatch = StorageError::new(
        std::io::Error::new(std::io::ErrorKind::InvalidData, "file size mismatch"),
        FileIndex(0),
        Operation::CheckResume,
    );
    s.disk_completions()
        .send(DiskCompleted::CheckFiles {
            ih,
            accepted: false,
            status: need_full,
            had_resume: true,
            have_bits: 2,
            err: mismatch,
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    let t = &s.torrents[&ih];
    assert!(t.checking, "rejected → recheck");
    assert!(!t.is_paused(), "verify mismatch is not fatal");
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::FastresumeRejected { .. })),
        "verify error + resume → alert"
    );

    // 4) fatal flag → on_disk_error path (paused + dropped)
    let (ih, mut s) = mk(14);
    s.torrents.get_mut(&ih).unwrap().auto_managed = true;
    s.disk_completions()
        .send(DiskCompleted::CheckFiles {
            ih,
            accepted: false,
            status: fatal,
            had_resume: true,
            have_bits: 2,
            err: StorageError::new(
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
                FileIndex(0),
                Operation::FileOpen,
            ),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    let t = &s.torrents[&ih];
    assert!(t.is_paused(), "fatal → pause");
    assert!(!t.auto_managed, "fatal → out of auto-manage");
}
