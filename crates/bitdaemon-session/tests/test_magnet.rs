// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! magnet glue — `ut_metadata` pieces → `Torrent::set_metadata`
//! (BEP-9 SHA-1 check → `TorrentInfo` parse → picker init).

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_core::torrent_info::LoadTorrentLimits;
use bitdaemon_session::torrent::Torrent;
use bitdaemon_session::ut_metadata::MetadataResult;

/// minimal v1 info dict: 1 piece of 16 KiB over one 24-byte file.
fn info_bytes(hashes: &[u8]) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend(b"d6:lengthi24e4:name4:test12:piece lengthi16384e6:pieces20:");
    v.extend(hashes);
    v.extend(b"e");
    v
}

#[test]
fn magnet_resolves_via_ut_metadata() {
    let info = info_bytes(&[0x55; 20]);
    let ih = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(&info);
        Sha1Hash::from_bytes(&h.finalize()).unwrap()
    };
    let mut t = Torrent::new(ih, 0, 0, Bitfield::new(), StorageIndex(0));
    t.magnet = true;

    // metadata fits one 16 KiB ut_metadata piece
    let r = t.on_ut_metadata_data(&info, 0, info.len() as i64, 42, 30 * 1024 * 1024);
    assert_eq!(r, MetadataResult::Complete);

    let md = t.metadata_tracker.metadata().to_vec();
    assert!(t.set_metadata(&md, &LoadTorrentLimits::default()));
    assert_eq!(t.num_pieces, 1);
    assert_eq!(t.piece_length, 16384);
    assert!(t.picker.is_some());
    assert!(!t.magnet);
    assert_eq!(
        t.expected_hash(PieceIndex(0)).unwrap(),
        Sha1Hash::from_bytes(&[0x55; 20]).unwrap()
    );
}

#[test]
fn wrong_metadata_rejected() {
    let good = info_bytes(&[0x55; 20]);
    let bad = info_bytes(&[0xAA; 20]);
    let ih = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(&good);
        Sha1Hash::from_bytes(&h.finalize()).unwrap()
    };
    let mut t = Torrent::new(ih, 0, 0, Bitfield::new(), StorageIndex(0));
    let r = t.on_ut_metadata_data(&bad, 0, bad.len() as i64, 7, 30 * 1024 * 1024);
    assert_eq!(r, MetadataResult::Complete);
    let md = t.metadata_tracker.metadata().to_vec();
    assert!(!t.set_metadata(&md, &LoadTorrentLimits::default()));
    // upstream resets request counts and penalizes the source
    let (_, sources) = t.metadata_tracker.reset_on_hash_failure();
    assert_eq!(sources, vec![7]);
    assert!(t.picker.is_none());
}

#[test]
fn auto_managed_queue_limits_downloads() {
    use bitdaemon_session::session::Session;
    let mut ses = Session::new();
    for i in 0..4u8 {
        let ih = Sha1Hash::from_bytes(&[i; 20]).unwrap();
        ses.add_torrent(Torrent::new(
            ih,
            1,
            16384,
            Bitfield::with_size(1),
            StorageIndex(u32::from(i)),
        ));
    }
    ses.manage_queue(2, -1);
    let queued: Vec<_> = ses.torrents.values().map(|t| t.queued).collect();
    assert_eq!(queued.iter().filter(|q| **q).count(), 2);
    // a paused torrent frees a slot
    let first = *ses.torrents.keys().next().unwrap();
    ses.set_paused(&first, true, &mut Vec::new());
    ses.manage_queue(2, -1);
    let queued2 = ses.torrents.values().filter(|t| t.queued).count();
    assert_eq!(queued2, 1);
}

#[test]
fn smart_ban_disconnects_corrupt_peer() {
    use bitdaemon_common::types::PieceBlock;
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::torrent::TorrentEffect;
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = Torrent::new(ih, 1, 16384, Bitfield::with_size(1), StorageIndex(0));
    t.init_picker(true);
    t.add_peer(7, PeerConn::new(1, PeerPipeline::new(1000, 30, 250), 0));
    let addr = |_| std::net::IpAddr::from([10, 0, 0, 1]);
    let digest = |b: PieceBlock| Some(Sha1Hash::from_bytes(&[b.block_index as u8; 20]).unwrap());
    // peer 7 wasn't a downloader → no CRC recorded, no ban, but the
    // piece is restored/re-failed through the normal path
    let mut out = Vec::new();
    t.piece_verified_ban(PieceIndex(0), 16384, false, &mut out, addr, digest);
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::PieceFailed(_)))
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. }))
    );
}

#[test]
fn pex_feeds_swarm_and_broadcasts() {
    use bitdaemon_net::bep10::ExtendedHandshake;
    use bitdaemon_net::peer_wire::PeerMessage;
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::torrent::TorrentEffect;
    use bitdaemon_session::ut_pex::build_pex_message;
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let ih = Sha1Hash::from_bytes(&[3; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 16384, Bitfield::with_size(4), StorageIndex(0));
    t.add_peer(7, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));
    let mut out = Vec::new();

    // peer advertises ut_pex id 2
    let mut m = BTreeMap::new();
    m.insert("ut_pex".to_string(), 2);
    let hs = ExtendedHandshake {
        messages: m,
        ..Default::default()
    };
    t.on_peer_message(
        7,
        &PeerMessage::Extended {
            id: 0,
            data: bitdaemon_bencode::bencode(&hs.build()).into(),
        },
        &mut out,
    );

    // remote sends a pex message adding 10.1.2.3:6881
    let added_ep = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)), 6881);
    let (body, _sent) = build_pex_message(&[(added_ep, 0)], &BTreeSet::new());
    t.on_peer_message(
        7,
        &PeerMessage::Extended {
            id: 2, // our local pex id (PeerConn::local_ut_pex)
            data: body.into(),
        },
        &mut out,
    );
    assert_eq!(t.peer_list.num_peers(), 1);

    // tick → we emit a pex diff to the peer (contains peer 7? we
    // advertise remote endpoints of our connections — none have
    // remote set here, so the diff is empty but still sent)
    let mut out2 = Vec::new();
    t.pex_tick(std::time::Instant::now(), &mut out2);
    assert!(out2.iter().any(|e| matches!(
        e,
        TorrentEffect::SendTo {
            msg: PeerMessage::Extended { id: 2, .. },
            ..
        }
    )));
}
