// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `Session::add_torrent_params` — AddTorrentParams → Torrent
//! (metadata, resume, peers, banned, paused).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bitdaemon_bencode::bdecode_checked;
use bitdaemon_common::flags::{PeerSourceFlags, TorrentFlags};
use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{InfoHash, Sha1Hash};
use bitdaemon_core::add_torrent_params::AddTorrentParams;
use bitdaemon_core::torrent_info::{LoadTorrentLimits, TorrentInfo};
use bitdaemon_session::session::Session;

fn info_bytes(hashes: &[u8]) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend(b"d6:lengthi24e4:name4:test12:piece lengthi16384e6:pieces20:");
    v.extend(hashes);
    v.extend(b"e");
    v
}

fn ep(a: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, a)), 6881)
}

#[test]
fn add_magnet_params() {
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let atp = AddTorrentParams {
        info_hashes: InfoHash::from_v1(ih),
        peers: vec![ep(1), ep(2)],
        banned_peers: vec![ep(9)],
        flags: TorrentFlags::PAUSED,
        ..Default::default()
    };

    let mut s = Session::new();
    let mut out = Vec::new();
    assert_eq!(
        s.add_torrent_params(&atp, StorageIndex(0), &mut out),
        Some(ih)
    );
    let t = s.torrents.get(&ih).unwrap();
    assert!(t.magnet && t.paused);
    assert_eq!(t.peer_list.num_peers(), 3);
    assert_eq!(t.peer_list.num_connect_candidates(), 2);
}

#[test]
fn add_torrent_with_metadata_and_resume() {
    let info = info_bytes(&[0x77; 20]);
    let node = bdecode_checked(&info).unwrap();
    let ti = TorrentInfo::from_info_node(&node, &LoadTorrentLimits::default()).unwrap();
    let ih = ti.info_hash_v1().unwrap();

    let atp = AddTorrentParams {
        ti: Some(Arc::new(ti)),
        info_hashes: InfoHash::from_v1(ih),
        peers: vec![ep(1)],
        ..Default::default()
    };

    let mut s = Session::new();
    let mut out = Vec::new();
    assert_eq!(
        s.add_torrent_params(&atp, StorageIndex(0), &mut out),
        Some(ih)
    );
    let t = s.torrents.get(&ih).unwrap();
    assert!(!t.magnet);
    assert_eq!(t.num_pieces, 1);
    assert!(t.picker.is_some());
    assert_eq!(
        t.expected_hash(PieceIndex(0)),
        Some(Sha1Hash::from_bytes(&[0x77; 20]).unwrap())
    );
    // resume-fed peer is tagged RESUME_DATA
    let i = t.peer_list.find_peer(ep(1).ip()).unwrap();
    assert!(
        t.peer_list.peers()[i]
            .source
            .contains(PeerSourceFlags::RESUME_DATA)
    );
}

#[test]
fn files_checked_verdict() {
    let mut t = bitdaemon_session::torrent::Torrent::new(
        Sha1Hash::from_bytes(&[5; 20]).unwrap(),
        2,
        128 * 1024,
        bitdaemon_common::Bitfield::with_size(2),
        StorageIndex(0),
    );
    t.init_picker(false);
    let mut eff = Vec::new();
    // resume accepted, all pieces had → seed
    t.have.set_bit(0);
    t.have.set_bit(1);
    t.on_files_checked(
        true,
        &Default::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert!(t.seeding && t.finished);
    assert!(
        eff.iter()
            .any(|e| matches!(e, bitdaemon_session::torrent::TorrentEffect::BecameSeed))
    );
    // rejected → have cleared
    t.on_files_checked(
        false,
        &Default::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut eff,
    );
    assert_eq!(t.have.count(), 0);
    assert!(!t.finished && !t.seeding);
}

/// A23 D1 — `atp.root_certificate` marks the torrent SSL and wins
/// over the embedded `ssl-cert` for the trust root
/// (`torrent.cpp:1977-1986`).
#[test]
fn add_torrent_root_certificate_override() {
    let mut info = Vec::new();
    info.extend(b"d6:lengthi24e4:name4:test12:piece lengthi16384e6:pieces20:");
    info.extend([0x77; 20]);
    info.extend(b"8:ssl-cert11:embedded-cae");
    let node = bdecode_checked(&info).unwrap();
    let ti = TorrentInfo::from_info_node(&node, &LoadTorrentLimits::default()).unwrap();
    let ih = ti.info_hash_v1().unwrap();

    // embedded `ssl-cert` alone → SSL torrent
    let atp = AddTorrentParams {
        ti: Some(Arc::new(ti.clone())),
        info_hashes: InfoHash::from_v1(ih),
        ..Default::default()
    };
    let mut s = Session::new();
    let mut out = Vec::new();
    s.add_torrent_params(&atp, StorageIndex(0), &mut out);
    let t = s.torrents.get(&ih).unwrap();
    assert!(t.is_ssl_torrent());
    assert_eq!(t.ssl_root_cert(), Some(b"embedded-ca".as_slice()));

    // `root_certificate` preferred (`torrent.cpp:1977-1979`)
    let atp = AddTorrentParams {
        ti: Some(Arc::new(ti)),
        info_hashes: InfoHash::from_v1(ih),
        root_certificate: b"override-root".to_vec(),
        ..Default::default()
    };
    let mut s = Session::new();
    let mut out = Vec::new();
    s.add_torrent_params(&atp, StorageIndex(0), &mut out);
    let t = s.torrents.get(&ih).unwrap();
    assert!(t.is_ssl_torrent());
    assert_eq!(t.ssl_root_cert(), Some(b"override-root".as_slice()));
}
