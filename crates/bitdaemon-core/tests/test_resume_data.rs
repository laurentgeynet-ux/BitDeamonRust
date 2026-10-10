//! Port of `test/test_read_resume.cpp` — wire-format cases + round-trip.
//! The resume format is byte-compatible with libtorrent (ADR-0009), so
//! these vectors pin both directions.
#![allow(clippy::field_reassign_with_default)]

use std::net::SocketAddr;

use bitdaemon_bencode::{Entry, bdecode, bencode};
use bitdaemon_common::flags::TorrentFlags;
use bitdaemon_common::types::DownloadPriority;
use bitdaemon_common::{Bitfield, Sha1Hash, Sha256Hash};
use bitdaemon_core::add_torrent_params::{AddTorrentParams, StorageMode};
use bitdaemon_core::error::Error;
use bitdaemon_core::resume_data::{read_resume_data, write_resume_data_buf};
use bitdaemon_core::torrent_info::LoadTorrentLimits;

fn parse(buf: &[u8]) -> Result<AddTorrentParams, Error> {
    read_resume_data(buf, &LoadTorrentLimits::default())
}

fn dict(pairs: &[(&[u8], Entry)]) -> Entry {
    Entry::Dict(pairs.iter().map(|(k, v)| (k.to_vec(), v.clone())).collect())
}

fn s(v: &[u8]) -> Entry {
    Entry::String(v.to_vec())
}

/// Upstream `read_resume` — v1 format, 1 byte/piece, flags off.
#[test]
fn read_resume_v1() {
    let rd = dict(&[
        (b"file-format", s(b"libtorrent resume file")),
        (b"file-version", Entry::Int(1)),
        (b"info-hash", s(b"abcdefghijklmnopqrst")),
        (b"pieces", s(b"\x01\x01\x01\x01\x01\x01")),
        (b"total_uploaded", Entry::Int(1337)),
        (b"total_downloaded", Entry::Int(1338)),
        (b"active_time", Entry::Int(1339)),
        (b"seeding_time", Entry::Int(1340)),
        (b"upload_rate_limit", Entry::Int(1343)),
        (b"download_rate_limit", Entry::Int(1344)),
        (b"max_connections", Entry::Int(1345)),
        (b"max_uploads", Entry::Int(1346)),
        (b"seed_mode", Entry::Int(0)),
        (b"i2p", Entry::Int(0)),
        (b"super_seeding", Entry::Int(0)),
        (b"added_time", Entry::Int(1347)),
        (b"completed_time", Entry::Int(1348)),
        (b"finished_time", Entry::Int(1352)),
        (b"last_seen_complete", Entry::Int(1353)),
        (b"piece_priority", s(b"\x01\x02\x03\x04\x05\x06")),
        (b"auto_managed", Entry::Int(0)),
        (b"sequential_download", Entry::Int(0)),
        (b"paused", Entry::Int(0)),
    ]);
    let atp = parse(&bencode(&rd)).unwrap();

    assert_eq!(
        atp.info_hashes.v1,
        Sha1Hash::from_bytes(b"abcdefghijklmnopqrst").unwrap()
    );
    assert_eq!(atp.have_pieces.size(), 6);
    assert_eq!(atp.have_pieces.count(), 6);
    assert_eq!(atp.total_uploaded, 1337);
    assert_eq!(atp.total_downloaded, 1338);
    assert_eq!(atp.active_time, 1339);
    assert_eq!(atp.seeding_time, 1340);
    assert_eq!(atp.upload_limit, 1343);
    assert_eq!(atp.download_limit, 1344);
    assert_eq!(atp.max_connections, 1345);
    assert_eq!(atp.max_uploads, 1346);

    let flags_mask = TorrentFlags::SEED_MODE
        | TorrentFlags::SUPER_SEEDING
        | TorrentFlags::AUTO_MANAGED
        | TorrentFlags::PAUSED
        | TorrentFlags::I2P_TORRENT
        | TorrentFlags::SEQUENTIAL_DOWNLOAD;
    assert_eq!(atp.flags & flags_mask, TorrentFlags::NONE);

    assert_eq!(atp.added_time, 1347);
    assert_eq!(atp.completed_time, 1348);
    assert_eq!(atp.finished_time, 1352);
    assert_eq!(atp.last_seen_complete, 1353);

    assert_eq!(
        atp.piece_priorities,
        [1u8, 2, 3, 4, 5, 6]
            .into_iter()
            .map(DownloadPriority)
            .collect::<Vec<_>>()
    );
}

#[test]
fn missing_info_hash() {
    let rd = dict(&[
        (b"file-format", s(b"libtorrent resume file")),
        (b"file-version", Entry::Int(1)),
    ]);
    assert!(matches!(
        parse(&bencode(&rd)),
        Err(Error::Torrent(
            bitdaemon_core::TorrentError::MissingInfoHash
        ))
    ));
}

#[test]
fn v2_only_info_hash_ok() {
    let rd = dict(&[
        (b"file-format", s(b"libtorrent resume file")),
        (b"file-version", Entry::Int(1)),
        (b"info-hash2", s(b"01234567890123456789012345678901")),
    ]);
    let atp = parse(&bencode(&rd)).unwrap();
    assert!(atp.info_hashes.has_v2());
    assert!(!atp.info_hashes.has_v1());
}

#[test]
fn missing_file_format() {
    let rd = dict(&[
        (b"file-version", Entry::Int(1)),
        (b"info-hash", s(b"abcdefghijklmnopqrst")),
    ]);
    assert!(matches!(
        parse(&bencode(&rd)),
        Err(Error::Torrent(bitdaemon_core::TorrentError::InvalidFileTag))
    ));
}

/// `mismatching_v1_hash` — embedded info dict whose hash doesn't match
/// the declared `info-hash` must be rejected.
#[test]
fn mismatching_v1_hash() {
    // minimal valid v1 info dict (single file)
    let info = dict(&[
        (b"length", Entry::Int(1024)),
        (b"name", s(b"test")),
        (b"piece length", Entry::Int(16384)),
        (b"pieces", s(&[0u8; 20])),
    ]);
    let rd = dict(&[
        (b"file-format", s(b"libtorrent resume file")),
        (b"file-version", Entry::Int(2)),
        (b"info-hash", s(b"abababababababababab")),
        (b"info", info),
    ]);
    assert!(matches!(
        parse(&bencode(&rd)),
        Err(Error::Torrent(
            bitdaemon_core::TorrentError::MismatchingInfoHash
        ))
    ));
}

/// `test_roundtrip` upstream — write then read, everything survives.
#[test]
fn roundtrip() {
    let mut input = AddTorrentParams::default();
    input.name = b"test".to_vec();
    input.info_hashes.v1 = Sha1Hash::from_bytes(&[0xab; 20]).unwrap();
    input.info_hashes.v2 = Sha256Hash::from_bytes(&[0xcd; 32]).unwrap();
    input.save_path = b"/some/path".to_vec();
    input.part_file_dir = b"/some/part".to_vec();
    input.storage_mode = StorageMode::Allocate;
    input.trackers = vec![
        b"http://t1/announce".to_vec(),
        b"http://t2/announce".to_vec(),
        b"udp://t3:80/announce".to_vec(),
    ];
    input.tracker_tiers = vec![0, 0, 3];
    input.url_seeds = vec![b"http://seed/".to_vec()];
    input.dht_nodes = vec![(b"node".to_vec(), 6881i32)];

    let mut have = Bitfield::with_size(11);
    have.set_bit(0);
    have.set_bit(3);
    have.set_bit(10);
    have.resize(have.num_bytes() * 8);
    input.have_pieces = have;
    let mut verified = Bitfield::with_size(16);
    verified.set_bit(0);
    verified.set_bit(5);
    input.verified_pieces = verified;

    input.file_priorities = vec![
        DownloadPriority(1),
        DownloadPriority(0),
        DownloadPriority(4),
        DownloadPriority(7),
    ];
    input.piece_priorities = vec![DownloadPriority(3), DownloadPriority(5)];

    input.renamed_files.insert(1, b"renamed".to_vec());
    input.peers = vec![
        "1.2.3.4:6881".parse::<SocketAddr>().unwrap(),
        "[::1]:42".parse::<SocketAddr>().unwrap(),
    ];
    input.banned_peers = vec!["5.6.7.8:1".parse::<SocketAddr>().unwrap()];

    let mut unfinished = Bitfield::with_size(8);
    unfinished.set_bit(2);
    input.unfinished_pieces.insert(7, unfinished);

    input.merkle_trees = vec![vec![
        Sha256Hash::from_bytes(&[1u8; 32]).unwrap(),
        Sha256Hash::from_bytes(&[2u8; 32]).unwrap(),
    ]];
    let mut mask = Bitfield::with_size(8);
    mask.set_bit(0);
    input.merkle_tree_mask = vec![mask];
    let mut vl = Bitfield::with_size(8);
    vl.set_bit(1);
    input.verified_leaf_hashes = vec![vl];

    input.total_uploaded = 1;
    input.total_downloaded = 2;
    input.active_time = 3;
    input.finished_time = 4;
    input.seeding_time = 5;
    input.last_seen_complete = 6;
    input.last_download = 7;
    input.last_upload = 8;
    input.added_time = 9;
    input.completed_time = 10;
    input.num_complete = 11;
    input.num_incomplete = 12;
    input.num_downloaded = 13;
    input.max_uploads = 14;
    input.max_connections = 15;
    input.upload_limit = 16;
    input.download_limit = 17;
    input.creation_date = 18;
    input.created_by = b"bitdaemon".to_vec();
    input.comment = b"a comment".to_vec();
    input.flags = TorrentFlags::PAUSED
        | TorrentFlags::AUTO_MANAGED
        | TorrentFlags::SEQUENTIAL_DOWNLOAD
        | TorrentFlags::DISABLE_DHT;

    let buf = write_resume_data_buf(&input);
    let out = parse(&buf).unwrap();

    assert_eq!(out.info_hashes, input.info_hashes);
    assert_eq!(out.name, input.name);
    assert_eq!(out.save_path, input.save_path);
    assert_eq!(out.part_file_dir, input.part_file_dir);
    assert_eq!(out.storage_mode, input.storage_mode);
    assert_eq!(out.trackers, input.trackers);
    // tiers are re-spread into lists: tier 0,0,3 → read back 0,0,3
    // (clamped ≤1024) — upstream same
    assert_eq!(out.tracker_tiers, input.tracker_tiers);
    assert_eq!(out.url_seeds, input.url_seeds);
    assert_eq!(out.have_pieces, input.have_pieces);
    assert_eq!(out.verified_pieces, input.verified_pieces);
    assert_eq!(out.file_priorities, input.file_priorities);
    assert_eq!(out.piece_priorities, input.piece_priorities);
    assert_eq!(out.renamed_files, input.renamed_files);
    assert_eq!(out.peers, input.peers);
    assert_eq!(out.banned_peers, input.banned_peers);
    assert_eq!(out.unfinished_pieces, input.unfinished_pieces);
    assert_eq!(out.merkle_trees, input.merkle_trees);
    assert_eq!(out.verified_leaf_hashes, input.verified_leaf_hashes);
    assert_eq!(out.merkle_tree_mask, input.merkle_tree_mask);
    assert_eq!(out.total_uploaded, 1);
    assert_eq!(out.total_downloaded, 2);
    assert_eq!(out.active_time, 3);
    assert_eq!(out.finished_time, 4);
    assert_eq!(out.seeding_time, 5);
    assert_eq!(out.last_seen_complete, 6);
    assert_eq!(out.last_download, 7);
    assert_eq!(out.last_upload, 8);
    assert_eq!(out.added_time, 9);
    assert_eq!(out.completed_time, 10);
    assert_eq!(out.num_complete, 11);
    assert_eq!(out.num_incomplete, 12);
    assert_eq!(out.num_downloaded, 13);
    assert_eq!(out.max_uploads, 14);
    assert_eq!(out.max_connections, 15);
    assert_eq!(out.upload_limit, 16);
    assert_eq!(out.download_limit, 17);
    assert_eq!(out.creation_date, 18);
    assert_eq!(out.created_by, input.created_by);
    assert_eq!(out.comment, input.comment);
    // written flags read back; NEED_SAVE_RESUME was never set anyway
    assert!(out.flags.contains(TorrentFlags::PAUSED));
    assert!(out.flags.contains(TorrentFlags::AUTO_MANAGED));
    assert!(out.flags.contains(TorrentFlags::SEQUENTIAL_DOWNLOAD));
    assert!(out.flags.contains(TorrentFlags::DISABLE_DHT));
    assert!(!out.flags.contains(TorrentFlags::SEED_MODE));
    assert!(!out.flags.contains(TorrentFlags::NEED_SAVE_RESUME));
    // resume-file flags are set by the read path itself
    assert!(
        out.flags
            .contains(TorrentFlags::DEPRECATED_OVERRIDE_TRACKERS)
    );
    assert!(
        out.flags
            .contains(TorrentFlags::DEPRECATED_OVERRIDE_WEB_SEEDS)
    );
}

/// `verified` bitfield with all-zero bits must not be written.
#[test]
fn all_zero_verified_skipped() {
    let mut p = AddTorrentParams::default();
    p.info_hashes.v1 = Sha1Hash::from_bytes(&[0xab; 20]).unwrap();
    p.verified_pieces = Bitfield::with_size(16); // all zero
    let buf = write_resume_data_buf(&p);
    let node = bdecode(&buf);
    assert_eq!(
        node.dict_find_string(b"verified").node_type(),
        bitdaemon_bencode::NodeType::None
    );
}

/// Seed mode dropped when a file is deselected (upstream sanity check).
#[test]
fn deselected_file_clears_seed_mode() {
    let rd = dict(&[
        (b"file-format", s(b"libtorrent resume file")),
        (b"file-version", Entry::Int(2)),
        (b"info-hash", s(b"abcdefghijklmnopqrst")),
        (b"seed_mode", Entry::Int(1)),
        (
            b"file_priority",
            Entry::List(vec![Entry::Int(0), Entry::Int(4)]),
        ),
    ]);
    let atp = parse(&bencode(&rd)).unwrap();
    assert!(!atp.flags.contains(TorrentFlags::SEED_MODE));
}
