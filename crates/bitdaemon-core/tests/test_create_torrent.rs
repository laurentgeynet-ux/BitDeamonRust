//! Conformance port of `test_create_torrent.cpp` (piece-size rules +
//! info-dict round-trip through `write_torrent_file`).
#![allow(clippy::identity_op)]

use bitdaemon_bencode::{NodeType, bdecode};
use bitdaemon_core::create_torrent::{CreateFileEntry, CreateFlags, CreateTorrent, canonicalize};
use bitdaemon_core::error::{Error, TorrentError};
use bitdaemon_core::file_storage::FileFlags;
use bitdaemon_core::resume_data::{WriteTorrentFlags, write_torrent_file_buf};
use bitdaemon_core::torrent_info::{LoadTorrentLimits, load_torrent_buffer};

fn fe(path: &str, size: i64) -> CreateFileEntry {
    CreateFileEntry {
        filename: path.as_bytes().to_vec(),
        size,
        flags: FileFlags::NONE,
        mtime: 0,
        symlink: Vec::new(),
    }
}

fn test_piece_size(piece_size: i32, flags: CreateFlags) -> Result<i32, Error> {
    let files = vec![fe("a", 100 * 1024 * 1024)];
    Ok(CreateTorrent::new(files, piece_size, flags)?.piece_length())
}

#[test]
fn auto_piece_size() {
    const KIB: i64 = 1024;
    const MIB: i64 = 1024 * 1024;
    const GIB: i64 = 1024 * 1024 * 1024;
    let samples = [
        (100i64, 16 * KIB as i32),
        (3 * MIB, 32 * KIB as i32),
        (11 * MIB, 64 * KIB as i32),
        (43 * MIB, 128 * KIB as i32),
        (172 * MIB, 256 * KIB as i32),
        (688 * MIB, 512 * KIB as i32),
        (3 * GIB, 1 * MIB as i32),
        (11 * GIB, 2 * MIB as i32),
        (44 * GIB, 4 * MIB as i32),
        (176 * GIB, 8 * MIB as i32),
        (704 * GIB, 16 * MIB as i32),
    ];
    for (total, expect) in samples {
        let ct = CreateTorrent::new(vec![fe("a", total)], 0, CreateFlags::NONE).unwrap();
        assert_eq!(ct.piece_length(), expect, "total {total}");
    }
}

#[test]
fn piece_size_restriction_16kb() {
    assert_eq!(
        test_piece_size(15000, CreateFlags::NONE).unwrap(),
        16 * 1024
    );
    assert_eq!(test_piece_size(500, CreateFlags::NONE).unwrap(), 16 * 1024);
    assert!(test_piece_size(15000, CreateFlags::V1_ONLY).is_err());
    assert!(test_piece_size(8000, CreateFlags::V1_ONLY).is_err());
    assert_eq!(test_piece_size(8192, CreateFlags::V1_ONLY).unwrap(), 8192);
}

#[test]
fn piece_size_quanta() {
    assert_eq!(
        test_piece_size(32 * 1024, CreateFlags::NONE).unwrap(),
        32 * 1024
    );
    assert_eq!(
        test_piece_size(32 * 1024, CreateFlags::V1_ONLY).unwrap(),
        32 * 1024
    );
    assert!(test_piece_size(48 * 1024, CreateFlags::NONE).is_err());
    assert_eq!(
        test_piece_size(48 * 1024, CreateFlags::V1_ONLY).unwrap(),
        48 * 1024
    );
    assert!(test_piece_size(47 * 1024, CreateFlags::V1_ONLY).is_err());
    assert!(test_piece_size(47 * 1024, CreateFlags::NONE).is_err());
}

#[test]
fn piece_size_limit() {
    assert_eq!(
        test_piece_size(0x8000000, CreateFlags::NONE).unwrap(),
        0x8000000
    );
    assert!(test_piece_size(0x8000001, CreateFlags::V1_ONLY).is_err());
    assert!(test_piece_size(0x10000000, CreateFlags::NONE).is_err());
}

#[test]
fn canonicalize_inserts_pads() {
    // two unaligned files → pad after first to piece-align second
    let files = vec![fe("test/a", 1000), fe("test/b", 1000)];
    // BEP-52: every file ends on a piece boundary — pad after each,
    // including the tail (since libtorrent 2.0.8)
    let (out, total) = canonicalize(files, 16384, false);
    // 3 entries: a + pad + b (the tail pad after the last file is only
    // inserted by the constructor path, canonicalize itself pads inside)
    assert_eq!(out.len(), 4);
    assert!(out[1].flags.contains(FileFlags::PAD_FILE));
    assert_eq!(out[1].size, 16384 - 1000);
    // platform separator between "test" and ".pad/…" like upstream
    let expect_pad = format!(
        "test{}.pad/15384",
        char::from(bitdaemon_core::path::SEPARATOR)
    );
    assert_eq!(out[1].filename, expect_pad.as_bytes());
    assert!(out[3].flags.contains(FileFlags::PAD_FILE));
    assert_eq!(total, 32768);
}

#[test]
fn canonicalize_no_tail_padding() {
    let files = vec![fe("test/a", 1000), fe("test/b", 1000)];
    let (out, total) = canonicalize(files, 16384, true);
    // backwards-compatible: pad BEFORE each file, none after the last
    assert_eq!(out.len(), 3);
    assert!(out[1].flags.contains(FileFlags::PAD_FILE));
    assert_eq!(out[2].filename, b"test/b");
    assert_eq!(total, 17384);
}

#[test]
fn v1_generate_round_trip() {
    let mut t = CreateTorrent::new(
        vec![fe("test/a", 0x4000), fe("test/b", 0x4000)],
        0x4000,
        CreateFlags::V1_ONLY,
    )
    .unwrap();
    for p in 0..t.num_pieces() {
        t.set_hash(
            bitdaemon_common::types::PieceIndex(p),
            bitdaemon_common::Sha1Hash::from_bytes(&[0xabu8; 20]).unwrap(),
        )
        .unwrap();
    }
    t.add_tracker(b"udp://testurl.com/announce", 0);
    t.set_comment(b"this is a test comment");
    t.set_creation_date(1337);

    let buf = t.generate_buf().unwrap();
    let atp = load_torrent_buffer(&buf, &LoadTorrentLimits::default()).unwrap();
    let ti = atp.ti.as_ref().unwrap();
    assert_eq!(ti.piece_length(), 0x4000);
    assert_eq!(atp.trackers, vec![b"udp://testurl.com/announce".to_vec()]);
    assert_eq!(atp.comment, b"this is a test comment");

    // write_torrent_file round-trip: info dict must be byte-identical
    let out = write_torrent_file_buf(&atp, WriteTorrentFlags::default()).unwrap();
    let in_node = bdecode(&buf);
    let out_node = bdecode(&out);
    assert!(in_node.error().is_none() && out_node.error().is_none());
    assert_eq!(
        in_node.dict_find(b"info").data_section(),
        out_node.dict_find(b"info").data_section()
    );
}

fn fixture_dir() -> Option<std::path::PathBuf> {
    let dir = std::path::PathBuf::from(
        std::env::var("LIBTORRENT_TEST_DIR")
            .unwrap_or_else(|_| r"D:\Projet\libtorrent\libtorrent\test\test_torrents".into()),
    );
    dir.is_dir().then_some(dir)
}

/// Upstream `test_round_trip_torrent` — every fixture's info dict +
/// piece layers must survive a load→write round-trip byte-identically.
#[test]
fn write_torrent_file_round_trip() {
    let Some(dir) = fixture_dir() else {
        eprintln!("skipping: no fixture dir");
        return;
    };
    let mut tested = 0;
    let mut skipped_missing_layers = 0;
    for name in [
        "base.torrent",
        "multi.torrent",
        "v2.torrent",
        "v2_hybrid.torrent",
        "v2_multipiece_file.torrent",
        "v2_multiple_files.torrent",
        "v2_hybrid-missing-tailpad.torrent",
        "base.torrent",
        "single_multi_file.torrent",
    ] {
        let path = dir.join(name);
        if !path.exists() {
            continue;
        }
        let buf = std::fs::read(&path).unwrap();
        let atp = match load_torrent_buffer(&buf, &LoadTorrentLimits::default()) {
            Ok(a) => a,
            Err(e) => panic!("{name}: load failed: {e}"),
        };
        let out = match write_torrent_file_buf(&atp, WriteTorrentFlags::default()) {
            Ok(o) => o,
            Err(Error::Torrent(TorrentError::TorrentMissingPieceLayer)) => {
                skipped_missing_layers += 1;
                continue;
            }
            Err(e) => panic!("{name}: write failed: {e}"),
        };
        let in_node = bdecode(&buf);
        let out_node = bdecode(&out);
        assert_eq!(
            in_node.dict_find(b"info").data_section(),
            out_node.dict_find(b"info").data_section(),
            "{name}: info section differs"
        );
        let in_layers = in_node.dict_find(b"piece layers");
        let out_layers = out_node.dict_find(b"piece layers");
        if in_layers.node_type() == NodeType::Dict {
            assert_eq!(
                in_layers.data_section(),
                out_layers.data_section(),
                "{name}: piece layers differ"
            );
        }
        tested += 1;
    }
    eprintln!(
        "{tested} fixtures round-tripped, {skipped_missing_layers} skipped (no piece layers)"
    );
    assert!(tested >= 6, "too few fixtures tested");
}
