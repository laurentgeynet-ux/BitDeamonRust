//! Port of `test_torrent_info.cpp` — `parse_torrents` named checks and
//! `parse_invalid_torrents` error-code table against upstream fixtures.
//!
//! Gated on `LIBTORRENT_TEST_DIR` (the libtorrent `test/` dir containing
//! `test_torrents/`); skipped otherwise. Synthetic cases (name
//! sanitization, v1/v2 hash) run unconditionally.

use std::path::PathBuf;

use bitdaemon_common::{FileIndex, Sha256Hash};
use bitdaemon_core::file_storage::FileFlags;
use bitdaemon_core::path::{SEPARATOR, combine_path};
use bitdaemon_core::torrent_info::{LoadTorrentLimits, load_torrent, parse_piece_layers};
use bitdaemon_core::{Error, TorrentError};

fn test_dir() -> Option<PathBuf> {
    std::env::var_os("LIBTORRENT_TEST_DIR").map(PathBuf::from)
}

fn torrent_path(file: &str) -> PathBuf {
    let root = test_dir().expect("LIBTORRENT_TEST_DIR not set");
    root.join("test_torrents").join(file)
}

fn load(name: &str) -> bitdaemon_core::AddTorrentParams {
    let buf = std::fs::read(torrent_path(name)).expect("read fixture");
    load_torrent(&buf).unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn sep(parts: &[&str]) -> Vec<u8> {
    let mut ret = Vec::new();
    for p in parts {
        if !ret.is_empty() {
            ret.push(SEPARATOR);
        }
        ret.extend_from_slice(p.as_bytes());
    }
    ret
}

/// `sanity_check` upstream — every fixture must parse and satisfy basic
/// invariants.
fn sanity_check(atp: &bitdaemon_core::AddTorrentParams) {
    let ti = atp.ti.as_ref().expect("ti");
    assert!(ti.is_valid());
    assert!(ti.num_pieces() > 0);
    assert_eq!(ti.total_size(), ti.files().total_size());
}

// ----- the upstream `test_torrents[]` named cases (fidelity subset) -----

#[test]
fn parse_all_valid_torrents() {
    let Some(_) = test_dir() else {
        eprintln!("LIBTORRENT_TEST_DIR not set — skipping");
        return;
    };
    // valid fixtures that upstream expects to parse (dup-rename cases
    // excluded: resolve_duplicate_filenames isn't ported yet)
    const VALID: &[&str] = &[
        "base.torrent",
        "empty_path.torrent",
        "parent_path.torrent",
        "hidden_parent_path.torrent",
        "single_multi_file.torrent",
        "slash_path.torrent",
        "slash_path2.torrent",
        "slash_path3.torrent",
        "backslash_path.torrent",
        "url_list.torrent",
        "url_list2.torrent",
        "url_list3.torrent",
        "httpseed.torrent",
        "empty_httpseed.torrent",
        "long_name.torrent",
        "whitespace_url.torrent",
        "pad_file.torrent",
        "creation_date.torrent",
        "no_creation_date.torrent",
        "url_seed.torrent",
        "url_seed_multi.torrent",
        "url_seed_multi_single_file.torrent",
        "url_seed_multi_space.torrent",
        "url_seed_multi_space_nolist.torrent",
        "empty_path_multi.torrent",
        "duplicate_web_seeds.torrent",
        "invalid_name2.torrent",
        "invalid_name3.torrent",
        "symlink1.torrent",
        "symlink2.torrent",
        "unordered.torrent",
        "symlink_zero_size.torrent",
        "symlink_filtered_path.torrent",
        "pad_file_no_path.torrent",
        "large.torrent",
        "absolute_filename.torrent",
        "invalid_filename.torrent",
        "invalid_filename2.torrent",
        "overlapping_symlinks.torrent",
        "v2.torrent",
        "v2_hybrid.torrent",
        "v2_single_file.torrent",
        "v2_multifile.torrent",
        "v2_multiple_files.torrent",
        "v2_only.torrent",
        "v2_zero_size.torrent",
        "collection.torrent",
        "collection2.torrent",
        "dht_nodes.torrent",
        "large_piece_size.torrent",
    ];
    for name in VALID {
        let path = torrent_path(name);
        if !path.exists() {
            continue;
        }
        let buf = std::fs::read(&path).expect("read fixture");
        let atp = load_torrent(&buf).unwrap_or_else(|e| panic!("{name} failed to parse: {e}"));
        sanity_check(&atp);
    }
}

#[test]
fn named_expectations() {
    if test_dir().is_none() {
        eprintln!("LIBTORRENT_TEST_DIR not set — skipping");
        return;
    }

    let atp = load("slash_path.torrent");
    let ti = atp.ti.as_ref().unwrap();
    assert_eq!(ti.num_files(), 1);
    assert_eq!(
        ti.files().file_path(FileIndex(0), b""),
        sep(&["temp", "_", "_", "bar"])
    );

    let atp = load("slash_path2.torrent");
    assert_eq!(
        atp.ti
            .as_ref()
            .unwrap()
            .files()
            .file_path(FileIndex(0), b""),
        sep(&["temp", "abc....def", "_", "bar"])
    );

    let atp = load("slash_path3.torrent");
    assert_eq!(
        atp.ti
            .as_ref()
            .unwrap()
            .files()
            .file_path(FileIndex(0), b""),
        b"temp....abc"
    );

    let atp = load("whitespace_url.torrent");
    assert_eq!(
        atp.trackers.first().map(Vec::as_slice),
        Some(b"udp://test.com/announce".as_slice())
    );

    let atp = load("pad_file.torrent");
    let ti = atp.ti.as_ref().unwrap();
    assert_eq!(ti.num_files(), 2);
    assert!(
        !ti.files()
            .file_flags(FileIndex(0))
            .contains(FileFlags::PAD_FILE)
    );
    assert!(
        ti.files()
            .file_flags(FileIndex(1))
            .contains(FileFlags::PAD_FILE)
    );

    let atp = load("creation_date.torrent");
    assert_eq!(atp.creation_date, 1234567);

    let atp = load("url_seed.torrent");
    assert_eq!(atp.url_seeds.len(), 1);
    assert_eq!(atp.url_seeds[0], b"http://test.com/file");

    let atp = load("url_seed_multi.torrent");
    assert_eq!(atp.url_seeds[0], b"http://test.com/file/");

    let atp = load("url_seed_multi_single_file.torrent");
    assert_eq!(atp.url_seeds[0], b"http://test.com/file/temp/foo/bar.txt");

    let atp = load("url_seed_multi_space.torrent");
    assert_eq!(atp.url_seeds[0], b"http://test.com/test%20file/foo%20bar/");

    let atp = load("duplicate_web_seeds.torrent");
    assert_eq!(atp.url_seeds.len(), 3);

    let atp = load("invalid_name2.torrent");
    // empty name → fallback to hex of v1 info-hash
    assert_eq!(
        atp.ti.as_ref().unwrap().name(),
        b"b61560c2918f463768cd122b6d2fdd47b77bdb35"
    );

    let atp = load("invalid_name3.torrent");
    // windows strips trailing spaces
    if cfg!(windows) {
        assert_eq!(atp.ti.as_ref().unwrap().name(), b"foobar");
    } else {
        assert_eq!(atp.ti.as_ref().unwrap().name(), b"foobar ");
    }

    let atp = load("symlink1.torrent");
    assert_eq!(atp.ti.as_ref().unwrap().num_files(), 2);
    assert_eq!(
        atp.ti.as_ref().unwrap().files().symlink(FileIndex(1)),
        sep(&["temp", "a", "b", "bar"])
    );

    let atp = load("symlink_filtered_path.torrent");
    let ti = atp.ti.as_ref().unwrap();
    assert_eq!(ti.num_files(), 2);
    assert_eq!(
        ti.files().file_path(FileIndex(0), b""),
        sep(&["temp", "a", "_", "bar"])
    );
    assert_eq!(
        ti.files().symlink(FileIndex(1)),
        sep(&["temp", "a", "_", "bar"])
    );

    let atp = load("pad_file_no_path.torrent");
    assert_eq!(
        atp.ti
            .as_ref()
            .unwrap()
            .files()
            .file_path(FileIndex(1), b""),
        combine_path(b".pad", b"2124")
    );

    let atp = load("absolute_filename.torrent");
    let ti = atp.ti.as_ref().unwrap();
    assert_eq!(ti.num_files(), 2);
    assert_eq!(
        ti.files().file_path(FileIndex(0), b""),
        combine_path(b"temp", b"abcde")
    );
    assert_eq!(
        ti.files().file_path(FileIndex(1), b""),
        combine_path(b"temp", b"foobar")
    );

    // v2 fixture — root hash + hybrid info-hash
    if torrent_path("v2.torrent").exists() {
        let atp = load("v2.torrent");
        let ti = atp.ti.as_ref().unwrap();
        assert_eq!(ti.num_files(), 1);
        assert_eq!(ti.files().file_path(FileIndex(0), b""), b"test64K");
        assert_eq!(ti.files().file_size(FileIndex(0)), 65536);
        assert_eq!(
            ti.files().root(FileIndex(0)).to_hex(),
            "60aae9c7b428f87e0713e88229e18f0adf12cd7b22a0dd8a92bb2485eb7af242"
        );
        assert!(ti.info_hashes().has_v1());
    }
}

// ----- `test_error_torrents` — expected error per fixture -----

#[test]
fn parse_invalid_torrents() {
    let Some(_) = test_dir() else {
        eprintln!("LIBTORRENT_TEST_DIR not set — skipping");
        return;
    };

    // (fixture, expected) — `E` = TorrentError variant name prefix match
    // on the error message; upstream asserts `ec.message()`.
    const CASES: &[(&str, &str)] = &[
        ("missing_piece_len.torrent", "missing 'piece length'"),
        ("invalid_piece_len.torrent", "missing 'piece length'"),
        ("negative_piece_len.torrent", "missing 'piece length'"),
        ("no_name.torrent", "missing name"),
        ("bad_name.torrent", "missing name"),
        ("invalid_name.torrent", "missing name"),
        ("invalid_info.torrent", "missing or invalid 'info'"),
        ("string.torrent", "not a dictionary"),
        ("negative_size.torrent", "invalid length"),
        ("negative_file_size.torrent", "invalid length"),
        ("invalid_path_list.torrent", "invalid 'name'"),
        ("missing_path_list.torrent", "missing name"),
        ("invalid_pieces.torrent", "'pieces'"),
        ("unaligned_pieces.torrent", "piece hashes"),
        ("invalid_file_size.torrent", "invalid length"),
        ("invalid_symlink.torrent", "invalid 'name'"),
        ("many_pieces.torrent", "too many pieces"),
        ("no_files.torrent", "no files"),
        ("zero.torrent", "invalid length"),
        ("zero2.torrent", "invalid length"),
        ("v2_mismatching_metadata.torrent", "does not match"),
        ("v2_no_power2_piece.torrent", "missing 'piece length'"),
        ("v2_invalid_file.torrent", "parse files"),
        ("v2_non_multiple_piece_layer.torrent", "piece layer"),
        ("v2_piece_layer_invalid_file_hash.torrent", "piece layer"),
        ("v2_invalid_piece_layer.torrent", "piece layer"),
        ("v2_invalid_piece_layer_root.torrent", "piece layer"),
        ("v2_unknown_piece_layer_entry.torrent", "piece layer"),
        ("v2_invalid_piece_layer_size.torrent", "piece layer"),
        ("v2_bad_file_alignment.torrent", "does not match"),
        ("v2_unordered_files.torrent", "bencoding"),
        ("v2_overlong_integer.torrent", "bencoding"),
        ("v2_missing_file_root_invalid_symlink.torrent", "root hash"),
        ("v2_large_file.torrent", "invalid length"),
        ("v2_large_offset.torrent", "too many pieces"),
        ("v2_piece_size.torrent", "missing 'piece length'"),
        ("v2_invalid_pad_file.torrent", "pad attribute"),
        ("v2_zero_root.torrent", "root hash"),
        ("v2_zero_root_small.torrent", "root hash"),
        ("v2_empty_filename.torrent", "parse files"),
    ];

    let mut checked = 0;
    for (file, expected) in CASES {
        let path = torrent_path(file);
        if !path.exists() {
            continue;
        }
        checked += 1;
        let buf = std::fs::read(&path).expect("read fixture");
        match load_torrent(&buf) {
            Ok(_) => panic!("{file}: expected error containing {expected:?}"),
            Err(e) => {
                let msg = e.to_string();
                eprintln!("{file}: got {msg:?} expected {expected:?}");
                assert!(
                    msg.contains(expected),
                    "{file}: error {msg:?} doesn't contain {expected:?}"
                );
            }
        }
    }
    if checked > 0 {
        eprintln!("checked {checked} invalid fixtures");
    }
}

// ----- synthetic cases (run without fixtures) -----

#[test]
fn parse_synthetic_v1() {
    // transcribed from upstream `parse_torrents` first case:
    // name.utf-8 preferred over name
    let torrent = bitdaemon_bencode::Entry::Dict(
        [(
            b"info".to_vec(),
            bitdaemon_bencode::Entry::Dict(
                [
                    (b"length".to_vec(), bitdaemon_bencode::Entry::Int(3245)),
                    (
                        b"name".to_vec(),
                        bitdaemon_bencode::Entry::String(b"test__".to_vec()),
                    ),
                    (
                        b"name.utf-8".to_vec(),
                        bitdaemon_bencode::Entry::String(b"test1".to_vec()),
                    ),
                    (
                        b"piece length".to_vec(),
                        bitdaemon_bencode::Entry::Int(16 * 1024),
                    ),
                    (
                        b"pieces".to_vec(),
                        bitdaemon_bencode::Entry::String(b"aaaaaaaaaaaaaaaaaaaa".to_vec()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        )]
        .into_iter()
        .collect(),
    );
    let buf = bitdaemon_bencode::bencode(&torrent);
    let atp = load_torrent(&buf).expect("parse");
    assert_eq!(atp.ti.as_ref().unwrap().name(), b"test1");
}

#[test]
fn invalid_name_falls_back_to_info_hash() {
    // multi-file v1: a name that sanitizes to nothing falls back to the
    // hex v1 info-hash (single-file v1 would fail in extract_single_file,
    // as upstream — the fallback covers the root dir)
    let file = bitdaemon_bencode::Entry::Dict(
        [
            (b"length".to_vec(), bitdaemon_bencode::Entry::Int(3245)),
            (
                b"path".to_vec(),
                bitdaemon_bencode::Entry::List(vec![bitdaemon_bencode::Entry::String(
                    b"f".to_vec(),
                )]),
            ),
        ]
        .into_iter()
        .collect(),
    );
    let torrent = bitdaemon_bencode::Entry::Dict(
        [(
            b"info".to_vec(),
            bitdaemon_bencode::Entry::Dict(
                [
                    (
                        b"files".to_vec(),
                        bitdaemon_bencode::Entry::List(vec![file]),
                    ),
                    (
                        b"name".to_vec(),
                        bitdaemon_bencode::Entry::String(b".".to_vec()),
                    ),
                    (
                        b"piece length".to_vec(),
                        bitdaemon_bencode::Entry::Int(16 * 1024),
                    ),
                    (
                        b"pieces".to_vec(),
                        bitdaemon_bencode::Entry::String(b"aaaaaaaaaaaaaaaaaaaa".to_vec()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        )]
        .into_iter()
        .collect(),
    );
    let buf = bitdaemon_bencode::bencode(&torrent);
    let atp = load_torrent(&buf).expect("parse");
    let ti = atp.ti.as_ref().unwrap();
    assert_eq!(ti.name(), ti.info_hash_v1().unwrap().to_hex().as_bytes());
}

#[test]
fn directory_depth_limit() {
    // `load_torrent_directory_depth_v1_exceeds_default` upstream
    let depth = 200usize;
    let path: Vec<bitdaemon_bencode::Entry> = (0..depth - 1)
        .map(|_| bitdaemon_bencode::Entry::String(b"d".to_vec()))
        .chain(std::iter::once(bitdaemon_bencode::Entry::String(
            b"file.txt".to_vec(),
        )))
        .collect();
    let file = bitdaemon_bencode::Entry::Dict(
        [
            (b"length".to_vec(), bitdaemon_bencode::Entry::Int(1)),
            (b"path".to_vec(), bitdaemon_bencode::Entry::List(path)),
        ]
        .into_iter()
        .collect(),
    );
    let torrent = bitdaemon_bencode::Entry::Dict(
        [(
            b"info".to_vec(),
            bitdaemon_bencode::Entry::Dict(
                [
                    (
                        b"files".to_vec(),
                        bitdaemon_bencode::Entry::List(vec![file]),
                    ),
                    (
                        b"name".to_vec(),
                        bitdaemon_bencode::Entry::String(b"root".to_vec()),
                    ),
                    (
                        b"piece length".to_vec(),
                        bitdaemon_bencode::Entry::Int(16 * 1024),
                    ),
                    (
                        b"pieces".to_vec(),
                        bitdaemon_bencode::Entry::String(b"aaaaaaaaaaaaaaaaaaaa".to_vec()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        )]
        .into_iter()
        .collect(),
    );
    let buf = bitdaemon_bencode::bencode(&torrent);
    let err = load_torrent(&buf).expect_err("depth must be rejected");
    assert!(matches!(
        err,
        Error::Torrent(TorrentError::TorrentDirectoryTooDeep)
    ));

    // configurable limit
    let cfg = LoadTorrentLimits {
        max_directory_depth: 300,
        ..Default::default()
    };
    let atp = bitdaemon_core::torrent_info::load_torrent_buffer(&buf, &cfg).expect("within limit");
    assert!(atp.ti.is_some());
}

#[test]
fn piece_layer_validation() {
    // a piece layer referencing an unknown root must fail
    use bitdaemon_core::file_storage::FileStorage;
    let mut fs = FileStorage::new();
    fs.set_piece_length(0x4000);
    let root = Sha256Hash::from_bytes(&[7u8; 32]).unwrap();
    fs.add_file(None, b"test/f", 0x8000, FileFlags::NONE, 0, b"", Some(root))
        .unwrap();
    fs.set_num_pieces(2);

    // wrong size layer for the right root
    let layers_dict = bitdaemon_bencode::Entry::Dict(
        [(
            root.as_bytes().to_vec(),
            bitdaemon_bencode::Entry::String(vec![0u8; 32]),
        )]
        .into_iter()
        .collect(),
    );
    let buf = bitdaemon_bencode::bencode(&layers_dict);
    let node = bitdaemon_bencode::bdecode(&buf);
    assert!(parse_piece_layers(&node, &fs).is_err());
}
