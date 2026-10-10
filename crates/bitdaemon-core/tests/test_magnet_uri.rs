//! Port of `test/test_magnet.cpp` — the parse-only cases (session-bound
//! tests like `remove_url`/`trailing_whitespace` add_torrent arrive with
//! the session layer).
#![allow(clippy::field_reassign_with_default)]

use bitdaemon_common::flags::TorrentFlags;
use bitdaemon_common::types::DownloadPriority;
use bitdaemon_common::{InfoHash, Sha1Hash, Sha256Hash};
use bitdaemon_core::error::Error;
use bitdaemon_core::escape_string::{base32decode, escape_string, unescape_string};
use bitdaemon_core::magnet_uri::{AddTorrentParams, make_magnet_uri, parse_magnet};

const CD40: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
const CD64: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

#[test]
fn escaped_hash_parameter() {
    let p =
        parse_magnet(b"magnet:?xt=urn%3Abtih%3Acdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd").unwrap();
    assert_eq!(p.info_hashes.v1.to_hex(), CD40);
}

#[test]
fn escaped_hash_parameter_in_hex() {
    let p =
        parse_magnet(b"magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdc%64").unwrap();
    assert_eq!(p.info_hashes.v1.to_hex(), CD40);
}

#[test]
fn mixed_case() {
    let p = parse_magnet(
        b"magnet:?XT=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdc%64&dN=foobar&Ws=http://foo.com/bar",
    )
    .unwrap();
    assert_eq!(p.info_hashes.v1.to_hex(), CD40);
    assert_eq!(p.name, b"foobar");
    assert_eq!(p.url_seeds, [b"http://foo.com/bar".to_vec()]);
}

#[test]
fn invalid_escaped_hash_parameter() {
    assert!(matches!(
        parse_magnet(b"magnet:?xt=urn%%3A"),
        Err(Error::Torrent(
            bitdaemon_core::TorrentError::InvalidEscapedString
        ))
    ));
}

#[test]
fn quoted_magnet() {
    let p = parse_magnet(b"magnet:?\"foo=bar&xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
        .unwrap();
    assert_eq!(p.info_hashes.v1.to_hex(), CD40);
}

#[test]
fn missing_hash() {
    assert!(matches!(
        parse_magnet(b"magnet:?dn=foo&dht=127.0.0.1:43"),
        Err(Error::MissingInfoHashInUri)
    ));
    assert!(matches!(
        parse_magnet(b"magnet:?xt=blah&dn=foo&dht=127.0.0.1:43"),
        Err(Error::MissingInfoHashInUri)
    ));
}

#[test]
fn base32_hash() {
    let p = parse_magnet(b"magnet:?xt=urn:btih:MFRGCYTBMJQWEYLCMFRGCYTBMJQWEYLC").unwrap();
    assert_eq!(
        p.info_hashes.v1,
        Sha1Hash::from_bytes(b"abababababababababab").unwrap()
    );
}

#[test]
fn web_seeds() {
    let p = parse_magnet(
        b"magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd&ws=http://foo.com/bar&ws=http://bar.com/foo",
    )
    .unwrap();
    assert_eq!(
        p.url_seeds,
        [
            b"http://foo.com/bar".to_vec(),
            b"http://bar.com/foo".to_vec()
        ]
    );
}

#[test]
fn invalid_hashes() {
    for uri in [
        "magnet:?xt=urn:btih:abababab",               // short
        "magnet:?xt=urn:btih:ababababababababababab", // long-ish (22)
        "magnet:?xt=urn:btih: abababababababababab",  // space
        "magnet:?xt=urn:btih:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", // non-hex
    ] {
        assert!(
            matches!(parse_magnet(uri.as_bytes()), Err(Error::InvalidInfoHash)),
            "{uri}"
        );
    }
}

#[test]
fn unsupported_protocol() {
    assert!(matches!(
        parse_magnet(b"http://foo/bar"),
        Err(Error::UnsupportedUrlProtocol)
    ));
}

#[test]
fn v2_hash() {
    let p = parse_magnet(
        b"magnet:?xt=urn:btmh:1220cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
    )
    .unwrap();
    assert_eq!(p.info_hashes.v2.to_hex(), CD64);
    assert!(!p.info_hashes.has_v1());
}

#[test]
fn v2_invalid() {
    for uri in [
        // short
        "magnet:?xt=urn:btmh:1220cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdccdcdcdcdcdcdcd",
        // wrong multihash prefix
        "magnet:?xt=urn:btmh:1221cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
        // non-hex
        "magnet:?xt=urn:btmh:1220zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
    ] {
        assert!(
            matches!(parse_magnet(uri.as_bytes()), Err(Error::InvalidInfoHash)),
            "{uri}"
        );
    }
}

#[test]
fn hybrid_uri() {
    let p = parse_magnet(
        b"magnet:?xt=urn:btmh:1220cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd&xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
    )
    .unwrap();
    assert_eq!(p.info_hashes.v1.to_hex(), CD40);
    assert_eq!(p.info_hashes.v2.to_hex(), CD64);
}

#[test]
fn parse_peer() {
    let p = parse_magnet(
        b"magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd&dn=foo&x.pe=127.0.0.1:43&x.pe=<invalid1>&x.pe=<invalid2>:100&x.pe=[::1]:45",
    )
    .unwrap();
    assert_eq!(p.peers.len(), 2);
    assert_eq!(p.peers[0].to_string(), "127.0.0.1:43");
    assert_eq!(p.peers[1].to_string(), "[::1]:45");
}

#[test]
fn parse_dht_node() {
    let p = parse_magnet(
        b"magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd&dn=foo&dht=127.0.0.1:43&dht=10.0.0.1:1337",
    )
    .unwrap();
    assert_eq!(
        p.dht_nodes,
        [
            (b"127.0.0.1".to_vec(), 43i32),
            (b"10.0.0.1".to_vec(), 1337i32)
        ]
    );
}

#[test]
fn i2p_tracker_flag() {
    let p = parse_magnet(
        b"magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd&tr=https://test.i2p/announce",
    )
    .unwrap();
    assert!(p.flags.contains(TorrentFlags::I2P_TORRENT));
    let p = parse_magnet(
        b"magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd&tr=https://test.com/announce",
    )
    .unwrap();
    assert!(!p.flags.contains(TorrentFlags::I2P_TORRENT));
}

#[test]
fn tracker_tiers() {
    let p = parse_magnet(
        b"magnet:?xt=urn:btih:a38d02c287893842a32825aa866e00828a318f07&tr=udp://tracker.openbittorrent.com:80&tr=udp://tracker.publicbt.com:80&tr=udp://tracker.ccc.de:80",
    )
    .unwrap();
    assert_eq!(
        p.trackers,
        [
            b"udp://tracker.openbittorrent.com:80".to_vec(),
            b"udp://tracker.publicbt.com:80".to_vec(),
            b"udp://tracker.ccc.de:80".to_vec(),
        ]
    );
    assert_eq!(p.tracker_tiers, [0, 1, 2]);
}

#[test]
fn invalid_trackers() {
    let p = parse_magnet(b"magnet:?tr=").unwrap_err();
    // no info-hash → error, but trackers must be empty anyway; check via
    // the params-out variant instead
    let mut params = AddTorrentParams::default();
    let _ = bitdaemon_core::magnet_uri::parse_magnet_uri(b"magnet:?tr=", &mut params);
    assert!(params.trackers.is_empty());
    let _ = p;
}

const Y: DownloadPriority = DownloadPriority::DEFAULT;
const N: DownloadPriority = DownloadPriority::DONT_DOWNLOAD;

fn select_only(uri: &str) -> Vec<DownloadPriority> {
    parse_magnet(uri.as_bytes()).unwrap().file_priorities
}

#[test]
fn select_only_cases() {
    let ih = "magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd&dn=foo";
    assert_eq!(
        select_only(&format!("{ih}&so=0,2,4,6-8")),
        [Y, N, Y, N, Y, N, Y, Y, Y]
    );
    assert_eq!(
        select_only(&format!("{ih}&so=0,2-4,3-5&dht=10.0.0.1:1337")),
        [Y, N, Y, Y, Y, Y]
    );
    assert_eq!(
        select_only(&format!("{ih}&so=2-4&dht=10.0.0.1:1337&so=1")),
        [N, Y, Y, Y, Y]
    );
    assert_eq!(
        select_only(&format!("{ih}&so=7-4,100000000&dht=10.0.0.1:1337&so=10")),
        [N, N, N, N, N, N, N, N, N, N, Y]
    );
    assert_eq!(
        select_only(&format!("{ih}&so=100000000&dht=10.0.0.1:1337&so=10")),
        [N, N, N, N, N, N, N, N, N, N, Y]
    );
    assert_eq!(select_only(&format!("{ih}&so=-4&so=1")), [N, Y]);
    assert_eq!(select_only(&format!("{ih}&so=3-&so=1")), [N, Y]);
    assert_eq!(select_only(&format!("{ih}&so=a&so=1")), [N, Y]);
    assert_eq!(select_only(&format!("{ih}&so=100000000&so=1")), [N, Y]);
    assert!(select_only(&format!("{ih}&so=&dht=10.0.0.1:1337&so=")).is_empty());
    assert_eq!(
        select_only(&format!("{ih}&so=&dht=10.0.0.1:1337&so=,,1")),
        [N, Y]
    );
}

#[test]
fn make_uri() {
    let mut p = AddTorrentParams::default();
    p.info_hashes = InfoHash {
        v1: Sha1Hash::from_bytes(&[0xcd; 20]).unwrap(),
        v2: Sha256Hash::default(),
    };
    p.name = b"foo & bar".to_vec();
    p.trackers = vec![b"http://t/announce".to_vec()];
    p.url_seeds = vec![b"http://foo.com/bar".to_vec()];
    let m = make_magnet_uri(&p);
    assert!(m.starts_with("magnet:?xt=urn:btih:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"));
    assert!(m.contains("&dn=foo%20%26%20bar"));
    assert!(m.contains("&tr=http%3a%2f%2ft%2fannounce"));
    // upstream: ws encodes the whole URL
    assert!(m.contains("&ws=http%3a%2f%2ffoo.com%2fbar"), "{m}");
}

#[test]
fn make_uri_v2_and_hybrid() {
    let mut p = AddTorrentParams::default();
    p.info_hashes.v2 = Sha256Hash::from_bytes(&[0xcd; 32]).unwrap();
    let m = make_magnet_uri(&p);
    assert!(m.contains("xt=urn:btmh:1220"));
    assert!(!m.contains("xt=urn:btih:"));

    p.info_hashes.v1 = Sha1Hash::from_bytes(&[0xcd; 20]).unwrap();
    let m = make_magnet_uri(&p);
    assert!(m.contains("xt=urn:btih:"));
    assert!(m.contains("xt=urn:btmh:1220"));
}

#[test]
fn make_uri_so() {
    let mut p = AddTorrentParams::default();
    p.info_hashes.v1 = Sha1Hash::from_bytes(&[0xcd; 20]).unwrap();
    p.file_priorities = vec![Y, N, Y, N, N, Y];
    let m = make_magnet_uri(&p);
    assert!(m.contains("&so=0,2,5"), "{m}");
}

#[test]
fn escape_roundtrip() {
    let src = b"hello world+&=?%";
    let enc = escape_string(src);
    assert_eq!(unescape_string(&enc).unwrap(), src);
    assert_eq!(unescape_string(b"a+b").unwrap(), b"a b");
    assert!(unescape_string(b"%zz").is_err());
    assert!(unescape_string(b"%").is_err());
}

#[test]
fn base32_roundtrip() {
    // upstream vector: sha1 "abababababababababab" → MFRGCYTBMJQWEYLCMFRGCYTBMJQWEYLC
    assert_eq!(
        base32decode(b"MFRGCYTBMJQWEYLCMFRGCYTBMJQWEYLC"),
        b"abababababababababab"
    );
    // lowercase accepted
    assert_eq!(
        base32decode(b"mfrgcytbmjqweylcmfrgcytbmjqweylc"),
        b"abababababababababab"
    );
    // invalid char → empty
    assert!(base32decode(b"MFRG!").is_empty());
}
