//! Conformance port of upstream `test_peer_priority.cpp` — BEP 40
//! CRC32C endpoint ranking — plus `torrent_peer` record semantics.

use std::net::{IpAddr, SocketAddr};

use bitdaemon_common::PeerSourceFlags;
use bitdaemon_common::crc32c::crc32c;
use bitdaemon_net::torrent_peer::{PeerAddr, ProtocolVersion, TorrentPeer, peer_priority};

fn ep(ip: &str, port: u16) -> SocketAddr {
    SocketAddr::new(ip.parse().unwrap(), port)
}

/// `hash_buffer("…")` — crc32c over the hex-decoded string.
fn hash_buffer(hex: &str) -> u32 {
    let bytes = bitdaemon_common::hex::from_hex(hex.as_bytes()).unwrap();
    crc32c(&bytes)
}

#[test]
fn peer_priority_conformance() {
    // when the IP is the same, we hash the ports, sorted
    let p = peer_priority(ep("230.12.123.3", 0x4d2), ep("230.12.123.3", 0x12c));
    assert_eq!(p, hash_buffer("012c04d2"));

    // same /24 — hash the full IPs, ordered
    let p = peer_priority(ep("230.12.123.1", 0x4d2), ep("230.12.123.3", 0x12c));
    assert_eq!(p, hash_buffer("e60c7b01e60c7b03"));

    // same /16 — mask 0xffffff55
    let p = peer_priority(ep("230.12.23.1", 0x4d2), ep("230.12.123.3", 0x12c));
    assert_eq!(p, hash_buffer("e60c1701e60c7b01"));

    // different /16 — mask 0xffff5555
    let p = peer_priority(ep("230.120.23.1", 0x4d2), ep("230.12.123.3", 0x12c));
    assert_eq!(p, hash_buffer("e60c5101e6781501"));

    // BEP 40 test vectors
    assert_eq!(
        peer_priority(ep("123.213.32.10", 0), ep("98.76.54.32", 0)),
        0xec2d_7224
    );
    assert_eq!(
        peer_priority(ep("123.213.32.10", 0), ep("123.213.32.234", 0)),
        0x9956_8189
    );
}

#[test]
fn peer_priority_v6() {
    // identical IPs: order and hash the ports
    let p = peer_priority(
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x12c),
    );
    assert_eq!(p, hash_buffer("012c04d2"));

    // order doesn't matter
    let p = peer_priority(
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x12c),
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
    );
    assert_eq!(p, hash_buffer("012c04d2"));

    // different /32 — full mask 0x…5555
    let p = peer_priority(
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
        ep("ffff:0fff:ffff:ffff:ffff:ffff:ffff:ffff", 0x12c),
    );
    assert_eq!(
        p,
        hash_buffer("ffff0fffffff55555555555555555555ffffffffffff55555555555555555555")
    );

    let p = peer_priority(
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
        ep("ffff:ffff:0fff:ffff:ffff:ffff:ffff:ffff", 0x12c),
    );
    assert_eq!(
        p,
        hash_buffer("ffffffff0fff55555555555555555555ffffffffffff55555555555555555555")
    );

    // same /48
    let p = peer_priority(
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
        ep("ffff:ffff:ff0f:ffff:ffff:ffff:ffff:ffff", 0x12c),
    );
    assert_eq!(
        p,
        hash_buffer("ffffffffff0fff555555555555555555ffffffffffffff555555555555555555")
    );

    // same /56
    let p = peer_priority(
        ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
        ep("ffff:ffff:ffff:0fff:ffff:ffff:ffff:ffff", 0x12c),
    );
    assert_eq!(
        p,
        hash_buffer("ffffffffffff0fff5555555555555555ffffffffffffffff5555555555555555")
    );
}

#[test]
fn torrent_peer_flags_and_accounting() {
    let mut p = TorrentPeer::new_v4(ep("1.2.3.4", 6881), true, PeerSourceFlags::TRACKER);
    // ctor defaults (upstream verbatim)
    assert_eq!(p.protocol(), ProtocolVersion::V1);
    assert!(p.supports_utp);
    assert!(!p.pe_support);
    assert!(!p.confirmed_supports_utp);
    assert_eq!(p.failcount, 0);
    assert!(p.connectable);
    assert_eq!(p.ip(), Some(ep("1.2.3.4", 6881)));

    // kiB accounting while disconnected
    p.prev_amount_download = 3;
    p.prev_amount_upload = 1;
    assert_eq!(p.total_download(), 3 * 1024);
    assert_eq!(p.total_upload(), 1024);

    // live stats win while connected
    struct FakeConn;
    impl bitdaemon_net::torrent_peer::PeerStats for FakeConn {
        fn total_payload_download(&self) -> i64 {
            9999
        }
        fn total_payload_upload(&self) -> i64 {
            777
        }
    }
    p.prev_amount_download = 0;
    p.prev_amount_upload = 0;
    p.connection = Some(std::rc::Rc::new(FakeConn));
    assert_eq!(p.total_download(), 9999);
    assert_eq!(p.total_upload(), 777);
}

#[test]
fn torrent_peer_rank() {
    let mut p = TorrentPeer::new(
        PeerAddr::V4("230.12.123.3".parse().unwrap()),
        0x12c,
        true,
        PeerSourceFlags::DHT,
    );
    let ext: IpAddr = "98.76.54.32".parse().unwrap();
    let r1 = p.rank(ext, 6881);
    // cached — recomputes identical value
    let r2 = p.rank(ext, 9999);
    assert_eq!(r1, r2);
    assert_ne!(r1, 0);
    assert_eq!(
        r1,
        peer_priority(ep("98.76.54.32", 6881), ep("230.12.123.3", 0x12c))
    );

    // v6 peer with v4 external: no rank
    let mut p6 = TorrentPeer::new_v6(ep("::1", 6881), true, PeerSourceFlags::TRACKER);
    assert_eq!(p6.rank(ext, 6881), 0);
}
