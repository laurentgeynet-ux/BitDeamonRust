//! Port of `ip_helpers.cpp` + the `hash_address` part of
//! `socket_io.cpp` — address classification helpers used by
//! `ip_voter`, `ip_filter` consumers and the DHT.

use std::net::IpAddr;

use bitdaemon_common::hash::Sha1Hash;
use sha1::{Digest, Sha1};

/// `is_local` — loopback/link-local/site-local/ULA + RFC-1918,
/// CGNAT 100.64/10.
#[must_use]
pub fn is_local(a: &IpAddr) -> bool {
    match a {
        IpAddr::V6(a6) => {
            a6.is_loopback()
                || is_v6_link_local(a6)
                || is_v6_site_local(a6)
                || a6.is_multicast() && (a6.segments()[1] & 0x0f) == 0x02 // link-local mcast
                || a6.is_multicast() && (a6.segments()[1] & 0x0f) == 0x05 // site-local mcast
                || (a6.octets()[0] & 0xfe) == 0xfc // fc00::/7 ULA
        }
        IpAddr::V4(a4) => {
            let ip = u32::from(*a4);
            (ip & 0xff000000) == 0x0a000000 // 10/8
                || (ip & 0xfff00000) == 0xac100000 // 172.16/12
                || (ip & 0xffff0000) == 0xc0a80000 // 192.168/16
                || (ip & 0xffff0000) == 0xa9fe0000 // 169.254/16
                || (ip & 0xff000000) == 0x7f000000 // 127/8
                || (ip & 0xffc00000) == 0x64400000 // 100.64/10
        }
    }
}

fn is_v6_link_local(a: &std::net::Ipv6Addr) -> bool {
    (a.segments()[0] & 0xffc0) == 0xfe80
}
fn is_v6_site_local(a: &std::net::Ipv6Addr) -> bool {
    (a.segments()[0] & 0xffc0) == 0xfec0
}

/// `is_global` — upstream: v6 = first byte masked by 0xe0 == 0x20
/// (IANA global unicast range); v4 = !multicast && !unspecified &&
/// !is_local.
#[must_use]
pub fn is_global(a: &IpAddr) -> bool {
    match a {
        IpAddr::V6(a6) => (a6.octets()[0] & 0xe0) == 0x20,
        IpAddr::V4(a4) => !(a4.is_multicast() || a4.is_unspecified() || is_local(a)),
    }
}

/// `is_teredo` — `2001::/32` (Teredo prefix, first 4 bytes
/// `20 01 00 00`).
#[must_use]
pub fn is_teredo(a: &IpAddr) -> bool {
    match a {
        IpAddr::V6(a6) => a6.octets()[..4] == [0x20, 0x01, 0, 0],
        IpAddr::V4(_) => false,
    }
}

/// `hash_address` — SHA-1 over the raw address bytes, used to key
/// voter identities in bloom filters.
#[must_use]
pub fn hash_address(a: &IpAddr) -> Sha1Hash {
    let mut h = Sha1::new();
    match a {
        IpAddr::V4(a4) => h.update(a4.octets()),
        IpAddr::V6(a6) => h.update(a6.octets()),
    }
    let out = h.finalize();
    Sha1Hash::from_bytes(&out).unwrap_or_default()
}
