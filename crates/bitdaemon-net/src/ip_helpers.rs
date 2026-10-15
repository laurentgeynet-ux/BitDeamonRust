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
                || a6.is_multicast() && (a6.octets()[1] & 0x0f) == 0x02 // link-local mcast
                || a6.is_multicast() && (a6.octets()[1] & 0x0f) == 0x05 // site-local mcast
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

/// `is_link_local` — v6: link-local unicast or link-local multicast;
/// v4: 169.254/16.
#[must_use]
pub fn is_link_local(a: &IpAddr) -> bool {
    match a {
        IpAddr::V6(a6) => {
            is_v6_link_local(a6) || a6.is_multicast() && (a6.octets()[1] & 0x0f) == 0x02
        }
        IpAddr::V4(a4) => (u32::from(*a4) & 0xffff0000) == 0xa9fe0000,
    }
}

/// `is_ip_address` — the host string parses as a literal IP (no DNS
/// needed). Upstream: `make_address(host, ec)` succeeds.
#[must_use]
pub fn is_ip_address(host: &str) -> bool {
    host.parse::<IpAddr>().is_ok()
}

/// `same_v6_prefix` — do two v6 addresses share a /56? The unit used
/// to bound how many peer-list entries one network can claim.
#[must_use]
pub fn same_v6_prefix(a: &[u8; 16], b: &[u8; 16]) -> bool {
    a[..7] == b[..7]
}

/// `ensure_v6` — an unspecified v4 maps to unspecified v6.
#[must_use]
pub fn ensure_v6(a: IpAddr) -> IpAddr {
    match a {
        IpAddr::V4(v4) if v4 == std::net::Ipv4Addr::UNSPECIFIED => {
            IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
        }
        _ => a,
    }
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

/// `in_local_network` approximation — do `a` and `b` share a
/// subnet? Upstream enumerates interfaces and their netmasks
/// (`enum_net::match_addr_mask` is the masked compare); with
/// injected transports we don't see netmasks, so the caller passes
/// its bound address as `a` and this uses the common LAN masks
/// (v4 /24, v6 /64).
#[must_use]
pub fn same_subnet(a: &IpAddr, b: &IpAddr) -> bool {
    match (a, b) {
        (IpAddr::V4(_), IpAddr::V4(_)) => crate::enum_net::match_addr_mask(
            a,
            b,
            &IpAddr::V4(std::net::Ipv4Addr::new(255, 255, 255, 0)),
        ),
        (IpAddr::V6(a6), IpAddr::V6(b6)) => a6.octets()[..8] == b6.octets()[..8],
        _ => false,
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
