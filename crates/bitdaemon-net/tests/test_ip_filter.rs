//! Conformance tests for `IpFilter`/`PortFilter` — ported from upstream
//! `test_ip_filter.cpp`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use bitdaemon_net::ip_filter::{BLOCKED, IpFilter, IpRange, PortFilter};

fn addr(s: &str) -> IpAddr {
    s.parse().unwrap_or_else(|_| unreachable!())
}

fn v4(s: &str) -> Ipv4Addr {
    s.parse().unwrap_or_else(|_| unreachable!())
}

fn v6(s: &str) -> Ipv6Addr {
    s.parse().unwrap_or_else(|_| unreachable!())
}

fn rng4(f: &str, l: &str, flags: u32) -> IpRange<Ipv4Addr> {
    IpRange {
        first: v4(f),
        last: v4(l),
        flags,
    }
}

/// `test_rules_invariant` — exported ranges must tile the whole space,
/// contiguously, and agree with `access`.
fn check_v4_invariant(r: &[IpRange<Ipv4Addr>], f: &IpFilter) {
    assert!(!r.is_empty());
    assert_eq!(r.first().map(|x| x.first), Some(v4("0.0.0.0")));
    assert_eq!(r.last().map(|x| x.last), Some(v4("255.255.255.255")));
    for w in r.windows(2) {
        let (i, j) = (&w[0], &w[1]);
        assert_eq!(f.access(IpAddr::V4(i.last)), i.flags);
        assert_eq!(f.access(IpAddr::V4(j.first)), j.flags);
        // i.last + 1 == j.first (byte-order carry)
        let next = u32::from_be_bytes(i.last.octets()) + 1;
        assert_eq!(j.first.octets(), next.to_be_bytes());
    }
}

fn check_v6_invariant(r: &[IpRange<Ipv6Addr>], f: &IpFilter) {
    assert!(!r.is_empty());
    assert_eq!(r.first().map(|x| x.first), Some(v6("::")));
    assert_eq!(
        r.last().map(|x| x.last),
        Some(v6("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))
    );
    for w in r.windows(2) {
        let (i, j) = (&w[0], &w[1]);
        assert_eq!(f.access(IpAddr::V6(i.last)), i.flags);
        assert_eq!(f.access(IpAddr::V6(j.first)), j.flags);
        let next = u128::from_be_bytes(i.last.octets()) + 1;
        assert_eq!(j.first.octets(), next.to_be_bytes());
    }
}

fn expected1() -> Vec<IpRange<Ipv4Addr>> {
    vec![
        rng4("0.0.0.0", "0.255.255.255", 0),
        rng4("1.0.0.0", "3.0.0.0", BLOCKED),
        rng4("3.0.0.1", "255.255.255.255", 0),
    ]
}

#[test]
fn joining_ranges_at_end() {
    let mut f = IpFilter::new();
    f.add_rule(addr("1.0.0.0"), addr("2.0.0.0"), BLOCKED);
    f.add_rule(addr("2.0.0.1"), addr("3.0.0.0"), BLOCKED);
    let (r, _) = f.export_filter();
    check_v4_invariant(&r, &f);
    assert_eq!(r, expected1());
}

#[test]
fn joining_ranges_at_start() {
    let mut f = IpFilter::new();
    f.add_rule(addr("2.0.0.1"), addr("3.0.0.0"), BLOCKED);
    f.add_rule(addr("1.0.0.0"), addr("2.0.0.0"), BLOCKED);
    let (r, _) = f.export_filter();
    check_v4_invariant(&r, &f);
    assert_eq!(r, expected1());
}

#[test]
fn joining_overlapping_ranges_at_start() {
    let mut f = IpFilter::new();
    f.add_rule(addr("2.0.0.1"), addr("3.0.0.0"), BLOCKED);
    f.add_rule(addr("1.0.0.0"), addr("2.4.0.0"), BLOCKED);
    let (r, _) = f.export_filter();
    check_v4_invariant(&r, &f);
    assert_eq!(r, expected1());
}

#[test]
fn joining_overlapping_ranges_at_end() {
    let mut f = IpFilter::new();
    f.add_rule(addr("1.0.0.0"), addr("2.4.0.0"), BLOCKED);
    f.add_rule(addr("2.0.0.1"), addr("3.0.0.0"), BLOCKED);
    let (r, _) = f.export_filter();
    check_v4_invariant(&r, &f);
    assert_eq!(r, expected1());
}

#[test]
fn joining_multiple_overlapping_ranges_1() {
    let mut f = IpFilter::new();
    for (a, b) in [
        ("1.0.0.0", "2.0.0.0"),
        ("3.0.0.0", "4.0.0.0"),
        ("5.0.0.0", "6.0.0.0"),
        ("7.0.0.0", "8.0.0.0"),
    ] {
        f.add_rule(addr(a), addr(b), BLOCKED);
    }
    f.add_rule(addr("1.0.1.0"), addr("9.0.0.0"), BLOCKED);

    let (r, _) = f.export_filter();
    check_v4_invariant(&r, &f);
    assert_eq!(
        r,
        vec![
            rng4("0.0.0.0", "0.255.255.255", 0),
            rng4("1.0.0.0", "9.0.0.0", BLOCKED),
            rng4("9.0.0.1", "255.255.255.255", 0),
        ]
    );
}

#[test]
fn joining_multiple_overlapping_ranges_2() {
    let mut f = IpFilter::new();
    for (a, b) in [
        ("1.0.0.0", "2.0.0.0"),
        ("3.0.0.0", "4.0.0.0"),
        ("5.0.0.0", "6.0.0.0"),
        ("7.0.0.0", "8.0.0.0"),
    ] {
        f.add_rule(addr(a), addr(b), BLOCKED);
    }
    f.add_rule(addr("0.0.1.0"), addr("7.0.4.0"), BLOCKED);

    let (r, _) = f.export_filter();
    check_v4_invariant(&r, &f);
    assert_eq!(
        r,
        vec![
            rng4("0.0.0.0", "0.0.0.255", 0),
            rng4("0.0.1.0", "8.0.0.0", BLOCKED),
            rng4("8.0.0.1", "255.255.255.255", 0),
        ]
    );
}

#[test]
fn ipv6() {
    let mut f = IpFilter::new();
    f.add_rule(addr("2::1"), addr("3::"), BLOCKED);
    f.add_rule(addr("1::"), addr("2::"), BLOCKED);

    let (_, r6) = f.export_filter();
    check_v6_invariant(&r6, &f);
    assert_eq!(
        r6,
        vec![
            IpRange {
                first: v6("::"),
                last: v6("0:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
                flags: 0
            },
            IpRange {
                first: v6("1::"),
                last: v6("3::"),
                flags: BLOCKED
            },
            IpRange {
                first: v6("3::1"),
                last: v6("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
                flags: 0
            },
        ]
    );
}

#[test]
fn default_empty() {
    let mut f = IpFilter::new();
    assert!(f.is_empty());
    f.add_rule(addr("1::"), addr("2::"), BLOCKED);
    assert!(!f.is_empty());

    let mut f = IpFilter::new();
    f.add_rule(addr("0.0.1.0"), addr("7.0.4.0"), BLOCKED);
    assert!(!f.is_empty());

    let mut f = IpFilter::new();
    f.add_rule(addr("0.0.1.0"), addr("7.0.4.0"), 0);
    assert!(f.is_empty());
}

#[test]
fn port_filter() {
    let mut pf = PortFilter::new();
    assert_eq!(pf.access(0), 0);
    assert_eq!(pf.access(65535), 0);
    assert_eq!(pf.access(6881), 0);

    pf.add_rule(100, 300, BLOCKED);
    assert_eq!(pf.access(0), 0);
    assert_eq!(pf.access(99), 0);
    assert_eq!(pf.access(100), BLOCKED);
    assert_eq!(pf.access(150), BLOCKED);
    assert_eq!(pf.access(300), BLOCKED);
    assert_eq!(pf.access(301), 0);
    assert_eq!(pf.access(6881), 0);
    assert_eq!(pf.access(65535), 0);
}
