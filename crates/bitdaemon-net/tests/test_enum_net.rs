// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `enum_net` — pure helpers (netmask/match/gateway/routes).

use bitdaemon_net::enum_net::{
    IfState, InterfaceFlags, IpInterface, IpRoute, build_netmask, device_for_address, get_gateway,
    has_any_internet_route, has_internet_route, match_addr_mask,
};
use bitdaemon_net::ip_helpers::is_global;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
}

#[test]
fn netmask_building() {
    assert_eq!(build_netmask(24, true), v4(255, 255, 255, 0));
    assert_eq!(build_netmask(17, true), v4(255, 255, 128, 0));
    assert_eq!(build_netmask(0, true), v4(0, 0, 0, 0));
    // capped at address len
    assert_eq!(build_netmask(40, true), v4(255, 255, 255, 255));
    // v6 /64
    let m = build_netmask(64, false);
    if let IpAddr::V6(m) = m {
        assert_eq!(&m.octets()[..8], &[0xff; 8]);
        assert_eq!(&m.octets()[8..], &[0; 8]);
    } else {
        panic!()
    }
}

#[test]
fn mask_matching() {
    let m = v4(255, 255, 255, 0);
    assert!(match_addr_mask(&v4(10, 0, 0, 1), &v4(10, 0, 0, 99), &m));
    assert!(!match_addr_mask(&v4(10, 0, 0, 1), &v4(10, 0, 1, 1), &m));
    // family mismatch → false
    assert!(!match_addr_mask(
        &v4(10, 0, 0, 1),
        &IpAddr::V6(Ipv6Addr::LOCALHOST),
        &m
    ));
}

#[test]
fn gateway_and_routes() {
    let eth = IpInterface {
        interface_address: v4(192, 168, 1, 5),
        netmask: v4(255, 255, 255, 0),
        name: "eth0".into(),
        friendly_name: "eth0".into(),
        description: String::new(),
        preferred: true,
        flags: InterfaceFlags::UP,
        state: IfState::Up,
    };
    let routes = vec![
        IpRoute {
            destination: v4(0, 0, 0, 0),
            netmask: v4(0, 0, 0, 0),
            gateway: v4(192, 168, 1, 1),
            source_hint: None,
            name: "eth0".into(),
            mtu: 1500,
        },
        IpRoute {
            destination: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            netmask: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            gateway: IpAddr::V6("fe80::1".parse().unwrap()),
            source_hint: None,
            name: "eth0".into(),
            mtu: 1500,
        },
    ];
    assert_eq!(get_gateway(&eth, &routes), Some(&v4(192, 168, 1, 1)));
    assert!(has_any_internet_route(&routes));
    assert!(has_internet_route("eth0", true, &routes));
    assert!(!has_internet_route("wlan0", true, &routes));
    assert_eq!(
        device_for_address(&v4(192, 168, 1, 5), std::slice::from_ref(&eth)),
        Some("eth0")
    );
    assert_eq!(device_for_address(&v4(9, 9, 9, 9), &[eth]), None);
}

#[test]
fn global_classification() {
    assert!(is_global(&v4(8, 8, 8, 8)));
    assert!(!is_global(&v4(192, 168, 1, 1)));
    assert!(!is_global(&v4(224, 0, 0, 1)));
    assert!(is_global(
        &"2001:4860:4860::8888".parse::<IpAddr>().unwrap()
    ));
    assert!(!is_global(&"fe80::1".parse::<IpAddr>().unwrap()));
}
