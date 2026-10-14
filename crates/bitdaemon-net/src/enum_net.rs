// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `enum_net.cpp`/`enum_net.hpp` + `aux_/netlink_utils.hpp` +
//! `route.h` (platform enum via `if-addrs`/`netroute` — ADR-0019).
//!
//! `enum_net` — `build_netmask`, `match_addr_mask`, `get_gateway`,
//! `has_internet_route`, `has_any_internet_route`, `device_for_address`
//! plus the default OS enumerations: interfaces via `if-addrs`,
//! routes via `netroute` (ADR-0019). Downstream can still inject its
//! own table — the helpers take data, not traits.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::ip_helpers::{is_global, is_local};

bitdaemon_common::bitfield_flag! {
    /// `interface_flags` (`if_flags::*`).
    InterfaceFlags, u32
}

#[allow(dead_code)]
impl InterfaceFlags {
    /// `if_flags::up`.
    pub const UP: Self = Self::bit(0);
    /// `if_flags::broadcast`.
    pub const BROADCAST: Self = Self::bit(1);
    /// `if_flags::loopback`.
    pub const LOOPBACK: Self = Self::bit(2);
    /// `if_flags::pointopoint`.
    pub const POINTOPOINT: Self = Self::bit(3);
    /// `if_flags::running`.
    pub const RUNNING: Self = Self::bit(4);
    /// `if_flags::noarp`.
    pub const NOARP: Self = Self::bit(5);
    /// `if_flags::promisc`.
    pub const PROMISC: Self = Self::bit(6);
    /// `if_flags::allmulti`.
    pub const ALLMULTI: Self = Self::bit(7);
    /// `if_flags::master`.
    pub const MASTER: Self = Self::bit(8);
    /// `if_flags::slave`.
    pub const SLAVE: Self = Self::bit(9);
    /// `if_flags::multicast`.
    pub const MULTICAST: Self = Self::bit(10);
    /// `if_flags::dynamic`.
    pub const DYNAMIC: Self = Self::bit(11);
    /// `if_flags::lower_up`.
    pub const LOWER_UP: Self = Self::bit(12);
    /// `if_flags::dormant`.
    pub const DORMANT: Self = Self::bit(13);
}

/// `if_state` — operational state of an interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IfState {
    /// `if_state::up`.
    Up,
    /// `if_state::dormant`.
    Dormant,
    /// `if_state::lowerlayerdown`.
    LowerLayerDown,
    /// `if_state::down`.
    Down,
    /// `if_state::notpresent`.
    NotPresent,
    /// `if_state::testing`.
    Testing,
    /// `if_state::unknown`.
    #[default]
    Unknown,
}

/// `ip_interface` — one local interface.
#[derive(Debug, Clone)]
pub struct IpInterface {
    /// `interface_address`.
    pub interface_address: IpAddr,
    /// `netmask`.
    pub netmask: IpAddr,
    /// `name` (device name).
    pub name: String,
    /// `friendly_name`.
    pub friendly_name: String,
    /// `description`.
    pub description: String,
    /// `preferred` — upstream: false iff the address is
    /// tentative/duplicate/deprecated (DAD state). `if-addrs` doesn't
    /// surface DAD state, so this stays `true` like upstream's default.
    pub preferred: bool,
    /// `flags` — `if_addrs` only lets us detect loopback; `UP` is
    /// assumed like upstream's default.
    pub flags: InterfaceFlags,
    /// `state`.
    pub state: IfState,
}

/// `ip_route` — one routing-table entry.
#[derive(Debug, Clone)]
pub struct IpRoute {
    /// `destination`.
    pub destination: IpAddr,
    /// `netmask`.
    pub netmask: IpAddr,
    /// `gateway`.
    pub gateway: IpAddr,
    /// `source_hint` — source address for this route.
    pub source_hint: Option<IpAddr>,
    /// `name` — interface name.
    pub name: String,
    /// `mtu`.
    pub mtu: i32,
}

/// `build_netmask_impl` — prefix bits → mask bytes (capped at the
/// address length, same defensive clamp as upstream).
fn build_netmask_impl(mask: &mut [u8], prefix_bits: i32) {
    let mut prefix_bits = prefix_bits.min(mask.len() as i32 * 8).max(0);
    let mut i = 0;
    while prefix_bits >= 8 {
        mask[i] = 0xff;
        prefix_bits -= 8;
        i += 1;
    }
    if i < mask.len() {
        mask[i] = ((0xffu16 << (8 - prefix_bits)) & 0xff) as u8;
        i += 1;
        while i < mask.len() {
            mask[i] = 0;
            i += 1;
        }
    }
}

/// `build_netmask(prefix_bits, family)` — family is `true` for v4.
#[must_use]
pub fn build_netmask(prefix_bits: i32, v4: bool) -> IpAddr {
    if v4 {
        let mut b = [0u8; 4];
        build_netmask_impl(&mut b, prefix_bits);
        IpAddr::V4(Ipv4Addr::from(b))
    } else {
        let mut b = [0u8; 16];
        build_netmask_impl(&mut b, prefix_bits);
        IpAddr::V6(Ipv6Addr::from(b))
    }
}

/// `match_addr_mask` — `(a1 & mask) == (a2 & mask)`; families must
/// match, v6 scope ids too.
#[must_use]
pub fn match_addr_mask(a1: &IpAddr, a2: &IpAddr, mask: &IpAddr) -> bool {
    if a1.is_ipv4() != a2.is_ipv4() || a1.is_ipv4() != mask.is_ipv4() {
        return false;
    }
    match (a1, a2, mask) {
        (IpAddr::V6(x), IpAddr::V6(y), IpAddr::V6(m)) => {
            // std::net::Ipv6Addr carries no scope id — upstream
            // compares it too; scoped addresses aren't representable
            // here so the check is vacuous (documented gap).
            let (b1, b2, mm) = (x.octets(), y.octets(), m.octets());
            (0..16).all(|i| (b1[i] & mm[i]) == (b2[i] & mm[i]))
        }
        (IpAddr::V4(x), IpAddr::V4(y), IpAddr::V4(m)) => {
            (u32::from(*x) & u32::from(*m)) == (u32::from(*y) & u32::from(*m))
        }
        _ => false,
    }
}

/// `get_gateway` — the default-route gateway on `iface`, or none.
/// Local (non-global) v6 addresses can't reach the internet.
#[must_use]
pub fn get_gateway<'a>(iface: &IpInterface, routes: &'a [IpRoute]) -> Option<&'a IpAddr> {
    if iface.interface_address.is_ipv6() && is_local(&iface.interface_address) {
        return None;
    }
    routes
        .iter()
        .find(|r| {
            r.destination.is_unspecified()
                && r.destination.is_ipv4() == iface.interface_address.is_ipv4()
                && !r.gateway.is_unspecified()
                && (r.source_hint.is_none() || r.source_hint == Some(iface.interface_address))
                && r.name == iface.name
        })
        .map(|r| &r.gateway)
}

/// `has_any_internet_route` — any route to unspecified or global
/// destination.
#[must_use]
pub fn has_any_internet_route(routes: &[IpRoute]) -> bool {
    routes
        .iter()
        .any(|r| r.destination.is_unspecified() || is_global(&r.destination))
}

/// `has_internet_route(device, family)` — `v4` picks the family.
#[must_use]
pub fn has_internet_route(device: &str, v4: bool, routes: &[IpRoute]) -> bool {
    routes.iter().any(|r| {
        r.destination.is_ipv4() == v4
            && r.name == device
            && (r.destination.is_unspecified() || is_global(&r.destination))
    })
}

/// `device_for_address` — the interface name owning `addr`.
#[must_use]
pub fn device_for_address<'a>(addr: &IpAddr, ifs: &'a [IpInterface]) -> Option<&'a str> {
    ifs.iter()
        .find(|i| i.interface_address == *addr)
        .map(|i| i.name.as_str())
}

/// `enum_net_interfaces` — the OS enumeration half. Upstream uses
/// `GetAdaptersAddresses`/`getifaddrs`; `if-addrs` wraps both and
/// yields the same (address, netmask, name) tuples. Loopbacks are
/// *kept* and flagged `LOOPBACK` — upstream returns them and the
/// session's unspecified-address expansion turns them into
/// local-network listen sockets. `if-addrs` doesn't surface the full
/// flag set, DAD state or friendly names — `UP`/`preferred`/`Unknown`
/// are upstream's defaults.
#[must_use]
pub fn enum_interfaces() -> Vec<IpInterface> {
    let mut out = Vec::new();
    let Ok(ifs) = if_addrs::get_if_addrs() else {
        return out;
    };
    for i in ifs {
        let (interface_address, netmask) = match &i.addr {
            if_addrs::IfAddr::V4(a) => (IpAddr::V4(a.ip), IpAddr::V4(a.netmask)),
            if_addrs::IfAddr::V6(a) => (IpAddr::V6(a.ip), IpAddr::V6(a.netmask)),
        };
        let flags = if i.is_loopback() {
            InterfaceFlags::UP | InterfaceFlags::LOOPBACK
        } else {
            InterfaceFlags::UP
        };
        out.push(IpInterface {
            interface_address,
            netmask,
            friendly_name: i.name.clone(),
            name: i.name,
            description: String::new(),
            preferred: true,
            flags,
            state: IfState::Unknown,
        });
    }
    out
}

/// `enum_routes` — the OS routing table. Upstream uses netlink
/// `RTM_GETROUTE`/`GetIpForwardTable2`; `netroute` wraps both and is
/// pure safe Rust. `source_hint`/`mtu` aren't surfaced by the crate —
/// `get_gateway` tolerates `None` hints, `mtu` stays 0.
#[must_use]
pub fn enum_routes() -> Vec<IpRoute> {
    let Ok(routes) = netroute::list_routes() else {
        return Vec::new();
    };
    routes
        .into_iter()
        .map(|r| IpRoute {
            destination: r.destination.addr,
            netmask: build_netmask(
                i32::from(r.destination.prefix_len),
                r.destination.addr.is_ipv4(),
            ),
            gateway: r.gateway.unwrap_or_else(|| {
                if r.destination.addr.is_ipv4() {
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                } else {
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED)
                }
            }),
            source_hint: None,
            name: r
                .ifname
                .or_else(|| r.ifindex.map(|i| i.to_string()))
                .unwrap_or_default(),
            mtu: 0,
        })
        .collect()
}

/// Convenience: the default IPv4 gateway — `start_natpmp`'s argument
/// (ADR-0019). First non-loopback interface with a default route.
#[must_use]
pub fn default_gateway_v4() -> Option<IpAddr> {
    // interface names don't line up with netroute's ifindex names —
    // find the v4 default route directly (upstream does the same
    // logical search per-interface then prefers the first hit).
    enum_routes()
        .into_iter()
        .find(|r| {
            r.destination.is_ipv4() && r.destination.is_unspecified() && !r.gateway.is_unspecified()
        })
        .map(|r| r.gateway)
}

/// `ip_notifier` — interface-change notification. Upstream hooks OS
/// route-notification APIs (netlink `RTMGRP_LINK`, `NotifyRouteChange`);
/// those can't run behind injected transports, so we poll-diff the
/// enumerated interface set: same "the set changed → alert" contract.
pub struct IpNotifier {
    /// last seen `(name, addr)` set.
    prev: std::collections::BTreeSet<(String, IpAddr)>,
}

impl Default for IpNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl IpNotifier {
    /// snapshot the current table as the baseline.
    #[must_use]
    pub fn new() -> Self {
        Self { prev: snapshot() }
    }

    /// `poll` — re-enumerate; `true` when the set changed (the caller
    /// posts `network_config_change_alert` upstream).
    pub fn poll(&mut self) -> bool {
        let cur = snapshot();
        let changed = cur != self.prev;
        if changed {
            self.prev = cur;
        }
        changed
    }
}

fn snapshot() -> std::collections::BTreeSet<(String, IpAddr)> {
    enum_interfaces()
        .into_iter()
        .map(|i| (i.name, i.interface_address))
        .collect()
}
