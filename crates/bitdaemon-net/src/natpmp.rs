// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `natpmp` — NAT-PMP wire codec (RFC 6886). Upstream's class owns
//! a socket + timers; here each datagram is built/parsed by the
//! caller (injected transport — and note: anonymous lanes should
//! NOT map real ports; this exists for clearnet fallbacks).

use std::net::Ipv4Addr;

/// `protocol_version` — `version_natpmp` / `version_pcp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolVersion {
    /// NAT-PMP (version 0).
    NatPmp,
    /// PCP (version 2).
    Pcp,
}

/// `portmap_protocol`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortmapProtocol {
    /// udp → op 1 (NAT-PMP) / 17 (PCP).
    Udp,
    /// tcp → op 2 (NAT-PMP) / 6 (PCP).
    Tcp,
}

/// `portmap_action`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortmapAction {
    /// `add` — create/renew (ttl > 0).
    Add,
    /// `delete` — ttl 0.
    Delete,
}

/// `opcode_map` — PCP MAP opcode (RFC 6887 §19.2).
const PCP_OPCODE_MAP: u8 = 1;

/// Parsed mapping response (`on_reply` result).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NatPmpResponse {
    /// opcode 0 reply — the gateway's public IP.
    PublicIp(Ipv4Addr),
    /// opcode 1/2 reply — a mapping result.
    Map {
        /// protocol the reply refers to (opcode 1 = UDP, 2 = TCP).
        protocol: PortmapProtocol,
        /// private port.
        private_port: u16,
        /// external port granted.
        public_port: u16,
        /// lease lifetime in seconds.
        lifetime: u32,
        /// result code (0 = success).
        result: u16,
        /// PCP nonce echo (12 bytes) — the caller matches it against
        /// the outstanding request like upstream's mapping table.
        nonce: Option<[u8; 12]>,
        /// PCP-assigned external address (`unspecified` for NAT-PMP).
        external_address: Option<std::net::IpAddr>,
    },
    /// `pcp_unsupp_version` — upstream falls back to NAT-PMP when
    /// the socket isn't v6; the caller should resend as NAT-PMP.
    UnsupportedVersion,
}

/// `send_map_request` — NAT-PMP mapping datagram (12 bytes:
/// `[0][op][0 0][priv][pub][ttl]`).
#[must_use]
pub fn build_map_request(
    protocol: PortmapProtocol,
    local_port: u16,
    external_port: u16,
    ttl: u32,
) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0] = 0; // version_natpmp
    out[1] = match protocol {
        PortmapProtocol::Udp => 1,
        PortmapProtocol::Tcp => 2,
    };
    out[4..6].copy_from_slice(&local_port.to_be_bytes());
    out[6..8].copy_from_slice(&external_port.to_be_bytes());
    out[8..12].copy_from_slice(&ttl.to_be_bytes());
    out
}

/// `send_map_request` PCP variant — 60-byte MAP request
/// (`[v=2][opcode_map][rsv][ttl][local-v6 16][nonce 12][proto]
/// [rsv 3][priv][pub][external-v6 16]`). `local_addr`/`external_addr`
/// select the v4-mapped encoding like upstream; `external_addr` of
/// `None` follows upstream's `is_local`/same-addr fallbacks.
#[must_use]
pub fn build_pcp_map_request(
    protocol: PortmapProtocol,
    local_port: u16,
    external_port: u16,
    ttl: u32,
    local_addr: std::net::IpAddr,
    external_addr: Option<std::net::IpAddr>,
    nonce: &[u8; 12],
) -> [u8; 60] {
    use std::net::IpAddr;
    let to_v6 = |a: IpAddr| -> std::net::Ipv6Addr {
        match a {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => v6,
        }
    };
    let mut out = [0u8; 60];
    out[0] = 2; // version_pcp
    out[1] = PCP_OPCODE_MAP;
    out[4..8].copy_from_slice(&ttl.to_be_bytes());
    out[8..24].copy_from_slice(&to_v6(local_addr).octets());
    out[24..36].copy_from_slice(nonce);
    out[36] = match protocol {
        PortmapProtocol::Tcp => 6,
        PortmapProtocol::Udp => 17,
    };
    out[40..42].copy_from_slice(&local_port.to_be_bytes());
    out[42..44].copy_from_slice(&external_port.to_be_bytes());
    // upstream external-address selection
    let ext = match external_addr {
        Some(a) if !a.is_unspecified() => to_v6(a),
        _ if crate::ip_helpers::is_local(&local_addr) => {
            if local_addr.is_ipv4() {
                std::net::Ipv4Addr::UNSPECIFIED.to_ipv6_mapped()
            } else {
                std::net::Ipv6Addr::UNSPECIFIED
            }
        }
        _ => to_v6(local_addr),
    };
    out[44..60].copy_from_slice(&ext.octets());
    out
}

/// `send_get_ip_address_request` — `[0][0]` (opcode 0 query).
#[must_use]
pub fn build_ip_request() -> [u8; 2] {
    [0, 0]
}

/// `on_reply` — parse a NAT-PMP/PCP response datagram.
/// `None` on version/opcode/length mismatch (upstream drops).
///
/// Layouts (upstream `natpmp::on_reply`):
/// - NAT-PMP: `[v=0][cmd][result u16][epoch u32][…]` — cmd 0x80 =
///   public-IP (12 B min); cmd 0x81/0x82 = map reply (16 B exact).
/// - PCP: `[v=2][cmd|0x80 masked][rsv u8][result u8][lifetime u32]
///   [epoch u32][rsv 12][nonce 12][proto u8][rsv 3][priv][pub]
///   [external-v6 16]` (60 B exact).
#[must_use]
pub fn parse_response(buf: &[u8]) -> Option<NatPmpResponse> {
    if buf.len() < 4 {
        return None;
    }
    let version = buf[0];
    if version != 0 && version != 2 {
        return None;
    }
    let pcp = version == 2;
    let op = if pcp { buf[1] & 0x7f } else { buf[1] };
    // result field: u8 for PCP (after a reserved byte), u16 for NAT-PMP
    let result = if pcp {
        u16::from(buf[3])
    } else {
        u16::from_be_bytes([buf[2], buf[3]])
    };
    // `pcp_unsupp_version` → upstream falls back to NAT-PMP
    if pcp && result == 1 {
        return Some(NatPmpResponse::UnsupportedVersion);
    }
    if (!pcp && buf.len() < 12) || (pcp && buf.len() < 24) {
        return None;
    }
    // PCP: lifetime precedes epoch; NAT-PMP: epoch at bytes 4..8
    let pcp_lifetime = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

    if !pcp && op == 0x80 {
        // public-IP reply — NAT-PMP only (`send_get_ip_address_request`)
        return Some(NatPmpResponse::PublicIp(Ipv4Addr::new(
            buf[8], buf[9], buf[10], buf[11],
        )));
    }

    // map replies must be the *exact* datagram size upstream requires
    if buf.len() != if pcp { 60 } else { 16 } {
        return None;
    }

    let (nonce, protocol, inx) = if pcp {
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&buf[24..36]);
        let protocol = if buf[36] == 6 {
            PortmapProtocol::Tcp
        } else {
            PortmapProtocol::Udp
        };
        (Some(nonce), protocol, 40)
    } else {
        // upstream: `cmd - 0x80 == 1` → udp, anything else → tcp
        (
            None,
            if buf[1] == 0x81 {
                PortmapProtocol::Udp
            } else {
                PortmapProtocol::Tcp
            },
            8,
        )
    };
    let private_port = u16::from_be_bytes([buf[inx], buf[inx + 1]]);
    let public_port = u16::from_be_bytes([buf[inx + 2], buf[inx + 3]]);
    let lifetime = if pcp {
        pcp_lifetime
    } else {
        u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]])
    };
    let external_address = if pcp {
        let mut o = [0u8; 16];
        o.copy_from_slice(&buf[44..60]);
        let v6 = std::net::Ipv6Addr::from(o);
        Some(match v6.to_ipv4_mapped() {
            Some(v4) => std::net::IpAddr::V4(v4),
            None => std::net::IpAddr::V6(v6),
        })
    } else {
        None
    };
    Some(NatPmpResponse::Map {
        protocol,
        private_port,
        public_port,
        lifetime,
        result,
        nonce,
        external_address,
    })
}

/// `from_result_code` — human-readable result.
#[must_use]
pub fn result_string(result: u16) -> &'static str {
    match result {
        0 => "success",
        1 => "unsupported version",
        2 => "not authorized",
        3 => "network failure",
        4 => "out of resources",
        5 => "unsupported opcode",
        _ => "unknown error",
    }
}
