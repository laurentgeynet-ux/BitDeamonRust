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

/// Parsed mapping response (`on_reply` result).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NatPmpResponse {
    /// opcode 0 reply — the gateway's public IP.
    PublicIp(Ipv4Addr),
    /// opcode 1/2 reply — a mapping result.
    Map {
        /// private port.
        private_port: u16,
        /// external port granted.
        public_port: u16,
        /// lease lifetime in seconds.
        lifetime: u32,
        /// result code (0 = success).
        result: u16,
    },
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

/// `send_get_ip_address_request` — `[0][0]` (opcode 0 query).
#[must_use]
pub fn build_ip_request() -> [u8; 2] {
    [0, 0]
}

/// `on_reply` — parse a NAT-PMP response datagram.
/// `None` on version/opcode/length mismatch (upstream drops).
#[must_use]
pub fn parse_response(buf: &[u8]) -> Option<NatPmpResponse> {
    if buf.len() < 4 || buf[0] != 0 {
        return None;
    }
    let op = buf[1];
    let result = u16::from_be_bytes([buf[2], buf[3]]);
    if op == 0 {
        // public-IP reply: [v][op][result u16][epoch u32][ip u32]
        if buf.len() < 12 {
            return None;
        }
        return Some(NatPmpResponse::PublicIp(Ipv4Addr::new(
            buf[8], buf[9], buf[10], buf[11],
        )));
    }
    if (op == 1 || op == 2) && buf.len() >= 16 {
        let private_port = u16::from_be_bytes([buf[8], buf[9]]);
        let public_port = u16::from_be_bytes([buf[10], buf[11]]);
        let lifetime = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        return Some(NatPmpResponse::Map {
            private_port,
            public_port,
            lifetime,
            result,
        });
    }
    None
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
