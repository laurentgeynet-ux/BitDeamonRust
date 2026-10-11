// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of the `ut_holepunch` pieces of `bt_peer_connection.cpp`
//! (BEP-55). Wire payload inside the extended message (after the
//! extension id byte):
//!
//! ```text
//! u8  msg_type  0=rendezvous 1=connect 2=failed
//! u8  addr_type 0=IPv4 1=IPv6
//!     endpoint  v4: 4+2 bytes, v6: 16+2 bytes
//! u32 error     only when msg_type == failed
//! ```
//!
//! Relay semantics (upstream `on_holepunch`): a `rendezvous(ep)`
//! asks us to introduce the sender to our connection at `ep`; we
//! answer `failed` when the target is unknown (`not_connected`),
//! lacks the extension (`no_support`), is the sender itself
//! (`no_self`), or would bridge local↔non-local (silently dropped).
//! On success both sides get `connect` of the other's endpoint.
//! `connect(ep)` marks `ep` as a uTP connect candidate — the caller
//! (torrent) does `add_peer`/`connect_to_peer`; we only surface the
//! decision here.

use std::net::SocketAddr;

/// `hp_message` — wire message type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HpMessage {
    /// `rendezvous` — "please introduce me to `ep`".
    Rendezvous,
    /// `connect` — "dial `ep` (uTP, holepunch mode)".
    Connect,
    /// `failed` — carries an [`HpError`] code.
    Failed,
}

/// `hp_error` — failure codes on `failed` messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum HpError {
    /// `no_such_peer` (1).
    NoSuchPeer = 1,
    /// `not_connected` (2) — we have no connection to `ep`.
    NotConnected = 2,
    /// `no_support` (3) — target lacks `ut_holepunch`.
    NoSupport = 3,
    /// `no_self` (4) — `ep` is the requester itself.
    NoSelf = 4,
}

impl HpError {
    fn from_u32(v: u32) -> Option<Self> {
        Some(match v {
            1 => Self::NoSuchPeer,
            2 => Self::NotConnected,
            3 => Self::NoSupport,
            4 => Self::NoSelf,
            _ => return None,
        })
    }
}

/// Decoded holepunch message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hp {
    /// message type.
    pub msg: HpMessage,
    /// endpoint carried by the message.
    pub ep: SocketAddr,
    /// error code (only `Failed`).
    pub error: Option<HpError>,
}

fn put_ep(out: &mut Vec<u8>, ep: &SocketAddr) {
    match ep {
        SocketAddr::V4(a) => {
            out.push(0);
            out.extend_from_slice(&a.ip().octets());
        }
        SocketAddr::V6(a) => {
            out.push(1);
            out.extend_from_slice(&a.ip().octets());
        }
    }
    out.extend_from_slice(&ep.port().to_be_bytes());
}

/// `write_holepunch_msg` — encode the payload (without the extended
/// header — the caller wraps it in `PeerMessage::Extended`).
#[must_use]
pub fn encode_hp(msg: HpMessage, ep: &SocketAddr, error: HpError) -> Vec<u8> {
    let mut out = Vec::with_capacity(35);
    out.push(match msg {
        HpMessage::Rendezvous => 0,
        HpMessage::Connect => 1,
        HpMessage::Failed => 2,
    });
    put_ep(&mut out, ep);
    if msg == HpMessage::Failed {
        out.extend_from_slice(&(error as u32).to_be_bytes());
    }
    out
}

/// `on_holepunch` decode — `None` on malformed input (upstream
/// silently ignores undersized payloads and unknown addr types).
#[must_use]
pub fn decode_hp(buf: &[u8]) -> Option<Hp> {
    if buf.len() < 2 {
        return None;
    }
    let msg = match buf[0] {
        0 => HpMessage::Rendezvous,
        1 => HpMessage::Connect,
        2 => HpMessage::Failed,
        _ => return None, // upstream ignores `> failed`
    };
    let ep = match buf[1] {
        0 => {
            if buf.len() < 2 + 4 + 2 {
                return None;
            }
            SocketAddr::from((
                [buf[2], buf[3], buf[4], buf[5]],
                u16::from_be_bytes([buf[6], buf[7]]),
            ))
        }
        1 => {
            if buf.len() < 2 + 16 + 2 {
                return None;
            }
            let mut a = [0u8; 16];
            a.copy_from_slice(&buf[2..18]);
            SocketAddr::from((
                std::net::Ipv6Addr::from(a),
                u16::from_be_bytes([buf[18], buf[19]]),
            ))
        }
        _ => return None,
    };
    let error = if msg == HpMessage::Failed {
        if buf.len() < 2 + ep_len(&ep) + 4 {
            return None;
        }
        let off = 2 + ep_len(&ep);
        HpError::from_u32(u32::from_be_bytes([
            buf[off],
            buf[off + 1],
            buf[off + 2],
            buf[off + 3],
        ]))
    } else {
        None
    };
    Some(Hp { msg, ep, error })
}

fn ep_len(ep: &SocketAddr) -> usize {
    match ep {
        SocketAddr::V4(_) => 6,
        SocketAddr::V6(_) => 18,
    }
}

/// Relay decision for an inbound `rendezvous` — returns
/// `(reply_to_requester, connect_to_target)` payloads to send.
/// `target` describes our connection at `ep` when one exists.
pub enum Rendezvous {
    /// send `connect(ep)` back and `connect(requester)` to `ep`'s
    /// connection — caller routes the second message.
    Bridge,
    /// send `failed(ep, err)` back.
    Fail(HpError),
    /// silently drop (local↔non-local bridging refusal upstream).
    Drop,
}

/// `rendezvous` decision — `target` is `Some((supports_holepunch,
/// is_self))` when we have a connection to `ep`; `remote_local` is
/// `is_local(requester)`.
#[must_use]
pub fn rendezvous(ep: &SocketAddr, target: Option<(bool, bool)>, remote_local: bool) -> Rendezvous {
    let Some((supports, is_self)) = target else {
        return Rendezvous::Fail(HpError::NotConnected);
    };
    if is_self {
        return Rendezvous::Fail(HpError::NoSelf);
    }
    if !supports {
        return Rendezvous::Fail(HpError::NoSupport);
    }
    if bitdaemon_net::ip_helpers::is_local(&ep.ip()) != remote_local {
        return Rendezvous::Drop;
    }
    Rendezvous::Bridge
}

/// `connect` gate — upstream refuses local endpoints coming from a
/// non-local peer. Returns whether the caller should `add_peer` +
/// `connect_to_peer` the endpoint.
#[must_use]
pub fn connect_allowed(ep: &SocketAddr, remote_local: bool) -> bool {
    !(bitdaemon_net::ip_helpers::is_local(&ep.ip()) && !remote_local)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_roundtrip() {
        for ep in [
            "1.2.3.4:6881".parse().unwrap(),
            "[2001:db8::1]:51413".parse().unwrap(),
        ] {
            for msg in [HpMessage::Rendezvous, HpMessage::Connect, HpMessage::Failed] {
                let buf = encode_hp(msg, &ep, HpError::NoSupport);
                let d = decode_hp(&buf).unwrap();
                assert_eq!((d.msg, d.ep), (msg, ep));
                assert_eq!(d.error.is_some(), msg == HpMessage::Failed);
            }
        }
    }

    #[test]
    fn codec_malformed() {
        assert!(decode_hp(&[]).is_none());
        assert!(decode_hp(&[0]).is_none());
        assert!(decode_hp(&[3, 0, 0, 0, 0, 0, 0, 0]).is_none());
        assert!(decode_hp(&[0, 0, 1]).is_none()); // truncated v4
        assert!(decode_hp(&[0, 2]).is_none()); // bad addr type
    }

    #[test]
    fn rendezvous_semantics() {
        let ep: SocketAddr = "8.8.8.8:1".parse().unwrap();
        // not connected
        assert!(matches!(
            rendezvous(&ep, None, false),
            Rendezvous::Fail(HpError::NotConnected)
        ));
        // no extension support
        assert!(matches!(
            rendezvous(&ep, Some((false, false)), false),
            Rendezvous::Fail(HpError::NoSupport)
        ));
        // self
        assert!(matches!(
            rendezvous(&ep, Some((true, true)), false),
            Rendezvous::Fail(HpError::NoSelf)
        ));
        // ok — global ↔ global
        assert!(matches!(
            rendezvous(&ep, Some((true, false)), false),
            Rendezvous::Bridge
        ));
        // local target from global requester → silent drop
        let local: SocketAddr = "192.168.1.2:6881".parse().unwrap();
        assert!(matches!(
            rendezvous(&local, Some((true, false)), false),
            Rendezvous::Drop
        ));
        assert!(matches!(
            rendezvous(&local, Some((true, false)), true),
            Rendezvous::Bridge
        ));
    }

    #[test]
    fn connect_gate() {
        let local: SocketAddr = "10.0.0.5:6881".parse().unwrap();
        let global: SocketAddr = "8.8.8.8:1".parse().unwrap();
        assert!(!connect_allowed(&local, false));
        assert!(connect_allowed(&local, true));
        assert!(connect_allowed(&global, false));
    }
}
