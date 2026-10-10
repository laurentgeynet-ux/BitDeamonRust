// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `ut_pex.cpp` (BEP-11) — Peer Exchange wire format.
//! The torrent plugin's 60-second rebuild cadence and the
//! `send_peer` eligibility rules live in the session; this module
//! owns the message codec (`{added, added.f, dropped, added6,
//! added6.f, dropped6}`) and the known-peer bookkeeping.

use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};

use bitdaemon_bencode::{Entry, NodeType, bdecode_checked, bencode};
use bitdaemon_net::socket_io::{Cursor, read_endpoint, write_endpoint};

/// `pex_flags_t` — flag bits carried in `added.f`/`added6.f`.
pub mod flags {
    /// `pex_encryption` — the peer supports protocol encryption.
    pub const PEX_ENCRYPTION: u8 = 0x01;
    /// `pex_seed` — the peer is a seed.
    pub const PEX_SEED: u8 = 0x02;
    /// `pex_utp` — the peer supports uTP.
    pub const PEX_UTP: u8 = 0x04;
    /// `pex_holepunch` — rendezvous candidate for holepunch.
    pub const PEX_HOLEPUNCH: u8 = 0x08;
    /// `pex_lt_v2` — internal libtorrent flag, stripped on receipt.
    pub const PEX_LT_V2: u8 = 0x80;
}

/// `max_peer_entries` — caps the pex packet size.
pub const MAX_PEER_ENTRIES: usize = 100;

/// One pex update's worth of added peers (`added` + flags).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PexPeer {
    /// peer endpoint.
    pub addr: SocketAddr,
    /// `added.f` flag bits (with `PEX_LT_V2` stripped on receipt).
    pub flags: u8,
}

/// Build a pex update message (`pex` bencoded dict) from the
/// current peer set vs. the previous one. Returns the encoded
/// body and the new `old_peers` set (all sent endpoints).
///
/// `peers` are `(endpoint, flags)` for currently connected,
/// pex-eligible peers (the caller applies `send_peer` semantics).
/// `dropped` is the previous message's sent set; endpoints absent
/// from `peers` are listed under `dropped`/`dropped6`.
#[must_use]
pub fn build_pex_message(
    peers: &[(SocketAddr, u8)],
    prev_sent: &BTreeSet<SocketAddr>,
) -> (Vec<u8>, BTreeSet<SocketAddr>) {
    let mut added = Vec::new();
    let mut added_f = Vec::new();
    let mut added6 = Vec::new();
    let mut added6_f = Vec::new();
    let mut dropped = Vec::new();
    let mut dropped6 = Vec::new();

    let mut sent = BTreeSet::new();
    let mut prev = prev_sent.clone();
    let mut num_added = 0usize;

    for (ep, fl) in peers {
        let is_new = !prev.contains(ep);
        // already-sent peers stay in `sent` but aren't re-listed;
        // they're also not dropped
        let is_v4 = matches!(ep, SocketAddr::V4(_));
        if is_new {
            if num_added >= MAX_PEER_ENTRIES {
                break;
            }
            if is_v4 {
                write_endpoint(*ep, &mut added);
                added_f.push(*fl);
            } else {
                write_endpoint(*ep, &mut added6);
                added6_f.push(*fl);
            }
            num_added += 1;
        }
        prev.remove(ep);
        sent.insert(*ep);
    }
    // whatever remains in `prev` was sent last time but is gone
    for ep in prev {
        if ep.is_ipv4() {
            write_endpoint(ep, &mut dropped);
        } else {
            write_endpoint(ep, &mut dropped6);
        }
    }

    let mut e = Entry::Dict(Default::default());
    let d = e.dict_mut().expect("dict");
    d.insert(b"added".to_vec(), Entry::String(added));
    d.insert(b"added.f".to_vec(), Entry::String(added_f));
    d.insert(b"dropped".to_vec(), Entry::String(dropped));
    d.insert(b"added6".to_vec(), Entry::String(added6));
    d.insert(b"added6.f".to_vec(), Entry::String(added6_f));
    d.insert(b"dropped6".to_vec(), Entry::String(dropped6));
    (bencode(&e), sent)
}

/// Parse a received pex message. Returns `(added, dropped)` —
/// added peers carry their flag bits (`pex_lt_v2` stripped, as
/// upstream does). `max_peers` mirrors `max_pex_peers`.
///
/// Malformed input (`ret != 0`, non-dict, `added.f` length
/// mismatch) yields empty lists — the caller disconnects with
/// `invalid_pex_message` as upstream does.
pub fn parse_pex_message(
    body: &[u8],
    max_peers: usize,
    already_known: usize,
) -> Option<(Vec<PexPeer>, Vec<SocketAddr>)> {
    let msg = bdecode_checked(body).ok()?;
    if msg.node_type() != NodeType::Dict {
        return None;
    }
    let mut added = Vec::new();
    let mut dropped = Vec::new();
    let mut known = already_known;

    for (key, is_v6, is_flags) in [
        (&b"added"[..], false, false),
        (&b"added6"[..], true, false),
        (&b"dropped"[..], false, true),
        (&b"dropped6"[..], true, true),
    ] {
        let node = msg.dict_find_string(key);
        if node.node_type() != NodeType::String {
            continue;
        }
        let s = node.string_value();
        let rec = if is_v6 { 18 } else { 6 };
        let n = s.len() / rec;
        if key == b"dropped" || key == b"dropped6" {
            let _ = is_flags;
            let mut cur = Cursor::new(&s[..n * rec]);
            for _ in 0..n {
                if let Ok(ep) = read_endpoint(&mut cur, is_v6) {
                    dropped.push(ep);
                }
            }
            continue;
        }
        // added — needs the matching `added[6].f` flags string
        let fkey: &[u8] = if is_v6 { b"added6.f" } else { b"added.f" };
        let fnode = msg.dict_find_string(fkey);
        if fnode.node_type() != NodeType::String {
            continue;
        }
        let fs = fnode.string_value();
        if fs.len() != n {
            continue;
        }
        let mut cur = Cursor::new(&s[..n * rec]);
        for &f in fs.iter().take(n) {
            let ep = read_endpoint(&mut cur, is_v6);
            let f = f & !flags::PEX_LT_V2;
            if let Ok(ep) = ep {
                if known >= max_peers {
                    break;
                }
                known += 1;
                added.push(PexPeer { addr: ep, flags: f });
            }
        }
    }
    Some((added, dropped))
}

/// `is_local` guard helper — upstream skips local added addresses
/// unless the sending peer is itself local.
#[must_use]
pub fn is_local(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => a.is_private() || a.is_loopback() || a.is_link_local(),
        IpAddr::V6(a) => {
            a.is_loopback()
                || (a.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10
                || (a.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7
        }
    }
}
