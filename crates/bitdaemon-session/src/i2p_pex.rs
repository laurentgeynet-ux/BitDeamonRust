// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `extensions/i2p_pex.cpp` — the `i2p_pex` extension
//! (BEP-10 id 9): peers exchange I2P *destination hashes* instead of
//! IP:port. Same message envelope as `ut_pex` (`added`, `added.f`,
//! `dropped`) but 32-byte entries and no flags.
//!
//! Upstream splits this into a `torrent_plugin` (per-swarm message
//! cache + 60 s rebuild tick) and a `peer_plugin` (per-connection
//! rate-limit + send state). Same split here: `I2pPexState` lives on
//! `Torrent`, `I2pPexPeerState` on `PeerConn`.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use bitdaemon_common::Sha256Hash;
use bitdaemon_net::peer_wire::PeerMessage;

use crate::peer_conn::PeerConn;
/// `extension_index` — the fixed `m` key upstream advertises.
pub const I2P_PEX_ID: u8 = 9;

/// `max_peer_entries`.
pub const MAX_PEER_ENTRIES: usize = 50;

/// `seconds(60)` rebuild/send cadence.
pub const PEX_INTERVAL: Duration = Duration::from_secs(60);

/// `i2p_pex_plugin` — the per-torrent side. Rebuilds the diff message
/// every `PEX_INTERVAL`; peers pick up `msg` verbatim.
#[derive(Debug, Default)]
pub struct I2pPexState {
    /// `m_old_peers` — destination hashes sent last round.
    old_peers: BTreeSet<Sha256Hash>,
    /// `m_last_msg`.
    last_msg: Option<Instant>,
    /// `m_i2p_pex_msg` — the bencoded `added/dropped/added.f` diff.
    msg: Vec<u8>,
    /// `m_peers_in_message`.
    peers_in_msg: usize,
}

/// `include_peer` — eligible connections: established i2p peers that
/// dialed us or shared a listen port.
fn include_peer(c: &PeerConn) -> bool {
    if !c.outgoing && !c.received_listen_port {
        return false;
    }
    if c.connecting || !c.handshake_done {
        return false;
    }
    c.i2p.is_some()
}

/// sha256(base64decode_i2p(dest)) — the peer identity used in pex.
fn peer_hash(dest: &[u8]) -> Option<Sha256Hash> {
    use sha2::Digest;
    let bin = bitdaemon_core::escape_string::base64decode_i2p(dest)?;
    let h: [u8; 32] = sha2::Sha256::digest(&bin).into();
    Sha256Hash::from_bytes(&h)
}

impl I2pPexState {
    /// `i2p_pex_plugin::tick` — rebuild the diff message. `peers` is
    /// the torrent's live connections; `disable_pex` mirrors
    /// `torrent_flags::disable_pex`.
    pub fn tick<'a>(
        &mut self,
        now: Instant,
        peers: impl Iterator<Item = &'a PeerConn>,
        disable_pex: bool,
    ) {
        if disable_pex {
            return;
        }
        if self
            .last_msg
            .is_some_and(|t| now.duration_since(t) < PEX_INTERVAL)
        {
            return;
        }
        self.last_msg = Some(now);

        let mut dropped: BTreeSet<Sha256Hash> = std::mem::take(&mut self.old_peers);
        let mut added = Vec::new();
        let mut added_f = Vec::new();
        self.peers_in_msg = 0;
        let mut num_added = 0;

        for c in peers {
            if !include_peer(c) {
                continue;
            }
            let Some(remote) = c.i2p.as_deref().and_then(peer_hash) else {
                continue;
            };
            self.old_peers.insert(remote);
            if dropped.remove(&remote) {
                continue; // still present — not dropped
            }
            if num_added >= MAX_PEER_ENTRIES {
                break;
            }
            added.extend_from_slice(remote.as_bytes());
            added_f.push(0); // no i2p pex flags
            num_added += 1;
            self.peers_in_msg += 1;
        }
        let mut dropped_bytes = Vec::new();
        for d in &dropped {
            dropped_bytes.extend_from_slice(d.as_bytes());
            self.peers_in_msg += 1;
        }

        let mut pex = bitdaemon_bencode::Entry::Dict(Default::default());
        if let Ok(d) = pex.dict_mut() {
            d.insert(b"added".to_vec(), bitdaemon_bencode::Entry::String(added));
            d.insert(
                b"added.f".to_vec(),
                bitdaemon_bencode::Entry::String(added_f),
            );
            d.insert(
                b"dropped".to_vec(),
                bitdaemon_bencode::Entry::String(dropped_bytes),
            );
        }
        self.msg = bitdaemon_bencode::bencode(&pex);
    }

    /// `peers_in_msg`.
    #[must_use]
    pub fn peers_in_msg(&self) -> usize {
        self.peers_in_msg
    }
}

/// `i2p_pex_peer_plugin` — per-connection state.
#[derive(Debug, Default)]
pub struct I2pPexPeerState {
    /// `m_last_pex[6]` — sliding receive window (upstream: 6 slots to
    /// survive buffered reads).
    pub last_pex: [Option<Instant>; 6],
    /// `m_last_msg` — last send.
    pub last_msg: Option<Instant>,
    /// `m_first_time` — full list vs. diff on first send.
    pub first_time: bool,
}

/// verdict of `I2pPexPeerState::on_message`.
pub enum I2pPexVerdict {
    /// added destination hashes — feed `peer_list.add_peer_i2p`.
    Added(Vec<Sha256Hash>),
    /// `pex_message_too_large` / `too_frequent_pex` /
    /// `invalid_pex_message` — drop the connection.
    Disconnect(&'static str),
    /// nothing actionable (empty `added`, stale state).
    Nothing,
}

impl I2pPexPeerState {
    /// ctor equivalent — `first_time = true`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            first_time: true,
            ..Default::default()
        }
    }

    /// `on_extended` — parse an inbound `i2p_pex` body.
    /// `length` is the announced packet length (upstream disconnects
    /// above 500 KiB before reading the body).
    pub fn on_message(&mut self, now: Instant, body: &[u8], length: usize) -> I2pPexVerdict {
        if length > 500 * 1024 {
            return I2pPexVerdict::Disconnect("pex_message_too_large");
        }
        if self.last_pex[0].is_some_and(|t| now.duration_since(t) < PEX_INTERVAL) {
            return I2pPexVerdict::Disconnect("too_frequent_pex");
        }
        self.last_pex.rotate_left(1);
        self.last_pex[5] = Some(now);

        let node = bitdaemon_bencode::bdecode(body);
        if node.error().is_some() || node.node_type() != bitdaemon_bencode::NodeType::Dict {
            return I2pPexVerdict::Disconnect("invalid_pex_message");
        }
        let mut added = Vec::new();
        let p = node.dict_find_string(b"added");
        if p.node_type() == bitdaemon_bencode::NodeType::String {
            for chunk in p.string_value().chunks_exact(32) {
                added.extend(Sha256Hash::from_bytes(chunk));
            }
        }
        if added.is_empty() {
            I2pPexVerdict::Nothing
        } else {
            I2pPexVerdict::Added(added)
        }
    }

    /// `peer_plugin::tick` — send the cached diff, or `full_list`
    /// (prebuilt by the caller — the borrow of `Torrent::peers` makes
    /// an in-loop build impossible) on first send.
    /// Returns the `Extended` message to emit, if any.
    pub fn tick(
        &mut self,
        now: Instant,
        ext_id: u8,
        state: &I2pPexState,
        full_list: &[u8],
        disable_pex: bool,
    ) -> Option<PeerMessage> {
        if disable_pex || ext_id == 0 {
            return None;
        }
        if self
            .last_msg
            .is_some_and(|t| now.duration_since(t) < PEX_INTERVAL)
        {
            return None;
        }
        self.last_msg = Some(now);
        if self.first_time {
            self.first_time = false;
            Some(PeerMessage::Extended {
                id: ext_id,
                data: full_list.to_vec().into(),
            })
        } else {
            if state.peers_in_msg == 0 {
                return None;
            }
            Some(PeerMessage::Extended {
                id: ext_id,
                data: state.msg.clone().into(),
            })
        }
    }
}

/// `send_i2p_peer_list` body — full `added`/`added.f` snapshot of the
/// current i2p connections.
pub fn build_full_list<'a>(peers: impl Iterator<Item = &'a PeerConn>) -> Vec<u8> {
    let mut added = Vec::new();
    let mut added_f = Vec::new();
    let mut num = 0;
    for c in peers {
        if !include_peer(c) {
            continue;
        }
        if num >= MAX_PEER_ENTRIES {
            break;
        }
        let Some(remote) = c.i2p.as_deref().and_then(peer_hash) else {
            continue;
        };
        added.extend_from_slice(remote.as_bytes());
        added_f.push(0);
        num += 1;
    }
    let mut pex = bitdaemon_bencode::Entry::Dict(Default::default());
    if let Ok(d) = pex.dict_mut() {
        d.insert(b"added".to_vec(), bitdaemon_bencode::Entry::String(added));
        d.insert(
            b"added.f".to_vec(),
            bitdaemon_bencode::Entry::String(added_f),
        );
        d.insert(
            b"dropped".to_vec(),
            bitdaemon_bencode::Entry::String(Vec::new()),
        );
    }
    bitdaemon_bencode::bencode(&pex)
}

/// `torrent::add_i2p_peer` glue — apply an `Added` verdict to the
/// swarm list; true when any peer was added (`do_connect_boost`).
pub fn apply_added(t: &mut crate::torrent::Torrent, added: &[Sha256Hash]) -> bool {
    let mut any = false;
    for h in added {
        any |= t
            .peer_list
            .add_peer_i2p(
                h.as_bytes().to_vec(),
                bitdaemon_common::flags::PeerSourceFlags::PEX,
                bitdaemon_common::flags::PexFlags::NONE,
                &t.peer_list_state,
            )
            .is_some();
    }
    any
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_conn::PeerConn;
    use crate::peer_pipeline::PeerPipeline;

    fn i2p_conn(dest: &[u8], ext_id: u8) -> PeerConn {
        let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
        c.i2p = Some(dest.to_vec());
        c.ext_i2p_pex = ext_id;
        c.connecting = false;
        c.handshake_done = true;
        c.received_listen_port = true;
        c
    }

    #[test]
    fn peer_hash_decodes_and_sha256() {
        // base64-I2P alphabet: "AAAA" decodes to 3 zero bytes.
        let h = peer_hash(b"AAAA").unwrap();
        use sha2::Digest;
        let expect: [u8; 32] = sha2::Sha256::digest([0u8; 3]).into();
        assert_eq!(h.as_bytes(), &expect);
        assert!(peer_hash(b"!!!not-base64").is_none());
    }

    #[test]
    fn full_list_encodes_added() {
        let dest = b"QUFBQUFB"; // decodes to "AAAAAA"
        let c = i2p_conn(dest, 9);
        let body = build_full_list([&c].into_iter());
        let node = bitdaemon_bencode::bdecode(&body);
        assert_eq!(node.node_type(), bitdaemon_bencode::NodeType::Dict);
        let added = node.dict_find_string(b"added");
        assert_eq!(added.string_value().len(), 32);
        let h = peer_hash(dest).unwrap();
        assert_eq!(added.string_value(), h.as_bytes());
    }

    #[test]
    fn on_message_added_and_rate_limit() {
        let mut st = I2pPexPeerState::new();
        let now = Instant::now();
        let hash = [7u8; 32];
        let mut pex = bitdaemon_bencode::Entry::Dict(Default::default());
        if let Ok(d) = pex.dict_mut() {
            d.insert(
                b"added".to_vec(),
                bitdaemon_bencode::Entry::String(hash.to_vec()),
            );
            d.insert(
                b"added.f".to_vec(),
                bitdaemon_bencode::Entry::String(Vec::new()),
            );
            d.insert(
                b"dropped".to_vec(),
                bitdaemon_bencode::Entry::String(Vec::new()),
            );
        }
        let body = bitdaemon_bencode::bencode(&pex);
        match st.on_message(now, &body, body.len()) {
            I2pPexVerdict::Added(v) => assert_eq!(v, vec![Sha256Hash::from_bytes(&hash).unwrap()]),
            _ => panic!("expected Added"),
        }
        // upstream's 6-slot sliding window allows 6 messages per 60 s;
        // the 7th is `too_frequent_pex`.
        for i in 1..6 {
            match st.on_message(now + Duration::from_secs(i), &body, body.len()) {
                I2pPexVerdict::Added(_) => {}
                _ => panic!("expected Added at {i}"),
            }
        }
        match st.on_message(now + Duration::from_secs(6), &body, body.len()) {
            I2pPexVerdict::Disconnect(r) => assert_eq!(r, "too_frequent_pex"),
            _ => panic!("expected Disconnect"),
        }
        // after the window slides, fine again.
        match st.on_message(
            now + PEX_INTERVAL + Duration::from_secs(1),
            &body,
            body.len(),
        ) {
            I2pPexVerdict::Added(_) => {}
            _ => panic!("expected Added"),
        }
    }

    #[test]
    fn on_message_empty_added_is_nothing() {
        let mut st = I2pPexPeerState::new();
        let now = Instant::now();
        let mut empty = bitdaemon_bencode::Entry::Dict(Default::default());
        if let Ok(d) = empty.dict_mut() {
            d.insert(
                b"dropped".to_vec(),
                bitdaemon_bencode::Entry::String(Vec::new()),
            );
        }
        let eb = bitdaemon_bencode::bencode(&empty);
        assert!(matches!(
            st.on_message(now, &eb, eb.len()),
            I2pPexVerdict::Nothing
        ));
    }

    #[test]
    fn on_message_malformed_disconnects() {
        let mut st = I2pPexPeerState::new();
        let now = Instant::now();
        assert!(matches!(
            st.on_message(now, b"not bencode", 11),
            I2pPexVerdict::Disconnect(_)
        ));
        // oversized message.
        let big = vec![b'x'; 0x100000];
        assert!(matches!(
            st.on_message(now + PEX_INTERVAL, &big, big.len()),
            I2pPexVerdict::Disconnect(_)
        ));
    }

    #[test]
    fn peer_tick_sends_full_then_diff() {
        let dest = b"QUFBQUFB";
        let c = i2p_conn(dest, 9);
        let mut tstate = I2pPexState::default();
        let mut ps = I2pPexPeerState::new();
        let now = Instant::now();
        tstate.tick(now, [&c].into_iter(), false);
        let full = build_full_list([&c].into_iter());
        // first send = full list.
        let msg = ps.tick(now, 9, &tstate, &full, false).unwrap();
        let PeerMessage::Extended { id, data } = msg else {
            panic!("expected Extended");
        };
        assert_eq!(id, 9);
        assert_eq!(&data[..], &full[..]);
        // inside the interval → nothing.
        assert!(
            ps.tick(now + Duration::from_secs(1), 9, &tstate, &full, false)
                .is_none()
        );
        // ext id 0 or disable_pex suppress.
        assert!(
            ps.tick(now + PEX_INTERVAL, 0, &tstate, &full, false)
                .is_none()
        );
        assert!(
            ps.tick(now + PEX_INTERVAL, 9, &tstate, &full, true)
                .is_none()
        );
    }
}
