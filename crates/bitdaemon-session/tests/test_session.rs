// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `session` — swarm glue: torrents registered, peer messages
//! dispatched, piece verification broadcast, disconnect cleanup.

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{PeerPacket, Session, SessionOut};
use bitdaemon_session::torrent::Torrent;

fn ih(b: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[b; 20]).unwrap()
}

fn mk_torrent(pieces: i32) -> Torrent {
    let mut have = Bitfield::new();
    have.resize_with(pieces as usize, false);
    Torrent::new(ih(1), pieces, 0x40000, have, StorageIndex(0))
}

#[test]
fn add_peer_and_dispatch() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    s.peer_connected(
        &ih,
        1,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 6881),
    );
    assert_eq!(s.num_peers(), 1);

    let mut out = Vec::new();
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Have(PieceIndex(0)),
    };
    s.on_peer_message(&p, &mut out);
    assert_eq!(
        s.torrents[&ih]
            .picker
            .as_ref()
            .unwrap()
            .piece_stats(PieceIndex(0))
            .peer_count,
        1
    );
}

#[test]
fn disconnect_cleans_up() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    s.peer_connected(
        &ih,
        9,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 6881),
    );
    // apply refcount via the normal Have path
    let p = PeerPacket {
        info_hash: ih,
        peer: 9,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Have(PieceIndex(2)),
    };
    let mut out = Vec::new();
    s.on_peer_message(&p, &mut out);
    s.peer_disconnected(&ih, 9, &mut out);
    assert_eq!(s.num_peers(), 0);
}

#[test]
fn verified_piece_emits_have_to_peers() {
    let mut s = Session::new();
    let mut t = mk_torrent(2);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    s.peer_connected(
        &ih,
        1,
        PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0),
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 6881),
    );
    s.peer_connected(
        &ih,
        2,
        PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0),
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 6881),
    );

    let mut out = Vec::new();
    s.on_piece_verified(&ih, PieceIndex(0), true, &mut out);
    let haves = out
        .iter()
        .filter(|o| {
            matches!(
                o,
                SessionOut::ToPeer {
                    msg: PeerMessage::Have(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(haves, 2);
}

#[test]
fn hash_mismatch_fails_piece() {
    use bitdaemon_session::session::DiskCompleted;
    let mut s = Session::new();
    let mut t = mk_torrent(2);
    t.init_picker(true);
    let good = Sha1Hash::from_bytes(&[0xAA; 20]).unwrap();
    t.piece_hashes = Some(vec![good, good]);
    let ih = s.add_torrent(t);

    // mismatched hash → piece failed, not verified
    let mut out = Vec::new();
    let bad = Sha1Hash::from_bytes(&[0xBB; 20]).unwrap();
    s.disk_completions()
        .send(DiskCompleted::Hash {
            ih,
            piece: PieceIndex(0),
            hash: bad,
            err: Default::default(),
        })
        .unwrap();
    s.drain_disk_completed(&mut out);
    assert!(!s.torrents[&ih].have.get_bit(0));
}

#[test]
fn lsd_emit_and_receive() {
    use bitdaemon_net::lsd::{LSD_ADDR4, LSD_PORT, render_lsd_packet};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let mut s = Session::new();
    let ih = Sha1Hash::from_bytes(&[7; 20]).unwrap();
    s.add_torrent(Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    ));
    // our announce goes to both multicast groups
    let mut out = Vec::new();
    s.lsd_tick(std::time::Instant::now(), &mut out);
    let v4 = out.iter().find(|e| matches!(e, SessionOut::UdpPacket(_, a) if *a == SocketAddr::new(IpAddr::V4(LSD_ADDR4), LSD_PORT)));
    assert!(v4.is_some());

    // a remote announces our ih on its own port → swarm learns it
    let hex = bitdaemon_common::hex::to_hex(ih.as_bytes());
    let pkt = render_lsd_packet(5555, &hex, 0xBEEF, false);
    let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 7)), 40000);
    s.on_udp(from, &pkt, &mut Vec::new());
    let t = &s.torrents[&ih];
    assert_eq!(t.peer_list.num_peers(), 1);
    let p = &t.peer_list.peers()[0];
    assert_eq!(p.ip().map(|a| a.port()), Some(5555));
}
