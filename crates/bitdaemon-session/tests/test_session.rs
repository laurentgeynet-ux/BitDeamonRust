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
    let mut out = Vec::new();
    s.peer_connected(
        &ih,
        1,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 6881),
        &mut out,
    );
    assert_eq!(s.num_peers(), 1);

    let mut out = Vec::new();
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Have(PieceIndex(0)),
    };
    s.on_peer_message(&p, 0, &mut out);
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
    let mut out = Vec::new();
    s.peer_connected(
        &ih,
        9,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 6881),
        &mut out,
    );
    // apply refcount via the normal Have path
    let p = PeerPacket {
        info_hash: ih,
        peer: 9,
        remote: "127.0.0.1:6881".parse().unwrap(),
        msg: PeerMessage::Have(PieceIndex(2)),
    };
    let mut out = Vec::new();
    s.on_peer_message(&p, 0, &mut out);
    s.peer_disconnected(&ih, 9, None, &mut out);
    assert_eq!(s.num_peers(), 0);
}

#[test]
fn verified_piece_emits_have_to_peers() {
    let mut s = Session::new();
    let mut t = mk_torrent(2);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    s.peer_connected(
        &ih,
        1,
        PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0),
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 6881),
        &mut out,
    );
    s.peer_connected(
        &ih,
        2,
        PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0),
        // distinct IP — `new_connection` drops same-IP incoming dups
        // (`allow_multiple_connections_per_ip` off by default).
        std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 2)),
            6881,
        ),
        &mut out,
    );

    // upstream `on_piece_verified` ignores pieces the picker doesn't
    // consider finished, and `announce_piece` skips handshaking
    // conns — finish the download and complete the handshakes.
    if let Some(t) = s.torrents.get_mut(&ih) {
        if let Some(p) = t.picker.as_mut() {
            for b in 0..p.blocks_in_piece(PieceIndex(0)) {
                p.mark_as_finished(
                    bitdaemon_common::types::PieceBlock {
                        piece_index: PieceIndex(0),
                        block_index: b,
                    },
                    None,
                );
            }
        }
        for c in t.peers.values_mut() {
            c.connecting = false;
            c.handshake_done = true;
        }
    }
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
            block_hashes: Vec::new(),
            picker_gen: 0,
            err: Default::default(),
        })
        .unwrap();
    s.drain_disk_completed(&mut out);
    assert!(!s.torrents[&ih].have.get_bit(0));
}

#[test]
fn peer_drain_releases_disk_gate() {
    // upstream `peer_connection::on_disk` — when the write queue
    // drains below the low watermark, the conn's `bw_disk` gate
    // releases even with writes still outstanding (the own-write
    // completion path is the *second* release, not the only one).
    use bitdaemon_disk::back_pressure::PeerWriteGates;
    use bitdaemon_session::counters::Counter;
    use bitdaemon_session::session::DiskCompleted;

    let mut s = Session::new();
    let gates = std::sync::Arc::new(PeerWriteGates::new());
    s.set_write_gates(gates.clone());
    let mut t = mk_torrent(2);
    t.init_picker(true);
    let ih = s.add_torrent(t);

    let mut c = PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0);
    c.disk_blocked = true;
    c.outstanding_writing_bytes = 0x4000;
    s.torrents.get_mut(&ih).unwrap().peers.insert(7, c);
    s.counters.inc(Counter::NumPeersDownDisk);
    gates.set_blocked((ih, 7));
    let gate = gates.gate((ih, 7));
    assert!(gate.load(std::sync::atomic::Ordering::Relaxed));

    s.disk_completions()
        .send(DiskCompleted::PeerDrain { ih, peer: 7 })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());

    let p = &s.torrents[&ih].peers[&7];
    assert!(!p.disk_blocked, "PeerDrain must clear bw_disk");
    assert!(
        !gate.load(std::sync::atomic::Ordering::Relaxed),
        "PeerDrain must release the read gate"
    );
    assert_eq!(s.counters.value(Counter::NumPeersDownDisk), 0);
    // outstanding bytes stay — the in-flight writes still drain via
    // their own completions (upstream leaves them alone too).
    assert_eq!(p.outstanding_writing_bytes, 0x4000);
}

#[test]
fn peer_drain_on_unblocked_peer_is_noop() {
    use bitdaemon_session::counters::Counter;
    use bitdaemon_session::session::DiskCompleted;

    let mut s = Session::new();
    let mut t = mk_torrent(2);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .peers
        .insert(7, PeerConn::new(2, PeerPipeline::new(1000, 30, 250), 0));

    s.disk_completions()
        .send(DiskCompleted::PeerDrain { ih, peer: 7 })
        .unwrap();
    // stale/gone peer — also a no-op
    s.disk_completions()
        .send(DiskCompleted::PeerDrain { ih, peer: 99 })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());

    assert!(!s.torrents[&ih].peers[&7].disk_blocked);
    assert_eq!(s.counters.value(Counter::NumPeersDownDisk), 0);
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

#[test]
fn connect_candidates_backoff_on_failure() {
    use bitdaemon_net::peer_list::PeerListState;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let mut s = Session::new();
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    );
    let ep = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)), 6881);
    t.add_swarm_peer(
        ep,
        bitdaemon_common::flags::PeerSourceFlags::TRACKER,
        &mut Vec::new(),
    );
    s.add_torrent(t);
    // ctor boost: `p.peers` non-empty → `do_connect_boost` dials the
    // peer immediately — the effect sits in `pending_out` until the
    // engine dispatch drains it.
    let dials: Vec<_> = s
        .pending_out
        .iter()
        .filter(|o| {
            matches!(
                o,
                bitdaemon_session::session::SessionOut::ConnectPeer { .. }
            )
        })
        .collect();
    assert_eq!(dials.len(), 1);
    let mut _o = Vec::new();
    s.tick(&mut _o); // session_time = 1 (0 means "never connected" upstream)

    let st = PeerListState {
        is_finished: false,
        external_ip: bitdaemon_net::ip_voter::ExternalIp::default(),
        external_port: 6881,
        min_reconnect_time: 60,
        max_peerlist_size: 0,
        max_failcount: 3,
        allow_multiple_connections_per_ip: false,
    };
    let pol = bitdaemon_net::pe_crypto::EncPolicy::Enabled;
    // the boost already handed the peer out (plaintext — `pe_support`
    // tentatively armed per upstream's toggle); nothing left to dial.
    assert!(
        s.torrents
            .get_mut(&ih)
            .unwrap()
            .try_connect_peer(0, &st, pol, &[])
            .is_none()
    );
    // dial fails → backoff window blocks re-dial at t=1
    s.peer_connect_failed(&ih, ep, false, false);
    assert!(
        s.torrents
            .get_mut(&ih)
            .unwrap()
            .try_connect_peer(1, &st, pol, &[])
            .is_none()
    );
    // after min_reconnect_time*(failcount+1)=120s it's a candidate again
    let c = s
        .torrents
        .get_mut(&ih)
        .unwrap()
        .try_connect_peer(200, &st, pol, &[]);
    // the failed plaintext dial left pe_support armed → MSE retry.
    assert_eq!(c.map(|x| (x.0, x.1)), Some((ep, true)));
}

#[test]
fn ip_filter_bans_and_disconnects() {
    use bitdaemon_net::ip_filter::{self, IpFilter};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let mut s = Session::new();
    let ih = Sha1Hash::from_bytes(&[3; 20]).unwrap();
    let mut t = Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    );
    let bad = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 66, 0, 1)), 6881);
    let good = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 66, 0, 2)), 6881);
    t.add_swarm_peer(
        bad,
        bitdaemon_common::flags::PeerSourceFlags::TRACKER,
        &mut Vec::new(),
    );
    t.add_swarm_peer(
        good,
        bitdaemon_common::flags::PeerSourceFlags::TRACKER,
        &mut Vec::new(),
    );
    t.add_peer(1, PeerConn::new(1, PeerPipeline::new(1, 1, 1), 0));
    s.add_torrent(t);
    // pretend peer 1 is connected from `bad`
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .peers
        .get_mut(&1)
        .unwrap()
        .remote = Some(bad);

    let mut f = IpFilter::new();
    f.add_rule(bad.ip(), bad.ip(), ip_filter::BLOCKED);
    let mut out = Vec::new();
    s.set_ip_filter(f, &mut out);
    let t = &s.torrents[&ih];
    // banned + erased: only `good` remains, live conn dropped
    assert_eq!(t.peer_list.num_peers(), 1);
    assert!(t.peers.is_empty());
}

#[test]
fn anonymous_mode_suppresses_lsd() {
    let mut s = Session::new();
    s.settings.anonymous_mode = true;
    let ih = Sha1Hash::from_bytes(&[11; 20]).unwrap();
    s.add_torrent(Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    ));
    let mut out = Vec::new();
    s.lsd_tick(std::time::Instant::now(), &mut out);
    assert!(out.is_empty());
}

#[test]
fn pause_announces_stopped() {
    let mut s = Session::new();
    let ih = Sha1Hash::from_bytes(&[21; 20]).unwrap();
    let mut t = Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    );
    let mut e = bitdaemon_session::tracker_list::AnnounceEntry::new("http://t/announce", 0);
    e.endpoints
        .push(bitdaemon_session::tracker_list::AnnounceEndpoint::default());
    e.endpoints[0].v1.start_sent = true;
    t.trackers.trackers.push(e);
    s.add_torrent(t);

    let mut out = Vec::new();
    s.set_paused(&ih, true, &mut out);
    let stopped = out.iter().any(|o| {
        matches!(
            o,
            SessionOut::TrackerAnnounce { ih: i, params, .. }
                if *i == ih && params.event == 3
        )
    });
    assert!(stopped, "pause should emit a stopped announce");
}

#[test]
fn session_pause_is_a_second_layer() {
    use bitdaemon_session::session::SessionOut;
    let mut s = Session::new();
    let ih = Sha1Hash::from_bytes(&[23; 20]).unwrap();
    let mut t = Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    );
    let mut e = bitdaemon_session::tracker_list::AnnounceEntry::new("http://t/announce", 0);
    e.endpoints
        .push(bitdaemon_session::tracker_list::AnnounceEndpoint::default());
    t.trackers.trackers.push(e);
    s.add_torrent(t);

    // session pause → effective pause without touching m_paused
    let mut out = Vec::new();
    s.set_session_paused(true, &mut out);
    {
        let t = s.torrents.get(&ih).unwrap();
        assert!(!t.paused && t.session_paused && t.is_paused());
    }

    // a user resume under a paused session is a no-op
    // (`m_paused` was already false — the effective state never
    // flips, exactly like upstream `torrent::set_paused(false)`).
    s.set_paused(&ih, false, &mut out);
    assert!(s.torrents.get(&ih).unwrap().is_paused());

    // session resume → start_announcing (`started` again)
    out.clear();
    s.set_session_paused(false, &mut out);
    {
        let t = s.torrents.get(&ih).unwrap();
        assert!(!t.is_paused() && !t.session_paused);
    }
    let started = out.iter().any(|o| {
        matches!(
            o,
            SessionOut::TrackerAnnounce { ih: i, params, .. }
                if *i == ih && params.event == 2
        )
    });
    assert!(started, "session resume should re-announce `started`");
}

#[test]
fn do_pause_disconnects_peers() {
    use bitdaemon_session::session::SessionOut;
    use bitdaemon_session::torrent::TorrentEffect;
    let mut s = Session::new();
    let ih = Sha1Hash::from_bytes(&[24; 20]).unwrap();
    let mut t = Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    );
    let pid = 7;
    t.add_peer(pid, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));
    s.add_torrent(t);

    // non-graceful pause → disconnect_all(errors::torrent_paused)
    let mut out = Vec::new();
    s.set_paused(&ih, true, &mut out);
    assert!(out.iter().any(|o| matches!(
        o,
        SessionOut::Torrent(TorrentEffect::DisconnectPeer { peer, .. })
            if *peer == pid
    )));
    assert!(s.torrents.get(&ih).unwrap().is_paused());
}

#[test]
fn idle_peer_times_out() {
    let mut s = Session::new();
    s.settings.peer_timeout = std::time::Duration::from_secs(5);
    let ih = Sha1Hash::from_bytes(&[33; 20]).unwrap();
    let mut t = Torrent::new(
        ih,
        1,
        16384,
        bitdaemon_common::Bitfield::with_size(1),
        StorageIndex(0),
    );
    // the engine registers conns post-handshake — mirror that.
    let mut c = PeerConn::new(1, PeerPipeline::new(1, 1, 1), 0);
    c.connecting = false;
    c.handshake_done = true;
    t.add_peer(1, c);
    s.add_torrent(t);
    // last_receive_ms=0; now_ms≈0 → no timeout yet
    let mut out = Vec::new();
    s.tick(&mut out);
    assert_eq!(s.torrents[&ih].peers.len(), 1);
    // age the conn-clock past `peer_timeout` (5 s)
    s.torrents.get_mut(&ih).unwrap().now_ms = 100_000;
    s.tick(&mut out);
    assert!(
        s.torrents[&ih].peers.is_empty(),
        "idle peer should be dropped"
    );
}

#[test]
fn session_state_roundtrip_through_session() {
    // 2-piece info dict (40 hash bytes)
    let mut info = Vec::new();
    info.extend(b"d6:lengthi16385e4:name4:test12:piece lengthi16384e6:pieces40:");
    info.extend([0xAAu8; 40]);
    info.extend(b"e");
    let node = bitdaemon_bencode::bdecode_checked(&info).unwrap();
    let ti = bitdaemon_core::torrent_info::TorrentInfo::from_info_node(
        &node,
        &bitdaemon_core::torrent_info::LoadTorrentLimits::default(),
    )
    .unwrap();
    let ih = ti.info_hash_v1().unwrap();

    let mut s = Session::new();
    let atp = bitdaemon_core::add_torrent_params::AddTorrentParams {
        ti: Some(std::sync::Arc::new(ti)),
        info_hashes: bitdaemon_common::info_hash::InfoHash::from_v1(ih),
        ..Default::default()
    };
    let mut out = Vec::new();
    s.add_torrent_params(&atp, StorageIndex(0), &mut out);
    s.torrents.get_mut(&ih).unwrap().have.set_bit(0);

    let st = s.save_state();
    assert!(st.torrents.contains_key(&ih));
    let buf =
        bitdaemon_bencode::bencode(&bitdaemon_session::session_state::save_session_state(&st));

    let mut s2 = Session::new();
    assert_eq!(s2.load_state(&buf, &mut Vec::new()), 1);
    let t2 = &s2.torrents[&ih];
    assert!(t2.have.get_bit(0) && !t2.have.get_bit(1));
    assert_eq!(t2.num_pieces, 2);
}

#[test]
fn web_seed_tick_and_block() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;

    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.web_seeds.push("http://seed.example/file.bin".into());
    let mut fs = FileStorage::new();
    fs.add_file(
        Some(b"file.bin"),
        b"file.bin",
        4 * 0x40000,
        bitdaemon_core::FileFlags::NONE,
        0,
        b"",
        None,
    )
    .unwrap();
    fs.set_piece_length(0x40000);
    fs.set_num_pieces(4);
    t.files = Some(std::sync::Arc::new(fs));
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tick(&mut out);
    let Some(SessionOut::WebSeedFetch {
        url,
        single_file,
        reqs,
        ..
    }) = out
        .iter()
        .find(|e| matches!(e, SessionOut::WebSeedFetch { .. }))
    else {
        panic!("no WebSeedFetch emitted: {out:?}");
    };
    assert_eq!(url, "http://seed.example/file.bin");
    assert!(single_file);
    assert!(!reqs.is_empty());

    // a returned block goes down the normal disk-write path
    let r0 = reqs[0];
    let mut out2 = Vec::new();
    s.on_web_seed_block(
        &ih(1),
        "http://seed.example/file.bin",
        r0,
        vec![0xAB; r0.length as usize],
        &mut out2,
    );
    let ih1 = ih(1);
    assert!(
        out2.iter().any(|e| matches!(
            e,
            SessionOut::Torrent(bitdaemon_session::torrent::TorrentEffect::DiskWrite { req, .. })
                if req.piece == r0.piece
        )) || out2.iter().any(|e| matches!(e, SessionOut::Torrent(_)))
    );
    let _ = ih1;

    // a failed fetch releases the blocks for re-pick
    let rest: Vec<PeerRequest> = reqs[1..].to_vec();
    s.on_web_seed_error(&ih(1), "http://seed.example/file.bin", &rest, "test error");
    let t2 = &s.torrents[&ih(1)];
    assert!(t2.web_seed_outstanding.len() <= 1);
}

#[test]
fn dht_ip_key_votes_external_ip() {
    let mut s = Session::new();
    s.enable_dht(
        "0.0.0.0".parse().unwrap(),
        None,
        bitdaemon_dht::dht_settings::DhtSettings::default(),
    );
    // a DHT packet carrying top-level "ip" → vote (upstream
    // dht.cpp does it on every incoming message)
    let mut pkt = b"d2:ip4:".to_vec();
    pkt.extend([203, 0, 113, 7]); // 203.0.113.7 (TEST-NET-3, global)
    pkt.extend_from_slice(b"1:y1:ee");
    let from: std::net::SocketAddr = "8.8.8.8:6881".parse().unwrap();
    let mut out = Vec::new();
    s.on_udp(from, &pkt, &mut out);
    assert_eq!(
        s.ip_voter.external_address(),
        Some(std::net::IpAddr::from([203, 0, 113, 7]))
    );
}

#[test]
fn unchoke_tick_unchokes_top_peer() {
    use bitdaemon_session::session::SessionOut;

    let mut s = Session::new();
    // 2 slots: `unchoke_set = allowed - max(1, allowed/5) = 1`
    // regular slot (the other goes to the optimistic rotation).
    s.settings.unchoke_slots_limit = 2;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    for pid in 1u64..=3 {
        s.peer_connected(
            &ih,
            pid,
            PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
            // distinct IPs — same-IP incoming dups are dropped.
            format!("127.0.0.{}:6881", pid).parse().unwrap(),
            &mut out,
        );
        // mark them interested + give pid 1 the best upload rate
        let t = s.torrents.get_mut(&ih).unwrap();
        let c = t.peers.get_mut(&pid).unwrap();
        c.connecting = false;
        c.peer_interested = true;
        c.stat_up_round = (4 - pid as i64) * 1000;
        // deterministic round-robin tiebreak: pid 1 waited longest
        c.last_unchoke =
            std::time::Instant::now() - std::time::Duration::from_secs(100 * (4 - pid));
    }
    // run 15 session ticks (the choker round fires at `unchoke_interval`)
    let mut unchoked = Vec::new();
    for _ in 0..15 {
        let mut out = Vec::new();
        s.tick(&mut out);
        for e in &out {
            if let SessionOut::ToPeer {
                peer,
                msg: bitdaemon_net::peer_wire::PeerMessage::Unchoke,
                ..
            } = e
            {
                unchoked.push(*peer);
            }
        }
    }
    // the best uploader (pid 1, highest stat_up_round) gets the slot
    assert_eq!(unchoked, vec![1]);
    assert!(!s.torrents[&ih].peers[&1].choked);
    assert!(s.torrents[&ih].peers[&2].choked);
}

#[test]
fn dht_announce_tick_issues_get_peers() {
    use bitdaemon_session::session::SessionOut;
    let mut s = Session::new();
    s.enable_dht(
        "0.0.0.0".parse().unwrap(),
        None,
        bitdaemon_dht::dht_settings::DhtSettings::default(),
    );
    // upstream `add_dht_router` — routers land in the table
    // directly and seed the traversal; `add_dht_node` would
    // ping-verify first and leave the table empty.
    s.dht_add_router("9.9.9.9:51413".parse().unwrap());
    let mut t = mk_torrent(4);
    t.init_picker(true);
    s.add_torrent(t);
    let mut out = Vec::new();
    s.tick(&mut out);
    let to_router = out.iter().any(|e| {
        matches!(e, SessionOut::UdpPacket(pkt, to)
            if to.port() == 51413 && pkt.windows(9).any(|w| w == b"get_peers"))
    });
    assert!(to_router, "expected get_peers datagram to router node");
}

#[test]
fn session_state_roundtrip_settings() {
    use bitdaemon_session::session_state::{load_session_state, save_session_state_buf};
    let mut s = Session::new();
    s.settings.unchoke_slots_limit = 42;
    s.settings.anonymous_mode = true;
    let st = s.save_state();
    let buf = save_session_state_buf(&st);
    let st2 = load_session_state(&buf).unwrap();
    let set = st2.settings.unwrap();
    assert_eq!(set.unchoke_slots_limit, 42);
    assert!(set.anonymous_mode);
    assert_eq!(set.connections_limit, 200); // defaults preserved
}

#[test]
fn seed_mode_leave_rechecks() {
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.seed_mode = true;
    t.seed_verified = Bitfield::with_size(4);
    t.seed_verifying = Bitfield::with_size(4);
    t.seeding = true;
    assert!(t.is_seed());
    t.leave_seed_mode(true, &mut Vec::new());
    assert!(!t.seed_mode);
    assert!(!t.is_seed());
    assert_eq!(t.num_verified, 0);
    // check_files path armed a full recheck
    assert!(t.checking);
    assert_eq!(t.checking_pieces, 4);
    // skip_checking keeps claimed state
    let mut t2 = mk_torrent(4);
    t2.seed_mode = true;
    t2.seeding = true;
    t2.leave_seed_mode(false, &mut Vec::new());
    assert!(!t2.seed_mode);
    assert!(!t2.checking);
}

#[test]
fn peer_class_assignment() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    // private addr over TCP → `local` (2) + `tcp` (1) classes; the
    // local class flags the conn `ignore_unchoke_slots`.
    s.peer_connected(
        &ih,
        1,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        "10.0.0.5:6881".parse().unwrap(),
        &mut out,
    );
    let c = &s.torrents[&ih].peers[&1];
    assert!(c.peer_classes.has_class(1));
    assert!(c.peer_classes.has_class(2));
    assert!(c.ignores_unchoke_slots(&s.peer_class_pool));
    // public addr over TCP → `global` (0) + `tcp` (1); not exempt.
    s.peer_connected(
        &ih,
        2,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        "8.8.8.8:6881".parse().unwrap(),
        &mut out,
    );
    let c = &s.torrents[&ih].peers[&2];
    assert!(c.peer_classes.has_class(0));
    assert!(c.peer_classes.has_class(1));
    assert!(!c.peer_classes.has_class(2));
    assert!(!c.ignores_unchoke_slots(&s.peer_class_pool));
    // uTP keeps only the ip classes — the `tcp` class isn't added.
    let mut c3 = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c3.sock_type = 3; // socket_type_t::utp
    s.peer_connected(&ih, 3, c3, "8.8.4.4:6881".parse().unwrap(), &mut out);
    let c = &s.torrents[&ih].peers[&3];
    assert!(c.peer_classes.has_class(0));
    assert!(!c.peer_classes.has_class(1));
}

#[test]
fn peer_error_disconnect_posts_peer_error_not_disconnected() {
    // `peer_connection::disconnect(ec, op, peer_error)` — upstream
    // posts `peer_error_alert`, never `peer_disconnected_alert`, and
    // bumps `error_peers`/`disconnected_peers`/`error_tcp_peers` +
    // the direction leg.
    use bitdaemon_net::bt_peer_conn::{DisconnectSeverity, PeerGone};
    use bitdaemon_session::alerts::Alert;
    use bitdaemon_session::counters::Counter;
    let mut s = Session::new();
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.outgoing = true;
    s.peer_connected(&ih, 7, c, "127.0.0.1:6881".parse().unwrap(), &mut out);
    s.pop_alerts();
    s.peer_disconnected(
        &ih,
        7,
        Some(PeerGone::Disconnected {
            severity: DisconnectSeverity::PeerError,
            reason: "invalid have index",
        }),
        &mut out,
    );
    assert_eq!(s.num_peers(), 0);
    let alerts = s.pop_alerts();
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::PeerError { error, .. } if error == "invalid have index")),
        "expected a peer_error alert: {alerts:?}"
    );
    assert!(
        !alerts
            .iter()
            .any(|a| matches!(a, Alert::PeerDisconnected { .. })),
        "peer_error never posts peer_disconnected upstream: {alerts:?}"
    );
    assert_eq!(s.counters.value(Counter::ErrorPeers), 1);
    assert_eq!(s.counters.value(Counter::DisconnectedPeers), 1);
    assert_eq!(s.counters.value(Counter::ErrorTcpPeers), 1);
    assert_eq!(s.counters.value(Counter::ErrorOutgoingPeers), 1);
}

#[test]
fn normal_disconnect_posts_peer_disconnected_once() {
    // severity `normal` → `peer_disconnected_alert`; the engine's
    // `gone` echo (`None`) re-enters `peer_disconnected` and must be
    // a silent no-op — no duplicate alert, no double counters.
    use bitdaemon_net::bt_peer_conn::{DisconnectSeverity, PeerGone};
    use bitdaemon_session::alerts::Alert;
    use bitdaemon_session::counters::Counter;
    let mut s = Session::new();
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    s.peer_connected(
        &ih,
        8,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        "127.0.0.1:6881".parse().unwrap(),
        &mut out,
    );
    s.pop_alerts();
    s.peer_disconnected(
        &ih,
        8,
        Some(PeerGone::Disconnected {
            severity: DisconnectSeverity::Normal,
            reason: "too_many_connections",
        }),
        &mut out,
    );
    // engine `gone` echo after the writer task exits.
    s.peer_disconnected(&ih, 8, None, &mut out);
    let alerts = s.pop_alerts();
    let disc = alerts
        .iter()
        .filter(|a| matches!(a, Alert::PeerDisconnected { .. }))
        .count();
    assert_eq!(disc, 1, "exactly one peer_disconnected alert: {alerts:?}");
    assert!(
        !alerts.iter().any(|a| matches!(a, Alert::PeerError { .. })),
        "normal severity never posts peer_error: {alerts:?}"
    );
    assert_eq!(s.counters.value(Counter::DisconnectedPeers), 1);
    assert_eq!(s.counters.value(Counter::TooManyPeers), 1);
    assert_eq!(s.counters.value(Counter::ErrorPeers), 0);
}

#[test]
fn transport_eof_is_normal_close() {
    // `on_receive` socket EOF — `eof_peers` counter,
    // `peer_disconnected_alert`, no `error_peers`.
    use bitdaemon_net::bt_peer_conn::{PeerConnError, PeerGone};
    use bitdaemon_session::alerts::Alert;
    use bitdaemon_session::counters::Counter;
    let mut s = Session::new();
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    s.peer_connected(
        &ih,
        8,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        "127.0.0.1:6881".parse().unwrap(),
        &mut out,
    );
    s.pop_alerts();
    s.peer_disconnected(
        &ih,
        8,
        Some(PeerGone::Transport(PeerConnError::Io(
            std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
        ))),
        &mut out,
    );
    let alerts = s.pop_alerts();
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::PeerDisconnected { .. })),
        "EOF closes with a peer_disconnected alert: {alerts:?}"
    );
    assert!(!alerts.iter().any(|a| matches!(a, Alert::PeerError { .. })));
    assert_eq!(s.counters.value(Counter::EofPeers), 1);
    assert_eq!(s.counters.value(Counter::ErrorPeers), 0);
}

#[test]
fn wire_bad_length_counts_packet_then_peer_error() {
    // a complete frame with a bad fixed size is counted in
    // `num_incoming_*` (upstream bumps the counter once the packet
    // finished, after the handler's `invalid_*` disconnect) — a
    // `have` with a wrong length lands on `num_incoming_have`.
    use bitdaemon_net::bt_peer_conn::{PeerConnError, PeerGone};
    use bitdaemon_net::peer_wire::WireError;
    use bitdaemon_session::alerts::Alert;
    use bitdaemon_session::counters::{Counter, incoming_counter_idx};
    let mut s = Session::new();
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    s.peer_connected(
        &ih,
        8,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        "127.0.0.1:6881".parse().unwrap(),
        &mut out,
    );
    s.pop_alerts();
    s.peer_disconnected(
        &ih,
        8,
        Some(PeerGone::Transport(PeerConnError::Wire(
            WireError::BadLength { id: 4, len: 6 },
        ))),
        &mut out,
    );
    assert_eq!(
        s.counters.value(Counter::NumIncomingHave),
        1,
        "the bad frame still counted by packet type"
    );
    // the quirky mapping maps id 4 to num_incoming_choke + 4.
    assert_eq!(incoming_counter_idx(4), Counter::NumIncomingHave as usize);
    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(|a| matches!(a, Alert::PeerError { .. })));
    assert_eq!(s.counters.value(Counter::ErrorPeers), 1);
}

#[test]
fn wire_unknown_id_skips_packet_counter() {
    // upstream's dispatch `default` leg disconnects before the
    // `num_incoming_*` bump — an unknown id must not count.
    use bitdaemon_net::bt_peer_conn::{PeerConnError, PeerGone};
    use bitdaemon_net::peer_wire::WireError;
    use bitdaemon_session::counters::{Counter, NUM_COUNTERS};
    let mut s = Session::new();
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    s.peer_connected(
        &ih,
        8,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        "127.0.0.1:6881".parse().unwrap(),
        &mut out,
    );
    s.peer_disconnected(
        &ih,
        8,
        Some(PeerGone::Transport(PeerConnError::Wire(
            WireError::UnknownId(0x55),
        ))),
        &mut out,
    );
    assert_eq!(
        s.counters.value(Counter::NumIncomingChoke)
            + s.counters.value(Counter::NumIncomingExtended),
        0,
        "unknown ids bypass the packet counter"
    );
    assert_eq!(s.counters.value(Counter::ErrorPeers), 1);
    let _ = NUM_COUNTERS;
}
