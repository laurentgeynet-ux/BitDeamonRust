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
    // the boost already handed the peer out; nothing left to dial.
    assert!(
        s.torrents
            .get_mut(&ih)
            .unwrap()
            .try_connect_peer(0, &st, &[])
            .is_none()
    );
    // dial fails → backoff window blocks re-dial at t=1
    s.peer_connect_failed(&ih, ep, false);
    assert!(
        s.torrents
            .get_mut(&ih)
            .unwrap()
            .try_connect_peer(1, &st, &[])
            .is_none()
    );
    // after min_reconnect_time*(failcount+1)=120s it's a candidate again
    let c = s
        .torrents
        .get_mut(&ih)
        .unwrap()
        .try_connect_peer(200, &st, &[]);
    assert_eq!(c.map(|x| x.0), Some(ep));
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
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.have.set_bit(0);
        // `pieces` is only persisted once `files_checked` proved the
        // storage (upstream `write_resume_data` gate) — simulate the
        // accepted verdict before saving state.
        t.files_checked = true;
    }

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
    // `maybe_connect_web_seeds` gates on `m_files_checked`.
    t.files_checked = true;
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
    s.on_web_seed_error(
        &ih(1),
        "http://seed.example/file.bin",
        &rest,
        "test error",
        bitdaemon_session::torrent::WebSeedBackoff::Retry,
    );
    let t2 = &s.torrents[&ih(1)];
    assert!(t2.web_seed_outstanding.len() <= 1);
}

/// `maybe_connect_web_seeds` exits when `!m_files_checked` — a
/// torrent that hasn't run the storage check must not fetch from
/// web seeds at all.
#[test]
fn web_seed_gated_on_files_checked() {
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
    // `files_checked` stays false — no fetch may be emitted.
    s.add_torrent(t);

    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(
        !out.iter()
            .any(|e| matches!(e, SessionOut::WebSeedFetch { .. })),
        "unchecked torrent must not fetch web seeds: {out:?}"
    );

    // after the check lands, picks resume.
    s.torrents.get_mut(&ih(1)).unwrap().files_checked = true;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(
        out.iter()
            .any(|e| matches!(e, SessionOut::WebSeedFetch { .. })),
        "checked torrent should fetch: {out:?}"
    );
}

/// `web_seed_t::retry` — a failed seed is not re-picked until
/// `now + urlseed_wait_retry`; other seeds keep serving.
#[test]
fn web_seed_error_backs_off_per_seed() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;
    use bitdaemon_session::torrent::WebSeedBackoff;

    let mut s = Session::new();
    s.settings.urlseed_wait_retry = std::time::Duration::from_secs(30);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.files_checked = true;
    t.web_seeds.push("http://a.example/file.bin".into());
    t.web_seeds.push("http://b.example/file.bin".into());
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
    let first_url = out
        .iter()
        .find_map(|e| match e {
            SessionOut::WebSeedFetch { url, .. } => Some(url.clone()),
            _ => None,
        })
        .expect("first fetch");

    // fail it — the seed backs off; the batch's blocks release.
    let reqs = out
        .iter()
        .find_map(|e| match e {
            SessionOut::WebSeedFetch { reqs, .. } => Some(reqs.clone()),
            _ => None,
        })
        .unwrap_or_default();
    s.on_web_seed_error(&ih(1), &first_url, &reqs, "boom", WebSeedBackoff::Retry);
    s.on_web_seed_done(&ih(1), &first_url, false);

    let st = &s.torrents[&ih(1)].web_seed_state[&first_url];
    assert!(st.retry_ms > 0, "failed seed must carry a retry deadline");
    assert!(!st.busy);
    assert!(s.torrents[&ih(1)].web_seed_outstanding.is_empty());

    // next tick must pick the *other* seed — the failed one is
    // inside its retry window.
    let mut out = Vec::new();
    s.tick(&mut out);
    let next_url = out.iter().find_map(|e| match e {
        SessionOut::WebSeedFetch { url, .. } => Some(url.clone()),
        _ => None,
    });
    assert_ne!(next_url.as_deref(), Some(first_url.as_str()));
    assert!(next_url.is_some(), "the healthy seed still serves");

    // finishing that fetch frees its slot; the *failed* seed stays
    // ineligible until the retry deadline passes.
    s.on_web_seed_done(&ih(1), next_url.as_deref().unwrap_or(""), false);
    let mut out = Vec::new();
    s.tick(&mut out);
    let third = out.iter().find_map(|e| match e {
        SessionOut::WebSeedFetch { url, .. } => Some(url.clone()),
        _ => None,
    });
    assert_ne!(
        third.as_deref(),
        Some(first_url.as_str()),
        "seed inside retry window must not be re-picked"
    );

    // past the deadline the seed is eligible again.
    let t = s.torrents.get_mut(&ih(1)).unwrap();
    t.now_ms += 31_000;
    let _ = PeerRequest {
        piece: PieceIndex(0),
        start: 0,
        length: 0,
    };
    let mut out = Vec::new();
    s.tick(&mut out);
    // the fetch may land on either seed (round-robin order), but
    // the previously-failed one is no longer filtered out.
    let eligible = !s.torrents[&ih(1)]
        .web_seed_state
        .get(&first_url)
        .is_some_and(|st| st.retry_ms > s.torrents[&ih(1)].now_ms);
    assert!(eligible, "retry deadline must expire");
}

/// `web->disabled` — `WebSeedBackoff::Disable` (invalid range,
/// SSRF, banned, bad protocol) never retries.
#[test]
fn web_seed_disable_is_terminal() {
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;
    use bitdaemon_session::torrent::WebSeedBackoff;

    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.files_checked = true;
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
    s.on_web_seed_error(
        &ih(1),
        "http://seed.example/file.bin",
        &[],
        "invalid range",
        WebSeedBackoff::Disable,
    );
    s.on_web_seed_done(&ih(1), "http://seed.example/file.bin", false);
    assert!(s.torrents[&ih(1)].web_seed_state["http://seed.example/file.bin"].disabled);

    // far future — a disabled seed still never picks.
    s.torrents.get_mut(&ih(1)).unwrap().now_ms += 10_000_000;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(
        !out.iter()
            .any(|e| matches!(e, SessionOut::WebSeedFetch { .. })),
        "disabled seed must never serve again: {out:?}"
    );
}

/// `web_connection_base::timeout()` — an in-flight batch that
/// outlives `urlseed_timeout` is swept: blocks release, the seed
/// backs off, a `url_seed_alert` posts.
#[test]
fn web_seed_timeout_sweep_releases() {
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;

    let mut s = Session::new();
    s.settings.urlseed_timeout = std::time::Duration::from_millis(500);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.files_checked = true;
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
    assert!(
        out.iter()
            .any(|e| matches!(e, SessionOut::WebSeedFetch { .. }))
    );
    let t = &s.torrents[&ih(1)];
    assert!(!t.web_seed_outstanding.is_empty());

    // the engine task never reports — the torrent clock walks past
    // the deadline and the sweep frees the picker blocks.
    let t = s.torrents.get_mut(&ih(1)).unwrap();
    t.now_ms += 600;
    let mut out = Vec::new();
    s.tick(&mut out);
    let t = &s.torrents[&ih(1)];
    assert!(
        t.web_seed_outstanding.is_empty(),
        "timed-out batch must release its blocks"
    );
    let st = &t.web_seed_state["http://seed.example/file.bin"];
    assert!(!st.busy);
    assert!(st.retry_ms > 0);
    assert_eq!(st.status.last_error.as_deref(), Some("timed out"));
}

/// `max_web_seed_connections` — an in-flight fetch occupies the
/// seed's conn slot; with the cap at 1 the second seed doesn't
/// start until the first resolves.
#[test]
fn web_seed_conn_cap() {
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;

    let mut s = Session::new();
    s.settings.max_web_seed_connections = 1;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.files_checked = true;
    t.web_seeds.push("http://a.example/file.bin".into());
    t.web_seeds.push("http://b.example/file.bin".into());
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
    let n = out
        .iter()
        .filter(|e| matches!(e, SessionOut::WebSeedFetch { .. }))
        .count();
    assert_eq!(n, 1, "one batch per pick");

    // still in flight — the cap keeps the second seed off.
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(
        !out.iter()
            .any(|e| matches!(e, SessionOut::WebSeedFetch { .. })),
        "busy seed must hold the conn slot"
    );

    // batch resolved → the other seed may connect.
    let url = out.iter().find_map(|e| match e {
        SessionOut::WebSeedFetch { url, .. } => Some(url.clone()),
        _ => None,
    });
    let _ = url;
    s.on_web_seed_done(&ih(1), "http://a.example/file.bin", false);
    let mut out = Vec::new();
    s.tick(&mut out);
    let url = out
        .iter()
        .find_map(|e| match e {
            SessionOut::WebSeedFetch { url, .. } => Some(url.clone()),
            _ => None,
        })
        .expect("post-done fetch");
    assert_eq!(url, "http://b.example/file.bin");
}

/// `report_web_seed_downloads` — web-seed payload folds into the
/// torrent's download counters (`m_stat` channels upstream); with
/// the flag off it must not.
#[test]
fn web_seed_bytes_count_toward_download() {
    use bitdaemon_common::types::PeerRequest;
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;

    for report in [true, false] {
        let mut s = Session::new();
        s.settings.report_web_seed_downloads = report;
        let mut t = mk_torrent(4);
        t.init_picker(true);
        t.files_checked = true;
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
        let reqs = out
            .iter()
            .find_map(|e| match e {
                SessionOut::WebSeedFetch { reqs, .. } => Some(reqs.clone()),
                _ => None,
            })
            .unwrap_or_default();
        let r0: PeerRequest = reqs[0];
        let mut out2 = Vec::new();
        s.on_web_seed_block(
            &ih(1),
            "http://seed.example/file.bin",
            r0,
            vec![0xAB; r0.length as usize],
            &mut out2,
        );
        let t = &s.torrents[&ih(1)];
        if report {
            assert_eq!(
                t.total_download(),
                i64::from(r0.length),
                "web-seed payload must count"
            );
            assert_eq!(t.web_seed_payload_downloaded, i64::from(r0.length));
        } else {
            assert_eq!(t.total_download(), 0, "report off → no accounting");
        }
    }
}

/// a block delivered *after* its batch was released (timeout,
/// remove) is stale — it must not write again; upstream's conn
/// teardown discards it, we count `redundant_bytes`.
#[test]
fn web_seed_stale_block_is_redundant() {
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;
    use bitdaemon_session::torrent::WebSeedBackoff;

    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.files_checked = true;
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
    let reqs = out
        .iter()
        .find_map(|e| match e {
            SessionOut::WebSeedFetch { reqs, .. } => Some(reqs.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let r0 = reqs[0];
    // release the whole batch (as a timeout would).
    s.on_web_seed_error(
        &ih(1),
        "http://seed.example/file.bin",
        &reqs,
        "released",
        WebSeedBackoff::Retry,
    );
    s.on_web_seed_done(&ih(1), "http://seed.example/file.bin", false);

    // the late task still reports its payload — stale, not written.
    let mut out2 = Vec::new();
    s.on_web_seed_block(
        &ih(1),
        "http://seed.example/file.bin",
        r0,
        vec![0xAB; r0.length as usize],
        &mut out2,
    );
    let t = &s.torrents[&ih(1)];
    assert!(
        t.redundant_bytes >= i64::from(r0.length),
        "stale block must count as redundant"
    );
    assert!(
        !out2.iter().any(|e| matches!(
            e,
            SessionOut::Torrent(
                bitdaemon_session::torrent::TorrentEffect::DiskWrite { req, .. }
            ) if req.piece == r0.piece && req.start == r0.start
        )),
        "stale block must not re-write"
    );
}

/// `remove_url_seed` frees the seed's in-flight blocks (upstream's
/// conn teardown does the same on `remove_web_seed_iter`).
#[test]
fn remove_url_seed_releases_blocks() {
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;

    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.files_checked = true;
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
    assert!(
        out.iter()
            .any(|e| matches!(e, SessionOut::WebSeedFetch { .. }))
    );

    let t = s.torrents.get_mut(&ih(1)).unwrap();
    t.remove_url_seed("http://seed.example/file.bin");
    assert!(t.web_seed_outstanding.is_empty());
    assert!(t.web_seeds.is_empty());
    // the in-flight entry is tombstoned until its task reports.
    assert!(
        t.web_seed_state["http://seed.example/file.bin"].removed
            || !t
                .web_seed_state
                .contains_key("http://seed.example/file.bin")
    );

    // `on_web_seed_done` GCs the tombstone.
    s.on_web_seed_done(&ih(1), "http://seed.example/file.bin", false);
    assert!(
        !s.torrents[&ih(1)]
            .web_seed_state
            .contains_key("http://seed.example/file.bin")
    );

    // the virtual `peer_has` refcount released with the last seed —
    // the picker no longer sees every piece available.
    let t = &s.torrents[&ih(1)];
    assert_eq!(t.picker.as_ref().map(|p| p.num_have()), Some(0));
}

/// `urlseed_max_request_bytes` — the contiguous merge stops at the
/// cap instead of growing one unbounded request.
#[test]
fn web_seed_merge_capped_by_max_request_bytes() {
    use bitdaemon_core::FileStorage;
    use bitdaemon_session::session::SessionOut;

    let mut s = Session::new();
    // block_size is min(piece_length, 0x4000)=0x4000 here; cap at
    // exactly one block → no merging.
    s.settings.urlseed_max_request_bytes = 0x4000;
    s.settings.web_seed_blocks_per_tick = 4;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.files_checked = true;
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
    let reqs = out
        .iter()
        .find_map(|e| match e {
            SessionOut::WebSeedFetch { reqs, .. } => Some(reqs.clone()),
            _ => None,
        })
        .unwrap_or_default();
    assert!(
        reqs.iter().all(|r| i64::from(r.length) <= 0x4000),
        "merged request must not exceed urlseed_max_request_bytes: {reqs:?}"
    );
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
    // dht.cpp does it on every incoming message); >20 bytes or
    // `dht_tracker::incoming_packet`'s framing check drops it.
    let mut pkt = b"d2:ip4:".to_vec();
    pkt.extend([203, 0, 113, 7]); // 203.0.113.7 (TEST-NET-3, global)
    pkt.extend_from_slice(b"1:t4:aaaa1:y1:ee");
    let from: std::net::SocketAddr = "8.8.8.8:6881".parse().unwrap();
    let mut out = Vec::new();
    s.on_udp(from, &pkt, &mut out);
    // votes land on the per-listen-socket voter upstream
    // (`listen_socket_t::external_address`) — assert via the
    // session aggregate `external_ip_agg`.
    assert_eq!(
        s.external_ip_agg()
            .external_address(&std::net::IpAddr::from([8, 8, 8, 8])),
        std::net::IpAddr::from([203, 0, 113, 7])
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
            // distinct public IPs — same-IP incoming dups are
            // dropped, and local-net remotes land in the
            // unchoke-exempt `local` class (id 2).
            format!("85.17.9.{}:6881", pid).parse().unwrap(),
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

/// A09 — `block_uploaded_alert` carries the block *index*
/// (`r.start / t->block_size()` upstream, bt_peer_connection.cpp:2693),
/// not the byte offset.
#[test]
fn block_uploaded_alert_uses_block_index() {
    use bitdaemon_common::PeerRequest;
    use bitdaemon_disk::disk_buffer_pool::DiskBufferHolder;
    use bitdaemon_session::session::DiskCompleted;

    let mut s = Session::new();
    let t = mk_torrent(4); // piece_length 0x40000 → block_size 0x4000
    let ih = s.add_torrent(t);
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .peers
        .insert(7, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    s.pop_alerts();

    s.disk_completions()
        .send(DiskCompleted::Read {
            ih,
            peer: 7,
            req: PeerRequest {
                piece: PieceIndex(0),
                start: 0x8000,
                length: 0x4000,
            },
            buf: DiskBufferHolder::from_owned(vec![0xAB; 0x4000]),
            err: bitdaemon_disk::storage_error::StorageError::default(),
            issue_time: std::time::Instant::now(),
        })
        .unwrap();
    let mut out = Vec::new();
    s.drain_disk_completed(&mut out);

    assert!(out.iter().any(|o| matches!(
        o,
        SessionOut::ToPeer {
            msg: PeerMessage::Piece { piece, offset, .. },
            ..
        } if *piece == PieceIndex(0) && *offset == 0x8000
    )));
    assert!(s.pop_alerts().iter().any(|a| matches!(
        a,
        bitdaemon_session::alerts::Alert::BlockUploaded { block: 2, .. }
    )));
}

/// A09 — `on_disk_read_complete` returns before `write_piece` when
/// `m_disconnecting`: a conn in teardown gets neither the block nor
/// the alert (peer_connection.cpp:5647).
#[test]
fn read_complete_skips_disconnecting_conn() {
    use bitdaemon_common::PeerRequest;
    use bitdaemon_disk::disk_buffer_pool::DiskBufferHolder;
    use bitdaemon_session::session::DiskCompleted;

    let mut s = Session::new();
    let t = mk_torrent(4);
    let ih = s.add_torrent(t);
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.disconnecting = true;
    c.reading_bytes = 0x4000;
    s.torrents.get_mut(&ih).unwrap().peers.insert(7, c);
    s.pop_alerts();

    s.disk_completions()
        .send(DiskCompleted::Read {
            ih,
            peer: 7,
            req: PeerRequest {
                piece: PieceIndex(0),
                start: 0,
                length: 0x4000,
            },
            buf: DiskBufferHolder::from_owned(vec![0xAB; 0x4000]),
            err: bitdaemon_disk::storage_error::StorageError::default(),
            issue_time: std::time::Instant::now(),
        })
        .unwrap();
    let mut out = Vec::new();
    s.drain_disk_completed(&mut out);

    // in-flight read accounting still clears (`m_reading_bytes`).
    assert_eq!(s.torrents[&ih].peers[&7].reading_bytes, 0);
    assert!(!out.iter().any(|o| matches!(
        o,
        SessionOut::ToPeer {
            msg: PeerMessage::Piece { .. },
            ..
        }
    )));
    assert!(
        !s.pop_alerts()
            .iter()
            .any(|a| matches!(a, bitdaemon_session::alerts::Alert::BlockUploaded { .. }))
    );
}

/// `on_disk_write_complete` — upstream calls `t->update_gauge()` at
/// the end of the error leg for *both* aborted and real write
/// failures (peer_connection.cpp:3255): a subscribed torrent must be
/// flagged for the next `state_update_alert` either way.
#[test]
fn failed_write_marks_state_updated() {
    use bitdaemon_common::types::{FileIndex, PeerRequest};
    use bitdaemon_disk::storage_error::{Operation, StorageError};
    use bitdaemon_session::session::DiskCompleted;

    let mut s = Session::new();
    let mut t = mk_torrent(2);
    t.init_picker(true);
    t.state_subscription = true;
    let ih = s.add_torrent(t);

    let req = PeerRequest {
        piece: PieceIndex(0),
        start: 0,
        length: 0x4000,
    };

    // aborted write (`operation_aborted`) → mark_as_canceled only,
    // update_gauge still runs.
    s.disk_completions()
        .send(DiskCompleted::Write {
            ih,
            peer: 7,
            req,
            picker_gen: s.torrents[&ih].picker_generation,
            err: StorageError::new(
                std::io::Error::new(std::io::ErrorKind::Interrupted, "aborted"),
                FileIndex(0),
                Operation::FileWrite,
            ),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    assert!(
        s.torrents[&ih].needs_state_update,
        "aborted write still runs update_gauge upstream"
    );

    // real failure → cancel/write_failed/clear_piece + update_gauge.
    s.torrents.get_mut(&ih).unwrap().needs_state_update = false;
    s.disk_completions()
        .send(DiskCompleted::Write {
            ih,
            peer: 7,
            req,
            picker_gen: s.torrents[&ih].picker_generation,
            err: StorageError::new(
                std::io::Error::other("disk gone"),
                FileIndex(0),
                Operation::FileWrite,
            ),
        })
        .unwrap();
    s.drain_disk_completed(&mut Vec::new());
    assert!(
        s.torrents[&ih].needs_state_update,
        "real write failure runs update_gauge"
    );
}
