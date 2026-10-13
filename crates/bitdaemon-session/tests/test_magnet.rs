// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! magnet glue — `ut_metadata` pieces → `Torrent::set_metadata`
//! (BEP-9 SHA-1 check → `TorrentInfo` parse → picker init).

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_core::torrent_info::LoadTorrentLimits;
use bitdaemon_session::torrent::Torrent;
use bitdaemon_session::ut_metadata::MetadataResult;

/// minimal v1 info dict: 1 piece of 16 KiB over one 24-byte file.
fn info_bytes(hashes: &[u8]) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend(b"d6:lengthi24e4:name4:test12:piece lengthi16384e6:pieces20:");
    v.extend(hashes);
    v.extend(b"e");
    v
}

#[test]
fn magnet_resolves_via_ut_metadata() {
    let info = info_bytes(&[0x55; 20]);
    let ih = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(&info);
        Sha1Hash::from_bytes(&h.finalize()).unwrap()
    };
    let mut t = Torrent::new(ih, 0, 0, Bitfield::new(), StorageIndex(0));
    t.magnet = true;

    // metadata fits one 16 KiB ut_metadata piece
    let r = t.on_ut_metadata_data(&info, 0, info.len() as i64, 42, 30 * 1024 * 1024);
    assert_eq!(r, MetadataResult::Complete);

    let md = t.metadata_tracker.metadata().to_vec();
    assert!(t.set_metadata(&md, &LoadTorrentLimits::default()));
    assert_eq!(t.num_pieces, 1);
    assert_eq!(t.piece_length, 16384);
    assert!(t.picker.is_some());
    assert!(!t.magnet);
    assert_eq!(
        t.expected_hash(PieceIndex(0)).unwrap(),
        Sha1Hash::from_bytes(&[0x55; 20]).unwrap()
    );
}

#[test]
fn wrong_metadata_rejected() {
    let good = info_bytes(&[0x55; 20]);
    let bad = info_bytes(&[0xAA; 20]);
    let ih = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(&good);
        Sha1Hash::from_bytes(&h.finalize()).unwrap()
    };
    let mut t = Torrent::new(ih, 0, 0, Bitfield::new(), StorageIndex(0));
    let r = t.on_ut_metadata_data(&bad, 0, bad.len() as i64, 7, 30 * 1024 * 1024);
    assert_eq!(r, MetadataResult::Complete);
    let md = t.metadata_tracker.metadata().to_vec();
    assert!(!t.set_metadata(&md, &LoadTorrentLimits::default()));
    // upstream resets request counts and penalizes the source
    let (_, sources) = t.metadata_tracker.reset_on_hash_failure();
    assert_eq!(sources, vec![7]);
    assert!(t.picker.is_none());
}

#[test]
fn auto_managed_queue_limits_downloads() {
    use bitdaemon_session::session::Session;
    let mut ses = Session::new();
    for i in 0..4u8 {
        let ih = Sha1Hash::from_bytes(&[i; 20]).unwrap();
        ses.add_torrent(Torrent::new(
            ih,
            1,
            16384,
            Bitfield::with_size(1),
            StorageIndex(u32::from(i)),
        ));
    }
    ses.manage_queue(2, -1);
    let queued: Vec<_> = ses.torrents.values().map(|t| t.queued).collect();
    assert_eq!(queued.iter().filter(|q| **q).count(), 2);
    // upstream `auto_manage_torrents`: pausing an auto-managed
    // torrent does NOT free its slot — the queue unpauses it on the
    // next pass (`set_paused(false)` within limits). A sticky pause
    // requires clearing `auto_managed` first.
    let first = *ses.torrents.keys().next().unwrap();
    ses.set_paused(&first, true, &mut Vec::new());
    ses.manage_queue(2, -1);
    let queued2 = ses.torrents.values().filter(|t| t.queued).count();
    assert_eq!(queued2, 2);
    // clearing auto_managed makes the pause sticky → the torrent
    // leaves the lists and frees its slot for the next one
    {
        let t = ses.torrents.get_mut(&first).unwrap();
        t.auto_managed = false;
        t.paused = true;
    }
    ses.manage_queue(2, -1);
    let queued2 = ses.torrents.values().filter(|t| t.queued).count();
    assert_eq!(queued2, 1);
    assert!(ses.torrents.get(&first).unwrap().paused);
}

#[test]
fn smart_ban_disconnects_corrupt_peer() {
    use bitdaemon_common::types::PieceBlock;
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::torrent::TorrentEffect;
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let mut t = Torrent::new(ih, 1, 16384, Bitfield::with_size(1), StorageIndex(0));
    t.init_picker(true);
    t.add_peer(7, PeerConn::new(1, PeerPipeline::new(1000, 30, 250), 0));
    let addr = |_| std::net::IpAddr::from([10, 0, 0, 1]);
    let digest = |b: PieceBlock| Some(Sha1Hash::from_bytes(&[b.block_index as u8; 20]).unwrap());
    // peer 7 wasn't a downloader → no CRC recorded, no ban, but the
    // piece is restored/re-failed through the normal path
    let mut out = Vec::new();
    t.piece_verified_ban(
        PieceIndex(0),
        16384,
        false,
        &bitdaemon_session::settings::SessionSettings::default(),
        &mut out,
        addr,
        digest,
    );
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::PieceFailed(_)))
    );
    assert!(
        !out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { .. }))
    );
}

#[test]
fn pex_feeds_swarm_and_broadcasts() {
    use bitdaemon_net::bep10::ExtendedHandshake;
    use bitdaemon_net::peer_wire::PeerMessage;
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::torrent::TorrentEffect;
    use bitdaemon_session::ut_pex::build_pex_message;
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let ih = Sha1Hash::from_bytes(&[3; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 16384, Bitfield::with_size(4), StorageIndex(0));
    t.add_peer(7, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));
    let mut out = Vec::new();

    // peer advertises ut_pex id 2
    let mut m = BTreeMap::new();
    m.insert("ut_pex".to_string(), 2);
    let hs = ExtendedHandshake {
        messages: m,
        ..Default::default()
    };
    t.on_peer_message(
        7,
        &PeerMessage::Extended {
            id: 0,
            data: bitdaemon_bencode::bencode(&hs.build()).into(),
        },
        0,
        &bitdaemon_session::settings::SessionSettings::default(),
        &mut out,
    );

    // remote sends a pex message adding 10.1.2.3:6881
    let added_ep = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)), 6881);
    let (body, _sent) = build_pex_message(&[(added_ep, 0)], &BTreeSet::new());
    t.on_peer_message(
        7,
        &PeerMessage::Extended {
            id: 2, // our local pex id (PeerConn::local_ut_pex)
            data: body.into(),
        },
        0,
        &bitdaemon_session::settings::SessionSettings::default(),
        &mut out,
    );
    assert_eq!(t.peer_list.num_peers(), 1);

    // tick → we emit a pex diff to the peer (contains peer 7? we
    // advertise remote endpoints of our connections — none have
    // remote set here, so the diff is empty but still sent)
    let mut out2 = Vec::new();
    t.pex_tick(std::time::Instant::now(), &mut out2);
    assert!(out2.iter().any(|e| matches!(
        e,
        TorrentEffect::SendTo {
            msg: PeerMessage::Extended { id: 2, .. },
            ..
        }
    )));
}

/// `send_peer` filter (upstream `ut_pex_peer_plugin::tick` →
/// `torrent::send_peer`): pex must advertise only connections that
/// dialed out OR shared their listen port (`p`), are past the
/// handshake, and inbound endpoints must carry the advertised `p`
/// port — not the ephemeral source port. Regression: the lifecycle
/// fields were never set in production, so the filter emptied the
/// list and live swarms never received usable `added` entries.
#[test]
fn pex_advertises_only_completed_dialable_peers() {
    use bitdaemon_net::bep10::ExtendedHandshake;
    use bitdaemon_net::peer_wire::PeerMessage;
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::torrent::TorrentEffect;
    use bitdaemon_session::ut_pex::parse_pex_message;
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let mut t = Torrent::new(ih, 4, 16384, Bitfield::with_size(4), StorageIndex(0));

    // recipient conn — speaks ut_pex but stays "connecting" so it is
    // not itself advertised
    t.add_peer(7, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));
    let mut m = BTreeMap::new();
    m.insert("ut_pex".to_string(), 2);
    let hs = ExtendedHandshake {
        messages: m,
        ..Default::default()
    };
    let mut scratch = Vec::new();
    t.on_peer_message(
        7,
        &PeerMessage::Extended {
            id: 0,
            data: bitdaemon_bencode::bencode(&hs.build()).into(),
        },
        0,
        &bitdaemon_session::settings::SessionSettings::default(),
        &mut scratch,
    );

    let mk = |remote: SocketAddr| {
        let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
        c.remote = Some(remote);
        c.connecting = false;
        c.handshake_done = true;
        c.num_pieces = 4; // post-handshake conns have seen a bitfield
        c
    };
    let seed_ep = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 9, 9, 1)), 6881);
    let mut seed = mk(seed_ep);
    seed.outgoing = true;
    seed.num_pieces = 4;
    seed.peer_pieces.set_all(); // seed → pex_seed flag
    t.add_peer(8, seed);

    // inbound conn that shared `p` — advertised at the listen port,
    // not the ephemeral source port
    let inbound_src = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 9, 9, 2)), 55000);
    let mut inbound = mk(inbound_src);
    inbound.received_listen_port = true;
    inbound.peer_listen_port = Some(6883);
    t.add_peer(9, inbound);

    // inbound without `p` — never advertised
    let mut nop = mk(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 9, 9, 3)),
        55001,
    ));
    nop.outgoing = false;
    t.add_peer(10, nop);

    // outgoing but pre-handshake — never advertised (the prod bug:
    // these fields stayed false forever)
    let mut hs_pending = mk(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 9, 9, 4)),
        6884,
    ));
    hs_pending.outgoing = true;
    hs_pending.handshake_done = false;
    t.add_peer(11, hs_pending);

    let mut out = Vec::new();
    t.pex_tick(std::time::Instant::now(), &mut out);
    let body = out
        .iter()
        .find_map(|e| match e {
            TorrentEffect::SendTo {
                peer: 7,
                msg: PeerMessage::Extended { id: 2, data },
            } => Some(data.clone()),
            _ => None,
        })
        .expect("pex message to peer 7");
    let (added, dropped) = parse_pex_message(&body, 50, 0).expect("valid pex");
    let eps: Vec<SocketAddr> = added.iter().map(|p| p.addr).collect();
    assert_eq!(
        eps,
        vec![
            seed_ep,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 9, 9, 2)), 6883)
        ]
    );
    assert_eq!(added[0].flags, bitdaemon_session::ut_pex::flags::PEX_SEED);
    assert_eq!(added[1].flags, 0);
    assert!(dropped.is_empty());
}

/// session-level: a magnet added with `StorageIndex::INVALID` must get
/// real storage when metadata resolves (`torrent::init` upstream) —
/// regression for magnet downloads stalling at 0% after the info dict
/// arrived (every disk job routed to a nonexistent index).
#[test]
fn magnet_metadata_creates_storage() {
    use bitdaemon_common::info_hash::InfoHash;
    use bitdaemon_core::add_torrent_params::AddTorrentParams;
    use bitdaemon_disk::disk_io::PreadDiskIo;
    use bitdaemon_net::peer_wire::PeerMessage;
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::session::{PeerPacket, Session};
    use bitdaemon_session::ut_metadata::{MetadataMsg, metadata_packet};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    let info = info_bytes(&[0x55; 20]);
    let ih = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(&info);
        Sha1Hash::from_bytes(&h.finalize()).unwrap()
    };

    let dir = std::env::temp_dir().join(format!("bd-mag-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut s = Session::new();
    s.set_disk(PreadDiskIo::new(1, Arc::new(|| {})));

    // magnet add — no `ti`, storage must stay INVALID for now
    let atp = AddTorrentParams {
        info_hashes: InfoHash::from_v1(ih),
        save_path: dir.as_os_str().as_encoded_bytes().to_vec(),
        ..Default::default()
    };
    let mut out = Vec::new();
    assert_eq!(
        s.add_torrent_params(&atp, StorageIndex::INVALID, &mut out),
        Some(ih)
    );
    assert!(s.torrents[&ih].magnet);
    assert_eq!(s.torrents[&ih].storage, StorageIndex::INVALID);

    // peer 1 connects BEFORE metadata and sends its bitfield while
    // our num_pieces is still unknown — it must be stored raw, not
    // truncated to zero bits
    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6881);
    s.peer_connected(
        &ih,
        1,
        PeerConn::new(1, PeerPipeline::new(1000, 30, 250), 0),
        remote,
        &mut out,
    );
    s.on_peer_message(
        &PeerPacket {
            info_hash: ih,
            peer: 1,
            remote,
            msg: PeerMessage::Bitfield(vec![0x80].into()),
        },
        &mut out,
    );

    // peer 1 then serves the single ut_metadata piece → MetadataReceived
    let packet = metadata_packet(MetadataMsg::Piece, 0, true, &info);
    s.on_peer_message(
        &PeerPacket {
            info_hash: ih,
            peer: 1,
            remote,
            msg: PeerMessage::Extended {
                id: 1,
                data: packet.into(),
            },
        },
        &mut out,
    );

    let t = &s.torrents[&ih];
    assert!(t.ti.is_some());
    assert!(!t.magnet);
    // deferred storage exists now — first real slot is index 0
    assert_eq!(t.storage, StorageIndex(0));
    // and the pre-metadata bitfield made the peer interesting →
    // `Interested` was emitted on the wire (peers stay choked
    // otherwise — this was the real 0%-forever failure)
    assert!(out.iter().any(|o| matches!(
        o,
        bitdaemon_session::session::SessionOut::ToPeer {
            msg: PeerMessage::Interested,
            ..
        }
    )));
}
