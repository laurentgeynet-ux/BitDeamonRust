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
    ses.settings.active_downloads = 2;
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
    ses.manage_queue(&mut Vec::new());
    let queued: Vec<_> = ses.torrents.values().map(|t| t.queued).collect();
    assert_eq!(queued.iter().filter(|q| **q).count(), 2);
    // upstream `auto_manage_torrents`: pausing an auto-managed
    // torrent does NOT free its slot — the queue unpauses it on the
    // next pass (`set_paused(false)` within limits). A sticky pause
    // requires clearing `auto_managed` first.
    let first = *ses.torrents.keys().next().unwrap();
    ses.set_paused(&first, true, &mut Vec::new());
    ses.manage_queue(&mut Vec::new());
    let queued2 = ses.torrents.values().filter(|t| t.queued).count();
    assert_eq!(queued2, 2);
    // clearing auto_managed makes the pause sticky → the torrent
    // leaves the lists and frees its slot for the next one
    {
        let t = ses.torrents.get_mut(&first).unwrap();
        t.auto_managed = false;
        t.paused = true;
    }
    ses.manage_queue(&mut Vec::new());
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
    // upstream `on_piece_verified` ignores pieces the picker doesn't
    // consider finished — simulate the download completing.
    if let Some(p) = t.picker.as_mut() {
        for b in 0..p.blocks_in_piece(PieceIndex(0)) {
            p.mark_as_finished(
                PieceBlock {
                    piece_index: PieceIndex(0),
                    block_index: b,
                },
                None,
            );
        }
    }
    let addr = |_| std::net::IpAddr::from([10, 0, 0, 1]);
    let digest = |b: PieceBlock| Some(Sha1Hash::from_bytes(&[b.block_index as u8; 20]).unwrap());
    // peer 7 wasn't a downloader → no CRC recorded, no ban, but the
    // piece is restored/re-failed through the normal path
    let mut out = Vec::new();
    t.piece_verified_ban(
        PieceIndex(0),
        false,
        &[],
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
        addr,
        digest,
    );
    // `PieceFailed` (`hash_failed` alert) posts after the
    // `clear_piece` fence drains — drain it inline like the session
    // would.
    t.on_piece_sync(
        PieceIndex(0),
        &[],
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
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
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );

    // remote sends a pex message adding 203.0.113.9:6881 — a public
    // endpoint (upstream ignores `is_local` adds unless the sender
    // itself is local; peer 7's remote is public here)
    t.peers.get_mut(&7).unwrap().remote = Some(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)),
        6881,
    ));
    let added_ep = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)), 6881);
    let (body, _sent, _in_msg) = build_pex_message(&[(added_ep, 0)], &BTreeSet::new());
    t.on_peer_message(
        7,
        &PeerMessage::Extended {
            id: 1, // our local pex id (`PeerConn::local_ut_pex` — upstream fixed id 1)
            data: body.into(),
        },
        0,
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert_eq!(t.peer_list.num_peers(), 1);

    // a second connected peer — upstream's peer tick skips pex
    // entirely when `num_peers() <= 1`
    t.add_peer(8, PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0));

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
        &bitdaemon_session::counters::Counters::default(),
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

    // default_flags add torrents paused — resume first (upstream
    // `attach_to_torrent` rejects incoming conns to paused torrents
    // unless the auto-managed queue activated it; a real client
    // resumes the magnet right away).
    s.set_paused(&ih, false, &mut out);

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
    // BEP-9: the peer's extended handshake advertised `ut_metadata`
    // and its `metadata_size` — then our scheduler asks for piece 0
    // (`maybe_send_request`). An unsolicited `piece` reply would be
    // dropped upstream ("UNWANTED / TIMED OUT").
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        let conn = t.peers.get_mut(&1).unwrap();
        conn.ext_ut_metadata = 1;
        conn.metadata_size = Some(info.len() as i64);
        let mut eff = Vec::new();
        t.metadata_tick(&mut eff);
        assert!(t.peers[&1].metadata_sent_requests.contains(&0));
    }
    s.on_peer_message(
        &PeerPacket {
            info_hash: ih,
            peer: 1,
            remote,
            msg: PeerMessage::Bitfield(vec![0x80].into()),
        },
        0,
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
                id: 2, // `local_ut_metadata` — upstream fixed id 2
                data: packet.into(),
            },
        },
        0,
        &mut out,
    );

    // upstream `torrent::init` → `async_check_files` — interest is
    // deferred until `files_checked` sets `m_connections_initialized`
    // and replays `pc->init()` per conn. Deliver the check verdict.
    s.disk_completions()
        .send(bitdaemon_session::session::DiskCompleted::CheckFiles {
            ih,
            accepted: true,
            status: 0,
            err: Default::default(),
        })
        .unwrap();
    s.drain_disk_completed(&mut out);

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

// ---------- A19 regressions ----------

/// Build a synthetic v2-only or hybrid single-file torrent via
/// `create_torrent`; returns `(info_dict_bytes, resolved_info_hashes)`.
/// The served `ut_metadata` payload is the raw `info` dict section —
/// never a re-encode (bencode is canonical so `data_section` ==
/// re-serialized here, but the torrent always hashes the verbatim
/// bytes).
fn synth_torrent(v2_only: bool) -> (Vec<u8>, bitdaemon_common::info_hash::InfoHash) {
    use bitdaemon_common::FileIndex;
    use bitdaemon_core::create_torrent::{CreateFileEntry, CreateFlags, CreateTorrent};
    use bitdaemon_core::file_storage::FileFlags;
    let flags = if v2_only {
        CreateFlags::V2_ONLY
    } else {
        CreateFlags::NONE
    };
    let mut ct = CreateTorrent::new(
        vec![CreateFileEntry {
            filename: b"test".to_vec(),
            size: 24,
            flags: FileFlags::from_bits(0),
            mtime: 0,
            symlink: Vec::new(),
        }],
        0x4000,
        flags,
    )
    .unwrap();
    if !v2_only {
        ct.set_hash(PieceIndex(0), Sha1Hash::from_bytes(&[0x77; 20]).unwrap())
            .unwrap();
    }
    ct.set_hash2(
        FileIndex(0),
        0,
        bitdaemon_common::Sha256Hash::from_bytes(&[0x88; 32]).unwrap(),
    )
    .unwrap();
    let buf = ct.generate_buf().unwrap();
    let info = bitdaemon_bencode::bdecode(&buf)
        .dict_find(b"info")
        .data_section()
        .to_vec();
    let atp =
        bitdaemon_core::torrent_info::load_torrent_buffer(&buf, &LoadTorrentLimits::default())
            .unwrap();
    (info, *atp.ti.as_ref().unwrap().info_hashes())
}

/// A19-D1: a v2-only magnet (`btmh`-keyed — the session map key is the
/// *truncated* v2) resolves metadata by SHA-256. Upstream
/// `torrent::set_metadata` checks `hasher256` when `has_v2()`; a
/// SHA-1-only check can never match.
#[test]
fn v2_only_magnet_resolves_by_sha256() {
    use bitdaemon_common::info_hash::{InfoHash, ProtocolVersion as PV};
    let (info, real) = synth_torrent(true);
    assert!(real.has_v2() && !real.has_v1());
    let key = real.get(PV::V2); // truncated v2 — the magnet map key
    let mut t = Torrent::new(key, 0, 0, Bitfield::new(), StorageIndex(0));
    t.info_hashes = InfoHash::from_v2(real.v2);
    t.magnet = true;

    let r = t.on_ut_metadata_data(&info, 0, info.len() as i64, 9, 30 * 1024 * 1024);
    assert_eq!(r, MetadataResult::Complete);
    let md = t.metadata_tracker.metadata().to_vec();
    assert!(t.set_metadata(&md, &LoadTorrentLimits::default()));
    assert!(t.ti.is_some());
    assert!(!t.magnet);
}

/// A19-D1: a `btih`-keyed magnet whose "v1" hash is actually a
/// truncated v2 (a v2-only torrent announced on a v1 key) resolves via
/// the upstream truncated-v2 fallback in `set_metadata`
/// (`torrent.cpp:8282-8292`).
#[test]
fn truncated_v2_in_v1_slot_resolves() {
    use bitdaemon_common::info_hash::{InfoHash, ProtocolVersion as PV};
    let (info, real) = synth_torrent(true);
    let key = real.get(PV::V2);
    let mut t = Torrent::new(key, 0, 0, Bitfield::new(), StorageIndex(0));
    t.info_hashes = InfoHash::from_v1(key); // declared "v1" = truncated v2
    t.magnet = true;

    let r = t.on_ut_metadata_data(&info, 0, info.len() as i64, 9, 30 * 1024 * 1024);
    assert_eq!(r, MetadataResult::Complete);
    let md = t.metadata_tracker.metadata().to_vec();
    assert!(t.set_metadata(&md, &LoadTorrentLimits::default()));
    // upstream `m_info_hash = m_torrent_file->info_hashes()` — the
    // resolved set is the real v2 hash, not the truncated key.
    assert!(t.info_hashes.has_v2());
}

/// A19-D1: a hybrid torrent reached through a v2-keyed magnet verifies
/// BOTH hashes (v1 truncated-fallback skipped, full SHA-256 required).
#[test]
fn v2_keyed_hybrid_magnet_resolves() {
    use bitdaemon_common::info_hash::{InfoHash, ProtocolVersion as PV};
    let (info, real) = synth_torrent(false);
    assert!(real.has_v1() && real.has_v2());
    let key = real.get(PV::V2);
    let mut t = Torrent::new(key, 0, 0, Bitfield::new(), StorageIndex(0));
    t.info_hashes = InfoHash::from_v2(real.v2);
    t.magnet = true;

    let md_ok = {
        let r = t.on_ut_metadata_data(&info, 0, info.len() as i64, 9, 30 * 1024 * 1024);
        assert_eq!(r, MetadataResult::Complete);
        t.metadata_tracker.metadata().to_vec()
    };
    assert!(t.set_metadata(&md_ok, &LoadTorrentLimits::default()));
    assert_eq!(t.info_hashes, real);
}

/// A19-D3: post-`init`, every conn re-runs `disconnect_if_redundant`
/// (`torrent.cpp:8445-8447`) — a conn that was only useful while we
/// lacked metadata and is now redundant gets dropped.
#[test]
fn set_metadata_disconnects_redundant_peers() {
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::torrent::TorrentEffect;
    let info = info_bytes(&[0x55; 20]);
    let ih = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(&info);
        Sha1Hash::from_bytes(&h.finalize()).unwrap()
    };
    let mut t = Torrent::new(ih, 0, 0, Bitfield::new(), StorageIndex(0));
    t.magnet = true;
    // we're upload-only; the remote flags upload-only too → once
    // metadata lands the conn is redundant (`upload_upload_connection`)
    t.upload_mode = true;
    let mut c = PeerConn::new(1, PeerPipeline::new(1000, 30, 250), 0);
    c.remote_upload_only = true;
    t.add_peer(7, c);

    let r = t.on_ut_metadata_data(&info, 0, info.len() as i64, 7, 30 * 1024 * 1024);
    assert_eq!(r, MetadataResult::Complete);
    let md = t.metadata_tracker.metadata().to_vec();
    let mut out = Vec::new();
    let v = t.set_metadata_verdict(
        &md,
        &LoadTorrentLimits::default(),
        &bitdaemon_session::settings::SessionSettings::default(),
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert!(matches!(v, bitdaemon_session::torrent::MetadataVerdict::Ok));
    assert!(
        out.iter()
            .any(|e| matches!(e, TorrentEffect::DisconnectPeer { peer: 7, .. }))
    );
}

/// A19-D4: `prioritize_pieces` on a seed is a no-op upstream
/// (`torrent.cpp:5967` — before the metadata check) and must not
/// dirty resume data when nothing changed.
#[test]
fn prioritize_pieces_noop_on_seed() {
    use bitdaemon_common::types::DownloadPriority;
    let ih = Sha1Hash::from_bytes(&[0x66; 20]).unwrap();
    let mut t = Torrent::new(ih, 1, 16384, Bitfield::with_size(1), StorageIndex(0));
    t.init_picker(true);
    t.seeding = true; // is_seed()
    let mut out = Vec::new();
    t.prioritize_pieces(
        &[DownloadPriority::DONT_DOWNLOAD],
        &bitdaemon_session::counters::Counters::default(),
        &mut out,
    );
    assert!(!t.need_save_resume.is_set());
    let p = t.picker.as_ref().unwrap();
    assert_eq!(p.piece_priority(PieceIndex(0)), DownloadPriority::DEFAULT);
}

/// A19-D5: `num_total_pieces_added` charges on `set_metadata`
/// (magnet path) — upstream `torrent.cpp:8442`.
#[test]
fn set_metadata_charges_pieces_counter() {
    let info = info_bytes(&[0x55; 20]);
    let ih = {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(&info);
        Sha1Hash::from_bytes(&h.finalize()).unwrap()
    };
    let mut t = Torrent::new(ih, 0, 0, Bitfield::new(), StorageIndex(0));
    let counters = bitdaemon_session::counters::Counters::default();
    let mut out = Vec::new();
    let v = t.set_metadata_verdict(
        &info,
        &LoadTorrentLimits::default(),
        &bitdaemon_session::settings::SessionSettings::default(),
        &counters,
        &mut out,
    );
    assert!(matches!(v, bitdaemon_session::torrent::MetadataVerdict::Ok));
    assert_eq!(
        counters.value(bitdaemon_session::counters::Counter::NumTotalPiecesAdded),
        1
    );
}

/// A19-D6: a v2-only magnet's resume data must carry `info-hash2`,
/// not the truncated v2 mislabeled as `info-hash` — upstream writes
/// `ret.info_hashes = m_info_hash` (`torrent.cpp:7562`).
#[test]
fn v2_magnet_resume_keeps_v2_hash() {
    use bitdaemon_common::info_hash::{InfoHash, ProtocolVersion as PV};
    let full_v2 = bitdaemon_common::Sha256Hash::from_bytes(&[0x42; 32]).unwrap();
    let key = Sha1Hash::from_bytes(&full_v2.as_bytes()[..20]).unwrap();
    let mut t = Torrent::new(key, 0, 0, Bitfield::new(), StorageIndex(0));
    t.info_hashes = InfoHash::from_v2(full_v2);
    t.magnet = true;

    let mut atp = bitdaemon_core::add_torrent_params::AddTorrentParams::default();
    t.to_resume_data(&mut atp, &bitdaemon_net::peer_class::PeerClassPool::new());
    assert!(!atp.info_hashes.has_v1());
    assert!(atp.info_hashes.has_v2());
    assert_eq!(atp.info_hashes.v2, full_v2);

    // full round-trip through the wire format — a v2 magnet must
    // restore as v2, not as a phantom v1 torrent
    let buf = t.write_resume_data(&bitdaemon_net::peer_class::PeerClassPool::new());
    let back =
        bitdaemon_core::resume_data::read_resume_data(&buf, &LoadTorrentLimits::default()).unwrap();
    assert!(!back.info_hashes.has_v1());
    assert!(back.info_hashes.has_v2());
    assert_eq!(back.info_hashes.get(PV::V2), key);
}

/// A19-D2: two metadata-less magnets resolving to the same swarm are
/// a `duplicate_torrent` error + pause on BOTH, plus
/// `torrent_conflict_alert` (upstream `torrent.cpp:8363-8395`) — and
/// no `metadata_received_alert` / storage creation.
#[test]
fn metadata_collision_conflicts_both_torrents() {
    use bitdaemon_common::info_hash::{InfoHash, ProtocolVersion as PV};
    use bitdaemon_core::add_torrent_params::AddTorrentParams;
    use bitdaemon_net::peer_wire::PeerMessage;
    use bitdaemon_session::peer_conn::PeerConn;
    use bitdaemon_session::peer_pipeline::PeerPipeline;
    use bitdaemon_session::session::{PeerPacket, Session};
    use bitdaemon_session::ut_metadata::{MetadataMsg, metadata_packet};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let (info, real) = synth_torrent(false); // hybrid: v1 + v2

    let mut s = Session::new();
    // torrent A: v1-keyed magnet for the same swarm (metadata-less)
    let mut out = Vec::new();
    let atp_a = AddTorrentParams {
        info_hashes: InfoHash::from_v1(real.v1),
        ..Default::default()
    };
    assert!(
        s.add_torrent_params(&atp_a, StorageIndex::INVALID, &mut out)
            .is_some()
    );
    // torrent B: v2-keyed magnet of the same hybrid torrent —
    // different map key, no add-time conflict
    let key_b = real.get(PV::V2);
    let atp_b = AddTorrentParams {
        info_hashes: InfoHash::from_v2(real.v2),
        ..Default::default()
    };
    assert_eq!(
        s.add_torrent_params(&atp_b, StorageIndex::INVALID, &mut out),
        Some(key_b)
    );

    s.set_paused(&key_b, false, &mut out);
    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6881);
    s.peer_connected(
        &key_b,
        1,
        PeerConn::new(1, PeerPipeline::new(1000, 30, 250), 0),
        remote,
        &mut out,
    );
    {
        let t = s.torrents.get_mut(&key_b).unwrap();
        let c = t.peers.get_mut(&1).unwrap();
        c.ext_ut_metadata = 1;
        c.metadata_size = Some(info.len() as i64);
        let mut eff = Vec::new();
        t.metadata_tick(&mut eff);
        assert!(t.peers[&1].metadata_sent_requests.contains(&0));
    }
    // serve the single metadata piece → set_metadata resolves a
    // hybrid whose v1 collides with torrent A
    let packet = metadata_packet(MetadataMsg::Piece, 0, true, &info);
    s.on_peer_message(
        &PeerPacket {
            info_hash: key_b,
            peer: 1,
            remote,
            msg: PeerMessage::Extended {
                id: 2,
                data: packet.into(),
            },
        },
        0,
        &mut out,
    );

    let a = &s.torrents[&real.v1];
    let b = &s.torrents[&key_b];
    assert_eq!(a.last_error, "duplicate torrent");
    assert_eq!(b.last_error, "duplicate torrent");
    assert!(a.paused && b.paused);

    let alerts = s.pop_alerts();
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, bitdaemon_session::alerts::Alert::TorrentConflict { .. }))
    );
    assert!(
        !alerts
            .iter()
            .any(|a| matches!(a, bitdaemon_session::alerts::Alert::MetadataReceived { .. }))
    );
}
