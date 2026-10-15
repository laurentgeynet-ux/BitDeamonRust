// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `choker` / `recalculate_(optimistic_)unchoke_slots` —
//! audit A11 regressions: `unchoke_sort` returns the uncapped slot
//! count, `ignore_unchoke_slots` conns stay out of both picks,
//! peer-class upload priority orders the sort, `choke_this_peer`
//! re-arms the scalers, and the `incoming_interested` tail re-runs
//! `maybe_unchoke_this_peer` (or re-sends a redundant `Unchoke`).

use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::choker::{
    ChokePeer, ChokerSettings, ChokingAlgorithm, SeedChokingAlgorithm, unchoke_sort,
};
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{PeerPacket, Session, SessionOut};
use bitdaemon_session::torrent::Torrent;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

fn ih(b: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[b; 20]).unwrap()
}

fn mk_torrent(pieces: i32, have: bool) -> Torrent {
    let mut bf = Bitfield::new();
    bf.resize_with(pieces as usize, have);
    Torrent::new(ih(1), pieces, 0x40000, bf, StorageIndex(0))
}

fn conn(pieces: i32) -> PeerConn {
    let mut c = PeerConn::new(pieces as usize, PeerPipeline::new(1000, 30, 250), 0);
    // post-handshake state — `is_connecting()` conns are skipped
    // by both choker sweeps upstream.
    c.connecting = false;
    c.handshake_done = true;
    c
}

fn ep(b: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, 9, b)), 6881)
}

/// attach a conn through the session attach path (assigns remote +
/// peer classes + the swarm entry).
fn attach(s: &mut Session, ih: &Sha1Hash, id: u64, addr: SocketAddr, c: PeerConn) {
    let mut out = Vec::new();
    s.peer_connected(ih, id, c, addr, &mut out);
}

fn interested(s: &mut Session, ih: &Sha1Hash, id: u64, addr: SocketAddr) {
    let mut out = Vec::new();
    s.on_peer_message(
        &PeerPacket {
            info_hash: *ih,
            peer: id,
            remote: addr,
            msg: PeerMessage::Interested,
        },
        0,
        &mut out,
    );
}

fn unchokes_to(out: &[SessionOut], peer: u64) -> usize {
    out.iter()
        .filter(|e| {
            matches!(
                e,
                SessionOut::ToPeer {
                    peer: p,
                    msg: PeerMessage::Unchoke,
                    ..
                } if *p == peer
            )
        })
        .count()
}

fn chokes_to(out: &[SessionOut], peer: u64) -> usize {
    out.iter()
        .filter(|e| {
            matches!(
                e,
                SessionOut::ToPeer {
                    peer: p,
                    msg: PeerMessage::Choke,
                    ..
                } if *p == peer
            )
        })
        .count()
}

fn choke_peer(id: u64) -> ChokePeer {
    ChokePeer {
        id,
        choked: true,
        interested: true,
        uploaded_last: 0,
        downloaded_last: 0,
        uploaded_since_unchoked: 0,
        last_unchoke: Instant::now(),
        seeding: false,
        num_pieces: 0,
        uploaded_total: 0,
        total_size: 4 * 0x40000,
        piece_length: 0x40000,
        priority: 1,
    }
}

fn settings(slots: i32) -> ChokerSettings {
    ChokerSettings {
        slots_limit: slots,
        algorithm: ChokingAlgorithm::FixedSlots,
        seed_algorithm: SeedChokingAlgorithm::RoundRobin,
        seeding_piece_quota: 20,
        rate_threshold: 8192,
    }
}

/// `unchoke_sort` returns `upload_slots` *uncapped* — upstream
/// `return upload_slots` (`choker.cpp`), not `min(slots, peers.len())`.
/// The caller derives `num_opt_unchoke`/`unchoke_set_size` from it;
/// capping starves real unchokes when the eligible set is small.
#[test]
fn unchoke_sort_returns_uncapped_slot_count() {
    let mut peers = vec![choke_peer(1), choke_peer(2)];
    let allowed = unchoke_sort(
        &mut peers,
        Duration::from_secs(15),
        &settings(8),
        Instant::now(),
    );
    assert_eq!(allowed, 8, "2 eligible peers must not cap the 8 slots");
}

/// With fewer eligible peers than `unchoke_slots_limit`, every one of
/// them is unchoked — upstream's `unchoke_set_size` is computed off
/// the uncapped `allowed_upload_slots`.
#[test]
fn sweep_unchokes_all_eligible_below_quota() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, true));
    attach(&mut s, &ih, 1, ep(1), conn(4));
    attach(&mut s, &ih, 2, ep(2), conn(4));
    interested(&mut s, &ih, 1, ep(1));
    interested(&mut s, &ih, 2, ep(2));

    s.torrents.get_mut(&ih).unwrap().triggered_unchoke = true;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert_eq!(unchokes_to(&out, 1), 1, "peer 1 unchoked: {out:?}");
    assert_eq!(unchokes_to(&out, 2), 1, "peer 2 unchoked: {out:?}");
    assert!(s.torrents[&ih].peers.values().all(|c| !c.choked));
}

/// `ignore_unchoke_slots` conns are excluded from the eligibility
/// sweep entirely — never sorted, never choked, never counted
/// (`session_impl.cpp` first skip arm).
#[test]
fn exempt_conn_skipped_by_sweep() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, true));
    attach(&mut s, &ih, 1, ep(1), conn(4));
    // flag the conn's class `ignore_unchoke_slots`.
    let cls = s.peer_class_pool.new_peer_class("exempt");
    s.peer_class_pool.at_mut(cls).unwrap().ignore_unchoke_slots = true;
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        let c = t.peers.get_mut(&1).unwrap();
        c.peer_classes.add_class(&mut s.peer_class_pool, cls);
        // unchoked + uninterested: a non-exempt conn would be choked
        // by the sweep's ineligible leg.
        c.choked = false;
        c.peer_interested = false;
    }
    s.torrents.get_mut(&ih).unwrap().triggered_unchoke = true;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert_eq!(
        chokes_to(&out, 1),
        0,
        "exempt conn must not be choked: {out:?}"
    );
    assert!(!s.torrents[&ih].peers[&1].choked);
    assert_eq!(unchokes_to(&out, 1), 0, "exempt conn is not in the sort");
}

/// Exempt conns are also out of the optimistic candidate set
/// (`session_impl.cpp` `!p->ignore_unchoke_slots()`).
#[test]
fn exempt_conn_not_an_optimistic_candidate() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, true));
    attach(&mut s, &ih, 1, ep(1), conn(4));
    attach(&mut s, &ih, 2, ep(2), conn(4));
    interested(&mut s, &ih, 1, ep(1));
    interested(&mut s, &ih, 2, ep(2));
    let cls = s.peer_class_pool.new_peer_class("exempt");
    s.peer_class_pool.at_mut(cls).unwrap().ignore_unchoke_slots = true;
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.peers
            .get_mut(&1)
            .unwrap()
            .peer_classes
            .add_class(&mut s.peer_class_pool, cls);
    }
    // optimistic recalc needs num_unchoke_slots > 0 — run one
    // regular pass first (peer 2 takes the real slot, peer 1 stays
    // choked because it was never a candidate).
    s.counters
        .set(bitdaemon_session::counters::Counter::NumUnchokeSlots, 8);
    s.torrents.get_mut(&ih).unwrap().triggered_optimistic = true;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert_eq!(
        unchokes_to(&out, 1),
        0,
        "exempt conn must not be optimistically unchoked: {out:?}"
    );
}

/// `compare_peers` orders on `get_priority(upload_channel)` first —
/// a conn in a higher-priority class wins the slot over an equal
/// one (`choker.cpp` `compare_peers` head).
#[test]
fn class_priority_orders_the_unchoke_set() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, true));
    // 2 slots ⇒ unchoke_set = 2 - max(1, 2/5) = 1 — only the top
    // sorted peer gets a regular unchoke.
    s.settings.unchoke_slots_limit = 2;
    attach(&mut s, &ih, 1, ep(1), conn(4));
    attach(&mut s, &ih, 2, ep(2), conn(4));
    interested(&mut s, &ih, 1, ep(1));
    interested(&mut s, &ih, 2, ep(2));
    let cls = s.peer_class_pool.new_peer_class("fast");
    s.peer_class_pool.at_mut(cls).unwrap().priority[bitdaemon_net::peer_class::UPLOAD] = 5;
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .peers
        .get_mut(&2)
        .unwrap()
        .peer_classes
        .add_class(&mut s.peer_class_pool, cls);

    s.torrents.get_mut(&ih).unwrap().triggered_unchoke = true;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert_eq!(
        unchokes_to(&out, 2),
        1,
        "higher-priority peer unchoked: {out:?}"
    );
    assert_eq!(
        unchokes_to(&out, 1),
        0,
        "peer 1 loses the only real slot: {out:?}"
    );
}

/// `choke_this_peer` re-arms `trigger_unchoke` — a freed slot is
/// refilled on the next choker pass, not at the next period
/// (`peer_connection.cpp` `t->trigger_unchoke()`).
#[test]
fn not_interested_rearms_the_unchoke() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, true));
    attach(&mut s, &ih, 1, ep(1), conn(4));
    interested(&mut s, &ih, 1, ep(1));
    s.torrents.get_mut(&ih).unwrap().triggered_unchoke = true;
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(unchokes_to(&out, 1) >= 1);
    assert!(!s.torrents[&ih].peers[&1].choked);

    s.on_peer_message(
        &PeerPacket {
            info_hash: ih,
            peer: 1,
            remote: ep(1),
            msg: PeerMessage::NotInterested,
        },
        0,
        &mut out,
    );
    assert!(s.torrents[&ih].peers[&1].choked);
    assert!(
        s.torrents[&ih].triggered_unchoke,
        "a vacated upload slot re-arms the unchoke pick"
    );
}

/// `incoming_interested` on an already-unchoked conn re-sends a
/// redundant `Unchoke` (`peer_connection.cpp` — peers that missed
/// the handshake unchoke get nudged).
#[test]
fn interested_on_unchoked_resends_unchoke() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, true));
    attach(&mut s, &ih, 1, ep(1), conn(4));
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .peers
        .get_mut(&1)
        .unwrap()
        .choked = false;

    let mut out = Vec::new();
    s.on_peer_message(
        &PeerPacket {
            info_hash: ih,
            peer: 1,
            remote: ep(1),
            msg: PeerMessage::Interested,
        },
        0,
        &mut out,
    );
    assert_eq!(
        unchokes_to(&out, 1),
        1,
        "redundant Unchoke on Interested: {out:?}"
    );
}

/// The `num_have > 0` gate is the handshake call site's, not
/// `maybe_unchoke_this_peer`'s — an exempt conn that sends
/// `Interested` while we hold nothing still gets unchoked
/// (`bt_peer_connection.cpp` vs `incoming_interested`).
#[test]
fn interested_unchoke_has_no_num_have_gate() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, false));
    attach(&mut s, &ih, 1, ep(1), conn(4));
    let cls = s.peer_class_pool.new_peer_class("exempt");
    s.peer_class_pool.at_mut(cls).unwrap().ignore_unchoke_slots = true;
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.peers
            .get_mut(&1)
            .unwrap()
            .peer_classes
            .add_class(&mut s.peer_class_pool, cls);
    }
    let mut out = Vec::new();
    s.on_peer_message(
        &PeerPacket {
            info_hash: ih,
            peer: 1,
            remote: ep(1),
            msg: PeerMessage::Interested,
        },
        0,
        &mut out,
    );
    assert_eq!(
        unchokes_to(&out, 1),
        1,
        "exempt conn unchokes on Interested even with num_have == 0: {out:?}"
    );
}

/// `free_upload_slots` counts only slot-consuming conns — an
/// exempt unchoked conn doesn't eat the `max_uploads` budget the
/// optimistic candidates check against.
#[test]
fn free_upload_slots_ignores_exempt_conns() {
    let mut s = Session::new();
    let ih = s.add_torrent(mk_torrent(4, true));
    s.torrents.get_mut(&ih).unwrap().set_max_uploads(1, false);
    attach(&mut s, &ih, 1, ep(1), conn(4));
    let cls = s.peer_class_pool.new_peer_class("exempt");
    s.peer_class_pool.at_mut(cls).unwrap().ignore_unchoke_slots = true;
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        let c = t.peers.get_mut(&1).unwrap();
        c.peer_classes.add_class(&mut s.peer_class_pool, cls);
        c.choked = false;
        assert!(
            t.free_upload_slots(&s.peer_class_pool),
            "an exempt unchoked conn doesn't consume max_uploads"
        );
    }
}
