// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! A07 regression — peer lifecycle & admission parity
//! (`session_impl::incoming_connection`, `peer_list::new_connection`,
//! `torrent::remove_peer`, per-conn peer ids, uTP→TCP fallback,
//! graceful-pause drain).

use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, PeerId, PeerSourceFlags, Sha1Hash};
use bitdaemon_net::peer_list::PeerListState;
use bitdaemon_session::counters::Counter;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{IncomingVerdict, Session, SessionOut};
use bitdaemon_session::torrent::{Torrent, TorrentEffect};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn ep(a: u8, port: u16) -> SocketAddr {
    // public-range IPs — private/local ones short-circuit
    // peer-class derivation and never qualify as connect candidates.
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, 0, a)), port)
}

fn ih(b: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[b; 20]).unwrap()
}

fn mk_torrent(pieces: i32) -> Torrent {
    let mut have = Bitfield::new();
    have.resize_with(pieces as usize, false);
    Torrent::new(ih(1), pieces, 0x40000, have, StorageIndex(0))
}

fn incoming_conn() -> PeerConn {
    PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0)
}

fn disconnects(out: &[SessionOut]) -> Vec<(u64, &'static str)> {
    out.iter()
        .filter_map(|o| match o {
            SessionOut::Torrent(TorrentEffect::DisconnectPeer { peer, reason, .. }) => {
                Some((*peer, *reason))
            }
            _ => None,
        })
        .collect()
}

// ---------------- incoming_connection gates (D5) ----------------

#[test]
fn incoming_paused_session_rejects() {
    let mut s = Session::new();
    s.paused = true;
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 0),
        IncomingVerdict::Reject
    );
}

#[test]
fn incoming_transport_gates() {
    let mut s = Session::new();
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    s.add_torrent(mk_torrent(4));
    s.settings.enable_incoming_tcp = false;
    // kind 0 = plain TCP listener
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 0),
        IncomingVerdict::Reject
    );
    s.settings.enable_incoming_tcp = true;
    s.settings.enable_incoming_utp = false;
    // kind 3 = uTP
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 3),
        IncomingVerdict::Reject
    );
    assert!(s.pop_alerts().iter().any(
        |a| matches!(a, bitdaemon_session::alerts::Alert::PeerBlocked { reason, .. } if reason == "utp_disabled")
    ));
    s.settings.enable_incoming_utp = true;
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 3),
        IncomingVerdict::Accept
    );
}

#[test]
fn incoming_interface_gate() {
    let mut s = Session::new();
    s.add_torrent(mk_torrent(4));
    s.settings.outgoing_interfaces = b"10.0.0.9".to_vec();
    // no `local` reported → can't verify the interface → reject.
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 0),
        IncomingVerdict::Reject
    );
}

#[test]
fn incoming_ip_filter_non_filter_torrents() {
    use bitdaemon_net::ip_filter::IpFilter;
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.apply_ip_filter = false; // opts out → bumps non_filter_torrents
    let ih = s.add_torrent(t);
    let mut f = IpFilter::new();
    f.add_rule(
        ep(1, 0).ip(),
        ep(1, 0).ip(),
        bitdaemon_net::ip_filter::BLOCKED,
    );
    let mut sink = Vec::new();
    s.set_ip_filter(f, &mut sink);
    // the opted-out torrent keeps the session gate open
    // (`non_filter_torrents > 0` skips the session-level filter).
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 0),
        IncomingVerdict::Accept
    );
    // flip the torrent back under the filter → gate closes.
    let t = s.torrents.get_mut(&ih).unwrap();
    t.apply_ip_filter = true;
    s.counters.set(Counter::NonFilterTorrents, 0);
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 0),
        IncomingVerdict::Reject
    );
}

#[test]
fn incoming_empty_and_cap() {
    let mut s = Session::new();
    // no torrents → silent reject
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 0),
        IncomingVerdict::Reject
    );
    let mut s = Session::new();
    s.add_torrent(mk_torrent(4));
    s.settings.connections_limit = 0;
    s.settings.connections_slack = 0;
    assert_eq!(
        s.on_incoming_connection(ep(1, 40000), None, 0),
        IncomingVerdict::Reject
    );
}

// ---------------- admission legs on attach (D2/D11) ----------------

#[test]
fn banned_peer_refused_at_attach() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.add_swarm_peer(ep(1, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    let ih = s.add_torrent(t);
    let i = s.torrents[&ih].peer_list.find_peer(ep(1, 0).ip()).unwrap();
    s.torrents.get_mut(&ih).unwrap().peer_list.ban_peer(i);

    let mut out = Vec::new();
    s.peer_connected(&ih, 1, incoming_conn(), ep(1, 6881), &mut out);
    assert!(s.torrents[&ih].peers.is_empty());
    assert!(disconnects(&out).contains(&(1, "peer_banned")));
}

#[test]
fn self_connection_via_dial_local() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    // a pending outbound dial to ep(5,6881) bound locally on
    // ep(5,9000): an inbound conn arriving FROM ep(5,9000) is us
    // connecting to ourselves.
    let pid = PeerId::from_bytes(&[7; 20]).unwrap();
    assert!(s.register_dial(&ih, ep(5, 6881), pid));
    s.register_dial_local(&ih, ep(5, 6881), ep(5, 9000));

    let mut out = Vec::new();
    s.peer_connected(&ih, 1, incoming_conn(), ep(5, 9000), &mut out);
    // both legs die at `self_connection` (failure severity); the
    // entry is banned; the pending dial mark is cleared.
    let t = &s.torrents[&ih];
    let i = t.peer_list.find_peer(ep(5, 0).ip()).unwrap();
    assert!(t.peer_list.peers()[i].banned);
    assert!(t.peer_list.peers()[i].connection.is_none());
    assert!(t.peers.is_empty());
    assert_eq!(t.num_connecting, 0);
    assert!(!t.dial_pids.contains_key(&ep(5, 6881)));
    assert!(!t.outgoing_pids.contains(&pid));
    assert!(disconnects(&out).contains(&(1, "self_connection")));
}

#[test]
fn cross_direction_dup_resolution() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    // pending dial to ep(5,40000).
    let pid = PeerId::from_bytes(&[3; 20]).unwrap();
    assert!(s.register_dial(&ih, ep(5, 40000), pid));

    // inbound conn arrives on local port 50000 > remote 40000 → the
    // pending (outgoing) conn wins → KillOther → dial mark cleared,
    // incoming conn attaches.
    let mut c = incoming_conn();
    c.local = Some(ep(0, 50000));
    let mut out = Vec::new();
    s.peer_connected(&ih, 9, c, ep(5, 9000), &mut out);
    let t = &s.torrents[&ih];
    assert!(t.peers.contains_key(&9));
    assert_eq!(t.num_connecting, 0);
    assert!(!t.dial_pids.contains_key(&ep(5, 40000)));

    // reverse: our listen port lower than the dial's remote →
    // KillSelf, the pending dial survives.
    let pid2 = PeerId::from_bytes(&[4; 20]).unwrap();
    assert!(s.register_dial(&ih, ep(6, 40000), pid2));
    let mut c = incoming_conn();
    c.local = Some(ep(0, 6000));
    let mut out = Vec::new();
    s.peer_connected(&ih, 10, c, ep(6, 9000), &mut out);
    let t = &s.torrents[&ih];
    assert!(!t.peers.contains_key(&10));
    assert_eq!(t.num_connecting, 1);
    assert!(t.dial_pids.contains_key(&ep(6, 40000)));
    assert!(disconnects(&out).contains(&(10, "duplicate_peer_id")));
}

// ---------------- per-conn peer ids (D7) ----------------

#[test]
fn outgoing_dials_get_distinct_peer_ids() {
    let mut s = Session::new();
    // no connect-boost — the test drives the dials itself.
    s.settings.torrent_connect_boost = 0;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    for n in 1..=2 {
        t.add_swarm_peer(ep(n, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    }
    let ih = s.add_torrent(t);
    let st = PeerListState::default();
    let t = s.torrents.get_mut(&ih).unwrap();
    let (e1, pid1) = t.try_connect_peer(0, &st, &[]).unwrap();
    let (e2, pid2) = t.try_connect_peer(0, &st, &[]).unwrap();
    assert_ne!(pid1, pid2);
    assert_ne!(e1, e2);
    // both registered for the self-connection probe.
    assert!(t.outgoing_pids.contains(&pid1));
    assert!(t.outgoing_pids.contains(&pid2));
    assert!(t.dial_pids.values().any(|p| *p == pid1));
    assert!(t.dial_pids.values().any(|p| *p == pid2));
    assert_eq!(t.num_connecting, 2);

    // attach conn 7 to the first dial — its pid leaves dial_pids and
    // stays in outgoing_pids; teardown removes it.
    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.outgoing = true;
    c.our_peer_id = Some(pid1);
    let mut out = Vec::new();
    s.peer_connected(&ih, 7, c, e1, &mut out);
    let t = &s.torrents[&ih];
    assert!(t.peers.contains_key(&7));
    assert!(!t.dial_pids.contains_key(&e1));
    assert!(t.outgoing_pids.contains(&pid1));
    s.peer_disconnected(&ih, 7, None, &mut out);
    let t = &s.torrents[&ih];
    assert!(!t.outgoing_pids.contains(&pid1));
}

// ---------------- uTP → TCP fallback (D4) ----------------

#[test]
fn utp_failure_retries_tcp_without_failcount() {
    let mut s = Session::new();
    s.settings.torrent_connect_boost = 0;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.add_swarm_peer(ep(1, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    let t2 = s.add_torrent(t);
    let ih = t2;
    let st = PeerListState::default();
    // mark a dial on the entry (uTP conns share the entry bookkeeping).
    let (ep1, _pid) = s
        .torrents
        .get_mut(&ih)
        .unwrap()
        .try_connect_peer(0, &st, &[])
        .unwrap();
    s.peer_connect_failed(&ih, ep1, true);
    let t = &s.torrents[&ih];
    let i = t.peer_list.find_peer(ep1.ip()).unwrap();
    let p = &t.peer_list.peers()[i];
    // upstream: uTP failure → `supports_utp` cleared, `fast_reconnect`
    // armed, NO failcount bump, immediate TCP retry on the same entry.
    assert!(!p.supports_utp);
    assert_eq!(p.failcount, 0);
    assert!(s.pending_out.iter().any(|o| matches!(
        o,
        SessionOut::ConnectPeer { ih: h, ep, .. } if *h == ih && *ep == ep1
    )));
    // and a normal TCP failure DOES count.
    s.peer_connect_failed(&ih, ep1, false);
    let p = &s.torrents[&ih].peer_list.peers()[i];
    assert_eq!(p.failcount, 1);
}

// ---------------- conn_key bookkeeping (D1/D2 tail) ----------------

#[test]
fn disconnect_survives_port_rewrite() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    s.peer_connected(&ih, 5, incoming_conn(), ep(1, 40000), &mut out);
    assert!(s.torrents[&ih].peers.contains_key(&5));
    // ext-handshake `p` rewrites the stored port — the conn_key bond
    // keeps the bookkeeping correct.
    let i = s.torrents[&ih].peer_list.find_peer(ep(1, 0).ip()).unwrap();
    s.torrents.get_mut(&ih).unwrap().peer_list.update_peer_port(
        6881,
        i,
        PeerSourceFlags::DHT,
        &PeerListState::default(),
    );
    s.peer_disconnected(&ih, 5, None, &mut out);
    let t = &s.torrents[&ih];
    assert!(t.peers.is_empty());
    let i = t.peer_list.find_peer(ep(1, 0).ip()).unwrap();
    assert!(t.peer_list.peers()[i].connection.is_none());
    assert_eq!(t.peer_list.peers()[i].conn_key, 0);
}

// ---------------- graceful-pause drain (D14) ----------------

#[test]
fn graceful_pause_posts_paused_on_last_drain() {
    use bitdaemon_session::alerts::Alert;
    let mut s = Session::new();
    s.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    s.peer_connected(&ih, 1, incoming_conn(), ep(1, 40000), &mut out);
    s.peer_connected(&ih, 2, incoming_conn(), ep(2, 40000), &mut out);
    s.set_paused_graceful(&ih, &mut out);
    assert!(s.torrents[&ih].graceful_pause);
    // idle conns (no outstanding bytes) disconnect immediately;
    // remaining ones drain. Remove them all → TorrentPaused.
    out.clear();
    s.peer_disconnected(&ih, 1, None, &mut out);
    s.peer_disconnected(&ih, 2, None, &mut out);
    assert!(s.torrents[&ih].peers.is_empty());
    assert!(!s.torrents[&ih].graceful_pause);
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::TorrentPaused { .. }))
    );
}

// ---------------- D17: oldest pending-dial eviction (round 2) ----------------

#[test]
fn full_torrent_evicts_oldest_pending_dial() {
    let mut s = Session::new();
    s.settings.torrent_connect_boost = 0;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    // cap 2 with two pending dials → the incoming conn can only get
    // a slot by evicting the longest-waiting half-open dial
    // (upstream `m_connections` counts dials from
    // `create_peer_connection`; `num_connecting > max_conn / 10`).
    t.max_connections = 2;
    t.add_swarm_peer(ep(1, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    t.add_swarm_peer(ep(2, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    let ih = s.add_torrent(t);
    let st = PeerListState::default();
    let (ea, _pa);
    let (eb, _pb);
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        // dial A stamped at t=0, dial B at t=5 — A is the oldest.
        (ea, _pa) = t.try_connect_peer(0, &st, &[]).unwrap();
        (eb, _pb) = t.try_connect_peer(5, &st, &[]).unwrap();
        assert_ne!(ea, eb);
        assert_eq!(t.num_connecting, 2);
    }
    let mut out = Vec::new();
    s.peer_connected(&ih, 7, incoming_conn(), ep(9, 40000), &mut out);
    let t = &s.torrents[&ih];
    // the incoming conn attached; the oldest dial mark died.
    assert!(t.peers.contains_key(&7));
    assert_eq!(t.num_connecting, 1);
    let ia = t.peer_list.find_peer(ea.ip()).unwrap();
    assert!(t.peer_list.peers()[ia].connection.is_none());
    let ib = t.peer_list.find_peer(eb.ip()).unwrap();
    assert!(t.peer_list.peers()[ib].connection.is_some());
    assert!(!t.dial_pids.contains_key(&ea));
    assert!(t.dial_pids.contains_key(&eb));
    // no DisconnectPeer for the incoming conn itself.
    assert!(!disconnects(&out).iter().any(|(p, _)| *p == 7));
    // and the evicted entry got its dial-cooldown restamp via
    // `connection_closed` (upstream `disconnect` on the conn).
    assert_eq!(t.peer_list.peers()[ia].failcount, 0);
}

// ---------------- D18: live-conn count basis (round 2) ----------------

#[test]
fn num_connections_counts_live_not_peer_list() {
    let mut s = Session::new();
    s.settings.torrent_connect_boost = 0;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    for n in 1..=4 {
        t.add_swarm_peer(ep(n, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    }
    let ih = s.add_torrent(t);
    let st = PeerListState::default();
    let mut out = Vec::new();
    s.peer_connected(&ih, 1, incoming_conn(), ep(9, 40000), &mut out);
    let t = s.torrents.get_mut(&ih).unwrap();
    t.try_connect_peer(0, &st, &[]).unwrap();
    t.peers.get_mut(&1).unwrap().disconnecting = true;
    // upstream `num_peers()` = m_connections − m_peers_to_disconnect:
    // 0 live attached + 1 in-flight dial = 1 — while the peer-list
    // pool sits at 5 entries (4 tracker + 1 incoming).
    assert_eq!(t.num_connections(), 1);
    assert_eq!(t.peer_list.num_peers(), 5);
}

// ---------------- D19: ListenPort collision severity (round 2) ----------------

#[test]
fn listen_port_collision_is_normal_severity() {
    use bitdaemon_net::bt_peer_conn::DisconnectSeverity;
    use bitdaemon_session::peer_conn::PeerEffect;

    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    // two entries on the same IP can only coexist under
    // `allow_multiple_connections_per_ip` — the collision the
    // upstream check guards.
    t.peer_list_state.allow_multiple_connections_per_ip = true;
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    // two live conns on the same IP, distinct ports.
    s.peer_connected(&ih, 5, incoming_conn(), ep(1, 40000), &mut out);
    s.peer_connected(&ih, 6, incoming_conn(), ep(1, 6881), &mut out);
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        let mut eff = Vec::new();
        // conn 5 advertises listen port 6881 → its entry collides
        // with conn 6's live endpoint → `update_peer_port` false →
        // `disconnect(errors::duplicate_peer_id)` at *normal*
        // severity upstream (never peer_error).
        t.apply_peer_effects(
            5,
            vec![PeerEffect::ListenPort(6881)],
            &s.settings,
            &s.counters,
            &mut eff,
        );
        assert!(eff.iter().any(|e| matches!(
            e,
            TorrentEffect::DisconnectPeer { peer: 5, reason, sev }
                if *reason == "duplicate_peer_id" && *sev == DisconnectSeverity::Normal
        )));
    }
}

// ---------------- D20: share-mode recalc ordering (round 2) ----------------

#[test]
fn share_mode_recalc_excludes_incoming_conn() {
    use bitdaemon_common::DownloadPriority;
    use bitdaemon_common::types::PieceIndex;

    let mut s = Session::new();
    // `share_mode_target` 0 → the rarity-vs-peers gate passes on
    // the first eligible piece.
    s.settings.share_mode_target = 0;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.share_mode = true;
    {
        // nothing wanted yet — piece 0 becomes the only rarest
        // candidate once the picker sees a peer holding it.
        let p = t.picker.as_mut().unwrap();
        for i in 0..4 {
            p.set_piece_priority(PieceIndex(i), DownloadPriority::DONT_DOWNLOAD);
        }
        // piece 0 has one holder (the conn about to attach — its
        // bitfield lands on the conn, not the picker, at attach).
        p.inc_refcount(PieceIndex(0));
    }
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    let mut c = incoming_conn();
    c.connecting = false;
    c.peer_pieces.resize_with(4, false);
    c.peer_pieces.set_bit(0);
    c.num_pieces = 1;
    s.peer_connected(&ih, 1, c, ep(1, 40000), &mut out);
    assert!(s.torrents[&ih].peers.contains_key(&1));
    // upstream `attach_peer` recalculates BEFORE inserting the
    // conn → num_peers=0 → early return, piece 0 stays
    // dont_download. Post-insert (the old bug) the new conn alone
    // makes num_peers=1 → the rarest pick promotes piece 0.
    let t = &s.torrents[&ih];
    assert_eq!(
        t.picker.as_ref().unwrap().piece_priority(PieceIndex(0)),
        DownloadPriority::DONT_DOWNLOAD,
        "share-mode recalc must run before the conn joins the census"
    );
}

// ---------------- D21: ban_web_seeds gate (round 2) ----------------

#[test]
fn ban_peer_respects_ban_web_seeds() {
    let mut s = Session::new();
    // upstream default is `true` — drop it to exercise the gate.
    s.settings.ban_web_seeds = false;
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.add_swarm_peer(ep(1, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    t.add_swarm_peer(ep(2, 6881), PeerSourceFlags::TRACKER, &mut Vec::new());
    let ih = s.add_torrent(t);
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        let i = t.peer_list.find_peer(ep(1, 0).ip()).unwrap();
        t.peer_list.peers_mut()[i].web_seed = true;
        // `ban_web_seeds` off → upstream `ban_peer` refuses
        // (`torrent.cpp:11919`) and the entry keeps its flag.
        assert!(!t.ban_peer(ep(1, 6881), &s.settings, &s.counters));
        assert!(!t.peer_list.peers()[i].banned);
        // a regular entry bans fine under the same setting.
        assert!(t.ban_peer(ep(2, 6881), &s.settings, &s.counters));
        let j = t.peer_list.find_peer(ep(2, 0).ip()).unwrap();
        assert!(t.peer_list.peers()[j].banned);
    }
    // flip the setting → the web-seed entry bans too.
    s.settings.ban_web_seeds = true;
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        assert!(t.ban_peer(ep(1, 6881), &s.settings, &s.counters));
        let i = t.peer_list.find_peer(ep(1, 0).ip()).unwrap();
        assert!(t.peer_list.peers()[i].banned);
    }
}

// ---------------- D8: abort gate on remove_peer ----------------

#[test]
fn remove_peer_skips_timed_out_blocks() {
    use bitdaemon_common::types::PieceBlock;
    use bitdaemon_common::types::PieceIndex;
    use bitdaemon_session::peer_pipeline::PendingBlock;
    use bitdaemon_session::piece_picker::PickerOptions;

    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    let mut c = incoming_conn();
    c.peer_id = Some(PeerId::from_bytes(&[9; 20]).unwrap());
    s.peer_connected(&ih, 3, c, ep(1, 40000), &mut out);

    let pb0 = PieceBlock {
        piece_index: PieceIndex(0),
        block_index: 0,
    };
    let pb1 = PieceBlock {
        piece_index: PieceIndex(0),
        block_index: 1,
    };
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        let key = t.peers[&3].peer_key();
        let picker = t.picker.as_mut().unwrap();
        // peer 3 requests block 0 — then it times out and ANOTHER
        // downloader re-picks it (key 2). Block 1 stays live on 3.
        assert!(picker.mark_as_downloading(pb0, key, PickerOptions::NONE));
        picker.abort_download(pb0, key); // the timeout release
        assert!(picker.mark_as_downloading(pb0, Some(2), PickerOptions::NONE));
        assert!(picker.mark_as_downloading(pb1, key, PickerOptions::NONE));
        // conn 3 still carries both queue entries — block 0 as
        // timed_out (its picker state was already released and
        // re-requested by peer 2).
        let c = t.peers.get_mut(&3).unwrap();
        let mut b0 = PendingBlock::new(pb0);
        b0.timed_out = true;
        c.pipeline.download_queue.push(b0);
        c.pipeline.download_queue.push(PendingBlock::new(pb1));
    }
    let before = s.torrents[&ih]
        .picker
        .as_ref()
        .unwrap()
        .num_peers_block(pb0);
    assert_eq!(before, 1);

    let mut eff = Vec::new();
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.peer_disconnected_addr(ep(1, 40000), 0, false, false, 3);
        t.remove_peer(3, &s.counters, &s.peer_class_pool, &mut eff);
    }
    let p = s.torrents[&ih].picker.as_ref().unwrap();
    // timed-out block untouched — peer 2's request survives.
    assert_eq!(p.num_peers_block(pb0), 1);
    assert!(p.is_requested(pb0));
    // live block released.
    assert_eq!(p.num_peers_block(pb1), 0);
    assert!(!p.is_requested(pb1));
}
