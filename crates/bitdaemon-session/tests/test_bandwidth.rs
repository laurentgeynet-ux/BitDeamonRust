// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `bandwidth_manager`/`bandwidth_channel` integration — the session
//! schedulers distribute quota through the peer-class channels:
//! request queueing, priority weighting, disconnect return, and the
//! per-tick IP/TCP overhead drain (`use_quota_overhead`).

use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_class::DOWNLOAD;
use bitdaemon_session::alerts::Alert;
use bitdaemon_session::peer_conn::{DOWNLOAD_CHANNEL, PeerConn, UPLOAD_CHANNEL};
use bitdaemon_session::peer_info::BandwidthState;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::torrent::Torrent;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::Ordering::Relaxed;

fn ih(b: u8) -> Sha1Hash {
    Sha1Hash::from_bytes(&[b; 20]).unwrap()
}

fn mk_torrent(pieces: i32) -> Torrent {
    let mut have = Bitfield::new();
    have.resize_with(pieces as usize, false);
    Torrent::new(ih(1), pieces, 0x40000, have, StorageIndex(0))
}

fn addr(port: u16) -> SocketAddr {
    // TEST-NET-3 — a *public* address: local-net peers bypass the
    // global rate limit upstream (they land on `local_peer_class`,
    // not `global_peer_class`).
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)), port)
}

fn add_conn(s: &mut Session, ih: &Sha1Hash, id: u64, out: &mut Vec<SessionOut>) {
    s.peer_connected(
        ih,
        id,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        addr(6881 + id as u16),
        out,
    );
}

#[test]
fn unthrottled_request_grants_immediately() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    // no throttled class channels → satisfied in place
    let ret = s.request_bandwidth(&ih, 1, DOWNLOAD_CHANNEL, 16 * 1024);
    assert!(matches!(ret, Some(n) if n > 0));
    let q = s.peer_quota(&ih, 1).unwrap();
    assert!(q.quota[DOWNLOAD_CHANNEL].load(Relaxed) > 0);
    // not queued → no `bw_limit`
    assert_eq!(
        q.channel_state[DOWNLOAD_CHANNEL].load(Relaxed) & BandwidthState::LIMIT.0,
        0
    );
}

#[test]
fn throttled_request_queues_and_grant_dispatches() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    // `download_rate_limit` lands on the global peer-class channel
    // — every conn carries it via `set_peer_classes`.
    let mut st = s.settings.clone();
    st.download_rate_limit = 1000;
    s.apply_settings(st);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    let q = s.peer_quota(&ih, 1).unwrap();
    // empty channel → queued, `bw_limit` set (upstream only
    // dispatches a grant once `assigned` covers the whole request —
    // a small ask lands in one distribution round)
    assert_eq!(s.request_bandwidth(&ih, 1, DOWNLOAD_CHANNEL, 1), Some(0));
    assert_ne!(
        q.channel_state[DOWNLOAD_CHANNEL].load(Relaxed) & BandwidthState::LIMIT.0,
        0
    );
    assert_eq!(s.download_rate.queue_size(), 1);

    // one distribution round: ~1000 B/s * 1 s lands, flag clears
    s.update_quotas(1000);
    assert!(q.quota[DOWNLOAD_CHANNEL].load(Relaxed) > 0);
    assert_eq!(
        q.channel_state[DOWNLOAD_CHANNEL].load(Relaxed) & BandwidthState::LIMIT.0,
        0
    );
    assert_eq!(s.download_rate.queue_size(), 0);
}

#[test]
fn torrent_and_conn_channels_both_constrain() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    // global 1 MB/s + per-torrent 1000 B/s: the torrent channel is
    // the binding one but both must appear in the request.
    let mut st = s.settings.clone();
    st.download_rate_limit = 1_000_000;
    s.apply_settings(st);
    let ih = s.add_torrent(t);
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.set_download_limit(1000, &mut s.peer_class_pool);
    }
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    // a small ask dispatches in one round; both class channels pay
    assert_eq!(s.request_bandwidth(&ih, 1, DOWNLOAD_CHANNEL, 1), Some(0));
    s.update_quotas(1000);
    let q = s.peer_quota(&ih, 1).unwrap();
    assert!(q.quota[DOWNLOAD_CHANNEL].load(Relaxed) > 0);
    // the torrent's class channel debited below its fresh refill
    let t = s.torrents.get(&ih).unwrap();
    let pc = s.peer_class_pool.at(t.peer_class).unwrap();
    assert!(pc.channel[DOWNLOAD].quota_left() < 1000);
}

#[test]
fn disconnecting_peer_returns_quota() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let mut st = s.settings.clone();
    st.download_rate_limit = 100;
    s.apply_settings(st);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);
    add_conn(&mut s, &ih, 2, &mut out);

    // both queue on the drained channel
    assert_eq!(s.request_bandwidth(&ih, 1, DOWNLOAD_CHANNEL, 4096), Some(0));
    assert_eq!(s.request_bandwidth(&ih, 2, DOWNLOAD_CHANNEL, 4096), Some(0));
    assert_eq!(s.download_rate.queue_size(), 2);

    // first round gives each a share; conn 1 dies mid-queue
    s.update_quotas(1000);
    let q1 = s.peer_quota(&ih, 1).unwrap();
    q1.disconnecting.store(true, Relaxed);
    let before = s.peer_class_pool.at(s.global_peer_class).unwrap().channel[DOWNLOAD].quota_left();

    // conn 1's request is still queued (`bw_limit` was never cleared
    // — no grant dispatched) — the next pass drops it and returns
    // its partial `assigned` quota.
    s.update_quotas(1000);
    let after = s.peer_class_pool.at(s.global_peer_class).unwrap().channel[DOWNLOAD].quota_left();
    // its assigned share was returned, not lost
    assert!(after >= before);
    // and its request is gone from the queue
    assert!(s.download_rate.queue_size() <= 1);
}

#[test]
fn overhead_drains_channels_and_alerts() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    // tiny limits: 40 B/s < the IP overhead of one packet → both
    // directions warn (`performance_alert` requires the mask bit).
    let mut st = s.settings.clone();
    // `throttle < overhead` is strict upstream — 30 < ~40 warns
    st.download_rate_limit = 30;
    st.rate_limit_ip_overhead = true;
    st.alert_mask = bitdaemon_session::alerts::AlertCategory(
        bitdaemon_session::alerts::AlertCategory::ERROR.0
            | bitdaemon_session::alerts::AlertCategory::PERFORMANCE.0,
    );
    s.apply_settings(st);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    // simulate wire traffic: both directions accumulate IP overhead
    let q = s.peer_quota(&ih, 1).unwrap();
    let oh = bitdaemon_session::peer_conn::PeerConn::ip_overhead_bytes(1460, false);
    q.ip_overhead[DOWNLOAD_CHANNEL].fetch_add(oh, Relaxed);
    q.ip_overhead[UPLOAD_CHANNEL].fetch_add(oh, Relaxed);
    // drain happens inside the per-conn tick leg (gated on now_ms)
    s.torrents.get_mut(&ih).unwrap().now_ms = 1000;
    let mut eff = Vec::new();
    s.torrents.get_mut(&ih).unwrap().check_request_timeouts(
        &s.settings,
        1,
        &s.counters,
        &s.peer_class_pool,
        s.alerts.mask(),
        &mut eff,
    );
    // throttle (40) < overhead (~80) → download warning posted
    assert!(
        eff.iter().any(|e| matches!(
            e,
            bitdaemon_session::torrent::TorrentEffect::Alert(Alert::Performance {
                warning,
            }) if warning == "download_limit_too_low"
        )),
        "expected download_limit_too_low alert, got: {eff:?}"
    );
    // counters got the overhead
    assert!(
        s.counters
            .value(bitdaemon_session::counters::Counter::RecvIpOverheadBytes)
            > 0
    );
}
