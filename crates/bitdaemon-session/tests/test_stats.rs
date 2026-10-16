// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `stat` channel fidelity — upstream `peer_connection::m_stat`
//! splits payload / protocol / ip-overhead per direction:
//! `peer_info.total_*` is payload-only, `up_speed`/`down_speed` are
//! all-channel rates, and `torrent_status::total_*` adds protocol
//! on top of payload (but not IP overhead). Sent bytes land at
//! socket-write completion, not enqueue.

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_session::alerts::Alert;
use bitdaemon_session::counters::Counter;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::{PeerPacket, Session, SessionOut};
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::status::StatusFlags;
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

/// non-piece messages account their whole frame to the protocol
/// channel and nothing to payload (`stat.received_bytes(0, wire)`).
#[test]
fn inbound_protocol_vs_payload_split() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    // Have frame = 4 (len) + 1 (id) + 4 (index) = 9 protocol bytes.
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: addr(6881),
        msg: PeerMessage::Have(PieceIndex(0)),
    };
    s.on_peer_message(&p, 9, &mut out);

    let t = &s.torrents[&ih];
    let c = &t.peers[&1];
    assert_eq!(c.stat_down_total, 0, "Have carries no payload");
    assert_eq!(c.stat_down_protocol, 9);
    assert_eq!(s.counters.value(Counter::RecvBytes), 9);
    assert_eq!(s.counters.value(Counter::RecvPayloadBytes), 0);

    // Piece frame = 13 protocol bytes + data.len payload.
    let data = vec![0xAAu8; 0x4000];
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: addr(6881),
        msg: PeerMessage::Piece {
            piece: PieceIndex(0),
            offset: 0,
            data: data.clone().into(),
        },
    };
    s.on_peer_message(&p, 13 + data.len(), &mut out);

    let c = &s.torrents[&ih].peers[&1];
    assert_eq!(c.stat_down_total, 0x4000, "piece payload lands");
    assert_eq!(c.stat_down_protocol, 9 + 13, "have 9 + piece hdr 13");
    assert_eq!(s.counters.value(Counter::RecvPayloadBytes), 0x4000);
}

/// `peer_info.total_download` is payload-only upstream; the
/// all-channel rate fields ride on `down_speed`, payload rate on
/// `payload_down_speed`.
#[test]
fn peer_info_totals_are_payload_only() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .peers
        .get_mut(&1)
        .unwrap()
        .now_ms = 10_000;

    // inbound piece + protocol
    let data = vec![0u8; 0x4000];
    let p = PeerPacket {
        info_hash: ih,
        peer: 1,
        remote: addr(6881),
        msg: PeerMessage::Piece {
            piece: PieceIndex(0),
            offset: 0,
            data: data.into(),
        },
    };
    s.on_peer_message(&p, 13 + 0x4000, &mut out);
    // socket send completion: 64 payload + 17 protocol.
    s.on_peer_sent(&ih, 1, 64 + 17, 64, &mut out);

    let t = &s.torrents[&ih];
    let pi = t.peer_info(1, None, &SessionSettings::default()).unwrap();
    assert_eq!(pi.total_download, 0x4000, "payload only");
    assert_eq!(pi.total_upload, 64, "payload only — protocol excluded");
    // upstream `p.send_quota`/`receive_quota` — live quota values.
    assert!(pi.send_quota >= 0 && pi.receive_quota >= 0);
    assert!(pi.last_request >= std::time::Duration::ZERO);
    assert!(pi.last_active <= std::time::Duration::from_secs(10));

    // status totals: payload + protocol, no ip-overhead.
    let st = s.torrents[&ih].status(
        &SessionSettings::default(),
        0,
        StatusFlags(0),
        &s.peer_class_pool,
    );
    assert_eq!(st.total_payload_download, 0x4000);
    assert_eq!(st.total_download, 0x4000 + 13);
    assert_eq!(st.total_payload_upload, 64);
    assert_eq!(st.total_upload, 64 + 17);
}

/// `on_peer_sent` — stats land when the bytes leave the socket;
/// the payload/protocol split is exact (`m_payloads` accounting).
#[test]
fn sent_bytes_split_at_write_completion() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    s.on_peer_sent(&ih, 1, 0x4000 + 13, 0x4000, &mut out);
    let c = &s.torrents[&ih].peers[&1];
    assert_eq!(c.stat_up_total, 0x4000);
    assert_eq!(c.stat_up_protocol, 13);
    assert_eq!(s.counters.value(Counter::SentBytes), 0x4000 + 13);
    assert_eq!(s.counters.value(Counter::SentPayloadBytes), 0x4000);
    // pure protocol batch — a Have write
    s.on_peer_sent(&ih, 1, 9, 0, &mut out);
    let c = &s.torrents[&ih].peers[&1];
    assert_eq!(c.stat_up_total, 0x4000, "unchanged");
    assert_eq!(c.stat_up_protocol, 13 + 9);
}

/// `trancieve_ip_packet` hits `m_stat` *unconditionally* — only the
/// quota debit is `rate_limit_ip_overhead`-gated.
#[test]
fn ip_overhead_stats_without_throttle() {
    let mut s = Session::new();
    let mut st = s.settings.clone();
    st.rate_limit_ip_overhead = false;
    s.apply_settings(st);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    let q = s.peer_quota(&ih, 1).unwrap();
    q.ip_overhead[bitdaemon_session::peer_conn::DOWNLOAD_CHANNEL].store(120, Relaxed);
    q.ip_overhead[bitdaemon_session::peer_conn::UPLOAD_CHANNEL].store(200, Relaxed);

    // drive the per-conn tick drain — `check_request_timeouts`
    // early-returns on a zero torrent clock; `now_ms` advances via
    // `update_rates`'s wall-delta, so seed it directly. Keep it
    // small so no disconnect ladder leg fires.
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.now_ms = 1_000;
        let c = t.peers.get_mut(&1).unwrap();
        c.now_ms = 1_000;
        c.last_receive_ms = 1_000;
        c.last_sent_ms = 1_000;
        c.connect_ms = 1_000;
        c.bitfield_received = true;
    }
    s.tick(&mut out);

    let c = &s.torrents[&ih].peers[&1];
    assert_eq!(c.stat_down_ip, 120, "stat folds even when quota gate off");
    assert_eq!(c.stat_up_ip, 200);
    assert_eq!(s.counters.value(Counter::RecvIpOverheadBytes), 120);
    assert_eq!(s.counters.value(Counter::SentIpOverheadBytes), 200);
    // no quota debited — channels were not throttled anyway, but the
    // accumulators must be drained either way.
    assert_eq!(
        q.ip_overhead[bitdaemon_session::peer_conn::DOWNLOAD_CHANNEL].load(Relaxed),
        0
    );
}

/// outgoing TCP conns carry `sent_syn` + `received_synack` overhead
/// on their stat channels at setup (`on_connection_complete`).
#[test]
fn outgoing_conn_syn_overhead() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();

    let mut conn = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    conn.outgoing = true;
    s.peer_connected(&ih, 1, conn, addr(6881), &mut out);

    let c = &s.torrents[&ih].peers[&1];
    // SYN (up) + SYN-ACK's ACK (up) + SYN-ACK itself (down).
    assert_eq!(c.stat_up_ip, 40 + 40);
    assert_eq!(c.stat_down_ip, 40);
    assert!(s.counters.value(Counter::SentIpOverheadBytes) >= 80);
    assert!(s.counters.value(Counter::RecvIpOverheadBytes) >= 40);
}

/// rates: `up_speed` is the all-channel rate, `payload_up_speed`
/// the payload channel — a protocol-heavy peer shows the split.
#[test]
fn per_peer_rate_channels() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);
    {
        let c = s.torrents.get_mut(&ih).unwrap().peers.get_mut(&1).unwrap();
        c.now_ms = 10_000;
        c.stat_up_total = 1000;
        c.stat_up_protocol = 200;
        c.stat_down_total = 8000;
        c.stat_down_protocol = 300;
        c.stat_down_ip = 60;
    }
    s.tick(&mut out);
    let t = &s.torrents[&ih];
    let pi = t.peer_info(1, None, &SessionSettings::default()).unwrap();
    assert!(pi.down_speed >= pi.payload_down_speed);
    assert!(pi.up_speed >= pi.payload_up_speed);
    // all-channel rate sees the protocol+overhead bytes
    assert!(
        pi.down_speed > pi.payload_down_speed || pi.payload_down_speed == 0,
        "down_speed folds protocol+ip"
    );
}

/// `bind_outgoing_socket` — `outgoing_port`/`num_outgoing_ports`
/// round-robins the local port; `0` leaves it to the OS.
#[test]
fn outgoing_port_rotation() {
    let mut s = Session::new();
    let mut st = s.settings.clone();
    st.outgoing_port = 40000;
    st.num_outgoing_ports = 2; // range [40000, 40002]
    s.apply_settings(st);

    let remote = addr(6881);
    let got: Vec<u16> = (0..5)
        .map(|_| s.outgoing_bind_addr(remote).unwrap().port())
        .collect();
    assert_eq!(got, vec![40000, 40001, 40002, 40000, 40001]);

    // family follows the remote
    let v6 = s
        .outgoing_bind_addr("[2001:db8::1]:6881".parse().unwrap())
        .unwrap();
    assert!(v6.is_ipv6());

    // 0 → OS pick
    let mut st = s.settings.clone();
    st.outgoing_port = 0;
    s.apply_settings(st);
    assert!(s.outgoing_bind_addr(remote).is_none());
}

/// accept-side EMFILE — `too_few_file_descriptors`, one conn of the
/// largest torrent dropped, `connections_limit` clamped to live.
#[test]
fn fd_exhausted_alert_and_disconnect() {
    let mut s = Session::new();
    let mut st = s.settings.clone();
    st.connections_limit = 200;
    st.alert_mask = bitdaemon_session::alerts::AlertCategory::ALL;
    s.apply_settings(st);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);
    add_conn(&mut s, &ih, 2, &mut out);
    out.clear();

    s.on_accept_fd_exhausted(&mut out);
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::Performance { warning }
                if warning == "too_few_file_descriptors")),
        "alert posted"
    );
    // one conn of the largest torrent marked disconnecting
    let t = &s.torrents[&ih];
    let disc = t.peers.values().filter(|c| c.disconnecting).count();
    assert_eq!(disc, 1, "one peer dropped");
    assert!(s.settings.connections_limit <= 200 && s.settings.connections_limit >= 10);

    // limit already tiny → no-op
    let mut s2 = Session::new();
    let mut st2 = s2.settings.clone();
    st2.connections_limit = 10;
    s2.apply_settings(st2);
    let mut out2 = Vec::new();
    s2.on_accept_fd_exhausted(&mut out2);
    assert!(s2.pop_alerts().is_empty());
}

/// session-scope `*_limit_too_low` — the per-tick session stat
/// overhead vs the *global class* throttle (upstream checks
/// `m_stat.*_ip_overhead() >= rate_limit(global)`).
#[test]
fn session_level_overhead_limit_alert() {
    let mut s = Session::new();
    let mut st = s.settings.clone();
    st.alert_mask = bitdaemon_session::alerts::AlertCategory::ALL;
    st.download_rate_limit = 30;
    s.apply_settings(st);
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    // 40 B of per-tick download overhead vs a 30 B/s global limit.
    let q = s.peer_quota(&ih, 1).unwrap();
    q.ip_overhead[bitdaemon_session::peer_conn::DOWNLOAD_CHANNEL].store(40, Relaxed);
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.now_ms = 1_000;
        let c = t.peers.get_mut(&1).unwrap();
        c.now_ms = 1_000;
        c.last_receive_ms = 1_000;
        c.last_sent_ms = 1_000;
        c.connect_ms = 1_000;
        c.bitfield_received = true;
    }
    s.tick(&mut out);
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::Performance { warning }
                if warning == "download_limit_too_low")),
        "global-class overhead alert"
    );
}

/// `update_unchoke_limit` — `num_optimistic >= limit/2` posts
/// `too_many_optimistic_unchoke_slots`; upstream has no `limit > 0`
/// guard (unlimited -1 still fires: 20 >= -1/2).
#[test]
fn optimistic_unchoke_slots_alert() {
    let mut s = Session::new();
    let mut st = s.settings.clone();
    st.alert_mask = bitdaemon_session::alerts::AlertCategory::ALL;
    st.unchoke_slots_limit = -1;
    s.apply_settings(st);
    assert!(
        s.pop_alerts()
            .iter()
            .any(|a| matches!(a, Alert::Performance { warning }
                if warning == "too_many_optimistic_unchoke_slots")),
        "fires on unlimited too (verbatim upstream)"
    );
}

// =====================================================================
// A29 — counter-sweep regressions
// =====================================================================

use bitdaemon_net::bt_peer_conn::{DialFail, FailOp};
use bitdaemon_session::counters::{
    disconnect_reason_counter, find_metric_idx, transport_error_counter,
};

/// `disconnect()`'s reason legs — every emitter must send a canonical
/// upstream `errors::*` name or the counter (and the uTP
/// `close_reason` map downstream) never sees it.
#[test]
fn disconnect_reason_counter_maps_errors_names() {
    for (reason, want) in [
        ("self_connection", Counter::UninterestingPeers),
        ("upload_upload_connection", Counter::UninterestingPeers),
        ("torrent_paused", Counter::UninterestingPeers),
        ("torrent_aborted", Counter::UninterestingPeers),
        ("uninteresting_upload_peer", Counter::UninterestingPeers),
        ("too_many_connections", Counter::TooManyPeers),
        ("no_memory", Counter::NoMemoryPeers),
        ("timed_out", Counter::TransportTimeoutPeers),
        ("timed_out_inactivity", Counter::TimeoutPeers),
        ("timed_out_no_request", Counter::TimeoutPeers),
        ("timed_out_no_interest", Counter::TimeoutPeers),
        ("timed_out_no_handshake", Counter::ConnectTimeouts),
    ] {
        assert_eq!(disconnect_reason_counter(reason), Some(want), "{reason}");
    }
    // not an upstream `errors::*` leg
    assert_eq!(disconnect_reason_counter("session_closing"), None);
    assert_eq!(disconnect_reason_counter("connect failed"), None);
}

/// `error::*` transport legs — the raw errno splits what `ErrorKind`
/// cannot (EPERM vs EACCES both read `PermissionDenied`; ENOBUFS and
/// ECANCELED land in `Uncategorized`).
#[test]
fn transport_error_counter_uses_raw_errno() {
    use std::io::ErrorKind as K;
    // EPERM — upstream `no_permission` → perm_peers.
    assert_eq!(
        transport_error_counter(K::PermissionDenied, Some(1)),
        Some(Counter::PermPeers)
    );
    // EACCES — `access_denied` → no_access_peers.
    assert_eq!(
        transport_error_counter(K::PermissionDenied, Some(13)),
        Some(Counter::NoAccessPeers)
    );
    // without a raw code the socket denial maps to access_denied.
    assert_eq!(
        transport_error_counter(K::PermissionDenied, None),
        Some(Counter::NoAccessPeers)
    );
    // ENOBUFS (linux) → buffer_peers — `ErrorKind` sees nothing.
    assert_eq!(
        transport_error_counter(K::Other, Some(105)),
        Some(Counter::BufferPeers)
    );
    // WSAENOBUFS — same counter on Windows.
    assert_eq!(
        transport_error_counter(K::Other, Some(10055)),
        Some(Counter::BufferPeers)
    );
    // ECANCELED → aborted_peers.
    assert_eq!(
        transport_error_counter(K::Other, Some(125)),
        Some(Counter::AbortedPeers)
    );
    // EINVAL via raw → invalid_arg_peers.
    assert_eq!(
        transport_error_counter(K::Other, Some(22)),
        Some(Counter::InvalidArgPeers)
    );
    // EHOSTUNREACH → unreachable_peers.
    assert_eq!(
        transport_error_counter(K::Other, Some(113)),
        Some(Counter::UnreachablePeers)
    );
    // plain `ErrorKind` legs when no raw code is available.
    assert_eq!(
        transport_error_counter(K::TimedOut, None),
        Some(Counter::TransportTimeoutPeers)
    );
    assert_eq!(
        transport_error_counter(K::ConnectionRefused, None),
        Some(Counter::ConnrefusedPeers)
    );
    assert_eq!(
        transport_error_counter(K::BrokenPipe, None),
        Some(Counter::BrokenPipePeers)
    );
    // unknown raw codes fall through to the kind table, then None.
    assert_eq!(transport_error_counter(K::Other, Some(999_999)), None);
    assert_eq!(transport_error_counter(K::Other, None), None);
}

/// `peer_connection::connect_failed` — a refused dial incs
/// `connect_timeouts` (unconditional upstream) + `disconnected_peers`
/// + the transport leg.
#[test]
fn connect_failed_incs_connect_timeouts() {
    let mut s = Session::new();
    let fail = DialFail::connect_failed(std::io::ErrorKind::ConnectionRefused, Some(111), false);
    s.peer_dial_failed(&ih(9), addr(7000), &fail);
    assert_eq!(s.counters.value(Counter::ConnectTimeouts), 1);
    assert_eq!(s.counters.value(Counter::DisconnectedPeers), 1);
    assert_eq!(s.counters.value(Counter::ConnrefusedPeers), 1);
    // severity failure (TCP) → error_tcp_peers + error_outgoing_peers.
    assert_eq!(s.counters.value(Counter::ErrorTcpPeers), 1);
    assert_eq!(s.counters.value(Counter::ErrorOutgoingPeers), 1);
    // not PeerError → no error_peers.
    assert_eq!(s.counters.value(Counter::ErrorPeers), 0);
}

/// the handshake-deadline leg: `timed_out_no_handshake` reads
/// `connect_timeouts` upstream (`disconnect(errors::timed_out_no_handshake)`),
/// while a mid-stream `timed_out` reads `transport_timeout_peers`.
#[test]
fn handshake_timeout_lands_on_connect_timeouts() {
    let mut s = Session::new();
    let mut fail = DialFail::connect_failed(std::io::ErrorKind::TimedOut, None, false);
    fail.op = FailOp::Bittorrent;
    fail.reason = "timed_out_no_handshake";
    s.peer_dial_failed(&ih(9), addr(7001), &fail);
    assert_eq!(s.counters.value(Counter::ConnectTimeouts), 1);
    // io leg: TimedOut → transport_timeout_peers too (upstream runs
    // both the `error::timed_out` leg and the `errors::*` leg).
    assert_eq!(s.counters.value(Counter::TransportTimeoutPeers), 1);
}

/// `num_tcp_peers + sock_type` — attach incs, remove decs. The
/// rtc-shift quirk (upstream `socket_type_idx` has no rtc slot)
/// is reproduced verbatim: `sock_type` indexes raw.
#[test]
fn socket_type_gauge_follows_attach_remove() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();

    // sock_type 0 = plain TCP (upstream `socket_type_t::tcp`).
    add_conn(&mut s, &ih, 1, &mut out);
    assert_eq!(s.counters.value(Counter::NumTcpPeers), 1);
    assert_eq!(s.counters.value(Counter::NumSslPeers), 0);

    // a uTP conn (sock_type 3) lands on `num_utp_peers` — different
    // IP: `find_peer` dedups by address when
    // `allow_multiple_connections_per_ip` is off (upstream).
    let mut c2 = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c2.sock_type = 3;
    let remote2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 2)), 6882);
    s.peer_connected(&ih, 2, c2, remote2, &mut out);
    assert_eq!(s.counters.value(Counter::NumUtpPeers), 1);

    s.torrents.get_mut(&ih).unwrap().remove_peer(
        1,
        &s.counters,
        &s.peer_class_pool,
        &mut Vec::new(),
    );
    assert_eq!(s.counters.value(Counter::NumTcpPeers), 0);
    assert_eq!(s.counters.value(Counter::NumUtpPeers), 1);
}

/// `on_tick`/`on_udp`/`on_write` event counters + the send-size
/// histogram bucket (`log2p1(bytes >> 3)` clamped at 17).
#[test]
fn event_counters_and_send_histogram() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    let ih = s.add_torrent(t);
    let mut out = Vec::new();
    add_conn(&mut s, &ih, 1, &mut out);

    let base_tick = s.counters.value(Counter::OnTickCounter);
    s.tick(&mut out);
    assert_eq!(s.counters.value(Counter::OnTickCounter), base_tick + 1);

    // a datagram that isn't DHT/LSD still counts `on_udp` —
    // upstream incs at `on_udp_packet` entry.
    s.on_udp(addr(6000), b"not-dht", &mut out);
    assert_eq!(s.counters.value(Counter::OnUdpCounter), 1);

    // 16 KiB write → bucket log2p1(16384 >> 3) = log2p1(2048) = 12.
    let bucket =
        usize::try_from(bitdaemon_common::ffs::log2p1((0x4000usize >> 3) as u32).min(17)).unwrap();
    s.on_peer_sent(&ih, 1, 0x4000, 0x4000, &mut out);
    assert_eq!(s.counters.value(Counter::OnWriteCounter), 1);
    assert_eq!(
        s.counters
            .value_idx(Counter::SocketSendSize3 as usize + bucket),
        1
    );
}

/// `update_gauge` buckets — a downloading torrent lands on
/// `num_downloading_torrents` at snapshot; pausing a non-auto-managed
/// torrent lands on `num_stopped_torrents`.
#[test]
fn torrent_state_gauges_at_snapshot() {
    let mut s = Session::new();
    let mut t = mk_torrent(4);
    t.init_picker(true);
    t.auto_managed = true;
    let ih = s.add_torrent(t);

    let _ = s.session_stats();
    assert_eq!(s.counters.value(Counter::NumDownloadingTorrents), 1);
    assert_eq!(s.counters.value(Counter::NumStoppedTorrents), 0);

    // paused + not auto-managed → num_stopped_torrents upstream.
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        t.paused = true;
        t.auto_managed = false;
    }
    let _ = s.session_stats();
    assert_eq!(s.counters.value(Counter::NumStoppedTorrents), 1);
    assert_eq!(s.counters.value(Counter::NumDownloadingTorrents), 0);
}

/// `find_metric_idx` resolves `"category.name"` to the value_index
/// into the stats snapshot — the same index `stats_metrics` reports.
#[test]
fn find_metric_idx_resolves_value_index() {
    assert_eq!(
        find_metric_idx("peer.error_peers"),
        Some(Counter::ErrorPeers as usize)
    );
    assert_eq!(
        find_metric_idx("ses.num_downloading_torrents"),
        Some(Counter::NumDownloadingTorrents as usize)
    );
    assert_eq!(find_metric_idx("no.such_metric"), None);
    // a bare counter name without its category doesn't resolve —
    // upstream's find is over `category.name` strings.
    assert_eq!(find_metric_idx("error_peers"), None);
}
