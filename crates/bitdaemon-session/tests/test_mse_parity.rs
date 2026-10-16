// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! A03 — MSE/PE parity at the session/torrent seam: `pe_support`
//! link-up toggling + write-backs, `fast_reconnect` on the MSE probe,
//! `disconnect()` counter/alert legs by severity + direction, the
//! v1/v2 dual index behind `known_torrent*`, and the
//! `sanitize_settings` crypto knobs.

use bitdaemon_common::flags::PeerSourceFlags;
use bitdaemon_common::info_hash::{InfoHash, ProtocolVersion};
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash, Sha256Hash};
use bitdaemon_net::bt_peer_conn::{DialFail, DisconnectSeverity, FailOp, PeerGone};
use bitdaemon_net::pe_crypto::{EncPolicy, enc_level, stream_key_hash};
use bitdaemon_session::alerts::Alert;
use bitdaemon_session::counters::Counter;
use bitdaemon_session::peer_conn::PeerConn;
use bitdaemon_session::peer_info::PeerFlags;
use bitdaemon_session::peer_pipeline::PeerPipeline;
use bitdaemon_session::session::Session;
use bitdaemon_session::settings::SessionSettings;
use bitdaemon_session::torrent::{PeerAttach, Torrent};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn ep(a: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, 0, a)), 6881)
}

fn torrent_v1(ih_byte: u8, peers: u8) -> Torrent {
    let mut t = Torrent::new(
        Sha1Hash::from_bytes(&[ih_byte; 20]).unwrap(),
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    );
    for n in 1..=peers {
        t.add_swarm_peer(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(85, 17, ih_byte, n)), 6881),
            PeerSourceFlags::TRACKER,
            &mut Vec::new(),
        );
    }
    t
}

fn session(s: SessionSettings) -> Session {
    let mut ses = Session::new();
    ses.apply_settings(s);
    // alert assertions below need the categories unmasked.
    ses.alerts
        .set_mask(bitdaemon_session::alerts::AlertCategory::ALL);
    ses
}

fn enabled() -> SessionSettings {
    SessionSettings {
        out_enc_policy: EncPolicy::Enabled,
        // boost consumes the swarm candidates into `pending_out`
        // dials at `add_torrent` — the tests here pick them manually.
        torrent_connect_boost: 0,
        ..Default::default()
    }
}

fn attach(outgoing: bool, encrypted: bool) -> PeerAttach {
    PeerAttach {
        outgoing,
        encrypted,
        protocol_v2: false,
        was_utp: false,
        out_enabled: true,
    }
}

fn peer_idx(s: &Session, ih: &Sha1Hash, remote: SocketAddr) -> usize {
    s.torrents[ih].peer_list.find_peer(remote.ip()).unwrap()
}

fn pe_support(s: &Session, ih: &Sha1Hash, remote: SocketAddr) -> bool {
    s.torrents[ih].peer_list.peers()[peer_idx(s, ih, remote)].pe_support
}

#[test]
fn pe_support_toggles_at_link_up() {
    // upstream `bt_peer_connection::on_connected`: the crypto choice
    // rides on `pe_support` at connect time — `Enabled` + pe_support
    // → MSE probe with a pessimistic clear; `Enabled` + !pe_support
    // → plaintext with an optimistic set; `Forced`/`Disabled` never
    // touch the flag.
    let mut s = session(enabled());
    let ih = s.add_torrent(torrent_v1(1, 4));
    let st = s.peer_list_state();

    // drain four distinct candidates.
    let e: Vec<SocketAddr> = (0..4)
        .map(|_| {
            s.torrents
                .get_mut(&ih)
                .unwrap()
                .try_connect_peer(0, &st, &[])
                .0
                .into_dial()
                .unwrap()
                .0
        })
        .collect();

    // entry defaults `pe_support = false` → plaintext, optimistic set.
    let (k0, enc0) = s.peer_link_up(&ih, e[0], false).unwrap();
    assert_eq!(k0.wire_ih, ih);
    assert!(!enc0 && pe_support(&s, &ih, e[0]));

    // now `pe_support` is set → MSE probe + pessimistic clear.
    let (_, enc1) = s.peer_link_up(&ih, e[0], false).unwrap();
    assert!(enc1 && !pe_support(&s, &ih, e[0]));

    // Forced → MSE without touching the flag.
    s.apply_settings(SessionSettings {
        out_enc_policy: EncPolicy::Forced,
        ..enabled()
    });
    let (_, enc2) = s.peer_link_up(&ih, e[1], false).unwrap();
    assert!(enc2 && !pe_support(&s, &ih, e[1]));

    // Disabled → plaintext without touching the flag.
    s.apply_settings(SessionSettings {
        out_enc_policy: EncPolicy::Disabled,
        ..enabled()
    });
    let (_, enc3) = s.peer_link_up(&ih, e[2], false).unwrap();
    assert!(!enc3 && !pe_support(&s, &ih, e[2]));

    // SSL conns force `pe_disabled` upstream (`is_ssl` streams are
    // already encrypted) — policy Enabled is bypassed.
    s.apply_settings(enabled());
    let (_, enc4) = s.peer_link_up(&ih, e[3], true).unwrap();
    assert!(!enc4);
}

#[test]
fn pe_support_writeback_on_attach() {
    // `init_bt_handshake` (MSE done) → `pe_support = true`;
    // completed plaintext (`!m_encrypted`) → `pe_support = false`.
    // Both gated `is_outgoing() && pe_enabled`; an incoming conn
    // never toggles it.
    let mut s = session(enabled());
    let ih = s.add_torrent(torrent_v1(1, 2));
    let st = s.peer_list_state();
    let (ep_out, pid_out) = s
        .torrents
        .get_mut(&ih)
        .unwrap()
        .try_connect_peer(0, &st, &[])
        .0
        .into_dial()
        .unwrap();

    // fresh entries start `pe_support = false` — the first link-up
    // is the plaintext leg (optimistic set); the second probes MSE.
    assert!(!s.peer_link_up(&ih, ep_out, false).unwrap().1);
    // MSE probe leg: clear at link-up, re-armed on completed attach.
    assert!(s.peer_link_up(&ih, ep_out, false).unwrap().1);
    s.torrents.get_mut(&ih).unwrap().peer_connected_outgoing(
        ep_out,
        attach(true, true),
        0,
        7,
        false,
        Some(pid_out),
    );
    assert!(pe_support(&s, &ih, ep_out));

    // the MSE conn goes away; a completed plaintext conn drives
    // `pe_support` back to false.
    s.torrents
        .get_mut(&ih)
        .unwrap()
        .peer_disconnected_addr(ep_out, 1, false, false, 7);
    s.torrents.get_mut(&ih).unwrap().peer_connected_outgoing(
        ep_out,
        attach(true, false),
        1,
        8,
        false,
        None,
    );
    assert!(!pe_support(&s, &ih, ep_out));

    // an incoming attach never runs the `pe_support` write-back —
    // the gate is `is_outgoing() && pe_enabled` upstream.
    let ep_in = ep(9);
    {
        let t = s.torrents.get_mut(&ih).unwrap();
        let i = match t.peer_connected_incoming(ep_in, None, false) {
            bitdaemon_net::peer_list::NewConnVerdict::Attach(i) => i,
            _ => panic!("incoming conn must attach"),
        };
        t.peer_connected_attach(i, attach(false, true), 1, 9, false, false);
    }
    assert!(!pe_support(&s, &ih, ep_in));
}

#[test]
fn pe_support_rearmed_when_mse_completed() {
    // `init_bt_handshake` re-arms `pe_support` the moment PE4
    // completes — *before* the inner handshake — so a conn whose
    // encrypted portion finished but whose BT leg failed still
    // retries encrypted.
    let mut s = session(enabled());
    let ih = s.add_torrent(torrent_v1(1, 1));
    let st = s.peer_list_state();
    let e = s
        .torrents
        .get_mut(&ih)
        .unwrap()
        .try_connect_peer(0, &st, &[])
        .0
        .into_dial()
        .unwrap()
        .0;
    let _ = s.peer_link_up(&ih, e, false); // plaintext — sets pe_support
    assert!(s.peer_link_up(&ih, e, false).unwrap().1); // MSE probe

    // handshake failure *after* PE4 (mse_done) → re-armed.
    let fail = DialFail {
        severity: DisconnectSeverity::Failure,
        op: FailOp::Bittorrent,
        reason: "invalid_info_hash",
        io_kind: None,
        os_err: None,
        encrypted: true,
        rc4: true,
        mse_done: true,
        utp: false,
    };
    s.peer_dial_failed(&ih, e, &fail);
    assert!(pe_support(&s, &ih, e));

    // a pre-PE4 failure keeps the pessimistic clear.
    let fail2 = DialFail {
        mse_done: false,
        ..fail
    };
    let e2 = s
        .torrents
        .get_mut(&ih)
        .unwrap()
        .try_connect_peer(0, &st, &[])
        .0
        .into_dial()
        .map(|x| x.0);
    if let Some(e2) = e2 {
        if s.peer_link_up(&ih, e2, false).unwrap().1 {
            s.peer_dial_failed(&ih, e2, &fail2);
            assert!(!pe_support(&s, &ih, e2));
        }
    }
}

#[test]
fn dial_failure_counters_and_alert() {
    // `disconnect()` accounting: severity > normal → error_tcp /
    // error_outgoing / (encrypted → error_encrypted [+ error_rc4]);
    // peer_error → `error_peers` + `peer_error_alert` (not
    // `peer_disconnected`).
    let mut s = session(enabled());
    let ih = s.add_torrent(torrent_v1(1, 1));
    let st = s.peer_list_state();
    let e = s
        .torrents
        .get_mut(&ih)
        .unwrap()
        .try_connect_peer(0, &st, &[])
        .0
        .into_dial()
        .unwrap()
        .0;
    let _ = s.peer_link_up(&ih, e, false);
    let fail = DialFail {
        severity: DisconnectSeverity::PeerError,
        op: FailOp::Encryption,
        reason: "invalid_encryption_constant",
        io_kind: None,
        os_err: None,
        encrypted: true,
        rc4: true,
        mse_done: false,
        utp: false,
    };
    s.peer_dial_failed(&ih, e, &fail);
    let c = &s.counters;
    assert_eq!(c.value(Counter::DisconnectedPeers), 1);
    assert_eq!(c.value(Counter::ErrorTcpPeers), 1);
    assert_eq!(c.value(Counter::ErrorOutgoingPeers), 1);
    assert_eq!(c.value(Counter::ErrorIncomingPeers), 0);
    assert_eq!(c.value(Counter::ErrorEncryptedPeers), 1);
    assert_eq!(c.value(Counter::ErrorRc4Peers), 1);
    assert_eq!(c.value(Counter::ErrorPeers), 1);
    let alerts = s.pop_alerts();
    assert!(
        alerts.iter().any(|a| matches!(a, Alert::PeerError { .. })),
        "peer_error severity posts peer_error_alert: {alerts:?}"
    );
    assert!(
        !alerts
            .iter()
            .any(|a| matches!(a, Alert::PeerDisconnected { .. })),
        "peer_error severity does not post peer_disconnected: {alerts:?}"
    );
}

#[test]
fn inbound_failure_counts_incoming() {
    // an accept-side handshake failure decrements half-open and
    // runs the same `disconnect()` legs with `error_incoming_peers`
    // — posted even when the ih resolved to no torrent (upstream's
    // alert carries an invalid handle then).
    let mut s = session(enabled());
    s.counters.inc(Counter::NumPeersHalfOpen);
    let fail = DialFail {
        severity: DisconnectSeverity::PeerError,
        op: FailOp::Encryption,
        reason: "invalid_pad_size",
        io_kind: None,
        os_err: None,
        encrypted: true,
        rc4: false,
        mse_done: false,
        utp: false,
    };
    s.peer_conn_failed_inbound(None, ep(9), &fail);
    let c = &s.counters;
    assert_eq!(c.value(Counter::NumPeersHalfOpen), 0);
    assert_eq!(c.value(Counter::ErrorIncomingPeers), 1);
    assert_eq!(c.value(Counter::ErrorOutgoingPeers), 0);
    assert_eq!(c.value(Counter::ErrorEncryptedPeers), 1);
    assert_eq!(c.value(Counter::ErrorRc4Peers), 0);
    let alerts = s.pop_alerts();
    assert!(
        alerts.iter().any(|a| matches!(a, Alert::PeerError { .. })),
        "inbound peer_error posts the alert even unattached: {alerts:?}"
    );
}

#[test]
fn attached_teardown_counts_crypto_legs() {
    // `peer_connection::disconnect` on an *attached* conn — the
    // `error_encrypted_peers`/`error_rc4_peers` legs read the conn's
    // final `supports_encryption()`/`rc4_encrypted()` flags
    // (`peer_connection.cpp:4414-4422`), gated `severity > normal`.
    let mut s = session(enabled());
    let ih = s.add_torrent(torrent_v1(1, 0));
    let mut out = Vec::new();
    let gone = || {
        Some(PeerGone::Disconnected {
            severity: DisconnectSeverity::Failure,
            reason: "invalid_info_hash",
        })
    };
    let enc_conn = |rc4: bool| {
        let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
        c.supports_encryption = true;
        c.rc4_encrypted = rc4;
        c
    };

    // RC4-encrypted conn → both legs.
    s.peer_connected(&ih, 7, enc_conn(true), ep(9), &mut out);
    s.peer_disconnected(&ih, 7, gone(), &mut out);
    assert_eq!(s.counters.value(Counter::ErrorEncryptedPeers), 1);
    assert_eq!(s.counters.value(Counter::ErrorRc4Peers), 1);

    // plaintext-over-MSE (encrypted but !rc4) → only the encrypted
    // leg — upstream's `rc4_encrypted()` is the narrower flag.
    s.peer_connected(&ih, 8, enc_conn(false), ep(10), &mut out);
    s.peer_disconnected(&ih, 8, gone(), &mut out);
    assert_eq!(s.counters.value(Counter::ErrorEncryptedPeers), 2);
    assert_eq!(s.counters.value(Counter::ErrorRc4Peers), 1);

    // unencrypted conn → neither leg.
    s.peer_connected(
        &ih,
        9,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        ep(11),
        &mut out,
    );
    s.peer_disconnected(&ih, 9, gone(), &mut out);
    assert_eq!(s.counters.value(Counter::ErrorEncryptedPeers), 2);
    assert_eq!(s.counters.value(Counter::ErrorRc4Peers), 1);

    // a normal-severity close never runs the error legs even on an
    // RC4 conn.
    s.peer_connected(&ih, 10, enc_conn(true), ep(12), &mut out);
    s.peer_disconnected(
        &ih,
        10,
        Some(PeerGone::Disconnected {
            severity: DisconnectSeverity::Normal,
            reason: "eof",
        }),
        &mut out,
    );
    assert_eq!(s.counters.value(Counter::ErrorEncryptedPeers), 2);
    assert_eq!(s.counters.value(Counter::ErrorRc4Peers), 1);
}

#[test]
fn fast_reconnect_skips_cooldown_on_probe_failure() {
    // the MSE-probe dial arms `fast_reconnect` — `connection_closed`
    // leaves the rewound `last_connected` alone → the entry is
    // re-dialable at the same session_time (upstream retries a
    // failed MSE probe instantly as plaintext).
    let mut s = session(enabled());
    let ih = s.add_torrent(torrent_v1(1, 1));
    let st = s.peer_list_state();
    let t = s.torrents.get_mut(&ih).unwrap();
    let (e, _pid) = t.try_connect_peer(0, &st, &[]).0.into_dial().unwrap();
    let _ = s.peer_link_up(&ih, e, false); // plaintext — sets pe_support
    assert!(s.peer_link_up(&ih, e, false).unwrap().1); // MSE probe leg
    // a connect-level refusal on the probe → fast reconnect: the
    // entry is pickable again immediately.
    s.peer_dial_failed(
        &ih,
        e,
        &DialFail::connect_failed(std::io::ErrorKind::ConnectionRefused, None, false),
    );
    let t = s.torrents.get_mut(&ih).unwrap();
    assert_eq!(
        t.try_connect_peer(0, &st, &[]).0.into_dial().map(|x| x.0),
        Some(e)
    );
}

#[test]
fn allowed_enc_level_sanitized() {
    // `sanitize_settings`: values outside {1, 2, 3} clamp to
    // `pe_both` upstream.
    let mut s = Session::new();
    s.apply_settings(SessionSettings {
        allowed_enc_level: 7,
        ..Default::default()
    });
    assert_eq!(s.pe_settings.allowed_enc_level, enc_level::BOTH);
    s.apply_settings(SessionSettings {
        allowed_enc_level: enc_level::RC4,
        ..Default::default()
    });
    assert_eq!(s.pe_settings.allowed_enc_level, enc_level::RC4);
}

#[test]
fn peer_info_encryption_flags() {
    // `get_peer_info` — an encrypted conn reports exactly one of
    // `rc4_encrypted`/`plaintext_encrypted` by `m_rc4_encrypted`;
    // an unencrypted conn reports neither.
    let mut s = session(enabled());
    let ih = s.add_torrent(torrent_v1(1, 0));
    let mut out = Vec::new();
    let flags_of = |s: &Session, id: u64| {
        s.torrents[&ih]
            .peer_info(id, None, &s.settings)
            .unwrap()
            .flags
    };

    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.supports_encryption = true;
    c.rc4_encrypted = true;
    s.peer_connected(&ih, 7, c, ep(9), &mut out);
    let f = flags_of(&s, 7);
    assert!(f.has(PeerFlags::RC4_ENCRYPTED));
    assert!(!f.has(PeerFlags::PLAINTEXT_ENCRYPTED));

    let mut c = PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0);
    c.supports_encryption = true;
    s.peer_connected(&ih, 8, c, ep(10), &mut out);
    let f = flags_of(&s, 8);
    assert!(!f.has(PeerFlags::RC4_ENCRYPTED));
    assert!(f.has(PeerFlags::PLAINTEXT_ENCRYPTED));

    s.peer_connected(
        &ih,
        9,
        PeerConn::new(4, PeerPipeline::new(1000, 30, 250), 0),
        ep(11),
        &mut out,
    );
    let f = flags_of(&s, 9);
    assert!(!f.has(PeerFlags::RC4_ENCRYPTED));
    assert!(!f.has(PeerFlags::PLAINTEXT_ENCRYPTED));
}

#[test]
fn known_torrent_dual_index() {
    // `torrent_list` indexes every torrent under its v1 hash, its
    // truncated v2 hash, and both `req2` stream-key obfuscations —
    // a v2 wire hash resolves to the v1-keyed entry with
    // `protocol_v2` set.
    let mut s = session(enabled());
    let v1 = Sha1Hash::from_bytes(&[0x11; 20]).unwrap();
    let v2 = Sha256Hash::from_bytes(&[0x22; 32]).unwrap();
    let mut t = torrent_v1(0x11, 0);
    t.info_hashes = InfoHash::new(v1, v2);
    let ih = s.add_torrent(t);
    assert_eq!(ih, v1);

    let v2w = InfoHash::new(v1, v2).get(ProtocolVersion::V2);
    // v1 wire hash → v1 conn.
    let k = s.known_torrent(&v1).unwrap();
    assert_eq!(k.ih, v1);
    assert_eq!(k.wire_ih, v1);
    assert!(!k.protocol_v2 && k.has_v2);
    // truncated v2 wire hash → same torrent, `protocol_v2`.
    let k = s.known_torrent(&v2w).unwrap();
    assert_eq!(k.ih, v1);
    assert_eq!(k.wire_ih, v2w);
    assert!(k.protocol_v2);
    // MSE skey probing hits both versions.
    let k = s.known_torrent_by_skey(&stream_key_hash(&v1)).unwrap();
    assert_eq!(k.ih, v1);
    assert!(!k.protocol_v2);
    let k = s.known_torrent_by_skey(&stream_key_hash(&v2w)).unwrap();
    assert_eq!(k.ih, v1);
    assert!(k.protocol_v2);
    // unknown hashes resolve to nothing.
    let other = Sha1Hash::from_bytes(&[0x99; 20]).unwrap();
    assert!(s.known_torrent(&other).is_none());
    assert!(s.known_torrent_by_skey(&stream_key_hash(&other)).is_none());
}
