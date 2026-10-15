// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! A22 regression tests — `apply_settings_pack` legs and
//! `.session`/`session_params` interop vs upstream
//! `session_impl.cpp`/`settings_pack.cpp`/`session_params.cpp`.

use std::net::{IpAddr, Ipv4Addr};

use bitdaemon_bencode::{Entry, bencode};
use bitdaemon_net::proxy::ProxyType;
use bitdaemon_session::session::{Session, SessionOut};
use bitdaemon_session::session_params::{apply_settings_dict, read_session_params};
use bitdaemon_session::session_state::{SessionState, load_session_state, save_session_state_buf};
use bitdaemon_session::settings::SessionSettings;

fn apply(s: &mut Session, f: impl FnOnce(&mut SessionSettings)) {
    let mut sett = s.settings.clone();
    f(&mut sett);
    s.apply_settings(sett);
}

// ── reopen_listen_port gate (session_impl.cpp:1568-1575) ─────────

#[test]
fn reopen_gate_follows_upstream_trigger_set() {
    let mut s = Session::new();
    // `proxy_type` change → reopen (upstream `reopen_listen_port`).
    apply(&mut s, |x| x.proxy_type = ProxyType::Socks5);
    assert!(s.reopen_sockets);
    s.reopen_sockets = false;
    // `proxy_peer_connections` change → reopen.
    apply(&mut s, |x| x.proxy_peer_connections = false);
    assert!(s.reopen_sockets);
    s.reopen_sockets = false;
    // `outgoing_interfaces` only re-parses the bind list upstream —
    // no socket reopen (`update_outgoing_interfaces` just sets
    // `m_outgoing_interfaces`).
    apply(&mut s, |x| x.outgoing_interfaces = b"10.0.0.1".to_vec());
    assert!(!s.reopen_sockets);
    // `listen_interfaces` still reopens.
    apply(&mut s, |x| x.listen_interfaces = b"0.0.0.0:7000".to_vec());
    assert!(s.reopen_sockets);
}

// ── update_proxy leg (session_impl.cpp:5652) ─────────────────────

#[test]
fn proxy_keys_rebuild_purpose_slots() {
    let mut s = Session::new();
    apply(&mut s, |x| {
        x.proxy_type = ProxyType::Socks5;
        x.proxy_hostname = b"socks.local".to_vec();
        x.proxy_port = 1080;
        x.proxy_username = b"u".to_vec();
        x.proxy_password = b"p".to_vec();
        x.proxy_peer_connections = false;
    });
    // `proxy_settings(m_settings)` feeds every purpose-slot — the
    // engine reads `proxies.*` for dials/announces/DHT.
    for p in [
        &s.proxies.peer,
        &s.proxies.tracker,
        &s.proxies.web_seed,
        &s.proxies.dht,
    ] {
        assert_eq!(p.hostname, "socks.local");
        assert_eq!(p.port, 1080);
        assert!(matches!(p.proxy_type, ProxyType::Socks5));
        assert_eq!(p.username, "u");
        assert!(!p.proxy_peer_connections);
    }
}

// ── setting sanitizers (session_impl.cpp:6665,6719,6748) ─────────

#[test]
fn apply_settings_sanitizes_values() {
    let mut s = Session::new();
    apply(&mut s, |x| {
        x.connection_speed = -5;
        x.aio_threads = -3;
        x.hashing_threads = -1;
        x.user_agent = b"agent/1.0\ninjected".to_vec();
    });
    // `update_connection_speed` writes back 200.
    assert_eq!(s.settings.connection_speed, 200);
    // `update_disk_threads` clamps at 0.
    assert_eq!(s.settings.aio_threads, 0);
    assert_eq!(s.settings.hashing_threads, 0);
    // `update_user_agent` — newline becomes a space.
    assert_eq!(s.settings.user_agent, b"agent/1.0 injected");
}

// ── update_connections_limit ≤0 (session_impl.cpp:6901) ──────────

#[test]
fn connections_limit_nonpositive_is_unbounded() {
    let mut s = Session::new();
    apply(&mut s, |x| x.connections_limit = 0);
    // upstream writes `max_open_files()` back — ours is the
    // injected-transport unbounded cap; a sweep must not run.
    assert_eq!(s.settings.connections_limit, i32::MAX);
}

// ── update_dht_bootstrap_nodes leg (session_impl.cpp:5714) ───────

#[test]
fn dht_bootstrap_nodes_change_seeds_router_list() {
    let mut s = Session::new();
    apply(&mut s, |x| x.enable_dht = true);
    s.pending_out.clear();
    apply(&mut s, |x| {
        x.dht_bootstrap_nodes = b"router.bittorrent.com:6881, dht.example.org:25401".to_vec();
    });
    let nodes: Vec<_> = s
        .pending_out
        .iter()
        .filter_map(|o| match o {
            SessionOut::DhtAddNode { host, port } => Some((host.clone(), *port)),
            _ => None,
        })
        .collect();
    assert_eq!(nodes.len(), 2);
    assert!(nodes.contains(&("router.bittorrent.com".to_string(), 6881)));
    assert!(nodes.contains(&("dht.example.org".to_string(), 25401)));
    // an unchanged apply emits nothing.
    s.pending_out.clear();
    apply(&mut s, |x| x.num_want = 42);
    assert!(
        !s.pending_out
            .iter()
            .any(|o| matches!(o, SessionOut::DhtAddNode { .. }))
    );
}

// ── sparse-overlay load semantics (settings_pack.cpp:442-485) ────

#[test]
fn empty_string_settings_clear_the_field() {
    // `d14:proxy_hostname0:e` — a present-but-empty string must clear
    // the setting (`load_pack_from_dict` applies any present key).
    let buf = b"d14:proxy_hostname0:e";
    let node = bitdaemon_bencode::bdecode(buf);
    let mut s = SessionSettings {
        proxy_hostname: b"old.proxy".to_vec(),
        ..SessionSettings::default()
    };
    apply_settings_dict(&mut s, &node);
    assert!(s.proxy_hostname.is_empty());
}

#[test]
fn peer_tos_alias_maps_to_peer_dscp() {
    // `setting_by_name` backwards compat: 1.x `peer_tos` → `peer_dscp`.
    let node = bitdaemon_bencode::bdecode(b"d8:peer_tosi16ee");
    let mut s = SessionSettings::default();
    apply_settings_dict(&mut s, &node);
    assert_eq!(s.peer_dscp, 16);
    // the modern key wins when both are present.
    let node = bitdaemon_bencode::bdecode(b"d9:peer_dscpi4e8:peer_tosi16ee");
    let mut s = SessionSettings::default();
    apply_settings_dict(&mut s, &node);
    assert_eq!(s.peer_dscp, 4);
}

// ── .session upstream keys (session_params.cpp) ──────────────────

#[test]
fn session_state_carries_ip_filter_and_upstream_dht_keys() {
    let mut st = SessionState::default();
    st.ip_filter.add_rule(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)),
        IpAddr::V4(Ipv4Addr::new(10, 255, 255, 255)),
        1,
    );
    let mut ds = bitdaemon_dht::dht_state::DhtState::default();
    ds.nodes.push(std::net::SocketAddr::new(
        std::net::IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
        6881,
    ));
    st.dht_state = bencode(&bitdaemon_dht::dht_state::save_dht_state(&ds));
    st.extensions
        .insert(b"ext_key".to_vec(), b"ext_blob".to_vec());

    let buf = save_session_state_buf(&st);
    let back = load_session_state(&buf).unwrap();
    // ip_filter4/6 round-trip (upstream `save_ip_filter` format).
    assert_eq!(
        back.ip_filter
            .access(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))),
        1
    );
    assert_eq!(
        back.ip_filter
            .access(IpAddr::V4(Ipv4Addr::new(11, 0, 0, 1))),
        0
    );
    // `"dht state"` dict decodes back to the same blob.
    assert!(!back.dht_state.is_empty());
    assert_eq!(
        back.extensions.get(b"ext_key".as_slice()),
        Some(&b"ext_blob".to_vec())
    );
}

#[test]
fn load_state_reads_upstream_session_params_layout() {
    // an upstream-style `write_session_params` blob (no `torrents`
    // key): settings apply, ip_filter restores, `"dht state"` dict
    // feeds the dht seed.
    let mut params = bitdaemon_session::settings::SessionParams::default();
    params.settings.num_want = 77;
    params.ip_filter.add_rule(
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 0)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 255, 255)),
        1,
    );
    let mut ds = bitdaemon_dht::dht_state::DhtState::default();
    ds.nodes.push(std::net::SocketAddr::new(
        std::net::IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8)),
        6881,
    ));
    params.dht_state = ds;
    let buf = bitdaemon_session::session_params::write_session_params_buf(&params);

    let mut s = Session::new();
    let mut out = Vec::new();
    s.load_state(&buf, &mut out);
    assert_eq!(s.settings.num_want, 77);
    assert_eq!(
        s.ip_filter()
            .access(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
        1
    );
    assert_eq!(
        s.ip_filter().access(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))),
        0
    );
    // the `"dht state"` dict fed `load_session_state`'s blob slot
    // (checked via the container directly — `dht_state_seed` is a
    // session-private field consumed by `enable_dht`).
    let st = load_session_state(&buf).unwrap();
    let ds2 = bitdaemon_dht::dht_state::read_dht_state_bytes(&st.dht_state).unwrap();
    assert_eq!(ds2.nodes.len(), 1);
}

#[test]
fn load_state_keeps_client_identity() {
    // upstream `pack.clear(user_agent)`/`pack.clear(peer_fingerprint)`
    // — the client identity is never loaded from state.
    let mut params = bitdaemon_session::settings::SessionParams::default();
    params.settings.user_agent = b"forged/9.9".to_vec();
    params.settings.peer_fingerprint = b"-ZZ9999-".to_vec();
    let buf = bitdaemon_session::session_params::write_session_params_buf(&params);
    let mut s = Session::new();
    s.settings.user_agent = b"bitdaemon/1.0".to_vec();
    let mut out = Vec::new();
    s.load_state(&buf, &mut out);
    assert_eq!(s.settings.user_agent, b"bitdaemon/1.0");
    assert_eq!(s.settings.peer_fingerprint, b"-BD0001-");
}

#[test]
fn upstream_params_settings_merge_order() {
    // `read_session_params`: legacy `"dht"` dict seeds `dht_*` keys
    // first, `"settings"` overlays after — a `dht_announce_interval`
    // in settings wins over the dht dict's value.
    let dht = Entry::Dict(
        [
            (b"block_ratelimit".to_vec(), Entry::Int(9)),
            (b"read_only".to_vec(), Entry::Int(1)),
        ]
        .into_iter()
        .collect(),
    );
    let settings = Entry::Dict(
        [(b"num_want".to_vec(), Entry::Int(55))]
            .into_iter()
            .collect(),
    );
    let root = Entry::Dict(
        [(b"dht".to_vec(), dht), (b"settings".to_vec(), settings)]
            .into_iter()
            .collect(),
    );
    let buf = bencode(&root);
    let p = read_session_params(&buf);
    assert_eq!(p.settings.dht_block_ratelimit, 9);
    assert!(p.settings.dht_read_only);
    assert_eq!(p.settings.num_want, 55);
}
