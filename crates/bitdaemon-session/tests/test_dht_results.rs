//! Phase L — DHT result plumbing + port-map / listen / stats alert
//! posts (ADR-0023). These exercise the *session* surfaces that
//! upstream delivers asynchronously: without a bound DHT node the
//! calls are graceful no-ops, and the alert paths fire directly.

use bitdaemon_common::Sha1Hash;
use bitdaemon_session::alerts::{Alert, AlertCategory};
use bitdaemon_session::session::{Session, SessionOut};

fn session() -> Session {
    let mut s = Session::new();
    s.alerts.set_mask(AlertCategory::ALL);
    s
}

#[test]
fn dht_calls_without_node_are_noops() {
    let mut s = session();
    let ih = Sha1Hash::from_bytes(&[7u8; 20]).unwrap_or_default();
    // none of these should panic or queue work without a node.
    s.dht_get_peers(ih);
    s.dht_announce(ih, 6881);
    s.dht_get_item(ih);
    s.dht_get_item_mut([1u8; 32], b"salt".to_vec());
    s.dht_sample_infohashes("1.2.3.4:6881".parse().unwrap(), ih);
    s.dht_direct_request(
        "1.2.3.4:6881".parse().unwrap(),
        bitdaemon_bencode::Entry::Int(1),
        42,
    );
    // `dht_put_item` still returns the canonical target.
    let target = s.dht_put_item(b"4:spam");
    assert_eq!(
        target,
        bitdaemon_dht::item::item_target_id_immutable(b"4:spam")
    );
    // mutable put signs → target = sha1(pk ‖ salt).
    let sk = [9u8; 64];
    if let Some(t) = s.dht_put_item_mut([2u8; 32], &sk, Vec::new(), 1, b"4:eggs") {
        assert_eq!(
            t,
            bitdaemon_dht::item::item_target_id_mutable(&[], &[2u8; 32])
        );
    }
}

#[test]
fn portmap_handle_lifecycle() {
    let mut s = session();
    // `add_port_mapping` hands out a handle and queues the engine op.
    let h = s.add_port_mapping(1, 6881, 6881);
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(
        out.iter().any(|o| matches!(
            o,
            SessionOut::PortmapAdd {
                protocol: 1,
                external_port: 6881,
                local_port: 6881
            }
        )),
        "PortmapAdd out missing"
    );
    // a confirmed map binds the same handle and posts `portmap_alert`.
    s.on_portmap_success(0, 1, 6881, 6881, "192.168.1.1".into(), 0);
    assert!(s.pop_alerts().iter().any(|a| matches!(
        a,
        Alert::Portmap {
            mapping,
            transport: 0,
            protocol: 1,
            ext_port: 6881,
            ..
        } if *mapping == h
    )));
    // `delete_port_mapping` resolves the handle → PortmapDelete out.
    s.delete_port_mapping(h);
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(out.iter().any(|o| matches!(
        o,
        SessionOut::PortmapDelete {
            transport: 0,
            protocol: 1,
            external_port: 6881
        }
    )));
    // unknown handle → silent no-op.
    s.delete_port_mapping(9999);
    let mut out = Vec::new();
    s.tick(&mut out);
    assert!(
        !out.iter()
            .any(|o| matches!(o, SessionOut::PortmapDelete { .. }))
    );
}

#[test]
fn portmap_error_and_log_alerts() {
    let mut s = session();
    s.on_portmap_error(1, 3, "router".into(), "denied".into());
    s.portmap_log(1, "router".into(), "soap send".into());
    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(|a| matches!(
        a,
        Alert::PortmapError {
            mapping: 3,
            transport: 1,
            ..
        }
    )));
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::PortmapLog { transport: 1, .. }))
    );
}

#[test]
fn listen_and_incoming_alerts() {
    let mut s = session();
    s.on_listen_succeeded("0.0.0.0:6881".into(), 0);
    s.on_listen_failed("0.0.0.0:6882".into(), "bind".into(), "in use".into(), 0);
    s.on_incoming_connection("1.2.3.4:5".parse().unwrap(), 0);
    let alerts = s.pop_alerts();
    assert!(alerts.iter().any(
        |a| matches!(a, Alert::ListenSucceeded { remote, kind: 0 } if remote == "0.0.0.0:6881")
    ));
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::ListenFailed { op, .. } if op == "bind"))
    );
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::IncomingConnection { .. }))
    );
}

#[test]
fn external_ip_posts_on_consensus_change() {
    let mut s = session();
    // IpVoter elects once enough distinct sources agree — one router
    // vote is already a new external address (first consensus).
    for i in 0..3u8 {
        s.vote_external(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, i)),
            bitdaemon_net::ip_voter::IpSourceFlags::SOURCE_TRACKER,
            &std::net::IpAddr::V4(std::net::Ipv4Addr::new(198, 51, 100, i)),
        );
    }
    let alerts = s.pop_alerts();
    assert!(
        alerts.iter().any(|a| matches!(a, Alert::ExternalIp { .. })),
        "no ExternalIp alert posted"
    );
}

#[test]
fn post_session_stats_posts_header_then_stats() {
    let mut s = session();
    s.post_session_stats();
    s.post_session_stats();
    let alerts = s.pop_alerts();
    let header = alerts
        .iter()
        .filter(|a| matches!(a, Alert::SessionStatsHeader {}))
        .count();
    let stats = alerts
        .iter()
        .filter(|a| matches!(a, Alert::SessionStats { .. }))
        .count();
    assert_eq!(header, 1, "header posts once");
    assert_eq!(stats, 2);
}

#[test]
fn pause_resume_post_alerts() {
    use bitdaemon_core::add_torrent_params::AddTorrentParams;
    use bitdaemon_core::torrent_info::{LoadTorrentLimits, TorrentInfo};
    let mut s = session();
    let mut info = Vec::new();
    info.extend(b"d6:lengthi24e4:name4:test12:piece lengthi16384e6:pieces20:");
    info.extend([0x77u8; 20]);
    info.extend(b"e");
    let node = bitdaemon_bencode::bdecode_checked(&info).unwrap();
    let ti = TorrentInfo::from_info_node(&node, &LoadTorrentLimits::default()).unwrap();
    let want_ih = ti.info_hash_v1().unwrap();
    let atp = AddTorrentParams {
        ti: Some(std::sync::Arc::new(ti)),
        ..Default::default()
    };
    let mut out = Vec::new();
    let ih = s
        .add_torrent_params(&atp, Default::default(), &mut out)
        .unwrap();
    assert_eq!(ih, want_ih);
    s.pop_alerts();
    // newly-added default-flagged torrents start paused+auto-managed
    // → resume first (transition), then pause (transition).
    s.set_paused(&ih, false, &mut out);
    s.set_paused(&ih, true, &mut out);
    let alerts = s.pop_alerts();
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::TorrentPaused { ih: h } if *h == ih))
    );
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, Alert::TorrentResumed { ih: h } if *h == ih))
    );
}
