// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later
//
//! A27 regressions — engine/session glue:
//! * `run()` must not drain the alert queue (`tick` no longer calls
//!   `pop_alerts` — upstream leaves alerts queued for the caller).
//! * `SessionHandle::add_torrent_params` routes its `SessionOut`s to
//!   `pending_out` instead of dropping them.
//! * the acceptor pumps each conn's handshake on its own task — a
//!   silent peer cannot head-of-line block later accepts.
//! * completion queues (`connect_rx` here) are served immediately by
//!   `run`, not batched to `tick_interval`.

use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bitdaemon::engine::Engine;
use bitdaemon_common::info_hash::InfoHash;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_core::add_torrent_params::AddTorrentParams;
use bitdaemon_net::peer_wire::write_handshake;
use bitdaemon_net::transport::{AcceptFut, BoxedPeerStream, PeerAcceptor, PeerConnector};
use bitdaemon_session::alerts::{Alert, AlertCategory};
use bitdaemon_session::session::SessionOut;
use bitdaemon_session::session_handle::{SessionHandle, lock};
use bitdaemon_session::torrent::Torrent;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
struct NoopConnector;

impl PeerConnector for NoopConnector {
    fn connect(
        self: Arc<Self>,
        _addr: SocketAddr,
    ) -> Pin<
        Box<dyn std::future::Future<Output = bitdaemon_net::Result<BoxedPeerStream>> + Send + Sync>,
    > {
        Box::pin(async { Err(std::io::Error::other("noop connector").into()) })
    }
}

fn ep(a: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, a)), 6881)
}

/// `run()` must leave posted alerts in the queue — upstream's
/// `alert_manager` only drains on the user's `pop_alerts`.
#[tokio::test(flavor = "current_thread")]
async fn run_leaves_alerts_queued() {
    let ih = Sha1Hash::from_bytes(&[7; 20]).unwrap();
    let eng = Engine::new(Arc::new(NoopConnector), None);
    let handle = SessionHandle::from_shared(eng.session.clone());
    handle.set_alert_mask(AlertCategory::from_bits(u32::MAX));
    let t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    lock(&eng.session).add_torrent(t);

    let eng = eng;
    let run = tokio::spawn(async move {
        let mut eng = eng;
        eng.run(tokio::time::sleep(Duration::from_millis(1600)), 0)
            .await;
    });
    run.await.unwrap();

    let alerts = handle.pop_alerts();
    assert!(
        alerts.iter().any(
            |a| matches!(a, Alert::TorrentAdded(i) | Alert::AddTorrent { ih: i, .. } if *i == ih)
        ),
        "tick-posted alerts must survive run(); got {alerts:?}"
    );
}

/// `add_torrent_params` on the handle queues its `SessionOut`s on
/// `pending_out` (upstream runs the add on the network thread — the
/// effects happen there, they must not be dropped).
#[tokio::test(flavor = "current_thread")]
async fn add_torrent_params_routes_effects() {
    let ih = Sha1Hash::from_bytes(&[6; 20]).unwrap();
    let eng = Engine::new(Arc::new(NoopConnector), None);
    let handle = SessionHandle::from_shared(eng.session.clone());

    let atp = AddTorrentParams {
        info_hashes: InfoHash::from_v1(ih),
        dht_nodes: vec![(b"router.bittorrent.com".to_vec(), 6881)],
        flags: bitdaemon_common::flags::TorrentFlags::PAUSED,
        ..Default::default()
    };
    assert!(handle.add_torrent_params(&atp).is_some());

    let ses = lock(&eng.session);
    assert!(
        ses.pending_out
            .iter()
            .any(|o| matches!(o, SessionOut::DhtAddNode { host, port } if host == "router.bittorrent.com" && *port == 6881)),
        "DhtAddNode must be queued, not dropped"
    );
}

#[derive(Debug)]
struct QueueAcceptor(tokio::sync::Mutex<VecDeque<(BoxedPeerStream, Option<SocketAddr>)>>);

impl PeerAcceptor for QueueAcceptor {
    fn accept(self: Arc<Self>) -> AcceptFut {
        let me = self.clone();
        Box::pin(async move {
            loop {
                if let Some(pair) = me.0.lock().await.pop_front() {
                    return Ok(pair);
                }
                tokio::task::yield_now().await;
            }
        })
    }
}

/// A silent inbound conn holds `handshake_timeout`; the accept loop
/// must keep serving — the second conn attaches well before the
/// timeout (upstream drives each `peer_connection` handshake async).
#[tokio::test(flavor = "current_thread")]
async fn acceptor_handshakes_dont_head_of_line() {
    let ih = Sha1Hash::from_bytes(&[5; 20]).unwrap();
    let (silent_mine, _silent_theirs) = tokio::io::duplex(64 * 1024);
    let (good_mine, mut good_theirs) = tokio::io::duplex(64 * 1024);
    let acc = Arc::new(QueueAcceptor(tokio::sync::Mutex::new(VecDeque::from([
        (Box::new(silent_mine) as BoxedPeerStream, Some(ep(1))),
        (Box::new(good_mine) as BoxedPeerStream, Some(ep(2))),
    ]))));

    let mut eng = Engine::new(Arc::new(NoopConnector), None);
    {
        let mut ses = lock(&eng.session);
        ses.apply_settings(bitdaemon_session::settings::SessionSettings {
            // huge bounds: if the accept loop serialized, the test
            // could only pass after this timeout.
            handshake_timeout: Duration::from_secs(600),
            tick_interval: Duration::from_secs(600),
            ..Default::default()
        });
    }
    let t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    lock(&eng.session).add_torrent(t);
    eng.start_acceptor_at(acc, None, 0);

    // remote peer: answer the responder handshake.
    let remote_pid = bitdaemon_common::peer_id::generate_peer_id(b"-qB4470-");
    let hs = write_handshake(&ih, &remote_pid, false, false, false, false);
    tokio::spawn(async move {
        // responder reads first — send our handshake, then drain
        // theirs + the extended handshake.
        let _ = good_theirs.write_all(&hs).await;
        let _ = good_theirs.flush().await;
        let mut buf = [0u8; 512];
        let _ = tokio::time::timeout(Duration::from_millis(300), good_theirs.read(&mut buf)).await;
        // keep the stream alive past attach.
        tokio::time::sleep(Duration::from_secs(10)).await;
    });

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let n = lock(&eng.session)
            .torrents
            .get(&ih)
            .map_or(0, |t| t.peers.len());
        if n == 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "second conn never attached — accept loop is head-of-line blocked"
        );
        eng.tick(Instant::now()).await;
        tokio::task::yield_now().await;
    }
}

/// `connect_rx` completions are applied immediately by `run()` —
/// a dial landing between ticks must attach without waiting for
/// `tick_interval` (upstream's io_context runs the handler at once).
#[tokio::test(flavor = "current_thread")]
async fn dial_completion_attaches_off_tick() {
    let ih = Sha1Hash::from_bytes(&[4; 20]).unwrap();
    let (mine, theirs) = tokio::io::duplex(64 * 1024);
    #[derive(Debug)]
    struct OneShot(tokio::sync::Mutex<Option<tokio::io::DuplexStream>>);
    impl PeerConnector for OneShot {
        fn connect(
            self: Arc<Self>,
            _addr: SocketAddr,
        ) -> Pin<
            Box<
                dyn std::future::Future<Output = bitdaemon_net::Result<BoxedPeerStream>>
                    + Send
                    + Sync,
            >,
        > {
            let me = self.clone();
            Box::pin(async move {
                me.0.lock()
                    .await
                    .take()
                    .map(|s| -> BoxedPeerStream { Box::new(s) })
                    .ok_or_else(|| std::io::Error::other("no peer queued").into())
            })
        }
    }

    let mut eng = Engine::new(Arc::new(OneShot(tokio::sync::Mutex::new(Some(mine)))), None);
    {
        let mut ses = lock(&eng.session);
        ses.apply_settings(bitdaemon_session::settings::SessionSettings {
            // 10-minute tick: attach can only come from the
            // immediate `connect_rx` arm.
            tick_interval: Duration::from_secs(600),
            torrent_connect_boost: 0,
            ..Default::default()
        });
    }
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_swarm_peer(
        ep(3),
        bitdaemon_common::flags::PeerSourceFlags::TRACKER,
        &mut Vec::new(),
    );
    lock(&eng.session).add_torrent(t);

    // scripted remote: real responder handshake, then stay alive.
    let remote = tokio::spawn(async move {
        let mut boxed: BoxedPeerStream = Box::new(theirs);
        let pid = bitdaemon_common::PeerId::default();
        let _ = bitdaemon_net::bt_peer_conn::run_handshake_responder(
            &mut boxed,
            &pid,
            false,
            false,
            |ih| {
                Some(bitdaemon_net::bt_peer_conn::KnownTorrent {
                    ih: *ih,
                    wire_ih: *ih,
                    protocol_v2: false,
                    has_v2: false,
                    v2_ih: None,
                    valid_metadata: true,
                })
            },
        )
        .await;
        tokio::time::sleep(Duration::from_secs(10)).await;
        boxed
    });
    let _ = &remote;

    let session = eng.session.clone();
    eng.spawn_dial(ih, ep(3), None);
    let mut eng = eng;
    let run = tokio::spawn(async move {
        eng.run(tokio::time::sleep(Duration::from_secs(8)), 0).await;
    });

    // poll for the attach — must happen in << tick_interval.
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        if lock(&session)
            .torrents
            .get(&ih)
            .is_some_and(|t| !t.peers.is_empty())
        {
            break;
        }
        if Instant::now() >= deadline {
            let mut ses = lock(&session);
            let alerts = ses.alerts.get_all();
            let t = ses.torrents.get(&ih).unwrap();
            panic!(
                "dial completion was not applied off-tick — peers={} connecting={} half_open={} alerts={alerts:?}",
                t.peers.len(),
                t.num_connecting,
                ses.counters
                    .value(bitdaemon_session::counters::Counter::NumPeersHalfOpen),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    run.abort();
}
