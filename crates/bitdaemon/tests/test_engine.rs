// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `Engine` e2e — scripted duplex transport: dial → handshake →
//! PeerConn registered → bitfield flows in.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use bitdaemon::engine::Engine;
use bitdaemon_common::types::StorageIndex;
use bitdaemon_common::{Bitfield, Sha1Hash};
use bitdaemon_net::peer_wire::{PeerMessage, write_handshake};
use bitdaemon_net::transport::{BoxedPeerStream, PeerConnector};
use bitdaemon_session::torrent::Torrent;
use tokio::io::{AsyncWriteExt, DuplexStream};

#[derive(Debug)]
struct DuplexConnector(tokio::sync::Mutex<Option<DuplexStream>>);

impl PeerConnector for DuplexConnector {
    fn connect(
        self: Arc<Self>,
        _addr: SocketAddr,
    ) -> Pin<
        Box<dyn std::future::Future<Output = bitdaemon_net::Result<BoxedPeerStream>> + Send + Sync>,
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

fn ep() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 6881)
}

#[tokio::test(flavor = "current_thread")]
async fn connect_and_receive_bitfield() {
    let ih = Sha1Hash::from_bytes(&[9; 20]).unwrap();
    let (mine, theirs) = tokio::io::duplex(64 * 1024);
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(Some(mine))));

    let mut eng = Engine::new(conn, None);
    let t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    eng.session.add_torrent(t);

    // scripted remote: handshake + bitfield(0,2) — keep the stream
    // alive after writes or the engine sees a disconnect.
    let remote_pid = eng.session.peer_id; // any id works; reuse ours
    let mut boxed: BoxedPeerStream = Box::new(theirs);
    let hs = write_handshake(&ih, &remote_pid, false, false, false, false);
    boxed.write_all(&hs).await.unwrap();
    let mut buf = [0u8; 68];
    let dial = eng.connect_peer(&ih, ep(), false);
    let (res, _) = tokio::join!(
        dial,
        tokio::io::AsyncReadExt::read_exact(&mut boxed, &mut buf)
    );
    let peer = res.unwrap();
    let mut bf = Bitfield::with_size(4);
    bf.set_bit(0);
    bf.set_bit(2);
    bitdaemon_net::bt_peer_conn::write_message(
        &mut boxed,
        &PeerMessage::Bitfield(bf.bytes().to_vec().into()),
    )
    .await
    .unwrap();

    // pump: reader delivers the bitfield → session routes
    tokio::task::yield_now().await;
    let _ = eng.tick(std::time::Instant::now()).await;
    let t = eng.session.torrents.get(&ih).unwrap();
    let c = t.peers.get(&peer).unwrap();
    assert!(c.peer_pieces.get_bit(0) && c.peer_pieces.get_bit(2));
}

#[tokio::test(flavor = "current_thread")]
async fn connect_pump_lists_candidates() {
    let ih = Sha1Hash::from_bytes(&[8; 20]).unwrap();
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(None)));
    let mut eng = Engine::new(conn, None);
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_swarm_peer(
        ep(),
        bitdaemon_common::flags::PeerSourceFlags::TRACKER,
        true,
    );
    eng.session.add_torrent(t);
    let st = bitdaemon_net::peer_list::PeerListState::default();
    let dialing = eng.connect_pump(0, &st, 10);
    assert_eq!(dialing, vec![(ih, ep())]);
}

#[tokio::test(flavor = "current_thread")]
async fn accept_inbound_handshake() {
    let ih = Sha1Hash::from_bytes(&[2; 20]).unwrap();
    let (mine, theirs) = tokio::io::duplex(64 * 1024);
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(None)));
    let mut eng = Engine::new(conn, None);
    eng.session.add_torrent(Torrent::new(
        ih,
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    ));

    let pid = eng.session.peer_id;
    let mut boxed: BoxedPeerStream = Box::new(theirs);
    let hs = write_handshake(&ih, &pid, false, false, false, false);
    let accept = eng.accept_stream(ep(), Box::new(mine));
    let (peer, _) = tokio::join!(accept, boxed.write_all(&hs));
    let peer = peer.unwrap().expect("known ih");
    // our reply handshake went out
    let mut buf = [0u8; 68];
    tokio::io::AsyncReadExt::read_exact(&mut boxed, &mut buf)
        .await
        .unwrap();
    assert!(eng.session.torrents[&ih].peers.contains_key(&peer));
}

#[derive(Debug)]
struct MapResolver;

impl bitdaemon_net::resolver::Resolver for MapResolver {
    fn resolve(
        &self,
        host: &str,
        _flags: bitdaemon_net::resolver::ResolveFlags,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<IpAddr>>> + Send + '_>> {
        let host = host.to_string();
        Box::pin(async move {
            if host == "tracker.example" {
                Ok(vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9))])
            } else {
                Err(std::io::Error::other("nxdomain"))
            }
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn http_announce_via_resolver() {
    let ih = Sha1Hash::from_bytes(&[7; 20]).unwrap();
    let (mine, theirs) = tokio::io::duplex(64 * 1024);
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(Some(mine))));

    let mut eng = Engine::new(conn, None);
    eng.set_resolver(Arc::new(MapResolver));
    eng.set_listen_port(6881);
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.add_tracker("http://tracker.example:6969/announce".into(), 0);
    eng.session.add_torrent(t);

    // scripted HTTP tracker: read the GET, answer compact peers.
    let mut srv: BoxedPeerStream = Box::new(theirs);
    let responder = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 2048];
        let n = srv.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("GET /announce?"));
        assert!(req.contains("Host: tracker.example:6969"));
        let peer = [10, 0, 0, 42, 26, 225]; // 10.0.0.42:6881
        let body = format!("d8:completei1e10:incompletei0e5:peers6:{}e", {
            let _ = &peer;
            String::from_utf8_lossy(&peer).into_owned()
        });
        let _ = body; // peers is binary — build raw
        let mut b = b"d8:completei1e10:incompletei0e5:peers6:".to_vec();
        b.extend_from_slice(&peer);
        b.push(b'e');
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", b.len());
        srv.write_all(head.as_bytes()).await.unwrap();
        srv.write_all(&b).await.unwrap();
        // keep alive
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });

    // force the announce due + run engine tick
    let now = std::time::Instant::now();
    eng.tick(now).await;
    // give the spawned announce task a slot to run
    for _ in 0..20 {
        tokio::task::yield_now().await;
        eng.tick(now).await;
        if eng.session.torrents[&ih].peer_list.num_peers() > 0 {
            break;
        }
    }
    responder.await.unwrap();
    assert_eq!(eng.session.torrents[&ih].peer_list.num_peers(), 1);
}
