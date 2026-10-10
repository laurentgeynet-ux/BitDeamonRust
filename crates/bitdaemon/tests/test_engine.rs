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
use bitdaemon_session::session_handle::lock;
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
    lock(&eng.session).add_torrent(t);

    // scripted remote: handshake + bitfield(0,2) — keep the stream
    // alive after writes or the engine sees a disconnect.
    let remote_pid = lock(&eng.session).peer_id; // any id works; reuse ours
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
    let ses = lock(&eng.session);
    let t = ses.torrents.get(&ih).unwrap();
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
    lock(&eng.session).add_torrent(t);
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
    lock(&eng.session).add_torrent(Torrent::new(
        ih,
        4,
        128 * 1024,
        Bitfield::with_size(4),
        StorageIndex(0),
    ));

    let pid = lock(&eng.session).peer_id;
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
    assert!(lock(&eng.session).torrents[&ih].peers.contains_key(&peer));
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
    lock(&eng.session).add_torrent(t);

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
        if lock(&eng.session).torrents[&ih].peer_list.num_peers() > 0 {
            break;
        }
    }
    responder.await.unwrap();
    assert_eq!(lock(&eng.session).torrents[&ih].peer_list.num_peers(), 1);
}

// ---------- UDP pump → DHT ----------

use tokio::sync::mpsc as tmpsc;

#[derive(Debug)]
struct ChanDatagram {
    inbound: tokio::sync::Mutex<tmpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>,
    inbound_tx: tmpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    sent: tokio::sync::Mutex<tmpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)>>,
    sent_tx: tmpsc::UnboundedSender<(Vec<u8>, SocketAddr)>,
}

impl ChanDatagram {
    fn new() -> Self {
        let (itx, irx) = tmpsc::unbounded_channel();
        let (stx, srx) = tmpsc::unbounded_channel();
        Self {
            inbound: tokio::sync::Mutex::new(irx),
            inbound_tx: itx,
            sent: tokio::sync::Mutex::new(srx),
            sent_tx: stx,
        }
    }
}

impl bitdaemon_net::transport::DatagramSocket for ChanDatagram {
    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
        Box::pin(async move {
            let n = buf.len();
            let _ = self.sent_tx.send((buf.to_vec(), target));
            Ok(n)
        })
    }
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> bitdaemon_net::transport::BoxFut<'a, (usize, SocketAddr)> {
        Box::pin(async move {
            let mut rx = self.inbound.lock().await;
            match rx.recv().await {
                Some((from, data)) => {
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    Ok((n, from))
                }
                None => Err(std::io::Error::other("closed")),
            }
        })
    }
    fn local_addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6881)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dht_ping_round_trip() {
    let dg = Arc::new(ChanDatagram::new());
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(None)));
    let mut eng = Engine::new(conn, Some(dg.clone()));
    lock(&eng.session).enable_dht(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        None,
        bitdaemon_dht::dht_settings::DhtSettings::default(),
    );
    eng.start_udp_pump();

    // a remote node pings our DHT
    let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)), 51413);
    let ping = b"d1:ad2:id20:aaaaaaaaaaaaaaaaaaaae1:q4:ping1:t2:aa1:y1:qe";
    dg.inbound_tx.send((from, ping.to_vec())).unwrap();

    for _ in 0..30 {
        tokio::task::yield_now().await;
        eng.tick(std::time::Instant::now()).await;
        if let Ok((data, to)) = dg.sent.lock().await.try_recv() {
            let s = String::from_utf8_lossy(&data);
            assert_eq!(to, from);
            assert!(s.contains("1:y1:r"), "response: {s}");
            return;
        }
    }
    panic!("no dht response routed");
}

#[tokio::test]
async fn spawn_dial_connects_peer() {
    let ih = Sha1Hash::from_bytes(&[42; 20]).unwrap();
    let (mine, theirs) = tokio::io::duplex(64 * 1024);
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(Some(mine))));
    let mut eng = Engine::new(conn, None);
    lock(&eng.session).add_torrent(Torrent::new(
        ih,
        1,
        16384,
        Bitfield::with_size(1),
        StorageIndex(0),
    ));
    let ep = ep();
    eng.spawn_dial(ih, ep, false);

    // scripted remote: answer our handshake + drain the ext handshake
    let remote = tokio::spawn(async move {
        let mut boxed: BoxedPeerStream = Box::new(theirs);
        let pid = bitdaemon_common::PeerId::default();
        let _ = bitdaemon_net::bt_peer_conn::run_handshake_responder(
            &mut boxed,
            &pid,
            false,
            false,
            |_| true,
        )
        .await;
        let mut tmp = [0u8; 512];
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            tokio::io::AsyncReadExt::read(&mut boxed, &mut tmp),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        boxed
    });
    for _ in 0..100 {
        eng.tick(std::time::Instant::now()).await;
        if !lock(&eng.session).torrents[&ih].peers.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(lock(&eng.session).torrents[&ih].peers.len(), 1);
    remote.abort();
}

#[tokio::test]
async fn acceptor_pump_registers_peer() {
    use bitdaemon_net::transport::PeerAcceptor;

    #[derive(Debug)]
    struct Acc(tokio::sync::Mutex<Option<DuplexStream>>);
    impl PeerAcceptor for Acc {
        fn accept(self: Arc<Self>) -> bitdaemon_net::transport::AcceptFut {
            let me = self.clone();
            Box::pin(async move {
                let s =
                    me.0.lock()
                        .await
                        .take()
                        .ok_or(bitdaemon_net::NetError::NoTransport("done"))?;
                Ok((Box::new(s) as BoxedPeerStream, None))
            })
        }
    }

    let ih = Sha1Hash::from_bytes(&[77; 20]).unwrap();
    let (mine, theirs) = tokio::io::duplex(64 * 1024);
    let mut eng = Engine::new(
        Arc::new(DuplexConnector(tokio::sync::Mutex::new(None))),
        None,
    );
    lock(&eng.session).add_torrent(Torrent::new(
        ih,
        1,
        16384,
        Bitfield::with_size(1),
        StorageIndex(0),
    ));
    eng.start_acceptor(Arc::new(Acc(tokio::sync::Mutex::new(Some(mine)))));

    // remote dials: initiator handshake
    let remote = tokio::spawn(async move {
        let mut b: BoxedPeerStream = Box::new(theirs);
        let pid = bitdaemon_common::PeerId::default();
        let _ = bitdaemon_net::bt_peer_conn::run_handshake(&mut b, &ih, &pid, false, false).await;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        b
    });
    for _ in 0..100 {
        eng.tick(std::time::Instant::now()).await;
        if !lock(&eng.session).torrents[&ih].peers.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(lock(&eng.session).torrents[&ih].peers.len(), 1);
    remote.abort();
}

#[tokio::test]
async fn rate_limiter_throttles() {
    use bitdaemon::engine::RateLimiter;
    use std::sync::Arc;
    let rl = Arc::new(RateLimiter::new(100)); // 100 B/s
    rl.acquire(60).await;
    // 40 left — this must wait for a refill
    let rl2 = rl.clone();
    let h = tokio::spawn(async move {
        rl2.acquire(80).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(!h.is_finished());
    rl.refill(1000); // +100 tokens
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(h.is_finished());
}

#[tokio::test]
async fn rate_limiter_unlimited() {
    use bitdaemon::engine::RateLimiter;
    let rl = RateLimiter::new(0);
    rl.acquire(1 << 30).await; // instant
}

#[tokio::test(flavor = "current_thread")]
async fn natpmp_map_and_ip_vote() {
    let dg = Arc::new(ChanDatagram::new());
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(None)));
    let mut eng = Engine::new(conn, Some(dg.clone()));
    let gw: SocketAddr = "192.168.0.1:5351".parse().unwrap();
    eng.start_natpmp(gw, 6881, 7200).await;

    // 3 datagrams sent: tcp map, udp map, ip query
    for _ in 0..3 {
        let (data, to) = dg.sent.lock().await.try_recv().expect("map request");
        assert_eq!(to, gw);
        assert_eq!(data[0], 0);
    }

    // gateway answers the TCP map + public IP
    let mut map = [0u8; 16];
    map[1] = 2; // tcp op
    map[8..10].copy_from_slice(&6881u16.to_be_bytes());
    map[10..12].copy_from_slice(&44123u16.to_be_bytes());
    map[12..16].copy_from_slice(&7200u32.to_be_bytes());
    let mut ipq = [0u8; 12];
    ipq[8..12].copy_from_slice(&[203, 0, 113, 42]);
    eng.start_udp_pump();
    dg.inbound_tx.send((gw, map.to_vec())).unwrap();
    dg.inbound_tx.send((gw, ipq.to_vec())).unwrap();
    tokio::task::yield_now().await;
    eng.tick(std::time::Instant::now()).await;

    let n = eng.natpmp.as_ref().expect("natpmp state");
    assert_eq!(n.mappings[0].2, 44123); // external tcp port
    assert_eq!(
        lock(&eng.session).ip_voter.external_address(),
        Some(std::net::IpAddr::from([203, 0, 113, 42]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn upnp_map_cycle() {
    use tokio::io::AsyncReadExt;
    // one stream per POST: GetExternalIPAddress, add TCP, add UDP
    let mut server_ends = Vec::new();
    let mut client_ends = Vec::new();
    for _ in 0..3 {
        let (c, s) = tokio::io::duplex(64 * 1024);
        client_ends.push(c);
        server_ends.push(s);
    }
    #[derive(Debug)]
    struct Q(tokio::sync::Mutex<Vec<DuplexStream>>);
    impl PeerConnector for Q {
        fn connect(
            self: Arc<Self>,
            _a: SocketAddr,
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
                    .pop()
                    .map(|s| -> BoxedPeerStream { Box::new(s) })
                    .ok_or_else(|| std::io::Error::other("empty").into())
            })
        }
    }
    client_ends.reverse();
    let conn = Arc::new(Q(tokio::sync::Mutex::new(client_ends)));

    let mut eng = Engine::new(conn, None);
    eng.portmap_add_device(
        bitdaemon_net::upnp::RootDevice {
            control_url: "http://192.168.0.1:49152/ctl/IPConn".into(),
            service_namespace: "urn:schemas-upnp-org:service:WANIPConnection:1".into(),
            ..Default::default()
        },
        6881,
        "bitdaemon".into(),
    );

    // serve 3 SOAP requests in order
    let ok_xml = "HTTP/1.1 200 OK\r\nContent-Length: 24\r\nContent-Type: text/xml\r\n\r\n<u>ok</u>               ";
    let ip_xml = "HTTP/1.1 200 OK\r\nContent-Length: 87\r\nContent-Type: text/xml\r\n\r\n<NewExternalIPAddress>203.0.113.9</NewExternalIPAddress>      ";
    tokio::spawn(async move {
        let bodies = [ip_xml, ok_xml, ok_xml];
        for (mut s, b) in server_ends.into_iter().zip(bodies) {
            let mut req = Vec::new();
            let mut tmp = [0u8; 2048];
            // read until end of SOAP body (headers contain
            // Content-Length but simplest: read until "</")
            loop {
                let n = s.read(&mut tmp).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                req.extend_from_slice(&tmp[..n]);
                if req.windows(9).any(|w| w == b"</s:Body>" || w == b"</Body>") {
                    break;
                }
            }
            s.write_all(b.as_bytes()).await.unwrap();
        }
    });

    for _ in 0..50 {
        tokio::task::yield_now().await;
        eng.tick(std::time::Instant::now()).await;
        if lock(&eng.session).ip_voter.external_address().is_some() {
            break;
        }
    }
    assert_eq!(
        lock(&eng.session).ip_voter.external_address(),
        Some(std::net::IpAddr::from([203, 0, 113, 9]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn per_torrent_rate_limiters() {
    // `torrent::set_upload_limit` → a per-ih `RateLimiter` the
    // writer pump acquires on payload sends.
    let ih = Sha1Hash::from_bytes(&[6; 20]).unwrap();
    let conn = Arc::new(DuplexConnector(tokio::sync::Mutex::new(None)));
    let mut eng = Engine::new(conn, None);
    let mut t = Torrent::new(ih, 4, 128 * 1024, Bitfield::with_size(4), StorageIndex(0));
    t.set_upload_limit(10);
    t.set_download_limit(20);
    lock(&eng.session).add_torrent(t);

    let _ = eng.tick(std::time::Instant::now()).await;
    let ul = eng.torrent_up_limiters.get(&ih).unwrap().clone();
    let dl = eng.torrent_down_limiters.get(&ih).unwrap().clone();

    // 10 B/s bucket starts full; drain then block until refill
    ul.acquire(10).await;
    let waiter = tokio::spawn({
        let ul = ul.clone();
        async move { ul.acquire(5).await }
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    ul.refill(1000);
    waiter.await.unwrap();

    // download limiter exists with its own budget
    dl.acquire(15).await;
    dl.acquire(5).await;
    // empty now — a further acquire must wait for refill
    let dl2 = dl.clone();
    let w2 = tokio::spawn(async move { dl2.acquire(1).await });
    tokio::task::yield_now().await;
    assert!(!w2.is_finished());
    dl.refill(1000);
    w2.await.unwrap();
}
