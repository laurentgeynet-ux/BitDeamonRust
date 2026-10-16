// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `utp_stream` — connector ↔ scripted responder over an in-memory
//! datagram pair (the injected transport).

use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bitdaemon_net::transport::{DatagramSocket, PeerConnector};
use bitdaemon_net::utp_socket::{UtpConfig, UtpSocket, UtpStat};
use bitdaemon_net::utp_stream::{UtpConnector, UtpManager};
use tokio::sync::{Mutex, mpsc};

fn a() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 4000)
}
fn b() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5000)
}

/// two ends of an in-memory "wire": send_to pushes to the peer's
/// recv channel.
#[derive(Debug)]
struct Wire {
    me: SocketAddr,
    out: mpsc::Sender<(Vec<u8>, SocketAddr)>,
    rx: Mutex<mpsc::Receiver<(Vec<u8>, SocketAddr)>>,
}

impl DatagramSocket for Wire {
    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        to: SocketAddr,
    ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
        Box::pin(async move {
            let _ = self.out.send((buf.to_vec(), self.me)).await;
            let _ = to;
            Ok(buf.len())
        })
    }
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> bitdaemon_net::transport::BoxFut<'a, (usize, SocketAddr)> {
        Box::pin(async move {
            let (d, from) = self
                .rx
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("closed"))?;
            let n = d.len().min(buf.len());
            buf[..n].copy_from_slice(&d[..n]);
            Ok((n, from))
        })
    }
    fn local_addr(&self) -> SocketAddr {
        self.me
    }
}

#[tokio::test(flavor = "current_thread")]
async fn connect_handshake_and_echo() {
    let (tx_a, rx_a) = mpsc::channel(64);
    let (tx_b, rx_b) = mpsc::channel(64);
    let wa = Arc::new(Wire {
        me: a(),
        out: tx_a,
        rx: Mutex::new(rx_b),
    });
    let wb = Arc::new(Wire {
        me: b(),
        out: tx_b,
        rx: Mutex::new(rx_a),
    });

    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let conn = Arc::new(UtpConnector {
        manager: mgr,
        datagram: wa.clone(),
    });

    // scripted responder at b(): accept SYN, relay everything
    let responder = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        let (n, from) = wb.recv_from(&mut buf).await.unwrap();
        // upstream: incoming SYN conn_id `id` → send_id = id,
        // recv_id = id + 1
        let id = u16::from_be_bytes([buf[2], buf[3]]);
        let mut sock = UtpSocket::new(id.wrapping_add(1), id, UtpConfig::default());
        assert!(sock.incoming_packet(&buf[..n], from, std::time::Instant::now()));
        sock.send_deferred_ack(); // SYN → deferred STATE (upstream drain)
        for d in sock.drain_outbox() {
            let _ = wb.send_to(&d.bytes, d.to).await;
        }
        // read payload "ping", echo "pong"
        let (n, from) = wb.recv_from(&mut buf).await.unwrap();
        sock.incoming_packet(&buf[..n], from, std::time::Instant::now());
        sock.send_deferred_ack();
        for d in sock.drain_outbox() {
            let _ = wb.send_to(&d.bytes, d.to).await;
        }
        let mut payload = vec![0u8; 64];
        let m = sock.read(&mut payload).unwrap_or(0);
        assert_eq!(&payload[..m], b"ping");
        sock.write(b"pong");
        for d in sock.drain_outbox() {
            let _ = wb.send_to(&d.bytes, d.to).await;
        }
        // hold the wire open while the client reads
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        (sock, wb)
    });

    let mut stream = conn.connect(b()).await.unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut stream, b"ping")
        .await
        .unwrap();
    let mut out = [0u8; 4];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut out)
        .await
        .unwrap();
    assert_eq!(&out, b"pong");
    responder.await.unwrap();
}

#[tokio::test]
async fn acceptor_handshake_and_echo() {
    use bitdaemon_net::transport::PeerAcceptor;
    use bitdaemon_net::utp_stream::{UtpAcceptor, UtpConnector};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // two channel-linked datagram sockets
    let (a_tx, a_rx) = tokio::sync::mpsc::unbounded_channel::<(Vec<u8>, std::net::SocketAddr)>();
    let (b_tx, b_rx) = tokio::sync::mpsc::unbounded_channel::<(Vec<u8>, std::net::SocketAddr)>();
    let a_ep = std::net::SocketAddr::from(([10, 0, 0, 1], 5000));
    let b_ep = std::net::SocketAddr::from(([10, 0, 0, 2], 6000));

    #[derive(Debug)]
    struct C {
        rx: tokio::sync::Mutex<
            tokio::sync::mpsc::UnboundedReceiver<(Vec<u8>, std::net::SocketAddr)>,
        >,
        tx: tokio::sync::mpsc::UnboundedSender<(Vec<u8>, std::net::SocketAddr)>,
        local: std::net::SocketAddr,
    }
    impl bitdaemon_net::transport::DatagramSocket for C {
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            target: std::net::SocketAddr,
        ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
            Box::pin(async move {
                let n = buf.len();
                let _ = self.tx.send((buf.to_vec(), target));
                Ok(n)
            })
        }
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> bitdaemon_net::transport::BoxFut<'a, (usize, std::net::SocketAddr)> {
            Box::pin(async move {
                let mut rx = self.rx.lock().await;
                match rx.recv().await {
                    Some((data, from)) => {
                        let n = data.len().min(buf.len());
                        buf[..n].copy_from_slice(&data[..n]);
                        Ok((n, from))
                    }
                    None => Err(std::io::Error::other("closed")),
                }
            })
        }
        fn local_addr(&self) -> std::net::SocketAddr {
            self.local
        }
    }

    let dg_a = Arc::new(C {
        rx: tokio::sync::Mutex::new(a_rx),
        tx: b_tx.clone(),
        local: a_ep,
    });
    let dg_b = Arc::new(C {
        rx: tokio::sync::Mutex::new(b_rx),
        tx: a_tx.clone(),
        local: b_ep,
    });

    // responder side: one manager routes dg_b, acceptor yields streams
    let mgr_b = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
    let _pump = mgr_b.pump_in(dg_b.clone());
    let acceptor = Arc::new(UtpAcceptor::new(mgr_b, dg_b).await);

    // initiator side
    let mgr_a = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
    let _pump2 = mgr_a.pump_in(dg_a.clone());
    let connector = Arc::new(UtpConnector {
        manager: mgr_a,
        datagram: dg_a,
    });

    let acc = acceptor.clone();
    let responder = tokio::spawn(async move {
        let (mut s, _from) = acc.accept().await.unwrap();
        // echo back whatever arrives
        let mut b = [0u8; 64];
        let n = s.read(&mut b).await.unwrap();
        s.write_all(&b[..n]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });

    let mut s = connector.connect(b_ep).await.unwrap();
    s.write_all(b"ping").await.unwrap();
    let mut b = [0u8; 64];
    let n = s.read(&mut b).await.unwrap();
    assert_eq!(&b[..n], b"ping");
    responder.await.unwrap();
}

#[tokio::test]
async fn bulk_transfer_throughput() {
    // 1 Mio responder → initiator on a channel wire: catches a
    // stuck/slow ACK path (the live C++ pair crept at ~1 KB/s).
    use bitdaemon_net::transport::PeerAcceptor;
    use bitdaemon_net::utp_stream::{UtpAcceptor, UtpConnector};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (a_tx, a_rx) = tokio::sync::mpsc::unbounded_channel::<(Vec<u8>, std::net::SocketAddr)>();
    let (b_tx, b_rx) = tokio::sync::mpsc::unbounded_channel::<(Vec<u8>, std::net::SocketAddr)>();
    let a_ep = std::net::SocketAddr::from(([10, 0, 0, 1], 5000));
    let b_ep = std::net::SocketAddr::from(([10, 0, 0, 2], 6000));

    #[derive(Debug)]
    struct C {
        rx: tokio::sync::Mutex<
            tokio::sync::mpsc::UnboundedReceiver<(Vec<u8>, std::net::SocketAddr)>,
        >,
        tx: tokio::sync::mpsc::UnboundedSender<(Vec<u8>, std::net::SocketAddr)>,
        local: std::net::SocketAddr,
    }
    impl bitdaemon_net::transport::DatagramSocket for C {
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            target: std::net::SocketAddr,
        ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
            Box::pin(async move {
                let n = buf.len();
                let _ = self.tx.send((buf.to_vec(), target));
                Ok(n)
            })
        }
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> bitdaemon_net::transport::BoxFut<'a, (usize, std::net::SocketAddr)> {
            Box::pin(async move {
                let mut rx = self.rx.lock().await;
                match rx.recv().await {
                    Some((data, from)) => {
                        let n = data.len().min(buf.len());
                        buf[..n].copy_from_slice(&data[..n]);
                        Ok((n, from))
                    }
                    None => Err(std::io::Error::other("closed")),
                }
            })
        }
        fn local_addr(&self) -> std::net::SocketAddr {
            self.local
        }
    }

    let dg_a = Arc::new(C {
        rx: tokio::sync::Mutex::new(a_rx),
        tx: b_tx.clone(),
        local: a_ep,
    });
    let dg_b = Arc::new(C {
        rx: tokio::sync::Mutex::new(b_rx),
        tx: a_tx.clone(),
        local: b_ep,
    });

    let mgr_b = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
    let _pb = mgr_b.pump_in(dg_b.clone());
    let acceptor = Arc::new(UtpAcceptor::new(mgr_b, dg_b).await);

    let mgr_a = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
    let _pa = mgr_a.pump_in(dg_a.clone());
    let connector = Arc::new(UtpConnector {
        manager: mgr_a,
        datagram: dg_a,
    });

    const TOTAL: usize = 1024 * 1024;
    let acc = acceptor.clone();
    let responder = tokio::spawn(async move {
        let (mut s, _from) = acc.accept().await.unwrap();
        let chunk = vec![0xABu8; 16 * 1024];
        let mut sent = 0;
        while sent < TOTAL {
            if s.write_all(&chunk).await.is_err() {
                break;
            }
            sent += chunk.len();
        }
        // keep the wire up while the reader drains
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });

    let mut s = connector.connect(b_ep).await.unwrap();
    // like the BitTorrent handshake: the initiator speaks first,
    // advertising a non-zero window (upstream SYN carries wnd=0).
    s.write_all(b"\x13BitTorrent handshake-pad").await.unwrap();
    let start = std::time::Instant::now();
    let mut got = 0usize;
    let mut buf = vec![0u8; 64 * 1024];
    while got < TOTAL {
        let n = tokio::time::timeout(std::time::Duration::from_secs(30), s.read(&mut buf))
            .await
            .expect("read timed out")
            .expect("read failed");
        if n == 0 {
            break;
        }
        assert!(buf[..n].iter().all(|&b| b == 0xAB));
        got += n;
    }
    let secs = start.elapsed().as_secs_f64();
    eprintln!(
        "{TOTAL} bytes in {secs:.2}s = {:.0} B/s",
        TOTAL as f64 / secs
    );
    assert_eq!(got, TOTAL);
    assert!(
        secs < 20.0,
        "uTP loopback must not take {secs:.0}s for 1 MiB"
    );
    responder.abort();
}

fn wire_pair() -> (Wire, Wire) {
    let (tx_a, rx_a) = mpsc::channel(64);
    let (tx_b, rx_b) = mpsc::channel(64);
    (
        Wire {
            me: a(),
            out: tx_a,
            rx: Mutex::new(rx_b),
        },
        Wire {
            me: b(),
            out: tx_b,
            rx: Mutex::new(rx_a),
        },
    )
}

/// scripted peer on `wb`: accepts the SYN, then keeps acking every
/// datagram it receives for the life of the test. Every datagram in
/// both directions is mirrored into the tap channel as
/// `(from_client, bytes)` so tests can read the real connection ids
/// and sequence numbers without racing `recv_from`.
fn spawn_peer(
    wb: Arc<Wire>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<(bool, Vec<u8>)>,
) {
    let (tap_tx, tap_rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        let Ok((n, from)) = wb.recv_from(&mut buf).await else {
            return;
        };
        let _ = tap_tx.send((true, buf[..n].to_vec()));
        let id = u16::from_be_bytes([buf[2], buf[3]]);
        let mut sock = UtpSocket::new(id.wrapping_add(1), id, UtpConfig::default());
        assert!(sock.incoming_packet(&buf[..n], from, std::time::Instant::now()));
        sock.send_deferred_ack();
        for d in sock.drain_outbox() {
            let _ = tap_tx.send((false, d.bytes.clone()));
            let _ = wb.send_to(&d.bytes, d.to).await;
        }
        while let Ok((n, from)) = wb.recv_from(&mut buf).await {
            let _ = tap_tx.send((true, buf[..n].to_vec()));
            sock.incoming_packet(&buf[..n], from, std::time::Instant::now());
            sock.send_deferred_ack();
            for d in sock.drain_outbox() {
                let _ = tap_tx.send((false, d.bytes.clone()));
                let _ = wb.send_to(&d.bytes, d.to).await;
            }
        }
    });
    (handle, tap_rx)
}

async fn connect_stream(
    mgr: &Arc<UtpManager>,
    wa: &Arc<Wire>,
) -> bitdaemon_net::utp_stream::UtpStream {
    let mut s = mgr
        .connect(wa.clone(), b(), UtpConfig::default())
        .await
        .unwrap();
    s.wait_connected().await.unwrap();
    s
}

/// collect the packet types arriving on `wb` for up to `dur`
/// (short-circuiting on `stop` when `want` is seen).
async fn collect_types(wb: &Arc<Wire>, dur: std::time::Duration, want: u8) -> Vec<u8> {
    let mut types = Vec::new();
    let deadline = std::time::Instant::now() + dur;
    loop {
        let mut buf = vec![0u8; 2048];
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            break;
        }
        match tokio::time::timeout(left, wb.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) if n >= 20 => {
                let ty = buf[0] >> 4;
                types.push(ty);
                if ty == want {
                    break;
                }
            }
            _ => break,
        }
    }
    types
}

/// same as `collect_types` but reads the `spawn_peer` tap channel,
/// keeping only packets flowing in `from_client` direction.
async fn collect_tap(
    tap: &mut tokio::sync::mpsc::UnboundedReceiver<(bool, Vec<u8>)>,
    from_client: bool,
    dur: std::time::Duration,
    want: u8,
) -> Vec<u8> {
    let mut types = Vec::new();
    let deadline = std::time::Instant::now() + dur;
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            break;
        }
        match tokio::time::timeout(left, tap.recv()).await {
            Ok(Some((fc, p))) if fc == from_client && p.len() >= 20 => {
                let ty = p[0] >> 4;
                types.push(ty);
                if ty == want {
                    break;
                }
            }
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    types
}

/// next tapped packet in `from_client` direction (or `None` on
/// timeout).
async fn tap_pkt(
    tap: &mut tokio::sync::mpsc::UnboundedReceiver<(bool, Vec<u8>)>,
    from_client: bool,
    dur: std::time::Duration,
) -> Option<Vec<u8>> {
    let deadline = std::time::Instant::now() + dur;
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return None;
        }
        match tokio::time::timeout(left, tap.recv()).await {
            Ok(Some((fc, p))) if fc == from_client => return Some(p),
            Ok(Some(_)) => {}
            _ => return None,
        }
    }
}

// D2 — `poll_shutdown` must emit a FIN on the wire (upstream
// `utp_stream::close` → `send_fin`). Pre-fix it was a no-op and the
// FIN path was dead code.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_emits_fin() {
    use tokio::io::AsyncWriteExt;

    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let (_peer, mut tap) = spawn_peer(wb);
    let mut stream = connect_stream(&mgr, &wa).await;

    stream.write_all(b"x").await.unwrap();
    stream.shutdown().await.unwrap();

    // ST_FIN = 1
    let types = collect_tap(&mut tap, true, std::time::Duration::from_secs(2), 1).await;
    assert!(
        types.contains(&1),
        "no ST_FIN reached the wire after shutdown(): {types:?}"
    );
}

// D6 — `accept_syn` must reject anything that isn't a valid v1 SYN
// (previously garbage fed `incoming_packet` and a phantom socket
// lingered ~connect_timeout).
#[tokio::test(flavor = "current_thread")]
async fn accept_syn_rejects_garbage() {
    let (wa, _wb) = wire_pair();
    let wa = Arc::new(wa);
    let mgr = Arc::new(UtpManager::new());

    // version != 1 → rejected by the version gate
    let mut bad = vec![0u8; 20];
    bad[0] = (4 << 4) | 2;
    assert!(
        mgr.accept_syn(wa.clone(), b(), &bad, UtpConfig::default())
            .await
            .is_err()
    );
    // v1 but ST_DATA, not SYN → rejected
    bad[0] = 1;
    assert!(
        mgr.accept_syn(wa.clone(), b(), &bad, UtpConfig::default())
            .await
            .is_err()
    );

    // a well-formed SYN is still accepted (the gate isn't overstrict)
    let mut syn = vec![0u8; 20];
    syn[0] = (4 << 4) | 1;
    syn[2..4].copy_from_slice(&7u16.to_be_bytes());
    assert!(
        mgr.accept_syn(wa.clone(), b(), &syn, UtpConfig::default())
            .await
            .is_ok()
    );
}

// D13 + D11 — `remove_datagram_socket` aborts every socket bound to
// the transport (upstream `remove_udp_socket`); the abort surfaces
// as `ConnectionAborted`, not a bare EOF.
#[tokio::test(flavor = "current_thread")]
async fn remove_datagram_socket_aborts_streams() {
    use tokio::io::AsyncReadExt;

    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let (_peer, _tap) = spawn_peer(wb);
    let mut stream = connect_stream(&mgr, &wa).await;

    let dg: Arc<dyn DatagramSocket> = wa;
    mgr.remove_datagram_socket(&dg).await;

    let mut buf = [0u8; 8];
    let err = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("read did not complete")
        .expect_err("expected an error, got EOF/bytes");
    assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
}

// D11 — a peer ST_RESET surfaces as `ConnectionReset`, not EOF.
#[tokio::test(flavor = "current_thread")]
async fn reset_maps_to_connection_reset() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let (_peer, mut tap) = spawn_peer(wb.clone());
    let mut stream = connect_stream(&mgr, &wa).await;

    // learn the client's recv_id and seq from the wire: a SYN
    // carries recv_id directly, DATA carries send_id = recv_id + 1.
    // The peer's STATE seq gives a seq_nr inside the client's
    // reorder window, and ack_nr must not ack past what the client
    // sent (both are sanity-checked in incoming_packet).
    stream.write_all(b"probe").await.unwrap();
    let pkt = tap_pkt(&mut tap, true, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(pkt.len() >= 20);
    let conn_id = u16::from_be_bytes([pkt[2], pkt[3]]);
    let recv_id = if pkt[0] >> 4 == 4 {
        conn_id
    } else {
        conn_id.wrapping_sub(1)
    };
    let client_seq = u16::from_be_bytes([pkt[16], pkt[17]]);
    let state = tap_pkt(&mut tap, false, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(state.len() >= 20);
    let peer_seq = u16::from_be_bytes([state[16], state[17]]);

    let mut rst = vec![0u8; 20];
    rst[0] = (3 << 4) | 1; // ST_RESET v1
    rst[2..4].copy_from_slice(&recv_id.to_be_bytes());
    rst[16..18].copy_from_slice(&peer_seq.to_be_bytes());
    rst[18..20].copy_from_slice(&client_seq.to_be_bytes());
    wb.send_to(&rst, a()).await.unwrap();

    let mut b = [0u8; 8];
    let err = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut b))
        .await
        .expect("read did not complete")
        .expect_err("expected an error");
    assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
}

// D7 — a stalled reader must not wedge the pump: once the 64-slot
// read channel is saturated the pump keeps feeding ACKs back
// (upstream: reads are pull-based, the receive path never blocks).
#[tokio::test(flavor = "current_thread")]
async fn stalled_reader_keeps_acking() {
    use tokio::io::AsyncWriteExt;

    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let (_peer, mut tap) = spawn_peer(wb.clone());
    // never polled for read — the read channel will saturate
    let mut stream = connect_stream(&mgr, &wa).await;

    // learn the client's ids and the seq windows from the wire —
    // crafted packets with zeroed seq/ack fields fail the
    // redundant/reorder sanity checks in incoming_packet
    stream.write_all(b"x").await.unwrap();
    let pkt = tap_pkt(&mut tap, true, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(pkt.len() >= 20);
    let conn_id = u16::from_be_bytes([pkt[2], pkt[3]]);
    let recv_id = if pkt[0] >> 4 == 4 {
        conn_id
    } else {
        conn_id.wrapping_sub(1)
    };
    let client_seq = u16::from_be_bytes([pkt[16], pkt[17]]);
    let state = tap_pkt(&mut tap, false, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(state.len() >= 20);
    let peer_seq = u16::from_be_bytes([state[16], state[17]]);

    let data = |seq: u16| {
        let mut d = vec![0u8; 21];
        d[0] = 1; // ST_DATA v1
        d[2..4].copy_from_slice(&recv_id.to_be_bytes());
        d[16..18].copy_from_slice(&seq.to_be_bytes());
        d[18..20].copy_from_slice(&client_seq.to_be_bytes());
        d[20] = 0xAB;
        d
    };

    // phase 1: flood past the 64-slot read channel and let the pump
    // saturate it (pre-fix the pump parks in `rd_tx.send` here and
    // never processes inbound again)
    // seq numbers in the responder's space, inside the client's
    // reorder window
    for i in 1..=70u16 {
        wb.send_to(&data(peer_seq.wrapping_add(i)), a())
            .await
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // drain whatever the client emitted so far — not the discriminator
    while tap.try_recv().is_ok() {}

    // phase 2: more data — a live pump keeps acking; a wedged one
    // stays silent forever (ST_STATE = 2)
    for i in 71..=80u16 {
        wb.send_to(&data(peer_seq.wrapping_add(i)), a())
            .await
            .unwrap();
    }
    let types = collect_tap(&mut tap, true, std::time::Duration::from_secs(2), 2).await;
    assert!(
        types.contains(&2),
        "pump wedged — no STATE acks after the read channel filled: {types:?}"
    );
}

// D4 — datagrams already queued when the pump wakes are processed
// as one receive round → a single deferred ACK per batch (upstream
// `m_deferred_ack_list`), not one ACK per datagram.
#[tokio::test(flavor = "current_thread")]
async fn deferred_ack_batches_per_receive_round() {
    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    // no pump_in — route() by hand so both datagrams are already
    // queued when the socket pump wakes (single-threaded runtime
    // guarantees the ordering)

    let mut syn = vec![0u8; 20];
    syn[0] = (4 << 4) | 1; // ST_SYN v1
    syn[2..4].copy_from_slice(&42u16.to_be_bytes());
    let _s = mgr
        .accept_syn(wa.clone(), b(), &syn, UtpConfig::default())
        .await
        .unwrap();
    // drain the SYN's STATE ack — capture it: its seq_nr anchors
    // the responder's send-seq space so our DATA can ack exactly
    // the last consumed seq (state_seq − 1)
    let mut raw = vec![0u8; 2048];
    let (_n, _) = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        wb.recv_from(&mut raw),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(raw[0] >> 4, 2, "expected a STATE ack for the SYN");
    let state_seq = u16::from_be_bytes([raw[16], raw[17]]);
    let ack_nr = state_seq.wrapping_sub(1);

    // two DATA packets addressed to the responder's recv_id =
    // SYN conn_id + 1 (accept_syn registers under that key)
    for seq in [1u16, 2] {
        let mut d = vec![0u8; 21];
        d[0] = 1;
        d[2..4].copy_from_slice(&43u16.to_be_bytes());
        d[16..18].copy_from_slice(&seq.to_be_bytes());
        d[18..20].copy_from_slice(&ack_nr.to_be_bytes());
        d[20] = 0xCD;
        mgr.route(b(), &d).await;
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let types = collect_types(&wb, std::time::Duration::from_millis(300), 99).await;
    let states = types.iter().filter(|&&t| t == 2).count();
    assert_eq!(
        states, 1,
        "expected one batched deferred ACK, got {types:?}"
    );
}

// D12 — `route` must never wait on a full per-socket queue: excess
// datagrams drop like upstream packet loss, the shared receive pump
// keeps running.
#[tokio::test(flavor = "current_thread")]
async fn route_drops_on_full_listener_queue() {
    let (wa, _wb) = wire_pair();
    let _wa = Arc::new(wa);
    let mgr = Arc::new(UtpManager::new());
    let mut listener_rx = mgr.set_listener().await;

    // nobody accepts — the 64-slot listener queue fills up; every
    // route() call must still return promptly
    let mut syn = vec![0u8; 20];
    syn[0] = (4 << 4) | 1;
    for i in 0..70u16 {
        syn[2..4].copy_from_slice(&(1000u16 + i).to_be_bytes());
        tokio::time::timeout(std::time::Duration::from_millis(100), mgr.route(b(), &syn))
            .await
            .expect("route() blocked on a full queue");
    }
    let mut got = 0;
    while listener_rx.try_recv().is_ok() {
        got += 1;
    }
    assert_eq!(got, 64, "overflowing datagrams must drop, not block");
}

// D5 — `dont_fragment` must reach `DatagramSocket::send_to_flags`
// on MTU probes. cwnd grows past 3×floor after the first acked
// payload, so the next ≥floor packet leaves as a DF-flagged probe.
#[tokio::test(flavor = "current_thread")]
async fn mtu_probe_carries_dont_fragment() {
    use bitdaemon_net::transport::DatagramSendFlags;
    use tokio::io::AsyncWriteExt;

    #[derive(Debug)]
    struct FlagWire {
        inner: Wire,
        flags: std::sync::Mutex<Vec<DatagramSendFlags>>,
    }
    impl DatagramSocket for FlagWire {
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            to: SocketAddr,
        ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
            self.inner.send_to(buf, to)
        }
        fn send_to_flags<'a>(
            &'a self,
            buf: &'a [u8],
            to: SocketAddr,
            flags: DatagramSendFlags,
        ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
            self.flags
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(flags);
            self.inner.send_to(buf, to)
        }
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> bitdaemon_net::transport::BoxFut<'a, (usize, SocketAddr)> {
            self.inner.recv_from(buf)
        }
        fn local_addr(&self) -> SocketAddr {
            self.inner.local_addr()
        }
    }

    let (wa, wb) = wire_pair();
    let fwa = Arc::new(FlagWire {
        inner: wa,
        flags: std::sync::Mutex::new(Vec::new()),
    });
    let wb = Arc::new(wb);
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(fwa.clone());
    let (_peer, _tap) = spawn_peer(wb);
    let mut stream = mgr
        .connect(fwa.clone(), b(), UtpConfig::default())
        .await
        .unwrap();
    stream.wait_connected().await.unwrap();

    // a first payload whose acks grow cwnd past 3×mtu_floor, then a
    // second one — its ≥floor packet leaves as an MTU probe and must
    // carry DONT_FRAGMENT through send_to_flags
    stream.write_all(&vec![0x55u8; 1400]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    stream.write_all(&vec![0x55u8; 2000]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let seen: Vec<DatagramSendFlags> = fwa.flags.lock().unwrap_or_else(|p| p.into_inner()).clone();
    assert!(
        seen.iter()
            .any(|f| f.contains(DatagramSendFlags::DONT_FRAGMENT)),
        "no DF-flagged datagram on the transport: {seen:?}"
    );
}

// ---------- A05 round-3 apply regressions ----------

/// datagram transport that swallows sends and never answers —
/// drives the connect-timeout path.
#[derive(Debug)]
struct Blackhole;

impl DatagramSocket for Blackhole {
    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        _to: SocketAddr,
    ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
        Box::pin(async move { Ok(buf.len()) })
    }
    fn recv_from<'a>(
        &'a self,
        _buf: &'a mut [u8],
    ) -> bitdaemon_net::transport::BoxFut<'a, (usize, SocketAddr)> {
        Box::pin(std::future::pending())
    }
    fn local_addr(&self) -> SocketAddr {
        a()
    }
}

// R1 — writes past `poll_shutdown` are `broken_pipe` upstream; the
// stream surface must refuse them (the pump also drops post-Close
// payload as backstop).
#[tokio::test(flavor = "current_thread")]
async fn write_after_shutdown_is_broken_pipe() {
    use tokio::io::AsyncWriteExt;
    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let (_peer, _tap) = spawn_peer(wb);
    let mut stream = connect_stream(&mgr, &wa).await;
    stream.shutdown().await.unwrap();
    let err = stream.write_all(b"late").await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
}

// R7+R8 — `UtpConnector` reads the manager's live config (upstream
// sockets consult the shared `session_settings` getters) and
// `wait_connected` propagates the socket's `m_error` — a connect
// timeout must surface `TimedOut`, not a generic failure.
#[tokio::test(flavor = "current_thread")]
async fn connect_failure_surfaces_real_error() {
    let mgr = Arc::new(UtpManager::new());
    mgr.set_config(UtpConfig {
        connect_timeout: 50,
        ..UtpConfig::default()
    });
    let conn = Arc::new(UtpConnector {
        manager: mgr,
        datagram: Arc::new(Blackhole),
    });
    match conn.connect(b()).await {
        Err(bitdaemon_net::NetError::Transport(e)) => {
            assert_eq!(
                e.kind(),
                std::io::ErrorKind::TimedOut,
                "expected the real TimedOut error, got {e:?}"
            );
        }
        Err(other) => panic!("expected transport error, got {other:?}"),
        Ok(_) => panic!("connect must fail against a blackhole"),
    }
}

// R10 — a `poll_read` pending when the peer's FIN arrives must
// observe EOF immediately (upstream fires `eof` on the read handler
// once the FIN'd data drains); pre-fix the reader hung until full
// teardown.
#[tokio::test(flavor = "current_thread")]
async fn fin_wakes_pending_read() {
    use tokio::io::AsyncReadExt;
    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let peer = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        let Ok((n, from)) = wb.recv_from(&mut buf).await else {
            return;
        };
        let id = u16::from_be_bytes([buf[2], buf[3]]);
        let mut sock = UtpSocket::new(id.wrapping_add(1), id, UtpConfig::default());
        assert!(sock.incoming_packet(&buf[..n], from, std::time::Instant::now()));
        sock.send_deferred_ack();
        sock.close(); // FIN with no payload behind it
        for d in sock.drain_outbox() {
            let _ = wb.send_to(&d.bytes, d.to).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    });
    let mut stream = mgr
        .connect(wa.clone(), b(), UtpConfig::default())
        .await
        .unwrap();
    stream.wait_connected().await.unwrap();
    let mut b = [0u8; 8];
    let n = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut b))
        .await
        .expect("pending read hung on FIN")
        .unwrap();
    assert_eq!(n, 0);
    peer.await.unwrap();
}

// R4 (residual) — the peer's `close_reason` extension is parsed by
// the socket; it must reach the stream so the session can surface
// it (upstream `peer_connection` calls `get_close_reason`).
#[tokio::test(flavor = "current_thread")]
async fn their_close_reason_reaches_the_stream() {
    use tokio::io::AsyncReadExt;
    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let _peer = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        let Ok((n, from)) = wb.recv_from(&mut buf).await else {
            return;
        };
        let id = u16::from_be_bytes([buf[2], buf[3]]);
        let mut sock = UtpSocket::new(id.wrapping_add(1), id, UtpConfig::default());
        assert!(sock.incoming_packet(&buf[..n], from, std::time::Instant::now()));
        sock.send_deferred_ack();
        sock.set_close_reason(7); // `close_reason_t` — ext 3 on the FIN
        sock.close();
        for d in sock.drain_outbox() {
            let _ = wb.send_to(&d.bytes, d.to).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    });
    let mut stream = mgr
        .connect(wa.clone(), b(), UtpConfig::default())
        .await
        .unwrap();
    stream.wait_connected().await.unwrap();
    let mut b = [0u8; 8];
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut b)).await;
    for _ in 0..40 {
        if stream.their_close_reason() == 7 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!(
        "peer close_reason never surfaced: {}",
        stream.their_close_reason()
    );
}

// R6/R10 — dropping the stream (`destroy()` upstream) still flushes
// the FIN: `close()` drains the queued data then sends FIN, only
// the unsent *write buffer* is dropped.
#[tokio::test(flavor = "current_thread")]
async fn drop_still_flushes_fin() {
    use tokio::io::AsyncWriteExt;
    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let mgr = Arc::new(UtpManager::new());
    let _pump = mgr.pump_in(wa.clone());
    let (_peer, mut tap) = spawn_peer(wb);
    let mut stream = connect_stream(&mgr, &wa).await;
    stream.write_all(b"queued").await.unwrap();
    drop(stream);
    let types = collect_tap(&mut tap, true, std::time::Duration::from_secs(2), 1).await;
    assert!(
        types.contains(&1),
        "FIN must reach the wire when the stream is dropped: {types:?}"
    );
}

// R11 — the demux gate is `size >= 20 && version == 1` upstream:
// packets with an unknown type (5..15) are *delivered* to a
// matching socket, which counts `utp_invalid_pkts_in`; an unmatched
// conn-id is not consumed (falls through to the other UDP users).
#[tokio::test(flavor = "current_thread")]
async fn unknown_type_counts_invalid_and_unmatched_falls_through() {
    let (wa, wb) = wire_pair();
    let (wa, wb) = (Arc::new(wa), Arc::new(wb));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s2 = seen.clone();
    let cfg = UtpConfig {
        counter_sink: Some(Arc::new(move |stat, _delta| {
            s2.lock().unwrap_or_else(|p| p.into_inner()).push(stat);
        })),
        ..UtpConfig::default()
    };
    let mgr = Arc::new(UtpManager::new());
    let _stream = mgr.connect(wa.clone(), b(), cfg).await.unwrap();

    // our recv_id travels in the SYN's conn_id field
    let mut buf = vec![0u8; 2048];
    let (n, _f) = tokio::time::timeout(std::time::Duration::from_secs(2), wb.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(n >= 20);
    let conn_id = u16::from_be_bytes([buf[2], buf[3]]);

    // a type-5 packet addressed to the live socket
    let mut p = vec![0u8; 20];
    p[0] = (5 << 4) | 1;
    p[2..4].copy_from_slice(&conn_id.to_be_bytes());
    assert!(
        mgr.route(b(), &p).await,
        "a matching socket must consume unknown-type packets"
    );

    // same packet, unmatched conn-id — upstream falls through
    p[2..4].copy_from_slice(&conn_id.wrapping_add(9).to_be_bytes());
    assert!(!mgr.route(b(), &p).await);

    for _ in 0..40 {
        if seen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&UtpStat::InvalidPktsIn)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("unknown packet type did not bump utp_invalid_pkts_in");
}

// R7 — `enable_incoming_utp` gates SYN acceptance on the manager,
// not acceptor installation: off → `route` drops unmatched SYNs
// (upstream returns false before the listen-socket branch).
#[tokio::test(flavor = "current_thread")]
async fn accept_incoming_gate_drops_syn() {
    let mgr = Arc::new(UtpManager::new());
    let _rx = mgr.set_listener().await;
    let mut syn = vec![0u8; 20];
    syn[0] = (4 << 4) | 1; // ST_SYN v1
    assert!(mgr.route(b(), &syn).await, "listener must take the SYN");
    mgr.set_accept_incoming(false);
    assert!(!mgr.route(b(), &syn).await, "enable_incoming_utp=off drops");
}
