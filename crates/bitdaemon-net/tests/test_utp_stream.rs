// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `utp_stream` — connector ↔ scripted responder over an in-memory
//! datagram pair (the injected transport).

use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bitdaemon_net::transport::{DatagramSocket, PeerConnector};
use bitdaemon_net::utp_socket::{UtpConfig, UtpSocket};
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
        cfg: UtpConfig::default(),
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
    let acceptor = Arc::new(
        UtpAcceptor::new(mgr_b, dg_b, bitdaemon_net::utp_socket::UtpConfig::default()).await,
    );

    // initiator side
    let mgr_a = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
    let _pump2 = mgr_a.pump_in(dg_a.clone());
    let connector = Arc::new(UtpConnector {
        manager: mgr_a,
        datagram: dg_a,
        cfg: bitdaemon_net::utp_socket::UtpConfig::default(),
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
    let acceptor = Arc::new(
        UtpAcceptor::new(mgr_b, dg_b, bitdaemon_net::utp_socket::UtpConfig::default()).await,
    );

    let mgr_a = Arc::new(bitdaemon_net::utp_stream::UtpManager::new());
    let _pa = mgr_a.pump_in(dg_a.clone());
    let connector = Arc::new(UtpConnector {
        manager: mgr_a,
        datagram: dg_a,
        cfg: bitdaemon_net::utp_socket::UtpConfig::default(),
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
