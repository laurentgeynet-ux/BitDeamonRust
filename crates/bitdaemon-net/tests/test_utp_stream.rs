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
