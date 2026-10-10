// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `tracker_client` — full announce exchanges (HTTP + UDP BEP-15).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use std::time::Duration;

use bitdaemon_net::tracker_client::{AnnounceParams, announce_http, announce_udp};
use bitdaemon_net::transport::{BoxFut, BoxedPeerStream, DatagramSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn params() -> AnnounceParams {
    AnnounceParams {
        info_hash: [0x11; 20],
        peer_id: [0x22; 20],
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 1024,
        event: 2,
        num_want: 50,
        key: 0xABCD,
    }
}

#[tokio::test]
async fn http_announce_e2e() {
    let (mut cli, mut srv): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let server = tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        let n = srv.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        assert!(req.starts_with("GET /announce?"));
        assert!(req.contains("info_hash="));
        assert!(req.contains("event=started"));
        assert!(req.contains("&key=abcd"));
        // announce reply: interval + one compact peer
        let mut body = b"d8:completei2e10:incompletei1e8:intervali1800e5:peers6:".to_vec();
        body.extend([10, 0, 0, 9, 0x1A, 0xE1]);
        body.extend(b"e");
        let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
        srv.write_all(resp.as_bytes()).await.unwrap();
        srv.write_all(&body).await.unwrap();
    });
    let r = announce_http(
        &mut cli,
        "tracker.example",
        "/announce",
        &params(),
        None,
        &[],
        1024 * 1024,
    )
    .await
    .unwrap();
    server.await.unwrap();
    assert_eq!(r.interval, 1800);
    assert_eq!(r.complete, 2);
    assert_eq!(
        r.peers,
        vec![SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)),
            6881
        )]
    );
}

#[tokio::test]
async fn udp_announce_e2e() {
    let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 6969);
    let udp = ScriptedUdp::default();
    let sock = &udp as &dyn DatagramSocket;
    let r = announce_udp(sock, target, &params(), Duration::from_secs(5), 3)
        .await
        .unwrap();
    assert_eq!(r.interval, 1800);
    assert_eq!(r.peers.len(), 1);
    assert_eq!(udp.sent_count(), 2);
}

/// echoes transaction ids back — connect + announce canned replies.
#[derive(Debug, Default)]
struct ScriptedUdp {
    sent: Mutex<Vec<Vec<u8>>>,
}

impl ScriptedUdp {
    fn sent_count(&self) -> usize {
        self.sent.lock().unwrap().len()
    }
}

impl DatagramSocket for ScriptedUdp {
    fn send_to<'a>(&'a self, buf: &'a [u8], _target: SocketAddr) -> BoxFut<'a, usize> {
        Box::pin(async move {
            self.sent.lock().unwrap().push(buf.to_vec());
            Ok(buf.len())
        })
    }
    fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> BoxFut<'a, (usize, SocketAddr)> {
        Box::pin(async move {
            let sent = self
                .sent
                .lock()
                .unwrap()
                .last()
                .cloned()
                .unwrap_or_default();
            let tid = u32::from_be_bytes([sent[12], sent[13], sent[14], sent[15]]);
            let resp: Vec<u8> = if sent[11] == 0 {
                // connect → conn id
                let mut v = vec![];
                v.extend(0u32.to_be_bytes());
                v.extend(tid.to_be_bytes());
                v.extend(0x41727101980u64.to_be_bytes());
                v
            } else {
                // announce → interval/leechers/seeders + 1 peer
                let mut v = vec![];
                v.extend(1u32.to_be_bytes());
                v.extend(tid.to_be_bytes());
                v.extend(1800i32.to_be_bytes());
                v.extend(1i32.to_be_bytes());
                v.extend(2i32.to_be_bytes());
                v.extend([10, 0, 0, 9, 0x1A, 0xE1]);
                v
            };
            buf[..resp.len()].copy_from_slice(&resp);
            Ok((
                resp.len(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6969),
            ))
        })
    }
    fn local_addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }
}
