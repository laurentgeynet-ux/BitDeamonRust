// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `http_conn` — request pump over duplex streams.

use bitdaemon_net::http_conn::{MAX_RESPONSE, http_get, recv_response, send_request};
use bitdaemon_net::transport::BoxedPeerStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn get_round_trip() {
    let (mut cli, mut srv): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let server = tokio::spawn(async move {
        let mut req = vec![0u8; 512];
        let n = srv.read(&mut req).await.unwrap();
        let req = String::from_utf8_lossy(&req[..n]).to_string();
        assert!(req.starts_with("GET /announce?info_hash=x HTTP/1.1"));
        assert!(req.contains("Host: tracker.example"));
        assert!(req.contains("User-Agent: bitdaemon"));
        srv.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
            .await
            .unwrap();
    });
    let r = http_get(
        &mut cli,
        "/announce?info_hash=x",
        "tracker.example",
        &[("User-Agent".into(), "bitdaemon".into())],
        MAX_RESPONSE,
    )
    .await
    .unwrap();
    server.await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.body(), b"hello");
}

#[tokio::test]
async fn chunked_response() {
    let (mut cli, mut srv): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let server = tokio::spawn(async move {
        let mut buf = [0u8; 256];
        let _ = srv.read(&mut buf).await.unwrap();
        srv.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
    });
    send_request(&mut cli, "GET", "/", "h", &[], &[])
        .await
        .unwrap();
    let r = recv_response(&mut cli, MAX_RESPONSE).await.unwrap();
    server.await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.body(), b"hello");
}
