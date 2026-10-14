// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `websocket` — upgrade handshake + RFC 6455 framing.

use bitdaemon_net::transport::BoxedPeerStream;
use bitdaemon_net::websocket::{
    WsOpcode, validate_handshake, ws_accept_key, ws_read_frame, ws_upgrade, ws_write_frame,
};
use tokio::io::AsyncReadExt;

// RFC 6455 §1.3 vector: key "dGhlIHNhbXBsZSBub25jZQ==" →
// accept "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
#[test]
fn accept_key_rfc_vector() {
    assert_eq!(
        ws_accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
}

#[test]
fn handshake_validation() {
    assert!(
        validate_handshake(
            101,
            Some("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="),
            "dGhlIHNhbXBsZSBub25jZQ=="
        )
        .is_ok()
    );
    assert!(
        validate_handshake(
            200,
            Some("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="),
            "dGhlIHNhbXBsZSBub25jZQ=="
        )
        .is_err()
    );
    assert!(validate_handshake(101, Some("bogus"), "k").is_err());
}

#[tokio::test]
async fn frame_round_trip_masked() {
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let writer = tokio::spawn(async move {
        ws_write_frame(&mut a, WsOpcode::Binary, b"announce-reply", 0xDEADBEEF)
            .await
            .unwrap();
        ws_write_frame(&mut a, WsOpcode::Text, &[0u8; 300], 0x11223344)
            .await
            .unwrap();
    });
    let (op, payload, fin) = ws_read_frame(&mut b).await.unwrap();
    assert!(matches!(op, WsOpcode::Binary));
    assert!(fin);
    assert_eq!(payload, b"announce-reply");
    let (op, payload, fin) = ws_read_frame(&mut b).await.unwrap();
    assert!(matches!(op, WsOpcode::Text));
    assert!(fin);
    assert_eq!(payload.len(), 300); // 16-bit length path
    writer.await.unwrap();
}

#[tokio::test]
async fn upgrade_request_verbatim() {
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        let n = b.read(&mut buf).await.unwrap();
        String::from_utf8(buf[..n].to_vec()).unwrap()
    });
    ws_upgrade(
        &mut a,
        "tracker.example",
        "/announce",
        "bitdaemon/1.0",
        "dGhlIHNhbXBsZSBub25jZQ==",
    )
    .await
    .unwrap();
    drop(a);
    let req = reader.await.unwrap();
    assert!(req.starts_with("GET /announce HTTP/1.1\r\nHost: tracker.example\r\n"));
    assert!(req.contains("Upgrade: websocket"));
    assert!(req.contains("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ=="));
    assert!(req.contains("Sec-WebSocket-Version: 13"));
    assert!(req.contains("User-Agent: bitdaemon/1.0"));
}

#[tokio::test]
async fn ws_announce_round_trip() {
    use bitdaemon_net::tracker_client::AnnounceParams;
    use bitdaemon_net::websocket::{WsOpcode, ws_accept_key, ws_read_frame, ws_write_frame};
    use bitdaemon_net::ws_tracker::{announce_ws, build_announce, latin1_utf8, parse_response};

    let ih = [7u8; 20];
    let p = AnnounceParams {
        url: "ws://tracker.example/announce".into(),
        info_hash: ih,
        peer_id: *b"-OB0001-123456789012",
        port: 6881,
        uploaded: 5,
        downloaded: 9,
        left: 100,
        corrupt: 0,
        redundant: 0,
        event: 2, // started
        num_want: 50,
        key: 0x0BADCAFE,
        tracker_id: Vec::new(),
        ipv4: Vec::new(),
        ipv6: Vec::new(),
        i2p: false,
        filter: None,
    };
    let payload = build_announce(&p, &[]);
    assert!(payload.contains("\"action\":\"announce\""));
    assert!(payload.contains("\"event\":\"started\""));
    assert!(payload.contains("\"key\":\"0BADCAFE\""));
    // latin-1 info_hash survives as a JSON string (control chars u-escaped)
    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(v["info_hash"].as_str().unwrap(), latin1_utf8(&ih));

    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: bitdaemon_net::transport::BoxedPeerStream = Box::new(a);
    let mut srv: bitdaemon_net::transport::BoxedPeerStream = Box::new(b);

    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // read the upgrade request — may arrive split
        let mut buf = Vec::new();
        let mut tmp = [0u8; 512];
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = srv.read(&mut tmp).await.unwrap();
            buf.extend_from_slice(&tmp[..n]);
        }
        let req = String::from_utf8_lossy(&buf);
        let key = req
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:"))
            .map(|l| l.split(':').nth(1).unwrap().trim().to_string())
            .unwrap();
        let accept = ws_accept_key(&key);
        srv.write_all(
            format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
        // read the announce frame — beast sends binary upstream
        let (op, data, fin) = ws_read_frame(&mut srv).await.unwrap();
        assert_eq!(op, WsOpcode::Binary);
        assert!(fin);
        assert!(String::from_utf8_lossy(&data).contains("\"action\":\"announce\""));
        // reply — unmasked server frame
        let resp = serde_json::to_string(&serde_json::json!({
            "action": "announce",
            "info_hash": latin1_utf8(&ih),
            "interval": 900,
            "complete": 3,
            "incomplete": 1,
        }))
        .unwrap();
        ws_write_frame(&mut srv, WsOpcode::Text, resp.as_bytes(), 0)
            .await
            .unwrap();
    });

    let r = announce_ws(
        &mut cli,
        "t.example",
        "/announce",
        "dGhlIHNhbXBsZSBub25jZQ==",
        &payload,
    )
    .await
    .unwrap();
    assert_eq!(r.info_hash, Some(ih));
    assert_eq!(r.interval, 900);
    assert_eq!(r.complete, 3);
    assert_eq!(r.incomplete, 1);

    // bad ih size is rejected
    let bad = parse_response(b"{\"info_hash\":\"short\",\"interval\":1}");
    assert!(bad.is_err());
}

/// mini ws tracker server: accepts the upgrade, reads one announce
/// frame, then writes each reply payload in order.
async fn run_ws_server(mut srv: bitdaemon_net::transport::BoxedPeerStream, replies: Vec<Vec<u8>>) {
    use bitdaemon_net::websocket::{WsOpcode, ws_accept_key, ws_read_frame, ws_write_frame};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = srv.read(&mut tmp).await.unwrap();
        buf.extend_from_slice(&tmp[..n]);
    }
    let req = String::from_utf8_lossy(&buf);
    let key = req
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:"))
        .map(|l| l.split(':').nth(1).unwrap().trim().to_string())
        .unwrap();
    srv.write_all(
        format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n", ws_accept_key(&key)).as_bytes(),
    )
    .await
    .unwrap();
    let (_op, _data, _fin) = ws_read_frame(&mut srv).await.unwrap();
    for resp in replies {
        ws_write_frame(&mut srv, WsOpcode::Text, &resp, 0)
            .await
            .unwrap();
    }
}

fn announce_params() -> bitdaemon_net::tracker_client::AnnounceParams {
    bitdaemon_net::tracker_client::AnnounceParams {
        url: "ws://tracker.example/announce".into(),
        info_hash: [7u8; 20],
        peer_id: *b"-OB0001-123456789012",
        port: 6881,
        uploaded: 5,
        downloaded: 9,
        left: 100,
        corrupt: 0,
        redundant: 0,
        event: 2,
        num_want: 50,
        key: 0x0BADCAFE,
        tracker_id: Vec::new(),
        ipv4: Vec::new(),
        ipv6: Vec::new(),
        i2p: false,
        filter: None,
    }
}

/// upstream `73f4832de`: a `failure reason` frame that carries no
/// `info_hash` cannot be correlated to an announce — it is ignored and
/// the read is re-issued until the real response arrives.
#[tokio::test]
async fn ws_announce_skips_uncorrelated_failure() {
    use bitdaemon_net::ws_tracker::{announce_ws, build_announce, latin1_utf8};

    let ih = [7u8; 20];
    let payload = build_announce(&announce_params(), &[]);
    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: bitdaemon_net::transport::BoxedPeerStream = Box::new(a);

    tokio::spawn(async move {
        let resp = serde_json::to_string(&serde_json::json!({
            "action": "announce",
            "info_hash": latin1_utf8(&ih),
            "interval": 900,
            "complete": 3,
            "incomplete": 1,
        }))
        .unwrap();
        run_ws_server(
            Box::new(b),
            vec![
                // uncorrelated failure frame — no info_hash
                br#"{"failure reason":"tracker busy"}"#.to_vec(),
                // the real announce response
                resp.into_bytes(),
            ],
        )
        .await;
    });

    let r = announce_ws(
        &mut cli,
        "t.example",
        "/announce",
        "dGhlIHNhbXBsZSBub25jZQ==",
        &payload,
    )
    .await
    .unwrap();
    assert_eq!(r.info_hash, Some(ih));
    assert_eq!(r.interval, 900);
    assert_eq!(r.complete, 3);
}

/// upstream `0ee23cc89`: a `failure reason` correlated to our
/// info_hash reaches the caller so `tracker_request_error` can fire.
#[tokio::test]
async fn ws_announce_correlated_failure_propagates() {
    use bitdaemon_net::ws_tracker::{announce_ws, build_announce, latin1_utf8};

    let ih = [7u8; 20];
    let payload = build_announce(&announce_params(), &[]);
    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: bitdaemon_net::transport::BoxedPeerStream = Box::new(a);

    tokio::spawn(async move {
        let resp = serde_json::to_string(&serde_json::json!({
            "info_hash": latin1_utf8(&ih),
            "failure reason": "unregistered torrent",
        }))
        .unwrap();
        run_ws_server(Box::new(b), vec![resp.into_bytes()]).await;
    });

    let r = announce_ws(
        &mut cli,
        "t.example",
        "/announce",
        "dGhlIHNhbXBsZSBub25jZQ==",
        &payload,
    )
    .await
    .unwrap();
    assert_eq!(r.info_hash, Some(ih));
    assert_eq!(r.failure_reason.as_deref(), Some("unregistered torrent"));
    assert_eq!(r.interval, 0);
    assert_eq!(r.complete, 0);
}
