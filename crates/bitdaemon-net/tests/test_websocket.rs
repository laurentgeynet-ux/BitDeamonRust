// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `websocket` — upgrade handshake + RFC 6455 framing.

use bitdaemon_net::transport::BoxedPeerStream;
use bitdaemon_net::websocket::{WsOpcode, validate_handshake, ws_accept_key,
    ws_read_frame, ws_upgrade, ws_write_frame};
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
    assert!(validate_handshake(101, Some("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), "dGhlIHNhbXBsZSBub25jZQ==").is_ok());
    assert!(validate_handshake(200, Some("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), "dGhlIHNhbXBsZSBub25jZQ==").is_err());
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
    let (op, payload) = ws_read_frame(&mut b).await.unwrap();
    assert!(matches!(op, WsOpcode::Binary));
    assert_eq!(payload, b"announce-reply");
    let (op, payload) = ws_read_frame(&mut b).await.unwrap();
    assert!(matches!(op, WsOpcode::Text));
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
    ws_upgrade(&mut a, "tracker.example", "/announce", "bitdaemon/1.0", "dGhlIHNhbXBsZSBub25jZQ==")
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
