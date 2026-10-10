// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `i2p` — SAM handshake over a scripted bridge.

use bitdaemon_net::i2p::{
    I2pError, I2pSessionOptions, i2p_accept, i2p_connect, i2p_name_lookup, i2p_session_create,
};
use bitdaemon_net::transport::BoxedPeerStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn pair() -> (BoxedPeerStream, BoxedPeerStream) {
    let (a, b) = tokio::io::duplex(64 * 1024);
    (Box::new(a), Box::new(b))
}

async fn read_line(s: &mut BoxedPeerStream) -> String {
    let mut out = Vec::new();
    loop {
        let mut b = [0u8; 1];
        s.read_exact(&mut b).await.unwrap();
        if b[0] == b'\n' {
            break;
        }
        out.push(b[0]);
    }
    String::from_utf8(out).unwrap()
}

#[tokio::test]
async fn session_create_transient() {
    let (mut cli, mut srv) = pair();
    let bridge = tokio::spawn(async move {
        assert_eq!(read_line(&mut srv).await, "HELLO VERSION MIN=3.1 MAX=3.1");
        srv.write_all(b"HELLO REPLY RESULT=OK VERSION=3.1\n")
            .await
            .unwrap();
        let cmd = read_line(&mut srv).await;
        assert!(cmd.starts_with("SESSION CREATE STYLE=STREAM ID=sid DESTINATION=TRANSIENT SIGNATURE_TYPE=7 i2cp.leaseSetEncType=4,0 inbound.quantity=3 outbound.quantity=3"));
        srv.write_all(b"SESSION STATUS RESULT=OK DESTINATION=BASE64DEST\n")
            .await
            .unwrap();
    });
    let opts = I2pSessionOptions::default();
    let dest = i2p_session_create(&mut cli, "sid", &opts).await.unwrap();
    bridge.await.unwrap();
    assert_eq!(dest, "BASE64DEST");
}

#[tokio::test]
async fn connect_and_accept_and_lookup() {
    // connect
    let (mut cli, mut srv) = pair();
    let bridge = tokio::spawn(async move {
        let _ = read_line(&mut srv).await;
        srv.write_all(b"HELLO REPLY RESULT=OK\n").await.unwrap();
        assert_eq!(
            read_line(&mut srv).await,
            "STREAM CONNECT ID=sid DESTINATION=dest.b32.i2p"
        );
        srv.write_all(b"STREAM STATUS RESULT=OK\n").await.unwrap();
    });
    i2p_connect(&mut cli, "sid", "dest.b32.i2p").await.unwrap();
    bridge.await.unwrap();

    // accept → status then incoming dest line
    let (mut cli, mut srv) = pair();
    let bridge = tokio::spawn(async move {
        let _ = read_line(&mut srv).await;
        srv.write_all(b"HELLO REPLY RESULT=OK\n").await.unwrap();
        assert_eq!(read_line(&mut srv).await, "STREAM ACCEPT ID=sid");
        srv.write_all(b"STREAM STATUS RESULT=OK\n").await.unwrap();
        srv.write_all(b"peer-destination-b64\n").await.unwrap();
    });
    let peer = i2p_accept(&mut cli, "sid").await.unwrap();
    bridge.await.unwrap();
    assert_eq!(peer, "peer-destination-b64");

    // name lookup → VALUE
    let (mut cli, mut srv) = pair();
    let bridge = tokio::spawn(async move {
        let _ = read_line(&mut srv).await;
        srv.write_all(b"HELLO REPLY RESULT=OK\n").await.unwrap();
        assert_eq!(read_line(&mut srv).await, "NAMING LOOKUP NAME=forum.i2p");
        srv.write_all(b"NAMING REPLY RESULT=OK NAME=forum.i2p VALUE=b64dest\n")
            .await
            .unwrap();
    });
    let v = i2p_name_lookup(&mut cli, "forum.i2p").await.unwrap();
    bridge.await.unwrap();
    assert_eq!(v, "b64dest");
}

#[tokio::test]
async fn sam_errors() {
    let (mut cli, mut srv) = pair();
    let bridge = tokio::spawn(async move {
        let _ = read_line(&mut srv).await;
        srv.write_all(b"HELLO REPLY RESULT=OK\n").await.unwrap();
        let _ = read_line(&mut srv).await;
        srv.write_all(b"STREAM STATUS RESULT=CANT_REACH_PEER MESSAGE=\"can't reach\"\n")
            .await
            .unwrap();
    });
    let e = i2p_connect(&mut cli, "sid", "d").await.unwrap_err();
    bridge.await.unwrap();
    assert!(matches!(e, I2pError::CantReachPeer));
}
