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

    // name lookup on an established session socket → NAMING LOOKUP
    // goes out *without* a new HELLO (upstream reuses m_sam_socket)
    let (mut cli, mut srv) = pair();
    let bridge = tokio::spawn(async move {
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
async fn name_lookup_fresh_socket() {
    use bitdaemon_net::i2p::i2p_name_lookup_fresh;
    let (mut cli, mut srv) = pair();
    let bridge = tokio::spawn(async move {
        let _ = read_line(&mut srv).await; // HELLO
        srv.write_all(b"HELLO REPLY RESULT=OK\n").await.unwrap();
        assert_eq!(read_line(&mut srv).await, "NAMING LOOKUP NAME=forum.i2p");
        srv.write_all(b"NAMING REPLY RESULT=OK NAME=forum.i2p VALUE=b64dest\n")
            .await
            .unwrap();
    });
    let v = i2p_name_lookup_fresh(&mut cli, "forum.i2p").await.unwrap();
    bridge.await.unwrap();
    assert_eq!(v, "b64dest");
}

#[tokio::test]
async fn open_session_keeps_socket_and_lookups_up_me() {
    use bitdaemon_net::i2p::I2pConnector;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    let (mine, theirs) = tokio::io::duplex(64 * 1024);
    let factory = Arc::new(SamFactory(tokio::sync::Mutex::new(vec![mine])));
    tokio::spawn(async move {
        let mut srv: BoxedPeerStream = Box::new(theirs);
        let _ = read_line(&mut srv).await; // HELLO
        srv.write_all(b"HELLO REPLY RESULT=OK\n").await.unwrap();
        let line = read_line(&mut srv).await; // SESSION CREATE
        assert!(line.starts_with("SESSION CREATE STYLE=STREAM ID=sess"));
        srv.write_all(b"SESSION STATUS RESULT=OK DESTINATION=BASE64DEST\n")
            .await
            .unwrap();
        // upstream: NAMING LOOKUP ME on the same socket, no HELLO
        assert_eq!(read_line(&mut srv).await, "NAMING LOOKUP NAME=ME");
        srv.write_all(b"NAMING REPLY RESULT=OK NAME=ME VALUE=my-dest\n")
            .await
            .unwrap();
        // a later name_lookup reuses the same socket, still no HELLO
        assert_eq!(read_line(&mut srv).await, "NAMING LOOKUP NAME=x.i2p");
        srv.write_all(b"NAMING REPLY RESULT=OK VALUE=x-dest\n")
            .await
            .unwrap();
    });
    let (conn, local) = I2pConnector::open_session(
        factory,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7656),
        "sess".into(),
        I2pSessionOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(local, "my-dest");
    assert_eq!(conn.local_endpoint(), "my-dest");
    let got = conn.name_lookup("x.i2p").await.unwrap();
    assert_eq!(got, "x-dest");
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

// ---------- I2pConnector / I2pAcceptor over a scripted SAM ----------

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use bitdaemon_net::transport::PeerConnector;
use tokio::io::{AsyncBufReadExt, BufReader};

struct SamFactory(tokio::sync::Mutex<Vec<tokio::io::DuplexStream>>);

impl std::fmt::Debug for SamFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SamFactory").finish()
    }
}

impl PeerConnector for SamFactory {
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
                .pop()
                .map(|s| -> BoxedPeerStream { Box::new(s) })
                .ok_or_else(|| std::io::Error::other("no sam socket queued").into())
        })
    }
}

#[tokio::test]
async fn i2p_connector_connect_dest() {
    use bitdaemon_net::i2p::I2pConnector;

    // two scripted SAM sockets: NAMING then CONNECT (each gets HELLO —
    // `I2pConnector::new` has no session socket so the lookup dials
    // a fresh SAM connection, like upstream's fallback).
    let (m1, t1) = tokio::io::duplex(64 * 1024);
    let (m2, t2) = tokio::io::duplex(64 * 1024);
    let factory = Arc::new(SamFactory(tokio::sync::Mutex::new(vec![m2, m1]))); // pop order
    tokio::spawn(async move {
        let mut r = BufReader::new(t1);
        let mut line = String::new();
        r.read_line(&mut line).await.unwrap(); // HELLO
        r.write_all(b"HELLO REPLY RESULT=OK VERSION=3.1\n")
            .await
            .unwrap();
        line.clear();
        r.read_line(&mut line).await.unwrap(); // NAMING LOOKUP
        assert!(line.contains("NAMING LOOKUP NAME=peerdest.i2p"));
        r.write_all(b"NAMING REPLY RESULT=OK NAME=peerdest VALUE=resolveddest\n")
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        let mut r = BufReader::new(t2);
        let mut line = String::new();
        r.read_line(&mut line).await.unwrap(); // HELLO
        r.write_all(b"HELLO REPLY RESULT=OK VERSION=3.1\n")
            .await
            .unwrap();
        line.clear();
        r.read_line(&mut line).await.unwrap(); // STREAM CONNECT
        assert!(
            line.contains("STREAM CONNECT ID=sess DESTINATION=resolveddest"),
            "got: {line:?}"
        );
        r.write_all(b"STREAM STATUS RESULT=OK\n").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    });

    let conn = Arc::new(I2pConnector::new(
        factory,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7656),
        "sess".into(),
    ));
    let mut s = conn.connect_dest("peerdest.i2p").await.unwrap();
    s.write_all(b"x").await.unwrap();
}
