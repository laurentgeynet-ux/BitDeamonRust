// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `bt_peer_conn` — handshake exchange over a duplex stream, ext
//! handshake, and the message read/write loop.

use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::{PeerId, Sha1Hash};

static IH: std::sync::LazyLock<Sha1Hash> =
    std::sync::LazyLock::new(|| Sha1Hash::from_bytes(&[0xAB; 20]).unwrap());
static P1: std::sync::LazyLock<PeerId> =
    std::sync::LazyLock::new(|| PeerId::from_bytes(&[1; 20]).unwrap());
static P2: std::sync::LazyLock<PeerId> =
    std::sync::LazyLock::new(|| PeerId::from_bytes(&[2; 20]).unwrap());
use bitdaemon_net::bep10::ExtendedHandshake;
use bitdaemon_net::bt_peer_conn::{
    PeerConnError, read_extended_handshake, read_message, run_handshake, send_extended_handshake,
    write_message,
};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_net::transport::BoxedPeerStream;

fn pair() -> (BoxedPeerStream, BoxedPeerStream) {
    let (a, b) = tokio::io::duplex(64 * 1024);
    (Box::new(a), Box::new(b))
}

#[tokio::test]
async fn handshake_round_trip() {
    let (mut a, mut b) = pair();
    let h = tokio::join!(
        run_handshake(&mut a, &IH, &P1, true, false),
        run_handshake(&mut b, &IH, &P2, true, false),
    );
    let (ha, hb) = (h.0.unwrap(), h.1.unwrap());
    assert_eq!(ha.peer_id, *P2);
    assert_eq!(hb.peer_id, *P1);
    assert!(ha.supports_dht() && ha.supports_extension());
}

#[tokio::test]
async fn wrong_info_hash_rejected() {
    let (mut a, mut b) = pair();
    let other = Sha1Hash::from_bytes(&[0xCD; 20]).unwrap();
    let h = tokio::join!(
        run_handshake(&mut a, &IH, &P1, false, false),
        run_handshake(&mut b, &other, &P2, false, false),
    );
    assert!(matches!(h.0, Err(PeerConnError::WrongInfoHash(_))));
}

#[tokio::test]
async fn extended_and_message_loop() {
    let (mut a, mut b) = pair();
    let _ = tokio::join!(
        run_handshake(&mut a, &IH, &P1, false, false),
        run_handshake(&mut b, &IH, &P2, false, false),
    );

    // both send ext handshake
    let hs = ExtendedHandshake {
        listen_port: Some(6881),
        client_version: Some(b"BD 0.1".to_vec()),
        ..Default::default()
    };
    let f = tokio::join!(
        send_extended_handshake(&mut a, &hs, None),
        send_extended_handshake(&mut b, &hs, None),
    );
    f.0.unwrap();
    f.1.unwrap();
    let r = tokio::join!(
        read_extended_handshake(&mut a),
        read_extended_handshake(&mut b)
    );
    assert_eq!(r.0.unwrap().listen_port, Some(6881));
    assert!(r.1.is_ok());

    // message pump: have + keepalive + piece
    write_message(&mut a, &PeerMessage::Have(PieceIndex(3)))
        .await
        .unwrap();
    write_message(&mut a, &PeerMessage::Keepalive)
        .await
        .unwrap();
    write_message(
        &mut a,
        &PeerMessage::Piece {
            piece: PieceIndex(0),
            offset: 0,
            data: bytes::Bytes::from_static(b"hello"),
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        read_message(&mut b).await.unwrap(),
        PeerMessage::Have(PieceIndex(3))
    ));
    assert!(matches!(
        read_message(&mut b).await.unwrap(),
        PeerMessage::Keepalive
    ));
    match read_message(&mut b).await.unwrap() {
        PeerMessage::Piece { data, .. } => assert_eq!(&data[..], b"hello"),
        _ => panic!("expected piece"),
    }
}
