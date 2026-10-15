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
    DisconnectSeverity, KnownTorrent, PeerConnError, read_extended_handshake, read_message,
    run_handshake, run_handshake_responder, send_extended_handshake, write_message,
};
use bitdaemon_net::peer_wire::{PeerMessage, parse_handshake, write_handshake};
use bitdaemon_net::transport::BoxedPeerStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn pair() -> (BoxedPeerStream, BoxedPeerStream) {
    let (a, b) = tokio::io::duplex(64 * 1024);
    (Box::new(a), Box::new(b))
}

#[tokio::test]
async fn handshake_round_trip() {
    let (mut a, mut b) = pair();
    let h = tokio::join!(
        run_handshake(&mut a, &IH, &P1, true, false, false),
        run_handshake(&mut b, &IH, &P2, true, false, false),
    );
    let (ha, hb) = (h.0.unwrap(), h.1.unwrap());
    assert_eq!(ha.peer_id, *P2);
    assert_eq!(hb.peer_id, *P1);
    assert!(ha.supports_dht() && ha.supports_extension());
}

#[tokio::test]
async fn wrong_info_hash_rejected() {
    // upstream: the initiator never re-validates the echoed
    // info_hash (the `read_info_hash` ih block sits under `if (!t)`,
    // and outgoing conns have `m_torrent` from `init()`); an
    // *unknown* hash on the responder leg fails the attach with
    // `invalid_info_hash` (failure).
    let (mut a, mut b) = pair();
    let other = Sha1Hash::from_bytes(&[0xCD; 20]).unwrap();
    let h = tokio::join!(
        run_handshake(&mut a, &IH, &P1, false, false, false),
        run_handshake(&mut b, &other, &P2, false, false, false),
    );
    assert!(matches!(h.0, Ok(ref hs) if hs.info_hash == other));

    // responder rejects the unknown hash → `invalid_info_hash`;
    // dropping its stream EOFs the initiator.
    let (mut a, mut b) = pair();
    let resp =
        tokio::spawn(
            async move { run_handshake_responder(&mut b, &P2, false, false, |_| None).await },
        );
    let init = run_handshake(&mut a, &other, &P1, false, false, false).await;
    assert!(matches!(init, Err(PeerConnError::Io(_))));
    let r = resp.await.unwrap();
    assert!(matches!(r, Err(PeerConnError::WrongInfoHash(ih)) if ih == other));
}

fn kt_v1(wire: Sha1Hash) -> KnownTorrent {
    KnownTorrent {
        ih: wire,
        wire_ih: wire,
        protocol_v2: false,
        has_v2: false,
        v2_ih: None,
        valid_metadata: true,
    }
}

#[tokio::test]
async fn responder_v2_bit_flips_before_reply() {
    // upstream `read_info_hash`: a received reserved `0x10` bit on a
    // hybrid torrent sets `protocol_v2` *before* `write_handshake` —
    // the reply carries `associated_info_hash()` (the truncated v2)
    // and doesn't re-advertise the bit.
    let (mut a, mut b) = pair();
    let v2w = Sha1Hash::from_bytes(&[0x33; 20]).unwrap();
    let peer = tokio::spawn(async move {
        let bytes = write_handshake(&IH, &P1, false, true, false, true);
        a.write_all(&bytes).await.unwrap();
        let mut reply = [0u8; 68];
        a.read_exact(&mut reply).await.unwrap();
        parse_handshake(&reply).unwrap()
    });
    let lookup = move |ih: &Sha1Hash| {
        (*ih == *IH).then(|| KnownTorrent {
            v2_ih: Some(v2w),
            has_v2: true,
            ..kt_v1(*ih)
        })
    };
    let (hs, kt) = run_handshake_responder(&mut b, &P2, false, false, lookup)
        .await
        .unwrap();
    assert!(hs.supports_v2());
    assert!(kt.protocol_v2);
    assert_eq!(kt.wire_ih, v2w);
    let reply = peer.await.unwrap();
    assert_eq!(reply.info_hash, v2w);
    assert!(!reply.supports_v2());
}

#[tokio::test]
async fn responder_rejects_v2_claim_on_v1_only() {
    // `recv[7] & 0x10` on a v1-only torrent with valid metadata →
    // `invalid_info_hash` at *normal* severity (a capability claim,
    // not an attack). Metadata-less magnets accept the claim
    // speculatively instead.
    let (mut a, mut b) = pair();
    let peer = tokio::spawn(async move {
        let bytes = write_handshake(&IH, &P1, false, true, false, true);
        a.write_all(&bytes).await.unwrap();
    });
    let lookup = |ih: &Sha1Hash| (*ih == *IH).then(|| kt_v1(*ih));
    let r = run_handshake_responder(&mut b, &P2, false, false, lookup).await;
    assert!(matches!(r, Err(PeerConnError::V2OnV1)));
    let (sev, _, reason) = PeerConnError::V2OnV1.handshake_fail(false);
    assert_eq!(sev, DisconnectSeverity::Normal);
    assert_eq!(reason, "invalid_info_hash");
    peer.await.unwrap();

    // metadata-less torrent: the claim stands (it may carry v2).
    let (mut a, mut b) = pair();
    let peer = tokio::spawn(async move {
        let bytes = write_handshake(&IH, &P1, false, true, false, true);
        a.write_all(&bytes).await.unwrap();
        let mut reply = [0u8; 68];
        a.read_exact(&mut reply).await.unwrap();
        parse_handshake(&reply).unwrap()
    });
    let lookup = |ih: &Sha1Hash| {
        (*ih == *IH).then(|| KnownTorrent {
            valid_metadata: false,
            ..kt_v1(*ih)
        })
    };
    let (_, kt) = run_handshake_responder(&mut b, &P2, false, false, lookup)
        .await
        .unwrap();
    assert!(kt.protocol_v2);
    // `associated_info_hash` = `get(has_v2 && protocol_v2 ? V2 :
    // V1)` — no v2 hash → the wire hash stays v1.
    assert_eq!(kt.wire_ih, *IH);
    let _ = peer.await.unwrap();
}

#[tokio::test]
async fn extended_and_message_loop() {
    let (mut a, mut b) = pair();
    let _ = tokio::join!(
        run_handshake(&mut a, &IH, &P1, false, false, false),
        run_handshake(&mut b, &IH, &P2, false, false, false),
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
