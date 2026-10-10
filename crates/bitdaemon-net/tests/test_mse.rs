// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `mse` — PE1-PE4 handshake: initiator+responder on a duplex
//! stream, RC4 keystream agreement both directions, skey recovery.

use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::{PeerId, Sha1Hash};
use bitdaemon_net::bt_peer_conn::{
    read_message_rc4, run_handshake_encrypted_initiator, run_handshake_encrypted_responder,
    write_message_rc4,
};
use bitdaemon_net::mse::{
    CRYPTO_PLAINTEXT, CRYPTO_RC4, MseError, mse_initiator, mse_responder, skey_candidate,
    skey_field,
};
use bitdaemon_net::pe_crypto::{init_pe_rc4_handler, stream_key_hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_net::transport::BoxedPeerStream;

fn ih() -> Sha1Hash {
    Sha1Hash::from_bytes(&[0x77; 20]).unwrap()
}

#[tokio::test]
async fn mse_full_handshake_rc4() {
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let skh = stream_key_hash(&want);

    let (ra, rb) = tokio::join!(
        mse_initiator(&mut a, &want, CRYPTO_RC4, 100),
        mse_responder(
            &mut b,
            50,
            move |c| if *c == skh { Some(want) } else { None },
            |provide| provide & CRYPTO_RC4,
        ),
    );
    let ra = ra.unwrap();
    let rb = rb.unwrap();
    assert!(ra.rc4_encrypted && rb.rc4_encrypted);

    // keystream agreement: encrypt on A → decrypt on B
    let mut buf = *b"hello peer world!!!!";
    let mut rc4_a = ra.rc4;
    let mut rc4_b = rb.rc4;
    rc4_a.encrypt(&mut buf);
    rc4_b.decrypt(&mut buf);
    assert_eq!(&buf, b"hello peer world!!!!");
    // reverse direction
    rc4_b.encrypt(&mut buf);
    rc4_a.decrypt(&mut buf);
    assert_eq!(&buf, b"hello peer world!!!!");
}

#[tokio::test]
async fn bad_skey_rejected() {
    // real initiator on `a`, responder with a find_skey that fails
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let ra = tokio::spawn(async move { mse_initiator(&mut a, &want, CRYPTO_RC4, 0).await });
    let rb = mse_responder(&mut b, 0, |_| None, |p| p).await;
    assert!(matches!(rb, Err(MseError::UnknownInfoHash)));
    // initiator never gets PE4 — abort it (upstream disconnects)
    ra.abort();
}

#[tokio::test]
async fn bt_conn_over_mse() {
    // full stack: MSE PE1-PE4 → BT handshake → encrypted messages
    let (a, b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let skh = stream_key_hash(&want);
    let pid_a = PeerId::from_bytes(&[0xAA; 20]).unwrap();
    let pid_b = PeerId::from_bytes(&[0xBB; 20]).unwrap();
    let ih_i = want;
    let ih_r = want;

    let initiator = tokio::spawn(async move {
        let mut a = a;
        let (hs, mse) =
            run_handshake_encrypted_initiator(&mut a, &ih_i, &pid_a, true, false, CRYPTO_RC4, 32)
                .await
                .unwrap();
        assert_eq!(hs.peer_id, pid_b);
        // encrypted have → responder
        let mut rc4 = mse.rc4;
        write_message_rc4(&mut a, &mut rc4, &PeerMessage::Have(PieceIndex(7)))
            .await
            .unwrap();
        let m = read_message_rc4(&mut a, &mut rc4).await.unwrap();
        assert!(matches!(m, PeerMessage::Have(PieceIndex(9))));
    });
    let responder = async move {
        let mut b = b;
        let (hs, mse) = run_handshake_encrypted_responder(
            &mut b,
            &pid_b,
            true,
            false,
            16,
            move |c| if *c == skh { Some(ih_r) } else { None },
            |provide| provide & CRYPTO_RC4,
        )
        .await
        .unwrap();
        assert_eq!(hs.peer_id, pid_a);
        let mut rc4 = mse.rc4;
        let m = read_message_rc4(&mut b, &mut rc4).await.unwrap();
        assert!(matches!(m, PeerMessage::Have(PieceIndex(7))));
        write_message_rc4(&mut b, &mut rc4, &PeerMessage::Have(PieceIndex(9)))
            .await
            .unwrap();
    };
    let (r1, ()) = tokio::join!(initiator, responder);
    r1.unwrap();
}

#[tokio::test]
async fn mse_plaintext_selected() {
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let skh = stream_key_hash(&want);
    let (ra, rb) = tokio::join!(
        mse_initiator(&mut a, &want, CRYPTO_RC4 | CRYPTO_PLAINTEXT, 0),
        mse_responder(
            &mut b,
            0,
            move |c| if *c == skh { Some(want) } else { None },
            |provide| provide & CRYPTO_PLAINTEXT,
        ),
    );
    let ra = ra.unwrap();
    let rb = rb.unwrap();
    assert_eq!(ra.crypto_select, CRYPTO_PLAINTEXT);
    assert!(!ra.rc4_encrypted && !rb.rc4_encrypted);
}

#[test]
fn skey_field_round_trip() {
    let secret = [42u8; 96];
    let f = skey_field(&secret, &ih());
    let c = skey_candidate(&secret, &f);
    assert_eq!(c, stream_key_hash(&ih()));
}

#[test]
fn rc4_longkeys_match_upstream_layout() {
    // outgoing encrypt key = SHA1("keyA"|S|SKEY); incoming = keyB
    let secret = [7u8; 96];
    let ih = ih();
    let mut out = init_pe_rc4_handler(&secret, &ih, true);
    let mut inc = init_pe_rc4_handler(&secret, &ih, false);
    let mut buf = [0xABu8; 32];
    out.encrypt(&mut buf);
    inc.decrypt(&mut buf);
    assert_eq!(buf, [0xAB; 32]);
}
