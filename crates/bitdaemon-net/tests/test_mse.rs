// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `mse` — PE1-PE4 handshake: initiator+responder on a duplex
//! stream, RC4 keystream agreement both directions, skey recovery.

use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::{PeerId, Sha1Hash};
use bitdaemon_net::bt_peer_conn::{
    HandshakeState, KnownTorrent, read_message_rc4, run_handshake_encrypted_initiator,
    run_handshake_encrypted_responder, write_message_rc4,
};
use bitdaemon_net::mse::{
    CRYPTO_PLAINTEXT, CRYPTO_RC4, MseError, mse_initiator, mse_responder, skey_candidate,
    skey_field,
};
use bitdaemon_net::pe_crypto::DhKeyExchange;
use bitdaemon_net::pe_crypto::{enc_level, init_pe_rc4_handler, stream_key_hash, sync_hash};
use bitdaemon_net::peer_wire::PeerMessage;
use bitdaemon_net::transport::BoxedPeerStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn ih() -> Sha1Hash {
    Sha1Hash::from_bytes(&[0x77; 20]).unwrap()
}

/// a v1 wire hash resolving to itself (the standalone tests have no
/// session map — the skey match carries the conn's `protocol_v2`).
fn kt(wire: Sha1Hash) -> KnownTorrent {
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
async fn mse_full_handshake_rc4() {
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let skh = stream_key_hash(&want);
    // IA rides in PE3 like upstream (len_ia = handshake_len)
    let ia: [u8; 68] = [0xcc; 68];

    let (ra, rb) = tokio::join!(
        mse_initiator(&mut a, &want, enc_level::RC4, &ia, Some(100)),
        mse_responder(
            &mut b,
            Some(50),
            move |c| if *c == skh { Some(kt(want)) } else { None },
            |provide| provide & CRYPTO_RC4,
        ),
    );
    let ra = ra.unwrap();
    let (rb, matched) = rb.unwrap();
    assert!(ra.rc4_encrypted && rb.rc4_encrypted);
    assert_eq!(matched.wire_ih, want);
    // upstream `len(ia)` path — the responder recovered the
    // piggybacked initial payload.
    assert_eq!(rb.initial_payload, ia);

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
    let ra =
        tokio::spawn(
            async move { mse_initiator(&mut a, &want, enc_level::RC4, &[], Some(0)).await },
        );
    let rb = mse_responder(&mut b, Some(0), |_| None, |p| p).await;
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
        let mut st = HandshakeState::default();
        let (hs, mse) = run_handshake_encrypted_initiator(
            &mut a,
            &ih_i,
            &pid_a,
            true,
            false,
            false,
            enc_level::RC4,
            Some(32),
            &mut st,
        )
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
        let mut st = HandshakeState::default();
        let (hs, mse) = run_handshake_encrypted_responder(
            &mut b,
            &pid_b,
            true,
            false,
            Some(16),
            move |c| if *c == skh { Some(kt(ih_r)) } else { None },
            |provide| provide & CRYPTO_RC4,
            &mut st,
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
        mse_initiator(&mut a, &want, enc_level::BOTH, &[], Some(0)),
        mse_responder(
            &mut b,
            Some(0),
            move |c| if *c == skh { Some(kt(want)) } else { None },
            |provide| provide & CRYPTO_PLAINTEXT,
        ),
    );
    let ra = ra.unwrap();
    let (rb, _) = rb.unwrap();
    assert_eq!(ra.crypto_select, CRYPTO_PLAINTEXT);
    assert!(!ra.rc4_encrypted && !rb.rc4_encrypted);
}

#[tokio::test]
async fn select_masked_by_allowed_level() {
    // upstream `crypto_field &= allowed_enc_level` on the initiator:
    // a responder selecting a level we didn't offer disconnects
    // `unsupported_encryption_mode_selected` (peer_error).
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let skh = stream_key_hash(&want);
    let (ra, rb) = tokio::join!(
        mse_initiator(&mut a, &want, enc_level::PLAINTEXT, &[], Some(0)),
        mse_responder(
            &mut b,
            Some(0),
            move |c| if *c == skh { Some(kt(want)) } else { None },
            |_| CRYPTO_RC4,
        ),
    );
    assert!(matches!(
        ra,
        Err(MseError::UnsupportedEncryptionModeSelected)
    ));
    // the responder still completed its own side (it sent PE4).
    assert!(rb.is_ok());
}

#[tokio::test]
async fn responder_select_zero_is_unsupported_mode() {
    // `crypto_select = provide & allowed == 0` on the responder →
    // `unsupported_encryption_mode` (failure) — a distinct variant
    // from the initiator's `_selected` (peer_error).
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let skh = stream_key_hash(&want);
    let ra =
        tokio::spawn(
            async move { mse_initiator(&mut a, &want, enc_level::BOTH, &[], Some(0)).await },
        );
    let rb = mse_responder(
        &mut b,
        Some(0),
        move |c| if *c == skh { Some(kt(want)) } else { None },
        |_| 0,
    )
    .await;
    assert!(matches!(rb, Err(MseError::UnsupportedEncryptionMode)));
    ra.abort();
}

#[tokio::test]
async fn initiator_rejects_oversized_len_pad() {
    // `read_pe4`: `len_pad > 512` disconnects `invalid_pad_size`
    // *before* the pad is read — craft a raw responder that
    // completes the DH/skey leg then advertises 513.
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let init =
        tokio::spawn(
            async move { mse_initiator(&mut a, &want, enc_level::BOTH, &[], Some(0)).await },
        );
    // raw responder: pubkey exchange, then read PE3, then a crafted
    // PE4 cryptofield with len_pad = 513.
    let mut remote = [0u8; 96];
    b.read_exact(&mut remote).await.unwrap();
    let mut dh = DhKeyExchange::new();
    b.write_all(dh.local_key()).await.unwrap();
    assert!(dh.compute_secret(&remote));
    let secret = *dh.secret().unwrap();
    // PE3: padA(0) + sync + skey + RC4'd cryptofield.
    let mut head = [0u8; 40];
    b.read_exact(&mut head).await.unwrap();
    assert_eq!(&head[..20], sync_hash(&secret).as_bytes());
    let mut rc4 = init_pe_rc4_handler(&secret, &want, false);
    let mut cf = [0u8; 14];
    b.read_exact(&mut cf).await.unwrap();
    rc4.decrypt(&mut cf);
    assert_eq!(&cf[..8], &[0u8; 8]);
    let len_pad = u16::from_be_bytes([cf[12], cf[13]]) as usize;
    let mut rest = vec![0u8; len_pad + 2];
    b.read_exact(&mut rest).await.unwrap();
    // crafted PE4 — VC + select + len_pad = 513, RC4'd.
    let mut pe4 = Vec::new();
    pe4.extend([0u8; 8]);
    pe4.extend(CRYPTO_PLAINTEXT.to_be_bytes());
    pe4.extend(513u16.to_be_bytes());
    rc4.encrypt(&mut pe4);
    b.write_all(&pe4).await.unwrap();
    let ra = init.await.unwrap();
    assert!(matches!(ra, Err(MseError::InvalidPadSize)));
}

#[tokio::test]
async fn responder_rejects_oversized_len_ia() {
    // `read_pe3`: `len_ia > 68` disconnects
    // `invalid_encrypt_handshake` (peer_error) — craft a raw
    // initiator whose cryptofield advertises len_ia = 70.
    let (mut a, mut b): (BoxedPeerStream, BoxedPeerStream) = {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    };
    let want = ih();
    let skh = stream_key_hash(&want);
    let resp = tokio::spawn(async move {
        mse_responder(
            &mut b,
            Some(0),
            move |c| if *c == skh { Some(kt(want)) } else { None },
            |p| p & CRYPTO_PLAINTEXT,
        )
        .await
    });
    // raw initiator: send pubkey, read responder's, then PE3.
    let mut dh = DhKeyExchange::new();
    a.write_all(dh.local_key()).await.unwrap();
    let mut remote = [0u8; 96];
    a.read_exact(&mut remote).await.unwrap();
    assert!(dh.compute_secret(&remote));
    let secret = *dh.secret().unwrap();
    a.write_all(sync_hash(&secret).as_bytes()).await.unwrap();
    a.write_all(skey_field(&secret, &want).as_bytes())
        .await
        .unwrap();
    let mut rc4 = init_pe_rc4_handler(&secret, &want, true);
    let mut cf = Vec::new();
    cf.extend([0u8; 8]);
    cf.extend(CRYPTO_PLAINTEXT.to_be_bytes());
    cf.extend(0u16.to_be_bytes()); // len_pad = 0
    cf.extend(70u16.to_be_bytes()); // len_ia = 70 > MAX_IA
    rc4.encrypt(&mut cf);
    a.write_all(&cf).await.unwrap();
    let rb = resp.await.unwrap();
    assert!(matches!(rb, Err(MseError::InvalidEncryptHandshake)));
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
