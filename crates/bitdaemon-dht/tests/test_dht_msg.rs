// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `test_dht.cpp` — `verify_message` schema checks + BEP-44
//! item sign/verify round-trip (upstream vectors use generated
//! keypairs; the signature itself is deterministic ed25519).

use bitdaemon_bencode::{BdecodeNode, bdecode_checked};
use bitdaemon_dht::item::{
    Item, canonical_string, item_target_id_immutable, item_target_id_mutable, sign_mutable_item,
    verify_mutable_item,
};
use bitdaemon_dht::msg::{KeyDesc, key_flags, verify_message};
use sha1::Digest as _;

fn node(buf: &[u8]) -> BdecodeNode<'_> {
    bdecode_checked(buf).unwrap()
}

#[test]
fn verify_message_get_response() {
    // the get-response schema from `test_dht.cpp`
    let desc = [
        KeyDesc {
            name: b"r",
            ty: Some(bitdaemon_bencode::NodeType::Dict),
            size: 0,
            flags: key_flags::PARSE_CHILDREN,
        },
        KeyDesc {
            name: b"id",
            ty: Some(bitdaemon_bencode::NodeType::String),
            size: 20,
            flags: 0,
        },
        KeyDesc {
            name: b"token",
            ty: Some(bitdaemon_bencode::NodeType::String),
            size: 0,
            flags: 0,
        },
        KeyDesc {
            name: b"ip",
            ty: Some(bitdaemon_bencode::NodeType::String),
            size: 0,
            flags: key_flags::OPTIONAL | key_flags::LAST_CHILD,
        },
        KeyDesc {
            name: b"y",
            ty: Some(bitdaemon_bencode::NodeType::String),
            size: 1,
            flags: 0,
        },
    ];
    let good = b"d1:rd2:id20:0123456789abcdefghij5:token3:abce1:y1:re";
    let m = node(good);
    let mut ret: [BdecodeNode; 5] = std::array::from_fn(|_| BdecodeNode::none());
    let mut err = String::new();
    assert!(verify_message(&m, &desc, &mut ret, &mut err), "{err}");
    assert_eq!(ret[4].string_value(), b"r");
    assert_eq!(ret[2].string_value(), b"abc");

    // missing required `id`
    let bad = b"d1:rd5:token3:abce1:y1:re";
    let m = node(bad);
    let mut ret: [BdecodeNode; 5] = std::array::from_fn(|_| BdecodeNode::none());
    assert!(!verify_message(&m, &desc, &mut ret, &mut err));
    assert!(err.contains("id"), "{err}");

    // wrong-size `id` (19 bytes)
    let bad2 = b"d1:rd2:id19:0123456789abcdefghi5:token3:abce1:y1:re";
    let m = node(bad2);
    let mut ret: [BdecodeNode; 5] = std::array::from_fn(|_| BdecodeNode::none());
    assert!(!verify_message(&m, &desc, &mut ret, &mut err));

    // non-dict root
    let m = node(b"i3e");
    let mut ret: [BdecodeNode; 5] = std::array::from_fn(|_| BdecodeNode::none());
    assert!(!verify_message(&m, &desc, &mut ret, &mut err));
    assert!(err.contains("dictionary"));
}

#[test]
fn canonical_string_layout() {
    // "4:salt6:foobar3:seqi4e1:v<value>" — verbatim BEP-44
    let v = b"12:Hello World!";
    let cs = canonical_string(v, 4, b"foobar");
    assert_eq!(cs, b"4:salt6:foobar3:seqi4e1:v12:Hello World!".to_vec());
    // no salt
    let cs = canonical_string(v, 4, b"");
    assert_eq!(cs, b"3:seqi4e1:v12:Hello World!".to_vec());
}

#[test]
fn mutable_item_sign_verify() {
    // upstream test keypair flow: sign then verify
    let mut sk = [0u8; 64];
    for (i, b) in sk.iter_mut().enumerate() {
        *b = i as u8;
    }
    // derive pk from the 32-byte seed (ed25519-dalek keypair =
    // seed ‖ pk, matching upstream `secret_key`)
    let skp = ed25519_dalek::SigningKey::from_bytes(&sk[..32].try_into().unwrap());
    let pk = skp.verifying_key().to_bytes();
    sk[32..].copy_from_slice(&pk);

    let v = b"12:Hello World!";
    let salt = b"foobar";
    let seq = 4i64;
    let sig = sign_mutable_item(v, salt, seq, &sk).expect("sign");
    assert!(verify_mutable_item(v, salt, seq, &pk, &sig));

    // broken signature fails (upstream flips sig[4])
    let mut bad = sig;
    bad[4] ^= 0x55;
    assert!(!verify_mutable_item(v, salt, seq, &pk, &bad));
    // wrong salt fails
    assert!(!verify_mutable_item(v, b"barfoo", seq, &pk, &sig));
    // wrong seq fails
    assert!(!verify_mutable_item(v, salt, 5, &pk, &sig));
    // salt > 64 bytes never verifies
    let big = vec![b'x'; 65];
    assert!(!verify_mutable_item(v, &big, seq, &pk, &sig));
}

#[test]
fn target_ids() {
    let v = b"12:Hello World!";
    let imm = item_target_id_immutable(v);
    // sha1(v)
    let mut h = sha1::Sha1::new();
    h.update(v);
    let expect = h.finalize();
    assert_eq!(imm.as_bytes(), &expect[..]);

    let pk = [0x42u8; 32];
    let mut1 = item_target_id_mutable(b"", &pk);
    let mut2 = item_target_id_mutable(b"salt", &pk);
    assert_ne!(mut1, mut2);
}

#[test]
fn item_assign() {
    let mut sk = [0u8; 64];
    for (i, b) in sk.iter_mut().enumerate() {
        *b = 200 - i as u8;
    }
    let skp = ed25519_dalek::SigningKey::from_bytes(&sk[..32].try_into().unwrap());
    let pk = skp.verifying_key().to_bytes();
    sk[32..].copy_from_slice(&pk);

    let mut it = Item::mutable_empty(pk, b"s");
    assert!(it.assign_signed(b"i1e".to_vec(), b"s", 7, pk, &sk));
    assert!(it.is_mutable());
    assert!(it.sig_verified());
    assert_eq!(it.seq(), 7);
    assert_eq!(it.salt(), b"s");

    // verified-assign rejects a bad signature
    let mut it2 = Item::mutable_empty(pk, b"");
    let mut badsig = it.sig().copied().unwrap();
    badsig[0] ^= 1;
    assert!(!it2.assign_verified(b"i1e".to_vec(), b"s", 7, pk, badsig));
    assert!(it2.assign_verified(b"i1e".to_vec(), b"s", 7, pk, *it.sig().unwrap()));
}
