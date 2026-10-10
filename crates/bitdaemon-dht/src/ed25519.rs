// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/ed25519.cpp` — key helpers backed by
//! `ed25519-dalek` (ADR-0007; the bundled C implementation is not
//! ported). `secret_key` keeps the upstream 64-byte seed ‖ pk
//! layout.

use bitdaemon_common::random::random_bytes;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::item::{PublicKey, SecretKey, Signature64};

/// `ed25519_create_seed` — 32 random bytes.
#[must_use]
pub fn ed25519_create_seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    random_bytes(&mut seed);
    seed
}

/// `ed25519_create_keypair` — `(pk, sk)` where `sk` is seed ‖ pk.
#[must_use]
pub fn ed25519_create_keypair(seed: &[u8; 32]) -> (PublicKey, SecretKey) {
    let sk = SigningKey::from_bytes(seed);
    let pk = sk.verifying_key().to_bytes();
    let mut secret = [0u8; 64];
    secret[..32].copy_from_slice(seed);
    secret[32..].copy_from_slice(&pk);
    (pk, secret)
}

/// `ed25519_sign`.
#[must_use]
pub fn ed25519_sign(msg: &[u8], sk: &SecretKey) -> Option<Signature64> {
    let key = SigningKey::from_keypair_bytes(sk).ok()?;
    Some(key.sign(msg).to_bytes())
}

/// `ed25519_verify`.
#[must_use]
pub fn ed25519_verify(sig: &Signature64, msg: &[u8], pk: &PublicKey) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(pk) else {
        return false;
    };
    vk.verify(msg, &Signature::from_bytes(sig)).is_ok()
}
