// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/item.cpp` — BEP-44 items: `canonical_string`
//! `4:salt<len>:<salt>3:seqi<seq>e1:v<v>`, `item_target_id`,
//! `sign_mutable_item`/`verify_mutable_item` via `ed25519-dalek`
//! (ADR-0007 — the bundled C implementation is not ported).

use bitdaemon_common::hash::Sha1Hash;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha1::{Digest as _, Sha1};

/// `public_key` — 32-byte ed25519 verifying key.
pub type PublicKey = [u8; 32];
/// `secret_key` — 64-byte ed25519 signing key (seed ‖ pk).
pub type SecretKey = [u8; 64];
/// `signature`.
pub type Signature64 = [u8; 64];
/// `sequence_number`.
pub type SequenceNumber = i64;

/// `canonical_string` — the buffer an ed25519 signature covers:
/// `4:salt<len>:<salt>` (when salt non-empty) then
/// `3:seqi<seq>e1:v<v>`. `None` on buffer overflow (upstream writes
/// into a fixed 1200-byte buffer; we allocate exactly).
#[must_use]
pub fn canonical_string(v: &[u8], seq: SequenceNumber, salt: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(salt.len() + v.len() + 64);
    if !salt.is_empty() {
        out.extend_from_slice(format!("4:salt{}:", salt.len()).as_bytes());
        out.extend_from_slice(salt);
    }
    out.extend_from_slice(format!("3:seqi{seq}e1:v").as_bytes());
    out.extend_from_slice(v);
    out
}

/// `item_target_id(v)` — immutable item target = `sha1(v)`.
#[must_use]
pub fn item_target_id_immutable(v: &[u8]) -> Sha1Hash {
    let mut h = Sha1::new();
    h.update(v);
    Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default()
}

/// `item_target_id(salt, pk)` — mutable item target =
/// `sha1(pk ‖ salt)`.
#[must_use]
pub fn item_target_id_mutable(salt: &[u8], pk: &PublicKey) -> Sha1Hash {
    let mut h = Sha1::new();
    h.update(pk);
    if !salt.is_empty() {
        h.update(salt);
    }
    Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default()
}

/// `verify_mutable_item` — salt capped at 64 bytes (BEP-44).
#[must_use]
pub fn verify_mutable_item(
    v: &[u8],
    salt: &[u8],
    seq: SequenceNumber,
    pk: &PublicKey,
    sig: &Signature64,
) -> bool {
    if salt.len() > 64 {
        return false;
    }
    let msg = canonical_string(v, seq, salt);
    let Ok(vk) = VerifyingKey::from_bytes(pk) else {
        return false;
    };
    let sig = Signature::from_bytes(sig);
    vk.verify(&msg, &sig).is_ok()
}

/// `sign_mutable_item` — `sk` is the upstream 64-byte ed25519
/// secret (seed ‖ public key), matching `SigningKey::from_keypair_bytes`.
#[must_use]
pub fn sign_mutable_item(
    v: &[u8],
    salt: &[u8],
    seq: SequenceNumber,
    sk: &SecretKey,
) -> Option<Signature64> {
    let msg = canonical_string(v, seq, salt);
    let sk = SigningKey::from_keypair_bytes(sk).ok()?;
    Some(sk.sign(&msg).to_bytes())
}

/// `item` — a stored DHT item (immutable or mutable).
#[derive(Debug, Clone)]
pub struct Item {
    /// `m_value` — bencoded value bytes.
    value: Vec<u8>,
    /// `m_salt`.
    salt: Vec<u8>,
    /// `m_pk`.
    pk: Option<PublicKey>,
    /// `m_sig`.
    sig: Option<Signature64>,
    /// `m_seq`.
    seq: SequenceNumber,
    /// `m_mutable`.
    mutable: bool,
}

impl Item {
    /// `item(entry)` — immutable.
    #[must_use]
    pub fn immutable(v: Vec<u8>) -> Self {
        Self {
            value: v,
            salt: Vec::new(),
            pk: None,
            sig: None,
            seq: 0,
            mutable: false,
        }
    }

    /// `item(pk, salt)` — empty mutable.
    #[must_use]
    pub fn mutable_empty(pk: PublicKey, salt: &[u8]) -> Self {
        Self {
            value: Vec::new(),
            salt: salt.to_vec(),
            pk: Some(pk),
            sig: None,
            seq: 0,
            mutable: true,
        }
    }

    /// `assign(v, salt, seq, pk, sk)` — sign a mutable value.
    pub fn assign_signed(
        &mut self,
        v: Vec<u8>,
        salt: &[u8],
        seq: SequenceNumber,
        pk: PublicKey,
        sk: &SecretKey,
    ) -> bool {
        let Some(sig) = sign_mutable_item(&v, salt, seq, sk) else {
            return false;
        };
        self.sig = Some(sig);
        self.salt = salt.to_vec();
        self.pk = Some(pk);
        self.seq = seq;
        self.mutable = true;
        self.value = v;
        true
    }

    /// `assign(v, salt, seq, pk, sig)` — adopt a received mutable
    /// item after verifying its signature.
    pub fn assign_verified(
        &mut self,
        v: Vec<u8>,
        salt: &[u8],
        seq: SequenceNumber,
        pk: PublicKey,
        sig: Signature64,
    ) -> bool {
        if !verify_mutable_item(&v, salt, seq, &pk, &sig) {
            return false;
        }
        self.pk = Some(pk);
        self.sig = Some(sig);
        self.salt = if salt.is_empty() {
            Vec::new()
        } else {
            salt.to_vec()
        };
        self.seq = seq;
        self.mutable = true;
        self.value = v;
        true
    }

    /// `value()`.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
    /// `salt()`.
    #[must_use]
    pub fn salt(&self) -> &[u8] {
        &self.salt
    }
    /// `pk()`.
    #[must_use]
    pub fn pk(&self) -> Option<&PublicKey> {
        self.pk.as_ref()
    }
    /// `sig()`.
    #[must_use]
    pub fn sig(&self) -> Option<&Signature64> {
        self.sig.as_ref()
    }
    /// `seq()`.
    #[must_use]
    pub fn seq(&self) -> SequenceNumber {
        self.seq
    }
    /// `is_mutable()` / `is_immutable()` in one.
    #[must_use]
    pub fn is_mutable(&self) -> bool {
        self.mutable
    }
    /// `sig_verified()` — mutable and verified signature present.
    #[must_use]
    pub fn sig_verified(&self) -> bool {
        self.mutable && self.sig.is_some()
    }
}
