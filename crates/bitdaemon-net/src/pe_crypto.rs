//! Port of `aux_/pe_crypto.hpp`/`pe_crypto.cpp` — the BitTorrent
//! Message Stream Encryption (MSE/PE) primitives:
//!
//! * `DhKeyExchange` — 768-bit DH (upstream's safe prime, generator 2),
//!   degenerate-key rejection verbatim (`< 2` or `>= p-1` rejected),
//!   `xor_mask` = `SHA1("req3" | S)`.
//! * `Rc4` — libTomCrypt-derived RC4 (key schedule identical to
//!   upstream `rc4_init`/`rc4_encrypt`, keystream byte-for-byte).
//! * `Rc4Handler` — two directions, 1024-byte keystream discard on key
//!   setup, `init_pe_rc4_handler` derivation (`keyA`/`keyB`).

use std::sync::OnceLock;

use num_bigint::BigUint;
use num_traits::Num;
use sha1::{Digest, Sha1};

use bitdaemon_common::Sha1Hash;

/// `dh_key_len` — 768-bit / 96-byte DH modulus width.
pub const DH_KEY_LEN: usize = 96;

/// `aux::dh_prime()` — the fixed 768-bit safe prime.
const DH_PRIME_HEX: &[u8] =
    b"FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B2251\
4A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C\
42E9A63A36210000000000090563";

fn dh_prime() -> &'static BigUint {
    static P: OnceLock<BigUint> = OnceLock::new();
    P.get_or_init(|| {
        BigUint::from_str_radix(std::str::from_utf8(DH_PRIME_HEX).expect("ascii hex"), 16)
            .expect("valid prime literal")
    })
}

fn dh_prime_minus_1() -> &'static BigUint {
    static P1: OnceLock<BigUint> = OnceLock::new();
    P1.get_or_init(|| dh_prime() - BigUint::from(1u8))
}

fn export_key(k: &BigUint) -> [u8; DH_KEY_LEN] {
    let bytes = k.to_bytes_be();
    let mut out = [0u8; DH_KEY_LEN];
    let n = bytes.len().min(DH_KEY_LEN);
    out[DH_KEY_LEN - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    out
}

fn sha1(parts: &[&[u8]]) -> Sha1Hash {
    let mut h = Sha1::new();
    for p in parts {
        h.update(p);
    }
    Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default()
}

/// `aux::dh_key_exchange`.
pub struct DhKeyExchange {
    local_secret: BigUint,
    local_key: [u8; DH_KEY_LEN],
    /// `m_dh_shared_secret`.
    shared_secret: Option<[u8; DH_KEY_LEN]>,
    /// `m_xor_mask` — `SHA1("req3" | S)`.
    xor_mask: Option<Sha1Hash>,
}

impl DhKeyExchange {
    /// `dh_key_exchange()` — 160-bit random secret, `2^s mod p`.
    #[must_use]
    pub fn new() -> Self {
        let mut random_key = [0u8; 20];
        rand::Rng::fill(&mut rand::rng(), &mut random_key);
        Self::with_secret(BigUint::from_bytes_be(&random_key))
    }

    /// Testable construction with a fixed secret.
    #[must_use]
    pub fn with_secret(local_secret: BigUint) -> Self {
        let local_key = export_key(&BigUint::from(2u8).modpow(&local_secret, dh_prime()));
        Self {
            local_secret,
            local_key,
            shared_secret: None,
            xor_mask: None,
        }
    }

    /// `get_local_key()` — DH-exported 96-byte big-endian.
    #[must_use]
    pub fn local_key(&self) -> &[u8; DH_KEY_LEN] {
        &self.local_key
    }

    /// `compute_secret(remote_pubkey)` — rejects degenerate keys
    /// (`< 2` or `>= p-1`); derives `S = remote^secret mod p` and the
    /// `req3` xor mask. Returns `false` on degenerate input.
    pub fn compute_secret(&mut self, remote_pubkey: &[u8; DH_KEY_LEN]) -> bool {
        let key = BigUint::from_bytes_be(remote_pubkey);
        if key < BigUint::from(2u8) || key >= *dh_prime_minus_1() {
            return false;
        }
        let secret = key.modpow(&self.local_secret, dh_prime());
        let s = export_key(&secret);
        self.xor_mask = Some(sha1(&[b"req3", &s]));
        self.shared_secret = Some(s);
        true
    }

    /// `get_secret()` — valid only after `compute_secret`.
    #[must_use]
    pub fn secret(&self) -> Option<&[u8; DH_KEY_LEN]> {
        self.shared_secret.as_ref()
    }

    /// `get_xor_mask()`.
    #[must_use]
    pub fn xor_mask(&self) -> Option<Sha1Hash> {
        self.xor_mask
    }
}

impl Default for DhKeyExchange {
    fn default() -> Self {
        Self::new()
    }
}

/// `aux::rc4` — RC4 state (identity perm when key is empty, matching
/// upstream's zero-len early exit).
pub struct Rc4 {
    x: u8,
    y: u8,
    /// `state->buf` — permutation of 0..=255.
    s: [u8; 256],
}

impl Rc4 {
    /// `rc4_init(key)` — key length capped at 256 bytes.
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        let mut s = [0u8; 256];
        for (i, b) in s.iter_mut().enumerate() {
            *b = i as u8;
        }
        if !key.is_empty() {
            let len = key.len().min(256);
            let mut j = 0usize;
            for i in 0..256 {
                j = (j + usize::from(s[i]) + usize::from(key[i % len])) & 0xff;
                s.swap(i, j);
            }
        }
        Self { s, x: 0, y: 0 }
    }

    /// One keystream byte (`rc4_encrypt` inner step).
    #[inline]
    fn step(&mut self) -> u8 {
        self.x = self.x.wrapping_add(1);
        self.y = self.y.wrapping_add(self.s[usize::from(self.x)]);
        self.s.swap(usize::from(self.x), usize::from(self.y));
        self.s[(self.s[usize::from(self.x)] as usize + self.s[usize::from(self.y)] as usize) & 0xff]
    }

    /// `rc4_encrypt` — XOR in place (RC4 is symmetric).
    pub fn apply(&mut self, buf: &mut [u8]) {
        for b in buf {
            *b ^= self.step();
        }
    }

    /// Produce keystream bytes.
    #[must_use]
    pub fn keystream(&mut self, n: usize) -> Vec<u8> {
        let mut out = vec![0u8; n];
        self.apply(&mut out);
        out
    }
}

/// `aux::rc4_handler` — directional RC4 with the upstream 1024-byte
/// keystream discard on key setup.
#[derive(Default)]
pub struct Rc4Handler {
    incoming: Option<Rc4>,
    outgoing: Option<Rc4>,
}

impl Rc4Handler {
    /// `set_incoming_key` — RC4 init + discard 1024 keystream bytes.
    pub fn set_incoming_key(&mut self, key: &[u8]) {
        let mut r = Rc4::new(key);
        r.apply(&mut [0u8; 1024]);
        self.incoming = Some(r);
    }

    /// `set_outgoing_key`.
    pub fn set_outgoing_key(&mut self, key: &[u8]) {
        let mut r = Rc4::new(key);
        r.apply(&mut [0u8; 1024]);
        self.outgoing = Some(r);
    }

    /// `encrypt(buf)` — in place; no-op when unset.
    pub fn encrypt(&mut self, buf: &mut [u8]) {
        if let Some(r) = self.outgoing.as_mut() {
            r.apply(buf);
        }
    }

    /// `decrypt(buf)` — in place; no-op when unset.
    pub fn decrypt(&mut self, buf: &mut [u8]) {
        if let Some(r) = self.incoming.as_mut() {
            r.apply(buf);
        }
    }

    /// `is_recv_plaintext` — no incoming crypto active.
    #[must_use]
    pub fn is_recv_plaintext(&self) -> bool {
        self.incoming.is_none()
    }

    /// `is_send_plaintext`.
    #[must_use]
    pub fn is_send_plaintext(&self) -> bool {
        self.outgoing.is_none()
    }
}

/// MSE constants — the three derivation tags.
const REQ1: &[u8; 4] = b"req1";
const REQ2: &[u8; 4] = b"req2";
const KEY_A: &[u8; 4] = b"keyA";
const KEY_B: &[u8; 4] = b"keyB";

/// `SHA1("req1" | S)` — the MSE sync hash.
#[must_use]
pub fn sync_hash(secret: &[u8]) -> Sha1Hash {
    sha1(&[REQ1, secret])
}

/// `SHA1("req2" | SKEY)` — the stream-key hash.
#[must_use]
pub fn stream_key_hash(skey: &Sha1Hash) -> Sha1Hash {
    sha1(&[REQ2, skey.as_bytes()])
}

/// `SHA1("req3" | S)` — `dh_key_exchange`'s xor mask.
#[must_use]
pub fn xor_mask(secret: &[u8]) -> Sha1Hash {
    sha1(&[b"req3", secret])
}

/// `init_pe_rc4_handler(secret, stream_key, outgoing)` — RC4 longkeys:
/// outgoing uses `keyA`/`keyB` swapped, exactly as upstream.
#[must_use]
pub fn init_pe_rc4_handler(secret: &[u8], stream_key: &Sha1Hash, outgoing: bool) -> Rc4Handler {
    let (enc_tag, dec_tag) = if outgoing {
        (KEY_A, KEY_B)
    } else {
        (KEY_B, KEY_A)
    };
    let local_key = sha1(&[enc_tag, secret, stream_key.as_bytes()]);
    let remote_key = sha1(&[dec_tag, secret, stream_key.as_bytes()]);

    let mut ret = Rc4Handler::default();
    ret.set_incoming_key(remote_key.as_bytes());
    ret.set_outgoing_key(local_key.as_bytes());
    ret
}

/// `pe_settings` (`session_settings.hpp`, deprecated ABI in upstream
/// but still the handle-level encryption knob).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeSettings {
    /// `out_enc_policy`.
    pub out_enc_policy: EncPolicy,
    /// `in_enc_policy`.
    pub in_enc_policy: EncPolicy,
    /// `allowed_enc_level` — bitmask of the [`enc_level`] bits.
    pub allowed_enc_level: u8,
    /// `prefer_rc4` — prefer RC4 when `allowed_enc_level == both`.
    pub prefer_rc4: bool,
}

/// `pe_settings::enc_policy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncPolicy {
    /// `forced` — encrypted connections only.
    Forced,
    /// `enabled` — encrypted preferred, plaintext allowed.
    Enabled,
    /// `disabled` — plaintext only.
    Disabled,
}

/// `pe_settings::enc_level` bits.
pub mod enc_level {
    /// `plaintext`.
    pub const PLAINTEXT: u8 = 1;
    /// `rc4`.
    pub const RC4: u8 = 2;
    /// `both`.
    pub const BOTH: u8 = 3;
}

impl Default for PeSettings {
    /// upstream defaults: enabled/enabled/both/false.
    fn default() -> Self {
        Self {
            out_enc_policy: EncPolicy::Enabled,
            in_enc_policy: EncPolicy::Enabled,
            allowed_enc_level: enc_level::BOTH,
            prefer_rc4: false,
        }
    }
}
