// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! MSE handshake — `write_pe1_2_dhkey`/`write_pe3_sync`/
//! `write_pe4_sync` + the incoming halves. BEP-8 message flow:
//!
//! ```text
//! A → B: [DH_A 96][padA]                                      (PE1)
//! B → A: [DH_B 96][padB]                                      (PE2)
//! A → B: [sync 20][skey 20] + RC4([VC 8][provide 4]
//!        [len_pad 2][pad][len_ia 2])                          (PE3)
//! B → A: RC4([VC 8][select 4][len_pad 2][pad])                (PE4)
//! ```
//!
//! Derivations are `pe_crypto`'s — `sync_hash`, `stream_key_hash`,
//! `xor_mask`, `init_pe_rc4_handler` (verbatim upstream).

use bitdaemon_common::Sha1Hash;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::pe_crypto::{
    DH_KEY_LEN, DhKeyExchange, Rc4Handler, init_pe_rc4_handler, stream_key_hash, sync_hash,
    xor_mask,
};
use crate::transport::BoxedPeerStream;

/// `pe_plaintext`/`pe_rc4` crypto-field values (`crypto_select`).
pub const CRYPTO_PLAINTEXT: u32 = 0x01;
/// see above.
pub const CRYPTO_RC4: u32 = 0x02;

/// Max pad length upstream emits (`random(512)`).
pub const MAX_PAD: usize = 512;

/// MSE handshake errors.
#[derive(Debug, thiserror::Error)]
pub enum MseError {
    /// transport.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// bad DH secret (public key out of range).
    #[error("dh key exchange failed")]
    DhFailed,
    /// `synchash` mismatch — wrong secret or not MSE.
    #[error("sync hash mismatch")]
    BadSync,
    /// `VC` wrong after RC4 decrypt — stream desync.
    #[error("verification constant mismatch")]
    BadVc,
    /// `crypto_select` unsupported (`0` or unknown).
    #[error("no supported crypto")]
    NoCrypto,
    /// no torrent matches the skey hash.
    #[error("unknown info_hash")]
    UnknownInfoHash,
}

/// `random_bytes` for pads — injectable for tests.
#[must_use]
pub fn random_pad(n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    bitdaemon_common::random::random_bytes(&mut v);
    v
}

/// `hash('req2'|SKEY) ^ hash('req3'|S)` — the PE3 skey field.
#[must_use]
pub fn skey_field(secret: &[u8], skey: &Sha1Hash) -> Sha1Hash {
    let skh = stream_key_hash(skey);
    let mask = xor_mask(secret);
    let mut out = [0u8; 20];
    for (i, b) in out.iter_mut().enumerate() {
        *b = skh.as_bytes()[i] ^ mask.as_bytes()[i];
    }
    Sha1Hash::from_bytes(&out).unwrap_or_default()
}

/// `field ^ hash('req3'|S)` — recover `sha1('req2'|SKEY)` from a
/// received PE3 skey field (responder side).
#[must_use]
pub fn skey_candidate(secret: &[u8], field: &Sha1Hash) -> Sha1Hash {
    let mask = xor_mask(secret);
    let mut out = [0u8; 20];
    for (i, b) in out.iter_mut().enumerate() {
        *b = field.as_bytes()[i] ^ mask.as_bytes()[i];
    }
    Sha1Hash::from_bytes(&out).unwrap_or_default()
}

/// `write_pe_vc_cryptofield` — `[VC 8][crypto 4][len_pad 2][pad][len_ia 2?]`.
fn vc_cryptofield(crypto: u32, pad: &[u8], outgoing: bool) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + 4 + 2 + pad.len() + 2);
    v.extend([0u8; 8]); // VC
    v.extend(crypto.to_be_bytes());
    v.extend((pad.len() as u16).to_be_bytes());
    v.extend(pad);
    if outgoing {
        v.extend(0u16.to_be_bytes()); // len(IA) = 0
    }
    v
}

/// Result of either handshake — RC4 state + negotiated crypto.
pub struct MseResult {
    /// the initialized RC4 handler (both directions keyed).
    pub rc4: Rc4Handler,
    /// `crypto_select` negotiated.
    pub crypto_select: u32,
    /// whether the negotiated crypto is rc4 (`m_rc4_encrypted`).
    pub rc4_encrypted: bool,
    /// IA — the second handshake's bytes the initiator piggybacked
    /// (responder side only; `len(ia)` > 0).
    pub initial_payload: Vec<u8>,
}

/// PE1+PE3 — the **initiator** side: send our DH key, read theirs,
/// compute S, send sync/skey + RC4 cryptofield, read PE4.
/// `pad_len` ∈ 0..=512 (upstream `random(512)`).
pub async fn mse_initiator(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    crypto_provide: u32,
    pad_len: usize,
) -> Result<MseResult, MseError> {
    // PE1 — send pub + pad
    let mut dh = DhKeyExchange::new();
    s.write_all(dh.local_key()).await?;
    s.write_all(&random_pad(pad_len.min(MAX_PAD))).await?;

    // PE2 — read remote pub
    let mut remote = [0u8; DH_KEY_LEN];
    s.read_exact(&mut remote).await?;
    if !dh.compute_secret(&remote) {
        return Err(MseError::DhFailed);
    }
    let secret = *dh.secret().ok_or(MseError::DhFailed)?;

    // PE3 — sync + skey (plaintext), then RC4 cryptofield
    s.write_all(sync_hash(&secret).as_bytes()).await?;
    s.write_all(skey_field(&secret, info_hash).as_bytes())
        .await?;
    let mut rc4 = init_pe_rc4_handler(&secret, info_hash, true);
    let pad = random_pad(pad_len.min(MAX_PAD));
    let mut cf = vc_cryptofield(crypto_provide, &pad, true);
    rc4.encrypt(&mut cf);
    s.write_all(&cf).await?;

    // PE4 — padB (plaintext, ≤512) precedes the RC4'd cryptofield.
    // upstream `read_pe_syncvc`: scan the raw stream for the 8
    // incoming-keystream bytes (= RC4(VC)); a probe handler yields
    // the pattern without consuming the real cipher's keystream.
    let mut probe = init_pe_rc4_handler(&secret, info_hash, true);
    let mut pattern = [0u8; 8];
    probe.decrypt(&mut pattern);
    let mut scanned = 0usize;
    let mut window = [0u8; 8];
    loop {
        let mut byte = [0u8; 1];
        s.read_exact(&mut byte).await?;
        window.rotate_left(1);
        window[7] = byte[0];
        scanned += 1;
        if scanned >= 8 && window == pattern {
            break;
        }
        if scanned >= MAX_PAD + 8 {
            return Err(MseError::BadVc);
        }
    }
    // VC found — its ciphertext is the start of the RC4 stream.
    let mut vc = pattern;
    rc4.decrypt(&mut vc);
    if vc != [0u8; 8] {
        return Err(MseError::BadVc);
    }
    let mut hdr = [0u8; 6];
    s.read_exact(&mut hdr).await?;
    rc4.decrypt(&mut hdr);
    let select = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    if select & 0x03 == 0 {
        return Err(MseError::NoCrypto);
    }
    let len_pad = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
    if len_pad > 0 {
        let mut pad_in = vec![0u8; len_pad];
        s.read_exact(&mut pad_in).await?;
        rc4.decrypt(&mut pad_in);
    }
    Ok(MseResult {
        rc4,
        crypto_select: select,
        rc4_encrypted: select == CRYPTO_RC4,
        initial_payload: Vec::new(),
    })
}

/// PE1+PE2+PE4 — the **responder** side: read DH, send ours,
/// verify the sync hash, recover the info-hash via `find_skey`
/// (upstream probes every torrent's SKEY), read PE3's cryptofield,
/// pick `crypto_select` via `select(provide)`, send PE4.
/// Returns the piggybacked IA bytes in `initial_payload`.
pub async fn mse_responder(
    s: &mut BoxedPeerStream,
    pad_len: usize,
    find_skey: impl Fn(&Sha1Hash) -> Option<Sha1Hash>,
    select: impl Fn(u32) -> u32,
) -> Result<MseResult, MseError> {
    // read their pub, send ours + pad
    let mut remote = [0u8; DH_KEY_LEN];
    s.read_exact(&mut remote).await?;
    let mut dh = DhKeyExchange::new();
    s.write_all(dh.local_key()).await?;
    s.write_all(&random_pad(pad_len.min(MAX_PAD))).await?;
    if !dh.compute_secret(&remote) {
        return Err(MseError::DhFailed);
    }
    let secret = *dh.secret().ok_or(MseError::DhFailed)?;

    // PE3 — padA (plaintext, ≤512) precedes the sync hash; upstream
    // `read_pe_synchash` scans the stream for hash('req1'|S).
    let want = sync_hash(&secret);
    let mut window = [0u8; 20];
    let mut scanned = 0usize;
    let mut found = false;
    while scanned < 20 + MAX_PAD {
        let mut byte = [0u8; 1];
        s.read_exact(&mut byte).await?;
        window.rotate_left(1);
        window[19] = byte[0];
        scanned += 1;
        if scanned >= 20 && &window == want.as_bytes() {
            found = true;
            break;
        }
    }
    if !found {
        return Err(MseError::BadSync);
    }
    // skey field → candidate = sha1('req2'|SKEY); match a torrent
    let mut field = [0u8; 20];
    s.read_exact(&mut field).await?;
    let candidate = skey_candidate(&secret, &Sha1Hash::from_bytes(&field).unwrap_or_default());
    let ih = find_skey(&candidate).ok_or(MseError::UnknownInfoHash)?;

    // RC4'd [VC][provide][len_pad][pad][len_ia][ia]
    let mut rc4 = init_pe_rc4_handler(&secret, &ih, false);
    let mut hdr = [0u8; 14];
    s.read_exact(&mut hdr).await?;
    rc4.decrypt(&mut hdr);
    if hdr[..8] != [0u8; 8] {
        return Err(MseError::BadVc);
    }
    let provide = u32::from_be_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]);
    let len_pad = u16::from_be_bytes([hdr[12], hdr[13]]) as usize;
    let mut tail = vec![0u8; len_pad + 2];
    s.read_exact(&mut tail).await?;
    rc4.decrypt(&mut tail);
    let ia_len = u16::from_be_bytes([tail[len_pad], tail[len_pad + 1]]) as usize;
    let mut initial_payload = vec![0u8; ia_len];
    if ia_len > 0 {
        s.read_exact(&mut initial_payload).await?;
        rc4.decrypt(&mut initial_payload);
    }

    // PE4 — select + send
    let crypto_select = select(provide);
    if crypto_select & 0x03 == 0 {
        return Err(MseError::NoCrypto);
    }
    let pad = random_pad(pad_len.min(MAX_PAD));
    let mut cf = vc_cryptofield(crypto_select, &pad, false);
    rc4.encrypt(&mut cf);
    s.write_all(&cf).await?;
    Ok(MseResult {
        rc4,
        crypto_select,
        rc4_encrypted: crypto_select == CRYPTO_RC4,
        initial_payload,
    })
}
