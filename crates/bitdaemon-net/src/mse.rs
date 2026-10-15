// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of the MSE half of `bt_peer_connection.cpp` +
//! `pe_crypto.cpp` (DH-768, RC4 key streams, PE1–PE4).
//!
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

use crate::bt_peer_conn::KnownTorrent;
use crate::pe_crypto::{
    DH_KEY_LEN, DhKeyExchange, Rc4Handler, init_pe_rc4_handler, stream_key_hash, sync_hash,
    xor_mask,
};
use crate::transport::BoxedPeerStream;

/// `pe_plaintext`/`pe_rc4` crypto-field values (`crypto_select`).
pub const CRYPTO_PLAINTEXT: u32 = 0x01;
/// see above.
pub const CRYPTO_RC4: u32 = 0x02;

/// Pad bound upstream uses (`random(512)` = `uniform_int_distribution(0,
/// 512)` — **inclusive**, so 512-byte pads are emitted) and the
/// sync-scan limit (`m_sync_bytes_read >= 512` → disconnect).
pub const MAX_PAD: usize = 512;

/// `handshake_len` cap upstream applies to `len_ia`
/// (`errors::invalid_encrypt_handshake` past 68 — the BT handshake
/// length; `invalid_pad_size` is the `len_pad` bound only).
pub const MAX_IA: usize = 68;

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
    /// `errors::unsupported_encryption_mode_selected` — **initiator**:
    /// the peer's `crypto_select` has no bit we allow after masking
    /// with `allowed_enc_level` (upstream severity: `peer_error`).
    #[error("unsupported encryption mode selected")]
    UnsupportedEncryptionModeSelected,
    /// `errors::unsupported_encryption_mode` — **responder**: the
    /// negotiated `crypto_select` is `0` or outside `{1, 2}` after
    /// `provide & allowed_enc_level` reduction (upstream severity:
    /// `failure`).
    #[error("unsupported encryption mode")]
    UnsupportedEncryptionMode,
    /// no torrent matches the skey hash.
    #[error("unknown info_hash")]
    UnknownInfoHash,
    /// `errors::invalid_pad_size` — `len_pad` > `MAX_PAD` on the
    /// wire (`len_pad < 0` upstream is subsumed: `u16` ≥ 0x8000
    /// exceeds the bound too).
    #[error("invalid pad size")]
    InvalidPadSize,
    /// `errors::invalid_encrypt_handshake` — `len_ia` > `MAX_IA`.
    #[error("invalid encryption handshake")]
    InvalidEncryptHandshake,
}

/// `random_bytes` for pads — injectable for tests.
#[must_use]
pub fn random_pad(n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    bitdaemon_common::random::random_bytes(&mut v);
    v
}

/// One pad draw — upstream calls `random(512)` per pad (PadA/B/C/D
/// each draw independently; `random` is inclusive → 0..=512).
/// `pinned` overrides the draw for tests.
fn draw_pad(pinned: Option<usize>) -> Vec<u8> {
    let n = pinned.unwrap_or_else(|| bitdaemon_common::random::random(MAX_PAD as u32) as usize);
    random_pad(n.min(MAX_PAD))
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
/// `len_ia` is written only on the initiating side — its value is the
/// IA length (`handshake_len` upstream), not 0.
fn vc_cryptofield(crypto: u32, pad: &[u8], ia_len: Option<u16>) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + 4 + 2 + pad.len() + 2);
    v.extend([0u8; 8]); // VC
    v.extend(crypto.to_be_bytes());
    v.extend((pad.len() as u16).to_be_bytes());
    v.extend(pad);
    if let Some(ia) = ia_len {
        v.extend(ia.to_be_bytes());
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
    /// the wire info-hash the handshake ran under — responder: the
    /// skey-matched hash (v2-truncated for a v2 conn, upstream
    /// `associated_info_hash()` after `find_encrypted_torrent`);
    /// initiator: the requested hash (v2-truncated when the swarm
    /// entry was `protocol_v2`-flagged).
    pub ih: Sha1Hash,
    /// IA — the second handshake's bytes the initiator piggybacked
    /// (responder side only; `len(ia)` > 0).
    pub initial_payload: Vec<u8>,
}

/// PE1+PE3 — the **initiator** side: send our DH key, read theirs,
/// compute S, send sync/skey + RC4 cryptofield, read PE4.
/// `ia` is the initial payload — upstream piggybacks the 68-byte BT
/// handshake (`len_ia = handshake_len`), sent RC4-encrypted right
/// after the cryptofield. `allowed_enc_level` is the sanitized
/// `pe_settings` mask: it is written verbatim as `crypto_provide`
/// and masks the peer's `crypto_select` before validation (upstream
/// `crypto_field &= allowed_enc_level` — an offered level we didn't
/// allow disconnects `unsupported_encryption_mode_selected`).
/// `pad` pins the pad draws for tests (`None` = `random(512)` per
/// pad, as upstream).
pub async fn mse_initiator(
    s: &mut BoxedPeerStream,
    info_hash: &Sha1Hash,
    allowed_enc_level: u8,
    ia: &[u8],
    pad: Option<usize>,
) -> Result<MseResult, MseError> {
    // PE1 — send pub + pad
    let mut dh = DhKeyExchange::new();
    s.write_all(dh.local_key()).await?;
    s.write_all(&draw_pad(pad)).await?;

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
    let pad = draw_pad(pad);
    let ia_len = u16::try_from(ia.len()).unwrap_or(MAX_IA as u16);
    let mut cf = vc_cryptofield(u32::from(allowed_enc_level), &pad, Some(ia_len));
    rc4.encrypt(&mut cf);
    s.write_all(&cf).await?;
    // len(IA) announces the BT handshake; it follows RC4'd.
    if ia_len > 0 {
        let mut enc_ia = ia[..usize::from(ia_len)].to_vec();
        rc4.encrypt(&mut enc_ia);
        s.write_all(&enc_ia).await?;
    }

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
    // upstream `crypto_field &= allowed_enc_level` — a peer selecting
    // a level we didn't offer is `unsupported_encryption_mode_-
    // selected`; a multi-bit select collapses to the allowed bits
    // (`3 & rc4-only == rc4`).
    let select =
        u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) & u32::from(allowed_enc_level);
    if select == 0 {
        return Err(MseError::UnsupportedEncryptionModeSelected);
    }
    let len_pad = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
    // upstream refuses `len_pad < 0 || > 512` *before* reading the
    // pad (`invalid_pad_size`, peer_error).
    if len_pad > MAX_PAD {
        return Err(MseError::InvalidPadSize);
    }
    if len_pad > 0 {
        let mut pad_in = vec![0u8; len_pad];
        s.read_exact(&mut pad_in).await?;
        rc4.decrypt(&mut pad_in);
    }
    Ok(MseResult {
        rc4,
        crypto_select: select,
        rc4_encrypted: select == CRYPTO_RC4,
        ih: *info_hash,
        initial_payload: Vec::new(),
    })
}

/// PE1+PE2+PE4 — the **responder** side: read DH, send ours,
/// verify the sync hash, recover the info-hash via `find_skey`
/// (upstream probes every torrent's SKEY over *both* the v1 and the
/// truncated v2 hash — `find_encrypted_torrent`), read PE3's
/// cryptofield, pick `crypto_select` via `select(provide)`, send
/// PE4. `pad` pins the PadB/PadD draws for tests (`None` =
/// `random(512)` per pad, as upstream). Returns the negotiated
/// [`MseResult`] plus the matched [`KnownTorrent`] (the wire hash is
/// also in `MseResult::ih`).
pub async fn mse_responder(
    s: &mut BoxedPeerStream,
    pad: Option<usize>,
    find_skey: impl FnMut(&Sha1Hash) -> Option<KnownTorrent>,
    select: impl Fn(u32) -> u32,
) -> Result<(MseResult, KnownTorrent), MseError> {
    mse_responder_prefix(s, &[], pad, find_skey, select).await
}

/// `mse_responder` with the head of the remote DH key already
/// consumed — upstream `on_receive` reads 20 bytes for the
/// plaintext-handshake probe, then re-interprets them as the start
/// of the DH key when they aren't `"BitTorrent protocol"`.
pub async fn mse_responder_prefix(
    s: &mut BoxedPeerStream,
    dh_prefix: &[u8],
    pad: Option<usize>,
    mut find_skey: impl FnMut(&Sha1Hash) -> Option<KnownTorrent>,
    select: impl Fn(u32) -> u32,
) -> Result<(MseResult, KnownTorrent), MseError> {
    // read their pub (continuing after the probe prefix), send ours + pad
    if dh_prefix.len() > DH_KEY_LEN {
        return Err(MseError::BadSync);
    }
    let mut remote = [0u8; DH_KEY_LEN];
    remote[..dh_prefix.len()].copy_from_slice(dh_prefix);
    s.read_exact(&mut remote[dh_prefix.len()..]).await?;
    let mut dh = DhKeyExchange::new();
    s.write_all(dh.local_key()).await?;
    s.write_all(&draw_pad(pad)).await?;
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
    // upstream `find_encrypted_torrent` also records whether the
    // match came through the v2 index (`protocol_v2` on the conn).
    let kt = find_skey(&candidate).ok_or(MseError::UnknownInfoHash)?;
    let ih = kt.wire_ih;

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
    // upstream refuses `len_pad < 0 || > 512` before reading the pad
    // (`invalid_pad_size`, peer_error) — not after a 64 KiB read.
    if len_pad > MAX_PAD {
        return Err(MseError::InvalidPadSize);
    }
    let mut tail = vec![0u8; len_pad + 2];
    s.read_exact(&mut tail).await?;
    rc4.decrypt(&mut tail);
    let ia_len = u16::from_be_bytes([tail[len_pad], tail[len_pad + 1]]) as usize;
    // upstream disconnects on len_ia > 68 with
    // `invalid_encrypt_handshake` (peer_error) — the IA is a BT
    // handshake, nothing larger is legitimate; `invalid_pad_size`
    // is the `len_pad` bound only.
    if ia_len > MAX_IA {
        return Err(MseError::InvalidEncryptHandshake);
    }
    let mut initial_payload = vec![0u8; ia_len];
    if ia_len > 0 {
        s.read_exact(&mut initial_payload).await?;
        rc4.decrypt(&mut initial_payload);
    }

    // PE4 — select + send. Upstream computes `crypto_select =
    // provide & allowed_enc_level` reduced to a single bit by
    // `prefer_rc4` (the `select` closure's job); `0` disconnects
    // `unsupported_encryption_mode` (failure). A multi-bit value
    // means the closure skipped the reduction — not a writable PE4.
    let crypto_select = select(provide);
    if !matches!(crypto_select, CRYPTO_PLAINTEXT | CRYPTO_RC4) {
        return Err(MseError::UnsupportedEncryptionMode);
    }
    let pad = draw_pad(pad);
    let mut cf = vc_cryptofield(crypto_select, &pad, None);
    rc4.encrypt(&mut cf);
    s.write_all(&cf).await?;
    Ok((
        MseResult {
            rc4,
            crypto_select,
            rc4_encrypted: crypto_select == CRYPTO_RC4,
            ih,
            initial_payload,
        },
        kt,
    ))
}
