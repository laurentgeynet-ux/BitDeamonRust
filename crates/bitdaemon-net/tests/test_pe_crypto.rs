//! Conformance tests for `pe_crypto` — ported from upstream
//! `test_pe_crypto.cpp`: DH-768 shared secret agreement, degenerate-key
//! rejection (BEP 8 security fix), RC4 handler round-trip, RFC 6229
//! keystream vectors at offsets 1024/4096 (the handler discards the
//! first 1024 bytes on key setup — upstream behavior verbatim).

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::hex::from_hex;
use bitdaemon_net::pe_crypto::{DhKeyExchange, Rc4Handler, init_pe_rc4_handler};
use sha1::{Digest, Sha1};

fn sha1_of(b: &[u8]) -> Sha1Hash {
    let mut h = Sha1::new();
    h.update(b);
    Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default()
}

#[test]
fn diffie_hellman() {
    for _ in 0..16 {
        let mut dh1 = DhKeyExchange::new();
        let mut dh2 = DhKeyExchange::new();

        assert!(dh1.compute_secret(dh2.local_key()));
        assert!(dh2.compute_secret(dh1.local_key()));

        assert_eq!(dh1.secret(), dh2.secret());
        // xor masks agree too
        assert_eq!(dh1.xor_mask(), dh2.xor_mask());
    }
}

/// `test_diffie_hellman_degenerate_key` — public keys outside
/// `[2, p-2]` must be rejected.
#[test]
fn diffie_hellman_degenerate_key() {
    let prime_hex: &[u8] = b"FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020\
BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D\
6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563";
    let pv = from_hex(prime_hex).expect("hex");
    assert_eq!(pv.len(), 96);
    let mut dh_prime = [0u8; 96];
    dh_prime.copy_from_slice(&pv);

    // subtract a small value from a 96-byte BE buffer
    fn decremented(mut v: [u8; 96], n: u16) -> [u8; 96] {
        let mut n = n;
        for i in (0..96).rev() {
            if n == 0 {
                break;
            }
            let d = i32::from(v[i]) - i32::from(n);
            v[i] = d as u8;
            n = u16::from(d < 0);
        }
        v
    }
    fn small(v: u8) -> [u8; 96] {
        let mut r = [0u8; 96];
        r[95] = v;
        r
    }

    for key in [small(0), small(1), decremented(dh_prime, 1), dh_prime] {
        assert!(
            !DhKeyExchange::new().compute_secret(&key),
            "degenerate key accepted"
        );
    }
    for key in [small(2), decremented(dh_prime, 2)] {
        assert!(
            DhKeyExchange::new().compute_secret(&key),
            "boundary rejected"
        );
    }
}

/// `test_rc4` — two handlers keyed opposite directions.
#[test]
fn rc4_handler_round_trip() {
    let k1 = sha1_of(b"test1_key");
    let k2 = sha1_of(b"test2_key");

    let mut a = Rc4Handler::default();
    a.set_incoming_key(k2.as_bytes());
    a.set_outgoing_key(k1.as_bytes());
    let mut b = Rc4Handler::default();
    b.set_incoming_key(k1.as_bytes());
    b.set_outgoing_key(k2.as_bytes());

    let plain: Vec<u8> = (0..1024 * 40).map(|i| (i * 31) as u8).collect();
    let mut buf = plain.clone();
    // encrypt split across several calls (state continuity)
    for chunk in buf.chunks_mut(777) {
        a.encrypt(chunk);
    }
    assert_ne!(buf, plain);

    let mut out = buf.clone();
    for chunk in out.chunks_mut(999) {
        b.decrypt(chunk);
    }
    assert_eq!(out, plain);
}

/// `init_pe_rc4_handler` — outgoing/incoming key swap agrees.
#[test]
fn pe_rc4_key_derivation() {
    let secret = [0x5au8; 96];
    let skey = sha1_of(b"skey");

    let mut outgoing = init_pe_rc4_handler(&secret, &skey, true);
    let mut incoming = init_pe_rc4_handler(&secret, &skey, false);

    let plain = vec![0xabu8; 2048];
    let mut buf = plain.clone();
    outgoing.encrypt(&mut buf);
    incoming.decrypt(&mut buf);
    assert_eq!(buf, plain);
}

/// `test_rc4_rfc6229_vectors` — 16 bytes at offsets 1024 and 4096 for
/// 40/128/256-bit keys, every split of the 16 bytes across two calls.
#[test]
fn rc4_rfc6229_vectors() {
    struct V {
        key_hex: &'static str,
        at_1024: &'static str,
        at_4096: &'static str,
    }
    let vectors = [
        V {
            key_hex: "0102030405",
            at_1024: "30abbcc7c20b01609f23ee2d5f6bb7df",
            at_4096: "ff25b58995996707e51fbdf08b34d875",
        },
        V {
            key_hex: "0102030405060708090a0b0c0d0e0f10",
            at_1024: "bdf0324e6083dcc6d3cedd3ca8c53c16",
            at_4096: "a36a4c301ae8ac13610ccbc12256cacc",
        },
        V {
            key_hex: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
            at_1024: "7fec5bfd9f9b89ce6548309092d7e958",
            at_4096: "f3e4c0a2e02d1d01f7f0a74618af2b48",
        },
    ];

    for v in vectors {
        let key = from_hex(v.key_hex.as_bytes()).expect("key hex");
        let exp_1024 = from_hex(v.at_1024.as_bytes()).expect("hex");
        let exp_4096 = from_hex(v.at_4096.as_bytes()).expect("hex");

        for split in 0..=16usize {
            let mut rc4 = Rc4Handler::default();
            rc4.set_outgoing_key(&key);
            let mut buf = [0u8; 16];
            // 16 bytes split across two encrypt calls
            rc4.encrypt(&mut buf[..split]);
            rc4.encrypt(&mut buf[split..]);
            assert_eq!(&buf[..], &exp_1024[..], "1024 split {split}");
        }

        for split in 0..=16usize {
            let mut rc4 = Rc4Handler::default();
            rc4.set_outgoing_key(&key);
            // handler already discarded 1024; skip to 4096
            let mut discard = vec![0u8; 4096 - 1024];
            rc4.encrypt(&mut discard);
            let mut buf = [0u8; 16];
            rc4.encrypt(&mut buf[..split]);
            rc4.encrypt(&mut buf[split..]);
            assert_eq!(&buf[..], &exp_4096[..], "4096 split {split}");
        }
    }
}
