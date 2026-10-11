// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/node_id.cpp` — BEP-42 node-id generation and
//! verification, distance metrics, and secret ids.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};

use bitdaemon_common::crc32c::{crc32c, crc32c_32};
use bitdaemon_common::hash::Sha1Hash;
use bitdaemon_common::random::{random, random_bytes};
use bitdaemon_net::ip_helpers::is_local;
use sha1::{Digest as _, Sha1};

fn sha1(parts: &[&[u8]]) -> Sha1Hash {
    let mut h = Sha1::new();
    for p in parts {
        h.update(p);
    }
    let out = h.finalize();
    Sha1Hash::from_bytes(&out).unwrap_or_default()
}

/// `node_id` — a 160-bit Kademlia id (`sha1_hash` upstream).
pub type NodeId = Sha1Hash;

/// `distance(n1, n2)` — XOR metric.
#[must_use]
pub fn distance(n1: &NodeId, n2: &NodeId) -> NodeId {
    *n1 ^ *n2
}

/// `compare_ref` — is `n1` closer to `ref` than `n2`?
#[must_use]
pub fn compare_ref(n1: &NodeId, n2: &NodeId, ref_id: &NodeId) -> bool {
    distance(n1, ref_id) < distance(n2, ref_id)
}

/// `distance_exp` — 159 minus the leading zeroes of the distance
/// (upstream keeps the off-by-one on purpose). Routed through
/// `ffs::count_leading_zeros` like upstream's word-at-a-time scan.
#[must_use]
pub fn distance_exp(n1: &NodeId, n2: &NodeId) -> i32 {
    let d = distance(n1, n2);
    let b = d.as_bytes();
    let mut words = [0u32; 5];
    for (w, c) in words.iter_mut().zip(b.chunks_exact(4)) {
        *w = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
    }
    (159 - bitdaemon_common::ffs::count_leading_zeros(&words)).max(0)
}

/// `min_distance_exp`.
#[must_use]
pub fn min_distance_exp(n1: &NodeId, ids: &[NodeId]) -> i32 {
    debug_assert!(!ids.is_empty());
    ids.iter().map(|i| distance_exp(n1, i)).min().unwrap_or(160)
}

/// `generate_id_impl(ip, r)` — BEP-42 node id. Public (as upstream
/// exposes it to tests) so `r` can be fixed for vector checking.
#[must_use]
pub fn generate_id_impl(ip: &IpAddr, r: u32) -> NodeId {
    const V4MASK: [u8; 4] = [0x03, 0x0f, 0x3f, 0xff];
    const V6MASK: [u8; 8] = [0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff];

    let (bytes, mask): (Vec<u8>, &[u8]) = match ip {
        IpAddr::V4(a) => (a.octets().to_vec(), &V4MASK),
        IpAddr::V6(a) => {
            // upstream masks the first 8 octets of the v6 address
            (a.octets()[..8].to_vec(), &V6MASK)
        }
    };
    let mut ip = bytes;
    for (i, b) in ip.iter_mut().enumerate() {
        *b &= mask[i];
    }
    ip[0] |= ((r & 0x7) << 5) as u8;

    // crc32c of the masked address, interpreted as upstream does:
    // `crc32c_32(memcpy<u32>(ip))` on little-endian reads ip[0] into
    // the low byte — i.e. LE word order
    let c = if ip.len() == 4 {
        crc32c_32(u32::from_le_bytes(ip[..4].try_into().expect("4")))
    } else {
        crc32c(&u64::from_le_bytes(ip[..8].try_into().expect("8")).to_ne_bytes())
    };

    let mut id = NodeId::default();
    let b = id.as_bytes_mut();
    b[0] = ((c >> 24) & 0xff) as u8;
    b[1] = ((c >> 16) & 0xff) as u8;
    b[2] = (((c >> 8) & 0xf8) | random(0x7)) as u8;
    for byte in b.iter_mut().take(19).skip(3) {
        *byte = (random(0xff) & 0xff) as u8;
    }
    b[19] = (r & 0xff) as u8;
    id
}

/// `generate_id(ip)` — BEP-42 id for `ip` with a random `r`.
#[must_use]
pub fn generate_id(ip: &IpAddr) -> NodeId {
    generate_id_impl(ip, random(0xffff_ffff))
}

/// `generate_random_id` — SHA-1 of 20 random bytes.
#[must_use]
pub fn generate_random_id() -> NodeId {
    let mut r = [0u8; 20];
    random_bytes(&mut r);
    sha1(&[&r])
}

/// `static secret` — per-process secret for `make_id_secret`.
static SECRET: AtomicU32 = AtomicU32::new(0);

/// `make_id_secret` — stamps the last 8 bytes of `id` with a
/// `sha1(secret ‖ rand)` signature so our own generated ids can be
/// recognized later (used for torrent "did we announce" checks).
pub fn make_id_secret(id: &mut NodeId) {
    let mut secret = SECRET.load(Ordering::Relaxed);
    if secret == 0 {
        secret = random(0xffff_fffe) + 1;
        SECRET.store(secret, Ordering::Relaxed);
    }
    let rand = random(0xffff_ffff);
    // upstream hashes raw native-endian words — be/ne identical on
    // the hosts we target; keep be for determinism across platforms
    let sh = sha1(&[&secret.to_ne_bytes(), &rand.to_ne_bytes()]);
    let b = id.as_bytes_mut();
    b[20 - 4..].copy_from_slice(&sh.as_bytes()[..4]);
    b[20 - 8..20 - 4].copy_from_slice(&rand.to_ne_bytes());
}

/// `generate_secret_id`.
#[must_use]
pub fn generate_secret_id() -> NodeId {
    let mut id = generate_random_id();
    make_id_secret(&mut id);
    id
}

/// `verify_secret_id`.
#[must_use]
pub fn verify_secret_id(nid: &NodeId) -> bool {
    let secret = SECRET.load(Ordering::Relaxed);
    if secret == 0 {
        return false;
    }
    let sh = sha1(&[&secret.to_ne_bytes(), &nid.as_bytes()[20 - 8..20 - 4]]);
    nid.as_bytes()[20 - 4..] == sh.as_bytes()[..4]
}

/// `verify_id` — BEP-42: does `nid` match `source_ip`? Local
/// addresses always pass.
#[must_use]
pub fn verify_id(nid: &NodeId, source_ip: &IpAddr) -> bool {
    if is_local(source_ip) {
        return true;
    }
    let h = generate_id_impl(source_ip, u32::from(nid[19]));
    nid[0] == h[0] && nid[1] == h[1] && (nid[2] & 0xf8) == (h[2] & 0xf8)
}

/// `matching_prefix` — does `(nid << offset)[0] & mask == prefix`?
/// `id <<= offset` is a *bit* shift across the whole id.
#[must_use]
pub fn matching_prefix(nid: &NodeId, mask: u8, prefix: u8, offset: i32) -> bool {
    let id = *nid << offset as u32;
    (id[0] & mask) == prefix
}

/// `generate_prefix_mask(bits)` — `bits` leading 1s.
#[must_use]
pub fn generate_prefix_mask(bits: i32) -> NodeId {
    debug_assert!(bits >= 0);
    let bits = bits.min(160);
    let mut mask = [0u8; 20];
    let mut b = 0i32;
    while b < bits - 7 {
        mask[(b / 8) as usize] |= 0xff;
        b += 8;
    }
    if bits < 160 {
        mask[(b / 8) as usize] |= (0xffu16 << (8 - (bits & 7))) as u8;
    }
    NodeId::from_bytes(&mask).unwrap_or_default()
}
