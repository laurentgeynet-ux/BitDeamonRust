//! Port of `libtorrent/test/test_bloom_filter.cpp`.

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::bloom_filter::{BloomFilter, count_zero_bits, has_bits, set_bits};
use sha1::Sha1;

fn sha1_of(data: &[u8]) -> Sha1Hash {
    use sha1::Digest as _;
    let mut h = Sha1::new();
    h.update(data);
    let out: [u8; 20] = h.finalize().into();
    Sha1Hash::from_bytes(&out).unwrap()
}

/// `test_set_and_get`.
#[test]
fn set_and_get() {
    let mut filter = BloomFilter::<32>::new();
    let k1 = sha1_of(b"test1");
    let k2 = sha1_of(b"test2");
    let k3 = sha1_of(b"test3");
    let k4 = sha1_of(b"test4");

    assert!(!filter.find(&k1));
    assert!(!filter.find(&k2));
    assert!(!filter.find(&k3));
    assert!(!filter.find(&k4));

    filter.set(&k1);
    assert!(filter.find(&k1));
    assert!(!filter.find(&k2));
    assert!(!filter.find(&k3));
    assert!(!filter.find(&k4));

    filter.set(&k4);
    assert!(filter.find(&k1));
    assert!(!filter.find(&k2));
    assert!(!filter.find(&k3));
    assert!(filter.find(&k4));
}

/// `test_set_bits` — every even bit index 0..32 sets exactly 0x55 bytes.
#[test]
fn set_bits_pattern() {
    let mut bits = [0x00u8; 4];

    for i in 0..4 * 8 {
        let t = [(i & 0xff) as u8, 0, (i & 0xff) as u8, 0];
        assert!(!has_bits(&t, &bits, 6));
    }

    for i in (0..4 * 8).step_by(2) {
        let t = [(i & 0xff) as u8, 0, (i & 0xff) as u8, 0];
        assert!(!has_bits(&t, &bits, 4));
        set_bits(&t, &mut bits, 4);
        assert!(has_bits(&t, &bits, 4));
    }

    assert_eq!(bits, [0x55, 0x55, 0x55, 0x55]);
}

/// `test_count_zeroes`.
#[test]
fn count_zeroes() {
    let mut bits = [0x00, 0xff, 0x55, 0xaa];

    assert_eq!(count_zero_bits(&bits), 16);

    let t = [4, 0, 4, 0];
    set_bits(&t, &mut bits, 4);
    assert_eq!(count_zero_bits(&bits), 15);
    assert_eq!(bits, [0x10, 0xff, 0x55, 0xaa]);
}

/// `test_to_from_string`.
#[test]
fn to_from_bytes() {
    let bits = [0x10, 0xff, 0x55, 0xaa];

    let mut filter = BloomFilter::<4>::new();
    filter.from_bytes(&bits);
    assert_eq!(filter.as_bytes(), &bits);

    // upstream builds sha1_hash from raw bytes "\x01\x00\x02\x00" + padding
    let mut raw = [0u8; 20];
    raw[..4].copy_from_slice(&[0x01, 0x00, 0x02, 0x00]);
    let k = Sha1Hash::from_bytes(&raw).unwrap();

    assert!(!filter.find(&k));
    filter.set(&k);
    assert!(filter.find(&k));

    assert_eq!(filter.as_bytes(), &[0x16, 0xff, 0x55, 0xaa]);
}

/// `size()` sanity: empty ≈ 0.5 (upstream's `min(zero_bits, m−1)` clamp
/// avoids `ln(1)=0`), grows with insertions, saturates.
#[test]
fn size_estimate() {
    let mut filter = BloomFilter::<128>::new();
    assert!(filter.size() < 1.0);

    for i in 0..10u32 {
        filter.set(&sha1_of(&i.to_le_bytes()));
    }
    let s = filter.size();
    assert!(s > 5.0 && s < 30.0, "size {s} should be near 10");

    // full filter → no zero bits → ln(0) → +inf (same as upstream:
    // the m−1 clamp only bounds the empty end)
    for i in 0..10_000u32 {
        filter.set(&sha1_of(&i.to_le_bytes()));
    }
    assert_eq!(filter.size(), f32::INFINITY);
}
