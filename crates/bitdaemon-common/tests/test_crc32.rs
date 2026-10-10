//! Port of `libtorrent/test/test_crc32.cpp` (vectors are RFC 3720 B.4).

use bitdaemon_common::crc32c::{crc32c, crc32c_32, crc32c_words};

#[test]
fn crc32() {
    // upstream: crc32c_32(host_to_network(0xeffea55a)) — the swap makes the
    // in-memory bytes EF FE A5 5A; to_le_bytes() reproduces that.
    assert_eq!(crc32c_32(0xeffea55au32.swap_bytes()), 0x5ee3b9d5);

    // 32 zero bytes
    let buf = [0u64; 4];
    assert_eq!(crc32c_words(&buf), 0x8a9136aa);

    // 32 0xff bytes
    let buf = [0xffffffffffffffffu64; 4];
    assert_eq!(crc32c_words(&buf), 0x62a8ab43);

    // bytes 0x00..0x1f
    let mut buf = [0u64; 4];
    for (i, w) in buf.iter_mut().enumerate() {
        *w = u64::from_le_bytes([
            (i * 8) as u8,
            (i * 8 + 1) as u8,
            (i * 8 + 2) as u8,
            (i * 8 + 3) as u8,
            (i * 8 + 4) as u8,
            (i * 8 + 5) as u8,
            (i * 8 + 6) as u8,
            (i * 8 + 7) as u8,
        ]);
    }
    assert_eq!(crc32c_words(&buf), 0x46dd794e);

    // byte-oriented entry point must agree
    let bytes: Vec<u8> = (0u8..32).collect();
    assert_eq!(crc32c(&bytes), 0x46dd794e);
}
