//! Port of `libtorrent/test/test_bitfield.cpp`.
//!
//! Each `#[test]` maps to a `TORRENT_TEST` upstream. `Option<usize>` replaces
//! the `-1` sentinel (`find_first_set`/`find_last_clear`).

use bitdaemon_common::Bitfield;

/// `TORRENT_TEST(bitfield)`
#[test]
fn bitfield() {
    let mut test1 = Bitfield::with_value(10, false);
    assert_eq!(test1.size(), 10);
    assert_eq!(test1.num_words(), 1);
    assert_eq!(test1.num_bytes(), 2);
    assert!(!test1.is_empty());
    assert_eq!(test1.count(), 0);
    test1.set_bit(9);
    assert_eq!(test1.count(), 1);
    test1.clear_bit(9);
    assert_eq!(test1.count(), 0);
    test1.set_bit(2);
    assert_eq!(test1.count(), 1);
    test1.set_bit(1);
    test1.set_bit(9);
    assert_eq!(test1.count(), 3);
    assert!(!test1.all_set());
    test1.clear_bit(2);
    assert_eq!(test1.count(), 2);
    assert_eq!(test1.iter().count(), 10);

    test1.set_all();
    assert_eq!(test1.count(), 10);

    test1.clear_all();
    assert_eq!(test1.count(), 0);

    test1.resize(2);
    test1.set_bit(0);
    test1.resize_with(16, true);
    assert_eq!(test1.count(), 15);
    test1.resize_with(20, true);
    assert_eq!(test1.count(), 19);
    assert!(test1.get_bit(0));
    assert!(!test1.get_bit(1));

    let test2 = test1.clone();
    assert_eq!(test2.count(), 19);
    assert!(test2.get_bit(0));
    assert!(!test2.get_bit(1));
    assert!(test2.get_bit(2));

    test1.set_bit(1);
    test1.resize(1);
    assert_eq!(test1.count(), 1);

    test1.resize_with(100, true);
    assert!(test1.all_set());
    assert_eq!(test1.count(), 100);
    test1.resize_with(200, false);
    assert!(!test1.all_set());
    assert_eq!(test1.count(), 100);
    test1.resize_with(50, false);
    assert!(test1.all_set());
    assert_eq!(test1.count(), 50);
    test1.resize_with(101, true);
    assert!(test1.all_set());
    assert_eq!(test1.count(), 101);

    let b1 = [0x08_u8, 0x10];
    assert!(test1.assign(&b1, 14));
    assert_eq!(test1.count(), 2);
    assert!(!test1.get_bit(3));
    assert!(test1.get_bit(4));
    assert!(!test1.get_bit(5));
    assert!(!test1.get_bit(10));
    assert!(test1.get_bit(11));
    assert!(!test1.get_bit(12));

    test1 = Bitfield::new();
    assert_eq!(test1.size(), 0);
    assert!(test1.is_empty());
    assert!(Bitfield::new().is_empty());

    test1 = test2;
    assert_eq!(test1.size(), 20);
    assert_eq!(test1.num_words(), 1);
    assert_eq!(test1.num_bytes(), 3);
    assert_eq!(test1.count(), 19);
    assert!(test1.get_bit(0));
    assert!(!test1.get_bit(1));
    assert!(test1.get_bit(2));
}

/// `TORRENT_TEST(test_assign3)`
#[test]
fn assign3() {
    let mut test1 = Bitfield::new();
    let b2 = [0x08_u8, 0x10, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x0c, 0x7f];
    assert!(test1.assign(&b2, 72));
    assert_eq!(test1.count(), 47);

    let b3 = [0x08_u8, 0x10, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x0c];
    assert!(test1.assign(&b3, 64));
    assert_eq!(test1.count(), 40);
}

/// `TORRENT_TEST(test_assign)`
#[test]
fn assign() {
    let mut b = [0_u8; 16];
    let mut test1 = Bitfield::new();
    for item in b.iter_mut().take(4) {
        *item = 0xc0;
        assert!(test1.assign(&[*item], 2));
        assert_eq!(test1.count(), 2);
        assert!(test1.all_set());
    }
}

/// `TORRENT_TEST(test_assign2)`
#[test]
fn assign2() {
    let mut b = [0_u8; 16];
    let mut test1 = Bitfield::new();
    for i in 0..4 {
        b[i..i + 5].fill(0xff);
        b[i + 5] = 0xc0;
        assert!(test1.assign(&b[i..], 32 + 8 + 2));
        assert_eq!(test1.count(), 32 + 8 + 2);
        assert!(test1.all_set());
    }
}

/// `TORRENT_TEST(test_iterators)` — resized fields iterate exactly `size` bits.
#[test]
fn iterators() {
    let mut test1 = Bitfield::new();
    for i in 0..100 {
        test1.resize_with(i, false);
        assert_eq!(test1.iter().count(), i);
        assert!(test1.iter().all(|b| !b));
        assert_eq!(test1.iter().len(), i);
    }
}

/// `TORRENT_TEST(test_iterator_arithmetic)` — `begin() + n` ↔ `iter().skip(n)`.
#[test]
fn iterator_offsets() {
    let mut test1 = Bitfield::with_size(128);
    test1.set_bit(0);
    test1.set_bit(1);
    test1.set_bit(40);
    test1.set_bit(127);
    assert_eq!(test1.iter().filter(|&b| b).count(), 4);
    assert_eq!(test1.iter().skip(1).filter(|&b| b).count(), 3);
    assert_eq!(test1.iter().skip(2).filter(|&b| b).count(), 2);
    assert_eq!(test1.iter().skip(2).take(39).filter(|&b| b).count(), 1);
    assert_eq!(test1.iter().skip(41).filter(|&b| b).count(), 1);
    assert_eq!(test1.iter().skip(41).take(85).filter(|&b| b).count(), 0);
    assert_eq!(test1.iter().skip(40).take(10).filter(|&b| b).count(), 1);
}

/// `TORRENT_TEST(test_resize_val)` — growing by `i` bits set to 1 adds `i` set bits.
#[test]
fn resize_val() {
    let b = [0xcc_u8; 8]; // char(-52): 4 set bits per byte
    let mut test1 = Bitfield::from_bytes(&b, 8 * 8).unwrap();
    assert_eq!(test1.size(), 8 * 8);
    assert_eq!(test1.count(), 4 * 8);

    for i in 1..4 * 8 {
        test1.resize_with(8 * 8 + i, true);
        assert_eq!(test1.count(), 4 * 8 + i, "i={i}");
    }
}

/// `TORRENT_TEST(test_resize_up)` — growing keeps the count unchanged.
#[test]
fn resize_up() {
    let b = [0xcc_u8; 8];
    let mut test1 = Bitfield::from_bytes(&b, 8 * 8).unwrap();
    assert_eq!(test1.size(), 8 * 8);
    assert_eq!(test1.count(), 4 * 8);

    for i in 1..5 * 8 {
        test1.resize(8 * 8 + i);
        assert_eq!(test1.size(), 8 * 8 + i);
        assert_eq!(test1.count(), 4 * 8, "i={i}");
    }
}

/// `TORRENT_TEST(test_resize_down)` — shrinking keeps the surviving bits.
#[test]
fn resize_down() {
    let b = [0x55_u8; 8]; // alternating bits — i/2 survive
    let mut test1 = Bitfield::from_bytes(&b, 8 * 8).unwrap();
    for i in (0..=8 * 8).rev() {
        test1.resize(i);
        assert_eq!(test1.size(), i);
        assert_eq!(test1.count(), i / 2, "i={i}");
    }
}

/// `TORRENT_TEST`s `find_first_set_{empty,small,large,early,late}`.
#[test]
fn find_first_set() {
    assert_eq!(Bitfield::with_size(0).find_first_set(), None);
    assert_eq!(Bitfield::with_value(10, false).find_first_set(), None);
    assert_eq!(Bitfield::with_value(100, false).find_first_set(), None);

    let mut t = Bitfield::with_value(100, false);
    t.set_bit(4);
    assert_eq!(t.find_first_set(), Some(4));

    let mut t = Bitfield::with_value(100, false);
    t.set_bit(98);
    assert_eq!(t.find_first_set(), Some(98));
}

/// `TORRENT_TEST`s `find_last_clear_{empty,small,large,early,late}`.
#[test]
fn find_last_clear() {
    assert_eq!(Bitfield::with_size(0).find_last_clear(), None);
    assert_eq!(Bitfield::with_value(10, true).find_last_clear(), None);
    assert_eq!(Bitfield::with_value(100, true).find_last_clear(), None);

    let mut t = Bitfield::with_value(100, true);
    t.clear_bit(4);
    assert_eq!(t.find_last_clear(), Some(4));

    let mut t = Bitfield::with_value(100, true);
    t.clear_bit(98);
    assert_eq!(t.find_last_clear(), Some(98));
}

/// `TORRENT_TEST(find_last_clear_misc)`
#[test]
fn find_last_clear_misc() {
    let mut t1 = Bitfield::with_value(100, true);
    t1.clear_bit(11);
    t1.clear_bit(91);
    assert_eq!(t1.find_last_clear(), Some(91));

    let mut t2 = Bitfield::with_value(78, true);
    t2.clear_bit(12);
    t2.clear_bit(43);
    t2.clear_bit(34);
    assert_eq!(t2.find_last_clear(), Some(43));

    let mut t3 = Bitfield::with_value(123, true);
    t3.clear_bit(49);
    t3.clear_bit(33);
    t3.clear_bit(32);
    t3.clear_bit(50);
    assert_eq!(t3.find_last_clear(), Some(50));

    let mut t4 = Bitfield::with_value(1000, true);
    t4.clear_bit(11);
    t4.clear_bit(91);
    t4.clear_bit(14);
    t4.clear_bit(15);
    t4.clear_bit(89);
    assert_eq!(t4.find_last_clear(), Some(91));
}

/// `TORRENT_TEST(not_initialized)`
#[test]
fn not_initialized() {
    let test1 = Bitfield::new();
    assert!(test1.none_set());
    assert!(!test1.all_set());
    assert_eq!(test1.size(), 0);
    assert_eq!(test1.num_words(), 0);
    assert_eq!(test1.num_bytes(), 0);
    assert!(test1.is_empty());
    assert!(test1.bytes().is_empty());
    assert_eq!(test1.count(), 0);
    assert_eq!(test1.find_first_set(), None);
    assert_eq!(test1.find_last_clear(), None);
}

/// MSB-first wire order — peer-wire correctness depends on it.
#[test]
fn wire_order() {
    // 0x80 is bit 0, 0x01 is bit 7 (network byte order, MSB first)
    let t = Bitfield::from_bytes(&[0x80], 8).unwrap();
    assert!(t.get_bit(0));
    assert!(!t.get_bit(1));
    let t = Bitfield::from_bytes(&[0x01], 8).unwrap();
    assert!(t.get_bit(7));
    assert!(!t.get_bit(0));

    // trailing bits beyond size are masked off
    let t = Bitfield::from_bytes(&[0xff], 4).unwrap();
    assert_eq!(t.count(), 4);
    assert_eq!(t.bytes(), &[0xf0]);

    // too-short buffer is rejected instead of panicking (upstream asserts)
    assert!(Bitfield::from_bytes(&[0xff], 9).is_none());
    let mut bf = Bitfield::new();
    assert!(!bf.assign(&[0xff], 9));
}

/// `TORRENT_TEST(self_assign)` equivalent + `swap`.
#[test]
fn equality_and_swap() {
    let a = Bitfield::from_bytes(&[0xaa], 8).unwrap();
    let b = Bitfield::from_bytes(&[0xaa], 8).unwrap();
    let c = Bitfield::from_bytes(&[0xaa], 4).unwrap();
    assert_eq!(a, b);
    assert_ne!(a, c);

    let mut a2 = a.clone();
    let mut b2 = c.clone();
    a2.swap(&mut b2);
    assert_eq!(a2, c);
    assert_eq!(b2, a);
}

/// `FromIterator` — grow bit-by-bit, MSB-first.
#[test]
fn collect() {
    let bits = vec![true, false, true, true, false, false, false, false, true];
    let bf: Bitfield = bits.into_iter().collect();
    assert_eq!(bf.size(), 9);
    assert_eq!(bf.count(), 4);
    assert_eq!(bf.bytes(), &[0b1011_0000, 0b1000_0000]);
}
