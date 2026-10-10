//! Conformance port of upstream `test_packet_buffer.cpp` — the
//! 16-bit wrapping circular packet store.

use bitdaemon_net::packet_buffer::PacketBuffer;

#[test]
fn insert() {
    let mut pb = PacketBuffer::default();
    assert_eq!(pb.capacity(), 0);
    assert_eq!(pb.size(), 0);
    assert_eq!(pb.span(), 0);

    pb.insert(123, 123u8);
    assert!(pb.at(123 + 16).is_none());
    assert_eq!(pb.at(123), Some(&123));
    assert!(pb.capacity() > 0);
    assert_eq!(pb.size(), 1);
    assert_eq!(pb.span(), 1);
    assert_eq!(pb.cursor(), 123);

    pb.insert(125, 125);
    assert_eq!(pb.at(125), Some(&125));
    assert_eq!(pb.size(), 2);
    assert_eq!(pb.span(), 3);
    assert_eq!(pb.cursor(), 123);

    pb.insert(500, 4);
    assert_eq!(pb.size(), 3);
    assert_eq!(pb.span(), 501 - 123);
    assert_eq!(pb.capacity(), 512);

    pb.insert(500, 5);
    assert_eq!(pb.size(), 3);
    assert_eq!(pb.insert(500, 4), Some(5));
    assert_eq!(pb.size(), 3);

    assert_eq!(pb.remove(123), Some(123));
    assert_eq!(pb.size(), 2);
    assert_eq!(pb.span(), 501 - 125);
    assert_eq!(pb.cursor(), 125);

    assert_eq!(pb.remove(125), Some(125));
    assert_eq!(pb.size(), 1);
    assert_eq!(pb.span(), 1);
    assert_eq!(pb.cursor(), 500);

    assert_eq!(pb.remove(500), Some(4));
    assert_eq!(pb.size(), 0);
    assert_eq!(pb.span(), 0);

    // sliding window churn across the wrap boundary
    for i in 0..0xffu32 {
        let mut index = (i + 0xfff0) & 0xffff;
        pb.insert(index, (index + 1) as u8);
        assert_eq!(pb.capacity(), 512);
        if i >= 14 {
            index = index.wrapping_sub(14) & 0xffff;
            assert_eq!(pb.remove(index), Some((index + 1) as u8));
            assert_eq!(pb.size(), 14);
        }
    }
}

#[test]
fn wrap() {
    let mut pb = PacketBuffer::default();
    assert_eq!(pb.size(), 0);
    pb.insert(0xfffe, 1u8);
    assert_eq!(pb.at(0xfffe), Some(&1));
    pb.insert(2, 2);
    assert_eq!(pb.at(2), Some(&2));
    pb.remove(0xfffe);
    assert!(pb.at(0xfffe).is_none());
    assert_eq!(pb.at(2), Some(&2));
}

#[test]
fn wrap_range_boundaries() {
    let mut pb = PacketBuffer::default();
    let first = 0xfffeu32;
    pb.insert(first, 1u8);
    let capacity = pb.capacity();
    let last = (first + capacity - 1) & 0xffff;
    pb.insert(last, 2);
    let before = (first - 1) & 0xffff;
    let after = (first + capacity) & 0xffff;
    assert_eq!(pb.at(first), Some(&1));
    assert_eq!(pb.at(last), Some(&2));
    assert!(pb.at(before).is_none());
    assert!(pb.at(after).is_none());
    assert!(pb.remove(before).is_none());
    assert!(pb.remove(after).is_none());
    assert_eq!(pb.size(), 2);
    assert_eq!(pb.cursor(), first);
    assert_eq!(pb.span(), capacity);
    assert_eq!(pb.at(first), Some(&1));
    assert_eq!(pb.at(last), Some(&2));
}

#[test]
fn insert_large_capacity_far_index() {
    // capacity grows to the full 0x10000-element span; an idx within
    // the window but >0x8000 "behind" cursor() by wrap semantics
    let mut pb = PacketBuffer::default();
    pb.insert(0, 1u8);
    pb.reserve(0xffff);
    assert_eq!(pb.cursor(), 0);
    assert_eq!(pb.capacity(), 0x10000);
    let idx = 0x9000u32;
    pb.insert(idx, 2);
    assert_eq!(pb.cursor(), 0);
    assert_eq!(pb.size(), 2);
    assert_eq!(pb.at(0), Some(&1));
    assert_eq!(pb.at(idx), Some(&2));
}

#[test]
fn wrap2() {
    let mut pb = PacketBuffer::default();
    assert_eq!(pb.size(), 0);
    pb.insert(0xfff3, 1u8);
    assert_eq!(pb.at(0xfff3), Some(&1));
    let new_index = (0xfff3 + pb.capacity()) & 0xffff;
    pb.insert(new_index, 2);
    assert_eq!(pb.at(new_index), Some(&2));
    let old = pb.remove(0xfff3);
    assert_eq!(old, Some(1));
    assert!(pb.at(0xfff3).is_none());
    assert_eq!(pb.at(new_index), Some(&2));
}

#[test]
fn reverse_wrap() {
    let mut pb = PacketBuffer::default();
    assert_eq!(pb.size(), 0);
    pb.insert(0xfff3, 1u8);
    assert_eq!(pb.at(0xfff3), Some(&1));
    let new_index = (0xfff3 + pb.capacity()) & 0xffff;
    pb.insert(new_index, 2);
    assert_eq!(pb.at(new_index), Some(&2));
    let old = pb.remove(0xfff3);
    assert_eq!(old, Some(1));
    assert!(pb.at(0xfff3).is_none());
    assert_eq!(pb.at(new_index), Some(&2));
    pb.insert(0xffff, 3);
}
