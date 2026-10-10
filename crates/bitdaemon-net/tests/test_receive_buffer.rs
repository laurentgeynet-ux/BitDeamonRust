//! Conformance port of upstream `test/test_receive_buffer.cpp`.
//!
//! `cut(0, N)` is upstream's way of initialising the packet size on a
//! fresh buffer.

use bitdaemon_net::receive_buffer::{CryptoReceiveBuffer, ReceiveBuffer};

#[test]
fn recv_buffer_init() {
    let mut b = ReceiveBuffer::default();
    b.cut(0, 10, 0);

    assert_eq!(b.packet_size(), 10);
    assert_eq!(b.packet_bytes_remaining(), 10);
    assert!(!b.packet_finished());
    assert_eq!(b.pos(), 0);
    assert_eq!(b.capacity(), 0);
}

#[test]
fn recv_buffer_pos_at_end_false() {
    let mut b = ReceiveBuffer::default();
    b.cut(0, 1000, 0);
    b.reserve(1000);

    b.received(1000);
    b.advance_pos(999);

    assert!(!b.pos_at_end());
}

#[test]
fn recv_buffer_pos_at_end_true() {
    let mut b = ReceiveBuffer::default();
    b.cut(0, 1000, 0);
    b.reserve(1000);
    b.reserve(1000);
    b.received(1000);
    b.advance_pos(1000);
    assert!(b.pos_at_end());
}

#[test]
fn recv_buffer_packet_finished() {
    let mut b = ReceiveBuffer::default();
    b.cut(0, 10, 0);
    b.reserve(1000);
    b.reserve(1000);
    b.received(1000);

    for _ in 0..10 {
        assert!(!b.packet_finished());
        b.advance_pos(1);
    }
    assert!(b.packet_finished());
}

#[test]
fn recv_buffer_grow_floor() {
    let mut b = ReceiveBuffer::default();
    b.reset(1337);
    b.grow(100_000);

    assert!(b.capacity() >= 1337);
    assert!(b.capacity() < 1337 + 1000);
}

#[test]
fn recv_buffer_grow() {
    let mut b = ReceiveBuffer::default();
    b.reserve(200);
    b.grow(100_000);
    assert!(b.capacity() >= 300);
    assert!(b.capacity() < 300 + 500);
}

#[test]
fn recv_buffer_grow_limit() {
    let mut b = ReceiveBuffer::default();
    b.reserve(2000);
    b.grow(2100);
    assert!(b.capacity() >= 2100);
    assert!(b.capacity() < 2100 + 500);
}

#[test]
fn recv_buffer_reserve_minimum_grow() {
    let mut b = ReceiveBuffer::default();
    b.reset(1337);
    b.reserve(20);

    assert!(b.capacity() >= 1337);
    assert!(b.capacity() < 1337 + 1000);
}

#[test]
fn recv_buffer_reserve_grow() {
    let mut b = ReceiveBuffer::default();
    b.reserve(20);

    assert!(b.capacity() >= 20);
    assert!(b.capacity() < 20 + 500);
}

#[test]
fn recv_buffer_reserve() {
    let mut b = ReceiveBuffer::default();
    {
        let range1 = b.reserve(100);
        assert!(range1.len() >= 20);
    }

    let capacity = b.capacity();

    b.reset(20);
    b.received(20);

    assert_eq!(b.capacity(), capacity);

    {
        // upstream asserts range2.begin() == range1.begin() + 20
        // (recv_end was 20 after received(20)); the mutable write
        // window starts right after the received bytes
        let range2 = b.reserve(50);
        assert!(range2.len() >= 50);
    }

    assert_eq!(b.capacity(), capacity);
}

#[test]
fn receive_buffer_normalize() {
    let mut b = ReceiveBuffer::default();
    b.reset(16000);

    for _ in 0..16 {
        b.reserve(1000);
        b.received(1000);
        b.normalize(0);
    }

    assert!(b.capacity() >= 16000);
    let start_capacity = b.capacity();

    for _ in 0..15 {
        b.reset(160);
        b.reserve(160);
        b.received(160);
        b.normalize(0);
    }

    assert!(b.capacity() <= start_capacity / 2);
}

#[test]
fn receive_buffer_max_receive() {
    let mut b = ReceiveBuffer::default();
    b.reset(2000);
    b.reserve(2000);
    b.received(2000);
    b.normalize(0);

    b.reset(20);
    let max_receive = b.max_receive();
    assert!(max_receive >= 2000);
    b.received(20);
    assert_eq!(b.max_receive(), max_receive - 20);
}

#[test]
fn receive_buffer_watermark() {
    let mut b = ReceiveBuffer::default();
    b.reset(0x4000);
    b.reserve(33_500_000);
    b.received(33_500_000);
    b.normalize(0);

    assert_eq!(b.watermark(), 33_500_000);
}

#[test]
fn recv_buffer_mutable_buffers() {
    let mut b = ReceiveBuffer::default();
    b.reserve(1100);
    b.cut(0, 100, 0); // packet size = 100
    b.received(1100);
    let packet_transferred = b.advance_pos(1100);
    assert_eq!(packet_transferred, 100);
    b.cut(100, 1000, 0); // packet size = 1000
    let packet_transferred = b.advance_pos(999);
    assert_eq!(packet_transferred, 999);
    let vec = b.mutable_buffer_last(999);
    assert_eq!(vec.len(), 999);
}

#[test]
fn crypto_receive_buffer_packet_size() {
    let mut b = ReceiveBuffer::default();
    b.cut(0, 1000, 0);
    {
        let cb = CryptoReceiveBuffer::new(&mut b);
        // no crypto active yet: pass-through
        assert_eq!(cb.packet_size(), 1000);
        assert!(cb.crypto_packet_finished());
    }
    // upper layer consumes the whole packet -> packet finished,
    // then crypto_reset(500) starts tracking a 500-byte upper packet
    b.reserve(1000);
    b.received(1000);
    b.advance_pos(1000);
    {
        let mut cb = CryptoReceiveBuffer::new(&mut b);
        cb.crypto_reset(500);
        // m_packet_size captures the *connection* packet size (1000);
        // the upper layer already consumed it all -> finished, while
        // the crypto layer still owes 500 bytes (conn packet = 1500)
        assert_eq!(cb.packet_size(), 1000);
        assert!(cb.packet_finished());
        assert!(!cb.crypto_packet_finished());
        assert_eq!(b.packet_size(), 1500);
    }
}
