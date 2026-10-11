//! Conformance tests for `chained_buffer` and `bandwidth_*`
//! (upstream `test_bandwidth_limiter.cpp` is a simulation; the
//! semantics ported here are the deterministic parts: refill,
//! proportional assignment, ttl expiry, disconnect quota return).

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use bitdaemon_net::bandwidth::{BandwidthChannel, BandwidthManager, BandwidthSocket};
use bitdaemon_net::chained_buffer::{BufferHolder, ChainedBuffer};

// ---------- chained_buffer ----------

#[test]
fn chained_buffer_basic() {
    let mut b = ChainedBuffer::default();
    assert_eq!(b.size(), 0);
    assert_eq!(b.capacity(), 0);
    assert!(b.is_empty());

    b.append_buffer(vec![1, 2, 3, 4], 4);
    b.append_buffer(vec![5, 6], 2);
    assert_eq!(b.size(), 6);
    assert_eq!(b.capacity(), 6);

    let iov = b.build_iovec(6);
    assert_eq!(iov, vec![&[1, 2, 3, 4][..], &[5, 6][..]]);

    // partial iovec
    let iov = b.build_iovec(3);
    assert_eq!(iov, vec![&[1, 2, 3][..]]);
}

#[test]
fn chained_buffer_pop_front() {
    let mut b = ChainedBuffer::default();
    b.append_buffer(vec![1, 2, 3, 4], 4);
    b.append_buffer(vec![5, 6, 7, 8], 4);

    // partial pop inside first buffer
    b.pop_front(2);
    assert_eq!(b.size(), 6);
    let iov = b.build_iovec(8);
    assert_eq!(iov, vec![&[3, 4][..], &[5, 6, 7, 8][..]]);

    // pop consumes the rest of buffer 1 and part of buffer 2
    b.pop_front(4);
    assert_eq!(b.size(), 2);
    let iov = b.build_iovec(8);
    assert_eq!(iov, vec![&[7, 8][..]]);
}

#[test]
fn chained_buffer_appendix() {
    let mut b = ChainedBuffer::default();
    // allocate 16 bytes, only 4 used -> 12 bytes of slack
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&[1, 2, 3, 4]);
    v.resize(16, 0);
    b.append_buffer(v, 4);
    assert_eq!(b.space_in_last_buffer(), 12);

    assert!(b.append(&[9, 9, 9]));
    assert_eq!(b.space_in_last_buffer(), 9);
    assert_eq!(b.size(), 7);

    // doesn't fit
    assert!(!b.append(&[0; 16]));

    let iov = b.build_iovec(7);
    assert_eq!(iov, vec![&[1, 2, 3, 4, 9, 9, 9][..]]);
}

#[test]
fn chained_buffer_prepend() {
    let mut b = ChainedBuffer::default();
    b.append_buffer(vec![3, 4], 2);
    b.prepend_buffer(vec![1, 2], 2);
    let iov = b.build_iovec(4);
    assert_eq!(iov, vec![&[1, 2][..], &[3, 4][..]]);
}

/// Custom holder — stands in for disk-buffer holders upstream.
/// `BufferHolder` requires `Send` (send buffers cross into the
/// writer task), so `Arc` replaces upstream's refcounted holder.
struct Shared(Arc<Vec<u8>>);

impl BufferHolder for Shared {
    fn data(&self) -> &[u8] {
        &self.0
    }

    fn data_mut(&mut self) -> &mut [u8] {
        // never used in this test
        unimplemented!()
    }
}

#[test]
fn chained_buffer_custom_holder() {
    let mut b = ChainedBuffer::default();
    b.append_buffer(Shared(Arc::new(vec![7, 8, 9])), 3);
    assert_eq!(b.size(), 3);
    assert_eq!(b.build_iovec(3), vec![&[7, 8, 9][..]]);
}

// ---------- bandwidth ----------

struct TestPeer {
    /// bytes received via `assign_bandwidth`.
    got: Cell<i64>,
    disconnecting: Cell<bool>,
}

impl TestPeer {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            got: Cell::new(0),
            disconnecting: Cell::new(false),
        })
    }
}

impl BandwidthSocket for TestPeer {
    fn assign_bandwidth(&self, _channel: i32, amount: i64) {
        // upstream dispatches with 0 for disconnecting peers
        self.got.set(self.got.get() + amount);
    }

    fn is_disconnecting(&self) -> bool {
        self.disconnecting.get()
    }
}

#[test]
fn channel_quota_refill() {
    let ch = BandwidthChannel::new();
    // 0 = infinite
    assert_eq!(ch.quota_left(), bitdaemon_net::bandwidth::INF);

    ch.throttle(1000);
    ch.update_quota(1000);
    // to_add = (1000*1000 + 500)/1000 = 1000
    assert_eq!(ch.quota_left(), 1000);

    // 3-second burst cap: quota never exceeds 3*limit
    for _ in 0..10 {
        ch.update_quota(1000);
    }
    assert_eq!(ch.quota_left(), 3000);
}

#[test]
fn channel_use_and_return() {
    let ch = BandwidthChannel::new();
    ch.throttle(1000);
    ch.update_quota(1000);
    ch.use_quota(400);
    assert_eq!(ch.quota_left(), 600);
    ch.return_quota(400);
    assert_eq!(ch.quota_left(), 1000);
}

#[test]
fn channel_need_queueing() {
    let ch = BandwidthChannel::new();
    ch.throttle(1000);
    ch.update_quota(3000); // q = 3000 (capped at 3*limit)
    // q - amount >= limit -> granted instantly
    assert!(!ch.need_queueing(1500));
    // now q = 1500; q - 1000 = 500 < 1000 -> queue
    assert!(ch.need_queueing(1000));
}

#[test]
fn manager_immediate_grant() {
    let mut mgr = BandwidthManager::new(1);
    // no channels -> immediate
    let peer = TestPeer::new();
    assert_eq!(mgr.request_bandwidth(peer, 0x4000, 1, &[]), 0x4000);

    // channel with plenty of quota -> immediate
    let ch = Rc::new(BandwidthChannel::new());
    ch.throttle(1000);
    ch.update_quota(3000);
    let peer = TestPeer::new();
    assert_eq!(mgr.request_bandwidth(peer, 100, 1, &[Rc::clone(&ch)]), 100);
    assert_eq!(mgr.queue_size(), 0);
}

#[test]
fn manager_queue_and_distribute() {
    let mut mgr = BandwidthManager::new(1);
    let ch = Rc::new(BandwidthChannel::new());
    ch.throttle(1000);

    let p1 = TestPeer::new();
    let p2 = TestPeer::new();
    // channel empty: both requests queue
    assert_eq!(
        mgr.request_bandwidth(
            Rc::clone(&p1) as Rc<dyn BandwidthSocket>,
            1000,
            1,
            &[Rc::clone(&ch)]
        ),
        0
    );
    assert_eq!(
        mgr.request_bandwidth(
            Rc::clone(&p2) as Rc<dyn BandwidthSocket>,
            1000,
            1,
            &[Rc::clone(&ch)]
        ),
        0
    );
    assert_eq!(mgr.queue_size(), 2);
    assert_eq!(mgr.queued_bytes(), 2000);

    // one second round: 1000 bytes for two equal-priority peers;
    // quota accumulates in `assigned` — peers are only notified on
    // completion (or ttl expiry)
    mgr.update_quotas(1000);
    assert_eq!(p1.got.get(), 0);
    assert_eq!(p2.got.get(), 0);
    assert_eq!(mgr.queue_size(), 2);
    assert_eq!(mgr.queued_bytes(), 1000);

    // second round completes them
    mgr.update_quotas(1000);
    assert_eq!(p1.got.get(), 1000);
    assert_eq!(p2.got.get(), 1000);
    assert_eq!(mgr.queue_size(), 0);
    assert_eq!(mgr.queued_bytes(), 0);
}

#[test]
fn manager_priority_split() {
    let mut mgr = BandwidthManager::new(1);
    let ch = Rc::new(BandwidthChannel::new());
    ch.throttle(1000);

    let hi = TestPeer::new();
    let lo = TestPeer::new();
    mgr.request_bandwidth(
        Rc::clone(&hi) as Rc<dyn BandwidthSocket>,
        1000,
        3,
        &[Rc::clone(&ch)],
    );
    mgr.request_bandwidth(
        Rc::clone(&lo) as Rc<dyn BandwidthSocket>,
        1000,
        1,
        &[Rc::clone(&ch)],
    );

    // round 1: quota = distribute*prio/tmp (tmp=4) -> hi 750, lo 250
    // (still queued — notification happens on completion)
    mgr.update_quotas(1000);
    assert_eq!(hi.got.get(), 0);
    assert_eq!(lo.got.get(), 0);
    // round 2: hi completes (750+250), lo at 500 still queued
    mgr.update_quotas(1000);
    assert_eq!(hi.got.get(), 1000);
    assert_eq!(lo.got.get(), 0);
    assert_eq!(mgr.queue_size(), 1);
    // round 3: lo alone gets the full channel and completes
    mgr.update_quotas(1000);
    assert_eq!(lo.got.get(), 1000);
    assert_eq!(mgr.queue_size(), 0);
}

#[test]
fn manager_disconnect_returns_quota() {
    let mut mgr = BandwidthManager::new(1);
    let ch = Rc::new(BandwidthChannel::new());
    ch.throttle(1000);

    let p = TestPeer::new();
    // 2000-byte request on a 1000 B/s channel -> partial assign
    mgr.request_bandwidth(
        Rc::clone(&p) as Rc<dyn BandwidthSocket>,
        2000,
        1,
        &[Rc::clone(&ch)],
    );
    mgr.update_quotas(1000); // assigns 1000, still queued
    assert_eq!(p.got.get(), 0);
    assert_eq!(mgr.queue_size(), 1);

    p.disconnecting.set(true);
    mgr.update_quotas(1000);
    // gone from queue; assigned quota was returned to the channel
    assert_eq!(mgr.queue_size(), 0);
    assert!(ch.quota_left() >= 1000);
}
