//! Port of `libtorrent/test/test_timestamp_history.cpp`, plus coverage of
//! the >120-sample step path upstream leaves as TODO.

use bitdaemon_common::timestamp_history::{TimestampHistory, compare_less_wrap};

/// `TORRENT_TEST(timestamp_history)`.
#[test]
fn timestamp_history() {
    let mut h = TimestampHistory::new();
    assert!(!h.initialized());

    assert_eq!(h.add_sample(0x32, false), 0);
    assert_eq!(h.base(), 0x32);
    assert!(h.initialized());

    assert_eq!(h.add_sample(0x33, false), 0x1);
    assert_eq!(h.base(), 0x32);

    assert_eq!(h.add_sample(0x3433, false), 0x3401);
    assert_eq!(h.base(), 0x32);

    assert_eq!(h.add_sample(0x30, false), 0);
    assert_eq!(h.base(), 0x30);

    // wrapping timestamps handled: 0xfffffff3 < 0x30 in wrap order
    h.add_sample(0xfffffff3, false);
    assert_eq!(h.base(), 0xfffffff3);
}

/// Wrap-aware comparison (`compare_less_wrap` from `utp_stream.cpp`).
#[test]
fn less_wrap() {
    // near-wrap: 0xffffffff is "less than" 0x01 (just wrapped past 0)
    assert!(compare_less_wrap(0xffffffff, 0x01, 0xffffffff));
    assert!(!compare_less_wrap(0x01, 0xffffffff, 0xffffffff));
    // ordinary order
    assert!(compare_less_wrap(10, 20, 0xffffffff));
    assert!(!compare_less_wrap(20, 10, 0xffffffff));
    // equal → not less
    assert!(!compare_less_wrap(7, 7, 0xffffffff));
}

/// Step path: after >120 samples a minute-step advances the ring and
/// recomputes base from history.
#[test]
fn step_advances_ring() {
    let mut h = TimestampHistory::new();
    h.add_sample(1000, false);
    // >120 samples (counter capped just below 0xfffe)
    for _ in 0..200 {
        h.add_sample(1000, false);
    }
    // step with a much lower sample: new slot, base = new min
    let ret = h.add_sample(500, true);
    assert_eq!(ret, 0);
    assert_eq!(h.base(), 500);

    // step again with a higher sample: base stays the lowest in window
    for _ in 0..200 {
        h.add_sample(700, false);
    }
    h.add_sample(700, true);
    assert_eq!(h.base(), 500);
}

/// `adjust_base` shifts base and clamps all history slots.
#[test]
fn adjust_base() {
    let mut h = TimestampHistory::new();
    h.add_sample(100, false);
    h.adjust_base(50);
    assert_eq!(h.base(), 150);
    // a new sample relative to adjusted base
    assert_eq!(h.add_sample(200, false), 50);
}
