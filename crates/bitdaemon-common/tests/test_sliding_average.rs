//! Port of `libtorrent/test/test_sliding_average.cpp`.

use bitdaemon_common::SlidingAverage;

/// Normal-distributed samples, mean=60 stddev=10 — verbatim from upstream.
const SAMPLES: [u8; 128] = [
    49, 51, 60, 46, 65, 53, 76, 59, 57, 54, 56, 51, 45, 80, 53, 62, 69, 67, 66, 56, 56, 61, 52, 61,
    61, 62, 59, 53, 48, 68, 47, 47, 63, 51, 53, 54, 46, 65, 64, 64, 45, 68, 64, 66, 53, 42, 57, 58,
    57, 47, 55, 59, 64, 61, 37, 67, 55, 52, 60, 60, 44, 57, 50, 77, 56, 54, 49, 68, 66, 64, 47, 60,
    46, 47, 81, 74, 65, 62, 44, 75, 65, 43, 58, 59, 53, 67, 49, 51, 33, 47, 49, 50, 54, 48, 55, 80,
    67, 51, 66, 52, 48, 57, 30, 51, 72, 65, 78, 56, 74, 68, 49, 66, 63, 57, 61, 62, 64, 62, 61, 52,
    67, 64, 59, 61, 69, 60, 54, 69,
];

/// `TORRENT_TEST(reaction_time)`.
#[test]
fn reaction_time() {
    let mut avg = SlidingAverage::<10>::default();
    avg.add_sample(-10);
    avg.add_sample(10);
    assert_eq!(avg.mean(), 0);
}

/// `TORRENT_TEST(reaction_time2)`.
#[test]
fn reaction_time2() {
    let mut avg = SlidingAverage::<10>::default();
    avg.add_sample(10);
    avg.add_sample(20);
    assert_eq!(avg.mean(), 15);
}

/// `TORRENT_TEST(converge)`.
#[test]
fn converge() {
    let mut avg = SlidingAverage::<10>::default();
    avg.add_sample(100);
    for _ in 0..20 {
        avg.add_sample(10);
    }
    assert!((avg.mean() - 10).abs() <= 3);
}

/// `TORRENT_TEST(converge2)`.
#[test]
fn converge2() {
    let mut avg = SlidingAverage::<10>::default();
    avg.add_sample(-100);
    for _ in 0..20 {
        avg.add_sample(-10);
    }
    assert!((avg.mean() + 10).abs() <= 3);
}

/// `TORRENT_TEST(random_converge)`.
#[test]
fn random_converge() {
    let mut avg = SlidingAverage::<10>::default();
    for &s in &SAMPLES {
        avg.add_sample(i32::from(s));
    }
    assert!((avg.mean() - 60).abs() <= 3);
}

/// `TORRENT_TEST(sliding_average)`.
#[test]
fn sliding_average() {
    let mut avg = SlidingAverage::<4>::default();
    assert_eq!(avg.mean(), 0);
    assert_eq!(avg.avg_deviation(), 0);
    avg.add_sample(500);
    assert_eq!(avg.mean(), 500);
    assert_eq!(avg.avg_deviation(), 0);
    avg.add_sample(501);
    assert_eq!(avg.avg_deviation(), 1);
    avg.add_sample(0);
    avg.add_sample(0);
    assert!((avg.mean() - 250).abs() < 50);
    assert!((avg.avg_deviation() - 250).abs() < 80);
}

/// `TORRENT_TEST(positive_overflow)`.
#[test]
fn positive_overflow() {
    let mut avg = SlidingAverage::<10>::default();
    avg.add_sample(2_000_000_000);
    assert_eq!(avg.mean(), 8_388_608);
}

/// `TORRENT_TEST(negative_overflow)`.
#[test]
fn negative_overflow() {
    let mut avg = SlidingAverage::<10>::default();
    avg.add_sample(-2_000_000_000);
    assert_eq!(avg.mean(), -8_388_607);
}

/// `TORRENT_TEST(overflow)`.
#[test]
fn overflow() {
    let mut avg = SlidingAverage::<10>::default();
    for _ in 0..20 {
        avg.add_sample(-2_000_000_000);
    }
    avg.add_sample(2_000_000_000);
    assert_eq!(avg.mean(), -6_710_885);
}
