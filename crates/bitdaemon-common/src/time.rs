//! `time.hpp` — libtorrent's clock/duration aliases.
//!
//! Upstream aliases `std::chrono` types (`steady_clock`, `milliseconds32`,
//! `time_point32`…) so every call site spells durations the same way. Rust
//! already has `std::time::{Duration, Instant}`; the aliases below carry
//! the same semantic weight, plus the `i32`-millisecond duration upstream
//! uses pervasively in timers (it wraps after ~24 days, by design).

use std::time::Duration;

/// `clock_type` — upstream's steady clock (monotonic).
pub type Clock = std::time::Instant;
/// `time_point` — a point on [`Clock`].
pub type TimePoint = std::time::Instant;
/// `time_duration` — duration between two [`TimePoint`]s.
pub type TimeDuration = Duration;

/// `milliseconds32` — a millisecond count in `i32` (upstream truncates
/// `duration_cast` to 32 bits; overflow wraps at ~24.8 days, matching
/// upstream's timer granularity assumptions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Milliseconds32(pub i32);

impl Milliseconds32 {
    /// `seconds32`/`minutes32` convenience builders.
    #[must_use]
    pub const fn ms(v: i32) -> Self {
        Self(v)
    }
    /// Wrap into a full [`Duration`] for arithmetic with [`TimePoint`].
    #[must_use]
    pub fn as_duration(self) -> Duration {
        Duration::from_millis(u64::try_from(self.0.max(0)).unwrap_or(u64::MAX))
    }
}

/// `minutes32`.
pub type Minutes32 = Milliseconds32;

/// `minutes(n)` → `Milliseconds32` (`minutes32` is `i32` *minutes* upstream,
/// flattened here to milliseconds since Rust has one `Duration`; saturates
/// past the ~24.8-day i32-ms range upstream can't express either way).
#[must_use]
pub const fn minutes(n: i32) -> Milliseconds32 {
    Milliseconds32(n.saturating_mul(60_000))
}

/// `seconds32(n)` — saturates like [`minutes`].
#[must_use]
pub const fn seconds32(n: i32) -> Milliseconds32 {
    Milliseconds32(n.saturating_mul(1_000))
}

/// `milliseconds32(n)`.
#[must_use]
pub const fn milliseconds32(n: i32) -> Milliseconds32 {
    Milliseconds32(n)
}

// `microseconds`, `milliseconds`, `seconds`, `minutes`, `hours` upstream
// are `std::chrono` duration aliases — `Duration` covers them all.

/// `aux::time_now()` — current point on the steady clock.
#[must_use]
pub fn time_now() -> TimePoint {
    std::time::Instant::now()
}

/// `total_seconds(d)` — whole seconds in `d`.
#[must_use]
pub fn total_seconds(d: Duration) -> u64 {
    d.as_secs()
}

/// `total_milliseconds(d)` — whole milliseconds in `d`.
#[must_use]
pub fn total_milliseconds(d: Duration) -> u64 {
    d.as_millis() as u64
}

/// `total_microseconds(d)` — whole microseconds in `d`.
#[must_use]
pub fn total_microseconds(d: Duration) -> u64 {
    d.as_micros() as u64
}

/// `min_time()`/`max_time()` for the steady clock don't exist portably;
/// upstream uses them for deadline sentinels. In Rust use
/// `Option<TimePoint>` — `None` = "no deadline" (upstream's `max_time`).
#[must_use]
pub const fn min_time() -> Option<TimePoint> {
    None
}
