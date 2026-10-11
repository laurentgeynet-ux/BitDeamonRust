//! Virtual time — `sim::timer_queue` equivalent (upstream runs the
//! whole sim on a single io_context clock; we keep a bare counter
//! the harness advances).

use std::time::{Duration, Instant};

/// `sim::chrono` — virtual monotonic clock.
#[derive(Debug)]
pub struct SimClock {
    t0: Instant,
    /// accumulated virtual offset.
    pub offset: Duration,
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

impl SimClock {
    /// anchored at the real `Instant::now()` + 0 offset.
    #[must_use]
    pub fn new() -> Self {
        Self {
            t0: Instant::now(),
            offset: Duration::ZERO,
        }
    }

    /// virtual now.
    #[must_use]
    pub fn now(&self) -> Instant {
        self.t0 + self.offset
    }

    /// advance the clock (upstream `sim::run` step).
    pub fn advance(&mut self, d: Duration) {
        self.offset += d;
    }
}
