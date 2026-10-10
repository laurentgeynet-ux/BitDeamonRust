//! `aux::timestamp_history` — circular history of the lowest uTP timestamp
//! seen over the last 20 minutes (`timestamp_history.cpp`). Also hosts
//! `compare_less_wrap`, defined in `utp_stream.cpp` upstream and shared
//! with the uTP layer.

/// `TIME_MASK` upstream.
const TIME_MASK: u32 = 0xffffffff;

/// `compare_less_wrap` — `lhs < rhs` on a wrapping counter (defined in
/// `utp_stream.cpp` upstream). If `lhs` is near `UINT_MAX` and `rhs` near
/// 0, `lhs` is assumed to have wrapped and is considered smaller.
#[must_use]
pub fn compare_less_wrap(lhs: u32, rhs: u32, mask: u32) -> bool {
    // distance walking from lhs to rhs, downwards / upwards
    let dist_down = lhs.wrapping_sub(rhs) & mask;
    let dist_up = rhs.wrapping_sub(lhs) & mask;
    // if the distance walking up is shorter, lhs < rhs
    dist_up < dist_down
}

/// `aux::timestamp_history` — base-delay estimator state.
#[derive(Debug, Clone)]
pub struct TimestampHistory {
    /// `m_history` — circular buffer, one slot per minute.
    history: [u32; Self::HISTORY_SIZE],
    /// `m_base` — lowest sample in the window.
    base: u32,
    /// `m_index` — current slot.
    index: u16,
    /// `m_num_samples` — samples since last step; `NOT_INITIALIZED` until
    /// the first sample arrives.
    num_samples: u16,
}

impl TimestampHistory {
    /// `history_size` upstream.
    pub const HISTORY_SIZE: usize = 20;
    const NOT_INITIALIZED: u16 = 0xffff;

    /// `timestamp_history()`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            history: [0; Self::HISTORY_SIZE],
            base: 0,
            index: 0,
            num_samples: Self::NOT_INITIALIZED,
        }
    }

    /// `initialized()`.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.num_samples != Self::NOT_INITIALIZED
    }

    /// `add_sample(sample, step)` — `step` indicates a minute elapsed.
    /// Returns `sample - base` (wrapping, like upstream).
    pub fn add_sample(&mut self, sample: u32, step: bool) -> u32 {
        if !self.initialized() {
            self.history.fill(sample);
            self.base = sample;
            self.num_samples = 0;
        }

        // don't let the counter wrap
        if self.num_samples < 0xfffe {
            self.num_samples += 1;
        }

        // if sample is less than base, update the base and the history
        // entry (it will be less than that too)
        if compare_less_wrap(sample, self.base, TIME_MASK) {
            self.base = sample;
            self.history[self.index as usize] = sample;
        }
        // if sample is less than our history entry, update it
        else if compare_less_wrap(sample, self.history[self.index as usize], TIME_MASK) {
            self.history[self.index as usize] = sample;
        }

        let ret = sample.wrapping_sub(self.base);

        // don't step base delay history unless we have at least 120
        // samples — anything less suggests an essentially idle connection
        if step && self.num_samples > 120 {
            self.num_samples = 0;
            self.index = (self.index + 1) % Self::HISTORY_SIZE as u16;

            self.history[self.index as usize] = sample;
            // update m_base
            self.base = sample;
            for &h in &self.history {
                if compare_less_wrap(h, self.base, TIME_MASK) {
                    self.base = h;
                }
            }
        }
        ret
    }

    /// `base()` — panics if uninitialized (upstream `TORRENT_ASSERT`).
    #[must_use]
    pub fn base(&self) -> u32 {
        debug_assert!(self.initialized());
        self.base
    }

    /// `adjust_base(change)` — shift the base and clamp every history
    /// slot so the adjustment sticks.
    pub fn adjust_base(&mut self, change: i32) {
        debug_assert!(self.initialized());
        self.base = self.base.wrapping_add(change as u32);
        for h in &mut self.history {
            if compare_less_wrap(*h, self.base, TIME_MASK) {
                *h = self.base;
            }
        }
    }
}

impl Default for TimestampHistory {
    fn default() -> Self {
        Self::new()
    }
}
