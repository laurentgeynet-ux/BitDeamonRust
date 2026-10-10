//! `aux::sliding_average<int, inverted_gain>` — exponential moving average
//! with fixed-point accumulation and an average-deviation estimator.
//!
//! Integer math is transcribed verbatim (including truncating division); the
//! only upstream instantiation is `i32`, so `INVERTED_GAIN` is a const
//! parameter on an `i32` accumulator rather than a fully generic type.

/// `sliding_average<Int, inverted_gain>` for `Int = i32`.
///
/// `INVERTED_GAIN` is `1/n` of the gain: once `num_samples` reaches it, the
/// mean update divides by `INVERTED_GAIN` instead of the sample count.
#[derive(Debug, Clone, Default)]
pub struct SlidingAverage<const INVERTED_GAIN: i32> {
    /// `m_mean` — fixed point (`sample * FIXED_POINT`).
    mean: i32,
    /// `m_average_deviation` — fixed point.
    average_deviation: i32,
    /// `m_num_samples` — capped at `INVERTED_GAIN`.
    num_samples: i32,
}

impl<const INVERTED_GAIN: i32> SlidingAverage<INVERTED_GAIN> {
    /// `fixed_point` — samples are scaled by this before accumulating.
    pub const FIXED_POINT: i32 = 64;

    /// `add_sample`.
    pub fn add_sample(&mut self, sample: i32) {
        // avoid integer overflow: clamp to the max representable sample
        // (already in fixed-point scale)
        let s = if sample >= i32::MAX / Self::FIXED_POINT / 4 {
            i32::MAX / 4
        } else if sample <= i32::MIN / Self::FIXED_POINT / 4 {
            i32::MIN / 4
        } else {
            sample * Self::FIXED_POINT
        };

        let deviation = if self.num_samples > 0 {
            (self.mean - s).abs()
        } else {
            0
        };

        if self.num_samples < INVERTED_GAIN {
            self.num_samples += 1;
        }

        self.mean += (s - self.mean) / self.num_samples;

        if self.num_samples > 1 {
            // deviation samples lag by 1 — you need 2 real samples for the
            // first deviation
            self.average_deviation += (deviation - self.average_deviation) / (self.num_samples - 1);
        }
    }

    /// `mean()` — back to whole units, rounded (+32 before shifting).
    #[must_use]
    pub fn mean(&self) -> i32 {
        if self.num_samples > 0 {
            (self.mean + 32) / Self::FIXED_POINT
        } else {
            0
        }
    }

    /// `avg_deviation()`.
    #[must_use]
    pub fn avg_deviation(&self) -> i32 {
        if self.num_samples > 1 {
            (self.average_deviation + 32) / Self::FIXED_POINT
        } else {
            0
        }
    }

    /// `num_samples()` — effective (capped) sample count.
    #[must_use]
    pub fn num_samples(&self) -> i32 {
        self.num_samples
    }
}
