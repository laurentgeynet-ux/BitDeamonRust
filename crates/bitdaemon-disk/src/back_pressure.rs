//! Port of `aux_/back_pressure.hpp`/`back_pressure.cpp`.
//!
//! Applies back-pressure to peers when the write-queue level reaches
//! `m_max_size`: interested parties register a `DiskObserver`, which is
//! posted back to the main thread once the level drains below the low
//! watermark. Hysteresis: flush starts at high watermark, stops at low.
//!
//! Upstream posts `watermark_callback` on the network `io_context`; here
//! the poster is injected — session wiring supplies the channel to its
//! event loop (direct call for tests).

use std::sync::{Arc, Mutex, Weak};

/// `disk_observer` — notified when the write queue drains below the
/// low watermark (peers resume requesting buffers).
pub trait DiskObserver: Send + Sync {
    /// `on_disk()`.
    fn on_disk(&self);
}

/// Poster for observer callbacks — upstream `post(ios, …)`. Takes the
/// locked observers; invoked without the back-pressure mutex held.
pub type Poster = Arc<dyn Fn(Vec<Weak<dyn DiskObserver>>) + Send + Sync>;

/// Default poster — invokes `on_disk` synchronously.
fn direct_post(cbs: Vec<Weak<dyn DiskObserver>>) {
    for w in cbs {
        if let Some(o) = w.upgrade() {
            o.on_disk();
        }
    }
}

struct Inner {
    /// `m_exceeded_max_size` — sticky until low watermark.
    exceeded: bool,
    /// `m_observers`.
    observers: Vec<Weak<dyn DiskObserver>>,
}

impl Default for BackPressure {
    fn default() -> Self {
        Self::new()
    }
}

/// `aux::back_pressure`.
pub struct BackPressure {
    /// `m_max_size` — level at which back-pressure applies (default 9).
    max_size: Mutex<i64>,
    /// `m_high_watermark` — start flushing.
    high_watermark: Mutex<i64>,
    /// `m_low_watermark` — stop flushing.
    low_watermark: Mutex<i64>,
    inner: Mutex<Inner>,
    /// `m_ios` — where callbacks get posted.
    poster: Poster,
}

impl BackPressure {
    /// `back_pressure(ios)` — synchronous callback posting (tests).
    #[must_use]
    pub fn new() -> Self {
        Self::with_poster(Arc::new(direct_post))
    }

    /// `back_pressure(ios)` with an injected poster (session's event
    /// loop / channel).
    #[must_use]
    pub fn with_poster(poster: Poster) -> Self {
        Self {
            max_size: Mutex::new(9),
            high_watermark: Mutex::new(0),
            low_watermark: Mutex::new(0),
            inner: Mutex::new(Inner {
                exceeded: false,
                observers: Vec::new(),
            }),
            poster,
        }
    }

    /// `set_max_size(max_size)` — derives watermarks exactly like
    /// upstream (`max/4*3` low, `max/8*7` high, with the same
    /// `int` saturation clamps).
    pub fn set_max_size(&self, max_size: i64) {
        *self.max_size.lock().unwrap_or_else(|e| e.into_inner()) = max_size;
        *self.low_watermark.lock().unwrap_or_else(|e| e.into_inner()) =
            std::cmp::min(max_size / 4 * 3, i64::from(i32::MAX / 3));
        *self
            .high_watermark
            .lock()
            .unwrap_or_else(|e| e.into_inner()) =
            std::cmp::min(max_size / 8 * 7, i64::from(i32::MAX / 2));
    }

    /// `should_flush(level)` — `Some(low_watermark)` while above high
    /// watermark or while the sticky exceeded flag is set.
    #[must_use]
    pub fn should_flush(&self, level: i64) -> Option<i64> {
        let high = *self
            .high_watermark
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let exceeded = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .exceeded;
        if level >= high || exceeded {
            Some(*self.low_watermark.lock().unwrap_or_else(|e| e.into_inner()))
        } else {
            None
        }
    }

    /// `has_back_pressure(level, observer)` — registers `o` and returns
    /// true when `level >= max_size`.
    pub fn has_back_pressure(&self, level: i64, o: Option<Arc<dyn DiskObserver>>) -> bool {
        let max = *self.max_size.lock().unwrap_or_else(|e| e.into_inner());
        if level >= max {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            g.exceeded = true;
            if let Some(o) = o {
                g.observers.push(Arc::downgrade(&o));
            }
            return true;
        }
        false
    }

    /// `check_buffer_level(level)` — below the low watermark after an
    /// exceed, posts the accumulated observers.
    pub fn check_buffer_level(&self, level: i64) {
        let low = *self.low_watermark.lock().unwrap_or_else(|e| e.into_inner());
        let cbs = {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if !g.exceeded || level > low {
                return;
            }
            g.exceeded = false;
            std::mem::take(&mut g.observers)
        };
        (self.poster)(cbs);
    }
}
