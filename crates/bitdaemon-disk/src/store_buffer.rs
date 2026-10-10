//! Port of `aux_/store_buffer.hpp` — the disk-bound read cache: buffers
//! read from disk waiting to be sent to peers, with back-pressure
//! watermark tracking.
//!
//! Upstream stores raw `char const*`; we keep `Arc<[u8]>` (`Box<[u8]>`
//! shared) since `forbid(unsafe_code)` forbids raw-pointing into the
//! disk buffer pool from an arbitrary lifetime.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bitdaemon_common::types::{PieceIndex, StorageIndex};

use crate::back_pressure::{BackPressure, DiskObserver, Poster};

/// `aux::torrent_location` — identifies a slice of a piece of a torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TorrentLocation {
    /// `torrent`.
    pub torrent: StorageIndex,
    /// `piece`.
    pub piece: PieceIndex,
    /// `offset`.
    pub offset: i32,
}

/// `aux::store_buffer`.
pub struct StoreBuffer {
    inner: Mutex<StoreInner>,
}

struct StoreInner {
    map: HashMap<TorrentLocation, Arc<[u8]>>,
    back_pressure: BackPressure,
}

impl StoreBuffer {
    /// `store_buffer(ios)` — `poster` dispatches back-pressure release
    /// notifications (upstream posts onto the network io_context).
    #[must_use]
    pub fn new(poster: Poster) -> Self {
        Self {
            inner: Mutex::new(StoreInner {
                map: HashMap::new(),
                back_pressure: BackPressure::with_poster(poster),
            }),
        }
    }

    /// `set_max_size` — total buffer capacity (drives the watermarks).
    pub fn set_max_size(&self, max_size: i64) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .back_pressure
            .set_max_size(max_size);
    }

    /// `get(loc, f)` — call `f` with the stored buffer if present.
    pub fn get<R>(&self, loc: TorrentLocation, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.map.get(&loc).map(|b| f(b))
    }

    /// `get2(loc1, loc2, f)` — call `f` with two (possibly absent)
    /// buffers; returns `None` if both miss.
    pub fn get2<R>(
        &self,
        loc1: TorrentLocation,
        loc2: TorrentLocation,
        f: impl FnOnce(Option<&[u8]>, Option<&[u8]>) -> R,
    ) -> Option<R> {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let b1 = g.map.get(&loc1).map(Arc::clone);
        let b2 = g.map.get(&loc2).map(Arc::clone);
        if b1.is_none() && b2.is_none() {
            return None;
        }
        drop(g);
        Some(f(b1.as_deref(), b2.as_deref()))
    }

    /// `insert(loc, buf, observer)` — returns true while under
    /// back-pressure (store buffer above the high watermark).
    pub fn insert(
        &self,
        loc: TorrentLocation,
        buf: Arc<[u8]>,
        observer: Option<Arc<dyn DiskObserver>>,
    ) -> bool {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.map.insert(loc, buf);
        let level = g.map.len() as i64;
        g.back_pressure.has_back_pressure(level, observer)
    }

    /// `erase(loc)` — removes the buffer; releases observers once the
    /// level drops below the low watermark.
    pub fn erase(&self, loc: TorrentLocation) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        debug_assert!(g.map.contains_key(&loc));
        g.map.remove(&loc);
        let level = g.map.len() as i64;
        g.back_pressure.check_buffer_level(level);
    }

    /// `size()` — upstream exposes this under asserts; kept for tests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map
            .len()
    }

    /// `is_empty`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
