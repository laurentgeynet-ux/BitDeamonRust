//! Port of `aux_/disk_buffer_pool.hpp`/`disk_buffer_pool.cpp` +
//! `disk_buffer_holder.hpp`.
//!
//! Upstream allocates raw `default_block_size` (16 KiB) blocks and hands
//! out `char*`; ownership returns via `disk_buffer_holder`. In Rust the
//! buffer is a boxed slice and `DiskBufferHolder` returns it to the
//! allocator on drop — same lifecycle, no raw pointers.
//!
//! The bound itself is not enforced here (upstream mallocs
//! unconditionally too); the cap lives in `back_pressure` +
//! `store_buffer` watermarks, wired in `disk_io` (wave 3).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// `libtorrent::default_block_size` — 16 KiB.
pub const DEFAULT_BLOCK_SIZE: usize = 0x4000;

/// One disk buffer. Upstream hands out `default_block_size` bytes.
pub type DiskBuffer = Box<[u8]>;

/// `buffer_allocator_interface` — frees buffers back to the pool.
/// Implemented by `DiskBufferPool`; downstream storage may inject
/// alternatives.
pub trait BufferAllocator: Send + Sync {
    /// `free_disk_buffer`.
    fn free_disk_buffer(&self, buf: DiskBuffer);
    /// `free_multiple_buffers` — batch free under a single lock.
    fn free_multiple_buffers(&self, bufs: Vec<DiskBuffer>) {
        for b in bufs {
            self.free_disk_buffer(b);
        }
    }
}

/// `aux::disk_buffer_pool`. Tracks `m_in_use` and a per-category
/// histogram (upstream `TORRENT_DEBUG_BUFFER_POOL` — kept always-on,
/// it's a single map increment).
#[derive(Default)]
pub struct DiskBufferPool {
    in_use: AtomicI64,
    histogram: Mutex<HashMap<&'static str, i64>>,
}

impl DiskBufferPool {
    /// `disk_buffer_pool()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `allocate_buffer(category)` — upstream may return nullptr on OOM;
    /// Rust allocation aborts, so this is infallible.
    #[must_use]
    pub fn allocate_buffer(self: &Arc<Self>, category: &'static str) -> DiskBufferHolder {
        let buf: DiskBuffer = vec![0u8; DEFAULT_BLOCK_SIZE].into_boxed_slice();
        self.in_use.fetch_add(1, Ordering::Relaxed);
        let mut h = self.histogram.lock().unwrap_or_else(|e| e.into_inner());
        *h.entry(category).or_insert(0) += 1;
        DiskBufferHolder {
            buf: Some(buf),
            alloc: Some(self.clone()),
            category,
        }
    }

    /// `in_use()` — buffers currently checked out.
    #[must_use]
    pub fn in_use(&self) -> i64 {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Per-category counts (upstream `m_histogram`).
    #[must_use]
    pub fn histogram(&self) -> HashMap<&'static str, i64> {
        self.histogram
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn free_one(&self, category: &'static str) {
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        let mut h = self.histogram.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = h.get_mut(category) {
            *n -= 1;
        }
    }
}

impl BufferAllocator for DiskBufferPool {
    fn free_disk_buffer(&self, _buf: DiskBuffer) {
        self.in_use.fetch_sub(1, Ordering::Relaxed);
    }
}

/// `disk_buffer_holder` — RAII owner; drop frees back to the allocator.
pub struct DiskBufferHolder {
    buf: Option<DiskBuffer>,
    alloc: Option<Arc<DiskBufferPool>>,
    category: &'static str,
}

impl DiskBufferHolder {
    /// Empty holder (upstream default ctor).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            buf: None,
            alloc: None,
            category: "",
        }
    }

    /// `data()`.
    #[must_use]
    pub fn data(&self) -> Option<&[u8]> {
        self.buf.as_deref()
    }

    /// mutable access — buffers returned to the network thread are
    /// writeable.
    #[must_use]
    pub fn data_mut(&mut self) -> Option<&mut [u8]> {
        self.buf.as_deref_mut()
    }

    /// `operator bool`.
    #[must_use]
    pub fn is_some(&self) -> bool {
        self.buf.is_some()
    }

    /// `reset()` — free now.
    pub fn reset(&mut self) {
        if let (Some(b), Some(a)) = (self.buf.take(), self.alloc.as_ref()) {
            a.free_one(self.category);
            drop(b);
        } else {
            self.buf = None;
        }
    }

    /// Take the buffer without freeing (ownership transfer, e.g. into a
    /// job's `disk_buffer_holder` field).
    #[must_use]
    pub fn take(&mut self) -> Option<DiskBuffer> {
        let b = self.buf.take()?;
        if let Some(a) = &self.alloc {
            a.free_one(self.category);
        }
        Some(b)
    }

    /// `rename_buffer` — retag the category (upstream debug facility;
    /// kept cheap and always-on).
    pub fn rename(&mut self, category: &'static str) {
        if let Some(a) = &self.alloc {
            let mut h = a.histogram.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(n) = h.get_mut(self.category) {
                *n -= 1;
            }
            *h.entry(category).or_insert(0) += 1;
        }
        self.category = category;
    }
}

impl Drop for DiskBufferHolder {
    fn drop(&mut self) {
        self.reset();
    }
}

/// `bulk_free_buffer` — accumulate holders, free in one batch.
#[derive(Default)]
pub struct BulkFreeBuffer {
    bufs: Vec<(DiskBuffer, &'static str)>,
    alloc: Option<Arc<DiskBufferPool>>,
}

impl BulkFreeBuffer {
    /// `bulk_free_buffer(alloc)`.
    #[must_use]
    pub fn new(alloc: Arc<DiskBufferPool>) -> Self {
        Self {
            bufs: Vec::new(),
            alloc: Some(alloc),
        }
    }

    /// `add(holder)` — move the buffer into the batch.
    pub fn add(&mut self, mut h: DiskBufferHolder) {
        if let Some(b) = h.buf.take() {
            self.bufs.push((b, h.category));
            h.alloc = None;
        }
    }
}

impl Drop for BulkFreeBuffer {
    fn drop(&mut self) {
        if let Some(a) = &self.alloc {
            let n = i64::try_from(self.bufs.len()).unwrap_or(0);
            a.in_use.fetch_sub(n, Ordering::Relaxed);
            let mut hist = a.histogram.lock().unwrap_or_else(|e| e.into_inner());
            for (_, cat) in &self.bufs {
                if let Some(c) = hist.get_mut(cat) {
                    *c -= 1;
                }
            }
        }
        self.bufs.clear();
    }
}
