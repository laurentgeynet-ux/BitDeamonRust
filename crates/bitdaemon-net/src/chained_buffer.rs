//! Port of `aux_/chained_buffer.hpp`/`.cpp` — the send-buffer: a chain
//! of heterogenous buffer holders with used/total-size accounting,
//! slack-append at the tail (`allocate_appendix`), front popping, and
//! scatter-gather vector construction (`build_iovec`).
//!
//! Upstream type-erases holders into a fixed storage blob with
//! `destruct_holder`/`move_holder` function pointers; the Rust
//! version uses `Box<dyn BufferHolder>` so `Drop` replaces the manual
//! destructor thunk, and `offset` replaces `buf += bytes` pointer
//! arithmetic.

use std::collections::VecDeque;

/// A buffer that can be chained into a `ChainedBuffer`.
/// `size()` is the *allocated* length of `data()` — the slack beyond
/// `used_size` is appendable when this is the last buffer.
pub trait BufferHolder: Send {
    /// The full allocated region (send window starts at `offset`).
    fn data(&self) -> &[u8];
    /// Mutable view of the full allocated region.
    fn data_mut(&mut self) -> &mut [u8];
}

impl BufferHolder for Vec<u8> {
    fn data(&self) -> &[u8] {
        self
    }

    fn data_mut(&mut self) -> &mut [u8] {
        self
    }
}

/// `buffer_t` — one entry: holder + logical window.
struct BufferT {
    /// `holder` — the owning allocation.
    holder: Box<dyn BufferHolder>,
    /// `buf` upstream = `data() + offset`.
    offset: usize,
    /// `size` — total allocation size remaining (shrinks on pop).
    size: usize,
    /// `used_size` — bytes to send from `offset`.
    used_size: usize,
}

/// `aux::chained_buffer`.
#[derive(Default)]
pub struct ChainedBuffer {
    /// `m_vec`.
    vec: VecDeque<BufferT>,
    /// `m_bytes` — total used bytes across the chain.
    bytes: usize,
    /// `m_capacity` — total allocated size across the chain.
    capacity: usize,
}

impl ChainedBuffer {
    /// `size()` — bytes queued to send.
    #[must_use]
    pub fn size(&self) -> usize {
        self.bytes
    }

    /// `capacity()`.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// `empty()`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    /// `append_buffer(buffer, used_size)` — `used_size` may be smaller
    /// than the allocation (`has_size` case upstream); the slack stays
    /// appendable at the tail.
    pub fn append_buffer<H: BufferHolder + 'static>(&mut self, buffer: H, used_size: usize) {
        let size = buffer.data().len();
        debug_assert!(size >= used_size);
        self.bytes += used_size;
        self.capacity += size;
        self.vec.push_back(BufferT {
            holder: Box::new(buffer),
            offset: 0,
            size,
            used_size,
        });
    }

    /// `prepend_buffer(buffer, used_size)`.
    pub fn prepend_buffer<H: BufferHolder + 'static>(&mut self, buffer: H, used_size: usize) {
        let size = buffer.data().len();
        debug_assert!(size >= used_size);
        self.bytes += used_size;
        self.capacity += size;
        self.vec.push_front(BufferT {
            holder: Box::new(buffer),
            offset: 0,
            size,
            used_size,
        });
    }

    /// `pop_front(bytes_to_pop)` — consume bytes from the head;
    /// whole buffers are dropped, partial pops advance the offset.
    pub fn pop_front(&mut self, bytes_to_pop: usize) {
        debug_assert!(bytes_to_pop <= self.bytes);
        let mut to_pop = bytes_to_pop;
        while to_pop > 0 && !self.vec.is_empty() {
            let b = &mut self.vec[0];
            if b.used_size > to_pop {
                b.offset += to_pop;
                b.used_size -= to_pop;
                b.size -= to_pop;
                self.capacity -= to_pop;
                self.bytes -= to_pop;
                break;
            }
            self.bytes -= b.used_size;
            self.capacity -= b.size;
            to_pop -= b.used_size;
            self.vec.pop_front();
        }
    }

    /// `space_in_last_buffer()` — appendable slack at the tail.
    #[must_use]
    pub fn space_in_last_buffer(&self) -> usize {
        self.vec.back().map_or(0, |b| b.size - b.used_size)
    }

    /// `append(buf)` — copy `buf` into the tail slack; `false` if it
    /// doesn't fit (upstream returns `nullptr`).
    #[must_use]
    pub fn append(&mut self, buf: &[u8]) -> bool {
        match self.allocate_appendix(buf.len()) {
            Some(slot) => {
                slot.copy_from_slice(buf);
                true
            }
            None => false,
        }
    }

    /// `allocate_appendix(s)` — reserve `s` bytes of tail slack and
    /// return the writable window (`None` if it doesn't fit).
    pub fn allocate_appendix(&mut self, s: usize) -> Option<&mut [u8]> {
        let b = self.vec.back_mut()?;
        if b.offset + b.used_size + s > b.offset + b.size {
            return None;
        }
        let start = b.offset + b.used_size;
        b.used_size += s;
        self.bytes += s;
        debug_assert!(self.bytes <= self.capacity);
        Some(&mut b.holder.data_mut()[start..start + s])
    }

    /// `build_iovec(to_send)` — up to `to_send` bytes as slices.
    #[must_use]
    pub fn build_iovec(&self, to_send: usize) -> Vec<&[u8]> {
        let mut vec = Vec::new();
        let mut bytes = to_send;
        for b in &self.vec {
            if bytes == 0 {
                break;
            }
            let n = b.used_size.min(bytes);
            vec.push(&b.holder.data()[b.offset..b.offset + n]);
            bytes -= n;
        }
        vec
    }

    /// `build_mutable_iovec(bytes, vec)`.
    pub fn build_mutable_iovec<'a>(&'a mut self, bytes: usize, vec: &mut Vec<&'a mut [u8]>) {
        let mut bytes = bytes;
        for b in &mut self.vec {
            if bytes == 0 {
                break;
            }
            let n = b.used_size.min(bytes);
            let off = b.offset;
            vec.push(&mut b.holder.data_mut()[off..off + n]);
            bytes -= n;
        }
    }

    /// `clear()`.
    pub fn clear(&mut self) {
        self.bytes = 0;
        self.capacity = 0;
        self.vec.clear();
    }
}
