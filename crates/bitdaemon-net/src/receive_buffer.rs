//! Port of `aux_/receive_buffer.hpp`/`.cpp` — the connection receive
//! buffer: a byte arena with three cursors (`recv_start` /
//! `recv_pos` / `recv_end`), packet-size tracking, watermark-driven
//! shrinking (`SlidingAverage<_, 20>`), and `CryptoReceiveBuffer` for
//! layered crypto framing.
//!
//! Layout (verbatim):
//!
//! ```text
//! | m_recv_buffer                                             |
//! |   m_recv_start |     |   m_recv_pos      |   m_recv_end   |
//! ```

use bitdaemon_common::sliding_average::SlidingAverage64;

/// `aux::receive_buffer`. `reserve()` yields a `&mut [u8]` view of the
/// write window; cursors are indices into `recv_buffer`.
#[derive(Default)]
pub struct ReceiveBuffer {
    /// `m_recv_buffer`.
    buf: Vec<u8>,
    /// `m_recv_start` — start of the logical buffer (current packet).
    recv_start: usize,
    /// `m_recv_pos` — consumed bytes (from `recv_start`).
    recv_pos: usize,
    /// `m_recv_end` — end of received data.
    recv_end: usize,
    /// `m_packet_size` — size of the message currently being received.
    packet_size: usize,
    /// `m_watermark` — `sliding_average<int64_t, 20>` on usage.
    watermark: SlidingAverage64<20>,
}

impl ReceiveBuffer {
    /// `packet_size()`.
    #[must_use]
    pub fn packet_size(&self) -> usize {
        self.packet_size
    }

    /// `packet_bytes_remaining()`.
    #[must_use]
    pub fn packet_bytes_remaining(&self) -> usize {
        debug_assert_eq!(self.recv_start, 0);
        debug_assert!(self.packet_size > 0);
        self.packet_size - self.recv_pos
    }

    /// `max_receive()` — free space at the tail.
    #[must_use]
    pub fn max_receive(&self) -> usize {
        self.buf.len() - self.recv_end
    }

    /// `packet_finished()`.
    #[must_use]
    pub fn packet_finished(&self) -> bool {
        self.packet_size <= self.recv_pos
    }

    /// `pos()`.
    #[must_use]
    pub fn pos(&self) -> usize {
        self.recv_pos
    }

    /// `capacity()`.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// `watermark()`.
    #[must_use]
    pub fn watermark(&self) -> i64 {
        self.watermark.mean()
    }

    /// `reserve(size)` — grow if needed, return the write window.
    /// `normalize()` must have been called first (recv_start == 0).
    pub fn reserve(&mut self, size: usize) -> &mut [u8] {
        debug_assert!(size > 0);
        debug_assert_eq!(self.recv_start, 0);
        if self.buf.len() < self.recv_end + size {
            let new_size = (self.recv_end + size).max(self.packet_size);
            let mut nb = vec![0u8; new_size];
            nb[..self.recv_end].copy_from_slice(&self.buf[..self.recv_end]);
            self.buf = nb;
            // avoid flapping the buffer size
            self.watermark = SlidingAverage64::default();
        }
        &mut self.buf[self.recv_end..self.recv_end + size]
    }

    /// `grow(limit)` — grow to one packet, then by 50% up to `limit`.
    pub fn grow(&mut self, limit: usize) {
        let current = self.buf.len();
        let new_size = if current < self.packet_size {
            self.packet_size
        } else {
            (current * 3 / 2).min(limit)
        };
        let mut nb = vec![0u8; new_size];
        nb[..self.recv_end].copy_from_slice(&self.buf[..self.recv_end]);
        self.buf = nb;
        self.watermark = SlidingAverage64::default();
    }

    /// `received(bytes)` — advance the end cursor.
    pub fn received(&mut self, bytes: usize) {
        debug_assert!(self.packet_size > 0);
        self.recv_end += bytes;
        debug_assert!(self.recv_end <= self.buf.len());
    }

    /// `advance_pos(bytes)` — advance the read cursor, clamped to the
    /// packet size; returns bytes actually advanced.
    pub fn advance_pos(&mut self, bytes: usize) -> usize {
        let limit = if self.packet_size > self.recv_pos {
            self.packet_size - self.recv_pos
        } else {
            self.packet_size
        };
        let sub = bytes.min(limit);
        self.recv_pos += sub;
        sub
    }

    /// `pos_at_end()`.
    #[must_use]
    pub fn pos_at_end(&self) -> bool {
        self.recv_pos == self.recv_end
    }

    /// `cut(size, packet_size, offset)` — drop `size` bytes at
    /// `recv_start + offset` (moving the tail left when offset > 0),
    /// and set the next packet size.
    pub fn cut(&mut self, size: usize, packet_size: usize, offset: usize) {
        debug_assert!(packet_size > 0);
        debug_assert!(self.recv_pos >= size + offset);
        debug_assert!(self.recv_start <= self.recv_end);

        if offset > 0 {
            if size > 0 {
                let dst = self.recv_start + offset;
                let src = dst + size;
                let n = self.recv_end - self.recv_start - size - offset;
                self.buf.copy_within(src..src + n, dst);
            }
            self.recv_pos -= size;
            self.recv_end -= size;
        } else {
            debug_assert!(self.recv_start + size <= self.recv_end);
            self.recv_start += size;
            self.recv_pos -= size;
        }
        self.packet_size = packet_size;
    }

    /// `get()` — the current packet `[start, pos)`.
    #[must_use]
    pub fn get(&self) -> &[u8] {
        &self.buf[self.recv_start..self.recv_start + self.recv_pos]
    }

    /// `mutable_buffer()` — `[start, end)` for crypto in-place work.
    pub fn mutable_buffer(&mut self) -> &mut [u8] {
        &mut self.buf[self.recv_start..self.recv_end]
    }

    /// `mutable_buffer(bytes)` — the last `bytes` received.
    pub fn mutable_buffer_last(&mut self, bytes: usize) -> &mut [u8] {
        let end = self.recv_start + self.recv_pos;
        &mut self.buf[end - bytes..end]
    }

    /// `normalize(force_shrink)` — compact consumed bytes to the front
    /// and shrink the arena if the running usage average is low.
    pub fn normalize(&mut self, force_shrink: usize) {
        debug_assert!(self.recv_end >= self.recv_start);

        self.watermark
            .add_sample((self.recv_end.max(self.packet_size)) as i64);

        let shrink = (self.buf.len() as i64) / 2 > self.watermark.mean()
            && self.watermark.mean() > (self.recv_end - self.recv_start) as i64;

        let n = self.recv_end - self.recv_start;
        if force_shrink > 0 {
            let target = force_shrink.max(n).max(self.packet_size);
            let mut nb = vec![0u8; target];
            nb[..n].copy_from_slice(&self.buf[self.recv_start..self.recv_end]);
            self.buf = nb;
        } else if shrink {
            let mut nb = vec![0u8; self.watermark.mean().max(0) as usize];
            nb[..n].copy_from_slice(&self.buf[self.recv_start..self.recv_end]);
            self.buf = nb;
        } else if self.recv_end > self.recv_start && self.recv_start > 0 {
            self.buf.copy_within(self.recv_start..self.recv_end, 0);
        }

        self.recv_end = n;
        self.recv_start = 0;
    }

    /// `normalized()`.
    #[must_use]
    pub fn normalized(&self) -> bool {
        self.recv_start == 0
    }

    /// `reset(packet_size)` — clear for the next packet; if more data
    /// was received than the current packet, cut it instead.
    pub fn reset(&mut self, packet_size: usize) {
        debug_assert!(packet_size > 0);
        if self.recv_end > self.packet_size {
            self.cut(self.packet_size, packet_size, 0);
            return;
        }
        self.recv_pos = 0;
        self.recv_start = 0;
        self.recv_end = 0;
        self.packet_size = packet_size;
    }

    /// Internal cursor access for `CryptoReceiveBuffer`.
    fn raw_end(&self) -> usize {
        self.recv_end
    }
}

/// `crypto_receive_buffer` — wraps a `ReceiveBuffer` when crypto is
/// layered under the BitTorrent protocol. `m_recv_pos == i32::MAX`
/// upstream = pass-through to the connection buffer; we model it as
/// `recv_pos: Option<usize>` (`None` = pass-through).
pub struct CryptoReceiveBuffer<'a> {
    /// `m_recv_pos` — `None` = `INT_MAX` pass-through.
    recv_pos: Option<usize>,
    /// `m_packet_size` — the upper-layer packet being received.
    packet_size: usize,
    /// `m_connection_buffer`.
    conn: &'a mut ReceiveBuffer,
}

impl<'a> CryptoReceiveBuffer<'a> {
    /// `crypto_receive_buffer(next)`.
    #[must_use]
    pub fn new(conn: &'a mut ReceiveBuffer) -> Self {
        Self {
            recv_pos: None,
            packet_size: 0,
            conn,
        }
    }

    /// `mutable_buffer()`.
    pub fn mutable_buffer(&mut self) -> &mut [u8] {
        self.conn.mutable_buffer()
    }

    /// `packet_finished()`.
    #[must_use]
    pub fn packet_finished(&self) -> bool {
        match self.recv_pos {
            None => self.conn.packet_finished(),
            Some(pos) => self.packet_size <= pos,
        }
    }

    /// `crypto_packet_finished()` — the crypto layer drained its input.
    #[must_use]
    pub fn crypto_packet_finished(&self) -> bool {
        self.recv_pos.is_none() || self.conn.packet_finished()
    }

    /// `packet_size()`.
    #[must_use]
    pub fn packet_size(&self) -> usize {
        self.recv_pos
            .map_or_else(|| self.conn.packet_size(), |_| self.packet_size)
    }

    /// `crypto_packet_size()` — the crypto-layer packet still open.
    #[must_use]
    pub fn crypto_packet_size(&self) -> usize {
        debug_assert!(self.recv_pos.is_some());
        self.conn.packet_size() - self.recv_pos.unwrap_or(0)
    }

    /// `pos()`.
    #[must_use]
    pub fn pos(&self) -> usize {
        self.recv_pos.unwrap_or_else(|| self.conn.pos())
    }

    /// `cut(size, packet_size, offset)`.
    pub fn cut(&mut self, size: usize, packet_size: usize, offset: usize) {
        let mut packet_size = packet_size;
        if let Some(pos) = self.recv_pos {
            debug_assert!(size <= pos);
            self.packet_size = packet_size;
            packet_size = self.conn.packet_size() - size;
            self.recv_pos = Some(pos - size);
        }
        self.conn.cut(size, packet_size, offset);
    }

    /// `crypto_cut(size, packet_size)` — cut the crypto layer at the
    /// current upper-layer position. No-op when crypto tracking is
    /// inactive (upstream `TORRENT_ASSERT(m_recv_pos.is_initialized())`).
    pub fn crypto_cut(&mut self, size: usize, packet_size: usize) {
        let Some(pos) = self.recv_pos else {
            return;
        };
        self.conn.cut(size, pos + packet_size, pos);
    }

    /// `reset(packet_size)`.
    pub fn reset(&mut self, packet_size: usize) {
        let mut packet_size = packet_size;
        if let Some(pos) = self.recv_pos {
            if self.conn.raw_end() > self.packet_size {
                self.cut(self.packet_size, packet_size, 0);
                return;
            }
            self.packet_size = packet_size;
            packet_size = self.conn.packet_size() - pos;
            self.recv_pos = Some(0);
        }
        self.conn.reset(packet_size);
    }

    /// `crypto_reset(packet_size)` — `0` disables crypto tracking;
    /// otherwise starts tracking at the connection buffer's pos and
    /// widens the connection packet to `pos + packet_size`.
    pub fn crypto_reset(&mut self, packet_size: usize) {
        debug_assert!(self.packet_finished());
        debug_assert!(self.crypto_packet_finished());
        debug_assert!(self.recv_pos.is_none_or(|p| p == self.conn.pos()));
        if packet_size == 0 {
            if self.recv_pos.is_some() {
                let ps = self.packet_size;
                self.conn.cut(0, ps, 0);
            }
            self.recv_pos = None;
        } else {
            if self.recv_pos.is_none() {
                self.packet_size = self.conn.packet_size();
            }
            self.recv_pos = Some(self.conn.pos());
            let ps = self.conn.pos() + packet_size;
            self.conn.cut(0, ps, 0);
        }
    }

    /// `advance_pos(bytes)` — when crypto is active, grows the
    /// connection packet by the decrypted amount rather than moving
    /// its cursor (upstream `cut(0, packet_size + sub)`).
    pub fn advance_pos(&mut self, bytes: usize) -> usize {
        match self.recv_pos {
            None => bytes,
            Some(pos) => {
                let limit = if self.packet_size > pos {
                    self.packet_size - pos
                } else {
                    self.packet_size
                };
                let sub = bytes.min(limit);
                self.recv_pos = Some(pos + sub);
                let ps = self.conn.packet_size() + sub;
                self.conn.cut(0, ps, 0);
                sub
            }
        }
    }

    /// `get()` — truncated to `recv_pos` when crypto is active.
    #[must_use]
    pub fn get(&self) -> &[u8] {
        let buf = self.conn.get();
        match self.recv_pos {
            Some(pos) if pos < buf.len() => &buf[..pos],
            _ => buf,
        }
    }

    /// `mutable_buffer(bytes)` — with crypto active, `bytes` is
    /// ignored and the *pending decryption* size is used.
    pub fn mutable_buffer_last(&mut self, bytes: usize) -> &mut [u8] {
        let pending = match self.recv_pos {
            Some(pos) => self.conn.packet_size() - pos,
            None => bytes,
        };
        self.conn.mutable_buffer_last(pending)
    }
}
