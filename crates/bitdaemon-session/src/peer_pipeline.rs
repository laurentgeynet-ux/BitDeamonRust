//! Port of the request-pipeline half of `aux_/peer_connection.cpp`:
//! `pending_block`, `request_queue`/`download_queue`, the
//! `aux::request_merger` (large-block coalescing from
//! `merge_block_requests.hpp`), `send_block_requests_impl`, the AIMD
//! `desired_queue_size` (`update_desired_queue_size`, slow-start,
//! snub) and the adaptive `request_timeout`.
//!
//! The socket side (write_request), extensions and alerts are left to
//! the caller: the pipeline only produces `PeerRequest`s.

use bitdaemon_common::sliding_average::SlidingAverage64;
use bitdaemon_common::{PeerRequest, PieceBlock, PieceIndex};

/// `pending_block::not_in_buffer`.
pub const NOT_IN_BUFFER: u32 = 0x1fff_ffff;

/// `min_request_queue` — lower bound for `desired_queue_size`.
pub const MIN_REQUEST_QUEUE: i32 = 2;

/// `aux::pending_block` — a block queued or in flight.
#[derive(Debug, Clone, Copy)]
pub struct PendingBlock {
    /// `block`.
    pub block: PieceBlock,
    /// `send_buffer_offset` — bytes into the send buffer this request
    /// sits at; `NOT_IN_BUFFER` until written.
    pub send_buffer_offset: u32,
    /// `not_wanted` — kept queued but we don't want the data.
    pub not_wanted: bool,
    /// `timed_out` — request timed out once (may be re-requested).
    pub timed_out: bool,
    /// `busy` — request a block already requested elsewhere.
    pub busy: bool,
    /// `time_critical` — counted in `queued_time_critical`.
    pub time_critical: bool,
}

impl PendingBlock {
    /// `pending_block(b)`.
    #[must_use]
    pub fn new(block: PieceBlock) -> Self {
        Self {
            block,
            send_buffer_offset: NOT_IN_BUFFER,
            not_wanted: false,
            timed_out: false,
            busy: false,
            time_critical: false,
        }
    }
}

/// `aux::request_merger` (`merge_block_requests.hpp`) — accumulates a
/// run of flat-contiguous blocks into one byte-range request.
#[derive(Debug)]
pub struct RequestMerger {
    piece_length: i64,
    block_size: i32,
    /// `m_piece_size` — size of the last piece merged (may be
    /// truncated at a file boundary).
    piece_size: i32,
    /// `m_end` — torrent-space byte offset where the run ends.
    end: i64,
}

impl RequestMerger {
    /// Constructed with the first block already merged in.
    #[must_use]
    pub fn new(
        first: PieceBlock,
        first_piece_size: i32,
        piece_length: i64,
        block_size: i32,
    ) -> Self {
        Self {
            piece_length,
            block_size,
            piece_size: first_piece_size,
            end: Self::start_offset(first, piece_length, block_size)
                + i64::from((first_piece_size - first.block_index * block_size).min(block_size)),
        }
    }

    fn start_offset(b: PieceBlock, piece_length: i64, block_size: i32) -> i64 {
        i64::from(b.piece_index.0) * piece_length + i64::from(b.block_index) * i64::from(block_size)
    }

    /// `continues(next)` — does `next` start exactly where the run
    /// ends? Also rejects a piece following a truncated one.
    #[must_use]
    pub fn continues(&self, next: PieceBlock) -> bool {
        Self::start_offset(next, self.piece_length, self.block_size) == self.end
    }

    /// `merge(next, next_piece_size)` — requires `continues(next)`;
    /// returns the bytes contributed.
    pub fn merge(&mut self, next: PieceBlock, next_piece_size: i32) -> i32 {
        debug_assert!(self.continues(next));
        let block_offset = next.block_index * self.block_size;
        let bs = (next_piece_size - block_offset).min(self.block_size);
        debug_assert!(bs > 0);
        debug_assert!(bs <= self.block_size);
        self.piece_size = next_piece_size;
        self.end += i64::from(bs);
        bs
    }

    /// `piece_size()` — size of the last merged piece.
    #[must_use]
    pub fn piece_size(&self) -> i32 {
        self.piece_size
    }
}

/// Torrent-side reads the pipeline needs (`t.*` upstream).
pub trait PipelineTorrent {
    /// `t->block_size()`.
    fn block_size(&self) -> i32;
    /// `t->torrent_file().piece_length()`.
    fn piece_length(&self) -> i64;
    /// `t->piece_size_for_req(piece)` — real (possibly truncated) size.
    fn piece_size_for_req(&self, piece: PieceIndex) -> i32;
    /// `t->to_req(block, piece_size)` — block → byte-range request.
    fn to_req(&self, block: PieceBlock, piece_size: i32) -> PeerRequest;
    /// `t->graceful_pause()`.
    fn graceful_pause(&self) -> bool {
        false
    }
    /// `t->upload_mode()`.
    fn upload_mode(&self) -> bool {
        false
    }
}

/// The request pipeline state of `peer_connection` (request queue →
/// download queue → wire requests).
#[derive(Debug)]
pub struct PeerPipeline {
    /// `m_request_queue` — picked blocks not yet sent.
    pub request_queue: Vec<PendingBlock>,
    /// `m_download_queue` — requests sent, awaiting data.
    pub download_queue: Vec<PendingBlock>,
    /// `m_desired_queue_size` — pipeline depth target (AIMD).
    pub desired_queue_size: u16,
    /// `m_max_out_request_queue` — `settings.max_out_request_queue`.
    pub max_out_request_queue: u16,
    /// `m_slow_start` — AIMD additive phase.
    pub slow_start: bool,
    /// `m_snubbed` — a request timed out; queue target drops to 1.
    pub snubbed: bool,
    /// `m_endgame_mode` — only busy blocks were pickable.
    pub endgame: bool,
    /// `m_outstanding_bytes` — payload bytes in flight.
    pub outstanding_bytes: i64,
    /// `m_queued_time_critical` — critical blocks in both queues.
    pub queued_time_critical: i32,
    /// `m_request_large_blocks` — coalesce via `RequestMerger`.
    pub request_large_blocks: bool,
    /// `m_request_time` — RTT samples (`sliding_average<int,20>`).
    pub request_time: SlidingAverage64<20>,
    /// `request_queue_time` setting (seconds).
    pub queue_time: i32,
    /// `request_timeout` setting (seconds).
    pub request_timeout_setting: i32,
    /// ms timestamp of last request sent (`m_last_request`).
    pub last_request_ms: i64,
    /// ms timestamp when the download queue last became non-empty
    /// (`m_requested` — drives the request timeout).
    pub requested_ms: i64,
    /// ms timestamp of last piece received (`m_last_piece`).
    pub last_piece_ms: i64,
}

impl PeerPipeline {
    /// `peer_connection` ctor defaults: `desired_queue_size = 4`,
    /// slow-start on.
    #[must_use]
    pub fn new(queue_time: i32, request_timeout: i32, max_out_request_queue: u16) -> Self {
        Self {
            request_queue: Vec::new(),
            download_queue: Vec::new(),
            desired_queue_size: 4,
            max_out_request_queue,
            slow_start: true,
            snubbed: false,
            endgame: false,
            outstanding_bytes: 0,
            queued_time_critical: 0,
            request_large_blocks: false,
            request_time: SlidingAverage64::<20>::default(),
            queue_time,
            request_timeout_setting: request_timeout,
            last_request_ms: 0,
            requested_ms: 0,
            last_piece_ms: 0,
        }
    }

    /// `desired_queue_size()` — collapses to 1 in end-game/snub.
    #[must_use]
    pub fn desired_queue_size(&self) -> i32 {
        if self.endgame || self.snubbed {
            1
        } else {
            i32::from(self.desired_queue_size)
        }
    }

    /// `update_desired_queue_size()` — snub ⇒ 1; slow-start only
    /// enforces bounds; else `queue_time * rate / block_size`,
    /// clamped to `[2, max_out_request_queue]`.
    pub fn update_desired_queue_size(&mut self, download_payload_rate: i64, block_size: i32) {
        if self.snubbed {
            self.desired_queue_size = 1;
            return;
        }
        if !self.slow_start {
            debug_assert!(block_size > 0);
            let desired =
                i64::from(self.queue_time) * download_payload_rate / i64::from(block_size);
            self.desired_queue_size = desired.clamp(0, i64::from(u16::MAX)) as u16;
        }
        // clamp can't invert when `max_out_request_queue` is set
        // below `MIN_REQUEST_QUEUE` — the cap wins (upstream floors
        // `max_out_request_queue` at MIN too).
        self.desired_queue_size = self.desired_queue_size.clamp(
            (MIN_REQUEST_QUEUE as u16).min(self.max_out_request_queue),
            self.max_out_request_queue,
        );
    }

    /// Slow-start bump (`incoming_piece`: `m_slow_start` ⇒ +1) plus
    /// the RTT sample (`m_request_time.add_sample`) and the snub
    /// reset — data arrived, the peer isn't stalling.
    pub fn on_piece_received(&mut self, now_ms: i64) {
        if self.slow_start {
            self.desired_queue_size += 1;
        }
        if self.last_request_ms != 0 && now_ms > self.last_request_ms {
            self.request_time.add_sample(now_ms - self.last_request_ms);
        }
        self.snubbed = false;
        self.last_piece_ms = now_ms;
    }

    /// `request_timeout()` — mean+4σ ms → seconds, capped by the
    /// setting, never below 2.
    #[must_use]
    pub fn request_timeout(&self) -> i64 {
        let deviation = self.request_time.avg_deviation();
        let avg = self.request_time.mean();
        let ret = if self.request_time.num_samples() < 2 {
            if self.request_time.num_samples() == 0 {
                return i64::from(self.request_timeout_setting);
            }
            avg + avg / 5
        } else {
            avg + deviation * 4
        };
        // ms → s (round up), capped by the setting, min 2 s
        ((ret + 999) / 1000)
            .min(i64::from(self.request_timeout_setting))
            .max(2)
    }

    /// `send_block_requests_impl` — move queued blocks to the wire.
    ///
    /// Returns the `PeerRequest`s to write (already coalesced when
    /// `request_large_blocks`). `already_downloaded` is the callback
    /// into the picker for the "arrived anyway" case: it must check
    /// `is_downloaded` and call `abort_download` — returning true
    /// drops the block without sending.
    pub fn send_block_requests<T: PipelineTorrent>(
        &mut self,
        t: &T,
        send_buffer_size: usize,
        now_ms: i64,
        mut already_downloaded: impl FnMut(PieceBlock) -> bool,
    ) -> Vec<PeerRequest> {
        if t.graceful_pause() || t.upload_mode() {
            return Vec::new();
        }
        if self.download_queue.len() as i32 >= self.desired_queue_size() {
            return Vec::new();
        }

        let mut out = Vec::new();
        let empty_download_queue = self.download_queue.is_empty();
        let mut send_offset = send_buffer_size as u32;

        while !self.request_queue.is_empty()
            && ((self.download_queue.len() as i32) < self.desired_queue_size()
                || self.queued_time_critical > 0)
        {
            let mut block = self.request_queue.remove(0);
            if self.queued_time_critical > 0 {
                self.queued_time_critical -= 1;
            }

            // arrived "unexpectedly" (timed-out re-request that came
            // anyway): don't send it
            if already_downloaded(block.block) {
                continue;
            }

            let piece_sz = t.piece_size_for_req(block.block.piece_index);
            let mut r = t.to_req(block.block, piece_sz);
            block.send_buffer_offset = send_offset;
            self.download_queue.push(block);
            self.outstanding_bytes += i64::from(r.length);

            if self.request_large_blocks {
                let mut merger =
                    RequestMerger::new(block.block, piece_sz, t.piece_length(), t.block_size());
                while let Some(&front) = self.request_queue.first() {
                    if !merger.continues(front.block) {
                        break;
                    }
                    let new_piece = front.block.piece_index != block.block.piece_index;
                    let next_sz = if new_piece {
                        t.piece_size_for_req(front.block.piece_index)
                    } else {
                        merger.piece_size()
                    };
                    let bs = merger.merge(front.block, next_sz);
                    let block = self.request_queue.remove(0);
                    self.download_queue.push(block);
                    if self.queued_time_critical > 0 {
                        self.queued_time_critical -= 1;
                    }
                    r.length += bs;
                    self.outstanding_bytes += i64::from(bs);
                }
            }

            // the *request* message (not the payload) occupies
            // 4 (len) + 1 (id) + 12 (request fields) bytes in the
            // send buffer
            send_offset = send_offset.saturating_add(17);
            self.last_request_ms = now_ms;
            out.push(r);
        }

        if !self.download_queue.is_empty() && empty_download_queue {
            self.requested_ms = now_ms;
        }
        out
    }
}
