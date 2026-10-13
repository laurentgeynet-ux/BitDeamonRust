// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/disk_cache.hpp` + `disk_cache.cpp` — the write-cache
//! bookkeeping: per-piece `cached_piece_entry`, per-block
//! `cached_block_entry` (buffer / flushed / pending-write-job),
//! the `flushed_cursor`/`hasher_cursor` pair and the insert flags
//! (`exceeded_limit`, `need_hasher_kick`).
//!
//! The concurrent hash-queue machinery upstream builds on top
//! (`piece_hasher` workers, `m_v2_hash_queue`, condition-variable
//! wake-ups) is folded into our single-owner disk thread pool —
//! the *state machine* and counters are what callers depend on.

use std::collections::HashMap;

use bitdaemon_common::PieceIndex;
use bitdaemon_common::types::StorageIndex;

/// `default_block_size` — 16 KiB blocks.
pub const DEFAULT_BLOCK_SIZE: i64 = 0x4000;

/// `piece_location` — the cache key.
pub type PieceLocation = (StorageIndex, PieceIndex);

/// [`DiskCache::take_coalesced`]'s return: `(job_loc, first_block,
/// spans, bufs)` where `spans` splits `bufs` into per-piece
/// `(piece, first_block, n_bufs)` runs.
pub type CoalescedFlush = (
    PieceLocation,
    i32,
    Vec<(PieceIndex, i32, usize)>,
    Vec<Vec<u8>>,
);

/// `cached_block_entry`.
#[derive(Debug)]
pub struct CachedBlockEntry {
    /// `buf` — owned buffer; `None` once the write job took it.
    pub buf: Option<Vec<u8>>,
    /// `is_flushed()` — already on disk.
    pub flushed: bool,
    /// `get_write_job() != nullptr` — a write job is in flight.
    pub has_write_job: bool,
    /// `piece_hasher` result — block SHA-256 computed at flush time
    /// (ADR-0016); `hash2` jobs reuse it instead of re-reading.
    pub block_hash: Option<bitdaemon_common::Sha256Hash>,
}

impl CachedBlockEntry {
    /// `data()`/`buf()` — the cached bytes.
    #[must_use]
    pub fn data(&self) -> Option<&[u8]> {
        self.buf.as_deref()
    }
}

/// `cached_piece_entry` flag bits.
pub mod piece_flags {
    /// `force_flush_flag` — flush on completion regardless.
    pub const FORCE_FLUSH: u16 = 1 << 0;
    /// `hashing_flag` — a hasher owns this piece right now.
    pub const HASHING: u16 = 1 << 1;
    /// `flushing_flag` — a flush is in flight.
    pub const FLUSHING: u16 = 1 << 2;
    /// `piece_hash_returned_flag` — the hash was delivered;
    /// flushes the remainder (`compute_force_flush`).
    pub const PIECE_HASH_RETURNED: u16 = 1 << 3;
    /// `v1_hashes_flag` — v1 hashes available for this piece.
    pub const V1_HASHES: u16 = 1 << 4;
}

/// `compute_force_flush` — a piece that hashed or completed its
/// hasher cursor must flush the tail blocks.
#[must_use]
pub fn compute_force_flush(piece: &CachedPieceEntry) -> bool {
    piece.hasher_cursor == piece.blocks.len() as u16
        || piece.flags & piece_flags::PIECE_HASH_RETURNED != 0
}

/// `compute_flushed_cursor` — count of contiguous flushed blocks
/// from index 0.
#[must_use]
pub fn compute_flushed_cursor(blocks: &[CachedBlockEntry]) -> u16 {
    let mut ret = 0;
    for b in blocks {
        if !b.flushed {
            return ret;
        }
        ret += 1;
    }
    ret
}

/// `count_jobs` — blocks with an in-flight write job.
#[must_use]
pub fn count_jobs(blocks: &[CachedBlockEntry]) -> u16 {
    blocks.iter().filter(|b| b.has_write_job).count() as u16
}

/// Longest contiguous run of flushable blocks (`buf` present, not
/// flushed, no in-flight write) — the piece's `runs_index` score.
#[must_use]
fn best_dirty_run(p: &CachedPieceEntry) -> usize {
    let mut best = 0;
    let mut cur = 0;
    for b in &p.blocks {
        if b.buf.is_some() && !b.flushed && !b.has_write_job {
            cur += 1;
            best = best.max(cur);
        } else {
            cur = 0;
        }
    }
    best
}

/// `cached_piece_entry`.
#[derive(Debug)]
pub struct CachedPieceEntry {
    /// `piece_size` (v1 view — full piece).
    pub piece_size: i64,
    /// `piece_size2` (v2 view — may differ at the tail).
    pub piece_size2: i64,
    /// one entry per `DEFAULT_BLOCK_SIZE` block.
    pub blocks: Vec<CachedBlockEntry>,
    /// `flushed_cursor` — contiguous blocks already on disk.
    pub flushed_cursor: u16,
    /// `hasher_cursor` — blocks the hasher consumed.
    pub hasher_cursor: u16,
    /// `num_jobs` — outstanding jobs referencing this piece.
    pub num_jobs: i32,
    /// `v2_pending` — blocks sitting in the v2 hash queue.
    pub v2_pending: u16,
    /// `cached_piece_flags`.
    pub flags: u16,
    /// `v1`/`v2` protocol kinds this piece feeds.
    pub v1: bool,
    /// v2 piece.
    pub v2: bool,
    /// when the first dirty block arrived — drives `cache_expiry`.
    pub dirty_since: std::time::Instant,
    /// this piece's contribution to [`DiskCache::num_unhashed`].
    pub unhashed: i32,
}

impl CachedPieceEntry {
    /// `blocks_in_piece`.
    #[must_use]
    pub fn blocks_in_piece(&self) -> u16 {
        self.blocks.len() as u16
    }
}

/// `insert_result_flags`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InsertResult {
    /// `exceeded_limit` — back-pressure watermark hit.
    pub exceeded_limit: bool,
    /// `need_hasher_kick` — the hasher can consume this block.
    pub need_hasher_kick: bool,
}

/// `disk_cache` — pieces indexed by `(storage, piece)`.
#[derive(Debug, Default)]
pub struct DiskCache {
    pieces: HashMap<PieceLocation, CachedPieceEntry>,
    /// `m_lru` — dirty pieces in oldest-first order; `take_oldest_run`
    /// drains from the front instead of scanning the whole map on
    /// every block (upstream keeps the same linked list). Stale
    /// entries (fully flushed / removed) are skipped lazily.
    lru: std::collections::VecDeque<PieceLocation>,
    /// Pieces with every block present (downloaded or hashed) —
    /// upstream's `force_flush`/`cheap` passes: they yield large
    /// contiguous runs (up to a whole piece) so they are flushed
    /// *before* random partial-piece runs, which produce small
    /// scattered writes that throttle the disk. Stale entries are
    /// skipped lazily like `lru`.
    ready: std::collections::VecDeque<PieceLocation>,
    /// Upstream's ordered `get<1>` index — pieces ranked by their
    /// longest contiguous dirty run (`hasher_cursor - flushed_cursor`
    /// upstream; our whole-piece hasher can't rank hashed partials,
    /// so the largest flushable run is the closest equivalent).
    /// `run_len` holds each piece's current score; `runs_index` is
    /// `(score, loc)` ordered so `next_back` is the best candidate.
    run_len: HashMap<PieceLocation, usize>,
    runs_index: std::collections::BTreeSet<(usize, PieceLocation)>,
    /// Elevator (SCAN disk-scheduling) index: every piece location
    /// with at least one flushable dirty block, ordered by piece
    /// index (a monotonic proxy for file offset — pieces lay out
    /// sequentially in the backing file(s)). On a seek-bound rotating
    /// disk, draining the cache in LRU/largest-run order jumps the
    /// head across the whole file on every flush; sweeping ascending
    /// piece order instead keeps consecutive flushes close together,
    /// the same trick kernel I/O schedulers use. Entries are removed
    /// lazily by [`Self::take_elevator_run`]/[`Self::remove_piece`].
    dirty_sorted: std::collections::BTreeSet<PieceLocation>,
    /// Where the last elevator flush left off — the next sweep
    /// resumes just past it, wrapping to the lowest piece index once
    /// past the end (classic "look"/SCAN cycle, not back-and-forth).
    scan_head: Option<PieceLocation>,
    /// `m_blocks` — blocks whose buffers the cache (or the v2 queue)
    /// owns; feeds `back_pressure`.
    pub num_cached_blocks: i64,
    /// `m_num_unhashed` — v1 blocks awaiting the hasher.
    pub num_unhashed: i64,
}

/// `piece_entry_params` — what `insert` needs to know about a job.
#[derive(Clone, Copy, Debug)]
pub struct PieceParams {
    /// `piece_size`.
    pub piece_size: i64,
    /// `piece_size2`.
    pub piece_size2: i64,
    /// v1 hash interest.
    pub v1: bool,
    /// v2 hash interest.
    pub v2: bool,
}

impl DiskCache {
    /// `insert(loc, block_idx, force_flush, params)` — register a
    /// write block. `has_buffer` mirrors "the job carried a buffer"
    /// (upstream moves it into the entry or the v2 queue).
    pub fn insert(
        &mut self,
        loc: PieceLocation,
        block_idx: usize,
        buf: Option<Vec<u8>>,
        force_flush: bool,
        params: PieceParams,
    ) -> InsertResult {
        let piece = self.pieces.entry(loc).or_insert_with(|| {
            let num_blocks =
                ((params.piece_size + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE) as usize;
            CachedPieceEntry {
                piece_size: params.piece_size,
                piece_size2: params.piece_size2,
                blocks: (0..num_blocks)
                    .map(|_| CachedBlockEntry {
                        buf: None,
                        flushed: false,
                        has_write_job: false,
                        block_hash: None,
                    })
                    .collect(),
                flushed_cursor: 0,
                hasher_cursor: 0,
                num_jobs: 0,
                v2_pending: 0,
                flags: 0,
                v1: params.v1,
                v2: params.v2,
                dirty_since: std::time::Instant::now(),
                unhashed: 0,
            }
        });
        // first dirty buffer on this piece → join the lru so
        // `take_oldest_run` finds it in O(1) (upstream `m_lru`).
        let had_dirty = piece.blocks.iter().any(|x| x.buf.is_some());
        if !had_dirty && buf.is_some() {
            self.lru.push_back(loc);
        }
        if buf.is_some() {
            self.dirty_sorted.insert(loc);
        }
        if let Some(b) = piece.blocks.get_mut(block_idx) {
            // `m_num_unhashed` counts cached blocks the hasher has
            // not consumed — only a freshly landed buffer adds one;
            // a duplicate insert replaces the buffer in place.
            if params.v1 && buf.is_some() && b.buf.is_none() {
                piece.unhashed += 1;
                self.num_unhashed += 1;
            }
            b.buf = buf;
            b.has_write_job = true;
        }
        // the piece just became complete (every block buffered or
        // already flushed) → candidate for a large contiguous flush
        // before partial pieces (upstream `force_flush` pass).
        if piece.blocks.iter().all(|b| b.buf.is_some() || b.flushed) {
            self.ready.push_back(loc);
        }
        if params.v2 {
            piece.v2_pending += 1;
        }
        self.num_cached_blocks += 1;
        if force_flush || compute_force_flush(piece) {
            piece.flags |= piece_flags::FORCE_FLUSH;
        }
        piece.num_jobs += 1;

        let mut ret = InsertResult::default();
        if params.v1
            && piece.hasher_cursor as usize == block_idx
            && piece.flags & piece_flags::PIECE_HASH_RETURNED == 0
        {
            ret.need_hasher_kick = true;
        }
        self.refresh_run_index(loc);
        ret
    }

    /// `try_clear_piece` — refuses while a write job or hashing is
    /// in flight; on success the piece is dropped and its buffers
    /// freed (returns the freed entry like upstream counts them).
    pub fn try_clear_piece(&mut self, loc: PieceLocation) -> bool {
        let Some(p) = self.pieces.get(&loc) else {
            return true;
        };
        if count_jobs(&p.blocks) > 0 || p.flags & piece_flags::HASHING != 0 {
            return false;
        }
        let freed = p.blocks.iter().filter(|b| b.buf.is_some()).count() as i64;
        self.num_cached_blocks -= freed;
        self.remove_piece(loc);
        true
    }

    /// `free_piece` — bookkeeping when a piece leaves the cache.
    pub fn free_piece(&mut self, loc: PieceLocation) {
        if let Some(p) = self.pieces.get(&loc) {
            let freed = p.blocks.iter().filter(|b| b.buf.is_some()).count() as i64;
            self.num_cached_blocks -= freed;
        }
        self.remove_piece(loc);
    }

    /// Single removal point — settles the piece's `unhashed` tally
    /// whatever the exit path (cleared, freed, fully flushed).
    fn remove_piece(&mut self, loc: PieceLocation) {
        if let Some(p) = self.pieces.remove(&loc) {
            self.num_unhashed -= i64::from(p.unhashed);
        }
        if let Some(o) = self.run_len.remove(&loc) {
            self.runs_index.remove(&(o, loc));
        }
        self.dirty_sorted.remove(&loc);
    }

    /// Recompute the piece's `runs_index` score after any mutation of
    /// its flushable-block set.
    fn refresh_run_index(&mut self, loc: PieceLocation) {
        let Some(p) = self.pieces.get(&loc) else {
            if let Some(o) = self.run_len.remove(&loc) {
                self.runs_index.remove(&(o, loc));
            }
            return;
        };
        let n = best_dirty_run(p);
        match self.run_len.get(&loc).copied() {
            Some(o) if o == n => {}
            Some(o) => {
                self.runs_index.remove(&(o, loc));
                if n > 0 {
                    self.runs_index.insert((n, loc));
                    self.run_len.insert(loc, n);
                } else {
                    self.run_len.remove(&loc);
                }
            }
            None => {
                if n > 0 {
                    self.runs_index.insert((n, loc));
                    self.run_len.insert(loc, n);
                }
            }
        }
    }

    /// read-side lookup — the block's cached bytes.
    #[must_use]
    pub fn get(&self, loc: PieceLocation, block_idx: usize) -> Option<&[u8]> {
        self.pieces
            .get(&loc)
            .and_then(|p| p.blocks.get(block_idx))
            .and_then(|b| b.data())
    }

    /// mark a write job finished — upstream `blocks[i].write_state`
    /// bookkeeping + `flushed_cursor` advance.
    pub fn write_job_done(&mut self, loc: PieceLocation, block_idx: usize, flushed: bool) {
        if let Some(p) = self.pieces.get_mut(&loc) {
            if let Some(b) = p.blocks.get_mut(block_idx) {
                b.has_write_job = false;
                if flushed {
                    b.flushed = true;
                    b.buf = None;
                    self.num_cached_blocks -= 1;
                }
            }
            p.flushed_cursor = compute_flushed_cursor(&p.blocks);
            p.num_jobs -= 1;
        }
        self.refresh_run_index(loc);
    }

    /// `try_flush_blocks` — take the first contiguous run of dirty,
    /// not-in-flight blocks of `loc` for a coalesced write. Buffers
    /// are moved out; blocks are marked `has_write_job` until
    /// [`Self::run_flushed`] retires them.
    pub fn take_flush_run(&mut self, loc: PieceLocation) -> Option<(i32, Vec<Vec<u8>>)> {
        let p = self.pieces.get_mut(&loc)?;
        let start = p
            .blocks
            .iter()
            .position(|b| b.buf.is_some() && !b.flushed && !b.has_write_job)?;
        let mut bufs = Vec::new();
        for b in &mut p.blocks[start..] {
            let Some(buf) = b.buf.take() else {
                break;
            };
            if b.flushed {
                break;
            }
            b.has_write_job = true;
            bufs.push(buf);
        }
        let out = (!bufs.is_empty()).then_some((start as i32, bufs));
        self.refresh_run_index(loc);
        out
    }

    /// Take the piece's *longest* contiguous run of dirty,
    /// not-in-flight blocks — backing [`Self::take_largest_run`]
    /// (upstream's ordered `get<1>` pass writes the most
    /// contiguous span first).
    fn take_longest_run(&mut self, loc: PieceLocation) -> Option<(i32, Vec<Vec<u8>>)> {
        let p = self.pieces.get_mut(&loc)?;
        let mut best_start = 0usize;
        let mut best_len = 0usize;
        let mut cur_start = 0usize;
        let mut cur_len = 0usize;
        for (i, b) in p.blocks.iter().enumerate() {
            if b.buf.is_some() && !b.flushed && !b.has_write_job {
                if cur_len == 0 {
                    cur_start = i;
                }
                cur_len += 1;
                if cur_len > best_len {
                    best_len = cur_len;
                    best_start = cur_start;
                }
            } else {
                cur_len = 0;
            }
        }
        if best_len == 0 {
            return None;
        }
        let mut bufs = Vec::with_capacity(best_len);
        for b in &mut p.blocks[best_start..best_start + best_len] {
            let Some(buf) = b.buf.take() else {
                break;
            };
            b.has_write_job = true;
            bufs.push(buf);
        }
        let out = (!bufs.is_empty()).then_some((best_start as i32, bufs));
        self.refresh_run_index(loc);
        out
    }

    /// One run from the piece with the largest contiguous dirty span
    /// — upstream's ordered flush pass ("flush pieces that have the
    /// most contiguous blocks"). Bigger writes coalesce into
    /// near-sequential disk traffic instead of scattered 2-3-block
    /// writes. Skips stale index entries lazily.
    pub fn take_largest_run(&mut self) -> Option<(PieceLocation, i32, Vec<Vec<u8>>)> {
        while let Some(&(len, loc)) = self.runs_index.iter().next_back() {
            self.runs_index.remove(&(len, loc));
            self.run_len.remove(&loc);
            if let Some((first, bufs)) = self.take_longest_run(loc) {
                return Some((loc, first, bufs));
            }
        }
        None
    }

    /// Elevator/SCAN pick: the lowest dirty piece location whose index
    /// is `>= scan_head`, wrapping to the overall lowest once past the
    /// end — a monotonic sweep instead of the LRU/largest-run order's
    /// essentially random jumps. Not upstream (libtorrent doesn't
    /// distinguish disk media), but a standard disk-scheduling
    /// technique and the dominant lever for seek-bound rotating
    /// disks: on an HDD, `fkb`-sized (tens of KiB) concurrent writes
    /// from several threads at scattered offsets cost one seek each —
    /// sweeping ascending keeps consecutive flushes close together.
    /// Stale/emptied entries are dropped lazily like the other
    /// indices.
    pub fn take_elevator_run(&mut self) -> Option<(PieceLocation, i32, Vec<Vec<u8>>)> {
        loop {
            let probe = self.scan_head.map_or_else(
                || self.dirty_sorted.iter().next().copied(),
                |head| {
                    self.dirty_sorted
                        .range(head..)
                        .next()
                        .or_else(|| self.dirty_sorted.iter().next())
                        .copied()
                },
            );
            let loc = probe?;
            if let Some((first, bufs)) = self.take_flush_run(loc) {
                // resume just past this piece next sweep; if it's
                // still dirty (a gap/in-flight write left more
                // behind), the next `range(head..)` call finds it
                // again naturally without special-casing.
                self.scan_head = Some((loc.0, PieceIndex(loc.1.0 + 1)));
                return Some((loc, first, bufs));
            }
            // emptied/stale — drop it and keep sweeping.
            self.dirty_sorted.remove(&loc);
        }
    }

    /// `coalesce_writes` — take one run (ready → elevator sweep →
    /// largest → oldest) then extend it across piece boundaries: a
    /// run ending at a piece's last block followed by the next
    /// piece's dirty blocks starting at 0 is one contiguous file
    /// region, flushed as a single writev like upstream builds its
    /// iovec over the ordered dirty-block set. Stops at `max_blocks`
    /// buffers or the first gap. Returns `(job_loc, first_block,
    /// spans, bufs)` where `spans` splits `bufs` into per-piece
    /// `(piece, first_block, n_bufs)` runs for `run_flushed`/
    /// `restore_run` bookkeeping.
    pub fn take_coalesced(&mut self, max_blocks: usize) -> Option<CoalescedFlush> {
        let (job_loc, job_first, mut bufs) = self
            .take_ready_run()
            .or_else(|| self.take_elevator_run())
            .or_else(|| self.take_largest_run())
            .or_else(|| self.take_oldest_run())?;
        let mut spans = vec![(job_loc.1, job_first, bufs.len())];
        let mut end_loc = job_loc;
        // exclusive block index just past the last buffer taken
        let mut end = usize::try_from(job_first).unwrap_or(0) + bufs.len();
        while bufs.len() < max_blocks {
            // the run must reach the end of its piece to continue
            let reaches_end = self
                .pieces
                .get(&end_loc)
                .is_some_and(|p| end == p.blocks.len());
            if !reaches_end {
                break;
            }
            let next = (end_loc.0, PieceIndex(end_loc.1.0 + 1));
            // …and the next piece must start dirty at block 0
            let starts_at_zero = self.pieces.get(&next).is_some_and(|p| {
                p.blocks
                    .first()
                    .is_some_and(|b| b.buf.is_some() && !b.flushed && !b.has_write_job)
            });
            if !starts_at_zero {
                break;
            }
            let Some((nfirst, nbufs)) = self.take_flush_run(next) else {
                break;
            };
            if nfirst != 0 {
                self.restore_run(next, usize::try_from(nfirst).unwrap_or(0), nbufs);
                break;
            }
            spans.push((next.1, nfirst, nbufs.len()));
            end = nbufs.len();
            end_loc = next;
            bufs.extend(nbufs);
        }
        Some((job_loc, job_first, spans, bufs))
    }

    /// All dirty contiguous runs of `loc` (for fence-time full flush).
    /// Repeatedly drains [`Self::take_flush_run`].
    pub fn take_all_runs(&mut self, loc: PieceLocation) -> Vec<(i32, Vec<Vec<u8>>)> {
        let mut runs = Vec::new();
        while let Some(r) = self.take_flush_run(loc) {
            runs.push(r);
        }
        runs
    }

    /// One dirty run from the oldest dirty piece — upstream
    /// `oldest_dirty_piece()` walks `m_lru` front-to-back; scanning
    /// the whole `pieces` map here made the over-limit drain
    /// quadratic (thousands of entries per incoming block on a
    /// 25 k-piece swarm). Stale lru entries are skipped lazily; a
    /// piece still dirty after the taken run re-queues at the back.
    pub fn take_oldest_run(&mut self) -> Option<(PieceLocation, i32, Vec<Vec<u8>>)> {
        while let Some(loc) = self.lru.pop_front() {
            let Some((first, bufs)) = self.take_flush_run(loc) else {
                continue;
            };
            // still dirty (another run behind a gap/in-flight
            // write) → re-queue so it isn't starved.
            let still_dirty = self.pieces.get(&loc).is_some_and(|p| {
                p.blocks
                    .iter()
                    .any(|b| b.buf.is_some() && !b.flushed && !b.has_write_job)
            });
            if still_dirty {
                self.lru.push_back(loc);
            }
            return Some((loc, first, bufs));
        }
        None
    }

    /// One dirty run from the oldest *complete* piece — upstream's
    /// optimistic/`force_flush` pass flushes fully-arrived (or
    /// hashed) pieces first because they coalesce into large
    /// contiguous writes. Returns `None` when no complete piece is
    /// queued; the caller falls back to [`Self::take_oldest_run`]
    /// (the "expensive" partial-piece pass).
    pub fn take_ready_run(&mut self) -> Option<(PieceLocation, i32, Vec<Vec<u8>>)> {
        while let Some(loc) = self.ready.pop_front() {
            let Some((first, bufs)) = self.take_flush_run(loc) else {
                continue;
            };
            // still complete with more dirty blocks behind a
            // gap/in-flight write → re-queue at the back.
            let still_ready = self.pieces.get(&loc).is_some_and(|p| {
                p.blocks.iter().all(|b| b.buf.is_some() || b.flushed)
                    && p.blocks
                        .iter()
                        .any(|b| b.buf.is_some() && !b.flushed && !b.has_write_job)
            });
            if still_ready {
                self.ready.push_back(loc);
            }
            return Some((loc, first, bufs));
        }
        None
    }

    /// Put a failed flush's buffers back — upstream `flush_piece`
    /// only sets `is_flushed` after a successful `writev`; a region
    /// whose write failed stays dirty so the next `take_*_run`
    /// retries it. Blocks already re-inserted (a duplicate write
    /// landed while the flush was in flight) keep their newer buffer.
    pub fn restore_run(&mut self, loc: PieceLocation, first: usize, bufs: Vec<Vec<u8>>) {
        let ready = {
            let Some(p) = self.pieces.get_mut(&loc) else {
                return;
            };
            for (i, buf) in bufs.into_iter().enumerate() {
                if let Some(b) = p.blocks.get_mut(first + i)
                    && b.buf.is_none()
                {
                    b.buf = Some(buf);
                    b.has_write_job = false;
                }
            }
            p.blocks.iter().all(|b| b.buf.is_some() || b.flushed)
        };
        if ready {
            self.ready.push_back(loc);
        }
        self.refresh_run_index(loc);
    }

    /// Every dirty run across the whole cache — used before a
    /// storage-level fence (move/delete) raises.
    pub fn take_all_storage_runs(&mut self) -> Vec<(PieceLocation, i32, Vec<Vec<u8>>)> {
        let locs: Vec<PieceLocation> = self.pieces.keys().copied().collect();
        let mut out = Vec::new();
        for loc in locs {
            for (first, bufs) in self.take_all_runs(loc) {
                out.push((loc, first, bufs));
            }
        }
        out
    }

    /// Mark a flushed region durable — frees the buffers' cache
    /// slots, advances `flushed_cursor`, drops fully-flushed pieces.
    pub fn run_flushed(&mut self, loc: PieceLocation, first: usize, len: usize) {
        let mut free = false;
        if let Some(p) = self.pieces.get_mut(&loc) {
            for b in p.blocks.iter_mut().skip(first).take(len) {
                b.has_write_job = false;
                b.flushed = true;
                b.buf = None;
                self.num_cached_blocks -= 1;
            }
            p.flushed_cursor = compute_flushed_cursor(&p.blocks);
            // the entry leaves the cache once every block is on disk —
            // except v2 pieces keep their (tiny) entry for the stored
            // `block_hash`s that `hash2` jobs consume (ADR-0016).
            free = p.blocks.iter().all(|b| b.flushed)
                && (!p.v2 || p.flags & piece_flags::PIECE_HASH_RETURNED != 0);
        }
        if free {
            self.remove_piece(loc);
        } else {
            self.refresh_run_index(loc);
        }
    }

    /// Store the `piece_hasher` results for a flushed region
    /// (ADR-0016): `hash2` reads them from the entry instead of
    /// re-reading the file.
    pub fn set_block_hashes(
        &mut self,
        loc: PieceLocation,
        first: usize,
        hashes: &[bitdaemon_common::Sha256Hash],
    ) {
        if let Some(p) = self.pieces.get_mut(&loc) {
            for (i, h) in hashes.iter().enumerate() {
                if let Some(b) = p.blocks.get_mut(first + i) {
                    b.block_hash = Some(*h);
                }
            }
        }
    }

    /// `cached_block_entry.block_hash` accessor for `hash2`.
    #[must_use]
    pub fn block_hash(
        &self,
        loc: PieceLocation,
        block_idx: usize,
    ) -> Option<bitdaemon_common::Sha256Hash> {
        self.pieces
            .get(&loc)
            .and_then(|p| p.blocks.get(block_idx))
            .and_then(|b| b.block_hash)
    }

    /// `piece_hash_returned_flag` — the piece hash was delivered;
    /// frees a fully-flushed v2 entry kept only for its block hashes.
    pub fn hash_returned(&mut self, loc: PieceLocation) {
        let mut free = false;
        if let Some(p) = self.pieces.get_mut(&loc) {
            p.flags |= piece_flags::PIECE_HASH_RETURNED;
            // whole-piece hashing consumes every queued v1 block at
            // once — settle the piece's `unhashed` tally here
            // (upstream does it block-by-block via `hashed_block`
            // as its incremental hasher advances).
            self.num_unhashed -= i64::from(p.unhashed);
            p.unhashed = 0;
            free = p.blocks.iter().all(|b| b.flushed);
            // upstream `force_flush_flag` on hash-returned: a hashed
            // piece with dirty blocks is flushed promptly, ahead of
            // partial pieces.
            if !free
                && p.blocks
                    .iter()
                    .any(|b| b.buf.is_some() && !b.flushed && !b.has_write_job)
            {
                self.ready.push_back(loc);
            }
        }
        if free {
            self.remove_piece(loc);
        }
    }

    /// `cache_expiry` sweep — every dirty run of pieces whose oldest
    /// block is older than `max_age` (upstream's periodic flush).
    pub fn take_expired_runs(
        &mut self,
        max_age: std::time::Duration,
    ) -> Vec<(PieceLocation, i32, Vec<Vec<u8>>)> {
        let locs: Vec<PieceLocation> = self
            .pieces
            .iter()
            .filter(|(_, p)| p.dirty_since.elapsed() >= max_age)
            .map(|(l, _)| *l)
            .collect();
        let mut out = Vec::new();
        for loc in locs {
            for (first, bufs) in self.take_all_runs(loc) {
                out.push((loc, first, bufs));
            }
        }
        out
    }

    /// `hasher_cursor` advance — the v1 hasher consumed `block_idx`.
    pub fn hashed_block(&mut self, loc: PieceLocation, block_idx: usize) {
        if let Some(p) = self.pieces.get_mut(&loc)
            && p.hasher_cursor as usize == block_idx
        {
            p.hasher_cursor += 1;
            p.unhashed = p.unhashed.saturating_sub(1);
            self.num_unhashed = self.num_unhashed.saturating_sub(1);
        }
    }

    /// piece accessor.
    #[must_use]
    pub fn piece(&self, loc: PieceLocation) -> Option<&CachedPieceEntry> {
        self.pieces.get(&loc)
    }

    /// `size()`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pieces.len()
    }

    /// empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pieces.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> PieceParams {
        PieceParams {
            piece_size: 3 * DEFAULT_BLOCK_SIZE,
            piece_size2: 3 * DEFAULT_BLOCK_SIZE,
            v1: true,
            v2: false,
        }
    }

    #[test]
    fn insert_and_flags() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        let r = c.insert(loc, 0, Some(vec![1u8; 4]), false, params());
        assert!(r.need_hasher_kick); // block 0 == hasher_cursor
        assert_eq!(c.num_cached_blocks, 1);
        assert_eq!(c.num_unhashed, 1);
        let r = c.insert(loc, 1, Some(vec![2u8; 4]), false, params());
        assert!(!r.need_hasher_kick);
        assert_eq!(c.piece(loc).unwrap().num_jobs, 2);
    }

    #[test]
    fn write_done_advances_cursor() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(1));
        c.insert(loc, 0, Some(vec![0; 4]), false, params());
        c.insert(loc, 1, Some(vec![0; 4]), false, params());
        c.write_job_done(loc, 0, true);
        assert_eq!(c.piece(loc).unwrap().flushed_cursor, 1);
        assert_eq!(c.num_cached_blocks, 1);
        // out-of-order flush: cursor stops at the hole
        c.write_job_done(loc, 2, true);
        c.write_job_done(loc, 1, true);
        assert_eq!(c.piece(loc).unwrap().flushed_cursor, 3);
        assert!(c.try_clear_piece(loc));
        assert!(c.is_empty());
    }

    #[test]
    fn clear_refuses_pending() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![0; 4]), false, params());
        assert!(!c.try_clear_piece(loc));
        c.write_job_done(loc, 0, true);
        assert!(c.try_clear_piece(loc));
    }

    #[test]
    fn force_flush_on_hash_returned() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![0; 4]), false, params());
        c.pieces.get_mut(&loc).unwrap().flags |= piece_flags::PIECE_HASH_RETURNED;
        c.insert(loc, 1, Some(vec![0; 4]), false, params());
        assert!(c.piece(loc).unwrap().flags & piece_flags::FORCE_FLUSH != 0);
    }

    /// `m_num_unhashed` must return to zero once every queued v1
    /// block has been hashed — a stuck counter made `queue_stats`
    /// report 183k unhashed blocks on a completed 2 GiB swarm.
    #[test]
    fn unhashed_settles_on_hash_and_clear() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![0; 4]), false, params());
        c.insert(loc, 1, Some(vec![0; 4]), false, params());
        assert_eq!(c.num_unhashed, 2);
        // duplicate inserts replace in place — no double counting
        c.insert(loc, 1, Some(vec![9; 4]), false, params());
        assert_eq!(c.num_unhashed, 2);
        c.hash_returned(loc);
        assert_eq!(c.num_unhashed, 0);
        // a piece evicted un-hashed settles too
        let loc2 = (StorageIndex(0), PieceIndex(1));
        c.insert(loc2, 0, Some(vec![0; 4]), false, params());
        assert_eq!(c.num_unhashed, 1);
        c.free_piece(loc2);
        assert_eq!(c.num_unhashed, 0);
    }
}
