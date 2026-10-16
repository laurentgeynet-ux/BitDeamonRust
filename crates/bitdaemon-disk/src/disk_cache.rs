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
use std::sync::Arc;

use bitdaemon_common::PieceIndex;
use bitdaemon_common::types::StorageIndex;
use sha1::{Digest as _, Sha1};

use crate::disk_job::DiskJob;

/// `default_block_size` — 16 KiB blocks.
pub const DEFAULT_BLOCK_SIZE: i64 = 0x4000;

/// `piece_location` — the cache key.
pub type PieceLocation = (StorageIndex, PieceIndex);

/// A contiguous run taken for flushing: `(first_block, bufs,
/// writers)` — `bufs[i]` shares the `Arc` still resident on its cache
/// block (upstream `borrowed_buf`: the in-flight buffer stays readable
/// until the write lands); `writers[i]` is the write job parked on the
/// block `bufs[i]` came from (upstream `write_state == disk_job*`,
/// moved out by `take_write_job`); `None` for blocks with no parked
/// writer. Aligned with `bufs` so [`DiskCache::restore_run`] can put
/// both back.
pub type TakenRun = (i32, Vec<Arc<Vec<u8>>>, Vec<Option<Box<DiskJob>>>);

/// [`DiskCache::take_oldest_run`]/located variants: `(loc,
/// first_block, bufs, writers)`.
pub type LocatedRun = (
    PieceLocation,
    i32,
    Vec<Arc<Vec<u8>>>,
    Vec<Option<Box<DiskJob>>>,
);

/// [`DiskCache::take_coalesced`]'s return: `(job_loc, first_block,
/// spans, bufs, writers)` where `spans` splits `bufs` into per-piece
/// `(piece, first_block, n_bufs)` runs and `writers` is aligned with
/// `bufs`.
pub type CoalescedFlush = (
    PieceLocation,
    i32,
    Vec<(PieceIndex, i32, usize)>,
    Vec<Arc<Vec<u8>>>,
    Vec<Option<Box<DiskJob>>>,
);

/// `cached_block_entry`.
#[derive(Debug)]
pub struct CachedBlockEntry {
    /// `buf` — the cached bytes, shared (`Arc`) with any in-flight
    /// flush job so reads/hash keep seeing them until the write
    /// lands — upstream's `borrowed_buf` on `write_state ==
    /// disk_buffer_ref`. `None` once `run_flushed` retires it.
    pub buf: Option<Arc<Vec<u8>>>,
    /// `is_flushed()` — already on disk.
    pub flushed: bool,
    /// `write_state == disk_buffer_ref` — a flush job holds a clone
    /// of `buf` and is writing it out (the buffer stays readable —
    /// upstream's borrowed view; `has_write_job` alone marks the
    /// flight).
    pub has_write_job: bool,
    /// `piece_hasher` result — block SHA-256 computed at flush time
    /// (ADR-0016); `hash2` jobs reuse it instead of re-reading.
    pub block_hash: Option<bitdaemon_common::Sha256Hash>,
    /// `write_state == disk_job*` — the write job parked on this
    /// block. Upstream completes it when the buffer is durable (or
    /// aborts it when the piece is dropped); a flush takes it along
    /// with the buffer.
    pub writer: Option<Box<DiskJob>>,
}

impl CachedBlockEntry {
    /// `data()`/`buf()` — the cached bytes.
    #[must_use]
    pub fn data(&self) -> Option<&[u8]> {
        self.buf.as_deref().map(Vec::as_slice)
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
    /// `needs_hasher_kick_flag` — a `need_hasher_kick` was already
    /// emitted for the current `hasher_cursor`; cleared when the
    /// hasher consumes the block (`hashed_block`).
    pub const NEEDS_HASHER_KICK: u16 = 1 << 5;
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

/// `count_jobs` — blocks with a job attached: a parked writer
/// (`write_state == disk_job*`) or an in-flight flush
/// (`disk_buffer_ref`).
#[must_use]
pub fn count_jobs(blocks: &[CachedBlockEntry]) -> u16 {
    blocks
        .iter()
        .filter(|b| b.has_write_job || b.writer.is_some())
        .count() as u16
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
    /// `cached_piece_flags`.
    pub flags: u16,
    /// `v1`/`v2` protocol kinds this piece feeds.
    pub v1: bool,
    /// v2 piece.
    pub v2: bool,
    /// `piece_hasher` — the incremental SHA-1 context, fed block by
    /// block from [`DiskCache::insert`] over the contiguous buffered
    /// prefix at `hasher_cursor` (upstream `kick_hasher`). Only
    /// allocated for `v1` pieces.
    pub ph: Option<Sha1>,
    /// The finalized piece SHA-1 once `hasher_cursor` reached
    /// `blocks_in_piece` — upstream `ph->final_hash()` served by
    /// `try_hash_piece`'s inline completion. `None` until then, and
    /// invalidated by any rewrite below the cursor.
    pub piece_hash: Option<bitdaemon_common::Sha1Hash>,
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
    /// Buffers taken out by `take_*_run` and not yet retired —
    /// upstream's in-flight `writev` region (its `m_blocks` still
    /// covers those until `flush_piece_impl` retires them, which is
    /// synchronous there; our flushes are async jobs, so the level
    /// checks subtract this to predict the post-completion level —
    /// `flush_to_disk`'s `buffer_level() <= target` exit test).
    pub in_flight_blocks: i64,
    /// `m_num_unhashed` — v1 blocks awaiting the hasher.
    pub num_unhashed: i64,
    /// Write jobs superseded by a duplicate insert or evicted with
    /// their piece — the cache can't complete jobs, so the caller
    /// drains this queue and pushes them aborted onto the completed
    /// queue (upstream `jobqueue_t& aborted` out-params).
    pub orphans: std::collections::VecDeque<Box<DiskJob>>,
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

/// Upstream asserts the cache is empty when it is destroyed
/// (`~disk_cache` requires drained pieces). Dropping a parked write
/// job silently would lose its handler invocation — drain any
/// leftovers and complete them aborted, the same exit path
/// `abort_jobs` uses.
impl Drop for DiskCache {
    fn drop(&mut self) {
        for (_, mut p) in self.pieces.drain() {
            for b in &mut p.blocks {
                if let Some(mut w) = b.writer.take() {
                    crate::disk_completed::abort(&mut w);
                    w.call_callback();
                }
            }
        }
        // jobs already evicted to `orphans` (superseded/cleared
        // writers) were aborted when they moved — their handlers
        // still run exactly once even if nobody drained the queue.
        for mut w in self.orphans.drain(..) {
            w.call_callback();
        }
    }
}

impl DiskCache {
    /// `insert(loc, block_idx, writer, force_flush, params)` —
    /// register a write block. `buf` mirrors "the job carried a
    /// buffer"; `writer` is the write job parked on the block
    /// (upstream `blk.write_state = write_job`) — it completes when
    /// the block flushes.
    pub fn insert(
        &mut self,
        loc: PieceLocation,
        block_idx: usize,
        buf: Option<Vec<u8>>,
        writer: Option<Box<DiskJob>>,
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
                        writer: None,
                    })
                    .collect(),
                flushed_cursor: 0,
                hasher_cursor: 0,
                num_jobs: 0,
                flags: 0,
                v1: params.v1,
                v2: params.v2,
                ph: params.v1.then(Sha1::new),
                piece_hash: None,
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
        // `need_hasher_kick` reports an insert landing exactly on the
        // hasher cursor — upstream wakes a hasher thread; ours feeds
        // `ph` inline below, so the flag is advisory (nothing
        // consumes it). Evaluated pre-feed, like upstream.
        let mut ret = InsertResult::default();
        if params.v1
            && piece.hasher_cursor as usize == block_idx
            && piece.flags & piece_flags::PIECE_HASH_RETURNED == 0
            && piece.flags & piece_flags::NEEDS_HASHER_KICK == 0
        {
            ret.need_hasher_kick = true;
            piece.flags |= piece_flags::NEEDS_HASHER_KICK;
        }
        // a rewrite below the hasher's consumed prefix invalidates
        // the stored piece hash — upstream rules this out by assert
        // (`block_idx >= hasher_cursor`); our graceful path re-dirties
        // and rehashes instead of answering a stale digest.
        let invalidate = buf.is_some()
            && block_idx < piece.hasher_cursor as usize
            && block_idx < piece.blocks.len();
        if invalidate {
            piece.piece_hash = None;
            piece.ph = params.v1.then(Sha1::new);
            piece.hasher_cursor = 0;
            piece.flags &= !(piece_flags::PIECE_HASH_RETURNED | piece_flags::NEEDS_HASHER_KICK);
        }
        let mut added = false;
        if let Some(b) = piece.blocks.get_mut(block_idx) {
            if buf.is_some() {
                // `m_num_unhashed` counts cached v1 blocks ahead of
                // the hasher cursor — only a freshly landed buffer
                // adds one; a duplicate replaces the buffer in place.
                if !invalidate && b.buf.is_none() && block_idx >= piece.hasher_cursor as usize {
                    piece.unhashed += 1;
                    self.num_unhashed += 1;
                }
                // a buffer landing on an already-`flushed` block is a
                // rewrite (upstream asserts `block_idx >=
                // flushed_cursor`) — clear the flag so the next flush
                // pass can take it instead of leaving it parked
                // forever with a writer that never completes.
                if b.flushed {
                    b.flushed = false;
                }
            }
            // `++m_blocks` upstream only fires when a buffer actually
            // moves into the slot (`if (wjob.buf)`); a duplicate write
            // replaces the buffer in place — the slot stays count 1.
            added = buf.is_some() && b.buf.is_none();
            b.buf = buf.map(Arc::new);
            // upstream `blk.write_state = write_job` — the writer
            // rides the block until a flush takes it (`take_write_job`)
            // or the piece is torn down (`aborted`).
            if let Some(w) = writer {
                // a duplicate write on a block that still holds a
                // parked writer: the older job is superseded — abort
                // it rather than leak its completion (upstream
                // overwrites write_state and aborts the old job). Its
                // buffer's slot charge transferred to the duplicate,
                // so the job must not be released again by the Flush
                // path: clear `CACHED` so `job_done` releases the
                // back-pressure slot it was charged at submit.
                if let Some(mut old) = b.writer.replace(w) {
                    old.flags &= !crate::disk_job::job_flags::CACHED;
                    crate::disk_completed::abort(&mut old);
                    self.orphans.push_back(old);
                    piece.num_jobs -= 1;
                }
            }
            piece.num_jobs += 1;
        } else if let Some(mut w) = writer {
            // upstream asserts `block_idx < blocks_in_piece` — a stray
            // write must still complete, aborted, once. Its charged
            // `write_blocks` slot never covered a cache slot — clear
            // `CACHED` so `job_done` releases it (same rule as the
            // superseded writer).
            w.flags &= !crate::disk_job::job_flags::CACHED;
            crate::disk_completed::abort(&mut w);
            self.orphans.push_back(w);
        }
        if invalidate {
            // the new `ph` re-consumes every buffered block — recount
            // `unhashed` over the whole piece (the cursor restart
            // re-unhashes the old prefix too).
            let now_unhashed =
                i32::try_from(piece.blocks.iter().filter(|b| b.buf.is_some()).count())
                    .unwrap_or(i32::MAX);
            self.num_unhashed += i64::from(now_unhashed) - i64::from(piece.unhashed);
            piece.unhashed = now_unhashed;
        }
        // upstream `kick_hasher`: feed the incremental piece_hasher
        // every contiguous buffered block from `hasher_cursor` — the
        // SHA-1 runs as data lands, before any flush takes it, so a
        // piece that's complete-and-hashed flushes *after* hashing
        // and a later `hash` job answers from `piece_hash` without
        // re-reading the file.
        if params.v1
            && piece.flags & piece_flags::PIECE_HASH_RETURNED == 0
            && piece.piece_hash.is_none()
        {
            while let Some(b) = piece.blocks.get(piece.hasher_cursor as usize) {
                let Some(buf) = b.buf.as_deref() else {
                    break;
                };
                let need = usize::try_from(
                    (piece.piece_size - i64::from(piece.hasher_cursor) * DEFAULT_BLOCK_SIZE)
                        .min(DEFAULT_BLOCK_SIZE),
                )
                .unwrap_or(0);
                // upstream asserts the cached buffer covers the whole
                // block (`TORRENT_ASSERT_VAL` in `buf()`) — a short
                // buffer stalls the cursor rather than feeding pad.
                if buf.len() < need {
                    break;
                }
                if let Some(ph) = piece.ph.as_mut() {
                    ph.update(&buf[..need]);
                }
                piece.hasher_cursor += 1;
                piece.flags &= !piece_flags::NEEDS_HASHER_KICK;
                if piece.unhashed > 0 {
                    piece.unhashed -= 1;
                    self.num_unhashed -= 1;
                }
            }
            if piece.hasher_cursor as usize == piece.blocks.len()
                && piece.piece_hash.is_none()
                && let Some(ph) = piece.ph.take()
            {
                piece.piece_hash = Some(
                    bitdaemon_common::Sha1Hash::from_bytes(&ph.finalize()).unwrap_or_default(),
                );
            }
        }
        // the piece just became complete (every block buffered or
        // already flushed) → candidate for a large contiguous flush
        // before partial pieces (upstream `force_flush` pass).
        if piece.blocks.iter().all(|b| b.buf.is_some() || b.flushed) {
            self.ready.push_back(loc);
        }
        if added {
            self.num_cached_blocks += 1;
        }
        if force_flush || compute_force_flush(piece) {
            piece.flags |= piece_flags::FORCE_FLUSH;
        }

        self.refresh_run_index(loc);
        ret
    }

    /// Drain write jobs superseded/evicted since the last call —
    /// caller completes them aborted (upstream `aborted` out-params).
    pub fn take_orphans(&mut self) -> Vec<Box<DiskJob>> {
        std::mem::take(&mut self.orphans).into_iter().collect()
    }

    /// Abort every writer still parked in the cache, queueing them
    /// on `orphans` — teardown callers (`remove_torrent`,
    /// `abort_jobs`) drain them through the completed queue so each
    /// handler still runs exactly once. Returns the number of cached
    /// buffers that evaporate with the cache (blocks still holding a
    /// `buf`, i.e. not out inside an in-flight flush — those release
    /// their slot when the Flush job completes) so the caller can
    /// release the matching `write_blocks` back-pressure slots.
    pub fn abort_all_writers(&mut self) -> i64 {
        let mut bufs = 0i64;
        for p in self.pieces.values_mut() {
            for b in &mut p.blocks {
                // in-flight buffers die with their Flush job — its
                // `job_done` releases the `write_blocks` slots, so
                // only resident buffers count here.
                if b.buf.is_some() && !b.has_write_job {
                    bufs += 1;
                }
                if let Some(mut w) = b.writer.take() {
                    crate::disk_completed::abort(&mut w);
                    self.orphans.push_back(w);
                }
            }
        }
        bufs
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
        let freed = p
            .blocks
            .iter()
            .filter(|b| b.buf.is_some() && !b.has_write_job)
            .count() as i64;
        self.num_cached_blocks -= freed;
        self.remove_piece(loc);
        true
    }

    /// `free_piece` — bookkeeping when a piece leaves the cache.
    /// Returns the number of cache blocks freed (for the caller's
    /// `write_blocks` back-pressure accounting) — writers parked on
    /// the piece are queued on `orphans`. Buffers out inside an
    /// in-flight flush don't count: the Flush job's `job_done`
    /// releases their `write_blocks` slots (they'd double-release).
    pub fn free_piece(&mut self, loc: PieceLocation) -> i64 {
        let mut freed = 0i64;
        if let Some(p) = self.pieces.get(&loc) {
            freed = p
                .blocks
                .iter()
                .filter(|b| b.buf.is_some() && !b.has_write_job)
                .count() as i64;
            self.num_cached_blocks -= freed;
        }
        self.remove_piece(loc);
        freed
    }

    /// Single removal point — settles the piece's `unhashed` tally
    /// whatever the exit path (cleared, freed, fully flushed), and
    /// moves any writer still parked on a block to `orphans`
    /// (upstream `aborted.push_back(cbe.take_write_job())` in
    /// `delete_cached_piece`).
    fn remove_piece(&mut self, loc: PieceLocation) {
        if let Some(mut p) = self.pieces.remove(&loc) {
            self.num_unhashed -= i64::from(p.unhashed);
            for b in &mut p.blocks {
                if let Some(mut w) = b.writer.take() {
                    crate::disk_completed::abort(&mut w);
                    self.orphans.push_back(w);
                }
            }
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

    /// `disk_cache::get2` — fetch two adjacent blocks for a spanning
    /// (unaligned) read; `get` covers the aligned case.
    #[must_use]
    pub fn get2(&self, loc: PieceLocation, block_idx: usize) -> (Option<&[u8]>, Option<&[u8]>) {
        self.pieces.get(&loc).map_or((None, None), |p| {
            (
                p.blocks.get(block_idx).and_then(|b| b.data()),
                p.blocks.get(block_idx + 1).and_then(|b| b.data()),
            )
        })
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
    /// not-in-flight blocks of `loc` for a coalesced write. The
    /// buffers are *shared* with the Flush job (`borrowed_buf`
    /// upstream): the block keeps serving its bytes to reads and the
    /// hasher until `run_flushed` retires them. The parked writers
    /// move out (upstream `take_write_job` per block); blocks are
    /// marked `has_write_job` until [`Self::run_flushed`] retires
    /// them.
    pub fn take_flush_run(&mut self, loc: PieceLocation) -> Option<TakenRun> {
        let p = self.pieces.get_mut(&loc)?;
        let start = p
            .blocks
            .iter()
            .position(|b| b.buf.is_some() && !b.flushed && !b.has_write_job)?;
        let mut bufs = Vec::new();
        let mut writers = Vec::new();
        for b in &mut p.blocks[start..] {
            // an in-flight block keeps its buffer (borrowed view) —
            // `has_write_job` must bound the run or the same bytes
            // would be handed to a second Flush job.
            if b.flushed || b.has_write_job {
                break;
            }
            let Some(buf) = b.buf.as_ref() else {
                break;
            };
            b.has_write_job = true;
            writers.push(b.writer.take());
            bufs.push(Arc::clone(buf));
        }
        let out = (!bufs.is_empty()).then_some((start as i32, bufs, writers));
        if let Some((_, bufs, _)) = &out {
            self.in_flight_blocks += bufs.len() as i64;
        }
        self.refresh_run_index(loc);
        out
    }

    /// Take the piece's *longest* contiguous run of dirty,
    /// not-in-flight blocks — backing [`Self::take_largest_run`]
    /// (upstream's ordered `get<1>` pass writes the most
    /// contiguous span first).
    fn take_longest_run(&mut self, loc: PieceLocation) -> Option<TakenRun> {
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
        let mut writers = Vec::new();
        for b in &mut p.blocks[best_start..best_start + best_len] {
            let Some(buf) = b.buf.as_ref() else {
                break;
            };
            b.has_write_job = true;
            writers.push(b.writer.take());
            bufs.push(Arc::clone(buf));
        }
        let out = (!bufs.is_empty()).then_some((best_start as i32, bufs, writers));
        if let Some((_, bufs, _)) = &out {
            self.in_flight_blocks += bufs.len() as i64;
        }
        self.refresh_run_index(loc);
        out
    }

    /// One run from the piece with the largest contiguous dirty span
    /// — upstream's ordered flush pass ("flush pieces that have the
    /// most contiguous blocks"). Bigger writes coalesce into
    /// near-sequential disk traffic instead of scattered 2-3-block
    /// writes. Skips stale index entries lazily.
    pub fn take_largest_run(&mut self) -> Option<LocatedRun> {
        while let Some(&(len, loc)) = self.runs_index.iter().next_back() {
            self.runs_index.remove(&(len, loc));
            self.run_len.remove(&loc);
            if let Some((first, bufs, writers)) = self.take_longest_run(loc) {
                return Some((loc, first, bufs, writers));
            }
        }
        None
    }

    /// Ascending-piece pick — upstream `flush_to_disk`'s last pass
    /// iterates `m_pieces`' `get<0>` index (ordered by
    /// `(storage, piece)`), flushing every dirty run of each piece it
    /// visits; the sweep is a monotonic `(storage, piece)` walk, so
    /// consecutive flushes stay close together on disk. We resume at
    /// `scan_head` instead of restarting from 0 each call, and a
    /// piece left dirty behind a gap is found again on wrap. Under
    /// the whole-piece hasher, pass 2 (`cheap_to_flush`) never has a
    /// positive score, so this ascending pass is the routine drain
    /// order — same as upstream once its hasher has nothing cheap
    /// left.
    /// Stale/emptied entries are dropped lazily like the other
    /// indices.
    pub fn take_elevator_run(&mut self) -> Option<LocatedRun> {
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
            if let Some((first, bufs, writers)) = self.take_flush_run(loc) {
                // resume just past this piece next sweep; if it's
                // still dirty (a gap/in-flight write left more
                // behind), the next `range(head..)` call finds it
                // again naturally without special-casing.
                self.scan_head = Some((loc.0, PieceIndex(loc.1.0 + 1)));
                return Some((loc, first, bufs, writers));
            }
            // emptied/stale — drop it and keep sweeping.
            self.dirty_sorted.remove(&loc);
        }
    }

    /// `coalesce_writes` — take one run then extend it across piece
    /// boundaries: a run ending at a piece's last block followed by
    /// the next piece's dirty blocks starting at 0 is one contiguous
    /// file region, flushed as a single writev like upstream builds
    /// its iovec over the ordered dirty-block set. Stops at
    /// `max_blocks` buffers or the first gap. Returns `(job_loc,
    /// first_block, spans, bufs, writers)` where `spans` splits `bufs`
    /// into per-piece `(piece, first_block, n_bufs)` runs for
    /// `run_flushed`/`restore_run` bookkeeping.
    ///
    /// Pick order mirrors the passes upstream's `flush_to_disk`
    /// actually reaches under a whole-piece hasher: ready pieces
    /// first (pass 1 — `force_flush_flag`, plus complete un-hashed
    /// pieces which upstream's incremental hasher promotes to
    /// force-flush as blocks land; ours must flush before the hash
    /// job can even be issued, since write completions drive it),
    /// then the ascending `(storage, piece)` sweep — upstream's
    /// pass-4 `get<0>` iteration order. Upstream's pass 2
    /// (`cheap_to_flush = hasher_cursor - flushed_cursor`, largest
    /// first) is structurally empty here: `hasher_cursor` only
    /// advances when the whole-piece hash job runs, so an unhashed
    /// piece's score is `0 - flushed_cursor ≤ 0` — upstream skips
    /// those in pass 2 and flushes them in pass 4. `largest` and
    /// `oldest` stay as unreachable-in-practice fallbacks for stale
    /// index entries (the run index covers every dirty piece).
    pub fn take_coalesced(&mut self, max_blocks: usize) -> Option<CoalescedFlush> {
        let (job_loc, job_first, mut bufs, mut writers) = self
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
            let Some((nfirst, nbufs, nwriters)) = self.take_flush_run(next) else {
                break;
            };
            if nfirst != 0 {
                // the run didn't start at block 0 — put bufs *and*
                // their parked writers back untouched
                self.restore_run(next, usize::try_from(nfirst).unwrap_or(0), nbufs, nwriters);
                break;
            }
            spans.push((next.1, nfirst, nbufs.len()));
            end = nbufs.len();
            end_loc = next;
            bufs.extend(nbufs);
            writers.extend(nwriters);
        }
        Some((job_loc, job_first, spans, bufs, writers))
    }

    /// All dirty contiguous runs of `loc` (for fence-time full flush).
    /// Repeatedly drains [`Self::take_flush_run`].
    pub fn take_all_runs(&mut self, loc: PieceLocation) -> Vec<TakenRun> {
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
    pub fn take_oldest_run(&mut self) -> Option<LocatedRun> {
        while let Some(loc) = self.lru.pop_front() {
            let Some((first, bufs, writers)) = self.take_flush_run(loc) else {
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
            return Some((loc, first, bufs, writers));
        }
        None
    }

    /// One dirty run from the oldest *complete* piece — upstream's
    /// optimistic/`force_flush` pass flushes fully-arrived (or
    /// hashed) pieces first because they coalesce into large
    /// contiguous writes. Returns `None` when no complete piece is
    /// queued; the caller falls back to [`Self::take_oldest_run`]
    /// (the "expensive" partial-piece pass).
    pub fn take_ready_run(&mut self) -> Option<LocatedRun> {
        while let Some(loc) = self.ready.pop_front() {
            let Some((first, bufs, writers)) = self.take_flush_run(loc) else {
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
            return Some((loc, first, bufs, writers));
        }
        None
    }

    /// Put a flush's run back — upstream `flush_piece` only sets
    /// `is_flushed` after a successful `writev`; a region whose write
    /// failed stays dirty so the next `take_*_run` retries it. The
    /// buffers never left their cache slot (`borrowed_buf` — the job
    /// held `Arc` clones): restoring only clears the in-flight flag
    /// and re-parks the writers. A block re-inserted while the run
    /// was out keeps its newer buffer; the returned writer is then
    /// superseded and completes aborted once.
    pub fn restore_run(
        &mut self,
        loc: PieceLocation,
        first: usize,
        bufs: Vec<Arc<Vec<u8>>>,
        mut writers: Vec<Option<Box<DiskJob>>>,
    ) {
        // every buffer handed in leaves the in-flight state.
        self.in_flight_blocks -= bufs.len() as i64;
        let ready = {
            let Some(p) = self.pieces.get_mut(&loc) else {
                return;
            };
            for (i, buf) in bufs.iter().enumerate() {
                let Some(b) = p.blocks.get_mut(first + i) else {
                    continue;
                };
                let w = writers.get_mut(i).and_then(Option::take);
                let same = b.buf.as_ref().is_some_and(|cur| Arc::ptr_eq(cur, buf));
                b.has_write_job = false;
                if same {
                    // the slot still holds the buffer that went out —
                    // plain put-back: re-park the writer if the slot
                    // is free, else the re-written block's newer write
                    // owns it and the returned one is superseded.
                    match b.writer {
                        None => {
                            // the run's job left the piece when the
                            // flush completed — a writer put back
                            // re-parks it; a bare buffer (failed
                            // flush) settles the count.
                            if w.is_none() {
                                p.num_jobs -= 1;
                            }
                            b.writer = w;
                        }
                        Some(_) => {
                            if let Some(mut w) = w {
                                w.flags &= !crate::disk_job::job_flags::CACHED;
                                crate::disk_completed::abort(&mut w);
                                self.orphans.push_back(w);
                            }
                            p.num_jobs -= 1;
                        }
                    }
                } else {
                    // the block was re-written while the run was
                    // out — its new buffer owns the slot; the
                    // returned buffer is dropped (the newer write
                    // keeps its own charge). The job's writer is
                    // superseded by the parked newer one.
                    if let Some(mut w) = w {
                        w.flags &= !crate::disk_job::job_flags::CACHED;
                        crate::disk_completed::abort(&mut w);
                        self.orphans.push_back(w);
                    }
                    p.num_jobs -= 1;
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
    pub fn take_all_storage_runs(&mut self) -> Vec<LocatedRun> {
        let locs: Vec<PieceLocation> = self.pieces.keys().copied().collect();
        let mut out = Vec::new();
        for loc in locs {
            for (first, bufs, writers) in self.take_all_runs(loc) {
                out.push((loc, first, bufs, writers));
            }
        }
        out
    }

    /// Mark a flushed region durable — retires each block's buffer
    /// (`flushed`), advances `flushed_cursor`, drops fully-flushed
    /// pieces. `bufs` is the span the flush actually wrote — the same
    /// `Arc`s as the cache slots. A block re-inserted while the write
    /// was in flight holds a *different* `Arc`: it keeps its newer
    /// buffer dirty (and its parked writer) instead of being clobbered
    /// by the retiring pass (upstream asserts the rewrite can't
    /// happen; we degrade gracefully).
    /// The writers the flush carried were already moved onto the
    /// `FlushJob` (`take_write_job`) and complete with it.
    pub fn run_flushed(&mut self, loc: PieceLocation, first: usize, bufs: &[Arc<Vec<u8>>]) {
        let mut free = false;
        let mut retired = 0i64;
        let mut inflight = 0i64;
        if let Some(p) = self.pieces.get_mut(&loc) {
            for (i, b) in p.blocks.iter_mut().skip(first).take(bufs.len()).enumerate() {
                if b.has_write_job {
                    p.num_jobs -= 1;
                    inflight += 1;
                }
                b.has_write_job = false;
                let same = b.buf.as_ref().is_some_and(|cur| Arc::ptr_eq(cur, &bufs[i]));
                if same {
                    b.buf = None;
                    b.flushed = true;
                    retired += 1;
                    // upstream `flush_piece_impl`: retiring an unhashed
                    // v1 block (`block_index >= hasher_cursor`) removes
                    // its `m_num_unhashed` contribution — the counter
                    // tracks *cached* unhashed blocks only.
                    if p.v1 && first + i >= usize::from(p.hasher_cursor) && p.unhashed > 0 {
                        p.unhashed -= 1;
                        self.num_unhashed -= 1;
                    }
                }
            }
            self.in_flight_blocks -= inflight;
            self.num_cached_blocks -= retired;
            p.flushed_cursor = compute_flushed_cursor(&p.blocks);
            // upstream evicts the cpe only once every block is flushed
            // *and* the piece hash has been delivered
            // (`piece_hash_returned_flag`) — the entry keeps the
            // `piece_hasher`'s final digest so `try_hash_piece` can
            // answer inline instead of re-reading the file. (v2
            // entries also carry the per-block `block_hash`s that
            // `hash2` consumes — same retention rule, ADR-0016.)
            free = p.blocks.iter().all(|b| b.flushed)
                && p.flags & piece_flags::PIECE_HASH_RETURNED != 0;
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
    pub fn take_expired_runs(&mut self, max_age: std::time::Duration) -> Vec<LocatedRun> {
        let locs: Vec<PieceLocation> = self
            .pieces
            .iter()
            .filter(|(_, p)| p.dirty_since.elapsed() >= max_age)
            .map(|(l, _)| *l)
            .collect();
        let mut out = Vec::new();
        for loc in locs {
            for (first, bufs, writers) in self.take_all_runs(loc) {
                out.push((loc, first, bufs, writers));
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
            // the kick was consumed — a later insert on the new
            // `hasher_cursor` may emit `need_hasher_kick` again
            // (upstream clears `needs_hasher_kick_flag` on advance).
            p.flags &= !piece_flags::NEEDS_HASHER_KICK;
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
        let r = c.insert(loc, 0, Some(vec![1u8; 4]), None, false, params());
        assert!(r.need_hasher_kick); // block 0 == hasher_cursor
        assert_eq!(c.num_cached_blocks, 1);
        assert_eq!(c.num_unhashed, 1);
        let r = c.insert(loc, 1, Some(vec![2u8; 4]), None, false, params());
        assert!(!r.need_hasher_kick);
        assert_eq!(c.piece(loc).unwrap().num_jobs, 2);
    }

    #[test]
    fn write_done_advances_cursor() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(1));
        c.insert(loc, 0, Some(vec![0; 4]), None, false, params());
        c.insert(loc, 1, Some(vec![0; 4]), None, false, params());
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

    /// a write job parked on a block — `write_state = disk_job*`.
    fn parked_write() -> Box<DiskJob> {
        Box::new(DiskJob {
            pool_counters: None,
            storage: StorageIndex(u32::MAX),
            flags: 0,
            ret: 0,
            error: crate::storage_error::StorageError::default(),
            action: crate::disk_job::JobKind::Write(crate::disk_job::WriteJob {
                handler: Box::new(|_| {}),
                buf: crate::disk_buffer_pool::DiskBufferHolder::empty(),
                piece: PieceIndex(0),
                offset: 0,
                buffer_size: 0,
                force_flush: false,
                volatile: false,
            }),
        })
    }

    #[test]
    fn clear_refuses_pending() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        // a parked writer blocks the clear (upstream `count_jobs` on
        // `write_state`); completing it through a flush frees the piece.
        c.insert(
            loc,
            0,
            Some(vec![0; 4]),
            Some(parked_write()),
            false,
            params(),
        );
        assert!(!c.try_clear_piece(loc));
        let (_first, bufs, writers) = c.take_flush_run(loc).unwrap();
        assert!(writers.iter().flatten().count() == 1);
        c.run_flushed(loc, 0, &bufs);
        assert!(c.try_clear_piece(loc));
    }

    /// a writer displaced by a duplicate insert lands in `orphans`,
    /// aborted — upstream aborts the overwritten `write_state` job.
    #[test]
    fn duplicate_insert_orphans_writer() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(
            loc,
            0,
            Some(vec![0; 4]),
            Some(parked_write()),
            false,
            params(),
        );
        c.insert(
            loc,
            0,
            Some(vec![1; 4]),
            Some(parked_write()),
            false,
            params(),
        );
        let orphans = c.take_orphans();
        assert_eq!(orphans.len(), 1);
        assert!(orphans[0].flags & crate::disk_job::job_flags::ABORTED != 0);
    }

    #[test]
    fn force_flush_on_hash_returned() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![0; 4]), None, false, params());
        c.pieces.get_mut(&loc).unwrap().flags |= piece_flags::PIECE_HASH_RETURNED;
        c.insert(loc, 1, Some(vec![0; 4]), None, false, params());
        assert!(c.piece(loc).unwrap().flags & piece_flags::FORCE_FLUSH != 0);
    }

    /// `m_num_unhashed` must return to zero once every queued v1
    /// block has been hashed — a stuck counter made `queue_stats`
    /// report 183k unhashed blocks on a completed 2 GiB swarm.
    #[test]
    fn unhashed_settles_on_hash_and_clear() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![0; 4]), None, false, params());
        c.insert(loc, 1, Some(vec![0; 4]), None, false, params());
        assert_eq!(c.num_unhashed, 2);
        // duplicate inserts replace in place — no double counting
        c.insert(loc, 1, Some(vec![9; 4]), None, false, params());
        assert_eq!(c.num_unhashed, 2);
        c.hash_returned(loc);
        assert_eq!(c.num_unhashed, 0);
        // a piece evicted un-hashed settles too
        let loc2 = (StorageIndex(0), PieceIndex(1));
        c.insert(loc2, 0, Some(vec![0; 4]), None, false, params());
        assert_eq!(c.num_unhashed, 1);
        c.free_piece(loc2);
        assert_eq!(c.num_unhashed, 0);
    }

    /// `flush_to_disk` pass order under the whole-piece hasher:
    /// ready pieces first, then the ascending `(storage, piece)`
    /// sweep — upstream's pass-4 `get<0>` order. A longer dirty run
    /// must NOT jump ahead of a lower piece index: upstream's
    /// pass-2 `cheap_to_flush` score is `hasher_cursor -
    /// flushed_cursor`, and `hasher_cursor` only advances when the
    /// whole-piece hash job runs — unhashed pieces always score
    /// `≤ 0` upstream and are left for the ascending pass.
    #[test]
    fn flush_pick_prefers_ready_then_ascending() {
        let mut c = DiskCache::default();
        let s = StorageIndex(0);
        let at = |p: i32| (s, PieceIndex(p));
        // piece 5 carries the *longest* partial run (2 blocks);
        // pieces 2 and 9 have one dirty block each.
        c.insert(at(5), 0, Some(vec![5; 4]), None, false, params());
        c.insert(at(5), 1, Some(vec![5; 4]), None, false, params());
        c.insert(at(9), 0, Some(vec![9; 4]), None, false, params());
        c.insert(at(2), 0, Some(vec![2; 4]), None, false, params());
        // piece 0 lands complete → ready queue (upstream pass 1:
        // its incremental hasher promotes complete pieces to
        // force-flush; our `ready` covers both).
        for b in 0..3 {
            c.insert(at(0), b, Some(vec![0; 4]), None, false, params());
        }

        let (loc, ..) = c.take_coalesced(16).unwrap();
        assert_eq!(loc, at(0), "complete piece flushes first");
        let (loc, ..) = c.take_coalesced(16).unwrap();
        assert_eq!(loc, at(2), "ascending piece order — not the largest run");
        let (loc, ..) = c.take_coalesced(16).unwrap();
        assert_eq!(loc, at(5));
        let (loc, ..) = c.take_coalesced(16).unwrap();
        assert_eq!(loc, at(9));
        assert!(c.take_coalesced(16).is_none());
    }

    /// A16 D2 — a duplicate write on a cached block replaces the
    /// buffer in place: `num_cached_blocks` counts *slots*, so it
    /// must not grow, and the superseded writer loses `CACHED` so
    /// `job_done` releases its back-pressure slot (it was charged at
    /// submit and no flush will ever retire it).
    #[test]
    fn duplicate_insert_keeps_block_count_and_supersedes() {
        use crate::disk_job::{JobKind, WriteJob, job_flags};
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        let writer = || {
            Box::new(DiskJob {
                pool_counters: None,
                storage: StorageIndex(0),
                flags: job_flags::CACHED,
                ret: 0,
                error: Default::default(),
                action: JobKind::Write(WriteJob {
                    handler: Box::new(|_| {}),
                    buf: crate::disk_buffer_pool::DiskBufferHolder::empty(),
                    piece: PieceIndex(0),
                    offset: 0,
                    buffer_size: 4,
                    force_flush: false,
                    volatile: false,
                }),
            })
        };
        c.insert(loc, 0, Some(vec![1; 4]), Some(writer()), false, params());
        assert_eq!(c.num_cached_blocks, 1);
        c.insert(loc, 0, Some(vec![2; 4]), Some(writer()), false, params());
        assert_eq!(
            c.num_cached_blocks, 1,
            "duplicate buffer replaces the slot, no +1"
        );
        let orphans = c.take_orphans();
        assert_eq!(orphans.len(), 1, "the older writer is superseded");
        assert_eq!(
            orphans[0].flags & job_flags::CACHED,
            0,
            "superseded writer releases its own back-pressure slot"
        );
        assert_ne!(
            orphans[0].flags & job_flags::ABORTED,
            0,
            "and completes aborted"
        );
        // and the block still holds the newer buffer
        assert_eq!(c.get(loc, 0), Some(&[2u8; 4][..]));
    }

    /// A16 D1 — buffers taken into a flush run count as in-flight so
    /// the watermark loop can predict the post-completion level;
    /// `run_flushed` and `restore_run` settle the count.
    #[test]
    fn in_flight_blocks_tracks_taken_runs() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        for b in 0..3 {
            c.insert(loc, b, Some(vec![0; 4]), None, false, params());
        }
        assert_eq!(c.num_cached_blocks, 3);
        assert_eq!(c.in_flight_blocks, 0);
        let (first, bufs, writers) = c.take_flush_run(loc).unwrap();
        assert_eq!(bufs.len(), 3);
        assert_eq!(c.in_flight_blocks, 3);
        // put back unflushed → back to cached, not in flight
        c.restore_run(loc, usize::try_from(first).unwrap_or(0), bufs, writers);
        assert_eq!(c.in_flight_blocks, 0);
        assert_eq!(c.num_cached_blocks, 3);
        let (first, bufs, _w) = c.take_flush_run(loc).unwrap();
        c.run_flushed(loc, usize::try_from(first).unwrap_or(0), &bufs);
        assert_eq!(c.in_flight_blocks, 0);
        assert_eq!(c.num_cached_blocks, 0);
    }

    /// A16 D6 — `need_hasher_kick` mirrors upstream's
    /// `needs_hasher_kick_flag`: emitted once per `hasher_cursor`
    /// position, re-armed when the hasher consumes the block.
    #[test]
    fn hasher_kick_emitted_once_per_cursor() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        assert!(
            c.insert(loc, 0, Some(vec![1; 4]), None, false, params())
                .need_hasher_kick
        );
        // duplicate insert on the same block — cursor still 0, no re-kick
        assert!(
            !c.insert(loc, 0, Some(vec![1; 4]), None, false, params())
                .need_hasher_kick
        );
        c.hashed_block(loc, 0);
        assert!(
            c.insert(loc, 1, Some(vec![1; 4]), None, false, params())
                .need_hasher_kick
        );
    }

    /// A16 — teardown: `abort_all_writers` reports the cached buffers
    /// that die with the cache so the caller can release their
    /// `write_blocks` slots; orphaned writers keep `CACHED` (their
    /// slot is covered by that count, not by `job_done`).
    #[test]
    fn abort_all_writers_reports_buffers() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![1; 4]), None, false, params());
        c.insert(loc, 1, Some(vec![1; 4]), None, false, params());
        assert_eq!(c.abort_all_writers(), 2);
        // a block out in an in-flight flush is NOT counted — its slot
        // is released when the Flush job completes. Piece 0's two
        // cached buffers still count; piece 1's in-flight one doesn't.
        let loc2 = (StorageIndex(0), PieceIndex(1));
        c.insert(loc2, 0, Some(vec![1; 4]), None, false, params());
        let _run = c.take_flush_run(loc2);
        assert_eq!(c.abort_all_writers(), 2);
    }

    /// A16 E1 — `kick_hasher`: v1 inserts feed the incremental
    /// `piece_hasher` as contiguous blocks land; completing the
    /// buffered prefix finalizes the digest and raises
    /// `FORCE_FLUSH` (upstream `ph->final_hash()` +
    /// `force_flush_flag`) so a later `hash` job answers without
    /// re-reading the file.
    #[test]
    fn incremental_hasher_stores_piece_hash() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        let bs = usize::try_from(DEFAULT_BLOCK_SIZE).unwrap_or(0);
        c.insert(loc, 0, Some(vec![0xAA; bs]), None, false, params());
        assert_eq!(c.piece(loc).unwrap().hasher_cursor, 1);
        // out-of-order tail: the cursor waits on block 1
        c.insert(loc, 2, Some(vec![0xCC; bs]), None, false, params());
        assert_eq!(c.piece(loc).unwrap().hasher_cursor, 1);
        c.insert(loc, 1, Some(vec![0xBB; bs]), None, false, params());
        let p = c.piece(loc).unwrap();
        assert_eq!(p.hasher_cursor, 3, "the feed drains the contiguous tail");
        let expect = {
            let mut h = Sha1::new();
            for fill in [0xAAu8, 0xBB, 0xCC] {
                h.update(vec![fill; bs]);
            }
            bitdaemon_common::Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default()
        };
        assert_eq!(p.piece_hash, Some(expect));
        assert!(p.flags & piece_flags::FORCE_FLUSH != 0);
    }

    /// A16 E1 — a rewrite below the hasher's consumed prefix
    /// invalidates the stored digest (upstream asserts the case
    /// away; we re-hash from the new bytes instead of serving a
    /// stale `piece_hash`).
    #[test]
    fn low_rewrite_invalidates_piece_hash() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        let bs = usize::try_from(DEFAULT_BLOCK_SIZE).unwrap_or(0);
        for (i, fill) in [0xAAu8, 0xBB, 0xCC].iter().enumerate() {
            c.insert(loc, i, Some(vec![*fill; bs]), None, false, params());
        }
        let first_hash = c.piece(loc).unwrap().piece_hash;
        assert!(first_hash.is_some());
        // rewrite block 0 — the digest must track the new bytes
        c.insert(loc, 0, Some(vec![0xDD; bs]), None, false, params());
        let p = c.piece(loc).unwrap();
        let expect = {
            let mut h = Sha1::new();
            for fill in [0xDDu8, 0xBB, 0xCC] {
                h.update(vec![fill; bs]);
            }
            bitdaemon_common::Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default()
        };
        assert_eq!(p.piece_hash, Some(expect));
        assert_ne!(p.piece_hash, first_hash);
    }

    /// A16 E2 — `borrowed_buf`: a buffer out in an in-flight flush
    /// keeps serving reads until `run_flushed` retires it (upstream
    /// keeps the block readable; taking the buffer out opened a
    /// stale-read window).
    #[test]
    fn in_flight_buffer_stays_readable() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![7; 4]), None, false, params());
        let (_first, bufs, _w) = c.take_flush_run(loc).unwrap();
        assert_eq!(c.in_flight_blocks, 1);
        assert_eq!(
            c.get(loc, 0),
            Some(&[7u8; 4][..]),
            "in-flight buffer keeps serving its bytes"
        );
        // and the run must not extend onto it a second time
        assert!(c.take_flush_run(loc).is_none());
        c.run_flushed(loc, 0, &bufs);
        assert!(c.get(loc, 0).is_none(), "retired once the write lands");
    }

    /// A16 E3 — a re-insert during the flush's flight installs a
    /// *newer* buffer on the slot: `run_flushed` must retire only
    /// the buffer the job actually wrote (`Arc` identity), leaving
    /// the newer one dirty for the next pass instead of clobbering it.
    #[test]
    fn reinsert_during_flush_keeps_newer_buffer() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![1; 4]), None, false, params());
        let (_first, bufs, _w) = c.take_flush_run(loc).unwrap();
        // the write races the in-flight flush — new buffer, new writer
        c.insert(loc, 0, Some(vec![2; 4]), None, false, params());
        c.run_flushed(loc, 0, &bufs);
        assert_eq!(
            c.get(loc, 0),
            Some(&[2u8; 4][..]),
            "the newer buffer survives the retiring pass"
        );
        assert!(
            !c.piece(loc).unwrap().blocks[0].flushed,
            "and stays dirty — the written-out bytes were the old ones"
        );
        // the next flush pass takes the newer buffer
        let (_f2, bufs2, _w2) = c.take_flush_run(loc).unwrap();
        assert_eq!(bufs2.len(), 1);
        c.run_flushed(loc, 0, &bufs2);
        assert!(c.piece(loc).unwrap().blocks[0].flushed);
    }

    /// A16 E3 — a buffer landing on an already-`flushed` block is a
    /// rewrite: the flag must clear or the block (and its parked
    /// writer) would sit in the cache forever, never flushable.
    #[test]
    fn reinsert_flushed_block_redirties() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        c.insert(loc, 0, Some(vec![1; 4]), None, false, params());
        let (_f, bufs, _w) = c.take_flush_run(loc).unwrap();
        c.run_flushed(loc, 0, &bufs);
        assert!(c.piece(loc).unwrap().blocks[0].flushed);
        c.insert(
            loc,
            0,
            Some(vec![9; 4]),
            Some(parked_write()),
            false,
            params(),
        );
        let b = &c.piece(loc).unwrap().blocks[0];
        assert!(!b.flushed, "rewrite re-dirties the block");
        let (_f2, bufs2, writers2) = c.take_flush_run(loc).unwrap();
        assert_eq!(bufs2.len(), 1);
        assert_eq!(writers2.iter().flatten().count(), 1, "writer rides along");
    }

    /// A16 E4 — a write whose block index is out of the piece never
    /// covered a cache slot: its orphan must complete aborted with
    /// `CACHED` cleared, or `job_done` would double-release the
    /// `write_blocks` charge.
    #[test]
    fn oob_write_orphan_aborted_uncached() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        let mut w = parked_write();
        w.flags |= crate::disk_job::job_flags::CACHED;
        c.insert(loc, 99, Some(vec![1; 4]), Some(w), false, params());
        let orphans = c.take_orphans();
        assert_eq!(orphans.len(), 1);
        assert_ne!(orphans[0].flags & crate::disk_job::job_flags::ABORTED, 0);
        assert_eq!(
            orphans[0].flags & crate::disk_job::job_flags::CACHED,
            0,
            "no cache slot was taken — no charge to release"
        );
    }

    /// A16 E5 — `m_num_unhashed` tracks *cached* unhashed v1 blocks:
    /// retiring a block the hasher never consumed must settle its
    /// contribution, not leave it for `hash_returned`.
    #[test]
    fn retiring_unhashed_block_settles_counter() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        // block 1 lands without block 0 — the feed stalls at cursor
        // 0, so block 1 stays unhashed-but-cached.
        let bs = usize::try_from(DEFAULT_BLOCK_SIZE).unwrap_or(0);
        c.insert(loc, 1, Some(vec![5; bs]), None, false, params());
        assert_eq!(c.num_unhashed, 1);
        let (_f, bufs, _w) = c.take_flush_run(loc).unwrap();
        c.run_flushed(loc, 1, &bufs);
        assert_eq!(
            c.num_unhashed, 0,
            "the retired block's contribution is settled"
        );
    }

    /// A16 — upstream keeps the cpe until `piece_hash_returned_flag`
    /// even when every block is flushed: the stored digest must
    /// survive for `try_hash_piece`'s inline answer.
    #[test]
    fn flushed_entry_lingers_until_hash_returned() {
        let mut c = DiskCache::default();
        let loc = (StorageIndex(0), PieceIndex(0));
        let bs = usize::try_from(DEFAULT_BLOCK_SIZE).unwrap_or(0);
        for i in 0..3 {
            c.insert(loc, i, Some(vec![0x33; bs]), None, false, params());
        }
        let (_f, bufs, _w) = c.take_flush_run(loc).unwrap();
        c.run_flushed(loc, 0, &bufs);
        assert!(
            c.piece(loc).is_some(),
            "flushed but unhashed — the cpe keeps the digest"
        );
        assert!(c.piece(loc).unwrap().piece_hash.is_some());
        c.hash_returned(loc);
        assert!(c.piece(loc).is_none(), "hash delivered → the entry retires");
    }
}
