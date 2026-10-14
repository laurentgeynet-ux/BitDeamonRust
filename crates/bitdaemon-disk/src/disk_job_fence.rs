//! Port of `aux_/disk_job_fence.hpp`/`disk_job_fence.cpp`.
//!
//! Per-torrent ordering: whenever a job needs exclusive access to a
//! torrent's storage (move_storage, delete_files, check_fastresume,
//! file_priority, …) it raises the fence; new jobs queue behind it
//! until all outstanding jobs drain, then the fence job runs alone and
//! the backlog is reposted — preserving submission order.
//!
//! C++ uses an intrusive `tailqueue<disk_job>`; here blocked jobs are a
//! `VecDeque<Box<DiskJob>>` owned by the fence. `job_complete` calls a
//! `repost` closure once per unblocked job — the caller (disk_io) moves
//! them to its own queue under the fence mutex, exactly like upstream's
//! in-lock repost.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use crate::disk_job::{DiskJob, job_flags};

/// `raise_fence` return values.
pub const FENCE_POST_FENCE: i32 = 0;
/// `fence_post_none` — the fence job was queued, don't post it.
pub const FENCE_POST_NONE: i32 = 1;

/// `aux::disk_job_fence`.
#[derive(Default)]
pub struct DiskJobFence {
    /// `m_has_fence` — number of queued fence jobs (>0 ⇒ storage blocked).
    has_fence: Mutex<HasFence>,
    /// `m_outstanding_jobs` — jobs belonging to this torrent in flight.
    outstanding: AtomicI64,
}

#[derive(Default)]
struct HasFence {
    fence: i32,
    /// `m_blocked_jobs`.
    blocked: VecDeque<Box<DiskJob>>,
}

/// `counters::blocked_disk_jobs` + the `num_fenced_*` per-kind
/// gauges — live counters so the TOCTOU-safe increments under the
/// fence mutex are preserved.
#[derive(Default)]
pub struct FenceCounters {
    /// `blocked_disk_jobs`.
    pub blocked_disk_jobs: AtomicUsize,
    /// `num_fenced_read + get_type()` — fence jobs currently in the
    /// system per `job_action_t` (incremented at `add_fence_job`,
    /// decremented when a `fence`-flagged job completes — upstream
    /// `add_completed_jobs_impl`).
    pub fenced_jobs: [AtomicI64; crate::disk_job::JobAction::COUNT],
}

impl FenceCounters {
    fn inc_blocked(&self) {
        self.blocked_disk_jobs.fetch_add(1, Ordering::Relaxed);
    }
    /// `dec_stats_counter(blocked_disk_jobs, -1)` — jobs reposted out
    /// of the blocked queue (wired by disk_io on repost).
    pub fn dec_blocked(&self) {
        self.blocked_disk_jobs.fetch_sub(1, Ordering::Relaxed);
    }

    /// `inc_stats_counter(num_fenced_read + get_type())` — called at
    /// `add_fence_job` submit time, before `raise_fence`.
    pub fn inc_fenced(&self, kind: crate::disk_job::JobAction) {
        self.fenced_jobs[kind.index()].fetch_add(1, Ordering::Relaxed);
    }
    /// `inc_stats_counter(num_fenced_read + get_type(), -1)` — called
    /// when a `fence`-flagged job lands on the completed queue.
    pub fn dec_fenced(&self, kind: crate::disk_job::JobAction) {
        self.fenced_jobs[kind.index()].fetch_sub(1, Ordering::Relaxed);
    }
}

impl DiskJobFence {
    /// `disk_job_fence()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `num_outstanding_jobs()`.
    #[must_use]
    pub fn num_outstanding_jobs(&self) -> i64 {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// `has_fence()`.
    #[must_use]
    pub fn has_fence(&self) -> bool {
        self.lock().fence != 0
    }

    /// `num_blocked()`.
    #[must_use]
    pub fn num_blocked(&self) -> usize {
        self.lock().blocked.len()
    }

    /// `take_blocked` — drain `m_blocked_jobs` for teardown (abort /
    /// storage removal): parked jobs' handlers still need to run, so
    /// the caller completes each of them aborted.
    #[must_use]
    pub fn take_blocked(&self) -> VecDeque<Box<DiskJob>> {
        let mut g = self.lock();
        std::mem::take(&mut g.blocked)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HasFence> {
        self.has_fence.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `raise_fence(j, cnt)` — `j` must have exclusive access.
    /// Returns `(FENCE_POST_FENCE, Some(job))` if the caller must post
    /// the job itself, `(FENCE_POST_NONE, None)` if the fence queued it.
    pub fn raise_fence(
        &self,
        mut j: Box<DiskJob>,
        cnt: &FenceCounters,
    ) -> (i32, Option<Box<DiskJob>>) {
        debug_assert!(j.flags & job_flags::IN_PROGRESS == 0);
        debug_assert!(j.flags & job_flags::FENCE == 0);
        j.flags |= job_flags::FENCE;

        let mut g = self.lock();
        if g.fence == 0 && self.outstanding.load(Ordering::Relaxed) == 0 {
            g.fence += 1;
            // the job is posted straight to the job queue — accounted
            // here without passing is_blocked()
            j.flags |= job_flags::IN_PROGRESS;
            self.outstanding.fetch_add(1, Ordering::Relaxed);
            return (FENCE_POST_FENCE, Some(j));
        }

        g.fence += 1;
        g.blocked.push_back(j);
        cnt.inc_blocked();
        (FENCE_POST_NONE, None)
    }

    /// `is_blocked(j, cnt)` — returns `Some(job)` if no fence is up
    /// (post it), `None` if the job was queued behind a raised fence.
    pub fn is_blocked(&self, mut j: Box<DiskJob>, cnt: &FenceCounters) -> Option<Box<DiskJob>> {
        let mut g = self.lock();
        if g.fence == 0 {
            debug_assert!(j.flags & job_flags::IN_PROGRESS == 0);
            j.flags |= job_flags::IN_PROGRESS;
            self.outstanding.fetch_add(1, Ordering::Relaxed);
            return Some(j);
        }
        g.blocked.push_back(j);
        // incremented under the mutex — closes the TOCTOU race vs.
        // job_complete() popping the job (upstream comment verbatim).
        cnt.inc_blocked();
        None
    }

    /// `job_complete(j, repost)` — the fence-mutex variant. Returns the
    /// number of jobs handed to `repost`. The callback fires *while
    /// still holding `has_fence`* (upstream's in-lock repost) — only
    /// pure cache consults are safe inside it; the second argument is
    /// `g.fence != 0` at repost time, since querying `has_fence` from
    /// inside the callback would re-enter the lock.
    pub fn job_complete<F>(&self, j: &mut DiskJob, repost: &mut F) -> i32
    where
        F: FnMut(Box<DiskJob>, bool),
    {
        let mut g = self.lock();

        debug_assert!(j.flags & job_flags::IN_PROGRESS != 0);
        j.flags &= !job_flags::IN_PROGRESS;

        debug_assert!(self.outstanding.load(Ordering::Relaxed) > 0);
        self.outstanding.fetch_sub(1, Ordering::Relaxed);

        if j.flags & job_flags::FENCE != 0 {
            debug_assert_eq!(self.outstanding.load(Ordering::Relaxed), 0);
            g.fence -= 1;

            let mut ret = 0;
            while let Some(mut bj) = g.blocked.pop_front() {
                if bj.flags & job_flags::FENCE != 0 {
                    if self.outstanding.load(Ordering::Relaxed) == 0 && ret == 0 {
                        bj.flags |= job_flags::IN_PROGRESS;
                        self.outstanding.fetch_add(1, Ordering::Relaxed);
                        ret += 1;
                        repost(bj, g.fence != 0);
                    } else {
                        g.blocked.push_front(bj);
                    }
                    return ret;
                }
                bj.flags |= job_flags::IN_PROGRESS;
                self.outstanding.fetch_add(1, Ordering::Relaxed);
                ret += 1;
                repost(bj, g.fence != 0);
            }
            return ret;
        }

        if self.outstanding.load(Ordering::Relaxed) > 0 || g.fence == 0 {
            return 0;
        }

        // fence up, nothing outstanding → run the fence job now
        let Some(mut bj) = g.blocked.pop_front() else {
            return 0;
        };
        debug_assert!(bj.flags & job_flags::FENCE != 0);
        bj.flags |= job_flags::IN_PROGRESS;
        self.outstanding.fetch_add(1, Ordering::Relaxed);
        repost(bj, true);
        1
    }

    /// Mark `j` `in_progress` and count it outstanding *without* the
    /// blocked-queue check — for jobs that must run while the fence is
    /// up: upstream's `m_fence_flush` drains a storage whose stacked
    /// fence waits on writes the repost just inserted, and those
    /// flushes run on a generic thread past `is_blocked` like this.
    /// Callable under the fence mutex (atomics only, no lock).
    pub fn account_outstanding(&self, j: &mut DiskJob) {
        debug_assert!(j.flags & job_flags::IN_PROGRESS == 0);
        j.flags |= job_flags::IN_PROGRESS;
        self.outstanding.fetch_add(1, Ordering::Relaxed);
    }
}
