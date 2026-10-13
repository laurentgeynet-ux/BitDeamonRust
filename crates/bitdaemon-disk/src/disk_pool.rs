//! Port of `aux_/disk_io_thread_pool.hpp`/`disk_io_thread_pool.cpp` +
//! `aux_/disk_job_pool.hpp`.
//!
//! Blocking worker pool: threads are spawned on demand when jobs arrive
//! (`job_queued`), block on a condvar when the queue drains, and shrink
//! via `should_exit`/`try_thread_exit`. Upstream reaps idle threads on a
//! 60 s `deadline_timer`; here threads exit when asked or on abort, and
//! `reap_idle_threads` is driven by the owner (session tick) — the
//! mechanism is identical, the clock is injected.
//!
//! `disk_job_pool` upstream is a slab allocator; in Rust jobs are boxed
//! and only the counters survive (`jobs_in_use`, read/write split).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::disk_job::DiskJob;

/// `m_job_cond.wait_for(l, 1s)` — the idle loop's poll cadence; each
/// wake re-runs the exit check and re-samples `min_idle`.
const IDLE_POLL: Duration = Duration::from_secs(1);

/// `jobqueue_t` — upstream intrusive tailqueue; a deque of owned jobs.
pub type JobQueue = VecDeque<Box<DiskJob>>;

/// `wait_result`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitResult {
    /// `new_job`.
    NewJob,
    /// `exit_thread`.
    ExitThread,
    /// `interrupt` — wait returned early (owner should re-check state).
    Interrupt,
}

/// `disk_job_pool` counters.
#[derive(Default)]
pub struct JobPoolCounters {
    /// `m_jobs_in_use`.
    pub jobs_in_use: AtomicI64,
    /// `m_read_jobs`.
    pub read_jobs: AtomicI64,
    /// `m_write_jobs`.
    pub write_jobs: AtomicI64,
}

struct PoolInner {
    queued: JobQueue,
    abort: bool,
    threads_to_exit: i64,
    /// `disk_job_pool` per-kind job counts — maintained on push/pop so
    /// `queue_stats` never walks the queue under the worker lock.
    queued_writes: usize,
    queued_hashes: usize,
    queued_other: usize,
}

/// per-kind bucket used by `queue_stats` (upstream `m_jobs[]` split:
/// writes get their own counter, hash jobs theirs, the rest lumped).
fn kind_slot(j: &crate::disk_job::DiskJob) -> usize {
    use crate::disk_job::JobKind;
    match j.action {
        JobKind::Write(_) => 0,
        JobKind::Hash(_) | JobKind::Hash2(_) => 1,
        _ => 2,
    }
}

/// `aux::disk_io_thread_pool`. Generic over the worker function —
/// `disk_thread_fun` upstream.
pub struct DiskIoThreadPool {
    inner: Mutex<PoolInner>,
    job_cond: Condvar,
    threads: Mutex<Vec<JoinHandle<()>>>,
    max_threads: AtomicI64,
    min_idle: AtomicI64,
    num_idle: AtomicI64,
    interrupt: AtomicBool,
    /// upstream `m_thread_fun` — spawned threads run this loop.
    worker: Arc<dyn Fn(Arc<DiskIoThreadPool>) + Send + Sync>,
}

impl DiskIoThreadPool {
    /// `disk_io_thread_pool(thread_fun, ios)` — `ios` (work guard) is a
    /// session concern; threads just run `worker` until `ExitThread`.
    #[must_use]
    pub fn new(worker: Arc<dyn Fn(Arc<DiskIoThreadPool>) + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(PoolInner {
                queued: VecDeque::new(),
                abort: false,
                threads_to_exit: 0,
                queued_writes: 0,
                queued_hashes: 0,
                queued_other: 0,
            }),
            job_cond: Condvar::new(),
            threads: Mutex::new(Vec::new()),
            max_threads: AtomicI64::new(0),
            min_idle: AtomicI64::new(0),
            num_idle: AtomicI64::new(0),
            interrupt: AtomicBool::new(false),
            worker,
        })
    }

    /// `set_max_threads(i)` — grows/shrinks the pool; eagerly spawns one
    /// thread so `interrupt()` always has someone to wake.
    pub fn set_max_threads(self: &Arc<Self>, i: i64) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if i == self.max_threads.load(Ordering::Relaxed) {
            return;
        }
        self.max_threads.store(i, Ordering::Relaxed);
        let threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
        let cur = i64::try_from(threads.len()).unwrap_or(0);
        if cur > i {
            // upstream `stop_threads` assigns — pending exits are
            // re-set, not accumulated.
            g.threads_to_exit = cur - i;
            self.job_cond.notify_all();
            return;
        }
        if i > 0 && threads.is_empty() && !g.abort {
            drop(threads);
            drop(g);
            self.spawn_thread();
        }
    }

    fn spawn_thread(self: &Arc<Self>) {
        let pool = Arc::clone(self);
        let worker = Arc::clone(&self.worker);
        // a failed spawn leaves the queue short a worker rather than
        // killing the session — the next `job_queued` retries.
        let Ok(handle) = thread::Builder::new()
            .name("bitdaemon-disk".to_string())
            .spawn(move || worker(pool))
        else {
            tracing::error!("failed to spawn disk worker thread");
            return;
        };
        self.threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
    }

    /// `job_queued(queue_size)` — spawn workers until idle+new cover
    /// the queue (`for i = num_idle; i < queue_size && threads < max`),
    /// capped at `max_threads`. Upstream counts *idle* threads as
    /// coverage — busy ones can't take the new jobs — and shrinks any
    /// pending exit request so arriving jobs keep their workers.
    pub fn job_queued(self: &Arc<Self>, queue_size: usize) {
        let qs = i64::try_from(queue_size).unwrap_or(i64::MAX);
        // cheap check first — enough idle workers already.
        if self.num_idle.load(Ordering::Relaxed) >= qs {
            return;
        }
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.abort {
            return;
        }
        let num_idle = self.num_idle.load(Ordering::Relaxed);
        // reduce the number of threads requested to stop — the new
        // jobs are going to need them.
        let keep = std::cmp::max(0, num_idle - qs);
        if g.threads_to_exit > keep {
            g.threads_to_exit = keep;
        }
        let max = self.max_threads.load(Ordering::Relaxed);
        let mut threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
        let mut covered = num_idle;
        while covered < qs && i64::try_from(threads.len()).unwrap_or(i64::MAX) < max {
            let pool = Arc::clone(self);
            let worker = Arc::clone(&self.worker);
            match thread::Builder::new()
                .name("bitdaemon-disk".to_string())
                .spawn(move || worker(pool))
            {
                Ok(h) => {
                    threads.push(h);
                    covered += 1;
                }
                Err(_) => break,
            }
        }
    }

    /// `append(jobs)` + `push_back` + `pop_front` — the queue is under
    /// the pool mutex upstream ("must be held"); Rust folds it in.
    /// The per-kind counters ride along so stats never rescan.
    pub fn push_back(&self, j: Box<DiskJob>) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match kind_slot(&j) {
            0 => g.queued_writes += 1,
            1 => g.queued_hashes += 1,
            _ => g.queued_other += 1,
        }
        g.queued.push_back(j);
    }

    /// `pop_front`.
    #[must_use]
    pub fn pop_front(&self) -> Option<Box<DiskJob>> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let j = g.queued.pop_front();
        if let Some(j) = &j {
            match kind_slot(j) {
                0 => g.queued_writes = g.queued_writes.saturating_sub(1),
                1 => g.queued_hashes = g.queued_hashes.saturating_sub(1),
                _ => g.queued_other = g.queued_other.saturating_sub(1),
            }
        }
        j
    }

    /// `queue_size`.
    #[must_use]
    pub fn queue_size(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queued
            .len()
    }

    /// `submit_jobs` — wake workers and spawn as needed. The wake is
    /// conditional: at high enqueue rates every push used to
    /// `notify_all` even when all workers were already running —
    /// thousands of spurious wakeups per second (observed as
    /// machine-wide stutter on a 25 k-block burst). Workers re-check
    /// `queued` under the lock before sleeping, so a running pool
    /// needs no signal at all.
    pub fn submit_jobs(self: &Arc<Self>) {
        let n = self.queue_size();
        if n == 0 {
            return;
        }
        if self.num_idle.load(Ordering::Relaxed) > 0 {
            self.job_cond.notify_all();
        }
        self.job_queued(n);
    }

    /// `interrupt()` — wake one thread early.
    pub fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Relaxed);
        self.job_cond.notify_one();
    }

    /// `notify_all`.
    pub fn notify_all(&self) {
        self.job_cond.notify_all();
    }

    /// `wait_for_job(l)` — worker-side wait, verbatim. The exit check
    /// only runs while (or before) going idle: a thread asked to exit
    /// with a non-empty queue drains it first unless other threads
    /// remain (`queued empty || num_threads > 1`). Each 1 s wait round
    /// emits one `thread_idle`/`thread_active` pair so
    /// `reap_idle_threads` samples the interval's real idle minimum.
    pub fn wait_for_job(&self) -> WaitResult {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.queued.is_empty() {
            if self.interrupt.swap(false, Ordering::Relaxed) {
                return WaitResult::Interrupt;
            }
            loop {
                if g.threads_to_exit > 0
                    && (g.queued.is_empty() || self.num_threads() > 1)
                    && self.try_thread_exit(&mut g)
                {
                    return WaitResult::ExitThread;
                }
                self.thread_idle();
                let (gg, _) = self
                    .job_cond
                    .wait_timeout(g, IDLE_POLL)
                    .unwrap_or_else(|e| e.into_inner());
                g = gg;
                self.thread_active();
                if self.interrupt.swap(false, Ordering::Relaxed) {
                    return WaitResult::Interrupt;
                }
                if !g.queued.is_empty() {
                    break;
                }
            }
        }
        WaitResult::NewJob
    }

    /// `should_exit()` — weak check.
    #[must_use]
    pub fn should_exit(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .threads_to_exit
            > 0
    }

    /// `try_thread_exit(id)` — consume one exit token. The exiting
    /// thread removes its own handle from `threads` (upstream detaches
    /// it) so `num_threads`/`job_queued`/`set_max_threads` never count
    /// dead workers; under `abort` handles stay for `abort()` to join.
    /// `g` must be the already-held pool lock — `threads` is taken
    /// under it everywhere, no new ordering.
    fn try_thread_exit(&self, g: &mut PoolInner) -> bool {
        if g.threads_to_exit > 0 {
            g.threads_to_exit -= 1;
            if !g.abort {
                let id = thread::current().id();
                let mut threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(pos) = threads.iter().position(|t| t.thread().id() == id) {
                    threads.remove(pos);
                }
            }
            return true;
        }
        g.abort
    }

    /// `thread_idle` / `thread_active` — idle bookkeeping (reaper input).
    pub fn thread_idle(&self) {
        self.num_idle.fetch_add(1, Ordering::Relaxed);
    }

    /// `thread_active`.
    pub fn thread_active(&self) {
        let n = self.num_idle.fetch_sub(1, Ordering::Relaxed) - 1;
        let mut cur = self.min_idle.load(Ordering::Relaxed);
        while n < cur
            && self
                .min_idle
                .compare_exchange_weak(cur, n, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            cur = self.min_idle.load(Ordering::Relaxed);
        }
    }

    /// `reap_idle_threads` — upstream's 60 s timer callback, verbatim:
    /// stop `min(max(min_idle, threads − max), threads − 1)` — the
    /// larger of the interval's idle minimum or the over-cap excess,
    /// always leaving one thread for interrupts. `min_idle` exchanges
    /// with the *current* idle count (not MAX): the next interval's
    /// baseline is now, so a first idle-less interval simply seeds it.
    pub fn reap_idle_threads(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.abort {
            return;
        }
        let nthreads = i64::try_from(self.threads.lock().unwrap_or_else(|e| e.into_inner()).len())
            .unwrap_or(i64::MAX);
        if nthreads == 0 {
            return;
        }
        let min_idle = self
            .min_idle
            .swap(self.num_idle.load(Ordering::Relaxed), Ordering::Relaxed);
        if min_idle <= 0 {
            return;
        }
        let to_stop = std::cmp::min(
            std::cmp::max(
                min_idle,
                nthreads - self.max_threads.load(Ordering::Relaxed),
            ),
            nthreads - 1,
        );
        if to_stop <= 0 {
            return;
        }
        g.threads_to_exit = to_stop;
        self.job_cond.notify_all();
    }

    /// `abort(wait)` — stop everything, optionally join.
    pub fn abort(&self, wait: bool) {
        {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if g.abort {
                return;
            }
            g.abort = true;
            g.threads_to_exit =
                i64::try_from(self.threads.lock().unwrap_or_else(|e| e.into_inner()).len())
                    .unwrap_or(0);
        }
        self.job_cond.notify_all();
        if wait {
            let mut threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
            for t in threads.drain(..) {
                let _ = t.join();
            }
        }
    }

    /// `max_threads()`.
    #[must_use]
    pub fn max_threads(&self) -> i64 {
        self.max_threads.load(Ordering::Relaxed)
    }

    /// `num_threads`.
    #[must_use]
    pub fn num_threads(&self) -> usize {
        self.threads.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// `(writes, hashes, other)` queued-job counts — O(1) counterpart
    /// of walking `queued` for `queue_stats`.
    #[must_use]
    pub fn kind_counts(&self) -> (usize, usize, usize) {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        (g.queued_writes, g.queued_hashes, g.queued_other)
    }

    /// `visit_jobs` — inspect queued jobs (abort scan).
    pub fn visit_jobs<F: FnMut(&DiskJob)>(&self, mut f: F) {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for j in &g.queued {
            f(j);
        }
    }

    /// `visit_jobs` mutating — `abort_hash_jobs` marks queued volatile
    /// hash jobs `aborted` (they complete with the aborted error when
    /// a worker dequeues them, like upstream's in-place flag set).
    pub fn mark_aborted(&self, mut pred: impl FnMut(&DiskJob) -> bool) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for j in &mut g.queued {
            if j.flags & crate::disk_job::job_flags::ABORTED == 0 && pred(j) {
                j.flags |= crate::disk_job::job_flags::ABORTED;
            }
        }
    }

    /// Drain all queued jobs (abort path).
    pub fn drain_jobs(&self) -> JobQueue {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.queued_writes = 0;
        g.queued_hashes = 0;
        g.queued_other = 0;
        std::mem::take(&mut g.queued)
    }
}
