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

use crate::disk_job::DiskJob;

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
            g.threads_to_exit += cur - i;
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
        let handle = thread::Builder::new()
            .name("bitdaemon-disk".to_string())
            .spawn(move || worker(pool))
            .expect("spawn disk thread");
        self.threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
    }

    /// `job_queued(queue_size)` — spawn workers while jobs outnumber
    /// threads (upstream policy: `queue_size > num_threads`, capped).
    pub fn job_queued(self: &Arc<Self>, queue_size: usize) {
        if queue_size == 0 || self.inner.lock().unwrap_or_else(|e| e.into_inner()).abort {
            return;
        }
        let max = self.max_threads.load(Ordering::Relaxed);
        if max <= 0 {
            return;
        }
        let mut threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
        let num_threads = i64::try_from(threads.len()).unwrap_or(0);
        let want = std::cmp::min(i64::try_from(queue_size).unwrap_or(0), max);
        for _ in num_threads..want {
            let pool = Arc::clone(self);
            let worker = Arc::clone(&self.worker);
            match thread::Builder::new()
                .name("bitdaemon-disk".to_string())
                .spawn(move || worker(pool))
            {
                Ok(h) => threads.push(h),
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

    /// `wait_for_job(l)` — worker-side wait. Caller pops the job itself
    /// on `NewJob`.
    pub fn wait_for_job(&self) -> WaitResult {
        if self.should_exit() {
            return WaitResult::ExitThread;
        }
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !g.queued.is_empty() {
            return WaitResult::NewJob;
        }
        self.num_idle.fetch_add(1, Ordering::Relaxed);
        loop {
            // upstream: untimed wait — a spurious wake just re-checks
            g = self.job_cond.wait(g).unwrap_or_else(|e| e.into_inner());
            if self.interrupt.swap(false, Ordering::Relaxed) {
                self.num_idle.fetch_sub(1, Ordering::Relaxed);
                return WaitResult::Interrupt;
            }
            if g.threads_to_exit > 0 || g.abort {
                self.num_idle.fetch_sub(1, Ordering::Relaxed);
                return WaitResult::ExitThread;
            }
            if !g.queued.is_empty() {
                self.num_idle.fetch_sub(1, Ordering::Relaxed);
                return WaitResult::NewJob;
            }
        }
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

    /// `try_thread_exit()` — last call before a worker returns; true ⇒
    /// the thread must exit.
    pub fn try_thread_exit(&self) -> bool {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.abort {
            return true;
        }
        if g.threads_to_exit > 0 {
            g.threads_to_exit -= 1;
            return true;
        }
        false
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

    /// `reap_idle_threads` — upstream's 60 s timer callback: stop
    /// threads that were idle for the whole interval.
    pub fn reap_idle_threads(&self) {
        let idle = self.min_idle.load(Ordering::Relaxed);
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // keep at least one thread to react to interrupts
        let to_stop = std::cmp::max(0, idle - 1);
        if to_stop > 0 {
            g.threads_to_exit += to_stop;
            self.job_cond.notify_all();
        }
        self.min_idle.store(i64::MAX, Ordering::Relaxed);
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

    /// Drain all queued jobs (abort path).
    pub fn drain_jobs(&self) -> JobQueue {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.queued_writes = 0;
        g.queued_hashes = 0;
        g.queued_other = 0;
        std::mem::take(&mut g.queued)
    }

    /// `m_min_idle_threads` reset happens in `reap_idle_threads`;
    /// initialize to MAX so the first reaping sees real mins.
    pub fn init_min_idle(&self) {
        self.min_idle.store(i64::MAX, Ordering::Relaxed);
    }
}
