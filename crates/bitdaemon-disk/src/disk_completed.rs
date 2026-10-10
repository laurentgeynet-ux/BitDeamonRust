//! Port of `aux_/disk_completed_queue.hpp`.
//!
//! Completed disk jobs queue here; when the queue grows 0→1 a drain
//! message is posted to the network thread (at most one in flight —
//! upstream `m_job_completions_in_flight`). `call_job_handlers` runs on
//! the network thread and invokes each job's callback.

use std::sync::{Arc, Mutex};

use crate::disk_job::{DiskJob, job_flags};
use crate::disk_pool::JobQueue;
use crate::storage_error::StorageError;

/// Poster for the drain message — upstream `post(ioc, call_job_handlers)`.
/// Called once per 0→1 transition; the supplied closure drains and runs
/// the handlers when invoked by the owner.
pub type DrainPoster = Arc<dyn Fn() + Send + Sync>;

/// `aux::disk_completed_queue`.
pub struct DiskCompletedQueue {
    inner: Mutex<Inner>,
    /// upstream `m_ios` — how we reach the network thread.
    poster: DrainPoster,
}

struct Inner {
    jobs: JobQueue,
    in_flight: bool,
}

impl DiskCompletedQueue {
    /// `disk_completed_queue(free_jobs, cnt)` — jobs are freed by drop;
    /// `poster` is the injected io_context post.
    #[must_use]
    pub fn new(poster: DrainPoster) -> Self {
        Self {
            inner: Mutex::new(Inner {
                jobs: JobQueue::new(),
                in_flight: false,
            }),
            poster,
        }
    }

    /// `append(jobs)` — queue completed jobs; post a drain on 0→1.
    pub fn append(&self, jobs: JobQueue) {
        if jobs.is_empty() {
            return;
        }
        let post = {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            g.jobs.extend(jobs);
            if g.in_flight {
                false
            } else {
                g.in_flight = true;
                true
            }
        };
        if post {
            (self.poster)();
        }
    }

    /// `append` single job.
    pub fn push(&self, j: Box<DiskJob>) {
        let mut q = JobQueue::new();
        q.push_back(j);
        self.append(q);
    }

    /// `abort_job(j)` — mark aborted and post (upstream fails with
    /// `operation_aborted` when it executes).
    pub fn abort_job(&self, j: Box<DiskJob>) {
        self.push(j);
    }

    /// `call_job_handlers` — runs on the network thread: drain and call
    /// each handler. Returns the number of callbacks invoked.
    pub fn call_job_handlers(&self) -> usize {
        let jobs: JobQueue = {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            g.in_flight = false;
            std::mem::take(&mut g.jobs)
        };
        let n = jobs.len();
        for mut j in jobs {
            j.call_callback();
        }
        n
    }

    /// pending completions (tests).
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .jobs
            .len()
    }
}

/// Helper: mark a job aborted with `operation_aborted` semantics —
/// upstream sets `j->flags |= aborted`; execution then immediately
/// errors. We additionally stamp the error so callers that never run
/// the job still see it.
pub fn abort(j: &mut DiskJob) {
    j.flags |= job_flags::ABORTED;
    j.error = StorageError::new(
        std::io::Error::from(std::io::ErrorKind::Interrupted),
        bitdaemon_common::types::FileIndex(-1),
        crate::storage_error::Operation::Unknown,
    );
}
