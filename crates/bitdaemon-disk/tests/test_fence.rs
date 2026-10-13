//! Conformance for `disk_job_fence` + `back_pressure` — ordering rules
//! transcribed from `disk_job_fence.cpp` semantics.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bitdaemon_common::types::PieceIndex;
use bitdaemon_disk::back_pressure::BackPressure;
use bitdaemon_disk::disk_buffer_pool::DiskBufferHolder;
use bitdaemon_disk::disk_job::{DiskJob, JobKind, ReadJob, ReleaseFilesJob};
use bitdaemon_disk::disk_job_fence::{
    DiskJobFence, FENCE_POST_FENCE, FENCE_POST_NONE, FenceCounters,
};

fn release_job() -> Box<DiskJob> {
    Box::new(DiskJob {
        storage: bitdaemon_common::types::StorageIndex(u32::MAX),
        flags: 0,
        ret: 0,
        error: Default::default(),
        action: JobKind::ReleaseFiles(ReleaseFilesJob {
            handler: Box::new(|| {}),
        }),
    })
}

fn read_job(piece: i32) -> Box<DiskJob> {
    Box::new(DiskJob {
        storage: bitdaemon_common::types::StorageIndex(u32::MAX),
        flags: 0,
        ret: 0,
        error: Default::default(),
        action: JobKind::Read(ReadJob {
            handler: Box::new(|_, _| {}),
            buf: DiskBufferHolder::empty(),
            buffer_size: 0x4000,
            piece: PieceIndex(piece),
            offset: 0,
            volatile: false,
            sequential: false,
        }),
    })
}

#[test]
fn fence_blocks_and_drains_in_order() {
    let fence = DiskJobFence::new();
    let cnt = FenceCounters::default();

    // two normal jobs in flight
    let mut j1 = fence.is_blocked(read_job(0), &cnt).unwrap();
    let mut j2 = fence.is_blocked(read_job(1), &cnt).unwrap();
    assert_eq!(fence.num_outstanding_jobs(), 2);

    // raise the fence — must wait for j1+j2
    let (ret, _queued) = fence.raise_fence(release_job(), &cnt);
    assert_eq!(ret, FENCE_POST_NONE);
    assert!(fence.has_fence());
    assert_eq!(cnt.blocked_disk_jobs.load(Ordering::Relaxed), 1);

    // new jobs queue behind the fence
    assert!(fence.is_blocked(read_job(2), &cnt).is_none());
    assert_eq!(fence.num_blocked(), 2);

    // j1 completes — fence still up, nothing reposted
    let mut reposted = Vec::new();
    let n = fence.job_complete(&mut j1, &mut |j, _up| reposted.push(j));
    assert_eq!(n, 0);

    // j2 completes — the fence job is reposted (alone)
    let n = fence.job_complete(&mut j2, &mut |j, _up| reposted.push(j));
    assert_eq!(n, 1);
    assert_eq!(reposted.len(), 1);
    assert!(reposted[0].flags & 1 != 0, "reposted job is the fence");

    // fence completes → queued read reposted
    let mut fj = reposted.pop().unwrap();
    let n = fence.job_complete(&mut fj, &mut |j, _up| reposted.push(j));
    assert_eq!(n, 1);
    assert!(!fence.has_fence());
    assert_eq!(
        reposted[0].get_type(),
        bitdaemon_disk::disk_job::JobAction::Read
    );
    assert_eq!(fence.num_blocked(), 0);
}

#[test]
fn raise_fence_with_no_outstanding_posts_immediately() {
    let fence = DiskJobFence::new();
    let cnt = FenceCounters::default();
    let (ret, job) = fence.raise_fence(release_job(), &cnt);
    assert_eq!(ret, FENCE_POST_FENCE);
    assert!(job.is_some());
    assert!(fence.has_fence());
    assert_eq!(fence.num_outstanding_jobs(), 1);
}

#[test]
fn second_fence_rearms() {
    let fence = DiskJobFence::new();
    let cnt = FenceCounters::default();
    let mut j1 = fence.is_blocked(read_job(0), &cnt).unwrap();

    fence.raise_fence(release_job(), &cnt);
    fence.raise_fence(release_job(), &cnt);
    assert_eq!(fence.num_blocked(), 2);

    let mut reposted = Vec::new();
    fence.job_complete(&mut j1, &mut |j, _up| reposted.push(j));
    // first fence reposted
    assert_eq!(reposted.len(), 1);
    let mut f1 = reposted.pop().unwrap();

    // completing first fence hits the second fence — re-armed, reposted alone
    let n = fence.job_complete(&mut f1, &mut |j, _up| reposted.push(j));
    assert_eq!(n, 1);
    assert!(reposted[0].flags & 1 != 0);
    assert!(fence.has_fence());
}

#[test]
fn back_pressure_watermarks() {
    struct Obs(AtomicUsize);
    impl bitdaemon_disk::back_pressure::DiskObserver for Obs {
        fn on_disk(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let bp = BackPressure::new();
    bp.set_max_size(100);
    // high = 100/8*7 = 87 ; low = 100/4*3 = 75
    assert_eq!(bp.should_flush(50), None);
    assert_eq!(bp.should_flush(90), Some(75));

    let obs = Arc::new(Obs(AtomicUsize::new(0)));
    assert!(!bp.has_back_pressure(10, None));
    assert!(bp.has_back_pressure(100, Some(obs.clone())));

    bp.check_buffer_level(80); // still above low
    assert_eq!(obs.0.load(Ordering::Relaxed), 0);
    bp.check_buffer_level(70); // drains → observer posted
    assert_eq!(obs.0.load(Ordering::Relaxed), 1);
    // exceeded flag cleared — no second flush storm
    bp.check_buffer_level(10);
    assert_eq!(obs.0.load(Ordering::Relaxed), 1);
}
