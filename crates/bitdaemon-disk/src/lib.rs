//! Disk I/O subsystem.
//!
//! Ports the `disk_interface` abstraction and its backends
//! (`mmap_disk_io`, `posix_disk_io`, `pread_disk_io`, `disabled_disk_io`),
//! storage (`mmap_storage`, `posix_storage`, `pread_storage`,
//! `storage_utils`), `part_file`, `file_pool`, `disk_buffer_pool`,
//! `disk_io_thread_pool`, `disk_job` / `disk_job_fence` / `disk_job_pool`,
//! `stat_cache`, `file_progress`, `hash_picker`,
//! `precomputed_block_hashes`, `merkle` / `merkle_tree`.
//!
//! Rust shape: an async `DiskIo` trait object per torrent, jobs
//! processed by a dedicated blocking pool (`tokio::task::spawn_blocking`
//! or a bespoke thread pool) — see ADR-0002 and `docs/architecture.md`.
//!
//! Status: **not ported yet** — see `docs/PORTING.md`.

#![forbid(unsafe_code)]

/// Async disk backend abstraction (`libtorrent::disk_interface`).
///
/// TODO: implement — `async_read`/`async_write`/`async_hash`/`async_hash2`/
/// `async_move_storage`/`async_release_files`/`async_delete_files`/
/// `async_check_files`/`async_rename_file`/`async_clear_piece` +
/// `new_torrent`/`remove_torrent`/`abort_jobs`/`update_stats`.
/// C++ uses a job-queue + completion-callback model; in Rust this maps
/// to `async fn` returning `Result<...>` or `mpsc` completion events.
pub trait DiskIo {}
