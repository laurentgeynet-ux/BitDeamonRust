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
//! Status: **in progress** — `open_mode`, `file`, `fs`, `stat_cache`,
//! `part_file` ported.

// `deny` (not `forbid`) so the single reviewed FFI boundary in
// `file::win32` can opt back in — ADR-0022 §3. Unsafe anywhere else
// in this crate remains a hard error.
#![deny(unsafe_code)]

/// `disk_interface` is in [`disk_interface::DiskIo`].
/// `aux_/file.hpp` — positioned I/O handle.
pub mod file;
/// `aux_/path.hpp` fs-half + `directory.cpp` + `drive_info.cpp` +
/// `copy_file.cpp`.
pub mod fs;
/// `aux_/open_mode.hpp` — file open flags.
pub mod open_mode;
/// `aux_/part_file.hpp`.
pub mod part_file;
/// `aux_/stat_cache.hpp`.
pub mod stat_cache;

/// `aux_/file_pool_impl.hpp` — LRU open-handle pool.
pub mod file_pool;
/// `storage_error.hpp` — operation-scoped disk errors.
pub mod storage_error;

/// `aux_/back_pressure.hpp` — write-queue watermarks + observers.
pub mod back_pressure;
/// `aux_/disk_buffer_pool.hpp` + `disk_buffer_holder.hpp`.
pub mod disk_buffer_pool;
pub mod disk_cache;
/// `aux_/disk_completed_queue.hpp` — network-thread completion drain.
pub mod disk_completed;
/// `disk_interface.hpp` — `DiskIo` trait, `StorageHolder`, `disabled`.
pub mod disk_interface;
/// `pread_disk_io.cpp` — positioned-I/O orchestrator.
pub mod disk_io;
/// `aux_/disk_job.hpp` — job types, flags, completion.
pub mod disk_job;
/// `aux_/disk_job_fence.hpp` — per-torrent exclusive ordering.
pub mod disk_job_fence;
/// `aux_/disk_io_thread_pool.hpp` + `disk_job_pool` counters.
pub mod disk_pool;
/// `aux_/file_progress.hpp` — per-file download accounting.
pub mod file_progress;
/// `aux_/hash_picker.hpp` — v2 merkle hash request scheduling.
pub mod hash_picker;
/// `aux_/precomputed_block_hashes.hpp` — in-memory block hash handoff.
pub mod precomputed_block_hashes;
/// `aux_/readwrite.hpp` — piece→file buffer walk.
pub mod readwrite;
/// `aux_/pread_storage.hpp` — positioned-I/O storage backend.
pub mod storage;
/// `aux_/storage_utils.hpp` — check/move/delete helpers.
pub mod storage_free_list;
pub mod storage_utils;
/// `aux_/store_buffer.hpp` — read cache + back-pressure watermarks.
pub mod store_buffer;
