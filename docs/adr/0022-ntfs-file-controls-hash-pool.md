# ADR-0022: NTFS file controls (sparse / `SetFileValidData`) + dedicated hashing thread pool

- Status: accepted
- Date: 2026-10-11
- Deciders: port team

## Context

Live benchmarks against the 6 GiB Ubuntu torrent (2026-10-11) show a
large gap vs qBittorrent on HDD (~58 MiB/s sustained, no stalls) while
BitDaemon oscillates around ~20 MiB/s with back-pressure plateaus.
On C: (SSD) the gap mostly disappears — the bottleneck is
mechanical, not network.

Source comparison (`D:\Projet\libtorrent`, `D:\Projet\qBittorrent`):

- qBittorrent's `DiskIOType::Default` resolves to upstream
  `default_disk_io_constructor`, which on Windows is
  **`pread_disk_io`** — the same backend we ported. The delta is not
  the backend.
- Upstream `pread_storage::open_file_impl` (`pread_storage.cpp:764`)
  adds `open_mode::sparse` for every file when `!m_allocate_files`,
  and for priority-0 files unconditionally. `file.cpp:581` then issues
  `DeviceIoControl(FSCTL_SET_SPARSE)`. On NTFS a non-sparse write
  beyond the Valid Data Length triggers **synchronous zero-fill of
  the gap inside the write call**; torrents write blocks out of order
  by design, so every write pays a hidden penalty. On HDD this alone
  explains the observed plateaus.
- Upstream additionally supports `enable_set_file_valid_data`
  (`pread_storage.cpp:731` → `allow_set_file_valid_data`;
  `file.cpp:585-600` → `SetFileValidData` after truncate), which
  skips zero-fill for preallocated files. qBittorrent sets
  `enable_set_file_valid_data = true` (`sessionimpl.cpp:1831`). Our
  `bitdaemon-cli` forces `StorageMode::Allocate`, so this is the
  relevant path for our benchmarks as well.
- Upstream runs two disk thread pools: `m_generic_threads`
  (`aio_threads` = **10**) and `m_hash_threads`
  (`hashing_threads` = **1**, dedicated to `hash`/`hash2` jobs). We
  run a single 4-thread pool — hash reads of 256 KiB pieces stall
  queued writes behind them.
- `OpenMode::SPARSE` and `OpenMode::ALLOW_SET_FILE_VALID_DATA` are
  already defined in our `open_mode.rs` port but **never consumed** —
  another instance of the "ported, not wired" pattern (cf. Phase I).

## Decision

### 1. Wire `OpenMode::SPARSE` (upstream `pread_storage.cpp:764-768`)

`PreadStorage::open_file` sets `mode |= SPARSE` when
`!self.allocate_files`, and unconditionally for files whose priority
is `DONT_DOWNLOAD`. `FileHandle::open` (Windows) issues
`DeviceIoControl(FSCTL_SET_SPARSE)` when `SPARSE && WRITE`, matching
`file.cpp:578-583` (failure tolerated — upstream ignores the result).

### 2. Wire `ALLOW_SET_FILE_VALID_DATA` (upstream `file.cpp:585-600`)

New setting `Settings::enable_set_file_valid_data` (upstream default
`false`; our CLI enables it to mirror qBittorrent). When set, storage
open modes gain `ALLOW_SET_FILE_VALID_DATA`; `FileHandle` then calls
`SetFileValidData` after `set_len` on `TRUNCATE && !SPARSE`, with a
best-effort `SE_MANAGE_VOLUME_NAME` privilege enable — upstream does
exactly this and tolerates failure.

### 3. Second `unsafe` carve-out — `bitdaemon-disk::file::{win32,posix}`

`FSCTL_SET_SPARSE` / `SetFileValidData` have no safe-Rust std
equivalent. `#![forbid(unsafe_code)]` in `bitdaemon-disk` becomes
`#![deny(unsafe_code)]` and a single `#[cfg(windows)] #[allow(
unsafe_code)]` module `file::win32` holds the `windows-sys` FFI
(three functions). This is the same pattern as the `bitdaemon-mmap`
exception (ADR-0010 §3), scoped smaller: unsafe is confined to one
file, Windows-only, with every call site reviewed. `windows-sys` is
added as a `cfg(windows)`-only dependency (ADR-0007 note: zero cost
on POSIX).

**Phase N extension**: `file::posix` is the symmetric POSIX sibling
(`#[cfg(unix)]`) holding `libc` calls for the advisory cache hints —
`posix_fadvise(DONTNEED|SEQUENTIAL)` and apple `fcntl(F_NOCACHE|
F_NODIRECT)` — plus the allocation flags `posix_fallocate`, apple
`fcntl(F_PREALLOCATE)` and linux `ioctl(FS_IOC_GETFLAGS|SETFLAGS)`
for `no_cow`. `nix` was evaluated first: its `FcntlArg` does not
expose `F_NOCACHE`/`F_NODIRECT`, so it would only cover fadvise —
raw `libc` (already a workspace dependency) is the single mechanism.
Every call is best-effort (upstream ignores hint failures); only
`posix_fallocate` surfaces errors (`ENOSPC` → `file_fallocate`).

Non-Windows platforms are otherwise unchanged — upstream's
sparse/valid-data knobs are `#ifdef _WIN32` too; on POSIX `SPARSE`
is a no-op (the FS already allocates on write).

### 4. Dedicated hashing pool (upstream `m_hash_threads`)

`PreadDiskIo` gains `hash_pool: DiskIoThreadPool`. `enqueue` routes
`Hash`/`Hash2` jobs to it; everything else stays on the generic pool.
`set_max_threads(n)` drives the generic pool (`aio_threads`),
`set_hash_threads(n)` the hashing pool (`hashing_threads`). Defaults
follow upstream: CLI uses `aio_threads=10`, `hashing_threads=1`.
Piece-ordering is preserved: `piece_gate` blocks hash jobs behind
in-flight writes *before* pool routing, so the split pools cannot
reorder a hash ahead of its writes.

### 5. Settings surface

`Settings` gains `aio_threads` (10), `hashing_threads` (1),
`enable_set_file_valid_data` (false) — same names/defaults as
`settings_pack`. CLI flags: `--aio-threads`, `--hashing-threads`,
`--enable-set-file-valid-data` (default on in the CLI to mirror the
qBittorrent baseline we benchmark against).

## Consequences

- Removes the dominant NTFS zero-fill cost on HDD; expected to lift
  D: throughput toward the qBittorrent baseline.
- `StorageParams` unchanged — injected storages (OnionBit private
  storage) are unaffected; the new `DiskStorage::open_mode_extra`
  trait hook is a default no-op.
- `windows-sys` joins the dependency list under an ADR note; `libc`
  joins as a `cfg(unix)`-only dependency for `file::posix` (already
  declared in the workspace root — zero new crate).
- The `forbid`→`deny` change weakens a compile-time guarantee inside
  `bitdaemon-disk`; reviewers must verify `unsafe` stays confined to
  `file::{win32,posix}` (enforced by code review + `#[allow]`
  scoping — `deny` still fires on any new unsafe elsewhere).
