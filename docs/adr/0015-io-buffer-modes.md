# ADR-0015: `disk_io_write_mode`/`disk_io_read_mode` — complete io_buffer_mode wiring

Status: Implemented
`--no-os-cache`, 2026-10-11); read side pending.
Date: 2026-10-11

## Context

Upstream `settings_pack::disk_io_write_mode` / `disk_io_read_mode`
take `io_buffer_mode_t`: `enable_os_cache` (default) /
`disable_os_cache_for_aligned_files` (v1 ABI) / `disable_os_cache`.

Live test finding (2026-10-11): on Windows, sustained ~80-110 MiB/s
downloads with cached writes accumulate dirty pages faster than the
cache manager trims → synchronous write throttling stalls the disk
pool → job queue balloons (~350 MiB) → whole-system freeze. The fix is
the upstream toggle, not a hack: `no_cache` →
`FILE_FLAG_WRITE_THROUGH` (Windows) / `O_SYNC` (POSIX), and — fixed in
the same pass — `FILE_FLAG_RANDOM_ACCESS` must never be set (upstream
`file.cpp` comment, KB2549369: it disables cache-manager trimming and
was the original freeze mechanism).

## Decision

1. `SessionSettings.disable_os_write_cache: bool` (landed) →
   `DiskIo::set_write_no_cache` → `OpenMode::NO_CACHE` OR'd into data
   write opens. CLI exposes `--no-os-cache`.
2. Pending: `disk_io_read_mode` symmetric wiring — `no_cache` on
   *read* opens (hash jobs, upload reads) for users on memory-tight
   systems; POSIX `posix_fadvise(DONTNEED)`/`F_NOCACHE` equivalents
   after reads, matching `file.cpp`'s advisory calls.
3. Pending: `disable_os_cache_for_aligned_files` — the
   `NO_BUFFERING` variant; only valid once writes are sector-aligned
   (currently they are not — 16 KiB blocks at arbitrary file offsets).
   Documented as `won't port` unless buffer alignment work lands.

## Consequences

- `--no-os-cache` gives users the qBittorrent "OS cache" escape hatch
  today; default stays `enable_os_cache` (upstream default).
- POSIX note: `O_SYNC` makes every 16 KiB write durable-synchronous —
  correct but slower; the ADR-0012 coalescing pass amortises it.
