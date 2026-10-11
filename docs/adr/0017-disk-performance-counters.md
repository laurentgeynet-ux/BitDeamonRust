# ADR-0017: Disk `performance_counters` wiring

Status: Accepted (implementation pending)
Date: 2026-10-11
Depends on: ADR-0012/0013 (counters must measure the real cache)

## Context

Upstream exposes ~200 `stats_metric` counters; the disk ones
(`disk.*` — blocks read/written, cache hits, queued bytes, job
latencies, fence waits, hasher queue depth, `num_writing_threads`,
`num_running_threads`) are how operators diagnose exactly the
orchestration issue we debugged live this week. We currently have an
ad-hoc `queue_stats()` snapshot on `DiskIo` and session `Counters` —
no upstream-shaped metric surface.

## Decision

1. Introduce `bitdaemon-session::metrics` (or reuse `Counters`) with
   the upstream `stats_metric[]` names for the disk group first;
   other groups (`net.*`, `peer.*`, `ses.*`) follow in their own
   passes — this ADR only commits the disk subset.
2. `PreadDiskIo` publishes atomic counters matching
   `mmap_disk_io`/`disk_io_thread`: `disk.blocks_in_use`,
   `disk.queued_write_bytes`, `disk.queued_hash_jobs`,
   `disk.block_cache_num_blocks`, `disk.write_mode`,
   `disk.read_mode`, `disk.num_jobs`, per-action job counts.
3. `TorrentHandle::session_stats`/`session_stats_metrics` surfaces
   them; the CLI `disk queue:` diagnostic line migrates to reading
   the counters rather than a bespoke snapshot (no duplicated
   plumbing).

## Consequences

- Diagnostics become upstream-shaped — downstream (OnionBit) can poll
  `session_stats` without touching internals.
- Replaces today's ad-hoc `queue_stats()` once coverage is equal;
  the trait method is removed at that point.
- Counter names/ids are pinned to upstream `stats.cpp` ordering where
  they exist so third-party tooling stays meaningful.
