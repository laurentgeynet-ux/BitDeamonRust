# ADR-0013: Disk cache watermarks + periodic flush scheduling

Status: Partially implemented (cache_expiry flush done; watermarks pending)
Date: 2026-10-11
Depends on: ADR-0012 (`store_buffer` write path)

## Context

With a real `store_buffer` in place (ADR-0012) the cache needs its
upstream pacing: without watermarks the store_buffer is just an
unbounded queue one level earlier.

Upstream knobs (`settings_pack` / `disk_io_thread`):

- `disk_cache_low_watermark` (default: 60 % of
  `max_queued_disk_bytes`) — resume-request threshold,
- `disk_cache_high_watermark` (implicit `max_queued_disk_bytes`) —
  stop-requesting / start-flushing threshold,
- `cache_expiry` (default 60 s) — stale cached blocks flush even
  below watermark,
- a **periodic flush timer** on the disk thread that walks dirty
  pieces oldest-first.

Current code only has `BackPressure::max_size` counted in *blocks in
the job queue* — which is empty precisely when the cache should be
counted.

## Decision

1. `SessionSettings` gains `disk_cache_low_watermark` (fraction or
   bytes — port upstream's ratio semantics verbatim) and
   `cache_expiry: Duration`.
2. `BackPressure` level is re-based to count **bytes held in
   `store_buffer` + in-flight write jobs** (upstream `m_blocks`
   semantic), not queued jobs alone.
3. The disk pool gets a lightweight periodic wake
   (`interval(cache_expiry tick)` ≈ upstream's flush timer) that
   flushes pieces whose oldest dirty block exceeded `cache_expiry`.
4. Hysteresis is preserved: above high watermark → flush + peers get
   `has_back_pressure = true`; below low watermark →
   `DiskObserver::on_disk` resumes requesters.

## Consequences

- Steady-state memory for the download path becomes
  `max_queued_disk_bytes` (default 100 MiB) — predictable under any
  peer count.
- Combined with write-through files (ADR-0015), total process +
  page-cache footprint stays bounded during > 100 MiB/s runs.
- Test impact: `store_buffer`/`disk_cache` unit tests already ported
  must now drive the wired path (flush ordering, expiry, watermark
  transitions).
