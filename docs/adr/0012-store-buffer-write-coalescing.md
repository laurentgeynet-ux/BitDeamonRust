# ADR-0012: `store_buffer` write path + write coalescing

Status: Implemented
Date: 2026-10-11
Extends: ADR-0009 (disk I/O fidelity — do not simplify)

## Context

`store_buffer.rs` and `disk_cache.rs` are ported as types but **never
instantiated**: `PreadDiskIo::async_write` enqueues one `Write` job per
16 KiB block, so every incoming piece block costs one queued job and one
positioned `write()` syscall.

Upstream (`disk_io_thread`/`mmap_disk_io`) instead inserts the block
into the per-torrent **`store_buffer`** (a `cached_piece_entry` map in
`disk_cache`), and a periodic/low-watermark flush drains **contiguous
block ranges** through a single `writev`-style call per region. Live
consequence measured against real swarms (Ubuntu 3.4 GB, ~100 MiB/s,
27+ peers):

- our write queue bursts to ~15 000-22 000 jobs (~240-350 MiB of
  checked-out `DiskBuffer`s) before `back_pressure` engages;
- `have`-piece progress arrives in stalls + bursts instead of steadily;
- the unbounded job buffer churn + Windows dirty-page buildup produced
  whole-system stalls mid-download (the "freeze" reported during live
  tests).

qBittorrent on the same swarm does none of this because libtorrent's
cache decouples *block arrival* from *syscall rate*.

## Decision

Route `async_write` through a real `StoreBuffer`/`DiskCache` instance
per storage:

1. `async_write` = `store_buffer.insert` + bookkeeping only; it does
   **not** create a disk job.
2. A **flush cursor** per storage emits one `Write` job per *contiguous
   block region* (upstream `store_buffer::check_for_flush` /
   `disk_cache::try_flush_write_blocks`), carrying a `writev` list of
   buffers instead of a single buffer. `WriteJob` gains
   `bufs: Vec<DiskBufferHolder>` + a `piece`/`offset` anchor.
3. Flush triggers, faithful to upstream:
   - piece-complete boundary (`flush_iovec` when a piece has all its
     blocks — the hasher boundary),
   - cache high watermark (`disk_cache_high_watermark`),
   - periodic expiry (`cache_expiry`, upstream default 60 s),
   - back-pressure low watermark resume,
   - `clear_piece`/`move_storage`/`delete_files` fence flushes
     (ADR-0009 ordering rule: a fence job must never observe a block
     still parked in `store_buffer`).
4. The `piece_gate` (write→hash ordering added for the read-while-write
   race) is preserved: hash jobs for a piece stay parked until that
   piece's *store_buffer region* is fully flushed, not merely until
   individual write jobs retire.

## Consequences

- Syscall rate at 100 MiB/s drops from ~6 400 writes/s to ~one
  `writev` per contiguous region (typically 10-100× fewer).
- Memory bound becomes *bytes-in-cache* (upstream
  `max_queued_disk_bytes` semantic) rather than blocks-in-job-queue —
  this is what `BackPressure` was designed to measure.
- Read path must consult `store_buffer` before touching the file —
  tracked separately in ADR-0014.
- Enables ADR-0013 (watermark timer) and unblocks faithful
  `performance_counters` (ADR-0017) since counters measure cache state.
