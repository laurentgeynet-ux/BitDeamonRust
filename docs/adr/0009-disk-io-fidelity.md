# 0009. Disk I/O — port the full upstream machinery, do not simplify

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner
- Extends: ADR-0001 (semantic port), ADR-0008 (StorageFactory seam)

## Context

The second half of the motivation for this port — alongside tunnels
(ADR-0008) — is that **rqbit's disk I/O and memory management fails the
downstream use case**. Concretely, from OnionBit's production history:

- **Eager init**: rqbit's storage created+allocated every file at
  add-time — ~31 ms/file on Windows → **349 s** restoring an
  11 310-file torrent (their ADR-0008). They patched in lazy
  `ensure_open` — i.e. re-implemented libtorrent's file pool by hand.
- **Lock granularity vs. real formats**: rqbit's per-piece write lock
  vs. their `OBD` chunked ciphertext (chunks straddling piece
  boundaries) → silent read-modify-write corruption risk → they had to
  add striped `RwLock`s themselves.
- **fastresume**: sampling check runs *after* torrents appear restored;
  "restored" and "checked" are two distinct states; their opaque-name
  zone must inject fastresume under HMAC'd names from the start.
- **Memory**: buffer/caching behavior under load is a known weak spot —
  the port must carry libtorrent's bounded-buffer machinery, not a
  naive `fs::write` per piece.

libtorrent's disk subsystem is ~20k LOC precisely *because* these
problems are real: `disk_io_thread` job queues, `disk_job_fence`
ordering, `disk_buffer_pool` memory caps, `file_pool` lazy-open LRU,
`part_file` edge-piece spill, `stat_cache`, `hash_picker` (v2),
`back_pressure`, `check_files`, resume-data round-trips. Simplifying
this layer would recreate rqbit's failure modes in safe Rust.

## Decision

1. **Phase 3 ports the machinery, not just the trait**: job queue +
   per-torrent fences, bounded buffer pool (`disk_buffer_pool`
   semantics + `settings_pack::max_queued_disk_bytes` /
   `send_buffer_watermark` knobs in typed `Settings`), lazy `FilePool`
   with handle limits, `PartFile`, `StatCache`, `HashPicker`,
   `FileProgress`, back-pressure.
2. **One backend + one optional**: `std::fs` positioned I/O is the
   default backend (covers `pread`/`posix`/`mmap_storage` triplication);
   `memmap2` backend behind a cargo feature. `disabled_disk_io` ports
   as the test backend.
3. **Deferred-error semantics are the spec**: files open lazily —
   invalid paths/permissions surface at first I/O as alerts, not at
   `add_torrent`. This matches upstream file-pool behavior *and* the
   deferred-error model OnionBit already standardized on (their
   ADR-0008). Documented in `PORTING.md` so it isn't "fixed" later.
4. **Resume data is byte-compatible with upstream**
   (`read_resume_data`/`write_resume_data`): loads libtorrent/Tribler
   `.resume` files and writes the same format back. OnionBit's opaque
   naming rides *above* via `StorageFactory` — the format underneath
   stays stock.
5. **Concurrency contract**: storage ops are ordered by per-torrent
   fences like upstream; custom storages (e.g. chunked-ciphertext OBD)
   get correct exclusive/write-shared semantics from the job model
   itself, not from ad-hoc locking added by the consumer.

## Consequences

- Phase 3 is heavier than a naive storage port — that is the point;
  it's the differentiation from rqbit.
- `Settings` must carry the disk knobs with upstream defaults from
  `settings_pack.cpp` (`file_pool_size`, `max_queued_disk_bytes`,
  `send_buffer_watermark`, `checking` options, `use_disk_cache_pool`).
- Checking throughput + memory caps are validated in Phase 3 exit
  criteria (hash a pre-seeded multi-GB dataset under a bounded buffer
  pool; restore an 11k-file torrent in milliseconds).
- The Phase-3 design review must keep the `StorageFactory` surface
  usable for OBD-style chunk-straddling formats without consumer-side
  locking hacks.
