# ADR-0014: Read path consults `store_buffer`/cache before disk

Status: Implemented
Date: 2026-10-11
Depends on: ADR-0012 (`store_buffer` write path)

## Context

Upstream `async_read`/`Hash`/`Hash2` jobs first ask the cache
(`disk_cache::try_read` / `store_buffer::get`) whether the block is
still resident — received blocks awaiting flush are served from RAM,
never re-read from the file. This is load-bearing for correctness too:
once writes are parked in `store_buffer` (ADR-0012), a hash job that
read the file directly would see *holes* where pending blocks live —
the `piece_gate` we added for the same race becomes redundant and can
be replaced by the upstream mechanism.

Currently every `Hash`/`Read` job hits the file directly — fine while
writes were synchronous-ish, broken as soon as ADR-0012 lands.

## Decision

1. `read_job`, `hash_job` and `hash2_job` resolve each requested block
   range through `store_buffer`/`disk_cache` first; cache hits are
   served from the cached `DiskBuffer`, misses fall through to
   positioned reads.
2. The `piece_gate` introduced for the write→hash race is retired once
   coverage is proven equivalent: upstream ordering guarantee is that
   the hasher reads through the cache, not that it waits on a gate.
   Keep the gate behind `debug_assert` parity tests first; remove after
   a live download passes without it.
3. Read cache retention follows upstream `cache_expiry` (ADR-0013):
   flushed piece blocks stay readable until expiry unless
   `disk_io_read_mode = disable_os_cache`/`disable_os_cache_for_aligned`.

## Consequences

- Hash jobs stop competing with writes on the storage device during
  high-rate downloads — the common "hash reads behind 15k writes"
  starvation observed live disappears.
- Semantic dependency: ADR-0014 is a **correctness prerequisite** for
  enabling ADR-0012 by default. Implementation order is
  0012 + 0014 together, flag-gated, then 0013 tuning.
