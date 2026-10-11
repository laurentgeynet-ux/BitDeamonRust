# ADR-0016: `piece_hasher` — concurrent v2 merkle hashing

Status: Implemented — block SHA-256 computed at hash/flush on disk worker
Date: 2026-10-11
Extends: ADR-0009

## Context

Upstream v2/hybrid torrents hash each 16 KiB block's SHA-256 *as it
arrives* on dedicated `piece_hasher` workers (`disk_io_thread` +
`m_v2_hash_queue` + condvar wake-up), so the per-piece `Hash2` job only
assembles the already-computed block tree. Blocks also hash while still
in `store_buffer` — the hasher reads the cache, not the file.

Our `hash2_job` computes block hashes lazily at piece-complete time,
single-threaded inside the job, and `disk_cache.rs`'s
`hasher_cursor`/`need_hasher_kick` bookkeeping is ported but unwired.

## Decision

1. Wire `need_hasher_kick` → a hasher side-channel on
   `DiskIoThreadPool`: when `store_buffer.insert` completes a block
   (ADR-0012), the block's SHA-256 is computed immediately on a worker
   and stored in `cached_block_entry.block_hash`.
2. `hash2_job` then only walks the precomputed block hashes into the
   piece layer (`precomputed_block_hashes.rs` already holds them) —
   matching upstream `piece_hasher` latency profile.
3. `hash_picker` v2 verification path is unchanged; only *when* block
   hashes get computed moves earlier.

## Consequences

- v2/hybrid torrents get upstream-grade hashing parallelism; v1-only
  torrents see no behavioural change.
- Removes the remaining double-read of piece data for v2 (arrival
  hash + piece hash).
- Scope: `disk_cache.rs` hasher fields, `disk_pool` wake mechanism,
  `hash2_job` assembly. No wire impact.
