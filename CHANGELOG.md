# Changelog

All notable changes to BitDaemon are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).
Until `1.0.0`, breaking changes may occur in any release.

## [Unreleased]

### Added

- Workspace scaffolding: 9 crates (`common`, `bencode`, `core`, `disk`,
  `net`, `dht`, `session`, facade `bitdaemon`, `bitdaemon-cli`).
- Project governance docs: `AGENTS.md`, `ROADMAP.md`, `CONTRIBUTING.md`,
  `docs/PORTING.md` (C++→Rust module mapping), `docs/architecture.md`.
- Architecture decision records `docs/adr/0001`–`0007` covering port
  strategy, tokio runtime, workspace layout, error handling, license,
  testing/conformance and dependency policy.
- CI workflow: `fmt`, `clippy -D warnings`, `cargo test`, MSRV 1.85,
  on Ubuntu and Windows.
- Reference pin: libtorrent `master` @ `v2.1.2-45-gc019e80ef`.
- ADR-0008 injectable transports: `bitdaemon_net::transport` defines
  `DatagramSocket`, `PeerConnector`, `PeerAcceptor`, `PeerStream` —
  no subsystem binds OS sockets; anonymous/tunnel lanes and the future
  simulator plug in without patching (replaces rqbit vendoring need).
- ROADMAP: downstream driver section (OnionBit rqbit replacement),
  milestone ONIONBIT-1, uTP moved to Phase 5.
- `bitdaemon_net::NetError`/`Result` foundation for transport errors.
- ADR-0009 disk-I/O fidelity: Phase 3 ports the full upstream machinery
  (job queue, per-torrent fences, bounded buffer pool, lazy file pool,
  part files, stat cache, hash picker, back-pressure) — the subsystem
  whose absence broke rqbit downstream (eager init = minutes-long
  restore, unbounded memory, per-piece-lock RMW corruption).
- `StorageFactory` injection point documented for downstream
  encrypted/opaque-name storage (OBD parity).
- `bitdaemon_common`: `hex`, `Digest<N>`/`Sha1Hash`/`Sha256Hash`
  (big-endian ordering/shifts/bitops, `count_leading_zeros`),
  `InfoHash` (v1/v2/hybrid), `Bitfield` (MSB-first, `resize_with`
  fill semantics, trailing-bit clearing), `PeerId` (70-char printable
  set), `types` (`PieceIndex`, `PieceBlock::INVALID`,
  `DownloadPriority`, …), `Error`/`Result`.
- `bitdaemon_bencode`: faithful port of `bdecode` — flat packed-token
  tree (`offset:29|kind:3` + `next_item:29|header:3`), lazy zero-copy
  `BdecodeNode` (`list_at`/`dict_at`/`dict_find*`, `data_section`,
  `string_offset`), back-patched partial trees on error, soft-error
  detection (unsorted/duplicate keys, leading zeros), upstream error
  kinds + positions; owned `Entry`, canonical `bencode` encoder,
  `print_node`/`print_entry` pretty-printer.
- `bitdaemon_common` Phase-1 remainder: `flags` (`bitfield_flag!` macro,
  `TorrentFlags` u64 bits 0-25 incl. `DEFAULT_FLAGS`, `PexFlags` u8,
  `AnnounceFlags` u8), `time` (`Clock`/`TimePoint`/`Milliseconds32`
  aliases), `SlidingAverage<G>` (fixed-point ×64, clamped samples,
  deviation estimator), `Stat`/`StatChannel` (6 channels, `second_tick`
  4/5+1/5 average, SYN costs, IP-overhead MTU formula), `BloomFilter<N>`
  (SHA-1-derived dual indices, `size()` estimator), `random`/`random_bytes`/
  `crypto_random_bytes`/`random_shuffle` (`random(m)` inclusive),
  `crc32c_32`/`crc32c_words` (Castagnoli, LE memory order).
- Tests: 67 conformance cases ported from `test_bdecode.cpp`, 16 from
  `test_bitfield.cpp`, 8 from `test_sliding_average.cpp`, 5 from
  `test_bloom_filter.cpp`, `test_crc32.cpp` vectors (RFC 3720 B.4),
  `test_sha1_hash.cpp` shift/bitops/leading-zero vectors, `stat` formula
  checks — all green; `clippy -D warnings` clean, `cargo fmt --check`
  clean.

[Unreleased]: https://github.com/laurentgeynet-ux/BitDeamonRust/compare/main...HEAD
