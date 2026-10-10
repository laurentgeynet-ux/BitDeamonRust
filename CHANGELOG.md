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
  `crc32c_32`/`crc32c_words` (Castagnoli, LE memory order),
  `IndexRange`/`IndexStep` (`index_range`), `TimestampHistory` +
  `compare_less_wrap` (uTP base-delay estimator, 20-slot ring).
  `stat_cache` deferred to Phase 3 (wraps `file_storage`/`stat_file`).
- `bitdaemon_core` Phase-2 first wave (metainfo):
  `path`/`utf8` byte-oriented helpers (upstream `std::string` semantics,
  traversal + invalid-char sanitization, ADS/device-name rules),
  `FileStorage` (file entries, pad files, piece/block mapping, symlink
  sanitization, path hashing via lowercase CRC32C), `TorrentInfo`
  (v1/v2/hybrid parse, info-hash SHA-1+SHA-256, file-tree walk,
  trackers/web-seeds/DHT nodes, `piece layers` **with per-file merkle
  root validation** — `v2_invalid_piece_layer_root` rejected),
  `merkle` (BEP-52 tree math: `merkle_num_leafs`, `merkle_pad`,
  `merkle_root`, `validate_piece_layer`), `escape_string` (`unescape`/
  `escape`/`escape_path`/`need_encoding`/`base32decode`/`split_string`/
  `parse_decimal`), `add_torrent_params` (single canonical struct for
  torrent/magnet/resume), `magnet_uri` (`parse_magnet_uri` +
  `make_magnet_uri`, `so=` selections, `x.pe`/`dht`/`tr`/`ws`/`dn`/`xt`
  v1+b32+btmh), `resume_data` (`read_resume_data`/`write_resume_data`
  file-version 1+2, bitfields, unfinished pieces, merkle trees, peers
  v4/v6, flags — byte-compatible both directions per ADR-0009).
  Conformance: `test_file_storage` 28/28, upstream `.torrent` fixtures
  (valid + invalid incl. piece-layer root), `test_magnet` 24/24,
  `test_read_resume` vectors + full round-trip.
- `bitdaemon_disk` Phase-3 first wave (the disk-fidelity milestone):
  `open_mode` flags (11 bits matching upstream), `file` positioned
  I/O (`pread_all`/`pwrite_all`/`pwritev_all`/`pread_best_effort`,
  Windows `FILE_FLAG_*` mapping, `set_len` prealloc), `fs` (`stat_file`
  with atime/mtime/ctime+attrs, `dont_follow_links`, `rename`,
  `create_directories`, `remove`, `exists`, `is_directory`,
  `hard_link`, `copy_file` hard-link-first, `drive_info` via fs2,
  `canonicalize_path`/`complete`/`remove_all`), `stat_cache`
  (error-caching with sentinel encoding, poison-tolerant mutex),
  `file_pool` — **the ADR-0009 core**: lazy open, LRU eviction at
  capacity, per-file open-waiter dedup (stalls counter), read→write
  upgrade race handling, ENOENT→mkdir retry, deferred close outside
  the mutex, `part_file` (byte-compatible header: `[u32 n][u32 ps]
  [i32 slot×n]` rounded to 1 KiB, slot reuse, `export_file` freeing
  full pieces, empty→delete), `readwrite` (upstream `readwrite_impl`/
  `readwrite_vec_impl` piece→file walk incl. empty-file skipping and
  short-read EOF semantics), `pread_storage` (read/write/writev/
  hash-with-feeder, pad-file zeroes, part-file redirection for
  `dont_download`, `initialize` with part-file compat + oversized
  detection, `set_file_priority` moving data out of the part file,
  `move_storage` with all 5 upstream modes + cross-volume copy
  fallback, `delete_files` with dir cleanup, `rename_file`,
  `verify_resume_data` seed/non-seed paths incl. hard-link pick-up),
  `storage_error` (`operation_t` parity). Tests: part_file 4/4,
  fs/stat_cache, file_pool 5/5 (LRU/upgrade/release), storage
  end-to-end 6/6 (cross-file piece I/O, part-file round-trip,
  resume verify incl. seed-mode size check).
- `bitdaemon_core` Phase-2 second wave:
  `merkle_tree` (full `aux::merkle_tree` port — sparse trees, piece-layer
  mode, `set_block`/`add_hashes` proofs, verified-bitfield tracking,
  `Unknown`/`Ok`/`HashFailed`/`BlockHashFailed` results),
  `create_torrent` (`CreateTorrent`/`CreateFileEntry`, `canonicalize`
  pad-file insertion, v1/v2/hybrid `generate` via canonical `Entry`,
  `set_piece_hashes` hashing files from disk, piece-size table rules),
  `write_torrent_file` (piece layers, tracker tiers, url seeds, DHT
  nodes, similar/collections, comment/creator/creation date),
  `fingerprint`/`identify_client` (Azureus/Shadow/Mainline/Transmission
  legacy styles + name map; `Fingerprint::generate`),
  `resolve` (`resolve_duplicate_filenames` — case-insensitive `.N`
  renames, directory precedence, pad-file collision exceptions,
  collision-limit error; wired into `parse_torrent_file` populating
  `renamed_files`), `web_seed` data struct. Conformance:
  `test_merkle_tree` 22/22, `test_create_torrent` 8/8,
  `test_identify_client` + generate vectors, `test_resolve_duplicates`
  12/12 (upstream table, POSIX-normalized for Windows separators).
- Tests: 67 conformance cases ported from `test_bdecode.cpp`, 16 from
  `test_bitfield.cpp`, 8 from `test_sliding_average.cpp`, 5 from
  `test_bloom_filter.cpp`, `test_crc32.cpp` vectors (RFC 3720 B.4),
  `test_sha1_hash.cpp` shift/bitops/leading-zero vectors, `stat` formula
  checks — all green; `clippy -D warnings` clean, `cargo fmt --check`
  clean.

[Unreleased]: https://github.com/laurentgeynet-ux/BitDeamonRust/compare/main...HEAD
