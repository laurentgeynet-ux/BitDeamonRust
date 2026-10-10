# Changelog

All notable changes to BitDaemon are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).
Until `1.0.0`, breaking changes may occur in any release.

## [Unreleased]

### Added — `bitdaemon-net` (proxies)

- `socks` — SOCKS4/SOCKS5 handshake (auth negotiation, user/pass
  sub-negotiation, CONNECT/UDP-ASSOCIATE, bound-address drain) as an
  `async fn` over `AsyncRead + AsyncWrite` — identical wire protocol,
  works over injected streams.
- `udp_tracker` — BEP-15 tracker protocol: `build_connect` /
  `build_announce` / `build_scrape` wire packets, tid-matched reply
  parsing (v4/v6 peer strides, tracker errors), and `udp_announce`
  driving connect→announce over the injectable `DatagramSocket`.
- `http_tracker` — announce query builder (`announce_query`) and
  `on_response` parser (compact `peers`/`peers6`, dict-list peers,
  scrape `files` entries, failure/warning, external ip).
- `bep10` — BEP-10 extended handshake: `ExtendedHandshake`
  build/parse (extension `m` map, `p`, `v`, `yourip`, `reqq`,
  `complete_ago`, `upload_only`, `share_mode`, `metadata_size`,
  `ipv4`/`ipv6` hints) with `[len][0x14][0x00]` wire framing —
  `write_extensions`/`on_extended_handshake` ported.
- `http_stream` — HTTP `CONNECT` proxy handshake (`Host` header,
  `Proxy-Authorization: Basic`, byte-wise header terminator scan,
  200-status check) plus `base64_encode`. 7 tests green.

### Added — `bitdaemon-session` (piece picking)

- `peer_conn` — the `peer_connection` state machine assembled:
  remote bitfield (have/have-none/have-all/dont-have), bilateral
  choke/interested flags, BEP-6 suggest + allowed-fast sets, BEP-10
  remote extension ids, `peer_key()` identity for the picker, and
  `drive_requests` which chains `request_a_block` →
  `send_block_requests` into wire `PeerRequest`s.
- `peer_pipeline` — the request-pipeline half of
  `aux_/peer_connection.cpp`: `pending_block`, request/download
  queues, `RequestMerger` block coalescing (byte-range merge with
  truncated-piece boundary handling), `send_block_requests_impl`,
  `update_desired_queue_size` (slow-start AIMD + snub collapse),
  adaptive `request_timeout` (RTT mean + 4σ, floor 2 s).
- `request_blocks` — port of `aux_/request_blocks.cpp`
  (`request_a_block`, `source_rank`): torrent/peer state is passed in
  as `TorrentRequestCtx`/`PeerRequestCtx` snapshots; `add_request` is
  an injected closure (upstream `peer_connection::add_request`); fast-
  set masking (BEP-6 choked), `whole_pieces_threshold` contiguous
  window, `strict_end_game_mode` and the busy-block fallback are
  faithful.

- `piece_picker` — full port of `aux_/piece_picker.hpp` +
  `piece_picker.cpp`: priority buckets (`prio_factor = 3`, 8 levels),
  four download queues with reverse variants, `BlockInfo` per-block
  states (none/requested/writing/finished) with `num_peers` end-game
  refcounts, `pick_pieces` covering every upstream path (partial
  priority, suggested pieces, sequential/reverse cursors, rarest-first
  forward and reverse, random fallback, 4 MiB piece-extent affinity,
  end-game duplicate requests), hash-job lifecycle
  (`started/completed_hash_job`, `piece_passed`, `piece_flushed`,
  `lock_piece`, `restore_piece`, `write_failed`), refcount/seed
  bookkeeping, `distributed_copies`, want/have/all_pieces accounting,
  pad bytes. `torrent_peer*` references become `Option<u64>` peer
  keys; the `counters` dependency is a `PickerStats` trait injected
  per call. 18 conformance cases ported from `test_piece_picker.cpp` —
  all green.


### Added

- `bitdaemon_net` Phase-4 wave 4 (peer records + uTP ring):
  `torrent_peer` — `PeerAddr` enum replaces the `ipv4_peer`/
  `ipv6_peer`/`i2p_peer` subclass hierarchy; packed bitfield → named
  fields (failcount:5, fast_reconnects:4, trust_points signed-4,
  source:6 as `PeerSourceFlags` — new flag type in `common::flags`);
  kiB-precision accounting with live-connection stats override;
  `peer_priority` — BEP-40 CRC32C ranking (v4 /24-/16 masks,
  v6 first-difference masking, ordered ports for identical IPs);
  `test_peer_priority.cpp` vectors byte-exact incl. the BEP-40
  constants `0xec2d7224`/`0x99568189`.
  `packet_buffer` — uTP circular store over a wrapping 16-bit
  sequence space (`Vec<Option<T>>` replaces `unique_ptr<packet[]>`);
  window grow/shift, backward insert via `compare_less_wrap`,
  cursor/span tracking; `test_packet_buffer.cpp` → 6/6.

- `bitdaemon_net` Phase-4 wave 3 (peer plumbing):
  `receive_buffer` — cursor-based recv arena verbatim
  (`recv_start`/`recv_pos`/`recv_end`, `reserve`/`grow`/`cut`/
  `normalize` with `sliding_average<int64_t,20>` watermark shrink —
  added `SlidingAverage64` to `bitdaemon-common` for the >i32
  samples), `crypto_receive_buffer` wrapper (pass-through ↔ crypto
  tracking via `INT_MAX` sentinel → `Option<usize>`);
  `test_receive_buffer.cpp` → 15/15.
  `chained_buffer` — heterogeneous send-buffer chain:
  `Box<dyn BufferHolder>` replaces upstream's type-erased holder +
  destruct/move function pointers; partial `pop_front`, tail-slack
  `append`/`allocate_appendix`, `build_iovec`/mutable variant.
  `bandwidth` — `BandwidthChannel` token bucket (`inf` = INT_MAX,
  3-second burst cap, `need_queueing` fast-path), `BwRequest`
  (priority pro-rata `distribute_quota * priority / tmp`, ttl=20),
  `BandwidthManager` (queue, disconnecting-peer quota return, LIFO
  dispatch); `Rc<BandwidthChannel>`/`Rc<dyn BandwidthSocket>` replace
  raw channel pointers and `shared_ptr<bandwidth_socket>`.
  Tests: `test_buffers_bandwidth` 12/12 covering refill caps,
  pro-rata split, queue/total accounting, disconnect return.

- `bitdaemon_net` Phase-4 wave 2 (filters + MSE crypto):
  `ip_filter` — `filter_impl<A>` over `BTreeMap` (implicit range ends,
  zero/plus_one/minus_one/max_addr carry verbatim), `IpFilter` (v4+v6),
  `PortFilter`, `export_filter` minimal tiling; `test_ip_filter.cpp`
  → 9/9. `pe_crypto` — `DhKeyExchange` (768-bit safe prime via
  `num-bigint`, 96-byte BE export, degenerate-key rejection
  `[2, p-2]`, `req1/req2/req3` derivation), `Rc4` (libTomCrypt KSA/PRGA
  byte-exact — RFC 6229 vectors at 1024/4096 pass for all key sizes and
  every call-split), `Rc4Handler` (1024-byte keystream discard,
  `keyA/keyB` direction swap verbatim).

- `bitdaemon_net` Phase-4 wave 1 (peer wire codec):
  `peer_wire` — 68-byte handshake (`write_handshake`/`parse_handshake`,
  reserved bits verbatim: DHT r7|0x01, BEP-10 r5|0x10, FAST r7|0x04,
  v2-upgrade r7|0x10), all 24 `message_type` ids encoded byte-exact
  (choke..hash_reject, v2 hash messages keyed by merkle root),
  `PeerWireCodec` (`tokio_util` Encoder/Decoder, length-prefixed
  framing, keepalive, `max_len` cap), `decode` dispatch.
  `socket_io` — BE u8/16/32/64 cursor, address/endpoint bytes (6/18 B),
  `read_endpoint_list` (compact peer lists), `parse_endpoint`.
  9/9 tests (byte-exact encodings, fragmented frames, round-trips).

- `bitdaemon_disk` Phase-3 wave 5: `store_buffer` — `aux::store_buffer`
  read cache (`TorrentLocation`-keyed `Arc<[u8]>` buffers instead of raw
  pointers — `forbid(unsafe_code)`), `get`/`get2`/`insert`/`erase` with
  `BackPressure` wired in (observer registration at `max_size`,
  posted release below low watermark).
- mmap backend deferred: `aux::mmap`/`mmap_disk_io`/`mmap_storage`
  require `unsafe` (`memmap2::map_mut`); the disk crate enforces
  `forbid(unsafe_code)` and `PreadDiskIo` is the reference backend —
  documented deviation in `PORTING.md`.

- `bitdaemon_disk` Phase-3 wave 4 (v2 hash machinery):
  `hash_picker` — `validate_hash_request`, `HashPicker` (512-piece
  piece-layer chunks + per-piece block requests, 3 s
  `min_request_interval`, `pick_hashes`/`add_hashes`/
  `set_block_hash`/`hashes_rejected`/`verify_block_hashes`/
  `have_hash`/`have_all`/`piece_verified`), both request shapes matched
  independently for the `blocks_per_piece == 1` coincidence;
  `precomputed_block_hashes` (all-zero-as-absent sentinel, mutex-guarded
  per-piece vectors, `store`/`take`/`take_block`/`drop`); plus
  `FileStorage::file_first_piece_node`/`file_first_block_node`.
  `test_hash_picker.cpp` → 10/10 cases (piece-layer picking order,
  empty trees, block-request timeout/resolution/retry, coincident
  layers, leaf + piece hash insertion).
- `bitdaemon_core::resolve_links` — `aux::resolve_links` for mutable
  torrents: v1 matching by piece hashes on size+alignment candidates,
  v2 matching by file merkle root, `RenamedFiles` path overlay.
  `test_resolve_links.cpp` fixture table → 14/14.

- `bitdaemon_disk` Phase-3 wave 3b (interface + injection):
  `disk_interface::DiskIo` — the full upstream `disk_interface`
  surface (`new_torrent`/`remove_torrent`/`async_read`/`async_write`
  (returns back-pressure bool)/`async_hash`/`async_hash2`/
  `async_move_storage`/`async_release_files`/`async_check_files`/
  `async_stop_torrent`/`async_rename_file`/`async_delete_files`/
  `async_set_file_priority`/`async_clear_piece`/`get_status`/`abort`/
  `submit_jobs`), `StorageHolder` RAII (drop → `remove_torrent`),
  `DisabledDiskIo` test backend, **`DiskStorage` trait +
  `StorageFactory` injection point** — the ADR-0008 seam for
  OnionBit's `PrivateStorageFactory` (upstream removed
  `storage_interface` in 2.0; we keep a narrow storage-only seam),
  `file_progress` (per-file passed-hash byte accounting,
  `completed_cb` on file finish; `init` takes the have-pieces
  bitfield — documented deviation from `piece_picker`), `DiskIo`
  impl for `PreadDiskIo` wiring fence jobs vs. normal jobs.
  Back-pressure level = buffer-pool `in_use`.

- `bitdaemon_disk` Phase-3 wave 2+3 (job machinery + orchestrator):
  `disk_job` (all 13 upstream `job_action_t` kinds, `fence`/
  `in_progress`/`aborted` flag bits, per-job completion handlers),
  `disk_job_fence` — verbatim `raise_fence`/`is_blocked`/`job_complete`
  incl. repost-under-lock ordering, stacked-fence re-arming and the
  TOCTOU-safe `blocked_disk_jobs` counter, `disk_pool`
  (`disk_io_thread_pool`: demand-spawned workers, condvar wait,
  `should_exit`/`try_thread_exit` shrink, injected idle-reap hook —
  60 s deadline_timer deferred to session tick; `disk_job_pool`
  counters), `disk_buffer_pool` + `DiskBufferHolder` RAII +
  `BulkFreeBuffer` + per-category histogram, `back_pressure`
  (`set_max_size` watermark math `max*7/8`/`max*3/4` verbatim,
  observer coalescing, injected poster), `disk_completed_queue`
  (single in-flight drain post, `call_job_handlers` on network
  thread), `disk_io::PreadDiskIo` — `pread_disk_io` orchestrator:
  `new_torrent`/`remove_torrent`, `add_job`/`add_fence_job`,
  `perform_job` dispatch (read/partial_read/write/hash+block-hashes/
  hash2/move/delete/rename/priority/check_fastresume), `immediate_execute`
  0-thread fallback, `abort_jobs`. Fixed a non-reentrant-mutex
  deadlock in `wait_for_job` caught by the threaded test.
  Conformance: fence order/re-arm 3/3, back-pressure watermarks,
  end-to-end write→read→hash through the pool (immediate + 4 threads).

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
- `bitdaemon_net` Phase-5 groundwork:
  `utp` first wave (`UtpPacket` header codec — 20-byte big-endian wire
  format verbatim, 5 packet types, `UtpExtensions` iterator for
  sack/close-reason chains, conn-id demux on injected `DatagramSocket`),
  `lsd` (BEP-14: `render_lsd_packet` byte-exact,
  `process_packet` parse with own-cookie filter, port validation
  `1..65535`, v1/v2-truncated infohashes — multicast loop/tick loop
  still pending `net::udp` interface enumeration). Tests: LSD
  render/parse vectors incl. self-announce rejection.
- Tests: 67 conformance cases ported from `test_bdecode.cpp`, 16 from
  `test_bitfield.cpp`, 8 from `test_sliding_average.cpp`, 5 from
  `test_bloom_filter.cpp`, `test_crc32.cpp` vectors (RFC 3720 B.4),
  `test_sha1_hash.cpp` shift/bitops/leading-zero vectors, `stat` formula
  checks — all green; `clippy -D warnings` clean, `cargo fmt --check`
  clean.

[Unreleased]: https://github.com/laurentgeynet-ux/BitDeamonRust/compare/main...HEAD
