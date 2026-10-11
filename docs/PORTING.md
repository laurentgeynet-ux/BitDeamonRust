# PORTING.md — C++ → Rust module ledger

This file is the **single source of truth** for port status.
Update it **in the same commit** as the code it describes — see
`AGENTS.md` §5 and §7.

Upstream: `libtorrent` @ `v2.1.2-45-gc019e80ef`
(local checkout `D:\Projet\libtorrent\libtorrent` — read-only).

## Status legend

| Status | Meaning |
|---|---|
| `not started` | C++ identified, no Rust code |
| `in progress` | Someone is actively porting it |
| `ported` | Rust code complete, ported unit tests pass |
| `verified` | Wire-format / integration test against C++ or live swarm |
| `won't port` | Deliberately skipped — reason required |
| `n/a` | Build system / platform shim, no Rust equivalent |

C++ `*.cpp` files are listed with their primary header in parentheses
when names differ. `aux_*` headers live in `include/libtorrent/aux_/`.

---

## bitdaemon-common

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `sha1_hash.cpp` / `sha256.cpp` (`digest32.hpp`) | `common::hash` | ported | `Digest<N>` + `Sha1Hash`/`Sha256Hash`; big-endian shifts, bitwise ops, `count_leading_zeros` — upstream test vectors pass |
| `info_hash.hpp` | `common::info_hash` | ported | v1/v2/hybrid `InfoHash`: `has_v1`/`has_v2`/`get`/`get_best` |
| `bitfield.cpp` | `common::bitfield` | ported | MSB-first wire order, `resize_with` fill semantics, trailing-bit clearing; 16 cases from `test_bitfield.cpp` pass |
| `peer_id.hpp` / `generate_peer_id.cpp` | `common::peer_id` | ported | 70-char printable set, `random(69)` inclusive quirk reproduced |
| `peer_request.hpp`, `piece_block.cpp` | `common::types` | ported | `PieceIndex`/`BlockIndex`…, `PieceBlock::INVALID` = `(max, i32::MAX)` |
| `error_code.cpp`, `error.hpp`, `close_reason.cpp` | `common::error` + `common::lt_error` + `net::close_reason` | **ported** | full `LtError` enum (codes 0-220 verbatim incl. gaps) + `HttpError` category + upstream message table; `CloseReason` enum + `close_reason_t` strings verbatim; `bencode::BdecodeErrorKind` for the bdecode category |
| `time.cpp`, `timestamp_history.cpp` | `common::time` | ported | `Clock`/`TimePoint` aliases on `Instant`, `Milliseconds32` i32 ticks; `timestamp_history` ported (`common::timestamp_history` — wrap-compare + base stepping verbatim) |
| `download_priority.hpp`, `operations.hpp`, `torrent_flags.hpp`, `pex_flags.hpp`, `announce_flags.hpp`, `flags.hpp` | `common::types`/`common::flags` | ported | `bitfield_flag!` macro + `TorrentFlags` (u64, bits 0-25, `DEFAULT_FLAGS`), `PexFlags` (u8), `AnnounceFlags` (u8) |
| `index_range.hpp` | `common::index_range` | ported | `IndexRange<I: IndexStep>` `IntoIterator`; `IndexStep` for `PieceIndex`/`FileIndex`/`i32` |
| `timestamp_history.cpp` + `compare_less_wrap` (`utp_stream.cpp`) | `common::timestamp_history` | ported | 20-slot ring, 120-sample step gate, wrap-aware comparisons; upstream test + step/adjust coverage |
| `random.cpp`, `dev_random.hpp` | `common::random` | ported | `random(m)` inclusive `[0,m]`, `random_bytes`, `crypto_random_bytes`, `random_shuffle` on `rand` |
| `hex.cpp` | `common::hex` | ported | to/from hex, validation; upstream messages |
| `crc32c.cpp`, `cpuid.cpp` | `common::crc32c` | ported | `crc32c` crate, no CPUID needed; `crc32c_32`/`crc32c_words` (LE memory order); RFC 3720 B.4 vectors pass |
| `sliding_average.hpp`, `stat.cpp` | `common::sliding_average`/`common::stat` | ported | `SlidingAverage<G>` (fixed-point ×64, clamped, deviation) — all 8 `test_sliding_average.cpp` cases pass; `StatChannel`+`Stat` (6 channels, SYN/IP-overhead formulas) |
| `stat_cache.cpp` | `disk::stat_cache` | **ported** | sentinel-encoded errors, poison-tolerant |
| `bloom_filter.cpp` | `common::bloom_filter` | ported | `BloomFilter<N>` + free `set_bits`/`has_bits`/`count_zero_bits`; `test_bloom_filter.cpp` cases pass |
| `assert.cpp`, `invariant_check.hpp` | — | won't port | → `debug_assert!` |
| `ffs.cpp` (`aux_/ffs.hpp`) | `common::ffs` | **ported** | `log2p1`, `count_leading_zeros`, `count_trailing_ones` — hw/sw split collapses to `u32::leading_zeros`/`trailing_zeros` (same results) |
| `version.cpp` (`version.hpp`) | `common::version` | **ported** | reports `BitDaemon/<crate version>` (deviation: identifies our impl) |
| `numeric_cast.hpp`, `noexcept_movable.hpp`, `copy_ptr.hpp`, `unique_ptr.hpp`, `alloca.hpp`, `array.hpp`, `container_wrapper.hpp`, `deque.hpp`, `vector.hpp`, `vector_utils.hpp`, `heterogeneous_queue.hpp`, `tailqueue.hpp`, `link.hpp`, `pool.hpp`, `stack_allocator.hpp`, `string_ptr.hpp`, `strview_less.hpp`, `byteswap.hpp`, `range.hpp`, `scope_end.hpp`, `throw.hpp`, `set_socket_buffer.hpp`, `keepalive.hpp`, `io.hpp`, `io_bytes.hpp`, `visit_block_iovecs.hpp` | idiomatic Rust | won't port | STL/boost shims — use std equivalents |

## bitdaemon-bencode

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `bdecode.cpp` (`bdecode.hpp`) | `bencode::decode` + `bencode::node` | ported | 8-byte packed tokens (`offset:29|kind:3`/`next:29|hdr:3`), lazy `int_value`, back-patched partial trees, soft errors, error positions — 60+ cases from `test_bdecode.cpp` pass |
| `entry.cpp` (`entry.hpp`) | `bencode::entry` | ported | `Entry` owned enum; dict = `BTreeMap<Vec<u8>,_>` = upstream sorted `map` |
| `bencode.hpp`, `aux_::bencoder.hpp` | `bencode::encode` | ported | canonical dict order; `Preformatted` verbatim; `Undefined`→`0:` |
| `print_entry` (in `bdecode.cpp`/`entry.cpp`) | `bencode::print` | ported | `print_node`/`print_entry`, upstream separators + 200-char one-liner rule |
| `gzip.cpp`, `puff.cpp` | `net::http_conn` | ported | `flate2` gzip encode/decode wired into `http_conn` (`Accept-Encoding: gzip` sent, `Content-Encoding` inflated); puff is C zlib-inflate — won't port |
| `xml_parse.cpp`, `xml_parse.hpp` | `core::xml_parse` | ported | byte-cursor scanner, same token sequence + error strings; `XmlToken` enum replaces the `xml_*` constants |

## bitdaemon-core

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `torrent_info.cpp` (`torrent_info.hpp`) | `core::torrent_info` | **ported** | v1+v2+hybrid; v2 piece-layer merkle validation; byte-oriented names |
| `file_storage.cpp` (`file_storage.hpp`), `file_layout.hpp` | `core::file_storage` | **ported** | byte-oriented paths (upstream `std::string` semantics); `test_file_storage.cpp` 28/28 |
| `create_torrent.cpp` (`create_torrent.hpp`) | `core::create_torrent` | **ported** | canonicalize + v1/v2/hybrid generate; `set_piece_hashes`; `test_create_torrent.cpp` 8/8 |
| `merkle.cpp` (`merkle.hpp`), `aux_/merkle_tree.hpp` + `merkle_tree.cpp` | `core::merkle`, `core::merkle_tree` | **ported** | full sparse/piece-layer tree, proofs, verified-bitfield; `test_merkle_tree.cpp` 22/22 |
| `magnet_uri.cpp` (`magnet_uri.hpp`) | `core::magnet_uri` | **ported** | `test_magnet.cpp` parse vectors 24/24 |
| `load_torrent.cpp` (`load_torrent.hpp`) | `core::torrent_info` (`load_torrent*`) | **ported** | `parse_torrent_file`/`load_torrent_buffer` |
| `announce_entry.cpp`, `tracker_list.cpp` | `session::tracker_list` | **ported** | `AnnounceInfohash`/`Endpoint`/`Entry`/`TrackerList` — quadratic backoff `5+f²·5·ratio/100` ≤ 60 min verbatim, `can_announce` (due/min/fail_limit), tier-order insert + dedup, `due()` failover |
| `web_seed_entry.cpp` (`web_seed_entry.hpp`) | `session::torrent` + `net::web_seed` | **ported** | url_seeds (BEP-19) + http_seeds (BEP-17) endpoint lists, round-robin cursor, `add_url_seed`/`remove_*` handle ops; `WebSeedEntry` typed variants folded into endpoint strings + `net::web_seed` request builder |
| `fingerprint.cpp`, `identify_client.cpp` | `core::fingerprint` | **ported** | az/shadow/mainline/TR-legacy styles + name map; `test_identify_client.cpp` 2/2 |
| `read_resume_data.cpp`, `write_resume_data.cpp` | `core::resume_data` | **ported** | file-version 1+2 read, v2 write; `test_read_resume.cpp` vectors + round-trip |
| `resolve_duplicate_filenames.cpp` | `core::resolve` | **ported** | case-insensitive `.N` renames, dir/pad rules; upstream table 12/12 (Windows sep-safe) |
| `apply_pad_files.hpp`, `resolve_duplicate_filenames.hpp` | `core::pad` | **ported** | folded into `resolve` |
| `resolve_links.cpp` (`resolve_links.hpp`) | `core::resolve_links` | **ported** | fixture table 14/14 (pad/align + v1 piece-hash + v2 root matching) |
| `add_torrent_params.cpp` (`add_torrent_params.hpp`) | `core::add_torrent_params` | **ported** | single canonical struct used by torrent/magnet/resume |
| `path.cpp` (string helpers), `aux_/path.hpp` | `core::path` | **ported** (string half) | `directory.cpp`/`drive_info.cpp` (fs-dependent) → Phase 3 |
| `entry`-adjacent `client_data.hpp`, `fwd.hpp`, `version.cpp` | `common::{version, client_data?}` | ported / won't port | `version.cpp` ported (`common::version`); `client_data.hpp` = `std::any` wrapper — idiomatic Rust uses `Any` directly; `fwd.hpp` is a C++ forward-decl header — no Rust equivalent |
| `session_types.hpp`, `session_status.hpp`, `storage_defs.hpp`, `torrent_status.cpp`, `download_priority.hpp` | `core::types` + `session::status` | **ported** (subset) | `DownloadPriority` in common; `TorrentStatus`/`TorrentState` snapshot (state, counters, scrape bests, rates, pieces, distributed_copies) + `post_torrent_updates`/`state_update_alert` |
| `escape_string.cpp`, `utf8.cpp`, `parse_url.cpp` (subset) | `core::escape_string`, `core::utf8` | **ported** (subset) | `socket_io`/`string_util`/`ip_helpers` land with net |

## bitdaemon-disk

> **Fidelity is the point here** (ADR-0009): this subsystem is the
> primary motivation for the port. Do not simplify away the job queue,
> fences, bounded buffer pool, lazy file pool or part-file — they are
> the fixes for real production failures. Deferred-error semantics
> (lazy open → error at first I/O, as alert) are upstream-specified
> behavior, not a bug.

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `disk_interface.hpp`, `disk_job.hpp`, `disk_job_fence.cpp`, `disk_job_pool.cpp` | `disk::disk_interface`, `disk::disk_job`, `disk::disk_job_fence`, `disk::disk_pool` | **ported** | full `DiskIo` trait + `StorageHolder`; boxed jobs (13 kinds); fence verbatim; pool slab → `Box` + counters |
| `disk_io_thread_pool.cpp`, `back_pressure.cpp`, `disk_completed_queue.hpp` | `disk::disk_pool`, `disk::back_pressure`, `disk::disk_completed` | **ported** | on-demand spawn, condvar wait, `try_thread_exit` shrink, injected idle-reaper; watermarks `max*7/8`/`max*3/4` verbatim; completion queue posts single in-flight drain |
| `storage_constructor` / `default_storage_constructor` (in `disk_interface.hpp`/`storage.hpp`) | `disk::StorageFactory` (`storage.rs`) | **ported** | **injectable storage** (ADR-0008) — `StorageFactory = Arc<dyn Fn(&StorageParams) -> Arc<dyn DiskStorage>>`; OnionBit `PrivateStorageFactory` parity |
| `disk_buffer_pool.cpp`, `disk_buffer_holder.cpp` | `disk::disk_buffer_pool` | **ported** | `Box<[u8]>` + RAII holder → allocator on drop; `in_use` + per-category histogram; `bulk_free_buffer` batch free |
| `disabled_disk_io.cpp` | `disk::disk_interface::DisabledDiskIo` | **ported** | synchronous no-op completions |
| `mmap.cpp`, `mmap_disk_io.cpp`, `mmap_storage.cpp`, `mmap_disk_job.hpp`, `file_view_pool.cpp` | `bitdaemon-mmap` | **ported** | `FileViewPool` (LRU view cache, size mismatch → remap) + `MmapStorage` (`DiskStorage` over `memmap2` views, part-file/pad/priority parity with `PreadStorage`) + `mmap_storage_constructor` factory; sole workspace `unsafe` carve-out (ADR-0010) |
| `posix_storage.cpp`, `posix_part_file.cpp` | `disk::storage` (shared) | merged | part file + storage identical to pread variant |
| `posix_disk_io.cpp` (`posix_disk_io.hpp`) | `disk::disk_io` | ported | folded into `PreadDiskIo` — upstream's posix/mmap trio differ only in the storage backend; `storage_free_list` slot reuse now wired into `new_torrent`/`remove_torrent` |
| `pread_disk_io.cpp` | `disk::disk_io` | **verified** | full `DiskIo` impl; `StorageFactory` injection point (ADR-0008 — downstream private/encrypted storage); `async_write` inserts into `DiskCache` (upstream `store_buffer.insert` semantic), coalesced `Flush` jobs write contiguous block runs (one syscall per run instead of one per 16 KiB block), `read`/`hash`/`hash2` overlay cached dirty blocks before touching disk (ADR-0012/0014), fences force-flush the piece cache first, `cache_expiry` flushes stale incomplete pieces (ADR-0013), v2 block SHA-256 computed worker-side at hash time (ADR-0016); `write_blocks` level + `job_done`/`check_buffer_level`, `max_queued_disk_bytes`→`set_max_size`, `call_job_handlers` drain; storage behind `RwLock`; validated on a real swarm at >100 MiB/s with bounded queue |
| `storage_utils.cpp` | `disk::storage_utils` | **ported** | `initialize_storage`, `move_storage`, `delete_files`, `rename_file`, `verify_resume_data`, `has_any_file` |
| `part_file.cpp` (`part_file.hpp`) | `disk::part_file` | **ported** | header format + slot reuse + export frees full pieces |
| `file.cpp`/`file.hpp`, `open_mode.hpp`, `file_pool_impl.cpp`/`file_pool.hpp` | `disk::file`, `disk::open_mode`, `disk::file_pool` | **ported** | `pread_all`/`pwrite_all`/`pwritev_all`; pool: LRU evict, open-waiter sharing, r/w upgrade race counter, ENOENT-mkdir retry, deferred close |
| `file_progress.cpp` | `disk::file_progress` | **ported** | `init` takes the have-pieces `Bitfield` (picker dep swapped — documented deviation); `completed_cb` on file finish |
| `hash_picker.cpp` (`hash_picker.hpp`) | `disk::hash_picker` | **ported** | single-request `pick_hashes` API; both request shapes, 3 s retry interval, `test_hash_picker.cpp` 10/10 |
| `precomputed_block_hashes.cpp` | `disk::precomputed_block_hashes` | **ported** | all-zero sentinel, `take`/`take_block`/`drop` |
| `stat_cache.cpp` (disk side) | `disk::stat_cache` | **ported** | — |
| `copy_file.cpp`, `drive_info.cpp`, `directory.cpp` | `disk::fs` | **ported** | hard-link-first copy; `free_space` via fs2; `truncate.cpp` folds into `set_len` |
| `disk_observer.hpp`, `disk_completed_queue.hpp` | `disk::back_pressure` + `disk::disk_completed` | **ported** | `DiskObserver` trait + coalesced completion queue (`m_job_completions_in_flight` → single in-flight drain), `has_back_pressure`/`tell` wiring |
| `pread_storage.cpp` (`aux_/pread_storage.hpp`), `pread_disk_job.hpp` | `disk::storage` | **ported** | covered by `storage.rs` (`//! Port of pread_storage`); row added for ledger completeness |
| `disk_cache.cpp` + `aux_/disk_cache.hpp` | `disk::disk_cache` + `disk::store_buffer` | **verified** | `CachedBlockEntry`/`CachedPieceEntry` + `flushed_cursor`/`hasher_cursor`/`hash_returned`, `insert` flags (need_hasher_kick/exceeded_limit), `try_clear_piece` pending-write refusal, `free_piece`, `compute_force_flush`/`compute_flushed_cursor`/`count_jobs`; **wired into `PreadDiskIo`** — per-storage cache absorbs `async_write`, `take_flush_run` produces contiguous `Flush` jobs, `overlay`/`overlay_hash` serve reads and hashes from dirty data; the concurrent hasher queue is folded into the disk thread pool |
| `storage_array.hpp`, `storage_free_list.hpp` | `disk::storage_free_list` | ported | free-list reuse semantics verbatim (reserve-in-new_index noexcept add); `storage_array` = `Vec`-indexed storage set — folded into `Storage` backends |
| `file_descriptor.hpp`, `file_pointer.hpp` | — | won't port | fd/FILE* RAII shims → `std::fs::File` |
| `win_file_handle.hpp`, `windows.hpp`, `win_util.hpp` | — | won't port | platform shims → `std::os` |

## bitdaemon-net

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `socket_io.cpp`/`socket_io.hpp` | `net::socket_io` | **ported** | BE int helpers, endpoint bytes (6/18 B), `read_endpoint_list` |
| `ip_filter.cpp` | `net::ip_filter` | **ported** | `BTreeMap` set-surgery verbatim; `test_ip_filter.cpp` 9/9 incl. overlap joins + v6 + port_filter |
| `pe_crypto.cpp` | `net::pe_crypto` | **ported** | DH-768 (`num-bigint` powmod, degenerate-key guard), RC4 RFC 6229 vectors exact, `keyA/keyB` longkey derivation, 1024-byte discard |
| `bt_peer_connection` packet layer | `net::peer_wire` | **ported** | handshake (68 B, reserved bits verbatim) + all 24 message ids, tokio codec, max-len cap; conn state machine lives in `session::peer_conn` (see peer_connection row) |
| `peer_connection.cpp` (`peer_connection.hpp`), `peer_connection_handle.cpp`, `peer_connection_interface.hpp` | `net::peer` (`session::peer_conn`) | **verified** | `snub_peer` (unsent-queue wipe + `timed_out`/`abort_download` on last eligible block when no free blocks remain), `reqq` caps `max_out_request_queue`, `m_requested`/RTT bookkeeping, AIMD `desired_queue_size` + slow-start exit; `PeerConn` full incoming dispatch → `PeerEffect`s, upload-queue validation, picker drive, super-seeding (`superseed_piece`, rarest-candidate selection on `Have`), initial `Bitfield`/per-piece `Have` on connect, 60 s idle keepalive in the engine writer, `graceful_pause` flag (requests stop, in-flight drains before lift) |
| `bt_peer_connection.cpp` (`bt_peer_connection.hpp`) | `net::bt_peer_conn` + `net::mse` | **ported** (crypto) | `run_handshake` (68B write+validate, reserved bits, info_hash check), `send`/`read_extended_handshake`, `read`/`write_message` pump over injected `BoxedPeerStream`; MSE PE1–PE4 (`mse_initiator`/`mse_responder`: synchash scan over PadA, skey `req2^req3` field, RC4 cryptofield, `crypto_select`, IA) + `read`/`write_message_rc4` negotiated path |
| `web_connection_base.cpp`, `web_peer_connection.cpp` | `net::web_seed` | **ported** | `build_piece_request` (BEP-19 GET + Range verbatim, multi-file `map_block`, pad-skip), `fetch_block` pump on `http_conn` over injected `BoxedPeerStream`; session `WebSeedFetch` effects + `on_web_seed_block`→`DiskWrite` |
| `request_blocks.cpp`, `merge_block_requests.hpp`, `suggest_piece.hpp`, `peer_list.cpp`, `peer_info.cpp`, `peer.cpp`, `peer.hpp` | `session::{request_blocks, peer_pipeline, peer_info}`, `net::peer_list` | ported | request merge/picking context, `PeerListEntry`/`PeerInfo` snapshots, `PeerList` with peer-class accounting; `peer_connection_handle` = actor commands (ADR-0001) |
| `receive_buffer.cpp` + `crypto_receive_buffer` | `net::receive_buffer` | **ported** | cursor buffer verbatim (start/pos/end, `reserve`/`grow`/`cut`/`normalize` + watermark shrink); `test_receive_buffer.cpp` 15/15 |
| `packet_buffer.cpp` | `net::packet_buffer` | **ported** | 16-bit wrapping circular store, window grow/shift verbatim (`compare_less_wrap`), `test_packet_buffer.cpp` 6/6; used by uTP (Phase 5) |
| `torrent_peer.cpp` + `aux_/torrent_peer.hpp` | `net::torrent_peer` | **ported** | `PeerAddr` enum replaces the ipv4/ipv6/i2p subclass hierarchy; packed bitfield → named fields; `peer_priority` BEP-40 verbatim; `test_peer_priority.cpp` vectors exact incl. v6 prefix masks |
| `socket_type.cpp`, `socket.hpp`, `polymorphic_socket.hpp`, `instantiate_connection.cpp`, `union_endpoint.hpp` | `net::socket` + `net::transport` | **ported** | `PeerStream`/`PeerConnector`/`PeerAcceptor` traits (ADR-0008 — `socket_type` variant becomes trait objects); `instantiate` decision table (uTP→SSL→i2p→proxy); `union_endpoint` = `SocketAddr`-only endpoints (we never need the C++ variant — tracker/http fields handled at call sites) |
| `socks5_stream.cpp`, `http_stream.hpp`, `ssl.cpp`, `ssl_stream.hpp`, `i2p_stream.cpp`, `rtc_stream.cpp`, `rtc_signaling.cpp`, `websocket_stream.cpp` | `net::{proxy, i2p, websocket}` | **ported** (partial) | `SocksConnector` (SOCKS5 CONNECT over injected connector), `i2p` SAM handshake + `I2pConnector`/`I2pAcceptor`, `websocket` RFC-6455 upgrade+frames (vector RFC testé); TLS via rustls (`ssl_stream`: tracker = OS root-store; `SniResolver` per-torrent PEM server config; `is_ssl_torrent` from embedded `ssl-cert` in `.torrent`, `set_ssl_certificate` cert+key; SSL dial wraps stream — strict mutual TLS ported — `TorrentPeerVerifier` verifies the peer chain against the torrent's embedded `ssl-cert` root via webpki and the leaf DNS SAN/CN against the torrent name or `"*"` (`torrent::verify_peer_cert`); `TorrentClientVerifier` requires client certs on the accept side (`verify_fail_if_no_peer_cert`); **verified live** vs libtorrent v2.1.2+OpenSSL in both directions (seed/leech, SHA-256 identical, rogue-cert rejection confirmed); `rtc_stream` ported as `RtcStream`/`RtcChannel` + `rtc_parse_endpoint`; `rtc_signaling` ported as `net::rtc_signaling` — full state machine (offer batches, `max_webtorrent_offers` cap, dedup, timeout) over injected `RtcPeerConnection`/`RtcPeerConnectionFactory`; concrete WebRTC stack binding is an optional leaf crate (ADR-0011) |
| `socks5_stream.cpp`/`http_stream.hpp` | `net::socks`, `net::http_stream` | **ported** | SOCKS4/5 + HTTP CONNECT handshakes as async fns over `AsyncRead+AsyncWrite` — same wire bytes, stream-agnostic (tunnel-friendly); byte-wise header scan verbatim |
| `udp_tracker_connection.cpp` + `http_tracker_connection.cpp` (wire) | `net::udp_tracker`, `net::http_tracker` | **ported** | BEP-15 connect→announce→scrape over `DatagramSocket` (tid matching, v4/v6 stride); HTTP announce query + response parse (compact/dict peers, scrape files, failure/warning); scrape scheduling `Session::scrape_tracker`/`on_scrape_response` + engine `spawn_scrape` (transports injectés) |
| BEP-10 extended handshake (half of `bt_peer_connection.cpp`) | `net::bep10` | **ported** | `ExtendedHandshake` build/parse (`m`, `p`, `v`, `yourip`, `reqq`, `complete_ago`, `upload_only`, `share_mode`, `metadata_size`, `ipv4/6`), `[len][0x14][0x00]` framing | SOCKS4/5 + HTTP CONNECT handshakes as async fns over `AsyncRead+AsyncWrite` — same wire bytes, stream-agnostic (tunnel-friendly); byte-wise header scan verbatim |
| `chained_buffer.cpp` (send-buffer) | `net::chained_buffer` | **ported** | `Box<dyn BufferHolder>` replaces type-erased holder + destruct/move thunks; slack-append, pop_front partial/full, iovec build |
| `http_connection.cpp`, `http_parser.cpp`, `parse_url.cpp`, `resolver.cpp`, `resolver_interface.hpp` | `net::http_parser`, `net::parse_url` | ported | incremental `HttpParser` (status/request line, header multimap, content-length/range, chunked tracking + tail headers, `parse_chunk_header` verbatim) + `parse_url_components`/`split_url`/`same_origin`/`is_valid_tracker_url`/`is_idna`; `http_connection` (+ `Accept-Encoding: gzip` émis, `Content-Encoding: gzip` décodé) + `CachingResolver` ported (700-entry cap, 1200s TTL, fail-TTL=timeout/8, cache_only, single-flight, raw-IP short-circuit) |
| `http_tracker_connection.cpp`, `udp_tracker_connection.cpp`, `websocket_tracker_connection.cpp`, `tracker_manager.cpp`, `tracker_list.cpp` (net side) | `net::{http_tracker,udp_tracker,ws_tracker}` + `session::tracker_list` | ported | announce retry/failover tiers, scrape (HTTP+UDP BEP-15), ws-signaling JSON; all via injected transports |
| `upnp.cpp`, `natpmp.cpp`, `portmap` | `net::upnp` + `net::natpmp` | **ported** | UPnP SOAP bodies verbatim + device parse + `UpnpDriver` map/delete/renew state machine; **SSDP wired** (ADR-0018): `msearch_request` burst on `start_upnp`, `parse_ssdp_location` in the UDP demux, `LOCATION` fetch via injected connector/resolver, `resolve_control_url`, `stop_upnp` deletes mappings; NAT-PMP codec + `NatpmpState` lifecycle + **`start_natpmp_auto` gateway discovery** via `enum_net::default_gateway_v4`/`netroute` (ADR-0019) |
| `lsd.cpp` (`lsd.hpp`) | `net::lsd` | **ported** | `render_lsd_packet` byte-exact (`BT-SEARCH * HTTP/1.1`, multicast `239.192.152.143`/`[ff15::efc0:988f]:6771`), `process_packet` parse + cookie self-filter + v2 truncated infohashes; session emit tick + inbound→`peer_list` on injected `DatagramSocket` |
| `udp_socket.cpp`, `session_udp_sockets.hpp`, `listen_socket_handle.cpp` | `net::socks_udp` + `net::os_transports` + `session::listen` | **ported** | `socks5_wrap`/`socks5_wrap_hostname`/`socks5_unwrap` (RSV/FRAG/ATYP verbatim, FRAG!=0 rejected); `Socks5UdpSocket` — full UDP-associate relay (`DatagramSocket` impl, control TCP held open, `BND` address, RFC-1928 header wrap/unwrap; mock end-to-end test); **session wiring** — `Engine::apply_udp_proxy` rebuilds the datagram lane through the relay when `proxies.dht` is socks5 (ADR-0020); `UdpSocket` wrapper over `DatagramSocket` (io::Result propagation, SOCKS integration); `Session::listen_sockets` + `SessionUdpSocket` vec, `listen_socket_handles`, per-acceptor accept pump, `reopen_network_sockets`/`use_interfaces`/`listen_on` |
| `utp_stream.cpp`, `utp_socket_manager.cpp` | `net::utp`, `net::utp_socket`, `net::utp_stream` | **verified** | `utp_socket_impl` full state machine: SYN/FIN/RESET, seq/ack reorder buffers (`PacketBuffer`), SACK ext, LEDBAT `do_ledbat` + slow-start/ssthres, MTU probing, dup-ack fast-resend, `tick` timeouts/backoff. **Adaptation**: sends drain through `outbox` → injected `DatagramSocket` (ADR-0008); callbacks → pollable `state()`/`error()`/`read()`; engine wiring: `Engine::enable_utp` prefers uTP on dials with TCP fallback, incoming acceptor + direct pump routing (1 Hz tick routing was the live-test stall); `write_all` waker fix. **Live-verified vs libtorrent v2.1.2: full 8 MiB transfer over uTP, hash identical** |
| `ip_filter.cpp`, `ip_voter.cpp`, `ip_notifier.cpp`, `enum_net.cpp`, `netlink.cpp`, `route.h`, `bind_to_device.hpp` | `net::iface` + `net::enum_net` | **ported** | `ip_filter` + `ip_helpers` (is_local/is_teredo/hash_address, CGNAT 100.64/10 + ULA fc00::/7) + `ip_voter` (bloom-keyed majority voting, 50-vote/5-min rotation, `num_votes*2/3` rule); `enum_net` + `IpNotifier` (if-addrs polling-diff) → `SessionOut::InterfacesChanged` on tick; `netlink`/`route.h`/`bind_to_device` folded in (platform detail → `if-addrs`) |
| `bandwidth_limit.cpp`, `bandwidth_manager.cpp`, `bandwidth_queue_entry.cpp`, `bandwidth_socket.hpp` | `net::bandwidth` | **ported** | `BandwidthChannel` token bucket (3 s burst cap, `inf`=INT_MAX), `BwRequest` (priority pro-rata `distribute*prio/tmp`, ttl 20), `BandwidthManager` queue + disconnect quota return; `Rc<BandwidthChannel>`/`Rc<dyn BandwidthSocket>` replace raw ptr/`shared_ptr` |
| `proxy_base.cpp`, `proxy_settings.cpp` | `net::proxy` | ported | `ProxySettings` + `SocksConnector` over injected connector |
| `extensions.hpp`, `i2p_pex.cpp` (ext part), `ut_pex` protocol bits | `session::{ut_metadata,ut_pex,i2p_pex}` + `net::peer_wire` ext ids | ported | BEP-10 handshake, ut_metadata magnet glue, ut_pex add/drop diff; i2p_pex ported (id 9, 32-byte dest hashes, 6-slot 60 s flood window, first-send full list) |
| `identify_client.cpp` (net side) | `common::identify_client` | ported | peer-id client identification |
| `peer_class.cpp`, `peer_class.hpp`, `aux_/peer_class_set.*`, `peer_class_type_filter.hpp` | `net::peer_class` | **ported** | pool refcount + free-list reuse, 15-slot `PeerClassSet` w/ swap-remove, limit/priority clamps verbatim, `PeerClassTypeFilter` 6 socket types |
| `kademlia`-indep. `socket_io.cpp`, `ip_helpers.cpp` | `net::{ip_helpers,enum_net}` | ported | is_local/in_net/is_global classification; iface enum injected |
| `win_cng.hpp`, `win_crypto_provider.hpp` | — | won't port | → RustCrypto |
| `route.h`, `netlink_utils.hpp`, `platform_util.cpp` | `net::enum_net` + `common::platform` | ported | `max_open_files`/`set_thread_name` via procfs (unsafe-free); `netlink`/`route` superseded by `if-addrs` enum + `IpNotifier` polling-diff |
| `string_util.cpp` (`aux_/string_util.hpp`) | `net::string_util` | **ported** | char classes, no-case compares, `strip/ltrim`, `search`, `split_string_quotes`, `parse_listen_interfaces` (+`print_`), comma-separated lists, `format_host_{for_connect,header}`; `split_string`/`parse_decimal`/`url_random` live in core/common |
| `hasher.cpp`, `hasher.hpp`, `aux_/hasher512.hpp`, `aux_/sha512.hpp`, `sha1.hpp`, `sha256.hpp` | RustCrypto (`sha1`, `sha2`) | won't port | C wrappers → crates (ADR-0007) |
| `units.hpp` (`peer_index_t`, `file_index_t`, `piece_index_t`…) | `common::types` | **ported** | typed unit newtypes |
| `tracker_event.hpp` | `net::tracker_client` | **ported** | AnnounceRequest event enum (none/started/stopped/completed/paused) — wire encoding verified vs BEP-3/15 |
| `config.hpp`, `io_context.hpp`, `io_service.hpp`, `span.hpp`, `string_view.hpp` | — | won't port | → `tokio`/`std` equivalents |
| `aux_/deadline_timer.hpp`, `allocating_handler.hpp`, `deferred_handler.hpp` | — | won't port | → `tokio::time` / handler wrappers unneeded |
| `deprecated.hpp`, `disable_warnings_*.hpp`, `export.hpp`, `debug.hpp`, `debug_disk_thread.hpp`, `set_traffic_class.hpp` | — | won't port | warning/export/debug shims |

## bitdaemon-dht (src/kademlia/*, include/libtorrent/kademlia/*)

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `node.cpp` (`node.hpp`) | `dht::dht_node` | **ported** | query dispatch ping/find_node/get_peers/announce/get/put/sample_infohashes, write tokens (4B sha1), BEP-42/44 verify, lookup pump — outbox injecté (ADR-0008) |
| `routing_table.cpp` (+`node_entry.cpp`, `node_id.cpp`) | `dht::routing_table`, `dht::node_entry`, `dht::node_id` | **ported** | BEP-42 vectors verbatim (5/5); k-bucket split (extended `{16,8,4,2}`× limits), `compare_ip_cidr` /24+//64 dedup, `restrict_routing_ips`, `classify_prefix` spread-replacement, `find_node` k-closest, `next_refresh`, `update_node_id` |
| `rpc_manager.cpp` | `dht::rpc` | **verified** (live) | tid dispatch, short/final timeout tick, ro; `want` only on cross-family queries like upstream — a v4 node must never send `want: n6` (bug found by live bootstrap: routers answered `nodes6`, useless to the v4 table) |
| `traversal_algorithm.cpp` + `observer.hpp` | `dht::traversal` | ported | sorted results, dedup, /24 diversity, branch factor, short-timeout |
| `find_data.cpp`, `get_peers.cpp`, `get_item.cpp`, `put_data.cpp`, `sample_infohashes.cpp` | `dht::lookups` + `dht::dht_node` | **ported** | per-kind query shapes, obfuscated get_peers, reply extraction, write phase announce/put |
| `dht_settings.cpp`, `dht_state.cpp`, `dht_storage.cpp`, `msg.cpp`, `item.cpp`, `ed25519.cpp`, `dos_blocker.cpp` | `dht::{dht_settings, dht_state, dht_storage, msg, item, ed25519, dos_blocker}` | **ported** | storage: announce/get_peers (scrape blooms, write-token gate), BEP-44 items (canonical_string verbatim, ed25519-dalek per ADR-0007, seq/sig/k/salt), BEP-51 sample_infohashes, tick expiry; `verify_message` schema walker; `dos_blocker` 10s-window ban — instantiated in `DhtNode::incoming`, pre-bdecode gate (ADR-0021); `dht_tracker` pending |
| `dht_tracker.cpp` | `dht::dht_tracker` | ported | v4/v6 mux, secret rotation, injected transport | session glue |
| (see `dht_storage` row) | — | merged | |
| `node_id.cpp`, `node_entry.cpp` | `dht::node_id`, `dht::node_entry` | **ported** | BEP-42 verify/generate, secret-id signature, distance/compare_ref/prefix masks; rtt EMA (2/3+1/3), fail-count semantics |
| `msg.cpp`, `item.cpp`, `ed25519.cpp` | `dht::msg`, `dht::item`, `dht::ed25519` | **ported** | schema-driven `verify_message` (key_desc_t flags verbatim); canonical_string + ed25519 sign/verify (dalek, ADR-0007); observer/direct_request pending |
| `ed25519.cpp` + `src/ed25519/*` (20 files) | `ed25519-dalek` | won't port | RustCrypto crate (ADR-0007) |
| `types.hpp`, `announce_flags.hpp` | `dht::{item, node_id, msg}` | ported | public_key/secret_key/signature/sequence_number live in `item.rs`; announce flags in `msg`/`lookups` |
| `refresh.cpp` (`kademlia/refresh.hpp`), `direct_request.hpp` | `dht::{lookups, dht_node}` | ported | `LookupKind::Bootstrap` = get_peers toward live node id, `a.bs=1` on initial seeds, done() pings unqueried results (`add_node`); `direct_request` → `DhtEvent::Direct` via reserved tag bit 63; `next_refresh` bucket hints in routing table |

## bitdaemon-session

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `session.cpp`, `session_impl.cpp` (`session_impl.hpp`), `session_call.cpp`, `session_handle.cpp` (`session_handle.hpp`), `session_interface.hpp`, `session_params.cpp`, `session_types.hpp` | `session::session` + `session::session_handle` | **ported** | `Session` core (torrents, dispatch, tick, dht outbox, **DiskIo job bridge** — `DiskRead/DiskWrite` → `async_read`/`async_write`, `call_job_handlers` + completions channel → `Piece`/`async_hash`/`piece_verified`; `auto_manage_scaler` + upstream `manage_queue` paused semantics; `apply_settings` hot-applies `alert_queue_size`/`max_queued_disk_bytes`; `disk_back_pressure` request gate) + `SessionHandle`/`TorrentHandle`, `SessionState` `.session` file + `session_params` (`write_session_params`/`read_session_params` upstream dict layout, sparse settings, ip_filter4/6, dht state; byte-exact vs v2.1.2 golden fixtures). Handle surface ≈ complete: pause/resume/is_paused, get_torrents, get_settings, alert mask/notify/wait_for_alert, ip/port filters, peer-class CRUD + filters, peer_id/key, per-purpose proxies, `PeSettings`, rate-limit/connection setters, `start_*`/`stop_*` service switches (`ServiceSwitches`), DHT ops (get_peers/get_item/sample/live_nodes/state/set_state/put_item via pending-put, post_dht_stats alert, set_dht_storage), listen_on/use_interfaces/ssl_listen_port/reopen_network_sockets, session_stats_metrics, num_connections/num_uploads, `dht_stats_alert`. Left: `set_load_function` (won't port — dynamic-lib loading), local_*_rate_limit (deprecated upstream) |
| `torrent.cpp` (`torrent.hpp`, `torrent_impl.hpp`), `torrent_handle.cpp`, `torrent_status.cpp`, `torrent_list.hpp`, `torrent_peer.cpp`, `torrent_peer_allocator.cpp` | `session::torrent` + `session::session_handle` | **ported** | `Torrent` per-swarm state: peer registry, picker refcount glue, `piece_verified` → have broadcast + seed + `stop_when_ready`; `TorrentHandle` ~110 methods (flags, limits incl. per-peer, http_seeds, ssl_cert(+buffer), tracker_login, peer_list_entry, add_extension, write_resume_data, `get_resume_data`, `set_sequential_range`, `get_renamed_files`, `write_torrent_file` (verbatim `info_section` bytes + live announce-list → info-hash-stable `.torrent`), `post_*` alert helpers, piece_layers, queue ops, reannounce by idx/url, `file_priority`, `post_status`); `PeerListEntry` snapshot; announce gates (`announce_dht/lsd/trackers`, `announcing`), `disconnect_all`/`abort`, `attach_peer`, `choke_peer`/`unchoke_peer`, `num_connecting`, `need_picker`, `peer_is_interesting`, `ready_for_connections`, `want_tick`, `is_aborted`/`is_upload_only`/`is_ssl_torrent`/`is_predictive_piece`, suggest pieces, `predictive` bitfield, `initialize_merkle_trees`, `recalc_share_mode`, `request_time_critical_pieces`, `enable_all_trackers`, `announce_with_tracker` (next_announce reset), `set_state`, `handle_disk_error`, `set_error`, `on_torrent_paused`, `retry_web_seed`, `maybe_connect_web_seeds`, `update_want_peers`; `session::peer_alloc` — `TorrentPeerAllocator` as accounting/limits (slab pool = won't port memory detail). Left: `check_invariant` (debug scaffolding — won't port), `local_*_rate_limit`/`set_load_function` (deprecated upstream) |
| `alert.cpp`, `alert_manager.cpp`, `alert_types.hpp`, `alert.hpp` | `session::alert` | **ported** | `Alert` enum (essential payload variants), `AlertCategory` bitmask verbatim, `AlertManager` (mask filter, queue limit + drop counter, notify hook on empty→non-empty), `SessionPlugin` extension trait |
| `settings_pack.cpp`, `session_settings.cpp`, `session_stats.cpp`, `performance_counters.cpp` | `session::settings` + `session::counters` | **ported** | `SessionSettings` typed struct (upstream defaults), `SessionParams` injected disk/plugins; `Counters` atomic bank + `SessionStats` snapshot wired into Session |
| `add_torrent_params.cpp` (session side) | `session::add` (`Session::add_torrent_params`, `Torrent::from_params`) | ported | resume data apply (picker/bitfield/priorities/trackers/seeds/dht_nodes) |
| `choker.cpp`, `peer_class.cpp`, `peer_class_set.cpp`, `peer_class_type_filter.hpp` | `session::choker` | **ported** | `unchoke_sort` verbatim (fixed-slots + rate-based + RR/fastest/anti-leech comparators, quota `piece_length·quota` + 60s, `nth_element` top-slots), `ChokePeer` value snapshot; `peer_class*` ported (see row above) |
| `peer_connection.hpp` (machine d'état) | `session::peer_conn` | **ported** | bitfield/have/dont-have/suggest/allowed-fast, choke/intérêt bilatéral, `drive_requests` assemble picker→queue→wire; sockets/extensions/alerts restent côté appelant (transport-agnostique) |
| `merge_block_requests.hpp` + pipeline de `peer_connection.cpp` | `session::peer_pipeline` | **ported** | `pending_block`, request/download queues, `RequestMerger` coalescing (bornes tronquées fidèles), `send_block_requests_impl`, AIMD `desired_queue_size` (slow-start/snub), `request_timeout` RTT `sliding_average<20>` mean+4σ |
| `request_blocks.cpp` (`request_a_block`) | `session::request_blocks` | **ported** | torrent/peer_conn inputs → `TorrentRequestCtx`/`PeerRequestCtx` snapshots, `add_request` closure, fast-set mask, strict-end-game + busy-block path verbatim |
| `piece_picker.cpp` (`piece_picker.hpp`) | `session::piece_picker` | **ported** | `PiecePos` buckets 0..7 w/ `prio_factor=3`, 4 download queues (partial/full/finished/zero-prio) incl. reverse variants, `BlockInfo` w/ `num_peers` end-game refcount, `pick_pieces` all paths (partials-heap, suggested, sequential/reverse, rarest-first forward+reverse, random, extent affinity, end-game busy-block re-request), `restore_piece`/`write_failed`/`lock_piece` hash-job lifecycle, `distributed_copies`, pad bytes; `torrent_peer*` → `Option<u64>` peer key, `counters` → `PickerStats` trait; `test_piece_picker` 18 cases (pick, whole, priorities, partials, block states, abort, write_failed+restore, sequential, reverse, suggested, end-game, clear_peer, cursor, rarest-first, distributed_copies, accounting, seeds, queue) — all green |
| `smart_ban.cpp` (`extensions/smart_ban.hpp`) | `session::smart_ban` | **ported (core)** | `m_block_hashes` map + ban rules verbatim (differing CRC on repeat failure, stored-CRC vs passing-piece mismatch); disk reads injected as `read_digest` closures, bans returned as `BanDecision` events instead of calling `torrent::ban_peer` |
| `ut_metadata.cpp`, `ut_pex.cpp`, `i2p_pex.cpp`, `extensions.hpp` | `session::ut_metadata`, `session::ut_pex`, `session::i2p_pex` | **ported** | BEP-9 codec+`UtMetadataTracker`, BEP-11 add/drop diff, i2p_pex torrent+peer plugin split (`I2pPexState`/`I2pPexPeerState`), I2P base64 dest → SHA-256 hash, flood window + 500 KiB cap; per-torrent `extensions` factory vec pending |
| `read_resume_data`/`write_resume_data` (session glue), `session_state` | `session::session_state` | **ported** | `.session` file: bencoded `{torrents: {ih: resume-blob}, dht}` round-trip reusing `write_resume_data_buf` |
| `ip_filter` application, `portmap` glue, `natpmp`/`upnp` session glue | `session` + `engine` | ported | `set_ip_filter` ban+disconnect, `UpnpDriver`/`NatPmp` pumps on injected transports |
| `stat.cpp` (session side), `session_stats.hpp`, `performance_counters.cpp` (`performance_counters.hpp`) | `session::counters` + `session_stats` | **ported** | full 318-entry `stats_counter_t`+`stats_gauge_t` index set verbatim, `Counters` atomic bank, `SessionStats` snapshot, `metrics()` = `session_stats_metrics()` (names+gauge boundary at `num_stats_counters=235`); increments wired where the emitting machinery is ported |
| `web_seed_entry` handling, `http_seed` logic in `torrent.cpp` | `session` web-seed glue + `net::web_seed` | ported | `WebSeedFetch` effects, round-robin, per-file Range requests; http_seed BEP-17 partial |
| `torrent_peer_allocator` (`torrent_peer_allocator.hpp`) | `session::peer_alloc` | **ported** | accounting fidelity only — upstream's freelist pool exists to bound peer memory; `TorrentPeerAllocator` tracks `m_live_allocations`/`m_allocations` and feeds `session_stats` counters; Rust `Vec`-backed peers need no arena |
| `session_udp_sockets`, `listen_socket_handle` glue | `session::listen` | **ported** | `ListenSocket` + route-check, `SessionUdpSocket` per listen socket, `ListenSocketHandle`; session fields `listen_sockets`/`udp_sockets`, `IpNotifier` polled in `tick` → `SessionOut::InterfacesChanged` (`on_ip_change` → rebind) |

## Out of scope / won't port

| C++ item | Reason |
|---|---|
| `bindings/` (python), `setup.py`, `pyproject.toml` | FFI bindings — separate future project |
| `Jamfile*`, `CMakeLists.txt`, `Makefile`, `cmake/` | build system → Cargo |
| `deps/` (try_signal, libsimulator) | vendored C++ deps → crates; simulator ported as `bitdaemon-sim` (Phase F) |
| `assert.cpp`, `invariant_check`, `debug.hpp` | → `debug_assert!` |
| `alloca.hpp`, `stack_allocator`, `heterogeneous_queue`, `tailqueue`, `copy_ptr`, `unique_ptr`, `noexcept_movable`, `deque`/`vector`/`array` shims | C++ container workarounds |
| `posix_*`/`pread_*`/`mmap_*` triplication | unified storage backend (Phase 3) |
| `win_*` platform shims | `std::os::*` + `windows-sys` where truly needed |
| `src/ed25519/*` | → `ed25519-dalek` (ADR-0007) |
| `puff.cpp` | → `flate2` (ADR-0007) |
| `disabled_disk_io` perf tests | perf scaffolding → `criterion` benches (Phase F) |
| `simulation/` harness | **ported** → `bitdaemon-sim` (`SimNet` delivery queue + per-link `Link` latency/jitter/loss, seeded splitmix64, `SimSocket` implementing `DatagramSocket`, `SimClock`, `run_to`/`drain` drivers) |
| `client_test.cpp` → `bitdaemon-cli` | example, tracked separately |
| `aux_/disable_warnings_push.hpp`, `aux_/disable_deprecation_warnings_push.hpp`, `aux_/disable_warnings_pop.hpp` | MSVC/GCC pragma wrappers — Rust has no equivalent warning model |
| `aux_/has_block.hpp` | trivial `have_block` helper — folded into `peer_conn`/`piece_picker` call sites |
| `aux_/packet_pool.hpp` | packet slab allocator — `net::packet_buffer`/`receive_buffer` (Rust allocation, no pool needed) |
| `aux_/piece_block_progress.hpp` | per-block progress counter — folded into `session::peer_pipeline` |
| `aux_/readwrite.hpp` | endian read/write helpers — `bytes::Buf`/`BufMut` + `to_be`/`from_be` |
| `kademlia/dht_observer.hpp` | C++ observer interface → `dht::DhtEvent` drain + `TraverseHooks` seam |

## Intentional deviations log

Record here every place the port *deliberately* diverges from C++
behavior (with reason). None yet.

---

*Update rule*: when you change a status, also update the phase summary
in `ROADMAP.md` if a phase boundary was crossed.
