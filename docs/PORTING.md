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
| `error_code.cpp`, `error.hpp`, `close_reason.cpp` | `common::error` | in progress | `bdecode` category done (`bencode::BdecodeErrorKind`); other `lt::errors` categories pending |
| `time.cpp`, `timestamp_history.cpp` | `common::time` | ported | `Clock`/`TimePoint` aliases on `Instant`, `Milliseconds32` i32 ticks; `timestamp_history` still pending (lands with session stats) |
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
| `numeric_cast.hpp`, `noexcept_movable.hpp`, `copy_ptr.hpp`, `unique_ptr.hpp`, `alloca.hpp`, `array.hpp`, `container_wrapper.hpp`, `deque.hpp`, `vector.hpp`, `vector_utils.hpp`, `heterogeneous_queue.hpp`, `tailqueue.hpp`, `link.hpp`, `pool.hpp`, `stack_allocator.hpp`, `string_ptr.hpp`, `strview_less.hpp`, `byteswap.hpp`, `range.hpp`, `scope_end.hpp`, `throw.hpp`, `set_socket_buffer.hpp`, `keepalive.hpp`, `io.hpp`, `io_bytes.hpp`, `visit_block_iovecs.hpp` | idiomatic Rust | won't port | STL/boost shims — use std equivalents |

## bitdaemon-bencode

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `bdecode.cpp` (`bdecode.hpp`) | `bencode::decode` + `bencode::node` | ported | 8-byte packed tokens (`offset:29|kind:3`/`next:29|hdr:3`), lazy `int_value`, back-patched partial trees, soft errors, error positions — 60+ cases from `test_bdecode.cpp` pass |
| `entry.cpp` (`entry.hpp`) | `bencode::entry` | ported | `Entry` owned enum; dict = `BTreeMap<Vec<u8>,_>` = upstream sorted `map` |
| `bencode.hpp`, `aux_::bencoder.hpp` | `bencode::encode` | ported | canonical dict order; `Preformatted` verbatim; `Undefined`→`0:` |
| `print_entry` (in `bdecode.cpp`/`entry.cpp`) | `bencode::print` | ported | `print_node`/`print_entry`, upstream separators + 200-char one-liner rule |
| `gzip.cpp`, `puff.cpp` | `bencode::gzip` (or net) | not started | `flate2` crate; puff is C zlib-inflate — don't port |
| `xml_parse.cpp`, `xml_parse.hpp` | `core::rss`? | not started | RSS support — low priority |

## bitdaemon-core

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `torrent_info.cpp` (`torrent_info.hpp`) | `core::torrent_info` | **ported** | v1+v2+hybrid; v2 piece-layer merkle validation; byte-oriented names |
| `file_storage.cpp` (`file_storage.hpp`), `file_layout.hpp` | `core::file_storage` | **ported** | byte-oriented paths (upstream `std::string` semantics); `test_file_storage.cpp` 28/28 |
| `create_torrent.cpp` (`create_torrent.hpp`) | `core::create_torrent` | **ported** | canonicalize + v1/v2/hybrid generate; `set_piece_hashes`; `test_create_torrent.cpp` 8/8 |
| `merkle.cpp` (`merkle.hpp`), `aux_/merkle_tree.hpp` + `merkle_tree.cpp` | `core::merkle`, `core::merkle_tree` | **ported** | full sparse/piece-layer tree, proofs, verified-bitfield; `test_merkle_tree.cpp` 22/22 |
| `magnet_uri.cpp` (`magnet_uri.hpp`) | `core::magnet_uri` | **ported** | `test_magnet.cpp` parse vectors 24/24 |
| `load_torrent.cpp` (`load_torrent.hpp`) | `core::torrent_info` (`load_torrent*`) | **ported** | `parse_torrent_file`/`load_torrent_buffer` |
| `announce_entry.cpp`, `tracker_list.cpp` | `core::tracker` | not started | tier + failover semantics |
| `web_seed_entry.cpp` (`web_seed_entry.hpp`) | `core::web_seed` | partial | `WebSeedEntry` data struct; endpoint/scrape logic is Phase 5 |
| `fingerprint.cpp`, `identify_client.cpp` | `core::fingerprint` | **ported** | az/shadow/mainline/TR-legacy styles + name map; `test_identify_client.cpp` 2/2 |
| `read_resume_data.cpp`, `write_resume_data.cpp` | `core::resume_data` | **ported** | file-version 1+2 read, v2 write; `test_read_resume.cpp` vectors + round-trip |
| `resolve_duplicate_filenames.cpp` | `core::resolve` | **ported** | case-insensitive `.N` renames, dir/pad rules; upstream table 12/12 (Windows sep-safe) |
| `apply_pad_files.hpp`, `resolve_duplicate_filenames.hpp` | `core::pad` | **ported** | folded into `resolve` |
| `resolve_links.cpp` (`resolve_links.hpp`) | `core::resolve_links` | **ported** | fixture table 14/14 (pad/align + v1 piece-hash + v2 root matching) |
| `add_torrent_params.cpp` (`add_torrent_params.hpp`) | `core::add_torrent_params` | **ported** | single canonical struct used by torrent/magnet/resume |
| `path.cpp` (string helpers), `aux_/path.hpp` | `core::path` | **ported** (string half) | `directory.cpp`/`drive_info.cpp` (fs-dependent) → Phase 3 |
| `entry`-adjacent `client_data.hpp`, `fwd.hpp`, `version.cpp` | misc | not started | |
| `session_types.hpp`, `session_status.hpp`, `storage_defs.hpp`, `torrent_status.cpp`, `download_priority.hpp` | `core::types` | partial | `DownloadPriority` in common; status types later |
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
| `storage_constructor` / `default_storage_constructor` (in `disk_interface.hpp`/`storage.hpp`) | `disk::StorageFactory` | not started | **injectable storage** (ADR-0008) — OnionBit `PrivateStorageFactory` parity |
| `disk_buffer_pool.cpp`, `disk_buffer_holder.cpp` | `disk::disk_buffer_pool` | **ported** | `Box<[u8]>` + RAII holder → allocator on drop; `in_use` + per-category histogram; `bulk_free_buffer` batch free |
| `disabled_disk_io.cpp` | `disk::disk_interface::DisabledDiskIo` | **ported** | synchronous no-op completions |
| `mmap.cpp`, `mmap_disk_io.cpp`, `mmap_storage.cpp`, `mmap_disk_job.hpp`, `file_view_pool.cpp` | `disk::mmap` | deferred | requires `unsafe` (`memmap2::map_mut`) — `forbid(unsafe_code)`; `PreadDiskIo` is the reference backend; revisit as a separate unsafe-allowed crate if needed |
| `posix_storage.cpp`, `posix_part_file.cpp` | `disk::storage` (shared) | merged | part file + storage identical to pread variant |
| `pread_disk_io.cpp` | `disk::disk_io` | **ported** | full `DiskIo` impl; `StorageFactory` injection point (ADR-0008 — downstream private/encrypted storage); storage registry as `dyn DiskStorage` |
| `storage_utils.cpp` | `disk::storage_utils` | **ported** | `initialize_storage`, `move_storage`, `delete_files`, `rename_file`, `verify_resume_data`, `has_any_file` |
| `part_file.cpp` (`part_file.hpp`) | `disk::part_file` | **ported** | header format + slot reuse + export frees full pieces |
| `file.cpp`/`file.hpp`, `open_mode.hpp`, `file_pool_impl.cpp`/`file_pool.hpp` | `disk::file`, `disk::open_mode`, `disk::file_pool` | **ported** | `pread_all`/`pwrite_all`/`pwritev_all`; pool: LRU evict, open-waiter sharing, r/w upgrade race counter, ENOENT-mkdir retry, deferred close |
| `file_progress.cpp` | `disk::file_progress` | **ported** | `init` takes the have-pieces `Bitfield` (picker dep swapped — documented deviation); `completed_cb` on file finish |
| `hash_picker.cpp` (`hash_picker.hpp`) | `disk::hash_picker` | **ported** | single-request `pick_hashes` API; both request shapes, 3 s retry interval, `test_hash_picker.cpp` 10/10 |
| `precomputed_block_hashes.cpp` | `disk::precomputed_block_hashes` | **ported** | all-zero sentinel, `take`/`take_block`/`drop` |
| `stat_cache.cpp` (disk side) | `disk::stat_cache` | **ported** | — |
| `copy_file.cpp`, `drive_info.cpp`, `directory.cpp` | `disk::fs` | **ported** | hard-link-first copy; `free_space` via fs2; `truncate.cpp` folds into `set_len` |
| `disk_observer.hpp`, `disk_completed_queue.hpp` | `disk::observer` | not started | → channel events |
| `win_file_handle.hpp`, `windows.hpp`, `win_util.hpp` | — | won't port | platform shims → `std::os` |

## bitdaemon-net

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `socket_io.cpp`/`socket_io.hpp` | `net::socket_io` | **ported** | BE int helpers, endpoint bytes (6/18 B), `read_endpoint_list` |
| `ip_filter.cpp` | `net::ip_filter` | **ported** | `BTreeMap` set-surgery verbatim; `test_ip_filter.cpp` 9/9 incl. overlap joins + v6 + port_filter |
| `pe_crypto.cpp` | `net::pe_crypto` | **ported** | DH-768 (`num-bigint` powmod, degenerate-key guard), RC4 RFC 6229 vectors exact, `keyA/keyB` longkey derivation, 1024-byte discard |
| `bt_peer_connection` packet layer | `net::peer_wire` | partial | handshake (68 B, reserved bits verbatim) + all 24 message ids, tokio codec, max-len cap; conn state machine later |
| `peer_connection.cpp` (`peer_connection.hpp`), `peer_connection_handle.cpp`, `peer_connection_interface.hpp` | `net::peer` | not started | base state machine |
| `bt_peer_connection.cpp` (`bt_peer_connection.hpp`) | `net::peer::bt` | not started | BT handshake + BEP-10 |
| `web_connection_base.cpp`, `web_peer_connection.cpp` | `net::peer::web` | not started | web seeds (Phase 8) |
| `request_blocks.cpp`, `merge_block_requests.hpp`, `suggest_piece.hpp`, `peer_list.cpp`, `peer_info.cpp`, `peer.cpp`, `peer.hpp` | `net::peer::*` | not started | |
| `receive_buffer.cpp` + `crypto_receive_buffer` | `net::receive_buffer` | **ported** | cursor buffer verbatim (start/pos/end, `reserve`/`grow`/`cut`/`normalize` + watermark shrink); `test_receive_buffer.cpp` 15/15; `packet_buffer.cpp` deferred to Phase 5 (uTP-only) |
| `socket_type.cpp`, `socket.hpp`, `polymorphic_socket.hpp`, `instantiate_connection.cpp`, `union_endpoint.hpp` | `net::socket` + `net::transport` | in progress | `PeerStream`/`PeerConnector`/`PeerAcceptor` traits scaffolded (ADR-0008) |
| `socks5_stream.cpp`, `http_stream.hpp`, `ssl.cpp`, `ssl_stream.hpp`, `i2p_stream.cpp`, `rtc_stream.cpp`, `rtc_signaling.cpp`, `websocket_stream.cpp` | `net::stream::*` | not started | proxies/TLS/I2P/RTC — rustls not openssl |
| `chained_buffer.cpp` (send-buffer) | `net::chained_buffer` | **ported** | `Box<dyn BufferHolder>` replaces type-erased holder + destruct/move thunks; slack-append, pop_front partial/full, iovec build |
| `http_connection.cpp`, `http_parser.cpp`, `parse_url.cpp`, `resolver.cpp`, `resolver_interface.hpp` | `net::http` | not started | |
| `http_tracker_connection.cpp`, `udp_tracker_connection.cpp`, `websocket_tracker_connection.cpp`, `tracker_manager.cpp`, `tracker_list.cpp` (net side) | `net::tracker` | not started | announce retry semantics; UDP tracker consumes `DatagramSocket` (ADR-0008) |
| `upnp.cpp`, `natpmp.cpp`, `portmap.cpp` | `net::nat` | not started | |
| `lsd.cpp` (`lsd.hpp`) | `net::lsd` | not started | |
| `udp_socket.cpp`, `session_udp_sockets.hpp`, `listen_socket_handle.cpp` | `net::udp` | not started | wraps `DatagramSocket` (ADR-0008); default impl = `TokioUdpSocket` |
| `utp_stream.cpp`, `utp_socket_manager.cpp` | `net::utp` | not started | **Phase 5** (OnionBit peer transport); runs on injected `DatagramSocket`, exposed as `PeerConnector`/`PeerAcceptor` |
| `ip_filter.cpp`, `ip_voter.cpp`, `ip_notifier.cpp`, `enum_net.cpp`, `netlink.cpp`, `route.h`, `bind_to_device.hpp` | `net::iface` | not started | |
| `bandwidth_limit.cpp`, `bandwidth_manager.cpp`, `bandwidth_queue_entry.cpp`, `bandwidth_socket.hpp` | `net::bandwidth` | **ported** | `BandwidthChannel` token bucket (3 s burst cap, `inf`=INT_MAX), `BwRequest` (priority pro-rata `distribute*prio/tmp`, ttl 20), `BandwidthManager` queue + disconnect quota return; `Rc<BandwidthChannel>`/`Rc<dyn BandwidthSocket>` replace raw ptr/`shared_ptr` |
| `proxy_base.cpp`, `proxy_settings.cpp` | `net::proxy` | not started | |
| `extensions.hpp`, `i2p_pex.cpp` (ext part), `ut_pex` protocol bits | `net::ext` | not started | extension registry |
| `identify_client.cpp` (net side), `peer_class` filtering | `net::peer::class` | not started | |
| `kademlia`-indep. `socket_io.cpp`, `ip_helpers.cpp` | `net::util` | not started | |
| `win_cng.hpp`, `win_crypto_provider.hpp` | — | won't port | → RustCrypto |
| `route.h`, `netlink_utils.hpp`, `platform_util.cpp` | `net::platform` | not started | interface enum per-OS |

## bitdaemon-dht (src/kademlia/*, include/libtorrent/kademlia/*)

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `node.cpp` (`node.hpp`) | `dht::node` | not started | main state machine; UDP socket injected via `DatagramSocket` (ADR-0008) |
| `routing_table.cpp` | `dht::routing_table` | not started | |
| `rpc_manager.cpp` | `dht::rpc` | not started | transaction/timeouts |
| `traversal_algorithm.cpp` | `dht::traversal` | not started | base algo |
| `find_data.cpp`, `get_peers.cpp`, `get_item.cpp`, `put_data.cpp`, `refresh.cpp`, `sample_infohashes.cpp` | `dht::ops::*` | not started | traversal specializations |
| `dht_storage.cpp`, `dht_settings.cpp`, `dht_state.cpp` | `dht::storage` | not started | |
| `dht_tracker.cpp` | `dht::tracker` | not started | session glue |
| `dos_blocker.cpp` | `dht::dos` | not started | rate limiting |
| `node_id.cpp`, `node_entry.cpp` | `dht::id` | not started | BEP-42 verify |
| `msg.cpp`, `item.cpp`, `io.hpp`, `direct_request.hpp`, `observer.hpp` | `dht::msg` | not started | bencode schemas |
| `ed25519.cpp` + `src/ed25519/*` (20 files) | `ed25519-dalek` | won't port | RustCrypto crate (ADR-0007) |
| `types.hpp`, `announce_flags.hpp` | `dht::types` | not started | |

## bitdaemon-session

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `session.cpp`, `session_impl.cpp` (`session_impl.hpp`), `session_call.cpp`, `session_handle.cpp` (`session_handle.hpp`), `session_interface.hpp`, `session_params.cpp`, `session_types.hpp` | `session::session` | not started | actor + handle (ADR-0001); `SessionParams` carries injected transports + anonymous-lane flags (ADR-0008) |
| `torrent.cpp` (`torrent.hpp`, `torrent_impl.hpp`), `torrent_handle.cpp`, `torrent_status.cpp`, `torrent_list.hpp`, `torrent_peer.cpp`, `torrent_peer_allocator.cpp` | `session::torrent` | not started | per-torrent state machine |
| `alert.cpp`, `alert_manager.cpp`, `alert_types.hpp`, `alert.hpp` | `session::alert` | not started | enum-based alerts |
| `settings_pack.cpp`, `session_settings.cpp`, `session_stats.cpp`, `performance_counters.cpp` | `session::settings` | not started | typed struct + defaults = spec |
| `add_torrent_params.cpp` (session side) | `session::add` | not started | |
| `choker.cpp`, `peer_class.cpp`, `peer_class_set.cpp`, `peer_class_type_filter.hpp` | `session::choker` | not started | upload slots |
| `piece_picker.cpp` (`piece_picker.hpp`), `piece_block_progress.hpp` | `session::picker` | not started | |
| `smart_ban.cpp` (`extensions/smart_ban.hpp`) | `session::ext::smart_ban` | not started | |
| `ut_metadata.cpp`, `ut_pex.cpp`, `i2p_pex.cpp` (ext side), `extensions.hpp` | `session::ext::*` | not started | BEP-9/11, i2p |
| `read_resume_data`/`write_resume_data` (session glue), `session_state` | `session::state` | not started | `.session` file compat |
| `ip_filter` application, `portmap` glue, `natpmp`/`upnp` session glue | `session::nat` | not started | |
| `stat.cpp` (session side), `session_stats.hpp` | `session::stats` | not started | |
| `web_seed_entry` handling, `http_seed` logic in `torrent.cpp` | `session::web_seed` | not started | |
| `torrent_peer_allocator` → slab | `session::alloc` | not started | `slab`/`typed_arena` |
| `session_udp_sockets`, `listen_socket_handle` glue | `session::listen` | not started | |

## Out of scope / won't port

| C++ item | Reason |
|---|---|
| `bindings/` (python), `setup.py`, `pyproject.toml` | FFI bindings — separate future project |
| `Jamfile*`, `CMakeLists.txt`, `Makefile`, `cmake/` | build system → Cargo |
| `deps/` (try_signal, libsimulator?) | vendored C++ deps → crates or simulator port (Phase 8) |
| `assert.cpp`, `invariant_check`, `debug.hpp` | → `debug_assert!` |
| `alloca.hpp`, `stack_allocator`, `heterogeneous_queue`, `tailqueue`, `copy_ptr`, `unique_ptr`, `noexcept_movable`, `deque`/`vector`/`array` shims | C++ container workarounds |
| `posix_*`/`pread_*`/`mmap_*` triplication | unified storage backend (Phase 3) |
| `win_*` platform shims | `std::os::*` + `windows-sys` where truly needed |
| `src/ed25519/*` | → `ed25519-dalek` (ADR-0007) |
| `puff.cpp` | → `flate2` (ADR-0007) |
| `disabled_disk_io` perf tests, `simulation/` harness | port sim later (Phase 8) |
| `client_test.cpp` → `bitdaemon-cli` | example, tracked separately |

## Intentional deviations log

Record here every place the port *deliberately* diverges from C++
behavior (with reason). None yet.

---

*Update rule*: when you change a status, also update the phase summary
in `ROADMAP.md` if a phase boundary was crossed.
