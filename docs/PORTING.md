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
| `time.cpp`, `timestamp_history.cpp` | `common::time` | not started | monotonic clock semantics |
| `download_priority.hpp`, `operations.hpp`, `torrent_flags.hpp`, `pex_flags.hpp`, `announce_flags.hpp`, `flags.hpp`, `index_range.hpp` | `common::types`/`common::flags` | in progress | `DownloadPriority` strong type done; flags enums pending |
| `random.cpp`, `dev_random.hpp` | `common::random` | not started | `rand` crate (already used by `peer_id`) |
| `hex.cpp` | `common::hex` | ported | to/from hex, validation; upstream messages |
| `crc32c.cpp`, `cpuid.cpp` | `common::crc32c` | not started | `crc32c` crate, no CPUID needed |
| `sliding_average.hpp`, `stat.cpp`, `stat_cache.cpp` | `common::stat` | not started | counters + averages |
| `bloom_filter.cpp` | `common::bloom_filter` | not started | used by DHT routing |
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
| `torrent_info.cpp` (`torrent_info.hpp`) | `core::torrent_info` | not started | v1+v2+hybrid; merkle verify |
| `file_storage.cpp` (`file_storage.hpp`), `file_layout.hpp` | `core::file_storage` | not started | path sanitization is security-critical |
| `create_torrent.cpp` (`create_torrent.hpp`) | `core::create` | not started | |
| `merkle.cpp`, `merkle_tree.cpp` (`merkle*.hpp`) | `core::merkle` | not started | v2 piece layers |
| `magnet_uri.cpp` (`magnet_uri.hpp`) | `core::magnet` | not started | |
| `load_torrent.cpp` (`load_torrent.hpp`) | `core::load` | not started | |
| `announce_entry.cpp`, `tracker_list.cpp` | `core::tracker` | not started | tier + failover semantics |
| `web_seed_entry.cpp` (`web_seed_entry.hpp`) | `core::web_seed` | not started | |
| `fingerprint.cpp`, `identify_client.cpp` | `core::fingerprint` | not started | client identification table |
| `read_resume_data.cpp`, `write_resume_data.cpp` | `core::resume` | not started | wire-compat required |
| `resolve_links.cpp`, `resolve_duplicate_filenames.cpp` | `core::resolve` | not started | |
| `apply_pad_files.hpp`, `resolve_duplicate_filenames.hpp` | `core::pad` | not started | |
| `add_torrent_params.cpp` (`add_torrent_params.hpp`) | `core::add_torrent` | not started | |
| `path.cpp`, `path.hpp`, `directory.cpp`, `drive_info.cpp` | `core::path` | not started | ADS/device-name sanitization |
| `entry`-adjacent `client_data.hpp`, `fwd.hpp`, `version.cpp` | misc | not started | |
| `session_types.hpp`, `session_status.hpp`, `storage_defs.hpp`, `torrent_status.cpp`, `download_priority.hpp` | `core::types` | not started | shared enums |
| `socket_io.cpp`, `parse_url.cpp`, `escape_string.cpp`, `utf8.cpp`, `string_util.cpp`, `ip_helpers.cpp` | `core::util` | not started | could move to common/net |

## bitdaemon-disk

> **Fidelity is the point here** (ADR-0009): this subsystem is the
> primary motivation for the port. Do not simplify away the job queue,
> fences, bounded buffer pool, lazy file pool or part-file — they are
> the fixes for real production failures. Deferred-error semantics
> (lazy open → error at first I/O, as alert) are upstream-specified
> behavior, not a bug.

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `disk_interface.hpp`, `disk_job.cpp`, `disk_job_fence.cpp`, `disk_job_pool.cpp` | `disk::job` | not started | job model → async traits |
| `disk_io_thread_pool.cpp`, `back_pressure.cpp` | `disk::pool` | not started | `spawn_blocking` pool |
| `storage_constructor` / `default_storage_constructor` (in `disk_interface.hpp`/`storage.hpp`) | `disk::StorageFactory` | not started | **injectable storage** (ADR-0008) — OnionBit `PrivateStorageFactory` parity |
| `disk_buffer_pool.cpp`, `disk_buffer_holder.cpp`, `store_buffer.hpp`, `receive_buffer.cpp`, `chained_buffer.cpp`, `packet_pool.hpp` | `disk::buffer` | not started | `Bytes` slab + **bounded** memory caps (`max_queued_disk_bytes`, `send_buffer_watermark`) |
| `disabled_disk_io.cpp` | `disk::disabled` | not started | test backend |
| `mmap.cpp`, `mmap_disk_io.cpp`, `mmap_storage.cpp`, `mmap_disk_job.hpp`, `file_view_pool.cpp` | `disk::mmap` | not started | `memmap2`, optional feature |
| `posix_disk_io.cpp`, `posix_storage.cpp`, `posix_part_file.cpp` | `disk::fs` | not started | one `std::fs` backend |
| `pread_disk_io.cpp`, `pread_storage.cpp` | merged into `disk::fs` | not started | positioned I/O |
| `storage_utils.cpp`, `storage_array.hpp`, `storage_free_list.hpp` | `disk::storage` | not started | |
| `part_file.cpp` (`part_file.hpp`) | `disk::part_file` | not started | |
| `file.cpp`, `file.hpp`, `file_pool_impl.cpp`, `file_pool.hpp`, `file_descriptor.hpp`, `file_pointer.hpp`, `open_mode.hpp` | `disk::file` | not started | lazy open + LRU handle cap — the 349 s→ms fix (ADR-0009) |
| `file_progress.cpp`, `has_block.hpp` | `disk::progress` | not started | |
| `hash_picker.cpp` (`hash_picker.hpp`), `precomputed_block_hashes.cpp` | `disk::hash_picker` | not started | v2 proofs |
| `stat_cache.cpp` (disk side), `debug_disk_thread.hpp` | `disk::cache` | not started | |
| `copy_file.cpp`, `truncate.cpp` | `disk::util` | not started | |
| `disk_observer.hpp`, `disk_completed_queue.hpp` | `disk::observer` | not started | → channel events |
| `win_file_handle.hpp`, `windows.hpp`, `win_util.hpp` | — | won't port | platform shims → `std::os` |

## bitdaemon-net

| C++ source | Rust target | Status | Notes |
|---|---|---|---|
| `peer_connection.cpp` (`peer_connection.hpp`), `peer_connection_handle.cpp`, `peer_connection_interface.hpp` | `net::peer` | not started | base state machine |
| `bt_peer_connection.cpp` (`bt_peer_connection.hpp`) | `net::peer::bt` | not started | BT handshake + BEP-10 |
| `web_connection_base.cpp`, `web_peer_connection.cpp` | `net::peer::web` | not started | web seeds (Phase 8) |
| `request_blocks.cpp`, `merge_block_requests.hpp`, `suggest_piece.hpp`, `peer_list.cpp`, `peer_info.cpp`, `peer.cpp`, `peer.hpp` | `net::peer::*` | not started | |
| `receive_buffer.cpp` (net side), `packet_buffer.cpp`, `bandwidth_queue_entry.cpp` | `net::buffer` | not started | |
| `socket_type.cpp`, `socket.hpp`, `polymorphic_socket.hpp`, `instantiate_connection.cpp`, `union_endpoint.hpp` | `net::socket` + `net::transport` | in progress | `PeerStream`/`PeerConnector`/`PeerAcceptor` traits scaffolded (ADR-0008) |
| `socks5_stream.cpp`, `http_stream.hpp`, `ssl.cpp`, `ssl_stream.hpp`, `i2p_stream.cpp`, `rtc_stream.cpp`, `rtc_signaling.cpp`, `websocket_stream.cpp` | `net::stream::*` | not started | proxies/TLS/I2P/RTC — rustls not openssl |
| `pe_crypto.cpp` (`pe_crypto.hpp`), `rc4` internals, `crypto_provider` | `net::crypto` | not started | MSE/PE; `chacha20poly1305`+`rc4` crates |
| `http_connection.cpp`, `http_parser.cpp`, `parse_url.cpp`, `resolver.cpp`, `resolver_interface.hpp` | `net::http` | not started | |
| `http_tracker_connection.cpp`, `udp_tracker_connection.cpp`, `websocket_tracker_connection.cpp`, `tracker_manager.cpp`, `tracker_list.cpp` (net side) | `net::tracker` | not started | announce retry semantics; UDP tracker consumes `DatagramSocket` (ADR-0008) |
| `upnp.cpp`, `natpmp.cpp`, `portmap.cpp` | `net::nat` | not started | |
| `lsd.cpp` (`lsd.hpp`) | `net::lsd` | not started | |
| `udp_socket.cpp`, `session_udp_sockets.hpp`, `listen_socket_handle.cpp` | `net::udp` | not started | wraps `DatagramSocket` (ADR-0008); default impl = `TokioUdpSocket` |
| `utp_stream.cpp`, `utp_socket_manager.cpp` | `net::utp` | not started | **Phase 5** (OnionBit peer transport); runs on injected `DatagramSocket`, exposed as `PeerConnector`/`PeerAcceptor` |
| `ip_filter.cpp`, `ip_voter.cpp`, `ip_notifier.cpp`, `enum_net.cpp`, `netlink.cpp`, `route.h`, `bind_to_device.hpp` | `net::iface` | not started | |
| `bandwidth_limit.cpp`, `bandwidth_manager.cpp`, `bandwidth_socket.hpp` | `net::bandwidth` | not started | shared w/ session — see ADR-0003 |
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
