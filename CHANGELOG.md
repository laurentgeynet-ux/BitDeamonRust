# Changelog

All notable changes to BitDaemon are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).
Until `1.0.0`, breaking changes may occur in any release.

## [Unreleased]

### Added

- `torrent::second_tick` ordering parity: `want_tick` tests the raw
  `m_paused` bit (session-paused torrents keep ticking stats), a
  paused non-graceful torrent now early-returns after its stat tick
  exactly like upstream (no conn work, web seeds, time-critical
  picks or `is_inactive` debounce while paused), plugin/extension
  ticks and `stats_alert` are tick-list-scoped, `disconnecting`
  conns are skipped by the per-conn tick (upstream's
  `m_disconnecting` early-out), and `request_time_critical_pieces`
  re-drives deadline pieces every tick while not in upload mode.
- Monotonic payload accounting (`m_stat`/`m_total_uploaded`
  parity): `total_upload`/`total_download`/`total_payload_*` no
  longer shrink when a peer disconnects — conn stat channels fold
  into torrent accumulators at `remove_peer`; resume
  `total_uploaded`/`total_downloaded` write the all-time counter
  (resume seed + session payload) like upstream; share-mode targets
  compare against the same all-time value; `state_updated()` marks
  on nonzero low-pass rate and `need_save_resume` marks on payload
  movement (`if_counters_changed`), so auto-save and
  `save_resume_data(only_if_modified)` behave like upstream.
- Peer-class subsystem wired end-to-end (`peer_class.cpp` +
  `session_impl`/`torrent`/`peer_connection` consumers): session pool
  + `init_peer_class_filter` defaults (global/tcp/local — local gets
  `ignore_unchoke_slots` + a 150% `connection_limit_factor`),
  attach-time `set_peer_classes` (address filter ∩ socket-type
  filter), `ignore_unchoke_slots()` evaluated live over the conn's
  *and* the torrent's class sets at every upstream gate (choke/unchoke
  gauges, `remove_peer` decrement, status `num_uploads`, the
  unlimited-slots sweep), and `peer_connection::maybe_unchoke_this_peer`
  — exempt conns unchoke right after the handshake, others via
  `preemptive_unchoke`. Connection caps are class-scaled
  (`max_connections * 100 / factor`) at both `peer_connected` and the
  engine's `on_incoming_connection` (plus the no-active-torrents
  silent-drop and `connections_slack`/`exceeded_limit` ladder with
  `find_disconnect_candidate_torrent` eviction). New
  `Session::{create,delete,set,get}_peer_class` + filter accessors;
  `apply_settings` pushes `upload_rate_limit`/`download_rate_limit`
  onto the global class channels, deprecated `local_*` onto the local
  class, `rate_limit_utp` toggles the type-filter's global-class bit
  on (SSL-)uTP, `ignore_limits_on_local_network` rebuilds the address
  filter, and `mixed_mode_algorithm`'s `peer_proportional` split runs
  per tick. Torrents carry a lazily-created `peer_class` now:
  `set_limit_impl`/`limit_impl` route per-torrent up/down limits
  through its `BandwidthChannel`s (upstream `-1`-for-unlimited API
  semantics, resume round-trip included), `setup_peer_class` fires on
  the first nonzero limit, and the class is released on
  `remove_torrent`/`abort`. Deferred until `BandwidthManager` wiring:
  class-channel consumption (`use_quota_overhead`,
  `rate_limit_ip_overhead` debits, `bw_network`/`bw_limit` channel
  state — `peer_proportional` currently counts with `!disk_blocked`
  as the `bw_network` stand-in).
- Disconnect severity propagation (`peer_connection::disconnect(ec, op,
  severity)` → `m_failed` → `peer_list::connection_closed` failcount):
  `PeerEffect::Disconnect`/`TorrentEffect::DisconnectPeer` now carry the
  upstream severity; protocol errors (invalid HAVE/DONT_HAVE/bitfield,
  too-many-requests-while-choked, invalid messages, PEX errors),
  `peer_banned`, self-connections and dial failures bump `failcount`,
  while timeouts, pause, duplicate peer-id, upload-upload, finished and
  ip/port-filter sweeps run at normal severity. `PeerConn::failed`
  survives into `peer_disconnected_addr` so `connection_closed` gets the
  real verdict; `Session::peer_disconnected` ORs it with the effect's
  flag (first disconnect wins, like upstream).
- `ip_filter`/`port_filter` semantics completed: `Session::set_ip_filter`
  now disconnects banned conns *before* erasing swarm entries (upstream
  `apply_ip_filter` order — `connection_closed` bookkeeping stays
  intact), drops the sockets via `DisconnectPeer`, and posts
  `peer_blocked_alert(ip_filter)` per banned address. New
  `Session::set_port_filter` runs the matching `apply_port_filter`
  sweep; `session_handle::set_port_filter` now applies instead of
  storing inert. `privileged_port_updated` got the same ordering fix.
  `Torrent` holds shared `Arc` copies of both filters (`m_ip_filter`/
  `m_ses.get_port_filter()` upstream), `add_swarm_peer` gates
  inserts with `peer_blocked_alert` (`ip_filter` gated on
  `apply_ip_filter`, `port_filter` unconditional), `peer_connected`
  refuses filtered attachers, `ban_ip` rebuilds + re-shares the filter
  and runs the full sweep, tracker announces carry `req.filter` when
  `apply_ip_filter_to_trackers` (resolved endpoints filtered →
  `banned_by_ip_filter` failure), and web-seed connects filter resolved
  endpoints like upstream's `on_name_lookup`.
- `on_parole`/`trust_points`/`hashfails` wiring — `piece_finished`'s
  trust loop and `penalize_peers` are now live: a passing piece clears
  parole and bumps trust (+1, cap 8) on every contributing peer; a
  failed piece marks downloaders on parole (`use_parole_mode`), costs
  2 trust points (floor −7), and bans repeat offenders or a certain
  culprit (`peer_ban_alert` + `peer_list.ban_peer` + disconnect).
  Parole peers pick with `on_parole | prioritize_partials`, snubbed
  peers with `reverse`, matching upstream `picker_options()`.
- Web seeds now honor `request_large_blocks` (`web_peer_connection`):
  contiguous picked blocks coalesce into a single HTTP byte-range
  request — piece-boundary crossings included — and responses slice
  back into per-block `mark_as_writing` + `DiskWrite`s.
- `num_peers_down_requests` accounting (peers with a non-empty
  download queue), endgame 5 s re-pick via `m_last_request`, and the
  `m_quota[download_channel] > 0` gate on the timeout detectors
  (approximated by the global download bucket) in
  `check_request_timeouts`.

### Fixed

- **Pause is now two-layered like upstream** — `m_paused` (user) and
  `m_session_paused` are distinct; `is_paused()` is their OR, so a
  user-resumed torrent stays paused under a paused session. Pause
  runs the real `do_pause` machinery: `announce_to_*` flags off,
  `disconnect_all` (graceful mode chokes peers with data in flight
  after `clear_request_queue` and disconnects only idle ones),
  `stop_announcing` + the `stopped` event; `do_resume` re-arms the
  announce flags, resets the tracker list (`started` again) and
  announces immediately. The extension `on_pause`/`on_resume` veto
  now fires *inside* `do_pause`/`do_resume` — the flag still
  flips, only the machinery is skipped, matching upstream. The
  auto-manage queue routes through `set_paused` (graceful
  demotion) instead of flipping flags, and
  `set_flags(flag_paused)` delegates to `pause(graceful_pause)` /
  `resume()` as upstream does.
- **`force_announce` honors tracker failover** —
  `force_reannounce(-1)` triggers every tracker but the
  `announce_state` selection still applies (one working tracker
  under default settings; `announce_to_all_tiers` reaches the
  rest), while `force_announce(idx)` dispatches that single entry
  directly, bypassing failover like upstream `announce_tracker_now`.
- **`part_file` on-disk format is now upstream-compatible**: header
  and slot table are big-endian (was little-endian — written part
  files were unreadable by libtorrent and vice versa), free slots are
  reused FIFO (was LIFO), write-opens create missing directories and
  retry (was a permanent failure), files open with the hidden
  attribute like upstream, and `move_partfile` skips empty files,
  tolerates ENOENT, and falls back to `copy_file` on cross-device
  moves.
- **Write jobs complete at flush, not at cache insert** — the core
  upstream invariant (`cached_block_entry::write_state = disk_job*`):
  the job is parked on its cache block, carried by the `FlushJob`,
  and its handler fires when the block is durable — with the flush's
  error propagated on failure. A failed `writev` retires the visited
  span as flushed (upstream `flushed.set_bit` on error) rather than
  retrying silently; evicted/superseded/teardown writers complete
  aborted exactly once via the completed queue with correct fence
  accounting.
- **Hash jobs no longer spin on a bounded wait** behind in-flight
  writes — they repost through the `piece_gate` and park until the
  flush lands (was a ~400 ms sleep loop that could hash a zero hole
  on a busy disk).
- `check_fastresume` returns the pure upstream `status_t` bitmask —
  `initialize()` errors surface as `fatal_disk_error` instead of
  being swallowed, `no_recheck_incomplete_resume` is honored, and the
  session decodes `need_full_check`/`fatal_disk_error` correctly.
- `move_storage` rolls back already-moved files on failure, honors
  `fail_if_exist`/`dont_replace`/`always_reset_files`, updates
  `save_path` on both `PreadStorage` and `MmapStorage`, and keeps
  `part_file_dir`/`stat_cache`/`file_pool` consistent after moves,
  deletes and `release_files`.
- `delete_files` continues past per-file errors (upstream
  best-effort), `has_any_file` only counts non-pad files with
  `size > 0` and recognizes dangling symlinks, `set_file_priority`
  applies partial mutations and exports part-file data when a file
  leaves `dont_download`.
- `file_pool` retries opens on `ERROR_IO_DEVICE` (Windows shared
  drives report it instead of ENOENT) and reports real `last_use`
  timestamps; `file_progress::init` is idempotent like upstream;
  `stat_cache::get_filesize` reports `ENOENT` for symlink-flagged
  files.
- `apply_settings` now resizes the disk thread pools — `aio_threads`
  and `hashing_threads` were read at construction but a runtime change
  was a silent no-op (upstream `update_disk_threads` semantics).
- `DiskIoThreadPool` aligned on `disk_io_thread_pool.cpp`: the idle
  reaper formula was `min_idle − 1` (and `min_idle` was never sampled,
  staying at `i64::MAX` — wiring the reaper would have emptied the
  pool); now verbatim `min(max(min_idle, threads − max), threads − 1)`
  with the upstream `exchange(num_idle)` re-seed. The wait loop uses
  the upstream 1 s `wait_for` cadence emitting one idle/active pair
  per round, `job_queued` counts *idle* threads as coverage (busy
  threads can't take new jobs) and clamps `threads_to_exit` on arrival,
  `stop_threads` assigns instead of accumulating, and `try_thread_exit`
  removes the dead handle — `num_threads` no longer counts exited
  workers (latent under-provisioning on any shrink).
- `reap_idle_threads` is now driven: the session tick calls it every
  60 s across all pools (upstream's per-pool `deadline_timer`).
- `async_write` now returns the back-pressure status even when the job
  is parked behind a fence — upstream `account_pending_write` runs
  before `is_blocked`, so a parked write still reports `exceeded`
  (the peer was free to keep requesting past the high watermark).
- Disk job flags are now plumbed through `pread_disk_io` (previously
  parsed and dropped): `volatile_read` reaches `advise_dont_need` on
  `read`/`partial_read`/`hash`/`hash2` read paths, `sequential_access`
  maps to `open_mode::SEQUENTIAL_ACCESS` (`FILE_FLAG_SEQUENTIAL_SCAN`/
  `fadvise(SEQUENTIAL)`) via `file_mode_for_job` parity, and `v1_hash`
  gates the SHA-1 pass — a v2-only hash job reads `piece_size2` and
  skips SHA-1 entirely like upstream `do_job(hash)`. `async_hash`
  wipes the caller's v2 span on entry (upstream's stale-bytes
  contract) and `hash_job` fills from `take_precomputed_v2` and only
  hashes blocks still marked unknown.
- `async_delete_files` aborts queued volatile hash jobs on the hash
  pool (upstream `abort_hash_jobs`) so a delete during a recheck
  doesn't grind the whole hash queue; `clear_piece` drains
  `piece_gate.blocked` for the piece (upstream `clear_piece_jobs`) so
  parked hash jobs complete aborted instead of hashing cleared data.
- Session-side job flags now match `torrent.cpp`: `add_piece` writes
  get `flush_piece | volatile_read` under `disk_io_write_mode ==
  disable_os_cache`; `verify_piece` hashes get `flush_piece` /
  `volatile_read` per write mode plus `v1_hash`;
  `start_checking`-style rehashes get `sequential_access |
  volatile_read | v1_hash`. `hash_job_v2_span` now sizes for every
  v2-capable torrent (upstream `has_v2()`, hybrid included), and
  `torrent::disable_v1_hashes` is stored and round-trips through
  `torrent::flags()` like upstream's `m_disable_v1_hashes`.
- `peer_info` queue fields now match upstream: `download_queue_length`
  counts sent *and* unsent requests, `requests_in_buffer` counts
  requests still sitting in the send buffer (drained via
  `on_send_data`), `num_hashfails` reads the real peer-list entry, and
  `target_dl_queue_length` uses the endgame/snub-collapsed accessor.
- Downloader attribution resolves picker keys (`peer_key()`) back to
  live connections — `smart_ban` bans/disconnects and the trust/parole
  updates now reach the right peer instead of looking up conn ids.
- Request-pipeline alignment with `peer_connection.cpp`/`request_blocks.cpp`
  (`session::{peer_pipeline,peer_conn,torrent}`): `drive_peer` now routes
  through `request_queue` → `send_block_requests` like upstream
  (`send_block_requests_impl`) instead of short-circuiting to the wire —
  a single request path. `send_block_requests` gained the upstream gates
  (`disconnecting`, `graceful_pause`, checking/metadata states,
  `!has_picker` seed drop, upload_mode) and compares the raw
  `m_desired_queue_size` member (not the endgame/snub-collapsed
  accessor). `add_request` enforces the one-busy-request cap and
  front-inserts time-critical blocks at `queued_time_critical`.
- Snub semantics now mirror `snub_peer`/`second_tick`: only in-flight
  requests (`download_queue`) trigger timeouts, parole peers keep their
  pipeline, `queued_time_critical` resets on every queue drain, the
  last eligible block times out only when its piece has no free blocks,
  the replacement pick runs before the abort (verbatim upstream
  ordering), `piece_timeout` (20 s) detector added, and un-snubbing is
  windowed to `now - m_requested < request_timeout()` instead of
  clearing on any block arrival.
- RTT/queue accounting fixes: request-time samples use
  `now - m_requested` (not `m_last_request`), `m_requested` re-arms on
  `download_queue` only, slow-start bumps re-clamp immediately through
  `update_desired_queue_size`, the `max_out_request_queue < 2` clamp
  order now matches upstream (floor wins), merged large-block requests
  share the run's `send_buffer_offset`, and `m_last_piece` is stamped
  at connect, per piece, and after each send (drives `piece_timeout`).
- Choked peers can now pick `allowed_fast` pieces (BEP-6): the drive
  early-return on `peer_choked` is gone — `request_a_block` restricts
  the mask itself, as upstream does.
- `block_size()` is metadata-aware (`min(piece_length, 16 KiB)`,
  `torrent.hpp:982`): `block_index` reconstruction in dispatch/
  `on_piece`/`on_reject`, cancel mirrors, AIMD block size and
  `TorrentRequestCtx` all use it instead of a hardcoded `0x4000`.
- Engine wiring bug: `max_out_request_queue` was fed from
  `max_allowed_in_request_queue` (default 2000 vs upstream 500) —
  now reads the right setting.
- Workspace-wide removal of `unwrap()`/`expect()`/`panic!` from shipped
  code paths (AGENTS.md §4.3) — ~85 sites across bencode/common/dht/
  disk/mmap/net/session. Invariant-by-construction sites were
  restructured instead of merely tolerated: DHT query/reply builders now
  construct `Dictionary` maps directly (no `dict_mut().expect`), the
  768-bit DH prime is embedded as bytes (`BigUint::from_bytes_be`, no
  runtime hex parse), fixed-size reads use `copy_from_slice`/byte
  literals, and guarded lookups use `let-else`/entry APIs.
- Real error paths now degrade instead of panicking: storage/mmap
  part-file access returns `StorageError` (`NotFound`,
  `partfile_read`/`partfile_write` op) when the `use_partfile`
  invariant is broken; `crypto_receive_buffer::crypto_cut` is a no-op
  with crypto tracking inactive; `bandwidth` queue removals and
  `ip_filter` set surgery are graceful on absent entries;
  `disk_pool::spawn_thread` logs and returns on spawn failure;
  `disk_job_fence` returns `0` when a raised fence has no blocked jobs;
  `lookups::on_reply` skips malformed compact peer entries instead of
  unwrapping.
- `bencode::entry::entry_mut` returns `Option<&mut Entry>` instead of
  panicking on non-dict entries (breaking change — no in-tree callers).
- `common::types::{PieceIndex,FileIndex}::get` debug-asserts on negative
  indices and clamps to `0` in release (upstream uses `TORRENT_ASSERT`).
- `net::torrent_peer::{new_v4,new_v6}` debug-assert the address family
  and fall back to the correct `PeerAddr` variant instead of panicking.
- `dht::dht_node::drive` tolerates `None` lookup slots (previously
  `expect("checked")` could fire on a tombstoned index).
- `disk::disk_io`: ported upstream `prepare_read` — read jobs now
  consult the write cache once, past the fence gate. Fully cached
  requests (including unaligned spans across two 16 KiB blocks) are
  served with no disk I/O; a span with only one side cached is
  converted to `partial_read` reading only the missing side. Fixes a
  real correctness hole too: reads of regions whose data lived only
  in the dirty cache used to hit the file and fail
  (`UnexpectedEof`/short reads), now they are served from cache like
  upstream. `disk_cache::get2` added for the two-block consult.
- `disk::disk_io`: `async_write` now passes `is_blocked` like
  upstream — a write arriving while a fence is up is parked and
  inserted into the cache at repost, instead of racing the exclusive
  job (`clear_piece` reset / storage teardown). Parked writes return
  `false` (no back-pressure signal) and still count against
  `write_blocks`. `job::write` no longer reaches the pool
  (`do_job(write)` is `TORRENT_ASSERT_FAIL` upstream).
- `disk::disk_io`: `abort_jobs` also drains jobs parked inside the
  per-torrent fence (`take_blocked`) — their handlers used to be
  dropped silently on shutdown.
- `disk::disk_io`: reworked the fence repost path to upstream's
  in-lock discipline — the repost callback (invoked while
  `has_fence` is held) now runs only the pure cache consults
  (`insert_write` for writes, `prepare_read` for reads); pool
  dispatch, flush scheduling and completions are deferred until the
  lock drops, exactly like upstream's `repost_state`. The previous
  shape re-entered `is_blocked` for flush jobs inside the callback —
  a real deadlock on the non-recursive fence mutex.
- `disk::disk_io`: ported upstream `m_fence_flush` — a stacked fence
  waiting on writes that were just reposted into the cache now
  drains the storage's whole dirty cache via `Flush` jobs counted
  outstanding *past* `is_blocked` (like upstream's dedicated
  fence-flush pass on a generic thread). Previously the fence ran
  with those blocks still dirty; `delete_files`/`move_storage` could
  observe a storage missing data that a parked write had supplied.
- `disk::disk_io`: `async_write`'s `FLUSH_PIECE` flag is now honored
  — a `force_flush`-flagged piece drains all its dirty runs on
  insert regardless of the watermark (upstream's first flush pass),
  not only when every block has arrived.
- `disk::disk_io`: `async_read` rejects `length <= 0` / `start < 0`
  inline with `invalid_request` (`Operation::FileRead`), like
  upstream — the job never enters the queue.
- `core::create_torrent`: `CreateFlags` now carries the deprecated
  upstream bits `OPTIMIZE_ALIGNMENT`/`OPTIMIZE` (0_bit),
  `MERKLE` (1_bit) and `MUTABLE_TORRENT_SUPPORT` (4_bit) for API
  parity — all no-ops like upstream ≥ 2.0.
- `session::peer_conn` & `torrent::drive_peer`: in-flight requests were
  tracked in `request_queue` instead of `download_queue` upon transmission,
  causing `check_request_timeouts` to wipe active downloads on 2s inactivity
  ticks and discard subsequent incoming blocks as unsolicited
  (`unwanted_block_alert`). Transmitted requests now enter `download_queue`
  via `PeerConn::send_request`, keeping the pipeline active and ensuring
  unsnubbing on piece arrival.
- `session::torrent`: removed swarm-wide `disk_back_pressure` freeze from
  `TorrentRequestCtx::graceful_pause`, resolving sawtoothing and throughput
  collapses caused by global picking halts during disk write bursts.
  Back-pressure is now managed per-peer via `PeerWriteGates` socket reading
  suspension.
- `session::torrent`: smoothed `rate_down_speed` and `rate_up_speed` using
  upstream libtorrent's 5-second exponential moving average (`stat_channel::second_tick`)
  to prevent single-tick instantaneous drops from collapsing `desired_queue_size`
  to minimum bounds, and guarded slow-start exit until reaching minimum queue
  depth (`desired_queue_size >= 8`).
- `net::os_transports`: enabled `TCP_NODELAY` on connected and accepted TCP
  streams in `TokioTcpConnector` and `TokioTcpAcceptor`, eliminating Nagle
  algorithm transmission delays on 17-byte request frames.
- `disk::file` (Windows): implemented `win32::pread` and `win32::pwrite`
  using `OVERLAPPED` offsets, removing file pointer locks for concurrent
  I/O; enabled OS write cache and sparse mode default in `bitdaemon-cli`
  to avoid synchronous zero-fill stalls on non-elevated NTFS writes.
- `disk::file`/`disk::storage`: aligned file allocation with upstream
  `file.cpp`/`pread_storage.cpp`. `open_mode::truncate` now means
  "size to `file_size` once per session" (gated by `m_file_created`,
  no longer mapped to `O_TRUNC`/`truncate(true)`); POSIX sparse-mode
  files are `ftruncate`d to full length with holes (reads of unwritten
  regions return zeros, and `verify_resume_data` no longer rejects
  them for size mismatch); `storage_mode_allocate` now really reserves
  blocks via `posix_fallocate` (and `F_PREALLOCATE` on macOS) so
  `ENOSPC` surfaces at first write-open as `file_fallocate` —
  `EINVAL`/`ENOTSUP` tolerated like upstream. Windows follows the
  upstream gate: `SetFilePointerEx`+`SetEndOfFile`+`SetFileValidData`
  only under `enable_set_file_valid_data` (default off → organic
  growth with NTFS zero-fill), with `SeManageVolumePrivilege` acquired
  before `CreateFileW` and the sparse attribute cleared on close when
  no ranges were ever allocated. Added `no_cow` (`FS_NOCOW_FL`),
  `O_NOATIME` retry-without on open failure, `executable`/`hidden`
  file flags from `file_storage`, and `Operation::FileTruncate`/
  `file_seek` error classification. `Session` now pushes
  `no_atime_storage`/`enable_set_file_valid_data` into the disk layer
  (`set_open_mode_extra`) at `set_disk` and `apply_settings` — live
  storages included — instead of relying on `bitdaemon-cli`.
  `bitdaemon-mmap` got the same once-only truncate/allocate/sparse
  flow through `file::open_raw`.

- `disk_io::job_done`: `JobKind::Flush` retired its cached blocks via
  `run_flushed` **unconditionally** — a failed (or aborted) region
  write left blocks flagged `flushed` without bytes on disk: silent
  holes inside hash-verified pieces (8 corrupted pieces observed on a
  6.5 GiB real-swarm Ubuntu ISO download; interior zero blocks).
  Upstream `flush_piece` only sets `is_flushed` after a successful
  `writev`; on error the buffers now return to the cache via
  `DiskCache::restore_run` and retry at the next watermark/expiry
  flush. `DiskIo::flush_error_count` feeds a session-tick
  `file_error_alert` (upstream `handle_disk_error` path). Covered by
  `flush_error_restores_buffers`.
- `engine::start_acceptor_at`: the inbound responder handshake ran
  unbounded — a peer that connected and stayed silent (or a stalled
  reconnect) wedged the accept loop permanently; later inbound
  connections were accepted but never reached `DialDone`. Now bounded
  by `settings_pack::handshake_timeout` (10 s), matching upstream.
- `torrent::build_merkle_trees`: iterating `file_range()` called
  `file_num_blocks`/`root` on `.pad/N` pad files (multi-file hybrid
  torrents), hitting their `!pad_file` debug_assert — pads now get an
  uninitialized placeholder so `merkle_trees` stays indexed by file
  index. Found by the real-swarm bench (multi-file hybrid add).
- `disk_io`: hash jobs parked by `piece_gate` behind in-flight writes
  were re-submitted through `add_job` on release, tripping the fence's
  `IN_PROGRESS` debug_assert (worker panic → stalled downloads) and
  double-counting `outstanding` in release builds (fence starvation).
  Released jobs now go straight to the pool (`enqueue`); covered by a
  deterministic `hash_parked_behind_inflight_write_is_released`
  regression test.

### Fixed — `bitdaemon-dht` upstream-fidelity audit (full crate sweep)

Every module of `bitdaemon-dht` was re-audited line-by-line against
`libtorrent @ v2.1.2-45-gc019e80ef` (`src/kademlia/*` +
`include/libtorrent/kademlia/*`; `docs/PORTING.md` statuses were *not*
trusted). Corrections per subsystem:

- `lookups`: the `nodes6` reply key was read as `nodes2`, corrupting
  the 38-byte IPv6 compact-node stride (fixed); Mainline's single-blob
  `values` reply form is now decoded alongside libtorrent's
  list-of-endpoints form; bootstrap targets `make_id_secret(nid)`
  (re-read from the live node id at invoke time) and sets `a.bs=1` on
  initial/router seeds like upstream `bootstrap`.
- `dht_node::incoming_request` restructured to upstream order and
  codes: top-level `verify_message` (203 on schema violation), `a.id`
  must be a 20-byte string, `heard_about` feeds the router-node pool
  on incoming queries, replies carry `ip`, `r.id`, source-port `r.p`
  and client version `v`. `put` derives its target from `pk‖salt`
  whenever `k` is present (not only on signed items), enforces the
  1000-byte value / 64-byte salt caps, the token *before* immutable
  stores, negative-seq rejection, CAS mismatch → 301, old seq → 302,
  bad signature → 206.
- `want` (`n4`/`n6`) cross-family replies now consult the sibling
  node's routing table through the injected `foreign` reference —
  upstream `m_get_foreign_node`.
- Write tokens harvested in `find_data::done` are capped to the
  `bucket_size` closest *alive* nodes like upstream (was: every
  token-holder got an announce/put).
- `obfuscated_get_peers` now runs upstream's phase 2: once the
  obfuscated traversal finishes, a new clear `get_peers` is spawned
  seeded by the ≤16 closest alive responders.
- `traversal`: `set_id` now re-sorts pending results
  (`resort_result`), zero node-ids become `NO_ID` observers with
  generated stand-in ids (exempt from `restrict_search_ips` like
  upstream), and `finish()` no longer clears `results` before the
  lookup's `done()` can collect write tokens.
- `rpc`: replies now report the real measured RTT to `node_seen`
  (was a hardcoded 0), and `unreachable()` fails pending
  transactions on transport errors — wired through
  `dht_tracker::incoming_error`.
- `routing_table`: `is_full` gained the upstream guards
  (`live ≥ limit ∧ replacements ≥ bucket_size ∧ bucket <
  num_buckets`), `next_refresh` returns the *node* (the caller
  computes `159 - distance_exp` like `node::send_single_refresh`),
  and a read-only `find_node_ref` serves sibling `want` queries.
- `dht_storage`: `values` is emitted whenever the entry exists (even
  empty — upstream `lookup_peers`), peer `name` is truncated to 100
  **bytes** on a UTF-8 boundary (was 100 chars), and full-table
  `no_token` semantics withhold the write token.
- `dht_tracker`: upload quota (`has_quota` token bucket, 3 s burst)
  is enforced on outgoing packets; `dht_ignore_dark_internet`
  (default on) drops packets from upstream's class-A list
  (9.x/56.x/…) before decode; `nids` (`addr → node_id`) persist in
  `dht_state` and are restored at `bind` like `new_socket`;
  `update_node_id` propagates the storage `node_ids` to both nodes.
- `dos_blocker` gained the upstream `set_block_timer`/`set_rate_limit`
  setters used by `refresh_timeout`.
- Session: `bind` now receives the persisted `DhtState` so `nids`
  seed the node id on rebind; `unchoke_sort` no longer calls
  `select_nth` on an empty candidate list (panic found by the
  workspace suite).
- New regression suite `tests/test_dht_protocol.rs` covering query
  schema errors (203), BEP-44 immutable/mutable put round-trips, CAS
  301 / old-seq 302 / bad-token rejections, `get` sequence filters,
  `get_peers` wire shape (nodes+token+values+name), announce
  implied-port, `want` cross-family, bootstrap `bs`/`make_id_secret`,
  write-token preservation and RPC unreachable/timeout paths.

### Fixed — `bitdaemon-net` upstream-fidelity audit (full crate sweep)

Every module of `bitdaemon-net` was re-audited line-by-line against
`libtorrent @ v2.1.2-45-gc019e80ef` (`docs/PORTING.md` statuses were
*not* trusted). Corrections per subsystem:

- **HTTP layer**: `http_parser` `read_until` now skips consecutive
  delimiters (`"HTTP/1.1  404"` parses), the reason phrase keeps the
  full rest of the status line, `content-length`/`content-range` use
  C `strtoll` prefix semantics (garbage → 0), chunk tail-headers dedup
  first-wins, and the missing helpers `is_ok_status`, `is_redirect`,
  `header_duration` and `resolve_redirect_location` are ported.
  `http_conn`/`http_get` now emits `GET` + `Connection: close`
  verbatim, resolves redirects against the base URL with the
  same-origin authorization rule, and non-SSL HTTP proxies send
  `GET <absolute-url>` without CONNECT like upstream.
- **Trackers**: HTTP announce params follow upstream's verbatim order
  and include the missing ones (`redundant_download`,
  `announce_crypto_support`, `min_announce_interval` clamp);
  `scrape_url` uses `find` not `rfind`; `event_t`/`key` encodings
  fixed. **UDP tracker sends each datagram exactly once** — upstream
  `m_attempts` is a write-only counter and `on_timeout` fails
  outright; the resend loop was removed and stray/malformed packets
  restart the receive deadline instead (`announce_udp`/`scrape_udp`
  signatures dropped `max_tries`). Tracker defaults now match
  `settings_pack` (`num_want` 200, `min_announce_interval` 300,
  `report_redundant_bytes`/`ssrf_mitigation`/`announce_crypto_support`
  true).
- **Peer encryption**: `mse` initiator writes `len_ia = handshake_len`
  and sends the BT handshake encrypted inline (BEP-8 IA), `ia_len > 68`
  disconnects, padding is bounded ≤ 511; `peer_wire::decode` enforces
  upstream's exact `packet_size()` checks; `receive_buffer::
  mutable_buffer()` returns `subspan(recv_start, recv_pos)` not
  `[start, end)`.
- **uTP**: `micros_now` reads the monotonic clock (was wall clock —
  NTP jumps broke LEDBAT), `remove_sack_header` preserves the
  extension chain (a `close_reason` after SACK is kept), SACK
  `acked_bytes` is assigned not accumulated, `insert_packet` drops the
  superseded seq from `needs_resend`, the fast-resend counter is
  incremented *before* `experienced_loss` (upstream dedup guard), and
  the missing `init_mtu`/`restrict_mtu`/`mtu_for_dest` per-destination
  MTU learning plus the SYN-flood cap are ported to `UtpManager`.
- **Proxies/transports**: SOCKS5 error-code mapping, domain `BND`
  replies and SOCKS4 `general_failure` corrected; `http_stream`
  CONNECT status parses via `parse_decimal` (digit prefix);
  `proxy_settings` defaults `proxy_hostnames`/`proxy_peer_connections`/
  `proxy_tracker_connections` to `true` and gains
  `send_host_in_connect`; `ssl_stream` SNI callback resolves a 40-hex
  info-hash to the torrent's SSL context like upstream.
- **I2P SAM**: the session control socket is now persistent —
  `NAMING LOOKUP` reuses it without a second HELLO, `.i2p` names go
  through lookup while raw destinations are used verbatim, the
  accept path reads the `DESTINATION=` line after `STREAM STATUS`,
  and `format_command` enforces the 1024-byte upstream cap.
- **WebSocket/WebRTC**: `RTC_MAX_SDP_SIZE` is 64 KiB (was 4 KiB),
  tracker replies read `min_interval`/`failure reason`, data frames
  are `Binary` (was `Text`), the spurious `Sec-WebSocket-Protocol`
  header is gone, fragmented frames reassemble and pings auto-reply.
  `rtc_signaling` completes a batch only via the description handler
  (timeouts no longer close it); `rtc_stream` chunks writes at
  `maxMessageSize`, reports `would_block` on `bufferedAmount > 0`,
  treats incoming text as `bad_message`, maps `onClosed` to
  `connection_reset`, and `rtc_parse_endpoint` matches `stoul`
  semantics (u16 truncation, sign/prefix accepted).
- **Port mapping**: `natpmp` ports the full PCP variant (60-octet
  request with nonce + v6-mapped addresses, exact 60-octet reply,
  `pcp_unsupp_version` → NAT-PMP fallback) and requires `== 16` bytes
  for NAT-PMP map replies; `upnp` M-SEARCH is byte-verbatim
  (`ST:upnp:rootdevice`, `MAN:"ssdp:discover"`, `MX:3`), renewal uses
  `lease*3/4`, `failcount` resets on success, error codes 716/727 are
  handled, `resolve_control_url` follows upstream's prefix/concat
  rules, delete replies route to a separate `on_upnp_unmap_response`,
  and `next()` performs the upstream wraparound sweep; `lsd` requires
  `ih_str.size() == 40` (v1), skips all-zero hashes and parses
  port/cookie with `strtol`/`strtoul` semantics.
- **Peer bookkeeping**: `torrent_peer::rank` takes `&ExternalIp` with
  no family guard (upstream feeds the unspecified address in),
  `compare_peer` ranks `primary_source`, `add_peer` rejects
  unspecified/port-0/port-1/v6-link-local addresses, dedups by
  `(addr, port)`, inserts sorted (i2p last, IP by address only), caps
  at `max_peerlist_size`, `update_peer` decrements `failcount` on
  tracker source and applies pex flags, `connection_closed` and
  `apply_ip_filter` (erases connected peers too) match upstream,
  `erase_peers` is the single-sweep variant, `compare_peer_for_erase`
  direction is fixed with the `trust_points` tiebreak, incoming peers
  are `connectable=false`, and `clear_peer_prio` is ported.
  `close_reason` table is now verbatim (no extra mappings).
- **Web seeds**: the request path is split into `plan`/`render` so
  headers are emitted at send time per connection —
  `m_first_request` flips per GET like upstream (`Connection:
  keep-alive` only on the first request/proxy). `Host` uses
  `format_host_header` (default port elided), `User-Agent` honors
  `anonymous_mode`/`always_send_user_agent`, URL-embedded
  `user:pass@` becomes `Authorization: Basic` (new `base64encode` in
  `escape_string`), proxy requests carry `Proxy-Authorization` +
  `Proxy-Connection: keep-alive` + absolute-form URIs, extra headers
  pass verbatim. Redirects are upstream-faithful: any 3xx resolves
  `Location` via `resolve_redirect_location` + `maybe_url_encode`,
  `split_url` produces a per-file ephemeral base with same-origin
  authorization forwarding, redirect loops are bounded, and the file
  index is surfaced so the engine registers `web_seed_t::redirects`.
  Only 200/206 are accepted, body length must match the requested
  slice exactly, and pad files synthesize zeroes without a GET.
- **Misc**: `parse_decimal` accepts digit prefixes (`"80x"` → 80,
  `from_chars` parity), `parse_comma_separated_string` keeps empty
  elements, `IpInterface` gains `flags`/`state`/`friendly_name`/
  `description` and keeps loopbacks flagged, `resolver` ports the
  `abort_on_shutdown` flag and clears cached failures on success,
  `parse_url` gains `maybe_url_encode`, `dht_storage::announce_peer`
  enforces the `/56` cap.
- Engine glue in `crates/bitdaemon`: `spawn_web_seed` rewired for
  per-base connections, redirect registration and settings-aware
  headers; UPnP/NAT-PMP drivers adapted to the corrected reply
  parsing; tracker call sites updated to the single-send UDP API.

### Fixed — real-swarm differential findings (vs libtorrent v2.1.2)

- `torrent::want_peers` returned `false` for seeds — a seed never
  dialed and its peer set never refreshed. Upstream
  `want_peers_finished` keeps seeds eligible, gated on
  `seeding_outgoing_connections` (default `true`); the gate is now
  ported (`update_want_peers` keeps seeds in the want-peers set like
  upstream).
- `ut_pex` advertised nothing in production:
  `PeerConn::{outgoing,connecting,handshake_done}` were never
  assigned outside tests, inbound connections stored `remote: None`
  (`0.0.0.0:0` in alerts), and the ext-handshake `p` listen port was
  parsed but unused. Lifecycle flags are set when the BT handshake
  completes, inbound remote endpoints are kept, and `p` feeds
  `peer_listen_port` — inbound peers are advertised with their
  announced listen port, matching `pex_peer_plugin`. Verified live:
  upstream parsed our added/dropped diffs and dialed a peer it knew
  only through our PEX. Regression:
  `pex_advertises_only_completed_dialable_peers`.
- Magnet downloads stalled at 0 %: metadata resolution left the
  torrent on `StorageIndex::INVALID` (new sentinel — upstream
  `storage_index_t{}`) so disk jobs routed to no storage; real
  storage is now created at `MetadataReceived`. A `Bitfield`
  received before metadata (`num_pieces == 0`) also produced an
  empty `peer_pieces` → never `Interested`; pre-metadata bitfields
  are kept (`pre_metadata_all`) and reconciled when metadata lands
  (truncate to `num_pieces`, bump availability refcounts,
  re-evaluate interest). Regressions:
  `magnet_metadata_creates_storage` +
  `pre_metadata_bitfield_reconciled_on_metadata`.
- `build_merkle_trees`: real 0-byte files hit the `blocks > 0`
  assert in `merkle` — upstream `merkle.cpp::generate` skips empty
  files; they now get placeholder roots like pad files.
- `check_fastresume`: a resume verdict carrying **no resume data**
  was unconditionally "accepted", so existing files were never
  hashed (`have 0/N`, full re-download on every plain add). Ported
  upstream `pread_disk_io` semantics:
  `contains_resume_data ? verify : (has_any_file ? need_full_check
  : ok)`; `DiskIo` gained `has_any_file`. Regression:
  `check_fastresume_no_resume_needs_full_check_when_files_exist`.
- `to_resume_data` exported a fraction of upstream
  `torrent::write_resume_data` — most importantly an **empty
  `save_path`**, so a `.session`-restored torrent silently
  re-downloaded into the process CWD (verified live: a second
  `payload.bin` appeared in the repo root, byte-perfect, while the
  real save-path kept a sparse file). Ported the full field set:
  `save_path`/`part_file_dir`, `flags()`, trackers (url+tier),
  swarm peers (connectable, `failcount == 0`, `trust >= 0`,
  never-connected fillers up to 100, banned list), file priorities
  (mutually exclusive with piece priorities, skipped in
  `seed_mode`), renamed files, up/down stats + limits, time
  counters.
- `Torrent::verified_bytes` (drives `torrent_status::progress`)
  ignored resume-claimed pieces — a restored torrent reported
  `68 %` while seeding 16/16. Claimed bytes now count toward
  `total_done` like upstream, and rewind with `have` when a
  fastresume is rejected or a full recheck is armed.
- Graceful teardown could drop dirty cache blocks queued behind
  in-flight writes — `PreadDiskIo::flush_all` enqueues a `Flush`
  for every dirty run (age-independent), and the CLI drains
  `call_job_handlers` until the queue empties before writing
  `.resume`/`.session`.

### Added — differential bench tooling

- `bitdaemon` CLI: `--resume <file>` loads an upstream-format
  fastresume (falls back to the positional `.torrent`/magnet for
  metadata), `--stop-after <s>` graceful bench stop that still
  writes `.resume`; a fastresume is saved per torrent on shutdown
  (`<v1-infohash>.resume` in save-path — loadable by libtorrent);
  `--session <file>` loads/writes full `.session` state
  (`session_params` layout) — `session restored: N torrent(s)`,
  disk settles via `flush_all` + `call_job_handlers` drain before
  any resume/session write.
- `tools/real_swarm/lt_client`: `makessl`/`sslseed`/`sslleech`/`info`
  modes, `.resume` save+reload, and
  `allow_multiple_connections_per_ip` for multi-client loopback
  benches.

### Verified — live interop vs libtorrent v2.1.2 (real_swarm bench)

- uTP both directions; magnet `ut_metadata` retrieval + full
  download; `.resume` round-trip both ways (incl. upstream detecting
  a truncated tail and re-fetching only the bad pieces); `ut_pex`
  driving upstream's dialer; boundary file layouts both ways
  (0-byte, tiny, deep paths, UTF-8/space names, pad files, hybrid
  v1+v2); SSL torrents both directions with mutual TLS (embedded
  torrent root CA, leaf CN == torrent name).

### Added — full `settings_pack` surface

- `SessionSettings` now exposes **every upstream `settings_pack`
  key** (~230 string/bool/int fields incl. deprecated ones for
  `.session` round-trip), named verbatim and ordered like
  `settings_pack.cpp`. Fields renamed to upstream names:
  `unchoke_slots` → `unchoke_slots_limit`,
  `optimistic_unchoke_slots` → `num_optimistic_unchoke_slots`,
  `max_allowed_in_queue` → `max_allowed_in_request_queue`,
  `max_disk_write_queue` → `max_queued_disk_bytes`,
  `dht_enabled` → `enable_dht`,
  `save_resume_interval` → `save_resume_data_interval`,
  `disable_os_write_cache`/`disable_os_read_cache` →
  `disk_io_write_mode`/`disk_io_read_mode` (new `DiskIoMode` enum).
- New typed enums: `SuggestMode`, `DiskIoMode`,
  `BandwidthMixedAlgo`, `MmapWriteMode`; `EncPolicy`, `ProxyType`,
  `ChokingAlgorithm`, `SeedChokingAlgorithm` serialize as upstream
  int values in `.session`/params dicts.
- **Default fixes vs upstream**: `tick_interval` 1 s → **500 ms**,
  `cache_expiry` 60 s → **300 s**; Windows now defaults
  `disk_io_write_mode=write_through` and `close_file_interval=240`
  like upstream (`DISK_WRITE_MODE`/`CLOSE_FILE_INTERVAL` macros).
- `save_settings_to_dict`/`read_settings_dict` now cover all keys;
  `session_state`'s embedded `settings` dict delegates to the same
  upstream-format functions (replaces the BitDaemon-internal key
  names — old internal `.session` settings keys are ignored on
  load, fields fall back to defaults).

### Added — POSIX read-cache hints (Phase N, ADR-0015/0022)

- `file::posix` — reviewed FFI sibling of `file::win32` (ADR-0022 §3):
  `posix_fadvise(POSIX_FADV_DONTNEED)` on `volatile_read` blocks
  (upstream `advise_dont_need`), `FADV_SEQUENTIAL` on
  `SEQUENTIAL_ACCESS` opens, apple `fcntl(F_NOCACHE)`+`F_NODIRECT`
  on `NO_CACHE`, `O_NOATIME` on `NO_ATIME` (linux/android). Completes
  `disk_io_read_mode` parity — previously the flags were honored as
  no-ops on POSIX. `libc` joins as a `cfg(unix)`-only dependency
  (already declared in the workspace root; `nix` evaluated and
  rejected — its `FcntlArg` lacks `F_NOCACHE`/`F_NODIRECT`).
- WebRTC leaf crate assessment recorded in ADR-0011: `str0m` maps
  the `RtcPeerConnection` seam cleanly but requires a per-connection
  Sans-IO driver over injected `DatagramSocket` (ADR-0008) — deferred
  until a consumer requires it (optional, non-blocking).

### Added — functional completions (Phase M, ADR-0023)

- **Extension seam live** (ADR-0023 D9): `Torrent::extensions` now
  holds instantiated `Arc<dyn SessionPlugin>`s (upstream
  `m_extensions`). `TorrentHandle::add_extension(factory)` invokes
  the factory immediately and walks existing peers through
  `new_peer_connection`, replaying `on_files_checked` when the
  storage check already ran. `SessionPlugin` gained the
  `torrent_plugin` hooks: `new_peer_connection`, `on_piece_pass`,
  `on_piece_failed`, `on_files_checked`, `on_pause` (veto), `tick`.
  New `PeerPlugin` trait (`on_message`, `on_tick`) stored per-conn;
  hooks fired in `add_peer`, `on_peer_message`, `piece_verified`,
  `set_paused`, and the session tick.
- **`MetadataFailed` verdict split**: `Torrent::set_metadata_verdict`
  distinguishes `HashMismatch` (retry — upstream source-penalty
  semantics) from `ParseError` (hash-correct but unparseable →
  `set_error(invalid_swarm_metadata)` + pause + `TorrentError`).
- **`docs/gui-parity.md`** — acceptance matrix mapping every
  qBittorrent-class widget to its handle methods + alert coverage.

### Changed — http_seed upstream-parity resolution (Phase M)

- **BEP-17 is upstream-removed**: libtorrent 2.x ChangeLog —
  "removed support for BEP 17 web seeds (in favor of BEP 19)".
  Ported the actual remaining contract instead of resurrecting a
  deleted protocol: `AddTorrentParams::http_seeds` merges into the
  BEP-19 `web_seeds` fetch path; `TorrentHandle::
  {add,remove}_http_seed` are upstream-faithful no-ops and
  `http_seeds()` returns empty; `to_resume_data` folds all web
  seeds into `atp.url_seeds` (the `httpseeds` resume key is
  write-only upstream — `read_resume_data` never reads it).
- **Breaking (pre-1.0)**: `Torrent::add_http_seed`/
  `remove_http_seed`/`http_seeds()` now behave as upstream's
  deprecated API instead of tracking a private never-fetched list —
  previously-added `http_seeds` entries were silently ignored by
  the fetch machinery anyway.

### Fixed — uTP pump startup race (Phase M)

- **`utp_stream::accept_syn`**: feed the SYN to `UtpSocket`
  synchronously before spawning the pump. Routing it through the
  pump's inbound channel let `tokio::select!` deliver a queued
  `write()` first, emitting data packets toward the unset remote
  (`0.0.0.0:0`) that were lost but kept `bytes_in_flight` — a rare
  permanent stall (intermittent `bulk_transfer_throughput` timeouts).
  The socket is now born `Connected`, matching upstream where the
  socket impl is constructed by the SYN itself.
- **State notifications** ride `tokio::sync::watch` instead of a
  `channel(1)` mpsc — a post-`Connected` transition nobody read could
  block `st_tx.send().await` and freeze the pump. `wait_connected`
  polls `borrow()`/`changed()` (latest-value semantics).

### Added — async result delivery & emission sites (Phase L, ADR-0023)

- **DHT typed result sinks**: `dht_result_map` replaces
  `dht_lookup_map`/`dht_pending_put` — every user lookup
  (`dht_get_peers`, `dht_get_item` immutable + mutable,
  `dht_sample_infohashes`, `dht_put_item`, `dht_announce`,
  `direct_request`, bootstrap) registers its semantic result type;
  `dht_drain_events` delivers `DhtGetPeersReply`,
  `DhtImmutableItem`, `DhtMutableItem`, `DhtSampleInfohashes`
  (incl. `r.nodes` compaction), `DhtPut`, `DhtAnnounce`,
  `DhtDirectResponse` (timeouts included), `DhtBootstrap`,
  `DhtLiveNodes`, `DhtError`, `DhtPacket` and `DhtLog` alerts.
- **Mutable BEP-44**: `dht_put_item(pk, sk, salt, entry)` /
  `dht_get_item(pk, salt)` overloads — canonical target +
  ed25519 signature per upstream `item.cpp`.
- **Port mapping lifecycle**: real `add_port_mapping`/
  `delete_port_mapping` handles (`SessionOut::PortmapAdd/Delete` →
  engine UPnP/NAT-PMP drivers); `Portmap`/`PortmapError`/
  `PortmapLog` alerts posted from driver results; NAT-PMP parser
  extended to carry the opcode protocol for mapping matching.
- **Listener/incoming alerts**: `ListenSucceeded`/`ListenFailed` on
  every acceptor bind (TCP/SSL/uTP/I2P), `IncomingConnection` on
  accept; `PeerBlocked` on ip-filter rejects (in+outbound).
- **`Session::vote_external`** wraps `ip_voter.cast_vote` at all
  sites (DHT, tracker, peer `yourip`) → `ExternalIp` alert on
  elected-address change only.
- **Stats**: `post_session_stats` → `SessionStatsHeader` (once) +
  `SessionStats` (category-0, unconditional); per-torrent `Stats`
  posted on the tick; `post_dht_stats` carries the full upstream
  `dht_stats_alert` payload.
- **Emission-site sweep**: `MetadataReceived`, `TorrentPaused`/
  `TorrentResumed`, `TorrentError`, `TorrentConflict`,
  `TorrentNeedCert`, `AddTorrent`, `FastresumeRejected`,
  `OversizedFile` (storage `initialize()` plumbed through
  `CheckFastresume`), `Trackerid`, `FileCompleted`,
  `BlockDownloading`/`BlockTimeout`/`BlockFinished`/
  `BlockUploaded`, `UnwantedBlock`, `InvalidRequest`,
  `PeerSnubbed`/`PeerUnsnubbed`, `PeerError`, `IpBan`, `Socks5`,
  `TrackerAnnounce`/`TrackerWarning`, `UdpError`, `LsdPeer`/
  `LsdError`, `UrlSeed`, `FilePrio`, `Log`/`TorrentLog`/`PeerLog`/
  `PickerLog`/`I2p`/`DhtLog` (gated via `should_post`),
  `SaveResumeDataFailed`, `SessionError`, `IpBan` on
  secret-id mismatches.

### Fixed — Phase L semantic corrections

- `IncomingRequest` posted at **accept** time (request queued for
  service), matching upstream `incoming_request` — was posted on raw
  message receipt.
- Malformed/out-of-range requests now emit `write_reject_request`
  (BEP-6, gated on `supports_fast`) + `invalid_request_alert`
  instead of disconnecting — upstream only disconnects after >300
  invalid requests while choked.
- `SaveResumeDataFailed` posted on `if_*_changed`-suppressed saves
  (upstream `resume_data_not_modified`) — was silent.
- `AlertsDropped` carries the dropped `alert_type` set; `DhtStats`
  payload boxed to keep `size_of::<Alert>() <= 96`.

### Added — alert surface scaffolding (Phase K, ADR-0023)

- `Alert` enum expanded to all 101 concrete upstream alert types
  (ids 3–107 verbatim from `alert_types.hpp`) plus a
  `ReadPieceFailed` split variant sharing type 5.
  `Alert::alert_type()`/`priority()`/`category()` expose the upstream
  `(alert_type, alert_priority, static_category)` tables; fat DHT
  payloads are `Box`ed (`DhtMutableItemAlert`, `DhtPutAlert`) and
  `size_of::<Alert>() <= 96` is test-guarded.
- **Breaking**: `AlertCategory` realigned to upstream
  `alert_category_t` bit positions (bits 0–24, 23 categories +
  `DEBUG`=`CONNECT` alias). Persisted `alert_mask` values now
  interoperate byte-exact with libtorrent `session_params` files.
- `SessionSettings::alert_mask` — real setting, upstream default
  `error` (previously the manager defaulted to `ALL`), serialized in
  `session_params`, hot-applied by `apply_settings` and
  `SessionHandle::set_alert_mask`. `SessionHandle::new` now applies
  `params.settings` and registers `params.plugins` (were dropped).
- `AlertManager` upstream parity: category-0 alerts
  (`session_stats`/`session_stats_header`/`dht_stats`) post
  unconditionally; the queue limit is priority-weighted
  (`len/(1+priority) >= limit`); dropped alerts are tracked per
  `alert_type` and a synthesized `alerts_dropped_alert` rides
  `get_all`; `post` fans out to `SessionPlugin::on_alert`;
  `should_post` exposed for call-site gating.
- New `tests/test_alert_types.rs` — 102-row conformance table ported
  from upstream `test_alert_types.cpp` plus mask/category-0/
  `session_params` round-trip cases.

### Added — docs: 1.0 API-surface program (ADR-0023)

- New `docs/adr/0023-alert-surface-and-api-completion.md`: full
  upstream alert coverage plan (~60 missing variants), `AlertCategory`
  re-aligned to upstream `alert_category_t` bit positions (fixes
  `alert_mask` interop in persisted `session_params`), DHT
  user-query result delivery via typed sinks (`dht_result_map`),
  mutable BEP-44 overloads, portmap handle table + real
  `delete_port_mapping`, BEP-17 `http_seed` fetch, extension-factory
  invocation.
- `ROADMAP.md`: Phase J code items verified-done (sparse files,
  `SetFileValidData`, `hashing_threads` — re-benchmark still pending);
  new phases K (async result delivery), L (alert surface), M
  (functional completions), N (edge transports/POSIX perf).

### Added — disk file controls & thread parity (Phase J, ADR-0022)

- **Sparse files on Windows**: `PreadStorage::open_file` sets
  `OpenMode::SPARSE` for every file when `!allocate_files` and for
  priority-0 files unconditionally (upstream `pread_storage.cpp:764-
  768`); `FileHandle::open` issues `DeviceIoControl(FSCTL_SET_SPARSE)`
  and sparse files are no longer pre-extended — they grow at each
  write, eliminating the synchronous NTFS valid-data-length zero-fill
  that caused multi-second stalls on HDD.
- **`SetFileValidData`**: `SessionSettings::enable_set_file_valid_data`
  (upstream default false) → `ALLOW_SET_FILE_VALID_DATA` flows through
  `PreadDiskIo::set_open_mode_extra` into a new default no-op
  `DiskStorage::set_open_mode_extra` hook; `file::set_size`/`open`
  call `SetFileValidData` after extension on non-sparse files, with a
  best-effort `SE_MANAGE_VOLUME_NAME` privilege enable. The CLI
  enables it by default (`--no-set-file-valid-data` to opt out) —
  qBittorrent parity.
- **Dedicated hashing pool** (upstream `m_hash_threads`): `PreadDiskIo`
  gains `hash_pool` + `set_hash_threads(n)`; `hash`/`hash2` jobs route
  to it when sized > 0. `SessionSettings` gains `aio_threads` (10),
  `hashing_threads` (1); CLI flags `--aio-threads`/`--hashing-threads`.
- **`bitdaemon-disk` unsafe carve-out**: `forbid`→`deny(unsafe_code)`
  with a single `#[cfg(windows)] #[allow(unsafe_code)]` `file::win32`
  module (second reviewed FFI boundary, cf. ADR-0010 §3); `windows-sys`
  added as Windows-only dependency.

### Added — wiring-completeness audit (Phase I second pass)

- `net::peer_alloc` (moved from `session`): `PeerList` now accounts
  every peer insert/erase/drop through `TorrentPeerAllocator` —
  upstream's `torrent_peer_allocator` counters.
- `disk::precomputed_block_hashes` wired on `PreadStorage`: hash jobs
  `store` v2 block hashes, `hash2` consumes via `take_precomputed_v2_block`
  (no re-read), `clear_piece` drops them. `DiskStorage` gained default
  no-op `store/take/drop` hooks so custom storages are unaffected.
- `core::resolve_links` wired into `Session::check_files`: mutable
  torrents (`similar_torrents`/`collections`) match files against
  seeding torrents; links feed `async_check_files` hard-linking.
- `net::instantiate` wired into `Engine::spawn_dial`: the upstream
  transport decision table now picks the connector — peer connections
  honor `proxies.peer` (`SocksConnector`/`HttpConnectConnector`) and
  uTP preference falls back to TCP.
- Send path uses `ChainedBuffer`: `write_messages` batches the peer's
  outbound queue into one `write_vectored` per round
  (`peer_connection::send_buffer` semantics).
- `dht::item` sign/verify now delegates to `dht::ed25519` helpers;
  `dht::node_id::distance_exp` uses `common::ffs::count_leading_zeros`.


### Added — disk cache (`store_buffer`) write path (ADR-0012/0013/0014/0016)

- `PreadDiskIo::async_write` now inserts blocks into a per-storage
  `DiskCache` instead of enqueueing one `Write` job per 16 KiB
  block. Dirty blocks flush as **coalesced contiguous runs**
  (`JobKind::Flush`), matching upstream's `store_buffer` +
  `disk_cache` model — one syscall per run instead of one per
  block.
- Reads and piece hashes (`read`, `hash`, `hash2`) overlay cached
  dirty blocks before touching disk, so verified data is visible
  before the flush completes.
- `cache_expiry` setting: incomplete cached pieces are
  force-flushed after the deadline, drained from
  `Session::drain_disk_completed`.
- Fences force-flush the cache of the fenced piece before
  arming, preserving `ClearPiece`/`move_storage` ordering.
- Cache watermarks (ADR-0013): above the high watermark the
  write path drains runs until the low watermark — upstream
  `check_buffer_level` hysteresis; back-pressure is measured in
  cached blocks (`max_queued_disk_bytes` / 16 KiB).
- `disable_os_read_cache` setting + `--no-os-read-cache` flag:
  `no_cache` open flags on read/hash jobs (ADR-0015).
- Disk performance counters (ADR-0017): `queue_stats()` now
  reports upstream `disk.*` gauges — `cached_blocks`,
  `num_unhashed`, `disk_blocks_in_use`, `blocked_disk_jobs`,
  `queued_write_bytes`, cumulative `num_blocks_written/read/
  hashed`, `num_write_ops/read_ops`, file-pool
  hits/misses/stalls/races; CLI status line shows
  `cache=`/`unhashed=`/`bufs=`.
- Live validation: Debian ISO swarm at ~115 MiB/s, disk queue
  depth `w=0` at every sample, `have` progress continuous with
  no verification plateau.

### Fixed — freeze / unbounded buffering

- **`FILE_FLAG_RANDOM_ACCESS` removed** from every Windows file
  open — upstream avoids it deliberately (`file.cpp` cites
  KB2549369: the cache manager cannot trim its working set,
  RAM grows unbounded → whole-machine stall). `no_cache` now
  maps to `FILE_FLAG_WRITE_THROUGH` alone (POSIX: `O_SYNC`,
  not `O_DIRECT` — upstream `file.cpp`).
- **Inbound peer messages were only drained at the 1 Hz tick**:
  `Engine::run` pumped `inbound_rx` once per second, so at
  ~100 MiB/s the unbounded channel accumulated hundreds of MiB
  (observed 390 MiB → 30 MiB sawtooth, disk/network at zero).
  The engine now pumps continuously in bounded batches.
- New `--no-os-cache` CLI flag + `disable_os_write_cache`
  setting → `FILE_FLAG_WRITE_THROUGH` on data writes (upstream
  `disk_io_write_mode = disable_os_cache`).

### Added — dormant-module wiring (Phase I, ADR-0018..0021)

- **UPnP/SSDP** (ADR-0018): `Engine::start_upnp` emits the M-SEARCH
  burst on the datagram socket; SSDP `LOCATION` replies are fetched
  through the injected connector+resolver, parsed into
  `RootDevice`s (relative `controlURL` resolved against the
  description URL), and each IGD gets TCP+UDP port mappings via the
  existing `UpnpDriver` chain. `stop_upnp` sends
  `DeletePortMapping`; `--no-upnp` disables.
- **NAT-PMP** (ADR-0019): `enum_net::enum_routes` (new `netroute`
  dep — safe, cross-platform routing table) +
  `default_gateway_v4`; `Engine::start_natpmp_auto` maps the listen
  port on the discovered gateway. NAT-PMP responses were already
  demuxed.
- **SOCKS5 UDP lanes** (ADR-0020): `Engine::apply_udp_proxy` wraps
  the session `DatagramSocket` in `Socks5UdpSocket` when
  `proxies.dht` is socks5 — DHT/uTP/UDP trackers now ride the
  proxy; `--proxy host:port` / `--proxy-auth user:pass` CLI flags;
  no silent direct fallback.
- **DHT `dos_blocker`** (ADR-0021): instantiated in `DhtNode`,
  gating `incoming` before bdecode — flood sources drop instead of
  being processed.

### Fixed — live Mainline DHT bootstrap

- **DHT never bootstrapped**: `DhtNode::bootstrap()` existed but
  nothing called it — upstream's `dht_tracker::start` runs a
  bootstrap lookup at socket start. The session now bootstraps on
  `enable_dht` and when the first router node arrives on an empty
  table.
- **`SessionOut::DhtAddNode` was silently dropped**:
  `Engine::dispatch_out` only handled `ToPeer`, discarding the
  CLI's bootstrap-node events. Non-peer effects now queue onto
  `pending_out`, drained by `Engine::tick` through the normal
  async dispatch.
- **`want: n6` sent on IPv4 queries** (inverted flag at every
  `rpc.invoke` call site): routers answered with `nodes6`, useless
  to the v4 table. `RpcManager::invoke` now matches upstream —
  `want` is only set on cross-family queries, requesting the
  node's *own* family.
- **`dht_announce_tick` burned its 15-min slot on an empty table**:
  `dht_last_announce` was stamped before bootstrap completed, so
  torrent `get_peers` lookups ran once with zero nodes and never
  retried. Announces now wait until a bound node family has a
  non-empty routing table.
- **DNS returned only the first resolved address** — IPv6-first
  answers (e.g. `dht.transmissionbt.com`) produced unusable router
  entries on a v4-only socket. All resolved endpoints are now fed
  to `dht_add_node`, like upstream `udp::resolver`.
- The CLI now seeds the full upstream `dht_bootstrap_nodes` list
  (`router.utorrent.com`, `router.bitcomet.com`, `dht.aelitis.com`
  added — `router.bittorrent.com` alone was not answering).

Verified live on the public swarm: bootstrap queries got real
replies, the routing table populated (~135 nodes), and `get_peers`
traversals for `ubuntu-24.04.4-live-server-amd64.iso` returned
819 peer endpoints from DHT nodes (7 `found peers` replies).

### Added — strict SSL torrent verification (verified live vs libtorrent v2.1.2+OpenSSL)

- **`TorrentPeerVerifier`** (client side): the peer's chain is
  webpki-verified against the torrent's embedded `ssl-cert` root
  and the leaf DNS SAN — falling back to the most-specific CN —
  must equal the torrent `name` or `"*"` (upstream
  `torrent::verify_peer_cert`). Replaces the interim verify-none.
- **`TorrentClientVerifier`** (accept side): mutual TLS is now
  mandatory (`verify_fail_if_no_peer_cert`) — the connecting
  peer's chain must verify against a registered torrent's root
  and match its leaf name. New deps `rustls-webpki` +
  `x509-parser` (ADR-0007 rationale: byte-exact X.509 semantics).
- Verified end-to-end against libtorrent v2.1.2 built with
  OpenSSL: C++ `sslseed` → BitDaemon leech 100 % hash-identical;
  BitDaemon `--seed --ssl-*` → C++ `sslleech` DONE 32/32
  hash-identical; a rogue self-signed cert was rejected by the C++
  peer (`certificate verify failed`).

### Fixed — uTP engine wiring (verified live vs libtorrent v2.1.2)

- **uTP was routed through the 1 Hz `Engine::tick`**: datagrams
  queued for routing waited ~1 s per round-trip, capping real
  transfers at ~1 packet/s (seed logged `type: uTP` but throughput
  stalled). `spawn_udp_pump` now classifies packets directly in the
  pump — uTP packets are pushed to the `UtpManager` immediately,
  DHT/tracker datagrams keep the normal queue. Verified end-to-end:
  `bitdaemon --utp` downloaded the full 8 MiB payload from a C++
  libtorrent seed over uTP (`uTP` wire type confirmed by the seed),
  SHA-256 identical.
- **`write_all` returned `Pending` forever**: `poll_write` drained
  the internal `write_buf` without waking the caller, so a writer
  blocked on a full outbox never resumed. The waker is now stored
  and woken when the pump drains the outbox.

### Added — uTP / SSL / SOCKS5

- `Engine::enable_utp(connector, acceptor)` + `--utp` CLI flag:
  outgoing dials prefer uTP with automatic TCP fallback; incoming
  uTP accepts run alongside TCP listeners.
- `bitdaemon-net::socks_udp`: full SOCKS5 UDP-associate relay —
  `Socks5UdpSocket` implements `DatagramSocket` over the proxied
  UDP association (control TCP kept alive, `BND` relay address,
  request/response header wrapping per RFC 1928 / BEP-29).
- SSL torrent plumbing: `.torrent` embedded `ssl-cert` detection
  (`is_ssl_torrent`), per-torrent cert+key storage
  (`set_ssl_certificate`), `SniResolver`-backed rustls server
  config on the acceptor, SSL peer streams on dial (`--ssl-cert`
  / `--ssl-key` CLI flags). Note: outbound dialing currently
  accepts any certificate (`verify_none`) — upstream additionally
  verifies the leaf SAN against the torrent name
  (`torrent.cpp`); that strict check is tracked in
  `docs/PORTING.md` as not yet ported.
- `close_reason` module: `CloseReason` enum + upstream
  `close_reason_t` message strings.
- `TorrentHandle` additions: `get_resume_data`,
  `set_sequential_range`, `get_renamed_files`,
  `write_torrent_file` (rebuilds the `.torrent` from verbatim
  `info_section` bytes + live announce-list, preserving the
  info-hash), `post_*` alert helpers.
- `TorrentStatus` additions: `info_hashes`, `next_announce`,
  `completed_time`, `last_seen_complete`, `is_seeding`.
- Session settings now serialize `dht_enabled` and `prefer_rc4`
  into `.session` state (upgrade→downgrade→upgrade round-trip
  covered by tests).

### Fixed

- `invalid tracker response` at end of announce: HTTP tracker
  announces now follow redirects and reject non-200 statuses before
  attempting bdecode (HTML error bodies previously surfaced as
  bencode failures).
- Negative transfer rates on peer disconnect: rates are summed from
  per-peer deltas, so a vanished peer contributes 0 to the next
  tick instead of subtracting its lifetime total.
- Magnet live resolution verified against the public swarm (Ubuntu
  torrent info-hash via trackers/DHT, metadata fetched, download
  completed).

### Fixed — public-swarm interop (verified on a live Ubuntu torrent)

- **HTTPS trackers** dialed plaintext TCP on port 443 — announces
  failed before any peer was returned. `spawn_announce` now wraps
  the stream in rustls via `ssl_stream::ssl_connect` with SNI, using
  the OS root store (`rustls-native-certs`, cached `OnceLock` — the
  `m_ssl_ctx` analogue). Verified: `torrent.ubuntu.com` returns
  50 peers over TLS.
- **Last piece never finished**: `Torrent::init_picker` fed
  `num_pieces * piece_length` as picker `total_size`; for a ragged
  tail the residue is 0, so `blocks_in_last_piece` fell back to a
  full `blocks_per_piece` — the last piece demanded blocks that can
  never arrive, sat at `have = num_pieces - 1`, and was re-requested
  forever (infinite re-download loop). Now passes
  `files().total_size()` like upstream. Verified end-to-end:
  `ubuntu-24.04.4-live-server-amd64.iso` (3.4 GB) downloaded at
  ~100 MB/s peak from the public swarm, `have 12991/12991`,
  `Finished`, and the file SHA-256 matches the official
  `SHA256SUMS` entry.
- **`piece_gate` write→hash ordering** (`disk_io.rs`): a `Hash` job
  could run on a pool thread while a `Write` job for the same piece
  was still in flight — torn reads failed verification and the
  piece entered a hash-fail/re-download loop (upstream hashes out
  of the disk buffer pool, which makes this impossible). Per-piece
  in-flight write counters now park hash jobs behind pending writes
  and repost them on the last write's completion; gate-blocked jobs
  are drained on abort.

### Added

- `bitdaemon-cli --limit <bytes/s>` — `download_rate_limit` wiring
  (shared `RateLimiter` bucket).
- `bitdaemon-cli` status line now prints alerts,
  `have`/`num_pieces`, missing-piece indices and tracker errors.

### Fixed — live-swarm interop (verified against libtorrent v2.1.2)

- **Peer pump desync**: `attach_stream` ran `read_message` inside
  `tokio::select!`; the two-phase `read_exact` is not
  cancellation-safe, so a winning write branch ate the 4-byte
  length and desynced the frame stream (`message too large` after
  ~a few pieces). Now `read_buf` + `decode_next` on a persistent
  buffer — verified live C++→Rust at 100%.
- **FAST extension bit** was never advertised (`run_handshake` got
  `fast=false`) while `HaveNone`/`AllowedFast` were still sent —
  upstream disconnects `invalid have-none`. Both handshake paths
  (initiator + responder) now advertise `DHT`/`FAST` bits like
  `bt_peer_connection` does (`dht` from `settings.dht_enabled`).
- **`hash_request` on v1-protocol connections**: `protocol_v2` is
  now tracked on `PeerConn` (handshake keyed by the v2 info-hash);
  `maybe_send_hash_request` and all three hash handlers are gated
  on it — upstream disconnects `invalid message` otherwise.
- **`HaveAll` refcount**: incoming `have_all` never incremented
  picker refcounts, so `dec_refcount` on disconnect underflowed
  `m_seeds` (`debug_assert seeds > 0` panic). Now emits
  `RefcountBulk` like upstream `peer_has_all`.
- **`have_pieces` resume bits** were never applied to `self.have`
  (only `verified_pieces` were) — a `--seed` torrent advertised
  `have_none`. Now restored like upstream `m_have_pieces`.
- **Auto `check_files` on add** (upstream `torrent::init` →
  `start_checking`): non-`seed_mode` adds run `check_fastresume`;
  a rejected verdict arms `arm_full_recheck` + per-piece
  `DiskHash` jobs instead of staying at 0% forever.
- `bitdaemon-cli --seed` now actually sets `SEED_MODE` + full
  `have_pieces` (flag was parsed but never applied).

### Added

- **`bitdaemon-mmap` crate** — the ADR-0010 `unsafe` carve-out:
  `FileViewPool` (`file_view_pool.cpp` LRU of `memmap2` mappings),
  `MmapStorage` (`mmap_storage.cpp` — full `DiskStorage` impl:
  view read/write/hash, part-file + pad-file semantics, priority
  export via views, unmap-before-move/delete for Windows),
  `mmap_storage_constructor` (`mmap_disk_io` factory seam).



### Added

- `net::rtc_signaling` — full port of `rtc_signaling.cpp`: offer
  batches, `max_webtorrent_offers` incoming cap, duplicate
  offer/answer dedup, negotiation timeout — over injected
  `RtcPeerConnection`/`RtcPeerConnectionFactory` (ADR-0011).
- `SessionSettings`: `webtorrent_stun_server`,
  `webtorrent_connection_timeout`, `max_webtorrent_offers` (upstream
  defaults + sparse `.session` serialization).



### Ledger & wiring (audit pass)

- `docs/PORTING.md` fully reconciled: every upstream `src/**/*.cpp` and
  `include/**/*.hpp` file resolves to `ported`, `verified`,
  `won't port`, or `n/a` — no `started`/`partial`/`in progress` rows
  remain. Stale statuses corrected for UPnP/NAT-PMP, UDP session
  sockets/listen socket handles, interface enumeration/`ip_notifier`,
  `web_seed_entry`, `bt_peer_connection` packet layer, `tracker_event`,
  and the `peer_connection` state machine (wiring verified in
  `session::peer_conn` and `bitdaemon::engine`).
- `rustls-pemfile` (unmaintained, RUSTSEC-2025-0134) replaced by the
  `rustls-pki-types` `PemObject` API; `cargo deny check` now passes
  (advisories, bans, licenses, sources). `bitdaemon-sim` carries the
  workspace license.
- `fuzz/README.md` documents that libFuzzer campaigns run on
  Linux/WSL (no MSVC sanitizer runtime).
- `session_params` uses the project-specific key
  `web_seed_blocks_per_tick` (no aliasing of upstream
  `urlseed_wait_retry`).



### Added
- Golden-vector conformance pipeline live end-to-end: libtorrent
  **v2.1.2 release** built on Windows/MSVC (`D:\Projet\libtorrent-v2.1.2`),
  `tools/golden_vectors/generate.cpp` compiled + run, fixtures committed
  under `crates/*/tests/data/`, and `tests/test_golden.rs` in
  `bitdaemon-bencode` + `bitdaemon-core` verify decode + **byte-exact**
  `write_resume_data_buf` output against upstream bytes.
- `session::session_params` — full port of `session_params.cpp`:
  upstream `write_session_params`/`read_session_params` dict layout
  (`dht`, `dht state`, `settings` sparse/non-default only,
  `extensions`, `ip_filter4`/`ip_filter6`), byte-exact vs v2.1.2
  fixtures (`session_default`, `session_custom`). `SessionParams`
  gains `dht_settings`, `dht_state`, `ip_filter`, `ext_state`;
  `SessionSettings` gains `user_agent`.
- Golden-vector suite extended to 12 fixtures (fingerprint, magnet
  URI, session params, dht_state, resume_rich, v2 torrent) —
  all byte-compared against upstream release output.
- `dht::dht_save_state` now writes only the 18 ABI-2 keys upstream
  `save_dht_settings` emits (ABI-3 fields still read) — required for
  `.session` byte-compat.
- `session::i2p_pex` — port of `extensions/i2p_pex.cpp` (BEP-10 id 9):
  I2P destination-hash exchange (`added`/`added.f`/`dropped`, 32-byte
  entries), SHA-256 over I2P-base64-decoded destinations, 6-slot
  60 s flood window (`too_frequent_pex` disconnect), 500 KiB cap,
  first-send full list; wired into `PeerConn` ext-handshake dispatch
  and `Torrent::pex_tick`.
- `common::lt_error` — full `lt::errors` code table + `HttpError`.
- `common::ffs`, `common::version`, `net::string_util`,
  `net::peer_class` (15-slot `PeerClassSet`, pool refcounts,
  `PeerClassTypeFilter` 6 socket types), `session::peer_alloc`
  (`torrent_peer_allocator` accounting).

### Changed
- **Project scope**: complete-fidelity port (ADR-0010) — every upstream
  `src/*.cpp`/`src/kademlia/*.cpp` file gets a Rust module or a
  reasoned `won't port` row; OnionBit no longer drives phase order.
  Roadmap rewritten (Phases A–G); new planned crates `bitdaemon-mmap`
  (feature-gated, sole `unsafe` carve-out) and `bitdaemon-sim`.

### Fixed
- **UDP tracker replies stolen by the shared socket pump** — the
  engine's UDP pump (DHT/LSD/NAT-PMP) raced `announce_udp` /
  `scrape_udp`, which `recv_from`'d the same injected
  `DatagramSocket`: replies were consumed by the pump and every UDP
  announce timed out (`udp tracker: tracker timeout`). A
  transaction-id demux (`UdpTrackerDemux`) now registers a waiter
  **before** `send_to`; the pump routes matching datagrams to the
  waiter and passes unmatched ones through to DHT/LSD/NAT-PMP
  (`engine::udp_pump` mirrors upstream's single-receiver design).
- IPv6 literals from DNS were attempted on the IPv4 datagram socket
  (Windows `os error 10047`); resolved tracker addresses are now
  filtered by socket family.

### Fixed
- **Real-swarm completion pipeline** (validated end-to-end on a
  public Debian torrent: 100 % at ~12 MiB/s, hash-verified):
  - `peer_list_state` deadlock — a `&slock(session)` borrow
    lifetime-extended a `MutexGuard` across a second `slock` on the
    same non-reentrant `Mutex`, freezing the engine at first tick;
  - trackers never seeded from `add_torrent_params` — `from_params`
    now tiers `atp.trackers` into `add_tracker` so announces fire;
  - `manage_queue` was never called and skipped `paused` torrents —
    upstream `auto_manage_torrents` unpauses auto-managed torrents
    within the limits (auto-managed `paused` is scheduler state, not
    a user pause); `auto_manage_scaler` now runs the pass every
    `auto_manage_interval` (30 s) and `add_torrent` forces an
    immediate pass;
  - IPv6 swarm entries panicked `TorrentPeer::new_v4` — `add_peer`
    now dispatches on `SocketAddr` family;
  - disk write jobs were **dropped** once `back_pressure` tripped
    (default limit was 9 buffers, reads counted instead of writes)
    — `async_write` now always enqueues (upstream
    `store_buffer.insert` semantics: the bool only signals the peer
    to slow down), `write_blocks` tracks the real level
    (`max_queued_disk_bytes` = 100 MiB → 6400 blocks) and
    `job_done` releases the slot + re-checks watermarks at every
    completion path;
  - `drain_disk_completed` never ran completion callbacks —
    `DiskIo::call_job_handlers` is invoked at the top of the drain;
  - `piece_verified` never incremented `verified_bytes` (progress
    pinned at 0.0 %) and never told the picker — `piece_passed`,
    `hashing_done`/`hashing_started`, `is_hashing`/`have_piece`
    guards wire the full `on_disk_write_complete` → `async_hash` →
    `piece_finished` chain without re-hash storms;
  - `piece_picker` bucket cascade could read `m_pieces` out of
    bounds on a stale `elem_index`/stale priority hint —
    `bucket_of` re-derives the bucket from the boundaries and
    inconsistent entries mark `dirty` for a rebuild instead of
    panicking;
  - `check_request_timeouts` snubbed peers but never freed dead
    requests — ported `snub_peer`: wipe the unsent queue, mark the
    last eligible in-flight block `timed_out` + `abort_download` so
    another peer re-picks it (the 99 % end-game stall);
  - request bookkeeping: `requested_ms` re-armed per block
    received, RTT samples on `incoming_piece`, `reqq` from the
    extended handshake caps `max_out_request_queue`, AIMD
    `desired_queue_size` + slow-start exit per `second_tick`,
    endgame flag propagated to the peer pipeline;
  - defaults aligned on `settings_pack.cpp`: `connections_limit`
    200, `active_limit` 500, `max_allowed_in_request_queue` 2000,
    `max_peerlist_size` 3000, `alert_queue_size` 2000,
    `max_queued_disk_bytes` 100 MiB, `connection_speed` 30;
  - wire `Request`/`Cancel` lengths were always `0x4000` — the last
    (partial) piece must be clamped to `piece_size - start`
    (upstream `send_block_request`); strict peers (rqbit) drop the
    connection on the out-of-range request — this was the repeated
    disconnect + stall at 80–99 % in loopback transfers;
  - `delete_files` never ran: `remove_torrent` erased the storage
    map entry synchronously before `async_delete_files` could reach
    it — `RemoveTorrent` is now a queued fence job (upstream
    `disk_io_thread::remove_torrent` semantics), and
    `Session::delete_files` enqueues the delete job *before* the
    teardown so pending writes then `delete_files` run in order;
  - CLI: injects `SystemResolver` + DHT bootstrap routers.

### Added
- Peer wire niceties: engine sends keepalives after 60 s of idle
  upstream ~2 min `inactivity_timeout`, `graceful_pause` flag +
  `SessionHandle`-visible `set_paused_graceful` (stop issuing
  requests, drain in-flight writes; flag lifts when the last
  outstanding block request is consumed).
- `TorrentHandle::set_upload_limit`/`set_download_limit` per-torrent
  rate limits (per-ih `RateLimiter` in the engine pump), global
  `upload_rate_limit`/`download_rate_limit` session settings.
- Super-seeding: initial `Bitfield`/`Have`-per-piece advertisement
  on `peer_connected`, `superseed_piece` per peer, `have` on the
  super-seeded piece picks the next rarest candidate.
- BitTorrent v2 session support: per-file merkle trees on `Torrent`,
  `async_hash` returns per-block SHA-256s, `verify_v2_blocks` via
  `HashPicker.set_block_hash`, `hash_request`/`hashes`/`hash_reject`
  wire messages (serve + add + throttled `maybe_send_hash_request`),
  sparse-tree fastresume round-trip, v2-only `add_torrent` fix.
- `torrent_handle::flags`/`set_flags`/`unset_flags` (masked
  `TorrentFlags` apply incl. `PAUSED`/`AUTO_MANAGED`/`SEQUENTIAL`/
  `UPLOAD_MODE`/`SHARE_MODE`), `trackers`/`add_tracker`/
  `replace_trackers`, `ban_peer` (+ `peer_ban_alert`, swarm ban +
  live disconnect), `set_max_connections`/`set_max_uploads`.

### Added

- **Streaming — `set_piece_deadline`/`read_piece`** —
  `Torrent::time_critical` (deadline + `alert_when_available`),
  bump `top_priority`, premier critique → `cancel_non_critical`
  (`Cancel` wire + `abort_download` sur requêtes hors-critiques),
  `PickerOptions::TIME_CRITICAL` dans `drive_peer`, à la vérif →
  `ReadPiece` → `async_read` pièce entière → `read_piece_alert`
  avec le payload (`ReadPieceFailed` sur erreur). API
  `TorrentHandle::{set_piece_deadline,reset_piece_deadline,read_piece}`.

- **File d'attente auto-managed** — `Torrent::queue_seq`
  (`m_sequence_number`, assigné à l'ajout), `Session::queue_position`
  (rang dans sa classe download/seed, -1 si pause/non-auto-managed),
  `queue_position_set` (Top/Bottom leap-frog, Up/Down swap voisin).
  `TorrentHandle::queue_position`/`queue_position_set`.

- **Force-ops + flush** — `Session::force_announce(ih, idx)`
  (effets `TrackerAnnounce` immédiats, un ou tous trackers),
  `force_dht_announce` (`dht_announce_one` factorisé — respecte
  `private`/anonymat, ignore l'intervalle), `flush_cache` →
  `async_release_files` → `DiskCompleted::ReleaseFiles` →
  `cache_flushed_alert`. API `TorrentHandle::{force_announce,
  force_dht_announce,flush_cache}`.

- **`save_resume_data` pipeline** — `Torrent::need_save_resume`
  (marqué par pièce vérifiée, priorités, métadonnées, verdict de
  check), `Session::save_resume_data(ih, only_if_modified)` poste
  `save_resume_data_alert` avec le blob bencode relisible par
  `read_resume_data`, flush périodique `save_resume_interval`
  (défaut 60 s) dans `tick`. `TorrentHandle::save_resume_data`.

- **Limites de débit par torrent** — `Torrent::{upload,download}_limit`
  (bytes/s, 0 = illimité) + `TorrentHandle::set_{up,down}load_limit`;
  l'engine maintient un `RateLimiter` par info-hash
  (`torrent_up_limiters`/`torrent_down_limiters`, créés/sync au tick,
  `acquire` sur les payloads `Piece` émis ET reçus — back-pressure
  TCP côté lecture comme upstream).

- **API `torrent_handle` complétée** — `scrape_tracker`,
  `get_peer_info`, `piece_availability`, `get_download_queue`
  (partial pieces upstream `partial_piece_info`), `connect_peer`
  (candidat de dial immédiat). `TorrentStatus.error` alimenté par
  `Torrent::last_error` (échecs d'annonce tracker, vidé au succès).

- **Ops stockage (`move_storage`/`rename_file`/`delete_files`)** —
  entrées `Session` + nouvelles variantes `DiskCompleted`
  (`MoveStorage`/`RenameFile`/`DeleteFiles`) routées via
  `disk_completions`/`drain_disk_completed`; alertes upstream
  `storage_moved{,_failed}`, `file_renamed{,_failed}`,
  `torrent_deleted{,_failed}`; `save_path` mis à jour au succès;
  `remove_torrent` poste désormais `torrent_removed_alert`.
  API `TorrentHandle::{move_storage,rename_file,delete_files}`.

- **Ops `torrent_handle`** — `force_recheck` (re-hash par pièce
  via `DiskHash`, `checking`/`checking_pieces` armé puis drainé par
  `on_piece_verified`), `prioritize_pieces`/`piece_priority`
  (overrides directs), `set_sequential_download` (flag +
  `PickerOptions::SEQUENTIAL` dans `drive_peer`, `atp` flag
  `SEQUENTIAL_DOWNLOAD` honoré).

- **Priorités par fichier + `file_progress`** —
  `Torrent::prioritize_files` applique les `DownloadPriority` par
  fichier puis recalcule les priorités de pièces (max des fichiers
  chevauchant chaque pièce, ordre upstream `check_resume` : fichiers
  puis `piece_priorities` explicites). `Torrent::file_progress`
  attribue les octets par fichier (`have` + blocs `Finished` du
  picker). API `TorrentHandle::prioritize_files`/`file_priorities`/
  `file_progress`.

- **`torrent_status` enrichi + `state_update_alert`** — nouveau
  module `session::status` : `TorrentState` (checking/metadata/
  downloading/finished/seeding), compteurs (total_done/wanted/
  upload/download, rates échantillonnés au tick), meilleurs scrape
  (`num_complete`/`incomplete`/`downloaded`/`last_scrape`), bitfield
  `pieces`, `distributed_copies`, `added_time`, `name`/`save_path`
  (peuplés depuis `AddTorrentParams`/`set_metadata`).
  `Session::post_torrent_updates` poste `Alert::StateUpdate`;
  `SessionHandle::status`/`torrent_statuses`/`post_torrent_updates`
  exposés. `Torrent::checking` marque `check_files` en vol.

- **Tracker scrape bout-en-bout** — `Session::scrape_tracker` émet
  `SessionOut::TrackerScrape`, l'engine `spawn_scrape` fait l'échange
  (HTTP `scrape_url` + `scrape_http`, UDP BEP-15 action 2 via
  `scrape_udp`) sur les transports injectés, `on_scrape_response`
  met à jour `num_seeds` + `scrape_{complete,incomplete,downloaded}`
  et poste `scrape_reply`/`scrape_failed`. Correction d'un bug :
  `AlertManager::default()` avait mask=0/queue=0 (tout droppé) —
  défauts upstream `all_categories`/`1000`.

- `.session` persiste `settings` — `SessionSettings` sérialisé en
  dict bencode (`save_settings`/`read_settings`, durées en ms,
  champs manquants → défauts, compatible avant/arrière) et
  appliqué par `Session::load_state` comme upstream.

### Added

- **DHT announce bout-en-bout** — `Session::dht_announce_tick`
  (15 min, non-privé, hors anonymat) lance `get_peers` sur chaque
  famille ; `DhtEvent::Done` emporte désormais les `write_tokens`
  `(id, ep, token)` et la session émet `announce_peer` vers les
  nœuds token-holders (phase 2 `dht::announce` upstream). Les
  peers découverts entrent dans `peer_list` (source DHT).
- `new_lookup` sème aussi `router_nodes` (observers no-id,
  `flag_initial`) comme `find_data::start` upstream.

### Added

- **UPnP câblé** — `Engine::portmap_add_device(RootDevice, port,
  desc)` enregistre un `UpnpDriver` par routeur, mappe le port
  d'écoute TCP+UDP et enchaîne `GetExternalIPAddress` →
  `AddPortMapping` ; les posts SOAP passent par le connecteur +
  résolveur injectés (`http_conn`), `ip_voter` alimenté
  (`SOURCE_ROUTER`), codes d'erreur 725/718/501/716 gérés par le
  driver. Désactivé sous `anonymous_mode`.

### Added

- **NATPMP câblé** — `Engine::start_natpmp(gateway, port, ttl)`
  envoie les mappings TCP+UDP + requête IP publique sur la socket
  datagramme injectée ; démux des réponses par adresse passerelle
  (upstream lie une socket dédiée), `ip_voter` alimenté
  (`SOURCE_ROUTER`), renouvellement à mi-lease dans `tick`.
  Désactivé sous `anonymous_mode` comme upstream.

### Added

- **Choker câblé** — `Session::unchoke_tick` toutes les 10 s :
  `unchoke_sort` (FixedSlots/RoundRobin) sur les compteurs payload
  `PeerConn` (`stat_up/down_total`+round, crédités aux `Piece`
  émis/reçus), envoie `Choke`/`Unchoke`, réinitialise les
  compteurs de round et `time_of_last_unchoke`.

### Added

- `ip_voter` câblé — votes `external_ip` depuis les trois sources
  upstream : clé `ip` des messages DHT (`SOURCE_DHT`, drainée via
  `DhtNode::drain_ip_votes`), `external ip` des réponses tracker
  (`SOURCE_TRACKER`), `yourip` des handshakes BEP-10
  (`SOURCE_PEER`, via `PeerEffect::YourIp` → `TorrentEffect::YourIp`).
- `.torrent` `nodes` → `SessionOut::DhtAddNode` : résolution via le
  résolveur injecté puis `dht_add_node` seed `router_nodes`
  (supprimé en `anonymous_mode` comme upstream).

### Added

- `proxy.rs` — `ProxySettings` (`settings_pack::proxy_*`) +
  `SocksConnector`/`HttpConnectConnector` : des `PeerConnector`
  qui décorent le transport injecté (dial du proxy via `inner`
  puis handshake SOCKS4/5 ou `CONNECT`) — aucune socket directe,
  compatible lanes anonymes.
- `SessionHandle` : `add_torrent_params`, `save_state_buf`,
  `load_state`, `session_stats`, `set_paused` — l'API publique
  couvre maintenant resume/magnet/persistence/stats.

### Added

- **Web seeds bout-en-bout** : `Torrent.web_seeds` (BEP-17/19
  `url-list`/`ws=`) alimente un pair virtuel `WEB_SEED_PEER` —
  `inc_refcount_all` (le seed a tout), `next_web_seed_blocks`
  pioche via le picker (mark `downloading`), `Session::tick` émet
  `SessionOut::WebSeedFetch` (`web_seed_blocks_per_tick`),
  `Engine::spawn_web_seed` fait `build_piece_request`/`fetch_block`
  via le connecteur+résolveur injectés, `on_web_seed_block` repasse
  par le chemin `DiskWrite`→hash, `on_web_seed_error` libère les
  blocs (`abort_download`).

### Added

- `Session::save_state`/`load_state` — le fichier `.session` :
  resume data par torrent (`to_resume_data` embarque le blob `info`
  via `metadata_raw`), état DHT v4+v6 fusionné ; `load_state`
  réinjecte via `add_torrent_params` et sème `router_nodes`
  (`bootstrap` upstream). `DhtNode::dht_state`/`load_state`,
  `DhtTracker::get_node_mut`, `Session::session_stats` snapshot.

### Added

- `RateLimiter` (token bucket async partagé, équivalent
  `bandwidth_channel`) câblé sur le chemin d'upload : les writers
  throttlent les payloads `Piece` selon
  `settings.upload_rate_limit`, recharge 1 Hz au tick.

### Added

- `peer_timeout` au tick : `PeerConn::last_active` (en
  `session_time`) est rafraîchi à chaque message entrant ; un pair
  silencieux plus de `peer_timeout` s est déconnecté
  (`second_tick` upstream).

### Added

- `Engine::start_acceptor` (`listen_socket` accept pump) : une tâche
  par accepteur — chaque stream inbound fait le handshake répondeur
  (info-hash filtré par le set partagé `known_ihs`, rafraîchi au tick)
  puis rejoint `connect_rx` comme un dial. `PeerAcceptor::accept`
  retourne `(stream, Option<SocketAddr>)` — `None` pour les
  transports non-IP (I2P/SAM).

### Added

- `settings_pack::anonymous_mode` : coupe l'émission LSD (déviation
  OnionBit documentée — upstream ne masque que `ip`/user-agent des
  announces ; broadcaster les info-hashes sur le LAN trahirait
  l'anonymat Tribler).
- `Session::remove_torrent`/`set_paused(true)` émettent l'annonce
  `stopped` sur les trackers ayant vu `started` (`~torrent` /
  `do_pause` upstream).

### Added

- Boucle `Engine::run` (`session_impl::run`) : tick 1 Hz +
  `connect_pump` + dials async. `spawn_dial` fait le handshake BT +
  BEP-10 en tâche et rapporte via `connect_rx` (`on_connection_attempt`
  → `peer_connected`/`peer_connect_failed`). `peer_list_state()`
  construit le `torrent_state` depuis `Session::settings` (nouvelles
  clés `min_reconnect_time`, `max_peerlist_size`, `max_failcount`,
  `allow_multiple_connections_per_ip`).

### Added

- `Session::set_ip_filter` (`session_impl::set_ip_filter` /
  `peer_list::apply_ip_filter`) : les pairs bannis déconnectés sont
  effacés, les connexions vivantes vers des IP filtrées sont coupées
  (`ip_filter_updated` upstream).

### Added

- `identify_client` (`src/identify_client.cpp`) : 4 styles de peer-id
  (azureus, shadow, transmission legacy, mainline) + 102 codes
  `name_map` + 37 `generic_mappings` verbatim ; `peer_info.client`
  tombe sur le fingerprint quand le `v` de l'ext-handshake est absent
  (`peer_connection::peer_info_struct` upstream).

### Added

- Cycle de connexion peer_list fidèle : `connect_candidates` marque
  `set_connected` au début du dial (comme le ctor `peer_connection`
  upstream), `peer_connect_failed`/`peer_disconnected_addr` appliquent
  `connection_closed` + `inc_failcount` avec le backoff
  `min_reconnect_time*(failcount+1)`. `Session::session_time`
  (`m_session_time`) alimente l'époque de reconnexion.

### Added

- LSD (BEP-14) côté session : `Session::lsd_tick` (annonces multicast
  v4+v6 toutes les 5 min, port d'écoute, cookie) et réception dans
  `Session::on_udp` → `peer_list.add_peer` avec `PeerSourceFlags::LSD`.
  `Torrent::private` (BEP-27) désactive l'émission LSD et l'ajout de
  pairs LSD/PEX — fidèle à `torrent::is_private` upstream.

### Ajouté
- `ut_pex` câblé : `PeerConn.remote`/`pex_sent`, `Session::peer_connected` prend l'endpoint, dispatch entrant sur `local_ut_pex`/`local_ut_metadata` (ids qu'on annonce vs ids du remote), `Torrent::on_pex_packet` → `peer_list`, `pex_tick` (diff 60 s vers les pairs ut_pex).
- Dispatch BEP-10/9/11 dans `PeerConn` : handshake étendu parsé (`ext_ut_metadata`, `ext_ut_pex`, `metadata_size`, `upload_only`, `v`), effets `ExtHandshake`/`MetadataPacket`/`PexPacket`.
- `Torrent` : `on_metadata_packet` (sert notre info dict via `metadata_raw`, rejette sans métadonnées), `metadata_tick`/`metadata_tick_peer` (demande la prochaine pièce aux pairs ayant `ut_metadata`), `set_metadata` conserve le dict brut ; `Session::tick` appelle `metadata_tick`. Magnet résolu e2e (handshake → request → piece → `set_metadata`).
- `UtpAcceptor` : `PeerAcceptor` uTP — `UtpManager` route les ST_SYN orphelins vers une file d'écoute (`set_listener`), `accept_syn` crée le socket répondeur (`send_id` = conn-id du SYN, `recv_id` +1 — sémantique upstream), pompe dédiée, handshake+echo testés en duplex complet sur datagrammes injectés.
- `Engine::start_udp_pump` : pompe datagramme → `session.on_udp` (DHT, réponses tracker UDP) ; `Session::enable_dht` (`start_dht` upstream) — round-trip ping DHT testé de bout en bout via socket injectée.
- `PeerConnector::connect_dest` : connexion par destination texte (`host:port` par défaut ; surchargeable pour les adresses non-IP).
- `I2pConnector`/`I2pAcceptor` : `PeerConnector`/`PeerAcceptor` sur le bridge SAM — `open_session` (`SESSION CREATE` STREAM), `connect_dest` fait `NAMING LOOKUP` (socket dédiée) puis `STREAM CONNECT` (socket dédiée), accept via `STREAM ACCEPT`.
- `bitdaemon-net::ws_tracker` : annonces `ws://`/`wss://` WebTorrent (JSON `announce` sur frames WS) — `build_announce`/`build_answer`/`parse_response` verbatim (latin-1↔UTF-8, `key` %08X, events completed/started/stopped/paused), `announce_ws` (upgrade + validation `Sec-WebSocket-Accept` + un frame aller-retour, Pong auto). Câblé dans `Engine` pour les URLs `ws`/`wss`.
- `web_seed::fetch_block` : driver async — écrit les GET `Range` de `build_piece_request` et lit une réponse HTTP par fichier non-pad (`got_get`), vérifie statut 2xx et longueur.
- `Engine` : résolution des hostnames de trackers via `Resolver` injecté (`set_resolver` — jamais de DNS direct ; les littéraux IP passent sans résolveur), URLs parsées via `parse_url_components`, ports par défaut http/https, annonces UDP résolues aussi.
- `bitdaemon-net::resolver` : trait `Resolver` injectable (`async_resolve` upstream), `resolve_host` (raccourci littéral IP), `SystemResolver` pour les déploiements non anonymes.
- `http_conn` : `Accept-Encoding: gzip` émis dans chaque requête et corps `Content-Encoding: gzip|x-gzip` décodé via flate2 (announces compressés).
- Pont uTP async (`bitdaemon-net::utp_stream`) : `UtpConnector` implémente `PeerConnector` sur `DatagramSocket` injecté — `UtpStream` en `AsyncRead`/`AsyncWrite` (canaux), pump qui draine l'outbox + tick périodique, routage des datagrammes entrants par conn_id ; le répondeur émet le STATE différé (`send_deferred_ack`) comme upstream.

### Added — Engine (session_impl async)

- `bitdaemon::engine::Engine` — boucle `tick` : draine messages
  entrants, complétions announce, disconnects, `session.tick` +
  `tracker_announce_due`, dispatch `SessionOut` (ToPeer→writer,
  UdpPacket→DatagramSocket, TrackerAnnounce→task HTTP/UDP).
- `connect_peer` — dial via `PeerConnector` injecté, handshake BT
  (ou MSE `crypto_provide` PLAINTEXT|RC4), `PeerConn` enregistré,
  stream split reader/writer tasks.
- `announce_udp` : `rand::random` à la volée (futur `Send` pour
  `tokio::spawn`).

### Added — tracker announce pump

- `Torrent.trackers` (`TrackerList` tiers) alimenté par
  `AddTorrentParams.trackers` + `add_tracker`.
- `Torrent::announce_params` (info_hash/peer_id/left/event/key).
- `Session.peer_id` (`generate_peer_id "-OB0001-"`), `listen_port`,
  `key` ; `SessionOut::TrackerAnnounce` émis par
  `tracker_announce_due` ; `on_announce_response` : pairs→
  `peer_list` (TRACKER), `complete`→`num_seeds`, `trackerid`
  perso, échec → backoff quadratique + `TrackerError`.

### Added — check_files wiring

- `Session::check_files` → `DiskIo::async_check_files` →
  `DiskCompleted::CheckFiles` → `Torrent::on_files_checked`
  (`torrent::files_checked` upstream) : resume accepté → seed ;
  rejeté (`need_full_check`) → `have` réinitialisé.

### Added — add_torrent entry point

- `Torrent::from_params` + `Session::add_torrent_params` — ctor
  fidèle : `ti` → tailles/hashes/picker, resume appliqué
  (`apply_resume_data`, jobs `DiskHash` dispatchés), `peers`→
  RESUME_DATA dans `peer_list`, `banned_peers` bannis, `PAUSED`,
  magnet sans `ti`.

### Added — PeerList × Torrent

- `Torrent.peer_list` + `add_swarm_peer` (feed tracker/PEX/DHT/LSD)
  + `connect_candidates` (pompe de connexion session, `is_finished`
  synchronisé).
- `TorrentPeer.connection` passe en `Arc<dyn PeerStats + Send + Sync>`
  — `Session` reste `Send`/`Sync` pour `SessionHandle`.
- Fix `PeerList::default()` : `max_failcount = 3` (défaut upstream)
  au lieu de 0 (aucun candidat).

### Added — peer_info

- `peer_info.rs` — snapshot public `peer_info` (flags
  `PeerFlags`/`ConnectionType`/`BandwidthState` verbatim) +
  `Torrent::peer_info` (peuplé depuis `PeerConn`/`PeerPipeline` —
  champs non trackés documentés à 0).

### Added — fastresume glue

- `Torrent::to_resume_data` / `apply_resume_data` — port fidèle de
  `write_resume_data`/`check_resume` : bitfields verified/have,
  `piece_priorities`, `unfinished_pieces` (bitmask des blocs
  Finished) → `mark_as_finished` par bloc ; pièce complète →
  `TorrentEffect::DiskHash` (`verify_piece`) routé vers
  `DiskIo::async_hash`.
- `PiecePicker::blocks_for_piece` public (accès fenêtre block_info
  pour le dump).

### Added — peer_list

- `peer_list.rs` — swarm list fidèle : `add_peer`/`update_peer_port`,
  `find_connect_candidates` (round-robin 300, top-10 `compare_peer`,
  élagage à 95 % de `max_peerlist_size`), `erase_peers` (force-erase
  115 %), `apply_ip_filter`, `ban_peer`, `primary_source`/`source_rank`.
- `enum_net.rs` + `ip_helpers::is_global` (vague précédente).

### Added — peripheral ports (enum_net / i2p / websocket / tracker client)

- `enum_net` — `build_netmask` / `match_addr_mask` / `get_gateway` /
  `has_internet_route` / `has_any_internet_route` / `device_for_address`;
  `ip_helpers::is_global`. Interface + route tables injected.
- `i2p.rs` — SAM handshake (HELLO, SESSION CREATE transient, STREAM
  CONNECT/ACCEPT, NAMING LOOKUP) over `BoxedPeerStream`.
- `websocket.rs` — RFC 6455 upgrade builder/validator, accept-key
  (spec vector), frame encode/decode (masked + extended lengths).
- `tracker_client.rs` — full announce exchanges: UDP BEP-15 connect +
  announce with `DatagramSocket`, HTTP GET + bencoded response parse;
  `TrackerList`-integrated e2e tests.
- Typed `Alert` variants replacing `Alert(String)` (peer/torrent/session
  lifecycle + errors).

### Added — session wave 4: piece-hash verify, peer classes, counters

- `Torrent::piece_hashes` + `expected_hash` — Hash job completion
  compares SHA-1 like `torrent::piece_finished` (mismatch → fail).
- `net::peer_class` — `PeerClass` bandwidth channels/priorities/
  limits, `PeerClassPool` slot-stable registry, `PeerClassSet`
  8-slot dedup (`peer_class.cpp` port).
- `session::counters` — `Counters` atomic bank (`stats_counter_t`
  subset) + `SessionStats::snapshot`; wired into `Session` for
  incoming message classes and connected-peer gauge.


### Added — session wave 3: disk-job bridge

- `session::Session::dispatch_disk` — `TorrentEffect::DiskRead`/
  `DiskWrite` translated into `async_read`/`async_write` on the
  injected `DiskIo` (`m_disk_thread` glue).
- `DiskCompleted` channel — job callbacks (any thread) push into
  an mpsc the session drains on `tick`: Read → `Piece` to the
  requesting peer, Write → `async_hash` when the picker reports the
  piece finished, Hash → `piece_verified` → have broadcast/seed.
- `StorageError::is_ok`/`is_err` — the `default()` success sentinel
  upstream relies on (`!error`).
- `DiskBufferHolder::from_owned` — pool-less holder for incoming
  peer data.
- `Torrent::piece_complete` — `is_piece_finished` bridge.


### Added — session wave 5: UPnP SOAP core

- `net::upnp` — `upnp.cpp` core: verbatim AddPortMapping/
  DeletePortMapping/GetExternalIPAddress SOAP bodies, `SOAPAction`
  header, `find_control_url` device-description parse (WANIP/PPP
  services), `GetExternalIPAddress` + SOAP fault `errorCode`
  parsing. SSDP/HTTP transport stays injected (ADR-0008).

### Added — session wave 4: magnet glue, auto-managed queue, smart_ban wiring

- `Torrent::set_metadata` — BEP-9: SHA-1 of the raw info section
  must equal `info_hash`; on match `TorrentInfo::from_info_node`
  fills `num_pieces`/`piece_length`/`piece_hashes` and inits the
  picker (`init_picker` now reachable from magnet-added torrents).
- `Torrent::on_ut_metadata_data` / `metadata_request` — glue over
  `ut_metadata::UtMetadataTracker` (source bookkeeping per piece).
- `Session::manage_queue` — auto-managed promotion/demotion against
  `active_downloads`/`active_seeds` (`Torrent::queued` is separate
  from user `paused`, like upstream `is_torrent_queued`).
- `Torrent::piece_verified_ban` — wires `smart_ban` into
  `piece_finished`/`on_piece_failed`: per-block CRC record on
  failure, re-check on success, `DisconnectPeer` effects on bans.

### Added — session wave 3: MSE/PE crypto over the wire driver

- `net::mse` — `bt_peer_connection` PE1–PE4 handshake verbatim:
  DH-768 exchange, `synchash` scan (PadA ≤512), `hash('req2'|SKEY) ^
  hash('req3'|S)` skey field, RC4 `[VC][provide][len_pad][pad][len_ia]`
  cryptofield, responder-side `crypto_select` + piggybacked IA.
- `net::bt_peer_conn` encrypted path — `run_handshake_encrypted_*`
  (initiator/responder) + `read`/`write_message_rc4`; BT handshake
  and messages flow through the negotiated RC4 handler, plaintext
  `crypto_select` keeps the plain wire. All over `BoxedPeerStream`.

### Added — session wave 2: wire driver, handles, session-state, web seeds, NAT-PMP

- `net::bt_peer_conn` — `bt_peer_connection` driver: `run_handshake`
  (send+validate 68 B, reserved bits, info_hash check), extended
  handshake send/read, `read_message`/`write_message` pump — all over
  the injected `BoxedPeerStream` so onion lanes carry peers verbatim.
- `session::session_handle` — `SessionHandle`/`TorrentHandle`
  (`session_call` equivalent): add/remove/find torrents, statuses,
  pop_alerts, peer-message feed, tick; poison-tolerant Mutex.
- `session::session_state` — `.session` file: bencoded
  `{torrents: {ih: resume-blob}, dht: …}` round-trip reusing
  `write_resume_data_buf` verbatim per swarm.
- `net::web_seed` — `web_peer_connection` core: `build_piece_request`
  (BEP-19 `GET path HTTP/1.1` + `Range: bytes=` verbatim, multi-file
  via `map_block`, pad files skipped), `add_headers` + `consume_body`.
- `net::natpmp` — NAT-PMP codec: `build_map_request` (12-byte
  datagram), public-IP request, `parse_response`, result strings.


### Added — session & torrent glue (Phase 6 wave 1)

- `session::torrent` — `Torrent` per-swarm state machine: peer registry,
  picker refcount glue (bitfield/have/have-all/peer-lost), `piece_verified`
  → have broadcast + seed transition + failed-piece restore,
  `drive_peer` → `request_a_block` scheduling (`torrent.cpp` subset).
- `session::session` — `Session` synchronous core: torrent map keyed by
  info_hash, `on_peer_message`/`peer_connected`/`peer_disconnected`/
  `on_piece_verified`, `tick` driving all peers + DhtTracker outbox
  (injected UDP), `SessionOut` effect routing (`session_impl` subset).
- `session::alerts` — `Alert` payload enum + `AlertCategory` bitmask
  verbatim + `AlertManager` (mask, queue limit, drop counter, notify
  hook), `SessionPlugin` extension trait (`alert_manager` subset).
- `session::settings` — `SessionSettings` typed struct with upstream
  `default_settings()` values; `SessionParams` with injected
  `Arc<dyn DiskIo>` (the OnionBit `StorageFactory` seam).
- `session::tracker_list` — `AnnounceEntry`/`TrackerList` with
  upstream quadratic retry backoff (`5s + fails²·5s·ratio/100`, capped
  60 min), `can_announce` (due/min_announce/fail_limit), tier-ordered
  failover + URL dedup (`announce_entry.cpp`/`tracker_list.cpp`).
- `session::choker` — `unchoke_sort` verbatim: fixed-slots and
  rate-based slot calculation, round-robin (quota-complete at
  `piece_length·quota` after 60 s) / fastest-upload / anti-leech
  comparators, `nth_element` top-slots partition on `ChokePeer`
  snapshots (`choker.cpp`).
- `piece_picker::NullPickerStats` — no-op `PickerStats` for sessions
  without a stats sink.


### Added

- `session::peer_conn::dispatch` — incoming wire message router
  (`peer_connection::incoming_*`): choke drains unsent requests with
  picker aborts, bitfield size validation, have/dont-have refcount
  effects, request validation (`validate_piece_request`), upload
  queue, piece → mark+write+re-pick, reject frees the block

### Added

- `dht`: write phase — `announce_peer`/`put` (immutable+mutable
  BEP-44 signed, verified on receipt) issued to write-token nodes
- `dht::dht_tracker` — per-family node mux (v4/v6), shared tick,
  5-min write-token secret rotation, injected transport

### Added

- `dht::lookups` — specialized traversals (`find_node`, `get_peers`,
  obfuscated `get_peers`, `get`, `sample_infohashes`, `put`) with
  reply extraction (token/values/nodes/samples/BEP-44 fields)
- `dht::dht_node` — full node orchestrator: query dispatch
  (ping/find_node/get_peers/announce_peer/get/put/sample_infohashes),
  BEP-42 id + write-token generation/verification, lookup pump,
  event surface — all I/O through an injected outbox, no sockets

### Added

- `dht`: iterative Kademlia `traversal` (port of `traversal_algorithm` +
  `observer`) — distance-sorted results, id dedup, /24 diversity
  (`restrict_search_ips`), router seeding, branch-factor growth on
  short timeout, 100-result cap
- `dht::rpc` — `rpc_manager` port: transaction-id dispatch by
  `(tid, endpoint)`, query construction (`y/q/a.id/t/ro/want`),
  BEP-42 `dht_enforce_node_id`, 1 s short-timeout / 15 s timeout tick,
  injected send hook (no direct sockets)

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
- `bitdaemon_net` uTP stream: `utp_socket::UtpSocket` — full port of
  `utp_socket_impl` (SYN handshake incl. the recv-id quirk, DATA/STATE/
  FIN/RESET, cumulative+SACK acks, reorder buffer via `PacketBuffer`,
  LEDBAT `do_ledbat` with fixed-point cwnd/ssthres/slow-start, MTU
  floor/ceiling probing, dup-ack fast resend, nagle hold-off,
  `packet_timeout` backoff, `tick` loss marking). Sends drain through
  a queued `outbox` delivered by the injected `DatagramSocket`
  (ADR-0008); upstream callbacks become pollable state. Tests:
  two-socket handshake/data/FIN + out-of-order reorder, e2e green.
- `bitdaemon_net` HTTP/URL layer: `http_parser` (incremental
  status/request line → header multimap → body; content-length,
  content-range with `strtoll` semantics, connection-close, chunked
  ranges + tail headers, `parse_chunk_header`, 100-continue restart)
  and `parse_url` (`parse_url_components` verbatim grammar — auth,
  v6 literals, LDH check, path `/`-injection — `split_url`,
  `same_origin`, `is_valid_tracker_url`, `is_idna`,
  `has_tracker_query_string`). `socks_udp` (SOCKS5 datagram
  wrap/unwrap, FRAG rejected, atyp 1/3/4). `ip_helpers` +
  `ip_voter` (`cast_vote`/`maybe_rotate` verbatim — bloom-keyed
  voter identity, 40-entry cap with upstream's stable-sort eviction,
  25-vote provisional / 50-vote or 5-min rotation, 2/3 majority
  rule).
- `bitdaemon_dht` — storage + crypto: `dht_storage`
  (announce/get_peers incl. scrape blooms + write-token gate,
  BEP-44 items with canonical_string verbatim + ed25519-dalek,
  BEP-51 infohashes sampling, tick expiry), `msg`
  (`verify_message` schema walker), `item`, `ed25519`,
  `dos_blocker` (10 s window → 5 min ban).
- `bitdaemon_dht` — first DHT modules: `node_id` (BEP-42
  generate/verify + secret ids + distance/prefix masks),
  `node_entry` (rtt EMA, fail-count, confirmed semantics),
  `dht_settings` (22-field struct + session-file dict),
  `dht_state` (nids/nodes save-load), `routing_table` (k-buckets
  with split, prefix-spread replacement, CIDR dedup, find_node
  k-closest, next_refresh).
- `bitdaemon_session::ut_metadata` (BEP-9) — message codec +
  `UtMetadataTracker` (download buffer, least-requested piece
  selection, 3 s throttle, `max_metadata_size` validation,
  hash-failure source penalties).
- `bitdaemon_session::ut_pex` (BEP-11) — pex message build/parse:
  `added`/`added.f`/`dropped` ×4 (v4/v6), flag constants,
  `pex_lt_v2` stripping, `max_peer_entries`/`max_pex_peers` caps.
- `bitdaemon_session::smart_ban` — `smart_ban` core ported: per-block
  CRC map keyed by `PieceBlock` (shared `common::types`), ban on
  differing digest from the same peer across failures and on stored
  CRC mismatch after a piece passes; disk reads injected as closures,
  decisions returned as `BanDecision` events.
- Tests: 67 conformance cases ported from `test_bdecode.cpp`, 16 from
  `test_bitfield.cpp`, 8 from `test_sliding_average.cpp`, 5 from
  `test_bloom_filter.cpp`, `test_crc32.cpp` vectors (RFC 3720 B.4),
  `test_sha1_hash.cpp` shift/bitops/leading-zero vectors, `stat` formula
  checks — all green; `clippy -D warnings` clean, `cargo fmt --check`
  clean.

### Fixed — `bitdaemon-common` upstream-fidelity audit (full crate sweep)

Every module of `bitdaemon-common` was re-audited line-by-line against
`libtorrent @ v2.1.2-45-gc019e80ef`. Corrections:

- **`identify_client` is now a single canonical implementation** in
  `common::identify_client` — `core::fingerprint::identify_client`
  delegates to it (the two copies had already drifted: different digit
  checks, different `name_map` key types). Fidelity fixes vs
  `identify_client.cpp`: `parse_az_style` now rejects `id[2] < '0'`
  like upstream (and bytes ≥ 0x80, which are negative under upstream's
  signed-`char` comparisons — the whole `id[2..7]` range now uses
  signed-char semantics); `parse_shadow_style` drops the dead
  `> 127` tests (unreachable on signed-`char` builds) and
  **sign-extends** binary version fields, so `S\x85…` reports
  `Shadow -123.x.x` exactly like upstream; `parse_mainline_style`
  reproduces `sscanf "%3d"` faithfully — whitespace skip *plus*
  optional sign inside the 3-char field (`"M-4-3-6--"` →
  `Mainline -4.3.6` upstream, and now here); `lookup` binary-searches
  the raw 2-byte client code like `compare_id`/`lower_bound`.
  `Fingerprint` fields renamed to the upstream `*_version` names.
- **`ffs::log2p1(0)` returns 0**, matching the DeBruijn-table result
  upstream actually produces (the previous `-1` sentinel contradicted
  the ported doc). Full `test_ffs.cpp` vectors ported: all 22
  `count_leading_zeros` hex cases plus the `count_trailing_ones` u32
  table (host-order convention documented).
- **`platform::max_open_files` returns upstream's real constants**:
  10 000 000 for `unlimited`/cap (was `i32::MAX`), 1024 on query
  failure (was `i32::MAX`), 10 000 on non-rlimit platforms — the
  simulator's 256 is a build define, noted in `PORTING.md`.
- **`time::minutes`/`seconds32` saturate** at the i32-ms cap instead
  of overflowing into a debug panic (upstream stores minutes/seconds,
  not ms — a >35 792-minute call used to wrap).
- `LtError` message table confirmed against the **ABI-2** layout
  (mechanical diff of all 221 codes + 254-string table); the ABI-1
  bdecode strings kept at codes 190-196 are a documented deviation —
  upstream ABI-2 returns `""` there.

`docs/PORTING.md` updated: `identify_client` single-source note,
`platform_util` constant table, `ffs` DeBruijn/`test_ffs` coverage,
`version.hpp` numeric constants recorded as intentionally unported
(they identify libtorrent, not BitDaemon), `time.cpp` wall-clock
helpers recorded as unused (`SystemTime` covers the need).

[Unreleased]: https://github.com/laurentgeynet-ux/BitDeamonRust/compare/main...HEAD
- Phase C networking: `UdpSocket` adapter over `DatagramSocket`, `CachingResolver` (LRU 700, TTL 1200s, failed-DNS TTL/8, single-flight), `ssl_stream` via rustls/tokio-rustls (peer verify-none + tracker root-store + `SniResolver`), `instantiate` transport decision table, `core::xml_parse` byte-level scanner port.
- Phase B/D: BEP-6 `send_allowed_set` deterministic allowed-fast (SHA-1 addr‖info-hash), `allowed_fast_set_size` setting; DHT `bootstrap` (refresh.cpp) + `add_node` ping + `direct_request`/`DhtEvent::Direct`; `platform_util` (procfs, unsafe-free), `storage_free_list`, web-seed URL normalization (`escape_file_path`, trailing-slash rules).
- Phase F conformance infrastructure: `bitdaemon-sim` expanded (per-link latency/jitter/loss, seeded RNG, `run_to`/`drain` drivers), `fuzz/` cargo-fuzz scaffold (`bdecode`, `peer_wire`, `resume_data` targets), criterion benches (`bdecode_bench`, `wire_bench`).
- `Torrent` surface completed vs `torrent.cpp`: `abort`/`disconnect_all`, `attach_peer`, `choke_peer`/`unchoke_peer`, `num_connecting` counters, `need_picker`/`peer_is_interesting`/`ready_for_connections`/`want_tick`, `is_aborted`/`is_upload_only`/`is_ssl_torrent`/`is_predictive_piece`, `announce_dht`/`announce_lsd`/`announce_trackers` + `announcing` gates (wired into tracker/DHT/LSD ticks), `enable_all_trackers`, `announce_with_tracker`, suggest-piece list, predictive bitfield, `initialize_merkle_trees`, `recalc_share_mode`, `request_time_critical_pieces`, `set_state`, `handle_disk_error`, `on_torrent_paused`, `set_file_priority`, `queue_position_*` shortcuts, `post_status`, `get_torrent_copy_with_hashes`, `retry_web_seed`, `maybe_connect_web_seeds`, `update_want_peers`.
- `SessionHandle` completed to upstream surface: session pause/resume, `get_torrents`/`get_settings`/`session_state`/`write_session_params`, alert mask/queue-limit/notify/`wait_for_alert`, ip/port filters, peer-class CRUD + `PeerClassTypeFilter`, `set_peer_id`/`set_key`, per-purpose proxies (`SessionProxies`), `PeSettings` (new in `net::pe_crypto`), rate-limit and connection setters, `start_*`/`stop_*` service switches, DHT handle ops incl. two-phase `dht_put_item` (`dht_pending_put`) and `dht_stats_alert`, `listen_on`/`use_interfaces`/`reopen_network_sockets`, `session_stats_metrics`. `DhtStorage` now `Clone`.
- `seed_mode` flag with upstream ctor gating (no dont_download, all have_pieces) + `leave_seed_mode` recheck; `ut_holepunch` BEP-55 codec + relay decisions + `PeerEffect::Holepunch` wiring.
- `disk::disk_cache` (per-block entries, flushed/hasher cursors, force-flush + hasher-kick flags, `try_clear_piece`); `disk::storage_free_list` wired into `PreadDiskIo` slot reuse (freed at remove-job execution, behind the fence).
- `rtc_stream` (RtcChannel seam + AsyncRead/Write + `rtc_parse_endpoint`); `bitdaemon-sim` crate: virtual clock, `SimNet` delivery queue, `SimSocket` DatagramSocket; session polls `IpNotifier` → `SessionOut::InterfacesChanged`.
