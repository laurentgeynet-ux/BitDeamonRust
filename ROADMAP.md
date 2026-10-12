# ROADMAP — libtorrent → Rust port (complete-fidelity edition)

Port of **all of** libtorrent `master` @ `v2.1.2-45-gc019e80ef`
(~170k LOC C++: ~114k `src/*.cpp` + `src/kademlia/*`, ~55k
`include/**/*.hpp`) to 100% safe Rust — every behavioral file ported,
every gap explicitly reasoned. See [ADR-0010](docs/adr/0010-complete-port.md).

Live file-level status: [`docs/PORTING.md`](docs/PORTING.md).
Definition of done per module: `AGENTS.md` §7.

**Completion metric**: `PORTING.md` resolves to 100%
`ported`/`verified`/`won't port`(+reason) — zero `in progress`,
`partial`, `started`, `not started` rows.

Legend: ⬜ not started · 🟨 in progress · ✅ done

---

## Scope rule (what "everything" means)

Every `src/*.cpp` + `src/kademlia/*.cpp` file maps to a Rust module,
**or** a `won't port` row with a stated reason. Permanently excluded:

- build system (`Jamfile*`, `CMakeLists.txt`, `cmake/`),
- `bindings/` (Python FFI),
- vendored C crypto: `src/ed25519/*`, `puff.cpp`, `sha1.cpp`,
  `sha256.cpp`, `win_cng` → RustCrypto crates (ADR-0007),
- C++/boost container & platform shims (`win_*`, `stack_allocator`,
  `heterogeneous_queue`, `tailqueue`, `copy_ptr`, `assert.cpp`,
  `invariant_check`, …) → std/idiomatic equivalents.

Everything else is in scope — including what was previously deferred
for OnionBit: SSL torrents, WebRTC, `ut_holepunch`, `i2p_pex`,
`peer_class`, `simulation`, RSS, and the `mmap` backend (new
feature-gated `bitdaemon-mmap` crate, sole `unsafe` carve-out —
ADR-0010 §3).

---

## Phase A — Ledger reconciliation ✅

Before new porting, make PORTING.md exact.

- [x] Sweep every `src/*.cpp` + `include/**/*.hpp` upstream file
      against PORTING.md; add missing rows (`ffs.cpp`, `truncate.cpp`,
      `version.cpp`, `xml_parse.cpp`, `rtc_*`, `stack_allocator.cpp`,
      `posix_disk_io.cpp`, `kademlia/refresh.cpp`, `dht types.hpp`,
      `client_data.hpp`, `fwd.hpp`, …).
- [x] Resolve inconsistent statuses (DHT `dht_storage`/`item`/
      `sample_infohashes`/`dos_blocker` marked ported in ledger vs
      "remaining" in old roadmap — verify code, fix one or the other).
- [x] Every `won't port` row gets an explicit reason.
- [x] `rust-toolchain.toml` + `cargo deny` config (ADR-0007/0010 —
      `cargo deny check` green: advisories, bans, licenses, sources).
- [x] Golden-vector generator (C++ program on the upstream checkout
      dumping bencode/peer-wire/resume fixtures) — carried from
      Phase 0, needed by Phase F differential tests.

**Exit**: PORTING.md is a complete, self-consistent inventory of the
upstream tree.

---

## Phase B — Session completeness ✅

Biggest remaining surface. `session_impl` parity.

`bitdaemon-session`:

- [x] `peer_class.cpp` / `peer_class_set.cpp` /
      `peer_class_type_filter.hpp` — bandwidth classes, filters
- [x] `torrent_handle.cpp` full ~90-method surface;
      `torrent_status.cpp` complete fields (incl. `info_hashes`,
      `next_announce`, `completed_time`, `last_seen_complete`,
      `is_seeding`); `torrent_list.hpp`
- [x] `peer_connection.cpp` full state machine +
      `peer_connection_handle.cpp` (verified on wire — PORTING.md)
- [x] `peer_list.cpp` / `peer_info.cpp` / `peer.cpp`
- [x] `smart_ban.cpp` session wiring (`SmartBan` per torrent,
      hash-fail bookkeeping → `ban_peer`)
- [x] `torrent_peer_allocator.cpp` (slab/arena)
- [x] `performance_counters.cpp` full counter surface (318-entry
      `stats_counter_t`/`stats_gauge_t` index set verbatim)
- [x] `session.cpp` / `session_impl.cpp` / `session_handle.cpp` /
      `session_call.cpp` / `session_params.cpp` — finish: listen
      sockets, `listen_socket_handle.cpp`, `session_udp_sockets`
      glue, `ip_notifier.cpp` / `enum_net.cpp` / `platform_util.cpp`
      (injected iface enum), `disk_observer` → channel events
- [x] `i2p_pex.cpp`, `extensions.hpp` plugin registry
- [x] `seed_mode` / `share_mode` corner cases; tracker retry parity
      details; `web_seed_entry` getright/holtb URL types (BEP-17/19)
- [x] `error_code.cpp` remaining `lt::errors` categories;
      `close_reason.cpp`; `version.cpp`; `string_util.cpp`
- [x] `ffs.cpp` (file-piece spread) if still present upstream at pin

**Exit**: `test_session`-equivalent coverage green; `TorrentHandle`
API surface complete; session stats match upstream counters.

---

## Phase C — Net completeness ✅

`bitdaemon-net`:

- [x] `udp_socket.cpp` full wrapper (`SessionUdpSocket` demux)
- [x] `resolver.cpp` caching layer (`Resolver::set_cache_timeout`)
- [x] `instantiate_connection.cpp` / `socket_type.cpp` finish
- [x] `ssl.cpp` / `ssl_stream` → **rustls** (ADR-0007): SSL torrents,
      HTTPS trackers, cert pinning, `ssl` context settings
- [x] `gzip.cpp` → `flate2` cabled outside HTTP too; `xml_parse.cpp`
      (RSS); `web_seed_entry` polish
- [x] SOCKS5 UDP-associate (`udp_socket` over SOCKS) completion —
      `Socks5UdpSocket` relay, mock end-to-end test green

**Exit**: all `test_socks5`/`test_ssl`/tracker-path tests green;
every connector/acceptor path goes through ADR-0008 traits.

---

## Phase D — DHT completeness ✅

`bitdaemon-dht`:

- [x] `refresh.cpp` (bucket refresh traversal)
- [x] `types.hpp` (`dht` settings/types leftovers)
- [x] Observer / `direct_request` wiring (`direct_request` API,
      `direct_traversal` replies)
- [x] Verify BEP-44/51/42 status against code; `test_dht` cases
      ported — live-Mainline verification ran during real-swarm test

**Exit**: `test_dht.cpp` coverage ported; bootstrap + get_peers +
announce + mutable items verified on live Mainline DHT.

---

## Phase E — Advanced / upstream tail ✅

- [x] `ut_holepunch` (BEP-55 rendezvous via mutual peer)
- [x] `rtc_stream.cpp`/`rtc_signaling.cpp` — ported over injected
      `RtcPeerConnection`/`RtcChannel` traits; stack choice = leaf
      crate, recorded in ADR-0011
- [x] **`bitdaemon-mmap` crate** (feature-gated, `unsafe` allowed):
      `file_view_pool.cpp`, `mmap.cpp`, `mmap_storage.cpp`,
      `mmap_disk_io.cpp`, `mmap_disk_job.hpp` — `FileViewPool` +
      `MmapStorage` + `mmap_storage_constructor`
- [x] `posix_disk_io.cpp` variant — `won't port`: `std::fs` positioned-I/O backend covers it (io_uring-adjacent? — evaluate vs
      `PreadDiskIo`; port or reasoned `won't port`)
- [x] `disk_cache.cpp` — block-level cache ported upstream
- [x] `merkle`/`hash_picker` edge cases; any `aux_` headers not yet
      mapped

---

## Phase F — Simulation & conformance 🟨

- [x] **`bitdaemon-sim`** scaffold (SimNet, deterministic RNG, per-link latency/jitter/loss): port `simulation/` harness — deterministic
      swarm tests over injected transports (ADR-0008 makes this the
      payoff). Port `test_dht.cpp`/`test_tracker`/`test_transfer`
      simulation cases onto it.
- [x] Differential tests: golden fixtures vs C++ v2.1.2 (bencode, resume, session, dht, torrents, magnet — byte-exact).
- [x] Live seed/leech pairs verified: C++ v2.1.2 seed → BitDaemon
      leech AND BitDaemon seed → C++ leech, byte-identical payloads
      both ways (`tools/real_swarm`). uTP pair verified (C++ seed →
      `--utp` leech, hash identical) and magnet resolution verified
      live. SSL pair verified against a libtorrent build with
      OpenSSL (`tools/real_swarm` + `build_ssl`): C++ sslseed → BD
      leech 100 % hash-identical, BD seed → C++ sslleech DONE
      (mutual TLS, embedded `ssl-cert` root, SAN=name enforced,
      rogue-cert rejected by C++). Dedicated DHT-bootstrap live run
      verified: public routers bootstrapped the v4 routing table
      (~135 nodes) and `get_peers` traversals returned real swarm
      peers for the Ubuntu info-hash.
- [x] Real-swarm validation: `bitdaemon-cli` downloaded
      `ubuntu-24.04.4-live-server-amd64.iso` (3.4 GB, v1) from the
      public swarm — ~32 peers, ~117 MB/s peak, `Finished`,
      official SHA-256 verified.
- [x] Fuzzing scaffold (`fuzz/` crate, 3 targets); campaign runs still pending (`cargo-fuzz`): bdecode, peer wire, DHT msg, resume,
      tracker responses — mirror upstream `fuzzers/`.
- [x] Resume/`.session` upgrade+downgrade round-trip matrix vs
      upstream (golden vectors + upgrade→downgrade→upgrade
      idempotence test).
- [x] Benchmarks scaffold (criterion: bdecode, peer-wire); vs-C++ numbers still pending (`examples/run_benchmarks` equivalent):
      hashing throughput, churn, memory @1000 torrents.

**Exit**: upstream clients cannot distinguish BitDaemon on the wire;
simulation suite replaces ad-hoc tests for regressions.

---

## Phase H — Disk cache & orchestration wiring ✅

Live-swarm diagnosis (2026-10-11): `store_buffer`/`disk_cache` are
ported but unwired — every 16 KiB block becomes an individual `Write`
job/syscall; the queue bursts to ~22k jobs (~350 MiB) and Windows
dirty-page throttling stalls the host. qBittorrent doesn't do this
because upstream decouples block arrival from syscall rate.

- [x] **Write coalescing via `store_buffer`** (ADR-0012):
      `async_write` = `store_buffer.insert`; flush emits one `Write`
      job per contiguous region (`writev`), triggered on
      piece-complete boundary, high watermark, `cache_expiry`, and
      fence orderings (`clear_piece`/`move_storage`/`delete_files`).
      `piece_gate` ordering semantics preserved through the gate→cache
      transition.
- [x] **Watermarks + flush timer** (ADR-0013): `disk_cache_low_/
      high_watermark` and `cache_expiry` settings; `BackPressure`
      re-based on cached bytes; periodic flush wake in the pool.
- [x] **Read path via cache** (ADR-0014): `read`/`hash`/`hash2` jobs
      hit `store_buffer`/`disk_cache` first; retire `piece_gate` after
      equivalence is proven. **Correctness prerequisite for ADR-0012.**
- [x] **io_buffer_mode wiring** (ADR-0015): `disable_os_write_cache` +
      `--no-os-cache` (write-through) landed; `FILE_FLAG_RANDOM_ACCESS`
      removed (upstream `file.cpp` KB2549369 comment). Read mode wired (`--no-os-read-cache`); `posix_fadvise`/`F_NOCACHE`
      still pending.
- [x] **`piece_hasher` v2** (ADR-0016): per-block SHA-256 at arrival
      on hasher workers; `hash2_job` assembles precomputed hashes.
- [x] **Disk `performance_counters`** (ADR-0017): upstream
      `stats_metric[]` disk group; replaces ad-hoc `queue_stats()`.

**Exit**: sustained ≥ 100 MiB/s download with bounded RSS and no
`have`-progress stalls; disk queue depth driven by cache watermarks,
not job count.

---

## Phase I — Dormant-module wiring ✅

Audit (2026-10-11): several ported modules are never reached by the
session/engine — the *wiring* is the missing port.

- [x] **UPnP SSDP** (ADR-0018): M-SEARCH burst on `start_upnp`,
      `LOCATION` XML fetch via injected resolver/connector, per-IGD
      TCP+UDP `AddPortMapping`, half-TTL renew, `DeletePortMapping`
      on `stop_upnp`, `Portmap` alerts.
- [x] **NAT-PMP gateway** (ADR-0019): `NetInterfaces`/`RouteEnumerator`
      injectable trait (default OS impl) → default gateway →
      `start_natpmp`; existing `natpmp_renew` handles the rest.
- [x] **SOCKS5 UDP lanes** (ADR-0020): `Socks5UdpSocket` injected as
      the session `DatagramSocket` when `proxy_*` says so — DHT, uTP
      and UDP trackers through the proxy; no silent direct fallback.
- [x] **DHT `dos_blocker`** (ADR-0021): instantiate in `dht::node`,
      `incoming()` gate at the top of the UDP demux.
- [x] **Send-path `chained_buffer`**: `write_messages` batches the
      peer's outbound queue through `ChainedBuffer` into one
      `write_vectored` per round (engine send pump drains ≤64 msgs).

Second pass (wiring-completeness audit): `net::peer_alloc` moved from
session and wired into `PeerList` (alloc/free accounting on
insert/erase/drop); `disk::precomputed_block_hashes` wired on
`PreadStorage` (hash-job `store` → `hash2` `take_block` →
`clear_piece` `drop`); `core::resolve_links` wired into
`Session::check_files` (similar_torrents/collections → hard links);
`net::instantiate` wired into `Engine::spawn_dial` (peer proxy
transport selection); `dht::item` delegates sign/verify to
`dht::ed25519` helpers; `dht::node_id::distance_exp` routed through
`common::ffs`. `net::receive_buffer` remains a utility — the engine
read pump uses an equivalent `BytesMut` arena.

**Exit**: inbound port mapping works on UPnP/NAT-PMP gateways (verified
live on a real router), proxied UDP lanes carry DHT/uTP traffic, and
the DHT drops flood sources.

---

## Phase J — Disk file controls & thread parity 🟨

Live benchmark (2026-10-11): with the store_buffer wired, C: (SSD)
sustains ~100 MiB/s, but D: (HDD) oscillates ~20 MiB/s with
back-pressure plateaus vs qBittorrent's ~58 MiB/s flat. Root cause:
NTFS non-sparse writes past the Valid Data Length pay synchronous
zero-fill per out-of-order write; hash jobs also share the write
pool.

- [x] **Sparse files** (ADR-0022): `open_mode::sparse` consumed for
      real — `pread_storage::open_file` sets it when `!allocate_files`
      and for priority-0 files; `file.cpp`'s
      `DeviceIoControl(FSCTL_SET_SPARSE)` ported into a scoped
      `file::win32` module (second reviewed `unsafe` carve-out, cf.
      ADR-0010 §3). Landed: `disk/file.rs` `win32` module,
      `open_mode::SPARSE` in `PreadStorage::open_file`.
- [x] **`SetFileValidData`** (ADR-0022): `enable_set_file_valid_data`
      setting → `allow_set_file_valid_data` → `SetFileValidData` on
      truncate + non-sparse, with best-effort `SE_MANAGE_VOLUME_NAME`.
      CLI enables it by default (qBittorrent parity — the CLI forces
      `StorageMode::Allocate`).
- [x] **`hashing_threads` pool** (ADR-0022): dedicated
      `DiskIoThreadPool` for `hash`/`hash2` jobs (upstream
      `m_hash_threads`, default 1); `aio_threads` default raised to
      10 (upstream) in the CLI.
- [x] Settings parity: `aio_threads`, `hashing_threads`,
      `enable_set_file_valid_data` in `Settings` with upstream
      defaults; CLI flags `--aio-threads`/`--hashing-threads`/
      `--enable-set-file-valid-data`.
- [ ] Re-benchmark 6 GiB torrent on D: vs qBittorrent baseline
      (~58 MiB/s, ~2 min). *Code landed — validation run pending.*

**Exit**: D: download sustains ≥ qBittorrent-class throughput with no
zero-fill stalls; `have` progress begins without the ~2 min
allocation/check plateau.

---

## Phase K — Alert scaffolding & category realignment (ADR-0023 D1, D2, D7) ✅

The enum must exist before any emission can land (Phase L), and the
category machinery has three *fidelity* bugs found on review, not
just missing variants: `AlertCategory` uses compacted bits while
upstream is bits 0–24 (`alert_mask` is persisted in `session_params`);
`alert_mask` isn't even a setting yet; and `AlertManager::post`
mask-filters inside `post` while upstream's `emplace_alert` is
unconditional with `should_post` at call sites.

- [x] **Enum expansion**: `Alert` → all 101 concrete upstream alert
      types (ids 3–107 verbatim) + `ReadPieceFailed` split sharing
      id 5. Fat DHT payloads `Box`ed (`DhtMutableItemAlert`,
      `DhtPutAlert`); `size_of::<Alert>() <= 96` guarded by test —
      the floor is 3-`String` variants. `DhtStats` payload → upstream
      `dht_stats_alert` shape (routing table + requests + nid +
      endpoint) moved to Phase L with the `dht_tracker::stats()` port.
- [x] **Upstream tables**: `Alert::category()`, `alert_type() -> u32`
      (ids verbatim — feeds the `alerts_dropped` type set),
      `priority()` (`alert_priority` normal/high/critical/meta).
- [x] **`AlertCategory` = `alert_category_t` verbatim** (bits 0–24,
      all 23 categories + `DEBUG`=`CONNECT` alias). Breaking-change
      note in CHANGELOG — persisted masks decode correctly both ways.
- [x] **`alert_mask` setting**: `SessionSettings::alert_mask`
      (default `error`, upstream `settings_pack` verbatim) +
      `session_params` `i!`/`g!` rows + `apply_settings` →
      `update_alert_mask` + `SessionHandle::set_alert_mask` persists
      to settings; `AlertManager::default` mask = `error`. CLI and
      existing tests opt into `ALL` explicitly (upstream idiom).
      `SessionHandle::new` now applies `params.settings` and fans
      `params.plugins` into `alerts.add_extension` (was dropped).
- [x] **`AlertManager` parity**: category-0 posts unconditionally
      (`SessionStats`/`SessionStatsHeader`/`DhtStats` land with
      `mask=0`); priority-weighted queue limit
      `len/(1+priority) >= limit`; `m_dropped` → per-type set →
      synthesized `AlertsDropped{types}` rides `get_all`; `post`
      fans out to `SessionPlugin::on_alert`; `should_post` exposed
      for call-site gating.
- [x] Conformance test ported: `tests/test_alert_types.rs` — 102-row
      table, every variant → `(alert_type, priority, category)`
      asserted verbatim + category-0/`mask=0`/`session_params`
      round-trip cases.

**Exit**: ✅ enum complete & compiling; `alert_mask` round-trips via
`session_params`; category-0 alerts deliver with `mask=0`;
`test_alert_types` green (3 tests, 102-row table).

---

## Phase L — Async result delivery & emission sites (ADR-0023 D3–D6, D8) ⬜

Upstream delivers every async query result through the alert stream;
our session currently drops several (`DhtEvent::Data` non-get_peers
payloads, `DhtEvent::Direct`, `ip_voter` winner changes, portmap
outcomes, `session_stats`). This is the only place where a handle call
returns `()` and the answer is *unreachable* — a GUI cannot work
around it. Requires Phase K's enum.

- [ ] `dht_lookup_map` → `dht_result_map: BTreeMap<(bool, usize),
      DhtResult>` typed sinks (`GetPeers`, `GetPeersQuery`, `GetItem`,
      `GetItemMutable{pk,salt}`, `Sample`, `Put`, `Announce`) —
      replaces `dht_pending_put`.
- [ ] `dht_get_peers`/`dht_get_item`/`dht_sample_infohashes` register
      user lookups; `dht_drain_events` routes `Data`/`Done` per sink:
      peers → `peer_list` + `DhtGetPeersReply`; item →
      `DhtImmutableItem`/`DhtMutableItem`; samples →
      `DhtSampleInfohashes`; `Done+Put` → `put` + `DhtPut`;
      `Done+Announce` → announce phase + `DhtAnnounce`;
      `Direct` → `DhtDirectResponse`; errors → `DhtError`;
      bootstrap → `DhtBootstrap`; liveness → `DhtLiveNodes`;
      `DhtPkt` → full `dht_pkt_alert` payload.
- [ ] Mutable BEP-44: `dht_put_item(pk, sk, salt, entry)` and
      `dht_get_item(pk, salt)` overloads (target =
      `item::canonical_target(pk, salt)`; sign via `dht::ed25519`).
- [ ] Portmap: `Engine::portmap_handles: BTreeMap<i32, PortmapSlot>`
      (upstream `m_portmap_handles`) → real `delete_port_mapping`;
      `Portmap`/`PortmapError`/`PortmapLog` alerts posted from UPnP
      driver actions and NAT-PMP `on_reply`.
- [ ] `ListenSucceeded`/`ListenFailed` on `start_acceptor`/
      `start_ssl_acceptor`/uTP/I2P bind results; `IncomingConnection`
      on accept.
- [ ] `ip_voter.external_address()` change detection (in `cast_vote`
      call sites or tick) → `ExternalIp` alert.
- [ ] `SessionHandle::post_session_stats` → `SessionStatsHeader`
      (once) + `SessionStats` (category-0 → unconditional post).
- [ ] **Emission sites** (ADR-0023 D8 table): `MetadataReceived`/
      `MetadataFailed` (`set_metadata` ok/fail + reject-exhaustion),
      `TorrentPaused`/`TorrentResumed`, `TorrentError`,
      `TorrentConflict`, `TorrentNeedCert`, `AddTorrent`,
      `FastresumeRejected`, `OversizedFile`, `Trackerid`,
      `FileCompleted` (wire `file_progress::update` `completed_cb`),
      `Block*`/`UnwantedBlock`/`RequestDropped`/`InvalidRequest`,
      `PeerSnubbed`/`PeerUnsnubbed`/`PeerBlocked`/`PeerError`,
      `IpBan`, `Socks5`, `TrackerAnnounce`/`TrackerWarning`,
      `UdpError`, `LsdPeer`/`LsdError`, `UrlSeed`, `FilePrio`,
      `Log`/`I2p`/`TorrentLog`/`PeerLog`/`PickerLog` (gated on
      `*_log` categories — cheap no-ops when masked off),
      `SaveResumeDataFailed`, `AlertsDropped`.

**Exit**: every handle call that produces an async result upstream
delivers it; DHT `get`/`put`/`sample`/`live_nodes`/`direct` round-trip
to the consumer; `delete_port_mapping` actually deletes; a GUI can
observe every state transition without polling internals.

---

## Phase M — Functional completions ⬜

- [ ] **BEP-17 `http_seed` (GetRight)**: `net::web_seed` gains the
      http_seed endpoint kind — URL join `base + file_path` (no
      `?piece=` query), redirect→seed-URL update, 4xx/503 →
      `retry_web_seed` backoff (upstream `http_seed_connection.cpp`
      semantics). `add_http_seed`/`remove_http_seed`/`http_seeds`
      already manage endpoints.
- [ ] **Extension factories invoked** (ADR-0023 D9): `ExtensionFactory`
      vec applied at torrent start + late `add_extension`;
      `SessionPlugin::new_peer_connection(&PeerInfo)` seam + minimal
      `PeerPlugin` trait (`on_message`, `on_tick`). Full
      `torrent_plugin`/`peer_plugin` surface stays deferred-with-reason
      until a consumer needs it.
- [ ] `MetadataFailed` on ut_metadata source-penalty exhaustion.
- [ ] **GUI parity matrix** (`docs/gui-parity.md`): alert/handle →
      widget checklist (add/remove/pause/queue/priorities/deadlines/
      trackers/peers/DHT/portmap/stats graphs) — acceptance doc for
      "GUI complet".

**Exit**: BEP-17 web seeds actually fetch; `add_extension` factories
run and receive peer connections; a consumer needs no internal access
for anything a qBittorrent-class UI shows.

---

## Phase N — Edge transports & POSIX perf (optional) ⬜

- [ ] **`bitdaemon-webrtc` leaf crate** (ADR-0011): concrete
      `RtcPeerConnectionFactory`/`RtcChannel` over `str0m` (preferred:
      safe Rust) or `webrtc`; offer/answer wired to the ported
      `rtc_signaling` state machine. Only affects WebTorrent peers —
      nothing in core depends on it.
- [ ] **`posix_fadvise`/`F_NOCACHE`** read hints — completes
      `io_buffer_mode` read-mode parity on Linux/macOS.
- [ ] I2P `i2p_stream` beyond SAM handshake if required by consumers
      (currently ported to upstream's scope).

**Exit**: optional; no functional blocker for TCP/uTP/SSL swarms.

---

## Phase G — 1.0 ⬜

- [x] `PORTING.md` = 100% resolved rows (the completion metric) — every upstream file is `ported`/`verified`/`won't port`/`n/a`.
- [ ] Alert/API surface = upstream parity (Phases K/L — ADR-0023);
      `session_params` `alert_mask` interop fixture green.
- [ ] Public API freeze on `bitdaemon` facade (semver).
- [ ] Crates.io publish order: common → bencode → core → disk →
      (mmap, opt) → net → dht → session → sim → bitdaemon → cli.
- [ ] `cargo deny` licenses/advisories green; MSRV pinned in
      `rust-toolchain.toml`.

---

## Downstream note

OnionBit remains the first consumer; its milestone needs
(`Engine::handle()`, `StorageFactory`, injected transports, `utp_only`)
are already satisfied. It no longer drives ordering — phase order is
upstream dependency depth.

---

## Workflow (unchanged — AGENTS.md §5/§7)

Per module: read C++ header → cpp → design idiomatic Rust API →
implement → port `test_*.cpp` cases → mark `ported` → update
PORTING.md + CHANGELOG **in the same commit**.
`//! Port of …` header on every ported module. No `unwrap`/`expect`/
`todo!` outside `#[cfg(test)]`. `cargo fmt`, `clippy -D warnings`,
`cargo test`, `cargo doc` clean before every commit.
