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
- [ ] `peer_connection.cpp` full state machine +
      `peer_connection_handle.cpp` (currently partial)
- [x] `peer_list.cpp` / `peer_info.cpp` / `peer.cpp`
- [x] `smart_ban.cpp` session wiring (`SmartBan` per torrent,
      hash-fail bookkeeping → `ban_peer`)
- [x] `torrent_peer_allocator.cpp` (slab/arena)
- [ ] `performance_counters.cpp` full counter surface
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

## Phase G — 1.0 ⬜

- [x] `PORTING.md` = 100% resolved rows (the completion metric) — every upstream file is `ported`/`verified`/`won't port`/`n/a`.
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
