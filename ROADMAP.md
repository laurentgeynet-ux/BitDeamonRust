# ROADMAP — libtorrent → Rust port

Port of libtorrent `master` @ `v2.1.2-45-gc019e80ef` (~170k LOC C++).
No dates — phases unlock each other. Live file-level status lives in
[`docs/PORTING.md`](docs/PORTING.md); this document is the strategy.

## Downstream driver: OnionBit

BitDaemon's first consumer is [**OnionBit**](https://github.com/laurentgeynet-ux)
(`D:\Projet\Tribler-Rust-Torrent`, GPL-3.0) — an anonymous P2P stack
porting Tribler, which currently embeds a **vendored + patched
librqbit** for its `onionbit-bittorrent` engine. BitDaemon replaces it.

Consequences that shape this roadmap:

- **Transports are injectable from day 1** (ADR-0008): DHT, uTP, UDP
  trackers consume `Arc<dyn DatagramSocket>`; peers come from
  `PeerConnector`/`PeerAcceptor`. Anonymous lanes route everything
  through onion `data` cells — a real UDP bind in the anonymous lane is
  a *leak*, not a fallback.
- **uTP is pulled forward to Phase 5** (upstream treats it as core;
  for OnionBit's anonymous lane it is the *only* peer transport).
- **BitTorrent v1 first.** v2 merkle/hash-picker, web seeds, SSL
  torrents, WebRTC stay deferred — OnionBit ships v1 swarms.
- Milestone **ONIONBIT-1** (end of Phase 6) = drop-in engine parity
  with vendored rqbit's feature subset.

Legend: ⬜ not started · 🟨 in progress · ✅ done (module-level status in PORTING.md)

> **Status snapshot (latest session)** — the engine is feature-complete
> enough for real-swarm validation: full peer wire (MSE/PE, BEP-10,
> ut_metadata/ut_pex/lt_donthave, FAST, super-seeding, keepalive,
> Interested/NotInterested), disk pipeline (write→hash v1/v2→Have,
> part_file, fences, back-pressure), trackers HTTP/UDP/WS + scrape,
> DHT announce/get_peers, LSD, NAT-PMP, UPnP, I2P/SAM, SOCKS5,
> WebSocket, uTP in+out, `Engine::handle()` shared `Arc<Mutex<Session>>`
> for OnionBit, default OS transports, working `bitdaemon-cli`,
> `apply_settings`, `remove_torrent` storage release.
> **Remaining work is frozen below** — next step is interop
> validation against a live swarm, not more porting.

---

## Phase 0 — Scaffolding ⬜ → 🟨

Project skeleton that lets parallel work happen safely.

- [x] Cargo workspace, 9 crates, dependency graph (ADR-0003)
- [x] Docs: AGENTS.md, ROADMAP, PORTING.md, ADRs, architecture
- [x] CI: fmt + clippy + test + MSRV
- [ ] `rust-toolchain.toml`, `cargo deny`/`cargo vet` policy (ADR-0007)
- [ ] Golden-vector generator: small C++ program (against upstream
      checkout) that dumps bencode/peer-wire/resume-data fixtures

**Exit criteria**: `cargo test` green on empty crates; fixtures pipeline documented.

---

## Phase 1 — Foundations ⬜ → 🟨

Everything else depends on these. *~15k LOC C++.*

`bitdaemon-common` *(done)*: `Sha1Hash`, `Sha256Hash`, `InfoHash`
(v1/v2/hybrid), `Bitfield`, `PeerId`, `PieceBlock`, `PeerRequest`,
`DownloadPriority`, `TorrentFlags`/`PexFlags`/`AnnounceFlags`
(`bitfield_flag!`), `time` helpers, `SlidingAverage`, `Stat`/`StatChannel`,
`BloomFilter`, `random`/`crypto_random_bytes`, `crc32c`, `IndexRange`,
`TimestampHistory` (+ `compare_less_wrap`), `error`/`Result`, `hex`.
*(still to do)*: `stat_cache` — blocked on `file_storage` (Phase 2/3),
not a standalone primitive.

`bitdaemon-bencode` *(done)*: `Entry` (owned, sorted-dict invariant),
`BdecodeNode` (lazy zero-copy, packed 8-byte tokens, soft errors,
back-patched partial trees), encoder, `bdecode_errors` mapping,
depth limits, pretty-printer.

**Exit criteria**: `test_bdecode.cpp` ✅ (67 cases),
`test_bitfield.cpp` ✅ (16), `test_sha1_hash.cpp` ✅ (bit ops, shifts,
`count_leading_zeroes` vectors), `test_bloom_filter.cpp` ✅ (5),
`test_sliding_average.cpp` ✅ (8), `test_crc32.cpp` ✅; bencode
round-trips every `.torrent` in upstream `test/` dir ✅ (107 parsed,
99 byte-identical), `test_timestamp_history.cpp` ✅ + step/adjust
coverage. Phase 1 exit criteria met except `stat_cache` (moved to
Phase 3 — it wraps `file_storage`/`stat_file`, not a primitive).

---

## Phase 2 — Metainfo ✅

*~10k LOC C++.* Unlocks loading real `.torrent` files and magnets.

`bitdaemon-core`: `FileStorage` (+ `path`, `resolve_duplicate_filenames`) ✅,
`TorrentInfo` (v1, v2, hybrid, merkle `info` verification ✅; `file_progress` pending),
`CreateTorrent` (+ `merkle_tree`, `merkle` hashing) ✅, `magnet_uri`
(parse + serialize) ✅, `load_torrent` ✅, `announce_entry`/`tracker_list`
(Phase 5 — session machinery), `web_seed_entry` (data struct ✅),
`info_hash` ✅, `fingerprint`/`identify_client` ✅,
`read_resume_data`/`write_resume_data` wire format ✅, `resolve_links` ✅.

**Exit criteria**: `test_torrent_info.cpp`, `test_create_torrent.cpp`,
`test_magnet.cpp`, `test_file_storage.cpp`, `test_read_resume.cpp`,
`test_merkle.cpp` pass; can parse every fixture torrent incl. v2/hybrid.
*Status*: all upstream fixture vectors green (file_storage 28/28,
torrent_info, magnet 24/24, resume, create_torrent 8/8,
merkle_tree 22/22, identify_client, resolve_duplicates 12/12);
remaining — `file_progress` (in disk), tracker machinery.

---

## Phase 3 — Disk I/O ✅

*~20k LOC C++.* Storage must be correct before any download exists —
and this phase is a **primary motivation for the port** (ADR-0009):
rqbit's I/O and memory behavior is what broke downstream. Port the
full machinery, not a simplified subset.

`bitdaemon-disk` — landed so far: `OpenMode` flags ✅, `File`/
`pread_all`/`pwrite_all`/`pwritev_all` ✅, `fs` (stat/rename/hardlink/
canonicalize/`copy_file`/`drive_info`) ✅, `StatCache` (error-caching,
poison-tolerant) ✅, `FilePool` (**lazy-open + LRU cap + open-waiter
sharing + read→write upgrade race counter + ENOENT→mkdir retry** —
the 349 s→ms fix, ADR-0009) ✅, `PartFile` ✅, `readwrite` piece→file
walk ✅, `pread_storage` (read/write/hash/initialize/set_file_priority/
move/delete/rename/verify_resume_data) ✅, `storage_utils` ✅.
`disk_job` types + flags ✅, `disk_job_fence` (verbatim repost order,
re-arming stacked fences) ✅, `disk_io_thread_pool` (on-demand spawn,
condvar wait, `try_thread_exit`) ✅, `disk_buffer_pool` + RAII
`DiskBufferHolder` + `BulkFreeBuffer` ✅, `back_pressure` (watermark
formulas verbatim) ✅, `disk_completed_queue` (single in-flight drain
post) ✅, `pread_disk_io` orchestrator (`perform_job` dispatch,
`immediate_execute` 0-thread fallback, abort) ✅.
`DiskIo` trait (full upstream surface) ✅, `StorageHolder` ✅,
`DisabledDiskIo` ✅, **`StorageFactory`/`DiskStorage` injection
point** (ADR-0008 — OnionBit `PrivateStorageFactory` parity) ✅,
`FileProgress` ✅, back-pressure wired into `async_write` ✅.
`HashPicker` (v2 piece layers, single-request API — 10/10 upstream
cases incl. coincident piece/block layer) ✅, `precomputed_block_hashes`
✅.
`store_buffer` (read cache + `BackPressure` watermarks) ✅.
*(still to do)*: `file_view_pool`/`mmap_disk_io` — deferred:
`mmap` requires `unsafe` and the crate is `forbid(unsafe_code)`;
`PreadDiskIo` is the reference backend (documented deviation).

**Fastresume** (`core::resume` lands Phase 2, applied here):
byte-compatible with upstream `read_resume_data`/`write_resume_data`
both directions — loads existing libtorrent/Tribler `.resume` files.

**Exit criteria**: `test_storage.cpp`, `test_part_file.cpp`,
`test_file_progress.cpp`, `test_stat_cache.cpp`, `test_mmap.cpp`
equivalents pass; `check_files` verifies a pre-seeded multi-GB dataset
byte-exact **under a bounded buffer pool**; lazy file pool restores an
11 310-file torrent in milliseconds (OnionBit regression:
349 s on eager init); deferred I/O errors surface as alerts at first
access (upstream semantics).

---

## Phase 4 — Peer wire ✅

*~25k LOC C++.* The core protocol, on tokio.

`bitdaemon-net`: `PeerWireCodec` (framing + all message types) ✅,
`socket_io` (BE helpers + endpoint codec) ✅,
`PeerConnection` state machine (choke/interest states, timeouts,
keepalive, `receive_buffer`, `chained_buffer` → `BytesMut`),
`BtPeerConnection` (BitTorrent handshake, extension protocol BEP-10),
`PacketBuffer`/`merge_block_requests`/`request_blocks`/`suggest_piece`,
`pe_crypto` (MSE/PE, RC4 via crate), `socket`/`socket_type` →
`AsyncSocket` trait + `enum Socket`, `instantiate_connection`,
`socks5_stream`, `http_stream`, `ssl_stream` (rustls — see ADR-0007),
`i2p_stream` (deferred to Phase 8?), `resolver`, `ip_filter` ✅,
`pe_crypto` (DH-768 + RC4, MSE constants) ✅,
`ip_voter`, `ip_notifier`, `enum_net`, `parse_url`, `escape_string`,
`utf8`, `socket_io` ✅, `bandwidth_limit`/`bandwidth_manager`/
`bandwidth_queue_entry` ✅ (`net::bandwidth`, token buckets + request
queue with ttl expiry and disconnect quota return),
`receive_buffer` + `crypto_receive_buffer` ✅ (cursor buffer,
watermark shrink), `chained_buffer` ✅ (send-buffer chain,
`Box<dyn BufferHolder>`).

`bitdaemon-session` (partial): `PiecePicker`, `piece_block_progress`,
`Torrent` skeleton for driving connections, `SmartBan` extension hooks.

**Exit criteria**: `test_peer_connection` fixtures, `test_pe_crypto.cpp`,
`test_socks5.cpp`, `test_ip_filter.cpp`, `test_http_connection.cpp`
pass; a scripted peer (test harness) completes handshake + bitfield +
piece round-trip.
*Status*: ✅ done — handshake + MSE/PE + BEP-10, bitfield/have
emission, keepalive (60 s idle), Interested/NotInterested, FAST
(HaveAll/HaveNone/AllowedFast/Suggest/Reject, BEP-6 deterministic set),
super-seed gating, lt_donthave, DhtPort both ways, unsolicited-block
drop, request gating (choked→allowed_fast only, missing→reject),
graceful pause, optimistic unchoke rotation, per-peer rate sampling,
keepalive. Remaining: `ut_holepunch` (deferred, Phase 8).

---

## Phase 5 — Trackers, discovery & uTP 🟨

*~20k LOC C++.* uTP lives here (not Phase 8) because OnionBit's
anonymous lane runs **peer traffic exclusively over uTP** routed
through injected datagram sockets (ADR-0008).

`bitdaemon-net`: `HttpConnection` + `http_parser` (incl. gzip via
`flate2`), `HttpTrackerConnection`, `UdpTrackerConnection` (**over
`DatagramSocket`**), `TrackerManager` (+ announce retry/backoff
semantics), `upnp`, `natpmp`, `portmap`, `lsd` (local service
discovery), `websocket_stream`/`websocket_tracker_connection`,
`udp_socket`, `session_udp_sockets` plumbing, `resolver` for tracker
DNS — **and** `utp_stream`, `utp_socket_manager`, `packet_buffer`,
`packet_pool` (LEDBAT congestion control, µs timestamps; port
constants verbatim before any tuning — AGENTS.md §9).

**Exit criteria**: announce to a local tracker stub (port
`test_tracker` harness) succeeds over both real UDP and an injected
in-memory `DatagramSocket`; LSD discovers a loopback peer; UPnP/NAT-PMP
mapping requests well-formed (mock IGD); uTP connects + transfers a
block loopback through an injected socket pair.
*Status*: 🟨 — HTTP/UDP announce + scrape end-to-end (hostname via
injected `Resolver`), WS tracker (BEP-RTC JSON signaling), uTP
connector + acceptor e2e on injected datagram sockets, LSD emit+receive,
NAT-PMP + UPnP SOAP e2e against scripted gateways, `parse_url`, gzip.
Remaining: `resolver` caching layer polish, `session_udp_sockets`
multi-interface bind, tracker retry parity details.

---

## Phase 6 — Session core 🟨

*~30k LOC C++.* Everything comes together — a working client.
Ends at milestone **ONIONBIT-1**: `onionbit-bittorrent` can drop
vendored rqbit and drive `bitdaemon` instead. Required parity surface
(their `EngineConfig`): injected `utp_socket`/`utp_listen_socket`/
`dht_socket`/`udp_tracker_socket`, `enable_tcp=false` anonymous lane,
`utp_only`, `enable_dht`/`disable_trackers`/`disable_lsd`,
`fastresume` (upstream-compatible resume data), bandwidth caps,
`StorageFactory` injection (their `PrivateStorageFactory`),
`OpaqueBitV`-style info-hash privacy support (Phase 8+ hardening,
design seam reserved).

`bitdaemon-session`: `Session` actor (`session_impl` equivalent),
`SessionHandle`/`SessionParams`, `Settings` (typed `settings_pack`
with upstream defaults), `AddTorrentParams`, `Torrent` full state
machine, `TorrentHandle` (~90 methods), `Alert` enum + `AlertManager`,
`Choker` (+ `peer_class`, `peer_class_set`, `peer_class_type_filter`),
`stat`/`session_stats`/`performance_counters`, `TorrentList`,
`torrent_peer`, `TorrentPeerAllocator` (slab/pool),
`listen_socket_handle`, save/load `.session` state
(`session_state`/`session_call`), `ip_filter` application,
`proxy_settings`, `back_pressure`, `resolve_links` apply,
extensions: `ut_metadata`, `ut_pex`, `smart_ban`, `metadata` +
`i2p_pex` stubs, `torrent_status`/`operations`/`close_reason`/
`download_priority`/`file_layout`/`storage_defs`/`session_types`.

`bitdaemon-cli`: real client (port of `client_test.cpp`).

**Exit criteria**: end-to-end download of a fixture torrent between two
BitDaemon instances on loopback; `test_session` equivalents pass;
alerts delivered in upstream-compatible order.
*Status*: 🟨 — `Engine` async actor + `SessionHandle` shared
(`Arc<Mutex<Session>>`, `Engine::handle()`), `add_torrent_params`
(resume+magnet+peers), `check_files`, choker + optimistic rotation,
queue positions, piece deadlines/`read_piece`, `save_resume_data`
(dirty flag), `move_storage`/`delete_files`/`rename_file`, alerts
(StateUpdate, ReadPiece, TorrentChecked, PeerBan…), `.session` state
round-trip incl. settings, `apply_settings` hot-swap, ip_filter
in+out, per-torrent + global rate limiters, web seeds fetched via
injected connector, v2 merkle verify + HASH_REQUEST/HASHES.
Remaining: `peer_class`/`peer_class_set`, `TorrentPeerAllocator`
(slab), `listen_socket_handle` multi-iface, `SmartBan`, `ut_holepunch`,
`i2p_pex` (I2P connector/acceptor done), full `performance_counters`.

---

## Phase 7 — DHT 🟨

*~12k LOC C++.* Trackerless operation.

`bitdaemon-dht`: `NodeId` + BEP-42, `Node` (main state machine),
`RoutingTable`, `RpcManager` (timeouts, transactions, rate limits),
`TraversalAlgorithm` + `FindData`, `GetPeers`, `GetItem`, `PutData`,
`Refresh`, `SampleInfohashes` (BEP-51), `DhtStorage`, `DhtState`
(persisted routing table), `DhtTracker` (session glue),
`DosBlocker`, `NodeEntry`, `msg`/`item` bencode schemas, `observer`.

`ed25519` → `ed25519-dalek` (ADR-0007), `mutable`/`immutable` item
put/get semantics per BEP-44.

**Exit criteria**: `test_dht.cpp` (all `simulation`-ported cases where
feasible), bootstrap + `get_peers`/`announce` against the live Mainline
DHT works.
*Status*: 🟨 — NodeId/BEP-42, routing table, RPC manager, traversal +
GetPeers, announce_peer after get_peers, DhtPort wire both ways,
external-IP votes → `ip_voter`, `enable_dht`/`bind`, `dht_add_node`
(from `.torrent` nodes + ping-only peer candidates), DhtState persist.
Remaining: BEP-44 mutable/immutable items (`GetItem`/`PutData`),
`SampleInfohashes` (BEP-51), `DosBlocker`, full observer wiring.

---

## Phase 8 — Advanced / v2+ features 🟨

Defer everything non-essential until Phase 6 is solid.
(uTP moved to Phase 5 — OnionBit anonymous lanes need it.)

- ~~**Web seeds**~~ ✅ done (`web_seed` driver: multi-file GET pipeline
  over `http_conn` on injected connector; engine `WebSeedFetch` pump).
  Remaining polish: `getright`/`holtb` URL-type quirks.
- **Stream encryption hardening** (`smart_ban`, full `pe_crypto`
  policy modes, `crypto_provider` abstraction incl. `win_cng`→RustCrypto).
- **I2P** — ✅ `i2p_stream` (SAM handshake) + `I2pConnector`/`I2pAcceptor`
  e2e; remaining: `i2p_pex`.
- **`ut_holepunch`** (BEP-55 rendezvous via a mutual peer).
- **SSL torrents** (`ssl` context, cert pinning, `ssl_stream`) —
  deferred; rustls per ADR-0007.
- **WebRTC signaling** (`rtc_stream`, `rtc_signaling`) — newest,
  least-stable upstream surface; evaluate `webrtc` crate vs bespoke.
- ~~**Streaming**~~ ✅ done (`set_piece_deadline`, `read_piece`,
  `time_critical`, `ReadPiece` alert, `cancel_non_critical`).
- ~~**`super_seeding`**~~ ✅ done (piece-at-a-time leak per peer).
  Remaining: `seed_mode`/`share_mode` corner cases.
- **`simulation` harness port** — libtorrent's network simulator for
  deterministic swarm tests; large but transformative for CI quality.

---

## Phase 9 — Hardening & conformance ⬜ ← NEXT

- [ ] **Real-swarm validation** — `bitdaemon-cli` against a public
      swarm (Linux ISO, hundreds of seeds, mixed clients). Watch:
      full download, sane rates, no disconnect storms,
      `piece_verified`/`Have`/seed transition. **This is the next
      action — the port stops here until it passes.**
- [ ] **ONIONBIT-1** — swap rqbit → bitdaemon in
      `Tribler-Rust-Torrent`. `Engine::handle()`, `StorageFactory`,
      `Resolver`, injected transports are ready; fragile point =
      `.bitv` private storage adapter.
- [ ] Fuzzing (`cargo-fuzz`): bdecode, peer-wire codec, DHT msg parser,
      resume data, tracker responses — mirroring upstream `fuzzers/`.
- [ ] Differential tests: BitDaemon ↔ C++ libtorrent client pairs
      (seed/leech both directions, resume, magnet, DHT).
- [ ] Golden-vector generator (Phase 0 leftover) — dump fixtures from
      the C++ checkout for wire/resume conformance.
- [ ] Benchmarks vs C++: piece hashing throughput, connection churn,
      memory under 1000-torrent load (upstream `examples/run_benchmarks`).
- [ ] `cargo deny` licenses/advisories green; `rust-toolchain.toml`.
- [ ] Resume/`.session` upgrade+downgrade round-trip matrix vs upstream.

---

## Remaining port work — frozen list

Deferred modules, in priority order. None blocks the OnionBit swap.

1. `ut_holepunch` (BEP-55) — NAT rendezvous via mutual peer.
2. `i2p_pex` — I2P destinations over PEX (I2P transport itself done).
3. SSL/TLS streams — `ssl_stream` (rustls), HTTPS trackers,
   SSL torrents.
4. `smart_ban` — hash-failure peer learning/banning.
5. `peer_class`/`peer_class_set` + `performance_counters` —
   upstream stats fidelity.
6. `TorrentPeerAllocator` (slab) — allocation perf, correctness-neutral.
7. `session_udp_sockets` multi-interface + `listen_socket_handle`.
8. DHT leftovers: BEP-44 items, BEP-51 `sample_infohashes`,
   `DosBlocker`, observer wiring.
9. `getright`/`holtb` web-seed URL quirks (BEP-17/19 variants).
10. `seed_mode`/`share_mode` corner cases; tracker retry parity details.
11. `file_view_pool`/`mmap_disk_io` — **permanently deferred**:
    `mmap` needs `unsafe`; `PreadDiskIo` is the reference backend.
12. WebRTC (`rtc_stream`/`rtc_signaling`) — evaluate `webrtc` crate.

---

## Phase 10 — 1.0 ⬜

- [ ] Public API freeze on `bitdaemon` facade (semver from here).
- [ ] `README` status → stable; `PORTING.md` 100% `verified` except
      explicitly-documented gaps.
- [ ] Crates.io publish order: common → bencode → core → disk → net →
      dht → session → bitdaemon → cli.

---

## Explicit non-goals

- Python bindings (`bindings/` upstream).
- BitTorrent v3 / BEP drafts not implemented upstream at the pin commit.
- GUI clients (out of scope; `bitdaemon-cli` is a test harness).
- Bug-for-bug parity with upstream *bugs* — document deviations in
  `docs/PORTING.md` when behavior intentionally differs.
