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
(v1/v2/hybrid), `Bitfield`, `PeerId`, `PieceBlock`, `DownloadPriority`,
`error`/`Result`, `hex`. *(still to do)*: `PeerRequest`, `TorrentFlags`,
`AnnounceFlags`, `time` helpers, `sliding_average`, `stat`,
`bloom_filter`, `random`, `crc32c`, `cpuid`-free paths.

`bitdaemon-bencode` *(done)*: `Entry` (owned, sorted-dict invariant),
`BdecodeNode` (lazy zero-copy, packed 8-byte tokens, soft errors,
back-patched partial trees), encoder, `bdecode_errors` mapping,
depth limits, pretty-printer.

**Exit criteria**: all ported `test/test_bdecode.cpp` ✅ (67 cases),
`test_bitfield.cpp` ✅ (16 cases); still needed:
`test_sha1.cpp` full vector set, `test_bloom_filter.cpp`,
`test_sliding_average.cpp`; bencode round-trips every `.torrent` in
upstream `test/` dir.

---

## Phase 2 — Metainfo ⬜

*~10k LOC C++.* Unlocks loading real `.torrent` files and magnets.

`bitdaemon-core`: `FileStorage` (+ `path`, `resolve_duplicate_filenames`),
`TorrentInfo` (v1, v2, hybrid, merkle `info` verification, `file_progress`),
`CreateTorrent` (+ `merkle_tree`, `merkle` hashing), `magnet_uri`
(parse + serialize), `load_torrent`, `announce_entry`, `tracker_list`,
`web_seed_entry`, `info_hash`, `fingerprint`, `identify_client`,
`read_resume_data`/`write_resume_data` wire format, `resolve_links`.

**Exit criteria**: `test_torrent_info.cpp`, `test_create_torrent.cpp`,
`test_magnet.cpp`, `test_file_storage.cpp`, `test_read_resume.cpp`,
`test_merkle.cpp` pass; can parse every fixture torrent incl. v2/hybrid.

---

## Phase 3 — Disk I/O ⬜

*~20k LOC C++.* Storage must be correct before any download exists —
and this phase is a **primary motivation for the port** (ADR-0009):
rqbit's I/O and memory behavior is what broke downstream. Port the
full machinery, not a simplified subset.

`bitdaemon-disk`: `DiskIo` trait + async job queue with per-torrent
`disk_job_fence` ordering, `disk_io_thread_pool` → blocking pool,
**bounded** `disk_buffer_pool` (`Bytes` slab + `max_queued_disk_bytes`/
`send_buffer_watermark` settings), `back_pressure`, `FilePool`
(lazy-open + LRU handle cap — a 10k-file torrent restores in
milliseconds, not minutes), `StatCache`, `FileProgress`, `PartFile`,
`storage_utils` (move/delete/rename, `apply_pad_files`,
`free_space`/`drive_info`), `HashPicker` (v2 piece layers),
`precomputed_block_hashes`, `copy_file`, `truncate`,
`file`/`directory`/`file_view_pool`/`mmap` wrappers, **`StorageFactory`
trait** (ADR-0008 — downstream `PrivateStorageFactory`/OBD parity).
Two backends: `std::fs` positioned I/O (default) + `memmap2` (feature),
plus `disabled` for tests.

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

## Phase 4 — Peer wire ⬜

*~25k LOC C++.* The core protocol, on tokio.

`bitdaemon-net`: `PeerWireCodec` (framing + all message types),
`PeerConnection` state machine (choke/interest states, timeouts,
keepalive, `receive_buffer`, `chained_buffer` → `BytesMut`),
`BtPeerConnection` (BitTorrent handshake, extension protocol BEP-10),
`PacketBuffer`/`merge_block_requests`/`request_blocks`/`suggest_piece`,
`pe_crypto` (MSE/PE, RC4 via crate), `socket`/`socket_type` →
`AsyncSocket` trait + `enum Socket`, `instantiate_connection`,
`socks5_stream`, `http_stream`, `ssl_stream` (rustls — see ADR-0007),
`i2p_stream` (deferred to Phase 8?), `resolver`, `ip_filter`,
`ip_voter`, `ip_notifier`, `enum_net`, `parse_url`, `escape_string`,
`utf8`, `socket_io`, `bandwidth_limit`/`bandwidth_manager`/
`bandwidth_queue_entry` (shared with session crate — decide home).

`bitdaemon-session` (partial): `PiecePicker`, `piece_block_progress`,
`Torrent` skeleton for driving connections, `SmartBan` extension hooks.

**Exit criteria**: `test_peer_connection` fixtures, `test_pe_crypto.cpp`,
`test_socks5.cpp`, `test_ip_filter.cpp`, `test_http_connection.cpp`
pass; a scripted peer (test harness) completes handshake + bitfield +
piece round-trip.

---

## Phase 5 — Trackers, discovery & uTP ⬜

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

---

## Phase 6 — Session core ⬜

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

---

## Phase 7 — DHT ⬜

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

---

## Phase 8 — Advanced / v2+ features ⬜

Defer everything non-essential until Phase 6 is solid.
(uTP moved to Phase 5 — OnionBit anonymous lanes need it.)

- **Web seeds** (`web_connection_base`, `web_peer_connection`,
  `http_seed`/`getright`/`holtb` URL types).
- **Stream encryption hardening** (`smart_ban`, full `pe_crypto`
  policy modes, `crypto_provider` abstraction incl. `win_cng`→RustCrypto).
- **I2P** (`i2p_stream`, `i2p_pex`).
- **SSL torrents** (`ssl` context, cert pinning, `ssl_stream`).
- **WebRTC signaling** (`rtc_stream`, `rtc_signaling`) — newest,
  least-stable upstream surface; evaluate `webrtc` crate vs bespoke.
- **Streaming** (`read_piece`, `have_piece` deadlines,
  `time_critical` piece logic inside `torrent`).
- **`super_seeding`**, `seed_mode`, `share_mode` corner cases.
- **`simulation` harness port** — libtorrent's network simulator for
  deterministic swarm tests; large but transformative for CI quality.

---

## Phase 9 — Hardening & conformance ⬜

- [ ] Fuzzing (`cargo-fuzz`): bdecode, peer-wire codec, DHT msg parser,
      resume data, tracker responses — mirroring upstream `fuzzers/`.
- [ ] Differential tests: BitDaemon ↔ C++ libtorrent client pairs
      (seed/leeach both directions, resume, magnet, DHT).
- [ ] Real-swarm validation against qBittorrent/Transmission/aria2.
- [ ] Benchmarks vs C++: piece hashing throughput, connection churn,
      memory under 1000-torrent load (upstream `examples/run_benchmarks`).
- [ ] `cargo deny` licenses/advisories green; `cargo doc` complete.
- [ ] Resume/`.session` upgrade+downgrade round-trip matrix vs upstream.

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
