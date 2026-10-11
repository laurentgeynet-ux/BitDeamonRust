# Changelog

All notable changes to BitDaemon are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).
Until `1.0.0`, breaking changes may occur in any release.

## [Unreleased]

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

[Unreleased]: https://github.com/laurentgeynet-ux/BitDeamonRust/compare/main...HEAD
- Phase C networking: `UdpSocket` adapter over `DatagramSocket`, `CachingResolver` (LRU 700, TTL 1200s, failed-DNS TTL/8, single-flight), `ssl_stream` via rustls/tokio-rustls (peer verify-none + tracker root-store + `SniResolver`), `instantiate` transport decision table, `core::xml_parse` byte-level scanner port.
- Phase B/D: BEP-6 `send_allowed_set` deterministic allowed-fast (SHA-1 addr‖info-hash), `allowed_fast_set_size` setting; DHT `bootstrap` (refresh.cpp) + `add_node` ping + `direct_request`/`DhtEvent::Direct`; `platform_util` (procfs, unsafe-free), `storage_free_list`, web-seed URL normalization (`escape_file_path`, trailing-slash rules).
- Phase F conformance infrastructure: `bitdaemon-sim` expanded (per-link latency/jitter/loss, seeded RNG, `run_to`/`drain` drivers), `fuzz/` cargo-fuzz scaffold (`bdecode`, `peer_wire`, `resume_data` targets), criterion benches (`bdecode_bench`, `wire_bench`).
- `Torrent` surface completed vs `torrent.cpp`: `abort`/`disconnect_all`, `attach_peer`, `choke_peer`/`unchoke_peer`, `num_connecting` counters, `need_picker`/`peer_is_interesting`/`ready_for_connections`/`want_tick`, `is_aborted`/`is_upload_only`/`is_ssl_torrent`/`is_predictive_piece`, `announce_dht`/`announce_lsd`/`announce_trackers` + `announcing` gates (wired into tracker/DHT/LSD ticks), `enable_all_trackers`, `announce_with_tracker`, suggest-piece list, predictive bitfield, `initialize_merkle_trees`, `recalc_share_mode`, `request_time_critical_pieces`, `set_state`, `handle_disk_error`, `on_torrent_paused`, `set_file_priority`, `queue_position_*` shortcuts, `post_status`, `get_torrent_copy_with_hashes`, `retry_web_seed`, `maybe_connect_web_seeds`, `update_want_peers`.
- `SessionHandle` completed to upstream surface: session pause/resume, `get_torrents`/`get_settings`/`session_state`/`write_session_params`, alert mask/queue-limit/notify/`wait_for_alert`, ip/port filters, peer-class CRUD + `PeerClassTypeFilter`, `set_peer_id`/`set_key`, per-purpose proxies (`SessionProxies`), `PeSettings` (new in `net::pe_crypto`), rate-limit and connection setters, `start_*`/`stop_*` service switches, DHT handle ops incl. two-phase `dht_put_item` (`dht_pending_put`) and `dht_stats_alert`, `listen_on`/`use_interfaces`/`reopen_network_sockets`, `session_stats_metrics`. `DhtStorage` now `Clone`.
- `seed_mode` flag with upstream ctor gating (no dont_download, all have_pieces) + `leave_seed_mode` recheck; `ut_holepunch` BEP-55 codec + relay decisions + `PeerEffect::Holepunch` wiring.
- `disk::disk_cache` (per-block entries, flushed/hasher cursors, force-flush + hasher-kick flags, `try_clear_piece`); `disk::storage_free_list` wired into `PreadDiskIo` slot reuse (freed at remove-job execution, behind the fence).
- `rtc_stream` (RtcChannel seam + AsyncRead/Write + `rtc_parse_endpoint`); `bitdaemon-sim` crate: virtual clock, `SimNet` delivery queue, `SimSocket` DatagramSocket; session polls `IpNotifier` → `SessionOut::InterfacesChanged`.
