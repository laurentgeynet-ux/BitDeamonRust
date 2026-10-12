# ADR-0023: Alert-surface completion & asynchronous result delivery (1.0 API)

- Status: accepted — Phases K+L implemented (D1–D8); D9–D10 in Phase M
- Date: 2026-10-12
- Deciders: port team
- Complements: ADR-0004 (error handling), ADR-0008 (injected
  transports), ADR-0011 (WebRTC stack stays a leaf crate)

## Context

The port is protocol-complete and live-verified (peer wire, uTP, MSE,
SSL torrents, DHT, resume/`.session` round-trips, real-swarm download
at ~117 MiB/s). What remains before the API can drive a complete GUI
(qBittorrent-class surface) is **surfacing**, not protocol work: the
session internally produces results that never reach the consumer.

Verified gaps (all confirmed in code):

- **`Alert` covers 38 of upstream's ~105 `*_alert` types**
  (`include/libtorrent/alert_types.hpp`). Missing concrete types
  include `metadata_received`, `metadata_failed`, `file_completed`,
  `torrent_paused`/`torrent_resumed`/`torrent_error`,
  `listen_succeeded`/`listen_failed`, `external_ip`,
  `portmap`/`portmap_error`/`portmap_log`, `session_stats`/
  `session_stats_header`, `fastresume_rejected`,
  `save_resume_data_failed`, `alerts_dropped`, `lsd_peer`/`lsd_error`,
  `incoming_connection`, `peer_snubbed`/`peer_unsnubbed`,
  `block_timeout`/`block_uploaded`/`block_downloading`,
  `unwanted_block`/`request_dropped`/`invalid_request`,
  `tracker_announce`/`tracker_warning`/`trackerid`, `udp_error`,
  `url_seed`, `ip_ban`, `socks5`, `i2p`, `incoming_request`,
  `torrent_conflict`, `torrent_need_cert`, `oversized_file`,
  `file_prio`, the whole `dht_*` family beyond `DhtStats`/`DhtPacket`,
  and the log family (`log`, `torrent_log`, `peer_log`, `picker_log`,
  `portmap_log` folded into `portmap_*`).
- **`AlertCategory` bit values diverge from upstream
  `alert_category_t`** (`include/libtorrent/alert.hpp`): upstream uses
  bits 0–24 (`error=0`, `peer=1`, `port_mapping=2`, `storage=3`,
  `tracker=4`, `connect=5`, `status=6`, `progress=7`, `ip_block=8`,
  `performance_warning=9`, `dht=10`, `stats=11`, `session_log=13`,
  `torrent_log=14`, `peer_log=15`, `incoming_request=16`,
  `dht_log=17`, `dht_operation=18`, `port_mapping_log=19`,
  `picker_log=20`, `file_progress=21`, `piece_progress=22`,
  `upload=23`, `block_progress=24`). Ours are compacted 0x01–0x4000.
  `settings_pack::alert_mask` is persisted in `.session` /
  `session_params` — once we add the field it must carry upstream bit
  positions. Worse: **`alert_mask` is missing entirely** from
  `SessionSettings` and `session_params` today (only
  `alert_queue_size` is wired), and our `AlertManager` default mask is
  `ALL` while upstream defaults to `alert_category::error` in both
  `alert_manager`'s ctor and `settings_pack::alert_mask` — a silent
  behavioral deviation.
- **`AlertManager` semantics diverge in three more places**:
  upstream `emplace_alert` never checks the mask — `should_post<T>()`
  is a **call-site** gate, so the three category-0 alerts
  (`session_stats`, `session_stats_header`, `dht_stats`) post
  *unconditionally*; our `post()` drops `category & mask == 0`,
  which would permanently discard them. Upstream's queue limit is
  **priority-weighted** (`queue.size() / (1 + priority) >= limit` —
  critical alerts get 3× headroom, meta 4×); ours is a flat cap. And
  upstream `m_dropped` is a **per-type bitset** keyed by `alert_type`
  feeding `alerts_dropped_alert`; ours is a bare count.
- **DHT user-initiated queries drop their results.**
  `SessionHandle::dht_get_peers`/`dht_get_item`/
  `dht_sample_infohashes` start lookups but register nothing;
  `Session::dht_drain_events` only resolves `dht_lookup_map` (torrent
  `get_peers`) — `GetItem`/`Sample` data is skipped entirely, and
  `DhtEvent::Direct` is an explicit no-op
  (`crates/bitdaemon-session/src/session.rs` ~line 2078). Upstream
  answers via `dht_get_peers_reply`, `dht_immutable_item`,
  `dht_mutable_item`, `dht_sample_infohashes`, `dht_live_nodes`,
  `dht_direct_response`, `dht_put`, `dht_reply` alerts.
- **BEP-44 mutable items**: only the immutable `dht_put_item(value)`
  overload exists; `dht_put_item(pk, sk, salt, entry)` and
  `dht_get_item(pk, salt)` are absent.
- **Port mapping**: `delete_port_mapping(handle)` is a no-op stub and
  no `portmap_*` alert is ever posted, although UPnP/NAT-PMP drives
  already track mappings on `Engine`.
- **`ip_voter`** is fully wired (DHT `ip`, peer `yourip`, tracker
  `external ip`, NAT-PMP/UPnP router answers) but
  `external_address()` changes are never surfaced.
- **Magnet metadata**: `Torrent::set_metadata` success/failure posts
  nothing (`metadata_received`/`metadata_failed`).
- **`file_progress::update`'s `completed_cb` exists but is unused** —
  no `file_completed` alert.
- **`post_session_stats`/`session_stats_alert`/
  `session_stats_header_alert`** missing (the synchronous
  `session_stats()` snapshot + `session_stats_metrics()` exist).
- **`ExtensionFactory` is stored, never invoked**:
  `TorrentHandle::add_extension` pushes factories into
  `Torrent::extensions`; nothing instantiates them, and
  `SessionPlugin` exposes only `on_alert` (upstream `torrent_plugin`
  additionally hooks `new_peer_connection`, `on_tick`,
  `on_piece_pass`/`on_piece_failed`, …).
- **BEP-17 `http_seed` (GetRight)**: endpoints are stored/managed
  (`add_http_seed`/`remove_http_seed`/`http_seeds`) but the fetch path
  only implements BEP-19 `url_seed` semantics.
- Small leftovers: `posix_fadvise`/`F_NOCACHE` read hints
  (`io_buffer_mode` read side), WebRTC concrete backend (ADR-0011).

## Decision

### D1 — Keep the single `Alert` enum; expand it to full upstream coverage

One enum stays (upstream's class hierarchy maps to variants, already
established). Add the ~60 missing variants, grouped by subsystem.
Payload rules:

- Heavy payloads become named `pub struct XxxAlert` types in
  `alerts.rs` (e.g. `DhtPktAlert`, `SessionStatsAlert`,
  `TrackerErrorAlert`), referenced as `Alert::Xxx(XxxAlert)` — matches
  the current `PeerInfo`/`FileProgress` style. `Box` the payload when
  a variant would exceed ~48 B (`SessionStats` is `Vec`-backed so it
  stays small); add a `debug_assert!(size_of::<Alert>() <= 48)` guard.
- Trivial variants keep inline fields (`TorrentPaused { ih }`).
- `handle()`/`torrent_handle` ergonomics: every torrent-scoped alert
  carries `ih` (and `name` where upstream carries it) — no
  `Alert`→`&Torrent` back-pointer.
- `Alert::category()` becomes the verbatim upstream `category()`
  table — each variant lists its upstream `*_alert::static_category`
  in a doc comment.
- `Alert::alert_type() -> u32` and `Alert::priority()` mirror upstream
  `alert::type()`/`*_alert::alert_type` and `alert_priority` — the
  type id is required by the `test_alert_types.cpp` conformance table
  **and** by the per-type `alerts_dropped` bitset (D7).
- Payload parity where an existing variant is a subset:
  `DhtStats` expands to upstream `dht_stats_alert` shape (routing
  table + requests + node id + endpoint).

### D2 — `AlertCategory` re-aligned + `alert_mask` becomes a real setting

Rewrite the constants to `alert_category_t` verbatim (bits 0–24 above,
incl. `debug = connect` alias) and add the missing categories:
`session_log`, `torrent_log`, `peer_log`, `incoming_request`,
`dht_log`, `dht_operation`, `port_mapping_log`, `picker_log`,
`block_progress`, `progress`. **Interop fix, not cosmetics**:
`alert_mask` round-trips through `session_params`. Flag as a breaking
change in CHANGELOG (pre-1.0, allowed).

In the same pass:

- Add `alert_mask: AlertCategory` to `SessionSettings`, upstream
  default `error` (bit 0) — **not** `ALL` — and wire
  `settings_pack::alert_mask` rows into the `session_params` dict
  (`i!`/`g!` macros) so the field actually persists.
- `apply_settings` hot-applies it (`update_alert_mask` upstream).
- `AlertManager::default()` mask → `error` (upstream ctor default);
  `set_alert_mask`/`get_alert_mask` handle methods keep working.

### D3 — Async results are delivered as alerts, never callbacks

Upstream semantic: fire via handle (`dht_get_item`, `scrape_tracker`,
`post_*`, `save_resume_data`…), result arrives as an alert. We keep
that contract — it gives mask filtering, ordering, and one consumer
stream for free. No `oneshot`/callback API on the handle surface.
Consumers wanting request/response correlation match on
`ih`/`target`/`name` fields like upstream clients do.

### D4 — `dht_lookup_map` → `dht_result_map` typed sinks

Replace `BTreeMap<(bool, usize), Sha1Hash>` with
`BTreeMap<(bool, usize), DhtResult>` where

```rust
enum DhtResult {
    GetPeers { ih: Sha1Hash },        // torrent-driven announce path
    GetPeersQuery { ih: Sha1Hash },   // user dht_get_peers
    GetItem { target: Sha1Hash },
    GetItemMutable { pk: [u8; 32], salt: Vec<u8>, target: Sha1Hash },
    Sample { target: Sha1Hash },
    Put { target: Sha1Hash, value: Vec<u8>,
          mutable: Option<([u8;32], Vec<u8>)> }, // replaces dht_pending_put
    Announce { ih: Sha1Hash },
}
```

`dht_drain_events` routes `DhtEvent::Data`/`Done` by sink kind:
torrent announce → `peer_list` (unchanged) + `DhtGetPeersReply`;
items → `DhtImmutableItem`/`DhtMutableItem`; samples →
`DhtSampleInfohashes`; `Done`+`Put` → `put` to token-holders then
`DhtPut`; `Done`+`Announce` → announce second phase (unchanged) +
`DhtAnnounce`; `DhtEvent::Direct` → `DhtDirectResponse`; routing-table
growth events → `DhtLiveNodes`; first bootstrap completion →
`DhtBootstrap`; rpc errors/timeouts → `DhtError`. `dht_log`/`dht_pkt`
payloads stay gated behind `dht_log`/`dht` categories (cheap no-ops
when masked — upstream idiom).

### D5 — BEP-44 mutable items

Add `dht_put_item(pk: [u8;32], sk: [u8;64], salt: &[u8], value: Entry)`
→ `target = item::canonical_target(pk, salt)` (sha1(pk‖salt)),
seq/sig via `dht::ed25519` helpers; add `dht_get_item(pk, salt)`.
Immutable path unchanged (`target = sha1(value)`). Salt carries on the
`DhtResult::GetItemMutable` sink so the alert can echo it.

### D6 — Portmap handle table + real `delete_port_mapping`

`Engine` gains `portmap_handles: BTreeMap<i32, PortmapSlot>` where
`PortmapSlot { transport: Upnp{dev,map} | Natpmp{proto}, ext_port,
int_port }` (upstream `m_portmap_handles`/`portmap_callback`).
`delete_port_mapping` dispatches `DeletePortMapping` /
NAT-PMP lifetime-0 map request. `Portmap` / `PortmapError` /
`PortmapLog` alerts posted from the UPnP driver actions and NAT-PMP
`on_reply` paths.

### D7 — `AlertManager` upstream parity details

- **Category-0 alerts post unconditionally.** Upstream
  `emplace_alert` never consults the mask — `should_post<T>()` gates
  the *call site* — so `session_stats`, `session_stats_header` and
  `dht_stats` always land. `AlertManager::post` must short-circuit the
  mask check for `category == 0` (or gain `post_unconditional` +
  call-site `should_post` gates — pick one, document the equivalent
  in the module rustdoc).
- **Priority-weighted queue limit.** Port
  `queue.size() / (1 + priority) >= limit` verbatim so
  critical/meta alerts get upstream's 3×/4× headroom; `Alert::priority()`
  (D1) feeds the formula.
- **Per-type dropped bitset.** `m_dropped` is a bitfield indexed by
  `alert_type`; `AlertsDropped` carries the bitset (upstream
  `alerts_dropped_alert::dropped_alerts`), synthesized on the next
  accepted post — not just a count.

### D8 — Emission points (normative table)

| Alert | Emission site |
|---|---|
| `MetadataReceived` / `MetadataFailed` | `Torrent::on_metadata_packet` → `set_metadata` ok/fail (incl. reject-exhaustion) |
| `FileCompleted` | `file_progress::update`'s `completed_cb` in `piece_verified` path |
| `TorrentPaused` / `TorrentResumed` | `Torrent::set_paused`/`on_torrent_paused`, `Session::set_paused` per torrent |
| `TorrentError`, `FastresumeRejected` | `set_error`, resume-data apply failures in `add_torrent_params`/`check_files` |
| `TorrentConflict` | `add_torrent`/`async_add_torrent` duplicate info-hash path |
| `TorrentNeedCert` | SSL dial/accept when torrent requires cert and none set |
| `ListenSucceeded` / `ListenFailed` | `start_acceptor`/`start_ssl_acceptor`/uTP/I2P bind results |
| `ExternalIp` | `ip_voter.cast_vote` sites + tick diff on `external_address()` change |
| `SessionStats` (+ `SessionStatsHeader` once) — **category-0, unconditional** | `SessionHandle::post_session_stats` (upstream: header precedes first stats alert) |
| `Portmap`/`PortmapError`/`PortmapLog` | UPnP map/delete/renew + NAT-PMP replies (D6) |
| `LsdPeer` / `LsdError` | LSD demux accepted-infohash / malformed paths |
| `IncomingConnection` | acceptor accept (post-handshake, info-hash resolved) |
| `PeerSnubbed`/`PeerUnsnubbed`, `PeerBlocked`, `PeerError` | snub transitions, ip_filter disconnects, peer transport errors |
| `BlockDownloading`/`BlockTimeout`/`BlockUploaded` | request send, RTT timeout, piece send (upload category) |
| `UnwantedBlock`/`RequestDropped`/`InvalidRequest`/`IncomingRequest` | pipeline validation paths in `peer_conn`/`peer_pipeline`; `IncomingRequest` at **accept** time (queued for service), not message receipt; `RequestDropped` is dead upstream (variant exists, no emission site) |
| `IpBan` | `set_ip_filter`/`apply_ip_filter` disconnect loops, `ban_peer` |
| `TrackerAnnounce`/`TrackerWarning`/`Trackerid`/`UdpError` | announce send, non-fatal tracker warns, `trackerid` capture, UDP socket errors |
| `UrlSeed` | web-seed connect/EOF/failure in `maybe_connect_web_seeds`/`WebSeedFetch` results |
| `OversizedFile` | `initialize_storage`/`set_size` >4 GiB-on-FAT32-style guard |
| `FilePrio` | `prioritize_files`/`file_priority` mutation |
| `SaveResumeDataFailed` | `save_resume_data`/`write_resume_data` error path **and** the `if_*_changed`-suppressed save (upstream posts `resume_data_not_modified`, not silence) |
| `AnonymousMode` | **dead upstream** — variant exists (id 59) but no `emplace_alert` site in C++; kept for table parity |
| `AddTorrent` (params+error) | `add_torrent`/`async_add_torrent` entry point |
| `TorrentLog`, `PeerLog`, `PickerLog`, `Log`, `I2p`, `Socks5` | gated on `*_log` categories — emit at upstream sites where the machinery exists; sites whose machinery isn't ported emit nothing (documented) |
| `AlertsDropped` | `AlertManager::post` overflow → queue one on next accepted post |
| DHT family | Phase L sinks (D4/D5) + `DhtPkt` extended to full `dht_pkt_alert` (dir, addr, msg) |

### D9 — Extension factories: instantiate, scoped plugin seam

- `Torrent::extensions` factories are invoked when the torrent starts
  (upstream `torrent::start` → `add_extension_func` application) and
  for late `add_extension` calls immediately; results stored as
  `Vec<Arc<dyn SessionPlugin>>` on the torrent.
- `SessionPlugin` gains one hook: `fn new_peer_connection(&self,
  peer: &PeerInfo) -> Option<Arc<dyn PeerPlugin>>` (default `None`),
  matching upstream `torrent_plugin::new_peer_connection`. `PeerPlugin`
  is a minimal trait (`on_message(ext_id, payload) -> bool`,
  `on_tick`) — the full `plugin`/`torrent_plugin`/`peer_plugin`
  surface stays **deferred with reason** until a concrete consumer
  (OnionBit IPv8) needs it; recorded in PORTING.md.
- Built-in extensions (smart_ban, ut_metadata, ut_pex, i2p_pex,
  ut_holepunch) keep their dedicated paths — refactoring them onto the
  generic seam is not required for parity.

### D10 — BEP-17 `http_seed` (GetRight) fetch completion

`net::web_seed` gains the `http_seed` endpoint kind: URL join is
`base_url + file_path` (no `?piece=` query), `Range` headers
identical, GetRight retry/redirect semantics from
`http_seed_connection.cpp` (redirect updates seed URL, 4xx/503 →
backoff `retry_web_seed`). `maybe_connect_web_seeds` handles both
endpoint kinds; BEP-19 path unchanged.

### Deferred (with reason)

- **WebRTC concrete backend** — `rtc_stream`/`rtc_signaling` traits
  are ported; the `RtcPeerConnectionFactory` impl is a leaf crate per
  ADR-0011 (`str0m` preferred over `webrtc` for no unsafe — decided at
  implementation). Only affects WebTorrent peers.
- **`posix_fadvise`/`F_NOCACHE`** — read-mode `io_buffer_mode` parity
  on Linux/macOS; perf hint only, no wire/state semantics.
- **`set_load_function`** — `dlopen` user plugins; won't port
  (incompatible with safe-Rust process model, recorded already).
- **`local_*_rate_limit`** — deprecated upstream.

## Alternatives considered

- **Callback/oneshot results** (`dht_get_item(target) -> impl
  Future<Item>`): rejected — diverges from upstream semantics, doubles
  the consumer surface (alerts + channels), loses mask filtering and
  ordering, and every upstream-ported test expects alert delivery.
- **Keep compacted `AlertCategory` bits**: rejected — `alert_mask` is
  persisted in `.session`/`session_params`; interop requires upstream
  bit positions.
- **One ADR per item**: rejected — the program is one decision
  ("surface the remaining upstream API through the alert stream with
  upstream field/category fidelity"); the emission table is the spec.

## Consequences

- The handle+alert surface reaches functional parity for GUI
  consumers: lifecycle, progress, file progress, peers, trackers,
  DHT ops with results, port mapping, external IP, stats.
- `Alert` grows to ~100 variants — `alerts.rs` splits payload structs
  into `alerts/` submodules if it exceeds readability limits; the enum
  itself stays one type.
- Breaking change pre-1.0: `AlertCategory` bit values, `Alert` gains
  variants (non-exhaustive matching already required), `SessionHandle`
  gains overloads, `delete_port_mapping` becomes real.
- All additions ride existing machinery (`AlertManager`, category
  mask, notify hook, `SessionOut` effects, injected transports) — no
  new runtime model, no `unsafe`, no new dependencies.

## Test plan

- Port `test_alert_types.cpp` verbatim: every variant asserts its
  upstream `category()`, `alert_type` id and `priority` — this is the
  conformance table, it lives next to the enum.
- Category-0 delivery: `alert_mask = 0`, `post_session_stats` /
  `post_dht_stats` still deliver `SessionStats`/`SessionStatsHeader`/
  `DhtStats`.
- `alert_mask` round-trips through `session_params` byte-exact vs an
  upstream-written fixture; `AlertManager` default = `error`; setting
  a mask filters exactly upstream's categories.
- Priority-weighted limit: queue full of normal alerts still accepts
  a critical one; `alerts_dropped` bitset names the right types.
- Golden fixture: `session_params` with a non-default `alert_mask`
  written by upstream v2.1.2 loads to the identical mask.
- DHT: unit-test sink routing (`GetItemMutable` data →
  `DhtMutableItem` incl. salt echo); live `dht_get_item`/`put_item`
  round-trip on Mainline bootstrap table.
- Portmap: mock `UpnpDriver`/`Natpmp` → `Portmap` alert on map,
  `delete_port_mapping` issues the delete request.
- `MetadataReceived` on the existing magnet live test;
  `FileCompleted` on a multi-file fixture download.
- `AlertsDropped`: set `alert_queue_size=1`, generate 2 alerts, assert
  third slot carries the dropped count.
