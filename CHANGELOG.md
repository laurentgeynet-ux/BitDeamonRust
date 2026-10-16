# Changelog

All notable changes to BitDaemon are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).
Until `1.0.0`, breaking changes may occur in any release.

## [Unreleased]

### Fixed

- **Tracker parity, round 2 (A04)**: the residual deviations from the
  post-apply re-audit are resolved. `prioritize_udp_trackers` now
  performs upstream's same-hostname udp↔non-udp *swap* across tiers
  and is actually invoked at the torrent ctor and
  `replace_trackers` under `prefer_udp_trackers` (it was dead code
  with wrong intra-tier semantics). `stop_tracker_timeout <= 0`
  fully suppresses stopped announces — no more phantom
  `fails++`/`TrackerError` on pause. Dispatch stamps
  `next_announce = min_announce = now` so stale `min interval`
  values no longer delay post-failure retries. `add_tracker` marks
  resume data dirty and both `add_tracker`/`replace_trackers`
  announce immediately when the torrent is announcing. Session
  pause aborts in-flight non-stopped announces with
  `announce_skipped` bookkeeping (upstream `abort_all_requests`)
  and a per-request `sent_seq` drops stale completions.
  `trackerid` is lossless bytes end-to-end with the alert fired
  only on change, `warning message` lands on the endpoint's
  `message` field, dict-peers echoing our own `peer_id` are
  skipped, scrape replies no longer mark the tracker `verified`,
  `ipv4`/`ipv6` announce params are populated for private
  non-anonymous torrents from the listen sockets, forced by-index
  announces honor `announce_trackers`, the failure cascade
  propagates `high_priority`, UDP tracker replies from a wrong
  source endpoint are ignored, endpoint resolution filters
  address family symmetrically, `stopped` announces resolve DNS
  cache-only, and the WS announce clamps `numwant` to
  `max_webtorrent_offers`, always serializes `offers`, and
  short-circuits `num_want == 0`.
- **uTP transport parity, round 3 (A05)**: the nagle drain no longer
  computes remaining space with unsigned arithmetic — a buffered
  packet resumed under a lower effective MTU (probe → floor
  transition) underflowed `size_left`, panicking debug builds and
  appending over-MTU payload in release; upstream's signed
  `size_left > 0` gate is now verbatim. Write-after-close returns
  `BrokenPipe` (upstream `check_fin_sent`) instead of overwriting a
  queued FIN. Datagram send errors reach the socket:
  `EMSGSIZE` → MTU ceiling lowered + immediate DF-less retransmit,
  `WouldBlock`/`TryAgain` → stalled leg, others → stored error +
  `ErrorWait`. Timeout resends unlink `needs_resend` so the next
  `send_pkt` no longer re-emits an in-flight packet; a pending
  `poll_read` wakes to EOF as soon as the buffer drains to the
  peer's FIN; teardown with in-flight inbound data still emits FIN.
  The ten `utp_*` session settings (target delay, LEDBAT gain,
  timeouts/resend caps, loss multiplier, cwnd reduce) plus
  `proxy_socks5` are mapped into `UtpConfig` — previously dead
  knobs — and hot-applied via `apply_settings` along with
  `connections_limit` and the `enable_{in,out}going_utp` gates.
  `wait_connected` propagates the socket's stored error; the peer's
  close reason is stamped on arrival and exposed as
  `UtpStream::their_close_reason()`; the demux filter accepts any
  version-1 packet type so unknown types matching a socket count as
  `utp_invalid_pkts_in` instead of leaking to the DHT consumer; and
  inbound-queue drops are traced rather than silent.
- **uTP transport parity (A05)**: `UtpSocket::close()` now carries
  upstream's `nagle_packet`/`outbuf` guards — a FIN can no longer
  leave the wire with piggybacked payload; `poll_shutdown` and stream
  `Drop` actually emit ST_FIN through the socket pump instead of
  silently unregistering (the FIN path was dead code); `close_reason`
  can be set on the socket and rides the FIN's extension chain.
  MTU probes propagate `dont_fragment` through
  `DatagramSocket::send_to_flags` — `TokioUdpSocket` toggles
  `IP_MTU_DISCOVER`/`IP_PMTUDISC_PROBE` (rustix, Linux) around the
  send so over-MTU probes fail instead of fragmenting. Receive path:
  inbound datagrams drain as one batch → a single deferred STATE
  ack per burst; a stalled reader no longer wedges the pump
  (`sock.read()` only when the read channel has capacity); `route()`
  clones the socket sender under the lock and `try_send`s outside it,
  dropping on full queues instead of head-of-line blocking.
  `accept_syn` rejects non-v1/non-SYN datagrams and honors
  `incoming_packet`'s verdict before registering the socket; the
  dormant `UtpSocketDemux` allocates random ids keyed `(endpoint,
  conn_id)` like upstream `equal_range`+`match`. Read errors surface
  as `ConnectionReset`/`ConnectionAborted` rather than clean EOF;
  `abort()` chains `test_socket_state()`; the 11 uTP counters
  (`utp_packets_*`, `utp_*_pkts_in`, loss/timeout/resend, LEDBAT
  samples) plus the per-state `num_utp_*` gauges are wired through an
  injected `UtpCounterSink` fed from `Session.counters`; and
  `remove_datagram_socket` aborts every socket bound to a dead
  transport (upstream `remove_udp_socket`).
- **Peer-wire codec & disconnect severity (A01)**: the frame
  length-prefix cap is now upstream's 1 MiB (`packet_size >
  1024*1024` → `packet_too_large`); a `piece` frame shorter than the
  9-byte header is consumed silently like upstream `on_piece` instead
  of failing the codec. Disconnect severity is typed end-to-end
  (`disconnect_severity_t`): `peer_error` posts `peer_error_alert` +
  `error_peers` (never `peer_disconnected_alert` — the alerts are
  mutually exclusive upstream), while normal/failure closes post
  `peer_disconnected_alert` exactly once. The engine's `gone`
  channel now carries the kill reason (`PeerGone`), so socket EOF/
  reset and codec violations reach the session with their severity —
  `peer_connection::disconnect`'s counter legs are wired
  (`disconnected_peers`, per-io-kind `eof`/`connreset`/… peers,
  per-reason `uninteresting`/`too_many`/`no_memory`/timeout peers,
  `error_tcp`/`error_utp` + `error_outgoing`/`error_incoming` on
  `> normal`), and severity mappings were corrected to upstream
  defaults (`self_connection`, filter bans, `too_many_connections`,
  `no_memory`, `too_frequent_pex`, `peer_banned`/smart-ban are normal
  closes, not peer errors). FAST-extension messages
  (`RejectRequest`/`AllowedFast`/`SuggestPiece`/`HaveAll`/`HaveNone`)
  now disconnect as `invalid_*` unless the handshake negotiated FAST;
  a non-FAST `Choke` rejects every download-queue entry through
  `incoming_reject_request` as upstream does. `incoming_dht_port`
  adds the announced endpoint unconditionally, flips `supports_dht`,
  and replies with our DHT port once the post-handshake bitfield step
  ran (`m_sent_bitfield` now stamped on every `write_bitfield` leg,
  including have_all/have_none/super-seed that send no bitfield
  bytes). `disconnect_if_redundant` runs after the extended
  handshake, matching upstream's `on_extended` tail.

- **rustls crypto provider ambiguity**: HTTPS tracker announces and
  SSL torrent configs no longer panic with "no process-level
  `CryptoProvider` available" when the embedding application
  compiles in a second provider (e.g. `ring` next to `aws_lc_rs`).
  All rustls `ClientConfig`/`ServerConfig` now build through
  `builder_with_provider` on an explicit shared `aws_lc_rs`
  provider instead of the ambient process default — the daemon
  stays self-sufficient and never pre-empts the app's own
  provider choice. `peer_client_config`/`tracker_client_config`
  now return `io::Result`.
- **Reject-request parity (download path)**: `incoming_reject_request`
  now validates the reject shape first (in-range piece, aligned
  start, exact block length) — a malformed reject is a no-op
  upstream, and so is BitComet's zero-length `piece` quirk (the
  `peer_error` alert still posts, but no queue/picker state moves and
  no re-pick runs). Valid rejects drop the download-queue entry and
  `outstanding_bytes` by the rejected length; `abort_download` now
  honors `!timed_out && !not_wanted` (a timed-out block keeps its
  picker state), parole peers requeue the block at the front of the
  request queue instead of freeing it, and seeds never abort. The
  graceful-pause check and conditional re-pick run only for
  well-formed rejects.
- **File-priority / storage-ops parity**: `prioritize_files` and
  `file_priority` now dispatch `async_set_file_priority` through the
  disk queue and apply priorities in the completion
  (`on_file_priority`), matching upstream. Concurrent updates
  serialize through `outstanding_file_priority` +
  `deferred_file_priorities` (per-index merge, last-writer-wins,
  replayed by the in-flight job's completion) instead of issuing
  concurrent disk jobs. The storage-*adjusted* priority vector feeds
  piece priorities and `m_file_priority`; `file_prio_alert` posts on
  completion only (never on error), failures post `file_error_alert`
  and `set_error`+`pause` the torrent. Pad files are forced to
  `dont_download` before dispatch (`fix_priorities`). Add-time
  `file_priorities` and the magnet `init` path apply directly to the
  fresh picker — no spurious disk job or `file_prio` alert.
  `move_storage` propagates the `move_storage` status bits
  (`need_full_check` → `force_recheck`, `fatal_disk_error` /
  `file_exist` → `storage_moved_failed_alert` carrying `file`/`op`),
  and `storage_moved_alert` reports `old_path`. `rename_file`
  bounds-checks the file index against the layout (metadata-less
  torrents return silently like upstream), resolves `old_name`
  before updating `m_renamed_files`, and marks resume/state dirty in
  the completion — a failed rename leaves resume state clean.
  `delete_files` keeps the disk fence ordering and posts
  `torrent_deleted{,_failed}` from the completion.
- **Piece-lifecycle parity (write/hash verification)**: in-flight
  disk write/hash completions now carry `picker_generation` and are
  dropped when a `force_recheck`/`handle_inconsistent_hashes`
  invalidated the picker (upstream `++m_picker_generation` gate).
  `piece_failed` no longer restores inline: it runs `lock_piece` +
  a `clear_piece` disk fence, then `on_piece_sync` restores — only
  the v2 bad-block list when the `hashes` message pinpointed them —
  posts `hash_failed_alert` after restoration, and re-marks
  in-flight duplicate requests for the restored blocks. Write
  failures keep the separate `on_piece_fail_sync` continuation
  (whole-piece restore + re-drive of newly interesting peers,
  `hash_fail_piece_picks`).
- **Piece-failure blame**: `found_on_disk` pieces (no swarm
  downloader) skip `add_failed_bytes`, plugin `on_piece_failed` and
  peer penalties; `penalize_peers` applies parole, trust −2
  (floor −7), `banned_for_hash_failure`, `update_want_peers` and a
  `failed`-flagged disconnect (`too_many_corrupt_pieces` is a
  peer error upstream).
- **Piece counters**: `num_piece_passed`, `num_piece_failed`,
  `num_have_pieces` are now incremented at the upstream sites;
  `we_have` is split from `piece_passed` so pad-only, checking and
  accepted-fastresume pieces get `Have` broadcast without download
  rewards/counters; per-file completion edges fire
  `file_completed_alert` (re-armed when a piece is lost or the
  picker rebuilds); `m_last_download`/`last_payload_down` stamps on
  the verification edge, not block arrival.
- **Wire-compat fix (upload path)**: `PeerMessage::DontHave` now
  carries the peer's negotiated `lt_donthave` extension id and
  encodes as BEP-10 `msg_extended` (10-byte frame). It was
  previously emitted as standard id `0x0f` + 4-byte piece index,
  which libtorrent parses as `have_none` and rejects with a
  disconnect (payload must be 1 byte). Outbound `DontHave` is
  gated on the negotiated `lt_donthave` id (predictive-piece and
  disk-read-error paths); outbound `RejectRequest` is gated on
  BEP-6 `supports_fast`. Inbound `have_none` with a payload is a
  decode error (disconnect), matching upstream
  `invalid_have_none`; inbound `lt_donthave` drops malformed
  frames silently and disconnects on out-of-bitfield indexes.
- **Request/reject counter parity**: `piece_requests`,
  `invalid_piece_requests`, `max_piece_requests`,
  `choked_piece_requests`, `cancelled_piece_requests` and
  `piece_rejects` are now incremented at the upstream sites
  (`piece_rejects` counts the reject decision even when BEP-6
  suppresses the wire message); `num_peers_up_requests` gauge
  decrements when predictive-piece drains empty a queue;
  `request_latency` blends the disk-read RTT at 5%
  (`blend_stats_counter`) using the issue-time stamp carried
  through `async_read`; per-message `num_incoming_*` /
  `num_outgoing_*` counters wired at `dispatch` and the outbound
  `send_to_peer` funnel, including the upstream FAST-range
  `num_incoming_suggest + packet_type` arithmetic quirk.
- `m_last_sent_payload` stamped per drained request (rejects
  included) and `fill_send_buffer` aborts on `is_aborted`, per
  upstream.
- Queue-full request rejections no longer raise
  `invalid_request_alert` (upstream logs only; the alert is for
  malformed/out-of-range requests).
- **Extension protocols — `ut_metadata`, `ut_pex`, FAST, holepunch,
  smart-ban (A02)**: plugin-extension messages (`ut_metadata`,
  `ut_pex`, `i2p_pex`) now disconnect `invalid_message` when the peer
  never announced the extension (`m_message_index == 0` upstream);
  local per-torrent plugin ids go to 0 when the plugin is absent —
  `ut_metadata` is no longer advertised on private torrents that
  already hold metadata (a private *magnet* keeps it), `ut_pex` stays
  suppressed on private/pure-I2P, and `i2p_pex` needs `!priv && i2p`.
  Malformed `ut_metadata` packets (over 17 KiB, non-dictionary, or
  missing `msg_type`/`piece` ints) disconnect
  `invalid_metadata_message` instead of being silently dropped;
  invalid metadata requests reply `dont_have` without bumping
  `num_incoming_metadata` (the counter lives at the upstream switch
  tail). Serving now honors the 160 KiB send-buffer watermark and
  the 1024-entry incoming-request queue with a `dont_have` fallback,
  a `piece` reply chains `maybe_send_request` immediately instead of
  waiting for the 1 Hz tick, and `dont_have` pushes the retry limit
  out by at least a minute. `send_allowed_set` fires once on the
  first inbound `Interested` (`m_sent_allowed_fast`) instead of at
  attach, actually records `accept_fast`/per-piece counters so choked
  peers can claim offered pieces, and uses the correct `upload_only`
  gate (ours, not the peer's). Negative `SuggestPiece`/`AllowedFast`
  indices are dropped before they can reach the wire. PEX/I2P-PEX
  counters bump only after a parsed-and-processed message;
  `too_frequent_pex` stays normal-severity. Holepunch `connect`
  ignores already-connected/banned targets, marks the entry
  uTP-capable, and carries `set_holepunch_mode` to the dialed conn
  via a `holepunch` flag on the swarm entry. Smart-ban now resolves
  the current peer by address, skips already-banned entries, keeps
  first-recorded digests, bans through `ban_peer` +
  `num_banned_peers` + `update_want_peers`, is disabled on aborted
  and v2 torrents (the plugin is never created upstream), and clears
  its ledger on `disconnect_all`/`remove_peer`. A handshake `p`
  listen-port update goes through `update_peer_port` — including the
  `duplicate_peer_id` disconnect on collision — and metadata
  responses no longer carry a stale PEX index.
- **Download request path vs upstream `peer_connection` (audit A08)**:
  incoming `Piece` now runs upstream's
  `start_receive_piece`/`incoming_piece` split — a block still sitting
  in `request_queue` migrates to `download_queue` on arrival, a
  never-requested block inserts at the front marked `not_wanted` and
  is *written* (upstream consumes the payload) instead of being
  dropped as `piece_unknown`, and `outstanding_bytes` is adjusted at
  insert/pop only. `validate_piece_request` is enforced: a `piece`
  with an out-of-range index, unaligned offset or a length that
  doesn't match `to_req` disconnects `invalid_piece` instead of being
  silently absorbed. The un-snub window moved after the
  `is_downloaded` early return, so redundant blocks no longer clear
  the snub or run `check_graceful_pause` — that check now fires only
  on the disk-write path, and `GracefulCheck` marks the conn
  `disconnecting` before emitting so later effects of the same batch
  observe `m_disconnecting` like upstream. The request-timeout leg of
  `second_tick` is gated on `bw_network` (`reading_socket`): a conn
  stalled on `bw_disk` or quota can't be snubbed/marked timed-out,
  while the piece-timeout leg stays ungated. The seed arm's inline
  `num_peers_down_requests` decrement is gone — the gauge moves only
  through the `sync_down_requests` snapshot (it was double-counting).
  Rejects now carry upstream's tail: `send_block_requests` flushes
  unconditionally after `incoming_reject_request`, and a non-FAST
  `Choke` runs the implicit-reject tail (graceful check, gated
  re-pick, flush) per download-queue entry instead of just emptying
  it.
- **MSE wire** : pads B/C tirés `random(512)` = 0..=512 inclusif comme
  upstream ; `crypto_select` répondeur masqué par `allowed_enc_level`
  puis réduit à un bit (`prefer_rc4`) ; `len_pad > 512` →
  `invalid_pad_size`/`peer_error`, `len_ia` hors `0..=68` →
  `invalid_encrypt_handshake`/`peer_error` ; IA partielle réinjectée en
  tête du handshake BT (`cut(0,20)` upstream) toujours sous RC4 ; skey
  `SHA1("req2"‖ih)` sondé sur v1 **et** v2 tronqué (index obfusqué
  `torrent_list`).
- **Ordre d'opérations** : la décision crypto se prend sur la socket
  connectée (`peer_link_up` = `on_connected`), pas au pick —
  `pe_forced`→MSE, `pe_disabled`→plaintext, `pe_enabled` toggle
  `pe_support` (probe MSE + clear pessimiste + `fast_reconnect` armé,
  ou plaintext + set optimiste) ; SSL force `pe_disabled` ;
  `pe_support` se ré-arme à la fin de PE4 (`mse_done` survit à un
  échec de jambe BT) et se re-clear sur handshake plaintext réussi,
  gates `is_outgoing && pe_enabled`.
- **v2** : le bit réservé `0x10` entrant flippe `protocol_v2` *avant*
  l'écriture de notre réponse ; `0x10` sur torrent v1-only →
  `invalid_info_hash`/`failure` ; la réponse porte
  `associated_info_hash()` = `ih.get(V2)` seulement si
  `has_v2 && protocol_v2` (un torrent v1-only garde son hash v1) ;
  swarm v2-only force `pex_lt_v2` à l'ajout (`add_swarm_peer` + PEX).
- **Sévérité/compteurs** : enveloppe `DialFail`{op, severity, reason,
  encrypted, rc4, mse_done, utp} reproduisant `disconnect()` —
  legs `error_{tcp,utp}_peers` + `error_{incoming,outgoing}_peers`,
  `error_encrypted`/`error_rc4` sous `op==bittorrent` + état crypto,
  `peer_error` vs `peer_disconnected` exclusifs, alertes postées même
  sans torrent attaché ; échec inbound post-attach comptabilisé sans
  bookkeeping peer-entry (`connection_closed(nullptr)`).
- **Retry uTP→TCP** : échec uTP = `normal`, `supports_utp=false` +
  `fast_reconnect` + retry TCP différé (pas un échec TCP classique).
- **Settings** : `sanitize_settings` — `allowed_enc_level` hors
  {1,2,3} → `pe_both`, `*_enc_policy` hors {0,1,2} → `pe_disabled`
  (`EncPolicy::from_int` idem) ; `supportcrypto` annoncé sous
  `in_enc_policy != disabled && announce_crypto_support`.
- **peer_info** : `rc4_encrypted`/`plaintext_encrypted` rapportés
  exactement l'un des deux sur conn MSE (`m_rc4_encrypted` gate).
- **Tracker announces vs upstream `tracker_manager`/`torrent` (audit A04)**:
  the announce pipeline is now per-(endpoint, info-hash version)
  like upstream's `announce_state` machine — `TrackerList::due`
  returns `(entry, version)` picks (hybrid torrents announce v1+v2
  independently), `failed_version`/`sent_version` write only the
  announced slot, and `stopped` reaches only `start_sent` slots.
  Failure semantics match `tracker_request_error`: the failure
  response's `interval`/`min interval` is honored as
  `retry_interval` above the quadratic backoff, HTTP 410 sets
  `dont_try_again` (`fail_limit = 1`), an entry with all supported
  slots dead is `deprioritize`d within its tier, and each failure
  re-runs `announce_with_tracker` so a dead tier-0 hands off to the
  next tier immediately. `tracker_error_alert` carries
  `times_in_row`/`status_code`/`operation_t` like upstream.

- **Announce parameters**: `uploaded`/`downloaded`/`corrupt`/
  `redundant` come from the real transfer counters
  (`report_true_downloaded` subtracts redundant + in-flight
  progress), `left` is `bytes_left` (total − verified have, tail
  piece corrected, upstream's 16 KiB no-metadata fallback),
  `num_want` honors `settings.num_want` (0 on `stopped`), `key` is
  the per-torrent `tracker_key`, `trackerid` echoes the
  tracker-assigned id with the `AddTorrentParams.trackerid`
  (`m_trackerid`) fallback, and `private` marks the request.

- **Tracker I/O deadlines and queue**: HTTP/UDP exchanges run under
  `tracker_completion_timeout` (or `stop_tracker_timeout` for
  `stopped`) with `tracker_receive_timeout` per-read deadlines —
  a silent tracker can no longer pin a slot `updating` forever.
  HTTP(S) announces respect `max_concurrent_http_announces`
  (`high_priority` jumps the queue head, `num_queued_tracker_announces`
  gauge). `AnnounceOptions` are settings-driven (`anonymous_mode`
  sends a generic UA unless the torrent is private, `announce_ip`,
  `min_announce_interval`, `announce_crypto_support`,
  `report_redundant`, `ssrf_mitigation`).

- **HTTP tracker transport**: `User-Agent` and URL-auth
  `Authorization: Basic` headers are sent like upstream
  `http_connection`; cross-origin redirects are re-dialed and
  re-GET verbatim with `Authorization` dropped (≤5 hops). The
  endpoint legs run in upstream order — resolve-all → SSRF
  (loopback + non-`/announce` path dropped) → `req.filter`/`ip_filter`
  → per-endpoint dial loop; UDP retries each resolved address
  (`udp_tracker_connection::fail`), and the BEP-15 `connection_id`
  is cached per tracker IP for `udp_tracker_token_expiry`.
  Tracker wire bytes (incl. UDP/IP overhead) feed
  `sent_tracker_bytes`/`recv_tracker_bytes`.

- **Response folding**: dict `hostname` peers are resolved locally
  unless peer connections+hostnames are proxied (dropped like
  upstream), the tracker `external ip` votes in `ip_voter` keyed by
  the resolved `tracker_ip`, `trackerid` persists on the entry and
  posts `trackerid_alert`, `warning message` posts
  `tracker_warning_alert`, and announce replies refresh the
  entry's scrape fields. Manual announces/scrapes post their
  reply/error alerts unconditionally (`triggered_manually` →
  `sent_manual`).

- **`completed`/`stopped` lifecycle**: `BecameSeed` routes the
  burst through `tracker_list::completed` + the regular selection
  with `updating`/`sent_event` instrumented — `completed` is
  emitted exactly once per version (`m_complete_sent` latch) and
  `stop_announcing` re-arms per-version `stopped` announces.
- **Peer lifecycle & admission vs upstream `new_connection`/`incoming_connection`/`attach_to_torrent` (audit A07)**:
  the inbound accept path now runs upstream's full gate order —
  session paused → transport enablement (`enable_incoming_tcp`/
  `enable_incoming_utp`, `peer_blocked_alert`) → outgoing-interface /
  bound-address verification → `has_incoming_connections` (sticky,
  non-local remotes only) → session `ip_filter` skipped while
  `non_filter_torrents > 0` → `m_torrents.empty()` →
  `connections_limit` + `connections_slack` →
  `incoming_starts_queued_torrents` / active-torrent check →
  `incoming_connections` counter + `incoming_connection_alert`.
  Per-torrent attach adds the `attach_to_torrent` legs: auto-resume
  of paused+auto-managed torrents, `paused`/`aborted`/`graceful_pause`
  rejection at `torrent_paused`/`peer_error`, and the SSL-only
  `requires_ssl_connection` gate.

- **Duplicate & self-connection resolution**: `PeerList::new_connection`
  is now wired into incoming attach — banned entries reject with
  `peer_banned`, endpoint self-connections (dialing our own listen
  address, detected through the pending dial's registered local
  endpoint) ban the swarm entry and tear down both legs at
  `self_connection` failure severity, same-direction duplicates are
  dropped, and cross-direction duplicates resolve by the
  local-vs-remote port comparison with `m_locked_peer` protecting the
  entry during the loser conn's synchronous teardown.

- **Per-connection peer ids**: outgoing connections shake hands with
  a fresh `generate_peer_id` per dial (tracked per torrent in
  `m_outgoing_pids`-equivalent `outgoing_pids` + per-endpoint
  `dial_pids`), so the remote no longer sees a `duplicate_peer_id`
  collision on our parallel dials, and the `is_self_connection`
  probe can spot a peer claiming one of our in-flight ids.

- **uTP → TCP fallback**: a failed uTP dial clears `supports_utp`,
  arms `fast_reconnect` (rewinds `last_connected` so the retry is not
  cooled out), runs `connection_closed` at *normal* severity (no
  `failcount` bump) and immediately re-dials the same entry over TCP
  via `pending_out` — matching upstream's
  `connect_failed → connect_to_peer(pi, ignore_limit)`. `PeerConn`
  carries `fast_reconnect`/`local`/`our_peer_id`/`conn_key` through
  `SessionOut::ConnectPeer` → `DialDone`; dial refusals (ip_filter,
  no transport) and transport failures take their upstream-distinct
  failcount legs.

- **BEP-10 `p` (listen port) wired**: the extension-handshake port now
  reaches `PeerList::update_peer_port`, so inbound peers become
  dialable and collide-merge with tracker/PEX entries; conn↔entry
  bookkeeping binds by `conn_key` so a port rewrite can't strand a
  live conn's teardown.

- **`remove_peer` correctness**: in-flight download blocks are only
  aborted back into the picker when `!timed_out && !not_wanted`
  (a timed-out block already released once would double-decrement
  `num_peers`), `prev_amount_*` byte totals are restored at conn
  creation and folded back at teardown, `outgoing_pids`/`dial_pids`/
  `dial_locals`/`num_connecting`/`num_seeds` bookkeeping is settled
  on every mark-teardown path, and the last conn draining under
  graceful pause completes the pause (`GracefulPauseDone` →
  `do_pause`) instead of posting `TorrentPaused` at pause initiation.
  `triggered_unchoke`/`triggered_optimistic` force the next choker
  pass like upstream's `trigger_unchoke`.
- `choke_peer` émet `Choke` **avant** de drainer la file d'upload avec
  des `RejectRequest` — ordre wire aligné sur `send_choke`
  (`write_choke()` puis boucle de rejects, peer_connection.cpp:3848→3859).
- Une requête de forme valide pour une pièce non détenue (ni
  prédictive ni seed_mode) passe désormais par la branche *invalid*
  complète : `invalid_piece_requests`, `num_invalid_requests`, et le
  rappel-choke tous les 10 / disconnect >300 (inerte comme upstream,
  conservé pour l'identité observable) — auparavant seuls
  `piece_rejects` + alerte étaient produits.
- `block_uploaded_alert.block` = index de bloc
  (`start / block_size`), plus l'offset octet.
- `on_disk_read_complete` ne pousse plus `Piece` ni
  `BlockUploaded` sur une conn `disconnecting`
  (early-return upstream sur `m_disconnecting`).
- Le watermark dynamique de `fill_send_buffer` suit
  `uploaded_last_second` (octets payload du dernier tick,
  `stat::last_payload_uploaded()`), plus l'EMA tout-canal
  `rate_up_speed` — conforme à `buffer_size_watermark` upstream.
- Le rejet super-seed (`piece not super-seeded`) s'exécute côté
  `peer_conn` **avant** `incoming_have_none` : une requête rejetée ne
  ranime plus `m_has_metadata` (ordre upstream
  peer_connection.cpp:2408→2447).
- **Piece-lifecycle blame/reward on dead connections (audit A10)**:
  `penalize_peers` and the `piece_finished` trust loop now resolve a
  picker's downloader key to its swarm entry even when the peer's
  connection is already gone — a `downloader_addrs` key→address map
  remembered at `add_peer` plays the role of upstream's
  `block_info.peer` being a `torrent_peer*` (which outlives
  connections, `torrent.cpp:5136-5203`). A lone/disconnected culprit
  that delivered a hash-failing piece is now still put on parole,
  docked trust −2 (floor −7), credited a `hashfail` and banned via
  `ban_peer` + `banned_for_hash_failure` + `update_want_peers` when
  `known_bad` — with no `DisconnectPeer` emitted since there is
  nothing left to drop (upstream's `if (p->connection)` gate), and
  `peer_ban_alert`'s `pid` empty like upstream's. The symmetric pass
  leg restores the lost `trust_points +1`/parole-clear reward.

- **`Alert::PeerBan` carries `pid`** (`peer_ban_alert` upstream):
  `Option<PeerId>`, populated from the live conn in
  `penalize_peers` and the manual `ban_peer` path; `None` when the
  conn is already gone (upstream's `peer_id()` fallback).
- **Choker slot accounting vs upstream `unchoke_sort` (audit A11)**:
  `unchoke_sort` now returns the *uncapped* `upload_slots` like
  upstream's `return upload_slots` — previously it returned
  `min(slots, peers.len())`, which under-dimensioned
  `unchoke_set_size = allowed - num_opt_unchoke` whenever the
  eligible set was smaller than the quota (e.g. limit 8 with 2
  eligible peers unchoked only 1, the second waiting for the
  optimistic rotation). The `num_unchoke_slots` gauge is also exact
  under the rate-based choker.

- **`ignore_unchoke_slots` conns excluded from the choker**: both
  `recalculate_unchoke_slots`' eligibility sweep and the optimistic
  candidate set now skip `ignore_unchoke_slots` conns (conn class
  set OR the torrent's own set — empty class set exempts, as
  upstream). Previously exempt conns (e.g. the local/loopback class
  upstream always flags) could be sorted into a slot, choked by the
  tail or the uninterested leg, or picked optimistically.

- **Peer-class upload priority orders the unchoke sort**:
  `ChokePeer.priority` now carries `get_priority(upload_channel)` —
  the max of `priority[UPLOAD]` over the conn's and the torrent's
  class sets — instead of a hardcoded 1, so `compare_peers`' first
  key and `upload_rate_compare` actually discriminate by class.

- **Rate-based choker measures the real round interval**: the rate
  threshold divide uses `now - m_last_choke` (new `Session::last_choke`
  stamp) instead of the configured `unchoke_interval`, so an early
  triggered recalc doesn't inflate peer rates.

- **Choker scaler re-arms restored**: `choke_this_peer`
  (`NotInterested`) now re-arms `triggered_unchoke` for
  slot-consuming conns and `triggered_optimistic` when the choked
  peer held the optimistic bit; the eligibility sweep re-arms the
  optimistic rotation when it chokes an optimistic-but-ineligible
  peer; promotion of an optimistic peer into the real unchoke set
  re-arms it too; and the optimistic pass ends with upstream's
  `num_unchoke_slots < num_peers_up_unchoked_all → scaler = 0` leg
  (new `Session::pending_unchoke` consumed by `unchoke_tick`).
  Freed slots now refill on the next tick instead of the next period.

- **`free_upload_slots` counts only slot-consuming conns**: exempt
  unchoked conns no longer eat the per-torrent `max_uploads` budget
  that optimistic candidacy checks (`m_num_uploads` semantics).

- **`incoming_interested` tail ported**: a choked conn re-runs
  `maybe_unchoke_this_peer` on `Interested` (skipped under
  `graceful_pause`/`disconnecting`, routed via new
  `PeerEffect::MaybeUnchoke` → `TorrentEffect::MaybeUnchoke`), and
  an already-unchoked conn gets upstream's redundant `Unchoke`
  re-send for clients that missed the handshake unchoke. The
  `num_have > 0` gate moved from inside `maybe_unchoke_this_peer`
  to the handshake call site — upstream's `incoming_interested`
  re-run carries no such gate (exempt conns unchoke on `Interested`
  even when we hold no pieces).
- **`request_time_critical_pieces` is the real upstream algorithm
  (audit A12)**: deadline pieces are now picked in deadline order from
  peers sorted by `download_queue_time` (bottom decile dropped), with
  the `avg + 4σ + 1 s` horizon gate, `first_requested`/`last_requested`
  tracking, and the stall-based busy re-pick (`pick_time_critical_block`
  — one extra request per block per elapsed `avg + dev/2` window).
  `can_request_time_critical` filters choked/uninteresting/parole/
  snubbed/disconnecting conns and deep queues (> 2× desired).
  `add_request(b, busy, true)` now actually front-inserts into the
  time-critical prefix, and `m_average_piece_time`/
  `m_piece_time_deviation` EWMA update on critical-piece completion.
  Previously the function merely re-drove `drive_peer` per conn and
  stalled deadline pieces were never re-requested.

- **`cancel_non_critical` deferred like upstream's `post()`**: the
  first-deadline sweep is armed (`pending_cancel_non_critical`) and
  runs at the next tick, so several consecutive `set_piece_deadline`
  calls register before the cancel pass — in-flight blocks on later
  critical pieces are no longer cancelled (was: extra wire `Cancel`s).

- **`cancel_non_critical` honors `cancel_request`'s `!is_requested`
  head gate**: blocks the picker no longer tracks (arrived via
  another peer) are left alone — no `not_wanted` mark, no `Cancel`.

- **Deadlines survive pause**: `on_torrent_paused` no longer clears
  `m_time_critical_pieces` — upstream `do_pause` keeps them
  (`torrent_handle::clear_piece_deadlines` is the only erase path).

- **`clear_piece_deadlines` posts `read_piece_alert`
  (`operation_canceled`)** for `alert_when_available` entries and
  restores `low_priority` per upstream `clear_time_critical`; it now
  takes an `out` vec (signature change).

- **Filtered pieces lose their deadlines**: `prioritize_pieces` and
  `update_piece_priorities` run the
  `remove_time_critical_pieces` sweep when priorities actually change
  (`filter_updated`) — a piece set to `dont_download` drops out of
  `time_critical` with a canceled alert instead of pinning
  `time_critical_mode` forever.

- **Deadline completion restores `low_priority`**: the
  piece-verified path goes through `remove_time_critical_piece` —
  `alert_when_available` readback, EWMA stats, priority drop — like
  upstream, instead of a bare erase.
- **`update_piece_state` re-cascades the priority index on every queue
  move (audit A13)**: upstream calls `update(prio, p.index)` whenever
  the piece had a bucket entry — a `downloading → full` transition
  drops it from `m_pieces` entirely (`priority()` → -1) and
  downloading↔reverse moves shift the `prio_factor` adjustment. The
  port only handled `-1 → ≥0` additions, leaving stale entries that
  could re-`add` the piece and let `pick_pieces` hand the same blocks
  out twice.
- **`recalc_share_mode` counts `want().num_pieces` like upstream**
  (audit A13): `num_downloaded` is `max(have, want)` where
  `want = num_pieces − num_filtered − num_have_filtered`; the port
  forgot `- num_have_filtered`, making the `share_mode_target` gate
  slightly too permissive for swarms with filtered-and-owned pieces.
- **Picker loop counters now reach `session_stats` (audit A13)**:
  `PickerStats` is implemented for `&Counters` (mapping the 8
  upstream `piece_picker_*_loops` slots) and all `pick_pieces` /
  `request_a_block` call sites pass the real session bank instead of
  `NullPickerStats` — upstream feeds `ses.stats_counters()`.
  `next_web_seed_blocks` gained a `counters` parameter for this.
- **`incoming_piece` `is_deleted` leg** : un bloc arrivant pendant le
  teardown du torrent (`t->is_deleted()` upstream) n'atteint plus le
  disque et ne marque plus `mark_as_writing` — l'arm
  `PeerEffect::DiskWrite` consulte désormais `aborted` après le leg
  redondant, avant l'un-snub, et supprime via `skipped_writes` les
  effets appariés (`MarkDownloaded`, `GracefulCheck`) comme le early
  return upstream (`peer_connection.cpp:2990`).
- **`update_gauge` sur échec d'écriture** : la complétion
  `DiskCompleted::Write` en erreur appelle `state_updated` pour les
  échecs aborted *et* réels, avant `handle_disk_error`
  (`peer_connection.cpp:3255`) — les jauges de pièces ne restent plus
  périmées jusqu'au prochain tick pour les torrents souscrits.
- `on_file_priority` error path now runs the full `pause()` machinery
  (`TorrentEffect::Pause` → `Session::set_paused` → `do_pause`) instead
  of a bare `paused` flag flip: announce gates off, `disconnect_all`,
  `stop_announcing` + `stopped` announces and `torrent_paused_alert`
  like upstream `torrent.cpp` `on_file_priority`.
- `TorrentHandle::file_priorities()` resizes the stored vector to
  `num_files` with `default_priority` when metadata is valid
  (upstream `torrent::file_priorities`).
- `move_storage` on a metadata-less torrent posts the *requested* path
  in `storage_moved_alert` while `save_path` stores the `complete()`'d
  form (upstream posts `save_path`, stores `complete(save_path)`).
- **Watermark loop drained the entire dirty cache** — `flush_after_insert`
  re-tested `should_flush(num_cached_blocks)` but the level only drops at
  flush *completion* (async jobs), so once over the high watermark every
  insert scheduled Flush jobs for the whole cache instead of stopping at
  the low watermark (upstream `flush_to_disk` exits its passes once
  `buffer_level() <= target`, `disk_cache.cpp:1027`). `DiskCache` now
  tracks `in_flight_blocks` (buffers taken into a Flush job) and the loop
  stops when the *predicted* post-completion level
  `write_blocks - in_flight` reaches the low watermark.
- **Flush level is now global** — the watermark drain evaluated only the
  inserting storage's `num_cached_blocks` while upstream `buffer_level()`
  covers all storages + pending writes (`disk_cache.hpp:691`). The loop
  now tests `write_blocks` (global) and drains `take_coalesced` across
  every storage's cache.
- **Duplicate-block write leaked two counters** — `num_cached_blocks`
  grew unconditionally even when the buffer replaced a filled slot, and
  the superseded writer's `write_blocks` slot was never released (its
  `CACHED` flag made `job_done` skip the decrement and no flush would
  ever retire it) → permanent back-pressure inflation per duplicate.
  `insert` now counts only freshly-occupied slots, and the superseded
  writer has `CACHED` cleared so `job_done` releases its own slot.
- **`remove_torrent`/`abort_jobs` leaked cached-buffer slots** — buffers
  dying with the cache kept their `write_blocks` charge forever;
  `abort_all_writers` now returns the resident-buffer count and callers
  release it (`check_buffer_level` included). Orphaned writers pending
  in the cache's `orphans` queue are now drained and completed on
  `remove_torrent` (previously their handlers could be dropped with the
  cache — upstream requires each handler to run exactly once); the
  `Drop` impl also drains leftover orphans.
- **Writer parked on an out-of-range block** is aborted into `orphans`
  instead of being silently dropped (upstream asserts
  `block_idx < blocks_in_piece`).
- **`need_hasher_kick` re-emission** — added upstream's
  `needs_hasher_kick_flag`: the kick is emitted once per
  `hasher_cursor` position and re-armed by `hashed_block`.
- **`v2_pending` dead counter removed** (never decremented/read; the
  v2-hash-queue deferral it models has no equivalent under the
  whole-piece hasher).
- **`StoreBuffer::insert` no longer overwrites** a duplicate key —
  upstream `unordered_map::insert` keeps the existing buffer and the
  level stays unchanged.
- **`file_pool` : ouvertures concurrentes du même fichier** —
  `m_opening_files` upstream est une *liste* (plusieurs opens en vol
  par `file_key`, ex. un open write pendant un open read-only) et le
  join des waiters n'est tenté que si le fichier est absent du pool
  (`i == key_view.end()`). Le port utilisait une seule entrée
  `HashMap<FileId, …>` : un second opener pouvait réveiller les waiters
  du premier avec son résultat — y compris une erreur — alors que
  l'open encore en cours aurait pu réussir. `opening` est désormais un
  `Vec<OpeningEntry>` identifié par `id` ; chaque opener ne notifie que
  ses propres waiters (`notify_file_open` efface `s_iterator_to(ofe)`,
  `file_pool_impl.cpp:62-98`, `:221-241`). Régression couverte par
  `parallel_openers_notify_only_own_waiters`,
  `parallel_opener_failure_does_not_poison_waiters`,
  `waiter_receives_inflight_result`.
- **`part_file::move_partfile` propage l'erreur du `remove` post-copy**
  — le fallback EXDEV `copy_file` + `remove` tolérait ENOENT sur le
  `remove` ; upstream ne tolère que le ENOENT du `rename` initial et
  propage l'échec du `remove(old_path, ec)` (`part_file.cpp:316-326`).
- **Codes d'erreur `add_file` alignés upstream** : dépassement du cap
  par-fichier (`file_size > max_file_size`) → nouvelle erreur
  `Error::FileTooLarge` (`errc::file_too_large`, generic_category) au
  lieu de `torrent_invalid_length`, qui ne s'applique plus qu'à
  l'overflow d'offset cumulé ; nom de fichier ≥ 4096 →
  `Error::FilenameTooLong` (`errc::filename_too_long`) au lieu d'un
  `io::Error` ad-hoc (`file_storage.cpp:819-843`).
- **`metadata_too_large`** : `from_info_node` rejette un info dict
  ≥ `i32::MAX` (cap `parse_info_section_impl` upstream), et
  `load_torrent_buffer` rapporte le dépassement de `max_buffer_size`
  avec `metadata_too_large` au lieu de `invalid_bencoding`.
- **Fallback `magnet-uri`** : un `.torrent` sans `info` mais avec une
  clé `magnet-uri` est désormais parsé comme un magnet
  (`load_torrent.cpp:159-171`) et retourne des `AddTorrentParams` sans
  `ti` — auparavant rejeté `torrent_missing_info`.
- **Valeurs d'énum `TorrentError`** : `TooManyDuplicateFilenames` prend
  sa vraie valeur upstream 145 (au lieu de 22 implicite) ;
  `MetadataTooLarge` = 100.
- **Magnet v2 / hybrid metadata verification (audit A19)**:
  `set_metadata_verdict` now hashes the raw `info` dict against the
  *complete* `InfoHash` — SHA-1 when v1 exists (with the upstream
  truncated-v2-in-v1-slot fallback: SHA-256 whose first 20 bytes match),
  then full SHA-256 equality when v2 exists (`torrent.cpp:8277-8310`).
  v2-only and v2-keyed hybrid magnets can now actually resolve their
  metadata instead of failing hash verification forever.

- **Post-resolution torrent collision detection**: resolving a magnet
  whose metadata turns out to belong to an already-known torrent now
  mirrors `torrent::set_metadata`/`update_torrent_info_hash`
  (`torrent.cpp:8363-8418`) — both torrents are marked conflicting,
  paused, and a `torrent_conflict_alert`-equivalent is emitted instead
  of silently keeping the provisional session key.

- **`disconnect_if_redundant` sweep after metadata init**: existing
  peers are re-evaluated once the picker/storage exist
  (`torrent.cpp:8445-8447`) — upload-only × upload-only and
  uninteresting upload peers acquired before metadata resolution are
  now dropped at resolve time instead of lingering.

- **`prioritize_pieces` seed gate and resume dirtiness**: matching
  `torrent.cpp:5967-5996`, the call is now a no-op on seeds and only
  sets `need_save_resume` when priorities actually changed
  (`filter_updated`) — a stray priority call can no longer corrupt a
  seed's picker state or dirty resume data on every invocation.

- **`num_total_pieces_added` counter**: now incremented by the resolved
  piece count at the same points upstream does (`torrent.cpp:458`,
  `torrent.cpp:8442`) — previously declared but permanently zero.

- **Magnet resume data preserves the full `InfoHash`**: `to_resume_data`
  no longer writes the (possibly truncated) session-key hash into the
  v1 `info-hash` slot — v2-only magnets round-trip `info-hash2`,
  hybrids keep both hashes (`torrent.cpp:7562`). Previously a v2
  magnet's resume blob was unreadable as anything but a phantom v1
  torrent keyed by the truncated v2 hash.
- **`stop_when_ready` — séquence upstream complète** : le flag est
  désormais restauré depuis `atp.flags`, routé par `set_flags` et le
  setter handle déclenche immédiatement quand le torrent est déjà dans
  un état downloading. L'edge checking→downloading (et le passage
  finished→seeding) exécute la séquence upstream
  `auto_managed(false)` + `pause()` complète (alerte, stopped
  announce) puis désarme le flag — au lieu du `self.paused = true` nu
  à l'edge finished.
- **Disconnect des seeds à `finished()`** gaté par
  `close_redundant_connections`, comme upstream.
- **`need_save_resume` → `ResumeDataFlags` bitmask** : les catégories
  `if_state_changed`/`if_download_progress`/`if_config_changed`/
  `if_metadata_changed`/`if_counters_changed` remplacent le booléen ;
  `save_resume_data(flags)` honore `ONLY_IF_MODIFIED` contre le mask.
- **Démotion auto-manage** ne marque plus le resume dirty
  (`set_paused` upstream ne marque pas ; seul `pause()` le fait).
- **`finished()`** : ajout d'`async_release_files` (→ `CacheFlushed`)
  et re-trigger `manage_queue` immédiat.
- **`resume_download`** renvoie le torrent en fin de queue
  (`queue_position = last_pos`).
- **`do_resume`/`start_announcing`** : reset `total_failed`/
  `total_redundant_bytes`/stats torrent, gate `files_checked`, et
  `high_priority` propagé depuis le connect boost.
- **`StateChanged` par transition** au lieu du diff de fin de tick —
  une alerte par edge, ordre upstream préservé.
- **`TorrentPaused` différé** à la complétion
  `async_stop_torrent`/drain graceful via `paused_alert_pending`.
- **Ctor** : restauration des flags `atp.flags` manquants —
  `pinned`, `apply_ip_filter`, update subscription, sequential /
  upload / share / super-seeding, disable DHT/LSD/PEX.
- **Resume `pieces` bitfield gating (audit A21)**: `to_resume_data`
  no longer writes `have_pieces` unconditionally. Matching
  `torrent.cpp:7624-7650`, nothing is emitted before `files_checked`,
  and a mid-`checking_files` snapshot emits only the
  `num_checked_pieces` prefix. Previously an unchecked torrent saved
  an all-zero bitfield which read back as *valid* resume data —
  suppressing the required full disk check and ignoring complete
  on-disk data.

- **Partial fastresume (`pieces` shorter than `num_pieces`)**: the
  `CheckFiles` verdict now carries the resume bitfield size; an
  accepted but short bitfield resumes `checking_files` at
  `end_index()` instead of re-hashing everything or accepting
  blindly (`torrent.cpp:2335-2350`, new `Torrent::on_resume_partial`).

- **Rejected fastresume unwinds all claims**: `unfinished_pieces` are
  now held in `pending_unfinished` and applied only in the accepted
  leg of `on_resume_data_checked` (`torrent.cpp:2366-2404`); the
  reject leg additionally clears `seed_mode` and drops the pending
  block claims, so no phantom `Finished` blocks survive a bad resume.

- **`has_error_status` mask includes `file_exist`**, and
  `fastresume_rejected_alert` is gated on `contains_resume_data`
  (`torrent.cpp:2314-2323`) — a no-resume `need_full_check` no longer
  alerts, a verify error now carries its real `operation`/`file`, and
  a nonfatal verify error no longer routes through
  `handle_disk_error` (pause + auto-manage drop). Only
  `fatal_disk_error` does.

- **`need_save_resume` preserved across the verdict**:
  `on_files_checked` saves and restores the dirty flag like upstream
  `on_resume_data_checked`, so a clean fastresume load no longer
  reports `need_save_resume_data() == true`.

- **`save_resume_data` completeness**: a removed torrent now posts
  `save_resume_data_failed_alert(torrent_removed)` instead of
  silence, and a new `flush_disk_cache` flag releases the storage's
  open files (`async_release_files`) before the snapshot
  (`torrent.cpp:10096-10129`). `TorrentHandle::save_resume_data_flush`
  exposes it.

- **Minor resume-data fidelity**: `piece_priority` is omitted when
  every piece is at `default_priority` (`torrent.cpp:7731-7749`);
  deferred peers are shuffled before the 100-peer cap
  (`aux::random_shuffle`); `need_save_resume` is seeded from the
  `NEED_SAVE_RESUME` atp flag at add time; `recheck_piece_done`
  sets `files_checked` + extension replay on drain; seed-mode
  construction treats an empty `have_pieces` as vacuously
  all-present like upstream `std::find`.
- **Settings hot-apply legs (audit A22)**: `apply_settings` now covers
  the upstream legs that were missing (`session_impl.cpp` callbacks):
  `connection_speed < 0` writes back 200, `aio_threads`/`hashing_threads`
  clamp at 0, `user_agent` newlines become spaces (would otherwise
  split web-seed HTTP request headers), `trigger_auto_manage` re-arms
  the queue scaler on `active_*` limit changes, `auto_sequential`
  propagates to live torrents, `update_proxy` rebuilds every
  purpose-slot from `proxy_settings(m_settings)`, and
  `update_dht_bootstrap_nodes` re-seeds the router list on a running
  DHT. `peer_dscp` and `mixed_mode_algorithm` remain documented
  no-live-effect keys like upstream.

- **`reopen_listen_port` gate**: now follows the upstream trigger set
  (`session_impl.cpp:1568-1575`) — `listen_interfaces`, `proxy_type`
  and `proxy_peer_connections` reopen sockets; `outgoing_interfaces`
  only re-parses the bind list and no longer forces a reopen.

- **`connections_limit <= 0`**: mapped to the unbounded cap
  (upstream `max_open_files()`) instead of `max(1)` — setting the
  limit to 0 no longer disconnects all peers but one
  (`session_impl.cpp:6901`).

- **`unchoke_slots_limit` apply**: a finite change only re-arms the
  unchoke scaler (`m_unchoke_time_scaler = 0` upstream); the
  immediate sweep stays reserved for `INT_MAX`→unlimited, and a
  negative limit no longer triggers a spurious unchoke sweep.

- **`.session`/session-params interop**: `SessionState` now carries
  the upstream `session_params` keys — `"dht state"` dict,
  `"extensions"`, `ip_filter4`/`ip_filter6` (12/36-byte stride,
  `first > last` entries skipped like `read_session_params`).
  `load_state` reads an upstream `write_session_params` blob: legacy
  `"dht"` dict seeds `dht_*` keys before `"settings"` overlays, the
  ip filter replaces the live one through `set_ip_filter`, and
  `user_agent`/`peer_fingerprint` are preserved (upstream
  `pack.clear` identity behavior). `SessionHandle::write_session_params`
  now emits the real `session_params` dict instead of the
  torrent-resume container.

- **Settings decoding parity**: `apply_settings_dict` applies any
  *present* string key — an empty string clears the setting
  (`load_pack_from_dict` semantics), and the 1.x alias `peer_tos`
  maps to `peer_dscp` (`setting_by_name` compat).

- **Proxy setters**: `set_proxy`/`set_peer_proxy` write the `proxy_*`
  pack keys through `apply_settings` (upstream `session_handle`
  semantics); `proxy()`/`peer_proxy()` return
  `proxy_settings(m_settings)` so every purpose-slot reflects the
  pack.

### Added

- **UI-facing API completeness (upstream parity sweep)**: every
  surface the desktop client needed is now exposed with upstream
  semantics —
  - `TorrentInfo::comment()`/`creator()`/`creation_date()` plus the
    `internal_set_*` backdoors `parse_torrent_file`/
    `read_resume_data` use to populate them from metainfo and fast
    resume (upstream `torrent_info` carries all three).
  - `Torrent` retains `comment`/`created_by`/`creation_date` from
    `AddTorrentParams` and writes them back on `to_resume_data` —
    the values now round-trip `.resume` like upstream's
    `torrent.cpp` ctor/`write_resume_data` pair.
  - `TorrentHandle::get_peer_info()` returns `Vec<PeerInfo>` for
    the whole swarm in one call (upstream `torrent_handle::
    get_peer_info`); the old per-peer lookup is `peer_info(id,
    addr)`.
  - `TorrentHandle::web_seed_statuses()` reports per-URL
    `WebSeedStatus` (outstanding requests, last error, error count)
    — upstream tracks the same per-`web_seed_t`, and errors are
    now attributed to the seed that actually served/failed the
    request instead of `web_seeds.first()`.
  - `Alert::message()` renders a human-readable line per variant
    (the `alert::message()` surface), and `Alert::TrackerList`
    carries full `AnnounceEntry` state like upstream's
    `tracker_list_alert`.
- **Web-seed per-URL accounting**: `WebSeedDone` carries the seed
  URL and a real error string through `Session::on_web_seed_error`;
  `url_seed_alert` names the failing seed and message, and
  `Torrent` keeps `web_seed_errors`/`web_seed_outstanding` per URL.
- `TorrentHandle::get_file_priority(index)` — upstream
  `torrent::file_priority` getter: `dont_download` for pad files and
  out-of-range/negative indexes, `default_priority` for unallocated
  slots.
- `Alert::FileError` gains the `op` field (upstream
  `file_error_alert.operation`) at all four construction sites.

### Changed

- `TorrentHandle::trackers()` returns `Vec<AnnounceEntry>` (full
  upstream `announce_entry` state — tier, verified, endpoints,
  scrape counters, announce timing) instead of `(url, tier)`
  pairs, matching `torrent_handle::trackers`. Callers consuming
  the tuple form must read `entry.url`/`entry.tier`.
- `TorrentHandle::apply_ip_filter` now routes through the torrent
  setter (`set_apply_ip_filter`) so the change marks resume data
  dirty like upstream's `torrent::set_apply_ip_filter`.

### Added

- Session `on_tick` leg parity: `try_connect_more_peers` verbatim
  (`m_prio_torrents` ticket queue, `connect_seed_every_n_download`
  downloading/finished round-robin split, `smooth_connects`,
  half-open-aware `connections_limit` gating, live `want_peers`
  rechecks), `do_connect_boost`/`m_connect_boost_counter`/
  `inc_boost_connections`/`prioritize_connections` wiring (ctor
  arming, resume re-arm, `start_announcing`/`tracker_response`
  dispatch), the u16 session-time wrap step-back (`peer_list::
  step_session_time`), auto-scrape for paused auto-managed
  torrents, `peer_turnover` disconnect sweep, outbound
  `handshake_timeout`, `outgoing_port` bind rotation +
  `too_few_outgoing_ports`, accept-loop EMFILE/ENFILE/WSAEMFILE
  retry with `too_few_file_descriptors`, and uTP dial-failure →
  `supports_utp` clear + TCP fallback (`confirmed_supports_utp` on
  success). Per-conn `m_stat` channel split lands in `peer_info`
  (`total_*` payload-only, `*_speed` all-channel) and
  `torrent_status` totals (payload+protocol).
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
  `remove_torrent`/`abort`.
- Upstream rate limiting wired end-to-end (`bandwidth_limit`/
  `bandwidth_manager`/`peer_connection::request_bandwidth` consumers):
  the session owns one `BandwidthManager` per direction and the
  class channels are the throttle state; conns carry `PeerQuota`
  (`m_quota`/`m_channel_state` atomics) — the engine's per-peer pump
  caps socket reads/writes at `min(quota, staged)`, queues
  (`bw_limit`) when channels are empty, and resumes on
  `assign_bandwidth` grants via `Notify`, matching upstream's
  `setup_receive`/`setup_send` gates. `wanted_transfer` (outstanding
  bytes + 30 / recv spare + 30 / rate-scaled tick), per-request
  `get_priority` (max over conn + torrent class sets),
  `copy_pertinent_channels` (only throttled channels), the
  `rate_limit_ip_overhead` drain in `check_request_timeouts`
  (`performance_alert::{down,up}load_limit_too_low`), disconnecting
  quota return and `manager.close()` on engine drop all follow
  `peer_connection.cpp`/`session_impl.cpp`. Replaces the bespoke
  per-engine `RateLimiter` buckets; the global/`download_quota_ok`
  approximation for the snub gate is gone — the real
  `m_quota[download_channel] > 0` check is used.
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
  (per-conn quota since the `BandwidthManager` wiring) in
  `check_request_timeouts`.
- `update_stats_counters` port: the disk backend's live gauges now
  land in `session_stats` on each snapshot — `queued_disk_jobs`,
  `blocked_disk_jobs`, `disk_blocks_in_use`, `cached_blocks`,
  `num_unhashed`, the `file_pool_*` set, `num_blocks_*`, `num_*_ops`,
  and the `num_fenced_*` range via upstream's
  `num_fenced_read + get_type()` raw-index arithmetic (stale counter
  names for kinds ≥ `hash2` replicated verbatim).
- `DiskIo::set_file_pool_size` — `settings_pack::file_pool_size`
  hot-resizes the `FilePool` (upstream `m_file_pool.resize` inside
  `settings_updated`); `set_disk` and `apply_settings` now push it
  alongside `aio_threads`/`hashing_threads` so a backend attached
  cold or reconfigured live picks up the full disk knob set.
- `DiskQueueStats::fenced_jobs` — per-`job_action_t` gauge of fence
  jobs currently in the system (incremented at `add_fence_job`,
  decremented when a `fence`-flagged job completes), exposed through
  `queue_stats` for the session-counters sync above.

### Changed

- Incoming block payloads no longer take two full copies on the way
  to disk: `PeerEffect::DiskWrite`/`TorrentEffect::DiskWrite` now
  carry the wire `Bytes` buffer (refcount clone at dispatch,
  refcounted `slice` per covered block on the `request_large_blocks`
  web-seed path), and `session` moves it into the
  `DiskBufferHolder` — the single remaining copy is the one into the
  write cache block, which owns its data.

### Fixed

- **`blocked_disk_jobs` gauge leak on fence repost**: `job_complete`
  returns the number of jobs popped out of a storage's blocked queue,
  and upstream turns it into `inc_stats_counter(blocked_disk_jobs,
  -ret)` — the return was dropped, so every reposted job left the
  gauge charged forever. `PreadDiskIo` now decrements per reposted
  job, matching `update_stats_counters` semantics.
- **`num_write_ops` counted failed flushes**: the per-iovec counter
  incremented unconditionally after `writev`; upstream only counts
  `if (!failed)` — the same condition that governs
  `num_blocks_written`. Failed spans still retire `flushed` and
  error their writers (upstream `flush_cache_blocks` behavior), only
  the stat was wrong.
- **Non-write abort in `insert_write` skipped the completion path**:
  a defensive `debug_assert!` leg pushed the job straight to the
  completed queue, bypassing `job_done` (leaked `write_blocks`
  back-pressure slot) and `job_complete` (the fence's outstanding
  count never drained, wedging a pending fence). It now runs the
  normal `add_completed_job` path.
- **Flush pick order documented/pinned to upstream semantics**:
  `take_coalesced` picks ready pieces first, then the ascending
  `(storage, piece)` sweep. That matches `flush_to_disk`'s reachable
  passes under our whole-piece hasher — upstream's pass 2
  (`cheap_to_flush = hasher_cursor - flushed_cursor`) is
  structurally empty when `hasher_cursor` only advances at hash-job
  time. A regression test pins the order (a longer dirty run must
  not jump ahead of a lower piece index).
- **Duplicate `compute_flushed_cursor`** call in the flush-complete
  path removed.

- **Engine tick starvation under sustained load** (the "runs fine,
  collapses for ~1 min, resumes" report): `Engine::run` created a
  fresh `tokio::time::sleep(tick_interval)` inside the `select!` on
  every loop iteration, so each inbound piece, disk completion or
  sent notification hit `continue` and restarted the deadline —
  under continuous traffic the timer never won and `tick()`
  (quotas, `connect_rx`/`gone_rx`/`announce_rx`/`udp_rx` drains,
  request timeouts, snubbing, dial pump, choker, tracker/DHT/LSD
  work) starved. The swarm then froze, burst into mass inactivity
  disconnects once a quiet gap let the timer through, and resumed.
  A single pinned `Sleep` now lives outside the loop and is reset
  only after firing — the upstream `deadline_timer` semantic.
- **Disk back-pressure gated peers could sleep ~`cache_expiry`**:
  `Session` passed `None` as the `DiskObserver` to `async_write`,
  so a peer write-gated by `max_queued_disk_bytes` was only
  released when its own blocks flushed (bounded by cache expiry)
  instead of when the global queue drained under the low watermark.
  A per-peer `PeerDiskObserver` now reports through the
  `DiskCompleted::PeerDrain` channel — upstream `peer_connection`'s
  `disk_observer`/`on_disk()` leg — and `check_buffer_level` at
  flush drains unblocks gated peers as soon as the queue falls
  under the low watermark.
- **Recheck no longer kills the torrent on a physically-short file**:
  `on_piece_hashed`'s skip-file leg — `UnexpectedEof`/`NotFound`
  (upstream `eof`, `file_too_short`, `no_such_file_or_directory`,
  `partfile_missing_piece`, Windows `ERROR_HANDLE_EOF`) reported by a
  checking hash now just leaves the piece not-have and lets the check
  continue, instead of `set_error` + `pause`. On Windows sparse files
  the never-written tail legitimately reads past the physical end, so
  every `force_recheck`/rejected-resume on a partial download paused
  the torrent permanently (0 peers, stalled). Non-eof errors during
  checking take upstream's fatal leg (`auto_managed(false)` + pause +
  `set_error`), and failed-check pieces no longer emit
  `piece_failed`/`hash_failed` alerts — upstream only posts those from
  the download path (`on_piece_failed`), never from `on_piece_hashed`.
- **`incoming_piece` fidelity sweep** (libtorrent `RC_2_1` @
  `e7381c7bc`): a `piece` message whose payload exceeds the torrent's
  `block_size()` now disconnects the peer (`packet_too_large`, like
  `bt_peer_connection::on_piece` at `recv_pos == 1`) instead of
  feeding up-to-4 MiB into a single cache block. The omitted-bitfield
  leg now runs the real `incoming_have_none` (`m_have_piece` clear +
  `m_num_pieces = 0` + `m_has_metadata`), and the slow-start bump +
  `update_desired_queue_size` moved ahead of the empty-piece/seed
  legs to match upstream's order. A `piece` received while *we* are
  seeding now takes upstream's `is_seed` leg — `piece_seed` waste,
  one stale `download_queue` front entry dropped per message, no
  `unwanted_block` alert — and `was_requested` consults
  `download_queue` only, so a block matching a still-unsent request
  is unwanted exactly like upstream. `mark_block_finished` and
  `torrent_status.down_bandwidth_queue` divide by `block_size()`
  instead of a hardcoded 16 KiB (correct for `piece_length < 16 KiB`
  torrents).
- **Downloaded blocks never reached disk**: `PeerEffect::DiskWrite`
  was emitted *after* `MarkDownloaded` in the same batch, so the
  redundant-block `is_downloaded` consult in `apply_peer_effects`
  (added in `69f2c8e`) observed the just-marked `Writing` state and
  dropped every payload — the swarm downloaded at full rate, progress
  climbed, but `have` stayed at 0, no flush ran and no file was ever
  created. The effects are now emitted in upstream `incoming_piece`
  order (`is_downloaded` consult + `async_write` dispatch, *then*
  `mark_as_writing`); blocks classified seed/redundant suppress their
  trailing `MarkDownloaded` exactly like upstream's early return. The
  receipt-time `block_finished_alert` emission is also gone — upstream
  posts it from `on_disk_write_complete` only (the session already
  does), so the alert no longer double-fires nor fires for blocks the
  disk rejected.
- Linux CI compile fix: `drive_kind`'s `queue/dax` probe compared
  `Result<&str, &io::Error>` with `==` (`io::Error` isn't
  `PartialEq`); the `#[cfg]`-gated arm only compiled on Linux, which
  is why the failure never showed on Windows. Also dropped a stray
  `unsafe` wrapper around `format!` in the same probe. CI matrices
  gained `fail-fast: false` so a Linux failure no longer cancels the
  Windows leg mid-run. `ViewEntry::mode` (`bitdaemon-mmap`) is only
  read by the Windows sparse-clear `Drop` path — it is now
  `cfg_attr`-`allow(dead_code)` off Windows so `RUSTFLAGS=-D warnings`
  stops failing the Linux legs (upstream stores `m_open_mode` on every
  platform, so the field stays).
- Upstream-sync fixes (libtorrent `RC_2_1` up to `v2.1.2-62-ge7381c7bc`):
  `file_piece_range_exclusive` no longer emits an inverted range when a
  mid-piece-start file owns no whole piece — `end` is clamped to `begin`
  and `index_range` asserts `begin ≤ end` (upstream `e7381c7bc`; the
  inverted `IndexIter` looped forever and could overflow `i32`).
  Trackers whose URL scheme is unsupported (`http|https|udp|ws|wss`
  only) are now skipped in `due()`, `next_announce_time`, force-announce
  and scrape paths instead of falling into the HTTP dispatch arm — they
  stay pristine rather than being marked failed (upstream `e0716cf24`).
  WebSocket trackers propagate a correlated `failure reason` as an
  announce error and ignore uncorrelated failure frames (no `info_hash`)
  while continuing the read — previously a failure frame produced a
  successful all-zeros response; announce intervals are clamped to
  `min_websocket_announce_interval` (upstream `0ee23cc89` + `73f4832de`).
  DHT `peer_prefix_limit`/`peer_prefix_limit_full` regression tests
  ported (the IPv6 /56 cap itself was already faithful — upstream
  `6089cdeea`). `bind_to_device` device-name socket binding is an
  explicit won't-port decision (upstream `3f5989b60`; see PORTING.md).
- `peer_class::set_*_limit` clamp kept `0` as *unlimited* like
  upstream (`limit < 10 && limit > 0 → 10`); a `0` limit was
  wrongly throttled to 10 B/s on the `set_peer_class` path.
- Bandwidth prefetch parity (`setup_receive`/`setup_send` head calls):
  `request_bandwidth` now runs unconditionally at the top of the
  engine pump's read and write gates, topping a queued request up to
  `wanted_transfer` so its grant lands before the current quota
  drains — upstream requests on every receive/send setup, not just
  on exhaustion. Previously the conn stalled up to one
  `update_quotas` round (500 ms) each time quota hit zero.
- `m_disconnecting` parity for bandwidth queues: arming
  `PeerQuota::disconnecting` now happens where `c.disconnecting =
  true` is decided (pause `disconnect_all`, graceful-pause idle
  conns, `too_many_connections` teardown), not only at
  `remove_peer`/pump exit — `update_quotas` drops a tearing-down
  conn's queued requests on the next pass and returns their quota
  like upstream.
- `rate_limit_ip_overhead` accounting drained the conn's IP/TCP
  overhead counters in *two* places (session tick head *and*
  `check_request_timeouts`); the first drain left the
  upstream-equivalent site (conn `second_tick`) reading zeros. The
  duplicate was removed — the overhead debit and the
  `download_limit_too_low`/`upload_limit_too_low` performance alerts
  now see real numbers.
- Engine tick split matches `session_impl::on_tick`: quota
  distribution (`update_quotas`) rides the 500 ms `tick_interval`
  timer while the per-conn `second_tick` work is gated on ≥1 s
  elapsed (`m_last_second_tick`); the run loop sleeps the
  settings-live `tick_interval` like upstream's `m_tick_timer`.

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

### Fixed — `bitdaemon-mmap` upstream-fidelity alignment

`bitdaemon-mmap` (`file_view_pool`/`mmap_storage`) and the shared
`bitdaemon-disk` storage paths were re-audited against upstream
`mmap.cpp`/`mmap_storage.cpp`/`file_view_pool.hpp`/`file_pool_impl`/
`mmap_disk_io.cpp` @ `v2.1.2-45-gc019e80ef` and every discrepancy was
repaired:

- **Part-file export offset** (`export_file` consumers, both
  backends): the callback receives an offset *relative to the
  exported range*; the write sites added `file_offset` a second time,
  so any file at index > 0 wrote past the end of its view/handle —
  silently returning 0 bytes while the part-file slots were freed
  (data loss on `dont_download`→wanted transitions). Export closure
  now propagates `Result` so a failed write aborts the export and
  keeps the slots, like upstream's exception path.
- **Zero-sized and small files**: `file_view_entry` now opens the fd
  but skips the mapping when the range is empty or `no_mmap` is set
  (`has_memory_map()==false` → `pread`/`pwrite` positioned I/O), and
  `mmap_file_size_cutoff` (40 × 16 KiB blocks upstream default) routes
  small files through `no_mmap`. `initialize` on a torrent with a
  0-byte file no longer dies on `CreateFileMapping`/`mmap` EINVAL.
- **View pool keyed by `(StorageIndex, FileIndex)`** like upstream
  `file_pool_impl`, with serialized concurrent opens (hits/misses/
  stalls counters), deferred destroy outside the lock, per-file
  open/unmap serialization, write-upgrade and size-change
  invalidation, scoped `release_one`/`release_storage`/`release_all`,
  `close_oldest`, `get_status` and `resize`. Clearing a torrent's
  views no longer evicts every torrent in a shared pool.
- **mmap write policy**: `initialize` resolves `m_use_mmap_writes`
  from `disk_write_mode` (`always_pwrite`/`always_mmap_write`/
  `auto_mmap_write` → `drive_info` `ssd_dax` gate) and disables full
  allocation on remote drives (`get_drive_info` ported to
  `fs::drive_kind` — Linux sysfs/DAX, Windows `GetDriveType`/DAX).
  `volatile_read`/`flush_piece` job flags translate into
  `madvise(MADV_COLD|DONTNEED)` and `page_out`/`msync` advice on the
  mapped range like upstream `hash`/`read`/`write` paths.
- **Priority transitions**: `set_file_priority` assigns the new
  priority before the transition work (the prio-lift `open_file` no
  longer opens `sparse`), restores the old priority on failure, opens
  the target file unconditionally, then exports part-file data.
- **Maintenance machinery**: `DiskStorage` grew `set_need_tick`/
  `do_tick`/`close_oldest`/`file_status`/`set_open_mode_extra`/
  `set_mmap_file_size_cutoff`/`set_disk_write_mode`/`set_no_cow`
  (default no-ops keep downstream `DiskStorage` impls source-compat).
  `disk_io` schedules deferred part-file metadata ticks ~2 min after
  uncached writes and kicks `close_oldest` on the
  `close_file_interval` cadence; the session pushes `no_atime`,
  `allow_set_file_valid_data`, `no_cow`, `no_cache`, write-mode and
  cutoff settings on `set_disk`/`apply_settings`.
- **View release coverage**: pooled views are released on
  `release_files`, `rename_file`, `move_storage`, `delete_files`,
  storage `Drop`, and after `initialize` (upstream closes the
  temporary open handles).
- `Operation::FileMmap` added so mapping failures surface with the
  right alert operation instead of `file_open`.

Regression tests: `part_file_export_lands_in_file` (both backends),
`zero_sized_file_initialize`, `below_cutoff_uses_fd_fallback`,
`read_missing_file_no_dirs`, `volatile_and_flush_hints_roundtrip`,
`precomputed_v2_roundtrip`, `release_is_scoped_to_storage`,
`external_view_survives_delete`, `status_and_close_oldest`,
`move_storage_reopens_at_new_path`.

### Fixed — `bitdaemon-disk` upstream-fidelity audit (full crate sweep)

`bitdaemon-disk` (`disk_io`/`disk_cache`/`storage`/`storage_utils`/
`part_file`/`fs`/`file`/`file_pool`/`stat_cache`/`hash_picker`/…)
was re-audited line-by-line against upstream `pread_disk_io.cpp`,
`pread_storage.cpp`, `disk_cache.cpp`/`aux_/disk_cache.hpp`,
`storage_utils.cpp`, `part_file.cpp`, `file.cpp`, `file_pool_impl.cpp`,
`copy_file.cpp`, `path.cpp`, `truncate.cpp`, `error_code.hpp` @
`v2.1.2-45-gc019e80ef` (delta `c019e80ef → d143290bb` covered), and
every discrepancy was repaired:

- **v2 block hashing bounded to `piece_size2`** in all three sites:
  `hash_job`, `hash2_job` (both the disk-read *and* the cached-block
  paths) and the flush-time stored block hashes. The port hashed the
  full 16 KiB buffer; upstream hashes only
  `min(block_size, piece_size2 - offset)` bytes
  (`v2_hash_queue_entry::hash_len`, `disk_cache::hash2`,
  `pread_storage::hash2`) — a hybrid piece whose last v2 block
  straddles a pad/file boundary produced wrong SHA-256 block hashes.
- **Strict positioned reads**: `storage::read`/`hash_with`, part-file
  slot export and the mmap fd fallback now use `pread_all`, surfacing
  `file_too_short` on short reads like upstream instead of silently
  accepting them.
- **`StorageError` truth**: `is_ok()`/`is_err()` now follow the
  recorded error code (upstream `operator bool() = ec.value() != 0`),
  not `file`/`operation` sentinels — aborted jobs (`Interrupted`,
  `FileIndex(-1)`, `Unknown`) no longer report success.
- **`stop_torrent`**: `async_stop_torrent` now releases the torrent's
  pooled files and stamps queued volatile `hash`/`hash2` jobs aborted
  at submit time, so a stopped torrent's parked hash work completes
  with `operation_aborted` instead of running or leaking.
- **`file_priority` job** writes the *stored* priorities back to the
  disk result on failure too — upstream `set_file_priority` restores
  and reports the pre-failure state.
- **`delete_files` error ordering**: per-file errors are last-wins,
  directory cleanup errors first-wins, and a part-file removal error
  always wins — matching upstream `storage_utils.cpp` overwrite rules.
- **`fs::copy_file`** now always performs a real sparse-aware copy;
  the hard-link preference was dropped (upstream `copy_file` never
  links — `DontReplace` moves must not leave source and destination
  sharing an inode).
- **`disk_io_write_mode` semantics**: `write_through` is plumbed
  session → `DiskIo` → storage and runs `sync_file` after each landed
  write (pread backend); `disable_os_cache`/`write_through` set
  `open_mode::no_cache` on *every* open, reads included (upstream
  `pread_storage::open_file`/`mmap_storage` mode check);
  `disk_io_read_mode = disable_os_cache` no longer sets an open flag —
  it routes `volatile_read` on peer and `read_piece` reads, landing as
  `advise_dont_need` after the read.
- **`initialize`**: `m_file_created.resize(num_files, false)` is
  extend-only like upstream (no longer resets prior state), remote
  drives gate `allocate_files` (`DRIVE_REMOTE`, Windows leg of
  `pread_storage::initialize`), `open_file` defensively resizes the
  bitset before indexing, and symlink-flagged files report their
  declared size via the stat-cache pretend instead of a real stat.
- **Path helpers**: `canonicalize_path` is a pure lexical cleanup (no
  symlink resolution / no FS access — upstream UNC-build semantics)
  and `complete()` strips a leading `./` like upstream `path.cpp`.
- **Part-file error index**: `move_partfile`, the `set_file_priority`
  metadata flush and `has_any_file` report `file_index` `-6`
  (upstream `error_file_partfile` sites).
- **`hash2` completion** no longer marks the piece `hash_returned`/
  `FORCE_FLUSH` — upstream `disk_cache::hash2` is a pure lookup.

Regression tests: `hash_v2_blocks_stop_at_file_boundary`,
`stop_torrent_releases_pooled_files`,
`stop_torrent_aborts_queued_volatile_hash`,
`delete_files_error_ordering`,
`move_storage_rolls_back_after_partfile_error`,
`move_storage_rollback_preserves_skipped_files`,
`move_storage_rollback_preserves_missing_source_files`.

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

### Fixed — `web seeds` upstream-fidelity audit (A06)

`web_peer_connection`/`web_connection_base`/`web_seed_t` semantics
re-audited against upstream `e7381c7bc`; 19 deviations corrected:

- **Per-seed `web_seed_t` runtime state** (`retry`, `disabled`,
  `removed`, `interesting`, `no_local_ips`, `busy`,
  `supports_keepalive`, per-seed picker key + outstanding set)
  replaces the single global `WEB_SEED_PEER` pseudo-peer — an
  error/timeout/release now only refunds the failing seed's blocks.
- **`maybe_connect_web_seeds` gate ladder**: `aborted`,
  `finished`, `files_checked`, paused/upload-only,
  `max_connections` and session `connections_limit` (in-flight
  fetches count as connections upstream-style),
  `max_web_seed_connections` with conn slots held by in-flight
  batches, per-seed `disabled`/`removed`/`retry`/`interesting`/
  `busy` skips — a failing seed is no longer hammered every tick.
- **`retry_web_seed` real backoff**: `urlseed_wait_retry`,
  `web_seed_name_lookup_retry` for resolver failures, `Retry-After`
  honored verbatim, `Disable`/`Remove` terminal classes; the old
  stub (which just reset the round-robin cursor) is gone.
- **`urlseed_timeout` enforced** both engine-side
  (`tokio::time::timeout` per dial/GET) and torrent-side
  (`check_web_seed_timeouts` sweep releases stuck `downloading`
  blocks, backs the seed off, posts `url_seed_alert`; the deadline
  re-arms per delivered block like upstream's per-op timer).
- **Response validation**: 206 requires a well-formed matching
  `Content-Range` (`invalid_range` → `disable()`); non-206
  requires `Content-Length` (`no_content_length` → `disable()`);
  well-formed-but-wrong ranges are retryable `RangeMismatch`.
- **URL normalization**: `normalize_web_seed_url` is now called at
  connect time — single-file seeds ending in `/` get the (escaped)
  filename appended; multi-file seeds get the trailing `/`.
- **SSRF mitigation + `no_local_ips`** applied to base URLs and
  every redirect hop; `ip_filter`, `port_filter` and `allow_idna`
  gates wired (`Disable`/`Remove` classes per upstream).
- **Proxy + HTTPS**: `web_seed` proxy honored end-to-end (HTTP
  absolute-form GETs with `Proxy-Authorization`, CONNECT for https,
  SOCKS4/5 incl. `SocksTarget::Name` so `proxy_hostnames` never
  resolves locally); `https://` seeds dial TLS via `ssl_connect`.
- **Keep-alive bookkeeping**: `Connection: close` clears
  `supports_keepalive` (widens the next preferred request size) and
  drops the picker refcount, matching upstream conn teardown.
- **Counters**: with `report_web_seed_downloads` on, web-seed
  payload feeds `total_download`, `down_rate`, `RecvPayloadBytes`
  and resume totals; stale (released) blocks count as
  `redundant_bytes` instead of double-writing.
- **Cleanup**: `remove_url_seed` releases the seed's in-flight
  blocks and tombstones a busy seed until its task reports;
  pure-pad request groups are answered with zeroes without
  dialing; `web_peer_connection` conn lifecycle drives per-seed
  `inc`/`dec_refcount_all`.

### Notes

- `cache_expiry` periodic flush (`flush_expired`) kept as a documented
  extension — upstream marks the setting `DEPRECATED_SET` (no consumer);
  ours drains dirty runs older than the configured age once per session
  tick.

### Tests

- `add_file_size_and_name_errors` : FileTooLarge / TorrentInvalidLength
  cumulatif / FilenameTooLong (`test_file_storage.cpp` parity).
- `magnet_uri_torrent_file` : `.torrent`-magnet accepté, `ti == None`,
  info_hashes remplis ; dict sans `info`/`magnet-uri` →
  `TorrentMissingInfo`.
- `load_torrent_buffer_size_cap` : `max_buffer_size` → `MetadataTooLarge`.
