# Audit A29 — rapport d'analyse

Périmètre : chaque `Counter` ↔ sites upstream
`inc_stats_counter`/`set_value`/`blend_stats_counter` ; snapshot
`session_stats` ; `sliding_average` ; gauges transitionnelles ;
histogrammes socket ; compteurs disque/DHT/fenced ; métriques exposées
(`session_stats_metrics`, `find_metric_idx`).
Baseline upstream `e7381c7bc` (`D:/Projet/libtorrent/libtorrent`).

Bank Rust : `crates/bitdaemon-session/src/counters.rs` —
`NUM_STATS_COUNTERS = 235`, `NUM_COUNTERS = 318`, `#[repr(usize)]`
index-identique à `performance_counters.hpp`. `COUNTER_NAMES`
vérifié exhaustif contre l'enum upstream (ordre + noms) ;
`METRIC_NAMES` vérifié contre la table `session_stats.cpp` (ordre de
présentation catégorié — vérifié par diff visuel intégral de la table,
et par le test `metric_order_matches_upstream`).

## Verdicts checklist

- Exhaustif : pour CHAQUE counter upstream, le site Rust correspondant
  (ou N/A documenté) — verdict : **déviation** — preuve : sweep complet
  ci-dessous ; ~230/318 slots câblés conformément, ~86 slots morts dont
  4 seulement parce que morts upstream (`piece_picker_sequential_loops`,
  `on_disk_queue_counter`, `num_blocks_hashed` — ces deux derniers
  étant d'ailleurs *alimentés* côté Rust), 20 `disk_read_latency*`
  flag-gated upstream ; le reste (DHT ×33, histogrammes socket ×36,
  disk-times ×4, job-pool ×6, socket-type gauges ×9, torrent-state
  gauges ×6, `on_*` ×8, legs io ×5, compteurs de tentative ×3,
  `recv_failed_bytes`, `num_read_back`, `num_outstanding_accept`)
  est vivant upstream et mort chez nous. [Compteurs]
- Jamais incrémentés : pattern trouvé >2× — suspecter tous les
  compteurs — verdict : **déviation** — preuve : census complet des
  variantes `Counter::*`/`C::*` utilisées dans le workspace (grep
  `Counter::[A-Za-z0-9_]+` + alias `C::`) croisé avec la bank de 318 :
  les familles mortes sont listées dans « Déviations D1–D12 ». Un
  cas plus grave que « jamais incrémenté » : des legs *émis* mais
  **filtrés par un mismatch de string** (D1/D2/D3 — wire-visible).
- Gauges transitionnels (`num_peers_*`/`num_utp_*`/fenced) : inc/dec
  aux bons edges, pas de drift — verdict : **partiel / déviation** —
  preuve : `num_peers_half_open` géré côté engine sur ~26 edges de
  dial (`engine.rs:3617-4637`), `num_peers_connected` recomputé après
  chaque attach/drop (`session.rs:4736-4739` — équivalent à l'inc/dec
  ctor/dtor upstream, plus robuste aux batch-drops), `num_utp_*` via
  `NumUtpIdle + state` arithmétique verbatim (`utp_socket.rs` ctor /
  `set_state` / `Drop` — inc/dec aux mêmes edges que
  `utp_stream.cpp`), fenced ×16 via `set_idx` offset brut
  (`session.rs` snapshot, incluant la quirk `RemoveTorrent` documentée).
  **MAIS** : `num_tcp_peers..num_ssl_utp_peers` jamais alimentés (D6),
  `num_outstanding_accept` jamais alimenté (D9), `num_peers_half_open`
  pour une conn `connecting` tuée par le leg `timed out: connect`
  repose sur le timeout du dial engine (converge mais pas au même
  edge — voir D3).
- sliding_average vs add-blend : `request_time`, `request_latency`,
  rates — verdict : **conforme** — preuve : `request_time` est un
  `sliding_average` per-conn (pas un counter) chez upstream
  (`peer_connection.cpp:2948,3032,4572-4593`) — porté dans
  `pipeline.request_time` (`peer_conn.rs:1061` `add_request_time_sample`,
  consommé par `request_timeout()` `torrent.rs:9942` et `rtt`
  `torrent.rs:3135`); `request_latency` blendée 5 % en µs sur
  read-réussi (`session.rs:1729` = `peer_connection.cpp` `m_counters
  .blend_stats_counter(request_latency, rtt_us, 5)` sur
  `async_read` completion); `Counters::blend` = `(v·(100−r)+x·r)/100`
  (`counters.rs:1087` = `performance_counters.cpp` `blend_stats_counter`);
  `sliding_average.rs` porte `sliding_average.hpp` verbatim (échelle
  fixe 64, moyenne/déviation entières, saturation) avec tests
  `test_sliding_average.rs` reflétant les 9 tests upstream.
- session_stats_metrics : noms/indices exposés aux clients — verdict :
  **partiel** — preuve : `metrics()` (`counters.rs:1521`) produit les
  318 lignes dans l'ordre `session_stats.cpp`, index résolus via
  `COUNTER_NAMES` ; tests `metrics_cover_every_counter_once` +
  `metric_order_matches_upstream` (`counters.rs:1540-1574`) =
  `test_session.cpp:229-241` (count + sort by `value_index` +
  bijectivité). **Manque** : l'API publique `find_metric_idx`
  (`session_stats.hpp`, testée `test_session.cpp:243-249`) — D13 ;
  `StatsMetric::is_gauge` bool ≙ `metric_type_t` ✓ ; snapshot
  `Session::session_stats()` ne re-évalue que downloading/seeding/
  peers/limiter/disk gauges — les autres gauges gardent leurs valeurs
  inc/decrementées (voir D7).
- waste_reason totals, disk_job counters, peer_class channels —
  verdict : **partiel / déviation** — preuve : `add_redundant_bytes`
  (`torrent.rs:3551-3567`) inc `recv_redundant_bytes` +
  `waste_piece_timed_out+cancelled+unknown+seed+end_game+closing`
  verbatim (`torrent.cpp:12810-12828`); `queued_write_bytes`
  inc/dec sur write-issue/completion (`session.rs` DiskWrite path,
  couvrant aussi `add_piece` — conforme `write_continue` upstream);
  fenced via offset brut ✓. **MAIS** `recv_failed_bytes` jamais inc
  (`torrent.rs:7653` met à jour `self.failed_bytes` sans counter — D10),
  `num_read_back`/job-pool/disk-time morts (D11), disk_read_latency
  inerte des deux côtés (N/A). peer_class channels : A28 — les canaux
  `limiter_*` gauges sont câblés (`session_stats()` copie
  `up_queue/down_queue/up_bytes/down_bytes`).

## Table exhaustive — 318 slots

Légende : `✓` câblé (site cité) · `~` partiel (un ou plusieurs legs
manquants → renvoi déviation) · `✗` vivant upstream, mort chez nous
(renvoi déviation) · `∅up` mort upstream aussi (N/A conforme, exporte
0 comme upstream) · `↑x` alimenté chez nous alors que mort upstream
(divergence d'export — à trancher).

### peer.* — disconnect & erreurs (indices 0-14)

| compteur | statut | preuve |
|---|---|---|
| error_peers | ✓ | `session.rs:4893` (`sev == PeerError` → `peer_error_alert`, mutuellement exclusif — `peer_connection.cpp:4353`) |
| disconnected_peers | ✓ | `session.rs:4838` (conn existait), `:5135` (dial fail) |
| eof_peers | ✓ | `session.rs:4845/5141` `UnexpectedEof` |
| connreset_peers | ✓ | `session.rs:4844/5140` `ConnectionReset` |
| connrefused_peers | ✓ | `session.rs:4846/5142` |
| connaborted_peers | ✓ | `session.rs:4847/5143` |
| notconnected_peers | ✓ | `session.rs:4848/5144` |
| perm_peers | ~ | `PermissionDenied` couvre EPERM **et** EACCES — upstream splitte `no_permission`→perm / `access_denied`→no_access (D4) |
| buffer_peers | ✗ | leg `error::no_buffer_space` absente (`peer_connection.cpp:4367`) — D4 |
| unreachable_peers | ✗ | leg `host_unreachable` absente (`:4369`) — D4 |
| broken_pipe_peers | ✓ | `session.rs:4850/5146` |
| addrinuse_peers | ✓ | `session.rs:4851/5147` |
| no_access_peers | ✗ | `access_denied` non mappé (`:4375`) — D4 |
| invalid_arg_peers | ✗ | `invalid_argument` non mappé (`:4377`) — D4 |
| aborted_peers | ✗ | `operation_aborted` non mappé (`:4379`) — D4 |

### requêtes pièce (15-20)

piece_requests ✓ (`peer_conn.rs` request path ×3) ·
max_piece_requests ✓ ×2 · invalid_piece_requests ✓ ×8 ·
choked_piece_requests ✓ ×2 · cancelled_piece_requests ✓ ×3 ·
piece_rejects ✓ ×6 (incl. `torrent.rs:7570` predictive-reject — leg
`write_reject_request` upstream compte même quand BEP-6 supprime le
message).

### erreurs directionnelles/crypto (21-26)

error_incoming_peers ✓ · error_outgoing_peers ✓ · error_rc4_peers ✓ ·
error_encrypted_peers ✓ · error_tcp_peers ✓ · error_utp_peers ✓ —
tous sous `sev > Normal`, legs `type()==bittorrent && op!=connect`
pour la crypto (`session.rs:4864-4888, 5159-5178` =
`peer_connection.cpp:4406-4420`).

### piece picks (27-34)

reject_piece_picks ✓ ×3 · **unchoke_piece_picks** ~ (leg unchoke-event
câblé `peer_conn.rs:1235` ; leg `on_files_checked` `torrent.cpp:9247`
manquant — `drive_peer` retourne bool ignoré `torrent.rs:6552` — D5) ·
incoming_redundant_piece_picks ✓ · incoming_piece_picks ✓ ·
end_game_piece_picks ✓ · snubbed_piece_picks ✓ ·
interesting_piece_picks ✓ · hash_fail_piece_picks ✓.

### picker loops (35-42)

partial ✓ · suggest ✓ · **sequential** ↑x (upstream mort — rien
n'incrémente `piece_picker_sequential_loops`, seulement déclaré
`performance_counters.hpp:88` + exporté `session_stats.cpp:288` ; Rust
le compte via `PickerCounter`→`counters.rs:1112`) — divergence
d'export, D14 · reverse_rare ✓ · rare ✓ · rand_start ✓ · rand ✓ ·
busy ✓. Rust a en plus `ReverseSequential/ReverseSuggest/
ReverseRandStart/ReverseRand` repliés sur les slots existants
(`counters.rs:1120-1126`) — granularité supplémentaire sans slot
dédié, documentée.

### legs de disconnect nommés (43-48)

| compteur | statut | preuve |
|---|---|---|
| connect_timeouts | ~ | `FailOp::Connect` ✓ (`session.rs:5132` = `connect_failed` `peer_connection.cpp:4212`) ; `timed_out_no_handshake` clé existe `counters.rs:62` mais le leg émis est `"timed out: no handshake"` → jamais compté — D3 |
| uninteresting_peers | ~ | `self_connection`/`torrent_paused`/`torrent_aborted`/`upload-upload`✓ ; `"upload-upload connection"` et `"uninteresting upload-only peer"` ne matchent pas → legs morts ; `"session_closing"` clé en trop — D1/D2/D12 |
| timeout_peers | ~ | clés `timed_out_inactivity/no_request/no_interest` ✓ mais strings émises `"timed out: *"` → jamais compté — D3 |
| no_memory_peers | ✓ | `"out of memory"` `session.rs:9569` |
| too_many_peers | ✓ | `"too_many_connections"` `session.rs:4206…` |
| transport_timeout_peers | ~ | `io::TimedOut` ✓ (`session.rs:4852/5148`) ; `"timed out: connect"` (=`errors::timed_out`) non matché — D3 |

### tentatives de connexion (49-56)

num_banned_peers ✓ ×2 · banned_for_hash_failure ✓ ×2 ·
connection_attempts ✓ (dial émis) · **connection_attempt_loops** ✗
(`session_impl.cpp` pompe — pas de `loop_counter` plombé — D12) ·
**boost_connection_attempts** ✗ (`inc_boost_connections` n'inc pas le
counter — D12) · **missed_connection_attempts** ✗ (candidat choisi
mais non connectable → replié dans `no_peer_connection_attempts` —
D12) · no_peer_connection_attempts ✓ · incoming_connections ✓
(accept → `session.rs`).

### dispatch on_* (57-65)

on_read ✗ · on_write ✗ · on_tick ✗ · on_lsd ✗ · on_lsd_peer ✗ ·
on_udp ✗ · on_accept ✗ — D8. on_disk_queue ∅up (déclaré/exporté,
jamais inc — `session_stats.cpp:155`). on_disk ✗ — D8.

### num_incoming_* / num_outgoing_* (66-104)

✓ tous — funnel unique : incoming `incoming_counter_idx`
(`counters.rs` — quirks verbatim : ids 0-9 → `num_incoming_choke+id`,
13-17 → `num_incoming_suggest+id` *débordant sur les slots outgoing*,
autres → `num_incoming_extended`, keepalive non compté —
`bt_peer_connection.cpp:2213-2221`) invoqué au dispatch
(`torrent.rs:5768`) **et** sur le leg `BadLength`-disconnect
(`session.rs:4797-4802` — une frame complète qui échoue la validation
compte quand même, comme `packet_finished()` upstream) ; outgoing
`count_outgoing` (`session.rs:9467-9514`) appelé au funnel
`send_to_peer` (`engine.rs:1289`) — ids FAST mappés proprement
`num_outgoing_suggest+(id-13)` (pas la quirk incoming), handshake et
sous-types extended dédiés (`NumIncomingPex/Metadata/ExtHandshake`),
`num_outgoing_hash_request/hashes/hash_reject` dédiés v2. Note :
`count_outgoing` compte avant `tx.send`, écrivain mort inclus — même
moment qu'upstream qui compte à l'append du send_buffer.

### ops pièce/disque/fichier (105-121)

num_piece_passed ✓ · num_piece_failed ✓ · num_have_pieces ✓ ·
num_total_pieces_added ✓ · num_blocks_written ✓ · num_blocks_read ✓ ·
**num_blocks_hashed** ↑x (mort upstream, alimenté chez nous via
`queue_stats` `session.rs:11294` — D14) · num_write_ops ✓ ·
num_read_ops ✓ · **num_read_back** ✗ (relecture de blocs écrits non
flushés — `disk_io_thread` upstream — D11) · file_pool_hits ✓ ·
file_pool_misses ✓ · file_pool_thread_stall ✓ · file_pool_race ✓ —
tous via `DiskQueueStats`→snapshot `session_stats()`.

### temps disque (122-125)

disk_read_time ✗ · disk_write_time ✗ · disk_hash_time ✗ ·
disk_job_time ✗ — pas de timing job dans `DiskQueueStats` — D11.

### waste (126-131)

waste_piece_timed_out/cancelled/unknown/seed/end_game/closing ✓ —
`add_redundant_bytes` `torrent.rs:3551-3567` appelé aux sites
`torrent.rs:2049, 4973, 4987, 5160, 5176` =
`torrent.cpp:12810-12828`.

### octets net (132-139)

sent_payload_bytes ✓ · sent_bytes ✓ · sent_ip_overhead_bytes ✓ ·
sent_tracker_bytes ✓ (`engine.rs:1895/2142` announce+scrape) ·
recv_payload_bytes ✓ · recv_bytes ✓ · recv_ip_overhead_bytes ✓ ·
recv_tracker_bytes ✓ · **recv_failed_bytes** ✗ (D10) ·
recv_redundant_bytes ✓. Rates/overhead : `stat_*` channels +
`test_bandwidth.rs` protège les montants.

### DHT (140-165)

**26 compteurs + 7 gauges, tous morts** — `dht_messages_in`,
`dht_messages_in_dropped`, `dht_messages_out`, `dht_messages_out_dropped`,
`dht_bytes_in`, `dht_bytes_out`, `dht_{ping,find_node,get_peers,
announce_peer,get,put,sample_infohashes}_{in,out}` (×18),
`dht_invalid_{announce,get_peers,find_node,put,get,sample_infohashes}`
(×6), gauges `dht_nodes`, `dht_node_cache`, `dht_torrents`,
`dht_peers`, `dht_immutable_data`, `dht_mutable_data`,
`dht_allocated_observers` — `bitdaemon-dht` n'a aucun sink vers
`Counters` (le `DhtTracker` produit l'état pour l'alerte `DhtStats`
sans compter) — D15.

### uTP (166-177)

✓ les 12 — `UtpCounterSink` traduit `UtpStat` en `Counter` :
packet_loss, timeout, packets_in/out, fast_retransmit, packet_resend,
samples_above/below_target, payload_pkts_in/out, invalid_pkts_in,
redundant_pkts_in (`utp_socket.rs` — même granularity que
`utp_stream.cpp`/`utp_socket_manager.cpp`).

### histogrammes socket (178-213)

socket_send_size3-20 (×18) ✗ · socket_recv_size3-20 (×18) ✗ —
upstream `session_impl::received_buffer`/`sent_buffer`
(`session_impl.cpp:3911-3921`) : `idx = min(log2p1(size>>3), 17)` —
jamais appelé chez nous — D13.

### disk_read_latency1-20 (214-233)

∅up — `TORRENT_DISK_LATENCY_STATS` off par défaut upstream → tous à 0
des deux côtés. Conforme.

## Gauges (indices ≥ 235)

### état torrent (0-8)

num_checking_torrents ✗ · num_stopped_torrents ✗ ·
num_upload_only_torrents ✗ · **num_downloading_torrents** ✓ ·
**num_seeding_torrents** ✓ (recompute `session_stats()`
`session.rs:~11260`) · num_queued_seeding_torrents ✗ ·
num_queued_download_torrents ✗ · num_error_torrents ✗ ·
**non_filter_torrents** ✓ ×6 (inc/dec ip-filter). Upstream maintient
ces gauges par transitions d'état — D7 (fix : recompute au snapshot,
même pattern que downloading/seeding).

### socket-type peers (9-17)

num_tcp_peers · num_socks5_peers · num_http_proxy_peers ·
num_utp_peers · num_i2p_peers · num_ssl_peers · num_ssl_socks5_peers ·
num_ssl_http_proxy_peers · num_ssl_utp_peers — **tous ✗** (D6).
Upstream : ctor/dtor `num_tcp_peers + socket_type_idx(m_socket)`
(`peer_connection.cpp:137,832`). Note quirk : `socket_type_t` a `rtc=5`
inséré (`socket_type.hpp:21-36`) sans renommer les gauges →
`rtc`→`num_ssl_peers`, `tcp_ssl`→`num_ssl_socks5_peers`, `utp_ssl`(9)
→ `num_peers_half_open` (!). `conn.sock_type` Rust utilise le même
numérotage (`session.rs:55-66`) → l'arithmétique verbatim reproduit
la quirk ; à documenter dans le fix.

### conn/peers (18-30)

num_peers_half_open ✓ (engine, ~26 edges — inc dial-issue
`engine.rs:3617/4037/4377/4589`, dec sur chaque issue de dial
`3633-3779/4394/4596/4637` = `peer_connection.cpp:4218` +
`session_impl` connect-fail legs) · num_peers_connected ✓ (recompute
`session.rs:4736`) · num_peers_up_interested ✓ ×3 ·
num_peers_down_interested ✓ ×5 · num_peers_up_unchoked_all ✓ ×4 ·
num_peers_up_unchoked_optimistic ✓ ×5 · num_peers_up_unchoked ✓ ×4 ·
num_peers_down_unchoked ✓ ×3 · num_peers_up_requests ✓ (transition
`upload_queue` vide→non-vide `torrent.rs:5049`, dec `5130/7584/
12475/4836`) · num_peers_down_requests ✓ ×5 · num_peers_up_disk ✓ ×5 ·
num_peers_down_disk ✓ ×7 (gates écriture/read-back) ·
num_peers_end_game ✓ ×2.

### divers session (31-49)

request_latency ✓ (blend 5 % µs `session.rs:1729`) ·
disk_blocks_in_use ✓ · cached_blocks ✓ · num_unhashed ✓ ·
queued_disk_jobs ✓ · **num_running_disk_jobs** ✗ · **num_read_jobs** ✗ ·
**num_write_jobs** ✗ · **num_jobs** ✗ · **num_writing_threads** ✗ ·
**num_running_threads** ✗ (job-pool/thread gauges — D11) ·
blocked_disk_jobs ✓ · file_pool_size ✓ · queued_write_bytes ✓
(inc/dec edges + snapshot) · num_unchoke_slots ✓ ×7 (choker) ·
num_fenced_read…num_fenced_tick_storage (×16) ✓ `set_idx` brut
— quirk `RemoveTorrent` documentée · has_incoming_connections ~
(set-1 on accept ; pas de recompute `any_of(listen_sockets)` — D16) ·
limiter_up_queue ✓ · limiter_down_queue ✓ · limiter_up_bytes ✓ ·
limiter_down_bytes ✓ (snapshot `session_stats()`) ·
num_utp_idle…num_utp_deleted (×6) ✓ `NumUtpIdle + state` ·
**num_outstanding_accept** ✗ (D9) · num_queued_tracker_announces ✓ ×3.

### Bilan

318 slots : ~230 conformes · 4 `∅up`/`↑x` divergences par excès ·
20 latences N/A flag-gated · ~64 slots morts à câbler, répartis en
D4-D15. La divergence la plus sérieuse n'est pas un slot mort mais le
**mismatch des strings de `DisconnectPeer.reason`** (D1-D3) : les legs
sont émis, filtrés par le matcher, et le `close_reason` uTP part à
`None` — **wire-visible**.

## Couverture des tests upstream (test/test_*.cpp)

- `test_sliding_average.cpp` — 9 tests (`reaction_time`×2, `converge`×2,
  `random_converge`, `sliding_average`, `positive_overflow`,
  `negative_overflow`, `overflow`) — **couvert** :
  `crates/bitdaemon-common/tests/test_sliding_average.rs` reflète
  réaction/convergence/déviation/overflow (port direct, échelle 64 +
  `SlidingAverage64`).
- `test_stat_cache.cpp` (`stat_cache`) — **partiel** :
  `bitdaemon-disk/src/stat_cache.rs` a des tests inline
  (dirty/clear/get_filesize, lignes 180+) ; les scénarios upstream de
  taille cachée après écriture partielle et d'erreur persistée
  (`stat_cache` conserve l'erreur tant que dirty) ne sont pas
  explicitement rejoués — risque faible (module simple, appelé par
  `storage.rs:637-690`).
- `test_session.cpp::session_stats` (lignes 229-250) — **partiel** :
  le tri `value_index` + bijectivité = `metrics_cover_every_counter_once`
  (`counters.rs:1541`) ✓ ; les 3 lookups `find_metric_idx` n'ont pas
  d'équivalent (API absente — D13).
- Alertes stats — **couvert** : `post_session_stats` →
  `SessionStatsHeader` puis `SessionStats`
  (`test_dht_results.rs:181-192`) ; post inconditionnel sous
  `alert_mask = 0` (`test_tracker_list.rs:154-157` — `emplace_alert`
  upstream bypasse le mask pour les stats).
- **Gap** : aucun test Rust n'exerce les legs de disconnect
  → counters (un test « disconnect par raison X inc le counter Y »
  aurait attrapé D1-D3 — le matcher est testé implicitement nulle
  part).

## Déviations

- `crates/bitdaemon-session/src/torrent.rs:6487,6489,6503` — raisons
  `"upload-upload connection"` / `"uninteresting upload-only peer"`
  émises mais `disconnect_reason_counter` (`counters.rs:45-65`)
  attend `"upload-upload"`/`"uninteresting_upload_peer"`, et
  `reason_to_close_reason` (`close_reason.rs:161`) les rejette vers
  `CloseReason::None`. Upstream `peer_connection.cpp:4381-4386`
  (`upload_upload_connection`/`uninteresting_upload_peer` →
  `uninteresting_peers`) et `close_reason` `UploadToUpload`/
  `NotInterestedUploadOnly` stampé sur la FIN uTP. **Sévérité :
  wire-visible** (champ `close_reason` uTP divergent) + état-interne
  (counters). Lentilles [Compteurs][Erreur]. NB : `torrent.rs:11942`
  émet `"upload-upload"` (matché) — les deux emitters ne sont pas
  cohérents entre eux.
  - **sketch** : au choix — (a) émettre les noms lt-error upstream
    `"upload_upload_connection"`/`"uninteresting_upload_peer"` aux 3
    sites (les deux matchers les connaissent déjà) ; (b) ajouter les
    variantes display-string aux deux matchers. Préférence (a) —
    aligne aussi `peer_disconnected` alert/log sur les noms
    `errors::*` upstream.
  - **test de régression** : torrent upload-only + pair
    `remote_upload_only` → `disconnect_if_redundant` → assert
    `UninterestingPeers` inc + `CloseReason::UploadToUpload` ;
    pair seed sans intérêt mutuel → `NotInterestedUploadOnly`.
- `crates/bitdaemon-session/src/torrent.rs:9799-9834` — les 5 legs de
  timeout (`"timed out: connect|inactivity|no handshake|no request|
  no interest"`) ne matchent ni `disconnect_reason_counter` ni
  `reason_to_close_reason`. Upstream `peer_connection.cpp:4388-4404`
  (`timed_out`→transport_timeout_peers, 3×`timed_out_*`→timeout_peers,
  `timed_out_no_handshake`→connect_timeouts) + close_reasons
  `Timeout`/`TimedOutActivity`/`TimedOutHandshake`/`TimedOutRequest`/
  `TimedOutInterest`. **Sévérité : wire-visible** (close_reason
  → `None`) + état-interne. Lentilles [Compteurs][Erreur].
  - **sketch** : émettre `"timed_out"`, `"timed_out_inactivity"`,
    `"timed_out_no_handshake"`, `"timed_out_no_request"`,
    `"timed_out_no_interest"` (noms `errors::*` — les matchers les
    ont déjà ; `"timed out: connect"`→`"timed_out"` couvre aussi le
    leg `connect_failed` `peer_connection.cpp:4212` si le dial est
    encore en vol — vérifier que le path dial-fail le compte via
    `FailOp::Connect`, sinon inc explicite `ConnectTimeouts` +
    `TransportTimeoutPeers`).
  - **test de régression** : `check_request_timeouts` sur conn âgée
    → `TimeoutPeers`/`ConnectTimeouts`/`TransportTimeoutPeers` incs
    selon le leg + close_reason exact.
- `crates/bitdaemon-session/src/session.rs:4843-4854,5137-5153` —
  5 legs transport io absentes : `no_buffer_space`→buffer_peers,
  `host_unreachable`→unreachable_peers, `access_denied`→no_access_peers,
  `invalid_argument`→invalid_arg_peers, `operation_aborted`→aborted_peers
  (`peer_connection.cpp:4367-4380`). `PermissionDenied`→PermPeers
  absorbe en plus EACCES qui devrait aller à no_access_peers.
  **Sévérité : état-interne.** Lentille [Compteurs].
  - **sketch** : `e.raw_os_error()` — `105/ENOBUFS`→BufferPeers,
    `113/EHOSTUNREACH`|`101/ENETUNREACH` (+ `ErrorKind::HostUnreachable`/
    `NetworkUnreachable`)→UnreachablePeers, `13/EACCES`→NoAccessPeers
    vs `1/EPERM`→PermPeers, `22/EINVAL`→InvalidArgPeers,
    `125/ECANCELED`/`995`→AbortedPeers — match `raw_os_error` avant
    `kind()` dans les deux blocs.
  - **test de régression** : table io-error→counter unitaire sur
    `disconnect_alert_counters`/`peer_disconnected`.
- `crates/bitdaemon-session/src/counters.rs:53` — clé
  `"session_closing"`→UninterestingPeers : upstream
  `session_is_closing` n'a **pas** de leg (pas dans la liste
  `peer_connection.cpp:4381-4386`) et rien ne l'émet → dead key +
  sur-comptage si jamais émise. **Sévérité : cosmétique.**
  - **sketch** : retirer la clé (ou la garder sans mapping → `None`).
- `crates/bitdaemon-session/src/torrent.rs:7653` — `add_failed_bytes`
  met à jour `self.failed_bytes` sans inc `recv_failed_bytes`
  (`torrent.cpp:12830-12835` : `m_ses.stats_counters()
  .inc_stats_counter(recv_failed_bytes, bytes)`). `counters` est déjà
  en scope (`piece_failed` `torrent.rs:7548`). **Sévérité :
  état-interne**, fix 1-ligne.
  - **sketch** : après `self.failed_bytes += …` →
    `counters.add(Counter::RecvFailedBytes, added)` (même montant :
    `psize` ou `blocks.len()*16384` — reproduire exactement
    l'arithmétique `piece_size`/`block_size` upstream).
  - **test** : hash-fail d'une pièce → `recv_failed_bytes` ==
    `total_failed_bytes` du torrent.
- `crates/bitdaemon-session/src/torrent.rs:6552` — leg
  `on_files_checked` : `drive_peer` retourne `bool` (picks faits)
  mais le résultat n'inc pas `unchoke_piece_picks`
  (`torrent.cpp:9247` inc à chaque `request_a_block` réussi).
  **Sévérité : état-interne.**
  - **sketch** : `if self.drive_peer(pid, s, counters, out) {
    counters.inc(Counter::UnchokePiecePicks) }` au site
    files-checked.
- `crates/bitdaemon-session/src/session.rs` (peer pump
  ~`try_connect_more_peers`) — `boost_connection_attempts`,
  `missed_connection_attempts`, `connection_attempt_loops` jamais inc
  (`session_impl.cpp:6006-6018` : loop++ par itération, missed++ quand
  `try_connect_peer` échoue sur candidat choisi, boost++ par
  `m_boost_connections`). **Sévérité : état-interne.**
  - **sketch** : porter `connection_attempt_loops`++ en tête de
    boucle de pump ; `missed++` quand le meilleur candidat retourne
    sans dial ; `boost++` par dial issu de `inc_boost_connections`.
- `crates/bitdaemon/src/engine.rs` + `session.rs` — gauges
  `num_tcp_peers..num_ssl_utp_peers` jamais peuplés (ctor/dtor
  `peer_connection.cpp:137,832` : `num_tcp_peers + socket_type_idx`).
  **Sévérité : état-interne** (mais visible via `session_stats`).
  - **sketch** : au `peer_connected` (attach) `add_idx(NumTcpPeers +
    sock_type, +1)`, au `peer_disconnected` `add_idx(…, -1)` —
    **arithmétique verbatim** y compris la quirk rtc-shift
    (`socket_type_t::rtc=5` → les gauges ssl décalés d'un cran,
    `utp_ssl=9` retombant sur `num_peers_half_open` — à commenter,
    décision fidélité : reproduire tel quel ou cartographier sainement
    et documenter en « tranché »).
- `crates/bitdaemon-session/src/session.rs` (`session_stats()`,
  ~11260) — gauges `num_checking_torrents`, `num_stopped_torrents`,
  `num_upload_only_torrents`, `num_queued_seeding/download_torrents`,
  `num_error_torrents` jamais peuplés (upstream les tient via
  transitions d'état). **Sévérité : état-interne.**
  - **sketch** : étendre le recompute existant (downloading/seeding)
    — classifier chaque torrent au snapshot (checking =
    `files_checked==false`, upload_only = `is_upload_only()`, stopped
    = paused+pas de conn, queued = `is_queued`, error = `aborted`).
- `engine.rs` — counters `on_read/on_write/on_tick/on_lsd/
  on_lsd_peer/on_udp/on_accept/on_disk` + gauge
  `num_outstanding_accept` jamais peuplés (`session_impl.cpp:2854/
  2877/3508/4036/5860` etc.). **Sévérité : état-interne**, diagnostics.
  - **sketch** : incs ponctuels — accept handler, boucle tick,
    batch disk-completions, inbound UDP, LSD datagram, read/write
    pumps ; `num_outstanding_accept` inc à l'émission de l'accept
    async, dec dans le handler.
- `engine.rs` read/write pumps — histogrammes `socket_send_size3-20`/
  `socket_recv_size3-20` jamais peuplés
  (`session_impl.cpp:3911-3921` : `min(log2p1(size>>3),17)`).
  **Sévérité : état-interne.**
  - **sketch** : aux sites qui comptent déjà `recv_bytes`/`sent_bytes`,
    `inc_idx(SocketRecvSize3+idx)` / `SocketSendSize3+idx` avec la
    même formule.
- `crates/bitdaemon-disk` + snapshot — `disk_read_time`,
  `disk_write_time`, `disk_hash_time`, `disk_job_time`, `num_read_back`,
  `num_read_jobs`, `num_write_jobs`, `num_jobs`, `num_running_disk_jobs`,
  `num_writing_threads`, `num_running_threads` jamais peuplés.
  **Sévérité : état-interne.** Plus gros morceau : nécessite timing
  par job dans le backend disque exposé via `DiskQueueStats`.
  - **sketch** : étendre `DiskQueueStats` (job_time cumulé par type,
    job-pool counters, running jobs/threads) → copie dans
    `session_stats()` ; `num_read_back` inc quand un read sert un
    bloc depuis le store-buffer (read-back path du backend).
- `crates/bitdaemon-dht` ↔ session — **33 slots DHT morts** (26
  compteurs + 7 gauges). Le `DhtTracker` calcule déjà les stats pour
  l'alerte `DhtStats` — les gauges sont récupérables par snapshot,
  les compteurs demandent un sink (même pattern que `UtpCounterSink`,
  injecté — `bitdaemon-dht` ne dépend pas de `-session`).
  **Sévérité : état-interne** (visibilité `session_stats` / OnionBit).
  - **sketch** : (a) `trait DhtCounterSink` dans `bitdaemon-dht`
    (ou common) implémenté côté session, inc aux krpc dispatch
    (messages in/out/dropped, bytes, RPC in/out par méthode,
    `invalid_*` sur requête malformée) ; (b) gauges via snapshot
    `dht_state()` dans `session_stats()` — nodes/cache/torrents/
    peers/immutable/mutable/observers.
- `session.rs` — `has_incoming_connections` set-1 à l'accept mais
  jamais recomputé (`any_of(m_listen_sockets, has_incoming)` sur
  rebind upstream). **Sévérité : cosmétique** — tous les listeners
  Rust acceptent, le gauge ne peut diverger qu'après un rebind sans
  socket entrante. Tranché possible.
- `crates/bitdaemon-session/src/counters.rs` — API publique
  `find_metric_idx(name) -> Option<usize>` absente
  (`session_stats.hpp`, testée `test_session.cpp:243-249`).
  **Sévérité : cosmétique** (surface API).
  - **sketch** : `pub fn find_metric_idx(name) -> Option<usize> {
    METRIC_NAMES.iter().position(|n| *n == name) }` + test parité
    (les 3 lookups upstream).
- `piece_picker_sequential_loops` (Rust compte, upstream mort) et
  `num_blocks_hashed` (Rust alimente, upstream mort) — exports
  divergents par excès. **Sévérité : cosmétique** — trancher :
  garder (observabilité) vs mettre à 0 pour parité stricte
  d'export. Recommandation : garder + documenter.
- `counters.rs:1526` — `unwrap_or(0)` sur nom non résolu : défendu
  par `metrics_cover_every_counter_once` (assert round-trip) mais un
  `expect` serait plus sûr. **Sévérité : cosmétique.**

## Invariants de frontière (Seam Checks)

- **Session ↔ Disk** : `DiskQueueStats` copié en un bloc dans
  `session_stats()` (queue_jobs, blocked, blocks_in_use, cached,
  unhashed, queued_write_bytes, ops cumulés, file-pool, fenced par
  offset brut). `queued_write_bytes` inc/dec aux edges DiskWrite —
  couvre `add_piece` (partagé) = `write_continue` upstream.
  `picker_gen` gate les complétions obsolètes — aucun inc de counter
  sur résultat stade (vérifié au read/write completion paths).
- **Session ↔ Net (peer wire)** : incoming compté au dispatch
  (quirk verbatim) + leg BadLength au disconnect ; outgoing au funnel
  `send_to_peer` — les deux seuls points de comptage, pas de double
  funnel. Keepalive jamais compté (pas d'id — comme upstream).
- **Session ↔ Net (uTP)** : `UtpCounterSink` — la session ne voit
  que `UtpStat` énumérés ; gauges d'état inc/dec à la transition et
  dec au `Drop` — aucune fuite de gauge si la socket est dropée en
  vol (vérifié ctor/set_state/Drop).
- **Session ↔ DHT** : **aucun** — la frontière n'existe pas pour les
  counters (gap D15 ; l'alerte `DhtStats` passe par un autre canal).
- **Session ↔ Engine** : `num_peers_half_open` vit côté engine,
  `num_peers_connected` côté session — répartition différente
  d'upstream (qui met tout dans `m_stats`), valeurs équivalentes.
- **Alertes** : `SessionStatsHeader`+`SessionStats` postés
  inconditionnellement (= `emplace_alert` upstream, mask-bypass
  vérifié par test).
- **Snapshot atomique** : `SessionStats::get` bounds-safe
  (`get().unwrap_or(0)`) ≙ `session_stats_alert::values[]`.

## Fichiers du fix

- `crates/bitdaemon-session/src/counters.rs` — matcher raisons (a
  ou b), `session_closing`, `find_metric_idx`, `expect` sur metrics()
- `crates/bitdaemon-session/src/session.rs` — legs io (×2 blocs),
  recompute gauges torrents, on_* incs, snapshot DHT
- `crates/bitdaemon-session/src/torrent.rs` — strings raisons (×8),
  `recv_failed_bytes`, `unchoke_piece_picks` files_checked
- `crates/bitdaemon-net/src/close_reason.rs` — alias strings (si
  option b retenue)
- `crates/bitdaemon/src/engine.rs` — histogrammes socket,
  on_accept/on_udp/on_read/on_write, `num_outstanding_accept`,
  gauges socket-type (ou session.rs à l'attach/detach)
- `crates/bitdaemon-dht/src/dht_tracker.rs` (+ glue session/engine) —
  sink counters + snapshot gauges
- `crates/bitdaemon-disk/src/disk_interface.rs` (+ backend) —
  `DiskQueueStats` étendu (job times, pool, running, read_back)
- tests : `crates/bitdaemon-session/tests/` (disconnect-legs→counters,
  close_reasons, snapshot gauges), `counters.rs` tests
  (`find_metric_idx`)

## Non-bugs tranchés

- `num_peers_connected` recomputé (sum `t.peers.len()`) au lieu
  d'inc/dec ctor/dtor — valeur identique, robuste aux batch-drops ;
  upstream inc/dec peut dériver, le recompute ne peut pas.
- `count_outgoing` compte à l'entrée du writer-pump (avant `tx.send`)
  = upstream comptant à l'append du send_buffer ; une conn morte sans
  writer compte quand même — écart marginal et sans direction biaisée.
- `disk_read_latency1-20` inerte — `TORRENT_DISK_LATENCY_STATS` est
  compilé off chez upstream aussi ; exporter 0 est la parité.
- `on_disk_queue_counter` inerte — mort upstream (déclaré + exporté,
  jamais inc).
- `session_closing` absent du shutdown path — le teardown de session
  drop les writers sans passer `peer_disconnected` ; upstream
  `~session_impl` fait pareil (les counters meurent avec la session,
  `disconnected_peers` non bumpé en masse).
- `PeerDisconnected` alert porte la raison en string vs `error_code`
  upstream — surface d'alerte, hors périmètre counters.
- `metrics()` `unwrap_or(0)` — inatteignable tant que le test
  `metrics_cover_every_counter_once` passe (round-trip asserté) ;
  suggestion cosmétique `expect`.
- Les compteurs sont tous `AtomicI64` dans une seule bank plate —
  fidèle à `aux::counters` (même index space, mêmes opérations
  relaxed) ; pas de risque de race interne.
