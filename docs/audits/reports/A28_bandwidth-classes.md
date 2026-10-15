# Audit A28 — rapport d'analyse

Périmètre : `bandwidth_manager`/`bandwidth_channel`/`bw_request`,
`peer_class`/`peer_class_set`/`peer_class_type_filter`, et les call-sites
session/conn (`request_bandwidth`, `use_quota_overhead`,
`set_peer_classes`, mixed-mode, `*_limit_too_low`, preemptive unchoke).
Baseline upstream `e7381c7bc` (`D:/Projet/libtorrent/libtorrent`).

## Verdicts checklist

- Channels : upload/download/protocol + classes (global/tcp/local/i2p) — assignation conn→class à l'attach — verdict : **conforme** — preuve : `session.rs:1066-1079` seede `global`(0)/`tcp`(1)/`local`(2) avec `ignore_unchoke_slots=true` + `connection_limit_factor=150` sur local (upstream `session_impl.cpp:652-660`), type-filter `Tcp/SslTcp/I2p → tcp(1)` (`session_impl.cpp:668-670`). `set_peer_classes` assigne le set à la création de conn (`session.rs:1167-1184`, appelé `peer_connected` ligne 3764 — même point que le ctor `peer_connection` upstream). Canaux = `channel[2]` par classe (`peer_class.rs:57` vs `peer_class.hpp`); "protocol/ip" sont des canaux *statistiques* chez upstream aussi, pas des canaux de quota — l'overhead est débité via `use_quota_overhead`. i2p : pas de classe dédiée chez upstream non plus (i2p → classe tcp via le type filter).
- request_bandwidth : quota granted, temp queuing, expiry — verdict : **conforme** — preuve : `session.rs:6374-6419` replique `peer_connection::request_bandwidth` (`peer_connection.cpp:5727-5804`) : early-out `bw_limit` → `bytes = max(wanted_transfer, bytes)` → `quota >= bytes` → `bytes -= quota` → priority `get_priority` (max sur set conn + set torrent, `session.rs:6347-6364` = `peer_connection.cpp:253-281`) → `copy_pertinent_channels` (canaux throttlés seulement, `session.rs:6322-6342` = `session_impl.cpp:1408-1427`, cap 10) → `mgr.request_bandwidth` → `bw_limit` flag ou `quota += ret`. Manager : file, `need_queueing` check-then-debit (`bandwidth.rs:110-121` = `bandwidth_limit.hpp:54-59`), `ttl=20` et dispatch `ttl<=0 && assigned>0` (`bandwidth.rs:351-358` = `bandwidth_manager.cpp:179-192`), drop disconnecting + `return_quota` (`bandwidth.rs:312-329` = `bandwidth_manager.cpp:133-158`), dispatch LIFO (`bandwidth.rs:366-368`). `update_quotas` tourne sur le tick timer de l'engine (`engine.rs:806-815`, même cadence `tick_interval` que `on_tick` — les branches événementielles du `select!` font `continue` sans passer par `tick()`, `engine.rs:3862-3901`, donc `ttl` reste ~20×500 ms comme upstream).
- Rate channels update : set_rate_limit per class propagé, `*_limit_too_low` perf alerts — verdict : **conforme** — preuve : `set_rate_limit` (`session.rs:6227-6242` = `session_impl.cpp:3395-3408`, `<=0 → 0` sinon cap `INT_MAX-1`); `update_{upload,download,local_upload,local_download}_rate` câblés par changement de setting (`session.rs:6121-6141` — upstream tire les callbacks `update_*` de `apply_pack` sur valeur changée, `settings_pack.cpp:308`); `set_peer_class` passe par `set_info` avec clamp `{<0→0, 0<l<10→10}` (`peer_class.rs:40-48` = `peer_class.cpp:16-30`). Alertes : per-conn drain conn-set + torrent-set + `performance_alert` (`torrent.rs:9045-9076` = `peer_connection.cpp:4866-4888`), torrent-level (`torrent.rs:9379-9393` = `torrent.cpp:10841-10861`), global session-level (`session.rs:7073-7100` = `session_impl.cpp:3767-3787`).
- type filter apply : `set_peer_class_filter`/`set_peer_class_type_filter` — classe résolue par ip/type — verdict : **conforme** — preuve : `PeerClassTypeFilter` (`peer_class.rs:307-363`) = `peer_class_type_filter.hpp` (mask/add 32 bits, `>31` rejeté, `apply` = `(m & mask) | add`); `socket_type_to_filter` (`session.rs:55-66`) reproduit la table `mapping` de `set_peer_classes` (`session_impl.cpp:1340-1351` : 0-2→tcp, 3→utp, 4→i2p, 5→rtc, 6-8→ssl_tcp, 9→ssl_utp — enum `socket_type.hpp:21-36`); `peer_class_mask` applique ip-filter puis type-filter (`session.rs:1131-1138` = `session_impl.cpp:1336-1354`), bits sans classe existante sautés (`session.rs:1176-1182` = `session_impl.cpp:1356-1366`); `set_peer_class_filter` remplace le filtre (`session.rs:5579-5600`).
- Mixed mode : uTP vs TCP split (`mixed_mode_algorithm` proportional/rate) — verdict : **conforme** — preuve : `update_mixed_mode` (`session.rs:6480-6538` = `session_impl.cpp:3660-3709`) : `prefer_tcp` dé-throttle la classe tcp ; `peer_proportional` compte conns `bw_network` par proto (`is_utp` upstream couvre utp+ssl_utp, `socket_type.cpp:67-75` ↔ Rust `Utp|SslUtp`), legs `download_queue > 1` / `upload_queue` non-vide, `num_peers[1]==0 || total<5 → 0`, sinon `rate * tcp*4 / total` plancher `{5000, 30000}` ; `stat_rate` = somme `t.up_rate/down_rate` = canaux wire (payload+protocol+ip, `torrent.rs:10055-10065`) ≈ `m_stat.upload_rate()` (`stat.hpp:166-176`). Relancé à chaque tick (`session.rs:6807` = `session_impl.cpp` top-of-`on_tick`) et sur changement de setting (`session.rs:6139-6141`).
- Overhead accounting : debit ip_overhead canaux séparés (`rate_limit_ip_overhead`) — verdict : **conforme (sauf web seeds → Déviations)** — preuve : `ip_overhead_bytes` (`peer_conn.rs:2299-2306` = `stat.hpp:128-141`, header `(ipv6?40:20)+20`, `max(1, ceil(bytes/packet))*header` sur les deux directions); accumulateurs `bw.ip_overhead[2]` incrémentés au read (`engine.rs:4294-4299`) et au write (`engine.rs:4503-4508`); drain per-second inconditionnel dans `stat_*`/counters, debit des classes conn+torrent sous `rate_limit_ip_overhead` (`torrent.rs:9023-9059` = `peer_connection.cpp:4866-4876` + `session_impl.cpp:1429-1452`); syn/synack chargés sur conns TCP sortantes (`session.rs:3767-3774`).
- Preemptive unchoke on quota, maybe_unchoke_this_peer — verdict : **conforme** — preuve : `maybe_unchoke_this_peer` (`torrent.rs:11628-11654` = `peer_connection.cpp:1851-1875`) : exempt (`ignore_unchoke_slots` conn ou torrent) → unchoke immédiat bypass slots ; sinon `preemptive_unchoke` = `fixed_slots && (num_peers_up_unchoked < num_unchoke_slots || unchoke_slots_limit < 0)` (`session_impl.cpp:6730-6738`); call-sites post-handshake (`session.rs:4496-4509`, gate `have>0` comme `bt_peer_connection`) et `incoming_interested` (`session.rs:8840`).

## Déviations

- `crates/bitdaemon/src/engine.rs:1572-1730` (spawn_web_seed / `WebSeedFetch` handling) — le fetch web-seed pull-model **bypass complet du bandwidth manager** : upstream `web_peer_connection` est un `peer_connection` dont les lectures passent par `setup_receive` → `request_bandwidth(download_channel)` (`peer_connection.cpp:5986,6193`), donc ses bytes consomment le quota des classes (global/local/tcp selon IP + classe du torrent). Chez nous, `fetch_block` lit sans jamais appeler `Session::request_bandwidth`, n'a pas de `PeerQuota`, et n'accumule pas `bw.ip_overhead` → `download_rate_limit`, `set_download_limit` torrent et `mixed_mode` ne s'appliquent **pas** au trafic web-seed ; leur overhead n'est pas débité des canaux. Sévérité : état/comportement (limite de débit non honorée sur url-seeds ; pas wire-visible).
  - **sketch** : introduire un `BandwidthSocket` non-peer pour les fetches web-seed —
    1. `peer_conn.rs` : généraliser `PeerQuota` ou ajouter `WebSeedQuota { quota: AtomicI64, channel_state, disconnecting, grant: Notify }` implémentant `BandwidthSocket` (upload channel inutilisé).
    2. `session.rs` : variante `request_bandwidth_web(&mut self, ih, bytes, handle) -> bool` — même corps que `request_bandwidth` mais channels = `copy_pertinent_channels(&t.peer_classes, DOWNLOAD, …)` + les classes conn qu'aurait eue la conn (upstream assigne le set par ip/type au connect : pour fidélité, stocker sur le web-seed le `PeerClassSet` calculé via `peer_class_mask(addr.ip(), FilterSocketType::Tcp)` au `spawn_web_seed`, en passant le set dans `SessionOut::WebSeedFetch`), priority via `request_priority(&ws_set, &t.peer_classes, DOWNLOAD)`.
    3. `engine.rs` (`spawn_web_seed`/`fetch_block` loop) : avant chaque read HTTP, boucle identique au download path (`engine.rs:4222-4261`) : `request_bandwidth_web` → attendre `grant` si `bw_limit` → debit `quota` des bytes lus → `ip_overhead` accumulé et drainé (relié à un point de drain — soit via une entrée `peers`-like, soit drain direct dans `on_web_seed_block`).
    Réf. upstream : `peer_connection.cpp:5727-5804` (request), `web_peer_connection` hérite ce chemin (`web_peer_connection.cpp`, pas de `request_bandwidth` propre — il utilise celui du parent).
    Test de régression : torrent avec `download_rate_limit` global petit + web-seed actif → assert que le débit fetch plafonne et que `upload/download_limit_too_low` reste cohérent ; test unitaire `Session::request_bandwidth_web` queue/expiry comme `test_bandwidth.rs`.

## Fichiers du fix

- `crates/bitdaemon/src/engine.rs` (web-seed quota gating)
- `crates/bitdaemon-session/src/session.rs` (`request_bandwidth_web` + set classes web-seed)
- `crates/bitdaemon-session/src/peer_conn.rs` (handle `BandwidthSocket` non-peer) — ou nouveau petit module
- `crates/bitdaemon-session/src/torrent.rs` (overhead drain web-seed si handle stocké hors `peers`)
- tests : `crates/bitdaemon-session/tests/test_bandwidth.rs`

## Non-bugs tranchés

- `num_connections` (`session.rs:1189-1195`) = peers attachés + `num_peers_half_open` ; upstream compte `m_connections.size()` qui inclut les conns acceptées non encore rattachées à un torrent. Chez nous l'accept→attach est atomique (`on_incoming_connection` verdict → `peer_connected`), une conn sans torrent n'existe pas ; les dials sont couverts par le gauge half-open. Écart de comptage résiduel ≈ conns en handshake entrant pré-attach — transitoire, pas de divergence de limite observable.
- `PeerQuota::assign_bandwidth` clamp de channel (`peer_conn.rs:112-120`) vs `TORRENT_ASSERT` upstream — assert-only, comportement identique (manager n'émet que 0/1).
- `BandwidthManager::close()` dispatche `assigned` (possiblement 0) aux requêtes vivantes — identique à `bandwidth_manager.cpp:27-41`; l'assert `amount>0 || is_disconnecting` de `peer_connection::assign_bandwidth` est debug-only upstream.
- `update_quotas` early-return `queue.is_empty()` → les canaux non sollicités ne re-remplissent pas leur bucket : identique à `bandwidth_manager.cpp:119-120`.
- Mixed-mode `stat_rate` exclut les conns sans torrent et les bytes *protocol* des web-seeds — ordre de grandeur identique, seul le payload web-seed est déjà replié (`torrent.rs:10039-10046`).
- `PeerClassPool::new()` seede seulement global+tcp, `local` ajouté par `Session::new` — même résultat que le ctor `session_impl` (ids 0/1/2 stables, `debug_assert_eq!`).

## Résultat

Mode `apply` exécuté dans le worktree dédié
(`C:/Projet/wt/LibtorrentRust-a28`, branche
`audit/28-bandwidth-classes`). Commit apply : `c8a2d65` (rebasée sur
`main` `14f2df0`, mergée en `0664520`).

### Déviation corrigée

**Fetch web-seed hors du bandwidth manager** — upstream
`web_peer_connection` est un `peer_connection` : ses lectures
passent par `setup_receive` → `request_bandwidth(download_channel)`
et consomment le quota des classes comme tout pair. Implémenté :

- `ThrottledWs` (`engine.rs`) — wrapper `AsyncRead`/`AsyncWrite`
  autour du `BoxedPeerStream` injecté : chaque opération est clampée
  au quota disponible, un bucket vide file `request_bandwidth_web`
  et parque la tâche sur le grant (`bw_limit` → `Notify`), chaque
  chunk transféré accumule l'overhead IP/TCP sur les deux directions
  (`trancieve_ip_packet`, `(ipv6?40:20)+20` par paquet).
- `Session::request_bandwidth_web` (`session.rs`) — même corps que
  `request_bandwidth` : classes conn-side résolues par
  `peer_class_mask(addr, FilterSocketType::Tcp)` (nom retenu par le
  proxy → fallback `global|tcp`), canaux pertinents mask + set
  torrent (cap 10), priorité = `get_priority` max sur les deux.
- `web_seed_bw` (`torrent.rs`) — registre `(Weak<PeerQuota>, mask)`
  par torrent : le `second_tick` draine `ip_overhead` via
  `quota_overhead_mask` + `quota_overhead` (torrent set) et poste
  `download_limit_too_low`/`upload_limit_too_low` sous
  `rate_limit_ip_overhead` ; les entrées `disconnecting`/mortes sont
  purgées (conn effacée de `m_connections` upstream).
- `WsQuotaGuard` (`engine.rs`) — `Drop` marque `disconnecting` sur
  toute sortie du task fetch → `update_quotas` supprime la requête
  en file et rend le quota assigné (`peer_connection::disconnect`).
- `http_conn`/`fetch_block` généralisés sur `S: AsyncRead +
  AsyncWrite + Unpin` pour interposer le wrapper (appelants
  inchangés).

### Tests ajoutés

`test_bandwidth.rs` (+3, 8/8 verts) :

- `web_seed_request_queues_grants_and_registers` — file sur canal
  throttlé, `bw_limit`, grant à `update_quotas`, registration
  `web_seed_bw` dedup par `Weak::ptr_eq`.
- `web_seed_overhead_drains_and_alerts` — drain `use_quota_overhead`
  (mask conn + set torrent) + `download_limit_too_low` sous
  `rate_limit_ip_overhead`.
- `web_seed_disconnect_drops_queued_request` — `disconnecting`
  supprime la requête en file, refuse `None`, purge du registre.

### Gates

`cargo fmt --all`, `cargo clippy --workspace --all-targets --
-D warnings`, `cargo test --workspace`, `cargo doc --workspace
--no-deps` — tous verts (re-run post-rebase : un seul conflit
`PORTING.md` ligne proxy A24, résolu en gardant les deux lignes).

### Note résiduelle

`request_bandwidth_web` n'applique pas `wanted_transfer` (pas de
file de requêtes par conn côté pull-model — upstream la calcule sur
`m_requests`) : la demande est la taille réelle du read HTTP, le
comportement de quota est identique à granularité de requête près.
