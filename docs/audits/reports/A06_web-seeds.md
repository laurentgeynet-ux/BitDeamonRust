# Audit A06 — rapport d'analyse

Périmètre : web seeds / HTTP peers — `web_peer_connection` +
`web_connection_base` + `web_seed_entry` + legs `torrent.cpp`
(`maybe_connect_web_seeds`, `connect_to_url_seed`, `retry_web_seed`,
`remove_web_seed_iter`, `on_name_lookup`, `ban_peer`) +
`torrent_info`/`load_torrent`/`resume_data` (BEP-17 `httpseeds`,
`url-list`). Baseline upstream `e7381c7bc`
(`D:/Projet/libtorrent/libtorrent`, lecture seule).

**Constat d'architecture v2.x** : `src/http_seed_connection.cpp`
**n'existe plus** à la baseline (BEP-17 retiré en v2) — tous les seeds,
y compris les entrées `atp.http_seeds`, passent par
`web_peer_connection` en sémantique BEP-19. `web_seed_entry::type_t`
est `TORRENT_DEPRECATED` et inerte (`web_seed_entry.hpp:32,82`). Côté
Rust il n'existe qu'un seul chemin `web_seed` — la suppression est
donc conforme, pas une lacune.

**Modèle Rust** (pull, par batch — ne pas confondre avec le modèle
conn-persistante upstream) : `Session::tick` →
`Torrent::next_web_seed_blocks` (pick ≤ `web_seed_blocks_per_tick`
blocs sous une clé picker virtuelle par seed) →
`SessionOut::WebSeedFetch` → `engine::spawn_web_seed` (task tokio :
dial→GETs→réponses→`WebSeedDone`) → `drain_webseeds` →
`on_web_seed_block`/`on_web_seed_error`/`on_web_seed_done`. État
persistant par seed : `Torrent::web_seed_state: BTreeMap<url, WebSeed>`
(`torrent.rs:295-362`).

## Verdicts checklist

- Range requests : pièces complètes vs blocs, `block_size` alignment,
  coalescing contigu — verdict : **conforme** — preuve : pick par
  `PieceBlock` `block_size()` = `min(piece_length, 0x4000)`
  (`torrent.rs:7313-7314` = upstream `t->block_size()`), merge des
  runs contigus cross-piece plafonné `urlseed_max_request_bytes`
  (`torrent.rs:6962-6975, 6936` = `request_merger` /
  `web_peer_connection::write_request`), expansion range→blocs
  `web_seed_blocks` (`torrent.rs:7006-7032`), groupes un-GET-par-fichier
  `plan_piece_request` (`web_seed.rs:201-280` =
  `m_file_requests` upstream). Test protecteur :
  `web_seed.rs` mod tests (`:563-701` — range multi-fichier/pad),
  `test_web_seed_natpmp.rs:54` (`web_seed_consume_body_splits_files`),
  `test_session.rs:1208-1250` (cap merge). [Limites]
- Erreurs : HTTP non-200, redirect loops, content-length mismatch, EOF
  mid-piece → `url_seed_alert` + seed marqué — verdict : **partiel →
  déviation** — preuve : statut non-ok → `handle_error` →
  `retry_web_seed` + `have_files.clear_bit(file)` upstream
  (`web_peer_connection.cpp:905-920`) — Rust `Http → Retry` mais sans
  `have_files` (D5) ; `Retry-After` → `Header(d)` ✓
  (`web_seed.rs:481`, `torrent.rs:12640`) ; 206 sans `Content-Range`
  ou non-206 sans `Content-Length` → `InvalidRange → Disable` ✓
  (`web_seed.rs:489-501`, `engine.rs:2747-2755` =
  `get_range` ec → `disable`, `web_peer_connection.cpp:948-952`) ;
  range mismatch → `RangeMismatch → Retry` ✓ (`:506-513` =
  `disconnect(invalid_range, peer_error)`) ; body court/EOF →
  `ShortBody → Retry` ✓ (`:514-520`) ; alerte `UrlSeed` émise à
  `session.rs:10743-10747`, filtrée par catégorie dans
  `alerts.post` (`alerts.rs:1787-1793` = `should_post` upstream) —
  **mais** payload du message divergent (D12) et boucles de
  redirection non persistées (D4). Tests :
  `test_web_seed_natpmp.rs:118,154,202,241` (Content-Range absent/
  mismatché, no-Content-Length, Retry-After) ;
  `test_session.rs:730-880` (retry/disable par seed). [Erreur]
- Retry : backoff par seed, `retry_web_seed`, bannissement temporaire
  — verdict : **conforme** — preuve : `WebSeedBackoff`
  `Retry|NameLookup|Header(d)|Disable|Remove` (`torrent.rs:348-362`),
  `retry_web_seed` = `now + retry.value_or(urlseed_wait_retry)` avec
  skip `removed||disabled` (`torrent.rs:12633-12650` =
  `torrent.cpp:11757-11770`) ; `NameLookup` utilise
  `web_seed_name_lookup_retry` ✓ ; `check_web_seed_timeouts` arme
  `now + urlseed_wait_retry` (`torrent.rs:7239-7285` =
  deadline `web_connection_base::timeout()`). Tests :
  `test_session.rs:730-832` (backoff + alternance de seeds),
  `:833-880` (disable terminal), `:885-936` (sweep timeout).
  [Cycle de vie]
- Attribution : erreurs/compteurs par URL — verdict : **conforme** —
  preuve : clé picker par seed (`next_web_seed_key` décroissant,
  `torrent.rs:6776-6777`), `outstanding`/`downloaded`/`last_error`
  par URL dans `WebSeedStatus` (`status.rs:63-73`,
  `torrent.rs:7088-7095`), release scopée à la clé du seed en
  `on_web_seed_error` (`torrent.rs:7179-7203`), `web_seed_statuses`
  snapshot par `web_seeds` (`torrent.rs:7291-7304`). Tests :
  `test_session.rs:1009-1072` (comptage octets par seed). [Compteurs]
- Request pacing : `web_seed_blocks_per_tick`, connection reuse vs
  reconnect — verdict : **déviation** — preuve :
  `web_seed_blocks_per_tick` **n'existe pas upstream** (absent de
  `settings_pack.cpp` — grep exhaustif) ; Rust le défaut à 4
  (`settings.rs:986`) et n'émet qu'**un** batch par tick et par
  torrent (`session.rs:7725, 7891-7907` → `next_web_seed_blocks`
  retourne un seul `Option<(url, reqs)>`) ; upstream appelle
  `connect_to_url_seed` pour **chaque** seed éligible du tick
  (`torrent.cpp:11013-11022`) puis stream continu
  (`send_block_requests`/`request_a_block`). Chaque batch Rust est une
  connexion TCP/TLS neuve (conn map task-locale
  `engine.rs:2434-2435`). → D1, D2. [Limites, Cycle de vie]
- Pseudo-peer `web_seed_t` : download queue partagée, legs upload
  N/A — verdict : **partiel** — preuve : `inc_refcount_all`/
  `dec_refcount_all` modélisant `peer_connection::init`/`~` ✓
  (`torrent.rs:6841-6850, 7151-7162`), `web_seed_outstanding`
  torrent-wide anti-double-request ✓ (`torrent.rs:443, 6947`),
  upload effectivement absent (les fetchs ne servent jamais —
  conforme à un conn download-only upstream). **Manques** :
  `have_files` per-file, `endpoints`, `resolving` (busy ≈),
  `restart_request`/`restart_data`, `redirects` persistants,
  `ephemeral`/`no_local_ips`/`interesting` champs déclarés mais
  **jamais alimentés** (`torrent.rs:321-322`, init `6784-6785`). →
  D4, D5, D6, D10. [Cycle de vie]

### Verdicts étendus (périmètre demandé)

- BEP-17 retiré / `atp.http_seeds` → chemin BEP-19 effectif —
  verdict : **conforme** — `Torrent::from_params` fusionne
  `atp.url_seeds` + `atp.http_seeds` dans `t.web_seeds`
  (`torrent.rs:1372-1387` = `torrent_info.cpp:1635-1647` poussant les
  deux listes dans `internal_web_seeds` → `m_web_seeds`, tous servis
  par `web_peer_connection`). [Cycle de vie]
- `add_http_seed`/`remove_http_seed`/`http_seeds()` no-op dépréciés —
  verdict : **conforme** — `torrent_info.cpp:1698-1714` (get → vide,
  add → no-op) ; test `test_extensions.rs:268-280` vérifie la
  sémantique no-op du handle Rust. [Compteurs]
- `url-list` (metainfo + resume) — verdict : **conforme** —
  `parse_torrent_file` pousse `url-list` (string ou liste, unique,
  `ensure_trailing_slash` multi-fichier) dans `atp.url_seeds`
  (`torrent_info.rs:1299-1326` = `load_torrent.cpp:286-310`) ;
  resume `url-list` → `ret.url_seeds` +
  `DEPRECATED_OVERRIDE_WEB_SEEDS` (`resume_data.rs:312-321` =
  `read_resume_data.cpp`) ; magnet `ws=` → `atp.url_seeds`
  (`magnet_uri.rs` = `magnet_uri.cpp:337`). Note frontière :
  `DEPRECATED_OVERRIDE_WEB_SEEDS` posé mais jamais consulté —
  voir Invariants de frontière. [Cycle de vie]
- `httpseeds` write-only en resume — verdict : **conforme** —
  `resume_data.rs:813-816` écrit la clé depuis `atp.http_seeds` ;
  ni upstream `read_resume_data.cpp` ni le lecteur Rust ne la
  relisent. [Limites]
- GET/headers wire : `Host`, `User-Agent` (premier GET ou
  `always_send_user_agent`, supprimé `anonymous_mode`), `Authorization`
  (extern > basic URL), `Proxy-Authorization` http_pw seul,
  `Proxy-Connection`, `Connection: keep-alive` (premier GET ou proxy)
  — verdict : **conforme** — `web_seed.rs:129-164` ↔
  `web_connection_base.cpp:110-148` ligne à ligne ; tests
  `web_seed.rs:563+` (`render_first_request_emits_keepalive`,
  proxy absolute-form, anonymous UA). Une réserve : GET
  absolute-form dans un tunnel CONNECT https (D8). [Limites]
- Chunked transfer — verdict : **conforme** — `recv_response`
  boucle jusqu'à `parser.finished()` et `body()` dé-frame via
  `parser.chunks()` (`http_conn.rs:130-140, 171-203`) ; upstream
  dé-frame incrémental `m_chunk_pos`
  (`web_peer_connection.cpp:983-1050`) — équivalent observable.
  [Limites]
- Keep-alive / reconnect — verdict : **partiel** — `Connection:
  close` retire la conn et baisse `supports_keepalive` → refcount
  drop + taille préférée 4 MiB au prochain pick
  (`engine.rs:2630-2634`, `torrent.rs:6852-6855, 7126-7147` =
  `m_received_disconnect` → reconnect + taille). **Mais** chaque
  batch = connexion neuve quelle que soit la réponse (pas de conn
  persistante cross-batch) → D2 ; et le tampon de réponse pipeliné
  est perdu → D1. Test : `test_web_seed_natpmp.rs:289`
  (`fetch_block_reports_connection_close`). [Cycle de vie]
- Redirects : résolution relative, `maybe_url_encode`, auth
  same-origin seulement, éphémère + `no_local_ips` — verdict :
  **partiel → déviation** — résolution/encodage/same-origin-auth
  ✓ (`engine.rs:2664-2731` = `handle_redirect`
  `web_peer_connection.cpp:648-700`) ; `no_local_ips` propagé
  depuis l'origine résolue globale ✓ (`engine.rs:2730`,
  `resolved_origin_global` `:5850-5858`) ; **mais** aucune graine
  éphémère n'entre dans `web_seeds`/`web_seed_state`, la map
  `redirects` meurt avec la tâche → le fichier d'origine est
  re-demandé à chaque batch → D4. [Cycle de vie]
- `maybe_connect_web_seeds` gates + limites de connexion — verdict :
  **partiel** — gates `aborted|empty|finished|!files_checked|
  num_peers>=max_connections|session>=connections_limit` ✓
  (`torrent.rs:6736-6758` = `torrent.cpp:10997-11007`) ;
  `--limit` par seed éligible ✓ (`:6803-6819`) ;
  `peer_info.connection||resolving` ≈ `busy` ✓ (`:6828`). **Écarts** :
  gate `upload_mode` supplémentaire, pas d'appel hors-tick
  (upstream en appelle aussi depuis le chemin unchoke
  `torrent.cpp:9255`), une seule connexion par tick (D2, D11).
  [Cycle de vie]
- `ban_web_seeds` — verdict : **partiel** — gate sur
  `peer_list.peers()[i].web_seed` ✓ (`torrent.rs:8651`,
  `4735-4738` = `torrent.cpp:11918-11929`) ; hash-fail certain →
  `st.disabled` + refcount drop + alerte `UrlSeed("peer banned")`
  (`torrent.rs:4669-4686` ≈ `received_invalid_data` → `ban_peer` →
  `peer_info.banned`) ; **mais** le seed banni reste listé par
  `url_seeds()`/`web_seed_statuses` alors qu'upstream filtre
  `peer_info.banned` (`torrent.cpp` `web_seeds()` getter) — le test
  upstream exige `th.url_seeds().empty()`
  (`web_seed_suite.cpp:184-190`) → D9. Test protecteur existant :
  `test_peer_lifecycle_a07.rs:531-564`. [Compteurs]
- `report_web_seed_downloads` — verdict : **conforme** —
  `ignore_stats(true)` ≙ gate sur `web_seed_payload_downloaded` +
  `RecvPayloadBytes` (`torrent.rs:7096-7101` ;
  upstream `peer_connection::received_bytes` early-return sur
  `m_ignore_stats`, `peer_connection.cpp:1129-1136`). [Compteurs]
- Intégration bandwidth manager (finding A28) — verdict :
  **conforme — non régressé** — `ThrottledWs` enrobe le stream,
  consomme `download_channel`/`upload_channel`, compte l'overhead IP
  (`engine.rs:475-661`, instancié `:2605-2612` ; `request_bandwidth_web`
  `session.rs:7025-7099` ; `WsQuotaGuard` marque disconnecting au
  teardown). Tests `test_bandwidth.rs:228-361` (grant queue,
  overhead, cleanup). **Le bypass A28 est bien corrigé — ne pas
  re-rapporter.** [Compteurs]
- Cleanup : timeout / remove / disconnect / complétion tardive —
  verdict : **conforme** — `remove_url_seed` : release blocks +
  refcount + tombstone `removed` GC au `done` (`torrent.rs:8576-8611`
  = `remove_web_seed_iter` deferred-delete `torrent.cpp:6752-6780`) ;
  timeout sweep release + retry + alerte (`torrent.rs:7239-7285`) ;
  blocs tardifs hors-`outstanding` → `redundant_bytes` sans écriture
  (`torrent.rs:7073-7087`). Tests : `test_session.rs:885-936`
  (timeout), `:1147-1203` (remove + GC tombstone), `:1073+`
  (bloc tardif = redondant). [Cycle de vie]
- SSRF / proxy / IDNA / port+protocole / HTTPS / auth — verdict :
  **partiel** — ordre des gates = upstream
  (`connect_to_url_seed` : idna → parse → protocole → hostname →
  port 0 → port-filter, `engine.rs:2259-2331` ↔
  `torrent.cpp:6813-6870`) ; `PeerBlocked` sur `ip_filter` et
  `ssrf` (IP locale + `?` dans le path, `engine.rs:2530-2565`) ✓ ;
  `proxy_hostnames` pour le nom du seed ✓ (`engine.rs:5718-5722` =
  `web_seed_hostname_via_proxy`) ; TLS SNI + ctx torrent/session ✓
  (`engine.rs:5800-5827`). **Écarts** : `no_local_ips` liste vide →
  `NameLookup` au lieu de `disabled` + `banned_by_ip_filter` (D6) ;
  proxy-resolve fail → `NameLookup` au lieu de `disabled` (D7) ;
  absolute-form dans CONNECT https (D8) ; `external_auth`/
  `extra_headers` de `web_seed_entry` jamais peuplés (D7b).
  [Limites, Erreur]
- EOF / corps court / débordement — verdict : **conforme** —
  `body.len() != f.length` → `ShortBody → Retry` (`web_seed.rs:514-520`)
  ≙ `disconnect(invalid_range, peer_error)` upstream ; débordement
  chunked couvert par le même leg (`web_peer_connection.cpp:994-1009`).
  Réserve : `max_response` 4 MiB codé en dur < `urlseed_max_request_bytes`
  16 MiB → D3. [Limites]
- Pad files — verdict : **conforme** — groupes sans GET réels →
  zéros sans socket (`engine.rs:2461-2469` = `handle_padfile`
  `web_peer_connection.cpp:1272-1324`) ; pads inter-fichiers
  contribuent des zéros inline (`engine.rs:2637-2646`). Test
  `web_seed.rs` mod (`multi_fs` pad 96 B entre deux fichiers).
  [Limites]
- Comptage upload/proto bytes — verdict : **partiel** — payload
  reçu compté ✓ (`RecvPayloadBytes` gate `report_web_seed_downloads`),
  overhead IP quota'd ✓ via `ThrottledWs` ; **mais** les octets de
  requête émis n'alimentent ni `sent_protocol_bytes` ni le stat
  envoyé (upstream `send_buffer(request)` → `sent_bytes(0, bytes)`
  → `m_statistics` + `trancieve_ip_packet` non gaté par
  `ignore_stats`) → D13. [Compteurs]

## Couverture des tests upstream (test/test_*.cpp)

`web_seed_suite.cpp::test_transfer` exécute une **matrice** :
protocole http/https × proxy {none, socks4, socks5, socks5_pw, http,
http_pw} × {keep-alive, no_keepalive} × {chunked} × {test_ban},
sur torrents multi-fichiers avec pad files ; assertions :
`is_seeding`, `total_payload_download − redundant == total_size − pad`,
existence des fichiers non-pad, `peer_disconnects == 0` sous
keep-alive, `th.url_seeds().empty()` après ban.

| test upstream | statut Rust | impact |
|---|---|---|
| `test_web_seed.cpp` (http/https, matrice proxy) | **manquant** — aucun fixture HTTP transport-level ; `fetch_block_over_duplex` (`test_web_seed_natpmp.rs:74`) couvre un happy-path local | la matrice complète wire (proxy×tls×keepalive) n'a aucun équivalent — D1/D8 non détectables |
| `test_url_seed.cpp` (keep-alive, no-keepalive, host_header, rename) | **partiel** — host header unit-testé (`web_seed.rs` tests), pas de fixture réelle ; keep-alive couvert par `fetch_block_reports_connection_close` | régression keep-alive/reconnect possible |
| `test_web_seed_chunked.cpp` | **manquant** — chunked couvert par `http_conn::body()` unit path, pas de fixture de transfert | risque faible (parser partagé) |
| `test_web_seed_redirect.cpp` | **manquant** — redirect unit-testé dans `engine`/`web_seed` grouping (`web_seed.rs` tests) mais pas le cycle éphémère persistant | D4 non détectable |
| `test_web_seed_ban.cpp` | **partiel** — gate `ban_web_seeds` testée (`test_peer_lifecycle_a07.rs:531-564`) ; pas de test « données corrompues → ban → `url_seeds()` vide » | D9 non détectable |
| `test_web_seed_http{,_pw}.cpp`, `test_web_seed_socks{4,5,5_pw,5_no_peers}.cpp` | **manquant** — `web_seed_dial` couvre socks4/5/http/http_pw code paths sans fixtures | chemins proxy non exercés de bout en bout |
| `web_seed_suite.cpp` — multi-fichiers + pads + compteurs payload | **partiel** — pads unit-testés (`web_seed.rs` mod tests, `engine.rs:2461-2469`), compteurs testés (`test_session.rs:1009+`), pas d'intégration complète | — |
| `test_webtorrent_transfer.cpp` (WebSocket seeds) | **hors périmètre** — `websocket` existe côté net (`test_websocket.rs`) mais aucun seed `ws(s)://` n'est routé | à vérifier en audit ultérieur |

Couverture Rust existante utile : `test_session.rs:602-1250`
(tick→block→disk-write, gates `files_checked`, retry, disable,
timeout sweep, cap connexions, comptage, bloc tardif, remove,
merge cap) ; `test_bandwidth.rs:228-361` ; `web_seed.rs` mod tests ;
`test_web_seed_natpmp.rs` (duplex fetch, erreurs de range,
`Retry-After`, `Connection: close`).

## Déviations

- `crates/bitdaemon-net/src/http_conn.rs:179-203` + `web_seed.rs:468`
  — `recv_response` accumule **tout** le flux lu dans `buf` (y
  compris le début de la réponse HTTP **suivante** sur une connexion
  keep-alive pipelinée) et le rend dans `raw`, que `fetch_block`
  abandonne — les octets de la 2ᵉ réponse sont perdus ; le
  `recv_response` suivant parse un flux désaligné ou attend un
  timeout `urlseed_timeout` → batch en échec répété. Upstream garde
  le reliquat dans `m_recv_buffer` entre deux réponses
  (`web_peer_connection.cpp` `on_receive` loop, `recv_buffer =
  recv_buffer.subspan(m_body_start)` `:925-928`). **Sévérité :
  critique** (live-lock sur tout batch multi-GET — c.-à-d. tout
  torrent multi-fichier dès que le serveur coalesce les réponses).
  [Limites, Cycle de vie]
  - **sketch** : donner à `recv_response` un tampon réentrant —
    signature `recv_response(s, max_response, read_timeout, carry:
    &mut Vec<u8>)` : amorcer `buf` avec `*carry`, et au retour
    copier dans `carry` les octets au-delà de `parser.body_end()` ;
    `fetch_block` garde un `carry` local entre ses `got_get` loops.
    Réf. `web_peer_connection.cpp:925-941`.
  - **test de régression** : duplex stream livrant **deux**
    réponses `HTTP/1.1 206` consécutives dans un seul `read` ;
    `fetch_block` sur un plan 2-GET doit rendre les deux segments.
    Oracle : `web_seed_suite` multi-fichier keep-alive.
- `crates/bitdaemon-session/src/session.rs:7725,7891-7907` +
  `settings.rs:986` + `torrent.rs:6729-6746` — pacing inventé :
  `web_seed_blocks_per_tick` (défaut 4) n'existe pas upstream ;
  `next_web_seed_blocks` ne rend qu'**un** (seed, ≤4 blocs) par
  tick → ~64 KiB/s/torrent toutes seeds confondues et **un**
  handshake TCP(+TLS) par batch. Upstream connecte chaque seed
  éligible du tick (`torrent.cpp:11013-11022`), maintient des conns
  persistantes et émet des requêtes en continu
  (`send_block_requests` → `request_a_block` → `write_request`),
  avec `desired_queue_size` ~ dizaines de blocs. Conséquence : les
  mécanismes `prefer_contiguous_blocks` (`torrent.rs:6879-6904`),
  `supports_keepalive` et `urlseed_max_request_bytes` sont
  opérationnels mais jamais atteignables. **Sévérité :
  wire-visible**. [Cycle de vie]
  - **sketch** : (a) boucler le dispatch tick :
    `while let Some((u,r)) = t.next_web_seed_blocks(cap_per_seed, …)`
    jusqu'à épuisement des seeds éligibles ou du
    `max_web_seed_connections` budget ; (b) remplacer
    `max_blocks = web_seed_blocks_per_tick` par un
    `desired_queue_size` par seed ≙ `request_queue_time`-dérivé
    (upstream `desired_queue_size()`), ou supprimer le setting de la
    boucle et le garder uniquement comme plafond par batch ;
    (c) à terme, persister `conns` par seed entre batchs
    (conn-map portée par `WebSeed` plutôt que par tâche) pour
    récupérer le keep-alive réel.
  - **test de régression** : session avec 2 seeds et ~200 blocs à
    télécharger → après N ticks, les deux seeds ont un fetch en vol
    et le débit agrégé dépasse `blocks_per_tick×block_size` — oracle
    `test_web_seed.cpp` `SEEDING` sous la matrice.
- `crates/bitdaemon/src/engine.rs:2614` — `fetch_block(…, 4*1024*1024)`
  borne la réponse à 4 MiB alors que le merge peut demander jusqu'à
  `urlseed_max_request_bytes` (défaut 16 MiB, `settings.rs:946`) →
  `TooLarge` → `Retry` à l'infini sur les grosses requêtes. Latent
  tant que D2 bride les batchs à 64 KiB ; devient actif dès le fix
  du pacing. **Sévérité : wire-visible**. [Limites]
  - **sketch** : passer `s.urlseed_max_request_bytes as usize +
    64*1024` (slack headers/chunk framing) dans le task via
    `SessionOut::WebSeedFetch` ou via le snapshot settings de
    `spawn_web_seed` — réf. `settings_pack.cpp:357`.
  - **test** : plan merge > 4 MiB contre un stream qui sert la
    réponse complète → succès attendu.
- `crates/bitdaemon/src/engine.rs:2423-2444,2664-2731` — les
  redirects ne produisent aucun `web_seed_t` éphémère : `redirects`,
  `bases`, `conns` sont des maps task-locales ; à la fin du batch la
  cible de redirection disparaît, le prochain batch re-demande l'URL
  d'origine et re-suit la 3xx à l'infini (compteur `num_redirects`
  remis à zéro à chaque batch). Upstream `handle_redirect` insère un
  `web_seed_t{ephemeral=true, redirects[file]=path}` dans
  `m_web_seeds`, soumis au round-robin/`max_web_seed_connections`,
  visible dans `web_seed_statuses`, exclu du resume — et efface
  `have_files[file]` sur l'origine (`web_peer_connection.cpp:648-700`,
  `torrent.hpp:160-205`). **Sévérité : wire-visible**.
  [Cycle de vie]
  - **sketch** : étendre `WebSeedDone`/`WebSeedFailure` avec
    `learned_redirects: Vec<(file_index, url, path, auth, no_local_ips)>` ;
    `Session::on_web_seed_done` replie chaque entrée :
    `add_web_seed_ephemeral(url)` → pousse dans `t.web_seeds` +
    `web_seed_state` (`ephemeral=true`, `interesting=true`,
    `no_local_ips`), `need_save_resume` **non** armé (upstream
    filtre `ephemeral` dans `web_seeds()`/`write_resume_data`), et
    efface `have_files[file]` de l'origine (dépend D5). Réf.
    `torrent.cpp` `add_web_seed` promotion `ephemeral → permanent`.
  - **test** : seed A répond 301→B pour file1 ; vérifier
    (i) `web_seed_statuses` contient l'éphémère, (ii) le batch
    suivant ne re-GET pas l'URL d'origine pour file1.
    Oracle : `test_web_seed_redirect.cpp`.
- `crates/bitdaemon-session/src/torrent.rs:6841-6850` — pas de
  `have_files` per-file : `inc_refcount_all` déclare chaque seed
  capable de servir **toutes** les pièces ; un 404/410 sur un
  fichier → `Retry` → le picker redemande le même fichier à la même
  seed → boucle 404 infinie (upstream `have_files.clear_bit(file)`
  `web_peer_connection.cpp:908-911`, bitfield `peer_info.have`
  reflétant seulement les fichiers servis, `interesting=false`
  quand plus rien). Le champ `WebSeed.interesting` est mort (toujours
  `true`, `torrent.rs:6783`). **Sévérité : wire-visible**.
  [Cycle de vie]
  - **sketch** : ajouter `WebSeed.have_files: Option<Bitfield>`
    (init `resize(num_files, true)` paresseux comme upstream),
    `peer_pieces` du pick = bitfield des pièces couvrant les
    fichiers `have_files` (map file→pièces via `FileStorage`),
    effacer le bit du fichier sur `Http`/`InvalidRange`/`Redirect`
    en portant `file_index` dans `WebSeedFailure` ; recalculer
    `interesting` = `have_files.any()`. Réf.
    `torrent.hpp:160-170`, `web_peer_connection.cpp:908-911`.
  - **test** : seed multi-fichier répondant 404 pour file1 —
    le picker ne doit plus proposer ses pièces à cette seed ;
    seed qui perd tous ses fichiers → `interesting=false` →
    skip au pick. Oracle : suite `web_seed` « seed missing file ».
- `crates/bitdaemon/src/engine.rs:5858-5866,5791-5799` — résolution
  mono-endpoint : `resolve()` garde `.next()` de `resolve_all` ; un
  échec de connexion ne tente pas l'adresse suivante ; la résolution
  se refait à chaque batch. Upstream empile **tous** les addrs dans
  `web->endpoints`, connecte `front()`, et à l'échec dépile et
  retente dans le même cycle (`torrent.cpp:7091-7116`,
  `on_disconnect` pop→`connect_web_seed`). Sous-chef : le leg
  `no_local_ips` (`engine.rs:2570-2579`) doit filtrer **la liste** —
  liste filtrée vide → `url_seed_alert(banned_by_ip_filter)` +
  `web->disabled = true` (`torrent.cpp:7107-7116`) ; Rust n'applique
  le test qu'à la première adresse et émet `NameLookup`.
  **Sévérité : état-interne**. [Erreur, Cycle de vie]
  - **sketch** : `resolve_all` → `Vec<SocketAddr>` par base dans
    `WebSeedDial`; boucle dial `for a in addrs` ; filtre
    `no_local_ips` sur la liste entière → si vide,
    `WebSeedBackoff::Disable` + msg `banned_by_ip_filter`.
  - **test** : resolver renvoyant [local, global] pour un seed
    `no_local_ips` → connexion sur le global ; liste 100 % locale →
    seed `disabled` + alerte.
- `crates/bitdaemon/src/engine.rs:2355-2366` — échec de résolution
  du **proxy** → `WebSeedBackoff::NameLookup` (retry 1800 s
  indéfiniment) ; upstream `on_proxy_name_lookup` →
  `url_seed_alert` + `web->disabled = true`
  (`torrent.cpp:6980-6991`). **Sévérité : état-interne**.
  [Erreur]
  - **sketch** : mapper l'échec `resolve(&proxy.hostname…)` sur
    `WebSeedBackoff::Disable`.
  - **test** : resolver proxy en échec → seed marqué disabled.
- `crates/bitdaemon/src/engine.rs:2397-2402,2584-2590` —
  `using_http_proxy` ignore `dial.tls` : pour une seed **https** via
  proxy http, le GET émis dans le tunnel CONNECT est en
  absolute-form (`GET https://…`) alors qu'upstream conditionne
  `using_proxy = (http||http_pw) && !m_ssl`
  (`web_connection_base.cpp` `using_proxy()` — le tunnel CONNECT est
  transparent → origin-form). **Sévérité : wire-visible** (config
  d'angle : https seed + proxy http). [Limites]
  - **sketch** : `let using_http_proxy = … && !dial.tls` — évalué
    par groupe de connexion (le `dial` est par base).
  - **test** : seed https + proxy http (fixture CONNECT) → la
    request-line doit être `GET /path HTTP/1.1`.
- `crates/bitdaemon-session/src/torrent.rs:4675-4686,7291-7304,
  8615-8617` — un seed banni (`known_bad && ban_web_seeds`) est
  `disabled` mais reste listé par `url_seeds()` et
  `web_seed_statuses()` ; upstream filtre `peer_info.banned ||
  removed` du getter `web_seeds()` — le test upstream exige
  `th.url_seeds().empty()` (`web_seed_suite.cpp:184-190`,
  `test_web_seed_ban.cpp`). **Sévérité : état-interne**
  (visible API ; le ban lui-même fonctionne). [Compteurs]
  - **sketch** : ajouter `WebSeed.banned: bool` (set sur le leg
    hash-fail et sur `ban_peer` pour entrées `web_seed`) ;
    `url_seeds()`/`web_seed_statuses()` filtrent
    `banned || removed`. Réf. `torrent.cpp` `web_seeds()` getter
    (`!peer_info.banned && !removed`).
  - **test** : hash-fail attribué au seed →
    `t.url_seeds().is_empty()` et `web_seed_statuses` vide.
    Oracle : `test_web_seed_ban.cpp`.
- `crates/bitdaemon-session/src/torrent.rs` (absent) — pas de
  `restart_request`/`restart_data` : un corps partiellement reçu
  avant déconnexion est abandonné et tout le range est re-demandé ;
  upstream conserve `web_seed_t::restart_request` + `restart_data`
  et ré-émet `Range: bytes=<reçu>-` (`torrent.hpp:180-190`,
  `web_peer_connection.cpp` `maybe_reconnect`/`on_disconnect`).
  Impact borné par la taille des batchs actuels.
  **Sévérité : état-interne**. [Cycle de vie]
  - **sketch** : stocker `(req, bytes_reçus)` dans `WebSeed` à la
    coupure (reporter l'offset partiel dans `WebSeedFailure`),
    réémettre le range résiduel au prochain pick pour cette seed —
    upstream `m_web->restart_request`/`restart_piece` + complément
    `incoming_suggest`.
  - **test** : réponse tronquée mi-corps → le batch suivant
    demande `Range:` commençant à l'offset reçu.
- `crates/bitdaemon-session/src/torrent.rs:6740-6741,12656-12664` —
  gates supplémentaires : `is_seed()|upload_mode|is_paused()` bloquent
  le pick ; upstream `maybe_connect_web_seeds` ne teste que
  `m_abort|empty|finished|!files_checked|limites`
  (`torrent.cpp:10997-11007`) — un torrent `upload_mode` avec pièces
  manquantes télécharge encore par web seeds upstream
  (`is_finished()=false`). L'appel secondaire depuis le chemin
  unchoke (`torrent.cpp:9255`) est aussi absent (latence de
  reconnexion). `is_paused`/`is_seed` : équivalents fonctionnels
  (paused → jamais tické ; `is_seed` ⇒ `finished`). **Sévérité :
  état-interne**. [Cycle de vie]
  - **sketch** : retirer `self.upload_mode` des deux sites
    (`is_seed` redondant avec `finished` mais inoffensif) ;
    invoquer l'équivalent `maybe_connect_web_seeds` (le pick tick)
    aussi après un unchoke/interest-change — réf.
    `torrent.cpp:9255,10897`.
  - **test** : torrent `upload_mode` + pièces manquantes + seed →
    batch émis.
- `crates/bitdaemon-net/src/web_seed.rs:477-482` + `engine.rs:2738-2745`
  — payload de `url_seed_alert` divergent : `message` porte le
  header `Server` et les erreurs non-ok rapportent `"server busy"` ;
  upstream poste `url_seed_alert(url, "<status> <reason>")` /
  `error_code.message()` (`web_peer_connection.cpp:608-645`).
  **Sévérité : cosmétique**. [Erreur]
  - **sketch** : `message: format!("{} {}", r.status(),
    r.parser.message())`.
- `crates/bitdaemon/src/engine.rs` (write path `ThrottledWs`) —
  octets de requête non comptés : upstream `send_buffer(request)` →
  `sent_bytes(0, bytes_transferred)` → `m_statistics` +
  `trancieve_ip_packet`/`sent_protocol_bytes` hors gate
  `ignore_stats` (`peer_connection.cpp:1139-1156`,
  `web_connection_base.cpp:168-172`). Rust ne remonte aucun octet
  émis pour les fetchs (seul le quota upload est consommé).
  **Sévérité : cosmétique**. [Compteurs]
  - **sketch** : dans `ThrottledWs::poll_write`, accumuler
    `sent_protocol_bytes`/`sent_ip_bytes` sur les counters session
    comme le fait `account_sent_bytes` pour les peers.
- `crates/bitdaemon/src/engine.rs:2377-2394` + `web_seed.rs:126` —
  `external_auth`/`extra_headers` de `web_seed_entry` jamais
  peuplés : `WebSeedHeaders.extra` existe mais aucune API ne
  transporte `web_seed_entry::auth`/`extra_headers`
  (upstream : `torrent_info::add_url_seed(url, auth, extra_headers)`,
  `add_headers` `web_connection_base.cpp:119-140`). L'auth URL-
  embedded fonctionne. **Sévérité : état-interne** (API absente —
  comportement indistinguable pour les usages courants).
  [Cycle de vie]
  - **sketch** : ajouter un `add_url_seed_auth(url, auth,
    extra_headers)` stocké dans `WebSeed`/`WebSeedHeaders` — ou
    documenter won't-port si le handle upstream n'expose plus cette
    forme à la baseline.

## Invariants de frontière (Seam Checks)

- `DEPRECATED_OVERRIDE_WEB_SEEDS` (`resume_data.rs:314`,
  `flags.rs:137`) est **posé mais jamais lu** — upstream l'utilise
  pour sauter `internal_web_seeds` du `ti` quand le resume fournit
  son propre `url-list` (`torrent.cpp:689`). En Rust le `ti` ne
  porte pas de seeds (ils vivent dans `atp.url_seeds`), donc
  l'invariant n'est préservé que si le mergeur caller **remplace**
  `atp.url_seeds` par la liste du resume. À documenter/garder :
  toute fusion `resume_atp` dans un `atp` dérivé de `load_torrent`
  doit écraser `url_seeds` quand le flag est posé — sinon les seeds
  du `.torrent` ressuscitent (divergence resume-compat).
- Frontière `WebSeedDone` tardif : un fetch qui survit à
  `remove_torrent`/`remove_url_seed` est routé vers
  `on_web_seed_block` — `ih` absent → drop ; `url` retirée → blocs
  comptés `redundant_bytes` (`torrent.rs:7073-7087`) au lieu d'être
  purement jetés (upstream : la conn est morte, les octets ne
  seraient jamais arrivés). Écart comptable bénin — documenté.
- `session_conns` = `num_connections()` + Σ `web_seed_inflight`
  (`session.rs:7730-7735`) ≙ `m_ses.num_connections()` upstream qui
  inclut les web conns ; le double-comptage interne
  `peers.len() + inflight >= max_connections`
  (`torrent.rs:6751-6754`) reflète `num_peers()` upstream ✓ — pas
  de sur-comptage détecté (les fetchs ne sont jamais dans
  `peers`).
- `want_tick` garde le torrent tické tant que des seeds existent et
  `!finished && files_checked` (`torrent.rs:11127-11129`) — sinon un
  torrent sans peers ne lancerait jamais ses fetchs ✓.
- Bande passante : les fetchs passent par `request_bandwidth_web`/
  `PeerQuota` (`session.rs:7025-7099`, `engine.rs:475-661`) —
  l'enregistrement handle → peer-class est résolu par adresse
  résolue ; sous proxy (`resolved=None`) l'attribution de classe
  retombe sur la classe globale — à revérifier au fix D2 si les
  conns deviennent persistantes (durée de vie du quota handle).
- `WebSeed.ephemeral`/`no_local_ips` sont des champs d'état
  **morts** aujourd'hui (init `false`, jamais lus ailleurs que le
  gate pick) — prévus pour D4 ; ne pas les laisser en dead-code.

## Fichiers du fix

- `crates/bitdaemon-net/src/http_conn.rs` — `recv_response` tampon
  réentrant (D1).
- `crates/bitdaemon-net/src/web_seed.rs` — `fetch_block` carry-over
  (D1), payload `Http.message` (D12).
- `crates/bitdaemon-session/src/torrent.rs` — `WebSeed.have_files` /
  `banned` (D5, D9), pick loop/`peer_pieces` (D2, D5), gates
  `upload_mode` (D11), `url_seeds`/`web_seed_statuses` filtres
  (D9), `restart_request` (D10), `ephemeral` fold-in (D4).
- `crates/bitdaemon-session/src/session.rs` — dispatch loop
  multi-seeds (D2), `on_web_seed_done` fold redirects (D4),
  `max_response` depuis settings (D3).
- `crates/bitdaemon-session/src/settings.rs` — sémantique
  `web_seed_blocks_per_tick` (re-scoper en plafond par batch ou
  retirer du gate) (D2).
- `crates/bitdaemon/src/engine.rs` — conn-map persistante (D2),
  `max_response` setting (D3), learned-redirects reporting (D4),
  multi-endpoint + `no_local_ips` liste (D6), proxy-resolve →
  Disable (D7), `using_http_proxy && !tls` (D8), sent-bytes
  comptage (D13), `external_auth`/`extra_headers` (D14).
- `crates/bitdaemon-session/src/status.rs` — `WebSeedStatus` si
  l'on expose `banned`/étendue.
- Tests à porter : fixture web-seed transport (serveur injecté) dans
  `crates/bitdaemon-net/tests/` ou `bitdaemon-sim` — matrice
  réduite de `web_seed_suite` (multi-GET pipelined, chunked,
  redirect éphémère, ban, 404 partiel, proxy CONNECT https).

## Non-bugs tranchés

- **Absence de `http_seed_connection`** : retirée upstream en v2.x
  (fichier absent à `e7381c7bc`) — le seul chemin `web_seed` Rust
  est la cible correcte ; `atp.http_seeds` rejoint le même fetch
  BEP-19 des deux côtés (`torrent.rs:1384` ↔
  `torrent_info.cpp:1635-1647`).
- **Bypass bandwidth A28** : `ThrottledWs` consomme les deux
  channels + overhead IP (`engine.rs:475-661`) ; finding A28
  corrigé, ne pas re-rapporter.
- **Alertes catégorie** : `Alert::UrlSeed`/`PeerBlocked` postés
  « inconditionnellement » aux call-sites passent le mask dans
  `alerts.post` (`alerts.rs:1787-1793`) — équivalent
  `should_post<>` upstream ; pas de fuite d'alertes.
- **`supports_keepalive` marqué sur n'importe quel `close`** vs
  upstream `m_num_responses==1` seulement — divergence
  comportementale *favorable* (une seed qui ferme en cours de route
  est traitée comme non-keepalive) ; sans impact wire.
- **`num_redirects` cap 64** (`engine.rs:2654`) : upstream n'a pas
  de cap explicite dans `web_peer_connection` (borne implicite via
  retry/timeout) — garde-fou Rust raisonnable, à conserver.
- **`is_paused`/`is_seed` en gates** : paused torrents ne tickent
  pas upstream et `is_seed ⇒ is_finished` — équivalents
  fonctionnels (seul `upload_mode` diverge — voir déviation).
- **`send_buffer` request-string concaténée** : Rust écrit tous les
  GETs d'un plan en une seule écriture comme upstream
  (`web_peer_connection.cpp:485+`) — conforme.
- **Type `web_seed_entry::type_t` non porté** : champ
  `TORRENT_DEPRECATED` inerte upstream — non-porté à juste titre.
