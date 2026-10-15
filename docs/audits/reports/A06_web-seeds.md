# Audit A06 — rapport d'analyse

Périmètre : `web_peer_connection` / `web_connection_base` (BEP-19 url
seeds) + legs torrent (`maybe_connect_web_seeds`, `retry_web_seed`,
`connect_to_url_seed`, `add/remove_web_seed`). `http_seed_connection.cpp`
n'existe pas upstream en 2.1 (BEP-17 supprimé, ChangeLog « removed
support for BEP 17 web seeds ») — le Rust le traite déjà en no-op
déprécié + merge dans `web_seeds`, conforme.

Baseline upstream : `e7381c7bc` (`D:\Projet\libtorrent\libtorrent`,
lecture seule). Modèle Rust : pull — `Torrent::next_web_seed_blocks`
émet `SessionOut::WebSeedFetch` par tick, `engine.rs::spawn_web_seed`
fait le dial+GET+parse, `Session::on_web_seed_block`/`on_web_seed_error`
réinjecte. Pas de connexion persistante `web_seed_t` : l'état par-URL
vit dans `Torrent::web_seed_state` (`WebSeedStatus`).

## Verdicts checklist

- **Range requests : pièces complètes vs blocs, `block_size` alignment,
  coalescing contigu** — verdict : **déviation** (partielle).
  Conforme : `plan_piece_request` mappe le span via `map_block`, pads
  incluses sans GET (`got_get:false`), `Range: bytes=start-end` inclusif
  — `crates/bitdaemon-net/src/web_seed.rs:201-285,349-353` ;
  `render_web_request` émet un GET par `file_request` non-pad dans
  l'ordre `m_file_requests` — `web_seed.rs:291-360` ; les blocs choisis
  contigus fusionnent en une seule `PeerRequest` franchissant les
  frontières de pièce (`request_large_blocks`) —
  `crates/bitdaemon-session/src/torrent.rs:5125-5138` ; le span v2
  pad-inclus est le `map_length` upstream —
  `web_seed.rs:191-198`. Déviant : `prefer_contiguous_blocks` non porté
  (`prefer_contiguous_blocks: 0` — `torrent.rs:5074`) et le sizing
  upstream `max(max(keepalive?1:4 MiB, piece_length),
  urlseed_max_request_bytes)` n'existe pas — les requêtes plafonnent à
  `web_seed_blocks_per_tick`×`block_size` ≈ 64 Kio par batch vs
  ~16 Mio upstream — `src/web_peer_connection.cpp:67-74` ;
  `urlseed_max_request_bytes` déclaré (`settings.rs:606`) jamais lu ;
  adaptation `supports_keepalive` (4 Mio min) absente.

- **Erreurs : HTTP non-200, redirect loops, content-length mismatch,
  EOF mid-piece → url_seed_alert + seed marqué** — verdict :
  **déviation**. Conforme : statut non-ok → erreur →
  `on_web_seed_error` → `Alert::UrlSeed` + `abort_download` des blocs —
  `session.rs:8431-8446`, `torrent.rs:5247-5262`. Déviant : (a) le
  payload d'alerte porte le header `Server` au lieu de
  `"<status> <reason>"` — `web_seed.rs:441-446` vs
  `handle_error` `to_string(status_code)+" "+m_parser.message()`
  (`web_peer_connection.cpp:619-626`) ; (b) aucune validation
  `Content-Range`/`content_length` (`get_range`,
  `web_peer_connection.cpp:524-555,948-981`) — un corps de bonne
  taille mais mauvais offset est accepté → hash-fail ; `no_content_
  length` non détecté ; (c) aucun marquage du seed — upstream
  `have_files.clear_bit(file)` sur non-2xx (`:911-918`), `disable()`
  permanent sur `invalid_range`/`missing_location` (`:631-646,979`) ;
  (d) redirects apprises en mémoire par *tâche de fetch* seulement —
  `engine.rs:1340-1343` vs `m_web->redirects` + `add_web_seed`
  éphémère persistants (`:648-784`) ; `Retry-After` ignoré
  (`header_duration("retry-after")`, `:619`) ; (e) EOF/parse-error :
  upstream disconnect `http_parse_error` après `received_bytes`, Rust
  `HttpError` → release — sémantique proche mais sans backoff
  (voir item retry).

- **Retry : backoff par seed, `retry_web_seed`, bannissement
  temporaire** — verdict : **déviation** (majeure). Aucun état de
  backoff : `web_seed_t::retry` (`urlseed_wait_retry` sur
  `error>=failure` + `uninteresting_upload_peer` —
  `web_peer_connection.cpp:233-235,276-280` ; `web_seed_name_lookup_
  retry` sur DNS fail — `torrent.cpp:7095-7098`), `disabled`
  (protocole non-http, IDNA, port 0, port_filter, SSRF, banned —
  `torrent.cpp:6815-6916,7210-7249`), `interesting`, `have_files` —
  rien d'équivalent dans `WebSeedStatus` (`status.rs:62-73`) et
  `next_web_seed_blocks` ne consulte jamais `last_error`
  (`torrent.rs:5037-5044`). `Torrent::retry_web_seed` est un stub à
  sémantique inversée (reset du curseur round-robin, `_url` ignoré,
  aucun caller) — `torrent.rs:9667-9673`. Conséquence : un seed en
  échec est re-sollicité à chaque tick (~1 s) au lieu de 30 s/30 min,
  et indéfiniment (pas de `disabled`) — martèlement réseau + spam
  d'alertes. `urlseed_wait_retry`, `web_seed_name_lookup_retry`,
  `ban_web_seeds`, `report_web_seed_downloads` déclarés+sérialisés
  jamais lus (grep workspace : seuls `settings.rs`/`session_params.rs`
  matchent).

- **Attribution : erreurs/compteurs par URL** (cohérence
  `on_web_seed_error(url)`) — verdict : **déviation** (partielle).
  Conforme : `WebSeedDone{ih,url,result}` transporte l'URL jusqu'au
  dispatch (`engine.rs:130-142,677-686`) ; `st.outstanding`/`st.
  downloaded`/`st.last_error` mis à jour sous la clé URL sur les deux
  legs (`torrent.rs:5147-5153,5216-5221,5249-5261`) ;
  `web_seed_statuses()` snapshot par `m_web_seeds` entry
  (`torrent.rs:5268-5280`). Déviant : les octets payload web-seed
  n'entrent **jamais** dans `payload_downloaded`/`stat_down_total`/
  `down_rate` — `total_download()` ne somme que les conns réelles +
  `payload_downloaded` (`torrent.rs:7960`) — upstream compte via
  `received_bytes`→`m_stat` sauf `report_web_seed_downloads=false`
  (`web_peer_connection.cpp:59-60`, `session_impl.cpp:6756-6766`).
  `total_download`, `total_payload_download`, `download_rate`, les
  compteurs session et `total_downloaded` du resume sous-reportent
  d'autant ; `report_web_seed_downloads` inerte. Edge :
  `piece_failed`/restores picker bypassent `on_web_seed_error` →
  `web_seed_outstanding` et `st.outstanding` peuvent dériver (bloc
  restauré côté picker encore marqué in-flight côté seed).

- **Request pacing : `web_seed_blocks_per_tick`, connection reuse vs
  reconnect** — verdict : **déviation**. Conforme : reuse par base à
  l'intérieur d'une tâche, `Connection: close`/`connection_close()` →
  conn retirée et re-dialée (`engine.rs:1365-1391,1389-1391`),
  `first_request` par connexion — `m_first_request` upstream
  (`web_seed.rs:301` note, `engine.rs:1365`). Déviant : aucun gate
  `max_web_seed_connections` (3), `m_max_connections` ni
  `connections_limit` (`maybe_connect_web_seeds`,
  `torrent.cpp:10997-11031`) — chaque tick peut spawner un fetch pour
  la même URL tant que des blocs sont pickables → conns concurrentes
  non bornées vers un seed lent ; aucun timeout de fetch
  (`urlseed_timeout` = timeout() upstream, `web_connection_base.cpp:
  76-81`, jamais lu) → une réponse qui pend laisse ses blocs
  `downloading` sous `WEB_SEED_PEER` pour toujours —
  `check_request_timeouts` n'itère que `self.peers`
  (`torrent.rs:7390`) ; requête pure-pad émet quand même un
  `WebSeedFetch` → dial + `write_all("")` alors qu'upstream livre les
  zéros localement (`num_pad_files == size` → `on_receive_padfile`,
  `web_peer_connection.cpp:499-505`) — un échec de dial fait échouer
  un bloc qui n'avait besoin d'aucun réseau ; gate `m_files_checked`
  absent — `next_web_seed_blocks` ne teste que `picker.is_some() &&
  files.is_some()` (`torrent.rs:5038-5043`), `want_tick` admet les
  torrents avec peers avant la fin du check (`torrent.rs:8609-8613`)
  → fetch possible avant vérification disque.

- **Pseudo-peer `web_seed_t` : download queue partagée, legs upload
  N/A** — verdict : **conforme** (au modèle pull près). Blocs trackés
  sous `WEB_SEED_PEER` (`torrent.rs:5030`), `inc_refcount_all` virtuel
  (`:5045-5051`), `mark_as_downloading`/`mark_as_writing`/
  `abort_download` sous la clé (`:5109,5223,5255`) — les vrais peers
  ne double-requestent pas ; jamais de leg upload (`m_upload_only=
  true` upstream, `web_connection_base.cpp:88`) ; skips web-seed dans
  choker/optimistic (`session.rs:7429-7430,7578`) vacuums mais
  corrects — nos seeds ne sont jamais des conns.

## Déviations

- `crates/bitdaemon-session/src/torrent.rs:5037` — aucun backoff /
  `disabled` / `have_files` par seed : `next_web_seed_blocks` re-pique
  une URL en échec à chaque tick. Upstream `web->retry` +
  `w->disabled || w->removed || w->retry > now || !w->interesting`
  (`torrent.cpp:11022`) ; `urlseed_wait_retry` (30 s), `web_seed_name_
  lookup_retry` (1800 s), `Retry-After` (`handle_error`,
  `web_peer_connection.cpp:619`), `disabled` permanent sur
  proto/IDNA/port/SSRF/ban/DNS-proxy. Fix : ajouter `retry_until` +
  `disabled` + `have_files` (ou bitfield fichiers-404) à
  `WebSeedStatus`/état interne, les consulter dans
  `next_web_seed_blocks`, parser `Retry-After` dans le leg erreur.
  Sévérité : **wire-visible** (martèlement d'un seed mort, absence de
  bannissement).

- `crates/bitdaemon-session/src/torrent.rs:5247` + `engine.rs:1287-
  1290` — pas de distinction transient/permanent ni marquage par
  fichier : un 404 sur `file N` ré-émet la requête sur ce fichier
  chaque tick ; upstream `have_files.clear_bit(file)` exclut le
  fichier du seed (`web_peer_connection.cpp:908-918`). Fix : bitfield
  `have_files` par seed (ou set de file_index 404) consulté au pick
  via les spans de `map_block`. Sévérité : wire-visible.

- `crates/bitdaemon-net/src/web_seed.rs:441-455` + `engine.rs:1387` —
  `fetch_block` ne valide ni `Content-Range` (206) ni
  `content_length` (non-206) : corps accepté si `len == want` quel que
  soit l'offset — upstream `get_range` + `invalid_range` → `disable`
  (`web_peer_connection.cpp:524-555,963-981`). Fix : lire
  `parser.content_range()`/`content_length`, vérifier
  `range_start==start && range_end==start+length`, marquer le seed
  `disabled` sur mismatch. Sévérité : wire-visible (données
  silencieusement fausses → hash-fail loop).

- `crates/bitdaemon-net/src/web_seed.rs:33-57` —
  `normalize_web_seed_url` **jamais appelée** : seed single-file
  finissant par `/` n'obtient jamais le nom de fichier → GET du
  répertoire (upstream ctor `m_path += escape_file_path(file 0)`,
  `web_peer_connection.cpp:86-99`). Le cas multi-file sans `/` final
  est sauvé par le render (`web_seed.rs:330-334`) mais le cas
  single-file est cassé. Fix : appeler la normalization au chargement
  (`torrent.rs:1104`) ou dans `spawn_web_seed`. Sévérité :
  wire-visible (fonctionnalité single-file seed inopérante pour les
  URL directory).

- `crates/bitdaemon/src/engine.rs:1228-1477` — aucun timeout sur le
  fetch : `resolve`/`connect`/`fetch_block` sans `tokio::time::
  timeout` ; upstream `web_connection_base::timeout() =
  urlseed_timeout` (20 s) tue la conn → disconnect libère les blocs.
  Chez nous un seed qui trickle → blocs `downloading` sous
  `WEB_SEED_PEER` à jamais (`check_request_timeouts` n'itère que
  `self.peers`, `torrent.rs:7390`). Fix : timeout `urlseed_timeout`
  par opération (connect/read) ou global par requête →
  `on_web_seed_error`. Sévérité : état (fuite de blocs permanente).

- `crates/bitdaemon/src/engine.rs:1340-1463` — `redirects` et bases
  éphémères vivent dans la tâche : ré-appris à chaque batch ;
  upstream persiste `web_seed_t::redirects` + `add_web_seed`
  éphémère réutilisable (`web_peer_connection.cpp:722-758`), avec
  `have_files` par fichier sur le seed d'origine ET le redirect. Fix :
  remonter la table redirects au `Torrent` (par (seed,file_index)) et
  propager `have_files` manquant. Sévérité : état/perf (round-trip
  redirect par batch, re-404 répétés).

- `crates/bitdaemon/src/engine.rs:1294-1308` — SSRF web-seed absent :
  `connect_web_seed` upstream bloque IP locale + query string quand
  `ssrf_mitigation` (`torrent.cpp:7231-7250`), et `handle_redirect`
  pose `no_local_ips` sur les redirects depuis une IP globale
  (`:670-672`) filtré à la résolution (`:7105`). Rust : rien — un
  seed internet peut rediriger vers 127.0.0.1/169.254.x. Helpers
  `is_local`/`is_global` existent (`ip_helpers.rs:13,83`). Fix :
  check `is_local(addr) && path contient '?'` au connect + filtrer
  les addrs résolues des redirect-targets si `ssrf_mitigation` et
  seed d'origine global. Sévérité : wire-visible + **sécurité**.

- `crates/bitdaemon/src/engine.rs:1333-1337` + `engine.rs:3670-3687`
  — failover multi-endpoints absent : `resolve` garde `.next()` seul ;
  upstream empile `web->endpoints` et pop le front sur échec de
  connect (`web_peer_connection.cpp:215-220`,
  `torrent.cpp:7101-7134`). Fix : garder la liste d'addrs par base,
  itérer à l'échec. Sévérité : état (connectivity).

- `crates/bitdaemon-session/src/torrent.rs:5194-5239` — compteurs :
  bytes web-seed hors `payload_downloaded`/`stat_down_*` →
  `total_download`, `down_rate`, `total_payload_download`, resume
  `total_downloaded` sous-reportent ; `report_web_seed_downloads`
  (défaut true) inerte — upstream compte via `m_stat` sauf opt-out
  (`web_peer_connection.cpp:59-60`). Fix : accumuler
  `payload_downloaded`/rate avec `data.len()` par leg (honorer le
  flag). Sévérité : état (stats/resume faux).

- `crates/bitdaemon/src/engine.rs:1370-1379` + `plan_piece_request`
  — bloc pure-pad fait dial+write("") : upstream synthétise les zéros
  sans réseau (`on_receive_padfile`, `web_peer_connection.cpp:
  499-505,1272-1324`). Fix : court-circuiter les plans sans `got_get`
  (livrer des zéros directement à `on_web_seed_block`). Sévérité :
  état (échec possible d'un bloc sans-besoin-réseau + dial inutile).

- `crates/bitdaemon-session/src/torrent.rs:5038-5043` — gate
  `m_files_checked` manquant (`maybe_connect_web_seeds`,
  `torrent.cpp:11004`) : un torrent non vérifié avec peers tick et
  peut fetcher des pièces déjà sur disque. Fix : ajouter
  `!self.files_checked` au early-out. Sévérité : état/perf.

- `crates/bitdaemon-session/src/session.rs:6206` — aucune borne de
  concurrence par torrent (`max_web_seed_connections` 3, conn limits)
  ni de `peer_info.connection` équivalent : un fetch par tick par URL
  pickable → accumulation. Fix : compter les fetches en vol par
  torrent (`web_seed_state.busy` ou set dédié), caper, ne pas
  re-spawner pour une URL déjà en vol. Sévérité : état/ressource.

- `crates/bitdaemon/src/engine.rs:1380-1381` — proxy web-seed jamais
  câblé : `using_proxy=false` constant, `proxies.web_seed` (=
  `m_ses.proxy()` upstream, filtré par `proxy_peer_connections`/
  `proxy_hostnames` — `torrent.cpp:6786-6791,6924-6946`) déclaré non
  lu. Fix : si proxy http/http_pw actif → conn via proxy (ou
  CONNECT), `using_proxy=true` (absolute-form + Proxy-Connection +
  Proxy-Authorization si http_pw), `proxy_hostnames` → résolution
  distante. Sévérité : wire-visible (feature morte).

- `crates/bitdaemon/src/engine.rs:1387` — payload d'alerte
  `url_seed_alert` : `"{status} {reason-phrase}"` upstream ; Rust
  envoie `"http {code}: {server_header}"` (`web_seed.rs:441-446` —
  utiliser `parser.message()` pas `header("server")`). Sévérité :
  cosmétique/observabilité.

- `crates/bitdaemon-session/src/torrent.rs:9667-9673` —
  `retry_web_seed` stub inversé + `maybe_connect_web_seeds`
  (`:9675-9679`) sans caller — dead code à faire vivre ou marquer ;
  upstream `retry_web_seed(conn, retry-after)` *pose* le backoff.
  Sévérité : cosmétique (l'API ne fait rien).

- `crates/bitdaemon-session/src/torrent.rs:3386-3396` — hash-fail :
  `WEB_SEED_PEER` correctement exclu du disconnect mais **rien** ne
  reproduit `received_invalid_data` web-seed (clear `have_files`
  fichier/pièce, ban si `num_have_pieces()==0` single-file, gate
  `ban_web_seeds` — `web_peer_connection.cpp:562-601`,
  `torrent.cpp:11919-11926`). Un seed qui sert du poison est
  re-sollicité indéfiniment. Sévérité : état/wire-visible.

- `crates/bitdaemon/src/engine.rs:1281-1286` — parsing URL : upstream
  distingue `invalid_hostname`/`invalid_port`/`unsupported_url_
  protocol`/`blocked_by_idna`/`port_blocked` → `disabled` (jamais
  retenté) ; Rust `parse_url_components` fail → "invalid url" →
  retenté chaque tick ; `port==-1→80` même pour https (443 attendu)
  et port 0 non rejeté ; `allow_idna`/`is_idna` non consulté ;
  `port_filter` non consulté. Sévérité : wire-visible.

- `crates/bitdaemon/src/engine.rs` — pas de `restart_request`/
  `restart_piece` (`web_peer_connection.cpp:178-183,240-266,
  340-366`) : un échec mid-body jette le buffer accumulé de la
  requête courante ; upstream resume au point d'interruption via
  suggest. Fix : conserver le fragment partiel par (seed,req) ou
  accepter le re-téléchargement entier. Sévérité : état/perf (mineur).

- `crates/bitdaemon/src/engine.rs` — pas de `supports_keepalive`
  appris (`connection_close` sur 1re réponse → `m_web->
  supports_keepalive=false` → requêtes 4 Mio min, reconnect immédiat
  sur EOF normal — `web_peer_connection.cpp:65-68,268-274,887-892`).
  Fix : stocker le flag par seed, l'utiliser dans le sizing.
  Sévérité : perf (mineur).

- `crates/bitdaemon-core/src/resume_data.rs:314` +
  `flags.rs:136-141` — `DEPRECATED_OVERRIDE_WEB_SEEDS` posé à la
  lecture mais jamais appliqué : le merge `resume.url_seeds` ⊕
  metainfo `url-list` (clear sauf `merge_resume_http_seeds` —
  `session_handle.cpp:272-294`) n'existe pas ; `merge_resume_*`
  flags absents. Frontière avec A21 — à coordonner. Sévérité : état.

- `crates/bitdaemon-session/src/torrent.rs:6524-6531` —
  `remove_url_seed` laisse `web_seed_outstanding` marqués
  `downloading` sous `WEB_SEED_PEER` jusqu'à la fin (ou la pendaison
  — cf. timeout) de la tâche en vol ; upstream `remove_web_seed_iter`
  → disconnect + `picker().clear_peer` immédiat (`torrent.cpp:
  6764-6776`). Fix : drainer `web_seed_outstanding` par URL (nécessite
  de tracker URL par bloc ou `clear_peer(WEB_SEED_PEER)` partiel).
  Sévérité : état (mineur).

- `engine.rs` — `https://` seeds non supportés : connector = TCP
  clair ; upstream wrappe `ssl_stream` + `m_ssl` +
  `format_host_header(443)` (`web_connection_base.cpp:61-67`,
  `torrent.cpp:7177-7185`). Recouvre le périmètre A23 (SSL) —
  noter comme dépendance. Sévérité : wire-visible (https seeds
  inopérants).

## Fichiers du fix

- `crates/bitdaemon-net/src/web_seed.rs` — validation Content-Range/
  content-length, `message()` dans l'erreur HTTP, gate `disabled`/
  `have_files` sur la planification (signature de
  `plan_piece_request` pour recevoir l'état par seed).
- `crates/bitdaemon-session/src/torrent.rs` — `WebSeedStatus`+état
  interne : `retry_until`, `disabled`, `have_files`, `busy`,
  redirects persistants ; gates `files_checked`/`retry`/`disabled`/
  caps dans `next_web_seed_blocks` ; `on_web_seed_error` varianté
  (permanent vs transient, fichier 404) ; compteurs payload ;
  `remove_url_seed` drain ; `piece_failed` hook
  `web_seed_outstanding`.
- `crates/bitdaemon-session/src/status.rs` — champs WebSeedStatus.
- `crates/bitdaemon-session/src/session.rs` — `on_web_seed_error`
  enrichi (kind/permanent/file_index), alertes `url_seed` payload
  `"{status} {message}"`, pacing caps.
- `crates/bitdaemon-session/src/session_handle.rs` — `retry_web_seed`
  réel (ou retrait), `web_seed_statuses` enrichi.
- `crates/bitdaemon/src/engine.rs` — timeout `urlseed_timeout`,
  pads-only short-circuit, multi-endpoints failover, SSRF checks,
  proxy web-seed, redirects persistants transmis, `https`.
- `crates/bitdaemon-core/src/resume_data.rs` + `flags.rs` —
  merge_resume_http_seeds/override (coord. A21).
- Tests : `crates/bitdaemon-net/tests/test_web_seed_natpmp.rs`,
  `crates/bitdaemon-session/tests/test_session.rs` (+ nouveau test
  backoff/404/disable/pure-pad).

## Non-bugs tranchés

- **Modèle pull vs connexion persistante** — divergence d'architecture
  assumée (PORTING.md row `web_connection_base.cpp` + ADR-0008
  transports injectés) : les seeds sont des tâches de fetch, pas des
  `peer_connection`. Les items ci-dessus ne portent que sur les
  *invariants wire-visible/état* que le modèle pull doit quand même
  respecter.
- **`is_seed()` vs `is_finished()`** dans `next_web_seed_blocks`
  (`torrent.rs:5039`) — upstream gate `is_finished()` ; le picker ne
  rend que des blocs voulus → un torrent finished-non-seed rend
  `picked` vide → `None` : même observable.
- **`urlseed_pipeline_size`** (5) — déclaré+sérialisé mais mort
  upstream aussi (`settings_pack.cpp:241` seule référence src/) ;
  `web_seed_blocks_per_tick` (4) est l'analogue documenté pull-model.
- **Legs upload/`m_upload_only`/`disconnect_if_redundant`/`choker`**
  — N/A pull-model ; les skips choker sur `p.web_seed` sont vacuums
  mais corrects.
- **BEP-17 `http_seed_connection`** — supprimé upstream en 2.1 ;
  `add/remove_http_seed` no-op + `http_seeds()` vide + merge
  `atp.http_seeds`→`web_seeds` : conforme (`torrent.rs:6472-6487,
  1109-1120`).
- **`m_first_request` par connexion** et headers (`User-Agent` gate
  `anonymous_mode`/`always_send_user_agent`, `Authorization` Basic +
  same-origin only, `Connection: keep-alive` 1re-req/proxy) —
  `web_seed.rs:129-164` conforme. Écart : upstream émet
  `User-Agent:` même vide (`web_connection_base.cpp:113-118`),
  Rust skip si `user_agent.is_empty()` — différence wire cosmétique
  assumée (fingerprint deviation documentée PORTING.md settings row).
- **`ephemeral`/resume** — nos seeds redirect ne sont jamais ajoutés
  à `web_seeds`, donc rien d'éphémère à exclure du resume ;
  `write_resume_data` émet tout `web_seeds` (`torrent.rs:2198-2202`)
  comme upstream émet non-ephemeral — la *perte* de persistance des
  redirects est la déviation listée plus haut.
