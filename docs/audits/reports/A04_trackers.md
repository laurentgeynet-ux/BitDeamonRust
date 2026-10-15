# Audit A04 — rapport d'analyse

Périmètre : announce HTTP/UDP/WS, scrape, events, backoff, failover tiers,
compteurs wire. Baseline upstream : `e7381c7bce60b96feb79442828ff2b912fd8a29f`
(checkout `D:\Projet\libtorrent\libtorrent`, vérifié, lecture seule).

Carte du chemin Rust (avant lecture upstream) :

- `bitdaemon-net/tracker_client.rs` — un échange announce par appel :
  `announce_http` (GET + `parse_response`) et `announce_udp` (connect →
  announce BEP-15 via `UdpTrackerDemux`, routage par `transaction_id` sur la
  socket datagramme partagée, ADR-0008). `scrape_http`/`scrape_udp` idem.
- `bitdaemon-session/tracker_list.rs` — `AnnounceEntry`/`AnnounceEndpoint`/
  `AnnounceInfohash` : `fails`, `next_announce`, `min_announce`, `updating`,
  `start_sent`, `complete_sent`, `triggered_manually`/`sent_manual` ;
  `due()` = machine `announce_state` de sélection par tier.
- `torrent.rs` — `announce_params()` construit le `AnnounceParams` ;
  `update_scrape_state()` agrège max des compteurs scrape.
- `session.rs` — `tracker_announce_due` (pompe à chaque tick engine),
  `dispatch_announce`, `announce_stopped_collect`, burst `completed` dans le
  handler `BecameSeed`, `on_announce_response`, `on_scrape_response`,
  `force_announce`, `scrape_tracker`/`scrape_tracker_url`, auto-scrape dans
  `tick()`.
- `bitdaemon/engine.rs` — `spawn_announce`/`spawn_scrape` : resolve →
  `filter_tracker_addr` (ip_filter) → connect → exchange → `AnnounceDone`
  → `Session::on_*_response` ; pump `tracker_announce_due` à chaque tick.

## Verdicts checklist

- **Events : none/started/stopped/completed — `completed` envoyé UNE fois au
  premier finish, re-announces sans event** — verdict : **déviation** —
  preuves : mapping d'event conforme (`dispatch_announce`,
  `session.rs:8190-8198` : `!start_sent`→started, `is_seed &&
  !complete_sent`→completed, sinon none ; `finished && !seeding`→paused
  `session.rs:8142-8146` = upstream `torrent.cpp:3269-3270`). Mais le burst
  `completed` de `BecameSeed` (`session.rs:8048-8077`) pousse les
  `TrackerAnnounce` sans `updating` ni `sent_event` → la réponse ne latch
  jamais `complete_sent` → `completed` réémis à chaque announce périodique
  suivante (voir D6). `stopped` sans `start_sent` est sauté
  (`session.rs:8199`) comme upstream `torrent.cpp:3362`, mais le collecteur
  `stopped` ignore `fail_limit` (`session.rs:3255` vs
  `announce_with_tracker` qui conserve le gate `can_announce`) → D14.
- **Backoff : échecs exponentiels, cap, jitter, `fail_limit` → tracker
  suivant** — verdict : **déviation (partielle)** — la formule est conforme
  bit-pour-bit : `tracker_list.rs:84-96` `max(retry_interval, min(60min, 5s
  + fails²·5s·backoff/100))`, `fails` saturant à 127
  (`announce_entry.cpp:226-243`). Pas de jitter upstream — aucun ici non
  plus. En revanche `retry_interval` est toujours `Duration::ZERO`
  (`session.rs:8592`) : upstream propage `interval ?: min_interval` de la
  réponse d'échec (`tracker_manager.cpp:183`), perdu ici → D7. Le
  failover intra-tier par `deprioritize_tracker` n'existe pas → D9.
- **Tiers : ordre, failover, `announce_to_all_tiers/trackers`** — verdict :
  **déviation** — la machine de sélection `TrackerList::due`
  (`tracker_list.rs:381-439`) reproduit l'ordre et les deux flags upstream
  (`torrent.cpp:3379-3412`) ; insertion stable par tier conforme
  (`add`, `tracker_list.rs:330-345` vs `tracker_list.cpp:67-88`). Mais :
  `deprioritize_tracker` absent (D9), `prefer_udp_trackers` setting présent
  (`settings.rs:175`, défaut true comme upstream) mais jamais consommé —
  `tracker_list.cpp:93-126` `prioritize_udp_trackers` non porté (D15),
  `dont_try_again` sur HTTP 410 absent (D8).
- **Scrape : `want_scrape`, auto_scrape_interval/min, réponse →
  complete/incomplete/downloaded** — verdict : **déviation légère** —
  scaler conforme (`session.rs:6263-6289` vs `session_impl.cpp:3799-3828` :
  `interval/max(1,N)` plancher `auto_scrape_min_interval`, round-robin,
  torrents `paused && auto_managed && !aborted`, skipped session pausée).
  `scrape_pick` fallback `last_working`→`first` conforme
  (`session.rs:8920-8932` vs `torrent.cpp:3648-3674`). Écarts : `verified`
  marqué sur réponse scrape (`session.rs:8307`) alors qu'upstream ne le fait
  que sur announce (`torrent.cpp:3868`) — casse le gate
  `use_dht_as_fallback` (`torrent.cpp:771`, `3010`) → D10 ; une seule
  info-hash scrapée par hybride (D13).
- **UDP tracker : connect→announce→scrape wire, transaction ids, tronqué,
  timeout/retry** — verdict : **déviation (timeouts + cache)** — wire
  format conforme : magic `0x41727101980`, actions 0/1/2/3, layout announce
  98 o + trailer `0x02,len,request_string≤255` (`udp_tracker.rs:110-157` vs
  `udp_tracker_connection.cpp:472`, `694-750`), strides peers 6/18
  (`parse_announce_response` `:169-218` vs upstream `:578-624`),
  `interval` clampé `min_announce_interval` + `min_interval=60` fixe —
  identique à upstream (`:572-574`). Écarts : timeout fixe 15 s/phase
  hardcodé (`engine.rs:981`, `:1160`) au lieu de
  `tracker_completion_timeout`/`tracker_receive_timeout`/
  `stop_tracker_timeout` (D1) ; pas de cache `connection_id`
  (D16) ; `stop_tracker_timeout ≤ 0` devrait supprimer `stopped`
  (`torrent.cpp:3239-3245`) — non implémenté (D14).
- **key/tracker_id génération + stabilité ; num_want, left/bytes
  accounting** — verdict : **déviation** — `key` est un `i32` aléatoire
  unique **par session** partagé par tous les torrents
  (`session.rs:975-978`, `torrent.rs:2734`) ; upstream `tracker_key()` =
  SHA1(ptr torrent, storage, session) → distinct par torrent
  (`torrent.cpp:5582-5593`) → D11. `tracker_id` per-entry propagé
  (`engine.rs:956`, `AnnounceEntry.tracker_id`) mais le fallback
  `m_trackerid` (atp) n'existe pas → D12. `num_want = -1` toujours, même
  pour `stopped` (`torrent.rs:2733`) vs `num_want(=200)`/`0` upstream
  (`torrent.cpp:3504-3505`) → D4.
- **Réponses : peer dicts (v4/v6), min interval, warning, trackerid** —
  verdict : **déviation** — parsing conforme : compact `peers`/`peers6`
  strides 6/18, dict-list avec tolérance par-entrée, `hostname_peers`
  résolus ensuite, `external ip` → `ip_voter`, `tracker id` →
  `Trackerid` alert, `interval` défaut 1800 / `min interval` 30 + clamp
  `min_announce_interval` (`http_tracker.rs:252-371`,
  `tracker_client.rs:253-258` vs `http_tracker_connection.cpp:433`,
  `541-543`). Écarts : `peer_ids` parsés mais jamais utilisés (le skip
  `pid == m_peer_id` upstream `torrent.cpp:3919-3921` est absent → D17) ;
  erreurs aplaties en `String` — `410→dont_try_again` et
  `interval`/`min_interval` des réponses d'échec perdus (D7/D8) ;
  `vote_external` auto-attribué au lieu de `tracker_ip` (D18) ; `warning
  message` non mémorisé sur l'endpoint (D19) ; résolution DNS limitée à
  la première adresse (D20).
- **`tracker_warning` count vs `tracker_error`, `next_announce` reset on
  force_announce** — verdict : **déviation** — `trigger()` =
  `max(t, min_announce)+1s`, `ignore_min_interval` → `t+1s` et clear —
  conforme (`tracker_list.rs:181-195` vs `torrent.cpp:4144-4160`).
  `force_announce(Some(i))` → `trigger` + dispatch immédiat si éligible =
  `announce_tracker_now` ✓. Écarts : `times_in_row` posé sur
  `TrackerWarning` (codé à 1, `session.rs:8570`) alors qu'upstream le
  porte sur `tracker_error_alert` (= `a.fails`) — `TrackerError` Rust ne
  porte ni count ni `error_code` ni `op` (`alerts.rs:196-214`) → D21 ;
  le gate `m_announce_to_trackers` absent du chemin `force_announce` by-idx
  (`torrent.cpp:3602` vs `session.rs:6798` qui ne teste que `is_paused`)
  → D22.

## Déviations

- `engine.rs:958-1106` + `http_conn.rs:151-176` — **aucun timeout sur le
  chemin HTTP** : `recv_response` boucle `read()` sans deadline,
  `TokioTcpConnector::connect` sans timeout (`os_transports.rs:50`), et
  l'intervalle `spawn`é n'a pas de garde-fou global. Upstream borne l'échange
  par `tracker_completion_timeout` (30 s, `settings_pack.cpp`) et chaque
  lecture par `tracker_receive_timeout` (10 s) via `timeout_handler`
  (`tracker_manager.cpp:92-141` ; `http_tracker_connection.cpp:251`
  `get(url, seconds(timeout), …)`). Conséquence : un tracker TCP-connecté
  qui ne répond jamais laisse `slot.updating = true` indéfiniment → cette
  entrée n'annonce plus jusqu'au prochain pause/resume ; un `stopped` peut
  pendre au lieu de se borner à `stop_tracker_timeout` (5 s). Sévérité :
  **haute** (état + wire). Fix proposé : délai global par exchange
  (`tokio::time::timeout`) + read-timeout par recv, paramétrés par
  `settings.tracker_completion_timeout`/`tracker_receive_timeout`, et
  `stop_tracker_timeout` pour `event == stopped` (annonce *et* scrape).

- `engine.rs:1098` + `http_conn.rs:70-85` — **pas de `User-Agent` HTTP** :
  `announce_http(.., &[], ..)` — seuls `Host`/`Accept-Encoding: gzip`/
  `Connection: close` partent. Upstream envoie toujours `User-Agent:
  <user_agent>` (ou `curl/7.81.0` si `anonymous_mode && !private`,
  `http_tracker_connection.cpp:232-236`). Trackers qui filtrent sur UA
  voient une requête sans en-tête → rejet/fingerprinting. Sévérité :
  **moyenne-haute** (wire-visible). Fix : passer `User-Agent` depuis
  `settings.user_agent` (+ variante curl en anonymous non-privé — nécessite
  de propager le flag `private` du torrent dans `AnnounceParams`).

- `http_conn.rs:218-220`, `engine.rs:1098-1101` — **redirects cross-origin
  échouées** : `http_get` remonte `CrossOriginRedirect(next)` ; l'engine le
  convertit en échec d'announce. Upstream `http_connection` reconnecte à la
  nouvelle origine (5 hops max, auth non forwardée). Un `http://`→`https://`
  301 (cas courant) rate l'announce → backoff → failover factice.
  Sévérité : **moyenne** (wire). Fix : boucle reconnect dans
  `spawn_announce` sur `CrossOriginRedirect` (re-resolve + connect + TLS si
  https) jusqu'à N hops.

- `torrent.rs:2722-2744` — **compteurs announce approximés** : `uploaded: 0`,
  `downloaded: verified_bytes` (octets *vérifiés*, pas payload wire),
  `left = num_pieces·piece_length − verified` (slack tail-piece + pièces
  non-désirées incluses ; magnet → `left=0`), `corrupt: 0`, `redundant: 0`,
  `num_want: -1` (y compris `stopped`). Upstream `build_tracker_request`
  (`torrent.cpp:3430-3505`) : `downloaded = total_payload_download −
  total_failed`, `uploaded = total_payload_upload`, `corrupt =
  m_total_failed_bytes`, `redundant = m_total_redundant_bytes`, `left =
  bytes_left().value_or(16*1024)` (wanted only, tail exacte ; 16 KiB avant
  metadata), `num_want = stopped ? 0 : num_want(200)` ; ajustement
  seed→`downloaded -= in-flight`/`redundant +=` et `report_true_downloaded`
  gating. Conséquences wire : trackers ratio-based voient `uploaded=0` et un
  `downloaded` fictif (seed pré-existant annonce sa taille complète) ;
  `left` surévalué ; `numwant=-1` retourne moins de pairs que 200 ; magnet
  annonce `left=0` (seed-like). Sévérité : **haute** (wire + état ratio).
  Fix : compteurs `total_payload_*` réels (existants : `failed_bytes`,
  `redundant_bytes` à `torrent.rs:617-620`), `bytes_left()` via
  `total_wanted`, `num_want` depuis settings (+0 sur stopped), reset
  `m_stat`/`failed`/`redundant` à `start_announcing` (upstream
  `torrent.cpp:10757-10761` — absent de `do_resume`, `session.rs:3199-3205`).

- `engine.rs:957` — **`AnnounceOptions::default()` jamais alimenté par les
  settings** : `anonymous_mode`, `announce_ip`, `announce_crypto_support`/
  `in_enc_policy`, `report_redundant_bytes`, `ssrf_mitigation`,
  `min_announce_interval` sont tous figés aux défauts malgré les champs de
  `SessionSettings` (`settings.rs:100,219,255,262,310,454`). Conséquences :
  `anonymous_mode` ne retire rien sur les announces tracker ; `announce_ip`
  configuré ignoré ; `min_announce_interval` custom ignoré dans le clamp.
  Sévérité : **moyenne** (wire + privacy). Fix : construire
  `AnnounceOptions` depuis `slock(&session).settings` dans
  `spawn_announce`/`spawn_scrape`.

- `session.rs:8048-8077` — **burst `completed` non instrumenté** : les
  `TrackerAnnounce` du `BecameSeed` ne posent ni `updating` ni
  `sent_event` sur les slots (`dispatch_announce` les pose, lui). La
  réponse retombe sur `sent_event == 0` → `complete_sent` jamais latché
  (slot *et* `t.complete_sent`) → chaque announce périodique ultérieure
  renvoie `event=completed` (session.rs:8194) jusqu'à ce qu'une announce
  via `dispatch_announce` verrouille — soit **`completed` émis ≥2× par
  tracker** contre une fois upstream (`torrent.cpp:3523-3525` +
  `3860-3866`, plus `a.updating=true` au send `:3579`). Bonus : fenêtre
  où le slot paraît annonçable alors qu'un vol est en cours. Sévérité :
  **moyenne** (wire-visible, répété). Fix : réutiliser `dispatch_announce`
  (ou poser `updating`/`sent_event=1`) dans le burst `BecameSeed`, et
  garder le gate `complete_sent` par slot.

- `session.rs:8584-8598` + `tracker_client.rs:253-256,351` —
  **erreurs réduites à `String`** : `AnnounceError::Failure(msg)` /
  `HttpError::Status(code)` ne transportent ni `interval`/`min_interval`
  de la réponse d'échec ni le code HTTP. Upstream propage
  `resp.interval`/`resp.min_interval` → `a.failed(backoff, retry_interval)`
  (`http_tracker_connection.cpp:439-445` ; `tracker_manager.cpp:183`
  `interval ?: min_interval`) — Rust appelle `failed(backoff, ZERO)`
  (`session.rs:8592`). De plus `send_tracker_request` remet
  `next_announce`/`min_announce` à `now` (`torrent.cpp:3580-3581`) ;
  Rust conserve `min_announce` pendant le vol → après échec le retry reste
  borné par l'ancien `min_announce` (retries plus *lents* qu'upstream quand
  le tracker a publié `min interval`). Sévérité : **moyenne** (timing
  wire-visible). Fix : `AnnounceError::Failure { msg, interval,
  min_interval }` + `HttpError::Status` non-stringifié ; propager à
  `failed()` ; remettre `min_announce = Some(now)` au dispatch.

- `session.rs:8584-8598` — **`dont_try_again` (HTTP 410) absent** :
  upstream `tracker_request_error` → `ec == 410 http` →
  `m_trackers.dont_try_again(ae)` → `fail_limit = 1` → tracker jamais
  réessayé (`torrent.cpp:12934`, `tracker_list.cpp:62-65` — aussi sur
  scrape `:12962`). Rust : status stringifié, aucune mémorisation → retry
  infini sur un tracker mort-vivant. Sévérité : **moyenne-basse** (état +
  trafic). Fix : porter le code HTTP dans l'erreur, appliquer
  `fail_limit = 1` sur l'entry.

- — **`deprioritize_tracker` absent** : quand tous les endpoints d'un
  tracker échouent, upstream le déplace en fin de son tier
  (`tracker_list.cpp:48-60`, appelé `torrent.cpp:12938-12948`) → le prochain
  announce essaie un autre tracker du même tier. Rust conserve l'ordre →
  dans un tier multi-trackers, le mort reste en tête et encaisse chaque
  cycle. Sévérité : **moyenne-basse**. Fix : rotation intra-tier dans
  `TrackerList` (déplacer l'entry après les mêmes-tier quand `fails>0` sur
  tous ses endpoints).

- `session.rs:8307` — **`verified` marqué par une scrape réussie** :
  upstream ne pose `ae->verified` que dans `tracker_response` (announce,
  `torrent.cpp:3868`). Rust le pose aussi en `on_scrape_response` →
  `any_verified` vrai sans announce réussi → `use_dht_as_fallback` est
  désactivé à tort (`torrent.cpp:771`, `3010`). Sévérité : **basse-moyenne**
  (état). Fix : ne pas toucher `verified` dans `on_scrape_response`.

- `session.rs:975-978` + `torrent.rs:2734` — **`key` unique par session** :
  upstream `tracker_key()` = SHA1(ptr torrent ∥ storage idx ∥ ptr session)
  → stable dans le temps mais *distinct par torrent* (`torrent.cpp:5582`).
  Rust : un seul `i32` random pour tous les torrents → même clé sur tous
  les swarms → corrélation cross-torrent par les trackers. Sévérité :
  **basse-moyenne** (privacy/wire). Fix : clé par torrent (hash de
  `info_hash` + sel de session) stockée sur `Torrent`.

- `bitdaemon-core` `AddTorrentParams` — **pas de `trackerid` torrent-level** :
  upstream `req.trackerid = ae.trackerid.empty() ? m_trackerid : ae.trackerid`
  (`torrent.cpp:3318`), `m_trackerid` venant de `add_torrent_params`/
  resume data. Rust : `tracker_id` uniquement par entry ; le champ n'existe
  pas dans `AddTorrentParams` (grep `bitdaemon-core`: aucun `trackerid`).
  Sévérité : **basse** (rare). Fix : ajouter `trackerid` à
  `AddTorrentParams` + round-trip resume (`merged resume` — hors scope
  A04 pur, à coordonner avec A21).

- `tracker_list.rs:402` + `session.rs:8189,8212-8214` + `torrent.rs:2724` —
  **une seule info-hash annoncée par échange** : upstream envoie une
  requête par hash par endpoint (`for ih : all_versions`,
  `torrent.cpp:3353`, `3621`, `3702`) → hybride = announces v1 *et*
  v2-tronqué. Rust : 1 announce par entry, `info_hash` = v1 (ou v2 tronqué
  pour v2-only) ; le slot v2 est marqué `start_sent`/`complete_sent` sans
  jamais émettre (`tracker_list.rs:294-304`). → le sous-swarm v2-only reste
  invisible côté tracker. Sévérité : **moyenne** (wire). Fix : émettre une
  announce par hash présent (ou séquencer v1→v2 sur les réponses), avec
  `sent_event` par slot.

- `session.rs:3255-3267` — **`stopped` envoyé sans `can_announce`/`fail_limit`
  ni `stop_tracker_timeout`** : `announce_stopped_collect` vise tout tracker
  ayant vu `started`. Upstream conserve `can_announce` dans la boucle
  (`torrent.cpp:3629`) — les trackers `fails ≥ fail_limit` sont sautés — et
  supprime tout `stopped` si `stop_tracker_timeout ≤ 0`
  (`torrent.cpp:3239-3245`). Le timeout du `stopped` est aussi borné à 5 s
  upstream vs 15 s/phase ici. Sévérité : **basse** (un datagramme de trop
  sur des trackers déjà morts). Fix : gate `can_announce`-like dans le
  collecteur + respecter `stop_tracker_timeout` (skip + timeout).

- `engine.rs` (absence) — **pas de queue `max_concurrent_http_announces`** :
  chaque `TrackerAnnounce` → `tokio::spawn` immédiat. Upstream plafonne à
  50 HTTP conns en vol et file le reste, high-priority devant
  (`tracker_manager.cpp:234-260`, `~280-335`). Au démarrage d'une session
  chargée : burst HTTP non borné (et `net.num_queued_tracker_announces`
  reste 0 — voir compteurs). Sévérité : **basse-moyenne** (ressource).
  Fix : sémaphore `max_concurrent_http_announces` + file FIFO avec
  front-insert pour `high_priority`.

- `engine.rs:981,1160` — **timeouts UDP hardcodés** : `15 s` par phase
  (connect puis announce/scrape) vs `tracker_receive_timeout` (10 s) par
  paquet et `tracker_completion_timeout`/`stop_tracker_timeout` globaux
  upstream. Pour `event=stopped` upstream borne à `stop_tracker_timeout`
  (5 s) ; Rust attend jusqu'à 30 s. Sévérité : **basse-moyenne**. Fix :
  lire les settings dans `spawn_announce`/`spawn_scrape`.

- `udp_tracker_connection` upstream vs `announce_udp` — **pas de cache
  `connection_id`** : upstream réutilise le `connection_id` par adresse dans
  `m_connection_cache` (`udp_tracker_connection.cpp:704-707` et la mise en
  cache à la réponse connect). Rust refait connect→announce à chaque cycle
  → +1 RTT/+1 datagramme par announce (légal BEP-15, plus verbeux).
  Sévérité : **basse** (perf wire). Fix : cache `connection_id` par
  endpoint avec TTL (~60 s upstream).

- `session.rs:8485-8499` + `engine.rs:1114-1119` — **pairs dict :
  `peer_ids` ignorés, hostnames résolus sans gate proxy** : upstream saute
  `i.pid == m_peer_id` (`torrent.cpp:3920`) et ne résout les `hostname` que
  si `!proxy_hostnames || is_ip_address || no proxy ||
  !proxy_peer_connections` (`torrent.cpp:3957-3960`) ; la résolution passe
  par `abort_on_shutdown`. Rust résout toujours via le resolver injecté et
  ignore les `peer_id` de dict. Sévérité : **basse** (privacy conditionnelle
  + edge self-connect). Fix : gate `proxy_hostnames`/`proxy_peer_connections`
  sur `hostname_peers` ; comparer `peer_id` avant `add_swarm_peer`.

- `session.rs:8619-8624` — **`vote_external` auto-attribué** :
  `vote_external(ip, SOURCE_TRACKER, &ip)` passe l'IP annoncée comme source ;
  upstream passe l'IP du tracker contacté (`torrent.cpp:3833-3836` — permet
  à `ip_voter` de détecter un votant qui se contredit). Conséquence : chaque
  IP externe rapportée compte comme son propre votant → le vote par source
  diverge. Sévérité : **basse**. Fix : transporter l'adresse distante du
  tracker dans `TrackerResponse`/`AnnounceDone` jusqu'ici.

- `session.rs:8483` + `tracker_list.rs:309-313` — **`warning message` non
  mémorisé** : upstream `a.message = warning_message` au succès
  (`torrent.cpp:3874`) → visible dans le statut tracker (`last_error` champ
  message). Rust fait `set_message("")` avant de lire le warning → le
  message d'endpoint reste vide. Sévérité : **cosmétique** (status). Fix :
  stocker `r.warning_message` sur l'endpoint.

- `alerts.rs:196-214` + `session.rs:8565-8571,8599-8608` — **champs
  d'alertes divergents** : `tracker_error_alert` upstream porte
  `times_in_row` (= `a.fails`), `error` (`error_code` — permet le
  410-check), `op`, `failure_reason` ; le warning n'a pas de count. Rust
  : `TrackerError { url, error: String }` (count/op/code perdus) et
  `TrackerWarning { times_in_row: 1 }` codé en dur. Sévérité : **basse**
  (surface d'alerte + observabilité). Fix : porter `fails` + code erreur
  structuré dans `TrackerError`, retirer `times_in_row` de Warning ou le
  câbler correctement.

- `engine.rs:969-986,1054-1058` — **une seule adresse résolue tentée** :
  `resolve()`/`resolve_for_socket()` prennent `.next()` → le premier A/AAAA
  record. Upstream `http_connection`/`udp_tracker_connection` itèrent tous
  les endpoints résolus (`on_filter` + retry connect). Un tracker dont le
  premier A est mort échoue l'announce au lieu de basculer. Sévérité :
  **moyenne-basse** (fiabilité wire). Fix : itérer la liste résolue au
  connect.

- `engine.rs:958-1106` — **mitigation SSRF incomplète** : le check
  `has_tracker_query_string` est porté (`tracker_client.rs:223-227`) mais la
  jambe *endpoints* upstream — jeter les adresses loopback résolues quand le
  path n'est pas `/announce` (`http_tracker_connection.cpp:312-350`) — est
  absente : `filter_tracker_addr` (`engine.rs:3658`) n'applique que
  l'ip_filter. Un announce URL vers `127.0.0.1/…` n'est bloqué que par la
  query-string rule. Sévérité : **moyenne** (sécurité). Fix : vérifier les
  IPs résolues (loopback/link-local) quand `ssrf_mitigation` et path ∉
  `/announce`.

- `engine.rs:1049-1097` — **HTTPS tracker : pas de pinning `ssl_ctx`
  torrent, `auth` URL ignorée** : upstream `req.ssl_ctx = m_ssl_ctx` pour les
  torrents SSL (`torrent.cpp:3446-3448` — cert tracker signé par le cert du
  torrent) et `req.auth` → `Authorization: Basic`. Rust utilise les racines
  OS (`tracker_tls_config`, `engine.rs:43`) et `c.auth` n'est jamais
  converti en header. Sévérité : **basse** (SSL torrents = scope A23 ; auth
  rare). Fix : plomber `ssl_ctx`/auth via `AnnounceParams`.

- `engine.rs` + `announce_params` — **`.i2p` non géré** : une URL
  `http://….i2p` passe `tracker_protocol_supported` puis tente une
  résolution DNS (`resolve`, `engine.rs:3670`) — fuite ou échec selon le
  resolver injecté. Upstream route via `i2pconn` (SAM) et `req.kind |= i2p`.
  Rust n'a pas de SAM — le marqueur `params.i2p` existe (`i2p_peers` parsés)
  mais rien ne le positionne. Sévérité : **basse** (privacy conditionnelle).
  Fix : détecter le TLD `.i2p` → `params.i2p = true` + refus poli sans
  resolver i2p.

- `engine.rs` — **`proxy_tracker_connections` ignoré** : `spawn_announce`/
  `spawn_scrape` passent par `self.connector` (le connector pair, injecté)
  sans consulter `settings.proxy_tracker_connections` ; UDP passe par la
  `datagram` injectée (qui peut être socks5-wrappée via `apply_udp_proxy`,
  `engine.rs:556-607` — mais gâtée par `settings.proxy.dht`, pas tracker).
  Upstream : `ps.proxy_tracker_connections ? &ps : nullptr`
  (`http_tracker_connection.cpp:251-253`). Conséquence : proxy SOCKS
  configuré pour trackers ne s'applique pas au TCP tracker. Sévérité :
  **basse-moyenne** (privacy/ADR-0008). Fix : appliquer le proxy tracker
  sur le connector (le connector injecté peut déjà le faire — documenter ou
  honorer le setting).

- `torrent.rs:8802` — **`Torrent::announce_with_tracker` mort** : jamais
  appelée (seul `force_reannounce_url` → `force_announce` est utilisé) ; son
  reset de `next_announce` ne pose pas `triggered_manually` ni le clamp
  `min_announce`. Sévérité : **cosmétique** (dead code) — supprimer ou
  router via `trigger()`.

- `session.rs:6783-6836` — **`force_announce(Some(i))` ignore
  `m_announce_to_trackers`** : upstream `announce_tracker_now` retourne si
  `!m_announce_to_trackers` (torrent.cpp:3602) — le gate `auto-manage` des
  announces. Rust ne teste que `is_paused` + `can_announce`. Sévérité :
  **basse**. Fix : ajouter `t.announce_trackers` au gate.

- `counters.rs:338-347` — **compteurs `net.sent_tracker_bytes` /
  `net.recv_tracker_bytes` jamais incrémentés** : les variants existent mais
  aucun `inc` ne les touche (grep workspace : zéro usage hors `counters.rs`).
  Upstream compte `sent_bytes(url+100)`/`received_bytes(body+headers)` HTTP
  et `sizeof+28` UDP (`http_tracker_connection.cpp:268,417`,
  `udp_tracker_connection.cpp:517,554,772` ; `tracker_manager.cpp:222-231`).
  Sévérité : **basse** (stats). Fix : incrémenter dans `announce_http`/
  `announce_udp`/`scrape_*` et remonter via `AnnounceDone` ou les counters
  session.

- `session.rs:8566-8571` — **`times_in_row` warning fixe à 1** : upstream
  ne porte pas de count sur le warning (c'est `tracker_error_alert` qui a
  `times_in_row`) — le champ Rust est soit à retirer, soit à alimenter d'un
  compteur de warnings consécutifs. Couvert par le point alertes ci-dessus.

## Fichiers du fix

Fichiers Rust que les corrections toucheront (planification des vagues) :

- `crates/bitdaemon-net/src/http_conn.rs` — timeout read/completion,
  `User-Agent`, boucle redirect cross-origin (surface reconnect).
- `crates/bitdaemon-net/src/http_tracker.rs` — `AnnounceOptions` depuis
  settings (helpers), `trackerid`/auth/`ip` plumbing inchangé à l'intérieur.
- `crates/bitdaemon-net/src/tracker_client.rs` — `AnnounceError` portant
  `interval`/`min_interval`/status ; `AnnounceParams` (+`auth`, `ssl_ctx`,
  `num_want` réel) ; timeout par phase paramétré.
- `crates/bitdaemon-net/src/udp_tracker.rs` — cache `connection_id` par
  adresse ; timeouts settings-driven.
- `crates/bitdaemon-session/src/tracker_list.rs` — `deprioritize_tracker`,
  `dont_try_again`, `prioritize_udp_trackers` (pref `prefer_udp_trackers`),
  par-hash announce state.
- `crates/bitdaemon-session/src/torrent.rs` — `announce_params` compteurs
  réels (`uploaded`/`downloaded`/`left`/`corrupt`/`redundant`/`num_want`),
  key par torrent, `announce_trackers` gate, suppression/`trigger`-ify de
  `announce_with_tracker`.
- `crates/bitdaemon-session/src/session.rs` — burst `completed`
  instrumenté, `failed()` avec interval propagation, 410/`deprioritize`,
  `stopped` gates, `verified` scrape, `vote_external` source_ip, alertes
  `TrackerError`/`TrackerWarning` payload, `is_i2p` flag announce.
- `crates/bitdaemon/src/engine.rs` — `AnnounceOptions` depuis settings,
  timeouts (completion/receive/stop), multi-addr resolve, proxy tracker,
  queue `max_concurrent_http_announces`, redirect reconnect, `tracker_ip`
  dans `AnnounceDone`, SSRF endpoint leg, hostname_peers gate.
- `crates/bitdaemon-session/src/alerts.rs` — payload `TrackerError`
  (`times_in_row`, code, op), `TrackerWarning` (retirer ou câbler).
- `crates/bitdaemon-core/src/` + resume — `AddTorrentParams.trackerid`
  (coord. A21).

## Non-bugs tranchés

- **Socket UDP partagée + demux par `transaction_id`** — upstream dédie une
  socket par `tracker_connection` ; Rust envoie depuis la socket datagramme
  injectée et route les réponses par action+tid (`tracker_client.rs:31-94`,
  `engine.rs:651`). Justification : ADR-0008 (aucun sous-système ne bind de
  socket ; les transports sont injectés — tunnels anonymes OnionBit). Le
  routage par tid est équivalent au demux upstream (`udp_socket` route aussi
  par tid) ; le port source diffère (partagé avec DHT/uTP) — visible sur le
  fil mais légal BEP-15.
- **Un endpoint logique au lieu d'un par listen socket** —
  `refresh_endpoint_list` upstream duplique chaque tracker par socket
  d'écoute (multi-homing v4/v6) ; Rust a un endpoint synthétique unique —
  conséquence directe d'ADR-0008 (la session ne connaît pas les sockets).
  Accepté : v2 des `ipv4`/`ipv6` announce params reste vide (upstream ne les
  envoie que pour les torrents `priv()` de toute façon).
- **Pompe par tick au lieu de `deadline_timer`** — upstream arme
  `m_tracker_timer` sur le `max(next_announce, min_announce)` le plus proche
  (`torrent.cpp:10560-10660`) ; Rust scanne `tracker_announce_due` à chaque
  tick engine (~`tick_interval`, `engine.rs:725`). Sémantique équivalente
  sous `can_announce` (+1 s slack identique), granularité de poll.
- **`trigger_entry` filtre `ep.enabled`** — upstream `trigger_announce`
  écrit aussi sur les endpoints désactivés (`torrent.cpp:4144-4162`) ;
  inerte puisque `can_announce` les rejette de toute façon — écart neutre.
- **`udp_announce` (variante non-demux) non utilisée par l'engine** — gardée
  pour les tests sim (`test_socks.rs:401`) ; l'engine utilise
  `announce_udp`+demux (`engine.rs:975`). Pas une divergence de chemin live.
- **`min_interval = 60` fixe sur UDP** — identique à upstream
  (`udp_tracker_connection.cpp:574`), conforme.
- **`min interval` scrape non clampé** — upstream clamp `resp.interval`
  (announce *et* scrape, `http_tracker_connection.cpp:433`) ; le champ
  interval d'une scrape n'est pas consommé côté session — neutre.
- **`completed` burst cible uniquement les trackers `started`** — upstream
  `tracker_list::completed` arme tous les slots sans `complete_sent` y
  compris jamais-annoncés ; ceux-là partent en `started` via le picker
  normal (leur `next_announce` est armée `now`→due au tick suivant dans les
  deux implémentations). Écart de timing d'un tick, pas de sémantique.
- **`TrackerAnnounce` pour ws/wss** — `ws_tracker::announce_ws` (WebTorrent
  JSON) est une implémentation dédiée avec clamp
  `min_websocket_announce_interval` (`engine.rs:1009-1034`) — protocole
  distinct de BEP-3, hors champ de comparaison bit-exacte.

## Résultat

### Déviations corrigées

- **Timeouts I/O absents** — `tracker_completion_timeout`/
  `tracker_receive_timeout`/`stop_tracker_timeout` armés sur les
  échanges HTTP et UDP ; un tracker muet ne fige plus `updating`.
- **Compteurs announce approximés** — `uploaded`/`downloaded`/
  `corrupt`/`redundant` issus des compteurs réels
  (`report_true_downloaded`), `left` = `bytes_left` corrigé pièce de
  queue, `num_want` réglable, `key` par torrent, `trackerid`
  echo+fallback `m_trackerid`.
- **Machine announce par hash** — slots `(endpoint, v1|v2)` comme
  `announce_state` upstream ; hybrides annoncent les deux versions,
  `stopped` ne touche que `start_sent`, burst `completed` latched
  une fois par version.
- **Sémantique d'échec** — `retry_interval` des réponses `failure`
  honorée au-dessus du backoff quadratique, 410 → `dont_try_again`,
  `deprioritize` quand tous les slots meurent, cascade failover vers
  le tier suivant ; `tracker_error_alert` porte
  `times_in_row`/`status_code`/`op` (`TrackerOp` = `operation_t`).
- **Transport** — `User-Agent` + `Authorization`, redirects
  cross-origin re-dialés, legs endpoints dans l'ordre upstream
  (resolve-all → SSRF → filter → boucle par adresse), retries UDP par
  adresse, cache `connection_id` (`udp_tracker_token_expiry`),
  compteurs wire bytes `sent/recv_tracker_bytes`, queue
  `max_concurrent_http_announces` (prioritaires en tête,
  `num_queued_tracker_announces`).
- **Réponses** — peers `hostname` filtrés par settings proxy, vote
  `external ip` par `tracker_ip` résolu, `trackerid` persisté +
  `trackerid_alert`, `warning message` → `tracker_warning_alert`,
  réponse announce rafraîchit les champs scrape, scrapes par version.

### Tests ajoutés

- `crates/bitdaemon-session/tests/test_tracker_a04.rs` — 13 cas :
  compteurs réels, hybride v1+v2, `failed_version` ciblé, latche
  `completed`, gates `stopped`, retry_interval, 410, deprioritize,
  cascade tiers, trackerid echo/fallback, scrape par version,
  payload `tracker_error_alert`.
- `test_tracker_client.rs` — `udp_conn_id_reused_within_ttl`,
  `announce_url_trackerid_and_ssrf` (SSRF args tracker + echo
  trackerid).
- `test_tracker_list.rs`/`test_announce.rs`/`test_scrape.rs`/
  `test_alert_types.rs`/`test_status.rs`/`test_recheck.rs`/
  `test_connect_pump.rs` — fixtures migrées sur les signatures
  versionnées.

### Livraison

- Commit apply : `bea03c0`
  `fix(trackers): upstream-fidelity apply of audit A04`
  (branche `audit/04-trackers`, worktree `LibtorrentRust-a04`)
- PR : https://github.com/laurentgeynet-ux/BitDeamonRust/pull/5
- Fragment changelog : `docs/audits/changelog/A04.md`
  (agrégé au prochain passage de `_aggregate_changelog.py`)
