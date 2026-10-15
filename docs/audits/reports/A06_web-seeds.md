# Audit A06 — rapport d'analyse (V2)

Ré-audit après apply de la V1 (`a084101`, PR #3). Périmètre inchangé :
`web_peer_connection` / `web_connection_base` (BEP-19 url seeds ;
`http_seed_connection.cpp` n'existe pas en 2.1, BEP-17 retiré — le
merge `http_seeds` → `web_seeds` Rust est conforme) + legs torrent
(`maybe_connect_web_seeds`, `retry_web_seed`, `connect_to_url_seed`,
`connect_web_seed`, `handle_redirect`, `received_invalid_data`).

Baseline upstream : `e7381c7bc` (`D:\Projet\libtorrent\libtorrent`,
lecture seule). Modèle Rust : pull — `Session::tick` →
`Torrent::next_web_seed_blocks` émet `SessionOut::WebSeedFetch`,
`engine.rs::spawn_web_seed` fait dial+GET+parse dans une tâche,
`on_web_seed_block`/`on_web_seed_error`/`on_web_seed_done`
réinjectent. État par-URL dans `Torrent::web_seed_state` (`WebSeed`).

## État V1 → V2

Corrigé et vérifié conforme cette passe : validation
`Content-Range`/`Content-Length` + `disable` (`web_seed.rs:483-507`),
`normalize_web_seed_url` appelée (`engine.rs:1414`), timeout
`urlseed_timeout` par fetch + sweep `check_web_seed_timeouts`
(`engine.rs:1671`, `torrent.rs:6004`), SSRF + `ip_filter` +
`no_local_ips` (`engine.rs:1599-1648`), proxy web-seed câblé
(`engine.rs:1425-1486`, `web_seed_dial`), backoff `retry_ms` /
`disabled` / `removed` / `NameLookup` (`torrent.rs:5937-5988`),
`prefer_contiguous_blocks` + sizing keep-alive (`torrent.rs:5657-5670`),
pure-pad sans GET (`engine.rs:1536-1541`), gate `files_checked` +
caps connexions (`torrent.rs:5519-5601`), compteurs payload +
`report_web_seed_downloads` (`torrent.rs:5864-5867`), drain
`remove_url_seed` (`torrent.rs:7305-7340`), ban-on-hash-fail
(`torrent.rs:3636-3647`).

## Verdicts checklist

- **Range requests : pièces complètes vs blocs, `block_size`
  alignment, coalescing contigu** — verdict : **conforme** (une
  déviation latente sous D1). `plan_piece_request` mappe via
  `map_block`, pads `got_get:false` sans GET — `web_seed.rs:201-285` ;
  GET par `file_request` non-pad dans l'ordre `m_file_requests`,
  `Range: bytes=start-end` inclusif — `web_seed.rs:291-360` ; blocs
  contigus fusionnés en une `PeerRequest` franchissant les pièces,
  cap `urlseed_max_request_bytes` — `torrent.rs:5701-5741` ;
  `prefer_contiguous_blocks = max(max(ka?1:4MiB, piece_length),
  urlseed_max_request_bytes)/block_size` — `torrent.rs:5661-5670` =
  `web_peer_connection.cpp:67-74`. Détail upstream confirmé :
  `urlseed_max_request_bytes` est une *préférence* de picking, pas un
  cap dur — `request_merger` n'a pas de borne (le nôtre borne le merge
  à 16 Mio, différence bénigne). `urlseed_pipeline_size` est mort
  upstream aussi (aucun usage `src/`) — non-bug.

- **Erreurs : HTTP non-200, redirect loops, content-length mismatch,
  EOF mid-piece → url_seed_alert + seed marqué** — verdict :
  **déviation**. Conforme : redirect traité avant le statut générique
  (`web_seed.rs:465-477` = `is_redirect` avant `is_ok_status`,
  `web_peer_connection.cpp:919-923`) ; 206 sans `Content-Range` ou
  non-206 sans `Content-Length` → `InvalidRange` → `Disable` =
  upstream `disable(errors::invalid_range / no_content_length)`
  (`web_peer_connection.cpp:524-555`) ; range mismatch → `Retry` =
  upstream `disconnect(invalid_range, peer_error)` qui re-trie avec
  backoff ; `Retry-After` parsé et propagé (`:475`). Déviant : corps
  trop court → `Retry`+30 s là où upstream EOF mid-body = sévérité
  `normal` sans backoff (D3) ; `Location` vide → boucle de 64 →
  `Retry` vs `disable(missing_location)` (D5) ; taxonomie d'alertes
  réduite à `UrlSeed` seul (D7) ; timeouts → backoff + UrlSeed là où
  upstream poste `peer_disconnected` sans backoff (D2).

- **Retry : backoff par seed, `retry_web_seed`, bannissement
  temporaire** — verdict : **déviation**. Conforme : `retry_ms` par
  seed + gate `retry_ms > now` (`torrent.rs:5611`) = `web->retry` ;
  `NameLookup` → `web_seed_name_lookup_retry` ; `Disable`/`Remove`
  mappés sur les bonnes gates (`engine.rs:1339-1408` ↔
  `torrent.cpp:6815-6916` : idna→disable ✓, parse→remove ✓,
  proto→disable ✓, hostname→remove ✓, port 0→remove ✓,
  port_filter→disable ✓). Déviant : backoff sur les erreurs
  mid-transfer qui n'en ont pas upstream (D3), `Retry-After` < 30 s
  honoré sans le plancher `urlseed_wait_retry` (D4), échec de
  résolution du *proxy* → retry 1800 s vs `disabled` upstream (D9),
  bannissement temporaire : upstream banisse via `peer_info.banned`
  (réversible via `unban`), le nôtre `disabled` est terminal — même
  effet pratique, note dans D10.

- **Attribution : erreurs/compteurs par URL** (cohérence du fix
  `on_web_seed_error(url)`) — verdict : **conforme**.
  `WebSeedDone{ih,url}` transporte l'URL de bout en bout ;
  `st.outstanding`/`downloaded`/`last_error` sous la clé URL sur les
  deux legs (`torrent.rs:5839-5860,5945-5988`) ; `abort_download(b,
  Some(key))` vérifie l'attribution du downloader (`torrent.rs:5967`)
  — un bloc re-piqué par un autre n'est pas décrémenté à tort ;
  `web_seed_statuses()` snapshot par entrée (`torrent.rs:6056`) ;
  payload sous `report_web_seed_downloads` (`torrent.rs:5864` ↔
  `web_peer_connection.cpp:59-60` + `update_report_web_seed_
  downloads`). Le masque d'alertes est appliqué dans `post`
  (`alerts.rs:1699-1705`) = le gate `should_post` upstream — le
  `UrlSeed` inconditionnel en surface est conforme.

- **Request pacing : `web_seed_blocks_per_tick`, connection reuse vs
  reconnect** — verdict : **conforme** (au modèle pull près, note D11).
  Batch ≤ `web_seed_blocks_per_tick` (4) blocs par tick par torrent
  (`session.rs:6085,6247`) ≈ `desired_queue_size` rate-based upstream
  (`urlseed_pipeline_size` mort des deux côtés) ; reuse par base à
  l'intérieur d'un batch avec vrai pipelining (tous les GET écrits en
  une fois, réponses lues en série — `web_seed.rs:456-461`,
  `engine.rs:1532-1601`) ; `Connection: close` → conn retirée +
  `supports_keepalive=false` + refcount drop (`engine.rs:1687-1689`,
  `torrent.rs:5896-5901`) = `m_received_disconnect` +
  `~peer_connection`. Caveats : une tâche = une connexion par batch
  (upstream garde la conn entre les ticks — handshake TCP par batch
  en plus, wire-visible mais sans effet sémantique) ; ≤ 1 seed
  démarré par tick/torrent vs la boucle complète upstream (D11).

- **Pseudo-peer `web_seed_t` : download queue partagée, legs upload
  N/A** — verdict : **déviation** (partielle). Conforme : clé picker
  par seed + `web_seed_outstanding` — jamais de double-request ;
  `mark_as_writing` + `DiskWrite` = le chemin `Piece` ; pas de leg
  upload (`m_upload_only` upstream implicite) ; `busy` = le slot
  `peer_info.connection || resolving`. Déviant : le timeout de la
  session n'aborde pas la tâche engine — zombie qui livre des blocs
  comptés `redundant_bytes` et dont le `on_web_seed_done` tardif peut
  écraser `busy`/`refcounted`/`supports_keepalive` d'un successeur
  (D2) ; `have_files`/`ephemeral`/`interesting` par fichier absents —
  redirects ré-appris par batch, pas de seeds éphémères enregistrés,
  non-certain hash-fail sans effet (D6/D10).

## Déviations

- `crates/bitdaemon/src/engine.rs:1671` — `fetch_block(s, &wreq, 4 *
  1024 * 1024)` : cap de réponse codé en dur à 4 Mio alors que le
  merge peut produire des `FileRequest` jusqu'à
  `urlseed_max_request_bytes` (16 Mio défaut, `torrent.rs:5702`).
  Upstream n'a aucun cap de réponse — `urlseed_max_request_bytes` ne
  borne que `prefer_contiguous_blocks` (`web_peer_connection.cpp:
  67-74`). Avec les défauts (`web_seed_blocks_per_tick=4` × 16 Kio)
  inatteignable ; dès que le knob monte ou `block_size` croît, un
  span >4 Mio échoue en `HttpError` → `Retry` à l'infini sur une
  requête que NOUS avons générée. Sévérité : wire-visible (latent).
  - **sketch** : `let cap = wreq.files.iter().filter(|f|
    f.got_get).map(|f| f.length as usize + 16 * 1024).max().
    unwrap_or(64 * 1024)` passé à `fetch_block` (cap par réponse =
    corps attendu + marge headers) — ou plafonner le merge lui-même.
    Test : serveur duplex répondant `206` + corps 5 Mio → succès ;
    corps > attendu → erreur.

- `crates/bitdaemon-session/src/torrent.rs:5999-6049` +
  `engine.rs:1671-1682` — timeout d'inactivité : (a) `retry_ms =
  now + urlseed_wait_retry` alors que `disconnect(timed_out_
  inactivity, bittorrent)` upstream est sévérité `normal` → **pas**
  de bump `retry` (`peer_connection.cpp:4988-4996` + `disconnect`
  default `normal` ; le bump n'arrive que si `error >= failure`) ;
  (b) `Alert::UrlSeed "web seed timed out"` au lieu de
  `peer_disconnected_alert` (`peer_connection.cpp:4486-4488`) ; (c)
  la tâche engine n'est pas tuée — le fetch continue en zombie,
  livre des blocs → `redundant_bytes`, et son `on_web_seed_done`
  tardif remet `busy=false`/`supports_keepalive=false`/drop le
  refcount d'un éventuel successeur (`torrent.rs:5892-5912` n'est
  pas génération-gardé). Sévérité : état + wire (pénalité 30 s
  injustifiée + race zombie).
  - **sketch** : (a) supprimer le `st.retry_ms.max(now + wait)` du
    leg timeout (le seed re-pique au tick suivant, = upstream) ;
    (b) poster `Alert::PeerDisconnected`-équivalent (ou variante
    dédiée) au lieu de `UrlSeed` ; (c) engine : stocker le
    `JoinHandle` par `(ih,url)` (`self.tasks` existe déjà) et
    `abort()` au timeout — ou passer une génération dans
    `WebSeedDone` et ignorer les done des générations périmées dans
    `on_web_seed_done`.

- `crates/bitdaemon/src/engine.rs:1574-1593,1674-1682,1812-1820` —
  erreurs transport mid-transfer (`WebSeedDialError::Io` post-
  connect inclus côté lecture, `fetch_block` Io/Timeout,
  `ShortBody`, EOF-entre-réponses → `Http{status:-1}`) →
  `WebSeedBackoff::Retry` = +30 s. Upstream : `disconnect(e,
  op_sock_read)` = sévérité `normal` → **aucun** backoff, re-dial au
  tick suivant (`peer_connection.cpp:4336-4339,6143` ; seuls les
  échecs de *connect* sont `failure` → +30 s, `:4274` — notre
  `WebSeedDialError::Io` au dial y correspond déjà). Sévérité :
  wire-visible (sous-débit sur seed flaky : 30 s de trou au lieu de
  reconnexion immédiate, et EOF mid-batch HTTP/1.0 pénalisé).
  - **sketch** : ajouter `WebSeedBackoff::Now` (release des blocs,
    `retry_ms` inchangé) pour les legs `fetch_block`
    `Transport`/`ShortBody`/`Timeout` mid-batch ; garder `Retry`
    pour les échecs de dial. Attention à l'EOF-entre-réponses :
    `recv_response` sur stream fermée renvoie un parser vide →
    `status()==-1` → aujourd'hui classé `Http{-1}` ; le classer
    `Now` aussi (upstream : EOF = déconnexion normale, la queue
    survit).

- `crates/bitdaemon/src/engine.rs:1789-1800` +
  `torrent.rs:5985-5986` — `Retry-After` honoré tel quel :
  `Header(d)` → `retry_ms = now + d` même si `d < 30 s`. Upstream
  `handle_error` fait `retry_web_seed(retry_after)` **puis**
  `disconnect(status, failure)` qui impose un second
  `retry_web_seed(urlseed_wait_retry)` → effectif `max(d, 30 s)`
  (`web_peer_connection.cpp:619-628`, `torrent.cpp:11773-11774` +
  leg `error >= failure` `:276-280`). Sévérité : état (politesse —
  on peut revenir avant le 30 s du serveur).
  - **sketch** : dans le bras `Header(d)` de `retry_web_seed`,
    `let eff = d.max(s.urlseed_wait_retry)` puis `retry_ms = now +
    eff`. Test : 503 + `Retry-After: 5` → `retry_ms == now+30 s` ;
    `Retry-After: 90` → `now+90 s`.

- `crates/bitdaemon/src/engine.rs:1726-1730` — `Location` vide :
  `resolve_redirect_location(&url, "")` renvoie le referrer →
  self-redirect → boucle jusqu'à `num_redirects > 64` → `Retry`.
  Upstream `handle_redirect` : `location.empty()` →
  `disable(errors::missing_location)` (`web_peer_connection.cpp:
  658-662`). Sévérité : wire-visible (seed en 302-vide martelé
  64×/batch au lieu d'être désactivé).
  - **sketch** : dans le bras `Err(Redirect{location,..})`, avant
    `resolve_redirect_location`, `if location.is_empty()` →
    `report!(done, reqs[i..], "missing location",
    WebSeedBackoff::Disable, saw_close)`. Test : 302 avec header
    `Location:` vide → `st.disabled` + alerte.

- `crates/bitdaemon/src/engine.rs:1487-1786` +
  `torrent.rs:255-305` — état redirect/éphémère non persistant : les
  maps `redirects`/`bases`/`base_addrs` meurent avec la tâche —
  chaque batch re-suit la même chaîne 302, `WebSeed.ephemeral`
  n'est jamais posé `true` (`torrent.rs:5567`), `interesting`
  jamais remis à `false` (upstream `m_web->have_files.none_set()` →
  `interesting=false`, et `have_files.clear_bit(file)` sur le seed
  d'origine — `web_peer_connection.cpp:723-757` + `on_connected`
  `:127-135`). Conséquences : GET+302 superflu par batch (wire) ;
  `no_local_ips` non posé quand l'origine a été jointe en
  `proxy_hostnames` (`resolved=None` → `resolved_origin_global`
  false, `engine.rs:4211-4219` vs upstream `is_global(remote() =
  proxy publique)` → flag posé) ; les seeds éphémères n'apparaissent
  ni dans `url_seeds()` ni dans les statuses (upstream les liste)
  ; leur état `retry`/`disabled` propre est inexistant (un redirect
  mort pénalise le seed d'origine). Sévérité : état/perf + SSRF
  edge.
  - **sketch** : monter `redirects: HashMap<file_index,
    RedirectTarget>` et les flags `ephemeral`/`no_local_ips` dans
    `WebSeed` (torrent.rs), renvoyés par `WebSeedDone` ; enregistrer
    les bases redirect comme entrées `web_seed_state` éphémères
    (jamais persistées — `write_resume_data` `torrent.rs:2391`
    devra les filtrer, upstream `:11746` `!i->ephemeral`). Fix de
    taille moyenne ; cœur du modèle par-fichier manquant (voir D10).

- `crates/bitdaemon-session/src/session.rs:8495-8499` — taxonomie
  d'alertes : tout échec → `Alert::UrlSeed`. Upstream : gates
  connect-time postent `url_seed_alert` ✓ (`torrent.cpp:6821-6911`)
  ; `handle_error` (statut non-ok) → `url_seed_alert` **+**
  `peer_disconnected_alert` (`web_peer_connection.cpp:619-628`) ;
  `disable()` (invalid_range, missing_location…) → `peer_error_
  alert` (sévérité `peer_error`, `peer_connection.cpp:4479-4484`) ;
  EOF/reset/timeout → `peer_disconnected_alert`. Nos `UrlSeed` ne
  distinguent pas, et aucun `PeerError`/`PeerDisconnected` n'est
  émis pour un web seed. Le payload `"http {code}: {server}"` reste
  écarté de `"{code} {reason-phrase}"` (`to_string(status)+" "+
  m_parser.message()` — V1 déjà noté, toujours présent :
  `web_seed.rs:472-476` utilise `header("server")`). Sévérité :
  observabilité (clients filtrant par type d'alerte perdent le
  signal).
  - **sketch** : router par `WebSeedBackoff` — `Disable`/`Remove`
    → `PeerError` (+`UrlSeed` pour les gates connect-time,
    identifiables par un flag dans `WebSeedFailure`) ; transport
    mid-transfer → `PeerDisconnected` ; `Http`/`Header` → `UrlSeed`
    (conforme). Corriger le message : `format!("{status} {reason}")`
    via `parser.message()` (raison stockée côté parser — sinon
    status text canonique). Test : 404 → `UrlSeed` ; EOF mid-body →
    `PeerDisconnected` ; range invalide → `PeerError`.

- `crates/bitdaemon/src/engine.rs:1295` +
  `session_handle.rs:529` — source du proxy web-seed : `proxies.
  web_seed` n'est peuplé que par `set_web_seed_proxy()` ; upstream
  cette API est **NO-OP** et le seed utilise `m_ses.proxy()` =
  `settings_pack::proxy_*` (`session_handle.cpp:1040-1043`,
  `session_impl.cpp:937-940`). `apply_settings` ne recopie jamais
  `proxy_*` vers `proxies.*` (`session.rs:5240+` ne touche pas
  `self.proxies`) → un proxy configuré via `apply_settings_pack`
  n'atteint pas les web seeds (ni `proxies.peer`, d'ailleurs — à
  coordonner avec A24). Sévérité : wire-visible (feature morte par
  le chemin settings).
  - **sketch** : dans `Session::apply_settings`, reconstruire
    `proxies.peer/web_seed/dht` depuis `s.proxy_*` quand les champs
    changent (`update_proxy` upstream), garder `proxies.tracker`
    gatée par `proxy_tracker_connections` ; `web_seed` =
    `peer` upstream (même `proxy()`). À arbitrer : soit mapper,
    soit documenter `won't port` du settings-path (A24 tranche).

- `crates/bitdaemon/src/engine.rs:1430-1443` — échec de résolution
  du *proxy* → `WebSeedBackoff::NameLookup` (1800 s). Upstream
  `on_proxy_name_lookup` error → `web->disabled = true`
  (`torrent.cpp:6992-7003`) — le retry `web_seed_name_lookup_retry`
  ne vaut que pour le *host* (`:7095-7098`). Sévérité : état.
  - **sketch** : bras `Err(e)` du `resolve(&proxy.hostname…)` →
    `WebSeedBackoff::Disable` (ou variante `ProxyNameLookup`).
    Test : proxy injoignable au DNS → `st.disabled`.

- `crates/bitdaemon-session/src/torrent.rs:3630-3648` — hash-fail :
  bras certain (`known_bad && ban_web_seeds`) → `disabled` ✓
  (`peer_info.banned` → `connect_to_url_seed` disable,
  `torrent.cpp:6852-6864` — même état terminal, alerte `peer_banned`
  ok). Bras non-certain : `continue` → **rien**. Upstream
  `received_invalid_data` multi-fichier : `incoming_dont_have` sur
  le(s) fichier(s)/pièce touchés (`web_peer_connection.cpp:562-
  601`), ban seulement à `num_have_pieces()==0`, sinon le seed
  continue sur les autres fichiers ; chez nous le seed entier est
  re-sollicité sur les mêmes données à chaque hash-fail non-certain.
  Sévérité : wire-visible (re-fetch de poison).
  - **sketch** : dépend du `have_files` par seed (item redirect) —
    à fixer ensemble : bitfield fichiers par `WebSeed`, hash-fail
    non-certain → clear les bits des fichiers couvrant la pièce,
    `none_set()` → `interesting=false`. Note : upstream distingue
    `banned` (caché de `web_seeds()`) de `disabled` (listé) — notre
    fold ban→disabled garde l'URL listée ; écart tolérable à
    documenter, ou ajouter `st.banned` pour filtrer `url_seeds()`.

- `crates/bitdaemon-session/src/session.rs:6247` — ramp-up : ≤ 1
  seed dispatché par tick et par torrent ; upstream boucle tous les
  éligibles à chaque `maybe_connect_web_seeds`
  (`torrent.cpp:11019-11030`). Converge en N ticks pour N seeds —
  plus lent, pas incorrect. Sévérité : perf (mineur — à documenter
  ou boucler `next_web_seed_blocks` tant qu'il renvoie Some).

## Fichiers du fix

- `crates/bitdaemon/src/engine.rs` — cap `fetch_block`, split des
  classes d'erreur (`Now`/`Disable` proxy), `Location` vide,
  redirects persistants remontés, kill-abort des tâches, proxy
  résolution classée.
- `crates/bitdaemon-session/src/torrent.rs` — timeout sans backoff,
  `retry_web_seed` floor `urlseed_wait_retry`, `WebSeed` :
  `redirects`/`ephemeral`/`no_local_ips`/`have_files`/`banned`,
  `on_web_seed_done` génération-gardé, `write_resume_data` filtre
  éphémères, `url_seeds()` filtre banned.
- `crates/bitdaemon-session/src/session.rs` — routage alertes par
  classe (`PeerError`/`PeerDisconnected`/`UrlSeed`), boucle ramp-up.
- `crates/bitdaemon-session/src/alerts.rs` — variantes payload si
  nécessaire.
- `crates/bitdaemon-net/src/web_seed.rs` — `MissingLocation`,
  message `{status} {reason}` (`parser.message()`).
- `crates/bitdaemon-session/src/status.rs` — champs WebSeedStatus
  si exposés.
- Tests : `crates/bitdaemon-net/tests/test_web_seed_natpmp.rs`
  (fetch 5 Mio, Location vide, Retry-After), nouveau test session
  (timeout sans backoff, zombie-done ignoré, redirect persistant).

## Non-bugs tranchés

- **Modèle pull vs connexion persistante** — assumé (ADR-0008,
  transports injectés). Reconnect-par-batch = un handshake TCP de
  plus par batch : wire-visible, sans divergence d'état. Le vrai
  pipelining intra-batch (tous les GET écrits d'un coup) est
  conforme au comportement upstream.
- **`urlseed_pipeline_size`** — dead setting upstream (aucun usage
  `src/`) ; notre `web_seed_blocks_per_tick` est un knob équivalent
  assumé, pas un port du setting mort.
- **`retry_ms = max(...)` vs overwrite** — upstream `retry = now +
  retry` écrase ; nous prenons le max (jamais plus tôt qu'un backoff
  existant). Direction plus conservative, sans divergence
  observable majeure.
- **`is_upload_only`/`is_paused` gatés dans `next_web_seed_blocks`**
  vs upstream dans `connect_web_seed` après resolve — même effet
  observable (pas de fetch), ordre des gates différent mais
  invisible.
- **`web_seed_inflight` hors `--limit` des seeds éligibles** : la
  consommation de slot par seed éligible-connecté est reproduite
  (`torrent.rs:5586-5601`).
- **`ephemeral` absent de `url_seeds()`** : upstream `web_seeds()`
  les liste (filtre `banned`/`removed` seuls) — couvert sous
  déviation, pas non-bug.
