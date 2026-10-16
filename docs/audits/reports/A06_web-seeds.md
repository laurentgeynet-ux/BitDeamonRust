# Audit A06 — rapport d'analyse (V3, passe 3)

Re-audit à froid après les merges A01–A28 post-rapport-V2
(`1d3966c`). Périmètre inchangé : `web_peer_connection` /
`web_connection_base` (BEP-19 url seeds ; `http_seed_connection.cpp`
n'existe pas en 2.1, BEP-17 retiré — le merge `http_seeds` →
`web_seeds` Rust est conforme) + legs torrent
(`maybe_connect_web_seeds`, `retry_web_seed`, `connect_to_url_seed`,
`connect_web_seed`, `on_name_lookup`/`on_proxy_name_lookup`,
`handle_redirect`, `received_invalid_data`, `web_seeds()`).

Baseline upstream : `e7381c7bc` (`D:\Projet\libtorrent\libtorrent`,
lecture seule). Modèle Rust : pull — `Session::tick` →
`Torrent::next_web_seed_blocks` émet `SessionOut::WebSeedFetch`,
`engine.rs::spawn_web_seed` fait dial+GET+parse dans une tâche,
`on_web_seed_block`/`on_web_seed_error`/`on_web_seed_done`
réinjectent. État par-URL dans `Torrent::web_seed_state` (`WebSeed`).
Nouvelle couche depuis V2 : les fetchs passent par `ThrottledWs`
(quota `download_channel`/`upload_channel` + IP overhead, A28) —
auditée ici.

## État V2 → V3

- **D8 résolu** : `apply_settings` reconstruit `proxies.web_seed`
  depuis les clés `proxy_*` (`session.rs:6600-6611`,
  `proxy_from_settings` `:6681`), upstream `update_proxy` /
  `m_ses.proxy()`. Test protecteur : `test_settings_apply_a22.rs:54-65`.
- **D1–D7, D9–D11 toujours présentes** — refs rafraîchies ci-dessous.
- **Nouvelles déviations trouvées** : D12 (broken-pipe half-close),
  D13 (`restart_piece`/`restart_request` absents), D14 (pas de liste
  d'endpoints / failover multi-IP), D15 (`no_local_ips` tout-filtré →
  `NameLookup` au lieu de `disabled`), D16 (SOCKS4 admis dans
  `name_via_proxy`, exclu upstream).
- **Renfort de D6** : upstream efface `have_files[file_index]` sur
  *tout* statut non-ok (`web_peer_connection.cpp:911-912`), pas
  seulement sur redirect — chez nous un fichier 404 est re-pollé à
  chaque cycle de backoff, à l'infini.

## Verdicts checklist

- **Range requests : pièces complètes vs blocs, `block_size`
  alignment, coalescing contigu** — verdict : **conforme** (D1 latente
  sous). `plan_piece_request` mappe via `map_block`, pads
  `got_get:false` sans GET — `web_seed.rs` (test
  `plan_skips_pad_and_groups_redirects`) ; GET par `file_request`
  non-pad, `Range: bytes=start-end` inclusif ; blocs contigus
  fusionnés en `PeerRequest` franchissant les pièces, cap
  `urlseed_max_request_bytes` (`torrent.rs:6865-6906`) ; sizing
  `prefer_contiguous_blocks = max(max(ka?1:4MiB, piece_length),
  urlseed_max_request_bytes)/block_size` (`torrent.rs:6809-6834`) =
  `web_peer_connection.cpp:67-74`. `urlseed_max_request_bytes` reste
  une *préférence* upstream (pas de borne dans `request_merger`) ;
  notre borne de merge à `max_req` est plus stricte — bénigne.
  [Lentilles : Limites ✓, Cycle de vie ✓]

- **Erreurs : HTTP non-200, redirect loops, content-length mismatch,
  EOF mid-piece → url_seed_alert + seed marqué** — verdict :
  **déviation**. Conforme : redirect avant statut générique
  (`web_seed.rs:471-477` = `is_redirect` avant `is_ok_status`) ; 206
  sans `Content-Range` / non-206 sans `Content-Length` →
  `InvalidRange` → `Disable` = `disable(invalid_range /
  no_content_length)` (`web_peer_connection.cpp:524-555` via
  `get_range` `:947-953`) ; range mismatch → `Retry` (upstream
  `disconnect(invalid_range, peer_error)` :979,1009,1088 — backoff +
  `peer_error_alert` chez eux, `UrlSeed`+30 s chez nous, écart sous
  D7) ; `Retry-After` parsé et propagé (`web_seed.rs:481`) ; chunked
  de-framé par `HttpResponse::body()` (`http_conn.rs:126-131`).
  Déviant : corps court / EOF mid-body / erreurs mid-transfer →
  `Retry`+30 s là où upstream = sévérité `normal`, aucun backoff (D3) ;
  `Location` vide → boucle 64→`Retry` vs `disable(missing_location)`
  (D5) ; taxonomie d'alertes plate `UrlSeed` (D7) ; timeout → backoff
  + `UrlSeed` vs `peer_disconnected` sans backoff (D2) ; écriture
  EPIPE traitée en fatal vs demi-fermeture ignorée (D12). [Lentilles :
  Erreur/Disconnect ✗, Limites ✓]

- **Retry : backoff par seed, `retry_web_seed`, bannissement
  temporaire** — verdict : **déviation**. Conforme : `retry_ms` par
  seed + gate `retry_ms > now` (`torrent.rs:6758`) = `web->retry` ;
  `NameLookup` → `web_seed_name_lookup_retry` (`torrent.rs:12514` =
  `torrent.cpp:7095-7098`) ; `Disable`/`Remove` mappés sur les bonnes
  gates (`engine.rs:2263-2331` ↔ `torrent.cpp:6815-6916` : idna→disable
  ✓, parse→remove ✓, proto→disable ✓, hostname→remove ✓, port 0→remove
  ✓, port_filter→disable ✓, banned→disable ✓ via `penalize_peers`).
  Déviant : backoff +30 s sur erreurs mid-transfer sans fondement
  upstream (D3), `Retry-After` < `urlseed_wait_retry` honoré sans le
  plancher que le `disconnect(failure)` impose (D4), échec de
  résolution du *proxy* → 1800 s vs `disabled` (D9), endpoints tous
  filtrés par `no_local_ips` → 1800 s vs `disabled` (D15), ban ≠
  invisible dans `url_seeds()` (D10). [Lentilles : Erreur ✗, Parité
  alertes ✗]

- **Attribution : erreurs/compteurs par URL** — verdict : **conforme**.
  `WebSeedDone{ih,url}` de bout en bout ; `st.outstanding`/
  `downloaded`/`last_error` sous clé URL (`torrent.rs:7109-7152`) ;
  `abort_download(b, Some(key))` vérifie l'attribution du downloader
  (`torrent.rs:7131` — un bloc re-piqué ailleurs n'est pas décrémenté
  à tort) ; `web_seed_statuses()` snapshot par entrée
  (`torrent.rs:7221`) ; payload sous `report_web_seed_downloads`
  (`torrent.rs:7028-7030` ↔ `update_report_web_seed_downloads`) ;
  masque d'alertes appliqué dans `post` = gate `should_post`.
  [Lentilles : Compteurs ✓, Cycle de vie ✓]

- **Request pacing : `web_seed_blocks_per_tick`, connection reuse vs
  reconnect** — verdict : **conforme** (au modèle pull près, D11).
  Batch ≤ `web_seed_blocks_per_tick` (4) par tick par torrent
  (`session.rs:7691,7856`) ≈ `desired_queue_size` upstream
  (`urlseed_pipeline_size` mort des deux côtés) ; reuse par base à
  l'intérieur d'un batch avec vrai pipelining (tous les GET écrits en
  une fois — `engine.rs:2460-2590`) ; `Connection: close` → conn
  retirée + `supports_keepalive=false` + refcount drop
  (`engine.rs:2631-2634`, `torrent.rs:7060-7064`) =
  `m_received_disconnect` + `~peer_connection`. Caveats persistants :
  une tâche = une connexion par batch (upstream garde la conn entre
  les ticks — handshake TCP de plus par batch) ; ≤ 1 seed dispatché
  par tick/torrent vs la boucle complète (D11) ; la deadline engine
  est *par fetch* alors que `urlseed_timeout` upstream est par
  connexion — un batch multi-groupes peut dépasser la deadline
  torrent-side (D2). [Lentilles : Concurrence ✓, Cycle de vie ✗ (D2)]

- **Pseudo-peer `web_seed_t` : download queue partagée, legs upload
  N/A** — verdict : **déviation** (partielle). Conforme : clé picker
  par seed + `web_seed_outstanding` — pas de double-request ;
  `mark_as_writing` + `DiskWrite` = le chemin `Piece` ; pas de leg
  upload ; `busy` = `peer_info.connection || resolving` ; refcount
  all-pieces pris au dispatch, rendu à la teardown
  (`torrent.rs:6771-6781`, `7081-7092`). Déviant : timeout session
  sans kill de la tâche engine — zombie qui livre des blocs
  `redundant_bytes` (`torrent.rs:7008-7016`) et dont le
  `on_web_seed_done` tardif écrase `busy`/`refcounted`/
  `supports_keepalive` d'un successeur — non génération-gardé
  (`torrent.rs:7056-7077`) (D2) ; `have_files`/`ephemeral`/
  `interesting` par fichier absents ou morts — redirects ré-appris
  par batch, 404 re-pollés à l'infini, ban visible dans
  `url_seeds()` (D6/D10). [Lentilles : Cycle de vie ✗, Erreur ✗]

## Couverture des tests upstream (`test/test_*.cpp`)

Le socle est `web_seed_suite.cpp::run_http_suite` — 7 formes de
torrent (layouts à pads omniprésents : padfiles début/fin/misalignés/
pièce entière/alignés, single-file, single-file en représentation
multi-fichier BEP-3) × proxy ∈ {none, socks4, socks5, socks5_pw, http,
http_pw, i2p} × {http, https} × flags {`chunked_encoding`, `test_ban`,
`no_keepalive`, `test_rename`, `expect_host_header`}. Assertions :
seeding complet, `payload − redundant == total − pad_size`,
**keep-alive → zéro `peer_disconnected`/`peer_error` alert**
(`web_seed_suite.cpp:40-50,182`), `test_ban` → `th.url_seeds()`
**vide** pour >3 pièces (`:161-166,:196`), padfiles jamais créés.

| Test upstream | Couverture Rust | Écart |
|---|---|---|
| suite ×7 formes (pads) | partielle — `plan_skips_pad_and_groups_redirects`, `single_file_request_is_range_only`, `web_seed_tick_and_block` couvrent le mapping/statique ; pas de transfert end-to-end multi-forme | les 7 shapes ne tournent pas contre un serveur simulé |
| proxy matrix (socks4/5/5pw/http/http_pw/i2p) | partielle — `render_proxy_emits_absolute_form_and_proxy_keepalive` + `web_seed_dial` non testé par proxy type ; pas de test CONNECT/socks e2e | `name_via_proxy`, `Proxy-Authorization`, fallback placeholders |
| `chunked_encoding` | parseur couvert (`m_chunked_ranges`, `body()` de-frame) ; pas de test `fetch_block` chunked | — |
| `test_ban` (`url_seeds()` vide) | manquant — chez nous le seed reste listé (`disabled`) | **D10** |
| `no_keepalive` (HTTP/1.0, reconnect par batch) | partielle — `fetch_block_reports_connection_close` + `supports_keepalive` sizing ; pas de test « zero disconnect alert » | **D7** (taxonomy) |
| `test_rename` (`renamed_files` re-target) | manquant — nos requêtes ignorent `m_renamed_files` ? à confirmer (upstream sert le *nom renommé* : `handle_rename`?) | risque latent |
| `expect_host_header` (`format_host_header`) | partielle — `Host` rendu dans `add_headers` | — |
| redirect (`handle_redirect`, ephemeral seeds) | manquant — aucun test redirect e2e ni `Location` vide | **D5/D6** |
| ban socks5_no_peers | manquant | — |

Tests Rust existants : `test_web_seed_natpmp.rs` ×6 (fetch duplex,
206 sans Content-Range, range mismatch, 200 sans Content-Length,
Retry-After, Connection:close), `web_seed.rs` unittests ×8,
`test_session.rs` ×8 (tick/block, gate files_checked, backoff per-seed,
disable terminal, timeout sweep, conn cap, compteurs download, stale→
redundant, remove_url_seed), `test_bandwidth.rs` ×3 (quota
register/grant/overhead/disconnect).

## Déviations

- `crates/bitdaemon/src/engine.rs:2614` — **D1** : cap de réponse codé
  en dur à 4 Mio (`fetch_block(&mut ts, &wreq, 4 * 1024 * 1024)`)
  alors que le merge peut produire des `FileRequest` jusqu'à
  `urlseed_max_request_bytes` (16 Mio défaut, `torrent.rs:6866`).
  `max_response` borne le buffer *entier* headers+body
  (`http_conn.rs:191` → `TooLarge` → `Transport` → `Retry`). Upstream
  n'a aucun cap de réponse. Avec les défauts (4 blocs × 16 Kio)
  inatteignable ; dès qu'un knob monte, un span >4 Mio boucle en
  `Retry` à l'infini sur une requête que NOUS avons générée.
  Sévérité : wire-visible (latent).
  - **sketch** : `let cap = wreq.files.iter().filter(|f| f.got_get)
    .map(|f| f.length as usize + 64 * 1024).sum()` (corps attendus +
    marge headers) — ou plafonner le merge lui-même.
    Test : duplex 206 + corps 5 Mio → succès.

- `crates/bitdaemon-session/src/torrent.rs:7169-7215` +
  `engine.rs` (tâche non tuée) — **D2** : timeout d'inactivité.
  (a) `st.retry_ms = st.retry_ms.max(now + wait)` (`:7188`) alors
  que `disconnect(timed_out_inactivity)` upstream est sévérité
  `normal` → **pas** de bump (`peer_connection.cpp:4988-4996` +
  `web_peer_connection.cpp:276-280` — seul `error >= failure`
  bump). (b) `Alert::UrlSeed "web seed timed out"` (`:7209-7213`)
  au lieu de `peer_disconnected_alert` (`peer_connection.cpp:4486`).
  (c) la tâche engine survit — zombie livrant des blocs comptés
  `redundant_bytes` (`torrent.rs:7008-7016`) dont le
  `on_web_seed_done` tardif (`torrent.rs:7056-7077`, non
  génération-gardé) peut écraser `busy`/`refcounted`/
  `supports_keepalive` d'un batch successeur → slots
  doublement occupés possibles. (d) A28 : le zombie garde aussi son
  `PeerQuota` enregistré dans `web_seed_bw` — des grants gaspillés
  jusqu'à sa mort naturelle. Sévérité : état + wire.
  - **sketch** : (a) supprimer le bump du leg timeout (re-pick au
    tick suivant = upstream) ; (b) poster `PeerDisconnected`-équivalent ;
    (c) `self.tasks` indexé par `(ih,url)` + `abort()` au sweep, ou
    génération dans `WebSeedDone` ignorée dans `on_web_seed_done`/
    `on_web_seed_block`. Test : sweep puis done zombie → `busy`
    du successeur intact.

- `crates/bitdaemon/src/engine.rs:2614-2627,2757-2765` +
  `web_seed.rs` — **D3** : erreurs transport *mid-transfer*
  (`fetch_block` Io/Timeout, `ShortBody`, EOF-entre-réponses →
  `Http{status:-1}`) → `Retry` = +30 s. Upstream :
  `disconnect(e, op_sock_read)` sévérité `normal` → aucun backoff,
  re-dial au tick suivant (`web_peer_connection.cpp:276-280` ; seuls
  connect/`failure` bump, `peer_connection.cpp:4274` — nos legs
  `WebSeedDialError::{Io,Timeout}` au dial y correspondent déjà).
  Sévérité : wire-visible (30 s de trou sur seed flaky ; EOF
  mid-batch HTTP/1.0 pénalisé).
  - **sketch** : `WebSeedBackoff::Now` (release des blocs, `retry_ms`
    inchangé) pour `Transport`/`ShortBody`/`Timeout`/EOF mid-batch ;
    `Retry` gardé pour les échecs de dial. Test : EOF après 1re
    réponse → `retry_ms` inchangé.

- `crates/bitdaemon-session/src/torrent.rs:12512-12516` — **D4** :
  `Header(d) => d` honoré sans plancher. Upstream `handle_error` fait
  `retry_web_seed(retry_after)` *puis* `disconnect(status, failure)`
  qui impose `max(retry, now + urlseed_wait_retry)` → effectif
  `max(d, 30 s)` (`web_peer_connection.cpp:619-628,:276-280` +
  `torrent.cpp:11773-11774`). Sévérité : état (politesse).
  - **sketch** : `WebSeedBackoff::Header(d) => d.max(s.urlseed_wait_retry)`.
    Test : 503 + `Retry-After: 5` → `now+30 s` ; `90` → `now+90 s`.

- `crates/bitdaemon/src/engine.rs:2648-2697` +
  `web_seed.rs:472-475` + `http_parser.rs:42-44` — **D5** :
  `Location` vide → `resolve_redirect_location` renvoie le referrer
  → self-redirect → 64 hops → `Retry`. Upstream :
  `location.empty()` → `disable(errors::missing_location)`
  (`web_peer_connection.cpp:658-661`). Sévérité : wire-visible
  (seed 302-vide martelé 64×/batch).
  - **sketch** : bras `Err(Redirect{location,..})` — si
    `location.is_empty()` → `report!(…, "missing location",
    WebSeedBackoff::Disable, …)`. Test : 302 `Location:` vide →
    `st.disabled` + alerte.

- `crates/bitdaemon/src/engine.rs:2423-2444,2711-2731` +
  `torrent.rs:315-342,6714` — **D6** : état redirect/éphémère non
  persistant. `redirects`/`bases`/`base_addrs` meurent avec la tâche
  → chaque batch re-suit la même chaîne 302 ; les champs
  `WebSeed.ephemeral`/`interesting`/`no_local_ips` existent
  (`torrent.rs:317-322`) mais ne sont **jamais écrits** (init
  `false`/`true` à `6714` — état mort) ; pas de `have_files` par
  fichier. Conséquences : GET+302 superflu par batch (wire) ; upstream
  clear `have_files[file]` sur *tout* statut non-ok
  (`web_peer_connection.cpp:911-912`) — chez nous un fichier 404 est
  re-pollé à chaque backoff, à l'infini ; origine redirectée garde
  son bit fichier (`:749-757` clear upstream) ; `no_local_ips` d'un
  seed-redirect n'est posé que si l'origine a résolu localement
  (`resolved_origin_global`, `engine.rs:2730` — via `proxy_hostnames`
  `resolved=None` → flag jamais armé, vs upstream `is_global(remote())
  = proxy publique` `:670-672`) ; seeds éphémères absents de
  `url_seeds()`/statuses (upstream les liste, filtre
  banned/removed seuls `torrent.cpp:11730-11335`) ; leur `retry`/
  `disabled` propre inexistant (redirect mort pénalise l'origine) ;
  `write_resume_data` n'aurait rien à filtrer aujourd'hui mais le
  commentaire `torrent.rs:2803-2806` promet le filtre `!ephemeral`
  (upstream `:10384`) — à implémenter avec la feature.
  Sévérité : état/perf + SSRF edge + boucle 404 (wire).
  - **sketch** : remonter `redirects`/`ephemeral`/`no_local_ips` et un
    `have_files: Bitfield` par `WebSeed` via `WebSeedDone` ;
    enregistrer les bases redirect comme entrées `web_seed_state`
    éphémères (`url_seeds()` les liste, `write_resume_data` les
    filtre) ; statut non-ok → `have_files.clear_bit(file)`,
    `none_set()` → `interesting=false`. Fix de taille moyenne —
    cœur du modèle par-fichier. Test : 302 sur file N → batch 2
    GET direct sur la base redirect sans re-touch 302 ; 404 file N →
    le seed n'est plus sollicité pour N.

- `crates/bitdaemon-session/src/session.rs:10712-10716` +
  `web_seed.rs:480` — **D7** : taxonomie d'alertes. Tout échec →
  `Alert::UrlSeed`. Upstream : gates connect-time →
  `url_seed_alert` ✓ (`torrent.cpp:6821-6911`) ; `handle_error` →
  `url_seed_alert` **+** `peer_disconnected_alert`
  (`web_peer_connection.cpp:619-628`, `peer_connection.cpp:4486`) ;
  `disable()` → `peer_error_alert` (`peer_error > failure`,
  `:4479-4484`) ; EOF/reset/timeout → `peer_disconnected_alert`
  (`:4486`, `error <= failure`). Le payload `"http {code}: {server}"`
  reste écarté de `"{code} {reason-phrase}"` (`to_string(status)+" "+
  m_parser.message()` `:622-624` — nous `header("server")`).
  Le test keep-alive upstream échoue au premier `peer_disconnect` —
  signal observable perdu chez nous. Sévérité : observabilité.
  - **sketch** : router par `WebSeedBackoff` — `Disable`/`Remove` →
    `PeerError` (+`UrlSeed` aux gates connect-time) ; transport
    mid-transfer/timeout → `PeerDisconnected` ; `Http`/`Header` →
    `UrlSeed` + `PeerDisconnected`. Message : `format!("{status}
    {reason}")` via la raison du parser. Tests : 404 → `UrlSeed`+`Peer
    Disconnected` ; EOF → `PeerDisconnected` ; range invalide →
    `PeerError`.

- `crates/bitdaemon/src/engine.rs:2354-2367` — **D9** : échec de
  résolution du *proxy* → `NameLookup` (1800 s). Upstream
  `on_proxy_name_lookup` error → `web->disabled = true`
  (`torrent.cpp:6992-7003`) — le retry `web_seed_name_lookup_retry`
  ne vaut que pour le *host* (`:7095-7098`). Sévérité : état.
  - **sketch** : bras `Err(e)` du `resolve(&proxy.hostname…)` →
    `WebSeedBackoff::Disable`. Test : proxy DNS-dead → `st.disabled`.

- `crates/bitdaemon-session/src/torrent.rs:4661-4680` +
  `:8543-8545` — **D10** : hash-fail. Bras certain
  (`known_bad && ban_web_seeds`) → `disabled` + refcount drop +
  `UrlSeed "peer banned"` ✓ (`peer_info.banned` →
  `connect_to_url_seed` disable, `torrent.cpp:6852-6864` — même état
  terminal). Bras non-certain : `continue` → rien (upstream
  `received_invalid_data` : `incoming_dont_have` par fichier, ban
  seulement à `num_have_pieces()==0`, `web_peer_connection.cpp:
  562-601`). **Et** : upstream cache les seeds `banned` de
  `web_seeds()` (`torrent.cpp:11732`) — le test `test_ban` exige
  `url_seeds().empty()` (`web_seed_suite.cpp:196`) ; chez nous le
  seed `disabled` reste listé → écart API-visible mesurable.
  Sévérité : wire-visible (re-fetch poison) + API.
  - **sketch** : dépend de `have_files` (D6) — hash-fail non-certain
    → clear les bits fichiers de la pièce, `none_set()` →
    `interesting=false` ; ajouter `st.banned` (ou retirer de
    `web_seeds`) pour filtrer `url_seeds()`. Test : hash-fail certain
    → `url_seeds()` vide.

- `crates/bitdaemon-session/src/session.rs:7856-7872` — **D11** :
  ramp-up ≤ 1 seed dispatché par tick/torrent ; upstream boucle tous
  les éligibles par `maybe_connect_web_seeds`
  (`torrent.cpp:11019-11030`, `--limit` par seed éligible). Converge
  en N ticks pour N seeds — plus lent, pas incorrect. Sévérité : perf
  (mineur — boucler `next_web_seed_blocks` tant qu'il renvoie Some).

- `crates/bitdaemon-net/src/web_seed.rs:460-462` +
  `engine.rs:2757-2765` — **D12** *(nouveau)* : EPIPE côté écriture.
  `s.write_all` → `Io` → `Transport` → `Retry` +30 s. Upstream :
  `op == sock_write && ec == broken_pipe` → **ignoré** — les serveurs
  HTTP/1.0 ferment leur read-end après le request ; on vide
  `m_send_buffer`, `incoming_choke()`, on *continue de lire*
  (`web_peer_connection.cpp:192-213`). Chez nous le batch entier
  tombe et backoffe là où upstream complète le transfert.
  Sévérité : wire-visible (seed HTTP/1.0 half-close intenables).
  - **sketch** : `fetch_block` — `write_all` `ErrorKind::BrokenPipe`
    → ne pas échouer ; marquer `write_end_dead` et passer à la lecture
    (plus de GET suivants sur cette conn — `conns.remove(&g.base)`
    en fin de groupe + `saw_close`). Test : duplex fermant son
    read-end après réception → réponse lue quand même.

- `crates/bitdaemon-session/src/torrent.rs` (`on_web_seed_error`,
  `check_web_seed_timeouts`) + `engine.rs` — **D13** *(nouveau)* :
  `restart_piece`/`restart_request` absents. Upstream sauve la pièce
  partielle en vol au disconnect (`m_web->restart_request =
  m_requests.front()`, `restart_piece = std::move(m_piece)` —
  `web_peer_connection.cpp:240-266`), la re-suggère au reconnect
  (`incoming_suggest`, `:178-182`) et réutilise le préfixe reçu quand
  la requête revient en tête (`:340-363`). Chez nous tout bloc
  partiellement reçu est relâché → re-téléchargé en entier (compté
  `redundant_bytes` si le zombie livre). Sévérité : perf/état
  (re-fetch du préfixe ; multiplié par la taille des ranges
  coalescés).
  - **sketch** : complexité réelle — le modèle pull ne garde pas de
    buffer par seed entre batches. Option minimale : `WebSeedDone`
    transporte les segments *partiels* reçus (`segs` à date) →
    `on_web_seed_block` écrit les blocs complets reçus (notre
    requête fusionnée = plusieurs réponses par fichier ; les blocs
    déjà complets avant l'erreur sont déjà livrés — le manque est le
    *corps partiel d'une réponse*, qu'upstream réutilise). À peser :
    peut rester `won't fix` documenté si le coût dépasse le gain.

- `crates/bitdaemon/src/engine.rs:5860-5870` — **D14** *(nouveau)* :
  `resolve()` ne garde que la **première** adresse ; pas de liste
  d'endpoints par seed ni failover intra-tentative. Upstream
  `on_name_lookup` empile *tous* les addrs dans `web->endpoints`
  (`torrent.cpp:7101-7108`) et un échec de connect pop
  `endpoints.front()` → retry immédiat sur l'IP suivante
  (`web_peer_connection.cpp:215-220`). Chez nous un premier A-record
  mort → `Retry`+30 s → le resolver re-rend probablement la même
  tête → échec répété. Sévérité : wire-visible (seed multi-IP à
  moitié mort = stall permanent).
  - **sketch** : `resolve_all` dans `web_seed_dial`, boucle dial sur
    la liste (abort au premier succès) ; ou stocker
    `endpoints: Vec<SocketAddr>` dans `WebSeedDial`. Test : resolver
    injecté 2 addrs, 1re refuse → connexion au 2e sans backoff.

- `crates/bitdaemon/src/engine.rs:2570-2579` — **D15** *(nouveau)* :
  `no_local_ips` tout-filtré → `NameLookup` (1800 s). Upstream :
  résolution d'un seed `no_local_ips` ne gardant aucun endpoint
  global → `web->disabled = true` + `url_seed_alert
  (banned_by_ip_filter)` (`torrent.cpp:7116-7127`). Sévérité : état
  (seed redirect-poisonné pollue le DNS toutes les 30 min au lieu de
  mourir) + alerte.
  - **sketch** : bras `dial.no_local_ips && !is_global` →
    `WebSeedBackoff::Disable`, msg `banned_by_ip_filter`.
    Test : resolver injecté renvoyant `10.0.0.1` pour un seed
    `no_local_ips` → `st.disabled` + `UrlSeed`.

- `crates/bitdaemon/src/engine.rs:5713-5722` — **D16** *(nouveau,
  mineur)* : `name_via_proxy` inclut `ProxyType::Socks4`. Upstream
  `web_seed_hostname_via_proxy` n'admet que http/http_pw/socks5/
  socks5_pw (`torrent.cpp:6786-6790`) — socks4 (pas de résolution de
  nom dans le protocole de base) résout **localement** → ip_filter /
  SSRF / `resolved_origin_global` s'appliquent. Chez nous socks4 +
  `proxy_hostnames` envoie le nom au proxy (SOCKS4a implicite via
  `SocksTarget::Name`) → `resolved=None` → gates aveugles. Sévérité :
  état/SSRF edge (cohérent avec D6).
  - **sketch** : `socks_px` de `name_via_proxy` restreint à
    `Socks5 | Socks5Password` (le *dial* socks4 reste possible, sur
    IP résolue localement). Test : socks4 + hostnames → le resolver
    local est appelé.

- *(noté, faible confiance)* `engine.rs:5730,5757,5766,5776,5785,5795`
  — le timeout de **connect** utilise `urlseed_timeout` ; upstream
  borne le connect par `peer_connect_timeout` (le `urlseed_timeout`
  ne vaut que l'inactivité de lecture, `web_connection_base.cpp:76-80`
  + `peer_connection.cpp:4988`). Sévérité : mineure (fenêtre
  d'abort de connexion différente : 20 s vs 15 s défauts). À
  confirmer à l'apply — séparer `dial_timeout`/`io_timeout` si vrai.

## Invariants de frontière (Seam Checks)

- **Session ↔ Engine** : `SessionOut::WebSeedFetch` unidirectionnel,
  `WebSeedDone` par `mpsc` — pas de perte (`send` borné mais le
  canal n'est jamais saturé en pratique — à surveiller sous
  `--jobs` massif). Le contrat `done`/`error` est exhaustif —
  chaque chemin du task passe par `report!` ✓.
- **Engine ↔ Net** : `ThrottledWs` interposé entre `fetch_block` et
  `BoxedPeerStream` — EOF-au-lieu-de-read sur teardown (`poll_read`
  → `Ok(())` quand `gate` renvoie false, `engine.rs:565`) : une mort
  de quota/teardown torrent se présente à `fetch_block` comme un EOF
  HTTP → `ShortBody`/`Http{-1}` → `Retry`. Inoffensif quand le
  torrent est parti ; si `disconnecting` est armé par erreur, le seed
  mange un backoff fantôme — à couvrir sous D2/D3.
- **Session ↔ Picker** : `web_seed_outstanding` + `st.outstanding`
  double-comptabilisés par conception (release par `reqs` *ou* par
  set — filets croisés `torrent.rs:7119-7127`) ; `mark_as_writing`
  sous clé seed ✓ ; pas de `cancel_block` légitime post-
  `mark_as_writing` (même piège upstream — inerte).
- **Disk** : `DiskWrite` par bloc découpé du span — `piece`/`start`/
  `length` recomputés (`torrent.rs:7035-7043`) ; les pads ne
  traversent pas le disque ✓ (upstream écrit aussi des zéros via le
  chemin normal — `handle_padfile` ; notre synthèse de zéros en
  mémoire est équivalente, les fichiers pad ne sont jamais créés —
  assertion upstream `exists(file_path) == !pad` satisfaite).
- **Settings hot-apply** : `proxy_*` → `proxies.web_seed` ✓ (D8
  résolu) ; `urlseed_timeout` lu au dispatch, pas au sweep (le
  deadline est armé à `now + urlseed_timeout` au pick —
  `torrent.rs:6927`) — un apply entre pick et sweep garde l'ancien
  délai : même comportement upstream (timeout fixé au connect).
- **Quota (A28)** : un `PeerQuota` par *tâche* (pas par conn —
  `engine.rs:2257`) : deux bases redirect parallèles d'un même batch
  partagent un quota ; upstream a un `m_quota` par connexion.
  Comptablement approximatif — sans impact de classement. Le mask
  enregistré dans `web_seed_bw` est celui du *premier* base ayant
  gaté (`session.rs:7088-7096` `ptr_eq` dedup) — l'overhead d'un
  redirect cross-classe draine sur le mask du seed d'origine.
  Cosmétique.

## Fichiers du fix

- `crates/bitdaemon/src/engine.rs` — cap `fetch_block` (D1), classes
  d'erreur (`Now`/`Disable` proxy/`no_local_ips`, D3/D9/D15),
  `Location` vide (D5), EPIPE half-close (D12), endpoints failover
  (D14), socks4 hors `name_via_proxy` (D16), kill-abort des tâches ou
  génération `WebSeedDone` (D2c), remontée `redirects`/`ephemeral`
  (D6).
- `crates/bitdaemon-session/src/torrent.rs` — timeout sans backoff
  (D2a), `retry_web_seed` floor `urlseed_wait_retry` (D4), `WebSeed` :
  `redirects`/`have_files`/`banned` + écriture effective de
  `ephemeral`/`interesting`/`no_local_ips` (D6/D10),
  `on_web_seed_done`/`on_web_seed_block` génération-gardés (D2c),
  `write_resume_data` filtre éphémères, `url_seeds()` filtre banned
  (D10), boucle ramp-up (D11).
- `crates/bitdaemon-session/src/session.rs` — routage alertes par
  classe (`PeerError`/`PeerDisconnected`/`UrlSeed`, D7/D2b), boucle
  dispatch (D11).
- `crates/bitdaemon-net/src/web_seed.rs` — write EPIPE non-fatal
  (D12), message `{status} {reason}` (D7), cap paramétrable déjà ok.
- `crates/bitdaemon-net/src/proxy.rs` — rien (le gate socks4 vit
  dans `engine.rs`).
- `crates/bitdaemon-session/src/alerts.rs`, `status.rs` — variantes/
  champs si exposés.
- Tests : `test_web_seed_natpmp.rs` (fetch >4 Mio, `Location` vide,
  `Retry-After` floor, EPIPE half-close, failover 2-IP, socks4
  resolve-local), `test_session.rs` (timeout sans backoff,
  zombie-done ignoré, redirect persistant, 404 → `have_files`,
  ban → `url_seeds()` vide, taxonomie alertes).

## Non-bugs tranchés

- **Modèle pull vs connexion persistante** — assumé (ADR-0008).
  Reconnect-par-batch = handshake TCP de plus par batch : wire-visible,
  sans divergence d'état. Le pipelining intra-batch (tous les GET
  écrits d'un coup) est conforme.
- **`urlseed_pipeline_size`** — dead setting upstream ; notre
  `web_seed_blocks_per_tick` est un knob équivalent assumé.
- **`retry_ms = max(...)` vs overwrite** — `check_web_seed_timeouts`
  prend le max (jamais plus tôt qu'un backoff existant) ; direction
  conservative, pas de divergence observable.
- **`is_upload_only`/`is_paused` gatés dans `next_web_seed_blocks`**
  vs upstream dans `connect_web_seed` post-resolve — même effet
  observable, ordre des gates différent mais invisible.
- **`web_seed_inflight` hors `--limit`** — la consommation de slot
  par seed éligible-connecté est reproduite (`torrent.rs:6733-6749`).
- **`http_seed_connection` (BEP-17)** — retiré upstream en 2.x
  (ChangeLog) ; nos `http_seeds` folded = conforme (PORTING row 201).
- **`web_seed_entry::extra_headers`** — les entrées sont foldées en
  strings (PORTING row 73) : `auth` survit via `user:pass@` URL, les
  `extra_headers` per-entry sont perdus — simplification documentée
  (upstream les forward même cross-origin, `web_peer_connection.cpp:
  711-712`). À documenter `won't port` ou reporter si OnionBit en a
  besoin.
- **Pas d'`endpoints` persistants entre batches** (D14 porte le
  failover *intra*-batch) — re-resolve à chaque batch est la
  conséquence du modèle pull, pas un bug en soi.
- **`WebSeedDialError::Io/Timeout` au dial → `Retry`** — conforme :
  connect-fail upstream = `failure` → bump (`peer_connection.cpp:4274`).
