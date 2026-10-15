# Audit A04 — rapport d'analyse (re-audit post-apply)

Périmètre : announce HTTP/UDP/WS, scrape, events, backoff, failover tiers,
compteurs wire, tracker manager, auto-scrape. Baseline upstream :
`e7381c7bce60b96feb79442828ff2b912fd8a29f` (checkout
`D:\Projet\libtorrent\libtorrent`, vérifié, lecture seule).

Ceci est une **re-vérification** : le premier passage A04 a été appliqué
(commit `bea03c0`, PR #5). Les constats ci-dessous reflètent l'état
*courant* du code — la plupart des déviations du premier rapport
(timeouts, `User-Agent`, redirects, compteurs, `key` par torrent,
`num_want`, cache `connection_id`, `deprioritize`, announces v1+v2,
410 → `dont_try_again`, `retry_interval`, alertes détaillées, queue HTTP
prioritaire) sont résorbées.

## Carte des chemins

Rust :
- `bitdaemon-net/src/tracker_client.rs` — `announce_url` (construction
  query + SSRF), `announce_http_get`/`scrape_http_get`,
  `UdpTrackerDemux` (routage par `transaction_id` + cache
  `connection_id` par IP), `announce_udp`/`scrape_udp`, `new_tid`.
- `bitdaemon-net/src/http_tracker.rs` — `announce_query` (ordre des
  paramètres), `parse_response` (announce + scrape), `scrape_url`.
- `bitdaemon-net/src/udp_tracker.rs` — builders/parsers BEP-15.
- `bitdaemon-net/src/ws_tracker.rs` — announce WebTorrent JSON.
- `bitdaemon-session/src/tracker_list.rs` — `AnnounceEntry`/
  `AnnounceEndpoint`/`AnnounceInfohash`, `due()` (machine
  `announce_state`), `trigger`, `completed`, `stop_announcing`,
  `deprioritize`, `dont_try_again`, `prioritize_udp_trackers`.
- `bitdaemon-session/src/torrent.rs` — `announce_params`
  (`build_tracker_request`), `bytes_left`, `add_tracker`,
  `replace_trackers`, `update_scrape_state`.
- `bitdaemon-session/src/session.rs` — `tracker_announce_due`,
  `dispatch_announce`, `announce_stopped_collect`, `force_announce`,
  `scrape_tracker`/`_url`, `on_announce_response`,
  `on_scrape_response`, auto-scrape dans `tick`.
- `bitdaemon/src/engine.rs` — `queue_announce` (cap
  `max_concurrent_http_announces` + file prioritaire),
  `tracker_settings`, `tracker_http_exchange` (redirects cross-origin),
  `http_tracker_endpoints` (SSRF loopback + ip_filter),
  `udp_tracker_endpoints` (`can_route` v4 + filter), legs UDP/WS,
  résolution des `hostname_peers`.

Upstream : `src/tracker_manager.cpp`, `src/http_tracker_connection.cpp`,
`src/udp_tracker_connection.cpp`, `src/announce_entry.cpp`,
`src/tracker_list.cpp`, `src/torrent.cpp` (`announce_with_tracker`
3220-3428, `build_tracker_request` 3430-3507, `send_tracker_request`
3510-3588, `announce_tracker_now` 3596-3640, `scrape_tracker*`
3642-3710, `force_tracker_request*` 4134-4262, `tracker_response`
3819+, `tracker_request_error` 12860-12998, `add_tracker` 6376-6382,
`replace_trackers` 6364-6373, `tracker_key` 5582-5593), tests
`test_tracker.cpp` / `test_tracker_list.cpp` / `test_tracker_manager.cpp`.

## Verdicts checklist

- **Events `none`/`started`/`stopped`/`completed`/`paused` — `completed`
  une seule fois** — verdict : **conforme**. `dispatch_announce`
  sélectionne `started`→`completed`→`none` par slot
  (`session.rs:10112-10123`), latch `start_sent`/`complete_sent` à la
  réponse via `sent_event` (`tracker_list.rs:125-136`),
  `m_complete_sent` global (`session.rs:10558-10560` =
  `torrent.cpp:3860-3866`). Conversion `none → paused` pour
  finished-non-seed (`session.rs:10033-10039` = `torrent.cpp:3269-3270`)
  et sérialisation `paused` sur le wire (`http_tracker.rs:166,177-179`).
  `stopped` n'atteint que les slots `start_sent` (`session.rs:10127`,
  `tracker_list.rs:533`). Remarque : `send_tracker_request` remet
  `next_announce = min_announce = now` à l'émission
  (`torrent.cpp:3580-3581`) — absent en Rust → D4.
- **Backoff quadratique, cap 60 min, `fail_limit`, `retry_interval`** —
  verdict : **conforme**. `failed()` reproduit la formule bit-pour-bit
  (`tracker_list.rs:84-96` = `announce_entry.cpp:226-243`), `fails`
  saturant 7 bits, `can_announce` (+1 s slack, `min_announce`,
  `fails < limit || limit == 0`, `!updating`, exemption
  `need_send_complete`) identique (`tracker_list.rs:100-108` =
  `:244-255`). `retry_interval` des réponses d'échec propagé
  (`AnnounceError::Failure { interval, min_interval }` →
  `retry_interval()` = `interval ?: min_interval`,
  `tracker_client.rs:288-303` = `tracker_manager.cpp:183`).
- **Tiers : ordre, failover, `deprioritize`, `announce_to_all_*`** —
  verdict : **déviation (UDP prio)** — `due()` reproduit la machine
  `announce_state` (tier working, `sent_announce`, `done`, flags
  `to_all_tiers`/`to_all_trackers`, gate stopped) fidèlement
  (`tracker_list.rs:500-569` = `torrent.cpp:3379-3412`) ;
  `deprioritize` en fin de tier quand tous les slots meurent
  (`session.rs:10635-10642` = `torrent.cpp:12936-12948`) ; insertion
  stable par tier + dedup-URL merge `source` conformes. Écart :
  `prioritize_udp_trackers` — sémantique différente **et** jamais
  appelé → D1.
- **Scrape : sélection, auto-scrape, compteurs** — verdict :
  **conforme (à une réserve près)** — `scrape_pick` (idx →
  `last_working` → `first`) conforme (`session.rs:10191-10243` =
  `torrent.cpp:3642-3674`) ; une requête scrape **par version**
  d'info-hash pour les hybrides (`session.rs:10213-10230` =
  `torrent.cpp:3702-3706`) ; auto-scrape = formule exacte
  `interval / max(1,N)` + plancher `auto_scrape_min_interval` +
  round-robin + filtres `paused && auto_managed && !aborted` et
  `!session.paused` (`session.rs:7863-7890` = `session_impl.cpp:
  3799-3829`) ; `update_scrape_state` max sur les entrées +
  `IF_COUNTERS_CHANGED` (`torrent.rs:11666`). Réserve : `verified` posé
  sur réponse scrape — upstream ne le pose que sur announce → D11.
- **UDP : connect/announce/scrape, tid, conn-id cache, endpoints** —
  verdict : **conforme (réserves)** — wire format exact (magic,
  actions, layout announce, trailer `0x02`+`path≤255`,
  `udp_tracker.rs:110-157` = `udp_tracker_connection.cpp:466-518,
  694-750`) ; strides peers 6/18, `min_interval=60`, clamp
  `min_announce_interval` (`tracker_client.rs:582-583` = `:572-574`) ;
  cache `connection_id` par **adresse** avec TTL
  `udp_tracker_token_expiry` (`tracker_client.rs:488-504` = `:704-707,
  440-443`) ; `new_tid` exclut 0 (`tracker_client.rs:420-422` =
  `:416-424`) ; boucle multi-endpoints avec deadline réarmée par cible
  (`engine.rs:1649-1674` ≈ `fail()`→next `:127-172`) ; strays ignorés +
  rearm du receive-timeout (≈`restart_read_timeout`). Réserves :
  source du datagramme non vérifiée (D5), `cache_only` DNS absent pour
  `stopped` (D16), `can_route` v6 partiel (D17).
- **HTTP : query params, headers, redirects, proxy, timeouts** —
  verdict : **conforme (réserves)** — ordre des paramètres identique à
  `http_tracker_connection::start` (`http_tracker.rs:148-206` =
  `:110-202`) : `info_hash,peer_id,port,uploaded,downloaded,left,
  corrupt,key=%08X,[event],numwant,compact=1,no_peer_id=1,
  [supportcrypto],[redundant],[trackerid],[ip],[ipv4]*,[ipv6]*` ;
  jointure `?`/`&` + rejet SSRF sur query pré-remplie
  (`tracker_client.rs:326-330` = `:92-108`) ; `User-Agent` /
  `curl/7.81.0` anonymous-non-privé (`engine.rs:1534-1539` = `:232-236`)
  ; `Authorization: Basic` depuis l'auth URL, abandonnée sur redirect
  cross-origin ; boucle 5 redirects (`engine.rs:5975+`) ; timeouts
  `tracker_completion_timeout`/`tracker_receive_timeout` +
  `stop_tracker_timeout` pour `event=stopped` (`engine.rs:1493-1539` =
  `tracker_manager.cpp:64-142`, `http_tracker_connection.cpp:225-227`)
  ; gzip `Accept-Encoding` + inflate (`http_conn.rs:97,144` = `:191,
  642-645`). Réserves : `ipv4`/`ipv6` jamais peuplés (D6), suppression
  `stopped` quand `stop_tracker_timeout ≤ 0` absente (D3), DNS
  `cache_only` absent (D16).
- **Compteurs : uploaded/downloaded/left/corrupt/redundant, num_want,
  key, trackerid** — verdict : **conforme** —
  `announce_params` (`torrent.rs:3453-3511` = `build_tracker_request`
  `torrent.cpp:3430-3507`) : `downloaded = payload − failed`,
  `uploaded = payload`, `corrupt = failed_bytes`, `redundant` réel +
  repli `report_true_downloaded=false` (repli in-flight sur seeds
  via `piece_recv_staged` ≈ `downloading_piece_progress`),
  `left = bytes_left().unwrap_or(16 KiB)`, `num_want = 0` sur stopped,
  `key` par torrent (`torrent.rs:1039` — nonce stable, sémantique
  équivalente au hash de pointeurs upstream), `trackerid` fallback
  `m_trackerid` torrent (`session.rs:10149-10153` = `:3318`).
- **Réponses : peers compact/dict/hostname, trackerid, warning,
  external ip** — verdict : **déviation (détails)** — parse conforme :
  défauts `interval=1800`/`min interval=30`, `failure reason` → erreur
  avec hints, `warning message` → alerte, `peers` stride 6 / `peers6`
  stride 18 / longueurs invalides → erreur, dict-list tolérante par
  entrée (fatal seulement si *toutes* invalides), `peer id` copié si
  20 octets, `port` 0-65535 requis, `ip` requis
  (`http_tracker.rs:231-383` = `http_tracker_connection.cpp:478-698`),
  hostname peers → `DictPeer` résolus par l'engine avec gate proxy
  (`engine.rs:1820-1825` ≈ `torrent.cpp:3957-3960`), `external ip` →
  `ip_voter` clé par `tracker_ip` (`session.rs:10720-10729` =
  `:3830-3836`). Écarts : self `peer_id` non sauté (D8),
  `trackerid_alert` émis même sans changement (D9), `a.message` =
  warning non mémorisé (D10), `trackerid` echo lossy-UTF8 (D12).
- **Force announce : by-idx vs all, `ignore_min_interval`,
  `high_priority`** — verdict : **conforme (réserves)** —
  `trigger` = `max(t, min_announce)+1s`, `ignore_min` → `t+1s` + clear
  (`tracker_list.rs:181-195` = `torrent.cpp:4144-4160`), by-idx dispatch
  direct bypass-tier ≈ `announce_tracker_now` (`session.rs:8386-8438` =
  `:3596-3640`), `-1` → `high_priority` pompe immédiate sinon rearm
  (`session.rs:8439-8453` = `:4198-4206`). Réserves : gate
  `announce_trackers` absent du chemin by-idx (D13), cascade d'échec
  perd `high_priority` (D14).
- **Stop/pause : `stopped` announce, timeouts, abort session** —
  verdict : **déviation** — `announce_stopped_collect` + `due(stopped)`
  re-arms + gate `start_sent` corrects ; timeout `stop_tracker_timeout`
  appliqué côté engine. Mais `stop_tracker_timeout ≤ 0` devrait
  *supprimer* tout `stopped` — Rust émet quand même → timeout immédiat
  → `fails++` + `tracker_error` fantôme (D3). Pause session : upstream
  `abort_all_requests(false)` échoue les vols non-stopped
  (`announce_skipped` → fails++ + alerte) ; Rust laisse les réponses
  tardives muter l'état (D15).
- **Add/replace trackers : dedup, resume-dirty, announce immédiat** —
  verdict : **déviation** — `TrackerList::add` dedup+merge `source` ✓ ;
  `replace_trackers` pose `IF_METADATA_CHANGED` ✓. Manquent :
  `need_save_resume` sur `add_tracker`, `announce_with_tracker()`
  immédiat si `m_announcing` dans les deux cas, et l'appel
  `prioritize_udp_trackers` (`torrent.cpp:6376-6382, 6364-6373`).
  L'announce diffère d'au plus un tick (entrée fraîche → `due`) — D2.
- **`current_tracker` / `next_announce` status** — verdict :
  **déviation légère** — `next_announce` = min des slots ✓ ;
  `current_tracker` ne fait que cloner `last_working_url` — upstream
  retombe sur le premier tracker `verified` non-updating quand aucun
  `last_working` (`torrent.cpp:~12611-12640`) → D7.
- **WS/WebTorrent tracker** — verdict : **déviation (port partiel)** —
  announce JSON + `failure reason` → erreur + clamp
  `min_websocket_announce_interval` ✓ (`engine.rs:1679-1751`) ; mais
  `offers` toujours vide, pas de clamp `max_webtorrent_offers`, pas de
  connexion ws persistante partagée (`m_websocket_conns`), pas de
  routage offer/answer vers rtc_stream, pas de court-circuit
  `num_want==0 → torrent_aborted` (`tracker_manager.cpp:343-399`) → D18.
- **Queue manager : cap HTTP, priorité, compteur** — verdict :
  **conforme** — `max_concurrent_http_announces` + file avec
  `push_front` pour `high_priority` + `num_queued_tracker_announces`
  (`engine.rs:1460-1490` = `tracker_manager.cpp:288-335,234-260`) ;
  UDP/WS non bridés comme upstream.
- **I2P trackers** — verdict : **gap assumé** — `p.i2p` → parse peers
  32-octets ✓ (`http_tracker.rs:323-333` = `:599-612`) ; `&ip=<ep>.i2p`
  non émis (`http_tracker_connection.cpp:159-173`), refus UDP `.i2p`
  non porté (`udp_tracker_connection.cpp:76-87`). Attendu : subsystem
  I2P.

## Déviations résiduelles

1. **`tracker_list.rs:463-480` — `prioritize_udp_trackers` mort et
   sémantique différente.** Upstream (`tracker_list.cpp:100-132`) : pour
   chaque `udp://`, on permute avec le premier tracker **non-udp de même
   hostname** situé avant lui — *quel que soit le tier*
   (`test_prioritize_udp_tier` : un udp tier 2 passe devant un http
   tier 0 du même host ; `test_prioritize_udp_noop` : hostnames
   différents → aucun réordonnancement). Rust : hissage de tout `udp:`
   au-dessus des non-udp **du même tier**, sans comparer les hostnames —
   et la fonction n'est appelée nulle part (`prefer_udp_trackers`
   existe, défaut `true`, mais aucun call site ni au ctor
   `torrent.cpp:400` ni à `replace_trackers` `:6369`). Effet net : un
   torrent `announce-list [http://t/..., udp://t/...]` annonce le http
   en premier ici, le udp upstream. Sévérité : **moyenne** (ordre wire).
   Fix : réimplémenter le swap par hostname (hors tier) + appeler aux
   deux sites sous `settings.prefer_udp_trackers`.

2. **`torrent.rs:3410, 8396` — `add_tracker`/`replace_trackers` :
   effets de bord manquants.** Upstream `add_tracker` →
   `set_need_save_resume(if_metadata_changed)` + `announce_with_tracker()`
   si `m_announcing && added` ; `replace_trackers` →
   `prioritize_udp_trackers` (sous setting) + `announce_with_tracker()`
   si announcing + resume-dirty. Rust : insertion/reconstruction seule,
   `need_save_resume` absent d'`add_tracker`. L'entrée fraîche est `due`
   au prochain tick (≤ `tick_interval`, 500 ms) — décalage temporel,
   pas de perte ; la persistance resume en revanche saute si le
   torrent est sauvegardé avant un autre changement. Sévérité :
   **basse-moyenne**.

3. **`session.rs:3344` + `engine.rs:1528` — `stop_tracker_timeout ≤ 0`
   non honoré comme suppression.** Upstream court-circuite tout
   `stopped` (`torrent.cpp:3236-3246`) : aucune requête, aucun état.
   Rust émet les `TrackerAnnounce` stopped puis l'engine les borne à
   `completion=0` → `tokio::time::timeout` expire *immédiatement* →
   `TimedOut` → `failed_version` → `fails++` + `TrackerError` fantôme à
   chaque pause. Sévérité : **moyenne** (état + alertes parasites).
   Fix : skipper la collecte `stopped` quand `stop_tracker_timeout ≤ 0`
   (côté session, avant `announce_stopped_collect`).

4. **`session.rs:10129-10140` — `next_announce`/`min_announce` non
   remis à `now` à l'émission.** Upstream `send_tracker_request` pose
   `a.next_announce = a.min_announce = now` au send
   (`torrent.cpp:3580-3581`) : après échec, le retry n'est bridé que
   par le backoff. Rust conserve le `min_announce` hérité de la dernière
   réponse — un retry post-échec peut rester bridé par un `min interval`
   obsolète (retries plus lents qu'upstream), et `trigger` sans
   `ignore_min` calcule `max(t, min_announce)` sur une valeur périmée.
   Sévérité : **basse-moyenne** (timing).

5. **`tracker_client.rs:148-157` — source UDP non vérifiée.** Upstream
   droppe toute réponse `m_target != ep` (hors mode hostname-proxied,
   `udp_tracker_connection.cpp:334-350`). Rust `wait_reply` jette
   `from` — tout datagramme au tid correspondant est accepté (spoofing
   borné par le tid 32 bits, mais le filtre existe upstream). Sévérité :
   **basse-moyenne**. Fix : comparer `from` à la cible pour
   `UdpTarget::Addr`.

6. **`torrent.rs:3501-3502` — `ipv4`/`ipv6` jamais remplis.** Upstream
   annonce `&ipv4=`/`&ipv6=` des adresses externes des listen sockets
   sur les torrents *private* (`http_tracker_connection.cpp:185-202`).
   Rust les laisse vides partout. Sévérité : **basse** (private
   trackers seulement).

7. **`torrent_status::current_tracker` — pas de fallback verified.**
   Upstream retombe sur le premier entry `verified` sans slot `updating`
   quand `last_working` est vide (`torrent.cpp:~12611-12640`). Rust ne
   renvoie que `last_working_url` → `current_tracker` vide tant que
   rien n'a répondu. Sévérité : **basse** (status).

8. **`session.rs:10497-10510` — self `peer_id` non sauté.** Upstream
   ignore `i.pid == m_peer_id` dans les dict-peers
   (`torrent.cpp:3919-3921`). Rust parse `peer_ids` mais ne compare
   jamais → un tracker qui nous renvoie notre propre peer-id produit
   une tentative de self-connect (filtrée plus bas par `add_swarm_peer`
   seulement si l'ip coïncide). Sévérité : **basse**.

9. **`session.rs:10553-10571` — `trackerid_alert` non conditionné au
   changement.** Upstream n'émet que si `ae->trackerid !=
   resp.trackerid` (`torrent.cpp:3877-3883`). Rust poste à chaque
   réponse contenant un `tracker id`. Sévérité : **basse** (bruit
   d'alertes).

10. **`session.rs:10495` — `a.message` du warning non mémorisé.**
    Upstream stocke `warning_message` (ou vide) dans
    `announce_endpoint::message` au succès (`torrent.cpp:3874`) — le
    warning reste visible dans `trackers()` status. Rust efface
    toujours. Sévérité : **basse**.

11. **`session.rs:10282` — `verified` posé sur scrape.** Upstream ne
    pose `ae->verified` que dans `tracker_response` (announce,
    `torrent.cpp:3868`). Rust le pose aussi en `on_scrape_response` →
    `use_dht_as_fallback` / fallback `current_tracker` voient le tracker
    vérifié plus tôt qu'upstream. Sévérité : **basse**.

12. **`session.rs:10554` — `trackerid` en `String` lossy.** Upstream
    `trackerid` est binaire (`std::string`) rééchi verbatim ; Rust
    convertit via `from_utf8_lossy` à la réception et à l'écho → un id
    non-UTF8 est manglé dans le `&trackerid=` suivant. Sévérité :
    **basse** (rare).

13. **`session.rs:8383-8438` — `force_announce` by-idx sans gate
    `announce_trackers`.** Upstream `announce_tracker_now` return quand
    `!m_announce_to_trackers` (`torrent.cpp:3602`). Rust ne teste que
    `is_paused` → un force ciblé peut émettre malgré
    `set_announce_to_trackers(false)`. Sévérité : **basse**.

14. **`session.rs:10700` — cascade d'échec perd `high_priority`.**
    Upstream relaie `r.kind & high_priority` dans le failover
    (`torrent.cpp:12992`) ; Rust dispatche la cascade avec `false` → un
    force-annonce raté retombe en priorité normale au tracker suivant.
    Sévérité : **basse**.

15. **Pause session — vols en cours non avortés.** Upstream
    `abort_all_requests(false)` échoue toutes les requêtes non-stopped
    (`announce_skipped` → `tracker_request_error` → `fails++`, alerte,
    `updating` libéré ; `tracker_manager.cpp:526-617`). Rust met
    `outstanding_announces = 0` mais laisse les tâches engine finir :
    une réponse tardive post-`stop_announcing` peut re-latcher
    `start_sent`/`verified` (puis renvoyer `stopped` plus tard), et les
    erreurs `announce_skipped` ne sont jamais rapportées. Sévérité :
    **basse-moyenne**.

16. **DNS `cache_only` absent pour `stopped`.** Upstream flag les
    lookups des announces stopped `cache_only | abort_on_shutdown`
    (`http_tracker_connection.cpp:254-257`,
    `udp_tracker_connection.cpp:107-111`) pour ne pas bloquer le
    shutdown sur un DNS lent. Rust résout normalement (borné par le
    completion timeout). Sévérité : **basse**.

17. **`can_route` v6 asymétrique.** Rust ne filtre les endpoints
    résolus que pour une socket v4 (`udp_tracker_endpoints`,
    `engine.rs:5826`) ; une socket v6 conserve les adresses v4 → une
    tentative en trop (échec send → endpoint suivant). Upstream
    `listen_socket.can_route` exclut dans les deux sens
    (`udp_tracker_connection.cpp:209-213`). Sévérité : **triviale**.

18. **WS tracker — signaling incomplet.** `tracker_manager.cpp:343-399` :
    clamp `num_want = min(num_want, max_webtorrent_offers)`, court-circuit
    `num_want==0 → torrent_aborted` sans reconnecter, connexion ws
    persistante par URL partagée (`m_websocket_conns`,
    `queue_request`), génération d'offers RTC via `generate_rtc_offers`,
    routage answer/offer → `rtc_stream`. Rust émet un announce JSON
    one-shot avec `offers` vide (`engine.rs:1700`) — le torrent ne peut
    pas établir de peers WebTorrent. Sévérité : **moyenne** (feature
    incomplète ; le parsing réponses et le mapping d'erreurs sont
    conformes).

19. **Endpoint synthétique unique (archi assumée).** Upstream annonce
    par (listen socket × info-hash) — `announce_endpoint` par socket,
    `address_family_not_supported` → `ep.enabled=false`
    (`torrent.cpp:12915-12923`). Rust : un endpoint par entry → une
    announce par (tracker, version) ; pas d'équivalent multi-socket ni
    de disable par famille. Documenté comme simplification ADR ; à
    réviser si multi-listen-socket arrive.

20. **`udp_tracker::udp_announce` (fn standalone) — tid inclut 0.**
    `rand::random()` brut (`udp_tracker.rs:286`) vs `new_tid()` du
    chemin réel qui exclut 0/`0xffffffff`. Fonction utilisée uniquement
    en tests ; doublon avec `tracker_client::announce_udp`. Note de
    nettoyage — considérer la supprimer ou la faire partager `new_tid`.

## Couverture tests — gaps vs upstream

Tests Rust existants : `test_tracker_a04.rs` (13 cas : hybride v1+v2,
`failed_version` ciblé, 410→`dont_try_again`, `tracker_error` payload,
`retry_interval`, gate stopped, deprioritize, cascade tiers, burst
completed, trackerid echo/fallback, compteurs réels, scrape par
version, verified), `test_announce.rs` (cycle e2e, protocole non
supporté, backoff), `test_scrape.rs` (cycle, échec→alerte),
`test_tracker_client.rs` (http e2e, udp e2e, conn-id TTL, URL/trackerid/
SSRF).

Non portés depuis upstream (`test_tracker.cpp`,
`test_tracker_list.cpp`, `test_tracker_manager.cpp`,
`udp_tracker.cpp` fake-tracker) :

- **Parser** : `parse_hostname_peers`, `parse_peers4/6`,
  `parse_*_invalid_length`, `parse_i2p_peers(+invalid)`,
  `parse_interval`, `parse_warning`, `parse_failure_reason`,
  `parse_scrape_response(+zero)`, `parse_external_ip(+6)`,
  `extract_peer*` (non-dict, missing ip/port, negative, overflow),
  `parse_websocket_tracker_*` (offers/answers/failure variants).
- **Tracker list** : `test_prioritize_udp{,_noop,_tier}` (couvrirait
  D1), `test_deprioritize_tracker{,_tier}`, `test_last_working`,
  `complete_sent`, `enable_all`, `test_add_sort_by_tier`,
  `test_replace_*`, `test_duplicate_add`.
- **Intégration** : `udp_tracker_v4/v6` (fake tracker UDP),
  `http_peers`, `current_tracker`, `websocket_tracker*`,
  `tracker_proxy`, `stop_tracker_timeout` + `_zero_timeout` (couvrirait
  D3).
- **Manager** : `tracker_protocol_supported`, queue/num_requests.

## Par rapport au premier rapport

Résorbés par `bea03c0` : timeouts HTTP/UDP depuis les settings,
`User-Agent` (+variante anonymous), redirects cross-origin, compteurs
payload réels + `bytes_left`, `num_want` settings + 0 sur stopped,
`key` par torrent, `AnnounceOptions` depuis settings,
`AnnounceError::Failure{interval,min_interval}` → `retry_interval`,
410→`dont_try_again`, `deprioritize`, announce par version (hybride),
cache `connection_id`, queue `max_concurrent_http_announces`,
`TrackerError{times_in_row,status_code,op}`, gate proxy sur
`hostname_peers`, `vote_external` clé `tracker_ip`, `triggered_manually`
→ alertes inconditionnelles.

Persistants : `verified` sur scrape (D11), `peer_ids` self-skip (D8),
warning non mémorisé (D10) — tous déjà signalés au premier passage —
plus les écarts nouvellement caractérisés ci-dessus (D1-D7, D9, D12-D20).

### Livraison initiale

- Commit apply : `bea03c0` `fix(trackers): upstream-fidelity apply of
  audit A04` (branche `audit/04-trackers`)
- PR : https://github.com/laurentgeynet-ux/BitDeamonRust/pull/5
- Fragment changelog : `docs/audits/changelog/A04.md`
