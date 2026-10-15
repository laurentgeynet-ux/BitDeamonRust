# Audit A08 — rapport d'analyse

Baseline upstream : `e7381c7bc` (`src/peer_connection.cpp` —
`validate_piece_request` 1262-1276, `incoming_choke`/`clear_request_queue`
1526-1555, `clear_download_queue` 1557-1571, `incoming_reject_request`
1577-1685, `start_receive_piece` 2685-2757, `incoming_piece` 2794-3148,
`send_block_requests` 4027-4193, `wanted_transfer`/`download_queue_time`,
`update_desired_queue_size` 4800-4852, `second_tick` 4988-5163,
`snub_peer` 5165-5260, `on_disk_write_complete` ;
`src/bt_peer_connection.cpp` — `on_piece` 1028-1113, `on_choke` 837-865,
`write_reject_request` 389-409 ; `src/request_blocks.cpp` —
`request_a_block`, `add_request` ; `src/disk_cache.cpp` —
`account_pending_write` ; `src/web_peer_connection.cpp` — merge
`request_large_blocks` 105, `incoming_piece` direct 1262 ;
`include/libtorrent/aux_/peer_connection.hpp` — `m_request_large_blocks`
1117 ; `include/libtorrent/aux_/merge_block_requests.hpp`).
Côté Rust : `crates/bitdaemon-session/src/{peer_conn,peer_pipeline,
request_blocks,torrent,session,piece_picker,peer_info,counters}.rs` ;
`crates/bitdaemon-disk/src/{disk_io,back_pressure}.rs` ;
`crates/bitdaemon/src/engine.rs`.

## Verdicts checklist

- **`request_a_block` : gates, options picker, endgame, busy cap** —
  verdict : **conforme** — preuve : les gates amont (choked hors
  allowed-fast, `!want_to_download`, torrent non prêt / métadonnées /
  checking, `num_requests <= 0`, `disconnecting`, `upload_mode`,
  `have_all`/`is_seed` self-side) et la boucle
  `pick_pieces` → `mark_as_downloading` → `add_request` sont reproduits
  dans `drive_requests` (`request_blocks.rs` ≈ `request_blocks.cpp`).
  `prefer_contiguous_blocks`, `time_critical` (sortie anticipée +
  ordre), endgame (`end_game` flag + `piece_end_game` waste) alignés.
  Nuance : `set_endgame(!choked)` dans le drive Rust vs upstream qui ne
  touche pas le flag sous choke — no-op net car `incoming_choke` l'a
  déjà effacé (cf. Non-bugs). Le picker Rust est créé à `init_picker`
  avec la taille payload réelle (`torrent.rs:2003-2027`), meilleur que
  `num_pieces × piece_length` pour la queue finale.
- **`add_request` : cap busy, ordre time-critical, stamps au wire** —
  verdict : **conforme** — le plafond "une requête busy hors
  time-critical" (`num_requests` dérivé de `desired_queue_size`,
  time-critical bypass), le positionnement `request_queue` (les
  time-critical en tête dans la fenêtre `m_queued_time_critical`,
  le reste en queue), et l'absence de stamp `m_requested`/`m_last_piece`
  au enqueue (upstream les pose dans `send_block_requests`, Rust dans
  `send_block_requests` `peer_pipeline.rs:391-398`) sont fidèles.
- **`send_block_requests` : limites, merge, purge is_downloaded,
  ordre, compteurs** — verdict : **conforme** — gates initiales
  (disconnecting, `request_queue` vide, quota `send_buffer` plein via
  `setup_send`), pop front → `num_peers_down_requests++` au premier
  push dlq, `m_outstanding_bytes += r.length` par bloc, purge
  `is_downloaded` via `picker` (abort, `continue`), ordre des envois =
  ordre de la queue, stamp `last_piece_ms` inconditionnel en fin
  (`peer_pipeline.rs:394` = `peer_connection.cpp:4183`) +
  `requested_ms` armé au passage vide→non-vide (`:396-398` =
  `:4185-4192`). Le merge `request_large_blocks` n'existe que pour les
  web seeds upstream (`web_peer_connection.cpp:105`,
  `peer_connection.cpp:119` init `false`) — le chemin BT ne fusionne
  jamais des deux côtés (cf. Non-bugs).
- **Réception `Piece` — validation de l'en-tête** — verdict :
  **déviation** (D2) — conforme : `packet_size - 9 > block_size` →
  `packet_too_large` disconnect (`peer_conn.rs:1399-1405` =
  `bt_peer_connection.cpp:1039-1046`) ; header-size 9 ; le payload
  vient du message codec complet. Divergent : `validate_piece_request`
  (`peer_connection.cpp:2700-2708` ← `:1262-1276`) — piece hors
  `[0, end_piece)`, `start < 0`/`>= piece_sz`/non aligné,
  `length != to_req` — n'a pas d'équivalent ; un `piece` malformé est
  traité au lieu de déconnecter `invalid_piece`. Le cas `length == 0`
  diverge aussi : `PeerError` alerte seule (`peer_conn.rs:1430-1433`)
  là où upstream déconnecte `invalid_piece` dès `start_receive_piece`
  pour un pair BT (le bras BitComet `peer_sent_empty_piece` +
  `incoming_reject_request` de `incoming_piece` 2856-2866 n'est
  atteignable que via `web_peer_connection::incoming_piece` 1262).
- **Réception `Piece` — `start_receive_piece` (migration rq→dlq,
  insertion `not_wanted`)** — verdict : **déviation** (D1) — upstream
  (`peer_connection.cpp:2713-2756`) : un bloc en `m_request_queue`
  migre en tête de `m_download_queue` (ajustant
  `m_queued_time_critical`, annulant l'envoi qui n'a jamais eu lieu) ;
  un bloc inconnu est inséré en tête avec `not_wanted`,
  `unwanted_block_alert`, `num_peers_down_requests++` si queue vide,
  `m_outstanding_bytes += length` — puis `incoming_piece` l'écrit sauf
  si `is_downloaded` (waste `piece_cancelled`/`timed_out` alors).
  Rust (`peer_conn.rs:1453-1458`) : `was_requested` ne regarde que la
  `download_queue` (`:972-981`) → `UnwantedBlock` → `piece_unknown`
  waste + alerte + drop. La doc `:964-970` affirme qu'upstream ne
  regarde que `m_download_queue` — la migration rq→dlq de
  `start_receive_piece` la contredit : le payload d'une requête encore
  en `request_queue` est écrit upstream, jeté ici, et la requête part
  quand même sur le fil.
- **Réception `Piece` — ordre interne `incoming_piece`** — verdict :
  **conforme** (modulo D1/D2/D4/D5) — have_none sur bitfield omis
  (`peer_conn.rs:1411-1414` = `peer_connection.cpp:2813-2825`) ;
  slow-start bump + `update_desired_queue_size` **avant** les bras
  empty/seed/requested (`:1418-1422` = `:2827-2854`) ; bras `is_seed`
  (pop front dlq + `piece_seed` waste, `:1439-1447` = `:2870-2884` —
  modulo le double-dec du compteur, D5) ; classification waste par
  flags `timed_out`/`not_wanted`/`busy` → `piece_timed_out`/
  `piece_cancelled`/`piece_end_game` (`:1464-1479` = `:2934-2938`) ;
  ordre `async_write` → `exceeded`-gate → `queued_write_bytes` →
  alerte seuil → `outstanding_writing_bytes` → `mark_as_writing` →
  `check_graceful_pause` → `request_a_block` → `send_block_requests`
  reproduit via la séquence d'effets `DiskWrite` → `MarkDownloaded` →
  `GracefulCheck` → `MaybePick` (`:1485-1504` = `:2986-3148`) ;
  `m_request_time` échantillonné avant le réarmement de `m_requested`,
  réarmé seulement si dlq non vide (`on_piece` `:948-960` =
  `:2948-2961`/`:3032-3045`); `suggested_pieces`/`allowed_fast`
  purgés à la réception (`:946-947`). Compteur pick swappé
  `incoming_redundant_piece_picks` quand `is_downloaded` (`:2963-2964`,
  côté `apply_peer_effects`). `maybe_send_hash_request` post-piece
  couvert via `MaybePick`→`drive_peer` (`torrent.rs:4344-4345`,
  `:4930-4935`) — mais pas sur les bras early-return (`is_seed`,
  `UnwantedBlock`) où upstream l'appelle inconditionnellement
  (`bt_peer_connection.cpp:1112`) — replié dans D1.
- **Réception `Piece` — unsnub** — verdict : **déviation** (D4) —
  Rust unsnub tout bloc `was_requested` dans la fenêtre
  (`peer_conn.rs:1480-1484`) ; upstream n'atteint le check
  (`peer_connection.cpp:2969-2980`) qu'après le `return` du bras
  `is_downloaded` (`:2932-2966`) : un bloc redondant
  (finished/timed_out/not_wanted) arrivé vite ne unsnub pas upstream.
- **Réception `RejectRequest`** — verdict : **déviation** (D6, queue
  seulement) — conforme : `invalid_reject` disconnect sans FAST
  (`peer_conn.rs:1509-1515`) ; validation de forme exacte (`on_reject`
  `:875-930` = `peer_connection.cpp:1589-1630` : piece hors range /
  `piece_sz == 0` → drop silencieux, pas de changement d'état) ;
  recherche dans `download_queue`, erase, `outstanding_bytes -= len`
  (clamp 0), `num_peers_down_requests` via sync ; bloc parole remis en
  tête de `request_queue` ; `abort_download` seulement si
  `!timed_out && !not_wanted` ; `suggested`/`allowed_fast` purgés ;
  `GracefulCheck` avant le pick (`:1533-1535` = `:1673`). Divergent :
  upstream termine par `send_block_requests()` inconditionnel
  (`:1682`) — la requête parole remise en tête et le reliquat de
  `request_queue` partent immédiatement ; Rust n'émet `MaybePick` que
  sous `request_queue.empty() && download_queue < 2` (`:1539-1544`)
  et rien qui ne fasse que flusher.
- **Choke non-FAST — rejects synthétiques** — verdict : **déviation**
  (D6, queue) — le mécanisme est fidèle : `!supports_fast` → pour
  chaque entrée dlq, `to_req(block)` reconstruit (`:1026-1043` =
  `bt_peer_connection.cpp:852-864`) puis `on_reject` →
  `AbortDownload`. Mais upstream exécute la queue complète de
  `incoming_reject_request` par entrée (`check_graceful_pause` + pick
  gaté + `send_block_requests` final) ; le bras Rust n'émet que
  l'abort — pas de graceful-check, pas de re-pick allowed-fast, pas de
  flush. Parole : `request_queue` préservée des deux côtés
  (`on_choke_with_aborts` `:826-828` = `clear_request_queue`
  `:1543-1554`).
- **Timeouts / snub** — verdict : **déviation** (D3, gate seulement)
  — conforme : quota `> 0` + dlq non vide requis (`torrent.rs:7596-
  7602` = `:5080-5083`/`:5141-5143`) ; corps `snub_peer` verbatim :
  flip `snubbed`+`slow_start=false`+`peer_snubbed_alert` une fois,
  `desired_queue_size = 1`, parole épargnée, wipe `request_queue`
  back-to-front + `queued_time_critical = 0`, dernier bloc éligible
  (`!timed_out && !not_wanted`), `free_blocks > 0` → send et retour,
  sinon `block_timeout_alert` → `dqs = 2` → `request_a_block`
  (`snubbed_piece_picks` au pick invoqué) → `dqs = 1` → `timed_out` +
  `abort_download` → `send_block_requests` final (`:7609-7723` =
  `:5173-5259`). Divergent : la leg `request_timeout` upstream exige
  en plus `reading_socket` (`bw_network` sur le canal download,
  `:5080`) — un conn `disk_blocked` ou rate-limité ne lit pas la
  socket et n'est jamais snubbé ; Rust snubbe dès que
  `requested_ms`/`last_piece_ms` expirent (`:7603-7608`). La leg
  piece-timeout upstream n'a pas ce gate — conforme. La même
  primitive manque sur l'échelle de déconnexion
  (`timed_out_inactivity`/`no_handshake`/`no_request`/`no_interest`
  `:4988`/`:5004`/`:5025`/`:5061`) — chevauche A07.
- **Endgame** — verdict : **conforme** — flag `endgame` posé au pick
  quand le picker entre en endgame, `piece_end_game` waste sur doublon
  (`:1474-1475`), retry au `second_tick` avec
  `end_game_piece_picks` au succès (`torrent.rs:7579-7583`),
  `set_endgame(false)` au choke (`:824-825` = `:1526`), compteur
  `num_peers_end_game` edge-triggered (`:809-818`). Busy = requêtes
  envoyées à plusieurs conns pour un même bloc, waste `piece_end_game`
  au doublon — sémantique préservée.
- **Back-pressure disque** — verdict : **conforme** — hystérésis
  globale high/low watermark + callback observer à la redescente
  (`back_pressure.rs:143-237` = `disk_cache.cpp::account_pending_write`
  + observer low-watermark) ; `exceeded` = `level + 1 >= max_size` en
  comptant le write traversant, parked-derrière-fence inclus
  (`disk_io.rs:2359-2427`) ; gate par pair `exceeded &&
  outstanding_writing_bytes > 0` évalué **avant** l'incrément —
  chaque pair garde droit à deux blocs disque
  (`session.rs:2413-2423` = `peer_connection.cpp:3008-3020`) ;
  `num_peers_down_disk++` seulement à la transition (`:2414-2417` =
  `:3010-3011`) ; `queued_write_bytes`/`max_queued_disk_bytes` alerte
  `too_high_disk_limit` edge-triggered (`:2392-2405` = `:3018-3030`).
- **Séquencement écriture disque** — verdict : **conforme** —
  `async_write` soumis avec `picker_gen` + observer par conn
  (`session.rs:2331-2374`), `queued_write_bytes`/`outstanding_writing_
  bytes` comptés avant complétion ; `MarkDownloaded` →
  `mark_as_writing` + annonce prédictive (`apply_peer_effects`) ;
  complétion → décrément `outstanding_writing`, clear `disk_blocked`
  + `num_peers_down_disk`, pièce `is_finished` → vérification hash →
  `piece_finished`/`block_finished` alertes. `t->is_deleted()` /
  `m_abort` inexistants structurellement (acteur). `cancel_block`
  multi-peers upstream (`:3049`/`:3070`) inerte post-`mark_as_writing`
  (gate `is_requested` par conn) — absence équivalente.
- **Compteurs** — verdict : **déviation** (D5) —
  `num_peers_down_requests` réconcilié par snapshot dans
  `sync_down_requests` (`torrent.rs:3211-3221`, appelé en fin de batch
  `session.rs:7847-7854`) — équivalent aux inc/dec site-par-site
  upstream **sauf** le bras seed qui décrémente aussi inline
  (`peer_conn.rs:1442-1444` = upstream `:2878-2881` — le site est
  fidèle, mais double avec le sync → drift permanent). Les autres
  compteurs (`num_peers_down_disk`, `queued_write_bytes`,
  `outstanding_writing`, waste buckets, `*_piece_picks`,
  `snubbed_piece_picks`, `incoming_redundant_piece_picks`,
  `reject_piece_picks`, `unchoke_piece_picks`, `end_game_piece_picks`,
  `piece_rejects`) sont aux bons sites avec les bons déclenchements.
- **Alertes & ordre** — verdict : **conforme** (modulo D4/D6) —
  `peer_snubbed`/`peer_unsnubbed`/`block_timeout`/`unwanted_block`/
  `peer_error`/`block_finished`/`performance` aux bons sites ;
  `GracefulCheck` avant les picks (upstream `check_graceful_pause` →
  `is_disconnecting` → `request_a_block` → `send_block_requests`).
- **Modèle différé** — verdict : **conforme** — `PeerEffect` batch +
  `apply_peer_effects` rejoue l'ordre upstream intra-message sous
  acteur mono-fil ; `m_deferred_send_block_requests` (coalescing par
  task postée) remplacé par des appels inline idempotents — même
  effet net.

## Déviations

- **D1 — `start_receive_piece` non porté : les `piece` non sollicités
  ou encore en `request_queue` sont jetés.** Rust
  (`peer_conn.rs:1453-1458`) émet `UnwantedBlock` dès que le bloc
  n'est pas dans `download_queue` (`was_requested` `:972-981`,
  commentaire `:964-970` factuellement faux — il prétend qu'upstream
  ne regarde pas `m_request_queue`). Upstream
  (`peer_connection.cpp:2713-2756`) : (a) un bloc en
  `m_request_queue` **migre** en tête de `m_download_queue`
  (erase rq + `--m_queued_time_critical` si dans la fenêtre) — le
  payload est consommé et la requête ne partira jamais ; (b) un bloc
  totalement inconnu est inséré `not_wanted` en tête
  (`unwanted_block_alert`, `num_peers_down_requests++` si vide,
  `m_outstanding_bytes += length`) puis `incoming_piece` l'écrit sauf
  si `is_downloaded` (waste `piece_cancelled`/`piece_timed_out` alors,
  `piece_unknown` seulement quand la queue n'a plus l'entrée).
  Conséquences : bande passante gaspillée (bloc reçu jeté puis
  re-demandé ; requête fantôme émise sur le fil — divergence visible),
  `piece_unknown` compté là où upstream écrit ou compte
  `piece_cancelled`, `outstanding_bytes`/`num_peers_down_requests`
  divergents, et `maybe_send_hash_request` sauté sur ce bras
  (upstream `bt_peer_connection.cpp:1112` l'appelle après tout
  `piece`). Fix : implémenter `start_receive_piece` dans `dispatch`
  **avant** `was_requested` — chercher `request_queue` (erase +
  ajustement `queued_time_critical`, insertion en tête dlq) sinon
  insérer `not_wanted` en tête (+`unwanted_block_alert`,
  `outstanding_bytes += len`, et inc du compteur via le modèle sync) ;
  le bras `UnwantedBlock`/`b == end` devient alors l'équivalent de
  l'assert-upstream (essentiellement inatteignable). Sévérité :
  **majeure** (divergence wire-visible + perte de données utiles).
- **D2 — `validate_piece_request` absent sur `piece` entrant.** Rust
  ne valide que `length > block_size` → `packet_too_large`
  (`peer_conn.rs:1399-1405`) ; upstream déconnecte `invalid_piece`
  (`peer_connection.cpp:2700-2708`) quand `piece` hors
  `[0, end_piece)`, `start < 0`/`>= piece_sz`/non aligné sur
  `block_size`, ou `length != min(piece_sz - start, block_size)`
  (`:1262-1276` ; strict car `request_large_blocks` est web-seed-only
  — un pair BT ne reçoit jamais de réponse fusionnée valide). Le cas
  `length == 0` diverge aussi : alerte `peer_sent_empty_piece` sans
  disconnect (`peer_conn.rs:1430-1433`) là où un pair BT est
  déconnecté `invalid_piece` (le bras BitComet `incoming_piece`
  `:2856-2866` n'est atteignable que par `web_peer_connection` `:1262`).
  Conséquences : un payload court/désaligné/hors-range atteint
  `was_requested`/`block_index` → écriture partielle ou drop
  silencieux (puis hash-fail → re-téléchargement, peer potentiellement
  banni pour une faute de protocole qu'upstream sanctionne par
  déconnexion immédiate — divergence wire-visible sur un peer
  malformé). Fix : gate `validate_piece_request` (in-range, `start`
  aligné `< piece_sz`, `length` exacte `to_req`) avant tout état,
  disconnect `invalid_piece` ; faire du `length == 0` BT un disconnect
  et réserver le bras BitComet-reject au chemin web-seed. Sévérité :
  **majeure**.
- **D3 — leg `request_timeout` sans gate `reading_socket`
  (`bw_network`).** Rust (`torrent.rs:7596-7608`) : `quota > 0` +
  dlq non vide + (`requested_ms` expiré **ou** `last_piece_ms` >
  `piece_timeout`) → snub + timeout-mark/abort. Upstream
  (`peer_connection.cpp:5080-5086`) exige en plus `reading_socket`
  sur la leg `m_requested` uniquement — une conn `disk_blocked`
  (`bw_disk`, aucun read posté → `NETWORK` clair) ou rate-limitée avec
  quota résiduel `> 0` n'est pas « lente », elle est *bloquée par
  nous* : upstream ne l'en punit pas (commentaire `:5040` — « if the
  peer is 'slow' because we are rate limiting it »). Rust lui marque
  `timed_out`, abort le bloc pické, et snubbe — churn de requêtes +
  alerte `peer_snubbed`/`block_timeout` parasites exactement dans le
  scénario que le gate protège. La leg `piece_timeout` (`:5141-5143`)
  n'a pas ce gate — conforme ; ne gater que la leg `request_timed_out`.
  L'état est exprimable : `c.bw.channel_state[DOWNLOAD] & NETWORK` est
  posé autour du `read().await` (`engine.rs:3366-3377`). Même
  primitive absente sur l'échelle de déconnexion (`:4988`/`:5004`/
  `:5025`/`:5061` — upstream `reading_socket` requis sur
  `timed_out_inactivity`/`no_handshake`/`no_request`/`no_interest`) :
  hors scope strict A08 mais même ligne de fix, à reprendre avec A07.
  Fix : ajouter `channel_state[DOWNLOAD] & NETWORK != 0` (ou
  `!disk_blocked` équivalent au sens upstream) à la leg
  `request_timed_out`. Sévérité : **majeure**.
- **D4 — `maybe_unsnub` exécuté avant le tri `is_downloaded`.** Rust
  (`peer_conn.rs:1480-1484`) unsnub tout bloc `was_requested` arrivé
  dans la fenêtre ; upstream (`peer_connection.cpp:2969-2980`)
  n'atteint ce check qu'après le `return` du bras `is_downloaded`
  (`:2932-2966`) — un bloc redondant (picker déjà `finished`, ou
  entrée `timed_out`/`not_wanted`) ne unsnub pas upstream.
  Conséquences : `peer_unsnubbed_alert` parasite + `snubbed = false`
  trop tôt → `desired_queue_size` remonte alors qu'upstream reste à 1
  jusqu'au prochain bloc utile. Fix : ne unsnub que sur le chemin
  écriture — `is_downloaded` étant lu côté torrent dans
  `apply_peer_effects`, déplacer l'émission `Unsnub` dans le bras
  `DiskWrite` quand `!is_downloaded && !is_seed` (la fenêtre
  `now - requested < request_timeout` reste évaluée conn-side au
  dispatch, ex. via un bool passé dans `DiskWrite`). Sévérité :
  **mineure**.
- **D5 — double décrément `num_peers_down_requests` dans le bras
  seed.** `peer_conn.rs:1440-1444` : pop front dlq +
  `counters.add(NumPeersDownRequests, -1)` inline si vidée — fidèle
  au site upstream (`peer_connection.cpp:2878-2881`) ; mais
  `sync_down_requests` (`torrent.rs:3211-3221`) recompte ensuite par
  snapshot (`delta = n - down_request_peers`) et applique le même -1
  une seconde fois → la gauge dérive de -1 à chaque occurrence (pop du
  dernier élément sous seed), jamais rattrapée. Fix : retirer le dec
  inline (`:1442-1444`) — le sync couvre la transition ; garder le
  commentaire upstream pour la sémantique. Sévérité : **mineure**
  (compteur stats, pas de gate comportemental dessus).
- **D6 — queue de `RejectRequest` tronquée (flush
  `send_block_requests` inconditionnel absent).** Upstream termine
  `incoming_reject_request` par `send_block_requests()`
  inconditionnel (`peer_connection.cpp:1682`) : le bloc parole remis
  en tête de `request_queue` et tout reliquat d'entrées non envoyées
  partent dans le même tick. Rust (`peer_conn.rs:1506-1545`) n'émet
  `MaybePick` que sous `request_queue.empty() && download_queue < 2`
  — rien ne flushe une `request_queue` non vide (le drive qui suivra
  passera `num_requests <= 0` si les deux queues sont remplies, et le
  flush ne court qu'au prochain `send_block_requests` déclenché par un
  autre message). Le bras Choke (`:1026-1051`) aggrave : les rejects
  synthétiques non-FAST n'émettent que `AbortDownload` — pas de
  `GracefulCheck`, pas de pick allowed-fast, pas de flush, là où
  upstream exécute la queue complète par entrée. Fix : émettre un
  effet flush (`send_block_requests` seul, sans pick) après tout
  reject valide et dans le bras Choke après la boucle ; rejouer
  `GracefulCheck` par entrée Choke. Sévérité : **mineure** (latence
  d'un tick sur la re-demande, pas de perte).

## Fichiers du fix

- `crates/bitdaemon-session/src/peer_conn.rs` — `dispatch` bras
  `Piece` : `start_receive_piece` (migration `request_queue` → tête
  `download_queue` + `queued_time_critical` ; insertion `not_wanted`
  + `unwanted_block_alert` + `outstanding_bytes` ; D1),
  `validate_piece_request` + disconnect `invalid_piece` (D2), unsnub
  déplacé post-`is_downloaded` (D4), tail Reject/Choke (flush +
  graceful par entrée ; D6), retrait du dec inline bras seed (D5),
  early-out `disconnecting` en tête de bras Piece (miroir
  `peer_connection.cpp:2817`).
- `crates/bitdaemon-session/src/peer_pipeline.rs` — insertion tête +
  flag `not_wanted` + `queued_time_critical` ajusté (D1).
- `crates/bitdaemon-session/src/torrent.rs` — gate `bw_network`/
  `channel_state & NETWORK` sur la leg `request_timed_out` de
  `check_request_timeouts` (D3) ; bras `DiskWrite`/`UnwantedBlock`
  d'`apply_peer_effects` : écriture disque des blocs `not_wanted` non
  downloaded, waste `piece_cancelled`, emission `Unsnub` (D1/D4).
- `crates/bitdaemon-session/src/session.rs` — rien à changer pour le
  gate (l'état `NETWORK` est déjà posé par `engine.rs:3366-3377` et
  lisible via `c.bw.channel_state`) ; vérifier que le `UnwantedBlock`→
  write réutilise `dispatch_disk` tel quel.
- Tests : `crates/bitdaemon-session/tests/test_torrent*.rs` /
  `test_peer*.rs` — régressions : `piece` matchant une requête encore
  en `request_queue` → écrit + requête jamais envoyée ; `piece` non
  sollicité → `unwanted_block_alert` + écriture quand non downloaded,
  waste `piece_cancelled` sinon ; `piece` malformé (hors range /
  start non aligné / length ≠ `to_req` / 0) → disconnect
  `invalid_piece` ; conn `disk_blocked` + `requested_ms` expiré →
  pas de snub ni timeout-mark ; seed + dernier bloc dlq → gauge
  `-1` exactement ; reject avec `request_queue` résiduelle → flush
  immédiat ; choke non-FAST → graceful + re-pick allowed-fast.

## Non-bugs tranchés

- **`m_deferred_send_block_requests`** — upstream coalesce via une
  task postée (`m_deferred_send_block_requests` flag) ; Rust appelle
  `send_block_requests` inline à chaque site. Mono-acteur +
  `send_block_requests` idempotent sur les gates → même effet net,
  seulement moins de coalescing intra-tick (plusieurs flushes
  successifs drainent une queue déjà vide au second appel).
- **`set_endgame(!choked)` dans `drive_requests`** — upstream ne
  touche pas `m_endgame` sous choke dans `request_a_block` ; Rust
  clear. Mais `incoming_choke`/`on_choke_with_aborts` l'a déjà
  effacé (`peer_conn.rs:824-825` = `peer_connection.cpp:1526`) →
  un conn choked a toujours `endgame == false` → no-op net.
- **Gates `add_request`** — `upload_mode`/`disconnecting` absents de
  l'`add_request` Rust : upstream les a mais ils sont inaccessibles
  depuis `request_a_block` (gates amont identiques) — inert.
- **`need_picker` paresseux** — upstream crée le picker à la demande
  dans `request_a_block`/`incoming_piece` ; Rust à
  `files_checked`/métadonnées/resume (`init_picker`
  `torrent.rs:2003-2027`, taille payload réelle — meilleur que
  `num_pieces × piece_length` pour le tail-block). Les call-sites où
  upstream n'a pas de picker (`!valid_metadata`) sont gatés en amont
  des deux côtés ; `picker_gen` couvre les resets.
- **Marquage picker différé post-boucle** — `drive_requests` Rust
  marque `mark_as_downloading` après la boucle de pick (upstream
  marque par bloc dans `add_request`) : `num_peers` par bloc +
  `pick_pieces` anti-doublon intra-passe rendent le fenêtre
  d'écart inobservable sauf pick échouant partiellement (sous-pique
  ≤ 1 bloc, rattrapé au drive suivant).
- **Hooks d'extension consommateurs** — `e->write_request`/`on_piece`
  /`on_reject` qui interceptent et remplacent la logique : surface
  d'extension non portée (scope A02) ; le modèle plugin Rust est
  observe-only — les sites existent.
- **`cancel_block` multi-peers** (`peer_connection.cpp:3049/3070`) —
  inerte post-`mark_as_writing` (l'entrée picker est `writing`,
  `cancel_request` gate sur `is_requested`) ; l'absence d'appel côté
  Rust est donc un no-op équivalent — divergence latente seulement
  si un futur caller annule un bloc encore `requested` en vol.
- **`incoming_piece_fragment` / `m_received_in_piece` /
  `m_receiving_block`** — upstream décrémente `m_outstanding_bytes`
  par fragment socket et trace le bloc en cours de réception ; le
  codec Rust délivre des messages complets → `-len` à la réception
  complète vs par fragment : net-équivalent (+len au `send` identique,
  `peer_connection.cpp:4101`/`4140` = `send_block_requests`
  `peer_pipeline.rs`).
- **`num_peers_down_requests` par réconciliation** — `sync_down_requests`
  (`torrent.rs:3211-3221`, fin de batch `session.rs:7847-7854`)
  reproduit les inc/dec site-par-site en état stable (un conn retiré
  sort du compte = dec upstream au disconnect) — le seul écart
  résiduel est D5 (double-dec bras seed).
- **Leg `piece_timeout` sans `reading_socket`** — conforme :
  upstream ne gate que la leg `m_requested` (`:5080`) ; la leg
  `m_last_piece` (`:5141-5143`) n'a que quota + dlq non vide — le
  Rust (`torrent.rs:7605`) est correct sur cette leg (c'est le
  partage du même `continue` qui fuit le gate sur la mauvaise leg).
- **Sortie slow-start** — `prev + 5000 >= cur` && `!choked` && prev
  > 0 (`torrent.rs:8468-8474` = `peer_connection.cpp:5096-5102`) +
  AIMD `update_desired_queue_size` (`:8489-8490`) + alerte
  `outstanding_request_limit_reached` au seuil `max_out_request_queue`
  (`:8495-8501` = `:5132-5137`).
- **Zero-length `piece`** — upstream déconnecte `invalid_piece` dès
  `start_receive_piece` pour un pair BT (validation `to_req == p`
  échoue, `peer_connection.cpp:2700-2708`) ; le bras BitComet
  `peer_sent_empty_piece` + `incoming_reject_request`
  (`:2856-2866`) n'est atteignable que via
  `web_peer_connection::incoming_piece` (`:1262`) — replié dans D2.
- **`is_seed` leg** — pop front dlq, `piece_seed` waste, aucun unsnub
  ni picker (`peer_conn.rs:1439-1447` = `:2870-2884`) — conforme,
  modulo D5.
- **`exceeded` sémantique** — `has_back_pressure(level + 1)` pré-inc,
  parked-derrière-fence comptés (`disk_io.rs:2359-2427`,
  `back_pressure.rs:143-237`) = `account_pending_write` upstream —
  les deux retournent `level >= max_size` en incluant le write
  traversant.
- **`queued_write_bytes` avant/après le gate `bw_disk`** — upstream
  ordonne gate (3008) puis wq++ (3018) ; Rust wq++ (2392) puis gate
  (2413) — permutation de deux pas indépendants (le check
  `wq - len < max` fait la même comparaison), sans effet observable.
- **`packet_too_large`** — `packet_size - 9 > block_size` au premier
  fragment → disconnect (`peer_conn.rs:1399-1405` =
  `bt_peer_connection.cpp:1039-1046`) — conforme.
- **`fill_send_buffer` au second_tick** (`:5162`) — chemin upload,
  scope A09 ; présence du site notée.
- **`we_are_interested`/quota legs additionnels du ladder** — hors
  A08 (admission/lifecycle), note de chevauchement dans D3.
