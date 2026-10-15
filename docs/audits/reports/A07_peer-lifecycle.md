# Audit A07 — rapport d'analyse

Baseline upstream : `e7381c7bc` (`src/torrent.cpp` — `attach_peer` 8470,
`remove_peer` 6603, `connect_to_peer`/`create_peer_connection` 7995-8220,
`update_peer_port` 11969 ; `src/peer_list.cpp` — `new_connection` 584,
`connect_one_peer`, `connection_closed`, `update_peer_port` 881,
`update_peer` 1058 ; `src/session_impl.cpp` — `incoming_connection` 3079,
`try_connect_more_peers` 4485, `find_disconnect_candidate_torrent` 4922,
`update_connections_limit` 6909 ; `src/peer_connection.cpp` —
`attach_to_torrent` 1278, `init` 750, `connect_failed` 4195,
`disconnect` 4280-4540, `fast_reconnect` 947, `clear_download_queue` 1557 ;
`src/bt_peer_connection.cpp` — `on_connected` 173, pid/self-conn 3558-3592,
ext-handshake `p` 2063 ; `include/libtorrent/peer_connection_interface.hpp:44-46`
pour `disconnect_severity_t`).
Côté Rust : `crates/bitdaemon-session/src/{session,torrent,peer_conn,counters,settings}.rs` ;
`crates/bitdaemon-net/src/{peer_list,torrent_peer,bt_peer_conn,close_reason}.rs` ;
`crates/bitdaemon/src/engine.rs`.

## Verdicts checklist

- **Ordre attach : refcount picker, `set_peer_class`, interested eval,
  bitfield init** — verdict : **conforme** (structure distribuée) —
  preuve : `set_peer_classes` en premier (`session.rs:3645-3648` =
  `peer_connection` ctor → `m_ses.set_peer_classes`) ; l'`init()`
  upstream (`peer_connection.cpp:750-815` : `peer_has` refcount +
  `peer_is_interesting`/`send_not_interested`/`update_interest` +
  dimensionnement `m_have_piece`) est reproduit par message :
  `PeerConn::new(num_pieces)` dimensionne `peer_pieces` dès la création
  (`peer_conn.rs`), le refcount et l'évaluation d'intérêt arrivent avec
  le premier bitfield/`have_all` via `PeerEffect::Refcount*` +
  `update_interest` (`torrent.rs:4092`) — à l'attach, le bitfield
  distant est vide dans les deux modèles, donc même état net. Le flag
  `connections_initialized` gate le refcount-dec du remove
  (`torrent.rs:3243-3255`) exactement comme `ready_for_connections()`
  upstream (`torrent.cpp:6649-6667`). Bitfield/`have_all`/`have_none`/
  super-seed leak + `sent_bitfield` + `DhtPort` envoyés à l'attach
  (`session.rs:4005-4067` = `write_bitfield`/`on_metadata`/`write_dht_port`).
  Extensions `new_peer_connection` instanciées dans `add_peer`
  (`torrent.rs:3197-3201`) — upstream les instancie avant l'insert
  peer-list (`torrent.cpp:8589-8597`) : ordre déplacé mais le plugin
  voit la conn attachée dans les deux cas. `peer_connect_alert`
  déplacé avant les gates → D5.
- **`remove_peer` : abort `!timed_out && !not_wanted`, refcounts,
  stats fold, gauges** — verdict : **déviation** (D8, D10, D3) —
  preuve conformité : refcount picker verbatim (seed →
  `dec_refcount_all`, sinon par pièce, gate `connections_initialized`,
  `torrent.rs:3238-3255` = `torrent.cpp:6653-6667`) ; `prev_amount_*`
  accumulés en KiB (`torrent.rs:3315-3322` = `:6683-6685`) ; canaux
  `m_stat` repliés dans les accumulateurs torrent (`:3324-3333`) ;
  gauges `interesting`/`peer_interested`/`unchoke`(+exempt)/`peer_choked`/
  `endgame`/`bw_disk` (`:3268-3300`) ; `optimistically_unchoked` reset +
  gauge (`:3306-3311` = `:6677-6686`) ; `num_seeds` (`:3312-3314` =
  `:6690-6693`) ; `update_want_peers` (`:3334`) — `update_want_tick`
  est un prédicat calculé, la suppression suffit (`:3335-3336`).
  Déviations : l'abort des blocs en vol n'a **pas** le gate
  `!timed_out && !not_wanted` (D8) ; pas de `trigger_unchoke`/
  `trigger_optimistic_unchoke` immédiat (D10) ; `prev_amount` n'est
  jamais restauré à la conn (D3) ; `m_outgoing_pids.erase` N/A sous
  le modèle pid-unique (D7) ; `m_peers_to_disconnect` différé →
  suppression immédiate (non-bug, acteur mono-fil).
- **Taxonomie disconnect : reason + `failed`, `get_peer_list_entry`
  failcount vs clean** — verdict : **conforme** (avec réserves
  mineures) — preuve : `DisconnectSeverity` 0/1/2 verbatim
  (`bt_peer_conn.rs:56-70` = `peer_connection_interface.hpp:44-46`) ;
  `m_failed` sticky via `c.failed |= sev > Normal` à l'émission
  (`torrent.rs:4079`) puis `conn_failed || sev > Normal` →
  `connection_closed(failed)` (`session.rs:4177-4186`) →
  `failcount += 1` cap 31 (`peer_list.rs:605-607`) — sépare bien
  clean (`Normal`, pas de bump) vs failed comme upstream
  (`peer_connection.cpp:4338` `m_failed = error > normal` ;
  `peer_list.cpp` `connection_closed` → `if (c.failed()) ++failcount`).
  Exclusivité alertes : `peer_error` → `PeerError` + `error_peers`,
  jamais `PeerDisconnected` (`session.rs:4236-4256` =
  `peer_connection.cpp:4477-4490` : `error > failure || socks` →
  error, `≤ failure` → disconnected ; le catch-all Rust route les
  erreurs socks/proxy en `PeerError`). Compteurs par erreur :
  `disconnected_peers` + jambes io-kind (`connreset`/`eof`/
  `connrefused`/`connaborted`/`notconnected`/`perm`/`broken_pipe`/
  `addrinuse`/`transport_timeout`) + `disconnect_reason_counter` par
  nom (`session.rs:4196-4234` = `peer_connection.cpp:4350-4404`) ;
  `sev > normal` → `error_tcp`/`error_utp` + `error_out`/`error_in`
  (`:4223-4234`). `get_peer_list_entry` → `full_peer_list` expose le
  `TorrentPeer` brut (`failcount`, `banned`, `source`…)
  (`torrent.rs:6464-6469` = `torrent.cpp:7806-7809`). Manques :
  `ban_peer` sur `self_connection` (D11) ; `set_close_reason` jamais
  appelé (D15) ; `connect_timeouts` incomplet (D4).
- **Half-open (`m_half_open`), backoff exponentiel, priorité
  récents** — verdict : **déviation** (D4, D12) — preuve
  conformité : `NumPeersHalfOpen` inc à l'accept/dial et déc à
  l'attach/drop dans l'engine (`engine.rs:2654-2659, 2378-2380,
  3189-3203` = `peer_connection` ctor/`on_connection_complete`/
  `connect_failed`/`disconnect`) ; `last_connected` stampé au dial
  (`torrent.rs:2932` = `torrent.cpp:8017`), à l'attach sauf
  fast-reconnect (`peer_list.rs:575-577` = `peer_list.cpp:854-855`),
  re-stampé à `connection_closed` (`peer_list.rs:602-604`) ; cooldown
  `(failcount+1)*min_reconnect_time` verbatim
  (`peer_list.rs:712-717` = `find_connect_candidates` upstream) ;
  `is_connect_candidate` verbatim (`peer_list.rs:624-631`) ;
  `connect_one_peer` + cache + sweep à 95% de `max_peerlist_size`
  (`peer_list.rs:656-795`) ; ordre BEP-40 `compare_peer` (récents
  privilégiés via `last_connected` upstream — même tri) ; échelle
  `>10%` connecting → éviction du half-open le plus ancien
  (`session.rs:3893-3936` = `torrent.cpp:8567-8586`,
  `connecting_time_compare` = `min_by_key(connect_ms)`).
  Déviations : `peer_connect_failed` bump `failcount` sur les legs
  uTP/refus où upstream non (D4) ; pas de rewind `fast_reconnect`
  ni de re-dial immédiat (D4) ; `num_seeds` pas bumpé pendant la
  fenêtre dial (D12) ; `connect_timeouts` partiel (D4) ;
  `half_open_limit` = deprecated upstream (no-op) → non-bug.
- **Admission entrante : silent drop no-torrent, dup, conflits
  peer-id** — verdict : **déviation** (D2, D5, D6, D7) — preuve
  partielle : cap global + slack + `AcceptExceeded`/`m_exceeded_limit`
  (`session.rs:5128-5164` = `session_impl.cpp:3200-3260`) ;
  `IncomingConnections` + `IncomingConnection` (`:5152-5157` =
  `:3256-3260`) ; unknown-ih → `WrongInfoHash` +
  `verify_secret_id` → `ban_ip` (`engine.rs:3185-3191` =
  `peer_connection.cpp:1325-1338`, sans l'alerte
  `invalid_info_hash`@failure) ; pid-dedup `(rpid < peer_id) ==
  outgoing` (`session.rs:3757` = `bt_peer_connection.cpp:3568` —
  même formule, accord bilatéral préservé) ; BitComet cap 50
  (`:3793-3803` = `:3581-3585`). Déviations : gates de transport/
  interface/ip_filter/`non_filter_torrents`/`has_incoming_connections`
  manquants (D5) ; gates torrent `paused`/`graceful`/`ssl`/auto-resume
  manquants (D6) ; dedup `new_connection` same-IP/direction/ban/
  endpoint-self-conn absent (D2) ; pid unique vs `m_outgoing_pids`
  (D7) ; ordre éviction-globale avant gates (D9).
- **`ip_filter`/`port_filter` sur conns actives +
  `peer_blocked_alert`** — verdict : **conforme** (avec une réserve)
  — preuve : teardown conn AVANT le sweep liste (entry encore
  résolvable pour `connection_closed`), puis `apply_ip_filter` /
  sweep port, `PeerBlocked` par adresse, tout en sévérité `Normal`
  → pas de bump failcount (`session.rs:4641-4708, 4721-4781` =
  `peer_list::apply_ip_filter`/`apply_port_filter` +
  `peer_blocked_alert(ip_filter|port_filter)` upstream) ; gate
  `t.apply_ip_filter` à l'attach (`session.rs:3832-3848` =
  `torrent.cpp:8521-8531`) ; gates engine au dial (`engine.rs:2289`)
  et à l'accept (`engine.rs:3126-3134`) ; `ban_ip` rebuild+reshare
  (`session.rs:5171-5189`). Réserve : `non_filter_torrents` jamais
  maintenu → le skip inbound du filtre quand un torrent opte out ne
  peut pas exister (D5) ; le refus de dial filtré brûle `failcount`
  (D4).
- **Web seeds `WEB_SEED_PEER`** — verdict : **conforme** —
  preuve : `u64::MAX` confiné à l'attribution de blocs picker,
  jamais inséré dans `t.peers`/`peer_list` ; `penalize_peers` le
  saute (`torrent.rs:3388-3395`) ; `web_peer`-style request handling
  keyé dessus (`:5243-5255`) ; `is_connect_candidate` exclut le flag
  `web_seed` (`peer_list.rs:627` — parité défensive, les entrées ne
  le portent jamais) ; `set_seed` saute `num_seeds` pour lui
  (`peer_list.rs:519-522` = `peer_list.cpp:522-525` upstream).

## Déviations

- **D1 — `update_peer_port` jamais appelé.** Le handshake étendu `p`
  est stocké dans `conn.peer_listen_port`/`received_listen_port`
  (`peer_conn.rs:1614-1619`) mais `PeerList::update_peer_port`
  (`peer_list.rs:452-503` — port verbatim incluant collision →
  `duplicate_peer_id` + `erase_peer`) n'a **aucun appelant** ;
  upstream l'invoque à l'ext-handshake (`bt_peer_connection.cpp:2063`
  → `torrent::update_peer_port` `torrent.cpp:11969` →
  `peers_erased` + `update_want_peers`). Conséquences : l'entrée
  swarm d'une conn entrante garde son port source éphémère et reste
  non-`connectable` pour toujours ; pas de merge avec l'entrée
  dialable tracker/PEX du même endpoint → deux entrées pour le même
  pair (une entrante, une dialable) → double-dial possible ;
  `peer_listen_port` n'est consommé que par ut_pex `send_peer`
  (`torrent.rs:1866-1872`). Fix : `PeerEffect` dédié depuis
  `process_extended_handshake` → `peer_list.update_peer_port` +
  `peers_erased`/`update_want_peers` + disconnect au retour `false`.
  Sévérité : état swarm / dialabilité.
- **D2 — `new_connection` sans les règles same-IP / direction /
  self-conn.** `peer_connected_addr` + `add_incoming`
  (`torrent.rs:3057-3100`, `peer_list.rs:428-446`) ne traitent que
  l'insert ; upstream applique (`peer_list.cpp:640-802`) :
  `i->banned` → `peer_banned` reject ; endpoint self-conn
  (`i->connection->remote() == c.local_endpoint() || local == remote`)
  → `self_connection`@failure des deux conns ; direction identique →
  `duplicate_peer_id` du nouveau ; directions opposées → résolution
  par ports (`our_port` local-out vs `other_port` cible, égalité →
  random, `disconnect1 &= !i->connection->failed()`), `m_locked_peer`
  pendant la déconnexion. Sous `allow_multiple_connections_per_ip =
  false` (défaut des deux côtés) deux clients distincts derrière la
  même IP peuvent s'attacher, et la même paire de machines garde deux
  conns croisées ; une entrée `banned` peut rattacher. Le dedup pid
  (`session.rs:3746-3790`) ne couvre que le cas même-pid.
  Conséquence secondaire : `set_connected` écrase `p.connection` →
  le premier `connection_closed` efface le flag alors que l'autre
  conn vit encore. Fix : porter les quatre legs dans
  `peer_connected_addr` (ou `add_incoming`) + `locked_peer` au
  disconnect. Sévérité : comportemental — conns redondantes,
  bannis réadmis, état `connection` faux.
- **D3 — `prev_amount` jamais restauré dans les stats conn.**
  Upstream `new_connection` fait `c.add_stat(prev*1024)` puis zéro
  (`peer_list.cpp:853-856`) et `create_peer_connection` de même
  (`torrent.cpp:8204-8207`) → `total_download/upload` du
  `peer_info` = cumul de vie pendant la conn. Rust accumule au
  disconnect (`torrent.rs:3317-3322`) sans jamais restaurer →
  `peer_info` sous-estime pour un pair reconnecté ; de plus
  `TorrentPeer::total_download` debug-assert `prev == 0` quand
  `connection.is_some()` (`torrent_peer.rs:241-247`) — un reconnect
  avec `prev != 0` la ferait tirer en debug si `total_download` est
  appelé sur l'entrée. Fix : à `peer_connected_addr`, charger
  `prev_amount_*` dans `conn.stat_*` et les mettre à 0.
  Sévérité : stats/rapport + assert debug.
- **D4 — `peer_connect_failed` : `failed=true` inconditionnel, pas
  de fast-reconnect, refus de dial bump `failcount`,
  `connect_timeouts` partiel.** (a) `connection_closed(.., was_utp,
  true, ..)` (`torrent.rs:3041-3044`) — upstream `connect_failed`
  disconnecte le leg uTP à `normal` (pas de bump) puis poste un
  `connect_to_peer(pi, ignore_limit)` immédiat
  (`peer_connection.cpp:4230-4250`) : le bump failcount Rust punit
  l'entrée pour un fallback transport. (b) `fast_reconnect(true)`
  upstream rembobine `last_connected -= min_reconnect_time *
  max_failcount` + `fast_reconnects++` (≤15) (`peer_connection.cpp:947-962`)
  ; Rust garde juste le stamp — le retry TCP attend le cooldown
  complet. (c) Les refus de dial `ip_filter` (`engine.rs:2289-2297`),
  `!utp_wanted && !tcp_ok` (`:2346`), `instantiate` `None` (`:2363`)
  passent par `peer_connect_failed` → bump ; upstream `connect_to_peer`
  `return false` ne bump pas (`torrent.cpp:8050-8067` — l'entrée
  reste candidate, throttlée par `last_connected`). (d) Upstream
  bump `connect_timeouts` à **tout** `connect_failed`
  (`peer_connection.cpp:4208`) ; Rust ne le couvre que via
  `disconnect_reason_counter("timed_out_no_handshake")`. Fix :
  `peer_connect_failed` prend la sévérité/flag failed en paramètre
  (uTP → normal) + chemin non-fail pour les refus ; porter le rewind
  + `fast_reconnects` (champ existant `torrent_peer.rs:128`) +
  re-dial TCP immédiat ; bump `connect_timeouts`.
  Sévérité : dial-backoff + compteur.
- **D5 — Gates `incoming_connection` manquants.**
  `on_incoming_connection` (`session.rs:5128-5165`) n'a que cap+slack,
  pas-torrent, compteur+alerte, exceeded. Upstream
  (`session_impl.cpp:3079-3260`) a en plus : `!enable_incoming_utp`
  → `peer_blocked(utp_disabled)` et `!enable_incoming_tcp` →
  `peer_blocked(tcp_disabled)` (uTP vérifié build-time seulement,
  `engine.rs:428` ; TCP jamais) ; `verify_incoming_interface` +
  `verify_bound_address` → `peer_blocked(invalid_local_interface)`
  quand `outgoing_interfaces` non vide ; `!is_local(addr)` →
  `has_incoming_connections = 1` (compteur jamais set) ;
  `non_filter_torrents == 0 && ip_filter blocked` →
  `peer_blocked(ip_filter)` avant accept (le compteur n'est jamais
  maintenu non plus — `counters.rs:557` défini, zéro inc) ;
  `m_torrents.empty()` drop **inconditionnel** (Rust : avec
  `incoming_starts_queued_torrents=true` et zéro torrent, la gate
  `any(!is_paused)` est court-circuitée → accepte) ; ordre —
  upstream teste `torrents.empty` **avant** la limite (drop
  silencieux), Rust teste la limite d'abord → `PeerDisconnected`
  posté là où upstream se tait. L'engine compense partiellement :
  `ip_access` à l'accept (`engine.rs:3126-3134`) mais appliqué
  **sans** le skip `non_filter_torrents` → un IP filtré pour les
  torrents filtrants est aussi refusé au torrent exempté. Fix :
  porter les gates + compteurs dans `on_incoming_connection` et
  l'accept engine ; maintenir `non_filter_torrents` à
  `set_apply_ip_filter`/add/remove torrent. Sévérité : admission +
  compteurs.
- **D6 — Gates `attach_to_torrent` torrent-level manquants.**
  `peer_connected` n'a pas (`session.rs:3832-3874` vs
  `peer_connection.cpp:1337-1390`) : `t->is_paused() ||
  is_aborted() || graceful_pause()` → `torrent_paused` à sévérité
  **peer_error** (alerte `peer_error` + `error_peers` + failcount) —
  un torrent en pause/queued/drain-graceful accepte les conns
  entrantes ; `t->is_paused() && is_auto_managed() &&
  incoming_starts_queued_torrents && !is_aborted()` → `t->resume()`
  — pas d'auto-resume sur conn entrante ; `is_ssl_torrent` →
  `requires_ssl_connection`/`invalid_ssl_cert` pour conns non-SSL —
  non gaté ; `t->is_aborted()` Rust poste `"session_closing"` alors
  qu'upstream réserve ce nom à `m_ses.is_aborted()` (le torrent
  avorté prend `torrent_paused`@peer_error). Fix : porter les gates
  dans `peer_connected` (resume auto-managed, paused/graceful →
  `PeerError`, ssl → `DisconnectPeer` Normal avec la bonne raison).
  Sévérité : comportemental — accepte des conns qu'upstream tue,
  alerte/sévérité divergentes.
- **D7 — Pid unique de session vs `generate_peer_id` par conn.**
  Upstream génère un pid par conn (`create_peer_connection`
  `torrent.cpp:8152`, inbound pack `session_impl.cpp:3302`), un pid
  par torrent pour le tracker (`m_peer_id` `torrent.cpp:234`),
  `m_outgoing_pids` suit les pids dial (`:8197`, erase
  `:6610-6613`), `is_self_connection` = membership
  (`torrent.cpp:2227`). Rust : `ses.peer_id` unique pour handshake
  **et** annonces → `is_self_connection` dégradé à
  `rpid == self.peer_id` (`session.rs:3807`). Conséquences
  wire-visibles : le remote voit le même pid sur toutes nos conns —
  son propre `duplicate_peer_id` tue nos conns parallèles (TCP+uTP
  ou re-dial concurrent) qu'upstream laisse vivre (pids distincts) ;
  la formule `(rpid < our) == outgoing` reste accord-compatible, mais
  la détection self-conn change de portée (Rust attrape aussi les
  échos d'pid sur autres torrents — sur-ensemble). Sévérité :
  wire-visible / modèle identité. Fix (majeur) : pid par conn à la
  `generate_peer_id` + `outgoing_pids` par torrent ; pid tracker par
  torrent séparé.
- **D8 — `remove_peer` : `abort_download` sans le gate
  `!timed_out && !not_wanted`.** `outstanding()` inclut les entrées
  déjà marquées (`peer_conn.rs:1836-1843`) et `remove_peer` aborte
  tout (`torrent.rs:3259-3264`) ; upstream gate dans
  `clear_download_queue` et la boucle `m_request_queue`
  (`peer_connection.cpp:1565-1570, 4499-4511`). Les entrées
  timed_out/not_wanted ont déjà été libérées au mark-time
  (`torrent.rs:7697`, `peer_conn.rs:913`) → un re-abort décrémente
  `num_peers`/`peer` d'un bloc repris par un autre pair → libération
  prématurée → re-pick duplicata/waste. Fix : filtrer
  `outstanding()` sur les flags (ou itérer les queues avec flags).
  Sévérité : cohérence picker.
- **D9 — Éviction globale avant les gates torrent.** Rust évalue
  `exceeded_limit` + `find_disconnect_candidate_torrent` avant les
  gates (`session.rs:3655-3728` avant `3832+`) ; upstream attache
  d'abord puis évalue `m_exceeded_limit` (`peer_connection.cpp:1387`
  puis `:1410-1446`). Une conn slack qu'un gate torrent rejettera
  (filtre, paused, ssl) déclenche quand même une éviction valide →
  kill d'une conn légitime gratuit. Comparator lui-même conforme
  (`session.rs:1423-1429` = `session_impl.cpp:4904-4933` :
  non-vide → finished → plus gros swarm). Fix : déplacer le bloc
  `exceeded_limit` après les gates d'attach. Sévérité : mineure.
- **D10 — Pas de `trigger_unchoke`/`trigger_optimistic_unchoke`
  immédiat.** Upstream `remove_peer` re-pique dès qu'un slot
  d'upload se libère (`torrent.cpp:6672-6676, 6680-6686` —
  `--m_num_uploads` + `trigger_unchoke()` ; optimistic →
  `trigger_optimistic_unchoke()`). Rust se contente des gauges et
  attend le `unchoke_tick` périodique (`torrent.rs:3301-3303`,
  `session.rs:7372-7404`) — slot vacant jusqu'à `unchoke_interval`.
  Fix : émettre un effet trigger-choker depuis `remove_peer` (et
  `ChokePeer` route déjà existante peut servir de modèle).
  Sévérité : latence upload.
- **D11 — `self_connection` ne banit pas l'entrée.** Upstream
  `disconnect` → `ec == self_connection && m_peer_info && t` →
  `t->ban_peer(m_peer_info)` (`peer_connection.cpp:4456-4459`) →
  `banned` → plus jamais candidat + refusé à l'accept
  (`peer_banned`). Rust : `DisconnectPeer(Normal)` seul
  (`session.rs:3807-3819`) → l'entrée reste dialable → auto-dial
  répété possible (tracker qui nous annonce). Fix : `ban_peer` sur
  le flag. Sévérité : mineure (boucle de redial).
- **D12 — Compteurs/flags session.** `has_incoming_connections`
  jamais set (upstream `session_impl.cpp:3166-3168` : `!is_local`
  → set 1) ; `non_filter_torrents` jamais maintenu (→ D5) ;
  `m_has_incoming` torrent : flag sticky posé à l'attach
  (`torrent.cpp:8510`) — Rust le calcule live
  `peers.any(!outgoing)` (`torrent.rs:8172`) → diffère après la
  dernière déconnexion entrante ; `num_seeds` : upstream bump à
  `create_peer_connection` pour les dials sur entrées seed
  (`torrent.cpp:8200-8204`) — Rust bump à l'attach seulement
  (`torrent.rs:3095-3097`) → fenêtre de dial non comptée (net ok
  après handshake, gauge divergente pendant le dial).
  Sévérité : compteurs.
- **D13 — Formes d'alertes réduites.** `Alert::PeerConnect{ih,
  remote}` vs upstream `(handle, endpoint, pid, socket_type,
  direction)` — pas de direction/pid/socket_type, émis avant les
  gates pour les deux directions alors qu'upstream poste
  post-attach (in, `peer_connection.cpp:1395`) et au dial (out,
  `:453`) → `PeerConnect` supplémentaires sur les conns rejetées ;
  `PeerDisconnected{ih, reason}` vs `(handle, ep, pid, op,
  socket_type, ec, close_reason)` ; `PeerBlocked` raison en string
  libre vs enum upstream (`ip_filter`, `port_filter`,
  `tcp_disabled`, `utp_disabled`, `i2p_mixed`,
  `invalid_local_interface`, `src_missing`, `read_uint16/32_failed`,
  `dht_port`, `invalid`, `banned_by_port_filter`…) — le vocabulaire
  `tcp_disabled`/`utp_disabled`/`invalid_local_interface` est
  inutilisable (→ D5) ; `invalid_info_hash` non posté sur l'ih
  inconnu (engine silent vs `disconnect(invalid_info_hash, failure)`
  upstream). Sévérité : surface alerte (pas de wire).
- **D14 — `TorrentPaused` posté à `do_pause` au lieu du dernier
  conn.** Sous `graceful_pause` upstream diffère l'alerte à
  `on_remove_peers` quand `m_connections.empty()` → `set_paused`
  (`torrent.cpp:6727-6736`) ; Rust poste à `do_pause` immédiatement
  (`session.rs:3144-3146`). Sévérité : timing alerte.
- **D15 — `close_reason` jamais propagé à la socket.**
  `error_to_close_reason` + plomberie uTP existent
  (`net/close_reason.rs`, `utp_socket.rs:598-733,1575-1580`) mais
  aucun appelant ne pose la raison au disconnect — upstream fait
  `set_close_reason(m_socket, error_to_close_reason(ec))` dans
  `disconnect` (`peer_connection.cpp:4290`) et le remote lit
  `their_close_reason`. Fix : setter + appel au teardown pour les
  conns uTP. Sévérité : wire uTP (info de fermeture absente).

## Fichiers du fix

- `crates/bitdaemon-session/src/session.rs` — gates
  `on_incoming_connection` (D5), ordre éviction (D9), gates
  paused/graceful/ssl/auto-resume à l'attach (D6), self-conn ban
  (D11), alertes (D13), pid model (D7).
- `crates/bitdaemon-session/src/torrent.rs` — gate abort dans
  `remove_peer` (D8), `prev_amount` restore (D3),
  `peer_connect_failed` legs (D4), `m_has_incoming` sticky (D12),
  alert graceful (D14), trigger unchoke (D10), `attach_to_torrent`
  resume/paused (D6).
- `crates/bitdaemon-session/src/peer_conn.rs` — `PeerEffect` pour
  `update_peer_port` (D1) ; queues pour le gate abort (D8).
- `crates/bitdaemon-net/src/peer_list.rs` — legs `new_connection`
  banned/self-conn/direction-port + `locked_peer` (D2) ;
  `update_peer_port` devenu callé (D1) ; rewind `fast_reconnect` +
  compteur `fast_reconnects` (D4) ; champ `connection` à épreuve
  des conns multiples (D2).
- `crates/bitdaemon/src/engine.rs` — `enable_incoming_tcp`/utp
  runtime (D5), refus de dial non-fail (D4), retry TCP immédiat
  post-uTP (D4), `ip_filter` + `non_filter_torrents` à l'accept
  (D5), `peer_connect_alert` au dial (D13), `invalid_info_hash`
  alert (D13).
- `crates/bitdaemon-session/src/counters.rs` +
  `session.rs`/`torrent.rs` — `non_filter_torrents`,
  `has_incoming_connections` (D12).
- `crates/bitdaemon-net/src/utp_socket.rs` /
  `bt_peer_conn` teardown — `set_close_reason` (D15).
- Tests : `crates/bitdaemon-session/tests/test_session*.rs`,
  `test_torrent*.rs`, `test_peer_list*.rs` — régressions :
  update_peer_port merge + collision-disconnect, same-IP dedup
  direction/port, banned reattach refusé, abort gate double-free,
  attach paused → peer_error, self-conn ban, uTP-fail pas de
  failcount, `prev_amount` restauré, `non_filter_torrents` skip.

## Non-bugs tranchés

- **Gate `m_paused` inbound** — répliqué fonctionnellement :
  `set_session_paused` propage `session_paused` sur tous les
  torrents (`session.rs:4797-4824`) → `any(!is_paused)` faux →
  `Reject` silencieux avant compteurs (`:5147-5151`) — même drop
  silencieux qu'upstream `m_paused` (`session_impl.cpp:3092`), à
  une étape de distance.
- **`m_abort` session** — pas de flag : l'actor/engine meurt avec
  la session ; les accepts s'arrêtent structurellement. L'équivalent
  par-torrent `t.aborted` existe et est gaté (`session.rs:3863`,
  nom d'erreur légèrement différent → D6).
- **`peer_not_constructed` (`m_ses.has_connection`)** — garanti
  par construction : toute conn passant à `peer_connected` est
  enregistrée engine.
- **`want_on_unknown_torrent`** — aucun plugin Rust ne l'annonce
  (`session.rs:5144-5146` le documente) ; upstream n'accepte qu'avec
  l'extension → parité.
- **`half_open_limit`** — deprecated upstream : setter no-op,
  getter fixe `8`, `DEPRECATED_SET(half_open_limit, 0)`
  (`session_handle.cpp:1102-1134`, `settings_pack.cpp:314`) ; le
  champ Rust inutilisé est la parité correcte. La vraie borne
  upstream = `connection_speed`/tick, couverte.
- **`m_peers_to_disconnect` différé** — `on_remove_peers` upstream
  (`torrent.cpp:6705-6740`) reporte `remove_connection` +
  `close_connection` pour itération-sûreté ; suppression immédiate
  sous acteur mono-fil = même effet net.
- **`peers_erased` / `m_peer_info` dangling** — les conns Rust ne
  stockent pas de pointeur entrée (re-lookup par endpoint via
  `peer_index`) → pas de refs pendantes ; effet équivalent
  (`peer_index → None` quand l'entrée est effacée = `peer_info_struct()
  == nullptr`).
- **Ordre `connection_closed` vs `remove_peer`** — upstream l'appelle
  à l'intérieur de `remove_peer` (`torrent.cpp:6695-6700`), Rust
  avant (`session.rs:4181-4189`) — synchrone mono-fil, aucun état
  lu entre les deux.
- **`PeerGone::Disconnected` vs `disconnect(ec,op,sev)`** — reason
  string `&'static` joue le rôle du `error_code` pour les compteurs
  par nom ; `None` (teardown engine) est gardé par `had` → pas de
  double alerte/compteur.
- **Dedup pid** — formule `(rpid < our) == outgoing` identique aux
  deux bouts → les deux côtés choisissent le même perdant ; le
  modèle pid-unique dévie ailleurs (D7), pas ici.
- **`num_connections()` / `num_peers_connected`** — recompte
  `attached + NumPeersHalfOpen` (`session.rs:1157-1163`,
  `sync_connected_peers` `:4109`) ≈ `m_connections.size()` +
  gauge inc/dec upstream ; bucketing conn-in-handshake différent
  mais comparaisons identiques.
- **`update_peer` (re-annonce)** — verbatim
  (`peer_list.rs:260-293` = `peer_list.cpp:1058-1097` : connectable,
  port, source |=, tracker failcount−1, `maybe_upload_only` seulement
  si `!connection`, pex flags, candidats delta).
- **`update_peer_port` collision `allow_multiple`** — le code de
  merge existe verbatim (`peer_list.rs:452-503`) ; c'est l'appelant
  qui manque (→ D1).
- **Websocket/rtc/i2p** — `rtc`/`i2p` conns : upstream sans
  `TORRENT_USE_I2P`/RTC compile-out ces branches ; le modèle Rust
  n'a pas de transport i2p → `p.ip() == None → inc_failcount` est
  le seul écart résiduel (folded dans D4/D13 par l'absence
  d'`i2p_alert`).
- **Alerte `TorrentPaused` checking** — `post_alert` différé au
  dernier `checking_pieces` (`session.rs:3069-3075`) conforme.

## Résultat

### Déviations corrigées

- **D1** — `update_peer_port` appelé depuis le handshake étendu `p`
  (BEP-10) via `PeerEffect::ListenPort` ; la lookup conn↔entrée passe
  par `conn_key`, robuste à la réécriture de port.
- **D2** — `peer_list::new_connection` porté à l'attach entrant :
  banned → `peer_banned`, self-conn par endpoint (incl. local endpoint
  des dials en vol via `dial_locals`), dedup same-direction, règle
  cross-direction par comparaison de ports (tiebreak `random(1)`),
  `m_locked_peer` autour du teardown du perdant.
- **D3** — `prev_amount_*` restaurés à la création de conn
  (`create_peer_connection`) et repliés dans les accumulateurs du
  torrent à la fermeture.
- **D4** — `peer_connect_failed` upstream : échec uTP →
  `supports_utp` clear, `fast_reconnect` armé, `connection_closed`
  normal (pas de failcount), retry TCP immédiat du même peer via
  `pending_out`. Refus de dial (ip_filter, transport absent) gardent
  leur leg séparé.
- **D5** — `on_incoming_connection` reproduit l'ordre upstream :
  paused → enable tcp/utp (`peer_blocked`) → interface sortante +
  bound address → `has_incoming_connections` → ip_filter
  (`non_filter_torrents` exempte la session) → torrents vides → cap
  `connections_limit` + `slack` → `incoming_starts_queued_torrents` /
  torrent actif → compteur `incoming_connections` + alerte.
- **D6** — attach par torrent : auto-resume paused+auto_managed,
  rejet `paused`/`aborted`/`graceful_pause` à `torrent_paused`
  peer_error, gate SSL `requires_ssl_connection`.
- **D7** — `generate_peer_id` par connexion sortante,
  `outgoing_pids`/`dial_pids` par torrent, fallback session-pid pour
  la détection self-conn.
- **D8** — `remove_peer` n'aborte au picker que les blocs
  `!timed_out && !not_wanted` (double-release de `num_peers`
  impossible).
- **D9** — l'éviction `find_disconnect_candidate_torrent` +
  `lowest_ranking_peer` tourne après les gates d'attach (conforme à
  `exceeded_limit` upstream).
- **D10** — `triggered_unchoke`/`triggered_optimistic` consommés par
  `unchoke_tick` au prochain passage.
- **D11** — `self_connection` banne l'entrée par index (pas par
  endpoint — `update_peer_port` peut l'avoir réécrit) et tue les deux
  legs à sévérité failure.
- **D14** — `GracefulPauseDone` route la complétion : `do_pause` ne
  poste `TorrentPaused` que quand le dernier pair a drainé.
- **D15** — `reason → close_reason_t` mapper + propagation jusqu'au
  socket uTP (`set_close_reason` à chaque tour du socket pump).

### Tests ajoutés

- `crates/bitdaemon-session/tests/test_peer_lifecycle_a07.rs` — 13
  cas : gates paused/transport/interface/ip_filter-`non_filter_torrents`/
  empty/cap, banned, self-conn via `dial_locals`, résolution
  cross-direction dans les deux sens, pids distincts par dial +
  lifecycle `outgoing_pids`, fallback uTP sans failcount + retry
  `pending_out`, `conn_key` survivant à `update_peer_port`, drain
  graceful pause, gate d'abort `!timed_out`.
- `crates/bitdaemon-net/tests/test_peer_list.rs` — legs
  `new_connection` (banned, self-conn endpoint, same-direction,
  cross-direction port rule, locked_peer).
- `test_connect_pump`/`test_session`/`test_bandwidth`/
  `test_dht_results`/`test_magnet`/`test_swarm_list`/
  `test_engine` — fixtures migrées : signatures à 5 tuples, conns
  multi-IP (dedup same-IP actif), boost désactivé dans les tests de
  dial manuel, mask d'alertes activé, magnet resumed avant attach
  (le gate paused upstream le refuse sinon).

### Livraison

- Branche `audit/07-peer-lifecycle`, worktree `LibtorrentRust-a07`.
- Commit apply : `05fbb80`
  `fix(peer-lifecycle): upstream-fidelity apply of audit A07`
- PR : à renseigner au merge.
- Fragment changelog : `docs/audits/changelog/A07.md`.
