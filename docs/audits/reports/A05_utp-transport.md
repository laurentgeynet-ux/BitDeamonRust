# Audit A05 — rapport d'analyse (re-audit, passe 3)

Troisième passe de l'audit A05. La première passe a produit 13
déviations (D1–D13, corrigées sur `audit/05-utp-transport`) ; la
seconde a listé 8 résidus (R1–R8). Cette passe re-vérifie `main`
contre upstream : **R4 et R5 sont résolus**, R1/R2/R3/R6/R7/R8 tiennent
encore, et 4 constats nouveaux (R9–R12) s'ajoutent.

Baseline upstream : `e7381c7bc` (`src/utp_stream.cpp`,
`src/utp_socket_manager.cpp`, `include/libtorrent/aux_/utp_stream.hpp`,
`include/libtorrent/aux_/utp_socket_manager.hpp`,
`src/packet_buffer.cpp`, `src/timestamp_history.cpp`,
`aux_/sliding_average.hpp`, `src/settings_pack.cpp` pour les défauts,
`src/session_impl.cpp` pour le dispatch UDP/tick).
Côté Rust : `crates/bitdaemon-net/src/utp.rs`, `utp_socket.rs`,
`utp_stream.rs`, `packet_buffer.rs`, `transport.rs`, `close_reason.rs` ;
`bitdaemon-common/src/{timestamp_history,sliding_average}.rs` ;
`bitdaemon-session/src/{settings,session_params,counters}.rs` ;
`bitdaemon/src/engine.rs` pour l'intégration.

## Verdicts checklist

- Packet types : ST_DATA/FIN/STATE/RESET/SYN — header 20B exact —
  verdict : **conforme** — `UtpHeader::encode`/`parse` (`utp.rs:83-118`)
  = `utp_header` upstream (big-endian identique) ; `UtpType` 0-4,
  version == 1 exigée ; chaîne d'extensions `UtpExtensions`
  (`utp.rs:124-160`) = walk `ptr`/`ph->extension` upstream (SACK id 1,
  close-reason id 3, id 2 déprécié-skippé). **Ordre de chaîne vérifié
  identique** : tête = SACK si présent sinon close-reason
  (`utp_socket.rs:736-742,806-824` = `utp_stream.cpp:1622-1623,
  1701-1716` — le next-type de l'entrée SACK pointe close-reason,
  l'entrée close-reason termine par 0). Ids : sortant `send_id=random,
  recv_id=send_id-1` ; entrant `send_id=syn_id, recv_id=+1`
  (`utp_stream.rs:215` = `utp_socket_manager.cpp:303-307` via
  `m_new_connection`) ; le SYN porte `m_recv_id`
  (`utp_socket.rs:572` = `utp_stream.cpp:1000`), tous les autres
  paquets `m_send_id` (`:750` = `:1624`), le RESET `m_send_id` +
  `seq` aléatoire (`:601-612` = `:1120-1145`).
- State machine : SYN_SENT → CONNECTED → FIN_SENT, syn retransmit,
  connect timeout — verdict : **conforme** — `UtpState` ordonné comme
  `state_t` ; kill `!m_confirmed && num_timeouts > 0` et
  `num_timeouts > num_resends` (`utp_socket.rs:1800-1807` =
  `utp_stream.cpp:3423-3429`) ; caps `syn/fin/num_resends` par état
  (`:1841-1845` = `:3495-3497`) ; `m_timeout` ctor = `connect_timeout` ;
  transition syn_sent→connected sur `ack_nr == m_seq_nr-1` ;
  `accept_syn` exige SYN v1 parsable + `incoming_packet` vrai
  (`utp_stream.rs:209-226` = `utp_socket_manager.cpp:163-201`). L'état
  FIN-acked → `Eof`/`ErrorWait` (`:1863-1867`) = upstream `:3543-3550`.
- Resend : RTO basé rtt/var, exponentiel, seq wrap — verdict :
  **conforme, réserve R9** — `packet_timeout` (`utp_socket.rs:1284-1300`
  = `:3342-3359`), marquage `need_resend` + file `needs_resend`
  (`:1824-1837` = `:3475-3487`, y compris `bytes_in_flight -=`
  et `m_bytes_in_flight == 0` garanti), resend du premier non-acké +
  `send_pkt` sinon (`:1839-1867` = `:3491-3551`), `ack_packet`
  désenfile `need_resend` (`:1067-1070` = `:2173-2181`),
  `insert_packet` désenfile le slot remplacé (`:902-915` =
  `:196-204`). Réserve : `resend_packet_seq` ne désenfile pas — R9.
- Delay measurement : tsdiff, LEDBAT — verdict : **conforme** —
  `UtpConfig` défauts verbatim (`utp_socket.rs` = `settings_pack.cpp`
  : target 100 ms, gain 3000, min_timeout 500, syn/fin/num 2/2/3,
  connect 3000, loss 50 %, cwnd_reduce 100) ; `do_ledbat` port verbatim
  (`:1302-1363` = `:3216-3340`, fixed-point <<16, ssthres,
  slow-start, cwnd_saturated, plancher 1 MSS) ; `m_reply_micro` +
  ajustement base borné ±10 ms ; `m_delay_sample_hist[3]` clampé
  min_rtt ; `timestamp_history`/`sliding_average` ports verbatim.
  Note : `m_close_reason != none` dans le gate de `num_timeouts`
  (`:1793` = `:3403`, le « hack SSL shutdown ») est présent.
- MTU discovery : probing, floor/ceiling — verdict : **conforme,
  réserve R3** — `update_mtu_limits`, `init_mtu`, gate probe
  (`m_mtu_seq==0 && cwnd > 3*floor`, `utp_socket.rs:679-686` =
  `:1503-1507`), fenêtre rolling `restrict_mtu` max-sur-3
  (`utp_stream.rs:173-178` = `utp_socket_manager.cpp:102-111`),
  `mtu_for_dest` Teredo/SOCKS5/v6, flag DF sur probes via
  `send_to_flags` + `DatagramSendFlags::DONT_FRAGMENT`
  (`utp_stream.rs:432-437`), probe-perdue → `m_mtu_ceiling = size-1`
  au resend (`:924-932` = `:1947-1956`) et au timeout quand la probe
  est le seul outstanding (`:1784-1791` = `:3386-3396`). Réserve :
  EMSGSIZE/erreurs d'envoi ne remontent jamais → R3.
- Out-of-order : SACK, gaps, reorder window — verdict : **conforme** —
  `consume_incoming_data` (`:1198-1266` = `:2322-2440` : eof-gate,
  capacity-gate, in-order + drain inbuf, dup/OOO gates) ; `parse_sack`
  (`:1090-1196` = `:1175-1336` : `resend[5]`, reverse-scan, prune,
  `cut_cwnd` unique, `m_fast_resend_seq_nr` avancé) ; `write_sack`
  (`:615-629` = `:1916-1935`) ; reorder `max(16, capacity/1100)`
  (`:1458` = `:2635`) ; `packet_buffer.rs` = port verbatim
  (`test_packet_buffer.cpp` 6/6 portés). SACK refresh/remove à
  l'émission d'un resend (`:956-991` = `:1995-2012`).
- wnd advertisement vs buffer réel, deferred close/ACK — verdict :
  **conforme, réserves R1/R2/R6/R10** — `wnd = max(capacity -
  buffered_incoming - receive_buffer_size, 0)` (`:854-857` =
  `:1753-1755`), `capacity = 1 MiB` des deux côtés (`:333` =
  `utp_stream.hpp:874`) ; gardes `close()` nagle+outbuf (`:460-469` =
  `:913-928`) ; FIN via `WriterMsg::Close`/drop (`utp_stream.rs:500-505`) ;
  deferred-ack une fois par batch inbound (`:480-491` ≈
  `socket_drained` + `m_deferred_ack` single-slot upstream — le flush
  par socket en fin de rafale est équivalent au flush croisé par
  `m_last_socket`) ; `m_adv_wnd` honoré dans send/resend. Réserves :
  write-post-close (R1), RST post-teardown (R2), FIN sauté sur drop-
  avec-données (R6), EOF jamais livré à un reader en attente (R10).
- `DatagramSocket` injectée, aucun bind direct — verdict : **conforme**
  — `UtpManager::{route,connect,accept_syn}` prennent `Arc<dyn
  DatagramSocket>` ; outbox → `send_to_flags` (`utp_stream.rs:422-438`) ;
  `remove_datagram_socket` = `remove_udp_socket` (`:300+`) ; routing
  immédiat depuis la pompe UDP (`engine.rs:843-845,1073`), pas le tick
  1 Hz ; `is_utp_datagram` = len ≥ 20 && version 1 && type ≤ 4
  (`engine.rs:6121-6123` — le filtre `type` est une addition, cf. R11).

## Statut des résidus de la passe 2

- **R4 — résolu (sortant), résidu entrant.** Le site d'appel existe :
  `engine.rs:1276-1291` — `SessionOut::Torrent(DisconnectPeer{reason})`
  → `disconnect_writer` → `mgr.set_close_reason(remote,
  reason_to_close_reason(reason))` (table `close_reason.rs` ↔
  `close_reason_t`). Le canal `close_slot` (`utp_stream.rs:79,241-245,
  311-318`) est relu **à chaque itération** du pump (`:416-419`) →
  indépendant du bras `wr_rx` : un reason post-`Close` atteint
  `m_close_reason` et monte même sur les retransmissions du FIN.
  Résidu : `their_close_reason` (`utp_socket.rs:290`) est parsé de
  l'ext-3 mais jamais propagé — ni `UtpStream` ni l'alerte
  `peer_disconnected` ne le lisent (upstream : `get_close_reason` →
  `on_close_reason` → alerte). Fix restant : exposer
  `their_close_reason` au démantèlement (dernier état pump → champ
  du conn-removal path).
- **R5 — résolu.** Couvert par `close_slot` ci-dessus pour le chemin
  prod (manager). Résidu cosmétique : `UtpStream::set_close_reason`
  (canal `WriterMsg::CloseReason`) reste bloqué par `if !write_closed`
  (`utp_stream.rs:495`) — inutilisé en prod.

## Déviations résiduelles (ouverts + nouveaux)

- **R1 — `write` après close/FIN accepté au lieu de `broken_pipe`
  (toujours ouvert).** `UtpSocket::write` (`utp_socket.rs:486-492`) ne
  teste ni `m_out_eof` ni `state >= FinSent` ; `UtpStream::poll_write`
  (`utp_stream.rs:611-620`) accepte toujours. Upstream :
  `check_fin_sent()` → `broken_pipe` dans `write_some`/`async_write_*`
  (`utp_stream.hpp:381-385,447-452,492-497`) + `TORRENT_ASSERT(
  !m_out_eof)` dans `add_write_buffer` (`utp_stream.cpp:546`).
  (a) via `UtpStream`, après `poll_shutdown`, les octets partent sur
  un canal dont le bras est désactivé → `Ok(len)` puis drop silencieux ;
  (b) via l'API `UtpSocket` directe en `FinSent`, `send_pkt` construit
  un DATA à `m_seq_nr` et `insert_packet` **écrase le FIN** dans
  l'outbuf (`packet_buffer.rs` remplace le slot) → FIN jamais émis,
  payload hors fenêtre pour le pair. Fix : `write` → erreur/`0` si
  `m_out_eof || state >= FinSent` ; `poll_write` → `Err(BrokenPipe)`
  si `write_closed` local ou état ≥ `FinSent`.
  Sévérité : wire-visible sur API directe / silent-drop via stream.
- **R2 — pas de RST après teardown (toujours ouvert).** Le pump sort
  sur `Deleting | ErrorWait` (`utp_stream.rs:464-466`) puis
  `unregister` (`:521`) : un paquet retransmis ensuite vers
  `(remote, conn_id)` tombe dans le vide. Upstream garde l'impl dans
  `m_utp_sockets` jusqu'à `should_delete()` (`state >= error_wait &&
  !m_attached && !m_stalled`, `utp_stream.cpp:823-847`) balayé au
  `utp_socket_manager::tick` ; tant que l'entrée vit, un paquet entrant
  obtient `send_reset(ph->seq_nr)` (`:3205-3210`) — le pair apprend la
  mort au premier resend au lieu du timeout complet. Fix : garder une
  entrée demux légère (ou le pump) en vie jusqu'au drop du
  `UtpStream` (= `!m_attached`), continuant à répondre RST.
  Sévérité : wire-visible mineur.
- **R3 — erreurs d'envoi jamais remontées (toujours ouvert).**
  `let _ = dg.send_to_flags(...)` (`utp_stream.rs:438`). Upstream
  traite dans `send_pkt` : (a) `message_size` → `m_mtu_ceiling =
  p->size-1`, `update_mtu_limits()`, `mtu_probe=false`,
  `m_mtu_seq=0`, renvoi immédiat sans DF (`utp_stream.cpp:1786-1809`) ;
  (b) autre erreur → `m_error`, `error_wait`, `test_socket_state`
  (`:1824-1831`). Sans (a) : une probe refusée n'est corrigée qu'au
  RTO, et sa perte non-unique compte comme congestion (`cut_cwnd`) ;
  sans (b) : ENETUNREACH/tunnel tombé n'est appris qu'au timeout.
  Fix : pump → `sock.note_send_error(size, was_probe, err)` —
  `send_to_flags` retourne déjà `io::Result` synchrone.
  Sévérité : wire-visible mineur (convergence PMTU).
- **R6 — drop avec données entrantes en vol saute le FIN (toujours
  ouvert).** `rd_tx.send(...).await.is_err() → break`
  (`utp_stream.rs:447-449`) quitte sans `sock.close()` : si le
  `UtpStream` est droppé avec du payload en attente dans `rd_tx`,
  aucun FIN ne part — upstream `~utp_stream → destroy() → close()`
  tente toujours le FIN (`utp_stream.cpp:930-964`). Note : upstream
  `destroy()` **vide aussi le write buffer** (`:950`) — le FIN part
  une fois l'outbuf drainé ; Rust flusherait davantage (mieux, pas
  pire). Fix : `sock.close()` + drain outbox avant le `break`.
  Sévérité : mineur.
- **R7 — settings `utp_*` morts : jamais mappés, pas seulement non
  hot-applied (ouvert, aggravé).** Les 10 knobs existent et
  round-trippent settings_pack (`settings.rs:517-534,613-614` ;
  `session_params.rs:713-721,764,1331-1339,1382`), mais
  `enable_utp`/`start_ssl_utp` construisent `UtpConfig {
  ..Default::default() }` (`engine.rs:770-776,805-811`) —
  `target_delay_ms`, `gain_factor`, `min_timeout`, `syn/fin/
  num_resends`, `connect_timeout`, `loss_multiplier`,
  `cwnd_reduce_timer` sont **ignorés dès le boot**, pas juste à chaud.
  Upstream lit `m_sett.get_int(utp_*)` **à chaque appel**
  (`utp_socket_manager.hpp:85-93`) — changement immédiat sur tous les
  sockets. `UtpManager::set_connection_limit` (`utp_stream.rs:168-170`)
  n'a aucun appelant → flood-guard figé à 200. Fix : mapper les champs
  au `enable_utp` (au minimum) ; pour la parité complète, config live
  (watch/lecture par le pump à chaque tick) + `apply_settings` →
  `set_connection_limit`. Conséquence en chaîne : le plancher
  `target_delay` diffère aussi — upstream `max(1, sett*1000)` µs
  contre Rust `target_delay_ms.max(1)*1000` (`utp_socket.rs:1306`) :
  un `utp_target_delay=0` donnerait 1 µs upstream vs 1000 µs Rust
  (moot tant que le knob est mort, à corriger dans le même fix).
  Sévérité : mineur (défauts conformes) → modéré pour quiconque règle
  LEDBAT (télécommande d'interopérabilité perdue).
- **R8 — `wait_connected` masque l'erreur (toujours ouvert).**
  `utp_stream.rs:543-549` → `"utp connect failed"` générique ;
  upstream propage `m_error` au connect-handler. Fix : `take_error()`
  → mapping `UtpError → io::Error` (réutiliser le match de
  `poll_read`). Sévérité : diagnostic.
- **R9 — entrée `needs_resend` périmée → retransmission en double
  (nouveau).** `resend_packet_seq` remet `p.need_resend = false`
  (`utp_socket.rs:975`) mais **ne retire jamais `seq` de
  `needs_resend`**. Upstream `resend_packet` efface l'entrée sur tout
  envoi non-would-block (`utp_stream.cpp:2044-2050`). Conséquence :
  `tick` pousse `acked+1` dans la file (`:1831`) puis le renvoie
  directement (`:1860`) → l'entrée reste → le prochain `send_pkt` la
  dépile (`:656-676`), `need_resend` est faux → `resend_packet_seq`
  réémet un paquet **déjà en vol** (compte `PacketResend`, incrémente
  `num_transmissions` → caps de resend atteints plus tôt, perte
  supplémentaire signalée au pair via dup). Fix : dans
  `resend_packet_seq`, si `need_resend` était vrai → retirer `seq`
  de `needs_resend` (miroir de l'erase upstream) ; dans la boucle
  `send_pkt`, ne `pop_front` que si `front() == Some(seq)` resté (les
  entrées orphelines `outbuf.at(seq).is_none()` continuent d'être
  purgées par `:657-659`). Sévérité : wire-visible mineur (un doublon
  par timeout par seq concerné ; biais du cwnd via perte fabriquée à
  la marge).
- **R10 — `poll_read` en attente jamais livré de l'EOF sur FIN
  (nouveau).** Quand le FIN du pair arrive et que le buffer se vide,
  `sock.read` renvoie `Err(Eof)` → le pump écrit `last_error`
  (`utp_stream.rs:454-458`) **mais garde `rd_tx` ouvert** : le reader
  dans `poll_read` reste `Pending` — `take_error()` n'est consulté que
  sur `Ready(None)` (`:589-604`), c.-à-d. seulement quand le pump
  termine (Deleting/ErrorWait, typiquement après ack de *notre* FIN).
  Upstream livre `eof` au read-handler dès que le buffer drainé touche
  le FIN (`maybe_trigger_receive_callback` + gate `m_in_eof &&
  m_in_eof_seq_nr == m_ack_nr`, `utp_stream.cpp:607-612,697-703,
  2866-2870`). Net : un reader bloqué après un FIN distant pend
  indéfiniment (jusqu'à timeout/close local) au lieu d'EOF immédiat —
  un `peer_conn` en lecture seule sur une conn half-closed ne se
  termine pas. Fix : sur `Err(_)` de `sock.read`, écrire `last_error`
  **puis dropper `rd_tx`** (close channel → `Ready(None)` → bras
  `take_error` → EOF/erreur correcte) ; le pump continue pour les
  writes/ACKs. Sévérité : modéré (hang de reader, pas de corruption
  wire).
- **R11 — filtre `type ≤ 4` en amont du demux (nouveau, trivial).**
  `is_utp_datagram` (`engine.rs:6121-6123`) exige `type ≤ 4` ;
  upstream ne teste que len ≥ 20 + version == 1 au niveau manager
  (`utp_socket_manager.cpp:119-125`) : un paquet type 5-15 dont le
  conn-id matche un socket est livré à l'impl → `utp_invalid_pkts_in`
  (`utp_stream.cpp:2527-2533`). Rust le jette au DHT/tracker (qui le
  rejettera) sans incrémenter le compteur. Fix si souhaité : livrer
  aussi les types > 4 à `route` (le socket les comptera invalides) —
  ou accepter la divergence compteur-seule. Sévérité : observabilité.
- **R12 — file inbound bornée (64) droppe en rafale sans compteur
  (nouveau).** `route` fait `try_send` sur `mpsc::channel(64)`
  (`utp_stream.rs:159-164,290`) — plein → drop silencieux, ni
  `utp_invalid_pkts_in` ni un compteur maison. Upstream traite chaque
  datagramme inline sur le network thread — pas de perte au demux.
  uTP tolère la perte (retransmission), mais sous rafale >64 paquets
  entre deux wakeups du pump le taux de drop diverge d'upstream ; la
  file listener (64, `:190`) et la même sémantique pour les SYN sont
  déjà testées (`route_drops_on_full_listener_queue`). Fix possible :
  compter le drop (`utp_invalid_pkts_in` ou une métrique dédiée) pour
  l'observabilité ; garder la borne (nécessaire à l'async).
  Sévérité : mineur/edge.

## Tests — couverture vs upstream

Baseline `e7381c7bc` :

- `test_utp.cpp` : `utp` — transfert e2e deux sessions, TCP désactivé
  (`enable_outgoing/incoming_tcp = false`, `setup_transfer`,
  128 KiB×6 pieces) ; `compare_less_wrap`.
- `test_packet_buffer.cpp` : `insert`, `wrap`, `wrap_range_boundaries`,
  `insert_large_capacity_far_index`, `wrap2`, `reverse_wrap`.

Rust :

- `test_packet_buffer.rs` : **6/6 portés verbatim** ✓.
- `compare_less_wrap` : couvert dans `bitdaemon-common` tests
  (`test_timestamp_history.rs`) ✓.
- `utp` (e2e) : ≈ `bulk_transfer_throughput` (1 MiB loopback via wires
  injectés) + validation live « 8 MiB vs libtorrent v2.1.2 »
  consignée dans PORTING.md ✓.
- Couverture Rust additionnelle (au-delà d'upstream) : handshake +
  DATA + FIN round-trip, OOO, close-vs-nagle ordering, abort→deleting,
  close-reason sur FIN, counter-sink, demux `(ep,id)`, shutdown→FIN,
  `remove_datagram_socket` abort, RESET→`ConnectionReset`, reader
  bloqué continue d'ACKer, deferred-ack par rafale, listener plein,
  DF sur probe.

Régressions manquantes pour les déviations ouvertes :

| Finding | Test à ajouter |
|---|---|
| R1 | write/poll_write après `poll_shutdown` → `BrokenPipe`/`0` |
| R2 | paquet vers socket en `ErrorWait` (stream vivant) → RST émis |
| R3 | `send_to_flags` → `EMSGSIZE` sur probe → ceiling clamp + renvoi sans DF ; `ENETUNREACH` → `error_wait` |
| R6 | drop du stream avec `rd_tx` plein → FIN quand même émis |
| R9 | après timeout+resend de `acked+1`, `send_pkt` ne réémet pas le même seq (`outbuf`/`num_transmissions` stable) |
| R10 | FIN distant + buffer drainé → `poll_read` retourne `Ok(0)` immédiat, sans attendre le teardown |
| R7 | `UtpConfig` non-défaut depuis `Settings` → `tick`/`packet_timeout` honorent `min_timeout`/`resends` custom |

## Non-bugs tranchés (rappel + nouveaux)

- `m_stalled`/`writable()`/`subscribe_writable()` et la branche
  `need_resend` de `insert_packet` quand `m_stalled` : inatteignables —
  l'outbox est infaillible (ADR-0008) ; `would_block`/`try_again`
  n'existent pas sur `DatagramSocket`. Les erreurs *réelles* existent
  → R3.
- `m_last_socket` + `m_deferred_ack` niveau manager : remplacé par
  flush fin-de-rafale par pump — équivalent ou plus rapide.
- `m_new_connection` : adoption des ids côté accept upstream
  (`utp_socket_manager.cpp:174-176,303-307`) = `accept_syn` qui fixe
  `send_id/recv_id` explicitement (`utp_stream.rs:215`) — même wire.
- SYN dupliqué → double accept : upstream `equal_range` sur `recv_id`
  rate la socket existante (clé `X+1` vs paquet `X`) → second accept ;
  Rust `route` même comportement via `(remote, conn_id)` miss →
  listener. Dédup au niveau peer (`duplicate_peer_id`) — parité.
- Callbacks/`m_userdata`/`m_attached`/`should_delete` → canaux +
  poll-model ; `test_socket_state` saute à `Deleting` direct (upstream
  reste `error_wait` sans handler — gauge `num_utp_error_wait`
  transitionne différemment, non wire-visible ; la fenêtre RST est
  tranchée par R2).
- `micros_now()` monotone vs `clock_type` — strictement meilleur.
- `// #error send reset` upstream (`utp_socket_manager.cpp:206`) :
  TODO jamais implémenté — `return false` = drop ; parité.
- `check_invariant`/`check_receive_buffers`/`TORRENT_UTP_LOG` :
  scaffolding debug → won't port.
- Buffer `pump_in` 2048 o (`utp_stream.rs:275`) : uTP n'émet jamais
  au-delà du MTU (~1472) — pas de troncation en pratique ; chemin
  prod `engine.rs` utilise 64 KiB.
- `Packet::new(effective_mtu)` = `acquire_packet(effective_mtu)`
  upstream (`utp_stream.cpp:1602`) — même sizing.
- Tick 50 ms par pump vs `tick_interval` (500 ms défaut) session :
  granularité de détection RTO plus fine ; `m_timeout` reste la
  référence — comportement wire compatible.
- Réception `ph->connection_id != m_recv_id` comptée invalide
  upstream (`utp_stream.cpp:2519-2525`) : injoignable côté Rust —
  `route` n'a livré que si `conn_id == recv_id` (clé exacte) ;
  demux upstream filtre pareil via `equal_range` + `match`.
- `UtpSocketDemux` (`utp.rs`) : hors chemin prod, tests only — conforme.
- `num_transmissions`/`m_num_timeouts` saturés u8 vs wrap : caps de
  resend/`!confirmed` tuent bien avant — inatteignable.
- `find_utp_socket` : inexistant au baseline.
- Flag `peer_connection` sur sends : désormais posé
  (`DatagramSendFlags::PEER_CONNECTION`, `utp_stream.rs:437`) pour le
  proxy-gate ; le ratelimit reste transversal (A28).
- `read_buf` VecDeque byte-pop + `poll_read` → `Pending` + re-wake
  après chunk : contrat tokio correct, sémantique identique.
- `decay()` upstream = `packet_pool` decay — pas de pool côté Rust
  (allocator) — N/A.

## Fichiers du fix (si apply ultérieur)

- `crates/bitdaemon-net/src/utp_socket.rs` — garde `write` sur
  `m_out_eof`/`FinSent` (R1) ; unlink `needs_resend` dans
  `resend_packet_seq` (R9) ; hook `note_send_error` (R3) ;
  `target_delay` floor (R7).
- `crates/bitdaemon-net/src/utp_stream.rs` — drop `rd_tx` sur
  `Err` (R10) ; `close()`+drain avant `break` (R6) ; pump/entrée
  légère en vie post-`ErrorWait` pour RST (R2) ; remonter l'erreur
  `send_to_flags` (R3) ; `wait_connected` → erreur réelle (R8) ;
  compteur de drop `try_send` (R12) ; propager `their_close_reason`
  (résidu R4).
- `crates/bitdaemon/src/engine.rs` — `UtpConfig` depuis `Settings`
  + `apply_settings` → `set_connection_limit`/config live (R7) ;
  étendre `is_utp_datagram` ou compter (R11).
- `crates/bitdaemon-net/tests/test_utp_{socket,stream}.rs` —
  régressions du tableau ci-dessus.

## Historique

- Passe 1 (branche `audit/05-utp-transport`) : 13 déviations corrigées
  (D1 gardes `close()`, D2 FIN via pump/`poll_shutdown`/Drop, D3
  mécanisme close-reason, D4 deferred-ack par batch, D5 DF +
  `IP_MTU_DISCOVER`, D6 gating `accept_syn`, D7 lecture subordonnée à
  la capacité lecteur, D8 `UtpSocketDemux` ids aléatoires `(ep,id)`,
  D9 `abort` → `test_socket_state`, D10 `UtpCounterSink` + 11
  compteurs + gauges, D11 propagation `m_error` → `AsyncRead`, D12
  `try_send` hors lock, D13 `remove_datagram_socket`).
- Passe 2 (re-audit sur `main`) : R1–R8.
- Entre-temps (A07) : `UtpManager::set_close_reason` + `close_slot`
  + `disconnect_writer` ont fermé R4 (sortant) et R5.
- Passe 3 (ce rapport) : R4/R5 fermés ; R1–R3, R6–R8 confirmés
  ouverts ; nouveaux R9 (resend double), R10 (EOF reader), R11
  (filtre type), R12 (drop file bornée).

## Conclusion

Le cœur protocolaire (framing, state machine, RTO/wrap, LEDBAT, MTU,
SACK/OOO, wnd, injection transport, demux conn-id) est **conforme** à
`e7381c7bc` — y compris les subtilités d'ordre d'extensions et de
sémantique send/recv-id. Les écarts résiduels sont concentrés sur les
bords du pump async (R2, R6, R10), la plomberie d'erreurs (R3, R8),
les settings morts (R7), et un défaut de bookkeeping de resend
nouvellement trouvé (R9). Aucune déviation n'empêche l'interop ; les
sévérités restent mineures à modérées.
