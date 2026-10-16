# Audit A05 — rapport d'analyse (re-audit, passe 4 — post-merge, rapport seul)

Quatrième passe de l'audit A05. Historique : la première passe a
produit 13 déviations (D1–D13, corrigées sur `audit/05-utp-transport`) ;
la seconde a listé 8 résidus (R1–R8) ; la troisième a confirmé les
résidus ouverts, ajouté R9–R13, et l'apply a tout corrigé (R2 déclassé
en non-bug — dead code upstream) puis mergé sur `main`
(`4bd1bb9 merge: audit/05-utp-transport`, `171e28e fix(net): A05
round-3`). **Cette passe re-vérifie `main` post-merge : elle trouve
4 déviations nouvelles, ouvertes — R14–R17** (dont une critique :
livelock possible du pump sur `WouldBlock`/EMSGSIZE répété). Aucun
code touché — mode rapport.

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

- **Packet types : ST_DATA/FIN/STATE/RESET/SYN — header 20B exact —
  verdict : conforme.**
  Preuve : `UtpHeader::encode`/`parse` (`utp.rs:83-118`) = `utp_header`
  upstream (big-endian identique : type_ver, extension, connection_id,
  timestamp, tsdiff, wnd, seq, ack) ; `UtpType` 0-4 (`utp.rs:14-25`),
  version == 1 exigée (`utp.rs:99-105` = `utp_stream.cpp:2510-2516` +
  `utp_socket_manager.cpp:125`) ; type nibble > 4 → `InvalidPktsIn`
  (`utp_socket.rs:1510-1515` = `:2527-2533`). Chaîne d'extensions :
  écriture `utp_socket.rs:934-952` = `utp_stream.cpp:1701-1716` (tête =
  SACK si présent sinon close-reason ; next du SACK pointe
  close-reason ; close-reason termine par 0), lecture
  `utp_socket.rs:1674-1708` = `:2737-2790` (SACK id 1, close-reason
  id 3, id 2 déprécié-skippé `utp.rs:50`). Ids : sortant
  `send_id = random`, `recv_id = send_id - 1`
  (`utp_stream.rs:417-418` = `utp_socket_manager.cpp:309-313`) ; le SYN
  porte `m_recv_id` (`utp_socket.rs:691-692` = `utp_stream.cpp:1000`),
  tous les autres paquets `m_send_id` (`utp_socket.rs:875` =
  `:1624`) ; entrant `send_id = syn_id`, `recv_id = +1`
  (`utp_stream.rs:282` = `utp_socket_manager.cpp:303-307`) ; RESET =
  `m_send_id` + `seq` aléatoire + `ack_nr` fourni
  (`utp_socket.rs:721-732` = `:1123-1152`).
  Lentilles : [Limites][Erreur][Compteurs].
  Tests : `utp_handshake_data_fin`, `demux_ids_and_endpoint_keying`
  (`tests/test_utp_socket.rs`), `accept_syn_rejects_garbage`,
  `unknown_type_counts_invalid_and_unmatched_falls_through`
  (`tests/test_utp_stream.rs`).

- **State machine : SYN_SENT → CONNECTED → FIN_SENT, syn retransmit,
  connect timeout — verdict : conforme (réserve R14 : un refus
  persistant du transport fige le pump avant d'atteindre la
  machine d'état — voir Déviations).**
  `UtpState` ordonné comme `state_t` ; kill
  `num_timeouts > num_resends || (num_timeouts > 0 && !m_confirmed)`
  (`utp_socket.rs:1941-1948` = `utp_stream.cpp:3400-3430`) ; caps
  `syn/fin/num_resends` par état sur le paquet en tête d'outbuf
  (`utp_socket.rs:1982-1997` = `:3495-3519`) ; `m_timeout` ctor =
  `connect_timeout` ; transition `syn_sent → connected` sur
  `ack_nr == m_seq_nr - 1` (`utp_socket.rs:1776-1787`) ;
  `accept_syn` exige SYN v1 parsable + `incoming_packet` vrai
  (`utp_stream.rs:269-298` = `utp_socket_manager.cpp:163-201`) ;
  FIN-acked/EOF → `Eof`/`ErrorWait` (`utp_socket.rs:1812-1822`,
  `:2004-2008` = `:3534-3540`) ; `test_socket_state` → `Deleting`
  (`utp_socket.rs:1842-1855` = `:2443+`).
  Lentilles : [Cycle de vie][Erreur][Concurrence].
  Tests : `connect_handshake_and_echo`, `acceptor_handshake_and_echo`,
  `shutdown_emits_fin`, `abort_progresses_to_deleting`,
  `remove_datagram_socket_aborts_streams`,
  `reset_maps_to_connection_reset`, `fin_wakes_pending_read`.

- **Resend : RTO basé rtt/var, exponentiel, seq wrap 16 bits —
  verdict : conforme.**
  `packet_timeout` (`utp_socket.rs:1420-1435` = `:3342-3359` :
  `max(min_timeout, rtt.mean + 2*dev)`, `+1s << (num_timeouts-1)`,
  caps `num_timeouts >= 7 → 60000`, état `none → 3000`) ; marquage
  `need_resend` + file `needs_resend` + `bytes_in_flight` vidé
  (`:1965-1978` = `:3475-3489`) ; resend du premier non-acké +
  `send_pkt` sinon (`:1980-2003` = `:3491-3533`) ; `ack_packet`
  désenfile `need_resend` (`:1199-1209` = `:2173-2182`) ;
  `resend_packet_seq` désenfile (`:1130-1134` = `:2044-2050` — fix
  R9) ; `insert_packet` désenfile le slot remplacé (`:1033-1042` =
  `:196-204`) ; fast-resend sur `dup_acks >= 3` à
  `m_fast_resend_seq_nr` (`:1714-1730`) ; wrap 16 bits via
  `compare_less_wrap` (`bitdaemon-common/src/timestamp_history.rs:13`,
  masque `ACK_MASK`).
  Lentilles : [Limites][Cycle de vie][Compteurs].
  Tests : `timeout_resend_unlinks_needs_resend`, `send_error_legs`,
  `counter_sink_observes_wire_activity`, `utp_out_of_order`.

- **Delay measurement : tsdiff, LEDBAT — verdict : conforme.**
  `UtpConfig` défauts verbatim = `settings_pack.cpp`
  (`utp_target_delay` 100, `utp_gain_factor` 3000, `utp_min_timeout`
  500, `utp_syn_resends`/`fin`/`num_resends` 2/2/3,
  `utp_connect_timeout` 3000, `utp_loss_multiplier` 50,
  `utp_cwnd_reduce_timer` 100) ; `do_ledbat` port verbatim
  (`utp_socket.rs:1438-1499` = `:3216-3340` : fixed-point `<<16`,
  ssthres, slow-start, `cwnd_saturated`, plancher 1 MSS,
  `target_delay.max(1)*1000` µs) ; `m_reply_micro` + ajustement de
  base borné ±10 ms (`:1539-1557` = `:2556-2580`) ;
  `m_delay_sample_hist[3]` clampé `min_rtt` (`:1630-1634`,
  `:1789-1795`) ; `timestamp_history`/`sliding_average` ports verbatim
  (`bitdaemon-common`). Le « hack SSL shutdown »
  `m_close_reason != 0` dans le gate de `num_timeouts` est présent
  (`:1934` = `:3403`). Réserve : granularité du tick (R16) — le calcul
  est conforme, la cadence de déclenchement des timeouts diffère.
  Lentilles : [Limites][Compteurs].
  Tests : `test_timestamp_history.rs` (bitdaemon-common :
  `compare_less_wrap`, base ajustée, step 60 s) ; comportement
  implicite via les tests de handshake.

- **MTU discovery : probing, floor/ceiling — verdict : conforme
  (réserve R14, jambe EMSGSIZE).**
  `update_mtu_limits` (`utp_socket.rs:1404-1417`), `init_mtu`
  (`:1859-1875` = `mtu_for_dest` appliqué à la création
  `utp_stream.rs:283,419`), gate probe `m_mtu_seq == 0 &&
  cwnd > 3*floor` (`utp_socket.rs:804-811` = `:1503-1507`), probe
  taguée + `m_mtu_seq` (`:972-977`), plancher remonté à l'ack de
  probe (`:1210-1213` = `:2184-2190`), plafond rabattu au resend
  (`:1052-1060` = `:1947-1956`) et au timeout quand la probe est le
  seul outstanding (`:1925-1932` = `:3386-3396`), fenêtre rolling
  `restrict_mtu` max-sur-3 (`utp_stream.rs:106-128,239-245` =
  `utp_socket_manager.hpp:102-110`) alimentée par
  `take_failed_mtu`/`mgr.restrict_mtu` (`utp_stream.rs:629-633` =
  `m_sm.restrict_mtu(m_mtu)` `:3512`), `mtu_for_dest`
  Teredo/SOCKS5/v6 (`utp_socket.rs:1885+` =
  `utp_socket_manager.cpp:65-94`), flag DF sur probes seulement via
  `DatagramSendFlags::DONT_FRAGMENT` (`utp_stream.rs:517-522`),
  EMSGSIZE → `m_mtu_ceiling = size-1` + clear probe + requeue sans DF
  (`utp_socket.rs:604-620` = `:1786-1809` — **réserve R14 : le retry
  est en boucle infinie si le refus persiste**).
  Lentilles : [Limites][Erreur][Compteurs].
  Tests : `mtu_probe_carries_dont_fragment`, `send_error_legs`,
  `nagle_pickup_below_effective_mtu_skips_append` (inline
  `utp_socket.rs:2057`).

- **Out-of-order buffer : SACK, gaps, reorder window —
  verdict : conforme.**
  `consume_incoming_data` (`utp_socket.rs:1334-1388` = `:2322-2440` :
  gate post-FIN, gate capacité, in-order + drain inbuf, dup/OOO gates,
  drop silencieux `return true` comme upstream) ; `parse_sack`
  (`:1226-1330` = `:1175-1336` : implicit-loss, `resend[5]`,
  reverse-scan `> dup_ack_limit`, prune hors fenêtre, `cut_cwnd`
  unique, `m_fast_resend_seq_nr` avancé) ; `write_sack` (`:735-749` =
  `:1916-1935`, bits dès `m_ack_nr + 2`) ; reorder-window
  `max(16, capacity/1100)` (`:1598-1610` = `:2633-2650`, compteur
  `RedundantPktsIn` idem) ; `packet_buffer.rs` = port verbatim de
  `src/packet_buffer.cpp` (wrap 16 bits, span, croissance
  power-of-two, replace) ; SACK refresh/remove à l'émission d'un
  resend (`:1082-1119` = `:1995-2012`, `remove_sack_header` chaîne
  préservée `:2027-2035`).
  Lentilles : [Limites][Erreur][Cycle de vie].
  Tests : `test_packet_buffer.rs` 6/6 portés (`insert`, `wrap`,
  `wrap_range_boundaries`, `insert_large_capacity_far_index`,
  `wrap2`, `reverse_wrap`), `utp_out_of_order`,
  `deferred_ack_batches_per_receive_round`.

- **wnd advertisement vs buffer réel, `m_suspended`/deferred close —
  verdict : conforme (réserve R17 pour le flush cross-socket).**
  `wnd = max(capacity - buffered_incoming - receive_buffer_size, 0)`
  (`utp_socket.rs:982-986` = `:1753-1755`), `capacity` 1 MiB des deux
  côtés ; gardes `close()` nagle+outbuf (`:472-482` = `:913-928`) ;
  `destroy()` = `close()` + purge buffers + drain (`:497-510` ≈
  `:930-964`) ; FIN via `WriterMsg::Close`/drop
  (`utp_stream.rs:612-626`) ; deferred-ack flushé une fois par batch
  inbound (`utp_stream.rs:599` ≈ `socket_drained` `utp_socket_manager.cpp:231-247` —
  réserve R17 : le flush *cross-socket* par `m_last_socket` n'a pas
  d'équivalent) ; reader suspendu ≠ pump bloqué (`rd_tx` capacity
  gate `utp_stream.rs:536-556` — le modèle `m_suspended` upstream) ;
  write post-close → `BrokenPipe` (`utp_stream.rs:770-782`,
  `utp_socket.rs` `write` gate — fix R1).
  Lentilles : [Concurrence][Cycle de vie][Erreur].
  Tests : `stalled_reader_keeps_acking`,
  `close_waits_for_pending_nagle`, `drop_still_flushes_fin`,
  `fin_wakes_pending_read`, `write_after_close_is_broken_pipe`,
  `write_after_shutdown_is_broken_pipe`,
  `their_close_reason_reaches_the_stream`.

- **Interface `DatagramSocket` injectée — aucun bind direct —
  verdict : conforme (réserve R14 pour le backpressure).**
  `UtpManager::{route,connect,accept_syn}` prennent `Arc<dyn
  DatagramSocket>` (`utp_stream.rs:185-231,269-339,409-459`) ; outbox
  → `send_to_flags` (`utp_stream.rs:507-531`) ;
  `remove_datagram_socket` = `remove_udp_socket` (`:392-405` =
  `utp_socket_manager.cpp:269-279`) ; routing immédiat depuis la
  pompe UDP (`engine.rs:876-899`, fallback `:1109-1118`, lane
  ssl-uTP `:843-861`) = « first dibs » upstream
  (`session_impl.cpp:2758-2780`) ; `is_utp_datagram` = len ≥ 20 &&
  version 1 (`engine.rs:6261-6267` = `utp_socket_manager.cpp:119-125`)
  ; `UtpConnector`/`UtpAcceptor` implémentent `PeerConnector`/
  `PeerAcceptor` (`utp_stream.rs:797-853`) ; le bind par connexion est
  ignoré comme les transports à socket partagée upstream.
  Lentilles : [Concurrence][Cycle de vie].
  Tests : `connect_handshake_and_echo`, `acceptor_handshake_and_echo`
  (transports simulés injectés), `remove_datagram_socket_aborts_streams`,
  `mtu_probe_carries_dont_fragment`, `accept_incoming_gate_drops_syn`,
  `route_drops_on_full_listener_queue`.

## Couverture des tests upstream (test/test_*.cpp)

- `test_utp.cpp::utp` (`test_transfer`) — transfert uTP-only de
  bout en bout entre deux `session` (6 × 128 KiB, pièces < 16 KiB,
  TCP désactivé des deux côtés) — **statut : partiel.** Le niveau
  transport est couvert (`connect_handshake_and_echo`,
  `acceptor_handshake_and_echo`, `bulk_transfer_throughput` sur
  transports injectés) ; il n'existe pas d'équivalent session-level
  uTP-only (peer_connection → uTP → disk → pieces). Couverture
  prévue via `bitdaemon-sim` / tests d'intégration engine — point de
  risque résiduel : l'interaction choker/peer_conn/cwnd n'est pas
  exercée de bout en bout.
- `test_utp.cpp::compare_less_wrap` — **statut : couvert** —
  `bitdaemon-common/tests/test_timestamp_history.rs:30-40` teste
  `compare_less_wrap` (masque `0xffffffff` ; le test upstream vise
  `0xffff` — la sémantique 16 bits est exercée via les tests
  `packet_buffer` dont `INDEX_MASK = 0xffff`, plus tout le chemin
  socket). Écart : le helper lui-même n'est pas testé au masque
  16 bits — couverture indirecte suffisante, risque nul.
- `test_packet_buffer.cpp::insert` — **couvert** (`insert`,
  `test_packet_buffer.rs:7-62`).
- `test_packet_buffer.cpp::wrap` — **couvert** (`wrap`, `:65-75`).
- `test_packet_buffer.cpp::wrap_range_boundaries` — **couvert**
  (`:78-98`).
- `test_packet_buffer.cpp::insert_large_capacity_far_index` —
  **couvert** (`:101-115`).
- `test_packet_buffer.cpp::wrap2` — **couvert** (`:118-130`).
- `test_packet_buffer.cpp::reverse_wrap` — **couvert** (`:133-146`).
- `test_transfer.cpp` — aucune variante uTP dédiée upstream (le
  transfert générique est TCP) ; pas d'orphelin.
- Scénarios upstream non couverts par un fichier de test mais présents
  dans le code — exercés côté Rust : resend/SACK (`utp_out_of_order`,
  `timeout_resend_unlinks_needs_resend`), erreurs d'envoi
  (`send_error_legs`), demux/clés (`demux_ids_and_endpoint_keying`),
  stalls/readers (`stalled_reader_keeps_acking`). **Gap restant** :
  pas de test Rust où un `DatagramSocket` injecté retourne
  `Err(WouldBlock)` de façon prête (cf. R14 — test de régression à
  ajouter avec le fix).

## Déviations

### Ouvertes (passe 4 — à corriger dans un apply ultérieur)

- **R14 — le drain d'outbox du pump ignore `m_stalled` : retry
  synchrone en boucle sur `WouldBlock` et sur EMSGSIZE persistant
  — sévérité `critique` — lentilles [Concurrence][Erreur].**
  `utp_stream.rs:507-531` : `while let Some(d) = sock.pop_outbox()`
  … `Err(e) => sock.note_send_error(d, &e)`. Or `note_send_error`
  re-push le datagramme en tête d'outbox dans les deux jambes de
  retry (`utp_socket.rs:618` EMSGSIZE, `:629` WouldBlock) : la
  boucle le re-pop immédiatement et renvoie.
  (a) **WouldBlock/Interrupted** : upstream gare le socket —
  `m_stalled = true` + `m_sm.subscribe_writable(this)`
  (`utp_stream.cpp:1816-1818`, `:2036-2038`, `:1026-1030` ;
  notification `writable()` via `utp_socket_manager::writable`
  `utp_socket_manager.cpp:211-229`) et **cesse tout envoi** pour ce
  socket jusqu'au front de writability. Rust continue de vider le
  reste de l'outbox dans un transport saturé, et quand l'impl
  `DatagramSocket` retourne `Err(WouldBlock)` *de façon prête*
  (transports injectés non-tokio : channels, sim, tunnels ADR-0008),
  `send_to_flags().await` ne yield jamais → **spin synchrone
  infini** : inbound, writer et tick (50 ms) du pump ne sont plus
  jamais atteints — connexion gelée + CPU grillé (starvation de
  l'exécuteur sur runtime mono-thread). Avec un transport tokio qui
  `Pending` au lieu d'erreur-prête, pas de spin mais divergence de
  backpressure maintenue.
  (b) **EMSGSIZE persistant** : upstream renvoie la même taille une
  fois inline (`utp_stream.cpp:1807-1808`) ; un second refus retombe
  sur la gestion générique d'`ec` (`:1811-1831` → stall ou
  `error_wait`). Rust re-queue le datagramme inchangé (DF retiré) →
  re-pop → EMSGSIZE → re-queue → **boucle sans convergence** (le
  plafond clampé `size-1` est déjà atteint, rien ne change).
  Note associée : la jambe stalled de `resend_packet` upstream
  re-marque `need_resend` + décrémente `bytes_in_flight`
  (`:2039-2042`) et celle du SYN idem (`:1031-1032`) ; Rust garde le
  datagramme en outbox (cohérent avec le modèle requeue : le paquet
  reste « en vol ») — écart interne bénin tant que le retry n'est
  pas synchrone.
  - **sketch** : `utp_stream.rs` — dans la boucle de drain,
    `Err(e) => { sock.note_send_error(d, &e); break; }` (arrêter le
    drain sur toute erreur, comme `while(send_pkt())` s'arrête sur
    `false` upstream ; le datagramme re-queue sera retenté au
    prochain événement/tick du pump — borne ≤ tick). Et
    `utp_socket.rs:605-620` — n'appliquer la jambe EMSGSIZE que si le
    datagramme n'a pas déjà été retenti (ex. flag
    `emsgsize_retried` sur `OutDatagram`, ou `d.dont_fragment` encore
    armé) ; un second refus → jambe hard-error (`m_error`,
    `ErrorWait`) comme le fallthrough upstream `:1824-1831`. Option
    structurelle : ajouter `poll_send_ready`/`wait_writable` à
    `DatagramSocket` (`transport.rs:86-140`) pour retrouver le
    signal `subscribe_writable` edge-triggered au lieu d'un retry
    borné par le tick.
  - **test de régression suggéré** : transport injecté fictif dont
    `send_to_flags` retourne `Err(WouldBlock)` de façon prête N fois
    puis `Ok` — vérifier que le pump continue de traiter les
    datagrammes entrants et le tick pendant le refus (pas de spin),
    puis reprend l'envoi après le premier succès (`on_writable`).
    Second test : `send_to_flags` retournant EMSGSIZE en permanence →
    le socket passe `ErrorWait` au lieu de boucler.
- **R15 — `m_timeout`/`m_out_packets`/`num_transmissions`/
  `PacketsOut` stampés à l'enqueue, pas au succès d'envoi —
  sévérité `état-interne` — lentilles [Compteurs][Erreur].**
  `utp_socket.rs:993-1002` : `send_packet` queue puis les compteurs
  et `m_timeout = now + packet_timeout()` (quand `bytes_in_flight ==
  0`) s'exécutent **avant** le verdict du transport ; upstream ne les
  applique que dans la branche `ec` vide (`utp_stream.cpp:1832-1845`,
  `:2061-2064` ; SYN `:1044`). Conséquences : (a) un premier envoi
  qui échoue (WouldBlock) recule quand même `m_timeout` → deadline de
  retransmission plus tardive qu'upstream (borne : un
  `packet_timeout`) ; (b) `m_out_packets`/`num_transmissions`
  comptent les tentatives échouées (la jambe hard-error compense via
  `-1` `:634-635`, la jambe stall non — le datagramme partira plus
  tard, donc un seul comptage au total, juste avancé dans le temps).
  - **sketch** : déplacer les compteurs/`m_timeout` derrière un
    acquittement du pump (ex. `note_send_ok(seq)`) — coût : il faut
    corréler datagramme↔paquet (le seq est lisible dans
    `d.bytes[16..18]`). Ou accepter : l'écart est borné et interne ;
    si retenu, corriger uniquement `m_timeout` (la plus visible) en
    reculant son update au premier `Ok` du drain quand
    `bytes_in_flight` vient de monter.
  - **test de régression suggéré** : transport qui refuse le premier
    datagramme puis accepte — vérifier que `m_timeout` n'est pas
    reculé par l'envoi échoué (timeout effectif ≥ upstream) et que
    `PacketsOut`/`num_transmissions` n'augmentent qu'au succès.
- **R16 — tick du pump à 50 ms vs `tick_interval` = 500 ms upstream
  — sévérité `wire-visible` (timing) — lentille [Concurrence].**
  `utp_stream.rs:484` `interval(Duration::from_millis(50))` ; upstream
  `m_utp_socket_manager.tick(now)` ne tourne qu'au rythme de
  `session_impl::on_tick` (`session_impl.cpp:3586`), cadencé par
  `settings_pack::tick_interval` = **500 ms** par défaut
  (`settings_pack.cpp:306`). La deadline `m_timeout` est la même des
  deux côtés, mais upstream la pollue à ±500 ms près (retransmits
  quantifiés jusqu'à ~500 ms de retard) là où Rust déclenche ≤ 50 ms
  après expiry — un sender systématiquement plus agressif sous perte
  + flush des deferred-acks 10× plus souvent. Bénin (plus réactif),
  mais pas verbatim.
  - **sketch** : ajouter `tick_interval` à `UtpConfig` mappé depuis
    `settings.tick_interval` (`engine.rs:803-811` le pousse déjà avec
    les autres) et l'utiliser pour l'`interval` du pump ; attention
    au couplage avec R14 — si le tick devient le retry-driver des
    sockets stalled, préférer d'abord le `poll_send_ready` de R14
    pour ne pas allonger la latence de reprise à 500 ms.
  - **test de régression suggéré** : mesurer le délai de
    retransmission d'un paquet perdu avec `tick_interval = 500` —
    borne haute `m_timeout + 500 ms` comme upstream.
- **R17 — pas de flush deferred-ack cross-socket — sévérité
  `état-interne` — lentille [Concurrence].**
  Upstream garde `m_deferred_ack` au niveau *manager* : un datagramme
  pour un socket B flush le ACK différé du socket A
  (`utp_socket_manager.cpp:140-149`), idem `socket_drained`
  (`:231-247` en fin de rafale de lecture UDP). Rust a un
  `m_deferred_ack` par socket, flushé en fin de batch inbound de ce
  socket (`utp_stream.rs:599`) ou à son prochain tick (≤ 50 ms). Un
  ACK différé sur A pendant que B streame attend donc le prochain
  événement de A — retard borné ≤ tick, aucun cas de perte.
  - **sketch** (optionnel) : `UtpManager` retient la clé du socket
    ayant un ACK différé (`Mutex<Option<(remote,id)>>` alimenté via
    un nouveau `InboundMsg`) ; `route` vers une autre clé pousse
    `FlushAck` à l'ancien. Ou accepter — le retard est borné.
  - **test de régression suggéré** : deux sockets, ACK différé armé
    sur A, rafale de datagrammes sur B → le STATE de A part avant la
    fin de la rafale (sans attendre un événement de A).

### Corrigées lors des passes 1–3 (appliquées, mergées `4bd1bb9`)

D1–D13 (passe 1) et R1–R13 (passes 2–3) — détail conservé dans
« Historique » ci-dessous. Récapitulatif des fix du dernier apply :
R1 write-post-close → `BrokenPipe` ; R3 `note_send_error` trois
jambes portées ; R4 `their_close_reason` exposé au stream ; R6
close+drain avant sortie du pump ; R7 mapping live des 10 knobs
`utp_*` + `enable_*_utp` + `connections_limit` ; R8
`wait_connected` propage l'erreur réelle ; R9 unlink
`needs_resend` au resend ; R10 EOF livré au reader en attente ; R11
`is_utp_datagram` ne filtre plus le type ; R12 compteur de drop
documenté ; R13 arithmétique signée pour le drain nagle. **R2
déclassé** (dead code upstream). Toutes vérifiées présentes sur
`main` en passe 4.

## Invariants de frontière (Seam Checks)

- **Session ↔ uTP** : `engine.rs:803-811` `apply_utp_settings` pousse
  `UtpConfig` (`from_settings` : les 10 knobs + `proxy_socks5`),
  `set_connection_limit` (`connections_limit`), `set_accept_incoming`
  (`enable_incoming_utp`) — hot-apply à chaque `apply_settings` comme
  la lecture live de `m_sett` upstream (`utp_socket_manager.hpp:85-93`).
  Gate sortant `utp_wanted` (`engine.rs:3983-3985`) =
  `enable_outgoing_utp && (!enable_outgoing_tcp || peer.supports_utp)`
  — table `connect_to_peer`/`instantiate_connection` upstream.
- **UDP pump ↔ demux** : uTP a « first dibs » immédiat
  (`engine.rs:882-886`), les datagrammes non-matchés tombent au
  DHT/trackers (`:887-888`, `:1109-1118`) = ordre upstream
  (`session_impl.cpp:2760-2780`). Erreurs de lecture non-fatales :
  le pump ré-arme (`engine.rs:891-899`) comme upstream `:2806-2833`.
- **Socket ↔ transport** : seules des `Arc<dyn DatagramSocket>`
  injectées (ADR-0008) ; DF uniquement sur probes ;
  `remove_datagram_socket` aborte les sockets liés.
- **Socket ↔ common** : `TimestampHistory`, `SlidingAverage`,
  `compare_less_wrap` partagés (`bitdaemon-common`) — ports verbatim
  utilisés par le delay/rtt.
- **Compteurs** : `UtpStat` → `counter_sink` → `counters::utp_*`
  session (`engine.rs` sink wiring, `bitdaemon-session/src/counters.rs`).
- **Cycle de vie** : le pump possède le `UtpSocket` ; sortie sur
  `Deleting | ErrorWait` (`utp_stream.rs:570-572`) puis `unregister`
  (`:648`) = erase upstream au tick (`utp_socket_manager.cpp:50-63`).
  Upstream diffère en gardant l'impl jusqu'à `should_delete()` —
  iso-sémantique, cf. R2 en non-bugs.
- **tick/decay** : `decay()` upstream = `m_packet_pool.decay()`
  seulement (`utp_socket_manager.hpp:119`) — pas d'équivalent requis
  (pas de pool de paquets).

## Fichiers du fix (si apply d'une passe 4)

- `crates/bitdaemon-net/src/utp_stream.rs` — R14 (break du drain sur
  erreur/stalled), R16 (`interval` depuis config), R17 (message
  `FlushAck` + bookkeeping manager si retenu).
- `crates/bitdaemon-net/src/utp_socket.rs` — R14 (jambe EMSGSIZE :
  retry-une-fois puis hard-error ; flag sur `OutDatagram`), R15
  (compteurs/`m_timeout` derrière acquittement).
- `crates/bitdaemon-net/src/transport.rs` — R14/R16 optionnels :
  `poll_send_ready`/`wait_writable` pour la parité
  `subscribe_writable`.
- `crates/bitdaemon-net/tests/test_utp_stream.rs` — régressions
  R14/R16/R17 ; `crates/bitdaemon-net/tests/test_utp_socket.rs` —
  régression R15.
- `crates/bitdaemon/src/engine.rs` — R16 seulement si
  `tick_interval` est plombé depuis `settings`.

## Non-bugs tranchés

- **R2 — pas de RST post-teardown : non-bug.** Le bras
  `send_reset` post-`error_wait`/`deleting` est du dead code upstream
  — l'early-return `state() == error_wait || deleting → return true`
  (`utp_stream.cpp:2873`) précède le `switch` (`:2875`) ; le bras
  `utp_socket.rs:1824-1826` conserve la même forme inerte.
- **FIN perdu si `close()` pendant `m_stalled` : iso-bug.** Upstream
  `send_fin` positionne `fin_sent` même quand `send_pkt(pkt_fin)`
  est repoussé par le stall (`utp_stream.cpp:1101-1107`), et le
  re-check de `writable()` exige `state() == connected`
  (`:1083-1088`) → le FIN n'est jamais re-tenté. Rust reproduit le
  même trou (`utp_socket.rs:708-718` + garde `state == Connected` à
  `:1019-1024`) — comportement identique, non corrigé volontairement
  (fidelity rule).
- **Gate capacité sans `m_read_buffer_size == 0`.** Upstream n'applique
  le drop « advertised window not honored » que sans read-buffer
  applicatif en cours (`utp_stream.cpp:2339-2350`) ; Rust n'a pas de
  read-buffer applicatif — le canal `rd_tx` plein joue ce rôle
  (`utp_stream.rs:536-556`) et la gate `utp_socket.rs:1341-1345`
  couvre le même cas effectif. Équivalent sous le modèle.
- **`m_last_socket`/cache de dernier socket.** Rust démultiplexe par
  clé `(remote, recv_id)` (`utp_stream.rs:195`) — optimisation
  upstream non sémantique (`utp_socket_manager.cpp:135-159`).
- **`subscribe_drained`/callbacks par rafale.** Le push par
  itération du pump vers `rd_tx` est l'équivalent pull-based ;
  upstream notifie les handlers utilisateurs
  (`utp_socket_manager.cpp:231-247`) — modèle différent, même effet.
- **Erreurs d'envoi d'un RESET ignorées.** Upstream envoie et ignore
  `ec` (`utp_stream.cpp:1142-1151`) ; Rust queue le RESET — un échec
  retombe sur `note_send_error` d'un socket déjà mourant. Neutre.
- **`UtpExtensions`/`UtpSocketDemux` (`utp.rs:124-243`)** restent des
  utilitaires de test/API — le chemin de production parse la chaîne
  inline (`utp_socket.rs:1674-1708`) et demux via `UtpManager::route`.
- **`decay()`** = `m_packet_pool.decay()` seul — sans objet hors
  pool de paquets.
- **Num_transmissions du SYN compté à l'enqueue** (`utp_socket.rs:701`)
  vs succès upstream (`:1044`) — absorbé par R15.
- **Close_reason non rafraîchi sur paquet naglé repris**
  (`utp_socket.rs:902-903`) — identique upstream (le ext header du
  paquet naglé n'est pas réécrit).

## Historique des passes

### Passe 3 — apply (mergé `4bd1bb9`, commit `171e28e`)

12 déviations corrigées, R2 déclassé :

- **R1** — `UtpSocket::write` refuse `m_out_eof`/état ≥ `FinSent`
  (`broken_pipe` ≈ `check_fin_sent` upstream) ; `poll_write` →
  `Err(BrokenPipe)` si `closed`. Tests `write_after_close_*`.
- **R2** — déclassé : bras `send_reset` post-`error_wait`
  inatteignable upstream (`utp_stream.cpp:2873` court-circuite
  `:2875`+) — pas de changement.
- **R3** — `note_send_error` porté : `MessageSize` →
  `m_mtu_ceiling = size-1` + `update_mtu_limits` + `mtu_probe=false`
  + renvoi sans DF ; `WouldBlock`/`TryAgain` → `m_stalled` +
  defer-ack ; autre → `m_error`, `ErrorWait`. Test `send_error_legs`.
- **R4 résidu** — `their_close_reason` exposé via `UtpStream`,
  stampé dès le paquet porteur. Test
  `their_close_reason_reaches_the_stream`.
- **R6** — le pump exécute `sock.close()` + drain outbox avant de
  sortir sur `rd_tx` mort (≈ `~utp_stream → destroy() → close()`).
- **R7** — `UtpConfig::from_settings` mappe les 10 `utp_*` +
  `proxy_socks5` ; `enable_utp`/`start_ssl_utp` l'utilisent ;
  `apply_settings` pousse `InboundMsg::SetConfig` (hot-apply) +
  `set_connection_limit` ; `enable_incoming_utp`/`enable_outgoing_utp`
  relus à chaud dans `utp_wanted`/`accept_incoming`.
- **R8** — `wait_connected()` propage l'erreur socket réelle via
  `UtpError → io::Error`. Test `connect_failure_surfaces_real_error`.
- **R9** — `resend_packet_seq` retire `seq` de `needs_resend` quand
  `need_resend` était armé (miroir de l'erase
  `utp_stream.cpp:2044-2050`). Test `timeout_resend_unlinks_needs_resend`.
- **R10** — `sock.read → Err` ferme `rd_tx` après `last_error` :
  un `poll_read` en attente reçoit EOF/erreur dès le buffer drainé.
  Test `fin_wakes_pending_read`.
- **R11** — `is_utp_datagram` ne borne que len ≥ 20 + version 1 ;
  les types 5-15 matchés atteignent le socket → `utp_invalid_pkts_in`,
  les non-matchés tombent au DHT. Test
  `unknown_type_counts_invalid_and_unmatched_falls_through`.
- **R12** — file inbound bornée (64) gardée (nécessité async) ;
  drop documenté `tracing::trace` (`utp_stream.rs:217-226`).
- **R13** — drain nagle en `i32` intermédiaire + append conditionné à
  `size_left > 0` (`utp_socket.rs:910-917`), verbatim upstream
  (`utp_stream.cpp:1672-1675`). Test inline
  `nagle_pickup_below_effective_mtu_skips_append`.

### Passe 4 (ce rapport)

Relecture statique post-merge de `main` : `send_pkt`/`send_syn`/
`resend_packet_seq`/`note_send_error`/`tick`/`incoming_packet`/
`consume_incoming_data`/`parse_sack`/`ack_packet`, le pump
(`socket_pump`), le demux (`route`/`is_utp_datagram`), le trait
`DatagramSocket`, l'intégration engine (`apply_utp_settings`,
`utp_wanted`, routing immédiat, `set_close_reason`) et les tests.
Constats : R14–R17 ouverts ci-dessus ; par ailleurs vérifications
négatives — `writable()` upstream vs `on_writable` (flush nagle +
FIN différé couverts par le chemin `send_pkt(0)` + `m_out_eof`),
`ack_packet` conforme (`:1198-1223` = `:2160-2209`), timeout-marking
conforme (`:1965-1978` = `:3475-3489`), `decay()` sans objet, FIN-
drop-stalled iso-bug. Pas de modification de code — mode rapport.
