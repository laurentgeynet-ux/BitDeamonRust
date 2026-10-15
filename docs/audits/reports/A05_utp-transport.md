# Audit A05 — rapport d'analyse (re-audit)

Seconde passe de l'audit A05. La première passe avait produit 13
déviations (D1–D13), toutes corrigées sur la branche
`audit/05-utp-transport` (voir « Historique » en bas). Ce rapport
re-vérifie l'état courant de `main` contre upstream et ne liste que les
constats **résiduels** (R1–R8).

Baseline upstream : `e7381c7bc` (`src/utp_stream.cpp`,
`src/utp_socket_manager.cpp`, `include/libtorrent/aux_/utp_stream.hpp`,
`include/libtorrent/aux_/utp_socket_manager.hpp`,
`src/packet_buffer.cpp`, `src/timestamp_history.cpp`,
`aux_/sliding_average.hpp`, `src/settings_pack.cpp` pour les défauts).
Côté Rust : `crates/bitdaemon-net/src/utp.rs`, `utp_socket.rs`,
`utp_stream.rs`, `packet_buffer.rs`, `transport.rs`, `close_reason.rs` ;
`bitdaemon-common/src/{timestamp_history,sliding_average}.rs` ;
`bitdaemon/src/engine.rs` pour l'intégration.

## Verdicts checklist

- Packet types : ST_DATA/FIN/STATE/RESET/SYN — header 20B exact
  (version, ext, conn_id, ts, tsdiff, wnd, seq, ack) — verdict :
  **conforme** — `UtpHeader::encode`/`parse` (`utp.rs:83-118`) =
  `aux_/utp_stream.hpp` `utp_header` (layout big-endian identique) ;
  `UtpType` 0-4, version == 1 exigée au parse ; chaîne d'extensions
  `UtpExtensions` (`utp.rs:124-160`) = walk `ptr`/`ph->extension`
  upstream (SACK id 1, close-reason id 3, id 2 déprécié-skippé). Ids
  d'allocation verbatim : sortant `send_id=random, recv_id=send_id-1`
  (`utp_stream.rs:315-316` = `utp_socket_manager.cpp:311-312`), entrant
  `send_id=syn_id, recv_id=+1` (`utp_stream.rs:207` = `:305-306`) ; le
  SYN porte `m_recv_id` (`utp_socket.rs:556+` = `utp_stream.cpp:1000`).
- State machine : SYN_SENT → CONNECTED → FIN_SENT, syn retransmit,
  connect timeout — verdict : **conforme** — `UtpState` ordonné comme
  `state_t` upstream ; kill `!m_confirmed && m_num_timeouts > 0` et
  `num_timeouts > num_resends` (`utp_socket.rs:1800-1807` =
  `utp_stream.cpp:3423-3428` dans `tick`) ; limites `syn_resends`/
  `fin_resends`/`num_resends` par état (`utp_socket.rs:1842-1845`) ;
  `m_timeout` ctor = `connect_timeout` ; transition syn_sent→connected
  sur `ack_nr == m_seq_nr-1` (`utp_socket.rs:~1637` =
  `utp_stream.cpp:2919-2952`) ; `accept_syn` exige un SYN version-1
  parsable et le retour vrai d'`incoming_packet`
  (`utp_stream.rs:194-219` = `utp_socket_manager.cpp:119-125,195-196`).
- Resend : RTO basé rtt/var, exponentiel, seq wrap 16 bits — verdict :
  **conforme** — `packet_timeout` (`utp_socket.rs:1284-1300` =
  `utp_stream.cpp:3342-3359` : `max(min_timeout, rtt.mean +
  2*avg_dev)` + `2^(n-1)` s, cap 60 s, `num_timeouts >= 7` → 60 s,
  state none → `connect_timeout` hors syn_sent) ; marquage
  `need_resend` + file `needs_resend` avec garde
  `i != m_acked_seq_nr && i != m_seq_nr` (`utp_socket.rs:1824-1837`) ;
  resend du premier non-acké + `send_pkt` sinon
  (`utp_socket.rs:1839-1867` = `:3489-3541`) ; wrap par `wrapping_*` +
  `compare_less_wrap` (port verbatim) ; resend-window gate et
  SACK-refresh/`remove_sack_header` conformes
  (`utp_socket.rs:918-1001, 1883-1894` = `:1937-2012, 1416-1439`).
- Delay measurement : tsdiff, constantes LEDBAT-ish — verdict :
  **conforme** — `UtpConfig` défauts verbatim
  (`utp_socket.rs:161-177` = `settings_pack.cpp` : target_delay 100,
  gain 3000, min_timeout 500, syn/fin/num_resends 2/2/3,
  connect_timeout 3000, loss_multiplier 50, cwnd_reduce_timer 100) ;
  `do_ledbat` port verbatim (`utp_socket.rs:1302-1363` =
  `utp_stream.cpp:3216-3340`, fixed-point <<16, ssthresh, slow-start,
  cwnd_saturated, plancher 1 MSS) ; `m_reply_micro` + ajustement
  base-change borné ±10 ms (`utp_socket.rs:1398-1416` =
  `:2559-2580`) ; `m_delay_sample_hist[3]` min-of-3 clampé min_rtt ;
  `timestamp_history`/`sliding_average` = ports verbatim.
- MTU discovery : probing, floor/ceiling — verdict : **conforme,
  réserve R3** — `update_mtu_limits` (`utp_socket.rs:1268-1282` =
  `:247-277`), `init_mtu` (`:1718-1731` = `:2472-2490`), gating probe
  (`m_mtu_seq==0 && cwnd > 3*floor`), fenêtre rolling `restrict_mtu`
  max-sur-3 (`utp_stream.rs:87-111,164-170` =
  `utp_socket_manager.cpp:43,93`), `mtu_for_dest` Teredo/SOCKS5/v6
  (`utp_socket.rs:1744-1758` = `:65-94`), flag DF sur les probes via
  `DatagramSendFlags::DONT_FRAGMENT` + `send_to_flags`
  (`transport.rs:38-93`, `utp_stream.rs:385-390` =
  `utp_socket_manager.cpp:96-112`). Réserve : le retour d'erreur
  d'envoi (EMSGSIZE & co) n'atteint jamais le socket → R3.
- Out-of-order buffer : selective ack, gaps, reorder window —
  verdict : **conforme** — `consume_incoming_data`
  (`utp_socket.rs:1198-1266` = `utp_stream.cpp:2322-2440` : eof-gate,
  capacity-gate `return false` vs drop-consommé `return true`, in-order
  + drain inbuf, dup/OOO gates) ; `parse_sack`
  (`utp_socket.rs:1090-1196` = `:1175-1336` : `resend[5]`,
  reverse-scan `dups > dup_ack_limit`, prune `last_resend`,
  `cut_cwnd` unique, `m_fast_resend_seq_nr` avancé au paquet
  retransmis) ; `write_sack` (`utp_socket.rs:615-630` =
  `:1916-1935`) ; reorder window `max(16, capacity/1100)`
  (`utp_socket.rs:1457-1469` = `:2634-2650`) ; `packet_buffer.rs` =
  port de `packet_buffer.cpp` (insert/remove identiques).
- wnd advertisement vs buffer réel, `m_suspended`/deferred close —
  verdict : **conforme, réserves R1/R2/R4-R6** — wnd :
  `wnd = max(capacity - buffered_incoming - receive_buffer_size, 0)`
  (`utp_socket.rs` send_pkt = `utp_stream.cpp:1753-1755`), `m_adv_wnd`
  honoré dans send_pkt/resend ; gardes `close()` nagle+outbuf
  (`utp_socket.rs:460-470` = `:913-928`) ; FIN via `WriterMsg::Close`/
  drop (`utp_stream.rs:447-460`) ; close-reason ext sur le FIN ;
  deferred-ack par batch (`utp_stream.rs:423-443`). Réserves :
  write-after-close (R1), RST post-teardown (R2), site d'appel
  close-reason absent (R4), `CloseReason` tardif droppé (R5), FIN
  sauté sur drop-avec-données (R6).
- Interface `DatagramSocket` injectée — aucun bind direct (ADR-0008) —
  verdict : **conforme** — `UtpManager::{route,connect,accept_syn,
  pump_in}` prennent `Arc<dyn DatagramSocket>` ; aucun `bind` dans
  `utp*.rs` ; `OutDatagram` drainé vers `send_to_flags` par le pump
  (`utp_stream.rs:377-391`) ; `remove_datagram_socket` =
  `remove_udp_socket` upstream (`utp_stream.rs:290-303` =
  `utp_socket_manager.cpp:269-279`) ; routing immédiat depuis la pompe
  UDP du engine (`engine.rs:491-519`), `is_utp_datagram` =
  len ≥ 20 && version 1 && type ≤ 4 (`engine.rs:4284-4286`).

## Déviations résiduelles

- **R1 — `write` après close/FIN accepté au lieu de `broken_pipe`.**
  `UtpSocket::write` (`utp_socket.rs:486-492`) ne teste ni `m_out_eof`
  ni `state >= FinSent` ; `UtpStream::poll_write`
  (`utp_stream.rs:563-572`) accepte toujours. Upstream :
  `check_fin_sent()` → `boost::asio::error::broken_pipe` dans
  `write_some`/`async_write_some`/`async_wait_write`
  (`aux_/utp_stream.hpp:381-385,447-452,492-497`) +
  `TORRENT_ASSERT(!m_out_eof)` dans `add_write_buffer`
  (`utp_stream.cpp:546`). Deux effets : (a) via `UtpStream`, après
  `poll_shutdown`, les octets sont empilés sur un canal dont le bras
  `wr_rx.recv(), if !write_closed` est désactivé
  (`utp_stream.rs:447`) → `Ok(len)` puis drop silencieux ; (b) via
  l'API `UtpSocket` directe en `FinSent`, `send_pkt` construit un DATA
  à `m_seq_nr` et `insert_packet` **écrase le FIN** dans l'outbuf
  (`packet_buffer.rs:60-70` remplace le slot occupé) → le FIN ne part
  jamais et le pair voit du payload hors fenêtre. Fix : dans
  `UtpSocket::write`, si `m_out_eof || state >= FinSent` → retourner
  erreur/`0` ; dans `UtpStream::poll_write`, sur `write_closed` local
  (flag côté stream) ou état `>= FinSent` → `Err(BrokenPipe)`.
  Sévérité : wire-visible sur API directe / silent-drop via stream.
- **R2 — pas de RST après teardown.** Le pump sort sur
  `ErrorWait | Deleting` (`utp_stream.rs:416-418`) et `unregister`
  retire la clé (`:473`) : les paquets arrivant ensuite sur
  `(remote, conn_id)` tombent dans le vide. Upstream conserve l'impl
  dans `m_utp_sockets` jusqu'à `should_delete()` (`state >= error_wait
  && !m_attached && !m_stalled`, `utp_stream.cpp:823-847`), balayé au
  `utp_socket_manager::tick` (`utp_socket_manager.cpp:48-63`) ; tant
  que l'entrée vit, `incoming_packet` répond `send_reset(ph->seq_nr)`
  (`utp_stream.cpp:3205-3210`) — le pair qui retransmet vers une conn
  morte reçoit un RST (fail-fast) plutôt qu'un silence jusqu'au
  timeout. Fix : garder le pump (ou une entrée demux légère) en vie
  après `ErrorWait` jusqu'à ce que le `UtpStream` soit droppé
  (équivalent `!m_attached` : les deux canaux fermés), en continuant à
  alimenter `incoming_packet` qui émet le RST — puis `unregister`.
  Sévérité : wire-visible mineur (fenêtre bornée côté upstream par le
  tick de session).
- **R3 — erreurs d'envoi datagramme jamais remontées au socket.**
  `let _ = dg.send_to_flags(...)` (`utp_stream.rs:390`) jette tout
  `io::Error`. Upstream traite deux cas dans `send_pkt` : (a)
  `error::message_size` → `m_mtu_ceiling = p->size - 1`,
  `update_mtu_limits()`, `mtu_probe = false`, `m_mtu_seq = 0` puis
  **renvoi immédiat sans DF** (`utp_stream.cpp:1786-1809`) ; (b) toute
  autre erreur → `m_error = ec`, `error_wait`, `test_socket_state`
  (`:1824-1831`). Sans ce feedback : une probe refusée par la stack
  (PMTU) n'est corrigée qu'au timeout RTO, et si elle n'était pas le
  seul paquet en vol sa perte compte comme congestion
  (`experienced_loss` → cut cwnd) ; une erreur de transport franche
  (ENETUNREACH, tunnel tombé) n'est apprise que par timeout. Fix :
  remonter l'erreur au pump → `sock.note_send_error(size, was_probe,
  err)` reproduisant (a)/(b) ; nécessite que `DatagramSocket` expose
  l'`io::Error` synchronement (déjà le cas — `send_to_flags` retourne
  `io::Result`). Sévérité : wire-visible mineur (convergence PMTU ;
  sans objet sur les transports qui ne renvoient jamais EMSGSIZE).
- **R4 — le site d'appel close-reason n'existe pas.** Le mécanisme est
  complet (`UtpSocket::set_close_reason` `utp_socket.rs:481-483`,
  émission ext-3 dans `send_pkt`, `WriterMsg::CloseReason`,
  `UtpStream::set_close_reason` `utp_stream.rs:509-511`,
  `error_to_close_reason` `close_reason.rs:80`), mais le trait
  `PeerStream` (`transport.rs:105-107`) n'expose pas
  `set_close_reason` : `peer_conn`/`session` n'ont aucun chemin pour
  appeler `error_to_close_reason(ec)` au disconnect, alors qu'upstream
  fait `set_close_reason(m_socket, error_to_close_reason(ec))` dans
  `peer_connection::disconnect` (`peer_connection.cpp:4290`) et lit
  `get_close_reason` côté reception (`on_close_reason`,
  `utp_stream.cpp:398`). En prod, aucun FIN ne porte l'extension.
  Fix : ajouter `set_close_reason(u16)`/`close_reason()` au trait
  `PeerStream` (no-op par défaut) + appel dans le chemin de
  disconnect peer avec la table déjà portée ; exposer
  `their_close_reason` pour l'alerte/logging.
  Sévérité : wire-visible mineur (interopérabilité d'observabilité —
  les pairs libtorrent logguent la cause).
- **R5 — `WriterMsg::CloseReason` post-Close droppé.** Le bras
  `wr_rx.recv(), if !write_closed` (`utp_stream.rs:447`) désactive
  *tout* le canal après `Close` ; un `set_close_reason` envoyé après
  le shutdown n'atteint jamais `m_close_reason`, alors qu'upstream
  lit `m_close_reason` à chaque `send_pkt` — y compris les
  **retransmissions** du FIN. Fix : garder le bras actif et ne
  dispatcher `Data` que si `!write_closed` (CloseReason toujours
  accepté). Sévérité : mineur (edge case).
- **R6 — drop avec données entrantes en vol saute le FIN.**
  `rd_tx.send(...).await.is_err() → break` (`utp_stream.rs:399-401`)
  quitte la boucle sans `sock.close()` : si le `UtpStream` est droppé
  pendant que du payload attend dans `rd_tx`, aucun FIN n'est émis —
  upstream `~utp_stream → destroy() → close()` tente toujours le FIN
  (`utp_stream.cpp:930-964`). Fix : appeler `sock.close()` + drainer
  l'outbox avant le `break`. Sévérité : mineur.
- **R7 — settings uTP non ré-appliqués à chaud.** Le flood-guard reste
  à 200 (`conn_limit` par défaut `utp_stream.rs:100`) ;
  `UtpManager::set_connection_limit` (`:160-162`) n'a aucun appelant ;
  `enable_incoming_utp`/`enable_outgoing_utp` et les `utp_*` sont
  figés à `enable_utp` (`engine.rs:434-459`) alors qu'upstream lit
  `m_sett` live à chaque paquet (`utp_socket_manager.cpp:163-171`) et
  `apply_settings_pack` propage `connections_limit`. Fix :
  `apply_settings` → `manager.set_connection_limit` + toggles
  listener/connector (ou config par `watch`). Sévérité : mineur
  (défaut = comportement upstream au boot).
- **R8 — `wait_connected` masque l'erreur réelle.**
  `utp_stream.rs:495-501` retourne `"utp connect failed"` générique au
  lieu de `last_error` (TimedOut/ConnectionReset/Aborted) qu'upstream
  transmet au connect-handler via `m_error`. Fix : `take_error()` →
  mapping `UtpError → io::Error` (réutiliser le match de
  `poll_read`). Sévérité : cosmétique/diagnostic.

## Fichiers du fix (si apply ultérieur)

- `crates/bitdaemon-net/src/utp_socket.rs` — garde `write` sur
  `m_out_eof`/`FinSent` (R1) ; hook `note_send_error` EMSGSIZE/erreur
  (R3).
- `crates/bitdaemon-net/src/utp_stream.rs` — bras `wr_rx` toujours
  actif + gate `Data` seulement (R5) ; `close()` avant `break` sur
  `rd_tx.send` err (R6) ; pump en vie post-`ErrorWait` jusqu'au drop
  du stream (R2) ; remonter l'erreur `send_to_flags` au socket (R3) ;
  `wait_connected` → erreur réelle (R8).
- `crates/bitdaemon-net/src/transport.rs` — `PeerStream::
  set_close_reason`/`close_reason` no-op par défaut (R4).
- `crates/bitdaemon-session/src/peer_conn.rs` (ou équivalent
  disconnect) — `set_close_reason(stream, error_to_close_reason(ec))`
  au disconnect (R4).
- `crates/bitdaemon/src/engine.rs` — `apply_settings` →
  `set_connection_limit` + toggles listener/connector (R7).
- `crates/bitdaemon-net/tests/test_utp_{socket,stream}.rs` —
  régressions : write-after-close → erreur (R1), RST émis vers conn
  morte tant que le stream vit (R2), EMSGSIZE clamp + renvoi immédiat
  (R3), close-reason tardif sur FIN retransmis (R5), drop-sans-FIN
  résolu (R6), erreur connect précise (R8).

## Non-bugs tranchés

- `m_stalled`/`writable()`/`subscribe_writable()` et la branche
  `need_resend` de `insert_packet` quand `m_stalled` : inatteignables
  structurellement — l'outbox est infaillible (`DatagramSocket` en
  fire-and-forget, ADR-0008) ; `would_block`/`try_again` n'existent
  pas sur cette interface. *Attention* : les erreurs d'envoi
  *réelles* existent — couvert par R3.
- `m_last_socket` + `m_deferred_ack` niveau manager (flush croisé
  quand un datagramme change de socket cible,
  `utp_socket_manager.cpp:140-149`) : remplacé par le flush fin-de-
  batch par pump — équivalent ou plus rapide ; pas d'ack inter-socket
  jamais différé plus d'une rafale.
- `micros_now()` = `Instant` monotone vs `clock_type` : strictement
  meilleur (upstream prévoit un fallback non-monotone).
- Callbacks (`on_read`/`on_write`/`on_connect`, `m_userdata`,
  `m_attached`) → état pollable + canaux ; `test_socket_state` saute
  directement à `Deleting` (`utp_socket.rs:1701-1714`) — équivalent
  "aucun handler" ; la fenêtre RST associée est tranchée par R2.
- `enable_incoming_utp`/`enable_outgoing_utp` runtime → présence du
  listener/connector ; le non-hot-apply est tranché par R7.
- `// #error send reset` (`utp_socket_manager.cpp:206`) : TODO
  upstream jamais implémenté — `return false` = drop ; parité.
- `check_invariant`/`check_receive_buffers`/`TORRENT_UTP_LOG` :
  scaffolding debug → won't port (AGENTS §8).
- `UtpSocketDemux` (`utp.rs`) : demux standalone hors chemin prod
  (`UtpManager` fait foi) ; conforme (ids random, clé `(ep,id)`),
  utilisé par les tests — conservé.
- `find_utp_socket` : inexistant au baseline `e7381c7bc`.
- Flag `udp_socket::peer_connection` sur les sends
  (`utp_socket_manager.cpp:110`) : sert au ratelimit/comptage de
  `udp_socket` — pas de consommateur tant que le bandwidth-manager
  n'est pas porté → transversal (A28), `DatagramSendFlags` pourra
  porter le bit le moment venu.
- `socket_pump` tick 50 ms vs cadence tick session upstream : borne
  supérieure de détection seulement ; `m_timeout` reste la référence
  (le `tick` upstream tourne aussi à `tick_interval`, non calé sur
  500 ms exacts).
- `poll_read` renvoie `Pending` après réception d'un chunk non vide +
  `wake_by_ref` (`utp_stream.rs:535-539`) : contrat tokio correct,
  sémantique identique (une poll de plus).
- `read()` borne `u16::MAX - header_size` + `m_error` surface-EOF :
  conforme `read_some` upstream (`utp_stream.cpp:641-655`) + extension
  pull-model.
- `send_fin` branche erreur sans `test_socket_state` : inatteignable
  (pas d'erreur de transport possible via outbox).
- `num_transmissions`/`m_num_timeouts` saturés (u8) vs wrap upstream :
  inatteignable — `num_resends`/`!confirmed` tuent avant.
- RST orphelin sur paquet non-SYN sans socket : upstream retourne
  `false` (traité comme non-uTP par la session), Rust droppe dans
  `route` — effet net identique.

## Historique — première passe (branche `audit/05-utp-transport`)

13 déviations corrigées (D1 gardes `close()`, D2 FIN via pump +
`poll_shutdown`/Drop, D3 mécanisme close-reason, D4 deferred-ack par
batch, D5 flag DF sur `send_to_flags` + `IP_MTU_DISCOVER`, D6 gating
version/retour dans `accept_syn`, D7 lecture subordonnée à la capacité
du canal lecteur, D8 `UtpSocketDemux` ids aléatoires + clé `(ep,id)`,
D9 `abort` → `test_socket_state`, D10 `UtpCounterSink` injecté + 11
compteurs + gauges d'état, D11 propagation `m_error` vers
`AsyncRead`, D12 `try_send` hors lock dans `route`, D13
`remove_datagram_socket`), tests de régression
`test_utp_{socket,stream}.rs` correspondants, gates verts. R4/R8
montrent que D3/D11 ont livré le *mécanisme* mais pas tous les *sites
d'appel* ; R1–R3/R5–R7 sont nouveaux.

## Conclusion

Le coeur protocolaire (framing, state machine, RTO/wrap, LEDBAT, MTU,
SACK/OOO, wnd, injection transport) est **conforme** à `e7381c7bc` et
les corrections de la première passe tiennent. Restent 8 déviations
résiduelles, toutes de sévérité mineure à modérée, concentrées sur les
bords : rejet d'écriture post-close (R1), fenêtre RST post-teardown
(R2), feedback d'erreur d'envoi/PMTU (R3), plomberie close-reason
jusqu'au peer (R4, R5), FIN sauté sur drop-avec-données (R6),
hot-apply des settings (R7), diagnostic de connect (R8).
