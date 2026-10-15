# Audit A05 — rapport d'analyse

Baseline upstream : `e7381c7bc` (`src/utp_stream.cpp`, `src/utp_socket_manager.cpp`,
`include/libtorrent/aux_/utp_stream.hpp`, `include/libtorrent/aux_/utp_socket_manager.hpp`,
`src/packet_buffer.cpp`, `src/timestamp_history.cpp`, `aux_/sliding_average.hpp`,
`src/settings_pack.cpp` pour les défauts).
Côté Rust : `crates/bitdaemon-net/src/utp.rs`, `utp_socket.rs`, `utp_stream.rs`,
`packet_buffer.rs`, `transport.rs` ; `bitdaemon-common/src/{timestamp_history,sliding_average}.rs`.

## Verdicts checklist

- Packet types : ST_DATA/FIN/STATE/RESET/SYN — header 20B exact (version, ext,
  conn_id, ts, tsdiff, wnd, seq, ack) — verdict : **conforme** — preuve :
  `utp.rs:53-116` (`UtpHeader::encode`/`parse`, layout identique à
  `aux_/utp_stream.hpp:120-133` : `type_ver`, `extension`, `be16 connection_id`,
  `be32 timestamp`, `be32 tsdiff`, `be32 wnd`, `be16 seq`, `be16 ack`) ;
  `UtpType` 0-4, version == 1 imposée au parse. Les ids d'allocation sont
  verbatim : sortant `send_id=random, recv_id=send_id-1`
  (`utp_stream.rs:247-248` = `utp_socket_manager.cpp:311-312`), entrant
  `send_id=syn_id, recv_id=+1` (`utp_stream.rs:172` = `:305-306`) ; le SYN
  porte `m_recv_id` (`utp_socket.rs:482` = `utp_stream.cpp:1000`).
- State machine : SYN_SENT → CONNECTED → FIN_SENT/CLOSE_WAIT, syn retransmit,
  connect timeout — verdict : **conforme** — preuve : `UtpState` ordonné comme
  `state_t` upstream (none/syn_sent/connected/fin_sent/error_wait/deleting,
  `utp_socket.rs:42-55` = `utp_stream.hpp:694+`) ; retransmission SYN et limite
  `syn_resends` dans `tick` (`utp_socket.rs:1722-1725` =
  `utp_stream.cpp:3495-3497`) ; connect-timeout via
  `!m_confirmed && m_num_timeouts > 0` (`utp_socket.rs:1680-1684` =
  `utp_stream.cpp:3423-3428`) ; `m_timeout` ctor = `connect_timeout`
  (`utp_socket.rs:248-249` = `:141`) ; transition syn_sent→connected sur
  `ack_nr == m_seq_nr-1` puis fallthrough partagé avec connected
  (`utp_socket.rs:1518-1529` = `utp_stream.cpp:2919-2952`).
- Resend : RTO basé rtt/var, exponentiel, seq wrap 16 bits — verdict :
  **conforme** — preuve : `packet_timeout` (`utp_socket.rs:1185-1200` =
  `utp_stream.cpp:3342-3359` : `max(min_timeout, rtt.mean + 2*avg_dev)` +
  `2^(n-1)` s, cap 60 s, `num_timeouts >= 7` → 60 s, state none → 3000 ms) ;
  marquage global `need_resend` + file `needs_resend` à l'expiration
  (`utp_socket.rs:1704-1717` = `:3475-3487` : même garde
  `i != m_acked_seq_nr && i != m_seq_nr`) ; wrap géré par `ACK_MASK`/
  `wrapping_*` + `compare_less_wrap` (port verbatim,
  `bitdaemon-common/timestamp_history.rs:13-19` = `utp_stream.cpp:122-134`) ;
  resend-window gate `!fast && size-hdr > window_left && in_flight > 0`
  (`utp_socket.rs:848-851` = `:1964-1970`) ; SACK-refresh / `remove_sack_header`
  (`utp_socket.rs:856-895, 1757-1765` = `:1998-2012, 1416-1439`).
- Delay measurement : tsdiff, constantes LEDBAT-ish — verdict : **conforme** —
  preuve : `UtpConfig` défauts verbatim (`utp_socket.rs:101-116` =
  `settings_pack.cpp:317-325,360` : target_delay 100, gain 3000,
  min_timeout 500, syn/fin/num_resends 2/2/3, connect_timeout 3000,
  loss_multiplier 50, cwnd_reduce_timer 100) ; `do_ledbat` port verbatim
  (`utp_socket.rs:1203-1254` = `utp_stream.cpp:3216-3330`, fixed-point <<16,
  ssthres, slow-start, cwnd_saturated, clamp `i64::MAX-cwnd-1`, plancher
  1 MSS) ; `m_reply_micro`/base-change ajustement borné ±10 ms
  (`utp_socket.rs:1289-1307` = `:2559-2580`) ; `m_delay_sample_hist[3]`
  min-of-3 clampé par min_rtt (`utp_socket.rs:1531-1536` = `:2960-2971`) ;
  `timestamp_history` et `sliding_average` = ports verbatim
  (`bitdaemon-common` vs `timestamp_history.cpp`, `aux_/sliding_average.hpp`).
- MTU discovery : probing, floor/ceiling — verdict : **déviation** (D5) —
  preuve : l'algorithme est conforme (`update_mtu_limits`
  `utp_socket.rs:1169-1182` = `:247-277` ; `init_mtu` `:1601-1614` =
  `:2472-2490` ; gating `mtu_probe` `m_mtu_seq==0 && seq!=0 && cwnd >
  floor*3` `:589-596` = `:1503-1507` ; fenêtre `restrict_mtu` max-sur-3
  `utp.rs:172-242` + `utp_stream.rs:61-83` = `utp_socket_manager.cpp:43,93` ;
  `mtu_for_dest` Teredo/SOCKS5/v6 `:1627-1641` = `:65-94`), **mais** le flag
  `dont_fragment` posé sur les probes est mort : `OutDatagram.dont_fragment`
  n'est jamais lu (`utp_stream.rs:311` — `dg.send_to(&d.bytes, d.to)`), le
  trait `DatagramSocket::send_to` n'a pas de paramètre flags ; upstream passe
  `udp_socket::dont_fragment` → IP_DF (`utp_socket_manager.cpp:96-112`).
- Out-of-order buffer : selective ack, gaps, reorder window — verdict :
  **conforme** — preuve : `consume_incoming_data` (`utp_socket.rs:1099-1153` =
  `utp_stream.cpp:2322-2440` : eof-gate, capacity-gate, in-order + drain
  inbuf, dup/OOO gates, `m_buffered_incoming_bytes`) ; `parse_sack`
  (`utp_socket.rs:991-1095` = `:1175-1336` : `resend[5]`, reverse-scan
  `dups > dup_ack_limit`, prune `last_resend`, `cut_cwnd` unique,
  `m_fast_resend_seq_nr` avancé au paquet retransmis) ; `write_sack`
  (`utp_socket.rs:525-539` = `:1916-1935`) ; reorder window
  `max(16, capacity/1100)` (`utp_socket.rs:1343-1354` = `:2634-2650`) ;
  `packet_buffer` = port de `packet_buffer.cpp` (insert/remove identiques).
- wnd advertisement vs buffer réel, `m_suspended`/deferred close — verdict :
  **déviation** (D1/D2/D3) — preuve conformité wnd : `wnd = max(capacity -
  buffered_incoming - receive_buffer_size, 0)` (`utp_socket.rs:763-767` =
  `utp_stream.cpp:1753-1755`), `m_adv_wnd` honoré dans send_pkt/resend
  (`utp_socket.rs:624-625, 842-843`). Déviations : `close()` sans les gardes
  nagle/outbuf (D1), aucun chemin n'émet le FIN (D2), `m_close_reason`
  jamais assignable (D3). Deferred ACK : mécanisme présent
  (`m_deferred_ack`) mais flush par datagramme, pas par drain (D4).
- Interface DatagramSocket injectée — aucun bind direct (ADR-0008) — verdict :
  **conforme** — preuve : `UtpManager::{pump_in,connect,accept_syn}` prennent
  `Arc<dyn DatagramSocket>` (`utp_stream.rs:160-276, 437-503`) ; aucun
  `bind` dans `utp*.rs` ; `OutDatagram` drainé vers `dg.send_to` par le pump.

## Déviations

- `crates/bitdaemon-net/src/utp_socket.rs:381-387` — `close()` ne garde que
  `state == Connected && m_write_buffer_size == 0` ; upstream exige aussi
  `!m_nagle_packet && m_outbuf.at(m_seq_nr) == nullptr`
  (`utp_stream.cpp:913-928`). Conséquences : `debug_assert!(nagle_packet.
  is_none())` dans `send_fin` (`utp_socket.rs:501`) peut tirer en debug ; en
  release `send_pkt(PKT_FIN)` récupère le paquet naglé → **FIN avec payload**
  sur le fil alors qu'upstream garantit `payload_size == 0` sur FIN
  (`utp_stream.cpp:1539`). Fix : porter les deux gardes ; le FIN différé reste
  couvert par la queue de `send_pkt` (`utp_socket.rs:799-805`). Sévérité :
  wire-visible + panic debug.
- `crates/bitdaemon-net/src/utp_stream.rs:420-422, 338-340, 350` —
  **aucun chemin n'émet le FIN** : `poll_shutdown` est un no-op, et un drop
  de `UtpStream` ferme `wr_tx` → le pump sort par `else` → `unregister`
  sans `sock.close()`. `UtpSocket::close`/`send_fin`/`UtpError::Eof`-graceful
  sont du code mort en prod (seuls les tests les exercent). Upstream
  `~utp_stream → destroy() → close()` émet le FIN puis garde l'impl détachée
  jusqu'à fin-ack/timeout (`utp_stream.cpp:930-964, 913-928`). Fix :
  message de contrôle vers le pump (variant dédiée ou watch flag) →
  `sock.close()` → continuer à pomper jusqu'à FinSent→ErrorWait/Deleting
  avant `unregister` ; câbler `poll_shutdown` et un `Drop` côté `UtpStream`.
  Sévérité : wire-visible (le pair apprend la fermeture par timeout, pas de
  half-close propre).
- `crates/bitdaemon-net/src/utp_socket.rs:204` — `m_close_reason` n'a
  **aucun setter ni appelant** : l'extension close-reason n'est jamais émise
  (le parse/emit filaire existe : `:600-614, 729-733, 1575-1580`). Upstream :
  `peer_connection.cpp:4290` `set_close_reason(m_socket,
  error_to_close_reason(ec))` ; `error_to_close_reason` est déjà porté
  (`net::close_reason`). Fix : `pub fn set_close_reason(u16)` sur
  `UtpSocket` + canal de contrôle `UtpStream` + appel depuis le teardown
  peer_conn/session. Sévérité : wire-visible mineur.
- `crates/bitdaemon-net/src/utp_stream.rs:336` — `send_deferred_ack()` après
  **chaque** datagramme entrant → un ST_STATE par paquet, là où upstream
  coalesce par batch UDP (`m_sm.defer_ack` + `socket_drained`,
  `utp_socket_manager.cpp:140-149, 231-247` ; `utp_stream.cpp:1405-1414`)
  → ≤ 1 ACK par rafale. Fix : drainer `inbound_rx` par `try_recv` jusqu'à
  vide avant un `send_deferred_ack` unique. Sévérité : wire-visible mineur
  (trafic ACK excédentaire).
- `crates/bitdaemon-net/src/utp_stream.rs:311` +
  `transport.rs:52` — `OutDatagram.dont_fragment` écrit, jamais honoré
  (`DatagramSocket::send_to` n'a pas de flags ; upstream
  `utp_socket_manager.cpp:110` → IP_DF). Sans DF, une probe sur-MTU est
  fragmentée par IP au lieu d'échouer → `m_mtu_floor` apprend un plafond
  supérieur au vrai path-MTU. Fix : ajouter un paramètre flags (ou
  `send_to_opts`) à `DatagramSocket`, impl `TokioUdpSocket` via
  `IP_MTU_DISCOVER`/`IP_DONTFRAG`. Sévérité : wire-visible (PMTU biaisée ;
  sans objet sur tunnels).
- `crates/bitdaemon-net/src/utp_stream.rs:166-184` — `accept_syn` ne teste
  pas la version (`syn[0] & 0xf == 1`) ni le retour booléen de
  `sock.incoming_packet` : un SYN invalide crée et enregistre quand même le
  socket (pump + entrée demux jusqu'au connect-timeout ~3 s). Upstream
  rejette `version != 1` avant création et drop l'impl sur `!ret`
  (`utp_socket_manager.cpp:125, 195-196`). Fix : parser le header dans
  `accept_syn`, respecter le booléen. Sévérité : état mineur.
- `crates/bitdaemon-net/src/utp_stream.rs:316` — `rd_tx.send(...).await`
  bloque la boucle du pump quand le lecteur stagne : plus de traitement
  entrant, plus d'ACK, plus de `tick` → timeouts spuriés possibles ; upstream
  continue d'acker (seule `adv_wnd` s'effondre). Fix : n'appeler
  `sock.read()` que si le canal a de la capacité (données laissées dans
  `receive_buffer`, comme upstream `m_receive_buffer`). Sévérité : état.
- `crates/bitdaemon-net/src/utp.rs:164-243` — `UtpSocketDemux` dormant :
  `new_socket` alloue `next_send_id+1` séquentiel au lieu de
  `random(0xffff)` (`utp_socket_manager.cpp:311`) et la clé demux = conn_id
  seul (upstream : `match(ep,id)` filtre aussi adresse+port → collisions
  inter-remotes possibles). Aucun utilisateur en prod (`UtpManager` fait
  foi) — latent ; corriger ou retirer avant tout usage. Sévérité : dormant.
- `crates/bitdaemon-net/src/utp_socket.rs:389-393` — `abort()` ne chaîne pas
  `test_socket_state()` (upstream `utp_stream.cpp:208-213` → error_wait puis
  deleting si handlers annulés). Aujourd'hui sans appelant ; le pump traite
  ErrorWait comme Deleting. Sévérité : cosmétique.
- `crates/bitdaemon-session/src/counters.rs:404-427` — les 11 variantes de
  compteurs uTP (`UtpPacketLoss`, `UtpTimeout`, `UtpPackets{In,Out}`,
  `UtpFastRetransmit`, `UtpPacketResend`, `UtpSamples{Above,Below}Target`,
  `UtpPayloadPkts{In,Out}`, `Utp{Invalid,Redundant}PktsIn`) sont déclarées
  mais **jamais incrémentées** — `bitdaemon-net` ne peut pas dépendre de
  `bitdaemon-session` ; il faut un sink injecté via `UtpConfig`/`UtpManager`
  (upstream `m_sm.inc_stats_counter`, ~15 sites dans `utp_stream.cpp`).
  Sévérité : observabilité (transversal — voir A29).
- `crates/bitdaemon-net/src/utp_stream.rs:320` — `Err(_) => break` :
  `TimedOut`/`ConnectionReset`/`Aborted` deviennent un EOF propre côté
  `AsyncRead` ; upstream forward `m_error` aux handlers
  (`utp_stream.cpp:2284-2320`). Sévérité : état mineur.
- `crates/bitdaemon-net/src/utp_stream.rs:104-125` — `route` tient
  `inner.lock()` pendant `tx.send().await` → head-of-line blocking entre
  sockets dès qu'un canal entrant (64) est plein. Fix : `try_send`, ou
  cloner le sender puis envoyer hors lock. Sévérité : état mineur / perf.
- `crates/bitdaemon-net/src/utp_stream.rs` — pas d'équivalent
  `utp_socket_manager::remove_udp_socket` (abort des conns dont le transport
  meurt, `utp_socket_manager.cpp:269-279`) : si le `DatagramSocket`
  sous-jacent disparaît, les conns uTP ne meurent que par timeout.
  Sévérité : état mineur.

## Fichiers du fix

- `crates/bitdaemon-net/src/utp_socket.rs` — gardes `close()` (D1), API
  `set_close_reason` (D3), `abort` → `test_socket_state` (D9).
- `crates/bitdaemon-net/src/utp_stream.rs` — chemin FIN via pump
  (`poll_shutdown`/Drop) (D2), batch drain + deferred-ack unique (D4),
  gating version/retour dans `accept_syn` (D6), capacité canal avant
  `sock.read()` (D7), portée du lock dans `route` (D12).
- `crates/bitdaemon-net/src/transport.rs` + `os_transports.rs` —
  flags `dont_fragment` sur `send_to` (D5).
- `crates/bitdaemon-net/src/utp.rs` — `UtpSocketDemux` : ids aléatoires +
  clé `(ep,id)` ou retrait (D8).
- `crates/bitdaemon-net/src/utp_stream.rs` ou `utp_socket.rs` +
  `bitdaemon-session` — sink compteurs injecté (D10) ; `set_close_reason`
  depuis le teardown peer_conn (D3).
- `crates/bitdaemon-net/tests/test_utp_socket.rs`,
  `tests/test_utp_stream.rs` — régressions (FIN propre, pas de FIN payload,
  coalescing ACK, accept refuse SYN v≠1, back-pressure ne tue pas le socket).

## Non-bugs tranchés

- `m_stalled` / `writable()` / `subscribe_writable()` / marquage
  `need_resend` dans `insert_packet` : l'outbox est infaillible par design
  (ADR-0008 — envois via `DatagramSocket` injecté en fire-and-forget) ; les
  branches `would_block`/`try_again` upstream sont structurellement
  inatteignables ici. La retransmission RTO couvre les pertes réelles du
  transport. Documenté dans le header de `utp_socket.rs`.
- `m_last_socket` + `m_deferred_ack` côté manager : remplacés par le pump
  par-socket ; seule la granularité du flush ACK diffère (→ D4).
- `micros_now()` = `Instant` monotone vs `clock_type` upstream : upstream
  prévoit un fallback non-monotone (rtt = 100 ms) ; `Instant` est
  strictement meilleur — documenté en code (`utp_socket.rs:1767-1774`).
- Buffers iovec utilisateur → `write_buffer` byte-queue + `read(&mut [u8])`
  pull API ; callbacks (`on_read`/`on_write`/`on_connect`) → état pollable.
  Adaptation documentée en tête de `utp_socket.rs`.
- `enable_incoming_utp` / `enable_outgoing_utp` : remplacés par la présence
  du listener (`UtpAcceptor::new`/`set_listener`) et le choix de connector.
- `// #error send reset` (reset sur paquet orphelin non-SYN) : TODO upstream
  non implémenté — `utp_socket_manager.cpp:206` retourne false, parité
  atteinte en ne faisant rien.
- `check_invariant`/`check_receive_buffers`/compteurs `utp_*_pkts_in` en
  early-return : scaffolding debug upstream → won't port (AGENTS §8) ;
  `m_in_packets`/`m_out_packets` locaux conservés.
- `m_attached`/`should_delete()`/`detach()` : pas de split stream/impl en
  Rust — le pump termine à `ErrorWait`/`Deleting` et `unregister` nettoie.
- `route` essaie `conn_id`, `conn_id-1`, `conn_id+1` (`utp_stream.rs:108-112`)
  : tolérance supplémentaire vs `equal_range(id)` upstream — le socket cible
  rejette les conn_id ≠ `m_recv_id` comme upstream `match`/`incoming_packet`,
  effet net identique (drop).
- `socket_pump` tick 50 ms vs cadence tick session upstream : détecte les
  timeouts plus tôt seulement ; `m_timeout` reste la référence.
- `UtpStream::poll_read` copie byte-par-byte (`VecDeque<u8>`) : perf, pas
  sémantique.
- Timestamps u32 wrap (`& 0xffff_ffff`) : identique upstream.
- `read()` borne `u16::MAX - header_size` et `to_copy` min triplé :
  `utp_socket.rs:427-436` = `utp_stream.cpp:641-655` — conforme.
- `send_fin` branche erreur sans `test_socket_state` (`utp_socket.rs:503-508`)
  : inatteignable (pas d'erreur de transport possible), couvert par D9.
