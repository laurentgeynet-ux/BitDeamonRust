# Audit A27 — rapport d'analyse

Périmètre : `crates/bitdaemon/src/engine.rs` (boucle `Engine::run`,
`tick`, dispatch des `SessionOut`, pompes acceptor/UDP/uTP,
NAT-PMP/UPnP) vs `session_impl.cpp` (`on_tick`, `run`, dispatch
io_context) — baseline `e7381c7bc`.

## Verdicts checklist

- Ordonnancement : ordre commandes vs ticks vs disk completions vs net
  events — verdict : **déviation** — preuve : `run()` ne sert
  `self.tick()` que sur le bras `tick_timer`
  (`engine.rs:3818-3908`) ; les files `gone_rx`, `connect_rx`,
  `udp_rx`, `announce_rx`, `webseed_rx`, `dhtnode_rx`, `upnp_rx`,
  `upnp_dev_rx` ne sont drainées que dans `tick()`
  (`engine.rs:703-804`), cadence ~`tick_interval` (500 ms), alors
  qu'upstream exécute chaque handler de complétion immédiatement sur
  l'io_context (`session_impl::on_connection_attempt`,
  `on_udp`/`udp_socket` read handler, `tracker_manager` completion).
  Conséquences : mort d'un pair → `peer_disconnected` + remboursement
  des blocs retardés ≤500 ms ; dial terminé → attach + bitfield
  retardés ≤500 ms (divise de fait le pacing `connection_speed` sur
  les connexions courtes) ; réponses DHT / tracker UDP / web-seed
  retardées d'autant. Seuls `inbound`, `sent`, `disk_notify`,
  `fd_exhausted` ont un bras immédiat. L'ordre *interne* de `tick()`
  (drains → `update_quotas` → gate 1 s `Session::tick` →
  `tracker_announce_due` → dispatch) suit fidèlement `on_tick`
  (`session_impl.cpp:3505-3895`), y compris les scalers auto-manage /
  auto-scrape / unchoke / optimistic / peer_turnover
  (`session.rs:6792-7248`, `unchoke_tick` `:8240`).
- Tick starvation : le bug corrigé — vérifier toutes les sources de
  réveil — verdict : **conforme** — preuve : `Sleep` pinné hors de la
  boucle + `reset` à `now + tick_interval` relu des settings à chaque
  armement (`engine.rs:3807-3825`) — même modèle que
  `m_timer.expires_at(now + tick_interval)` upstream
  (`session_impl.cpp:3571-3573`). `select!` sans `biased` choisit un
  bras prêt au hasard : un bras permanent prêt ne peut pas bannir le
  timer une fois son échéance passée, et chaque bras non-timer fait
  `continue` sans bloquer. Les sources de réveil listées (inbound,
  disk_notify, fd_exhausted, sent, timer, stop) couvrent les files à
  latence immédiate ; le reste est drainé au tick par conception
  (voir déviation ordonnancement). Point résiduel :
  `last_second_tick = Instant::now()` à la construction
  (`engine.rs:441`) retarde le premier `second_tick` d'1 s pleine ;
  upstream prime `m_last_second_tick = m_created - 900ms`
  (`session_impl.cpp:512`) pour que le premier tick lourd arrive ~100
  ms après le démarrage.
- Command channel : ordre FIFO préservé ? priorités ? — verdict :
  **déviation (mineure)** — preuve : pas de canal de commandes —
  `SessionHandle` verrouille `Arc<Mutex<Session>>` et exécute
  synchronement (`session_handle.rs:185`, `add_torrent` `:202`,
  `lock()`/`slock`). Upstream poste `session_call` sur l'io_context →
  FIFO garanti entre appelants et sérialisation homogène avec les
  handlers réseau. Chez nous : l'acquisition du `Mutex` n'est pas FIFO
  (ordre d'appel non garanti entre threads concurrents) et un appel
  handle peut s'intercaler *entre deux drains* d'un même `tick()` (le
  verrou est repris à chaque `slock`, `engine.rs:703-829`). Les effets
  `SessionOut` produits par les handle APIs vont bien dans
  `Session::pending_out` et drainent au tick suivant
  (`session.rs:7217`, `route_into_pending` `:3718`) — sauf le cas
  `add_torrent_params` ci-dessous.
- Alert routing : broadcast + notify + wait_for_alert — verdict :
  **déviation** — preuve : `AlertManager` lui-même est conforme
  (notify sur transition vide→non-vide `alerts.rs:1795-1804`,
  `get_all` swap + synthèse `AlertsDropped` `:1810-1818`, queue-limit
  pondérée `:1791`). Mais `Engine::tick` termine par
  `pop_alerts()` (`engine.rs:829`) et `run()` jette le `Vec<Alert>`
  retourné (`engine.rs:3908` — `self.tick(Instant::now()).await;`
  sans consommer la valeur). Résultat : sous `run()` — le driver
  canonique, utilisé par `bitdaemon-cli` (`main.rs:711`) — chaque
  alerte postée pendant le tick est **vidée puis perdue** ;
  `SessionHandle::pop_alerts`/`wait_for_alert` ne voient plus rien
  sauf les alertes postées entre deux drains par les handle calls.
  Upstream laisse la queue remplie jusqu'à `pop_alerts` utilisateur
  (`alert_manager::get_all`). `wait_for_alert` en polling 1 ms
  (`session_handle.rs:416-431`) est sémantiquement conforme au condvar
  upstream.
- Peer pump : connect/dial ordering, acceptor pump,
  handshake_timeout — verdict : **déviation** — preuve : dial ordering
  conforme — `try_connect_more_peers` tourne dans `Session::tick` à la
  position upstream exacte (après auto-scrape, avant les scalers
  unchoke — `session.rs:7135-7152` vs `session_impl.cpp:3830`), et le
  dial pose `PeerConnect` + `handshake_timeout` des deux côtés
  (`engine.rs:3340`, `:3517`). **Mais** la pompe acceptor
  (`start_acceptor_at`, `engine.rs:3445-3588`) exécute le handshake
  responder **inline dans la boucle d'accept** : un pair muet bloque
  tous les accepts suivants jusqu'à `handshake_timeout` (défaut 10 s),
  et N pairs muets sérialisent N×timeout de head-of-line. Upstream
  crée un `peer_connection` par accept — chaque handshake est async et
  les accepts continuent ; la deadline `handshake_timeout` des conns
  sans torrent est appliquée depuis `on_tick`
  (`session_impl.cpp:3719-3740`), pas en sérialisant l'accept.
- Timers : chaque deadline upstream a son tick — verdict : **conforme
  (une réserve)** — preuve : second_tick `session.rs:6792` (incl.
  wrap `session_time` 65000 `:6799`, mixed-mode `:6807`, auto-manage
  `:6811`, reaper disque 60 s `:6823-6831`, DHT `dht_announce_tick`
  `:7257` — pacing `dht_announce_interval/N` en budget ms, LSD
  `lsd_tick` `:6728` — `local_service_announce_interval/N`, auto-scrape
  `:7107`, connect pump `:7140`, unchoke/optimistic `:8240`,
  peer_turnover `:7159`). uTP : `utp_socket_manager::tick/decay` est
  porté en tâche pump interne 50 ms par socket
  (`utp_socket.rs:1770`, `:408-507` — `decay()` upstream n'est que
  `m_packet_pool.decay()`, N-A sans pool). Réserve :
  `m_stat.second_tick` session-level n'existe pas en tant que tel —
  couvert par `update_rates` par torrent (`session.rs:6867`) +
  compteurs partagés ; le check `rate_limit_ip_overhead` global est
  porté `:7073-7101`.

## Déviations

- `engine.rs:3908` + `engine.rs:829` — `run()` appelle
  `self.tick()` qui finit par `pop_alerts()` ; le `Vec<Alert>` est
  jeté → **toutes les alertes produites pendant un tick sont
  perdues** quand la session est drivée par `run()` (le cas normal —
  `bitdaemon-cli` l'utilise). Upstream : les alertes restent en queue
  jusqu'à `pop_alerts`/`wait_for_alert` utilisateur. Sévérité :
  **état** (API-visible — `TorrentAdded`, `Stats`, `PieceFinished`,
  `tracker_reply`… inobservables).
  - **sketch** : ne pas drainer dans `tick`. `engine.rs:829`
    `slock(&self.session).pop_alerts()` → `Vec::new()` ; ou mieux,
    garder la signature : faire retourner à `tick` uniquement les
    alertes d'un canal broadcast dédié. Fix minimal sans changement
    d'API : dans `tick`, remplacer la ligne finale par
    `Vec::new()` (aucun test ne consomme la valeur de retour — tous
    les `eng.tick(..)` dans `test_engine.rs` l'ignorent) et corriger
    la rustdoc. Variante plus fidèle si on veut un flux push : ajouter
    `Option<broadcast::Sender<Alert>>` sur `Engine`, alimenté dans
    `run()` par le retour de `tick()` — alors `tick` garde le drain
    mais rien n'est perdu. Réf. upstream `alert_manager::get_all`
    (`alert_manager.cpp`) + `session_impl::pop_alerts`. Test de
    régression suggéré : `run()` démarré, ajouter un torrent, attendre
    > 2 ticks, `handle.pop_alerts()` doit contenir
    `Alert::TorrentAdded`/`AddTorrent`.
- `engine.rs:3445-3588` (`start_acceptor_at`) — handshake responder
  inline dans la boucle d'accept → head-of-line : chaque accept
  attend `run_handshake_responder_auto` borné par `handshake_timeout`
  avant le prochain `accept()`. Upstream : handshake async par conn ;
  l'accept immédiat continue (`session_impl::on_accept` →
  `peer_connection` self-driven ; timeout torrent-less via `on_tick`
  `session_impl.cpp:3719-3740`). Sévérité : **wire-visible** (N pairs
  muets retardent tous les accepts entrants de N×10 s).
  - **sketch** : dans la boucle d'accept (après le verdict
    `Accept`/`AcceptExceeded` à `engine.rs:3485`), `tokio::spawn` une
    tâche par conn contenant le bloc `:3502-3585` (lecture
    `pe`/`hs_timeout`, `our_pid`, timeout + handshake, envoi du
    `DialDone`/`peer_conn_failed_inbound`) au lieu de l'`await`er ;
    pousser le `JoinHandle` dans un `Vec` local ou un
    `JoinSet` (ou `tasks` via un canal dédié — `tasks` est
    `&mut self` hors de la tâche spawnée, donc simplement `drop` du
    handle ou `spawn` détaché : les `DialDone` remontent déjà par
    `connect_tx`). Réf. `session_impl.cpp` `on_accept` +
    `peer_connection::start`. Test de régression : deux accepts
    simultanés, premier pair silencieux → le second handshake
    complète avant `handshake_timeout`.
- `engine.rs:706-804` vs bras `run()` — les complétions
  `gone_rx`/`connect_rx`/`udp_rx`/`announce_rx`/`webseed_rx`/
  `dhtnode_rx`/`upnp_rx`/`upnp_dev_rx` ne sont servies qu'au tick
  (≤500 ms de latence ajoutée) ; upstream les exécute à réception sur
  l'io_context. Les plus sensibles : `gone_rx` (remboursement des
  blocs en vol, compteurs, alertes de déco), `connect_rx` (attach +
  bitfield retardés), `udp_rx` (réponses DHT/tracker-UDP), `webseed_rx`
  (turnaround des batches). Sévérité : **état / timing wire**.
  - **sketch** : ajouter dans le `select!` de `run()`
    (`engine.rs:3818-3903`) des bras drain-immédiat calqués sur
    `sent_rx`/`inbound_rx` :
    `_ = self.gone_rx.recv() => { drain gone → peer_disconnected → dispatch; continue }`,
    `d = self.connect_rx.recv() => { on_dial_done(first) + drain ≤N → dispatch out; continue }`,
    `_ = self.udp_rx.recv() => { drain avec la même demux que tick:746-771 (extraire ce bloc en `fn drain_udp(&mut self, out)`) → dispatch; continue }`,
    `_ = self.announce_rx.recv()` et `_ = self.webseed_rx.recv()` de
    même (extraire `drain_announces`/`drain_webseeds` de `tick`).
    Les `try_recv` dans `tick()` deviennent des restes — conserver en
    l'état. Réf. `session_impl` io_context (handlers postés à chaque
    complétion). Attention : `connect_rx` à haute fréquence peut
    voler le timer — borner chaque drain (comme les `for _ in 0..1024`
    existants) puisque `select!` est équitable ça reste sûr. Test de
    régression : kill d'un pair → `peer_disconnected` appliqué en
    << 500 ms ; dial complété → `ToPeer` handshake/bitfield émis sans
    attendre le tick.
- `session_handle.rs:231-243` — `SessionHandle::add_torrent_params`
  collecte `out` puis le **drop** : les `SessionOut` produits
  (`DhtAddNode` des `nodes` du .torrent, `route(eff)` incl.
  `TrackerAnnounce`/`ConnectPeer` immédiats) sont perdus pour les
  appelants du handle. Upstream `add_torrent` s'exécute sur le thread
  réseau, ses effets arrivent. L'annonce tracker se régénère au tick
  suivant via `tracker_announce_due` (`session.rs:9120`), mais
  `DhtAddNode` est perdu définitivement. Sévérité : **état**.
  - **sketch** : `session_handle.rs:231` — après l'appel,
    `s.pending_out.append(&mut out)` avant de relâcher le verrou
    (`pending_out` est `pub`, drainé par `Session::tick`
    `session.rs:7217`) — ou faire retourner `out` au caller comme le
    fait déjà `dispatch_out` côté engine. Diff minimal :
    `let mut s = lock(&self.inner); let ih = s.add_torrent_params(atp, INVALID, &mut out)?; s.pending_out.append(&mut out);`
    Test : `add_torrent_params` avec `dht_nodes` non vides →
    `SessionOut::DhtAddNode` présent dans le prochain drain.
- `engine.rs:441` — `last_second_tick: Instant::now()` retarde le
  premier `second_tick` d'une seconde pleine ; upstream prime à
  `m_created - 900 ms` (`session_impl.cpp:512`) pour un premier tick
  lourd ~100 ms post-start (reprends les web-seeds, conn-init,
  annonces rapides). Sévérité : **cosmétique/état** (démarrage plus
  paresseux).
  - **sketch** : `Engine::new` → `last_second_tick:
    Instant::now() - Duration::from_millis(900)` (`Instant`
    supporte `- Duration` ; ou `checked_sub` + unwrap_or(now) pour
    rester sans panique). Réf. `session_impl.cpp:512`. Test : tick
    #1 à t=+100 ms doit exécuter le chemin lourd (ex. `stats_alert`
    émis au premier tick).

## Fichiers du fix

- `crates/bitdaemon/src/engine.rs` (run select arms, tick tail,
  acceptor spawn, `last_second_tick` init)
- `crates/bitdaemon-session/src/session_handle.rs` (`add_torrent_params`
  → `pending_out`)
- tests : `crates/bitdaemon/tests/test_engine.rs` (+ éventuellement un
  nouveau `test_engine_glue.rs`)

## Non-bugs tranchés

- `m_utp_socket_manager.tick/decay` : tick porté en pump interne 50 ms
  par socket (`utp_socket.rs` `interval(50ms)` → `sock.tick`), plus
  fin que le 500 ms upstream ; `decay()` n'est que le déclin du pool
  de paquets C++ (`m_packet_pool.decay()`), sans équivalent mémoire
  chez nous. Conforme par structure différente.
- `wait_for_alert` en busy-poll 1 ms au lieu du condvar : sémantique
  identique, coût CPU négligeable côté appelant.
- Absence de `m_undead_peers`/abort-stage-2 : notre arrêt est
  `stop` future + `JoinHandle` abort — modèle différent, pas de
  wire-semantics associée.
- `m_abort`/`raw_tick_ms < 0` gate de test upstream : `Duration`
  ne peut pas être négatif — non portable tel quel, justifié.
- `tracker_announce_due` au pas 500 ms vs timers par requête
  upstream : la granularité est bornée par `tick_interval`, comme
  tout le modèle de pacing du port (dht/lsd budget ms) — cohérent
  avec les choix d'architecture, précision ≤500 ms sur l'heure
  d'annonce effective.
