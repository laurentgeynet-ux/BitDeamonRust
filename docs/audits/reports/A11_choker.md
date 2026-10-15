# Audit A11 — rapport d'analyse

Périmètre : choker & optimistic unchoke. Upstream `src/choker.cpp`
(comparateurs + `unchoke_sort`), `src/session_impl.cpp`
(`recalculate_unchoke_slots`, `recalculate_optimistic_unchoke_slots`,
tick scalers, `update_unchoke_limit`, `preemptive_unchoke`),
`src/peer_connection.cpp` (`send_choke`, `send_unchoke`,
`choke_this_peer`, `maybe_unchoke_this_peer`), `src/torrent.cpp`
(`choke_peer`, `unchoke_peer`, `remove_peer` legs),
`src/bt_peer_connection.cpp` (gate `num_have > 0`). Baseline
`e7381c7bc` (`D:/Projet/libtorrent/libtorrent`).

## Verdicts checklist

- `unchoke_sort` verbatim : comparateurs rate-based/RR/fastest/anti-leech, quota `piece_length·quota` + 60 s, nth_element top-slots — verdict : **déviation** — les comparateurs eux-mêmes sont conformes
  (`choker.rs:70-146` vs `choker.cpp:28-170`), le quota
  `> piece_length * pieces && > 60 s` est verbatim
  (`choker.rs:83-87` vs `choker.cpp:70-80`), le `nth_element`
  est porté en `select_nth_unstable_by(slots-1)` — même partition
  top-`slots`. **Mais** la valeur de retour est plafonnée à
  `min(upload_slots, peers.len())` (`choker.rs:199-211`) alors
  qu'upstream retourne `upload_slots` non plafonné
  (`choker.cpp:278`). Voir Déviations #1. Autre sous-point :
  `upload_rate_compare` trie par `uploaded_last * priority`
  descendant (`choker.rs:184` vs `choker.cpp:172-182`) mais
  `priority` est câblé à 1 à l'appel — voir #4.
- Slots : fixed vs rate-based, `unchoke_slots_limit`, seed expansions — verdict : **déviation** — fixed/unlimited
  (`slots_limit < 0 → i32::MAX`, `choker.rs:173-177` vs
  `choker.cpp:198-200`) et le seuil rate-based `+2048` sont
  conformes (`choker.rs:178-195` vs `choker.cpp:212-239`), mais
  le `rate` rate-based est calculé sur l'intervalle **configuré**
  (`settings.unchoke_interval`, `session.rs:8102`) au lieu de
  l'intervalle **mesuré** `now - m_last_choke`
  (`session_impl.cpp:4618`). Voir #5.
- Optimistic unchoke : période, rotation, `num_opt_unchoke` — verdict : **déviation** — période
  `optimistic_unchoke_interval` + rotation par
  `last_optimistically_unchoked` u16 croissant + quota
  `num_optimistic==0 → max(1, slots/5)` sont conformes
  (`session.rs:7996,8223-8233` vs `session_impl.cpp:3850-3855,4404-4419`),
  mais les candidats oublient le filtre `!ignore_unchoke_slots()`
  (`session_impl.cpp:4394`) et le leg final
  `num_unchoke_slots < num_peers_up_unchoked_all → m_unchoke_time_scaler = 0`
  (`session_impl.cpp:4478-4482`) n'a pas d'équivalent. Voir #3 et #6d.
- Snub/parole/endgame peers exclus ou inclus selon upstream — verdict : **N-A** — upstream n'a aucun filtre
  snub/parole/endgame dans le choker ; le Rust non plus
  (`session.rs:8058-8093` ne filtre que paused/web_seed/pi-none/
  uninterested/disconnecting/connecting).
- Choke/unchoke effects : envoi msg + `expect_choke` mark + pending requests handling — verdict : **conforme** (avec réserves
  comptées ailleurs) — `send_choke` : bit optimistic droppé +
  counter, suggestions clear, `m_last_choke`, `num_invalid_requests=0`,
  reject de toute la `upload_queue` hors accept-fast avec
  `choked_piece_requests` + `piece_rejects` comptés même sans
  `supports_fast`, `num_peers_up_requests` quand la queue se vide
  (`torrent.rs:10837-10891` vs `peer_connection.cpp:3822-3876`).
  `send_unchoke` : suggestions `suggest_read_cache` avant l'Unchoke,
  `m_last_unchoke`, `uploaded_at_last_unchoke`, counters
  (`torrent.rs:10930-10948` vs `peer_connection.cpp:3878-3908`).
  `max_uploads` veto sauf optimistic (`torrent.rs:10921` vs
  `torrent.cpp:6407`). Le mark « expect_choke » de la checklist n'a
  pas d'équivalent upstream (le concept est `m_last_choke`, porté).
  Réserve : les triggers unchoke/optimistic que `choke_this_peer`
  arme upstream (`peer_connection.cpp:1936-1939`) manquent — voir #6a.
- peer_class filters dans le calcul des slots — verdict : **déviation** — `ChokePeer.priority` est câblé à `1`
  (`session.rs:8091`) alors que `compare_peers` upstream trie d'abord
  sur `get_priority(upload_channel)` (max des priorités du class set,
  `choker.cpp:30-33` + `peer_connection.cpp:263-275`). Et
  `ignore_unchoke_slots()` — conn exemptée du choker — n'est pas
  filtré dans le sweep d'éligibilité (`session_impl.cpp:4642`) ni
  dans les candidats optimistic (`session_impl.cpp:4394`). Voir #2, #3, #4.

## Déviations

- `crates/bitdaemon-session/src/choker.rs:199-211` — `unchoke_sort`
  retourne `slots = min(upload_slots, peers.len())` ; upstream retourne
  `upload_slots` brut (`src/choker.cpp:278`, `return upload_slots;`).
  Conséquence : quand les pairs éligibles sont moins nombreux que le
  quota, `unchoke_set_size = allowed - num_opt_unchoke`
  (`session.rs:8129`, upstream `session_impl.cpp:4708`) est
  sous-dimensionné — ex. `unchoke_slots_limit=8`, 2 pairs éligibles :
  upstream `num_opt = max(1, 8/5) = 1`, `unchoke_set = 7` → les 2 pairs
  unchokés ; Rust `allowed = 2` → `unchoke_set = 1` → le 2ᵉ reste
  choké jusqu'au tour optimistic. **Wire-visible.** Sous rate-based,
  la gauge `num_unchoke_slots` est aussi fausse
  (`session.rs:8112-8119` vs `session_impl.cpp:4687-4688`).
  - **sketch** : dans `choker.rs`, renvoyer `upload_slots as usize`
    (saturé `usize::MAX` quand `slots_limit < 0`) au lieu de `slots`,
    en gardant `slots` local pour `select_nth_unstable_by` ; dans
    `recalculate_unchoke_slots`, utiliser `allowed` comme borne du set
    (le `unchoke_set > 0` déjà existant fait le reste — itérer plus de
    pairs que de slots est sans effet). Test : fixed-slots=8, 2 pairs
    éligibles → les 2 doivent recevoir `Unchoke`.
- `crates/bitdaemon-session/src/session.rs:8058-8093` — le sweep
  d'éligibilité n'exclut pas `c.ignores_unchoke_slots(pool)` /
  `t.ignores_unchoke_slots(pool)` ; upstream skippe ces conns en
  premier (`src/session_impl.cpp:4642-4647`, reset counters inclus).
  Conséquence : une conn exemptée (ex. classe `ignore_unchoke_slots`,
  comme `m_local_peer_class` upstream `session_impl.cpp:656`) entre
  dans le sort, consomme un slot et peut être chokée par la queue ou
  par la branche uninterested. **Wire-visible.**
  - **sketch** : dans la boucle d'éligibilité, avant le test
    `paused || web_seed || pi.is_none()`, ajouter
    `if c.ignores_unchoke_slots(&self.peer_class_pool) || t_ign { c.stat_up_round = 0; c.stat_down_round = 0; continue; }`
    (`t_ign` calculé une fois par torrent comme dans `unchoke_peer`).
    Réf. `src/session_impl.cpp:4642-4647`. Test : conn flaggée
    `ignore_unchoke_slots` unchokée + uninterested → reste unchokée
    après le tick.
- `crates/bitdaemon-session/src/session.rs:8204-8220` — les candidats
  optimistic oublient `!p->ignore_unchoke_slots()`
  (`src/session_impl.cpp:4394`).
  - **sketch** : ajouter `|| c.ignores_unchoke_slots(pool) || t_ign`
    au `continue` de la boucle candidats. Test : conn exemptée chokée
    ne doit pas recevoir d'Unchoke optimistic.
- `crates/bitdaemon-session/src/session.rs:8091` — `priority: 1`
  câblé ; upstream `get_priority(upload_channel)` = max des
  `peer_class.priority[UPLOAD]` sur le class set de la conn
  (`src/choker.cpp:30-33`, `src/peer_connection.cpp:263-275`).
  Conséquence : `compare_peers` et `upload_rate_compare` perdent le
  premier critère de tri — les classes à `upload_priority` > 1 ne sont
  pas préférées. **État/wire-visible** (ordre d'unchoke).
  - **sketch** : ajouter à `PeerConn` un
    `fn upload_priority(&self, pool) -> i32` calqué sur
    `ignores_unchoke_slots` (`max` des `pc.priority[UPLOAD]`,
    défaut 1 si set vide — vérifier upstream : `get_priority` part de
    la prio de la classe par défaut), remplir `ChokePeer.priority`
    avec. Réf. `src/peer_connection.cpp` `peer_connection::get_priority`.
    Test : deux pairs, même `downloaded_last`, classes prio 1 vs 3 →
    le prio 3 est devant après `unchoke_sort`.
- `crates/bitdaemon-session/src/session.rs:8102` — l'intervalle passé
  à `unchoke_sort` est `settings.unchoke_interval` (configuré) ;
  upstream passe `now - m_last_choke` (mesuré, `session_impl.cpp:4618-4619`).
  N'affecte que le choker `rate_based` (division du rate). **État.**
  - **sketch** : champ `last_choke: Instant` sur `Session`, initialisé
    au premier tick ; dans `recalculate_unchoke_slots` calculer
    `let interval = now.duration_since(self.last_choke); self.last_choke = now;`
    et passer `interval` à `unchoke_sort`. Réf.
    `src/session_impl.cpp:4617-4619`. Test : tick déclenché par
    `triggered_unchoke` avant l'échéance → le rate rate-based est
    calculé sur l'intervalle réel.
- Triggers manquants (3 sites, même fix sémantique) :
  - (a) `peer_conn.rs:1194` → `TorrentEffect::ChokePeer` →
    `session.rs:8506-8513` : upstream `choke_this_peer` appelle
    `t->trigger_unchoke()` (slot libéré → refill immédiat) et
    `t->trigger_optimistic_unchoke()` si le pair était optimistic
    (`src/peer_connection.cpp:1932-1939`). Le Rust ne ré-arme rien →
    le slot reste vacant jusqu'au prochain tick périodique.
  - (b) `session.rs:8094-8097` (sweep choke d'un pair optimistic
    uninterested/disconnecting) : upstream met
    `m_optimistic_unchoke_time_scaler = 0` (`session_impl.cpp:4660-4668`).
  - (c) `session.rs:8163-8167` (promotion d'un optimistic dans le set
    réel) : upstream met aussi `m_optimistic_unchoke_time_scaler = 0`
    (`session_impl.cpp:4737-4741`).
  - **sketch** : `Torrent::choke_peer` positionne
    `self.triggered_optimistic = true` quand il clear
    `optimistically_unchoked`, et `self.triggered_unchoke = true`
    quand il choke une conn non-exempte qui était unchokée — cela
    couvre (a) et (b) puisque tout passe par `choke_peer`. Pour (c),
    `self.triggered_optimistic = true` (ou équivalent session) dans le
    leg de promotion. Réf. `src/session_impl.cpp:4665,4740` et
    `src/peer_connection.cpp:1936-1939`. Attention : les flags sont
    consommés en début de `unchoke_tick` — un flag posé *pendant* le
    recalc doit survivre au clear du début de tick (le clear a lieu
    avant, OK). Test : NotInterested d'un pair unchoké → tick suivant
    re-pick immédiat.
  - (d) fin de `recalculate_optimistic_unchoke_slots` : upstream
    `if (num_unchoke_slots < num_peers_up_unchoked_all)
    m_unchoke_time_scaler = 0;` (`session_impl.cpp:4478-4482`) —
    le surplus d'unchokés (optimistic poussant au-delà du quota)
    déclenche un recalc régulier au prochain tick. Rust : absent.
    - **sketch** : en fin de fonction, comparer les deux counters et
      si dépassement, re-exécuter `recalculate_unchoke_slots` au
      prochain tick (flag session `pending_unchoke_recalc` consommé
      dans `unchoke_tick`, ou appel direct en fin de fonction avec le
      même `fixed`/`now` — upstream diffère au tick suivant, préférer
      le flag). Test : 1 slot + 1 optimistic → le recalc suivant choke
      le surplus.
- `crates/bitdaemon-session/src/torrent.rs:10991-10995` —
  `free_upload_slots` compte toutes les conns unchokées ; upstream
  `m_num_uploads` ne compte que les slot-consommateurs
  (`include/libtorrent/aux_/torrent.hpp:741`,
  invariant `torrent.cpp:9559` : `!is_choked() &&
  !ignore_unchoke_slots()`). Conséquence : des conns exemptes
  unchokées peuvent bloquer les candidats optimistic sur un torrent à
  `max_uploads` serré. **État.**
  - **sketch** : passer `pool` à `free_upload_slots` (ou précalculer
    `t_ign` + filtrer `!c.ignores_unchoke_slots(pool)`) à l'appelant
    `session.rs:8201` ; réutiliser le même filtre que
    `unchoke_peer:10911-10915`. Test : torrent `max_uploads=1`, une
    conn exempte unchokée → `free_upload_slots` reste vrai.
- `crates/bitdaemon-session/src/peer_conn.rs:1184` — à la réception
  d'`Interested`, upstream appelle `maybe_unchoke_this_peer()`
  (`src/peer_connection.cpp:1848`) et, si déjà unchoké, émet un
  `write_unchoke()` redondant (quirk pour clients qui ratent le
  premier Unchoke du handshake, `peer_connection.cpp:1831-1845`) ;
  upstream court-circuite aussi sous `graceful_pause` (1822-1829). Le
  Rust n'a ni le rappel de `maybe_unchoke` ni le leg redondant.
  Conséquence : conn attachée quand `num_have()==0` (ou hors
  preemptive) puis Interested → attend le tick choker ; et le unchoke
  redondant n'est jamais émis. **Mineur, wire-visible** (le Unchoke
  dupliqué est un comportement observable).
  - **sketch** : ajouter `PeerEffect::MaybeUnchoke` émis par le leg
    `Interested` (après `on_interested`, sauf si `disconnecting` ou
    `graceful_pause` du torrent — flag à faire remonter), routé vers
    `t.maybe_unchoke_this_peer` dans `session.rs` ; dans ce cas,
    upstream n'a **pas** le gate `num_have > 0` (il est côté
    `bt_peer_connection.cpp:3646`, handshake seulement) — sortir le
    gate `have.count()==0` de `maybe_unchoke_this_peer` vers
    l'appelant handshake, et pour une conn déjà `!c.choked` émettre un
    `PeerMessage::Unchoke` redondant au lieu de no-op. Test : conn
    déjà unchokée reçoit un second `Unchoke` sur Interested.
- `crates/bitdaemon-session/src/torrent.rs:10958-10987` —
  `maybe_unchoke_this_peer` porte le gate `num_have > 0` en interne
  alors qu'upstream le place à l'appelant handshake uniquement
  (`bt_peer_connection.cpp:3644-3652`). Lié à la déviation précédente
  (même sketch).

## Fichiers du fix

- `crates/bitdaemon-session/src/choker.rs` (valeur de retour)
- `crates/bitdaemon-session/src/session.rs` (sweep, candidats
  optimistic, priorité, intervalle mesuré, triggers, route
  `MaybeUnchoke`, `last_choke`)
- `crates/bitdaemon-session/src/torrent.rs` (`free_upload_slots`,
  triggers dans `choke_peer`, gate `num_have` déplacé)
- `crates/bitdaemon-session/src/peer_conn.rs` (`upload_priority`,
  `PeerEffect::MaybeUnchoke` sur `Interested`)

## Non-bugs tranchés

- Tri complet du top-`slots` après `select_nth_unstable_by`
  (`choker.rs:210`) : upstream s'arrête à `nth_element`, l'ordre
  interne du set n'affecte pas sa composition — non-déviation.
- `anti_leech_score` : upstream `abs((have - total/2) * 2000 / total)`
  vs Rust `abs(have - total/2) * 2000 / total` — même valeur pour les
  entrées réelles (le signe survit à `*2000/total` puisque total>0).
- `remove_peer` : les triggers `triggered_unchoke` (filtré
  `!ignores_unchoke_slots`) et `triggered_optimistic` sont portés
  (`torrent.rs:3959-3974` vs `torrent.cpp:6675-6689`) — conforme.
- `update_unchoke_limit` : gauge brute (négatif affiché tel quel),
  alerte `too_many_optimistic_unchoke_slots` sans garde `limit > 0`,
  sweep unchoke-everyone avec les skips upstream — conforme
  (`session.rs:6292-6344` vs `session_impl.cpp:6676-6717`).
- `preemptive_unchoke` : fixed-slots + (`unchoked < slots` ou
  `limit < 0`) — conforme (`torrent.rs:10980-10987` vs
  `session_impl.cpp:6730-6736`).
- Comparateur optimistic u16 : `last_optimistically_unchoked` en `<`
  brut des deux côtés (`session_impl.cpp:4330-4334`,
  `session.rs:8232`) — même comportement wrap-around ; les plugins
  `get_unchoke_priority` upstream sont une extension non portée.
- `recalculate_optimistic_unchoke_slots` quand
  `num_unchoke_slots == 0` et fixed+unlimited → early-return, conforme
  (`session.rs:8192` + `session.rs:8030` vs
  `session_impl.cpp:4349-4354`).
- Ordre choke-puis-unchoke vs unchoke-puis-choke dans la passe
  optimistic (`session.rs:8239-8248` vs `session_impl.cpp:4426-4474`) :
  état final identique (les optimistic conservés sont dans `keep`),
  le veto `max_uploads` est bypassé pour `optimistic` des deux côtés —
  différence d'ordre de messages sans conséquence d'état.
