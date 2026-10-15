# Audit A02 — rapport d'analyse : protocoles d'extension

Périmètre : `ut_metadata` (BEP-9), `ut_pex`/`i2p_pex` (BEP-11), `lt_donthave`,
`suggest`/`allowed_fast` (BEP-6), `smart_ban`, `upload_only`/`share_mode`
(BEP-21), `ut_holepunch` (BEP-55). Baseline upstream `e7381c7bc`.

Références upstream : `src/ut_metadata.cpp`, `src/ut_pex.cpp`,
`src/i2p_pex.cpp`, `src/smart_ban.cpp`, `src/peer_connection.cpp`
(`incoming_suggest` 1689, `incoming_dont_have` 2123, `incoming_allowed_fast`
3498, `send_allowed_set` 605, `set_upload_only` 6884, `set_share_mode` 6874),
`src/bt_peer_connection.cpp` (`on_holepunch` 1499, `on_extended` 1885,
`on_extended_handshake` 2004, `write_extensions` 2433, `write_upload_only`
2226), `src/torrent.cpp` (`send_upload_only` 903, `send_share_mode` 889).

## Verdicts checklist

- **ut_metadata : request/data/reject, throttling, caps `metadata_size`,
  chunk 16 K ordering, tracker-hash reset** — verdict : **déviation** —
  cœur fidèle (`ut_metadata.rs` : request/data/reject, 16 K chunks,
  `metadata_request` least-requested + fenêtre 3 s + `source` disconnecting,
  `received_metadata` : caps `total_size > 0 && <= max`, cohérence
  `total_size`, overflow `offset+buf`, `num_requests = MAX`,
  `reset_on_hash_failure` + pénalité `now+5min` si mono-source,
  `dont_have` → `request_limit = max(now+60s)` et effacement de
  `sent_requests`, gate `m_sent_requests.size() < 2`) — **mais** :
  (a) aucun gate « pair a annoncé l'extension » ; (b) trame > 17 K et
  corps non-dict/clés absentes ne déconnectent pas ; (c) pas de file
  `incoming_requests` ni cap `send_buffer_limit` ; (d) pas de requête
  suivante immédiate après réception d'un `piece`. Preuves :
  `peer_conn.rs:1648-1652`, `torrent.rs:1532-1654`, upstream
  `ut_metadata.cpp:271-342,345-365,384-414`.
- **ut_pex : added/dropped, flags, IPv6, max pairs, `lt_v2` strip** —
  verdict : **déviation mineure** — conforme : codec `added/dropped/added6/
  added6.f/dropped6`, strip `pex_lt_v2`, cap `max_pex_peers` (50) dans la
  boucle apply, garde `is_local`, flood-guard 6×60 s → `too_frequent_pex`
  **normal**, `pex_message_too_large`/`invalid_pex_message` → peer_error,
  `peers_added → do_connect_boost` (`want_peers`), send_peer éligibilité
  (`outgoing || received_listen_port`, `!connecting`, `handshake_done`),
  port `p` pour les entrants, flags seed/encryption/utp/holepunch, cap
  100 added, `peers_in_msg==0 → pas d'envoi`, premier envoi = liste
  complète, i2p_pex id 9 + `include_peer` filtré i2p. Déviations : gate
  « pair a annoncé » absente, compteur `num_incoming_pex` compté au
  dispatch (upstream : fin de handler — les legs disconnect ne comptent
  pas), ordre `dropped`-avant-`added` inversé (edge dégénéré). Preuves :
  `ut_pex.rs:54-182`, `torrent.rs:1750-1949`, `i2p_pex.rs`,
  `peer_conn.rs:1653-1658`; upstream `ut_pex.cpp:243-390,84-160,399-460`.
- **lt_donthave inbound** — verdict : **conforme** — `data.len() != 4`
  (= `packet_size != 6` upstream) drop silencieux ; index hors plage
  torrent → drop silencieux ; hors bitfield → `invalid_dont_have`
  peer_error ; redondant ignoré ; `peer_lost` + `PeerSeed(false)` ;
  `has_metadata` armé. Preuves : `peer_conn.rs:1683-1706,790-799` ;
  upstream `bt_peer_connection.cpp:1970-1983`,
  `peer_connection.cpp:2123-2186`.
- **suggest/allowed_fast sets** — verdict : **déviation** — conforme :
  front-insert cap `max_suggest_pieces` (16), purge des deux sets sur
  piece reçue (`on_piece` + `we_have`), purge reject (choked →
  `allowed_fast`, sinon `suggested_pieces`), purge hors-range à
  l'arrivée metadata (`torrent.rs:1471-1472`), `send_allowed_set`
  déterministe SHA-1(ip‖ih) + re-hash + `loops > 500`, gates
  `valid_metadata`/`super_seeding`/`remote_upload_only`, request hors-set
  choked → reject. Déviations : (a) `send_allowed_set` déclenché à
  l'attach au lieu du premier `Interested` ; (b) la variante
  torrent-level ne remplit **jamais** `accept_fast` → les requêtes
  choked sur pieces offertes sont rejetées ; (c) `allowed_fast` inbound
  sans validation (négatif/OOB/déjà-possédé/disconnecting) ni jambe
  `peer_is_interesting` ; (d) `suggested_pieces` accepte `index < 0`.
  Preuves : `session.rs:4082-4083`, `torrent.rs:747-820`,
  `peer_conn.rs:701-703,1546-1568,4060-4072`; upstream
  `bt_peer_connection.cpp:905-910` (on_interested),
  `peer_connection.cpp:1689-1746,3498-3560`.
- **smart_ban** — verdict : **déviation mineure** — conforme : CRC par
  bloc, `record_failed_block` (même pair, digest différent → ban),
  `on_piece_passed` re-lecture + ban, clear sur `is_seed`,
  `peer_banned` à sévérité normale, `get_downloaders` par bloc, lecture
  digest via callback disque. Déviations : (a) pas de gate `is_aborted`
  avant `on_piece_failed` ; (b) pas de gate `!p->banned` ; (c) clé
  conn-derived au lieu de `find_peers(addr)` — un ban visant un pair
  reconnecté est perdu (`peer_addr` stocké, jamais utilisé) ; (d)
  `std::map::insert` upstream conserve le *premier* enregistrement pour
  un bloc (autre pair, insert no-op) — `BTreeMap::insert` écrase ;
  (e) `erase_peers`/`clear` jamais appelés sur éviction/teardown de la
  peer-list. Preuves : `smart_ban.rs:60-183`, `torrent.rs:5640-5664,
  5779-5797,3342-3346`; upstream `smart_ban.cpp:58-144,148-260`,
  `peer_connection.cpp` (`peers_erased`).
- **upload_only (les deux sens)** — verdict : **conforme** — inbound
  `data.len()==1` gate → `remote_upload_only` + `peer_list.set_upload_only`
  + `disconnect_if_redundant` (`peer_conn.rs:1659-1665`,
  `torrent.rs:4034-4051`); outbound `send_upload_only` = `share_mode ||
  super_seeding || !close_redundant_connections → return`,
  `send_not_interested`/`disconnect_if_redundant` par conn, skip
  `connecting`, gate `ext_upload_only != 0` (`torrent.rs:8991-9034`);
  clé handshake `upload_only` conditionnée identiquement
  (`engine.rs:3111-3112`); `disconnect_if_redundant` ré-évalué sur
  ext-handshake ✓. Upstream `peer_connection.cpp:6884-6900`,
  `torrent.cpp:903-940`, `bt_peer_connection.cpp:2226-2246`.
- **holepunch rendez-vous** — verdict : **déviation mineure** —
  conforme : codec msg_type/addr_type/endpoint/error, drop silencieux
  msg_type>2, addr_type inconnu, trames tronquées ; gate
  `ext_holepunch != 0` ; `no_self` quand `ep == remote` ; ordre
  `not_connected → no_support → no_self` ; bridging local↔non-local
  silencieux ; `connect` → `add_peer(PEX)` + gate `connection|banned`
  + `supports_utp` + `want_peers` ; `failed` = log-only. Déviations :
  (a) `connect_to_peer` + `set_holepunch_mode` absents — une connexion
  holepunchée ne colle pas à uTP ni ne retente plus vite
  (`ut_holepunch.rs` n'a pas de concept de mode, `torrent.rs:4188-4208`) ;
  (b) le port listen du pair (`p` handshake) reste sur la conn — pas de
  `update_peer_port` vers l'entrée peer-list. Upstream
  `bt_peer_connection.cpp:1499-1706`.

## Déviations

### Wire-visible

1. **`peer_conn.rs:1648-1658`** — les ids plugin `local_ut_metadata` (2),
   `local_ut_pex` (1), `local_i2p_pex` (9) routent sans vérifier que le
   pair a annoncé l'extension (`ext_ut_metadata`/`ext_ut_pex`/
   `ext_i2p_pex == 0`). Upstream : `on_extended` → `m_message_index == 0
   → return false` → `invalid_message` **peer_error disconnect**
   (`ut_metadata.cpp:271-272`, `ut_pex.cpp:243-244`, `i2p_pex.cpp`).
   Fix : routage conditionné — id plugin non annoncé → bras `else`
   (`invalid message`). Note : les ids built-in (3,4,7,8) restent
   inconditionnels — upstream les dispatche par constante, pas par
   annonce. Sévérité : **wire-visible** (un client réel testant un id
   non annoncé devrait être déconnecté).

2. **`torrent.rs:1532-1543`** — `ut_metadata` malformé : corps > 17 K,
   non-dict, `msg_type`/`piece` absent ou non-int → `parse_metadata_packet`
   `None` → drop silencieux. Upstream `on_metadata` déconnecte
   `invalid_metadata_message` (peer_error) sur chacun
   (`ut_metadata.cpp:274-307`). Fix : distinguer « non parseable » →
   `DisconnectPeer{PeerError}` ; le `msg_type` hors {0,1,2} reste un
   drop silencieux (upstream switch sans default). Sévérité :
   **wire-visible**.

3. **`torrent.rs:1545-1576`** — pas de throttle serveur ut_metadata :
   upstream sert immédiatement si `send_buffer_size < 160 KiB`
   (`send_buffer_limit = 0x4000*10`), sinon file `m_incoming_requests`
   (cap `max_incoming_requests = 1024`) drainée au tick, sinon
   `dont_have`. Rust répond toujours immédiatement. Fix : leg
   `send_buffer` watermark → file par conn + drain tick ; overflow →
   `Reject`. Sévérité : **wire-visible** sous charge (upstream renvoie
   `dont_have` quand saturé).

4. **`torrent.rs:1578-1640`** — après un `piece` ut_metadata accepté,
   upstream appelle `maybe_send_request()` immédiatement
   (`ut_metadata.cpp:364`) ; Rust attend le `metadata_tick` (~1 Hz).
   Pipeline cassé → latence par chunk. Fix : `metadata_tick_peer(peer)`
   après `Stored`. Sévérité : **wire-visible** (tempo).

5. **`engine.rs:3103`** — `ut_metadata` annoncé même pour les torrents
   privés (`create_ut_metadata_plugin` retourne `{}` sur `priv()` —
   l'index 2 est absent du `m` dict). Fix : gate `!is_priv` comme pour
   `ut_pex`. Sévérité : **wire-visible** (handshake différent).

6. **`session.rs:4083` + `torrent.rs:747-820`** — `send_allowed_set`
   envoyé à l'attach ; upstream le diffère au premier `Interested`
   (`bt_peer_connection.cpp:905-910`, `m_sent_allowed_fast` once-flag)
   *parce que* le bitfield du pair est alors connu — Rust offre donc
   des pieces que le pair possède déjà. Pire : la variante
   `Torrent::send_allowed_set` pousse les messages `AllowedFast` mais
   **n'enregistre jamais `c.accept_fast`/`accept_fast_piece_cnt`** —
   la variante `peer_conn.rs:712` qui maintient ces sets n'est appelée
   que par les tests. Résultat : un pair choked qui requête une piece
   qu'on lui a offerte se prend `choked_piece_requests` + `RejectRequest`
   (`torrent.rs:3725-3731`, `peer_conn.rs:1329+`). Fix : un seul chemin
   (déclencher sur le premier `Interested`, `supports_fast`-gaté,
   once-flag), écrire `accept_fast` + compteurs à l'envoi. Sévérité :
   **wire-visible** (BEP-6 cassé de bout en bout).

7. **`peer_conn.rs:1558-1568` → `torrent.rs:4060-4072`** — SuggestPiece :
   `index < 0` n'est pas filtré (upstream `index < 0 → return`,
   `peer_connection.cpp:1708-1714`) ; `PieceIndex(-k)` entre dans
   `suggested_pieces` et peut être pick → `Request` avec index négatif
   sur le fil. Fix : `i.0 < 0` dans la condition `drop`. Sévérité :
   **wire-visible**.

8. **`peer_conn.rs:1546-1556`** — AllowedFast : aucune validation —
   upstream `incoming_allowed_fast` droppe silencieusement
   `is_disconnecting`, `index < 0`, `index >= m_have_piece` (si
   metadata) et `have_piece` (`peer_connection.cpp:3498-3547`) puis
   `t->peer_is_interesting(*this)` quand le pair a la piece et qu'on la
   veut (`3550-3558`) — la jambe d'intérêt immédiat manque aussi. Fix :
   validation + `PeerEffect::PeerInteresting`-like. Sévérité :
   **état** (set pollué, intérêt retardé au tick).

9. **`torrent.rs:4188-4208`** — holepunch `connect` : pas de
   `set_holepunch_mode` sur la conn créée (upstream marque la conn
   uTP-sticky + retry rapide, `bt_peer_connection.cpp:1674-1679`). Fix :
   propager un flag `holepunch_mode` dans le peer entry → appliqué à
   l'attach. Sévérité : **état** (comportement reconnexion).

10. **`peer_conn.rs:1614-1619`** — `p` ext-handshake ne met à jour que
    `conn.peer_listen_port` ; upstream `update_peer_port` écrit aussi
    `peer_list` entry port (re-dial / gossip). Fix : effet vers
    `peer_list`. Sévérité : **état**.

### Compteurs / cosmétique

11. **`peer_conn.rs:1651,1654,1657`** — `NumIncomingMetadata` /
    `NumIncomingPex` comptés à l'entrée du bras ; upstream les incrémente
    à la fin du handler — les legs `disconnect`/`unwanted` ne comptent
    pas (`ut_metadata.cpp:351-357,373,379`, `ut_pex.cpp:388`).
    Sévérité : **compteur**.

12. **`torrent.rs:1593`** — `max_metadata_size` littéral `30*1024*1024`
    au lieu de `s.max_metadata_size` (la valeur par défaut coincide,
    le réglage est ignoré). Sévérité : **cosmétique**.

13. **`torrent.rs:749`** — `NUM_ALLOWED` const 5 au lieu de
    `s.allowed_fast_set_size` (même défaut, réglage ignoré). Le leg
    « set ≥ num_pieces » filtre en plus `!self.have` (upstream offre
    toute piece que le pair n'a pas, même si on ne l'a pas).
    Sévérité : **cosmétique**.

14. **`smart_ban`** — `is_aborted` gate absent avant `on_piece_failed`
    (upstream `smart_ban.cpp:104-106`) ; `!p->banned` gate absent
    (`smart_ban.cpp:~205`) ; `BTreeMap::insert` écrase l'entrée
    existante quand un *autre* pair envoie le même bloc — upstream
    `map::insert` est un no-op sur clé existante (premier serveur
    conservé) ; `erase_peers`/`clear` non câblés à `peers_erased`/
    teardown peer-list. Sévérité : **état**.

15. **`peer_conn.rs:1598`** — `remote_share_mode` posé sans gate
    `support_share_mode` (upstream `on_extended_handshake` :
    `m_settings.support_share_mode && share_mode-key`). Sévérité :
    **cosmétique**.

16. **`bep10.rs:47-50`** — champs `ipv4`/`ipv6` du handshake : jamais
    remplis par `engine.rs:3057` (upstream ne les émet pas non plus —
    retirés du protocole 2.x). Dead code. Sévérité : **cosmétique**.

## Fichiers du fix

- `crates/bitdaemon-session/src/peer_conn.rs` — gates `ext_*` dans le bras
  `Extended` (D1), validation suggest/allowed_fast (D7, D8), effet
  `PeerInteresting` (D8).
- `crates/bitdaemon-session/src/torrent.rs` — `on_metadata_packet`
  (disconnects D2, throttle D3, requête suivante D4), `send_allowed_set`
  refonte + déclencheur (D6), `on_holepunch` holepunch_mode (D9),
  smart_ban gates (D14), `max_metadata_size`/`allowed_fast_set_size`
  réglages (D12, D13), `update_peer_port` (D10).
- `crates/bitdaemon-session/src/session.rs` — déclencheur
  `send_allowed_set` sur premier `Interested` (D6).
- `crates/bitdaemon-session/src/smart_ban.rs` — first-wins insert,
  `!banned` gate, re-résolution par adresse (D14).
- `crates/bitdaemon-session/src/settings.rs` — rien (réglages déjà
  présents : `max_metadata_size`, `allowed_fast_set_size`,
  `support_share_mode`, `close_redundant_connections`, `max_pex_peers`).
- `crates/bitdaemon/src/engine.rs` — gate `ut_metadata` sur torrents
  privés (D5).
- `crates/bitdaemon-net/src/bep10.rs` — retirer `ipv4`/`ipv6` morts
  (D16, optionnel).

Chevauchements vague V4 : `peer_conn.rs`/`torrent.rs`/`session.rs` sont
les fichiers chauds — séquencer avec les autres audits session-core.

## Non-bugs tranchés

- **Index d'extension locaux identiques à upstream** : ut_pex=1,
  ut_metadata=2, upload_only=3, ut_holepunch=4, lt_donthave=7,
  share_mode=8, i2p_pex=9 (`peer_conn.rs:495-566`,
  `bt_peer_connection.hpp:80-84`, `ut_pex.cpp:38`, `ut_metadata.cpp:176`,
  `i2p_pex.cpp:38`). Les trois ids builtin supplémentaires (3,4,7,8) sont
  dispatchés par constante upstream — tout message entrant sur ces ids
  est légal indépendamment de l'annonce ; Rust idem.
- **`share_mode` sortant** : `m_share_mode_id` n'est jamais renseigné
  upstream (`on_extended_handshake` ne lit pas `m["share_mode"]`) →
  `write_share_mode` est dead code ; Rust n'émet pas de message
  share_mode non plus. Équivalent (le mode passe par la clé handshake).
- **pex diff par conn vs diff torrent partagé** : upstream construit un
  diff unique (`m_old_peers` torrent-level) envoyé à tous les conns ;
  Rust diffère par conn (`c.pex_sent`). Convergence identique (chaque
  pair apprend added/dropped → état courant) ; l'ordre `dropped`
  après `added` dans le parse est un edge dégénéré (un pair dans les
  deux). Non-bug.
- **Ext-handshake malformé** : drop silencieux des deux côtés
  (`bt_peer_connection.cpp:2015-2025` log-only ; Rust `parse → None`
  silencieux). ✓
- **`has_metadata`** : ctor true, `set_has_metadata(false)` quand
  ut_metadata annoncé sans `metadata_size`, re-armé par tout message
  piece-level (have/bitfield/dont_have/…). Rust identique.
- **`metadata_size` handshake hint** : upstream pré-dimensionne
  `m_requested_metadata` (cap 4 MiB — hint seulement) ; la taille
  réelle est fixée par `total_size` au premier piece (cap
  `max_metadata_size` 30 MiB). Rust saute la pré-allocation — même
  résultat observable.
- **`m_request_limit` sur `dont_have` ut_metadata** : `max(now+60s,
  limit)` + erase `sent_requests` ✓ (`torrent.rs:1641-1652`).
- **PEX flood-guard** : 6 timestamps / fenêtre 60 s, `too_frequent_pex`
  à sévérité *normale* ✓ (`torrent.rs:1773-1790`).
- **`is_local` guard** : skip des adresses privées sauf pair local —
  `ut_pex.rs:187-195` ✓ (incl. fc00::/7, fe80::/10).
- **`holepunch` ordre des échecs** : `no_support` rapporté avant
  `no_self` quand `p == this` sans support — `ut_holepunch.rs:189-193`
  reflète l'ordre upstream (find_peer → supports → self).
- **`upload_only`/`share_mode` entrant à 1 byte** : `data.len() != 1`
  drop (= `packet_size != 3` upstream) ✓ ; `share_mode` ignoré sur seed
  (`peer_conn.rs:1672`) ✓.

## Résultat

Apply exécuté sur la branche `audit/02-extension-protocols` (worktree
`../LibtorrentRust-a02`, parallèle à A05). Gates : `cargo fmt --all` ✓ ·
`cargo clippy --workspace --all-targets -- -D warnings` ✓ ·
`cargo test --workspace` ✓ (107 suites) · `cargo doc --workspace
--no-deps` ✓.

### Déviations corrigées

| # (rapport) | Correction |
|---|---|
| W1 — plugin ids acceptés sans annonce | dispatch `peer_conn` : `local_* != 0 && ext_* != 0` sinon `invalid_message` peer_error ; ids locaux remis à 0 à l'attach quand le plugin est absent (privé/i2p) |
| W2 — metadata malformé drop | `parse_metadata_packet` tri-état (`Msg`/`UnknownType`/`Malformed`) ; >17 KiB / non-dict / clés absentes → `invalid_metadata_message` peer_error |
| W3 — pas de throttle serveur | file `metadata_incoming_requests` par conn (cap 1024) + watermark 160 KiB + `dont_have` en overflow ; drain dans `metadata_tick` |
| W4 — pas de requête chaînée | `maybe_send_metadata_request` après chaque `piece` acceptée (et au handshake), séparée du drain de queue |
| W5 — `ut_metadata` sur privé | handshake engine : `!(is_priv && valid_metadata)` ; `metadata_size` suit le plugin ; ids locaux alignés |
| W6 — `send_allowed_set` cassé | déclenché au premier `Interested` (`sent_allowed_fast`), supprimé de l'attach ; délègue à la version conn qui peuple `accept_fast`/compteurs ; gate `upload_only` corrigé (notre état, pas celui du pair) |
| — `SuggestPiece` index négatif | drop explicite `i.0 < 0` côté torrent |
| — compteurs pex/i2p | `num_incoming_pex` en fin de traitement réussi uniquement ; requêtes metadata invalides hors `num_incoming_metadata` |
| — holepunch `connect` | legs `p->connection`/`p->banned` → ignore ; `supports_utp` + flag `holepunch` sur l'entrée → `conn.holepunch_mode` à l'attach (dial async) |
| — smart_ban | `Ban(key, addr)` : re-résolution par IP (`find_peers`), gate `!banned`, first-wins par bloc, `ban_peer` + `num_banned_peers` + `update_want_peers`, `erase_peers`/`clear` câblés à `remove_peer`/`disconnect_all`, désactivé sur v2/abort |
| — `update_peer_port` | port `p` du handshake → `peer_list.update_peer_port` + `duplicate_peer_id` disconnect sur collision + `update_want_peers` |
| — `local_ut_metadata_id` | champ mort à valeur fausse (1 = id pex) supprimé ; les envois utilisent `conn.ext_ut_metadata` (id remote, = upstream `m_message_index`) |

### Robustesse ajoutée

- `metadata_packet` clamp le payload hors-borne (plus de panic sur
  `piece` invalide — le bras servant vérifie déjà, filet de sécurité).

### Tests ajoutés/ajustés

- `test_extensions.rs` : `ut_metadata_requires_announced_extension`,
  `ut_metadata_malformed_disconnects_peer_error`,
  `ut_metadata_request_serving_and_counter_timing` (dont_have sans
  compteur / piece avec compteur),
  `ut_metadata_piece_chains_next_request` (sizing handshake + chaînage),
  `allowed_fast_deferred_to_first_interested` (+ `accept_fast` peuplé,
  pas de re-send), `suggest_negative_index_dropped`,
  `ut_pex_severities` (>500 KiB et malformé → peer_error, compteur
  intouché).
- `test_ut_metadata.rs` : `parse_verdicts` (tri-état + borne
  `data_section`), appels `parse_msg` migrés sur le nouvel enum.
- `test_piece_picker.rs` : `BanDecision::Ban` porte l'adresse.
- `test_interest.rs` : `allowed_fast_set_on_fast_peer` réécrit sur la
  sémantique upstream (rien à l'attach, set au premier `Interested`).

### Non-corrections assumées (trace)

- `i2p_pex` per-conn socket gate : le transport I2P/SAM n'existe pas
  encore (scope audit transport) — `conn.i2p` n'est jamais set en prod,
  le gate torrent `!priv && is_i2p` est l'approximation documentée.
- `do_connect_boost` après PEX `peers_added` : nécessite
  `session_time`/politique d'encryption côté session — approximé par
  `want_peers = true` + `state_updated` (dial au prochain tick ≤1 s).

### Livraison

- Commit apply : `6dfcdca`
  `fix(session): extension-protocol parity — BEP-9/10/11 gates, FAST, smart-ban, holepunch`
- PR : https://github.com/laurentgeynet-ux/BitDeamonRust/pull/2
  (branche `audit/02-extension-protocols`, worktree `LibtorrentRust-a02`)
