# Audit A02 — rapport d'analyse : protocoles d'extension (re-audit round 2)

Périmètre : `ut_metadata` (BEP-9), `ut_pex`/`i2p_pex` (BEP-11), `lt_donthave`,
`suggest`/`allowed_fast` (BEP-6), `smart_ban`, `upload_only`/`share_mode`
(BEP-21), `ut_holepunch` (BEP-55). Baseline upstream `e7381c7bc`
(inchangée depuis le round 1).

**Contexte** : re-audit report-only demandé par l'utilisateur — le round 1
(rapport `5be36fd`, apply `6dfcdca`, merge `1cf1381`, PR #2) a été relu
sur le code **post-merge** (A08–A10, A14–A15 touchant `peer_conn.rs`,
`torrent.rs`, `session.rs`, `engine.rs`). Objectifs : vérifier la tenue
des 16 corrections round-1, reprendre la checklist à froid, détecter les
déviations nouvelles ou ratées. **Aucune correction appliquée.**

Références upstream : `src/ut_metadata.cpp`, `src/ut_pex.cpp`
(`tick` 87-149, `send_ut_peer_diff` 431, `send_ut_peer_list` 477,
`on_extended` 243-390), `src/i2p_pex.cpp` (`include_peer` 44-51,
`on_extended` 195-270), `src/smart_ban.cpp`, `src/peer_connection.cpp`
(`send_allowed_set` 605-721, `incoming_suggest` 1689-1748,
`incoming_dont_have` 2123-2191, `incoming_allowed_fast` 3498-3560,
`send_not_interested`/`send_upload_only` 3928-3979,
`set_share_mode`/`set_upload_only` 6874-6900),
`src/bt_peer_connection.cpp` (`on_interested` 891-914, `on_holepunch`
1499-1705, `on_extended` 1885-2002, `on_extended_handshake` 2004-2132,
`write_upload_only`/`write_share_mode` 2226-2266, `write_extensions`
2433-2552), `src/torrent.cpp` (`send_share_mode`/`send_upload_only`
889-938, `update_peer_port`/`set_upload_only` 11951-11979),
`include/libtorrent/aux_/io_bytes.hpp` (`read_uint32` big-endian).

## Verdicts checklist (round 2, à froid)

- **ut_metadata : request/data/reject, throttling, caps `metadata_size`,
  chunk 16 K ordering, tracker-hash reset, compteurs** — verdict :
  **conforme** — toutes les corrections round-1 en place et fidèles :
  tri-état `MetadataParse` (Malformed → `invalid_metadata_message`
  peer_error, UnknownType → compté-consommé), corps > 17 KiB →
  peer_error, file `metadata_incoming_requests` (cap 1024) + watermark
  `send_buffer < 160 KiB`, dont_have en overflow, requête chaînée après
  chaque piece acceptée + au handshake, `request_limit = max(now+60s)`
  sur dont_have, `metadata_sent_requests` fenêtre 2, penalité
  `now+5min` mono-source sur hash-fail, compteur `num_incoming_metadata`
  en fin de handler (invalid-request/unwanted-piece/unknown-dont_have
  non comptés — vérifié bras par bras). Deux déviations **nouvelles**
  résiduelles (N5 tie-break, N6 gate v2, N12 `total_size` dont_have —
  cosmétique). Preuves : `ut_metadata.rs:110-268`,
  `torrent.rs:1562-1788`, `peer_conn.rs:1804-1822` ; upstream
  `ut_metadata.cpp` entier.
- **ut_pex : added/dropped, flags, IPv6, max pairs, `lt_v2` strip,
  cadence 60 s** — verdict : **déviation** — cœur fidèle (codec,
  strip `pex_lt_v2`, `is_local` guard, flood-guard 6×60 s sévérité
  normale, oversized/malformed peer_error, `pex_known` insert-avant-
  apply, `want_peers`/`state_updated` sur added, compteur en fin de
  handler, `peers_in_msg==0 → pas d'envoi`, premier envoi = liste
  complète, port `p` pour les entrants, flags encryption/utp/holepunch)
  — **mais trois déviations nouvelles** : N1 flag `pex_seed` posé sur
  **chaque** conn (tautologie), N2 `local_ut_pex` ignore
  `allow_i2p_mixed`, N3 cap `max_peer_entries` → dropped fantômes +
  oscillation. Plus N8 cap `pex_known` combiné vs par-famille (état).
  Preuves : `torrent.rs:1975-2133`, `ut_pex.rs:54-182`,
  `session.rs:4076`, `engine.rs:3454` ; upstream
  `ut_pex.cpp:116-127,477-530`.
- **lt_donthave inbound** — verdict : **conforme** — `data.len() != 4`
  drop silencieux (= `packet_size != 6`), hors plage torrent → drop,
  hors bitfield pair → `invalid_dont_have` peer_error, redondant
  ignoré, `RefcountDec` + `PeerSeed(false)`. Preuves :
  `peer_conn.rs:1870-1893`.
- **suggest/allowed_fast sets** — verdict : **conforme** — corrections
  round-1 tenues : `send_allowed_set` au premier `Interested`
  (once-flag `sent_allowed_fast`, gate `supports_fast`, SHA-1
  remote‖ih big-endian, re-hash, `loops > 500`, peuple
  `accept_fast`+compteurs), suggest `index < 0` droppé, allowed_fast
  validé (négatif/OOB/déjà-possédé → drop, metadata-inconnu stocké
  pour validation ultérieure), jambe `peer_is_interesting` quand le
  pair a la piece, purges OOB à l'arrivée metadata
  (`torrent.rs:1500-1502`), front-insert cap 16. Codec valide
  `packet_size` d'`Interested`/`NotInterested` (`peer_wire.rs:585-591`
  = `invalid_interested` upstream). Preuves :
  `peer_conn.rs:603-605,941-962,1690-1701` ; upstream
  `peer_connection.cpp:605-721,1689-1748,3498-3560`,
  `bt_peer_connection.cpp:891-914`.
- **smart_ban** — verdict : **conforme** — corrections round-1 tenues :
  `Ban(key, addr)` re-résolution par IP via `find_peers`, gate
  `!banned`, first-wins par bloc (insert no-op sur clé existante),
  `ban_peer` + `num_banned_peers` + `update_want_peers`, `erase_peers`/
  `clear` câblés à remove/disconnect_all, gate `!v2`/`!aborted`, clear
  sur `is_seed`. Preuves : `smart_ban.rs`, `torrent.rs` (legs
  piece-failed/passed).
- **upload_only (les deux sens)** — verdict : **conforme** — inbound
  1-byte gate → `remote_upload_only` + peer-list + disconnect_redundant ;
  outbound gates `share_mode || super_seeding || !close_redundant`,
  skip `connecting`, `ext_upload_only != 0` ; clé handshake conditionnée
  `is_upload_only && !share_mode && valid_metadata && !super_seeding`
  (`engine.rs`). Jambe `send_not_interested` présente (soustrait
  N7 — legs compteur/slow_start manquants). Preuves :
  `torrent.rs:9831-9868`, `peer_conn.rs:1845-1851` ; upstream
  `torrent.cpp:903-940`, `peer_connection.cpp:3928-3979,6884-6900`.
- **share_mode** — verdict : **conforme** — inbound 1-byte, gate
  `support_share_mode` + `!is_seed` (torrent-side), clé handshake
  `support_share_mode && t.share_mode` ; message share_mode sortant
  = dead code upstream (`m_share_mode_id` jamais renseigné) — Rust
  idem. Preuves : `peer_conn.rs:1852-1860`.
- **holepunch rendez-vous/connect** — verdict : **conforme** — ordre
  `not_connected → no_support → no_self` préservé, bridging
  local↔non-local silencieux, `connect` → `add_peer(PEX)` + gates
  `connection|banned|already-connected` ignore, `supports_utp` +
  `holepunch_mode` à l'attach (uTP-sticky + retry), `update_want_peers`,
  `failed` = log-only. Preuves : `ut_holepunch.rs:189-193`,
  `torrent.rs` (legs connect).
- **i2p_pex** — verdict : **déviation mineure** — cœur fidèle (codec
  sha256-destination, cadence 60 s, cap 50, flood per-conn → disconnects
  sévérités upstream, `update_want_peers` ≈ `do_connect_boost`) ;
  déviations : N4 `local_i2p_pex` annoncé/accepté sur conns non-i2p
  (gate par-socket upstream), N10 verdict `Nothing` non compté,
  `apply_added` dedup → boost sauté sur added tout-duplicata (upstream
  booste dès `num_peers ≥ 1`). Preuves : `i2p_pex.rs`,
  `torrent.rs:2135-2174`, `session.rs:4077` ; upstream
  `i2p_pex.cpp:44-51,195-270`.
- **extended handshake (BEP-10) sortant/entrant** — verdict :
  **conforme** — sortant : `p` outgoing-only port≠0, `v` sauf
  anonymous, `yourip` sauf i2p, `reqq`, ids m{} (upload_only=3,
  holepunch=4, share_mode=8 gate support, dont_have=7 + plugins),
  `complete_ago` (`time_since_complete` = −1 si jamais vu ✓
  `torrent.rs:10053-10061`), `upload_only`/`share_mode` keys sous
  gates identiques. Entrant : ids remote stockés, `p` →
  `peer_listen_port` + `update_peer_port` + `duplicate_peer_id`,
  `complete_ago` → `last_seen_complete` conn (max torrent),
  `reqq` → `max_out_request_queue`, `upload_only`/`share_mode` keys →
  effets, yourip → ip_voter. Soustraits : N9 compteur pré-parse,
  `peers_erased` non notifié (N11). Preuves : `bep10.rs`,
  `peer_conn.rs:1719-1802`, `engine.rs:3406-3475` ; upstream
  `bt_peer_connection.cpp:2004-2132,2433-2552`.

## Tenue des corrections round-1 (fix-hold status)

Les 16 corrections du commit `6dfcdca` relues **post-merge** — aucune
régression des merges A08–A10/A14–A15 sur les jambes extension.

| # | Correction | Statut | Preuve |
|---|---|---|---|
| W1 | gates `ext_*` au dispatch (`local_* != 0 && ext_* != 0` sinon `invalid_message` peer_error ; ids locaux reset à l'attach) | **tient** | `peer_conn.rs:1804-1844`, `session.rs:4075-4077` |
| W2 | metadata malformé → tri-état + `invalid_metadata_message` | **tient** | `torrent.rs:1573-1597`, `ut_metadata.rs` (enum `MetadataParse`) |
| W3 | throttle serveur (file 1024 + watermark 160 KiB + dont_have overflow + drain tick) | **tient** | `torrent.rs:1638-1656`, `metadata_tick` |
| W4 | requête chaînée après piece acceptée + handshake | **tient** | `torrent.rs:1759`, handshake leg |
| W5 | `ut_metadata` supprimé sur privé+valid_metadata | **tient** | `session.rs:4075`, `engine.rs:3464-3468` |
| W6 | `send_allowed_set` différé au premier `Interested`, `accept_fast` peuplé, gate upload_only corrigé | **tient** | `peer_conn.rs` (Interested arm), `send_allowed_set` conn-side |
| D7 | `SuggestPiece` index négatif droppé | **tient** | `peer_conn.rs:1703-1713` + gate torrent |
| D8 | AllowedFast validation + `peer_is_interesting` | **tient** | `peer_conn.rs:1690-1701`, torrent arm |
| D9 | holepunch `connect` : gates + `holepunch_mode` attach | **tient** | `torrent.rs` holepunch legs |
| D10 | `p` handshake → `update_peer_port` + `duplicate_peer_id` | **tient** | `peer_conn.rs:1766-1775` + torrent arm |
| D11 | compteurs pex/metadata en fin de handler | **tient** | `torrent.rs:1652,1699,1787,2026` |
| D12 | `s.max_metadata_size` utilisé | **tient** | `torrent.rs:1696` (`s.max_metadata_size`) |
| D13 | `s.allowed_fast_set_size` utilisé | **tient** | `send_allowed_set` |
| D14 | smart_ban gates/first-wins/re-résolution/cleanup | **tient** | `smart_ban.rs`, torrent hooks |
| D15 | `remote_share_mode` gate `support_share_mode` | **tient** | torrent arm `RemoteShareMode` |
| D16 | champs `ipv4`/`ipv6` bep10 morts (optionnel, non retirés) | **tient** (dead code inerte) | `bep10.rs` |

## Déviations nouvelles (round 2)

### Wire-visible

- **N1 — `pex_seed` posé sur chaque pair** (`torrent.rs:2058`) :
  `c.peer_pieces.count() == c.num_pieces` compare le popcount du
  bitfield au compteur maintenu des bits posés — **toujours vrai par
  construction** (y compris `0 == 0` sur un pair sans bitfield) →
  `PEX_SEED` envoyé pour toute conn éligible. Upstream `send_ut_peer_
  list`/`tick` : `flags = p->is_seed() ? 2 : 0` où `is_seed()` =
  `m_num_pieces == m_have_piece.size() && m_num_pieces > 0 &&
  t->valid_metadata()` (`ut_pex.cpp:516,149-160`). Fix :
  `self.valid_metadata() && c.is_seed()` (la méthode existe,
  `peer_conn.rs:635`). Introduit par `8151875` (pré-round-1 — raté par
  les deux audits).
- **N2 — `local_ut_pex` ignore `allow_i2p_mixed`** (`session.rs:4076`
  vs `engine.rs:3454`) : l'attach pose `!is_priv && !torrent_i2p` alors
  que `create_ut_pex_plugin` upstream ne refuse que `priv() || (i2p()
  && !allow_i2p_mixed)` — le handshake (fidèle) annonce `ut_pex:1` sur
  un torrent i2p+mixed mais `local_ut_pex == 0` → tout message entrant
  sur l'id 1 tombe dans `invalid_message` → **peer_error disconnect**
  d'un pair conforme. Fix : `!(torrent_i2p && !s.allow_i2p_mixed)`.
- **N3 — cap `max_peer_entries` → `dropped` fantômes + oscillation**
  (`ut_pex.rs:70-91`) : `break` au 100e `added` court-circuite
  `prev.remove(ep)`/`sent.insert(ep)` → les pairs **toujours
  connectés** situés après le point de coupure restent dans `prev` →
  émis en `dropped`, puis réémis en `added` au tick suivant
  (oscillation permanente + messages contradictoires). Upstream
  `tick()` insère `m_old_peers.insert(remote)` **avant** le check de
  cap (`ut_pex.cpp:121-127`) → jamais de fantôme ; corollaire : les
  nouveaux au-delà du cap sont marqués « vus » → **jamais annoncés**,
  alors que Rust les annonce aux ticks suivants. Fix : déplacer
  `prev.remove`/`sent.insert` avant le test de cap (marquer toute
  conn éligible, qu'elle soit écrite ou non).
- **N4 — `local_i2p_pex` annoncé/accepté sur conns non-i2p**
  (`session.rs:4077`) : `(!is_priv && torrent_i2p) || conn.i2p.is_some()`
  claim l'id 9 sur **toute** conn d'un torrent i2p — upstream ne crée
  le plugin que sur `is_i2p(socket)` (`i2p_pex.cpp:50`) → id absent du
  handshake et messages id-9 non consommés (`invalid_message`). Rust
  sur-annonce et consomme sur les conns clearnet. (Résidu de
  l'approximation documentée round-1 — la conséquence filaire est
  maintenant explicite.)

### État / comportement

- **N5 — tie-break `metadata_request` inversé**
  (`ut_metadata.rs:179-185`) : `Iterator::min_by_key` retourne le
  **dernier** minimum à égalité, `std::min_element` le **premier** →
  parmi les pieces à `num_requests` égal (ex. toutes à 0), Rust
  requête l'index le plus haut, upstream le plus bas. Ordre de
  requête inversé (les pieces finissent toutes par arriver — tempo
  seulement).
- **N6 — gate `maybe_send_metadata_request` : `piece_hashes.is_some()`
  vs `valid_metadata()`** (`torrent.rs:1827+`) : `valid_metadata()` =
  `ti.is_some() || picker.is_some()`. Un torrent v2-only a `ti` mais
  pas de vec sha1 `piece_hashes` → Rust continue d'émettre des
  requêtes metadata qu'upstream n'émettrait pas (throttlées à ~1/min
  par `request_limit`, mais présentes). Fix : gate sur
  `self.valid_metadata()`.
- **N7 — `send_upload_only` legs `send_not_interested` amputés**
  (`torrent.rs:9840-9848`) : le bras interested nettoie
  `we_sent_interested` et émet `NotInterested`, mais n'exécute pas
  les legs upstream `send_not_interested` : décrément
  `num_peers_down_interested`, `m_slow_start = false`
  (`c.pipeline.slow_start` existe), `disconnect_if_redundant` après
  clear, skip `m_disconnecting`. La divergence est rattrapée aux
  prochains ticks — legs manquants, pas de bug.
- **N8 — cap `pex_known` combiné v4+v6** (`torrent.rs:1990`) :
  upstream borne `m_peers` et `m_peers6` **séparément** à
  `max_pex_peers` chacun (100 appris au total) ; Rust borne le set
  fusionné à 50 → un flux v6 peut évincer de la capacité v4 et
  réciproquement.
- **N9 — `num_incoming_ext_handshake` compté avant parse**
  (`peer_conn.rs:1724`) : upstream incrémente en fin de
  `on_extended_handshake` — un handshake ext malformé (return
  anticipé) n'est pas compté ; Rust le compte. Compteur seul.
- **N10 — `num_incoming_pex` i2p : verdict `Nothing` non compté**
  (`torrent.rs:2154-2162`) : upstream compte tout dict parsé avec
  succès, `added` présent ou non (`i2p_pex.cpp:266`) ; Rust ne compte
  que le verdict `Added`. Compteur seul.

### Cosmétique

- **N11 — `update_peer_port` sans notification `peers_erased`** :
  upstream `torrent::update_peer_port` notifie `peers_erased` pour les
  entrées évincées en collision ; Rust met à jour le port sans la
  passe de nettoyage des doublons. (Reliquat de D10.)
- **N12 — `dont_have` ut_metadata sans `total_size`**
  (`torrent.rs:1622-1627`) : `metadata_packet(Reject, …, false, &[])`
  omet `total_size` ; upstream `write_metadata_packet` inclut la clé
  dès `valid_metadata()`. Byte différent, sémantique nulle (le
  récepteur ne lit `total_size` que sur `piece`).

## Non-bugs tranchés (addenda round 2)

- **`Interested`/`NotInterested` taille** : le codec exige
  `expect_len == 1` (`peer_wire.rs:585-591`) = `packet_size != 1 →
  invalid_*` upstream. Hors scope A02 (codec = A01), conforme.
- **`send_upload_only` à l'attach** (`session.rs` attach leg) : le
  message upload_only est émis dès l'attach en plus de la clé
  handshake — upstream ne l'émet que sur transition d'état. Effet
  net identique : le distant traite le message comme la clé
  (`set_upload_only` + `disconnect_if_redundant`, déjà évalué sur la
  clé). Non-bug.
- **`metadata()` plugin = `info_section`** : upstream bascule
  `m_metadata` → `info_section()` quand valid_metadata flips ; Rust
  sert `metadata_raw` — même buffer, mêmes bornes. Non-bug.
- **per-conn pex diff vs diff torrent partagé** : approximation
  round-1 toujours valide — convergence identique ; upstream borne en
  plus l'ensemble annoncé (cf. N3 pour le cap, seule vraie divergence).
- **Ordre `dropped`-avant-`added` à la réception** : edge dégénéré
  (pair dans les deux listes) inchangé — upstream traite `added` puis
  `dropped` (ordre dict), Rust l'inverse. Non-bug assumé.
- **Compteur `num_outgoing_pex`/`num_outgoing_extended`** : périmètre
  A29 (sweep compteurs), pas de régression relevée.

## Résultat

### Round 2 (ce rapport)

Re-audit report-only — **aucune correction appliquée**. Verdict :
corrections round-1 intègres post-merge ; **5 déviations nouvelles
wire-visible ou d'état** (N1–N4, N6) + 3 compteur/tempo (N5, N9, N10)
+ 4 mineures (N7, N8, N11, N12). La plus saillante : **N1 `pex_seed`
sur chaque pair** — un récepteur conforme nous voit annoncer des seeds
partout (peut déclencher des comportements seed-only chez des clients
réels). Corrections en attente d'une phase apply à demander
explicitement.

### Round 1 (historique — apply exécuté)

Apply sur `audit/02-extension-protocols` (worktree
`../LibtorrentRust-a02`). Gates : `cargo fmt --all` ✓ · `cargo clippy
--workspace --all-targets -- -D warnings` ✓ · `cargo test --workspace`
✓ (107 suites) · `cargo doc --workspace --no-deps` ✓. Commit
`6dfcdca` — 16 déviations corrigées (table « tenue » ci-dessus), tests
de régression ajoutés (`test_extensions.rs`, `test_ut_metadata.rs`,
`test_piece_picker.rs`, `test_interest.rs`), PR
https://github.com/laurentgeynet-ux/BitDeamonRust/pull/2 mergée
`1cf1381`.

### Non-corrections assumées (trace, toujours valides)

- `i2p_pex` per-conn socket gate : transport I2P/SAM inexistant —
  `conn.i2p` jamais set en prod (cf. N4 pour la conséquence filaire).
- `do_connect_boost` après PEX `peers_added` ≈ `want_peers` +
  `state_updated` (dial au prochain tick ≤1 s).
