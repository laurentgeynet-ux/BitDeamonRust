# Audit A02 — rapport d'analyse : protocoles d'extension (re-audit round 3)

Périmètre : `ut_metadata` (BEP-9), `ut_pex`/`i2p_pex` (BEP-11), `lt_donthave`,
`suggest`/`allowed_fast` (BEP-6), `smart_ban`, `upload_only`/`share_mode`
(BEP-21), `ut_holepunch` (BEP-55). Baseline upstream `e7381c7bc`
(inchangée depuis les rounds 1–2).

**Contexte round 3** : re-audit report-only à froid — chaque item de
checklist re-croisé avec la lecture intégrale des jambes upstream
(`ut_metadata.cpp`, `ut_pex.cpp`, `smart_ban.cpp`,
`bt_peer_connection.cpp::on_extended`/`on_extended_handshake`/`on_holepunch`,
`peer_connection.cpp::send_allowed_set`/`incoming_*`/`disconnect_if_redundant`/
`set_upload_only`/`incoming_have_none`/`incoming_interested`) et des
oracles de test (`test_fast_extension.cpp`, `test_magnet.cpp`,
`test_privacy.cpp`, `test_utp.cpp`). Le round 2 (rapport `51ddf18`)
reste d'actualité sauf contre-indication ci-dessous ; **N5 est un faux
positif retiré** (cf. `## Corrections round 3`). 6 déviations
**nouvelles** sont relevées (N13–N18), dont une majeure (N15 :
`smart_ban` jamais alimenté en production). **Aucune correction
appliquée.**

## Verdicts checklist (round 3)

- **ut_metadata** — verdict : **conforme** — le cap à deux niveaux
  est fidèle : `metadata_size` du handshake borné à 4 MiB
  (`ut_metadata.rs:147` = `ut_metadata.cpp:125`) et `total_size` à la
  réception borné à `max_metadata_size` = 30 MiB
  (`ut_metadata.rs:218` = `ut_metadata.cpp:515`). Fenêtre
  `m_sent_requests` = 2, piece non sollicitée consommée sans compteur,
  `dont_have` → `request_limit = max(now+60s)` avant le find (même
  non sollicité), penalité `single piece → now+5min + 20–70 s`,
  compteur en fin de switch. Tie-break : voir correction N5.
- **ut_pex** — verdict : **déviation** — N1/N2/N3/N8 (round 2)
  toujours ouvertes + **N14** (première liste différée jusqu'à 60 s
  pour une conn fraîche — timer torrent-global vs `m_last_msg`
  per-conn upstream). Réception conforme sinon (ordre
  dropped-then-added par famille équivalent, flood-guard 6×60 s,
  cap/dedup/local-filter dans le bon ordre).
- **lt_donthave** — verdict : **déviation mineure** — gate
  `is_disconnecting` upstream manquant avant le disconnect
  `invalid_dont_have` (**N17**) ; le reste est conforme (taille,
  plage torrent, plage bitfield → peer_error, redondant ignoré).
- **suggest/allowed_fast** — verdict : **déviation** — gates de
  validation et insertion conformes ; **N13** : `send_allowed_set`
  borne sur **notre** `is_upload_only()` alors qu'upstream borne sur
  l'`upload_only()` **distant** (`m_upload_only || is_seed() ||
  m_have_all` — état du pair). Rejets hors-set et purges conformes.
- **smart_ban** — verdict : **déviation majeure** — la logique est
  fidèle mais **inerte en production** : `read_digest` vaut `|_|
  None` aux deux points d'appel (**N15**) → `record_failed_block`
  jamais invoqué → aucun ban par CRC différentiel.
- **upload_only / share_mode** — verdict : **conforme** — sauf
  N13/N16/`is_disconnecting` (cf. détails) : sync peer-list +
  `disconnect_if_redundant`, clé handshake, share_mode ignoré sur
  seed (les deux chemins), `share_mode` msg non gaté par
  `support_share_mode` (upstream `on_extended` — conforme).
- **holepunch** — verdict : **conforme** — ordre des erreurs,
  bridging local/non-local, `add_peer(PEX)` + `supports_utp` +
  `holepunch` flag + force-dial sur entrée existante non connectée
  (`peer_list::add_peer` retourne les entrées existantes — fidèle à
  `torrent::add_peer`), `failed` log-only.
- **extended handshake (BEP-10)** — verdict : **conforme** — `reqq`
  gate `> 0` + clamp u16 identique (`clamp_assign` upstream), `p` →
  `update_peer_port` + `received_listen_port`, `complete_ago`,
  `yourip` → ip_voter (v4-mapped inclus upstream, Rust idem),
  `upload_only`/`share_mode` keys, compteur en fin de parse.
  `ipv4`/`ipv6` ignorés à la réception = upstream (envoi seulement).

## Corrections round 3 (révision du round 2)

- **N5 est un faux positif — retiré.** `Iterator::min_by_key`
  retourne le **premier** élément minimal à égalité (sémantique std
  documentée), exactement comme `std::min_element` — le tie-break de
  `metadata_request` (`ut_metadata.rs:179-185`) est **conforme**.
  Le round 2 l'avait classé « dernier minimum » par erreur.
- **Lecture complémentaire de W6 (round 1)** : le fix `send_allowed_
  set` a posé le gate sur notre `is_upload_only()` — mauvais opérande
  (cf. N13). Le statut « tient » du round 2 est corrigé en
  « opérande erroné ».

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

## Déviations nouvelles (round 3)

### Wire-visible

- **N13 — `send_allowed_set` borne sur le mauvais `upload_only`**
  (`torrent.rs:997-1003`) : Rust retourne quand `self.is_upload_only()`
  (notre torrent terminé) — upstream `send_allowed_set` retourne quand
  `upload_only()` du **peer_connection** est vrai, i.e.
  `m_upload_only || is_seed() || m_have_all` côté **distant**
  (`peer_connection.cpp:631`, `peer_connection.hpp:400`). Conséquences
  symétriques : (a) nous upload-only + pair leecher → upstream émet le
  set, Rust n'émet rien → les requêtes choked du pair tombent en reject
  au lieu de toucher l'allowed-fast offert ; (b) pair upload-only/seed +
  nous leecher → upstream saute, Rust envoie un set inutile et remplit
  `accept_fast`. Introduit par le fix round-1 (opérande inversé ; la
  vérification round-2 « tient » était erronée). Fix : gater sur
  `c.remote_upload_only || c.is_seed()` après fetch de la conn (le
  `num_allowed_pieces <= 0` / `supports_fast` sont déjà corrects).
- **N14 — première liste PEX différée jusqu'à 60 s pour une conn
  fraîche** (`torrent.rs:2370-2452`) : `last_pex` est un timer
  **torrent-global** estampillé à chaque appel éligible — y compris
  quand `!enable_pex` ou `peers.len() <= 1` (le stamp précède les
  gates). Upstream garde `m_last_msg` **par conn**, initialisé
  `min_time()` : la première plugin-tick de la conn (~1 s post-
  handshake) émet `send_ut_peer_list` (liste complète) dès que
  `m_message_index != 0 && num_peers() > 1` — et le `num_peers <= 1`
  early-return tombe **avant** le stamp (`ut_pex.cpp:404-428`).
  Effet : un pair qui rejoint en cours de cycle attend jusqu'à 60 s
  sa première liste ; une phase à pair unique ou pex désactivé brûle
  le stamp pour tout le monde. Le contenu (`pex_first_time` → liste
  complète, diffs ensuite) est fidèle — c'est la cadence qui diverge.
  Fix : `pex_last_msg: Option<Instant>` par conn (`None` → envoi
  plein au prochain tick sous les mêmes gates), ou au minimum
  déplacer le stamp après les gates.

### État / comportement

- **N15 — `smart_ban` jamais alimenté : protection inerte**
  (`session.rs:5332`, `torrent.rs:7237`) : `piece_verified_ban` reçoit
  `read_digest = |_| None` aux deux points d'appel → `on_piece_failed`
  n'enregistre jamais de CRC par bloc (`block_hashes` toujours vide)
  et `on_piece_passed` ne re-vérifie rien → `BanDecision::Ban`
  inatteignable en production. Upstream `smart_ban_plugin::
  on_piece_failed` lance un `async_read` par bloc et mémorise le SHA-1
  réel (`smart_ban.cpp`). La logique du module est fidèle et testée,
  mais le chemin production ne l'atteint jamais — un pair émettant des
  blocs corrompus n'est puni que par la voie `penalize_peers`/
  `banned_for_hash_failure` (4 échecs, ou échec unique en
  `known_bad`), jamais par le ban précis-par-bloc. Fix sans re-lecture
  disque : le hash job de la pièce lit déjà tous les octets — propager
  les digests SHA-1 par bloc dans le résultat du job (ou bufferiser la
  pièce hashée) et les passer à `piece_verified_ban`.
- **N16 — `has_metadata` : set manquant sur `Interested`, set en trop
  sur `DontHave`** (`peer_conn.rs:1204-1228, 1363`) : upstream pose
  `m_has_metadata = true` dans `incoming_interested`
  (`peer_connection.cpp:1817`, avant `disconnect_if_redundant`) et ne
  le pose **pas** dans `incoming_dont_have` (2123-2191). Rust fait
  l'inverse : le bras `M::Interested` ne set rien, le bras
  `M::DontHave` set. Effet dans les deux sens sur le gate
  `has_metadata()` de `disconnect_if_redundant` : (a) un pair qui a
  annoncé `ut_metadata` sans `metadata_size` puis envoie `Interested`
  reste protégé chez Rust alors qu'upstream le coupe s'il est
  redondant ; (b) un `lt_donthave` seul rend le pair éligible à la
  coupure chez Rust, pas chez upstream.
- **N17 — `lt_donthave`/`DontHave` : gate `is_disconnecting` manquant
  avant le disconnect `invalid_dont_have`** (`peer_conn.rs:1354-1359,
  1979-1984`) : upstream `incoming_dont_have` retourne silencieusement
  sur `is_disconnecting()` **avant** le contrôle de borne bitfield
  (`peer_connection.cpp:2148`). Rust émet `Disconnect(peer_error)` +
  `failed |= true` inconditionnellement → une conn en cours de
  teardown reçoit un `failed` qui pollue le `failcount` à
  `connection_closed`. (Les bras `SuggestPiece`/`AllowedFast`
  vérifient bien `c.disconnecting` — asymétrie interne.)
- **N18 — `disconnect_if_redundant` : pas de garde `disconnecting`,
  pas de veto `can_disconnect`** (`torrent.rs:6323-6358`) : upstream
  retourne tôt sur `m_disconnecting` (`peer_connection.cpp:2323`) —
  Rust peut émettre un `DisconnectPeer` dupliqué sur une conn déjà
  morte (benign si la session déduplique le teardown). Point
  structurel : `can_disconnect(ec)` permet aux peer-plugins de
  veto-er les coupures de ce type ; le trait Rust `PeerPlugin`
  n'expose aucun hook (`on_extended`, `on_suggest`, `on_allowed_fast`,
  `on_dont_have`, `on_have_none`, `on_interested`,
  `on_unknown_message`, `can_disconnect`) — les points
  d'interception upstream n'existent pas dans le modèle plugin
  actuel. Sans plugin chargé le comportement filaire est identique ;
  c'est une limite d'extensibilité, pas un bug.

## Non-bugs tranchés (addenda round 3)

- **`metadata_size` 4 MiB vs `max_metadata_size` 30 MiB** : les deux
  caps coexistent upstream (`ut_metadata.cpp:125` vs `:515`) et en
  Rust (`ut_metadata.rs:147` vs `:218`) — pas de divergence.
- **Ordre `dropped`/`added` à la réception PEX** : upstream traite
  dropped-v4 → added-v4 → dropped6 → added6 ; Rust applique tous les
  dropped puis tous les added — résultat identique par famille (le
  dedup `pex_known` interdit les effets croisés). Non-bug.
- **`ipv4`/`ipv6` keys du handshake** : upstream ne les lit jamais à
  la réception (seul `yourip` nourrit `set_external_address`) —
  champs morts Rust confirmés conformes.
- **`disconnect_if_redundant` sur handshake ext malformé** : upstream
  l'invoque inconditionnellement en queue d'`on_extended` id==0
  (`bt_peer_connection.cpp:1914`) même si le bdecode échoue ; Rust ne
  l'appelle que sur parse réussi. Fenêtre réelle minuscule (il faut
  un flag upload_only préalablement posé + un handshake corrompu) —
  cosmétique.
- **`need_interest_update`** : pas d'équivalent Rust car l'intérêt
  est calculé eager dans `update_interest` (queue de
  `on_peer_message`) avant `disconnect_if_redundant` — l'invariant
  « intérêt à jour » tient par construction.
- **`PeerSeed(false)` émis même quand le pair n'était pas seed** :
  upstream n'appelle `set_seed(pi, false)` que sur `was_seed`
  (peer_connection.cpp:2186) ; l'effet Rust est idempotent sur la
  peer-list. Cosmétique.

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

## Test-gap analysis (round 3)

| Scénario upstream (`test/`) | Couverture Rust | Statut |
|---|---|---|
| `test_fast_extension::reject_fast` (reject purge `allowed_fast` en choked) | logique présente (`peer_conn.rs` `on_reject`) — pas de test dédié | **gap** |
| `allowed_fast_survives_metadata` / `suggested_piece_survives_metadata` | `retain` présent (`torrent.rs:1820-1823`) — pas de test requête post-metadata | **gap** |
| `reject_suggest` (reject purge `suggested_pieces` unchoked) | logique présente — non testée | **gap** |
| `suggest_order` (front-insert, dernier-suggest prioritaire) | `on_suggest` front-insert — ordre de requête non testé | **gap** |
| `dont_have` (seed → non-seed, bitfield clear, pieces count) | aucun test `dont_have`/`lt_donthave` trouvé | **gap** |
| `invalid_suggest` | `suggest_negative_index_dropped` | couvert |
| `invalid_metadata_request` | `ut_metadata_request_serving_and_counter_timing` | couvert |
| `invalid_request` (piece OOB) | `invalid_request_rejected_and_alerted` | couvert |
| `extension_handshake` (ids `m{}` annoncés) | couvert via tests engine/handshake (A01) | couvert |
| holepunch end-to-end (rendezvous → 2×connect ; connect → swarm entry + uTP flag + force-dial ; banned/connected abort) | units codec/décision (`ut_holepunch.rs`) — intégration `Torrent::on_holepunch` non testée | **gap** |
| PEX première liste à conn fraîche (~1 s upstream) | `pex_feeds_swarm_and_broadcasts` couvre le broadcast ; le timing première-liste non testé (aurait vu N14) | **gap** |
| smart_ban chemin production (digests réels) | `smart_ban_differing_crc`/`smart_ban_on_piece_passed` injectent les digests à la main — le chemin `piece_verified_ban` (N15) non couvert | **gap** |
| `test_magnet.cpp::magnet` (résolution ut_metadata complète) | `magnet_resolves_via_ut_metadata`, `wrong_metadata_rejected` | couvert |
| `test_utp.cpp`, `test_privacy.cpp` | périmètres A05/A24 | hors scope |

## Résultat

### Round 3 — apply (cette passe)

Apply exécuté sur `audit/02-extension-protocols` (worktree
`../LibtorrentRust-a02`). **Toutes les déviations round-2 + round-3
corrigées** — N1–N4, N6–N18 (N5 retiré en tant que faux positif).

Correctifs notables :

- **N15 smart_ban opérationnel** : `HashJob`/`async_hash` émettent
  désormais les SHA-1 par bloc 16 Kio (`block_digests`) — les octets
  sont déjà lus pour le hash de pièce, zéro I/O disque additionnel
  vs le `async_read` par bloc upstream — propagés via
  `DiskCompleted::Hash` → `on_piece_verified` → `read_digest`.
- **N14 cadence PEX par conn** : `pex_last_msg` par connexion
  (init `min_time()`), suppression du stamp torrent-global.
- **N13 allowed-fast** : gate sur `upload_only()` du pair distant
  (`remote_upload_only || is_seed()`), plus sur le nôtre.
- **N8 caps PEX** : `pex_known`/`pex_known6` bornés séparément, un
  ledger v4 saturé `continue` au lieu de `break` (les boucles
  upstream `added`/`added6` sont indépendantes).
- **N7 `send_upload_only`** : jambes `send_not_interested`
  complètes (gauge `num_peers_down_interested`, `became_uninteresting`,
  slow-start, `disconnect_if_redundant` avant l'émission wire).
- **N18** : garde `m_disconnecting` + nouveau hook
  `PeerPlugin::can_disconnect` (veto upstream des extensions sur le
  teardown redondant).
- **N2/N4** : `local_ut_pex` honore `allow_i2p_mixed` ;
  `local_i2p_pex` uniquement sur sockets I2P.
- **N16/N17** : `has_metadata` posé sur `Interested` (pas
  `DontHave`) ; `is_disconnecting` early-out avant
  `invalid_dont_have` sur les deux chemins.
- **N1/N6/N9/N10/N11/N12** : flag `pex_seed` sous `valid_metadata`,
  gate `valid_metadata()` sur les requêtes metadata, compteur
  ext-handshake post-parse, `num_incoming_pex` sur dicts i2p_pex
  sans `added`, `update_peer_port` → `update_want_peers`,
  `dont_have` embarque `total_size`+dict.

Tests de régression ajoutés (upstream-derived) :
`cap_tripper_marked_seen_not_readded` (N3, test_ut_pex),
`pex_tick_per_conn_first_list` (N14), `pex_seed_flag_uses_valid_metadata`
(N1), `pex_known_capped_per_family` (N8),
`allowed_set_gates_on_remote_upload_only` (N13),
`interested_sets_has_metadata_donthave_doesnt` (N16),
`dont_have_on_disconnecting_conn_is_silent` (N17),
`send_upload_only_full_not_interested_legs` (N7),
`disconnect_if_redundant_honors_plugin_veto` (N18),
`smart_ban_fed_by_hash_job_digests` (N15 — chemin production via
`Session::on_piece_verified`).

Limites structurelles conservées : `PeerPlugin` n'expose toujours
pas `on_extended`/`on_suggest`/`on_dont_have`/`on_unknown_message`
(hooks upstream non atteignables — voir N18) ; seul
`can_disconnect` a été ajouté.

Gates : `cargo fmt --all` ✓ · `cargo clippy --workspace
--all-targets -- -D warnings` ✓ · `cargo test --workspace` ✓ ·
`cargo doc --workspace --no-deps` ✓. Commit `7a21639` — merge
`--no-ff` sur `main`.

### Round 3 — audit (historique)

Re-audit report-only — **aucune correction appliquée**. Les verdicts
round-2 tiennent sauf **N5 retiré** (faux positif :
`min_by_key` = premier minimum = `std::min_element`). **6 déviations
nouvelles** : N13 (mauvais opérande `upload_only` dans
`send_allowed_set` — wire), N14 (première liste PEX ≤60 s vs ~1 s —
wire/tempo), **N15 (`smart_ban` jamais alimenté — protection inerte,
la plus saillante)**, N16 (`has_metadata` : Interested/DontHave
inversés), N17 (`is_disconnecting` manquant sur `lt_donthave`), N18
(`disconnect_if_redundant` : garde `disconnecting` + veto plugin
`can_disconnect` absents — structurel). 9 scénarios-test orphelins
identifiés (table ci-dessus), dont deux qui auraient détecté N14/N15.

### Round 2 (historique)

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
