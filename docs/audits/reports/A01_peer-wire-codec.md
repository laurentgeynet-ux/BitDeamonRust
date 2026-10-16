# Audit A01 — rapport d'analyse

Baseline upstream : `e7381c7bc` (`src/bt_peer_connection.cpp`,
`src/peer_connection.cpp`, `src/hash_picker.cpp`,
`include/libtorrent/aux_/bt_peer_connection.hpp`,
`include/libtorrent/performance_counters.hpp`).

**Passe 4** — re-audit complet du périmètre (code + tests upstream
relus en profondeur). Les sept déviations ouvertes de la passe 3
(D1, D2, D4, D5, D6, D7, D8) sont **toutes vérifiées corrigées**
dans le code courant — voir `## Corrigées depuis la passe 3`.
Quatre déviations mineures subsistent (D-A à D-D), toutes de
sévérité faible.

## Verdicts checklist

- Length-prefix 4B BE inclut l'id, keepalive = len 0 — bornes
  exactes upstream (`packet_size` checks par message) — verdict :
  **conforme** — preuve : `decode_next` lit le préfixe BE u32,
  `n == 0` → `Keepalive`, `n > 1 MiB` → erreur
  (`bt_peer_conn.rs:29-32,736-750` ; `PeerWireCodec` idem,
  `peer_wire.rs:741-757` ; `decode(&[])` → `Keepalive`,
  `peer_wire.rs:570-572`). Upstream : `packet_size > 1024*1024 ||
  packet_size < 0` → `packet_too_large` peer_error, `== 0` →
  `incoming_keepalive` (`bt_peer_connection.cpp:3695-3713`). Le
  `< 0` C++ est absorbé par le cast u32 (`0x80000000`+ tombe dans
  la borne `> 1 MiB` → même erreur). Lentilles : [Limites]
  [Disconnect]. Tests : `decoder_default_cap_is_one_mib`,
  `decoder_enforces_max_len`, `decoder_waits_for_full_frame`
  (test_peer_wire.rs).
- Chaque `msg_*` : round-trip encode/decode + rejet des tailles
  invalides (have len 5, request len 13, bitfield len =
  ceil(n/8)+1…) — verdict : **conforme** — preuve : `expect_len`
  exhaustif et byte-exact : choke/unchoke/interested/not-int = 1,
  have/suggest/allowed_fast = 5, request/cancel/reject = 13,
  dht_port = 3, have_all/have_none = 1, hash_request/hash_reject =
  49, extended ≥ 2 (`peer_wire.rs:556-561,577-704`). Upstream :
  mêmes bornes `packet_size != N` → `invalid_*` peer_error
  (`bt_peer_connection.cpp:843,878,898,928,946,1006,1125,1381,
  1410,1430,1444,1457,1481,1156,1324,1891`). Le bitfield est le
  seul check contextuel : taille exacte `ceil(num_pieces/8)`
  vérifiée session-side (`peer_conn.rs:1419-1435` =
  `on_bitfield`, `bt_peer_connection.cpp:978-983`). `hashes` :
  taille exacte `49 + (count + proofs)·32` recalculée au codec
  (`peer_wire.rs:665-690` = `bt_peer_connection.cpp:1268-1277`).
  Lentilles : [Limites] [Disconnect]. Tests :
  `message_encodings_byte_exact`, `round_trip_all`,
  `have_none_rejects_payload`, `lt_donthave_decodes_as_extended`,
  `dont_have_encodes_as_extended` (test_peer_wire.rs) ;
  `oversized_bitfield_disconnects`,
  `undersized_bitfield_disconnects` (test_peer_dispatch.rs).
- Handshake 68B : validation info_hash/proto, peer_id, reserved
  bits (BEP-10, FAST, v2) — verdict : **déviation** (D-A bit DHT ;
  D-C pid WebTorrent — cœur conforme) — preuve : `parse_handshake`
  exige pstrlen 19 + pstr exact + ≥ 68 B (`peer_wire.rs:309-336`)
  = `read_protocol_identifier` upstream ; `run_handshake*` envoie
  puis lit 68 B, écho ih re-validé (`check_echo`,
  `bt_peer_conn.rs:119-131` = `bt_peer_connection.cpp:3488-3503`),
  attache ih v1 + v2-tronqué, `apply_v2_bit` rejette `V2OnV1`
  (`bt_peer_conn.rs:98-110` = `3471-3480`). Peer_id : dup
  `(pid < our) == outgoing`, self-conn `is_self_connection`, cap
  BitComet `-BC` → 50 (`session.rs:4504-4581` =
  `bt_peer_connection.cpp:3557-3594`). Reserved bits : r5|0x10 et
  r7|0x04 inconditionnels, r7|0x10 ⇔ `!protocol_v2 && has_v2` —
  conformes (`peer_wire.rs:245-272` vs `bt_peer_connection.cpp:
  739-755`) **sauf** r7|0x01 DHT subordonné à `enable_dht` en
  Rust (engine.rs:3662,3987,4345,4561,5071) alors qu'upstream le
  pose inconditionnellement (`bt_peer_connection.cpp:739-742`) ;
  et le pid `-WW`/`-WD` WebTorrent ne déclenche pas l'`interested`
  simulé (`bt_peer_connection.cpp:3653-3658`). Lentilles :
  [Limites] [Disconnect]. Tests : `handshake_layout`,
  `handshake_rejects_bad_pstr`, `handshake_round_trip`,
  `responder_v2_bit_flips_before_reply`,
  `responder_rejects_v2_claim_on_v1_only` (test_bt_peer_conn.rs) ;
  `self_connection_via_dial_local` (test_peer_lifecycle_a07.rs).
- Messages hors-ordre : have/bitfield avant handshake, piece avant
  bitfield, reject sans request — verdict : **conforme** — preuve :
  les messages ne sont dispatchés qu'après le handshake + attach
  (pump démarrée post-`run_handshake`, engine.rs:5241+ ; upstream
  `state_t` machine `read_peer_id` → `read_packet_size`,
  `bt_peer_connection.cpp:3517-3663`). Bitfield omis →
  `on_have_none()` implicite avant `Have`, `Request`, `Piece`
  (`peer_conn.rs:1286-1289,1484-1487,1679-1682` =
  `peer_connection.cpp:1965,2447`). Reject hors requête → no-op
  (`peer_conn.rs:1801-1804`, test `malformed_reject_is_a_noop`) ;
  piece non demandée → insert `not_wanted` + `unwanted_block_alert`
  (`peer_conn.rs:1665-1673`, test
  `unsolicited_piece_inserts_not_wanted_and_writes`). Piece avant
  bitfield → test `piece_before_bitfield_runs_have_none`. Reject
  multiple bitfield → recount via `RefcountBulk(old, false)`
  (`peer_conn.rs:1441-1443`, test `multiple_bitfields_recount_share`).
  Lentilles : [Cycle de vie] [Disconnect].
- Dépassements : oversized piece (packet_too_large), truncated
  frames, length incohérente → disconnect vs drop silencieux —
  verdict : **conforme** (nuance D-D — kill plus tardif, même
  issue) — preuve : `piece` de corps < 9 B → `Malformed{id:7}`
  silencieux (`peer_wire.rs:602-611` = `on_piece` `recv_pos <
  header_size → return`, `bt_peer_connection.cpp:1093`), compté
  `num_incoming_piece` via `wire_id()` ; `hashes` < 49 B →
  `Malformed{id:22}` idem (`peer_wire.rs:665-672` =
  `bt_peer_connection.cpp:1245-1248`), compté
  `num_incoming_extended`. Piece > `block_size` → disconnect
  `packet_too_large` (`peer_conn.rs:1624-1630` =
  `bt_peer_connection.cpp:1041-1046`, test
  `oversized_piece_disconnects`). Piece de taille 0 ou forme
  invalide → `invalid_piece` (`peer_conn.rs:1641-1660`, tests
  `empty_piece_disconnects_invalid_piece`,
  `invalid_piece_shapes_disconnect`). Trames tronquées : buffer
  `BytesMut` persistant, rien de consommé tant que la frame est
  incomplète (`decode_next` → `Ok(None)`, engine.rs:5367-5408) —
  cancellation-safe dans `select!`. Lentilles : [Limites]
  [Disconnect]. Tests : `short_piece_decodes_to_silent_malformed`,
  `short_hashes_decodes_to_silent_malformed`,
  `decoder_waits_for_full_frame`.
- Compteurs `num_incoming_*` par type de message (funnel
  dispatch) — verdict : **conforme** — preuve :
  `incoming_counter_idx` reproduit le mapping upstream **avec la
  quirk** : ids 0-9 → `num_incoming_choke + id` ; ids 13-17 →
  `num_incoming_suggest + id` (déborde sur `num_outgoing_have..cancel`
  — bug upstream conservé verbatim, `counters.rs:18-37` =
  `bt_peer_connection.cpp:2214-2218` + enum
  `performance_counters.hpp:139-177`) ; ids 10-12, 18-23 →
  `num_incoming_extended`. Bump unique dans
  `Torrent::on_peer_message` (`torrent.rs:5767-5769`), keepalive
  exclu (`wire_id` = `u8::MAX`). Frame complète rejetée
  (`BadLength`) → comptée au teardown, `UnknownId` → non comptée
  (`session.rs:4778-4803` = upstream `default:` return avant le
  bloc compteur, `bt_peer_connection.cpp:2184-2197`).
  `NumIncomingExtHandshake` bumpé post-parse seulement
  (`peer_conn.rs:1942`, test `ext_handshake_counts_only_after_parse`).
  Plus de double-comptage session/torrent (`session.rs:3865-3869`).
  Lentilles : [Compteurs] [Erreur].
- Port/dht_port message, extension id 0 handshake vs ids étendus —
  verdict : **déviation** (D-B `yourip` v4-mappée — dispatch
  conforme) — preuve : `DhtPort` → `add_dht_node` inconditionnel +
  flip `supports_dht` + réponse si `sent_bitfield`
  (`torrent.rs:5462-5491` = `on_dht_port`/`incoming_dht_port`,
  `bt_peer_connection.cpp:1393-1402` ; `peer_connection.cpp:
  3305+`). Ext id 0 → `ExtendedHandshake::parse`, échec → drop
  silencieux + jamais compté (`peer_conn.rs:1859-1943` =
  `bt_peer_connection.cpp:1911-1916,2015-2027`) ; ids distants
  lus dans `m` (`remote_ids`, bep10.rs:150-155 = `2052-2057`) ;
  ids locaux fixes 1/2/3/4/7/8/9 = upstream `pex_msg=1,
  metadata_msg=2, upload_only_msg=3, holepunch_msg=4,
  dont_have_msg=7, share_mode_msg=8` + plugins `extension_index`
  (ut_pex.cpp:38, i2p_pex.cpp:36). Bornes conformes :
  `data.len()!=1` ⇔ `packet_size != 3` → return silencieux
  (`peer_conn.rs:1988-2000` = `bt_peer_connection.cpp:1921-1928,
  1942-1949`) ; `data.len()!=4` ⇔ `!= 6` pour lt_donthave
  (`peer_conn.rs:2014-2038` = `1973-1983`) ; message vers un id
  local dont le pair n'a jamais annoncé l'extension → disconnect
  `invalid message` (`peer_conn.rs:1950-1956,1966-1972,1977-1983`
  = `m_message_index == 0` → plugin refuse → `invalid_message`,
  ut_pex.cpp:243-244, ut_metadata.cpp:272, i2p_pex.cpp:198 +
  `bt_peer_connection.cpp:2001`) ; id inconnu → `invalid message`
  (`peer_conn.rs:2043-2047`). **Mais** `yourip` v4-mappée n'est
  pas déballée en v4 (D-B). Lentilles : [Disconnect] [Compteurs].
- Messages v2 : validation base/index/count/proof_layers (jamais
  de panique sur int négatif ou wrap) — verdict : **conforme** —
  preuve : les champs filaires sont lus `as i32` (signé, pas de
  wrap `as usize`, `peer_wire.rs:537-550`) ;
  `validate_hash_request` (hash_picker.cpp:43-77 = port
  `bitdaemon_disk::hash_picker::validate_hash_request`) s'exécute
  **avant** tout accès arbre : `on_hash_request` → `HashReject`
  jamais fatal (`torrent.rs:5913-5946` =
  `bt_peer_connection.cpp:1199-1211`, incl.
  `file_index_for_root = -1` → `hr.file < 0` → reject) ;
  `on_hashes` → validate AVANT le match `m_hash_requests` puis
  `!result.valid` → `invalid_hashes` peer_error
  (`torrent.rs:5966-6008` = `1262-1266,1302-1305`). Non-v2 ou
  sans metadata → disconnect `invalid_message` peer_error
  (`torrent.rs:5786-5809,5887-5889` = `1150-1154,1233-1237,
  1331-1339`). `hash_reject` poke tous les conns v2
  (`torrent.rs:6151-6159` = `torrent::hashes_rejected`). Lentilles
  : [Limites] [Disconnect] [Cycle de vie]. Tests :
  `v2_hash_request_serve_and_add`,
  `v2_hash_request_out_of_range_is_rejected_not_fatal`,
  `v2_hashes_invalid_descriptor_disconnects`,
  `v2_hash_reject_pokes_all_v2_peers` (test_recheck.rs).
- Bitfield surdimensionné ou sous-dimensionné : rejet strict —
  verdict : **conforme** — preuve : `num_pieces > 0 &&
  bits.len() != num_pieces.div_ceil(8)` → `invalid bitfield size`
  peer_error (`peer_conn.rs:1429-1435` = `packet_size-1 !=
  (num_pieces+7)/8` → `invalid_bitfield_size`,
  `bt_peer_connection.cpp:978-983`) ; leg magnet (`num_pieces
  <= 0`) sans borne exacte comme upstream `!valid_metadata`
  (`peer_conn.rs:1414-1423` = `976-991`). Lentilles : [Limites]
  [Disconnect]. Tests : `oversized_bitfield_disconnects`,
  `undersized_bitfield_disconnects`,
  `multiple_bitfields_recount_share`.

## Couverture des tests upstream (test/test_*.cpp)

- `test_receive_buffer.cpp` — tous les tests portés
  (`recv_buffer_init`, `pos_at_end`, `packet_finished`, `grow*`,
  `reserve*`, `normalize`, `max_receive`, `watermark`,
  `mutable_buffers` + `crypto_receive_buffer_packet_size`,
  `crates/bitdaemon-net/tests/test_receive_buffer.rs`) —
  **couvert**. Note périmètre : ces tests portent sur le buffer
  interne `aux::receive_buffer`, pas sur le dispatch BT (l'engine
  utilise un `BytesMut` + `decode_next` équivalent).
- `test_fast_extension.cpp` :
  - `reject_fast` (fast msgs sans négociation → conn tuée) —
    **couvert** (`fast_messages_require_fast_negotiation`,
    test_peer_dispatch.rs:1035).
  - `invalid_suggest` (suggest `-234` → conn vivante, jamais
    requestée) — sémantique portée (`i.0 < 0` → drop,
    torrent.rs:5588-5597 = `incoming_suggest`,
    peer_connection.cpp:1709-1716) — **test dédié manquant**
    (faible : la borne négative est couverte par le code, pas
    par un test).
  - `reject_suggest`/`suggest_order` — insert-front du set de
    suggestions — **partiel** (ordre exact non testé).
  - `reject_predictive_piece_requests` — couvert en substance
    (`choked_*`, test_peer_dispatch.rs:224,273).
  - `multiple_bitfields` — **couvert**
    (`multiple_bitfields_recount_share`, test_peer_dispatch.rs:1304).
  - `multiple_have_all` — recount `have_all` répété — **manquant**
    (le code décrémente l'ancien set, peer_conn.rs:1343-1345).
  - `dont_have` — flow lt_donthave bout-en-bout — couvert côté
    codec (`dont_have_encodes_as_extended`,
    `lt_donthave_decodes_as_extended`) et dispatch
    (`invalid dont_have index`, peer_conn.rs:2028-2034) ;
    scénario complet session→wire non dédié — **partiel**.
  - `extension_handshake` — couvert (`extended_and_message_loop`,
    test_bt_peer_conn.rs:232 ; `ext_handshake_counts_only_after_parse`).
  - `invalid_metadata_request` — réponse `dont-have` (msg_type 2)
    sans disconnect — périmètre A02/ut_metadata (test_ut_metadata.rs).
  - `invalid_request` (piece 124134235 → reject, conn vivante) —
    **couvert** (`invalid_request_rejected_and_alerted`,
    test_peer_dispatch.rs:141 = `incoming_request`,
    peer_connection.cpp:2519-2560).
  - `outgoing_have_all`/`incoming_have_all` — couvert via
    `fast_messages_accepted_when_negotiated` +
    `multiple_bitfields_recount_share` — **partiel**.
  - `dht_port_no_support` — dht_port reçu sans bit DHT → absorbé,
    pas de réponse — implémenté (torrent.rs:5480-5491) —
    **manquant** (test trivial à porter).
  - `allowed_fast_survives_metadata`,
    `suggested_piece_survives_metadata` — hors périmètre codec
    (magnet, A19).
- `test_hash_picker.cpp` — `validate_hash_request*`
  (`_pad_file`, `_empty_file`, `_single_block_file`, …) :
  fonction portée (`bitdaemon_disk::hash_picker`) **et désormais
  appelée** sur le chemin wire (torrent.rs:5930,5982) ; tests
  wire-level ajoutés (test_recheck.rs:1055,1115,1154) —
  **couvert** côté dispatch v2.
- `test_buffer.cpp`, `test_bitfield.cpp` — primitives couvertes
  dans common/disk ; rien de wire-spécifique résiduel.

## Déviations (ouvertes)

- `crates/bitdaemon/src/engine.rs:3662` (et 3987, 4345, 4561,
  5071) — le bit DHT du handshake (`r7 | 0x01`) suit
  `ses.settings.enable_dht`. Upstream le pose **inconditionnellement**
  dès que la DHT est compilée (`#ifndef TORRENT_DISABLE_DHT`,
  `bt_peer_connection.cpp:739-742`) — `enable_dht` ne gate que
  `m_ses.has_dht()`/`write_dht_port` (`bt_peer_connection.cpp:323`),
  jamais l'annonce de capacité. Effet filaire : avec
  `enable_dht = false`, un pair libtorrent ne nous envoie jamais
  `dht_port` (sa `m_supports_dht_port` reste fausse,
  `bt_peer_connection.cpp:3445-3446`), alors qu'upstream
  l'annoncerait quand même (les `dht_port` reçus tombent alors
  dans `add_dht_node`, no-op sans DHT). Sévérité : **wire-visible
  (faible)** — bit de capacité observable, comportement pair
  différent. [Erreur]
  - **sketch** : aux call sites handshake, passer `dht = true`
    (le support est compilé par construction — l'équivalent du
    `#ifndef TORRENT_DISABLE_DHT`), ou supprimer le paramètre et
    poser le bit dans `write_handshake` directement
    (`peer_wire.rs:257-259`). Ne rien changer à `local_dht_port`
    (gate `has_dht()` correcte côté envoi `DhtPort`).
  - **test de régression suggéré** : `write_handshake` produite
    avec `enable_dht = false` conserve `reserved[7] & 0x01 != 0`
    — assertion byte-level dans test_peer_wire.rs
    (variante de `handshake_layout`) + test engine-level que le
    `dht` passé est constant.

- `crates/bitdaemon-session/src/peer_conn.rs:1931-1935` — le champ
  `yourip` de l'extended handshake est poussé brut dans
  `PeerEffect::YourIp` → `vote_external` ; une adresse v6
  v4-mappée (`::ffff:a.b.c.d`) vote dans le voter **v6**.
  Upstream déballe : `ipv6_address.is_v4_mapped() →
  make_address_v4(v4_mapped, …)` → vote v4
  (`bt_peer_connection.cpp:2105-2113`). Conséquence : consensus
  d'IP externe dans le mauvais bucket de famille pour ce cas
  d'edge. Sévérité : **état-interne (faible)**. [Limites]
  - **sketch** : à la réception du `yourip` 16 B, ou dans
    `Torrent::on_peer_message` avant `TorrentEffect::YourIp` —
    `if let IpAddr::V6(v6) = ip && let Some(v4) =
    v6.to_ipv4_mapped() { ip = IpAddr::V4(v4); }`. Même endroit
    que le leg v4 pour rester symétrique.
  - **test de régression suggéré** : ext handshake `yourip` =
    `::ffff:1.2.3.4` → `vote_external` reçoit `V4(1.2.3.4)`
    (test_peer_dispatch.rs, cas existant `your_ip` à étendre).

- `crates/bitdaemon-session/src/session.rs:4582` — aucune trace du
  quirk WebTorrent : upstream simule `incoming_interested()` pour
  tout peer-id `-WW…`/`-WD…` après `set_pid`
  (`bt_peer_connection.cpp:3653-3658`, `#if TORRENT_USE_RTC` —
  WebTorrent n'envoie parfois jamais `interested`, la conn resterait
  morte côté upload). Rust n'a aucun leg équivalent (grep `WW`/`WD`
  vide sur le chemin peer-id). Sévérité : **état-interne
  (faible)** — un pair WebTorrent silencieux reste non-intéressant
  à vie. [Cycle de vie]
  - **sketch** : dans le `peer_connected` session (après
    `conn.peer_id = Some(rpid)` / self-connection, session.rs:
    4549-4582) : `if b[0]==b'-' && b[1]==b'W' && (b[2]==b'W' ||
    b[2]==b'D') { conn.on_interested(); conn.peer_interested =
    true; }` — en réutilisant le même leg que `M::Interested`
    (`num_peers_up_interested` + `on_interested` +
    `MaybeUnchoke` effect).
  - **test de régression suggéré** : handshake pid `-WW0001-…` →
    le conn devient interested et peut être unchokée
    (test_peer_lifecycle_a07.rs).

- `crates/bitdaemon-net/src/bt_peer_conn.rs:744` +
  `engine.rs:5374-5407` — `decode_next` attend la **frame complète**
  (≤ 1 MiB) avant que la session ne puisse killer un `piece`
  oversized ; upstream disconnect dès `recv_pos == 1` quand
  `packet_size - 9 > block_size` (`bt_peer_connection.cpp:
  1039-1046`) — i.e. après ~5 octets, pas ~1 MiB. L'issue est
  identique (peer_error `packet_too_large`), mais un pair hostile
  peut faire lire jusqu'à ~1 MiB de données inutiles par frame
  avant le kill (coût one-shot borné : la conn meurt ensuite).
  Sévérité : **faible** (ressource/timing — la limite 1 MiB rend
  l'écart borné et non cumulable). [Limites]
  - **sketch** : dans `decode_next`, après lecture du préfixe et
    si `buf.len() >= 5`, offrir un hook précoce — p.ex.
    `decode_next(buf, too_large: impl Fn(u8, usize) -> bool)`
    appelé avec `(id = buf[4], len - 1)` ; l'engine fournit
    `|id, payload_len| id == 7 && payload_len - 8 > block_size`
    (le `block_size` torrent remonte via la session, comme
    `wire_bytes`). Alternative minimale : garder l'écart comme
    non-bug tranché (borne 1 MiB déjà protectrice) et documenter.
  - **test de régression suggéré** : frame `piece` annoncée à
    `block_size + 1` → erreur remontée dès 5 octets lus, sans
    attendre le reste du corps (test unitaire sur `decode_next`).

## Invariants de frontière (Seam Checks)

- **Net → Session (codec → dispatch)** : `WireError::{Truncated,
  BadLength, UnknownId}` → `PeerGone::Transport` →
  `peer_disconnected` mappe `BadLength{id}` → bump
  `num_incoming_*` + `PeerError`, `UnknownId` → `PeerError` sans
  bump, io-kinds → jambes `*_peers` (session.rs:4765-4803). Le
  séquencement upstream (compteur `finished` après le handler,
  jamais pour `default:`) est respecté. `Malformed{id}` traverse
  proprement : `wire_id()` → compteur, dispatch no-op.
- **Bornes codec vs contextuelles** : tailles fixes au codec,
  tailles dépendantes du torrent (bitfield/num_pieces, piece/
  block_size, hashes/arbres, protocol_v2/metadata) en session —
  correspond exactement au split `bt_peer_connection` /
  `peer_connection`/`torrent` upstream. D-D est la seule borne
  contextuelle qu'on ne peut pas remonter au framer sans hook.
- **Net → Session (handshake)** : les bits réservés sont lus
  côté net (`Handshake::supports_*`) et appliqués côté session
  (`supports_fast`, `supports_dht`, `protocol_v2`,
  `supports_extension` → envoi ext-handshake, engine.rs:5194-5199
  = `bt_peer_connection.cpp:3612`). Ordre upstream post-handshake
  (ext handshake → bitfield → dht_port → hash_request) préservé
  (ext handshake inline avant `peer_connected` effects).
- **Session → Disk** : `validate_hash_request` vit dans
  `bitdaemon_disk::hash_picker` et est appelée par `torrent.rs`
  avant tout accès arbre — la frontière ne re-fait pas confiance
  au wire.
- **Engine pump** : `BytesMut` persistant + `decode_next` —
  aucun `read_exact` dans `select!` (le commentaire engine.rs:
  5237-5240 documente la non-cancellation-safety évitée) ;
  RC4 appliqué en place avant décodage (engine.rs:5370-5373 =
  `crypto_receive_buffer` upstream).

## Fichiers du fix

- `crates/bitdaemon/src/engine.rs` — `dht = true` aux 5 call
  sites handshake (D-A) ; hook `too_large` pour `decode_next`
  (D-D).
- `crates/bitdaemon-net/src/bt_peer_conn.rs` — signature
  `decode_next` + peek id/longueur (D-D).
- `crates/bitdaemon-session/src/peer_conn.rs` — déballage
  `yourip` v4-mappée (D-B).
- `crates/bitdaemon-session/src/session.rs` — leg pid `-WW`/`-WD`
  → `on_interested` (D-C).
- `crates/bitdaemon-net/tests/test_peer_wire.rs` — régression
  D-A + D-D.
- `crates/bitdaemon-session/tests/test_peer_dispatch.rs` —
  régressions D-B ; gaps `invalid_suggest`, `multiple_have_all`,
  `dht_port_no_support`, `suggest_order`.
- `crates/bitdaemon-session/tests/test_peer_lifecycle_a07.rs` —
  régression D-C.

## Corrigées depuis la passe 3

| Déviation | État | Preuve |
|---|---|---|
| D1 — `on_hash_request` sans `validate_hash_request` (panic possible) | **corrigée** | validate avant `get_hashes` (`torrent.rs:5923-5932`) → `HashReject` ; test `v2_hash_request_out_of_range_is_rejected_not_fatal` |
| D2 — `on_hashes` sans validate ni kill `invalid_hashes` | **corrigée** | validate avant match (`torrent.rs:5974-5986`), `!result.valid` → `peer_error_effect` (`torrent.rs:6003-6008`) ; test `v2_hashes_invalid_descriptor_disconnects` |
| D4 — `hashes` < 49 B → `BadLength` | **corrigée** | `Malformed{id:22}` silencieux (`peer_wire.rs:665-672`) ; test `short_hashes_decodes_to_silent_malformed` |
| D5 — bitfield surdimensionné accepté | **corrigée** | borne exacte `div_ceil(8)` (`peer_conn.rs:1429-1435`) ; tests `oversized_bitfield_disconnects`, `undersized_bitfield_disconnects` |
| D6 — double-comptage `num_incoming_*` ids 0-8 | **corrigée** | bump unique dans `Torrent::on_peer_message` (`torrent.rs:5767-5769`), commentaire explicite `session.rs:3865-3869` |
| D7 — `NumIncomingExtHandshake` pré-parse | **corrigée** | bump à l'intérieur du `if let Some(hs)` (`peer_conn.rs:1938-1942`) ; test `ext_handshake_counts_only_after_parse` |
| D8 — `hash_reject` ne poke qu'une conn | **corrigée** | boucle `v2_peers` → `maybe_send_hash_request` (`torrent.rs:6151-6159`) ; test `v2_hash_reject_pokes_all_v2_peers` |

## Non-bugs tranchés

- `hash_request`/`hashes`/`hash_reject` sur conn non-v2 ou sans
  metadata → disconnect `PeerError` avec libellé descriptif
  (`"hash_request on non-v2 connection"`) vs `invalid_message`
  upstream — même sévérité, même teardown, nom cosmétique
  (torrent.rs:5786-5809 vs bt_peer_connection.cpp:1150-1154,
  1223-1227, 1318-1322).
- `hashes`/`hash_reject` non matchés → drop silencieux : upstream
  identique (`std::remove` sur `m_hash_requests` puis return,
  bt_peer_connection.cpp:1281-1284 ; torrent.rs:5993-5996,
  6126-6129).
- Ordre validate-taille dans `hashes` : Rust vérifie la taille au
  codec **avant** `validate_hash_request` session, upstream
  valide d'abord (`bt_peer_connection.cpp:1262-1277`). Les deux
  ordres tuent la connexion sur toute entrée invalide — seuls
  les cas « taille OK pour un descriptor invalide » divergent en
  *chemin*, jamais en issue (count=-1/proof=1/len=49 → decode OK
  → validate session → même `invalid_hashes` ; count=-1/len≠formule
  → `BadLength` → même disconnect).
- `hash_request`/`hash_reject` de taille ≠ 49 sur conn **v1** :
  upstream teste `protocol_v2` avant la taille → `invalid_message` ;
  Rust teste la taille d'abord → `BadLength`. Disconnect peer_error
  dans les deux cas.
- `piece` de corps < 9 B et `hashes` < 49 B → `Malformed` :
  reproduction fidèle des returns incrémentaux upstream (paquet
  fini, compté, conn vivante — jamais disconnect).
- `PeerWireCodec` n'est pas sur le chemin prod (l'engine utilise
  `decode_next` qui conserve les `WireError` typés) ; le codec
  tokio_util reste exposé avec le même cap 1 MiB pour cohérence.
- `num_pieces <= 0` (magnet) : bitfield sans borne exacte —
  upstream saute le check sur `!valid_metadata` et stocke
  `(packet_size-1)*8` bits ; Rust stocke `bits.len()*8` —
  équivalent (peer_conn.rs:1414-1423 vs
  bt_peer_connection.cpp:976-991). Même chose pour `have` :
  croissance de `peer_pieces` bornée `max_piece_count` conforme
  (`peer_conn.rs:1300-1306` = `peer_connection.cpp:1985-2001`).
- `on_extended` `associated_torrent().expired()` →
  `invalid_extended` : sans objet — les conns Rust meurent avec
  leur torrent.
- `on_unknown_message` (hook plugin pour ids inconnus,
  bt_peer_connection.cpp:2186-2192) : non porté — aucun plugin
  upstream ne l'implémente (tous retournent false →
  `invalid_message` identique).
- `reqq` borné `u16::MAX` (peer_conn.rs:1915-1918) =
  `clamp_assign<std::uint16_t>` upstream (peer_connection.cpp:4792).
- `upload_only`/`share_mode`/`holepunch`/`lt_donthave` aux ids
  locaux fixes même si le pair n'a rien négocié : upstream
  identique (checks `extended_id == *_msg` constants,
  bt_peer_connection.cpp:1918-1984).
- `quirk` compteurs FAST (ids 13-17 → offsets `num_outgoing_*`) :
  bug upstream conservé volontairement pour parité stats.
- `parse_handshake` exige les 68 B d'un coup (upstream lit
  20+28+20) : même validation, seul le buffering diffère —
  `run_handshake_responder_auto` fait déjà le probe 20 B
  (bt_peer_conn.rs:624-646).

## Historique — déviations corrigées (passe 1 et 2)

| Déviation | Correction |
|---|---|
| Cap frame = 4 MiB (`read_message`) / 2 MiB (codec) | 1 MiB partout — upstream `packet_size > 1024*1024` → `packet_too_large` à la lecture du préfixe |
| `piece` de corps < 9 B → erreur codec | `PeerMessage::Malformed{id}` — upstream consomme le sous-header silencieusement |
| Canal `gone` = `(ih, peer)` — erreur socket/codec jetée avant teardown | `gone_tx` porte `Option<PeerGone>` |
| `DisconnectPeer { failed: bool }` | `DisconnectSeverity` typée (Normal/Failure/PeerError = `disconnect_severity_t`) |
| Alertes dupliquées | `peer_disconnected` = site unique : `peer_error` → `peer_error_alert` + `error_peers` (mutuellement exclusif avec `peer_disconnected_alert`) |
| Compteurs `disconnect()` jamais incrémentés | `disconnected_peers` + classification io-kind + `disconnect_reason_counter` + jambes `error_*` |
| `BadLength` non compté | Frame complète rejetée → `num_incoming_*` bumpée AVANT l'alerte ; `UnknownId` → pas de bump |
| Sévérités erronées | table `self_connection`, `invalid_*`, `packet_too_large`, etc. calée upstream |
| `peer_error_effect` double-post | ne pousse que le Disconnect ; l'alerte part de `peer_disconnected` |
| FAST msgs non gatés | `RejectRequest`/`AllowedFast`/`SuggestPiece`/`HaveAll`/`HaveNone` exigent `supports_fast` |
| `on_choke` sans FAST ne rejetait pas la dlq | `incoming_reject_request(to_req(block))` par entrée |
| `DhtPort` gaté sur `supports_dht`, pas de réponse | `add_dht_node` inconditionnel + flip + réponse si `sent_bitfield` |
| `m_sent_bitfield` jamais posé | stampé à l'attach quand `valid_metadata` |
| `disconnect_if_redundant` absent post-ext-handshake | appelé à `PeerEffect::ExtHandshake` + `RemoteUploadOnly` |
| Éviction `too_many_connections` bypassait le teardown | `DisconnectPeer` route tout le cycle |
| D3 — négociation v2 (4 legs) | `advertise_v2`, `apply_v2_bit`/`V2OnV1`, `known_torrent` v1+v2, `wire_ih(proto_v2)` |
| D9 — échec handshake entrant | `dial_fail` + `peer_conn_failed_inbound` + compteurs/alerte |
