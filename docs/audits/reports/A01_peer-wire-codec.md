# Audit A01 — rapport d'analyse

Baseline upstream : `e7381c7bc` (`src/bt_peer_connection.cpp`,
`src/peer_connection.cpp`, `src/hash_picker.cpp`,
`src/session_impl.cpp`, `src/torrent.cpp`).

**Passe 3** — re-audit du périmètre. Les déviations D3 (négociation
v2) et D9 (routage échec handshake entrant) sont **corrigées** depuis
la passe 2 — détails en `## Corrigées depuis la passe 2`. Les
déviations D1, D2, D4, D5, D6, D7, D8 restent **ouvertes** (numéros
de ligne rafraîchis). Passe 1 en `## Historique`.

## Verdicts checklist

- Length-prefix 4B BE inclut l'id, keepalive = len 0 — bornes exactes
  upstream (`packet_size` checks par message) — verdict : **conforme**
  — preuve : `decode` sur corps complet + `PeerWireCodec` cap
  `1024*1024` = `packet_too_large` upstream (peer_wire.rs:714-717,
  742-747) ; `decode(&[])` → `Keepalive` (peer_wire.rs:570-572).
  Upstream `on_receive_impl` : `packet_size > 1024*1024 || < 0` →
  disconnect `packet_too_large` peer_error (bt_peer_connection.cpp:
  3686-3693) ; le `< 0` C++ est couvert en Rust par le cast `u32`
  (un préfixe > i32::MAX tombe dans la même borne). Tests :
  `decoder_default_cap_is_one_mib`, `decoder_enforces_max_len`,
  `decoder_waits_for_full_frame` (test_peer_wire.rs).
- Chaque `msg_*` : round-trip encode/decode + rejet des tailles
  invalides (have len 5, request len 13, bitfield len = ceil(n/8)+1…)
  — verdict : **déviation** (bitfield surdimensionné accepté —
  D5) — preuve : `expect_len` par message (peer_wire.rs:556-561,
  bras 577-698) ; `Bitfield` sans borne exacte au codec
  (peer_wire.rs:597), validée session-side par
  `Bitfield::from_bytes` qui **tronque** le surplus
  (bitfield.rs:53-63, peer_conn.rs:1375-1403) vs `on_bitfield`
  exigeant `packet_size-1 == (num_pieces+7)/8`
  (bt_peer_connection.cpp:978-983). Tests :
  `message_encodings_byte_exact`, `round_trip_all`,
  `have_none_rejects_payload` (test_peer_wire.rs).
- Handshake 68B : validation info_hash/proto, peer_id, reserved bits
  (BEP-10, FAST, v2) — verdict : **conforme** (D3 corrigée) —
  preuve : `parse_handshake` strict (peer_wire.rs:309-336) =
  `packet_size != 19 || pstr != "BitTorrent protocol"` → disconnect
  upstream (bt_peer_connection.cpp:3330-3350) ; bits reserved
  identiques (peer_wire.rs:28-46 vs write_handshake
  bt_peer_connection.cpp:736-756) ; négociation v2 complète —
  `apply_v2_bit` (bt_peer_conn.rs:98-108, rejet `V2OnV1`),
  `advertise_v2` dial (engine.rs:4116), `wire_ih(proto_v2)` choisit
  l'ih filaire (torrent.rs:3777-3782), résolution ih v1+v2-tronqué
  (`known_torrent`, session.rs:5120-5134). Tests :
  `handshake_layout`, `handshake_rejects_bad_pstr` (test_peer_wire),
  `handshake_round_trip`, `responder_v2_bit_flips_before_reply`,
  `responder_rejects_v2_claim_on_v1_only` (test_bt_peer_conn).
- Messages hors-ordre : have/bitfield avant handshake, piece avant
  bitfield, reject sans request — verdict : **conforme** — preuve :
  les messages ne sont dispatchés qu'après attach torrent (le
  handshake gate en amont) ; `!bitfield_received → on_have_none()`
  implicite avant `Have`, `Request`, `Piece` (peer_conn.rs:1245-1248,
  1428-1431, 1623-1626) ; `on_reject` sans request → counters +
  `flush_requests` seulement (= `incoming_reject_request` upstream).
  Test : `piece_before_bitfield_runs_have_none`,
  `malformed_reject_is_a_noop` (test_peer_dispatch.rs).
- Dépassements : oversized piece (packet_too_large), truncated
  frames, length incohérente → disconnect vs drop silencieux —
  verdict : **déviation** (`hashes` < 49 B → disconnect au lieu de
  drop silencieux — D4) — preuve : peer_wire.rs:666-668 `BadLength`
  vs upstream `recv_buffer.size() < header_size → return`
  (bt_peer_connection.cpp:1245-1248). `piece` court → `Malformed`
  silencieux conforme (peer_wire.rs:609-611, test
  `short_piece_decodes_to_silent_malformed`) ; `piece` >
  `block_size` → `packet_too_large` (peer_conn.rs:1568-1574 vs
  bt_peer_connection.cpp:1041-1046, test `oversized_piece_disconnects`).
- Compteurs `num_incoming_*` par type de message (funnel dispatch)
  — verdict : **déviation** (double-comptage ids 0-8 — D6 ; ordre
  du bump `ext_handshake` — D7) — preuve : mapping
  `incoming_counter_idx` (counters.rs:29-37) conforme à
  `dispatch_message` (bt_peer_connection.cpp:2213-2221, quirks
  conservés) ; mais session.rs:3790-3800 + torrent.rs:5678-5680
  bumpent **deux fois** les ids 0-8 ;
  `NumIncomingExtHandshake` bumpé pré-parse (peer_conn.rs:1807) vs
  fin de `on_extended_handshake` upstream.
- Port/dht_port message, extension id 0 handshake vs ids étendus —
  verdict : **conforme** — preuve : `DhtPort` → `add_dht_node`
  inconditionnel + flip supports + réponse `DhtPort` si
  `sent_bitfield` (torrent.rs:5387-5414 = bt_peer_connection.cpp:
  1393-1402) ; ext id 0 → `ExtendedHandshake::parse`, échec → drop
  silencieux = upstream (bt_peer_connection.cpp:1911-1916, 2006).
  Ids étendus locaux (`upload_only`/`share_mode`/`holepunch`/
  `dont_have`/`ut_*`) avec les mêmes bornes (`data.len()!=1` ⇔
  `packet_size != 3`, `data.len()!=4` ⇔ `!= 6`, peer_conn.rs:
  1932-1960 vs bt_peer_connection.cpp:1921,1942,1973).
- Messages v2 : validation base/index/count/proof_layers (jamais de
  panique sur int négatif ou wrap) — verdict : **déviation**
  (D1 critique : panique possible sur `hash_request` malveillant ;
  D2 : pas de kill `invalid_hashes` ; D8 : `hash_reject` ne poke
  qu'une conn) — preuve : `on_hash_request` n'appelle pas
  `validate_hash_request` (torrent.rs:5817-5843 vs
  bt_peer_connection.cpp:1199-1203) ; `on_hashes` idem et ignore
  `result.valid` (torrent.rs:5849-5941 vs
  bt_peer_connection.cpp:1262-1266, 1302-1305) ; `on_hash_reject`
  ne poke que la conn refusante (torrent.rs:5999-6032 vs
  torrent.cpp `hashes_rejected` bouclant tous les peers v2).
- Bitfield surdimensionné ou sous-dimensionné : rejet strict —
  verdict : **déviation** (D5 — surdimensionné accepté/tronqué ;
  sous-dimensionné rejeté = conforme) — preuve :
  `Bitfield::from_bytes` `bits > data.len()*8 → None` → disconnect
  (conforme) mais `data.len() > ceil(n/8)` accepté silencieusement
  (bitfield.rs:53-63) vs `invalid_bitfield_size` peer_error upstream
  (bt_peer_connection.cpp:978-983).

## Couverture des tests upstream (test/test_*.cpp)

- `test_receive_buffer.cpp` — tous les tests portés
  (`recv_buffer_*`, `receive_buffer_*`, `crypto_receive_buffer_*`,
  test_receive_buffer.rs) — couvert.
- `test_fast_extension.cpp` :
  - `invalid_suggest` (suggest négatif → pas de request) — couvert
    partiellement (drop négatif dans `apply_peer_effects`,
    torrent.rs:5496-5510 ; test FAST-gating
    `fast_messages_require_fast_negotiation`) ; scénario dédié
    suggest `-234` non porté — **manquant** (faible, borne négative
    déjà testée par le gate).
  - `multiple_bitfields` — second bitfield → `peer_lost` refcount :
    implémenté (`RefcountBulk(old, false)`, peer_conn.rs:1385-1387)
    mais **pas de test dédié** — manquant.
  - `multiple_have_all` — même pattern (`RefcountBulk` décrément
    avant re-set, peer_conn.rs:1302-1304) — **manquant**.
  - `dht_port_no_support` — dht_port sans bit DHT au handshake →
    accepté sans réponse : implémenté (torrent.rs:5387-5414) —
    **manquant** (test trivial à porter).
  - `invalid_request`, `reject_fast`, `suggest_order`,
    `reject_predictive_piece_requests` — couverts en substance par
    `invalid_request_rejected_and_alerted`,
    `choked_request_rejected_then_disconnects`,
    `choked_accept_fast_request_served_with_budget`,
    `request_queue_cap_rejects` (test_peer_dispatch.rs) ; la séquence
    exacte `suggest_order` (priorité front-insert) non portée —
    partiel.
  - `allowed_fast_survives_metadata`, `suggested_piece_survives_metadata`
    — hors périmètre codec (magnet/metadata, cf. A19).
- `test_hash_picker.cpp::validate_hash_request` (+ `_pad_file`,
  `_empty_file`, `_single_block_file`) — la fonction est portée
  (hash_picker.rs:91-118) mais **non appelée** sur le chemin
  `on_hash_request`/`on_hashes` (D1/D2) ; les tests deviennent les
  régressions des fixes — manquants côté wire.
- `test_buffer.cpp`, `test_bitfield.cpp` — cœur buffer/bitfield
  couvert ailleurs (common/disk) ; rien de wire-spécifique.

## Déviations (ouvertes)

- `crates/bitdaemon-session/src/torrent.rs:5817` — `on_hash_request`
  appelle `merkle_tree.get_hashes` avec `req.base/index/count/
  proof_layers` bruts : `req.base as usize` sur un i32 négatif wrappe
  → `merkle_num_layers(num_leafs) - base` **panic d'underflow**
  (merkle_tree.rs:855) ; `Vec::with_capacity(count)` sur `count`
  ≈ 2³¹ → **abort OOM** (merkle_tree.rs:859). Upstream valide
  d'abord : `!validate_hash_request(hr, files) → write_hash_reject`
  (bt_peer_connection.cpp:1199-1203) — jamais de disconnect, jamais
  d'accès tree non borné. Sévérité : **critique** (panic distant
  via frame 49 B craftée sur conn v2 ; `file_root` est metadata
  publique). [Limites][Erreur]
  - **sketch** : dans `on_hash_request`, résoudre `f` puis
    `hp_req = HashRequest { file: f, base, index, count,
    proof_layers }` ; gater `resp` sur
    `bitdaemon_disk::hash_picker::validate_hash_request(&hp_req, fs)`
    (helper existant, hash_picker.rs:91) — `None` → `HashReject`
    (upstream écrit `hash_reject`). Régression : `hash_request` avec
    `base = -1`, `count = 0x40000000`, `index` hors `level_size` →
    réponse `HashReject`, session vivante.

- `crates/bitdaemon-session/src/torrent.rs:5849` — `on_hashes` :
  `validate_hash_request` absent ET `result.valid` ignoré. Upstream
  tue `invalid_hashes` peer_error **avant** le match
  `m_hash_requests` (bt_peer_connection.cpp:1262-1266) puis
  **après** `add_hashes` quand `!valid` (1302-1305). Rust : req
  invalide → `hash_requests.iter().position` jamais matché → drop
  silencieux ; `valid=false` → conn vivante. Sévérité :
  wire-visible. [Erreur][Limites]
  - **sketch** : réordonner — résoudre `f = file_index_for_root`,
    construire `hp_req`, `!validate_hash_request(&hp_req, &fs) →
    peer_error_effect(peer, "invalid_hashes")` ; puis match
    `hash_requests` (ordre upstream : validate → size[déjà en
    decode] → match → add) ; après `picker.add_hashes`,
    `!result.valid → peer_error_effect(peer, "invalid_hashes")`.
    Régression : `hashes` avec `count=9000` ou `proof_layers` hors
    bornes → `PeerError` disconnect `invalid_hashes` ; `hashes`
    matchée dont les preuves échouent → idem.

- `crates/bitdaemon-net/src/peer_wire.rs:666` — trame `hashes` de
  corps < 49 B → `BadLength` → disconnect `PeerError` (comptée
  `num_incoming_extended` via peer_disconnected). Upstream :
  `recv_buffer.size() < header_size → return` — consommée
  **silencieusement**, comptée `num_incoming_extended`, conn
  vivante (bt_peer_connection.cpp:1245-1248 — même schéma que
  `on_piece` < 9 B). Sévérité : wire-visible. [Erreur]
  - **sketch** : `if buf.len() < 49 { return Ok(PeerMessage::
    Malformed { id }); }` — symétrie du bras `piece`
    (peer_wire.rs:609-611). `Malformed{id:22}` →
    `incoming_counter_idx(22)` → `num_incoming_extended`
    automatiquement. Régression :
    `short_hashes_decodes_to_silent_malformed`.

- `crates/bitdaemon-session/src/peer_conn.rs:1369` — bitfield
  surdimensionné accepté : `Bitfield::from_bytes(bits, num_pieces)`
  ne rejette que les trames **trop courtes** et tronque le reste
  (`data[..bits.div_ceil(8)]`, bitfield.rs:53-63). Upstream exige
  `packet_size - 1 == (num_pieces + 7) / 8` exactement →
  `invalid_bitfield_size` peer_error (bt_peer_connection.cpp:
  978-983). Sévérité : wire-visible. [Limites][Erreur]
  - **sketch** : avant `from_bytes`, `num_pieces > 0 &&
    bits.len() != (num_pieces as usize + 7) / 8 →
    PeerEffect::Disconnect("invalid bitfield size", PeerError)` —
    `from_bytes` reste un helper générique ; la branche
    `num_pieces <= 0` (magnet) reste sans borne exacte — upstream
    aussi (`!valid_metadata` saute le check). Régression :
    bitfield `ceil(n/8)+1` B → disconnect peer_error.

- `crates/bitdaemon-session/src/session.rs:3790` — double-comptage
  `num_incoming_*` pour les ids 0-8 : le `match p.msg` session bump
  `NumIncoming{Choke..NotInterested}`, puis
  `Torrent::on_peer_message` rebump `incoming_counter_idx(wire_id)`
  (torrent.rs:5678-5680) → choke/have/request/piece/… comptés
  **×2**. Upstream compte une fois (`dispatch_message`, et seulement
  quand `associated_torrent()` est vivant — return early sinon,
  bt_peer_connection.cpp:2141-2145). Sévérité : état
  (`session_stats` visible). [Compteurs]
  - **sketch** : supprimer le bloc `match p.msg { … }` session
    (session.rs:3790-3800) — le funnel torrent couvre tous les ids
    dont les 9 (même mapping) ; un msg sur torrent absent n'est pas
    compté, = upstream. `RecvBytes` reste. Régression : envoyer
    `Have` → `NumIncomingHave == 1` (pas 2) ; msg sur ih inconnu →
    compteurs inchangés.

- `crates/bitdaemon-session/src/peer_conn.rs:1807` —
  `NumIncomingExtHandshake` bumpé **avant**
  `ExtendedHandshake::parse` ; upstream en toute fin de
  `on_extended_handshake` — un handshake bdecode-invalide n'est pas
  compté (bt_peer_connection.cpp:2006 + 2131). Sévérité :
  cosmétique (compteur). [Compteurs]
  - **sketch** : déplacer le `counters.inc` à la fin du bloc
    `if let Some(hs)` (après `eff.push(ExtHandshake)`).

- `crates/bitdaemon-session/src/torrent.rs:6028` — `on_hash_reject`
  ne poke que la conn refusante ; upstream `torrent::hashes_rejected`
  boucle **tous** les peers et appelle `maybe_send_hash_request` sur
  chaque conn v2 — une conn v2 oisive reprend la demande au lieu
  d'attendre le prochain événement. Sévérité : état (vivacité du
  pipeline hash). [Cycle de vie]
  - **sketch** : après `picker.hashes_rejected(&hp_req)`, itérer
    `self.peers` filtré `c.protocol_v2` →
    `maybe_send_hash_request(p)` (le gate `!protocol_v2 || len>1`
    interne garde l'idempotence).

## Invariants de frontière (Seam Checks)

- **Net → Session** : le codec (`decode`) classe les erreurs en
  `WireError::{Truncated,BadLength,UnknownId}` → `PeerGone::
  Transport` ; `peer_disconnected` mappe `BadLength{id}` → bump
  `num_incoming_*` puis `PeerError`, `UnknownId` → `PeerError` sans
  bump — conforme au séquencement upstream (compteur après handler,
  jamais pour `default:`). La frontière `Malformed` (piece<9 B)
  traverse proprement (`wire_id` → compteur, dispatch no-op).
- **Codec → Torrent** : la borne `packet_size` exacte des messages à
  taille fixe vit au codec ; les bornes **contextuelles** (bitfield
  vs `num_pieces`, piece vs `block_size`, hash vs arbres) vivent
  session-side — la séparation correspond à upstream
  (`bt_peer_connection` vs `peer_connection`/`torrent`). D1/D2/D5
  sont précisément des bornes contextuelles manquantes.
- **Session → Disk** : `HashPicker::add_hashes` partagé entre wire
  (`on_hashes`) et chemins internes ; son `debug_assert!(validate)`
  est seulement défensif — la validation wire doit vivre dans
  `torrent.rs` (sketch D2), pas dans le picker.

## Fichiers du fix

- `crates/bitdaemon-net/src/peer_wire.rs` — `Malformed` pour
  `hashes` court (D4).
- `crates/bitdaemon-session/src/peer_conn.rs` — borne exacte
  bitfield (D5), ordre compteur ext-handshake (D7).
- `crates/bitdaemon-session/src/torrent.rs` —
  `validate_hash_request` sur `on_hash_request`/`on_hashes` (D1,
  D2), `result.valid` (D2), poke-all v2 sur `hash_reject` (D8).
- `crates/bitdaemon-session/src/session.rs` — retrait
  double-comptage (D6).
- `crates/bitdaemon-net/tests/test_peer_wire.rs` —
  `short_hashes_decodes_to_silent_malformed` (D4).
- `crates/bitdaemon-session/tests/test_peer_dispatch.rs` /
  `test_torrent.rs` — régressions D1, D2, D5, D6, D7, D8 +
  `multiple_bitfields`, `dht_port_no_support` (gaps de test).

## Corrigées depuis la passe 2

| Déviation | État | Preuve |
|---|---|---|
| D3 — négociation v2 (4 legs) | **corrigée** | (a) `advertise_v2 = has_v2 && !proto_v2` au dial (engine.rs:4116) ; (b) `apply_v2_bit` marque `protocol_v2` et rejette `V2OnV1` → `invalid_info_hash` (bt_peer_conn.rs:98-108), `conn.protocol_v2 = kt.protocol_v2` (engine.rs:4357,4554,5026,3665) ; (c) `known_torrent`/`known_torrent_by_skey` résolvent v1 + v2-tronqué (session.rs:5120-5142) ; (d) `peer_link_up` retourne `wire_ih(proto_v2)` (torrent.rs:3777-3782). Tests : `responder_v2_bit_flips_before_reply`, `responder_rejects_v2_claim_on_v1_only` (test_bt_peer_conn.rs) |
| D9 — échec handshake entrant | **corrigée** | `run_handshake_responder` `Err` → `dial_fail` + `peer_conn_failed_inbound` → `disconnect_alert_counters` (engine.rs:4975-4983, session.rs:4989-5030) : `DisconnectedPeers`, legs io-kind/par-nom, `error_incoming_peers`, alerte — sévérités calées upstream |

## Non-bugs tranchés

- `hashes`/`hash_reject` non matchés → drop silencieux : upstream
  identique (`std::remove` sur `m_hash_requests` puis return,
  bt_peer_connection.cpp:1281-1284, 1362-1364). La déviation D2 est
  l'absence de kill sur **req invalide** et sur `add_hashes`
  invalide — pas le drop non-matché, qui est conforme.
- `hash_request` sur conn non-v2 → `PeerError` : le libellé Rust est
  `"hash_request on non-v2 connection"` vs `invalid_message`
  upstream — même sévérité, même teardown ; nom cosmétique. Idem
  `hashes`/`hash_reject`.
- `on_hash_reject` sans `validate_hash_request` : upstream ne valide
  pas non plus dans ce handler (parse + match direct,
  bt_peer_connection.cpp:1311-1368) — conforme.
- `debug_assert!(validate_hash_request)` dans
  `HashPicker::add_hashes` (hash_picker.rs:347) : injouable depuis le
  fil — `on_hashes` ne l'atteint qu'après match d'une requête qu'on
  a émise (forcément valide). Le gap est l'absence de kill, pas le
  panic.
- `num_pieces <= 0` (magnet) : bitfield acceptée sans borne exacte —
  upstream saute le check sur `!valid_metadata` aussi
  (bt_peer_connection.cpp:978-979) et stocke `(packet_size-1)*8`
  bits ; conforme.
- `is_all_zeros` ih echo : upstream la refuse explicitement ; Rust
  la refuse via `hs.info_hash != *info_hash` — sous-ensemble,
  équivalent.
- `PeerWireCodec` (tokio_util) n'est pas sur le chemin prod —
  l'engine utilise le décodage typé qui conserve les `WireError` ;
  le codec applicatif a le cap 1 MiB pour cohérence.
- Piece > `block_size` : vérifié session-side
  (`packet_too_large`), pas codec-side — upstream compare à
  `t->block_size()` qui n'existe pas sans contexte torrent.
- `PeerEffect::PeerError`/`InvalidRequest` (alerte non fatale)
  conservé : upstream poste des `peer_error_alert` non-fatales
  (`invalid_request` hors-limites → reject + alerte, pas de
  disconnect sauf >300 choked — peer_conn.rs:1466-1491).
- Extension `on_unknown_message` (plugin hook upstream pour ids
  inconnus, bt_peer_connection.cpp:2186-2192) : non portée — aucun
  plugin Rust ne consomme d'ids hors protocole ; si un plugin
  l'exige, il passera par une révision de `decode`. Tranché :
  écart assumé (pas de plugin tiers dans ce périmètre).
- `hashes`/`hash_request`/`hash_reject` comptés
  `num_incoming_extended` : conforme (`packet_type <=
  msg_allowed_fast` upstream envoie 21-23 dans le bucket
  extended).

## Historique — déviations corrigées (passe 1)

| Déviation | Correction |
|---|---|
| Cap frame = 4 MiB (`read_message`) / 2 MiB (codec) | 1 MiB partout — upstream `packet_size > 1024*1024` → `packet_too_large` à la lecture du préfixe (`peer_wire.rs`, `bt_peer_conn.rs`, `PeerWireCodec::default`) |
| `piece` de corps < 9 B → erreur codec | Nouveau `PeerMessage::Malformed{id}` — upstream `on_piece` consomme le sous-header **silencieusement** (paquet compté, conn vivante). Dispatch : `M::Malformed => {}`, compté via `wire_id` → `num_incoming_piece` |
| Canal `gone` = `(ih, peer)` — l'erreur socket/codec était jetée avant teardown | `gone_tx` porte `Option<PeerGone>` : `Transport(PeerConnError)` pour EOF/reset/decode-error, `Disconnected{severity, reason}` pour la réservation session |
| `DisconnectPeer { failed: bool }` | Sévérité typée `DisconnectSeverity` (Normal/Failure/PeerError = `disconnect_severity_t`) de bout en bout |
| Alertes dupliquées (`route` postait + `peer_disconnected` repostait) | `peer_disconnected` = site unique : `peer_error` → `peer_error_alert` + `error_peers` (jamais `peer_disconnected_alert` — mutuellement exclusif upstream) ; `normal`/`failure` → `peer_disconnected_alert`. L'écho `gone` post-teardown retombe sur `had == false` → no-op |
| Compteurs `disconnect()` jamais incrémentés | `disconnected_peers` à chaque close ; classification io-kind ; par-nom (`disconnect_reason_counter`) ; `sev > normal` → `error_tcp`/`error_utp` + `error_outgoing`/`error_incoming` |
| `BadLength` non compté | Frame complète rejetée → `num_incoming_*` bumpée AVANT l'alerte. `UnknownId` → **pas** de bump (le `default:` upstream return avant le bloc compteur) |
| Sévérités erronées | `self_connection`, `too_many_corrupt_pieces`, `banned_by_ip/port_filter`, dup peer-id, too_many_connections, `no_memory`, `too_frequent_pex` → **Normal** ; `pex_message_too_large`, `invalid_*`, `packet_too_large`, hash-msg sur conn non-v2 → **PeerError** |
| `peer_error_effect` postait l'alerte + DisconnectPeer (double post) | Ne pousse plus que le Disconnect ; l'alerte part de `peer_disconnected` avec `reason` = nom d'erreur upstream |
| FAST msgs non gatés | `RejectRequest`/`AllowedFast`/`SuggestPiece`/`HaveAll`/`HaveNone` exigent `supports_fast` — sinon `invalid_*` peer_error |
| `on_choke` sans FAST ne rejetait pas la dlq | Chaque entrée `download_queue` passe par `incoming_reject_request(to_req(block))` (abort picker + `outstanding_bytes` restauré). Avec FAST : la queue stalle simplement |
| `DhtPort` gaté sur `supports_dht`, pas de réponse | `incoming_dht_port` = `add_dht_node` **inconditionnel** + flip `supports_dht` + `write_dht_port()` si `sent_bitfield` |
| `m_sent_bitfield` jamais posé | Stampé à l'attach quand `valid_metadata` — y compris les legs have_all/have_none/super-seed (gate de la réponse DHT) |
| `disconnect_if_redundant` absent post-ext-handshake | Appelé à `PeerEffect::ExtHandshake` + `RemoteUploadOnly` |
| Éviction `too_many_connections` bypassait le teardown | Le `DisconnectPeer` route désormais tout le cycle (remove + compteurs + alerte + `disconnect_writer`) |

## Tests de régression ajoutés (passe 1)

`decoder_default_cap_is_one_mib`, `short_piece_decodes_to_silent_malformed`,
`unknown id → UnknownId` (test_peer_wire) ;
`peer_error_disconnect_posts_peer_error_not_disconnected`,
`normal_disconnect_posts_peer_disconnected_once`,
`transport_eof_is_normal_close`,
`wire_bad_length_counts_packet_then_peer_error`,
`wire_unknown_id_skips_packet_counter` (test_session) ;
`fast_messages_require_fast_negotiation`,
`fast_messages_accepted_when_negotiated`,
`choke_without_fast_rejects_download_queue` (test_peer_dispatch).
