# Audit A01 — rapport d'analyse

Baseline upstream : `e7381c7bc` (`src/bt_peer_connection.cpp`,
`src/peer_connection.cpp`, `src/hash_picker.cpp`,
`src/session_impl.cpp`, `src/torrent.cpp`).

**Passe 2** — re-audit du périmètre après la passe 1 (fixes déjà
mergés, ex-A06 renuméroté A01) : les déviations corrigées alors sont
consignées en `## Historique` en fin de document ; les déviations
ci-dessous sont **ouvertes**.

## Verdicts checklist

- Length-prefix 4B BE inclut l'id, keepalive = len 0 — bornes exactes
  upstream (`packet_size` checks par message) — verdict : **conforme**
  — preuve : `read_message`/`decode_next` (bt_peer_conn.rs:337-370) —
  cap `MAX_MESSAGE_LEN = 1024*1024` = `packet_too_large` upstream ;
  `decode(&[])` → `Keepalive` (peer_wire.rs:570-572). Upstream
  `on_receive_impl` : `packet_size > 1024*1024` → disconnect
  `packet_too_large` peer_error ; Rust mappe `Protocol("message too
  large")` → `PeerGone::Transport` → `PeerError` (session.rs:4192-4194).
- Chaque `msg_*` : round-trip encode/decode + rejet des tailles
  invalides (have len 5, request len 13, bitfield len = ceil(n/8)+1…)
  — verdict : **déviation** (bitfield surdimensionné accepté —
  D5) — preuve : `expect_len` par message (peer_wire.rs:577-660) ;
  `Bitfield` sans borne exacte (peer_wire.rs:597), validée session-side
  par `Bitfield::from_bytes` qui **tronque** (bitfield.rs:53-63,
  peer_conn.rs:1300-1334) vs `on_bitfield` exigeant
  `packet_size-1 == (num_pieces+7)/8` (bt_peer_connection.cpp:978-983).
- Handshake 68B : validation info_hash/proto, peer_id optionnel,
  reserved bits (BEP-10, FAST, v2) — verdict : **déviation**
  (négociation v2 absente — D3) — preuve : `write_handshake`
  toujours appelé `supports_v2=false` (bt_peer_conn.rs:103,132,153,
  265,299 vs upstream `!protocol_v2 && has_v2` → bit `reserved[7]&0x10`,
  bt_peer_connection.cpp:752-755) ; `hs.supports_v2()` parsé mais
  jamais consommé (session.rs:3826-3829 vs upstream
  bt_peer_connection.cpp:3471-3493).
- Messages hors-ordre : have/bitfield avant handshake, piece avant
  bitfield, reject sans request — verdict : **conforme** — preuve :
  `!bitfield_received → on_have_none()` implicite avant `Have`,
  `Request`, `Piece` (peer_conn.rs:1176-1179, 1345-1348, 1540-1543) ;
  `on_reject` sans request → counters + `flush_requests` seulement
  (= `incoming_reject_request` upstream).
- Dépassements : oversized piece (packet_too_large), truncated frames,
  length incohérente → disconnect vs drop silencieux — verdict :
  **déviation** (`hashes` < 49 B → disconnect au lieu de drop
  silencieux — D4) — preuve : peer_wire.rs:666-668 `BadLength` vs
  upstream `recv_buffer.size() < header_size → return`
  (bt_peer_connection.cpp:1245-1248). `piece` court → `Malformed`
  silencieux (peer_wire.rs:609-611) : conforme.
- Compteurs `num_incoming_*` par type de message (funnel dispatch)
  — verdict : **déviation** (double-comptage ids 0-8 — D6 ; ordre
  du bump `ext_handshake` — D7) — preuve : mapping
  `incoming_counter_idx` (counters.rs:29-37) conforme à
  `dispatch_message` (quirks inclus) ; mais session.rs:3590-3601 +
  torrent.rs:4656-4658 bumpent **deux fois** les ids 0-8 ;
  `NumIncomingExtHandshake` bumpé pré-parse (peer_conn.rs:1724) vs fin
  de `on_extended_handshake` (bt_peer_connection.cpp:2131).
- Port/dht_port message, extension id 0 handshake vs ids étendus —
  verdict : **conforme** — preuve : `DhtPort` → `add_dht_node`
  inconditionnel + flip supports + `write_dht_port` si
  `sent_bitfield` (passe 1) ; ext id 0 → `ExtendedHandshake::parse`,
  échec → drop silencieux = upstream (bt_peer_connection.cpp:2015-2027).
- Messages v2 (`hash_request`/`hashes`/`hash_reject`) — item
  complémentaire — verdict : **déviation** (D1, D2, D8) — preuve :
  `on_hash_request` n'appelle pas `validate_hash_request`
  (torrent.rs:4800-4811 vs bt_peer_connection.cpp:1199-1203) ;
  `on_hashes` idem et ignore `result.valid` (torrent.rs:4826-4867 vs
  bt_peer_connection.cpp:1262-1266, 1302-1305) ; `on_hash_reject`
  ne poke que la conn refusante (torrent.rs:5005-5008 vs
  torrent.cpp:7445-7456).

## Déviations

- `crates/bitdaemon-session/src/torrent.rs:4800` — `on_hash_request`
  appelle `merkle_tree.get_hashes` avec `req.base/index/count/
  proof_layers` bruts : `req.base as usize` sur un i32 négatif wrappe
  → `merkle_num_layers(num_leafs) - base` **panic d'underflow** en
  debug ; `Vec::with_capacity(count)` sur `count` ≈ 2³¹ → **abort
  OOM** (merkle_tree.rs:855-859). Upstream valide d'abord :
  `!validate_hash_request(hr, files) → write_hash_reject`
  (bt_peer_connection.cpp:1199-1203) — jamais de disconnect, jamais
  d'accès tree non borné. Sévérité : wire-visible + robustesse
  (frame 49 B craftée sur conn v2 ; `file_root` est metadata publique).
  - **sketch** : dans `on_hash_request`, résoudre `f` puis construire
    `hp_req = HashRequest { file: f, base, index, count, proof_layers }`
    et gater `resp` sur `validate_hash_request(&hp_req, fs)` (helper
    existant, hash_picker.rs:91) — `None` → `HashReject` (upstream
    écrit `hash_reject`). Régression : `hash_request` avec
    `base = -1`, `count = 0x40000000`, `index` hors `level_size` →
    réponse `HashReject`, session vivante.

- `crates/bitdaemon-session/src/torrent.rs:4826` — `on_hashes` :
  `validate_hash_request` absent ET `result.valid` ignoré. Upstream
  tue `invalid_hashes` peer_error **avant** le match
  `m_hash_requests` (bt_peer_connection.cpp:1262-1266) puis **après**
  `add_hashes` quand `valid == false` (1302-1305). Rust : req
  invalide → `hash_requests.iter().position` jamais matché → drop
  silencieux ; `valid=false` → conn vivante. Sévérité : wire-visible
  (on garde des pairs qu'upstream élimine ; l'ordre validate→match
  est aussi inversé).
  - **sketch** : réordonner — résoudre `f = file_index_for_root`,
    construire `hp_req`, `!validate_hash_request(&hp_req, &fs) →
    peer_error_effect(peer, "invalid_hashes")` ; puis match
    `hash_requests` (ordre upstream : validate → size[déjà en decode]
    → match → add) ; après `picker.add_hashes`, `!result.valid →
    peer_error_effect(peer, "invalid_hashes")`. Régression : `hashes`
    avec `count=9000` ou `proof_layers` hors bornes → `PeerError`
    disconnect `invalid_hashes` ; `hashes` matchée dont les preuves
    échouent → idem.

- `crates/bitdaemon-net/src/peer_wire.rs:666` — trame `hashes` de
  corps < 49 B → `BadLength` → disconnect `PeerError` (comptée
  `num_incoming_extended`). Upstream : `recv_buffer.size() <
  header_size → return` — consommée **silencieusement**, comptée
  `num_incoming_extended`, conn vivante (bt_peer_connection.cpp:
  1245-1248 — même schéma que `on_piece` < 9 B). Sévérité :
  wire-visible.
  - **sketch** : `if buf.len() < 49 { return Ok(PeerMessage::Malformed
    { id }); }` — symétrie du bras `piece` (peer_wire.rs:609).
    `Malformed{id:22}` → `incoming_counter_idx(22)` →
    `num_incoming_extended` automatiquement. Régression :
    `short_hashes_decodes_to_silent_malformed`.

- `crates/bitdaemon-net/src/bt_peer_conn.rs` + `session.rs` +
  `engine.rs` — **négociation v2 incomplète (4 legs)** :
  a. `write_handshake` toujours `supports_v2=false` — le bit
     `reserved[7]&0x10` n'est jamais émis ; upstream :
     `!peer_info_struct()->protocol_v2 && t->info_hash().has_v2()`
     → bit (bt_peer_connection.cpp:752-755). Un pair upstream marque
     la conn v1 → il ne servira jamais `hash_request`.
  b. `hs.supports_v2()` parsé mais jeté (`DialDone` ne le transporte
     pas ; `conn.protocol_v2` = match d'ih seul, session.rs:3826-3829).
     Upstream, entrant : `recv_buffer[7]&0x10` → `protocol_v2 = true`
     — le bit **seul** suffit — avec garde `valid_metadata && !has_v2
     → disconnect invalid_info_hash` (bt_peer_connection.cpp:3471-3479).
     Un pair v1-ih + bit v2 sur hybride nous envoie `hash_request` →
     chez nous `peer_error` disconnect, chez upstream accepté. La garde
     "bit v2 sur torrent v1-only → kill" manque aussi.
  c. `known_ihs` (engine.rs:748-749) et `skeys` (engine.rs:3039-3043)
     ne contiennent que les clés map (v1 pour hybride) : handshake
     entrant clé-v2-tronqué d'un torrent hybride → `WrongInfoHash` →
     refusé ; upstream `find_torrent` résout les deux hashes
     (session_impl.cpp:4779-4796 ; req2 sur `for_each` des deux,
     bt_peer_connection.cpp:3006-3013).
  d. `ConnectPeer.ih` = clé map ; `spawn_dial` envoie donc toujours
     la v1 — le flag `TorrentPeer.protocol_v2` (PEX `LT_V2`, DHT)
     n'est jamais consulté ; upstream envoie `associated_info_hash()`
     = v2-tronqué pour ces entrées (peer_connection.cpp:1115-1127 +
     write_handshake:776). Jamais de conn v2 sortante sur hybride.
  Ensemble : un torrent hybride ne parle jamais le protocole hash v2
  (les gates `protocol_v2` restent faux), refuse les conns v2-clées,
  et tue les pairs qui envoient des msgs hash sur conn v1-ih+bit.
  Sévérité : wire-visible / interop.
  - **sketch** : (a) passer `supports_v2` en param des
    `run_handshake*` comme `dht`/`fast` — dial :
    `swarm_peer.protocol_v2 || ti.has_v2()` → `!pv2 && has_v2` ;
    responder : `t.has_v2() && !hs.supports_v2()` (upstream n'émet le
    bit que si le pair n'est pas déjà v2). (b) `DialDone.supports_v2`
    → `peer_connected` : `conn.protocol_v2 = ih==v2trunc ||
    (supports_v2 && ti.has_v2)` ; `supports_v2 && valid_metadata &&
    !has_v2 → DisconnectPeer("invalid_info_hash", Failure)`. (c)
    `known_ihs`/`skeys` : insérer aussi `Sha1Hash(v2[..20])` pour
    chaque torrent avec `info_hash_v2()` ; résoudre wire-ih → clé map
    dans `peer_connected` (`conn.protocol_v2` suit l'ih filaire).
    (d) `spawn_dial` : `p.protocol_v2 && ti.has_v2` → ih filaire =
    `Sha1Hash(v2[..20])`. Régressions : handshake v1-ih+bit-v2 sur
    hybride → `hash_request` accepté ; inbound v2-clé hybride →
    attach ok ; bit v2 sur v1-only → `invalid_info_hash` ; dial sur
    entrée `protocol_v2` → ih v2-tronqué sur le fil.

- `crates/bitdaemon-session/src/peer_conn.rs:1300` — bitfield
  surdimensionné accepté : `Bitfield::from_bytes(bits, num_pieces)`
  ne rejette que les trames **trop courtes** et tronque le reste
  (`data[..bits.div_ceil(8)]`, bitfield.rs:53-63). Upstream exige
  `packet_size - 1 == (num_pieces + 7) / 8` exactement →
  `invalid_bitfield_size` peer_error (bt_peer_connection.cpp:978-983).
  Sévérité : wire-visible.
  - **sketch** : avant `from_bytes`, `num_pieces > 0 &&
    bits.len() != (num_pieces as usize + 7) / 8 →
    PeerEffect::Disconnect("invalid bitfield size", PeerError)` —
    `from_bytes` reste un helper générique ; la branche
    `num_pieces <= 0` (magnet) reste sans borne exacte upstream aussi.
    Régression : bitfield `ceil(n/8)+1` B → disconnect peer_error.

- `crates/bitdaemon-session/src/session.rs:3589` — double-comptage
  `num_incoming_*` pour les ids 0-8 : le `match p.msg` session bump
  `NumIncoming{Choke..NotInterested}`, puis `Torrent::on_peer_message`
  rebump `incoming_counter_idx(wire_id)` (torrent.rs:4656-4658) →
  choke/have/request/piece/… comptés **×2**. Upstream compte une fois
  (`dispatch_message`, et seulement quand `associated_torrent()` est
  vivant — return early sinon, bt_peer_connection.cpp:2141-2145).
  Sévérité : état (`session_stats` visible).
  - **sketch** : supprimer le bloc `match p.msg { … }` session
    (lignes 3589-3601) — le funnel torrent couvre tous les ids dont
    les 9 (même mapping) ; un msg sur torrent absent n'est pas compté,
    = upstream. `RecvBytes` reste. Régression : envoyer `Have` →
    `NumIncomingHave == 1` (pas 2) ; msg sur ih inconnu → compteurs
    inchangés.

- `crates/bitdaemon-session/src/peer_conn.rs:1724` —
  `NumIncomingExtHandshake` bumpé **avant** `ExtendedHandshake::parse`
  ; upstream en toute fin de `on_extended_handshake` — un handshake
  bdecode-invalide n'est pas compté (bt_peer_connection.cpp:2015-2027
  + 2131). Sévérité : cosmétique (compteur).
  - **sketch** : déplacer le `counters.inc` à la fin du bloc
    `if let Some(hs)` (après `eff.push(ExtHandshake)`).

- `crates/bitdaemon-session/src/torrent.rs:5005` — `on_hash_reject`
  ne poke que la conn refusante ; upstream `torrent::hashes_rejected`
  boucle **tous** les peers et appelle `maybe_send_hash_request` sur
  chaque conn v2 (torrent.cpp:7445-7456) — une conn v2 oisive reprend
  la demande au lieu d'attendre le prochain événement. Sévérité :
  état (vivacité du pipeline hash).
  - **sketch** : après `picker.hashes_rejected(&hp_req)`, iterer
    `self.peers` filtré `c.protocol_v2` → `maybe_send_hash_request(p)`
    (le gate `!protocol_v2 || len>1` interne garde l'idempotence).

- `crates/bitdaemon/src/engine.rs:3083` — échec de handshake
  **entrant** (`WrongInfoHash`, `bad handshake`, policy MSE) :
  seule la jauge `NumPeersHalfOpen` est décrémentée ; upstream a une
  `peer_connection` vivante → `disconnect()` poste
  `peer_disconnected_alert`/`peer_error_alert` +
  `disconnected_peers` + legs de kind. Sévérité : état (alertes +
  compteurs) — **frontière A07** (lifecycle), à confirmer/fixer là.
  - **sketch** : router l'`Err` de `run_handshake_responder_auto` vers
    un chemin session (`peer_handshake_failed`) qui poste alerte +
    `DisconnectedPeers` + kind-counter (`invalid_info_hash` →
    `peer_disconnected_alert`, policy → idem, `packet too large`…)
    plutôt que le seul `-1` jauge. Vérifier la sévérité upstream par
    cause avant apply.

## Fichiers du fix

- `crates/bitdaemon-net/src/peer_wire.rs` — `Malformed` pour `hashes`
  court (D4).
- `crates/bitdaemon-net/src/bt_peer_conn.rs` — param `supports_v2`
  sur `run_handshake*` + `write_bt_handshake` (D3a).
- `crates/bitdaemon-session/src/peer_conn.rs` — borne exacte
  bitfield (D5), ordre compteur ext-handshake (D7).
- `crates/bitdaemon-session/src/torrent.rs` —
  `validate_hash_request` sur `on_hash_request`/`on_hashes` (D1, D2),
  `result.valid` (D2), poke-all v2 sur `hash_reject` (D8).
- `crates/bitdaemon-session/src/session.rs` — retrait double-comptage
  (D6), `conn.protocol_v2` depuis le bit (D3b), résolution ih→clé
  (D3c/d), garde bit-v2-sur-v1-only (D3b).
- `crates/bitdaemon/src/engine.rs` — `DialDone.supports_v2` (D3b),
  `known_ihs`/`skeys` dual-hash (D3c), choix ih v2 au dial (D3d),
  route d'échec handshake entrant (D9).

## Non-bugs tranchés

- `PeerConn::handshake` (peer_conn.rs:2140) — helper **mort**
  (aucun appelant) ; s'il est ravivé il doit prendre les flags
  `dht`/`fast`/`v2` comme `write_handshake` — aujourd'hui sans effet
  observable.
- `hashes`/`hash_reject` non matchés → drop silencieux : upstream
  identique (`std::remove` sur `m_hash_requests` puis return,
  bt_peer_connection.cpp:1281-1284, 1362-1364).
- `hash_request` sur conn non-v2 → `PeerError` : le libellé Rust est
  `"hash_request on non-v2 connection"` vs `invalid_message` upstream
  — même sévérité, même teardown ; le nom d'erreur diffère
  (cosmétique). Idem `hashes`/`hash_reject`.
- `on_hash_reject` sans `validate_hash_request` : upstream ne valide
  pas non plus dans ce handler (parse + match direct,
  bt_peer_connection.cpp:1311-1368) — conforme.
- `debug_assert!(validate_hash_request)` dans
  `HashPicker::add_hashes` (hash_picker.rs:347) : injouable depuis le
  fil — `on_hashes` ne l'atteint qu'après match d'une requête qu'on a
  émise (forcément valide). Le gap est l'absence de kill, pas le
  panic.
- `num_pieces <= 0` (magnet, pas de metadata) : bitfield acceptée
  sans borne exacte — upstream saute le check sur `!valid_metadata`
  aussi (bt_peer_connection.cpp:978-979) et stocke `(packet_size-1)*8`
  bits ; conforme.
- `is_all_zeros` ih echo : upstream la refuse explicitement ; Rust la
  refuse via `hs.info_hash != *info_hash` (un ih tout-zéro ne peut
  égaler le nôtre) — sous-ensemble, équivalent.
- `PeerWireCodec` (tokio_util) n'est **pas** sur le chemin prod —
  l'engine utilise `decode_next` qui conserve les `WireError` typés ;
  le codec applicatif a quand même reçu le cap 1 MiB pour cohérence.
- Piece > `block_size` : vérifié session-side (`packet_too_large`
  peer_error), pas codec-side — upstream `on_piece` compare à
  `t->block_size()` qui n'existe pas sans contexte torrent.
- `PeerEffect::PeerError(msg)` (alerte sans disconnect) conservé :
  upstream poste des `peer_error_alert` non-fatales.

## Historique — déviations corrigées (passe 1)

| Déviation | Correction |
|---|---|
| Cap frame = 4 MiB (`read_message`) / 2 MiB (codec) | 1 MiB partout — upstream `packet_size > 1024*1024` → `packet_too_large` à la lecture du préfixe (`peer_wire.rs`, `bt_peer_conn.rs`, `PeerWireCodec::default`) |
| `piece` de corps < 9 B → erreur codec | Nouveau `PeerMessage::Malformed{id}` — upstream `on_piece` consomme le sous-header **silencieusement** (paquet compté, conn vivante). Dispatch : `M::Malformed => {}`, compté via `wire_id` → `num_incoming_piece` |
| Canal `gone` = `(ih, peer)` — l'erreur socket/codec était jetée avant teardown | `gone_tx` porte `Option<PeerGone>` : `Transport(PeerConnError)` pour EOF/reset/decode-error, `Disconnected{severity, reason}` pour la réservation session |
| `DisconnectPeer { failed: bool }` | Sévérité typée `DisconnectSeverity` (Normal/Failure/PeerError = `disconnect_severity_t`) de bout en bout |
| Alertes dupliquées (`route` postait + `peer_disconnected` repostait) | `peer_disconnected` = site unique : `peer_error` → `peer_error_alert` + `error_peers` (jamais `peer_disconnected_alert` — mutuellement exclusif upstream) ; `normal`/`failure` → `peer_disconnected_alert`. L'écho `gone` post-teardown retombe sur `had == false` → no-op |
| Compteurs `disconnect()` jamais incrémentés | `disconnected_peers` à chaque close ; classification io-kind (`eof/connreset/connrefused/connaborted/notconnected/perm/broken_pipe/addrinuse/transport_timeout`) ; par-nom (`disconnect_reason_counter` : uninteresting = self_connection/upload-upload/torrent_paused/session_closing, too_many_peers, no_memory_peers, timeouts) ; `sev > normal` → `error_tcp`/`error_utp` + `error_outgoing`/`error_incoming` |
| `BadLength` non compté | Frame complète rejetée → `num_incoming_*` bumpée AVANT l'alerte (upstream compte dès `packet_finished`, le disconnect vit dans le handler). `UnknownId` → **pas** de bump (le `default:` upstream return avant le bloc compteur) |
| Sévérités erronées | `self_connection`, `too_many_corrupt_pieces`, `banned_by_ip/port_filter`, dup peer-id, too_many_connections, `no_memory`, `too_frequent_pex` (ut_pex **et** i2p_pex sans arg de sévérité = normal), `peer_banned` smart_ban → **Normal** ; `pex_message_too_large`, `invalid_*`, `packet_too_large`, hash-msg sur conn non-v2 → **PeerError** |
| `peer_error_effect` postait l'alerte + DisconnectPeer (double post) | Ne pousse plus que le Disconnect ; l'alerte part de `peer_disconnected` avec `reason` = nom d'erreur upstream |
| FAST msgs non gatés | `RejectRequest`/`AllowedFast`/`SuggestPiece`/`HaveAll`/`HaveNone` exigent `supports_fast` (handshake reserved bit) — sinon `invalid_*` peer_error |
| `on_choke` sans FAST ne rejetait pas la dlq | Leg upstream : chaque entrée `download_queue` passe par `incoming_reject_request(to_req(block))` (abort picker + `outstanding_bytes` restauré). Avec FAST : la queue stalle simplement |
| `DhtPort` gaté sur `supports_dht`, pas de réponse | `incoming_dht_port` = `add_dht_node` **inconditionnel** (`ping_only`) + flip `supports_dht` + `write_dht_port()` si `sent_bitfield` |
| `m_sent_bitfield` jamais posé | Stampé à l'attach quand `valid_metadata` — y compris les legs have_all/have_none/super-seed qui n'émettent pas de bitfield (gate de la réponse DHT) |
| `disconnect_if_redundant` absent post-ext-handshake | Appelé à `PeerEffect::ExtHandshake` (upload_only/share_mode arrivent dans le handshake) + `RemoteUploadOnly` |
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
