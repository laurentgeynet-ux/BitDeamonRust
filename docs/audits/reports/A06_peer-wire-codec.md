# Audit A06 — Codec peer-wire & framing

Baseline upstream : `e7381c7bc` (`src/bt_peer_connection.cpp`,
`src/peer_connection.cpp`, `peer_connection_interface.hpp`).
Mode : direct sur `main` (audit séquentiel lancé avant le protocole
worktree ; corrections validées par les gates workspace).

## Déviations corrigées

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

## Non-bugs tranchés

- `PeerWireCodec` (tokio_util) n'est **pas** sur le chemin prod —
  l'engine utilise `decode_next` qui conserve les `WireError` typés ;
  le codec applicatif a quand même reçu le cap 1 MiB pour cohérence.
- Piece > `block_size` : vérifié session-side (`packet_too_large`
  peer_error), pas codec-side — upstream `on_piece` compare à
  `t->block_size()` qui n'existe pas sans torrent contexte.
- `malformed` encode une frame `len=1 id` — le variant n'est jamais
  émis (decode-only) ; l'encode existe pour la complétude de
  `wire_len`.
- `PeerEffect::PeerError(msg)` (alerte sans disconnect) conservé :
  upstream poste des `peer_error_alert` non-fatales (empty piece
  BitComet, …).

## Tests de régression ajoutés

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
