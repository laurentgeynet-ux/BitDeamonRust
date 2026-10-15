# Audit A09 — rapport d'analyse

Périmètre : Request entrant → validation → upload queue → read disque →
Piece ; drain on choke, rejects, compteurs. Upstream de référence :
`peer_connection::incoming_request` / `fill_send_buffer` /
`write_reject_request` / `send_choke` / `incoming_cancel` /
`on_disk_read_complete` (`src/peer_connection.cpp` @ e7381c7bc,
bt_peer_connection.cpp pour les émissions wire).

## Verdicts checklist

- `incoming_request` : validation piece/start/len, reject vs accept —
  verdict : **déviation** — preuve : la validation shape
  (piece ∈ [0,num_pieces), 0 ≤ start < piece_size, 0 < length,
  length + start ≤ piece_size, length ≤ block_size = 16 KiB) est
  conforme dans `peer_conn.rs:885-903` (`validate_request`) ;
  `piece_requests`, queue-full (`max_piece_requests`,
  `peer_conn.rs:1395-1399`), pas-de-metadata (`peer_conn.rs:1378-1382`),
  choked/allowed-fast et les escalades (3×blocks_per_piece,
  tolérance 2 s, 10e rappel / >300 disconnect, `peer_conn.rs:1441-1470`,
  `1424-1434`) matchent `incoming_request` (peer_connection.cpp:2388-2641).
  **Mais** la jambe « pièce non détenue » (upstream : même branche
  `invalid` — `invalid_piece_requests` + `m_num_invalid_requests++` +
  rappel-choke/disconnect) est rejetée côté torrent sans ces compteurs
  (`torrent.rs:4517-4540`) → déviation D2 ; ordre `incoming_have_none`
  vs super-seed → déviation D6.
- Upload queue : FIFO, choked peers non servis, `fill_send_buffer`
  ordering — verdict : **déviation** — preuve : l'itération complète de
  la queue avec hold prédictif, gate seed-mode (`verifying` /
  `outstanding_piece_verification ≥ 3`), erase-par-index et la
  décrémentation `num_peers_up_requests` à vide sont conformes
  (`torrent.rs:4215-4283` vs peer_connection.cpp:5319-5420), le tail
  share-mode aussi (`torrent.rs:4287` vs :5424). La quantité du
  watermark diffère : upstream `m_uploaded_last_second` =
  `last_payload_uploaded()` (payload de la dernière seconde,
  peer_connection.cpp:5278), Rust `rate_up_speed` = EMA 5 s tout-canal
  (`torrent.rs:4208`, `9768-9796`) → déviation D5.
- Drain on choke : requests en vol annulées + `Rejected` wire +
  `clear_request_queue` — verdict : **déviation** — preuve : le drain
  de `upload_queue` en gardant `accept_fast`, `choked_piece_requests`,
  `piece_rejects`, `num_peers_up_requests`, `num_invalid_requests = 0`,
  `last_choke`, optimistic-clear, `num_peers_up_unchoked[_all]` sont
  conformes (`torrent.rs:10826-10892` vs peer_connection.cpp:3822-3876).
  **Mais** upstream émet `write_choke()` *avant* la boucle de rejects ;
  Rust pousse `Choke` après les `RejectRequest` → ordre wire inversé,
  déviation D1. (Les reads disque en vol ne sont pas annulés — conforme,
  upstream non plus.)
- `DontHave`/`Have`/`Rejected` wire encodings — bit-level — verdict :
  **conforme** — preuve : `peer_wire.rs:456-466` (DontHave : len=6,
  msg_extended, ext_id, i32 ≡ bt_peer_connection.cpp:2640-2643 ;
  RejectRequest = `write_request` id 0x10, 17 B total ≡
  `send_message(msg_reject_request)` bt_peer_connection.cpp:403) ;
  Have = id 4 + i32 (`peer_wire.rs:419`, `wire_len` = 5/9/17 à
  `peer_wire.rs:510-515`). `write_dont_have`/`write_reject_request`
  gatés sur `ext_dont_have != 0` / `supports_fast`
  (`torrent.rs:4273-4278`, `6866-6874`, `session.rs:1608-1621` ≡
  bt_peer_connection.cpp:2638, :395).
- Compteurs upload : `num_uploads`, `upload_payload`, read-quotas par
  canal — verdict : **déviation** — preuve : `num_uploads` recalculé en
  excluant `ignore_unchoke_slots` (`torrent.rs:9576-9579`, `10908-10915`),
  `upload_payload` via comptabilisation `m_payloads`-style à
  `on_peer_sent` (`session.rs:8461-8474`, `engine.rs:4326-4392`),
  `num_outgoing_piece`/`num_outgoing_reject` via `count_outgoing`
  (`session.rs:8294-8333`), `num_peers_up_disk` + alerte
  `send_buffer_watermark_too_low` + gate quota conformes à `setup_send`
  (`torrent.rs:4299-4330` vs peer_connection.cpp:5851-5891),
  `request_latency` blend 5 % (`session.rs:1661-1665` vs :5661),
  `disk_read_failures` >100 → disconnect (`session.rs:1634-1651` vs
  :5628-5629). Déviations : `invalid_piece_requests` manquant sur la
  jambe not-have (D2) et `block_uploaded_alert.block` = offset octet au
  lieu de l'index de bloc (D3).
- fast-extension : allowed_fast requests servis même choked — verdict :
  **conforme** — preuve : `peer_conn.rs:1441-1470` (`accept_fast` ≡
  `m_accept_fast`, incrément `accept_fast_piece_cnt`, disconnect à
  `3 * blocks_per_piece`) et le drain de choke qui conserve les pièces
  `accept_fast` (`torrent.rs:10860-10877` ≡ peer_connection.cpp:3859-3874).

## Déviations

- `crates/bitdaemon-session/src/torrent.rs:10860-10891` (`choke_peer`) —
  les `TorrentEffect::SendTo{RejectRequest}` sont poussés pendant le
  drain puis `Choke` en dernier ; upstream `send_choke` émet
  `write_choke()` *avant* la boucle de rejects
  (src/peer_connection.cpp:3848 puis :3859-3874). Ordre wire inversé —
  sévérité : **wire-visible**.
  - **sketch** : dans `choke_peer`, déplacer le `out.push(SendTo{Choke})`
    (aujourd'hui lignes 10887-10890) juste après `c.choked = true` /
    `c.last_choke_ms = c.now_ms` (avant `let mut keep`), en gardant les
    décréments `NumPeersUpUnchoked*` où ils sont (compteurs — ordre
    interne libre). Réf. src/peer_connection.cpp:3848. Test : peer FAST
    avec 2 requêtes upload queued → `choke_peer` → effets sortants dans
    l'ordre `[Choke, RejectRequest, RejectRequest]`.
- `crates/bitdaemon-session/src/torrent.rs:4517-4540` (branche else de
  `ReadRequest`) — une requête de forme valide pour une pièce que nous
  n'avons pas (ni prédictive ni seed_mode) n'incrémente ni
  `invalid_piece_requests` ni `c.num_invalid_requests`, et ne déclenche
  ni le rappel `Choke` tous les 10 ni le disconnect >300. Upstream, la
  condition `!user_have_piece && !is_predictive_piece && !seed_mode`
  est dans la *même* branche « invalid request » que la forme
  (src/peer_connection.cpp:2519-2582). Sévérité : **état + compteurs +
  escalation** (le disconnect >300 est observable côté conn).
  - **sketch** : dans le `else` de `torrent.rs:4517`, après
    `counters.add(PieceRejects)` ajouter :
    `counters.inc(InvalidPieceRequests)` ; dans le `get_mut` :
    `c.num_invalid_requests += 1;` puis la machinerie upstream :
    `if !c.peer_interested && c.num_invalid_requests % 10 == 0 && c.choked
    { if c.num_invalid_requests > 300 && !c.peer_choked {
    out.push(DisconnectPeer{.., sev: PeerError}) } else {
    c.last_choke_ms = c.now_ms; out.push(SendTo{Choke}) } }` — calqué
    sur `peer_conn.rs:1424-1434`. Réf. src/peer_connection.cpp:2562-2579.
    Test : peer unchoked requérant une pièce absente →
    `invalid_piece_requests` +1 et `num_invalid_requests` incrémenté ;
    après 10 requêtes invalides en état choked → `Choke` émis.
- `crates/bitdaemon-session/src/session.rs:1691-1696` — `Alert::BlockUploaded
  { block: req.start as u32 }` passe l'offset octet ; upstream passe
  `r.start / t->block_size()` (index de bloc dans la pièce,
  bt_peer_connection.cpp:2693). Sévérité : **état** (payload d'alerte
  consommé par downstream).
  - **sketch** : `block: (req.start.max(0) / t.block_size().max(1)) as
    u32` — le `t` est déjà résolu dans la portée pour `remote` ;
    calculer `let blk = …` avant `alerts.post`. Réf.
    bt_peer_connection.cpp:2690-2694. Test : read complétée sur
    start = 2×block_size → `block == 2`.
- `crates/bitdaemon-session/src/session.rs:1671-1697`
  (`DiskCompleted::ReadPiece`, branche succès) — upstream
  `on_disk_read_complete` rend tôt si `m_disconnecting` avant
  `write_piece` (src/peer_connection.cpp:5647) : une conn en teardown ne
  reçoit pas le bloc ni l'alerte. Rust pousse `Piece` + `BlockUploaded`
  sans vérifier `c.disconnecting`. Sévérité : **wire-visible mineure**
  (octets envoyés à une conn mourante).
  - **sketch** : mémoriser `let gone = t.peers.get(&peer).is_some_and
    (|c| c.disconnecting)` dans la branche `if let Some(t)` existante,
    puis n'exécuter le bloc `if let Some(data)…push_to_peer +
    BlockUploaded` que si `!gone` (le `reading_bytes` decrement, le
    reset `disk_read_failures`, `request_latency` et
    `add_suggest_piece` restent inconditionnels — upstream les exécute
    avant le gate). Réf. src/peer_connection.cpp:5647-5670. Test :
    marquer `c.disconnecting = true`, compléter une ReadPiece → aucun
    `ToPeer{Piece}` ni `BlockUploaded` émis.
- `crates/bitdaemon-session/src/torrent.rs:4208-4211` (`fill_send_buffer`)
  — le watermark dynamique utilise `c.rate_up_speed` (EMA 5 s
  payload+protocole+IP, calculé `torrent.rs:9779-9796`) ; upstream
  `buffer_size_watermark = m_uploaded_last_second * factor / 100` où
  `m_uploaded_last_second = m_statistics.last_payload_uploaded()` =
  octets *payload* du dernier `second_tick`
  (src/peer_connection.cpp:5114, :5278-5288). Sévérité : **état/perf**
  (dimensionne combien de reads sont admis — ni EMA ni protocole/IP
  upstream).
  - **sketch** : ajouter `uploaded_last_second: i64` à `PeerConn`
    (init 0) ; dans la boucle de rates (`torrent.rs:9770` sq.) :
    `c.uploaded_last_second = (c.stat_up_total - c.last_up_total).max(0)`
    (payload du dernier intervalle, ≡ `last_payload_uploaded()`) ;
    remplacer `c.rate_up_speed` par `c.uploaded_last_second` dans le
    calcul du watermark ligne 4208. Réf. src/peer_connection.cpp:5114.
    Test : forcer `stat_up_total`/`last_up_total`, vérifier que le
    watermark suit le delta payload et pas l'EMA.
- `crates/bitdaemon-session/src/peer_conn.rs:1371-1374` +
  `torrent.rs:4446-4478` — `on_have_none()` + `has_metadata = true`
  s'exécutent pour *toute* requête avant le rejet super-seed ; upstream
  le gate super-seed (`piece not super-seeded`) précède
  `incoming_have_none` (src/peer_connection.cpp:2408-2447 vs :2447), si
  bien qu'une requête rejetée-super-seed ne ranime pas `m_has_metadata`
  (visible quand le pair avait annoncé `metadata_size` invalide,
  `set_has_metadata(false)` à peer_conn.rs:1766). Sévérité :
  **état mineure**.
  - **sketch** : passer `super_seeding: bool` à `PeerConn::dispatch` et
    déplacer la branche super-seed de `torrent.rs:4446-4478` dans le
    `M::Request` de `peer_conn.rs` *avant* la jambe
    `!bitfield_received` (le test `c.superseed_pieces` est déjà sur la
    conn — `torrent.rs:4452`) ; la branche émet `InvalidPieceRequests` +
    `num_invalid_requests += 1` + `RejectRequest` + `InvalidRequest`
    puis `return`, comme upstream :2412-2441. Le `on_have_none` reste
    ensuite à sa place pour tous les autres chemins. Réf.
    src/peer_connection.cpp:2408-2447. Test : super_seeding + requête
    hors slots leaked → reject + alerte, `has_metadata` inchangé.

## Fichiers du fix

- `crates/bitdaemon-session/src/torrent.rs` (choke_peer, ReadRequest,
  fill_send_buffer, boucle de rates)
- `crates/bitdaemon-session/src/peer_conn.rs` (ordre super-seed /
  have_none, champ `uploaded_last_second`, `dispatch` signature)
- `crates/bitdaemon-session/src/session.rs` (BlockUploaded.block,
  disconnecting-gate sur ReadPiece)
- `crates/bitdaemon-session/tests/` (régressions par déviation)

Recouvrements probables avec les audits V4 voisins : `torrent.rs` +
`peer_conn.rs` + `session.rs` sont aussi dans le périmètre A07/A08/A10 —
merges séquentiels requis.

## Non-bugs tranchés

- **Pas d'exemption `allowed_fast` sur la longueur** : la checklist
  mentionne « len ≤ 32 K sauf allowed_fast » — upstream n'a aucune
  exemption de taille ; `r.length > t->block_size()` (16 KiB) rejette
  même pour une pièce allowed-fast (peer_connection.cpp:2530). La
  seule exemption BEP-6 porte sur l'état *choked*. Rust conforme.
- **Plugin `on_request`/`on_cancel` non-consommant** : upstream permet
  à une extension d'avaler la requête (`e->on_request(r) → return`,
  peer_connection.cpp:2451-2454, :3320-3323). Le port a scopé
  `PeerPlugin` en observation seule (`alerts.rs:1881-1896`) — écart
  volontaire documenté.
- **`is_deleted` → drain avec rejects** : upstream vide `m_requests`
  avec `write_reject_request` quand le storage est supprimé
  (peer_connection.cpp:5303-5312, commentaire « can this happen
  here? ») ; Rust supprime la table `peers` à la suppression du torrent
  — effet net équivalent, upstream lui-même doute de l'atteignabilité.
- **`!t` à la complétion disque** : upstream déconnecte le peer si le
  torrent a disparu entre l'émission et la complétion du read
  (peer_connection.cpp:5615-5619, :5649-5653). En Rust un peer ne
  survit pas à son torrent — `peers.get(&peer)` est `None` — le cas est
  structurellement impossible.
- **`add_suggest_piece` appelé deux fois** upstream
  (peer_connection.cpp:5638-5645 et :5666-5669) ; Rust une fois, le
  mode `suggest_read_cache` vérifié dans `add_suggest_piece`
  (`torrent.rs:10165`) — le double appel upstream est une bizarrerie
  sans effet observable (`add_piece` sur le même index).
- **Commentaire « once per crossing »** dans `update_up_disk_gate`
  (`torrent.rs:4296-4297`) inexact : upstream *et* Rust postent
  l'alerte `send_buffer_watermark_too_low` à chaque appel où les
  conditions tiennent (peer_connection.cpp:5864-5885). Code conforme,
  commentaire à corriger au passage dans le fix.
- **`file_error_alert` sans champ `op`** : upstream embarque
  `error.operation` ; l'`Alert::FileError` Rust porte ih/path/error
  (session.rs:1629-1633) — surface d'alerte déjà scopée.
