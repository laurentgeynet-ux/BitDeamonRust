# Audit A10 — rapport d'analyse

Périmètre : incoming_piece → flush → hash → piece_verified → Have →
hash_failed → restore. Références upstream : `src/torrent.cpp`
(`on_piece_verified` 4496, `we_have` 4630, `piece_passed` 4890,
`piece_failed` 4996, `penalize_peers` 5133, `on_piece_sync` 5224,
`on_piece_fail_sync` 1235, `on_piece_hashed` 2674,
`on_blocks_hashed` 4748) et `src/peer_connection.cpp`
(`received_piece` 963, `announce_piece` 1000,
`on_disk_write_complete` 3169), baseline `e7381c7bc`.

## Verdicts checklist

- `picker_gen` gates write ET hash completions — in-flight invalidé par
  force_recheck/inconsistent — verdict : **conforme** — preuve :
  `session.rs:2378` (stamp à l'émission du write), `session.rs:1736-1742`
  (gate avant succès *et* erreur, comme
  `peer_connection.cpp:3214`), `session.rs:1866`/`2546` (stamp hash
  verify + checking), `session.rs:2133-2146` (gate hash, comme
  `torrent.cpp:4513` et `2690`), bumps `torrent.rs:8194`
  (force_recheck) et `torrent.rs:10038` (handle_inconsistent_hashes) —
  upstream `++m_picker_generation` aux deux mêmes sites
  (`torrent.cpp:1226`, `2507`).
- hash-fail : `lock_piece` + `ClearPiece` fence → `on_piece_sync`
  différé (restore filtré v2 blocks, hash_failed_alert post-restore,
  re-mark duplicates) — verdict : **conforme** — preuve :
  `torrent.rs:6973-6979` (`lock_piece` puis effet `ClearPiece`, ordre
  upstream `torrent.cpp:5113-5120`), `session.rs:8554-8586`
  (`async_clear_piece` → `DiskCompleted::PieceSync` → `on_piece_sync`,
  leg `!has_storage` inline), `torrent.rs:7193-7248` (`restore_piece`
  avec `blocks` filtrant — `piece_picker.rs:1836` — puis effet
  `PieceFailed` routé en `Alert::HashFailed` `session.rs:8550`, puis
  re-mark des blocs `download_queue` ∪ `request_queue` filtrés par
  `blocks` avec `picker_options` parole/snubbed `torrent.rs:7234-7243`
  — `torrent.cpp:5244-5266`). Écart anodin : gate `aborted` en plus
  (`torrent.rs:7201`), voir Non-bugs.
- write-fail vs hash-fail : `on_piece_fail_sync` séparé (restore entier
  + `hash_fail_piece_picks`) — verdict : **conforme** — preuve :
  `session.rs:1743-1802` (aborted→`mark_as_canceled` ; sinon
  `cancel_block` + `write_failed` + `async_clear_piece` →
  `DiskCompleted::ClearPiece` → `on_piece_fail_sync`, inline sans
  storage — `peer_connection.cpp:3217-3258`), `torrent.rs:9190-9220`
  (`restore_piece` pièce entière, `state_updated` ≈ `update_gauge`,
  `update_interest` sur les non-interesting + `drive_peer` dont le
  retour compte `HashFailPiecePicks` — `torrent.cpp:1245-1267`).
- `found_on_disk` : pas de blame (failed_bytes/ext/penalize) mais
  restore/fence conservés — verdict : **conforme** — preuve :
  `torrent.rs:6906` (`set.len()==1 && contains(&None)` —
  `torrent.cpp:5066`), tout le bloc blame (verify_block_hashes v2,
  `failed_bytes`, `on_piece_failed` extensions, smart_ban,
  `penalize_peers`) sous `if !found_on_disk` `torrent.rs:6907-6965`,
  `lock_piece`+`ClearPiece` hors du leg `torrent.rs:6967-6979` —
  `torrent.cpp:5068-5130`.
- `piece_passed` vs `we_have` split : pads/checking/fastresume passent
  par we_have direct — verdict : **conforme** — preuve : checking
  `torrent.rs:6741-6752` (`piece_flushed` + `need_save_resume` +
  `we_have`, miss → return sans piece_failed —
  `torrent.cpp:2780-2790`) ; pads `torrent.rs:1151-1158`,
  `torrent.rs:2565-2570` (`init_picker` retourne les padded →
  `we_have` direct, upstream `apply_pad_files` → `we_have`) ;
  adjacent-v2 `torrent.rs:7456-7464` (`piece_flushed` + `we_have`,
  pas de `piece_passed`/counters — `torrent.cpp:4862-4874`) ; resume
  via `we_have(loading_resume=true)` `torrent.rs:6996`.
- penalize_peers : parole conn+peer_list, trust −2 min −7,
  `banned_for_hash_failure`, `update_want_peers`, disconnect
  `failed:true` — verdict : **déviation (partielle)** — preuve :
  `torrent.rs:4093-4182` — parole posée sur la conn *et* le swarm
  entry (4141-4153), `trust_points -2` plancher −7 (4156),
  `hashfails: u8` saturant 255 (`torrent_peer.rs:123`),
  `Counter::BannedForHashFailure` (4162), `update_want_peers` (4165),
  `PeerBan` alert + `DisconnectPeer` reason "too many corrupt pieces"
  severity Normal→failcount (4166-4179) — `torrent.cpp:5152-5200`.
  **MAIS** `conn_by_key` ne résout que via les conns vivantes
  (4135) : un culprit dont la conn est déjà morte est `continue`d —
  upstream pénalise le `torrent_peer` avec `p->connection==nullptr`
  → `allow_disconnect` reste true → ban sur `known_bad_peer`. Voir
  Déviations #1.
- Compteurs : `num_piece_passed`/`num_piece_failed`/`num_have_pieces`
  aux sites exacts — verdict : **conforme** — preuve :
  `NumPiecePassed` `torrent.rs:6775` (après `need_save_resume`, avant
  suggest/rewards — `torrent.cpp:4906-4908`) ; `NumPieceFailed`
  `torrent.rs:6836` en tête de `piece_failed`, avant la rétraction
  prédictive (`torrent.cpp:5011-5030`) ; `NumHavePieces` inconditionnel
  en tête de `we_have` `torrent.rs:7003` (`torrent.cpp:4635`).
  Bonus vérifiés : `PieceRejects`/`NumPeersUpRequests` dans la
  rétraction prédictive `torrent.rs:6841-6862`, `DontHave` BEP-10
  gaté `ext_dont_have` `torrent.rs:6866` (≈ `write_dont_have`).
- `file_completed` edges par fichier, `m_last_download` stampé au edge
  vérifié — verdict : **conforme** — preuve :
  `file_completed_edges` `torrent.rs:8431-8453` appelé dans `we_have`
  après `piece_finished_alert` (7113-7116 — `torrent.cpp:4704-4717`),
  `files_completed` pré-marque pads/fichiers vides (8395-8411),
  re-armement sur `mark_lost_piece` (8459-8480). `last_payload_down`
  stampé `torrent.rs:7179` après le gate non-downloading
  (`checking || checking_resume` → return 7130) —
  `torrent.cpp:4723-4738`, `is_downloading_state` `torrent.cpp:121-138`
  (finished/seeding inclus → stamp conservé, checking exclu).

## Déviations

- `torrent.rs:4135` / `torrent.rs:4069` — clés downloader conn-dérivées
  non résolubles sans conn vivante — sévérité : **état** (trust/ban
  jamais appliqués à un culprit déconnecté ; récompense +1/parole-off
  perdue aussi — upstream `torrent.cpp:5136-5203` boucle sur les
  `torrent_peer*` qui survivent aux conns ; `p->connection==nullptr`
  ne désarme que l'appel `received_invalid_data`, pas la pénalité).
  - **sketch** : stocker l'adresse du downloader à côté de la clé dans
    `block_info.peer` (ex. `get_downloaders` retourne
    `Option<(u64, SocketAddr)>` ou table `key→remote` tenue sur le
    torrent, alimentée à `mark_as_downloading`/`mark_as_finished`
    depuis `c.remote`). Dans `penalize_peers`, quand `conn_by_key`
    échoue, résoudre `peer_index(remote)` directement : appliquer
    `on_parole` (parole mode) + `trust_points -2` plancher −7 +
    `hashfails` saturant, et si `trust <= -7 || known_bad` →
    `ban_peer` + `update_want_peers` + `BannedForHashFailure` +
    alerte (pas de `DisconnectPeer`, conn déjà morte — upstream
    `if (p->connection)` gate le disconnect, pas le ban,
    `torrent.cpp:5189-5201`). Même résolution dans
    `reward_downloader` (remplacer le `return` documenté). Test :
    télécharger une pièce via 1 pair, le déconnecter, forcer le hash
    fail → swarm entry `trust_points == -2` / `banned` si known_bad.
- `alerts.rs:345-350` — `Alert::PeerBan` ne porte pas `pid` —
  upstream `peer_ban_alert(handle, ip, pid)` (`torrent.cpp:5177-5181`
  passe `p->connection ? p->connection->pid() : peer_id()`) —
  sévérité : **cosmétique** (payload alerte/API).
  - **sketch** : ajouter `pid: Option<bitdaemon_common::PeerId>` à
    `Alert::PeerBan`, le remplir dans `penalize_peers` depuis
    `self.peers.get(&id).and_then(|c| c.peer_id)` (champ conn à
    vérifier — `their_peer_id` ou équivalent ; `None` si handshake
    incomplet), `Some(pid)`/`None` aux autres émetteurs
    (`session.rs:4670`). Test : ban sur hash-fail → alerte porte le
    pid du pair.

## Fichiers du fix

- `crates/bitdaemon-session/src/torrent.rs` (`penalize_peers`,
  `reward_downloader`, résolution clé→swarm entry, `get_downloaders`
  call-sites dans `piece_verified_impl`/`piece_failed`)
- `crates/bitdaemon-session/src/piece_picker.rs` (si
  `get_downloaders`/`block_info.peer` s'enrichit d'une adresse)
- `crates/bitdaemon-session/src/alerts.rs` (`PeerBan.pid`)
- `crates/bitdaemon-session/src/session.rs` (émetteur `PeerBan` 4670)
- `docs/PORTING.md` (rows torrent.cpp / peer_connection.cpp)

## Non-bugs tranchés

- `on_piece_sync` ajoute un early-return `aborted` absent upstream
  (`torrent.cpp:5224` ne teste que `!has_picker()`) — un torrent
  avorté ignore la restore ; équivalent fonctionnel, l'effet est
  destructeur de toute façon.
- `update_gauge()` upstream n'a pas d'équivalent 1:1 — les compteurs
  `num_have`/have-all sont dérivés du bitfield `have` chez nous
  (`state_updated` marque le dirty) ; sites `update_gauge`
  (`torrent.cpp:4856`, `4872`, `4955`, `5155`→`on_piece_fail_sync`
  `1248`) couverts par `state_updated` ou rien à recomputer.
- `peer_connection::received_valid_data` dans le leg pass — upstream
  n'y fait que des hooks plugin (`torrent.cpp:4946-4951`) ; nos hooks
  vivent déjà dans `m_extensions.on_piece_pass` (commentaire
  `reward_downloader` 4065-4067).
- Double `remove_time_critical_piece` upstream (`piece_passed` fin +
  `we_have`) — le second appel est un no-op par construction ; le
  nôtre unique dans `we_have` (7119-7125) suffit.
- `maybe_done_flushing` — upstream libère picker/hash_picker/
  file_progress en seeding-complet depuis le write path
  (`peer_connection.cpp:3271`) ; choix documenté de garder le picker
  (`torrent.rs:7166-7176`), reset de `hash_picker_state` = analogue.
- Web seeds dans `penalize_peers` — analogue documenté : ban direct
  `web_seed_state.disabled` quand `known_bad && ban_web_seeds`
  (`torrent.rs:4103-4130`) vs `received_invalid_data` upstream ;
  écart assumé (pas de bitfields per-file pour les seeds).
- `is_disconnecting` matérialisé par scan des effets `DisconnectPeer`
  émis (`torrent.rs:7021-7025`, `7045-7052`) — équivalent à l'early
  `continue` upstream (`torrent.cpp:4663`).
- `seed_mode → verified(piece)` après `we_have` upstream vs avant chez
  nous (`torrent.rs:6806-6811`) — marque `m_verified`/compteur seed
  sans effet filaire ; ordre non observable.
- Picker `None` hors-seeding dans `piece_verified_impl` : upstream
  `need_picker()` recrée ; chez nous tous les jobs stale sont déjà
  dropés par `picker_gen` (inconsistent/force_recheck bumpent en
  même temps qu'ils dropent le picker) — inatteignable en pratique.
