# Audit A12 — rapport d'analyse

Périmètre : `set_piece_deadline`, `request_time_critical_pieces`,
`make_time_critical`, `cancel_non_critical`, expiration de deadlines.
Baseline upstream `e7381c7bc` (`D:/Projet/libtorrent/libtorrent`).

## Verdicts checklist

- Deadlines : insertion, watch via ticker, alert expiry → recancel —
  verdict : **déviation** — preuve : insertion conforme
  (`torrent.rs:8600-8675`), tick présent (`session.rs:6748-6752`) mais
  `request_time_critical_pieces` est un stub (D1) ; pas de suivi
  `first_requested`/`last_requested` → aucun re-pick sur stall ;
  pas de `read_piece_alert` annulé sur `clear_time_critical` (D4).
- `queued_time_critical` : compteur de préfixe exact
  (insert/remove/choke-clear/shift) — verdict : **conforme** — preuve :
  insert en préfixe `peer_conn.rs:2001-2004`, remove avec décrément
  `peer_conn.rs:2031-2033` (cancel_request) et `:1059-1061`
  (migration request→download), clear sur choke `peer_conn.rs:891`,
  clear sur snub `torrent.rs:9024`, shift dans
  `send_block_requests` `peer_pipeline.rs:348-351,384-386` dont le
  bypass `|| queued_time_critical > 0` reproduit
  `peer_connection.cpp:4073-4078` ; contribution à
  `download_queue_time` `peer_conn.rs:2321` =
  `peer_connection.cpp:1105`.
- `make_time_critical` : déplacement vers le préfixe, refus si déjà
  critique ou absent — verdict : **conforme** — preuve :
  `peer_conn.rs:2048-2066` =
  `peer_connection.cpp:3590-3611` (find, refuse si
  `pos < queued_time_critical`, erase + insert à
  `queued_time_critical`, `saturating_add`).
- `cancel_non_critical` = `cancel_request(force=true)` : not_wanted
  marking, wire Cancel conditionnel, abort picker — les deux files —
  verdict : **déviation (mineure)** — preuve : `torrent.rs:9148-9211`
  reproduit le marking `not_wanted`, le Cancel conditionnel
  `outstanding_bytes >= len` (`peer_connection.cpp:3803-3814`) et
  l'`abort_download` — mais sans l'early-out
  `!picker().is_requested(block)` upstream (`peer_connection.cpp:3780`)
  — voir D5. Par ailleurs upstream *diffère* l'appel via `post()`
  (`torrent.cpp:5671-5678`) ; Rust l'exécute de façon synchrone (D2).
- Priorité picking : les blocs critiques passent devant le pick normal —
  verdict : **déviation** — preuve : `add_request(b, busy,
  time_critical)` n'est *jamais* appelé avec `time_critical=true`
  (seuls appelants `torrent.rs:9087`, `peer_conn.rs:2198` passent
  `false`) ; seuls les blocs *déjà* en file passent en préfixe via
  `make_time_critical` ; les nouveaux picks de pièces critiques vont
  en queue normale — voir D1.
- watch-timeouts vs piece timeout : indépendance — verdict :
  **conforme** — preuve : `torrent.rs:8997-9002` évalue
  `request_timed_out` (gated `reading_socket`, =
  `peer_connection.cpp:4984` env.) et `piece_timed_out`
  indépendamment avant le snub.

## Déviations

### D1 — `request_time_critical_pieces` est un stub

- `torrent.rs:10856-10869` — Rust se contente d'appeler `drive_peer`
  sur chaque pair chaque tick. Upstream
  (`torrent.cpp:11550-11720` + `pick_time_critical_block`
  `11360-11546`) implémente un algorithme dédié :
  1. filtre `can_request_time_critical()` (`peer_connection.cpp:3572-3588` :
     `!has_peer_choked && is_interesting && queues ≤ 2×desired &&
     !on_parole && !disconnecting && !upload_mode && !snubbed`) — la
     fonction n'existe pas en Rust ;
  2. trie les pairs par `download_queue_time`, élimine le dernier
     décile (`11573-11581`) ;
  3. itère `m_time_critical_pieces` **trié par deadline** (Rust :
     `BTreeMap<PieceIndex>` → ordre par index de pièce, `torrent.rs:693`) ;
  4. horizon : `break` si `deadline > now + avg_piece_time +
     4×deviation + 1000` sauf la première pièce (`11619-11631`) ;
  5. pièce saturée (`free_to_request == 0`) : `timed_out =
     (now - last_requested) / (avg + dev/2)` → re-pick de blocs
     *busy* via `pick_busy_blocks` (`11645-11697`) — aucun suivi
     `first_requested`/`last_requested` ni
     `m_average_piece_time`/`m_piece_time_deviation` en Rust
     (`grep average_piece_time` → 0 résultat hors de ce rapport) ;
  6. `add_request(b, time_critical | busy?)` — les nouveaux blocs
     critiques entrent en préfixe ; en Rust le flag
     `time_critical` n'est jamais passé `true` ;
  7. `peers_with_requests` : envoi batché à la fin (`11700-11710`).
  Sévérité : **état + wire-visible** (un pair choked/snubbed peut se
  voir demander des blocs critiques qu'upstream exclut ; les pièces
  critiques bloquées ne sont jamais re-demandées — régression de
  latence streaming).
  - **sketch** :
    a. Structurer `time_critical` comme une liste triée par deadline :
       remplacer `BTreeMap<PieceIndex, (Instant, bool)>` par une petite
       struct `TimeCriticalPiece { piece, deadline, alert, first_requested:
       Option<Instant>, last_requested: Option<Instant> }` dans un
       `Vec` trié par `deadline` (insertion par `upper_bound` comme
       `torrent.cpp:5719-5721`), ou conserver le `BTreeMap` pour la
       lookup + un `Vec` trié reconstruit au tick.
    b. Ajouter `m_average_piece_time`/`m_piece_time_deviation` (ms) sur
       `Torrent` ; les mettre à jour dans la branche finish de
       `on_piece_verified` (`torrent.rs:7166-7172`) en reprenant la
       formule `torrent.cpp:5777-5790` (`avg = (avg*9 + dl)/10`,
       `dev = (dev*9 + |dl-avg|)/10`), seulement si
       `first_requested.is_some()`.
    c. Ajouter `PeerConn::can_request_time_critical(&self, t_upload_only,
       desired_queue_size)` reprenant `peer_connection.cpp:3572-3588`.
    d. Réécrire `request_time_critical_pieces` : construire le `Vec` de
       pairs filtrés, trier par `download_queue_time(16*1024)`
       (`peer_conn.rs:2321` existe), tronquer à `(n*9+9)/10`, iter par
       deadline croissante avec l'horizon `11619`, appeler un
       `pick_time_critical_block` porté (boucle `do` : choisir le pair
       le plus rapide qui `has_piece`, `picker.add_blocks` sans
       affinité, `busy_mode` si `timed_out > 0`, doublon download_queue
       → `ignore_peers`, doublon request_queue → `make_time_critical`,
       sinon `add_request(b, busy_mode, time_critical=true)`), puis
       drainer `send_block_requests` pour les pairs touchés.
    e. Test de régression : torrent avec 2 pairs, deadline sur pièce
       dont tous les blocs sont requis mais bloqués (`last_requested`
       ancien) → un busy re-pick part chez le 2ᵉ pair ; pair choked
       exclu du set.

### D2 — `cancel_non_critical` exécuté synchroniquement au 1ᵉʳ deadline

- `torrent.rs:8627-8653` — upstream *poste* `cancel_non_critical` en
  fin de queue io (`torrent.cpp:5671-5678`) pour que plusieurs
  `set_piece_deadline` consécutifs (streaming : le client pose
  souvent plusieurs deadlines d'un coup) soient tous enregistrés
  **avant** le balayage : les requêtes déjà émises sur les pièces 2..n
  ne sont alors pas annulées. Rust annule dès la première insertion
  avec `critical = {piece1}` → des requêtes en vol sur piece2, pas
  encore critique, sont marquées `not_wanted` + Cancel wire.
  Sévérité : wire-visible (Cancel supplémentaires), faible.
  - **sketch** : introduire un flag `pending_cancel_non_critical: bool`
    posé à la première insertion ; effectuer le balayage en début du
    `request_time_critical_pieces` du tick suivant (ou via un effet
    `SessionOut` différé si le moteur a une file post-tick —
    vérifier `session.rs` : sinon, exécuter au début du prochain
    `drive_peer` round). Test : deux `set_piece_deadline` successifs
    avec une requête en vol sur piece2 → aucun Cancel pour piece2.

### D3 — `on_torrent_paused` vide `time_critical` alors qu'upstream le conserve

- `torrent.rs:10172-10175` — le commentaire affirme « upstream clears
  `m_time_critical_pieces` on pause » : **faux**. Upstream
  `do_pause`/`on_torrent_paused` (`torrent.cpp:10219-10300`,
  `5565-5575`) ne touchent pas `m_time_critical_pieces` ; seul
  `torrent_handle::clear_piece_deadlines` l'efface
  (`torrent_handle.cpp:953-958`). Rust perd les deadlines au pause ;
  upstream les garde (les `time_point` deviennent passés → re-pick
  agressif à la reprise). De plus le `clear()` Rust ne restaure pas la
  priorité `low` ni ne poste les `read_piece_alert` annulés.
  Sévérité : état (les priorités TOP restent sans la deadline
  correspondante) + alertes manquantes.
  - **sketch** : supprimer `self.time_critical.clear()` de
    `on_torrent_paused` (corriger le commentaire). Conserver les
    entrées ; les deadlines expirées sont gérées naturellement par
    l'horizon de D1. Test : pause/resume avec deadline posée →
    l'entrée survit au pause.

### D4 — `clear_piece_deadlines` ne poste ni alertes annulées ni restaure `low_priority`

- `session_handle.rs:1405-1408` — `t.time_critical.clear()` brut.
  Upstream `clear_time_critical` (`torrent.cpp:5805-5818`) poste pour
  chaque entrée `alert_when_available` un `read_piece_alert`
  (`operation_canceled`) et remet `low_priority` dans le picker.
  Même défaut dans `reset_piece_deadline` ? Non — `reset` (`torrent.rs:8681-8693`)
  fait les deux correctement ; c'est `clear` (et le `clear()` de D3)
  qui saute les deux effets. Idem pour le clear implicite éventuel à
  la suppression du torrent — vérifier le chemin remove.
  Sévérité : alertes manquantes + état picker.
  - **sketch** : factoriser `reset_piece_deadline` en
    `remove_time_critical_piece(piece, finished=false)` et faire
    `clear_piece_deadlines` itérer les pièces en appelant cette
    fonction (avec `&mut Vec<TorrentEffect>` pour les effets).
    Test : `clear_piece_deadlines` avec `alert_when_available` →
    `ReadPieceFailed("operation canceled")` émis et priorité LOW.

### D5 — `cancel_non_critical` : early-out `is_requested` absent

- `torrent.rs:9163-9179` — upstream `cancel_request` retourne
  immédiatement si `!t->picker().is_requested(block)`
  (`peer_connection.cpp:3780`) : un bloc dont le picker a déjà été
  remis à zéro (reçu d'un autre pair entre-temps) n'est ni marqué
  `not_wanted` ni annulé. Rust marque `not_wanted` + peut émettre un
  Cancel pour un tel bloc.
  Sévérité : mineure (wire Cancel surnuméraire, cas rare).
  - **sketch** : dans les deux boucles de `cancel_non_critical`,
    `continue` quand `picker.is_requested(b.block)` est faux (nécessite
    de lire `self.picker` avant la boucle — faire le filtrage en deux
    temps : collecter la liste des blocs, filtrer par `is_requested`,
    puis muter). Test : bloc arrivé via un autre pair juste avant le
    deadline → pas de Cancel.

### D6 — `prioritize_pieces`/`update_piece_priorities` ne retirent pas les deadlines dont la priorité tombe à 0

- `torrent.rs:7924-7938`, `8424-8454` — upstream retire les entrées
  `m_time_critical_pieces` dont la pièce passe `dont_download` :
  `set_piece_priority` → `remove_time_critical_piece(index)`
  (`torrent.cpp:5894`) et `update_piece_priority` →
  `remove_time_critical_pieces(pieces)` (`torrent.cpp:6000`), avec
  alerte annulée. Rust ne retire rien : une pièce mise à priorité 0
  reste dans `time_critical`, maintient `time_critical_mode` actif et
  (si `alert_when_available`) ne poste jamais l'alerte annulée.
  Sévérité : état + alerte manquante.
  - **sketch** : après les boucles `set_piece_priority` dans
    `prioritize_pieces` et `update_piece_priorities` (et tout autre
    chemin qui applique des priorités — `prioritize_files`), appeler
    un helper `remove_time_critical_pieces_where_dont_download(out)`
    reproduisant `torrent.cpp:5821-5838` (alerte
    `operation_canceled` par entrée flaggée, erase). Test : deadline
    posée sur piece, puis `prioritize_pieces` → `DONT_DOWNLOAD` →
    entrée supprimée + alerte annulée.

## Fichiers du fix

- `crates/bitdaemon-session/src/torrent.rs` (D1–D6 — struct
  `time_critical`, `request_time_critical_pieces`, `cancel_non_critical`,
  `on_torrent_paused`, `prioritize_pieces`, `update_piece_priorities`,
  completion path)
- `crates/bitdaemon-session/src/peer_conn.rs` (D1 —
  `can_request_time_critical`, `add_request` flag)
- `crates/bitdaemon-session/src/request_blocks.rs` (D1 — éventuel
  `pick_time_critical_block` partagé)
- `crates/bitdaemon-session/src/session_handle.rs` (D4 —
  `clear_piece_deadlines`)
- `crates/bitdaemon-session/src/peer_pipeline.rs` (D1 — vérifier que
  le drain `send_block_requests` suffit pour le batching)
- `crates/bitdaemon-session/tests/` (régressions)
- `docs/audits/changelog/A12.md`, `docs/PORTING.md` (row
  `set_piece_deadline`/`streaming`)

Chevauchements probables avec A07/A08 (`peer_conn.rs`, `torrent.rs`)
et A13 (`piece_picker.rs`/`request_blocks.rs`) — merges séquentiels
recommandés.

## Non-bugs tranchés

- **`m_receiving_block` non vérifié dans `cancel_non_critical`** :
  upstream `cancel_request` n'a pas ce garde non plus (il n'existe que
  dans `cancel_all_requests`, `peer_connection.cpp:3749-3753`) — Rust
  conforme.
- **Pas d'expiration passive des deadlines** : upstream n'émet aucune
  alerte à l'échéance d'une deadline — l'échéance pilote l'horizon et
  le `timed_out` re-pick uniquement. L'absence de timer dédié en Rust
  est conforme (dans la limite de D1).
- **`make_time_critical` sur bloc déjà en download_queue** : upstream
  ne cherche que dans `m_request_queue` (`peer_connection.cpp:3593`)
  — un bloc déjà parti reste ordinaire ; Rust idem
  (`peer_conn.rs:2049-2056`).
- **Ordre des priorités dans `set_piece_deadline` (ré-deadline)** :
  upstream fait le bump `top_priority` à l'intérieur de la branche
  « pièce déjà présente » (`torrent.cpp:5698-5707`) ; Rust le fait
  avant le test `existed` (`torrent.rs:8635-8649`) — effets nets
  identiques (`filter_updated` toujours vrai quand
  `prev == dont_download`).
- **`send_block_requests` dépasse `desired_queue_size` pour les
  critiques** : `peer_pipeline.rs:344-346` reproduit le
  `|| m_queued_time_critical > 0` de `peer_connection.cpp:4073`.
- **Keepalive/ordered-drain du préfixe** : le `.min(len)` à
  `peer_conn.rs:2001-2002` couvre le cas où le compteur dépasserait la
  file (borne défensive ; upstream a un `TORRENT_ASSERT` équivalent à
  `peer_connection.cpp:6560`).
