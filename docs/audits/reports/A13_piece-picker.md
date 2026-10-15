# Audit A13 — rapport d'analyse

Périmètre : `crates/bitdaemon-session/src/piece_picker.rs`,
`request_blocks.rs`, plus `torrent.rs::recalc_share_mode` (le leg
share-mode vit côté torrent, comme upstream `torrent.cpp`).
Upstream : `src/piece_picker.cpp`, `src/request_blocks.cpp`,
`src/torrent.cpp` @ `e7381c7bc`.

## Verdicts checklist

- Rarest-first : weighted random, ordre des priority buckets —
  **conforme** — formule de `priority()` identique (`availability *
  (8 - prio) * 3 + adjustment`, adjustments -2/-3/-1 ; `piece_full`/
  `piece_finished` → -1 mais pas les variantes reverse), bucketing +
  shuffle par bucket dans `update_pieces`, itération front→back —
  `piece_picker.rs:154-176, 3029-3145` ; upstream
  `aux_/piece_picker.hpp` `piece_pos::priority()` (~l.716-747),
  `piece_picker.cpp:2267-2282`.
- Reverse : ordre de pick pour snubbed peers (convergence sur mêmes
  pièces) — **conforme** — buckets itérés de la fin vers le début et
  `end-1 → start` dans chaque bucket ; `peer_conn.rs:2194-2196` pose
  `REVERSE` quand `pipeline.snubbed` — `piece_picker.rs:3036-3067` ;
  upstream `piece_picker.cpp:2204-2227`.
- Partials : prefer_contiguous_blocks, prioritize_partials,
  time-critical d'abord — **déviation (passe 2)** — le cœur du
  picker est conforme (cap `num_partials > num_peers*3/2 ||
  *blocks_per_piece > 2048` → `PRIORITIZE_PARTIALS` +
  `prefer_contiguous = 0` ; tri rarest-first des partials ;
  `add_blocks_downloading` reproduit requested_from/exclusive/
  backup_blocks à l'identique) mais l'*amont* `picker_options()`
  est incomplet : `initial_picker_threshold`,
  `prioritize_partial_pieces` et `piece_extent_affinity` jamais
  câblés, `RAREST_FIRST|SEQUENTIAL` posé en séquentiel — voir
  Passe 2 — `piece_picker.rs:2841-2919, 2734-2814`,
  `torrent.rs:6454-6461`, `peer_conn.rs:2237` ; upstream
  `piece_picker.cpp:1981-2114, 2752-2840`,
  `peer_connection.cpp:896-940`.
- Priorities : piece 0/dont_download skip, top-priority first,
  per-piece vs per-file — **conforme** — `filtered()` exclu via
  `is_piece_free`/`can_pick`, downloading-filtré → queue `ZeroPrio`
  dans `update_piece_state` ; la passe séquentielle dépile
  `m_pieces` tant que `piece_priority == TOP` ; mapping
  file→piece priority dans `torrent.rs:1633,2708,8089` —
  `piece_picker.rs:1500,1959-1971,2951-2973` ; upstream
  `piece_picker.cpp:2142-2155`.
- Streaming : set_sequential_range, sequential mode —
  **conforme** — `cursor`/`reverse_cursor` bornés et avancés en
  sautant flushed+filtered, identique à upstream ; boucles
  séquentielle et reverse-séquentielle + exclusion de la fenêtre
  `[cursor, reverse_cursor)` dans les passes rarest/random —
  `piece_picker.rs:648-666, 2946-3026` ; upstream
  `piece_picker.cpp:356-371, 2137-2193`.
- Share mode : recalc_share_mode rebalance — **déviation** —
  structure fidèle (census, disconnect seeds >50%,
  `missing_pieces - 2*num_seeds`, rareté → `set_piece_priority`) mais
  `num_downloaded` ne reproduit pas `want().num_pieces` — voir
  Déviations #2 — `torrent.rs:10838-10978` ; upstream
  `torrent.cpp:11034-11167`.
- Endgame : busy blocks requestables en dernier (un busy par peer
  sauf TC) — **déviation (passe 2)** — le gate pré-pick
  `dont_pick_busy_blocks = (strict && queue < want_left) ||
  outstanding > 0` est conforme (upstream le calcule aussi avant la
  boucle), mais le test post-boucle upstream réévalue `dq.size() +
  rq.size()` en live — les blocs mis en file *pendant* la boucle
  comptent, et chez nous ils sont stagés — voir Passe 2 ; pool
  end-game restreint à `piece_full` capé à 200, un bloc random
  `state_requested` d'un autre peer —
  `request_blocks.rs:166-212`, `piece_picker.rs:3239-3285` ;
  upstream `request_blocks.cpp:172-292`, `piece_picker.cpp:2373-2485`.
- Invariants : num_peers == downloaders réels, refcount == peers
  ayant la pièce, transitions block_info — **déviation** —
  `mark_as_downloading`/`mark_as_writing`/`mark_as_finished`/
  `mark_as_canceled`/`abort_download`/`write_failed`/
  `restore_piece`/`lock_piece` fidèles (vérifié ligne à ligne),
  refcount inc/dec/break_one_seed fidèle ; **mais**
  `update_piece_state` ne re-cascade pas la priorité quand le bucket
  change sans passer par -1 → voir Déviations #1 —
  `piece_picker.rs:1493-1541` ; upstream `piece_picker.cpp:2977-3055`.

## Déviations

- `piece_picker.rs:1535-1538` — `update_piece_state` : après le
  changement de queue, Rust ne fait `add(index)` que si `prio ==
  -1 → != -1`. Upstream fait aussi `else if (prio != -1)
  update(prio, p.index)` (`piece_picker.cpp:3049-3052`). Conséquence :
  une transition `Downloading → Full` (ou `Finished`, ou
  `Downloading ↔ DownloadingReverse` qui change l'adjustment
  -3 → -1) modifie `priority()` mais ne touche jamais `m_pieces` :
  la pièce reste dans un bucket obsolète (doublon à la prochaine
  `add`, drifts des `priority_boundaries`, ordre rarest-first
  faussé jusqu'au prochain `m_dirty` rebuild). Sévérité : état
  (ordre de pick + intégrité transitoire de l'index ; les gardes
  `dirty` dans `update`/`remove` évitent la corruption mais
  masquent le drift).
  - **sketch** : dans `update_piece_state`, remplacer le bloc
    `if !self.dirty && prio == -1 && … { self.add(index); }` par
    ```rust
    if !self.dirty {
        if prio == -1 {
            if self.piece_map[index.0 as usize].priority(self.seeds) != -1 {
                self.add(index);
            }
        } else {
            let elem = self.piece_map[index.0 as usize].index;
            self.update(prio as usize, elem);
        }
    }
    ```
    (miroir de `piece_picker.cpp:3048-3052` : `update` re-lit la
    nouvelle priorité et retire/relocate). Test de régression :
    pièce `Downloading` → `mark_as_finished` jusqu'à `Full` ;
    vérifier que `piece_stats(index).priority == -1` ET que la
    pièce n'apparaît plus dans `m_pieces` (e.g. via un pick
    rarest-first qui ne la ressort pas même avec un peer qui l'a,
    et `distributed_copies` stable).
- `torrent.rs:10916` — `recalc_share_mode` : `num_downloaded =
  num_have().max(num_pieces() - num_filtered())`. Upstream :
  `max(have().num_pieces, want().num_pieces)` où
  `want().num_pieces = num_pieces - num_filtered - num_have_filtered`
  (`piece_picker.cpp:3897-3902`, `torrent.cpp:11093`). Rust oublie
  `- num_have_filtered` : avec des pièces filtrées déjà détenues la
  gate `share_mode_target` passe plus facilement (surestime le
  nombre de pièces "à partager"). Sévérité : état (share-mode
  repriorise un poil trop souvent).
  - **sketch** : remplacer `p.num_pieces() - p.num_filtered()` par
    `p.want().num_pieces` (méthode déjà portée, `piece_picker.rs:1360`).
    Test : torrent avec pièces filtrées-détenues, swarm en share
    mode, `all_time_upload` juste sous le seuil — upstream ne
    repriorise pas, Rust oui sans le fix.
- `torrent.rs:6104, 6399, 9206, 9322` (+ `peer_conn.rs:2173`) — les
  compteurs du picker sont déclarés dans `counters.rs:143-158` mais
  `PickerStats` n'est implémenté que pour `NullPickerStats` ; tous
  les call sites passent `NullPickerStats`. Upstream passe
  `ses.stats_counters()` (`request_blocks.cpp:149-153`). Les
  `piece_picker_*_loops` du `session_stats_alert` restent à 0.
  Sévérité : stats (visible dans `session_stats` wire/log).
  - **sketch** : `impl PickerStats for Counters` (ou pour
    `&Counters` si les compteurs sont `Cell`/`Atomic`) mappant
    `PickerCounter` → indices des compteurs déclarés, et passer le
    vrai `counters` aux sites `request_a_block`/`pick_pieces`.
    Test : `pick_pieces` sur un swarm à plusieurs pièces →
    `counters.piece_picker_rare_loops > 0`.

## Fichiers du fix

- `crates/bitdaemon-session/src/piece_picker.rs`
- `crates/bitdaemon-session/src/torrent.rs`
- `crates/bitdaemon-session/src/counters.rs` (+ call sites
  `torrent.rs` / `peer_conn.rs` pour le wiring des compteurs)

## Non-bugs tranchés

- Heap vs sort pour les partials rarest-first : upstream
  `use_heap` est une optimisation de coût (`num_blocks*10 <
  num_free_blocks`), l'ordre de pick est identique — Rust trie
  toujours, sémantique conservée.
- `endgame` pool : upstream calcule `partials_size =
  min(200, downloading + full)` mais ne remplit que depuis
  `piece_full` — Rust itère `Full` avec cap 200, équivalent.
- `request_a_block` renvoie `(bool, Option<bool>)` vs upstream
  `bool` + `c.set_endgame` — la forme diffère, les gates
  (`is_seed`, `no_download`, `upload_mode`, `disconnecting`,
  `!valid_metadata`, `!files_checked`, `graceful_pause`, choked→
  `allowed_fast` mask, busy-block rules) sont à l'identique.
- `num_downloaded <= 0` early-return dans `recalc_share_mode`
  (Rust ajoute un garde qu'upstream n'a pas ; quand
  `num_downloaded == 0` upstream peut encore tomber dans le scan
  de rareté si `dl_queue == 0` — cas quasi impossible : 0 pièces
  détenues ni voulues. Gardé comme divergent-bénin à aligner si le
  fix #2 touche la zone.)

## Couverture des tests upstream (test/test_piece_picker.cpp)

121 `TORRENT_TEST` upstream vs ~40 tests Rust
(`tests/test_piece_picker.rs`). Orphelins notables :

- `piece_extent_affinity*` (6 tests) — inatteignables : l'option
  n'était jamais armée (déviation #5c).
- `cursors_*` (11 tests sweep up/down/in × we_have/set_priority/
  seq_range) — partiel : `cursor`/`sequential` couvrent la logique
  de base mais pas les balayages dirigés.
- `pad_*` / `mark_as_pad_*` (~15 tests) — les pad-bytes v2 ont
  `set_pad_bytes` testé mais pas les scénarios de pick.
- `restore_piece*`, `reprioritize_*`, `download_filtered_piece`,
  `break_one_seed`, `started_hash_job`, `resize` — non portés.

## Passe 2 — déviations complémentaires

Relecture approfondie de `request_blocks.cpp` et
`peer_connection::picker_options()` après le premier apply.

- `request_blocks.rs:207` — le gate post-boucle
  `busy_block == INVALID || num_outstanding > 0` utilise le snapshot
  `num_outstanding` pré-pick. Upstream réévalue
  `dq.size() + rq.size() > 0` **après** la boucle — or
  `c.add_request(pb, {})` pousse dans `m_request_queue` *pendant* la
  boucle (`request_blocks.cpp:264-267`). Comme `drive_requests`
  stage les picks (`picked`) et ne marque/queue qu'au retour, un
  passage qui met en file ≥1 bloc libre sans remplir `num_requests`
  envoie quand même le busy-block ; upstream attend que la file soit
  totalement vide. Conséquence : requête dupliquée wire-visible une
  passe plus tôt en endgame (bornée par le cap un-busy-par-peer).
  Sévérité : wire-visible (ordre/volume des requêtes endgame).
  - **sketch** : compter les `add_request` réussis dans la boucle
    (`queued` ) et remplacer le test par
    `busy_block == INVALID || num_outstanding + queued > 0` —
    équivalent exact du `dq+rq` live upstream. Test : 1 pièce libre
    + 1 pièce busy, `desired_queue_size` > blocs libres — upstream
    ne demande pas le busy, Rust le demandait.
- `torrent.rs:6454-6461` — `picker_options()` incomplet vs
  `peer_connection.cpp:896-940`. Upstream :
  `sequential` XOR (`num_have() < initial_picker_threshold` →
  `prioritize_partials`) XOR (`rarest_first` + `reverse` si snubbed
  sinon `piece_extent_affinity` si setting et pas de TC), puis
  `prioritize_partials` si `prioritize_partial_pieces`, puis
  `on_parole|prioritize_partials` si parole.
  - (a) `initial_picker_threshold` (défaut 4) jamais consulté :
    upstream fait un pick **random + partials** (pas de
    `rarest_first`) tant qu'on a < 4 pièces — stratégie de
    démarrage délibérée ; Rust fait rarest_first dès le début.
  - (b) `prioritize_partial_pieces` setting jamais lu →
    `PRIORITIZE_PARTIALS` ne vient que du cap interne et de parole.
  - (c) `piece_extent_affinity` setting jamais lu →
    `PIECE_EXTENT_AFFINITY` inatteignable : `record_downloading_piece`,
    la passe extent de `pick_pieces` et les 6 tests upstream sont
    morts.
  - (d) `RAREST_FIRST | SEQUENTIAL` en séquentiel vs upstream
    `sequential` seul (l'assert upstream `rarest_first XOR
    sequential ≤ 1` échouerait) : sous séquentiel+partials, Rust
    trie les partials rarest-first au lieu de l'ordre
    `m_downloads`, et un peer snubbed reçoit `REVERSE` qu'upstream
    ne pose pas (le leg vit dans la branche rarest_first).
  - (e) `peer_conn.rs:2237` — le leg snubbed→`REVERSE` n'a pas de
    `else` extent-affinity (`else if piece_extent_affinity &&
    num_time_critical_pieces()==0`).
  Sévérité : état/ordre de pick (démarrage random, partials sous
  séquentiel, settings morts).
  - **sketch** : helper `piece_picker::picker_options(sequential,
    num_have, threshold, prioritize_partial_pieces)` pour les legs
    torrent-level ; `PeerConn::picker_options(base, extent_affinity,
    tc)` pour les legs per-peer (snubbed→REVERSE else
    extent-affinity, parole) ; `TorrentRequestCtx` gagne
    `piece_extent_affinity`. Call sites : `torrent.rs:6454`, `9523`,
    web-seed `6785`, re-marks `7836`/`12104`.

## Résultat

Branche `audit/13-piece-picker` (worktree `C:/Projet/wt/LibtorrentRust-a13`).

Déviations corrigées :

- `piece_picker.rs::update_piece_state` — re-cascade `update(prio,
  index)` sur tout changement de queue avec `prio != -1`, plus
  `add` sur l'edge `-1 → ≥0` (miroir `piece_picker.cpp:3048-3052`).
- `torrent.rs::recalc_share_mode` — `num_downloaded` = `max(have,
  want().num_pieces)` (avec `- num_have_filtered`) ; le garde
  supplémentaire `num_downloaded <= 0` a été retiré pour suivre la
  forme upstream (`dl_queue > num_downloaded / 20` seulement).
- Compteurs picker — `impl PickerStats for &Counters` dans
  `counters.rs` (8 slots upstream `piece_picker_*_loops`) ;
  `NullPickerStats` remplacé par `&Counters` aux 4 call sites ;
  `next_web_seed_blocks` a gagné un param `counters`.

Tests ajoutés (`tests/test_piece_picker.rs`) :

- `full_piece_readded_once` — Downloading→Full→abort→re-add ; un
  pick couvrant tout le torrent ne duplique aucun bloc.
- `want_excludes_have_filtered` — `want().num_pieces` exclut les
  pièces had+filtered.
- `picker_counters_wired` — `pick_pieces` incrémente
  `PiecePickerRareLoops` dans la banque `Counters` réelle.

Gates : `cargo fmt --all` · `cargo clippy --workspace --all-targets
-D warnings` · `cargo test --workspace` (33/33 picker, workspace
vert) · `cargo doc --workspace --no-deps` — toutes passées.
