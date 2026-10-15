# Audit A14 — rapport d'analyse

Périmètre : pipeline disque entrant → écriture, baseline upstream
`e7381c7bc` (`src/peer_connection.cpp` `incoming_piece` /
`on_disk_write_complete` / `cancel_request`, `src/mmap_disk_io.cpp`
`async_write` / `add_job` / `add_completed_jobs*`,
`src/disk_cache.cpp` `account_pending_write` / `insert` /
`flush_piece_impl` / `flush_to_disk`, `src/torrent.cpp`
`cancel_block`) vs `crates/bitdaemon-disk` (`disk_io.rs`,
`disk_cache.rs`, `disk_job*.rs`, `disk_buffer_pool.rs`,
`back_pressure.rs`, `readwrite.rs`, `storage.rs`) +
`crates/bitdaemon-session` (`peer_conn.rs`, `torrent.rs`,
`session.rs` dispatch + `DiskCompleted::Write`).

Date : 2025 — mode `rapport` (analyse read-only ; aucun fix appliqué).

## Résumé exécutif

Le pipeline Rust est **très fidèle** : ordre `async_write` →
`mark_as_writing` conservé, comptabilisation avant verdict sur la
complétion, porte `picker_generation` équivalente, fences ordonnant
`clear_piece`/`move_storage`/`delete_files`, back-pressure globale +
par pair, lazy open, part-file routing, coalescing par runs
contigus. Aucune déviation confirmée de sévérité haute.

Deux écarts mineurs identifiés (voir §Déviations) :

- `incoming_piece` : pas de gate `is_deleted`/`aborted` entre la
  levée de l'entrée de queue et le dispatch `DiskWrite`.
- chemin d'erreur d'écriture : pas d'appel `update_gauge()`
  (`state_updated`) avant le traitement d'erreur — jauges
  potentiellement périmées jusqu'au prochain tick.

Plusieurs différences structurelles assumées sont sémantiquement
justifiées (piece_gate pour le hash-vs-write, pool de flush dédié,
sérialiseur d'écriture storage, abort drain explicite) — listées en
« non-déviations ».

## Verdicts checklist

- **Dispatch `DiskWrite`** : ordre async_write *avant*
  `mark_as_writing` — verdict : **conforme** —
  `torrent.rs` émet `PeerEffect::DiskWrite` puis `MarkDownloaded`
  (`torrent.rs:4590-4642`, `:4848+`) ; `peer_conn.rs` émet les
  effets dans cet ordre (`peer_conn.rs:1547-1711`) ==
  `peer_connection.cpp` `incoming_piece` (`async_write` avant
  `picker().mark_as_writing`). `picker_gen` stampé au dispatch dans
  `dispatch_disk` (`session.rs:2388-2487`) ==
  `picker().get_generation()` capturé avant `async_write`.
- **is_deleted** : gate `t->is_deleted()` après erase de la queue —
  verdict : **déviation mineure** — voir D1.
- **Bloc introuvable / seed / zero-length** : **conforme** — seed
  ignore + waste accounting (`torrent.rs:4590+`,
  `peer_conn.rs` seed arm) ; bloc hors `download_queue` →
  unwanted/redundant ; zero-length rejeté à la validation taille.
- **Part-file routing** : **conforme** — `storage::writev` route
  les fichiers `DONT_DOWNLOAD` vers `part_file` ==
  `pread_storage` + `use_partfile` ; fichiers pad/skipped couverts.
- **I/O positionné multi-fichiers** : **conforme** —
  `readwrite_buf`/`readwrite_mut` (`readwrite.rs`) == marche
  `file_index_at_offset`/`file_offset` de `aux::readwrite.hpp`,
  fichiers vides sautés, EOF court → `UnexpectedEof` ==
  `asio::error::eof`. Sparse via `open_mode SPARSE`.
- **`DiskCompleted::Write`** : compteurs avant verdict, gate de
  génération — verdict : **conforme** — `QueuedWriteBytes` puis
  `outstanding_writing_bytes` décrémentés, gates
  `NumPeersDownDisk`/`PeerWriteGates` relâchées à zéro, *puis*
  `picker_generation != picker_gen` → drop (`session.rs:1718-1885`)
  == `on_disk_write_complete` (comptabilisation avant gate, drop
  identique pour succès et échec).
- **Erreur d'écriture** : aborted vs réelle — verdict : **conforme
  (avec réserve D2)** — `Interrupted` → un-mark seulement ; erreur
  réelle → cancel du bloc, mark failed, `clear_piece` schedulé ==
  upstream. `on_disk_error` (`session.rs:8659-8729`) ==
  `handle_disk_error` : `file_error_alert`, write-soft-list →
  upload_mode, sinon `set_error` + pause + `torrent_error_alert`.
- **Lazy file opening** : **conforme** — `open_file`/`open_file_at`
  au premier I/O seulement ; erreurs reportées comme `StorageError`
  sur le job → `file_error` alert, pas à `add_torrent`. Le seul open
  eager est `initialize` sous fence de checking (comme upstream).
- **Bounded buffer pool / back-pressure** : **conforme** — cap pas
  dans `disk_buffer_pool` (comme upstream — malloc inconditionnel) ;
  borne via `BackPressure` hystérésis (observe au-dessus de
  `max_size`, callback au low watermark, `back_pressure.rs:143+`)
  + `PeerWriteGates` par `(ih, peer)` == `bw_disk` /
  `num_peers_down_disk` + `m_channel_state`. `QueuedWriteBytes`
  alerte edge-triggered sur `max_queued_disk_bytes`.
- **Niveau de buffer effectif** : **conforme** — le niveau inclut
  blocs cachés + v2 hash queue + pending writes comme upstream
  `account_pending_write` (incrément *avant* insertion, `insert`
  reprend la comptabilisation sous mutex).
- **Coalescing d'écritures** : **conforme** — `take_flush_run` /
  `take_largest_run` / `take_expired_runs` (`disk_cache.rs`)
  sélectionnent runs contigus, priorité aux pièces complètes /
  force-flush, puis plus grande run, puis blocs éligibles ==
  `disk_cache::flush_to_disk`. Frontière pièce-complète respectée
  (`flushed` bit + hash gate).
- **Fences** (`clear_piece`/`move_storage`/`delete_files` vs writes
  en vol) : **conforme** — `disk_job_fence.rs:144+` `is_blocked`
  compte in-progress + queue derrière la clôture, `job_complete`
  décrémente, abaisse les clôtures, reposte les jobs bloqués *dans
  l'ordre* sous le mutex ; `add_fence_job` drain le cache avant
  `raise_fence` ; writes repostés sous clôture empilée →
  `flush_for_fence` (comptés `account_outstanding`, == upstream
  `m_fence_flush` storage). `clear_piece` libère les blocs cachés +
  abort hash jobs et writers parkés. `remove_torrent` = job fenced
  (upstream supprime immédiatement — équivalent sémantique car la
  removal attend aussi les fences).
- **Timers/engine : complétions non affamées** : **conforme** —
  `add_completed_jobs`/`add_completed_jobs_impl` ==
  `add_completed_job` : drain sous mutex, repost sérialisé, handlers
  sur le thread session (comme network thread upstream). Pool de
  flush dédié séparé du pool générique — voir N4.
- **`cancel_block`** : **non-bug / dead-code upstream** —
  `torrent::cancel_block` n'est appelé upstream que quand le bloc
  est `state_writing` (après `mark_as_writing` ou dans le chemin
  d'erreur post-mark) ; `peer_connection::cancel_request`
  (`peer_connection.cpp:3763`) early-return sur
  `!is_requested` ⇒ l'appel est **inerte** upstream dans les deux
  cas. Notre `cancel_block` (`torrent.rs:9482`) partage le même
  guard `!is_requested` et n'est jamais appelé dans ces contextes
  — observable identique. Pas de divergence.
- **Comportement BitComet / zero-length piece** : **conforme** —
  rejet précoce côté validation.

## Déviations détaillées

### D1 — pas de gate `is_aborted` avant `DiskWrite` (mineure, état)

- **Sévérité** : basse (état interne, fenêtre courte).
- **Upstream** : `incoming_piece` (`peer_connection.cpp:~2990`)
  retire l'entrée de queue puis `if (t->is_deleted()) return;`
  avant `async_write` et `mark_as_writing`.
- **Rust** : l'arm `PeerEffect::DiskWrite` (`torrent.rs:4590+`)
  vérifie `is_seed`/`is_downloaded`/`pending_unsnub` mais pas
  `aborted`. `abort()` (`torrent.rs:10226`) appelle
  `disconnect_all` qui vide `peers`, mais un message Piece
  arrivant entre l'abort effectif et le dernier `on_peer_message`
  déjà routé pousse quand même `TorrentEffect::DiskWrite` →
  `async_write` sur un storage en cours de démantèlement.
- **Impact** : le write job atterrit en cache ou est aborted par
  `abort_jobs`/fence `RemoveTorrent` ; le handler complete une fois
  (orphan-safe), donc pas de fuite ni double-free — divergence
  limitée à une écriture de plus que upstream sur une fenêtre de
  teardown.
- **Recommandation** : ajouter un guard `self.aborted` au début de
  l'arm `DiskWrite` (et symétriquement `MarkDownloaded`), ou un
  test qui démontre que la fenêtre est impossible. À confirmer par
  un test de retrait torrent + piece en vol.

### D2 — `update_gauge` manquant sur le chemin d'erreur d'écriture (mineure, stats)

- **Sévérité** : basse (jauges/alertes de statut uniquement).
- **Upstream** : `on_disk_write_complete` chemin erreur appelle
  `t->update_gauge()` avant `handle_disk_error`.
- **Rust** : l'arm erreur de `DiskCompleted::Write`
  (`session.rs:1762-1821`) fait `on_piece_fail_sync` /
  `on_disk_error` mais pas de `state_updated`.
- **Impact** : `num_pieces`/jauges téléchargées peuvent rester
  périmées jusqu'au prochain `state_updated` (tick ou autre
  événement). Aucun effet wire.
- **Recommandation** : appeler `t.state_updated()` dans le chemin
  d'erreur non-abortée.

## Non-déviations / justifications structurelles

- **N1 — Piece gate hash-vs-write** : le `piece_gate` park les hash
  jobs tant que des writes sont en vol sur la pièce. Upstream utilise
  les états `flushed`/hash queue du cache + `wait_for_hashing`.
  Structure différente, invariant identique (un hash n'observe jamais
  un write en vol). À valider par test de race.
- **N2 — Pool de flush dédié** : pool supplémentaire à côté de
  generic/hash. Non-upstream ; validé si aucun ordre de complétion
  n'est observable et pas de famine (les jobs fence-bound ne passent
  pas par ce pool — à confirmer par test de contention).
- **N3 — `write_serializer`** : mutex autour des syscalls de write
  storage. Non-upstream ; cible la protection média. Réduit le
  parallélisme de flush vs upstream — divergence *performance*
  possible, pas sémantique. Marquer pour mesure.
- **N4 — `abort_jobs` drain explicite** : drain des pool queues,
  piece gates, fences et blocs cache pour garantir « handler appelé
  exactement une fois » == `abort` upstream (qui ne doit pas orphelin
  les jobs repostés). Comportement équivalent, implémentation plus
  explicite.
- **N5 — Read overlay** : la lecture chevauche les blocs dirty du
  cache après lecture storage et sert les reads full-cache sans I/O
  — même observable que `disk_cache` upstream (dirty blocks servis
  aux peers), mécanisme différent.
- **N6 — `mark_as_canceled`/`abort_download` dans le cancel** : notre
  `cancel_request` envoie le Cancel fil et abort picker là où
  upstream pose `not_wanted` sur l'entrée in-flight et n'abort que
  sous `force`. À re-vérifier dans A08 (déjà traité ?) — le point
  `cancel_block`-post-mark étant inerte des deux côtés, le résidu
  porte sur le chemin `force` uniquement.

## Tests / lacunes

- Tester : write arrivant après `abort()` → aucun `DiskWrite` émis
  (D1) ; erreur d'écriture non-soft → `state_updated` (D2).
- Tester race piece_gate : hash démarré pendant qu'un write flush
  de la même pièce est en vol.
- Tester `flush_for_fence` sous clôture empilée (write reposté
  pendant `add_fence_job` F2).
- Mesurer l'impact du `write_serializer` sous charge flush.
- Vérifier que `queued_write_bytes` alerte est bien edge-triggered
  (un seul `Performance` alert par franchissement).

## Fichiers pour le fix (si mode apply)

- `crates/bitdaemon-session/src/torrent.rs` — guard `aborted` arm
  `PeerEffect::DiskWrite` (~4590).
- `crates/bitdaemon-session/src/session.rs` — `state_updated` arm
  erreur (~1762-1821).
- Tests : `crates/bitdaemon-session/tests/` (fenêtre abort + piece)
  et `crates/bitdaemon-disk/tests/` (race piece_gate / flush_fence).

## Conclusion

Pipeline disque entrant→écriture quasi-conforme : ordre des effets,
compteurs, gates de génération, fences, back-pressure et coalescing
reproduisent la sémantique upstream. Deux écarts mineurs (D1, D2)
restreints à une fenêtre de teardown et aux jauges de statut ; les
mécanismes additionnels Rust (piece gate, flush pool, serializer,
abort drain) sont des ré-implémentations justifiées des invariants
upstream, à couvrir par les tests listés ci-dessus.

## Résultat

Corrections appliquées sur `audit/14-disk-write-pipeline` :

- **D1 corrigée** — guard `aborted` dans l'arm `PeerEffect::DiskWrite`
  de `torrent.rs`, placé après le leg redondant et avant l'un-snub
  (position `is_deleted` upstream) ; `skipped_writes` supprime les
  effets appariés `MarkDownloaded`/`GracefulCheck`. Test :
  `aborted_torrent_drops_incoming_piece` (`test_download_path.rs`).
- **D2 corrigée** — `state_updated()` appelé dans l'arm erreur de
  `DiskCompleted::Write` (`session.rs`) pour les échecs aborted *et*
  réels, avant `on_disk_error` (== `update_gauge()` puis
  `handle_disk_error`). Test : `failed_write_marks_state_updated`
  (`test_session.rs`).

Gates : `cargo fmt --all`, `cargo clippy --workspace --all-targets
-- -D warnings`, `cargo test --workspace` (113 suites, 0 échec),
`cargo doc --workspace --no-deps` — toutes vertes.

Row `peer_connection.cpp` de `docs/PORTING.md` enrichie (clause A14) ;
fragment changelog `docs/audits/changelog/A14.md`.
