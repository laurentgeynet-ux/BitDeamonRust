# Audit A17 — rapport d'analyse

Périmètre : `src/part_file.cpp`, `src/file_pool_impl.cpp`,
`src/posix_part_file.cpp` (upstream `e7381c7bc`) vs
`crates/bitdaemon-disk/src/part_file.rs`, `file_pool.rs` (+ usage dans
`storage.rs`, cadence dans `disk_io.rs`).

Note de cadrage : le backend par défaut `pread_disk_io`/`pread_storage`
utilise **`aux::part_file`** (`src/part_file.cpp`, fd + `pread/pwrite`),
pas `posix_part_file` (FILE*, backend `posix_storage` — row PORTING.md
« merged »). `PartFile` suit donc la bonne référence. Différences notables
`posix_part_file` vs `part_file` (non portées, volontairement — backend
fusionné) : `export_file` ne libère PAS les slots, `read` retourne
`file_too_short` sur lecture courte, open `read_write`+`fopen`. Le port
suit `part_file.cpp` partout.

## Verdicts checklist

- part_file : header format (magic, version, endianness!), slot
  alloc/free FIFO, export_on_flush, bit `flushed` — verdict :
  **conforme** — preuve : `part_file.rs:3-11` (doc format),
  `parse_header` `i32::from_be_bytes` (`part_file.rs:100-110`) ==
  `read_uint32`/`read_int32` (`part_file.cpp:87-101`), header arrondi
  KiB `round_up` (`part_file.rs:29-31` == `part_file.cpp:56`), test
  golden `header_is_big_endian` (`part_file.rs:449-490`). FIFO :
  `free_slots.remove(0)` + `push` (`part_file.rs:142`, `:245`) ==
  `front()`/`erase(begin())` + `push_back` (`part_file.cpp:136-137`,
  `:298`); test `free_slots_are_fifo`. « flushed » = `dirty_metadata`
  (`part_file.rs:46`, `:144`, `:356-357`, `:389`) ==
  `m_dirty_metadata`. Le format n'a pas de magic/version (upstream non
  plus — détecté par match `num_pieces`/`piece_size`).
- Corruption : header illisible → comportement upstream, missing file,
  taille mismatch — verdict : **conforme** — open/read échoués ou
  lecture courte `< header_size` → map vide (`part_file.rs:85-92` ==
  `part_file.cpp:73-82`) ; mismatch `num_pieces`/`piece_size` → vide
  sans marquer dirty (`part_file.rs:103-105` == `part_file.cpp:92`) ;
  slot invalide `>= num_pieces` ignoré (`part_file.rs:116-119` ==
  `:105-106`) ; test `little_endian_header_rejected`.
- Migration : prioritize → bytes straddling vers .parts, export quand
  pièce complète — verdict : **conforme** — lift prio-0 : open write
  puis `export_file` si `m_part_file && use_partfile(i)`, offset
  file-relative (`storage.rs:802-832` == `pread_storage.cpp:139-176`,
  `partfile_write`/`ec.file(i)` inclus) ; drop → `use_partfile(i,
  !file_exists)` — upstream n'implémente pas le move-into non plus
  (`pread_storage.cpp:178-193` « this is not implemented yet »,
  `storage.rs:833-844`) ; export libère les slots des pièces
  complètes avec re-lookup post-I/O (`part_file.rs:333-341` ==
  `part_file.cpp:375-390`) ; `initialize` remplit `use_partfile`
  (`storage.rs:654-686` == `pread_storage.cpp:252-273`, symlink =
  inexistant inclus) ; `restore-on-failure` par fichier
  (`storage.rs:800`, `:847-849` == `scope_end` `pread_storage.cpp:134-137`).
- file_pool : LRU eviction, cap `file_pool_size`, open/upgrade
  read→write, reopen on error — verdict : **déviation (mineure)** —
  LRU tick-ordonné + éviction à `size - 1` avant l'open
  (`file_pool.rs:271-275`, `:486-493` == `file_pool_impl.cpp:123-128`),
  hit-ro-depuis-rw / jamais l'inverse (`file_pool.rs:231-233` ==
  `:104-105`), compteurs hits/misses/stalls/read_write_race
  (`file_pool.rs:241`, `:251`, `:277`, `:303-304` == `:71`, `:119`,
  `:130`, `:173`), upgrade ro→rw remplace l'entrée avec close différé
  (`file_pool.rs:302-318` == `:171-179`), `resize`/`release_*`/
  `close_oldest`/`get_status` conformes (`file_pool.rs:398-483`,
  cadence `close_file_interval` + fallback 1 min `disk_io.rs:1113-1141`
  == `pread_disk_io.cpp:2130-2141`). Pas de reopen-on-error upstream :
  l'échec n'est pas caché (rien inséré) → retry naturel — conforme.
  **Déviation** : un seul `opening` entry par `FileId` + join possible
  même quand le fichier est déjà dans le pool — voir Déviation 1.
- Sparse/allocate flags propagés, `file_open_failed` retry — verdict :
  **conforme / N-A** — `sparse` quand `!allocate_files` ou prio-0,
  `no_atime`, `no_cache` sur `write_mode ∈ {disable_os_cache,
  write_through}`, `truncate` une fois par session (`m_file_created`),
  exec/hidden, `allow_set_file_valid_data` via `open_mode_extra`
  (`storage.rs:280-331` == `pread_storage.cpp:712-800`) ; retry
  ENOENT + `ERROR_IO_DEVICE`(1117) → mkdir+réopen
  (`file_pool.rs:374-393` == `file_pool_impl.cpp:265-301`).
  `file_open_failed` n'existe pas en upstream 2.x (aucun hit dans
  `src/`/`include/`) — item N-A, le retry couvert est celui du
  répertoire manquant.

## Déviations

- `file_pool.rs:246-281` — Rust tient **une seule** entrée `opening`
  par `FileId` (`HashMap<FileId, OpeningEntry>`) et permet le join dès
  qu'un open compatible est en cours, même si le fichier figure déjà
  dans `files` avec un mode insuffisant. Upstream : `m_opening_files`
  est une **liste** (plusieurs ouvertures concurrentes du même fichier
  possibles) et le scan des waiters n'a lieu que si le fichier est
  **absent** du pool (`if (i == key_view.end())`,
  `file_pool_impl.cpp:62-98`). Conséquences :
  (a) un demandeur write qui trouve une entrée ro dans `files` et un
  open rw en cours attend l'open en cours au lieu d'ouvrir un second
  descripteur — bénin, voire meilleur ;
  (b) le **second** opener (ex. write alors qu'un open ro est en vol)
  fait `or_insert` sur l'entrée existante — il n'est pas enregistré —
  puis ouvre en parallèle ; s'il termine en premier, `opening.remove`
  réveille les waiters du **premier** opener avec son résultat — y
  compris une **erreur** alors que l'open encore en cours aurait pu
  réussir (upstream : chaque `opening_file_entry` notifie ses propres
  waiters). Sévérité : état, rare (races d'open concurrents du même
  fichier avec modes asymétriques).
  - **sketch** : modéliser `opening` comme upstream — liste plutôt que
    map. Concrètement dans `Inner` :
    `opening: Vec<OpeningEntry>` avec `OpeningEntry { file_key, mode,
    waiters: Vec<Arc<Waiter>>, id: u64 }` (id = `next_tick()` ou un
    compteur dédié). Au miss-path : pousser une nouvelle entrée,
    retenir son `id`. Join uniquement si `!files.contains_key(&file_key)`
    (le garde `i == key_view.end()` upstream, `file_pool_impl.cpp:62`)
    **et** `!(mode & WRITE) || oe.mode & WRITE` sur la première entrée
    correspondante (`find_if`, `:64-68`). À la completion :
    `opening.retain(|e| e.id != id)` + `mem::take` des waiters de
    l'entrée retirée — seuls ses propres waiters sont notifiés. Le
    second opener qui termine trouve son entrée (la sienne), la
    retire, insère/remplace dans `files` (la logique
    `read_write_race` existante couvre le `added == false` upstream).
    Réf. `src/file_pool_impl.cpp:62-98`, `:153-197`, `:221-241`.
    Test de régression suggéré : threads A(open ro) + B(open rw) sur le
    même `FileId` avec open injecté lent (wrapper autour de
    `open_file_at` ou fichier sur chemin lent) ; waiter C(ro) joint
    l'entrée de A ; B échoue → C doit recevoir le résultat de A, pas
    l'erreur de B. Plus simple à tester : deux `open_file_at` rw
    concurrents où le premier échoue (chemin non créable) et le second
    réussit — chacun ne doit voir que son propre résultat, et un waiter
    joiné au premier ne doit être réveillé que par lui.
- `part_file.rs:269-273` — `move_partfile` tolère `ENOENT` sur le
  `remove(old)` post-`copy_file` ; upstream propage
  (`part_file.cpp:326` : `remove(old_path, ec)` sans clear — seul le
  ENOENT du `rename` est effacé, `:316-317`). Sévérité : cosmétique /
  race rare (source disparue entre copy et remove).
  - **sketch** : remplacer le bras
    `Err(e) if e.kind() == io::ErrorKind::NotFound => {}` du `match
    fs::remove(&old)` par propagation : `Err(e) => return Err(e)`.
    Réf. `src/part_file.cpp:326`. Test : difficile à déterminer sans
    injection de faute — acceptable de couvrir par un mock `fs::remove`
    ou de laisser sans test (le bras devient un simple `?`).

## Fichiers du fix

- `crates/bitdaemon-disk/src/file_pool.rs` (déviation 1)
- `crates/bitdaemon-disk/src/part_file.rs` (déviation 2)
- `docs/PORTING.md` (rows `part_file.cpp` l.106, `file_pool_impl.cpp`
  l.107 — enrichir les notes)
- `docs/audits/changelog/A17.md`

## Non-bugs tranchés

- `writev` en `pwrite` séquentiels (`part_file.rs:201-207`) : upstream
  émet un `pwritev_all` unique ; mêmes octets observables, atomicité
  intra-fichier non requise par le format (documenté en commentaire).
- `hash_feed` ré-alloue par appel et re-lookup le slot via `read`
  (`part_file.rs:232-238`) : upstream `do_hash` garde le slot sous
  lock puis lit après unlock — le re-lookup Rust est strictement plus
  sûr (pièce libérée entre-temps → `missing_piece` au lieu d'une
  lecture de slot recyclé). Même contrat observable.
- `PartFile::open_file` sans lock (`part_file.rs:192`) vs upstream
  `open_file` sous mutex (`part_file.cpp:157`) : ordre des effets
  identique (échec d'open ⇒ aucun slot consommé), fenêtre de lock
  réduite — amélioration neutre.
- Éviction LRU : Rust ferme le descripteur évincé dans
  `open_file_impl` *avant* l'open suivant (`file_pool.rs:371`),
  upstream le détruit en fin de fonction (`file_pool_impl.cpp:52`,
  `:141-197`) — hors mutex dans les deux cas ; le nôtre libère le fd
  plus tôt (favorable EMFILE).
- `pread_best_effort` pour le header (`part_file.rs:87`) vs
  `pread_all` upstream (`part_file.cpp:78`) : les deux bornent `n <
  header_size → vide` ; comportement équivalent.
- `hash2` de `pread_storage` (`pread_storage.cpp:647-707`) n'a pas de
  chemin part-file dédié dans `hash_with` — si : `hash_with` couvre le
  cas part-file via `hash_feed` (`storage.rs:527-545`) == upstream
  `:659-677`. Conforme.
- Drop de `PreadStorage` appelle `pool.release_storage` (`storage.rs:
  888-898` == `pread_storage.cpp:61-69`) ; le flush part-file repose
  sur `Drop`/`release_files`/`tick` — conforme.
- `release_files` ordre : `tick()` (flush part-file) →
  `release_storage` → `stat_cache.clear` (`storage.rs:601-605`) ==
  `pread_storage.cpp:324-338`. Conforme.
- `delete_files` : pool release → take part_file (drop = flush) →
  delete incl. `.parts` (`storage.rs:754-768` ==
  `pread_storage.cpp:340-361`). Conforme.
- `to_file_open_mode` bits `read_write=1<<1`, `no_atime=1<<3`,
  `mmapped=1<<7` (`file_pool.rs:54-67` == `file_pool_impl.cpp:304-312`)
  ; `has_mapping=false` correct pour le backend pread. Conforme.
