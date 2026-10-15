# Audit A15 — rapport d'analyse

Périmètre : `move_storage` / `delete_files` / `rename_file` /
`prioritize_files` / `file_priority` + complétions.
Baseline upstream : `e7381c7bc` (`D:/Projet/libtorrent/libtorrent`).

## Verdicts checklist

- `prioritize_files` async : `SetFilePriority` → `async_set_file_priority` →
  `on_file_priority` — jamais inline — **conforme** —
  `crates/bitdaemon-session/src/torrent.rs:8487-8515` (dispatch via
  `TorrentEffect::SetFilePriority`), routage
  `crates/bitdaemon-session/src/session.rs:8933-8951` →
  `disk.async_set_file_priority` (fence job,
  `crates/bitdaemon-disk/src/disk_io.rs:2587-2600`,
  `add_fence_job` comme upstream `pread_disk_io.cpp:1098`), complétion
  `DiskCompleted::SetFilePriority` → `t.on_file_priority`
  (`session.rs:2065-2082`). Upstream : `torrent::prioritize_files`
  (`src/torrent.cpp:6106`).
- Sérialisation `outstanding_file_priority` + `deferred_file_priorities`
  (merge par index, replay à la complétion) — **conforme** —
  `torrent.rs:8495-8502` (clear + merge ordonné dans la BTreeMap,
  `emplace_hint`-equivalent), `torrent.rs:11937-11940` (`set_file_priority`
  défère par index), drain à la complétion `torrent.rs:8563-8579` (resize
  à `max_idx+1` avec `DEFAULT`, puis re-`prioritize_files` — même logique
  que `torrent.cpp:6085-6103`). Gate `!m_abort` → `!self.aborted` ✓.
- Vecteur appliqué = storage-ajusté, pas demandé ; `file_prio` posté à la
  complétion seulement ; `file_error` → set_error+pause — **déviation** —
  le vecteur storage-ajusté est bien renvoyé (`disk_io.rs:1382-1394` :
  `f.prio = st.file_priorities()` sur succès *et* échec — miroir du
  `prio = m_file_priority` du `restore_prio` upstream) et
  `FilePrio` n'est posté qu'à la complétion (`torrent.rs:8554-8560`) ;
  `m_file_priority` n'est muté que dans `on_file_priority`
  (`torrent.rs:8531-8535` + save-resume + `recalc_share_mode` ✓). Mais la
  branche erreur (`torrent.rs:8540-8553`) fait `self.paused = true` à la
  place de `pause()` upstream (`torrent.cpp:6071-6075` : `set_error` +
  `pause()` = `do_pause` complet : gates announce, `disconnect_all`,
  `stop_announcing` + stopped announce, alert `torrent_paused`). Voir
  Déviations D1. Bonus : `Alert::FileError` ne porte pas le champ
  `operation` de `file_error_alert` (`alerts.rs:535`) — voir D5.
- `fix_priorities` : force `dont_download` sur pads avant dispatch ;
  migration part-file — **conforme** —
  `torrent.rs:8452-8480` (resize à `num_files`/DEFAULT, pads →
  `DONT_DOWNLOAD`, clamp `TOP`; metadata-less = clamp seul, miroir de
  `torrent.cpp:6030-6047`). Migration part-file dans
  `crates/bitdaemon-disk/src/storage.rs:777-863` : `old==0 → new!=0`
  ouvre le fichier puis `part_file.export_file` vers le vrai fichier
  (miroir `mmap_storage.cpp:185-230` ; le move-into-partfile est un
  not-implemented upstream — `use_partfile(false)` quand le fichier
  existe, `storage.rs:833-844` ✓) ; restore du `old_prio` par fichier en
  cas d'erreur ✓ ; `need_partfile()` quand la nouvelle prio est 0 ✓ ;
  `flush_metadata` final avec `ec.file(-6)` ✓.
- `move_storage` : status bits (`need_full_check`→recheck,
  `fatal_disk_error`/`file_exist`→failure), `old_path` dans l'alerte,
  rollback disque — **déviation mineure** —
  bits et alertes : `session.rs:1927-1978` (`has_error` =
  `FATAL_DISK_ERROR|FILE_EXIST`, `StorageMoved{path, old_path}` +
  `need_save_resume`, `NEED_FULL_CHECK` → `force_recheck`, sinon
  `StorageMovedFailed{file=resolve_filename, op, error}`) — miroir de
  `torrent::on_storage_moved` (`torrent.cpp:9373-9395`). Legs
  `m_abort`/`!valid_metadata`/`!m_storage` : `session.rs:5135-5199` ✓
  (abort → `StorageMovedFailed(operation_aborted)`, pas de metadata →
  adopt + alerte sans job, pas de storage → adopt + alerte). Rollback
  disque : `storage_utils.rs:181-417` complet (fail_if_exist →
  `FILE_EXIST` à l'ancien path, mkdir/stat → `FATAL_DISK_ERROR`,
  `reset_save_path` → `NEED_FULL_CHECK`, `dont_replace` → skip +
  `NEED_FULL_CHECK`, rename→copy fallback sauf EINVAL/EACCES,
  mismatching symlink → `mismatching_file_type`, part-file move après
  les fichiers avec `ec.file(-6)` + rollback qui balaie tout,
  suppression des originaux copiés + prune des sous-dirs). **Déviation
  D4** : la branche metadata-less poste `complete(path)` dans l'alerte
  alors qu'upstream poste `save_path` brut (`torrent.cpp:9332-9335`).
- `rename_file` : bounds layout (metadata-less → silent), `old_name`
  résolu avant `renamed_files`, dirty dans la complétion — **conforme** —
  bounds `session.rs:5214-5218` (`index<0 || index>=num_files` → silent ;
  `files.is_none()` borne tout index, miroir du `layout().end_file()` de
  l'empty layout upstream `torrent.cpp:9291-9304`) ; `!m_storage` →
  `FileRenameFailed("session is closing")` ✓ (`session.rs:5220-5229`) ;
  complétion `session.rs:1980-2022` : `old_name` lu depuis
  `renamed_files`/layout *avant* l'enregistrement, puis insert +
  `need_save_resume` + `state_updated` (miroir
  `torrent::on_file_renamed` `torrent.cpp:5541-5560`) ; échec →
  `FileRenameFailed{index, error}` ✓.
- `delete_files` : fence'd disk job → `torrent_deleted{,_failed}`,
  no-disk leg → remove silencieux — **conforme** —
  `session.rs:5251-5282` : `async_delete_files` en fence job
  (`disk_io.rs:2578-2584`) dispatché *avant* `remove_torrent` (ordre
  voulu : le delete précède le teardown fence — upstream fait pareil,
  `torrent::delete_files` `torrent.cpp:9880-9900` enqueue avant que
  `remove_torrent_impl` libère le storage) ; complétion
  `session.rs:2054-2063` → `TorrentDeleted`/`TorrentDeleteFailed`
  (miroir `on_files_deleted` `torrent.cpp:5524-5539`) ; no-disk leg →
  `TorrentDeleteFailed{error:""}` + remove (`session.rs:5256-5267`,
  miroir `remove_torrent_impl` `session_impl.cpp:5528-5546` qui poste
  `torrent_delete_failed_alert` avec `error_code()` vide). Disque :
  `storage_utils.rs:437-494` (ENOENT ignoré par fichier, dernière
  erreur de fichier gagne, première erreur de dir seulement si phase
  fichiers clean, part-file supprimé sous `delete_files|delete_partfile`
  et son erreur écrase tout).
- Add-time `atp.file_priorities` + magnet init : apply direct picker,
  pas de job ni alerte — **conforme** —
  ctor metadata `torrent.rs:1230-1258` (`m_file_priority = atp…`,
  truncate/resize `dont_download`, `update_piece_priorities` direct —
  storage a déjà reçu `priorities` via `StorageParams::priorities` à
  `initialize()`, aucun job, aucune alerte) ; magnet arm
  `torrent.rs:1283-1285` (stockage brut, upstream
  `m_file_priority.assign` `torrent.cpp:422`) puis `init`
  `torrent.rs:1603-1637` avec sauvegarde/restauration de
  `need_save_resume` autour du sweep (miroir de `torrent.cpp:2053-2059`
  `auto const ns = m_need_save_resume_data`).

## Déviations

### D1 — `on_file_priority` : la pause d'erreur bypass `do_pause`

- `crates/bitdaemon-session/src/torrent.rs:8549-8553` — sur
  `file_error`, le code fait `self.paused = true` + `state_updated()`.
  Upstream (`src/torrent.cpp:6071-6075`) appelle `pause()` =
  `set_paused(true, 0)` → `do_pause` : `m_announce_to_*` off,
  `disconnect_all(errors::torrent_paused)`, `stop_announcing` +
  announce `stopped`, `torrent_paused_alert`. Ici les pairs restent
  connectés, les announces continuent, et `TorrentPaused` n'est jamais
  posté.
- Sévérité : **état / wire-visible** (le swarm voit le torrent rester
  actif après l'erreur disque ; alerte manquante).
- **sketch** : ajouter un effet `TorrentEffect::Pause` (ou réutiliser un
  hook existant) que `session::route` traduit en
  `self.set_paused(&ih, true, out)` — `do_pause` décide alors
  disconnect-all vs graceful selon l'état (upstream appelle `pause()`
  non-graceful).
  ```rust
  // torrent.rs:8550 — remplacer
  self.paused = true;
  self.state_updated();
  // par
  out.push(TorrentEffect::Pause); // upstream pause() → set_paused(true,0)
  ```
  ```rust
  // session.rs, bras de route() :
  TorrentEffect::Pause => self.set_paused(&ih, true, out),
  ```
  (attention au re-entrant : `set_paused` flippe `t.paused` puis
  `do_pause` ; laisser `set_error` avant). Note : le même raccourci
  `self.paused = true` existe à `torrent.rs:1932`, `:7364`, `:7906`,
  `:10384` — hors périmètre A15 (A10/A14), à signaler aux audits
  concernés ; D1 ne corrige que la branche `on_file_priority`.
- Réf. upstream : `src/torrent.cpp:6071-6075`,
  `torrent::do_pause`. Test de régression : forcer un échec
  `set_file_priority` (storage qui retourne `StorageError`), vérifier
  `FileError` + `TorrentPaused` postés et les pairs déconnectés.

### D2 — `file_priorities()` ne resize pas à `num_files`

- `crates/bitdaemon-session/src/session_handle.rs:1012-1017` retourne
  `t.file_priorities.clone()` brut. Upstream `torrent::file_priorities`
  (`torrent.cpp:6232-6244`) resize à `num_files` avec `default_priority`
  quand les métadonnées sont valides (un vecteur court stocké pré-
  metadata doit ressortir à taille pleine).
- Sévérité : **cosmétique / API** (payload du getter et de
  `file_priorities_alert`).
- **sketch** : dans `TorrentHandle::file_priorities`, après clone :
  ```rust
  let mut v = t.file_priorities.clone();
  if let Some(fs) = &t.files { v.resize(fs.num_files() as usize, DownloadPriority::DEFAULT); }
  v
  ```
  Réf. `src/torrent.cpp:6232-6244`. Test : torrent avec métadonnées,
  `file_priorities` stocké court → getter retourne `num_files` entrées.

### D3 — getter `file_priority(index)` absent

- `session_handle.rs` n'expose que le setter `file_priority(index, prio)`
  ; upstream `torrent_handle::file_priority(index)` getter existe
  (`torrent.cpp:6200-6222` : `index<0`/`>=end_file` → `dont_download`,
  pad → `dont_download`, slot non alloué → `default_priority`).
- Sévérité : **cosmétique / API**.
- **sketch** : ajouter `TorrentHandle::file_priority_get(index) ->
  DownloadPriority` (le nom `file_priority` étant pris par le setter,
  suivre la convention existante, ex. `get_file_priority`) implémentant
  les trois gardes ci-dessus sur `t.files` + `t.file_priorities`.
  Réf. `src/torrent.cpp:6200-6222`.

### D4 — `move_storage` metadata-less : alerte poste le path complété

- `crates/bitdaemon-session/src/session.rs:5151-5160` — la branche
  `t.files.is_none()` poste `StorageMoved{path: newp}` où
  `newp = complete(&path)`. Upstream (`torrent.cpp:9331-9338`) poste
  `save_path` **brut** dans `storage_moved_alert` puis stocke
  `m_save_path = complete(path)`. Le `old_path` est correct des deux
  côtés.
- Sévérité : **cosmétique** (payload d'alerte).
- **sketch** : poster `path: path.to_string_lossy().into_owned()`
  (l'argument brut) et garder `t.save_path = newp…`. Réf.
  `src/torrent.cpp:9331-9338`. Test : `move_storage` sur magnet sans
  métadonnées → `StorageMoved.path` == argument exact.

### D5 — `Alert::FileError` ne porte pas `operation`

- `crates/bitdaemon-session/src/alerts.rs:535-542` — `FileError` a
  `{ih, path, error}` ; upstream `file_error_alert(handle, ec,
  filename, op)` porte l'`operation_t` (`torrent.cpp:6067-6069`).
- Sévérité : **cosmétique** (payload d'alerte).
- **sketch** : ajouter `op: String` à `FileError` et passer
  `e.operation.name()` à `torrent.rs:8544-8548` (et aux autres sites
  `FileError`, cf. `handle_disk_error`). Réf. `src/torrent.cpp:6067`,
  `include/libtorrent/alert_types.hpp` `file_error_alert`.

## Fichiers du fix

- `crates/bitdaemon-session/src/torrent.rs` (D1 — effet `Pause`,
  branche `on_file_priority` ; D5 call-site)
- `crates/bitdaemon-session/src/session.rs` (D1 — bras de route ; D4)
- `crates/bitdaemon-session/src/session_handle.rs` (D2, D3)
- `crates/bitdaemon-session/src/alerts.rs` (D5 — champ `op`)

Recouvrement V4 : `torrent.rs` + `session.rs` — mergers séquentiels avec
les audits session-core concurrents.

## Non-bugs tranchés

- **Leg `!m_storage` de `prioritize_files`/`set_file_priority` skippe
  l'assign quand le vecteur est identique** (`torrent.rs:8509-8512`,
  `11951-11954`) — upstream assigne inconditionnellement puis
  `set_need_save_resume(if_config_changed)` : `if_config_changed` ne
  dirty que si la config a changé ; vecteur identique → pas de dirty →
  équivalent.
- **Pas de flag `m_deleted` porté** : upstream survit à
  `remove_torrent` (objet `torrent` en shared_ptr jusqu'à `abort()`),
  donc `is_deleted()` gate le code post-delete ; ici le torrent est
  retiré de la map dans le même appel — les gates sont la structure de
  contrôle elle-même.
- **Ordre delete : job avant `remove_torrent`** alors qu'upstream fait
  `disconnect_all`/`stop_announcing` avant d'enqueuer
  (`torrent.cpp:9886-9896`) — notre `remove_torrent` fait
  l'announce-stopped + teardown juste après l'enqueue, sur le même
  thread ; le job delete doit rester en queue avant le teardown fence
  (commentaire `session.rs:5270-5273`). Effets nets identiques.
- **`set_file_priority` ne force pas `dont_download` sur les pads** :
  upstream ne le fait pas non plus dans le chemin single-slot (clamp
  `dont_download..=top_priority` seulement, `torrent.cpp:6154-6196`) ;
  le `pad → 0` n'est garanti que par `fix_priorities`/`file_priority()`
  getter/storage — conforme.
- **Clés `i32::MAX` saturées dans `deferred_file_priorities`**
  (`i32::try_from(i).unwrap_or(i32::MAX)`) — `file_index_t` est i32
  upstream ; un index > i32::MAX ne peut pas exister, le clamp est
  inerte.
- **`storage_utils::delete_files` trie les dirs par profondeur** plutôt
  que l'ordre lexicographique-reverse upstream — superset correct de
  « subdirectories first » (un dir parent ne peut pas être supprimé
  avant son enfant dans les deux ordres).
