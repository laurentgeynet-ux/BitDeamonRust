---
name: audit-14-v4-disk-write-pipeline
description: "[V4] Audit sémantique vs libtorrent — Pipeline disque — bloc entrant → écriture"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A14 — Pipeline disque — bloc entrant → écriture

**Périmètre** : incoming block → async_write → cache → flush → completion.

**Vague d'apply** : V4 (session-core) — tous sur `torrent.rs`/`peer_conn.rs`/`session.rs` — max 2-3 concurrents, merges séquentiels.
Choix des audits concurrents par recouvrement de fichiers,
affiner avec la section `## Fichiers du fix` du rapport.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, lecture seule) : `src/disk_io_thread.cpp`, `src/disk_job.cpp`, `src/disk_job_fence.cpp`, `src/peer_connection.cpp` (incoming_piece)
  Checkout upstream, résolu dans l'ordre : `$LIBTORRENT_ROOT` si
  définie → `D:/Projet/libtorrent/libtorrent` (poste local) → sinon
  `git clone https://github.com/arvidn/libtorrent ../libtorrent`
  puis `git -C ../libtorrent checkout e7381c7bc` (cloud/CI).
- **Rust** (repo courant) : `crates/bitdaemon-disk/src/disk_io.rs`, `disk_cache.rs`, `disk_job*.rs`, `crates/bitdaemon-session/src/peer_conn.rs`, `torrent.rs`

## Checklist spécifique

- [ ] Dispatch `DiskWrite` : stamp `picker_gen`, fence ordering, part-file routing
- [ ] Positioned I/O : mapping bloc → offsets fichier multi-fichiers, sparse writes
- [ ] Completion `DiskCompleted::Write` : gate generation, bytes accounting avant verdict
- [ ] Lazy file opening : erreurs différées à la première I/O, pas à add_torrent
- [ ] Bounded `disk_buffer_pool` : back-pressure `bw_disk`/`num_peers_down_disk`
- [ ] Write coalescing : runs contigus, frontières de flush pièce-complète
- [ ] Fences : clear_piece/move_storage/delete_files vs writes en vol
- [ ] Timers engine : pas de starvation des disk completions

## Protocole — deux modes

L'invocation précise le mode ; **défaut : `rapport`**. Le découpage existe
parce que plusieurs audits peuvent tourner en parallèle : la phase
d'analyse est read-only sur le repo (zéro conflit), la phase de
correction s'exécute dans un worktree + branche dédiés, mergée via PR.

### Analyse commune (mode A l'exécute, mode B s'appuie sur le rapport)

1. **Cartographier le chemin Rust d'abord** (grep/lecture), sans lire upstream —
   noter la machine d'état, les gates, les compteurs, les alertes, l'ordre des effets.
2. **Lire les fonctions upstream correspondantes** dans le checkout
   résolu ci-dessus (baseline `e7381c7bc`, lecture seule — ne jamais
   modifier).
3. **Diff sémantique** : ordre des opérations, gates, compteurs `counters::`,
   alertes (type + payload + timing), gestion d'erreur, comportement filaire.

   Méthode : les fichiers upstream font 6-10k lignes (`torrent.cpp`,
   `peer_connection.cpp`, `session_impl.cpp`) — ne jamais les lire en
   bloc. `grep` le nom de la fonction visée, lire ±80 lignes autour,
   suivre les appels ; ça préserve la fenêtre de contexte. Avant de
   consigner une déviation, `grep` aussi les crates sous-jacentes
   (`bitdaemon-common`, `-core`, `-net`, `-disk`) : la logique C++ du
   fichier audité a pu être déplacée dans un helper d'une couche
   inférieure — une absence dans le fichier listé n'est pas une
   déviation.

### Mode A — `rapport` (read-only, 100% parallélisable)

4. Écrire le rapport dans `docs/audits/reports/A14_disk-write-pipeline.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A14 — rapport d'analyse

   ## Verdicts checklist
   Un verdict par item — `conforme` / `déviation` / `N-A` — chacun avec
   une preuve `fichier.rs:ligne` (ou réf. upstream `src/foo.cpp`,
   fonction). Tout item marqué `déviation` DOIT avoir une entrée dans
   `## Déviations`. Un rapport « tout conforme » sans preuve par item
   est incomplet — la preuve est ce qui rend le verdict vérifiable.
   - Dispatch `DiskWrite` : stamp `picker_gen`, fence ordering, part-file routing — verdict : … — preuve : …
   - Positioned I/O : mapping bloc → offsets fichier multi-fichiers, sparse writes — verdict : … — preuve : …
   - Completion `DiskCompleted::Write` : gate generation, bytes accounting avant verdict — verdict : … — preuve : …
   - Lazy file opening : erreurs différées à la première I/O, pas à add_torrent — verdict : … — preuve : …
   - Bounded `disk_buffer_pool` : back-pressure `bw_disk`/`num_peers_down_disk` — verdict : … — preuve : …
   - Write coalescing : runs contigus, frontières de flush pièce-complète — verdict : … — preuve : …
   - Fences : clear_piece/move_storage/delete_files vs writes en vol — verdict : … — preuve : …
   - Timers engine : pas de starvation des disk completions — verdict : … — preuve : …

   ## Déviations
   Chaque déviation porte un **sketch de fix concret** — hunk de diff
   ou edit-spec exacte (quel symbole, quel code à la place, réf.
   upstream à la ligne près). Un « fix proposé » vague (« ajouter une
   vérif ») est incomplet : l'apply doit pouvoir l'exécuter sans
   re-designer. Le sketch n'est PAS un patch garanti : il date de
   l'analyse, le contexte peut avoir bougé — l'apply l'applique en
   sémantique et boucle les gates. Format :
   - `fichier.rs:ligne` — comportement Rust actuel / attendu upstream
     (`src/foo.cpp`, fonction) — sévérité (wire-visible / état /
     cosmétique)
     - **sketch** : `avant → après` minimal (diff ou edit-spec) +
       réf. upstream `src/foo.cpp:ligne` + test de régression suggéré

   ## Fichiers du fix
   Fichiers Rust que les corrections toucheront (planification des vagues).

   ## Non-bugs tranchés
   Écarts volontaires, avec justification upstream.
   ```

   Ne modifier **aucun** fichier existant en dehors du rapport. Pas de
   build/test cargo — l'analyse ne compile pas (et `target/`
   verrouillé sérialiserait les audits parallèles).

5. **Commit du rapport sur `main`** — pathspec-limited : le `-- <path>`
   n'embarque que ce fichier, même si un audit voisin a stagé autre
   chose dans l'index partagé entre-temps :

   ```sh
   git add docs/audits/reports/A14_disk-write-pipeline.md
   git commit -m "docs(audit): rapport A14 disk-write-pipeline" \
       -- docs/audits/reports/A14_disk-write-pipeline.md
   ```

   Ce commit est **requis** : un worktree `apply` branché depuis `main`
   ne voit que du contenu commité. Les commits de rapports ne se
   conflitent jamais entre eux (fichiers disjoints, fast-forward).

6. **STOP côté code** : pas de fix, pas de gate. Le mode `apply` est
   une invocation séparée qui part du rapport commité **et relu**.

### Mode B — `apply` (worktree + branche dédiée)

Prérequis : le rapport `docs/audits/reports/A14_disk-write-pipeline.md` est
**commité sur `main` ET relu/validé par l'humain** — les déviations
contestées ont basculé dans `## Non-bugs tranchés`. Ne jamais lancer
`apply` sur un rapport brut : il peut contenir des faux positifs.
Si un seul audit tourne à la fois, le checkout principal suffit ; dès
que plusieurs `apply` sont parallèles, le worktree est obligatoire
(sinon `target/` partagé sérialise les builds, `cargo fmt --all` se
marchent dessus, et `cargo test` tourne sur le code transitoire des
autres).

7. **Worktree + branche** :

   ```sh
   git worktree add ../LibtorrentRust-a14 -b audit/14-disk-write-pipeline
   ```

   Travailler dans `D:/Projet/LibtorrentRust-a14` : `target/` isolé,
   builds réellement parallèles. `sccache` mutualise la compilation
   des deps entre worktrees : `RUSTC_WRAPPER=sccache` (bash) ou
   `$env:RUSTC_WRAPPER="sccache"` (PowerShell).

8. **Corriger en Rust idiomatique** — pas de translittération ; commenter
   les legs subtils avec leur référence upstream.
9. **Tests de régression** pour chaque déviation corrigée.
10. **Gates obligatoires** (dans le worktree) : `cargo fmt --all` ·
    `cargo clippy --workspace --all-targets -- -D warnings` ·
    `cargo test --workspace` · `cargo doc --workspace --no-deps`.
11. **Changelog en fragment** : écrire les entrées Keep-a-Changelog dans
    `docs/audits/changelog/A14.md` sous `### Fixed` (ou autre section) —
    **ne pas éditer `CHANGELOG.md`** (conflit de merge garanti entre
    audits parallèles ; agrégation post-merge via
    `python docs/audits/_aggregate_changelog.py`). `docs/PORTING.md` :
    n'éditer que les rows du périmètre de l'audit, si leur statut change.
12. Commit `fix(<crate>): <scope> parity — <mot-clé>` **sur la branche
    `audit/14-disk-write-pipeline`** — jamais sur `main`. Push la branche,
    `gh pr create`. Merges séquentiels (un audit à la fois) ; si conflit
    sur les fichiers chauds (`torrent.rs`/`peer_conn.rs`/`session.rs`),
    rebaser sur `main` frais puis re-run les gates avant merge.
13. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit, lien PR).
    Après merge : `git worktree remove ../LibtorrentRust-a14`.

## Pièges connus (checklists transverses)

- Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés.
- Legs silencieux upstream (validation early-return, `is_disconnecting`,
  `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- Ordre effets wire-visible : messages sortants dans l'ordre upstream exact.
- Comportement dead-code upstream à conserver inerte (ex. `cancel_block`
  post-`mark_as_writing` — `is_requested` false en `state_writing`).
- `unwrap`/`expect`/`todo!` interdits hors tests ; `#![forbid(unsafe_code)]`.
