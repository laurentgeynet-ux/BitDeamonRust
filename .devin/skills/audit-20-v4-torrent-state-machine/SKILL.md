---
name: audit-20-v4-torrent-state-machine
description: "[V4] Audit sémantique vs libtorrent — Machine d'état torrent"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A20 — Machine d'état torrent

**Périmètre** : Transitions add→checking→downloading→finished→seeding, pauses, state_updated/save_resume triggers, seed_mode..

**Vague d'apply** : V4 (session-core) — tous sur `torrent.rs`/`peer_conn.rs`/`session.rs` — max 2-3 concurrents, merges séquentiels.
Choix des audits concurrents par recouvrement de fichiers,
affiner avec la section `## Fichiers du fix` du rapport.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, lecture seule) : `src/torrent.cpp` (set_state, files_checked, on_torrent_paused, do_pause/do_resume, finished, start/stop_announcing)
  Checkout upstream, résolu dans l'ordre : `$LIBTORRENT_ROOT` si
  définie → `D:/Projet/libtorrent/libtorrent` (poste local) → sinon
  `git clone https://github.com/arvidn/libtorrent ../libtorrent`
  puis `git -C ../libtorrent checkout e7381c7bc` (cloud/CI).
- **Rust** (repo courant) : `crates/bitdaemon-session/src/torrent.rs` (set_state, checking flags, finished/seeding), `session.rs`

## Checklist spécifique

- [ ] Ladder : checking_files/checking_resume/downloading_metadata/downloading/finished/seeding + transitions interdites
- [ ] paused : `m_paused | m_session_paused`, graceful pause (announces stop, disconnects différés)
- [ ] `state_updated()` sites exhaustifs (status change → save_resume_data_alert au bon moment)
- [ ] `need_save_resume` — CHAQUE site upstream (if_counters_changed, if_config_changed, if_download_progress, if_state_changed)
- [ ] seed_mode : entrée (flag + all pieces + no dont_download), sortie → force_recheck
- [ ] auto_managed : transitions paused↔queued↔active, ordre `manage_queue`
- [ ] stop_when_ready : edge finished → pause

## Protocole — deux modes

L'invocation précise le mode ; **défaut : `rapport`**. Le découpage existe
parce que plusieurs audits peuvent tourner en parallèle : la phase
d'analyse est read-only sur le repo (zéro conflit), la phase de
correction s'exécute dans un worktree + branche dédiés ; le merge est
local et décidé par l'humain.

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

4. Écrire le rapport dans `docs/audits/reports/A20_torrent-state-machine.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A20 — rapport d'analyse

   ## Verdicts checklist
   Un verdict par item — `conforme` / `déviation` / `N-A` — chacun avec
   une preuve `fichier.rs:ligne` (ou réf. upstream `src/foo.cpp`,
   fonction). Tout item marqué `déviation` DOIT avoir une entrée dans
   `## Déviations`. Un rapport « tout conforme » sans preuve par item
   est incomplet — la preuve est ce qui rend le verdict vérifiable.
   - Ladder : checking_files/checking_resume/downloading_metadata/downloading/finished/seeding + transitions interdites — verdict : … — preuve : …
   - paused : `m_paused | m_session_paused`, graceful pause (announces stop, disconnects différés) — verdict : … — preuve : …
   - `state_updated()` sites exhaustifs (status change → save_resume_data_alert au bon moment) — verdict : … — preuve : …
   - `need_save_resume` — CHAQUE site upstream (if_counters_changed, if_config_changed, if_download_progress, if_state_changed) — verdict : … — preuve : …
   - seed_mode : entrée (flag + all pieces + no dont_download), sortie → force_recheck — verdict : … — preuve : …
   - auto_managed : transitions paused↔queued↔active, ordre `manage_queue` — verdict : … — preuve : …
   - stop_when_ready : edge finished → pause — verdict : … — preuve : …

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
   git add docs/audits/reports/A20_torrent-state-machine.md
   git commit -m "docs(audit): rapport A20 torrent-state-machine" \
       -- docs/audits/reports/A20_torrent-state-machine.md
   ```

   Ce commit est **requis** : un worktree `apply` branché depuis `main`
   ne voit que du contenu commité. Les commits de rapports ne se
   conflitent jamais entre eux (fichiers disjoints, fast-forward).

6. **STOP côté code** : pas de fix, pas de gate. Le mode `apply` est
   une invocation séparée qui part du rapport commité **et relu**.

### Mode B — `apply` (worktree + branche dédiée)

Prérequis : le rapport `docs/audits/reports/A20_torrent-state-machine.md` est
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
   git worktree add ../LibtorrentRust-a20 -b audit/20-torrent-state-machine
   ```

   Travailler dans `D:/Projet/LibtorrentRust-a20` : `target/` isolé,
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
    `docs/audits/changelog/A20.md` sous `### Fixed` (ou autre section) —
    **ne pas éditer `CHANGELOG.md`** (conflit de merge garanti entre
    audits parallèles ; agrégation post-merge via
    `python docs/audits/_aggregate_changelog.py`).
    **`docs/PORTING.md` — obligatoire** : enrichir les rows du périmètre
    avec ce que le fix a changé en parité (détails wire, compteurs,
    sémantique d'erreur — la colonne notes documente l'état réel, pas
    seulement le statut). Mettre le statut à jour si le fix le change
    (`ported` → `verified`…). Les edits sont par row → recouvrements
    quasi nuls entre apply parallèles ; en cas de conflit au merge,
    rebase + gates comme pour le reste.
12. Commit `fix(<crate>): <scope> parity — <mot-clé>` **sur la branche
    `audit/20-torrent-state-machine`** — jamais sur `main`. **Tout reste local** :
    pas de push, pas de PR — le merge est décidé et exécuté par
    l'humain (`git merge audit/20-torrent-state-machine` dans le checkout
    principal). Signaler « prêt à merger » et s'arrêter là. Si le
    merge révèle des conflits (`torrent.rs`/`peer_conn.rs`/
    `session.rs`/`PORTING.md`) : rebase de la branche sur `main` frais
    **dans le worktree** + re-run des gates, puis redonner le feu.
13. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit).
    **Hygiène post-merge** (après merge par l'humain) : `git worktree
    remove ../LibtorrentRust-a20` puis `git branch -d
    audit/20-torrent-state-machine`. Une branche mergée qui traîne encombre la vue
    et trompe la planification.

## Pièges connus (checklists transverses)

- Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés.
- Legs silencieux upstream (validation early-return, `is_disconnecting`,
  `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- Ordre effets wire-visible : messages sortants dans l'ordre upstream exact.
- Comportement dead-code upstream à conserver inerte (ex. `cancel_block`
  post-`mark_as_writing` — `is_requested` false en `state_writing`).
- `unwrap`/`expect`/`todo!` interdits hors tests ; `#![forbid(unsafe_code)]`.
