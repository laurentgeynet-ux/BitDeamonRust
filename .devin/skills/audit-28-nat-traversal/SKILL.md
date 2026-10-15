---
name: audit-28-nat-traversal
description: "Audit sémantique vs libtorrent — NAT traversal (LSD/UPnP/NAT-PMP)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A28 — NAT traversal (LSD/UPnP/NAT-PMP)

**Périmètre** : LSD multicast, UPnP IGD lifecycle, NAT-PMP/PCP mappings, external IP election..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/lsd.cpp`, `src/upnp.cpp`, `src/natpmp.cpp`, `src/session_impl.cpp` (portmap legs)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/lsd*.rs`, `upnp*.rs`, `natpmp*.rs`, `session.rs`/`engine.rs`

## Checklist spécifique

- [ ] LSD : multicast group/port, cadence par torrent, cookie, receive → peer feed, off en anonymous_mode
- [ ] UPnP : SSDP discover, WANIP/WANPPP, AddPortMapping + renew, DeletePortMapping au stop
- [ ] NAT-PMP/PCP : mapping request, external address, epoch, renew, delete
- [ ] External IP election : consensus trackers/DHT/UPnP/NAT-PMP → external_ip_alert
- [ ] portmap alerts : mapping add/error/log transport+protocol
- [ ] Mapping per listen port : tcp+udp, proto field correct

## Protocole — deux modes

L'invocation précise le mode ; **défaut : `rapport`**. Le découpage existe
parce que plusieurs audits peuvent tourner en parallèle : la phase
d'analyse est read-only sur le repo (zéro conflit), la phase de
correction s'exécute dans un worktree + branche dédiés, mergée via PR.

### Analyse commune (mode A l'exécute, mode B s'appuie sur le rapport)

1. **Cartographier le chemin Rust d'abord** (grep/lecture), sans lire upstream —
   noter la machine d'état, les gates, les compteurs, les alertes, l'ordre des effets.
2. **Lire les fonctions upstream correspondantes** dans `D:/Projet/libtorrent/libtorrent`
   (baseline `e7381c7bc`, lecture seule — ne jamais modifier).
3. **Diff sémantique** : ordre des opérations, gates, compteurs `counters::`,
   alertes (type + payload + timing), gestion d'erreur, comportement filaire.

### Mode A — `rapport` (read-only, 100% parallélisable)

4. Écrire le rapport dans `docs/audits/reports/A28_nat-traversal.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A28 — rapport d'analyse

   ## Déviations
   - `fichier.rs:ligne` — comportement Rust actuel / attendu upstream
     (`src/foo.cpp`, fonction) — fix proposé — sévérité
     (wire-visible / état / cosmétique)

   ## Fichiers du fix
   Fichiers Rust que les corrections toucheront (planification des vagues).

   ## Non-bugs tranchés
   Écarts volontaires, avec justification upstream.
   ```

   Ne modifier **aucun** fichier existant. Pas de build/test cargo —
   l'analyse ne compile pas (et `target/` verrouillé sérialiserait les
   audits parallèles).

5. **STOP.** Pas de fix, pas de gate, pas de commit. Le mode `apply`
   est une invocation séparée qui part de ce rapport.

### Mode B — `apply` (worktree + branche dédiée)

Prérequis : le rapport `docs/audits/reports/A28_nat-traversal.md` existe.
Si un seul audit tourne à la fois, le checkout principal suffit ; dès
que plusieurs `apply` sont parallèles, le worktree est obligatoire
(sinon `target/` partagé sérialise les builds, `cargo fmt --all` se
marchent dessus, et `cargo test` tourne sur le code transitoire des
autres).

4. **Worktree + branche** :

   ```sh
   git worktree add ../LibtorrentRust-a28 -b audit/28-nat-traversal
   ```

   Travailler dans `D:/Projet/LibtorrentRust-a28` : `target/` isolé,
   builds réellement parallèles. `sccache` (`RUSTC_WRAPPER=sccache`)
   mutualise la compilation des deps entre worktrees.

5. **Corriger en Rust idiomatique** — pas de translittération ; commenter
   les legs subtils avec leur référence upstream.
6. **Tests de régression** pour chaque déviation corrigée.
7. **Gates obligatoires** (dans le worktree) : `cargo fmt --all` ·
   `cargo clippy --workspace --all-targets -- -D warnings` ·
   `cargo test --workspace` · `cargo doc --workspace --no-deps`.
8. **Changelog en fragment** : écrire les entrées Keep-a-Changelog dans
   `docs/audits/changelog/A28.md` sous `### Fixed` (ou autre section) —
   **ne pas éditer `CHANGELOG.md`** (conflit de merge garanti entre
   audits parallèles ; agrégation post-merge via
   `python docs/audits/_aggregate_changelog.py`). `docs/PORTING.md` :
   n'éditer que les rows du périmètre de l'audit, si leur statut change.
9. Commit `fix(<crate>): <scope> parity — <mot-clé>` **sur la branche
   `audit/28-nat-traversal`** — jamais sur `main`. Push la branche,
   `gh pr create`. Merges séquentiels (un audit à la fois) ; si conflit
   sur les fichiers chauds (`torrent.rs`/`peer_conn.rs`/`session.rs`),
   rebaser sur `main` frais puis re-run les gates avant merge.
10. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit, lien PR).
    Après merge : `git worktree remove ../LibtorrentRust-a28`.

## Pièges connus (checklists transverses)

- Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés.
- Legs silencieux upstream (validation early-return, `is_disconnecting`,
  `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- Ordre effets wire-visible : messages sortants dans l'ordre upstream exact.
- Comportement dead-code upstream à conserver inerte (ex. `cancel_block`
  post-`mark_as_writing` — `is_requested` false en `state_writing`).
- `unwrap`/`expect`/`todo!` interdits hors tests ; `#![forbid(unsafe_code)]`.
