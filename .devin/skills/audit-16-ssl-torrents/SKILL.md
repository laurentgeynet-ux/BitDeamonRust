---
name: audit-16-ssl-torrents
description: "Audit sémantique vs libtorrent — Torrents SSL"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A16 — Torrents SSL

**Périmètre** : ssl_stream : fingerprint cert vs info-hash, mutual TLS, cert settings, listen ssl, alertes..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/ssl_stream.cpp`, `include/libtorrent/ssl_stream.hpp`, `src/torrent.cpp` (ssl legs), `src/session_impl.cpp` (ssl listen)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/ssl_stream.rs`, `crates/bitdaemon-session/src/session.rs` (ssl listen legs), `engine.rs`

## Checklist spécifique

- [ ] Fingerprint : digest cert SHA-256/SHA-1 vs info-hash v1/v2 → règles d'acceptation exactes
- [ ] Mutual TLS : verify_peer + verify_fail_if_no_peer_cert, client cert depuis `ssl_cert`/`ssl_key`
- [ ] verify_callback : chain custom — CN/SAN ne font pas foi d'identité torrent upstream
- [ ] ssl_listen_port, accept inbound ssl, flags (`is_ssl_torrent`, `m_ssl`)
- [ ] Alertes : erreurs peer_ssl → mapping disconnect reason
- [ ] crypto provider : builder_with_provider exhaustif (suite du fix récent)

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

4. Écrire le rapport dans `docs/audits/reports/A16_ssl-torrents.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A16 — rapport d'analyse

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

Prérequis : le rapport `docs/audits/reports/A16_ssl-torrents.md` existe.
Si un seul audit tourne à la fois, le checkout principal suffit ; dès
que plusieurs `apply` sont parallèles, le worktree est obligatoire
(sinon `target/` partagé sérialise les builds, `cargo fmt --all` se
marchent dessus, et `cargo test` tourne sur le code transitoire des
autres).

4. **Worktree + branche** :

   ```sh
   git worktree add ../LibtorrentRust-a16 -b audit/16-ssl-torrents
   ```

   Travailler dans `D:/Projet/LibtorrentRust-a16` : `target/` isolé,
   builds réellement parallèles. `sccache` (`RUSTC_WRAPPER=sccache`)
   mutualise la compilation des deps entre worktrees.

5. **Corriger en Rust idiomatique** — pas de translittération ; commenter
   les legs subtils avec leur référence upstream.
6. **Tests de régression** pour chaque déviation corrigée.
7. **Gates obligatoires** (dans le worktree) : `cargo fmt --all` ·
   `cargo clippy --workspace --all-targets -- -D warnings` ·
   `cargo test --workspace` · `cargo doc --workspace --no-deps`.
8. **Changelog en fragment** : écrire les entrées Keep-a-Changelog dans
   `docs/audits/changelog/A16.md` sous `### Fixed` (ou autre section) —
   **ne pas éditer `CHANGELOG.md`** (conflit de merge garanti entre
   audits parallèles ; agrégation post-merge via
   `python docs/audits/_aggregate_changelog.py`). `docs/PORTING.md` :
   n'éditer que les rows du périmètre de l'audit, si leur statut change.
9. Commit `fix(<crate>): <scope> parity — <mot-clé>` **sur la branche
   `audit/16-ssl-torrents`** — jamais sur `main`. Push la branche,
   `gh pr create`. Merges séquentiels (un audit à la fois) ; si conflit
   sur les fichiers chauds (`torrent.rs`/`peer_conn.rs`/`session.rs`),
   rebaser sur `main` frais puis re-run les gates avant merge.
10. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit, lien PR).
    Après merge : `git worktree remove ../LibtorrentRust-a16`.

## Pièges connus (checklists transverses)

- Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés.
- Legs silencieux upstream (validation early-return, `is_disconnecting`,
  `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- Ordre effets wire-visible : messages sortants dans l'ordre upstream exact.
- Comportement dead-code upstream à conserver inerte (ex. `cancel_block`
  post-`mark_as_writing` — `is_requested` false en `state_writing`).
- `unwrap`/`expect`/`todo!` interdits hors tests ; `#![forbid(unsafe_code)]`.
