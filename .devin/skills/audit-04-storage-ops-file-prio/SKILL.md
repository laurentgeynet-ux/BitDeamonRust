---
name: audit-04-storage-ops-file-prio
description: "Audit sémantique vs libtorrent — Ops storage & priorités fichiers" (re-vérification — déjà passé)
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A04 — Ops storage & priorités fichiers

> **Déjà passé** (done — commit `24fffc8`). Le relancer = re-vérifier que les
> correctifs tiennent toujours (non-régression) et que la checklist
> reste verte après les changements intermédiaires.

**Périmètre** : move_storage / delete_files / rename_file / prioritize_files / file_priority + complétions.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/torrent.cpp` (move_storage, delete_files, rename_file, prioritize_files, on_file_priority), `src/storage_utils.cpp` (move_storage rollback, delete_files), `src/session_impl.cpp` (remove_torrent)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/session.rs`, `torrent.rs`, `session_handle.rs`, `alerts.rs`

## Checklist spécifique

- [ ] `prioritize_files` async : `SetFilePriority` → `async_set_file_priority` → `on_file_priority` — jamais inline
- [ ] Sérialisation `outstanding_file_priority` + `deferred_file_priorities` (merge par index, replay à la complétion)
- [ ] Vecteur appliqué = storage-ajusté, pas demandé ; `file_prio` posté à la complétion seulement ; `file_error` → set_error+pause
- [ ] `fix_priorities` : force `dont_download` sur pads avant dispatch ; migration part-file
- [ ] `move_storage` : status bits (`need_full_check`→recheck, `fatal_disk_error`/`file_exist`→failure), `old_path` dans l'alerte, rollback disque
- [ ] `rename_file` : bounds layout (metadata-less → silent), `old_name` résolu avant `renamed_files`, dirty dans la complétion
- [ ] `delete_files` : fence'd disk job → `torrent_deleted{,_failed}`, no-disk leg → remove silencieux
- [ ] Add-time `atp.file_priorities` + magnet init : apply direct picker, pas de job ni alerte

## Protocole (fixe — même méthodologie que les audits précédents)

1. **Cartographier le chemin Rust d'abord** (grep/lecture), sans lire upstream —
   noter la machine d'état, les gates, les compteurs, les alertes, l'ordre des effets.
2. **Lire les fonctions upstream correspondantes** dans `D:/Projet/libtorrent/libtorrent`
   (baseline `e7381c7bc`, lecture seule — ne jamais modifier).
3. **Diff sémantique** : ordre des opérations, gates, compteurs `counters::`,
   alertes (type + payload + timing), gestion d'erreur, comportement filaire.
4. **Corriger en Rust idiomatique** — pas de translittération ; commenter les
   legs subtils avec leur référence upstream.
5. **Tests de régression** pour chaque déviation trouvée.
6. **Gates obligatoires** : `cargo fmt --all` · `cargo clippy --workspace --all-targets -- -D warnings` ·
   `cargo test --workspace` · `cargo doc --workspace --no-deps`.
7. Mettre à jour `docs/PORTING.md` (row concernée) + `CHANGELOG.md`.
8. Commit `fix(<crate>): <scope> parity — <mot-clé>` + push `origin/main`.
9. **Rapport final** : déviations trouvées/corrigées, non-bugs tranchés
   (avec justification upstream), tests ajoutés, hash du commit.

## Pièges connus (checklists transverses)

- Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés.
- Legs silencieux upstream (validation early-return, `is_disconnecting`,
  `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- Ordre effets wire-visible : messages sortants dans l'ordre upstream exact.
- Comportement dead-code upstream à conserver inerte (ex. `cancel_block`
  post-`mark_as_writing` — `is_requested` false en `state_writing`).
- `unwrap`/`expect`/`todo!` interdits hors tests ; `#![forbid(unsafe_code)]`.
