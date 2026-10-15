---
name: audit-19-partfile-filepool
description: "Audit sémantique vs libtorrent — part_file & file_pool"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A19 — part_file & file_pool

**Périmètre** : Slots part-file, export, migration priorités, récupération corruption ; pool fd LRU, caps, erreurs reopen..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/part_file.cpp`, `src/file_pool.cpp`, `src/posix_part_file.cpp`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-disk/src/part_file.rs`, `file_pool.rs`

## Checklist spécifique

- [ ] part_file : header format (magic, version, endianness!), slot alloc/free FIFO, export_on_flush, bit `flushed`
- [ ] Corruption : header illisible → comportement upstream, missing file, taille mismatch
- [ ] Migration : prioritize → bytes straddling vers .parts, export quand pièce complète
- [ ] file_pool : LRU eviction, cap `file_pool_size`, open/upgrade read→write, reopen on error
- [ ] Sparse/allocate flags propagés, `file_open_failed` retry

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
