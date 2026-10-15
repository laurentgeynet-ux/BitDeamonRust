---
name: audit-18-disk-cache-pool
description: "Audit sémantique vs libtorrent — Cache disque & buffer pool"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A18 — Cache disque & buffer pool

**Périmètre** : Coalescing, watermark, éviction, dirty expiry, bornes pool, back-pressure cache..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/disk_buffer_pool.cpp`, `src/store_buffer.cpp`, `src/disk_io_thread.cpp` (cache legs), `src/disk_job.cpp` (FLUSH)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-disk/src/disk_cache.rs`, `disk_buffer*`, `disk_io.rs`

## Checklist spécifique

- [ ] Coalescing : runs contigus par pièce, un syscall par run
- [ ] Watermark : seuil haut blocage vs bas déblocage, hystérésis exacte
- [ ] Éviction : ordre upstream, dirty expiry `cache_expiry` flushes stale
- [ ] Pool : `max_queued_disk_bytes` cap, check_buffer_level, job_done comptage
- [ ] Read-side : consult cache avant disque (prepare_read/get2/partial_read), spanning reads
- [ ] FLUSH_PIECE/force_flush_flag drain une pièce indépendamment du watermark
- [ ] Superseded/teardown writers : abort exactly-once

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
