---
name: audit-01-disk-write-pipeline
description: "Audit sémantique vs libtorrent — Pipeline disque — bloc entrant → écriture" (re-vérification — déjà passé)
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A01 — Pipeline disque — bloc entrant → écriture

> **Déjà passé** (done — audit initial + commit `5541959`). Le relancer = re-vérifier que les
> correctifs tiennent toujours (non-régression) et que la checklist
> reste verte après les changements intermédiaires.

**Périmètre** : incoming block → async_write → cache → flush → completion.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/disk_io_thread.cpp`, `src/disk_job.cpp`, `src/disk_job_fence.cpp`, `src/peer_connection.cpp` (incoming_piece)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-disk/src/disk_io.rs`, `disk_cache.rs`, `disk_job*.rs`, `crates/bitdaemon-session/src/peer_conn.rs`, `torrent.rs`

## Checklist spécifique

- [ ] Dispatch `DiskWrite` : stamp `picker_gen`, fence ordering, part-file routing
- [ ] Positioned I/O : mapping bloc → offsets fichier multi-fichiers, sparse writes
- [ ] Completion `DiskCompleted::Write` : gate generation, bytes accounting avant verdict
- [ ] Lazy file opening : erreurs différées à la première I/O, pas à add_torrent
- [ ] Bounded `disk_buffer_pool` : back-pressure `bw_disk`/`num_peers_down_disk`
- [ ] Write coalescing : runs contigus, frontières de flush pièce-complète
- [ ] Fences : clear_piece/move_storage/delete_files vs writes en vol
- [ ] Timers engine : pas de starvation des disk completions

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
