---
name: audit-24-piece-picker
description: "Audit sémantique vs libtorrent — Piece picker — cas limites"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A24 — Piece picker — cas limites

**Périmètre** : reverse/partial/rarest-first, priorities, streaming, share mode, endgame busy, invariants..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/piece_picker.cpp` (pick_pieces, update_pieces, share mode legs)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/piece_picker.rs`, `request_blocks.rs`

## Checklist spécifique

- [ ] Rarest-first : weighted random, ordre des priority buckets
- [ ] Reverse : ordre de pick pour snubbed peers (convergence sur mêmes pièces)
- [ ] Partials : prefer_contiguous_blocks, prioritize_partials, time-critical d'abord
- [ ] Priorities : piece 0/dont_download skip, top-priority first, per-piece vs per-file
- [ ] Streaming : set_sequential_range, sequential mode (état picker + override priorities)
- [ ] Share mode : recalc_share_mode rebalance pour diffuser la rareté
- [ ] Endgame : busy blocks requestables en dernier (un busy par peer sauf TC)
- [ ] Invariants : num_peers == downloaders réels, refcount == peers ayant la pièce, transitions block_info

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
