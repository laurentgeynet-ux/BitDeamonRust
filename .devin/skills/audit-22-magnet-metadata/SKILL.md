---
name: audit-22-magnet-metadata
description: "Audit sémantique vs libtorrent — Magnet & acquisition metadata"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A22 — Magnet & acquisition metadata

**Périmètre** : Magnet add, ut_metadata pacing, info-hash verify, priorités/trackers conservés, ops sans metadata..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/torrent.cpp` (init, on_metadata, set_metadata), `src/ut_metadata.cpp` (request pacing)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/torrent.rs` (init/magnet path), `session.rs` (metadata dispatch)

## Checklist spécifique

- [ ] Phases : magnet add → metadata download → verify hash → init picker/files → apply pending state
- [ ] ut_metadata pacing upstream (intervalle entre requêtes, ban on flood)
- [ ] pending state préservé : file/piece priorities, trackers, peers, flags à l'arrivée metadata
- [ ] Ops metadata-less : API qui doivent no-op ou defer (file_priority bounds, trackers add, save_resume)
- [ ] Metadata reject : hash mismatch → ban peers contributeurs, retry autres
- [ ] info re-encode : jamais re-sérialiser info pour le hash (bytes verbatim)

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
