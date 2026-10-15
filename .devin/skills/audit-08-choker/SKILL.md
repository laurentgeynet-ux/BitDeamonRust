---
name: audit-08-choker
description: "Audit sémantique vs libtorrent — Choker & optimistic unchoke"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A08 — Choker & optimistic unchoke

**Périmètre** : Sélection des pairs unchokés : comparateurs, quotas, rotation optimiste, interaction snub/seed..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/choker.cpp` (unchoke_sort), `src/peer_connection.cpp` (choke_this_peer, maybe_unchoke_this_peer), `src/session_impl.cpp` (unchoke tick)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/choker.rs`, `torrent.rs` (unchoke legs), `session.rs`

## Checklist spécifique

- [ ] `unchoke_sort` verbatim : comparateurs rate-based/RR/fastest/anti-leech, quota `piece_length·quota` + 60 s, nth_element top-slots
- [ ] Slots : fixed vs rate-based, `unchoke_slots_limit`, seed expansions
- [ ] Optimistic unchoke : période, rotation, `num_opt_unchoke`
- [ ] Snub/parole/endgame peers exclus ou inclus selon upstream
- [ ] Choke/unchoke effects : envoi msg + `expect_choke` mark + pending requests handling
- [ ] peer_class filters dans le calcul des slots

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
