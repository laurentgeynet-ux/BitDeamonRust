---
name: audit-27-engine-glue
description: "Audit sémantique vs libtorrent — Glue engine/session"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A27 — Glue engine/session

**Périmètre** : engine.rs : boucle dispatch, ordonnancement commandes, timers, pont disque, starvation..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/session_impl.cpp` (dispatch loop ordering) — l'équivalent local est `engine.rs`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon/src/engine.rs`

## Checklist spécifique

- [ ] Ordonnancement : ordre commandes vs ticks vs disk completions vs net events
- [ ] Tick starvation : le bug corrigé — vérifier toutes les sources de réveil
- [ ] Command channel : ordre FIFO préservé ? priorités ?
- [ ] Alert routing : broadcast + notify + wait_for_alert
- [ ] Peer pump : connect/dial ordering, acceptor pump, handshake_timeout
- [ ] Timers : chaque deadline upstream a son tick (second_tick, unchoke, stats, lsd, auto-scrape, peer_turnover)

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
