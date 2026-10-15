---
name: audit-25-counters-sweep
description: "Audit sémantique vs libtorrent — Sweep conformité compteurs/stats"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A25 — Sweep conformité compteurs/stats

**Périmètre** : Chaque `Counter` ↔ sites upstream inc/dec/set ; session_stats snapshot ; sliding_average..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `include/libtorrent/performance_counters.hpp`, tous les `inc_stats_counter`/`set_value`/`blend_stats_counter`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/counters.rs` + tous les sites d'appel

## Checklist spécifique

- [ ] Exhaustif : pour CHAQUE counter upstream, le site Rust correspondant (ou N/A documenté)
- [ ] Jamais incrémentés : pattern trouvé 2× — suspecter tous les compteurs
- [ ] Gauges transitionnels (num_peers_*) : inc/dec aux bons edges, pas de drift
- [ ] sliding_average vs add-blend : request_time, request_latency, rates
- [ ] session_stats_metrics : noms/indices exposés aux clients
- [ ] waste_reason totals, disk_job counters, peer_class channels

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
