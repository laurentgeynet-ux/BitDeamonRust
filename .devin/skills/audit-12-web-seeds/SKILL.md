---
name: audit-12-web-seeds
description: "Audit sémantique vs libtorrent — Web seeds (HTTP peer)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A12 — Web seeds (HTTP peer)

**Périmètre** : web_peer_connection/http_seed_connection : GET ranges, keep-alive, erreurs, retry, comptabilité par seed..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/web_peer_connection.cpp`, `src/http_seed_connection.cpp`, `src/web_connection_base.cpp`, `src/torrent.cpp` (retry_web_seed, maybe_connect_web_seeds)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/web_seed*`, `crates/bitdaemon-session/src/session.rs` (on_web_seed_*), `torrent.rs` (web_seeds)

## Checklist spécifique

- [ ] Range requests : pièces complètes vs blocs, `block_size` alignment, coalescing contigu
- [ ] Erreurs : HTTP non-200, redirect loops, content-length mismatch, EOF mid-piece → url_seed_alert + seed marqué
- [ ] Retry : backoff par seed, `retry_web_seed`, bannissement temporaire
- [ ] Attribution : erreurs/compteurs par URL (cohérence du fix récent `on_web_seed_error(url)` sur les deux legs)
- [ ] Request pacing : `web_seed_blocks_per_tick`, connection reuse vs reconnect
- [ ] Pseudo-peer `web_seed_t` : download queue partagée, legs upload N/A

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
