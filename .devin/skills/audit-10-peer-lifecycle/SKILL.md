---
name: audit-10-peer-lifecycle
description: "Audit sémantique vs libtorrent — Cycle de vie des pairs & admission"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A10 — Cycle de vie des pairs & admission

**Périmètre** : attach/detach, états de connexion, limites half-open, taxonomie des disconnects, admission entrante..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/torrent.cpp` (attach_peer, remove_peer, connect_to_peer, peer_list), `src/session_impl.cpp` (incoming conn gates, max half-open)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/torrent.rs` (attach_peer/remove_peer), `session.rs` (connect pump), `peer_list.rs`

## Checklist spécifique

- [ ] Ordre attach : refcount picker, `set_peer_class`, interested eval, bitfield init
- [ ] remove_peer : abort outstanding (gate `!timed_out && !not_wanted` !), refcount dec, stats fold, gauges
- [ ] Disconnect taxonomy : reason + `failed` flag → `get_peer_list_entry` failcount vs clean
- [ ] Half-open limits (`m_half_open`), connect_backoff exponentiel, prioritisation des peers récents
- [ ] Admission inbound : no-torrent silent drop, duplicate conn handling, peer_id conflicts
- [ ] ip_filter/port_filter application aux conns actives (disconnect + peer_blocked_alert)
- [ ] web seeds comme pseudo-peers (WEB_SEED_PEER key) — ne pas casser

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
