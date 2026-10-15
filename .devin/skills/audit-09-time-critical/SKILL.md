---
name: audit-09-time-critical
description: "Audit sémantique vs libtorrent — Pipeline time-critical (deadlines)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A09 — Pipeline time-critical (deadlines)

**Périmètre** : set_piece_deadline, request_time_critical_pieces, make_time_critical, cancel_non_critical, expiration de deadlines..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/torrent.cpp` (set_piece_deadline, update_piece_deadline, request_time_critical_pieces), `src/peer_connection.cpp` (make_time_critical, can_request_time_critical)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/torrent.rs` (time_critical map, cancel_non_critical), `peer_conn.rs` (make_time_critical, queued_time_critical)

## Checklist spécifique

- [ ] Deadlines : insertion, watch via ticker, alert expiry → recancel
- [ ] `queued_time_critical` : compteur de préfixe exact (insert/remove/choke-clear/shift)
- [ ] make_time_critical : déplacement vers le préfixe, refus si déjà critique ou absent
- [ ] cancel_non_critical = `cancel_request(force=true)` : not_wanted marking, wire Cancel conditionnel, abort picker — les deux files
- [ ] Priorité picking : les blocs critiques passent devant le pick normal
- [ ] watch-timeouts vs piece timeout : indépendance

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
