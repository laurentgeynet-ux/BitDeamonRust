---
name: audit-23-settings-session-state
description: "Audit sémantique vs libtorrent — Settings hot-apply & .session"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A23 — Settings hot-apply & .session

**Périmètre** : apply_settings legs par clé, session_params/.session round-trip, deprecated keys..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/settings_pack.cpp`, `src/session_impl.cpp` (apply_settings, load_state, save_state), `src/session_params.cpp`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/settings.rs` (apply_settings legs), `session.rs` (SessionState)

## Checklist spécifique

- [ ] apply_settings : chaque clé ayant un effet live vs celles ne s'appliquant qu'au prochain storage/conn
- [ ] Clés sans effet live : documenté comme upstream
- [ ] .session file : layout dict, ip_filter4/6, dht_state embed, version compat
- [ ] Deprecated keys : conservés en écriture pour round-trip interop?
- [ ] Diff ciblé : parmi les 266 keys validées par défaut, lesquelles sont réellement consommées

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
