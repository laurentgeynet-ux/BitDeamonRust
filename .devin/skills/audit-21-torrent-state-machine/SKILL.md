---
name: audit-21-torrent-state-machine
description: "Audit sémantique vs libtorrent — Machine d'état torrent"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A21 — Machine d'état torrent

**Périmètre** : Transitions add→checking→downloading→finished→seeding, pauses, state_updated/save_resume triggers, seed_mode..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/torrent.cpp` (set_state, files_checked, on_torrent_paused, do_pause/do_resume, finished, start/stop_announcing)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/torrent.rs` (set_state, checking flags, finished/seeding), `session.rs`

## Checklist spécifique

- [ ] Ladder : checking_files/checking_resume/downloading_metadata/downloading/finished/seeding + transitions interdites
- [ ] paused : `m_paused | m_session_paused`, graceful pause (announces stop, disconnects différés)
- [ ] `state_updated()` sites exhaustifs (status change → save_resume_data_alert au bon moment)
- [ ] `need_save_resume` — CHAQUE site upstream (if_counters_changed, if_config_changed, if_download_progress, if_state_changed)
- [ ] seed_mode : entrée (flag + all pieces + no dont_download), sortie → force_recheck
- [ ] auto_managed : transitions paused↔queued↔active, ordre `manage_queue`
- [ ] stop_when_ready : edge finished → pause

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
