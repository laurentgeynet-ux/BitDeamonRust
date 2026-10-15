---
name: audit-20-resume-data
description: "Audit sémantique vs libtorrent — Resume data — round-trip + snapshot sous I/O"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A20 — Resume data — round-trip + snapshot sous I/O

**Périmètre** : read/write resume complet, verify_resume_data verdicts, cohérence du snapshot pendant écritures en vol..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/read_resume_data.cpp`, `src/write_resume_data.cpp`, `src/torrent.cpp` (check_fastresume, on_resume_data_checked, save_resume_data)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/session.rs`/`torrent.rs` (resume paths), `core::resume*`

## Checklist spécifique

- [ ] Champs : pieces bitfield, unfinished, priorities, trackers, peers+banned, seeds_dht, limits, totals, active/seeding time, flags (upload/share/seed/auto_managed/paused/apply_ip_filter), save_path, renamed/mapped_files, file_format_version, ih v2, comment/created_by/creation_date
- [ ] verify_resume_data verdicts : no_resume/need_full_check, fichiers manquants/modifiés/tailles, `partial`
- [ ] Snapshot sous I/O : dirty cache blocks cohérents avec le disque post-reload — écritures en vol + fences pendant write_resume_data
- [ ] Round-trip BIDIRECTIONNEL : fichier upstream → lu ici ; écrit ici → lu par upstream (golden vectors)
- [ ] Versions : upgrade/downgrade du format resume entre versions libtorrent
- [ ] save_resume_data timing : dirty flags, déclencheurs `need_save_resume` exhaustifs

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
