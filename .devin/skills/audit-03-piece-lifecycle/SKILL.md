---
name: audit-03-piece-lifecycle
description: "Audit sémantique vs libtorrent — Cycle de vie des pièces" (re-vérification — déjà passé)
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A03 — Cycle de vie des pièces

> **Déjà passé** (done — commit `e957a83`). Le relancer = re-vérifier que les
> correctifs tiennent toujours (non-régression) et que la checklist
> reste verte après les changements intermédiaires.

**Périmètre** : incoming_piece → flush → hash → piece_verified → Have → hash_failed → restore.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/torrent.cpp` (on_piece_verified, piece_passed, piece_failed, on_piece_sync, on_piece_fail_sync, we_have, penalize_peers, received_piece, announce_piece)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/torrent.rs`, `piece_picker.rs`, `session.rs`

## Checklist spécifique

- [ ] `picker_gen` gates write ET hash completions — in-flight invalidé par force_recheck/inconsistent
- [ ] hash-fail : `lock_piece` + `ClearPiece` fence → `on_piece_sync` différé (restore filtré v2 blocks, hash_failed_alert post-restore, re-mark duplicates)
- [ ] write-fail vs hash-fail : `on_piece_fail_sync` séparé (restore entier + `hash_fail_piece_picks`)
- [ ] `found_on_disk` : pas de blame (failed_bytes/ext/penalize) mais restore/fence conservés
- [ ] `piece_passed` vs `we_have` split : pads/checking/fastresume passent par we_have direct
- [ ] penalize_peers : parole conn+peer_list, trust −2 min −7, `banned_for_hash_failure`, `update_want_peers`, disconnect `failed:true`
- [ ] Compteurs : `num_piece_passed`/`num_piece_failed`/`num_have_pieces` aux sites exacts
- [ ] `file_completed` edges par fichier, `m_last_download` stampé au edge vérifié

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
