---
name: audit-26-path-metainfo-security
description: "Audit sémantique vs libtorrent — Sanitization chemins & parsing metainfo (sécurité)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A26 — Sanitization chemins & parsing metainfo (sécurité)

**Périmètre** : file_storage sanitize (.., ADS, device names, drives, abs), symlink, doublons ; metainfo malformé..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/file_storage.cpp` (sanitize_path), `src/torrent_info.cpp` (parse errors), `src/path.cpp`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-core/src/file_storage.rs`, `path*.rs`, `torrent_info.rs`

## Checklist spécifique

- [ ] `..` traversal : sanitization identique upstream (éléments retirés, pas rejet global)
- [ ] ADS `file:`/`file$`, noms réservés Windows (CON/PRN/AUX/NUL/COM1-9/LPT1-9) → renaming identique
- [ ] Drive letters `C:`, abs paths, UNC → traités en noms de fichier upstream
- [ ] Symlink flag : création, dangling links, `file_symlink` paths
- [ ] Doublons de chemins : disambiguation ou erreur
- [ ] Metainfo : piece_length 0/négatif/énorme, sizes négatives, utf-8 invalide, bencode non-canonique, piece string length mismatch → erreurs exactes
- [ ] Zero-length files & pad files : création ignorée, comptage

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
