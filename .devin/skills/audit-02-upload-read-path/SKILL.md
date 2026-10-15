---
name: audit-02-upload-read-path
description: "Audit sémantique vs libtorrent — Chemin upload/read (Request → fill_send_buffer → async_read)" (re-vérification — déjà passé)
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A02 — Chemin upload/read (Request → fill_send_buffer → async_read)

> **Déjà passé** (done — commit `b518598` (DontHave wire fix)). Le relancer = re-vérifier que les
> correctifs tiennent toujours (non-régression) et que la checklist
> reste verte après les changements intermédiaires.

**Périmètre** : Request entrant → validation → upload queue → read disque → Piece ; choke drain, rejects, compteurs.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/peer_connection.cpp` (incoming_request, fill_send_buffer, write_reject_request, on_choke drain), `src/torrent.cpp` (on_read_piece)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/peer_conn.rs`, `torrent.rs`, `net::peer_wire`

## Checklist spécifique

- [ ] `incoming_request` : validation piece/start/len (len ≤ 32K sauf allowed_fast), reject vs accept
- [ ] Upload queue : FIFO, choked peers non servis, `fill_send_buffer` ordering
- [ ] Drain on choke : requests en vol annulées + `Rejected` wire + `clear_request_queue`
- [ ] `DontHave`/`Have`/`Rejected` wire encodings — bit-level
- [ ] Compteurs upload : `num_uploads`, `upload_payload`, read-quotas par canal
- [ ] fast-extension : allowed_fast requests servis même choked

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
