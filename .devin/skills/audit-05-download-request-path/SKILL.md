---
name: audit-05-download-request-path
description: "Audit sémantique vs libtorrent — Chemin download (request→write) & rejects" (re-vérification — déjà passé)
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A05 — Chemin download (request→write) & rejects

> **Déjà passé** (done — commit `cbee65c`). Le relancer = re-vérifier que les
> correctifs tiennent toujours (non-régression) et que la checklist
> reste verte après les changements intermédiaires.

**Périmètre** : request_a_block → add_request → send_block_requests → Piece → on_reject → timeouts/snub/endgame.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/peer_connection.cpp` (request_a_block, add_request, send_block_requests_impl, incoming_reject_request, incoming_piece, snub_peer, second_tick)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-session/src/peer_conn.rs`, `peer_pipeline.rs`, `torrent.rs`

## Checklist spécifique

- [ ] `send_block_requests` : merger `request_large_blocks`, gates disconnecting/pause/checking/upload_mode, `m_last_piece` stampé en sortie
- [ ] `incoming_piece` : ordre is_downloaded→async_write→mark_as_writing, waste buckets, unsnub fenêtré, `piece_seed`
- [ ] `incoming_reject_request` : validation forme (piece in-range, start aligné, length conforme), `!timed_out && !not_wanted` gate, parole → requeue front, seed → pas d'abort, `outstanding_bytes` dec
- [ ] Piece length-0 : quirk BitComet — validation échoue toujours → peer_error pur
- [ ] Timeouts : snub collapse à 1, `free_blocks>0` gate, re-pick avant timeout-mark, `piece_timeout` via `last_piece`
- [ ] Back-pressure : `exceeded && outstanding_writing_bytes>0` → `bw_disk`, `queued_write_bytes`, drain complet à la complétion
- [ ] Slow-start exit, AIMD `desired_queue_size`, `num_peers_down_requests` transitions
- [ ] Dead-code upstream conservée inerte : `cancel_block` post-`mark_as_writing`

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
