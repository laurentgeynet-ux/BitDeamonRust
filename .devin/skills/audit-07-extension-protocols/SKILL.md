---
name: audit-07-extension-protocols
description: "Audit sémantique vs libtorrent — Protocoles d'extension (ut_metadata, ut_pex, lt_*, suggest, smartban, holepunch)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A07 — Protocoles d'extension (ut_metadata, ut_pex, lt_*, suggest, smartban, holepunch)

**Périmètre** : Extensions BEP-10 : métadata, pex, dont_have/have_none, suggest+allowed_fast, smart_ban, upload_only, holepunch..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/ut_metadata.cpp`, `src/ut_pex.cpp`, `src/smart_ban.cpp`, `src/bt_peer_connection.cpp` (extended legs), `src/peer_connection.cpp` (incoming_suggest/allowed_fast/dont_have)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/bep10.rs`, `crates/bitdaemon-session/src/peer_conn.rs` (extended arm), `session.rs` (metadata/pex routing)

## Checklist spécifique

- [ ] ut_metadata : request/data/reject, throttling, caps `metadata_size`, chunk 16K ordering, tracker-hash reset
- [ ] ut_pex : added/dropped, flags (seed/upload_only/encryption/utp/holepunch), IPv6 form, max 50 pairs, `lt_v2` flag strip
- [ ] lt_donthave inbound — drop silencieux si `packet_size != 6`, disconnect si index hors bitfield, redondant ignoré
- [ ] suggest/allowed_fast sets : sémantique d'insertion/purge (choke → allowed_fast, unchoked → suggested), request hors-set → reject
- [ ] smart_ban : hash par bloc, bannissement persistant, `banned_for_hash_failure`
- [ ] upload_only message : sémantique sur les deux sens (peer announces vs we announce)
- [ ] holepunch rendez-vous : relay, connect msg, erreurs

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
