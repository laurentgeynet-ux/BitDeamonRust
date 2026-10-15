---
name: audit-06-peer-wire-codec
description: "Audit sémantique vs libtorrent — Codec peer-wire & framing"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A06 — Codec peer-wire & framing

**Périmètre** : Encodage/décodage de tous les messages, bornes de longueur, entrées malformées, handshake..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/bt_peer_connection.cpp` (on_receive, dispatch), `include/libtorrent/bt_peer_connection.hpp`, `src/peer_connection.cpp` (incoming_* dispatch)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/peer_wire.rs`, `crates/bitdaemon-session/src/peer_conn.rs` (dispatch)

## Checklist spécifique

- [ ] Length-prefix 4B BE inclut l'id, keepalive = len 0 — bornes exactes upstream (`packet_size` checks par message)
- [ ] Chaque `msg_*` : round-trip encode/decode + rejet des tailles invalides (have len 5, request len 13, bitfield len = ceil(n/8)+1…)
- [ ] Handshake 68B : validation info_hash/proto, peer_id optionnel, reserved bits (BEP-10, FAST, v2)
- [ ] Messages hors-ordre : have/bitfield avant handshake, piece avant bitfield, reject sans request
- [ ] Dépassements : oversized piece (packet_too_large), truncated frames, length incohérente → disconnect vs drop silencieux
- [ ] Compteurs `num_incoming_*` par type de message (funnel dispatch)
- [ ] Port/dht_port message, extension id 0 handshake vs ids étendus

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
