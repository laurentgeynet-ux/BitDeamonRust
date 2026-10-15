---
name: audit-17-dht
description: "Audit sémantique vs libtorrent — DHT (krpc, traversals, items, sécurité)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A17 — DHT (krpc, traversals, items, sécurité)

**Périmètre** : Mainline DHT : messages, traversals, tokens, BEP-44/51, routage, refresh, boot, rate limiting..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/kademlia/*.cpp`, `src/dht_tracker.cpp`, `src/node.cpp`, `src/routing_table.cpp`, `src/item.cpp`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-dht/src/*.rs`, `session.rs` (dht outbox/announce)

## Checklist spécifique

- [ ] KRPC : encode/decode q/r/e, transaction ids, error [code,msg]
- [ ] Traversals : find_node, get_peers (token collect), announce_peer (write-token check, implied_port), sample_infohashes
- [ ] BEP-44 : immutable put/get, mutable (k sig seq salt cas), seq monotonic, vérif sig ed25519
- [ ] Tokens : secret id rotatif, durée de vie, vérif par IP
- [ ] Routing table : buckets K=8, split, replace-oldest vs replace-pinger, refresh, verification
- [ ] BEP-42 : node id derivation (ip mask + rand), enforcement entrant
- [ ] dos_blocker : fenêtre 10 s, rate limit par IP/préfixe
- [ ] dht_state : persist nodes + node_id, obfuscation XOR, round-trip
- [ ] get_peers response : ordre values + nodes + token

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
