---
name: audit-16-ssl-torrents
description: "Audit sémantique vs libtorrent — Torrents SSL"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A16 — Torrents SSL

**Périmètre** : ssl_stream : fingerprint cert vs info-hash, mutual TLS, cert settings, listen ssl, alertes..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/ssl_stream.cpp`, `include/libtorrent/ssl_stream.hpp`, `src/torrent.cpp` (ssl legs), `src/session_impl.cpp` (ssl listen)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/ssl_stream.rs`, `crates/bitdaemon-session/src/session.rs` (ssl listen legs), `engine.rs`

## Checklist spécifique

- [ ] Fingerprint : digest cert SHA-256/SHA-1 vs info-hash v1/v2 → règles d'acceptation exactes
- [ ] Mutual TLS : verify_peer + verify_fail_if_no_peer_cert, client cert depuis `ssl_cert`/`ssl_key`
- [ ] verify_callback : chain custom — CN/SAN ne font pas foi d'identité torrent upstream
- [ ] ssl_listen_port, accept inbound ssl, flags (`is_ssl_torrent`, `m_ssl`)
- [ ] Alertes : erreurs peer_ssl → mapping disconnect reason
- [ ] crypto provider : builder_with_provider exhaustif (suite du fix récent)

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
