---
name: audit-15-proxies
description: "Audit sémantique vs libtorrent — Proxies (SOCKS5/HTTP CONNECT)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A15 — Proxies (SOCKS5/HTTP CONNECT)

**Périmètre** : socks5_stream, http_stream, instantiate_connection — auth, UDP associate, erreurs, settings par usage..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/socks5_stream.cpp`, `src/http_stream.cpp`, `src/instantiate_connection.cpp`, `src/session_impl.cpp` (proxy settings)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/proxy*.rs` / `socks5*.rs` / `http_stream*.rs`, transport

## Checklist spécifique

- [ ] SOCKS5 : méthodes auth (no-auth, user/pass), commandes connect/UDP associate, erreurs → mapping
- [ ] HTTP CONNECT : auth basic, tunnel keep-alive, host header
- [ ] UDP associate : datagrammes encapsulés (RSV/FRAG/ATYP), trackers UDP via proxy
- [ ] Proxy par usage : `proxy_peer_connections`/`proxy_tracker_connections`/`proxy_dht` flags par proto
- [ ] Hostname vs résolution via proxy, `resolve_hostnames` flag
- [ ] Instantiate : choix socket_type selon proxy + ssl

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
