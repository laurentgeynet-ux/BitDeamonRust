---
name: audit-28-nat-traversal
description: "Audit sémantique vs libtorrent — NAT traversal (LSD/UPnP/NAT-PMP)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A28 — NAT traversal (LSD/UPnP/NAT-PMP)

**Périmètre** : LSD multicast, UPnP IGD lifecycle, NAT-PMP/PCP mappings, external IP election..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/lsd.cpp`, `src/upnp.cpp`, `src/natpmp.cpp`, `src/session_impl.cpp` (portmap legs)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/lsd*.rs`, `upnp*.rs`, `natpmp*.rs`, `session.rs`/`engine.rs`

## Checklist spécifique

- [ ] LSD : multicast group/port, cadence par torrent, cookie, receive → peer feed, off en anonymous_mode
- [ ] UPnP : SSDP discover, WANIP/WANPPP, AddPortMapping + renew, DeletePortMapping au stop
- [ ] NAT-PMP/PCP : mapping request, external address, epoch, renew, delete
- [ ] External IP election : consensus trackers/DHT/UPnP/NAT-PMP → external_ip_alert
- [ ] portmap alerts : mapping add/error/log transport+protocol
- [ ] Mapping per listen port : tcp+udp, proto field correct

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
