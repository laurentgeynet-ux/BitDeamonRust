---
name: audit-29-bandwidth-classes
description: "Audit sémantique vs libtorrent — Bandwidth manager & peer classes"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A29 — Bandwidth manager & peer classes

**Périmètre** : Quotas par canal, request_bandwidth, class filters, rate channels, mixed mode, overhead..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/bandwidth_manager.cpp`, `src/bandwidth_queue_entry.hpp`, `src/bandwidth_limit.cpp`, `src/peer_class.cpp`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/peer_class*.rs`, `crates/bitdaemon-session/src/session.rs` (bw quota paths)

## Checklist spécifique

- [ ] Channels : upload/download/protocol + classes (global/tcp/local/i2p) — assignation conn→class à l'attach
- [ ] request_bandwidth : quota granted, temp queuing, expiry
- [ ] Rate channels update : set_rate_limit per class propagé, `*_limit_too_low` perf alerts
- [ ] type filter apply : `set_peer_class_filter`/`set_peer_class_type_filter` — classe résolue par ip/type
- [ ] Mixed mode : uTP vs TCP split (`mixed_mode_algorithm` proportional/rate)
- [ ] Overhead accounting : debit ip_overhead canaux séparés (`rate_limit_ip_overhead`)
- [ ] Preemptive unchoke on quota, maybe_unchoke_this_peer

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
