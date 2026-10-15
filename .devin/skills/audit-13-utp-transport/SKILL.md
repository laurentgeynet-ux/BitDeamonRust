---
name: audit-13-utp-transport
description: "Audit sémantique vs libtorrent — Transport uTP"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A13 — Transport uTP

**Périmètre** : utp_stream : paquets, états connexion, resend, delay/rtt, MTU, buffers out-of-order, fermeture..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/utp_stream.cpp`, `src/utp_socket_manager.cpp`, `include/libtorrent/utp_socket_manager.hpp`
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/utp*.rs`

## Checklist spécifique

- [ ] Packet types : ST_DATA/FIN/STATE/RESET/SYN — header 20B exact (version, ext, conn_id, ts, tsdiff, wnd, seq, ack)
- [ ] State machine : SYN_SENT → CONNECTED → FIN_SENT/CLOSE_WAIT, syn retransmit, connect timeout
- [ ] Resend : RTO basé rtt/var, exponentiel, seq wrap 16 bits
- [ ] Delay measurement : tsdiff, constantes LEDBAT-ish — PORTER LES CONSTANTES VERBATIM avant tout tuning
- [ ] MTU discovery : probing, floor/ceiling
- [ ] Out-of-order buffer : selective ack, gaps, reorder window
- [ ] wnd advertisement vs buffer réel, `m_suspended`/deferred close
- [ ] Interface DatagramSocket injectée — aucun bind direct (ADR-0008)

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
