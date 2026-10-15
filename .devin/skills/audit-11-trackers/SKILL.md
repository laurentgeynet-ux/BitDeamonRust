---
name: audit-11-trackers
description: "Audit sémantique vs libtorrent — Trackers (HTTP/UDP announce, scrape, backoff, tiers)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A11 — Trackers (HTTP/UDP announce, scrape, backoff, tiers)

**Périmètre** : Cycle announce complet : events, backoff, failover tiers, scrape, réponses, erreurs..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/tracker_client.cpp`, `src/http_tracker_connection.cpp`, `src/udp_tracker_connection.cpp`, `src/torrent.cpp` (announce_with_tracker, enable_all_trackers, scrape_tracker)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/tracker*` (clients http/udp), `crates/bitdaemon-session/src/tracker_list.rs`, `torrent.rs` (announce legs), `engine.rs`

## Checklist spécifique

- [ ] Events : none/started/stopped/completed — completed envoyé UNE fois au premier finish, re-announces sans event
- [ ] Backoff : échecs exponentiels `announce_interval * (1.5^n)`, cap, jitter, `fail_limit` → tracker suivant du tier
- [ ] Tiers : ordre, failover après N échecs vs immédiat, `announce_to_all_tiers/trackers` flags
- [ ] Scrape : déclenchement (`want_scrape`, auto_scrape_interval/min), réponse → complete/incomplete/downloaded
- [ ] UDP tracker : connect → announce → scrape wire format, transaction ids, réponses tronquées, timeout retry
- [ ] key/tracker_id génération + stabilité session ; num_want, left/bytes accounting
- [ ] Réponses : peer dicts (v4/v6), min interval, warning message, trackerid update
- [ ] `tracker_warning` count vs `tracker_error`, `next_announce` reset on force_announce

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
