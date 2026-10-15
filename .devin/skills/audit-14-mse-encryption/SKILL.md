---
name: audit-14-mse-encryption
description: "Audit sémantique vs libtorrent — Chiffrement MSE/PE"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A14 — Chiffrement MSE/PE

**Périmètre** : Handshake DH, crypto_provide/select, RC4, padding, politiques enc..

## Fichiers

- **Upstream** (baseline `e7381c7bc`, `D:/Projet/libtorrent/libtorrent`, lecture seule) : `src/pe_crypto.cpp`, `src/encryption.cpp`, `src/peer_connection.cpp` (enc handshake)
- **Rust** (`D:/Projet/LibtorrentRust`) : `crates/bitdaemon-net/src/pe_crypto.rs` / `encryption*.rs`, `peer_wire.rs` (enc wrapper)

## Checklist spécifique

- [ ] Handshake : DH 768 bits, SKEY = info_hash côté initiator, génération clés publiques
- [ ] crypto_provide/crypto_select : bits plaintext/rc4, sync sur len(PadC), IA vs req1/req2/req3
- [ ] RC4 : clés A→B et B→A distinctes, drop 1024, application header+payload
- [ ] Padding : len(PadA/B/C) 0-512, VC constant
- [ ] Politiques : enc_disabled/enabled/required — entrantes ET sortantes, `allowed_enc_level`, pe_forced
- [ ] Upgrade : conn plaintext → enc mid-handshake, fallback sur échec
- [ ] Aucun unsafe : RC4/DH via crates sûres

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
