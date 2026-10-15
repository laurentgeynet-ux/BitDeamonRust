# Registre des audits — couverture complète BitDaemon vs libtorrent

Baseline upstream : `v2.1.2-62-ge7381c7bc` (`D:\Projet\libtorrent\libtorrent`, lecture seule).
Chaque audit = un skill `.devin/skills/audit-NN-<slug>/` lançable via `/audit-NN-<slug>`.
**Mode d'emploi complet : [`GUIDE.md`](GUIDE.md).**

| # | Audit | Périmètre | Statut |
|---|---|---|---|
| A01 | Pipeline disque — bloc entrant → écriture | incoming block → async_write → cache → flush → completion | done — audit initial + commit `5541959` |
| A02 | Chemin upload/read (Request → fill_send_buffer → async_read) | Request entrant → validation → upload queue → read disque → Piece ; choke drain, rejects, compteurs | done — commit `b518598` (DontHave wire fix) |
| A03 | Cycle de vie des pièces | incoming_piece → flush → hash → piece_verified → Have → hash_failed → restore | done — commit `e957a83` |
| A04 | Ops storage & priorités fichiers | move_storage / delete_files / rename_file / prioritize_files / file_priority + complétions | done — commit `24fffc8` |
| A05 | Chemin download (request→write) & rejects | request_a_block → add_request → send_block_requests → Piece → on_reject → timeouts/snub/endgame | done — commit `cbee65c` |
| A06 | Codec peer-wire & framing | Encodage/décodage de tous les messages, bornes de longueur, entrées malformées, handshake. | à faire |
| A07 | Protocoles d'extension (ut_metadata, ut_pex, lt_*, suggest, smartban, holepunch) | Extensions BEP-10 : métadata, pex, dont_have/have_none, suggest+allowed_fast, smart_ban, upload_only, holepunch. | à faire |
| A08 | Choker & optimistic unchoke | Sélection des pairs unchokés : comparateurs, quotas, rotation optimiste, interaction snub/seed. | à faire |
| A09 | Pipeline time-critical (deadlines) | set_piece_deadline, request_time_critical_pieces, make_time_critical, cancel_non_critical, expiration de deadlines. | à faire |
| A10 | Cycle de vie des pairs & admission | attach/detach, états de connexion, limites half-open, taxonomie des disconnects, admission entrante. | à faire |
| A11 | Trackers (HTTP/UDP announce, scrape, backoff, tiers) | Cycle announce complet : events, backoff, failover tiers, scrape, réponses, erreurs. | à faire |
| A12 | Web seeds (HTTP peer) | web_peer_connection/http_seed_connection : GET ranges, keep-alive, erreurs, retry, comptabilité par seed. | à faire |
| A13 | Transport uTP | utp_stream : paquets, états connexion, resend, delay/rtt, MTU, buffers out-of-order, fermeture. | à faire |
| A14 | Chiffrement MSE/PE | Handshake DH, crypto_provide/select, RC4, padding, politiques enc. | à faire |
| A15 | Proxies (SOCKS5/HTTP CONNECT) | socks5_stream, http_stream, instantiate_connection — auth, UDP associate, erreurs, settings par usage. | à faire |
| A16 | Torrents SSL | ssl_stream : fingerprint cert vs info-hash, mutual TLS, cert settings, listen ssl, alertes. | à faire |
| A17 | DHT (krpc, traversals, items, sécurité) | Mainline DHT : messages, traversals, tokens, BEP-44/51, routage, refresh, boot, rate limiting. | à faire |
| A18 | Cache disque & buffer pool | Coalescing, watermark, éviction, dirty expiry, bornes pool, back-pressure cache. | à faire |
| A19 | part_file & file_pool | Slots part-file, export, migration priorités, récupération corruption ; pool fd LRU, caps, erreurs reopen. | à faire |
| A20 | Resume data — round-trip + snapshot sous I/O | read/write resume complet, verify_resume_data verdicts, cohérence du snapshot pendant écritures en vol. | à faire |
| A21 | Machine d'état torrent | Transitions add→checking→downloading→finished→seeding, pauses, state_updated/save_resume triggers, seed_mode. | à faire |
| A22 | Magnet & acquisition metadata | Magnet add, ut_metadata pacing, info-hash verify, priorités/trackers conservés, ops sans metadata. | à faire |
| A23 | Settings hot-apply & .session | apply_settings legs par clé, session_params/.session round-trip, deprecated keys. | à faire |
| A24 | Piece picker — cas limites | reverse/partial/rarest-first, priorities, streaming, share mode, endgame busy, invariants. | à faire |
| A25 | Sweep conformité compteurs/stats | Chaque `Counter` ↔ sites upstream inc/dec/set ; session_stats snapshot ; sliding_average. | à faire |
| A26 | Sanitization chemins & parsing metainfo (sécurité) | file_storage sanitize (.., ADS, device names, drives, abs), symlink, doublons ; metainfo malformé. | à faire |
| A27 | Glue engine/session | engine.rs : boucle dispatch, ordonnancement commandes, timers, pont disque, starvation. | à faire |
| A28 | NAT traversal (LSD/UPnP/NAT-PMP) | LSD multicast, UPnP IGD lifecycle, NAT-PMP/PCP mappings, external IP election. | à faire |
| A29 | Bandwidth manager & peer classes | Quotas par canal, request_bandwidth, class filters, rate channels, mixed mode, overhead. | à faire |

## Utilisation

```
/audit-06-peer-wire-codec            # mode rapport (défaut)
/audit-06-peer-wire-codec en mode apply
```

Le skill embarque : périmètre, fichiers upstream/Rust, checklist,
protocole à deux modes, pièges connus.

## Workflow parallèle — 2 phases

Le protocole historique (analyse + fix + gates + push `main` en une
passe) n'est pas parallélisable : `target/` verrouillé sérialise les
builds, `fmt --all` se marchent dessus, `CHANGELOG.md`/`PORTING.md`
conflitent au merge, et les pushs `main` se rejettent en cascade.
D'où le découpage :

### Phase 1 — rapports (parallélisme total)

Mode `rapport` : read-only sur le code, écrit uniquement
`docs/audits/reports/ANN_<slug>.md` puis le commit sur `main` en
pathspec-limited (`git commit -- <path>` — n'embarque que ce fichier
même si l'index partagé contient le rapport d'un voisin). Tous les
audits peuvent tourner en même temps dans le même checkout.

**Gate obligatoire avant la phase 2** : chaque rapport est relu et
validé par un humain ; les déviations contestées basculent dans
`## Non-bugs tranchés`. Un `apply` sur rapport brut applique des
faux positifs possibles en branche.

### Phase 2 — application (worktrees + PRs)

Mode `apply` par audit — prérequis : rapport commité + validé :

```sh
git worktree add ../LibtorrentRust-aNN -b audit/NN-<slug>
# l'agent travaille dans ce dossier (target/ isolé → builds parallèles)
# gates complets, fragment docs/audits/changelog/ANN.md, puis :
git push -u origin audit/NN-<slug> && gh pr create
# après merge :
git worktree remove ../LibtorrentRust-aNN
```

Merges **séquentiels** ; en cas de conflit sur les fichiers chauds,
rebase sur `main` frais + re-run des gates avant merge.

### Vagues d'application conseillées (recouvrement de fichiers)

| Vague | Audits | Raison |
|---|---|---|
| 1 — isolés | A13, A15, A17, A18, A19, A26, A28 | modules/crates disjoints (utp, proxy, dht, disk×2, core/path, nat) |
| 2 — session-edge | A20, A23, A24, A27, A29, A11, A12, A16 | fichiers dédiés (resume, settings, picker, engine, peer_class, tracker*, web_seed*, ssl) — edges torrent/session modérés |
| 3 — net-peer | A06 puis A14 | `peer_wire.rs` commun aux deux → séquentiels entre eux |
| 4 — session-core | A07, A08, A09, A10, A21, A22 | tous sur `torrent.rs`/`peer_conn.rs`/`session.rs` — max 2-3 concurrents, merges séquentiels |
| 5 — final | A25 | transverse (sweep compteurs), seul, en dernier |

A18+A19 partagent `bitdaemon-disk` mais des modules disjoints — OK
ensemble. Dans la vague 4, préférer des paires (A08+A24-style) et
merger une PR avant d'en appliquer une autre sur le même fichier.

### Agrégation changelog

```sh
python docs/audits/_aggregate_changelog.py   # après merge des PRs
```

Fusionne `docs/audits/changelog/A*.md` dans `CHANGELOG.md` sous
`## [Unreleased]` (regroupé par section `### Fixed` etc.) puis
supprime les fragments. Commit : `docs(changelog): aggregate audit entries`.

### Ressources

- Un `target/` par worktree (plusieurs GB chacun). `sccache` mutualise
  la compilation des deps : `cargo install sccache` puis
  `RUSTC_WRAPPER=sccache` (bash) ou `$env:RUSTC_WRAPPER="sccache"`
  (PowerShell).
- 2-4 builds workspace concurrents max en local (cargo utilise déjà tous
  les cœurs). Pour du parallélisme massif : sessions cloud séparées,
  une par audit — chacune a sa machine, son checkout, sa PR.

## Ordre conseillé (séquentiel, si un seul agent)

Par blast radius d'interop : A06/A07 (wire+ext) → A11/A13 (réseau) →
A20 (resume) → A21/A24 (état) → A17 (DHT) → A25 (sweep compteurs final).
