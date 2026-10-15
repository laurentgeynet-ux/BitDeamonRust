---
name: audit-09-v4-upload-read-path
description: "[V4] Audit sémantique vs libtorrent — Chemin upload/read (Request → fill_send_buffer → async_read)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A09 — Chemin upload/read (Request → fill_send_buffer → async_read)

**Périmètre** : Request entrant → validation → upload queue → read disque → Piece ; choke drain, rejects, compteurs.

**Vague d'apply** : V4 (session-core) — tous sur `torrent.rs`/`peer_conn.rs`/`session.rs` — max 2-3 concurrents, merges séquentiels.
Choix des audits concurrents par recouvrement de fichiers,
affiner avec la section `## Fichiers du fix` du rapport.

## Fichiers

- **Upstream code** (baseline `e7381c7bc`, lecture seule) : `src/peer_connection.cpp` (incoming_request, fill_send_buffer, write_reject_request, on_choke drain), `src/torrent.cpp` (on_read_piece)
- **Upstream tests** (oracle de spécification) : `test/test_read_piece.cpp`, `test/test_fast_extension.cpp`, `test/test_transfer.cpp`
  Checkout upstream, résolu dans l'ordre : `$LIBTORRENT_ROOT` si
  définie → `D:/Projet/libtorrent/libtorrent` (poste local) → sinon
  `git clone https://github.com/arvidn/libtorrent ../libtorrent`
  puis `git -C ../libtorrent checkout e7381c7bc` (cloud/CI).
- **Rust** (repo courant) : `crates/bitdaemon-session/src/peer_conn.rs`, `torrent.rs`, `net::peer_wire`

## Checklist spécifique

- [ ] `incoming_request` : validation piece/start/len (len ≤ 32K sauf allowed_fast), reject vs accept
- [ ] Upload queue : FIFO, choked peers non servis, `fill_send_buffer` ordering
- [ ] Drain on choke : requests en vol annulées + `Rejected` wire + `clear_request_queue`
- [ ] `DontHave`/`Have`/`Rejected` wire encodings — bit-level
- [ ] Compteurs upload : `num_uploads`, `upload_payload`, read-quotas par canal
- [ ] fast-extension : allowed_fast requests servis même choked

## Protocole d'audit renforcé — deux modes

L'invocation précise le mode ; **défaut : `rapport`**. Le découpage existe
parce que plusieurs audits peuvent tourner en parallèle : la phase
d'analyse est read-only sur le repo (zéro conflit), la phase de
correction s'exécute dans un worktree + branche dédiés ; l'apply finit
par un merge automatique sur `main` local + cleanup — tout local.

### Méthodologie d'analyse (mode A l'exécute, mode B s'appuie sur le rapport)

L'audit ne doit JAMAIS être un simple survol superficiel ou un diff nominal de fonctions.
Il suit 4 étapes rigoureuses :

1. **Cartographie Rust & inventaire des tests existants** (read-only) :
   - Grep/lecture du chemin Rust : noter la machine d'état, les gates, les compteurs, les alertes, l'ordre des effets.
   - **Vérifier la couverture de tests Rust actuelle** dans `crates/<crate>/tests/` et les sous-modules `#[cfg(test)]` : quels invariants sont réellement testés vs non couverts ?

2. **Exploration Upstream (Code C++ ET Tests `test/test_*.cpp`)** :
   - Résoudre le checkout upstream (baseline `e7381c7bc`, lecture seule).
   - **Ne pas se limiter à ±80 lignes autour de la fonction** : traquer l'ensemble du cycle de vie de l'état :
     - Valeurs par défaut issues de `settings_pack` et constructeurs.
     - Flags `m_flags` et mutations inter-méthodes.
     - **Destructeurs RAII et méthodes de nettoyage (`disconnect()`, `abort()`, `clear()`, `teardown`)**.
   - **Examiner les tests upstream listés (`test/test_*.cpp`)** : les tests C++ sont la spécification vivante de libtorrent (15 ans de bugs et d'edge cases encodés). Relever chaque scénario de test (malformed input, out-of-order, timeouts, rechecks).

3. **Grille d'Analyse à 5 Lentilles d'Audit (MANDATOIRE pour chaque point)** :
   Chaque point de la checklist et chaque chemin critique doit être passé au crible des 5 lentilles :
   - **Lentille 1 — Fuzzing & Bornes (Malformed / Adversarial)** :
     - Casts d'entiers dangereux (`as usize` sur des entiers signés `i32` pouvant être négatifs, dépassement `u32::MAX`).
     - Payloads anormaux : taille 0, payload tronqué, oversized (> 16 KiB / 2 MiB), buffers incohérents.
     - Indices hors-bornes : piece/block index hors limites, bitfield mal dimensionné, hashes v2 non validés.
   - **Lentille 2 — Taxonomie d'Erreur & Disconnect (Drop Silencieux vs Disconnect)** :
     - Upstream fait-il `disconnect(errors::...)` avec alerte `peer_error_alert` et incrémentation de `failcount`, ou Rust fait-il un simple `return Ok(())` / drop silencieux laissant le pair zombie ?
     - Différenciation nette entre drop silencieux (bruit réseau bénin) et disconnect obligatoire (violation de protocole).
   - **Lentille 3 — Cycle de vie, Nettoyage & Invariants d'état** :
     - Sur erreur, déconnexion, pause, recheck ou hash-failure : toutes les requêtes en vol sont-elles purgées ? Les blocs picker libérés (`abort_download`) ? Les timers annulés ?
     - Invalidation des complétions asynchrones obsolètes : utilisation de tokens/générations (`picker_gen`) pour éviter qu'une réponse disque ou réseau tardive n'altère un état réinitialisé.
   - **Lentille 4 — Concurrence, Canaux Tokio & Backpressure** :
     - Canaux `tokio::sync::mpsc` : comportement en cas de canal saturé (`try_send` qui drop sans avertissement vs `send().await`). Risques de deadlock ou de perte d'événements critiques (complétions disque, alertes).
     - Ordonnancement des événements : pas de starvation des timers par les événements réseau/disque.
   - **Lentille 5 — Parité des Alertes, Compteurs & Hot-Apply** :
     - Types et timing d'alertes : payload exact, `alert_mask` respecté.
     - Compteurs `counters::` : inc/dec/set aux sites exacts, pas de double comptage ou de compteur mort.
     - Hot-apply : les modifications via `apply_settings` prennent-elles effet immédiatement ou seulement au prochain redémarrage ?

4. **Test Gap Analysis (Scénarios de test orphelins)** :
   - Recenser les tests upstream (`test/test_*.cpp`) qui n'ont AUCUN équivalent dans la suite de tests Rust. Tout test absent constitue une zone de risque prioritaire.

### Mode A — `rapport` (read-only, 100% parallélisable)

5. Écrire le rapport dans `docs/audits/reports/A09_upload-read-path.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A09 — rapport d'analyse

   ## Verdicts checklist
   Un verdict par item — `conforme` / `déviation` / `N-A`.
   Chaque verdict DOIT obligatoirement inclure :
   1. La preuve exacte (`fichier.rs:ligne` vs `src/foo.cpp:ligne`).
   2. La ou les lentilles auditées (ex: [Limites], [Disconnect], [Cycle de vie]).
   3. Le test Rust protecteur (`crates/<crate>/tests/...`) ou la preuve formelle.
   *Règle anti-complaisance* : Un verdict « conforme » sans preuve vérifiable ou sans test associé est irrecevable.
   - `incoming_request` : validation piece/start/len (len ≤ 32K sauf allowed_fast), reject vs accept — verdict : … — preuve : …
   - Upload queue : FIFO, choked peers non servis, `fill_send_buffer` ordering — verdict : … — preuve : …
   - Drain on choke : requests en vol annulées + `Rejected` wire + `clear_request_queue` — verdict : … — preuve : …
   - `DontHave`/`Have`/`Rejected` wire encodings — bit-level — verdict : … — preuve : …
   - Compteurs upload : `num_uploads`, `upload_payload`, read-quotas par canal — verdict : … — preuve : …
   - fast-extension : allowed_fast requests servis même choked — verdict : … — preuve : …

   ## Couverture des tests upstream (test/test_*.cpp)
   Inventaire des scénarios testés par libtorrent upstream sur ce périmètre vs statut en Rust :
   - `test_foo.cpp::test_nom_du_test` — statut : couvert (`crates/.../tests/foo.rs`) / manquant / partiel — impact

   ## Déviations
   Chaque déviation porte un **sketch de fix concret** — hunk de diff
   ou edit-spec exacte (quel symbole, quel code à la place, réf.
   upstream à la ligne près). Format :
   - `fichier.rs:ligne` — comportement Rust actuel / attendu upstream
     (`src/foo.cpp:ligne` ou `test/test_foo.cpp:ligne`) — sévérité (`critique` / `wire-visible` / `état-interne` / `cosmétique`) — lentille ([Limites], [Erreur], etc.)
     - **sketch** : `avant → après` minimal (diff ou edit-spec) +
       réf. upstream `src/foo.cpp:ligne`
     - **test de régression suggéré** : description précise du test unitaire
       à porter (basé sur le scénario de test upstream correspondant)

   ## Invariants de frontière (Seam Checks)
   Vérification des interactions aux frontières avec les autres sous-systèmes (Session ↔ Disk ↔ Net ↔ Core).

   ## Fichiers du fix
   Fichiers Rust que les corrections toucheront (planification des vagues).

   ## Non-bugs tranchés
   Écarts volontaires avec justification upstream ou contrainte safe Rust (ADR).
   ```

   Ne modifier **aucun** fichier existant en dehors du rapport. Pas de
   build/test cargo — l'analyse ne compile pas (et `target/`
   verrouillé sérialiserait les audits parallèles).

6. **Commit du rapport sur `main`** — pathspec-limited : le `-- <path>`
   n'embarque que ce fichier, même si un audit voisin a stagé autre
   chose dans l'index partagé entre-temps :

   ```sh
   git add docs/audits/reports/A09_upload-read-path.md
   git commit -m "docs(audit): rapport A09 upload-read-path" \
       -- docs/audits/reports/A09_upload-read-path.md
   ```

   Ce commit est **requis** : un worktree `apply` branché depuis `main`
   ne voit que du contenu commité. Les commits de rapports ne se
   conflitent jamais entre eux (fichiers disjoints, fast-forward).

7. **STOP côté code** : pas de fix, pas de gate. Le mode `apply` est
   une invocation séparée qui part du rapport commité **et relu**.

### Mode B — `apply` (worktree + branche dédiée)

Prérequis : le rapport `docs/audits/reports/A09_upload-read-path.md` est
**commité sur `main` ET relu/validé par l'humain** — les déviations
contestées ont basculé dans `## Non-bugs tranchés`. Ne jamais lancer
`apply` sur un rapport brut : il peut contenir des faux positifs.
Si un seul audit tourne à la fois, le checkout principal suffit ; dès
que plusieurs `apply` sont parallèles, le worktree est obligatoire
(sinon `target/` partagé sérialise les builds, `cargo fmt --all` se
marchent dessus, et `cargo test` tourne sur le code transitoire des
autres).

8. **Worktree + branche** :

   ```sh
   git worktree add C:/Projet/wt/LibtorrentRust-a09 -b audit/09-upload-read-path
   ```

   Travailler dans `C:/Projet/wt/LibtorrentRust-a09` : `target/` isolé,
   builds réellement parallèles — **le worktree vit sur C: (SSD)**, pas
   sur D: qui est lent ; seuls les pointeurs `.git` restent sur D:. Le
   checkout principal reste `D:/Projet/LibtorrentRust` : dans les
   commandes `git -C` ci-dessous, c'est ce chemin absolu qui est visé.
   `sccache` mutualise la compilation des deps entre worktrees :
   `RUSTC_WRAPPER=sccache` (bash) ou `$env:RUSTC_WRAPPER="sccache"`
   (PowerShell).

9. **Corriger en Rust idiomatique** — pas de translittération ; commenter
   les legs subtils avec leur référence upstream.
10. **Tests de régression obligatoires** : porter le scénario de test
    upstream identifié pour chaque déviation corrigée.
11. **Gates obligatoires** (dans le worktree) : `cargo fmt --all` ·
    `cargo clippy --workspace --all-targets -- -D warnings` ·
    `cargo test --workspace` · `cargo doc --workspace --no-deps`.
12. **Changelog en fragment** : écrire les entrées Keep-a-Changelog dans
    `docs/audits/changelog/A09.md` sous `### Fixed` (ou autre section) —
    **ne pas éditer `CHANGELOG.md`** (conflit de merge garanti entre
    audits parallèles ; agrégation post-merge via
    `python docs/audits/_aggregate_changelog.py`).
    **`docs/PORTING.md` — obligatoire** : enrichir les rows du périmètre
    avec ce que le fix a changé en parité (détails wire, compteurs,
    sémantique d'erreur — la colonne notes documente l'état réel, pas
    seulement le statut). Mettre le statut à jour si le fix le change
    (`ported` → `verified`…). Les edits sont par row → recouvrements
    quasi nuls entre apply parallèles ; en cas de conflit au merge,
    rebase + gates comme pour le reste.
13. Commit `fix(<crate>): <scope> parity — <mot-clé>` **sur la branche
    `audit/09-upload-read-path`** — jamais sur `main`. **Tout reste local** :
    pas de push, pas de PR, pas de GitHub.
14. **Merge sur `main` — automatique, dernier acte de l'apply.** Pour
    garantir un merge propre, `git rebase main` d'abord dans le
    worktree (résoudre les éventuels conflits `torrent.rs`/
    `peer_conn.rs`/`session.rs`/`PORTING.md` là + re-run des gates),
    puis `git -C D:/Projet/LibtorrentRust merge --no-ff
    audit/09-upload-read-path` — le merge s'exécute dans le checkout
    principal (apply solo dans le checkout principal : `git checkout
    main` puis `git merge --no-ff` en place). Les merges se chaînent
    **un à la fois** — si le checkout principal est verrouillé ou déjà
    en merge, attendre et réessayer. Cleanup immédiat :
    `git worktree remove C:/Projet/wt/LibtorrentRust-a09` puis
    `git branch -d audit/09-upload-read-path`.
15. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit) — commitée
    sur la branche **avant** le merge pour partir avec lui.

## Pièges connus (checklists transverses d'anomalies fréquentes)

- **Casts d'entiers signés / non-signés** : tout `as usize` sur une valeur issue du réseau ou du disque (ex: `i32`) doit être précédé d'un check `>= 0` sous peine de panique ou d'allocation géante (OOM).
- **Drop silencieux vs Disconnect** : ne jamais remplacer un `disconnect(errors::...)` upstream par un `return Ok(())` silencieux — cela crée des pairs zombies.
- **Fuites de ressources d'état** : sur déconnexion, vérifier que les requêtes en vol sont annulées, les blocs picker libérés (`abort_download`), et les timers arrêtés.
- **Stale generation & Race conditions** : vérifier que les complétions asynchrones (disque, hash, DNS) vérifient toujours la génération (`picker_gen`) pour ignorer les résultats obsolètes post-recheck ou pause.
- **Saturation des canaux Tokio (`mpsc`)** : traquer tout appel `try_send` susceptible de perdre des messages critiques en cas de forte charge, ou `send.await` susceptible de deadlocker l'acteur.
- **Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés** ou doublement incrémentés.
- **Legs silencieux upstream** (validation early-return, `is_disconnecting`, `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- **Ordre effets wire-visible** : messages sortants dans l'ordre upstream exact.
- **Comportement dead-code upstream à conserver inerte** (ex. `cancel_block` post-`mark_as_writing` — `is_requested` false en `state_writing`).
- **`unwrap`/`expect`/`todo!` interdits hors tests** ; `#![forbid(unsafe_code)]` respecté rigoureusement.
