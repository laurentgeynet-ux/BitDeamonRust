---
name: audit-01-v3-peer-wire-codec
description: "[V3] Audit sémantique vs libtorrent — Codec peer-wire & framing"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A01 — Codec peer-wire & framing

**Périmètre** : Encodage/décodage de tous les messages, bornes de longueur, entrées malformées, handshake..

**Vague d'apply** : V3 (net-peer) — `peer_wire.rs` commun aux deux → séquentiels entre eux.
Choix des audits concurrents par recouvrement de fichiers,
affiner avec la section `## Fichiers du fix` du rapport.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, lecture seule) : `src/bt_peer_connection.cpp` (on_receive, dispatch), `include/libtorrent/bt_peer_connection.hpp`, `src/peer_connection.cpp` (incoming_* dispatch)
  Checkout upstream, résolu dans l'ordre : `$LIBTORRENT_ROOT` si
  définie → `D:/Projet/libtorrent/libtorrent` (poste local) → sinon
  `git clone https://github.com/arvidn/libtorrent ../libtorrent`
  puis `git -C ../libtorrent checkout e7381c7bc` (cloud/CI).
- **Rust** (repo courant) : `crates/bitdaemon-net/src/peer_wire.rs`, `crates/bitdaemon-session/src/peer_conn.rs` (dispatch)

## Checklist spécifique

- [ ] Length-prefix 4B BE inclut l'id, keepalive = len 0 — bornes exactes upstream (`packet_size` checks par message)
- [ ] Chaque `msg_*` : round-trip encode/decode + rejet des tailles invalides (have len 5, request len 13, bitfield len = ceil(n/8)+1…)
- [ ] Handshake 68B : validation info_hash/proto, peer_id optionnel, reserved bits (BEP-10, FAST, v2)
- [ ] Messages hors-ordre : have/bitfield avant handshake, piece avant bitfield, reject sans request
- [ ] Dépassements : oversized piece (packet_too_large), truncated frames, length incohérente → disconnect vs drop silencieux
- [ ] Compteurs `num_incoming_*` par type de message (funnel dispatch)
- [ ] Port/dht_port message, extension id 0 handshake vs ids étendus

## Protocole — deux modes

L'invocation précise le mode ; **défaut : `rapport`**. Le découpage existe
parce que plusieurs audits peuvent tourner en parallèle : la phase
d'analyse est read-only sur le repo (zéro conflit), la phase de
correction s'exécute dans un worktree + branche dédiés ; l'apply finit
par un merge automatique sur `main` local + cleanup — tout local.

### Analyse commune (mode A l'exécute, mode B s'appuie sur le rapport)

1. **Cartographier le chemin Rust d'abord** (grep/lecture), sans lire upstream —
   noter la machine d'état, les gates, les compteurs, les alertes, l'ordre des effets.
2. **Lire les fonctions upstream correspondantes** dans le checkout
   résolu ci-dessus (baseline `e7381c7bc`, lecture seule — ne jamais
   modifier).
3. **Diff sémantique** : ordre des opérations, gates, compteurs `counters::`,
   alertes (type + payload + timing), gestion d'erreur, comportement filaire.

   Méthode : les fichiers upstream font 6-10k lignes (`torrent.cpp`,
   `peer_connection.cpp`, `session_impl.cpp`) — ne jamais les lire en
   bloc. `grep` le nom de la fonction visée, lire ±80 lignes autour,
   suivre les appels ; ça préserve la fenêtre de contexte. Avant de
   consigner une déviation, `grep` aussi les crates sous-jacentes
   (`bitdaemon-common`, `-core`, `-net`, `-disk`) : la logique C++ du
   fichier audité a pu être déplacée dans un helper d'une couche
   inférieure — une absence dans le fichier listé n'est pas une
   déviation.

### Mode A — `rapport` (read-only, 100% parallélisable)

4. Écrire le rapport dans `docs/audits/reports/A01_peer-wire-codec.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A01 — rapport d'analyse

   ## Verdicts checklist
   Un verdict par item — `conforme` / `déviation` / `N-A` — chacun avec
   une preuve `fichier.rs:ligne` (ou réf. upstream `src/foo.cpp`,
   fonction). Tout item marqué `déviation` DOIT avoir une entrée dans
   `## Déviations`. Un rapport « tout conforme » sans preuve par item
   est incomplet — la preuve est ce qui rend le verdict vérifiable.
   - Length-prefix 4B BE inclut l'id, keepalive = len 0 — bornes exactes upstream (`packet_size` checks par message) — verdict : … — preuve : …
   - Chaque `msg_*` : round-trip encode/decode + rejet des tailles invalides (have len 5, request len 13, bitfield len = ceil(n/8)+1…) — verdict : … — preuve : …
   - Handshake 68B : validation info_hash/proto, peer_id optionnel, reserved bits (BEP-10, FAST, v2) — verdict : … — preuve : …
   - Messages hors-ordre : have/bitfield avant handshake, piece avant bitfield, reject sans request — verdict : … — preuve : …
   - Dépassements : oversized piece (packet_too_large), truncated frames, length incohérente → disconnect vs drop silencieux — verdict : … — preuve : …
   - Compteurs `num_incoming_*` par type de message (funnel dispatch) — verdict : … — preuve : …
   - Port/dht_port message, extension id 0 handshake vs ids étendus — verdict : … — preuve : …

   ## Déviations
   Chaque déviation porte un **sketch de fix concret** — hunk de diff
   ou edit-spec exacte (quel symbole, quel code à la place, réf.
   upstream à la ligne près). Un « fix proposé » vague (« ajouter une
   vérif ») est incomplet : l'apply doit pouvoir l'exécuter sans
   re-designer. Le sketch n'est PAS un patch garanti : il date de
   l'analyse, le contexte peut avoir bougé — l'apply l'applique en
   sémantique et boucle les gates. Format :
   - `fichier.rs:ligne` — comportement Rust actuel / attendu upstream
     (`src/foo.cpp`, fonction) — sévérité (wire-visible / état /
     cosmétique)
     - **sketch** : `avant → après` minimal (diff ou edit-spec) +
       réf. upstream `src/foo.cpp:ligne` + test de régression suggéré

   ## Fichiers du fix
   Fichiers Rust que les corrections toucheront (planification des vagues).

   ## Non-bugs tranchés
   Écarts volontaires, avec justification upstream.
   ```

   Ne modifier **aucun** fichier existant en dehors du rapport. Pas de
   build/test cargo — l'analyse ne compile pas (et `target/`
   verrouillé sérialiserait les audits parallèles).

5. **Commit du rapport sur `main`** — pathspec-limited : le `-- <path>`
   n'embarque que ce fichier, même si un audit voisin a stagé autre
   chose dans l'index partagé entre-temps :

   ```sh
   git add docs/audits/reports/A01_peer-wire-codec.md
   git commit -m "docs(audit): rapport A01 peer-wire-codec" \
       -- docs/audits/reports/A01_peer-wire-codec.md
   ```

   Ce commit est **requis** : un worktree `apply` branché depuis `main`
   ne voit que du contenu commité. Les commits de rapports ne se
   conflitent jamais entre eux (fichiers disjoints, fast-forward).

6. **STOP côté code** : pas de fix, pas de gate. Le mode `apply` est
   une invocation séparée qui part du rapport commité **et relu**.

### Mode B — `apply` (worktree + branche dédiée)

Prérequis : le rapport `docs/audits/reports/A01_peer-wire-codec.md` est
**commité sur `main` ET relu/validé par l'humain** — les déviations
contestées ont basculé dans `## Non-bugs tranchés`. Ne jamais lancer
`apply` sur un rapport brut : il peut contenir des faux positifs.
Si un seul audit tourne à la fois, le checkout principal suffit ; dès
que plusieurs `apply` sont parallèles, le worktree est obligatoire
(sinon `target/` partagé sérialise les builds, `cargo fmt --all` se
marchent dessus, et `cargo test` tourne sur le code transitoire des
autres).

7. **Worktree + branche** :

   ```sh
   git worktree add C:/Projet/wt/LibtorrentRust-a01 -b audit/01-peer-wire-codec
   ```

   Travailler dans `C:/Projet/wt/LibtorrentRust-a01` : `target/` isolé,
   builds réellement parallèles — **le worktree vit sur C: (SSD)**, pas
   sur D: qui est lent ; seuls les pointeurs `.git` restent sur D:. Le
   checkout principal reste `D:/Projet/LibtorrentRust` : dans les
   commandes `git -C` ci-dessous, c'est ce chemin absolu qui est visé.
   `sccache` mutualise la compilation des deps entre worktrees :
   `RUSTC_WRAPPER=sccache` (bash) ou `$env:RUSTC_WRAPPER="sccache"`
   (PowerShell).

8. **Corriger en Rust idiomatique** — pas de translittération ; commenter
   les legs subtils avec leur référence upstream.
9. **Tests de régression** pour chaque déviation corrigée.
10. **Gates obligatoires** (dans le worktree) : `cargo fmt --all` ·
    `cargo clippy --workspace --all-targets -- -D warnings` ·
    `cargo test --workspace` · `cargo doc --workspace --no-deps`.
11. **Changelog en fragment** : écrire les entrées Keep-a-Changelog dans
    `docs/audits/changelog/A01.md` sous `### Fixed` (ou autre section) —
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
12. Commit `fix(<crate>): <scope> parity — <mot-clé>` **sur la branche
    `audit/01-peer-wire-codec`** — jamais sur `main`. **Tout reste local** :
    pas de push, pas de PR, pas de GitHub.
13. **Merge sur `main` — automatique, dernier acte de l'apply.** Pour
    garantir un merge propre, `git rebase main` d'abord dans le
    worktree (résoudre les éventuels conflits `torrent.rs`/
    `peer_conn.rs`/`session.rs`/`PORTING.md` là + re-run des gates),
    puis `git -C D:/Projet/LibtorrentRust merge --no-ff
    audit/01-peer-wire-codec` — le merge s'exécute dans le checkout
    principal (apply solo dans le checkout principal : `git checkout
    main` puis `git merge --no-ff` en place). Les merges se chaînent
    **un à la fois** — si le checkout principal est verrouillé ou déjà
    en merge, attendre et réessayer. Cleanup immédiat :
    `git worktree remove C:/Projet/wt/LibtorrentRust-a01` puis
    `git branch -d audit/01-peer-wire-codec`.
14. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit) — commitée
    sur la branche **avant** le merge pour partir avec lui.

## Pièges connus (checklists transverses)

- Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés.
- Legs silencieux upstream (validation early-return, `is_disconnecting`,
  `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- Ordre effets wire-visible : messages sortants dans l'ordre upstream exact.
- Comportement dead-code upstream à conserver inerte (ex. `cancel_block`
  post-`mark_as_writing` — `is_requested` false en `state_writing`).
- `unwrap`/`expect`/`todo!` interdits hors tests ; `#![forbid(unsafe_code)]`.
