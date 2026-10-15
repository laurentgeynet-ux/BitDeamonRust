---
name: audit-25-v1-nat-traversal
description: "[V1] Audit sémantique vs libtorrent — NAT traversal (LSD/UPnP/NAT-PMP)"
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# Audit A25 — NAT traversal (LSD/UPnP/NAT-PMP)

**Périmètre** : LSD multicast, UPnP IGD lifecycle, NAT-PMP/PCP mappings, external IP election..

**Vague d'apply** : V1 (isolés) — modules/crates disjoints (utp, proxy, dht, disk×2, core/path, nat).
Choix des audits concurrents par recouvrement de fichiers,
affiner avec la section `## Fichiers du fix` du rapport.

## Fichiers

- **Upstream** (baseline `e7381c7bc`, lecture seule) : `src/lsd.cpp`, `src/upnp.cpp`, `src/natpmp.cpp`, `src/session_impl.cpp` (portmap legs)
  Checkout upstream, résolu dans l'ordre : `$LIBTORRENT_ROOT` si
  définie → `D:/Projet/libtorrent/libtorrent` (poste local) → sinon
  `git clone https://github.com/arvidn/libtorrent ../libtorrent`
  puis `git -C ../libtorrent checkout e7381c7bc` (cloud/CI).
- **Rust** (repo courant) : `crates/bitdaemon-net/src/lsd*.rs`, `upnp*.rs`, `natpmp*.rs`, `session.rs`/`engine.rs`

## Checklist spécifique

- [ ] LSD : multicast group/port, cadence par torrent, cookie, receive → peer feed, off en anonymous_mode
- [ ] UPnP : SSDP discover, WANIP/WANPPP, AddPortMapping + renew, DeletePortMapping au stop
- [ ] NAT-PMP/PCP : mapping request, external address, epoch, renew, delete
- [ ] External IP election : consensus trackers/DHT/UPnP/NAT-PMP → external_ip_alert
- [ ] portmap alerts : mapping add/error/log transport+protocol
- [ ] Mapping per listen port : tcp+udp, proto field correct

## Protocole — deux modes

L'invocation précise le mode ; **défaut : `rapport`**. Le découpage existe
parce que plusieurs audits peuvent tourner en parallèle : la phase
d'analyse est read-only sur le repo (zéro conflit), la phase de
correction s'exécute dans un worktree + branche dédiés ; le merge est
local et décidé par l'humain.

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

4. Écrire le rapport dans `docs/audits/reports/A25_nat-traversal.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A25 — rapport d'analyse

   ## Verdicts checklist
   Un verdict par item — `conforme` / `déviation` / `N-A` — chacun avec
   une preuve `fichier.rs:ligne` (ou réf. upstream `src/foo.cpp`,
   fonction). Tout item marqué `déviation` DOIT avoir une entrée dans
   `## Déviations`. Un rapport « tout conforme » sans preuve par item
   est incomplet — la preuve est ce qui rend le verdict vérifiable.
   - LSD : multicast group/port, cadence par torrent, cookie, receive → peer feed, off en anonymous_mode — verdict : … — preuve : …
   - UPnP : SSDP discover, WANIP/WANPPP, AddPortMapping + renew, DeletePortMapping au stop — verdict : … — preuve : …
   - NAT-PMP/PCP : mapping request, external address, epoch, renew, delete — verdict : … — preuve : …
   - External IP election : consensus trackers/DHT/UPnP/NAT-PMP → external_ip_alert — verdict : … — preuve : …
   - portmap alerts : mapping add/error/log transport+protocol — verdict : … — preuve : …
   - Mapping per listen port : tcp+udp, proto field correct — verdict : … — preuve : …

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
   git add docs/audits/reports/A25_nat-traversal.md
   git commit -m "docs(audit): rapport A25 nat-traversal" \
       -- docs/audits/reports/A25_nat-traversal.md
   ```

   Ce commit est **requis** : un worktree `apply` branché depuis `main`
   ne voit que du contenu commité. Les commits de rapports ne se
   conflitent jamais entre eux (fichiers disjoints, fast-forward).

6. **STOP côté code** : pas de fix, pas de gate. Le mode `apply` est
   une invocation séparée qui part du rapport commité **et relu**.

### Mode B — `apply` (worktree + branche dédiée)

Prérequis : le rapport `docs/audits/reports/A25_nat-traversal.md` est
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
   git worktree add ../LibtorrentRust-a25 -b audit/25-nat-traversal
   ```

   Travailler dans `D:/Projet/LibtorrentRust-a25` : `target/` isolé,
   builds réellement parallèles. `sccache` mutualise la compilation
   des deps entre worktrees : `RUSTC_WRAPPER=sccache` (bash) ou
   `$env:RUSTC_WRAPPER="sccache"` (PowerShell).

8. **Corriger en Rust idiomatique** — pas de translittération ; commenter
   les legs subtils avec leur référence upstream.
9. **Tests de régression** pour chaque déviation corrigée.
10. **Gates obligatoires** (dans le worktree) : `cargo fmt --all` ·
    `cargo clippy --workspace --all-targets -- -D warnings` ·
    `cargo test --workspace` · `cargo doc --workspace --no-deps`.
11. **Changelog en fragment** : écrire les entrées Keep-a-Changelog dans
    `docs/audits/changelog/A25.md` sous `### Fixed` (ou autre section) —
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
    `audit/25-nat-traversal`** — jamais sur `main`. **Tout reste local** :
    pas de push, pas de PR, pas de GitHub. Signaler « prêt à merger »
    et **s'arrêter** — le merge n'a lieu que sur ordre explicite de
    l'humain (« merge »). Sur cet ordre : `git merge
    audit/25-nat-traversal` dans le checkout principal — **un merge à la
    fois** ; si conflits (`torrent.rs`/`peer_conn.rs`/`session.rs`/
    `PORTING.md`), rebase de la branche sur `main` frais **dans le
    worktree** + re-run des gates, puis nouveau `git merge`.
13. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit).
    **Hygiène post-merge** : `git worktree remove
    ../LibtorrentRust-a25` puis `git branch -d audit/25-nat-traversal`.
    Une branche mergée qui traîne encombre la vue et trompe la
    planification.

## Pièges connus (checklists transverses)

- Compteurs `counters::` déclarés mais jamais incrémentés/décrémentés.
- Legs silencieux upstream (validation early-return, `is_disconnecting`,
  `is_deleted`, `!has_picker`, `!has_storage`, `session_is_closing`).
- Ordre effets wire-visible : messages sortants dans l'ordre upstream exact.
- Comportement dead-code upstream à conserver inerte (ex. `cancel_block`
  post-`mark_as_writing` — `is_requested` false en `state_writing`).
- `unwrap`/`expect`/`todo!` interdits hors tests ; `#![forbid(unsafe_code)]`.
