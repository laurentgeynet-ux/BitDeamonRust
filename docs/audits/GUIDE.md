# Guide utilisateur — audits de parité BitDaemon vs libtorrent

Mode d'emploi du système d'audits en deux phases (rapports parallèles,
puis corrections en worktrees + PRs). Le registre et les vagues sont
dans [`INDEX.md`](INDEX.md) ; ce guide explique **comment les
opérer**, pas ce qu'ils contiennent.

## Le principe en 30 secondes

Chaque audit A01…A29 a deux modes. **Les numéros sont l'ordre
d'application conseillé** — un agent seul applique A01→A29 ; les
audits `done` (A08–A10, A14–A15) gardent leur place.

| Mode | Ce qu'il fait | Parallélisable ? |
|---|---|---|
| `rapport` | Analyse C++ vs Rust, écrit `docs/audits/reports/ANN_<slug>.md`, le commit sur `main` | **Oui, totalement** — read-only, un seul fichier écrit |
| `apply` | Corrige le code dans un worktree + branche `audit/NN-<slug>`, gates, fragment changelog, PR | Oui, **par worktree**, merges séquentiels |

Pourquoi ce découpage : la phase coûteuse (lire le C++ upstream) est
read-only → elle scale à l'infini. La phase dangereuse (modifier le
code, compiler, commiter) est isolée par worktree → pas de verrous
`target/` partagés, pas de `fmt --all` croisés, pas de conflit sur
`CHANGELOG.md` (chaque audit écrit un fragment).

## Prérequis (une fois)

- [ ] `main` propre : aucun travail non commité dans le checkout
      principal (les worktrees `apply` partent de `main` — le
      non-commité n'est ni propagé ni protégé).
- [ ] `gh` CLI installé et authentifié (les PRs sont ouvertes par
      `gh pr create`).
- [ ] Checkout upstream C++ résolvable : `$LIBTORRENT_ROOT` défini, ou
      `D:/Projet/libtorrent/libtorrent` présent. En session cloud, le
      skill clone `arvidn/libtorrent` au baseline `e7381c7bc` tout seul.
- [ ] (optionnel, recommandé) `cargo install sccache` —
      mutualise la compilation des deps entre worktrees :
      `RUSTC_WRAPPER=sccache` (bash) ou `$env:RUSTC_WRAPPER="sccache"`
      (PowerShell).

## Phase 1 — produire les rapports

Dans n'importe quelle session Devin ouverte sur le repo (même
dossier, plusieurs sessions en même temps — c'est prévu pour) :

```
/audit-05-v1-utp-transport
```

Le mode `rapport` est le défaut ; inutile de le préciser. L'agent :

1. lit le chemin Rust, puis les fonctions upstream ciblées par grep
   (jamais les fichiers entiers — certains font 10 000 lignes) ;
2. écrit `docs/audits/reports/A05_utp-transport.md` (déviations +
   fichiers que le fix touchera + non-bugs tranchés) ;
3. le commit sur `main` en pathspec-limited (`git commit -- <path>`) :
   n'embarque que son rapport même si un audit voisin a stagé autre
   chose entre-temps.

Vous pouvez lancer les 24 audits restants d'un coup — en sessions
locales parallèles ou en sessions cloud. Aucune coordination
nécessaire : les seuls points de contact sont des commits de fichiers
disjoints, toujours fast-forward.

## Gate humain — la relecture (obligatoire)

**Avant tout `apply`, lisez le rapport.** Chaque déviation listée doit
être confirmée ou contestée :

- Confirmée → reste dans `## Déviations`.
- Contestée / écart volontaire → déplacez-la dans `## Non-bugs
  tranchés` avec la justification, et commitez la correction du
  rapport.

Un rapport brut contient des faux positifs possibles (logique déplacée
dans une crate inférieure, legs intentionnellement divergents). C'est
10 minutes par rapport et ça évite des PRs qui "corrigent" du
comportement correct.

## Phase 2 — apply (worktrees + PRs)

Pour chaque rapport validé :

```sh
git worktree add ../LibtorrentRust-a05 -b audit/05-utp-transport
```

Puis ouvrez une session Devin **sur ce dossier**
(`D:\Projet\LibtorrentRust-a05`) — les skills sont commités, donc
découverts dans le worktree aussi — et lancez :

```
/audit-05-v1-utp-transport en mode apply
```

L'agent corrige, porte les tests de régression, fait tourner les gates
complets dans le worktree (`fmt --all`, `clippy --workspace`,
`test --workspace`, `doc`), écrit le fragment
`docs/audits/changelog/A05.md`, push la branche et ouvre la PR.

**Cas particulier — un seul audit à la fois** : si rien d'autre ne
tourne, le checkout principal suffit (branche `audit/NN-<slug>` en
place). Le worktree n'est obligatoire que pour la concurrence.

### Les vagues (d'INDEX.md)

| Vague | Audits | Contrainte |
|---|---|---|
| 1 — isolés | A05, A16, A17, A18, A24, A25, A26 | parallèle libre |
| 2 — session-edge | A04, A06, A13, A21, A22, A23, A27, A28 | parallèle libre |
| 3 — net-peer | A01 puis A03 | `peer_wire.rs` commun → séquentiels |
| 4 — session-core | A02, A07, A11, A12, A19, A20 | max 2-3 concurrents |
| 5 — final | A29 | seul, en dernier (transverse) |

### Merges

- **Une PR à la fois.** En cas de conflit sur `torrent.rs` /
  `peer_conn.rs` / `session.rs` : l'agent rebase sur `main` frais,
  re-run des gates, puis merge.
- Après merge : `git worktree remove ../LibtorrentRust-aNN`.

### Limites machine

En local, 2-4 `apply` concurrents réalistes — cargo sature déjà tous
les cœurs par build. Pour du parallélisme massif : sessions Devin
Cloud séparées (une par audit — machine, checkout et `target/`
dédiés, zéro contention).

## Phase 3 — agrégation changelog (une fois, à la fin)

Après merge des PRs :

```sh
python docs/audits/_aggregate_changelog.py
git add CHANGELOG.md docs/audits/changelog
git commit -m "docs(changelog): aggregate audit entries"
```

Le script fusionne les fragments `docs/audits/changelog/A*.md` dans
`CHANGELOG.md` sous `## [Unreleased]` (première section `### Fixed` /
`### Added` correspondante ; les sections inconnues sont créées en fin
de bloc), puis supprime les fragments. Relisez le diff avant de
committer.

## Anti-patterns — ce qu'il ne faut PAS faire

- ❌ Lancer `apply` sur un rapport non relu.
- ❌ `apply` dans le checkout principal pendant qu'un autre `apply`
  tourne (verrou `target/`, `fmt` croisés, tests sur code transitoire).
- ❌ Éditer `CHANGELOG.md` ou `PORTING.md` dans une branche apply
  (fragments `changelog/ANN.md` à la place ; `PORTING.md` seulement
  pour les rows du périmètre).
- ❌ Push sur `main` depuis un apply — toujours branche + PR.
- ❌ Lancer A29 avant la fin des autres (il re-audite les compteurs
  que les fixes précédents auront touchés).
- ❌ Modifier le checkout upstream — lecture seule, toujours.

## FAQ

**Un audit marqué "done" (A08–A10, A14–A15) se relance ?**
Oui — et le skill ne sait PAS qu'il est déjà passé : un re-run est un
audit complet à blanc, jamais une vérif allégée. Le statut `done` ne
sert qu'à ton suivi (colonne Statut de l'INDEX) et au filtrage de la
table des vagues — il ne biaise pas l'analyse de l'agent.

**Le rapport dit « fix toucherait torrent.rs » mais l'audit est en
vague 1 — contradiction ?**
Non : les vagues se basent sur le périmètre *déclaré*, la section
`## Fichiers du fix` du rapport affinera la réalité. Si un rapport
révèle un recouvrement chaud, re-planifiez-le vers la vague 4.

**Conflit de merge sur `torrent.rs` ?**
Normal en vague 4 — c'est pour ça que les merges sont séquentiels.
`git checkout main && git pull && git rebase main` dans la branche,
re-run gates, merge.

**Une session cloud n'a pas `D:\` ?**
Le skill résout upstream via `$LIBTORRENT_ROOT` puis clone
`arvidn/libtorrent` au baseline `e7381c7bc` si absent — rien à faire.

**Modifier le protocole pour les 29 audits ?**
Éditez `docs/audits/_gen_audits.py` (constante `PROTOCOL` + table
`AUDITS`), relancez `python docs/audits/_gen_audits.py` — tout est
régénéré, rien d'écrit à la main dans les SKILL.md n'est perdu (il ne
faut rien y écrire à la main, justement).
