# Audit A20 — rapport d'analyse

Périmètre : machine d'état torrent (`src/torrent.cpp` : `set_state`,
`files_checked`, `finished`, `resume_download`, `completed`,
`pause`/`set_paused`/`do_pause`, `resume`/`do_resume`,
`start_announcing`/`stop_announcing`, `stop_when_ready`,
`leave_seed_mode`, `save_resume_data`, `state_updated` ;
`session_impl::recalculate_auto_managed_torrents` +
`auto_manage_{checking_,}torrents`).

Baseline upstream : `e7381c7bc` (`D:/Projet/libtorrent/libtorrent`).

Modèle Rust : pas de `m_state` — `Torrent::state()`
(`torrent.rs:9638`) dérive `TorrentState` des flags
`checking`/`checking_resume`/`magnet`+picker/`finished`/`seeding`.
`Torrent::set_state` (`torrent.rs:11963`) n'est appelé par personne
(aucun caller dans `crates/`) — les sites de transition écrivent les
flags directement. La session diff `t.state()` en fin de tick pour
`StateChanged` (`session.rs:7221-7247`).

## Verdicts checklist

- Ladder : checking_files/checking_resume/downloading_metadata/
  downloading/finished/seeding + transitions interdites — verdict :
  **conforme** — preuve : `Torrent::state()` dérive correctement les 7
  états (`torrent.rs:9638-9657`) ; `arm_full_recheck`
  (`torrent.rs:8410-8429`, gate `picker.is_none() || checking`),
  `recheck_piece_done` (`torrent.rs:8435`), `on_files_checked`
  (`torrent.rs:2963-3083`), edge finished dans `on_piece_pass`
  (`torrent.rs:7338-7378`), `leave_seed_mode` (`torrent.rs:7814-7842`).
  Les asserts upstream (`set_state` : seeding⇒is_seed etc.) sont
  debug-only — pas de runtime à porter (cf. Non-bugs).
- paused : `m_paused | m_session_paused`, graceful pause — verdict :
  **conforme** — preuve : `is_paused()` = `paused || session_paused`
  (`torrent.rs:10276`) ; `set_paused_mode` (`session.rs:3012-3059`)
  reflète `set_paused` (strip graceful sans peers :3031,
  graceful→hard :3035, flip effectif :3042-3048) ; `do_pause`
  (`session.rs:3099-3227`) : veto extension, announce_* off,
  `checking_files` → disconnect_all + alert différée à la drainée des
  hash jobs (:3146-3152, upstream `m_checking_piece ==
  m_num_checked_pieces` ≈ `checking_pieces <= 0` vu que tous les jobs
  sont dispatchés et s'exécutent), graceful → choke busy/disconnect
  idle sans alerte immédiate, `stop_announcing` + `stopped` events ;
  `GracefulPauseDone` (`session.rs:8860`) re-passe en do_pause quand
  le dernier peer part ; `set_session_paused` (`session.rs:5507-5534`)
  ne bascule que sur flip effectif. `on_torrent_paused` Rust
  (`torrent.rs:10444`) est plus léger qu'upstream (voir Déviation 6).
- `state_updated()` sites exhaustifs — verdict : **conforme** —
  preuve : `state_updated()` (`torrent.rs:7727`) = bit
  `needs_state_update` gated `state_subscription` (= upstream
  `m_state_subscription` early-return, torrent.cpp:12430), drainé par
  `post_torrent_updates` (`session.rs:9881`). Sites vérifiés :
  `add_swarm_peer` (:3287), `set_metadata` (:1891), `on_files_checked`
  (:2988), `piece_passed` (:7315), finished edge (:7343), pause/resume
  (session.rs:3121, 3255), `set_flags` (:7804), priority updates
  (:8128, :8137), queue flips (`session.rs:3477, 3554, 3565`),
  `recheck_piece_done` (:8441). Résidu : `StateChanged` coalescé en
  fin de tick (Déviation 7).
- `need_save_resume` — CHAQUE site upstream — verdict : **déviation**
  — preuve : upstream stocke un bitmask
  (`m_need_save_resume_data`, `torrent_handle::if_*` bits 3-7) et
  `save_resume_data(flags)` filtre par catégorie
  (`torrent.cpp:10104-10120`) ; Rust a un seul bool
  `need_save_resume` (`torrent.rs:664`) et `save_resume_data` n'a que
  `only_if_modified` (`session.rs:9833`, handle `session_handle.rs:1172`).
  Voir Déviations 3 et 4.
- seed_mode : entrée (flag + all pieces + no dont_download), sortie →
  force_recheck — verdict : **conforme** — preuve : ctor gate
  `prios_ok` = pas de `dont_download` (fichier+pièce) et
  `have_pieces.all_set()` (`torrent.rs:1392-1407`, upstream
  torrent.cpp:409-412) ; `leave_seed_mode(check_files)` →
  `have.clear_all()` + `arm_full_recheck` + `DiskHash` par pièce
  (`torrent.rs:7814-7842`, upstream torrent.cpp:542-582, gate
  `checking_resume` équivalent à `state() != checking_resume_data`) ;
  sortie en `skip_checking` sur `all_verified()` via
  `seed_mode_hashed` (`torrent.rs:7855+`) ; clear via `set_flags`
  (`torrent.rs:7799-7801`, upstream torrent.cpp:985-988).
- auto_managed : transitions paused↔queued↔active, ordre
  `manage_queue` — verdict : **conforme** (avec réserve, Déviation 4)
  — preuve : `manage_queue` (`session.rs:3414-3576`) reflète
  `recalculate_auto_managed_torrents` (session_impl.cpp:4206-4274) :
  early-return session paused, listes checking/downloaders/seeds =
  `auto_managed && !last_error` + state mapping (checking_files →
  checking ; checking_resume_data → hors liste ; seeds = `t.finished`
  ≈ `is_finished()` upstream), `partial_sort` équivalent (sort seulement
  si `hard_limit > 0`), inactifs consomment dht/tracker/lsd/hard sans
  `type_limit`, within-limit → `set_paused(false)` + `queued` off,
  beyond → `set_paused(true, graceful)` + announce off. Ordre
  seeds/downloaders selon `auto_manage_prefer_seeds`. Checking:
  `pause()` hard au-delà de `active_checking` — Rust marque
  `need_save_resume` ici comme upstream `t->pause()`.
- stop_when_ready : edge finished → pause — verdict : **déviation** —
  preuve : upstream déclenche sur la transition *vers* un état de
  téléchargement (`!is_downloading_state(m_state) &&
  is_downloading_state(s)`, torrent.cpp:12364-12366) — i.e. « as soon
  as it's done checking » (torrent_flags.hpp:130-146), plus le
  déclenchement immédiat dans le setter (torrent.cpp:12320-12328) —
  avec `auto_managed(false); pause(); m_stop_when_ready=false`. Rust
  déclenche sur l'edge *finished* (`torrent.rs:7363`) par un simple
  `self.paused = true`, sans `do_pause`, sans clearer le flag ni
  `auto_managed`, et le flag n'est ni appliqué depuis `atp.flags` ni
  traité par `set_flags`/`stop_when_ready()` handle. Voir Déviation 1.

## Déviations

- `torrent.rs:7363-7365`, `session_handle.rs:1670-1674`,
  `torrent.rs:1107`, `torrent.rs:7739+` — `stop_when_ready` porté sur
  la mauvaise edge et dépouillé de sa machinerie. Upstream
  (`torrent.cpp:12314-12396`) : le flag se déclenche quand le torrent
  *quitte* un état non-downloading (checking_*) vers un état
  downloading (download_metadata/downloading/finished/seeding) — ou
  immédiatement dans le setter si déjà en downloading state. Le
  déclenchement fait `auto_managed(false)` + `pause()` (machinerie
  complète : announces stopped, disconnects, `TorrentPaused` alert,
  `need_save_resume(if_state_changed)`) puis clear le flag. Rust :
  (a) edge erronée — pausé à la fin du *download* au lieu de la fin du
  *check* ; (b) `self.paused = true` ne passe pas par `do_pause` — pas
  de stopped announce, pas de disconnect, pas d'alerte, pas de
  `need_save_resume`, pas de `state_updated` ; (c) flag jamais cleared
  au déclenchement (seulement dans `on_torrent_paused` :10445, que
  upstream n'utilise pas pour ça) ; (d) `atp.flags STOP_WHEN_READY`
  jamais appliqué au ctor (:1107 force `false`) ; (e) `set_flags`
  ignore le bit (upstream torrent.cpp:1013-1014 route vers le setter) ;
  (f) le setter handle n'a pas le déclenchement immédiat.
  Sévérité : état (comportement user-visible + alertes manquantes).
  - **sketch** : ajouter `Torrent::set_stop_when_ready(&mut self, b: bool,
    out)` — upstream `stop_when_ready()` : set flag ; si `b &&
    is_downloading_state(state())` → pousser `TorrentEffect::` dédié
    (ou flag consommé par session) pour `auto_managed=false;
    do_pause-like; stop_when_ready=false`. Détecter la vraie edge :
    dans `on_files_checked` (et tout site qui éteint `checking` /
    `checking_resume`), si `stop_when_ready && !was_downloading &&
    is_downloading_state(state())` → même séquence. Plus simple et
    fidèle : calculer `was_dl` avant la mutation et `is_dl` après dans
    `on_files_checked`, `arm_full_recheck`-drain (`recheck_piece_done`
    retourne true) et `leave_seed_mode`. Retirer le bloc :7363-7365.
    Ctor : `t.stop_when_ready = atp.flags.contains(F::STOP_WHEN_READY)`
    (upstream torrent.cpp:248). `set_flags` : `if
    mask.contains(F::STOP_WHEN_READY) { self.set_stop_when_ready(...) }`.
    `on_torrent_paused` : ne plus clearer le flag (upstream le clear
    uniquement au déclenchement). Handle `stop_when_ready(on)` doit
    router la séquence dans session (il a besoin de `&mut Session`
    pour `do_pause`). Réf. upstream torrent.cpp:12314-12404.
    Test : ajout checking+stop_when_ready → `TorrentPaused` +
    `auto_managed=false` quand `files_checked` passe ; setter sur un
    torrent déjà downloading → pause immédiate.
- `torrent.rs:7347-7359` — disconnect des peers upload-only à
  `finished()` sans gate `close_redundant_connections`. Upstream
  (`torrent.cpp:9016-9037`) n'opère le sweep `p->upload_only() &&
  can_disconnect(torrent_finished)` que si le setting est on (défaut
  true, mais désactivable). Bonus : upstream vérifie
  `p->can_disconnect(errors::torrent_finished)`. Sévérité :
  wire-visible (connexions coupées alors que le setting est off).
  - **sketch** : gate le bloc `uo` par `self.close_redundant_connections`
    (le champ existe, :852). `can_disconnect` ≈ notre
    `!c.disconnecting` — ajouter le check.
    Réf. torrent.cpp:9016. Test : `close_redundant_connections=false`
    + piece_passed → seed non déconnecté.
- `torrent.rs:664`, `session.rs:9833` — `need_save_resume` bool vs
  bitmask `m_need_save_resume_data` upstream. Conséquences :
  `save_resume_data(if_counters_changed)` devrait sauver alors que
  seul un flag state_changed est dirty et inversement — impossible à
  distinguer ; `need_save_resume_data(flags)` handle absent. Sévérité
  : état/API (le comportement `only_if_modified` — union de tous les
  bits — est correct ; seul le filtrage par catégorie manque).
  - **sketch** : `need_save_resume: bool` → `need_save_resume:
    ResumeDataFlags` (bitflags : COUNTERS=bit3, DOWNLOAD_PROGRESS=bit4,
    CONFIG=bit5, STATE=bit6, METADATA=bit7 — numérotation
    torrent_handle.hpp:716-757). Remplacer les `= true` par
    `|= F::STATE_CHANGED` etc. selon la catégorie du site (les sites
    sont déjà annotés `if_*` dans les commentaires) ;
    `save_resume_data(flags)` : `conditions = flags & ALL; if
    conditions.any() && !t.need_save_resume.intersects(conditions) →
    SaveResumeDataFailed` ; clear `t.need_save_resume = NONE`. Handle :
    ajouter `save_resume_data_flags(flags)` et
    `need_save_resume_data(flags)`. Réf. torrent.cpp:10092-10136.
    Test : marquer seulement counters → `save_resume_data(STATE)`
    échoue `resume_data_not_modified`, `(COUNTERS)` réussit.
- `session.rs:3026-3028` — `set_paused_mode` marque
  `need_save_resume` sur tout flip `paused`, y compris les démotions
  auto-manage (`manage_queue` :3568). Upstream : seul `torrent::pause()`
  (chemin user + checking-demotion) marque `if_state_changed`
  (torrent.cpp:10213) ; `set_paused` — utilisé par
  `auto_manage_torrents` — ne marque pas. Effet : churn de queue
  → resume-dirty → `save_resume_data` spurius + timing d'alertes.
  Sévérité : état mineur.
  - **sketch** : descendre la marque hors de `set_paused_mode` —
    paramètre `mark_save: bool` ; `set_paused`/`set_paused_graceful`
    (user) et le bras checking de `manage_queue` passent `true`, le
    bras downloaders/seeds `false`. Ou dédier `pause_user` vs
    `pause_auto`. Réf. torrent.cpp:10205-10217 vs session_impl.cpp:4191.
    Test : auto-manage démotive un torrent → `need_save_resume`
    reste false ; `pause()` user → true.
- `session.rs:9020-9110` (handler `BecameSeed`) — `finished()`
  upstream fait aussi : `async_release_files` (fermeture/flush des
  handles → réouverture read-only à la demande, torrent.cpp:9043-9049)
  et `if (m_auto_managed) m_ses.trigger_auto_manage()` (9054-9055).
  Rust : ni `ReleaseFiles`, ni `auto_manage_scaler = 0` — la recalcule
  attend le prochain tick `auto_manage_interval` (session.rs:6812-6815),
  et les fichiers restent ouverts en écriture. Sévérité : état.
  - **sketch** : dans le handler `BecameSeed`, si `t.auto_managed` →
    `self.auto_manage_scaler = 0` ; si `t.storage != INVALID` →
    `disk.async_release_files(storage, …)` en réutilisant le chemin de
    `flush_cache` (`session.rs:5294-5299`). Réf. torrent.cpp:9039-9055.
    Test : deux torrents auto-managed, `active_downloads=1` — le finish
    du premier démarre le second sans attendre l'intervalle.
- `torrent.rs:8129-8138` — `resume_download` leg de
  `update_peer_interest` ne remet pas le torrent en fin de queue.
  Upstream `resume_download` fait `set_queue_position(last_pos)`
  (torrent.cpp:9075) — le torrent re-téléchargeant repart en queue de
  file ; Rust garde l'ancien `queue_seq` → il récupère sa position
  antérieure (peut sauter devant des torrents qui ont attendu).
  Sévérité : état (ordre de queue).
  - **sketch** : dans le leg `was_finished && !finished`, assigner un
    nouveau `queue_seq` = max des non-finished + 1 — le champ vit en
    session (upstream `m_sequence_number` alloué par la session), donc
    `update_peer_interest` devra remonter un effect ou la session
    réassigne au retour de l'appel. Réf. torrent.cpp:9074-9075 +
    `session_impl::set_queue_position`. Test : A finished, B
    downloading ; un-prioritize un fichier de A → A repasse
    downloading *derrière* B.
- `session.rs:3233-3331` (`do_resume`/`start_announcing` équivalent) —
  trois éléments de `start_announcing` upstream non portés :
  (a) `m_total_failed_bytes = 0; m_total_redundant_bytes = 0;
  m_stat.clear()` (torrent.cpp:10759-10761) — les compteurs de session
  d'announce sont censés repartir à zéro à chaque reprise annoncée ;
  Rust conserve `failed_bytes`/`redundant_bytes`/`protocol_*` sur le
  cycle pause→resume → `status().total_*` dérive ; (b) le gate
  `!m_files_checked && valid_metadata() → return` (10734) — un torrent
  resumed avant sa première vérification ne doit pas annoncer ; (c)
  `high_priority = m_connect_boost_counter > 0` propagé à
  `announce_with_tracker` (10765-10766). Sévérité : état/stats.
  - **sketch** : dans `do_resume`, avant les announces : `t.failed_bytes
    = 0; t.redundant_bytes = 0;` + reset des accumulateurs
    `protocol_*`/conn-stat si le modèle le permet (upstream clear
    `m_stat` entièrement — vérifier ce qui est « per-announcing-session »
    chez nous) ; gate `if !t.files_checked && t.valid_metadata() {
    return }` avant `t.announcing = true` ; propager un
    `high_priority = t.connect_boost_counter > 0` dans `trackers.due`
    (ajouter le param). Réf. torrent.cpp:10720-10770.
- `session.rs:7221-7247` — `StateChanged` coalescé en fin de tick sur
  diff de `state()` : deux transitions dans le même tick produisent une
  alerte avec `prev` = état début-de-tick (upstream poste à chaque
  `set_state` avec le vrai prev). Ex. checking→downloading→finished
  dans un tick rapporte prev=checking_files, next=finished — upstream
  émet deux alertes. Sévérité : mineure (alertes).
  - **sketch** : si fidélité stricte voulue, pousser
    `TorrentEffect::StateChanged { prev, next }` aux sites de
    transition au lieu du diff de tick ; sinon documenter comme
    déviation acceptée. Réf. torrent.cpp:12349-12355.
- `session.rs:3099-3227` — `do_pause` non-graceful poste
  `TorrentPaused` immédiatement ; upstream diffère l'alerte à
  `on_torrent_paused` (fin du `async_stop_torrent` — flush+close des
  fichiers, torrent.cpp:10288-10296) quand `m_storage` existe. Aussi
  `pause_flags_t::clear_disk_cache` (utilisé par la démotion
  auto-manage, session_impl.cpp:4191-4192) n'est pas modélisé —
  upstream relâche le cache disque à la démotion. Sévérité : état
  (timing d'alerte / cache).
  - **sketch** : émettre `DiskCompleted::StopTorrent`-like après
    `async_stop_torrent` (ou `async_release_files` comme approximation
    si stop_torrent n'existe pas dans `bitdaemon-disk`) et poster
    `TorrentPaused` là ; `on_torrent_paused` redevient le callback qui
    poste l'alerte (et ne clear pas `stop_when_ready` — voir D1).
    Ajouter un bit `clear_disk_cache` au chemin de pause démotion →
    `ReleaseFiles`. Réf. torrent.cpp:10281-10299, 5565-5572.

## Fichiers du fix

- `crates/bitdaemon-session/src/torrent.rs` — `set_stop_when_ready`,
  ctor atp flags (`STOP_WHEN_READY`, `PINNED` — voir note),
  `leave_seed_mode`/edge download dans `on_files_checked` +
  `recheck_piece_done`, gate `close_redundant_connections` sur le
  sweep finished, `update_peer_interest` resume leg (queue seq via
  effect), `need_save_resume` → bitmask, `set_flags` STOP_WHEN_READY.
- `crates/bitdaemon-session/src/session.rs` — `set_paused_mode`
  (param mark_save), `do_pause` (alerte différée / clear_disk_cache),
  `do_resume` (stat reset, gate files_checked, high_priority),
  `BecameSeed` (auto_manage re-arm + release_files), StateChanged
  timing (si retenu).
- `crates/bitdaemon-session/src/session_handle.rs` —
  `stop_when_ready(on)` route la séquence ; `save_resume_data(flags)`
  + `need_save_resume_data(flags)` si le bitmask est porté.
- `crates/bitdaemon-common/src/flags.rs` — éventuel `ResumeDataFlags`.

Note annexe : `pinned` n'est pas non plus appliqué depuis
`atp.flags` au ctor (`torrent.rs:1106` force `false` ; seul
`set_pinned` handle existe, session_handle.rs:1663). Upstream :
`m_pinned(p.flags & torrent_flags::pinned)` (torrent.cpp ~248). À
fusionner dans le même fix ctor que STOP_WHEN_READY.

## Non-bugs tranchés

- `Torrent::set_state` sans callers + `update_state_list` stub
  (`torrent.rs:10833`) : les listes auto-manage sont recalculées dans
  `manage_queue` à chaque passe au lieu d'être maintenues
  incrémentalement — équivalent fonctionnel, modèle pull du port.
- `update_gauge`/`update_want_peers`/`update_want_tick` dans le
  `set_state` upstream : chez nous les états (want_peers, gauge
  counters) sont des prédicats évalués à la demande
  (`want_peers_download`, `want_peers_finished`, `want_tick`) — pas de
  liste à invalider.
- `paused` pendant `checking_files` : upstream diffère l'alerte à
  `m_checking_piece == m_num_checked_pieces` ; Rust à
  `checking_pieces <= 0`. Comme tous les `DiskHash` sont dispatchés
  d'un coup et s'exécutent à terme, les deux postent quand les jobs en
  vol ont fini — équivalent.
- `connect_boost_counter` armé dans `do_pause` upstream
  (torrent.cpp:10247) vs `do_resume` chez nous (session.rs:3252) :
  résultat identique — rien ne lit le compteur entre pause et resume.
- `on_torrent_paused` upstream ne fait que poster
  `torrent_paused_alert` ; le clear `stop_when_ready` Rust est une
  divergence — traitée dans Déviation 1 (sera corrigée là, pas ici).
- `TorrentPaused` synchrone vs post-`async_stop_torrent` : classé
  déviation mineure (timing) plutôt que non-bug car l'alerte upstream
  garantit « fichiers fermés » à l'émission.

## Résultat

Corrections appliquées sur `audit/20-torrent-state-machine` :

- **D1 corrigée** — `stop_when_ready` restauré depuis `atp.flags`
  (ctor), routé par `set_flags` (`TorrentEffect::SetStopWhenReady`),
  setter handle à trigger immédiat en état downloading ; l'edge
  checking→downloading émet `StopWhenReadyCheck` → session exécute
  `auto_managed(false)` + `pause()` complet puis désarme
  (`torrent.rs`, `session.rs`).
- **D2 corrigée** — sweep de déconnexion des seeds dans `finished()`
  gaté par `close_redundant_connections`.
- **D3 corrigée** — `need_save_resume` devient `ResumeDataFlags`
  (`bitdaemon-common::flags`) ; tous les sites de mutation taggués
  par catégorie ; `save_resume_data(flags)` filtre `ONLY_IF_MODIFIED`
  sur le mask et le clear après save.
- **D4 corrigée** — `set_paused` (démotion auto-manage) ne marque
  plus le resume dirty ; seul `pause()` marque `IF_STATE_CHANGED`.
- **D5 corrigée** — `finished()` émet `async_release_files`
  (`DiskCompleted::ReleaseFiles` → `CacheFlushed`) et re-trigger
  `manage_queue` via `BecameSeed`.
- **D6 corrigée** — `resume_download` émet `QueueTail`
  (`set_queue_position(last_pos)`).
- **D7 corrigée** — `start_announcing` : gate `!paused &&
  files_checked`, reset `total_failed`/`total_redundant_bytes`/stats
  torrent, `high_priority` depuis connect boost.
- **D8 corrigée** — `StateChangedEdge` émis par transition
  (`state_prev`), diff de tick supprimé.
- **D9 corrigée** — `TorrentPaused` différé via
  `paused_alert_pending` jusqu'à la fin du drain graceful /
  `async_stop_torrent`.
- **Note annexe** — ctor restaure aussi `pinned`,
  `apply_ip_filter`, update subscription, sequential/upload/share/
  super-seeding et disable DHT/LSD/PEX depuis `atp.flags`.

Tests : `test_state_machine_a20.rs` — 9 cas (edge stop_when_ready,
pas d'edge entre états downloading, setter immédiat, désarmement,
gate close_redundant, démotion non-dirty, queue tail, bitmask
resume, StateChanged par transition) — tous verts.

Gates : `cargo fmt --all`, `cargo clippy --workspace --all-targets
-- -D warnings`, `cargo test --workspace` (0 échec),
`cargo doc --workspace --no-deps` — toutes vertes.

Row `torrent.cpp` de `docs/PORTING.md` enrichie (clause A20) ;
fragment changelog `docs/audits/changelog/A20.md`.
