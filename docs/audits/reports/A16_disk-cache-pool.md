# Audit A16 — rapport d'analyse

Périmètre : cache disque & buffer pool — `disk_cache.cpp` (write cache),
`disk_buffer_pool.cpp`, `aux_/store_buffer.hpp`, `back_pressure.cpp`,
legs cache de `pread_disk_io.cpp`. Baseline upstream `e7381c7bc`.
Côté Rust : `crates/bitdaemon-disk/src/{disk_cache,disk_buffer_pool,
store_buffer,back_pressure,disk_io}.rs`.

## Verdicts checklist

- Coalescing : runs contigus par pièce, un syscall par run — verdict :
  **conforme (étendu)** — preuve : `disk_cache.rs:518` (`take_flush_run`
  = run contigu, buffers + writers déplacés ensemble), `disk_cache.rs:664`
  (`take_coalesced` prolonge le run **au-delà** de la frontière de pièce en
  un seul writev plat, `disk_io.rs:1492-1529`). Upstream
  `flush_cache_blocks`/`visit_block_iovecs` (`pread_disk_io.cpp:1770-1855`)
  émet un writev par run contigu *au sein d'une pièce* ; notre coalescing
  inter-pièces écrit les mêmes octets aux mêmes offsets (pièces
  séquentielles dans le fichier) — équivalent filaire, voir Non-bugs.
- Watermark : seuil haut blocage vs bas déblocage, hystérésis exacte —
  verdict : **conforme** (formules) / **déviation** (boucle de flush ne
  s'arrête pas au low watermark + niveau évalué par storage au lieu de
  global) — preuve formules : `back_pressure.rs:94-103` ==
  `back_pressure.cpp` `set_max_size` (low=`max/4*3` clamp `INT_MAX/3`,
  high=`max/8*7` clamp `INT_MAX/2`), `should_flush`/`has_back_pressure`/
  `check_buffer_level` identiques (`back_pressure.rs:113-171` vs
  `back_pressure.cpp:35-65`). Déviation : `disk_io.rs:984-1001`.
- Éviction : ordre upstream, dirty expiry `cache_expiry` flushes stale —
  verdict : **déviation (mineure)** — ordre : conforme au design
  (ready → sweep ascendant `(storage,piece)` ≈ passes 1+4 upstream,
  la passe 2 « cheap `hasher_cursor - flushed_cursor` » est vide sous
  notre hasher whole-piece — `disk_cache.rs:649-663` le documente) ;
  **mais** `cache_expiry` est `DEPRECATED_SET` upstream
  (`settings_pack.cpp:264`, aucun consommateur) alors que nous
  implémentons un flush périodique réel (`disk_io.rs:361-390`,
  `session.rs:1571`). Voir Déviations D4.
- Pool : `max_queued_disk_bytes` cap, `check_buffer_level`, `job_done`
  comptage — verdict : **déviation** — cap conforme
  (`disk_io.rs:315-316` == `pread_disk_io.cpp:462`,
  `set_max_queued_bytes/0x4000` ; back-pressure soft cap via
  `has_back_pressure` == `account_pending_write` `buffer_level()+1`,
  `disk_io.rs:2409-2410`) ; **comptage fuyant** sur double écriture
  d'un même bloc (D2) et niveau de flush par-storage au lieu de
  global (D1/D3). `job_done` décrémente au bon point
  (`disk_io.rs:551-609` ≈ `m_blocks--` post-`f()` dans
  `flush_piece_impl` `disk_cache.cpp:804-848`).
- Read-side : consult cache avant disque (prepare_read/get2/
  partial_read), spanning reads — verdict : **conforme** — preuve :
  `disk_io.rs:1794-1907` (`prepare_read` : hit complet → complète sans
  I/O ; spanning `get2` → côté manquant devient `PartialRead`,
  `disk_io.rs:1863-1885` == `pread_disk_io.cpp:669-691` bitmask→
  partial_read ; échec alloc → `no_memory`/`alloc_cache_piece`,
  `disk_io.rs:1825-1833` == `pread_disk_io.cpp:652-658`). Overlay
  post-lecture disque (`overlay_cached`, `disk_io.rs:1698-1726`) est
  un ajout volontaire — Non-bugs.
- FLUSH_PIECE/force_flush_flag drain une pièce indépendamment du
  watermark — verdict : **conforme** — preuve :
  `disk_io.rs:968-978` (`forced` → `take_all_runs`, hors boucle
  watermark) + `hash_returned` → `ready` + `take_all_runs`
  (`disk_io.rs:632-652`, `disk_cache.rs:902-927`) ≈ passe 1 de
  `flush_to_disk` (`disk_cache.cpp:968-1009`, `force_flush_flag`
  sans check de target).
- Superseded/teardown writers : abort exactly-once — verdict :
  **conforme** — preuve : supersede → `orphans` + abort
  (`disk_cache.rs:340-343`, drainé par `drain_cache_orphans`
  `disk_io.rs:867-876` → `add_completed_job` → `job_done` +
  callback) ; teardown : `abort_all_writers` (`disk_cache.rs:382`,
  appelé par `abort_jobs` `disk_io.rs:511` et `RemoveTorrent`
  `disk_io.rs:1626`), `remove_piece`→orphans
  (`disk_cache.rs:428-442`), `Drop` abort les restes
  (`disk_cache.rs:260-271`). Upstream : `clear_piece_impl`/
  `aborted` out-params (`disk_cache.cpp:205-252`, `1414`).

## Déviations

### D1 — La boucle watermark vide *tout* le cache au lieu de s'arrêter au low watermark
`disk_io.rs:984-1001` — le `while let Some(low) = should_flush(
cache.num_cached_blocks)` teste `num_cached_blocks <= low`, mais
`num_cached_blocks` n'est décrémenté qu'à la *complétion* des flushes
(`run_flushed` `disk_cache.rs:854`), pas à la prise des runs. Dans la
boucle le niveau ne bouge donc jamais : une fois `should_flush` actif,
`take_coalesced` est appelé jusqu'à épuisement — chaque insert au-dessus
du high watermark (ou pendant `exceeded`) schedule des Flush pour la
totalité des blocs dirty, pas seulement jusqu'au low watermark.
Upstream `flush_to_disk` (`disk_cache.cpp:1017-1125`) exécute le writev
*synchrone* via `f()` dans chaque passe, donc `buffer_level()` chute
réellement et la boucle sort dès `<= target_blocks` (low watermark).
Conséquence : l'hystérésis haut/bas est morte — rafales de jobs Flush
par insert, flush prématuré de morceaux qui auraient pu être complétés.
Sévérité : état/perf (pas wire-visible).
- **sketch** : ajouter `in_flight_blocks: i64` à `DiskCache` ;
  `take_flush_run`/`take_longest_run` font `+= bufs.len()` (buffers
  sortis dans un Flush), `run_flushed` et `restore_run` font `-= n`.
  La boucle de `flush_after_insert` devient :
  `while let Some(low) = should_flush(level) { if level - in_flight <= low { break } … }`
  où `level = cache.num_cached_blocks` (ou `write_blocks`, cf. D3) et
  `in_flight = cache.in_flight_blocks` — le test reproduit la sortie
  anticipée upstream « post-complétion prédite ».
  Réf. upstream `disk_cache.cpp:1027` (`if (buffer_level() <=
  target_blocks) return;` entre passes synchrones).
  Test de régression : remplir le cache au-delà du high watermark,
  vérifier que les Flush schedulés ne dépassent pas
  `level - low_watermark` blocs.

### D2 — Insert en double d'un bloc : `num_cached_blocks` et `write_blocks` fuient
`disk_cache.rs:355` + `disk_io.rs:2409,551-554` — upstream asserte
`TORRENT_ASSERT(!blk.has_buf())` (`disk_cache.cpp:290`) : un second
write sur un bloc déjà caché est impossible ; `++m_blocks` n'a lieu que
si un buffer entre réellement (`disk_cache.cpp:338`). Chez nous le
supersede est toléré (le vieux writer part en `orphans`, cf.
`disk_cache.rs:340-343`) mais :
- `self.num_cached_blocks += 1` est inconditionnel alors que le buffer
  est remplacé en place → +1 fantôme par doublon ;
- le writer supersédé complète aborted avec `CACHED` → `job_done`
  (`disk_io.rs:551`) saute le décrément `write_blocks`, et son slot
  n'est « transféré » au doublon que comptablement — or le doublon
  s'est *déjà* chargé +1 à `async_write` (`disk_io.rs:2409`) ; le flush
  ne relâche qu'un bloc → +1 fantôme permanent par doublon dans
  `write_blocks` aussi.
Après N doublons (re-requests post-échec/cancel — ça arrive en
production), les deux niveaux dérivent vers le haut : back-pressure
collée, `exceeded` jamais relâché correctement. Sévérité : état
(persistance, croît avec le temps).
- **sketch** :
  1. `disk_cache.rs` `insert` : `if b.buf.is_none() { self.num_cached_blocks += 1 }` (ou `+= (buf remplace un slot vide)`),
     garder la garde `b.buf.is_none()` déjà présente pour `unhashed`.
  2. `disk_io.rs:551` : pour un `Write` complété avec erreur,
     décrémenter `write_blocks` *même* si `CACHED` — un writer aborted
     n'aura jamais son slot couvert par un Flush (les writers d'un
     flush complètent *dans* le job Flush, pas par le bras `Write`).
     `JobKind::Write(_) if j.error.is_err()` → `fetch_sub(1)` +
     `check_buffer_level`, sans le filtre `!CACHED`. Vérifier au
     re-read qu'aucun chemin ne complète un writer CACHED en erreur
     *après* un flush qui aurait déjà décrémenté (le Flush décrète par
     `bufs.len()`, côté buffers, pas côté writers — OK).
  Test : insert ×2 même bloc → `num_cached_blocks` et `write_blocks`
  reviennent à 0 après flush+drain des orphans.

### D3 — Niveau de flush évalué par storage ; upstream est global
`disk_io.rs:984` — `should_flush(cache.num_cached_blocks)` où `cache`
est le `DiskCache` **par `StorageEntry`** ; upstream `buffer_level()`
(`disk_cache.hpp:691-693`) = `m_blocks + m_v2_hash_queue.size() +
m_pending_write_blocks`, compteur **global** tous storages + writes
parkées derrière une fence (`account_pending_write` charge avant
`is_blocked`). Conséquences : (a) un storage peu dirty ne déclenche
jamais la passe globale quand la pression vient d'un autre storage ;
(b) les writes parkées ne poussent pas le niveau vu par la boucle (le
compteur `write_blocks` global, lui, les compte — la pression
observer est correcte, c'est le *déclenchement/arrêt* du flush qui est
étroit). Sévérité : état/perf.
- **sketch** : évaluer la boucle sur `self.write_blocks.load()` (=
  équivalent le plus proche de `buffer_level()`) et drainer en
  itérant *toutes* les entrées de `self.storages` (`take_coalesced`
  par cache, tant que `write_blocks - in_flight_global > low`).
  Réf. upstream `disk_cache.hpp:691`, `disk_cache.cpp:1027/1067/1106`.

### D4 — `cache_expiry` implémenté alors que le setting est un no-op upstream
`disk_io.rs:361-393` + `session.rs:1571` — upstream
`DEPRECATED_SET(cache_expiry, 300, nullptr)` (`settings_pack.cpp:264`)
: rien ne lit le setting, aucun flush temporel de blocs dirty
n'existe dans `pread_disk_io`. Notre `flush_expired` draine
(`take_expired_runs`, `disk_cache.rs:931`) chaque tick de session.
Comportement *supplémentaire* : flush anticipé de pièces partielles
(upstream les garderait jusqu'au watermark/force/fence) → plus de
read-back au hash. Sévérité : perf/comportement. À trancher humain :
soit garder (documenter comme extension), soit supprimer
l'appel/le réglage.
- **sketch** (option retrait) : supprimer `PreadDiskIo::flush_expired`,
  `set_cache_expiry`, le champ `cache_expiry`, l'appel
  `session.rs:1571` ; marquer le setting `deprecated` inerte dans
  `settings.rs:410`. Option conservation : laisser tel quel et
  documenter en Non-bug.

### D5 — `v2_pending` incrémenté, jamais décrémenté ni consulté
`disk_cache.rs:162,353` — `piece.v2_pending += 1` à chaque insert v2 ;
aucun décrément, aucune lecture (le gate de `try_clear_piece` upstream
sur `v2_pending > 0`, `disk_cache.cpp:246-250`, n'existe pas chez nous
— pas de v2 hash queue, ADR-0016). Compteur mort qui monte.
Sévérité : cosmétique.
- **sketch** : supprimer le champ et l'incrément, ou garder avec un
  commentaire « réservé drain v2 » ; préférer la suppression (le
  hasher whole-piece n'a pas de queue à drainer).

### D6 — `need_hasher_kick` peut être re-émis (pas de `needs_hasher_kick_flag`)
`disk_cache.rs:362-367` vs `disk_cache.cpp:351-357` : upstream pose
`needs_hasher_kick_flag` et ne ré-émet qu'après progression du hasher ;
nous ré-émettons à chaque insert sur `hasher_cursor`. Effet : kicks
hasher redondants (idempotent côté session ?). Sévérité : cosmétique.
- **sketch** : ajouter le bit `NEEDS_HASHER_KICK` dans `piece_flags`,
  le poser quand `ret.need_hasher_kick` est émis, l'effacer dans
  `hashed_block` ; `ret.need_hasher_kick` conditionné à l'absence du
  flag.

### D7 — `StoreBuffer::insert` écrase une clé existante (upstream conserve)
`store_buffer.rs:91-92` — `map.insert(loc, buf)` remplace l'entrée ;
upstream `unordered_map::insert` échoue silencieusement si la clé
existe (l'ancien buffer reste, et `m_store_buffer.size()` ne bouge pas
→ back-pressure inchangée). Module **non câblé** (aucun usage hors
tests — `store_buffer` upstream est le cache read de `mmap_disk_io`,
remplacé par `disk_cache` sur pread). Sévérité : cosmétique/dead-code.
- **sketch** : `g.map.entry(loc).or_insert(buf)` + recalcul du level
  seulement si insertion effective — ou marquer won't-port/câblage
  ultérieur avec `bitdaemon-mmap`.

## Fichiers du fix

- `crates/bitdaemon-disk/src/disk_cache.rs` (D1 in_flight, D2 gardes
  compteurs, D5, D6)
- `crates/bitdaemon-disk/src/disk_io.rs` (D1 boucle watermark, D2 bras
  `Write` de `job_done`, D3 niveau global + drain multi-storages,
  D4 `flush_expired`)
- `crates/bitdaemon-disk/src/store_buffer.rs` (D7)
- `crates/bitdaemon-session/src/session.rs` (D4, appel `flush_expired`)
- `crates/bitdaemon-disk/tests/` (tests de régression D1/D2)

## Non-bugs tranchés

- **Coalescing inter-pièces** (`take_coalesced`) : upstream groupe
  l'iovec par pièce (`visit_block_iovecs`) ; nous traversons la
  frontière quand run-at-tail + next-piece-at-block-0. Mêmes octets,
  mêmes offsets disque (pièces contiguës dans le fichier) — strictement
  équivalent, meilleur pour le disque.
- **Flush immédiat d'une pièce complète non hashée**
  (`flush_after_insert`, `disk_io.rs:968-972`) : upstream ne la
  flusherait qu'en passe 4 (score passe 2 = `hasher_cursor -
  flushed_cursor` ≤ 0 avant hash) ; notre hasher whole-piece *exige*
  les données sur disque pour tourner, donc le flush eager des pièces
  complètes est structurellement nécessaire — documenté dans
  `disk_cache.rs:649-663`.
- **`overlay_cached` post-lecture** (`disk_io.rs:1698`, `1918-1920`) :
  upstream ne re-consulte pas le cache dans `do_job(read)`/
  `partial_read` ; notre overlay couvre la fenêtre gate→exécution où
  un write peut avoir atterri — supérieur, pas contradictoire.
- **Supersede toléré** là où upstream asserte
  (`!blk.has_buf()`, `disk_cache.cpp:290`) : defensive, cohérent avec
  « les handlers tournent exactement une fois » ; la fuite comptable
  associée est D2.
- **Passe 3 upstream** (free des buffers `needed_by_hasher` retenus
  pendant le hashing incrémental, `disk_cache.cpp:1051-1097`) : sans
  objet — notre `hash_job` *copie* les buffers cachés au départ
  (`disk_io.rs:2006-2017`) au lieu de les épingler.
- **`try_clear_piece`** refuse quand `count_jobs > 0` vs upstream qui
  *pare* le clear sur la cpe : chez nous `ClearPiece` passe par la
  fence, donc les jobs de la pièce sont déjà drainés — la branche
  defer upstream est structurellement inatteignable.
- **`StoreBuffer` non câblé** : port du header `aux_/store_buffer.hpp`
  (cache read de l'ère mmap) gardé pour `bitdaemon-mmap` ; le write
  cache vivant est `DiskCache` — conforme au découpage upstream.
- **`num_write_ops` compté seulement si `!failed`**
  (`disk_io.rs:577-579`) == `pread_disk_io.cpp:1857-1865`.
- **Erreur de flush → blocs marqués flushed + writers en erreur, pas
  de remise en cache** (`disk_io.rs:580-603`) ==
  `flush_cache_blocks` (`pread_disk_io.cpp:1837-1853`,
  `flushed.set_bit` même en erreur).

## Résultat

Appliqué sur `audit/16-disk-cache-pool` (branche du worktree
`C:/Projet/wt/LibtorrentRust-a16`).

- **D1 corrigé** : `DiskCache::in_flight_blocks` trace les buffers pris
  par `take_*_run` ; la boucle watermark de `flush_after_insert` sort
  quand le niveau *prédit* `write_blocks - in_flight` atteint le low
  watermark (`disk_io.rs`, `disk_cache.rs`).
- **D2 corrigé** : `num_cached_blocks` ne croît que sur slot réellement
  occupé ; le writer supersédé perd `CACHED` → `job_done` relâche son
  slot `write_blocks`. Fuites adjacentes couvertes : buffers résiduels
  relâchés via le compte retourné par `abort_all_writers`
  (`remove_torrent`, `abort_jobs`), orphans drainés avant le drop du
  storage (handlers exactement une fois), `Drop` draine les orphans
  restants, writer hors-bornes aborté au lieu d'être droppé.
- **D3 corrigé** : le drain évalue `write_blocks` global et itère
  `take_coalesced` sur toutes les storages (snapshot `Arc`, pas de
  lock storages→cache maintenu — ordre inverse utilisé par
  `RemoveTorrent`).
- **D4 gardé** (choix humain) : `cache_expiry`/`flush_expired` restent
  une extension documentée — déplacé en Non-bugs.
- **D5 corrigé** : `v2_pending` supprimé.
- **D6 corrigé** : `NEEDS_HASHER_KICK` posé à l'émission, effacé dans
  `hashed_block`.
- **D7 corrigé** : `StoreBuffer::insert` → `entry().or_insert`.

Tests de régression ajoutés :
`disk_cache::tests::{duplicate_insert_keeps_block_count_and_supersedes,
in_flight_blocks_tracks_taken_runs, hasher_kick_emitted_once_per_cursor,
abort_all_writers_reports_buffers}`,
`test_disk_io::duplicate_write_releases_back_pressure_slot`,
`test_store_buffer::duplicate_insert_keeps_first_buffer`.

Gates : `cargo fmt` ✓ · `clippy -D warnings` ✓ · `cargo test
--workspace` ✓ (0 échec) · `cargo doc` ✓.
