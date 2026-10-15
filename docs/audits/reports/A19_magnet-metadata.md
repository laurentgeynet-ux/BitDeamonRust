# Audit A19 — rapport d'analyse

Périmètre : magnet add, `ut_metadata` pacing, info-hash verify,
pending state à l'arrivée metadata, ops metadata-less.
Baseline upstream `e7381c7bc` (`src/ut_metadata.cpp`,
`src/torrent.cpp::{init,set_metadata,prioritize_*}`).

## Verdicts checklist

- Phases : magnet add → metadata download → verify hash → init picker/files → apply pending state — **conforme** — `Torrent::from_params` garde `storage=INVALID` pour les magnets (`session.rs:2882`, `torrent.rs:1263-1282`), `UtMetadataTracker` accumule (`ut_metadata.rs:206`), `set_metadata_verdict` vérifie puis fait `init_picker` + replay des bitfields pré-metadata + `update_interest_all` (`torrent.rs:1543-1709`), `TorrentEffect::MetadataReceived` crée le storage puis `check_files` (`session.rs:8983-8993`). Écart de couverture sur le *verify* — voir déviation 1.
- ut_metadata pacing upstream (intervalle entre requêtes, ban on flood) — **conforme** — requête min par pièce 3 s sauf source morte (`ut_metadata.rs:189-194` vs `ut_metadata.cpp:478-486`), `sent_requests < 2` + `has_metadata()` (`torrent.rs:2057-2081` vs `ut_metadata.cpp:396-419`), `dont_have` → `request_limit = max(now+1min, …)` (`torrent.rs:1954-1956` vs :369), hash-fail → `20+random(50)s`, base `+5min` si pièce unique (`torrent.rs:1985-2001` vs :422-424, :583), serveur : `send_buffer < 160 KiB` sinon file ≤1024 sinon `dont_have` (`torrent.rs:1818-1836` vs :337-342). Il n'existe **pas** de ban sur flood upstream — le « ban » de la checklist est le `request_limit` (voir Non-bugs).
- pending state préservé : file/piece priorities, trackers, peers, flags — **conforme** — `pending_piece_priorities` appliquées après `init_picker` (`torrent.rs:1638-1646`), `file_priorities` stockées sans storage puis injectées (`torrent.rs:1608-1637`, `set_file_priority` jambe `!m_storage` :11947-11955), trackers ajoutés dès l'add (`add_tracker` :3088), `protocol_v2` périmé purgé swarm-wide (`:1590-1597` vs `torrent.cpp:8410-8414`).
- Ops metadata-less (file_priority bounds, trackers add, save_resume) — **déviation** — bounds + defer conformes (`torrent.rs:11926-11935` vs `torrent.cpp:6162-6166` ; `prioritize_files` `fix_priorities(nullptr)` ok), mais `prioritize_pieces` manque le early-return `is_seed()` (déviation 4) et `to_resume_data` écrit un `info-hash` v1 erroné pour les magnets v2-only (déviation 6).
- Metadata reject : hash mismatch → pénalise les sources, retry ailleurs — **conforme** — `HashMismatch` → `apply_metadata_hash_fail_penalty` + `MetadataFailed` (`torrent.rs:1911-1914`), `reset_on_hash_failure` remet `num_requests=0` pour retenter les autres (`ut_metadata.rs:264-274`). Le payload hash-valide mais non parsable → `set_error(invalid_swarm_metadata)` + pause, sans pénaliser au-delà (`torrent.rs:1915-1933` vs `torrent.cpp:8348-8361`).
- info re-encode : jamais re-sérialiser info pour le hash — **conforme** — le hash porte sur le buffer verbatim (`torrent.rs:1551-1554`), `metadata_raw` conserve les bytes servis (`:1585`), côté serveur `metadata_packet` découpe `metadata_raw` (`:1794-1845`).

## Déviations

- `crates/bitdaemon-session/src/torrent.rs:1551-1557` — `set_metadata_verdict` ne vérifie que SHA-1 contre `self.info_hash`. Attendu upstream (`src/torrent.cpp:8277-8310`) : si `info_hashes.has_v1()` → SHA-1 == v1, sinon SHA-256 tronqué == v1 (magnet v1-keyed d'un torrent v2-only) ; puis si `has_v2()` → SHA-256 == v2. Aujourd'hui un magnet **v2-only** (`self.info_hash` = v2 tronqué) ou hybride ajouté par clé v2 ne peut **jamais** résoudre sa metadata — `HashMismatch` permanent, pénalité appliquée aux pairs qui servaient pourtant la bonne metadata. Sévérité : wire-visible / bloquant fonctionnel.
  - **sketch** : remplacer le bloc `sha1 … if actual != self.info_hash` par :
    ```rust
    use sha2::Digest as _;
    if self.info_hashes.has_v1() {
        let v1 = self.info_hashes.get(PV::V1);
        let sha1_ok = sha1::Sha1::digest(metadata).as_slice() == v1.as_bytes();
        let trunc_ok = !sha1_ok
            && sha2::Sha256::digest(metadata)[..20] == v1.as_bytes()[..];
        if !sha1_ok && !trunc_ok { return MetadataVerdict::HashMismatch; }
    } else if self.info_hashes.has_v2() {
        let v2 = self.info_hashes.get(PV::V2);
        if sha2::Sha256::digest(metadata).as_slice() != v2.as_bytes() {
            return MetadataVerdict::HashMismatch;
        }
    } else { return MetadataVerdict::HashMismatch; }
    ```
    Réf. upstream `src/torrent.cpp:8277-8310`. Dépend de `sha2` (déjà workspace dep pour les merkle trees — vérifier `Cargo.toml` session). Test de régression : `test_magnet.rs` — magnet v2-only + info dict dont le SHA-256 matche → `set_metadata` → `MetadataVerdict::Ok` ; magnet v1-keyed pour info v2-only (hash déclaré = v2 tronqué dans le slot v1) → Ok via la jambe tronquée.

- `crates/bitdaemon-session/src/session.rs:8983-8993` — `MetadataReceived` ne fait que storage + alerte : ni re-key de `session.torrents` (`update_torrent_info_hash`, `torrent.cpp:8418`) ni détection de collision avec un autre torrent metadata-less (`find_torrent` + `duplicate_torrent` + `torrent_conflict_alert`, `torrent.cpp:8363-8395`). Conséquences : (a) un magnet v2-keyed qui résout en hybride ne route jamais les conns entrantes présentant le hash v1 (`torrents: BTreeMap<Sha1Hash, Torrent>` mono-clé, `session.rs:431`) ; (b) deux magnets résolvant vers le même torrent coexistent au lieu de s'erreur+pause mutuellement. Sévérité : wire-visible (routing) + état.
  - **sketch** : dans le handler `TorrentEffect::MetadataReceived` (ou un nouvel effet `InfoHashChanged`), pour chaque hash de `t.info_hashes` non présent comme clé : si la clé existe déjà pour un autre torrent → `set_error("duplicate_torrent", ERROR_FILE_METADATA)` + pause sur les deux + `Alert::TorrentConflict` ; sinon insérer la clé → le même torrent (la map étant `BTreeMap<Sha1Hash, Torrent>` sans duplication possible, tenir un index secondaire `BTreeMap<Sha1Hash, Sha1Hash>` résolu au lookup, ou changer la map en `Sha1Hash → Sha1Hash` alias — décision structurelle à trancher à l'apply ; upstream indexe chaque hash dans `m_torrents`). Réf. `src/torrent.cpp:8363-8418`, `session_impl::update_torrent_info_hash`. Test : magnet v2-only, metadata hybride servie, conn entrante avec le v1 → attachée, pas `invalid_info_hash`.

- `crates/bitdaemon-session/src/torrent.rs:1700-1709` — la queue de `set_metadata_verdict` n'exécute pas le sweep `disconnect_if_redundant` post-`init` (upstream `torrent.cpp:8445-8447` : `for (auto* p : m_connections) p->disconnect_if_redundant()`). Les conns devenues redondantes (upload-only × upload-only, uninteresting upload peer) survivent jusqu'au prochain déclencheur. Sévérité : état/ressource, faible.
  - **sketch** : à la fin de `set_metadata_verdict` (branche Ok, après `update_interest_all`), `let ids: Vec<u64> = self.peers.keys().copied().collect(); for id in ids { self.disconnect_if_redundant(id, out); }` — la fonction existe déjà (`torrent.rs:5980`). Test : seed connecté pré-metadata, post-metadata les deux sont seeds → `DisconnectPeer{upload-upload}` dans les effets.

- `crates/bitdaemon-session/src/torrent.rs:8079-8092` — `prioritize_pieces` sans le early-return `is_seed()` (upstream `torrent.cpp:5967` : `if (is_seed()) return;`, *avant* même le check `valid_metadata`) et pose `need_save_resume = true` inconditionnellement (upstream : seulement quand `filter_updated`, :5993-5996). Un appel sur un seed mute le picker (pièces marquées unwanted → `is_finished`/`is_seed` incohérents). Sévérité : état.
  - **sketch** : tête de fonction → `if self.is_seed() { return; }` ; déplacer `self.need_save_resume = true;` dans le `if filter_updated`. Test : seed + `prioritize_pieces(&[DONT_DOWNLOAD; n])` → picker et `need_save_resume` inchangés.

- `crates/bitdaemon-session/src/counters.rs:293` — `NumTotalPiecesAdded` déclaré/exporté mais jamais incrémenté ; upstream inc `num_pieces` au ctor avec metadata (`torrent.cpp:458`) et dans `set_metadata` (`torrent.cpp:8442`). Compteur toujours 0 → métrique mensongère. Sévérité : compteurs (chevauche A29).
  - **sketch** : `counters.add(Counter::NumTotalPiecesAdded, i64::from(self.num_pieces))` dans `set_metadata_verdict` (branche Ok, position ~`torrent.rs:1700`) et à l'endroit équivalent du ctor `from_params` pour les adds avec metadata (chercher le `TorrentEffect`/compteur existant en fin de `from_params`). Test : add torrent + magnet → compteur == num_pieces cumulé.

- `crates/bitdaemon-session/src/torrent.rs:2465-2467` — `to_resume_data` pour un torrent sans `ti` écrit `atp.info_hashes.v1 = self.info_hash` : pour un magnet v2-only, le v2 tronqué atterrit dans le slot `info-hash` (v1) du resume — upstream écrit `ret.info_hashes = m_info_hash` (`torrent.cpp:7562`), donc le hash complet dans `info-hash2`. Round-trip : `read_resume_data` (upstream ET nôtre) relit ce blob comme un torrent v1 au hash tronqué — jamais réconciliable avec le vrai v2. Sévérité : wire/format de fichier.
  - **sketch** : `atp.info_hashes = self.info_hashes;` dans le `else` (le champ `info_hashes` du torrent porte déjà `atp.info_hashes` depuis l'add, `torrent.rs:1273-1276`). Test : magnet v2-only → `write_resume_data` → re-parse → `info_hashes.has_v2() && !has_v1()`.

## Fichiers du fix

- `crates/bitdaemon-session/src/torrent.rs` (déviations 1, 3, 4, 6)
- `crates/bitdaemon-session/src/session.rs` (déviation 2)
- `crates/bitdaemon-session/src/counters.rs` (déviation 5 — seulement si un alias/renommage s'avère utile)
- `crates/bitdaemon-session/tests/test_magnet.rs` (tests de régression)
- `docs/audits/changelog/A19.md`, `docs/PORTING.md` (rows magnet/ut_metadata)

Chevauchements probables : A20 (state machine, `torrent.rs`), A21 (resume, `torrent.rs:2455`), A29 (compteurs) — séquencer les merges.

## Non-bugs tranchés

- **Pas de ban sur flood `ut_metadata`** : la checklist mentionne « ban on flood », mais upstream n'a *aucun* ban — le back-pressure est `m_request_limit` (1 min sur `dont_have`, 20–70 s + base sur hash-fail) et la file entrante bornée à 1024. Le port reproduit ces deux mécanismes ; ne pas ajouter de ban.
- **Compteur `num_incoming_metadata` en fin de handler** : upstream l'inc pour tout message atteignant le switch ; le Rust reproduit exactement les early-returns non comptés (invalid-piece `dont_have` + `return true` :1789-1817 vs `ut_metadata.cpp:321-336`, unwanted `piece`/`dont_have` :1864-1873/:1957-1964).
- **`metadata_size > 4 MiB` ignoré au handshake**, `max_metadata_size` (30 MiB) à la réception des pièces : les deux caps distincts upstream sont reproduits (`ut_metadata.rs:147` vs :125, `:218` vs :515).
- **Handshake sortant `metadata_size`** : annoncé via `engine.rs:3973-3977` quand `!(priv && valid_metadata)` — conforme à `add_handshake`/`create_ut_metadata_plugin` (`ut_metadata.cpp:173-179, 606-612`).
- **`has_metadata` conn** : ctor `true`, effacé par handshake `ut_metadata` sans `metadata_size`, reposé par les messages piece-level (`peer_conn.rs:197-202, 1817, 1236-1241`) — conforme à `m_has_metadata`/`set_has_metadata` upstream.
- **Commentaire `metadata_tick_peer` trompeur** (`torrent.rs:2004-2009` dit « first drain … then maybe_send » alors que le code fait `maybe_send` d'abord — comme upstream `tick()` :386-393) : docstring à corriger à l'apply, pas un bug.
