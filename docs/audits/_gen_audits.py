# -*- coding: utf-8 -*-
"""Generate per-audit SKILL.md workflows + docs/audits/INDEX.md."""
import os
import re
import shutil

PROTOCOL = """## Protocole — deux modes

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

4. Écrire le rapport dans `docs/audits/reports/A{aid}_{slug}.md` —
   **seul fichier écrit**, unique à cet audit, donc plusieurs audits en
   mode rapport tournent simultanément dans le même checkout. Template :

   ```markdown
   # Audit A{aid} — rapport d'analyse

   ## Verdicts checklist
   Un verdict par item — `conforme` / `déviation` / `N-A` — chacun avec
   une preuve `fichier.rs:ligne` (ou réf. upstream `src/foo.cpp`,
   fonction). Tout item marqué `déviation` DOIT avoir une entrée dans
   `## Déviations`. Un rapport « tout conforme » sans preuve par item
   est incomplet — la preuve est ce qui rend le verdict vérifiable.
{checklist_verdicts}

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
   git add docs/audits/reports/A{aid}_{slug}.md
   git commit -m "docs(audit): rapport A{aid} {slug}" \\
       -- docs/audits/reports/A{aid}_{slug}.md
   ```

   Ce commit est **requis** : un worktree `apply` branché depuis `main`
   ne voit que du contenu commité. Les commits de rapports ne se
   conflitent jamais entre eux (fichiers disjoints, fast-forward).

6. **STOP côté code** : pas de fix, pas de gate. Le mode `apply` est
   une invocation séparée qui part du rapport commité **et relu**.

### Mode B — `apply` (worktree + branche dédiée)

Prérequis : le rapport `docs/audits/reports/A{aid}_{slug}.md` est
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
   git worktree add ../LibtorrentRust-a{aid} -b audit/{aid}-{slug}
   ```

   Travailler dans `D:/Projet/LibtorrentRust-a{aid}` : `target/` isolé,
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
    `docs/audits/changelog/A{aid}.md` sous `### Fixed` (ou autre section) —
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
    `audit/{aid}-{slug}`** — jamais sur `main`. **Tout reste local** :
    pas de push, pas de PR, pas de GitHub. Signaler « prêt à merger »
    et **s'arrêter** — le merge n'a lieu que sur ordre explicite de
    l'humain (« merge »). Sur cet ordre : `git merge
    audit/{aid}-{slug}` dans le checkout principal — **un merge à la
    fois** ; si conflits (`torrent.rs`/`peer_conn.rs`/`session.rs`/
    `PORTING.md`), rebase de la branche sur `main` frais **dans le
    worktree** + re-run des gates, puis nouveau `git merge`.
13. **Résultat** : ajouter une section `## Résultat` au rapport
    (déviations corrigées, tests ajoutés, hash du commit).
    **Hygiène post-merge** : `git worktree remove
    ../LibtorrentRust-a{aid}` puis `git branch -d audit/{aid}-{slug}`.
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
"""

AUDITS = [
    ("01", "disk-write-pipeline", "Pipeline disque — bloc entrant → écriture",
     "incoming block → async_write → cache → flush → completion",
     "`src/disk_io_thread.cpp`, `src/disk_job.cpp`, `src/disk_job_fence.cpp`, `src/peer_connection.cpp` (incoming_piece)",
     "`crates/bitdaemon-disk/src/disk_io.rs`, `disk_cache.rs`, `disk_job*.rs`, `crates/bitdaemon-session/src/peer_conn.rs`, `torrent.rs`",
     ["Dispatch `DiskWrite` : stamp `picker_gen`, fence ordering, part-file routing",
      "Positioned I/O : mapping bloc → offsets fichier multi-fichiers, sparse writes",
      "Completion `DiskCompleted::Write` : gate generation, bytes accounting avant verdict",
      "Lazy file opening : erreurs différées à la première I/O, pas à add_torrent",
      "Bounded `disk_buffer_pool` : back-pressure `bw_disk`/`num_peers_down_disk`",
      "Write coalescing : runs contigus, frontières de flush pièce-complète",
      "Fences : clear_piece/move_storage/delete_files vs writes en vol",
      "Timers engine : pas de starvation des disk completions"], "", "done — audit initial + commit `5541959`"),
    ("02", "upload-read-path", "Chemin upload/read (Request → fill_send_buffer → async_read)",
     "Request entrant → validation → upload queue → read disque → Piece ; choke drain, rejects, compteurs",
     "`src/peer_connection.cpp` (incoming_request, fill_send_buffer, write_reject_request, on_choke drain), `src/torrent.cpp` (on_read_piece)",
     "`crates/bitdaemon-session/src/peer_conn.rs`, `torrent.rs`, `net::peer_wire`",
     ["`incoming_request` : validation piece/start/len (len ≤ 32K sauf allowed_fast), reject vs accept",
      "Upload queue : FIFO, choked peers non servis, `fill_send_buffer` ordering",
      "Drain on choke : requests en vol annulées + `Rejected` wire + `clear_request_queue`",
      "`DontHave`/`Have`/`Rejected` wire encodings — bit-level",
      "Compteurs upload : `num_uploads`, `upload_payload`, read-quotas par canal",
      "fast-extension : allowed_fast requests servis même choked"], "", "done — commit `b518598` (DontHave wire fix)"),
    ("03", "piece-lifecycle", "Cycle de vie des pièces",
     "incoming_piece → flush → hash → piece_verified → Have → hash_failed → restore",
     "`src/torrent.cpp` (on_piece_verified, piece_passed, piece_failed, on_piece_sync, on_piece_fail_sync, we_have, penalize_peers, received_piece, announce_piece)",
     "`crates/bitdaemon-session/src/torrent.rs`, `piece_picker.rs`, `session.rs`",
     ["`picker_gen` gates write ET hash completions — in-flight invalidé par force_recheck/inconsistent",
      "hash-fail : `lock_piece` + `ClearPiece` fence → `on_piece_sync` différé (restore filtré v2 blocks, hash_failed_alert post-restore, re-mark duplicates)",
      "write-fail vs hash-fail : `on_piece_fail_sync` séparé (restore entier + `hash_fail_piece_picks`)",
      "`found_on_disk` : pas de blame (failed_bytes/ext/penalize) mais restore/fence conservés",
      "`piece_passed` vs `we_have` split : pads/checking/fastresume passent par we_have direct",
      "penalize_peers : parole conn+peer_list, trust −2 min −7, `banned_for_hash_failure`, `update_want_peers`, disconnect `failed:true`",
      "Compteurs : `num_piece_passed`/`num_piece_failed`/`num_have_pieces` aux sites exacts",
      "`file_completed` edges par fichier, `m_last_download` stampé au edge vérifié"], "", "done — commit `e957a83`"),
    ("04", "storage-ops-file-prio", "Ops storage & priorités fichiers",
     "move_storage / delete_files / rename_file / prioritize_files / file_priority + complétions",
     "`src/torrent.cpp` (move_storage, delete_files, rename_file, prioritize_files, on_file_priority), `src/storage_utils.cpp` (move_storage rollback, delete_files), `src/session_impl.cpp` (remove_torrent)",
     "`crates/bitdaemon-session/src/session.rs`, `torrent.rs`, `session_handle.rs`, `alerts.rs`",
     ["`prioritize_files` async : `SetFilePriority` → `async_set_file_priority` → `on_file_priority` — jamais inline",
      "Sérialisation `outstanding_file_priority` + `deferred_file_priorities` (merge par index, replay à la complétion)",
      "Vecteur appliqué = storage-ajusté, pas demandé ; `file_prio` posté à la complétion seulement ; `file_error` → set_error+pause",
      "`fix_priorities` : force `dont_download` sur pads avant dispatch ; migration part-file",
      "`move_storage` : status bits (`need_full_check`→recheck, `fatal_disk_error`/`file_exist`→failure), `old_path` dans l'alerte, rollback disque",
      "`rename_file` : bounds layout (metadata-less → silent), `old_name` résolu avant `renamed_files`, dirty dans la complétion",
      "`delete_files` : fence'd disk job → `torrent_deleted{,_failed}`, no-disk leg → remove silencieux",
      "Add-time `atp.file_priorities` + magnet init : apply direct picker, pas de job ni alerte"], "", "done — commit `24fffc8`"),
    ("05", "download-request-path", "Chemin download (request→write) & rejects",
     "request_a_block → add_request → send_block_requests → Piece → on_reject → timeouts/snub/endgame",
     "`src/peer_connection.cpp` (request_a_block, add_request, send_block_requests_impl, incoming_reject_request, incoming_piece, snub_peer, second_tick)",
     "`crates/bitdaemon-session/src/peer_conn.rs`, `peer_pipeline.rs`, `torrent.rs`",
     ["`send_block_requests` : merger `request_large_blocks`, gates disconnecting/pause/checking/upload_mode, `m_last_piece` stampé en sortie",
      "`incoming_piece` : ordre is_downloaded→async_write→mark_as_writing, waste buckets, unsnub fenêtré, `piece_seed`",
      "`incoming_reject_request` : validation forme (piece in-range, start aligné, length conforme), `!timed_out && !not_wanted` gate, parole → requeue front, seed → pas d'abort, `outstanding_bytes` dec",
      "Piece length-0 : quirk BitComet — validation échoue toujours → peer_error pur",
      "Timeouts : snub collapse à 1, `free_blocks>0` gate, re-pick avant timeout-mark, `piece_timeout` via `last_piece`",
      "Back-pressure : `exceeded && outstanding_writing_bytes>0` → `bw_disk`, `queued_write_bytes`, drain complet à la complétion",
      "Slow-start exit, AIMD `desired_queue_size`, `num_peers_down_requests` transitions",
      "Dead-code upstream conservée inerte : `cancel_block` post-`mark_as_writing`"], "", "done — commit `cbee65c`"),
    ("06", "peer-wire-codec", "Codec peer-wire & framing",
     "Encodage/décodage de tous les messages, bornes de longueur, entrées malformées, handshake.",
     "`src/bt_peer_connection.cpp` (on_receive, dispatch), `include/libtorrent/bt_peer_connection.hpp`, `src/peer_connection.cpp` (incoming_* dispatch)",
     "`crates/bitdaemon-net/src/peer_wire.rs`, `crates/bitdaemon-session/src/peer_conn.rs` (dispatch)",
     ["Length-prefix 4B BE inclut l'id, keepalive = len 0 — bornes exactes upstream (`packet_size` checks par message)",
      "Chaque `msg_*` : round-trip encode/decode + rejet des tailles invalides (have len 5, request len 13, bitfield len = ceil(n/8)+1…)",
      "Handshake 68B : validation info_hash/proto, peer_id optionnel, reserved bits (BEP-10, FAST, v2)",
      "Messages hors-ordre : have/bitfield avant handshake, piece avant bitfield, reject sans request",
      "Dépassements : oversized piece (packet_too_large), truncated frames, length incohérente → disconnect vs drop silencieux",
      "Compteurs `num_incoming_*` par type de message (funnel dispatch)",
      "Port/dht_port message, extension id 0 handshake vs ids étendus"], "", None),
    ("07", "extension-protocols", "Protocoles d'extension (ut_metadata, ut_pex, lt_*, suggest, smartban, holepunch)",
     "Extensions BEP-10 : métadata, pex, dont_have/have_none, suggest+allowed_fast, smart_ban, upload_only, holepunch.",
     "`src/ut_metadata.cpp`, `src/ut_pex.cpp`, `src/smart_ban.cpp`, `src/bt_peer_connection.cpp` (extended legs), `src/peer_connection.cpp` (incoming_suggest/allowed_fast/dont_have)",
     "`crates/bitdaemon-net/src/bep10.rs`, `crates/bitdaemon-session/src/peer_conn.rs` (extended arm), `session.rs` (metadata/pex routing)",
     ["ut_metadata : request/data/reject, throttling, caps `metadata_size`, chunk 16K ordering, tracker-hash reset",
      "ut_pex : added/dropped, flags (seed/upload_only/encryption/utp/holepunch), IPv6 form, max 50 pairs, `lt_v2` flag strip",
      "lt_donthave inbound — drop silencieux si `packet_size != 6`, disconnect si index hors bitfield, redondant ignoré",
      "suggest/allowed_fast sets : sémantique d'insertion/purge (choke → allowed_fast, unchoked → suggested), request hors-set → reject",
      "smart_ban : hash par bloc, bannissement persistant, `banned_for_hash_failure`",
      "upload_only message : sémantique sur les deux sens (peer announces vs we announce)",
      "holepunch rendez-vous : relay, connect msg, erreurs"], "", None),
    ("08", "choker", "Choker & optimistic unchoke",
     "Sélection des pairs unchokés : comparateurs, quotas, rotation optimiste, interaction snub/seed.",
     "`src/choker.cpp` (unchoke_sort), `src/peer_connection.cpp` (choke_this_peer, maybe_unchoke_this_peer), `src/session_impl.cpp` (unchoke tick)",
     "`crates/bitdaemon-session/src/choker.rs`, `torrent.rs` (unchoke legs), `session.rs`",
     ["`unchoke_sort` verbatim : comparateurs rate-based/RR/fastest/anti-leech, quota `piece_length·quota` + 60 s, nth_element top-slots",
      "Slots : fixed vs rate-based, `unchoke_slots_limit`, seed expansions",
      "Optimistic unchoke : période, rotation, `num_opt_unchoke`",
      "Snub/parole/endgame peers exclus ou inclus selon upstream",
      "Choke/unchoke effects : envoi msg + `expect_choke` mark + pending requests handling",
      "peer_class filters dans le calcul des slots"], "", None),
    ("09", "time-critical", "Pipeline time-critical (deadlines)",
     "set_piece_deadline, request_time_critical_pieces, make_time_critical, cancel_non_critical, expiration de deadlines.",
     "`src/torrent.cpp` (set_piece_deadline, update_piece_deadline, request_time_critical_pieces), `src/peer_connection.cpp` (make_time_critical, can_request_time_critical)",
     "`crates/bitdaemon-session/src/torrent.rs` (time_critical map, cancel_non_critical), `peer_conn.rs` (make_time_critical, queued_time_critical)",
     ["Deadlines : insertion, watch via ticker, alert expiry → recancel",
      "`queued_time_critical` : compteur de préfixe exact (insert/remove/choke-clear/shift)",
      "make_time_critical : déplacement vers le préfixe, refus si déjà critique ou absent",
      "cancel_non_critical = `cancel_request(force=true)` : not_wanted marking, wire Cancel conditionnel, abort picker — les deux files",
      "Priorité picking : les blocs critiques passent devant le pick normal",
      "watch-timeouts vs piece timeout : indépendance"], "", None),
    ("10", "peer-lifecycle", "Cycle de vie des pairs & admission",
     "attach/detach, états de connexion, limites half-open, taxonomie des disconnects, admission entrante.",
     "`src/torrent.cpp` (attach_peer, remove_peer, connect_to_peer, peer_list), `src/session_impl.cpp` (incoming conn gates, max half-open)",
     "`crates/bitdaemon-session/src/torrent.rs` (attach_peer/remove_peer), `session.rs` (connect pump), `peer_list.rs`",
     ["Ordre attach : refcount picker, `set_peer_class`, interested eval, bitfield init",
      "remove_peer : abort outstanding (gate `!timed_out && !not_wanted` !), refcount dec, stats fold, gauges",
      "Disconnect taxonomy : reason + `failed` flag → `get_peer_list_entry` failcount vs clean",
      "Half-open limits (`m_half_open`), connect_backoff exponentiel, prioritisation des peers récents",
      "Admission inbound : no-torrent silent drop, duplicate conn handling, peer_id conflicts",
      "ip_filter/port_filter application aux conns actives (disconnect + peer_blocked_alert)",
      "web seeds comme pseudo-peers (WEB_SEED_PEER key) — ne pas casser"], "", None),
    ("11", "trackers", "Trackers (HTTP/UDP announce, scrape, backoff, tiers)",
     "Cycle announce complet : events, backoff, failover tiers, scrape, réponses, erreurs.",
     "`src/tracker_client.cpp`, `src/http_tracker_connection.cpp`, `src/udp_tracker_connection.cpp`, `src/torrent.cpp` (announce_with_tracker, enable_all_trackers, scrape_tracker)",
     "`crates/bitdaemon-net/src/tracker*` (clients http/udp), `crates/bitdaemon-session/src/tracker_list.rs`, `torrent.rs` (announce legs), `engine.rs`",
     ["Events : none/started/stopped/completed — completed envoyé UNE fois au premier finish, re-announces sans event",
      "Backoff : échecs exponentiels `announce_interval * (1.5^n)`, cap, jitter, `fail_limit` → tracker suivant du tier",
      "Tiers : ordre, failover après N échecs vs immédiat, `announce_to_all_tiers/trackers` flags",
      "Scrape : déclenchement (`want_scrape`, auto_scrape_interval/min), réponse → complete/incomplete/downloaded",
      "UDP tracker : connect → announce → scrape wire format, transaction ids, réponses tronquées, timeout retry",
      "key/tracker_id génération + stabilité session ; num_want, left/bytes accounting",
      "Réponses : peer dicts (v4/v6), min interval, warning message, trackerid update",
      "`tracker_warning` count vs `tracker_error`, `next_announce` reset on force_announce"], "", None),
    ("12", "web-seeds", "Web seeds (HTTP peer)",
     "web_peer_connection/http_seed_connection : GET ranges, keep-alive, erreurs, retry, comptabilité par seed.",
     "`src/web_peer_connection.cpp`, `src/http_seed_connection.cpp`, `src/web_connection_base.cpp`, `src/torrent.cpp` (retry_web_seed, maybe_connect_web_seeds)",
     "`crates/bitdaemon-net/src/web_seed*`, `crates/bitdaemon-session/src/session.rs` (on_web_seed_*), `torrent.rs` (web_seeds)",
     ["Range requests : pièces complètes vs blocs, `block_size` alignment, coalescing contigu",
      "Erreurs : HTTP non-200, redirect loops, content-length mismatch, EOF mid-piece → url_seed_alert + seed marqué",
      "Retry : backoff par seed, `retry_web_seed`, bannissement temporaire",
      "Attribution : erreurs/compteurs par URL (cohérence du fix récent `on_web_seed_error(url)` sur les deux legs)",
      "Request pacing : `web_seed_blocks_per_tick`, connection reuse vs reconnect",
      "Pseudo-peer `web_seed_t` : download queue partagée, legs upload N/A"], "", None),
    ("13", "utp-transport", "Transport uTP",
     "utp_stream : paquets, états connexion, resend, delay/rtt, MTU, buffers out-of-order, fermeture.",
     "`src/utp_stream.cpp`, `src/utp_socket_manager.cpp`, `include/libtorrent/utp_socket_manager.hpp`",
     "`crates/bitdaemon-net/src/utp*.rs`",
     ["Packet types : ST_DATA/FIN/STATE/RESET/SYN — header 20B exact (version, ext, conn_id, ts, tsdiff, wnd, seq, ack)",
      "State machine : SYN_SENT → CONNECTED → FIN_SENT/CLOSE_WAIT, syn retransmit, connect timeout",
      "Resend : RTO basé rtt/var, exponentiel, seq wrap 16 bits",
      "Delay measurement : tsdiff, constantes LEDBAT-ish — PORTER LES CONSTANTES VERBATIM avant tout tuning",
      "MTU discovery : probing, floor/ceiling",
      "Out-of-order buffer : selective ack, gaps, reorder window",
      "wnd advertisement vs buffer réel, `m_suspended`/deferred close",
      "Interface DatagramSocket injectée — aucun bind direct (ADR-0008)"], "", None),
    ("14", "mse-encryption", "Chiffrement MSE/PE",
     "Handshake DH, crypto_provide/select, RC4, padding, politiques enc.",
     "`src/pe_crypto.cpp`, `src/encryption.cpp`, `src/peer_connection.cpp` (enc handshake)",
     "`crates/bitdaemon-net/src/pe_crypto.rs` / `encryption*.rs`, `peer_wire.rs` (enc wrapper)",
     ["Handshake : DH 768 bits, SKEY = info_hash côté initiator, génération clés publiques",
      "crypto_provide/crypto_select : bits plaintext/rc4, sync sur len(PadC), IA vs req1/req2/req3",
      "RC4 : clés A→B et B→A distinctes, drop 1024, application header+payload",
      "Padding : len(PadA/B/C) 0-512, VC constant",
      "Politiques : enc_disabled/enabled/required — entrantes ET sortantes, `allowed_enc_level`, pe_forced",
      "Upgrade : conn plaintext → enc mid-handshake, fallback sur échec",
      "Aucun unsafe : RC4/DH via crates sûres"], "", None),
    ("15", "proxies", "Proxies (SOCKS5/HTTP CONNECT)",
     "socks5_stream, http_stream, instantiate_connection — auth, UDP associate, erreurs, settings par usage.",
     "`src/socks5_stream.cpp`, `src/http_stream.cpp`, `src/instantiate_connection.cpp`, `src/session_impl.cpp` (proxy settings)",
     "`crates/bitdaemon-net/src/proxy*.rs` / `socks5*.rs` / `http_stream*.rs`, transport",
     ["SOCKS5 : méthodes auth (no-auth, user/pass), commandes connect/UDP associate, erreurs → mapping",
      "HTTP CONNECT : auth basic, tunnel keep-alive, host header",
      "UDP associate : datagrammes encapsulés (RSV/FRAG/ATYP), trackers UDP via proxy",
      "Proxy par usage : `proxy_peer_connections`/`proxy_tracker_connections`/`proxy_dht` flags par proto",
      "Hostname vs résolution via proxy, `resolve_hostnames` flag",
      "Instantiate : choix socket_type selon proxy + ssl"], "", None),
    ("16", "ssl-torrents", "Torrents SSL",
     "ssl_stream : fingerprint cert vs info-hash, mutual TLS, cert settings, listen ssl, alertes.",
     "`src/ssl_stream.cpp`, `include/libtorrent/ssl_stream.hpp`, `src/torrent.cpp` (ssl legs), `src/session_impl.cpp` (ssl listen)",
     "`crates/bitdaemon-net/src/ssl_stream.rs`, `crates/bitdaemon-session/src/session.rs` (ssl listen legs), `engine.rs`",
     ["Fingerprint : digest cert SHA-256/SHA-1 vs info-hash v1/v2 → règles d'acceptation exactes",
      "Mutual TLS : verify_peer + verify_fail_if_no_peer_cert, client cert depuis `ssl_cert`/`ssl_key`",
      "verify_callback : chain custom — CN/SAN ne font pas foi d'identité torrent upstream",
      "ssl_listen_port, accept inbound ssl, flags (`is_ssl_torrent`, `m_ssl`)",
      "Alertes : erreurs peer_ssl → mapping disconnect reason",
      "crypto provider : builder_with_provider exhaustif (suite du fix récent)"], "", None),
    ("17", "dht", "DHT (krpc, traversals, items, sécurité)",
     "Mainline DHT : messages, traversals, tokens, BEP-44/51, routage, refresh, boot, rate limiting.",
     "`src/kademlia/*.cpp`, `src/dht_tracker.cpp`, `src/node.cpp`, `src/routing_table.cpp`, `src/item.cpp`",
     "`crates/bitdaemon-dht/src/*.rs`, `session.rs` (dht outbox/announce)",
     ["KRPC : encode/decode q/r/e, transaction ids, error [code,msg]",
      "Traversals : find_node, get_peers (token collect), announce_peer (write-token check, implied_port), sample_infohashes",
      "BEP-44 : immutable put/get, mutable (k sig seq salt cas), seq monotonic, vérif sig ed25519",
      "Tokens : secret id rotatif, durée de vie, vérif par IP",
      "Routing table : buckets K=8, split, replace-oldest vs replace-pinger, refresh, verification",
      "BEP-42 : node id derivation (ip mask + rand), enforcement entrant",
      "dos_blocker : fenêtre 10 s, rate limit par IP/préfixe",
      "dht_state : persist nodes + node_id, obfuscation XOR, round-trip",
      "get_peers response : ordre values + nodes + token"], "", None),
    ("18", "disk-cache-pool", "Cache disque & buffer pool",
     "Coalescing, watermark, éviction, dirty expiry, bornes pool, back-pressure cache.",
     "`src/disk_buffer_pool.cpp`, `src/store_buffer.cpp`, `src/disk_io_thread.cpp` (cache legs), `src/disk_job.cpp` (FLUSH)",
     "`crates/bitdaemon-disk/src/disk_cache.rs`, `disk_buffer*`, `disk_io.rs`",
     ["Coalescing : runs contigus par pièce, un syscall par run",
      "Watermark : seuil haut blocage vs bas déblocage, hystérésis exacte",
      "Éviction : ordre upstream, dirty expiry `cache_expiry` flushes stale",
      "Pool : `max_queued_disk_bytes` cap, check_buffer_level, job_done comptage",
      "Read-side : consult cache avant disque (prepare_read/get2/partial_read), spanning reads",
      "FLUSH_PIECE/force_flush_flag drain une pièce indépendamment du watermark",
      "Superseded/teardown writers : abort exactly-once"], "", None),
    ("19", "partfile-filepool", "part_file & file_pool",
     "Slots part-file, export, migration priorités, récupération corruption ; pool fd LRU, caps, erreurs reopen.",
     "`src/part_file.cpp`, `src/file_pool.cpp`, `src/posix_part_file.cpp`",
     "`crates/bitdaemon-disk/src/part_file.rs`, `file_pool.rs`",
     ["part_file : header format (magic, version, endianness!), slot alloc/free FIFO, export_on_flush, bit `flushed`",
      "Corruption : header illisible → comportement upstream, missing file, taille mismatch",
      "Migration : prioritize → bytes straddling vers .parts, export quand pièce complète",
      "file_pool : LRU eviction, cap `file_pool_size`, open/upgrade read→write, reopen on error",
      "Sparse/allocate flags propagés, `file_open_failed` retry"], "", None),
    ("20", "resume-data", "Resume data — round-trip + snapshot sous I/O",
     "read/write resume complet, verify_resume_data verdicts, cohérence du snapshot pendant écritures en vol.",
     "`src/read_resume_data.cpp`, `src/write_resume_data.cpp`, `src/torrent.cpp` (check_fastresume, on_resume_data_checked, save_resume_data)",
     "`crates/bitdaemon-session/src/session.rs`/`torrent.rs` (resume paths), `core::resume*`",
     ["Champs : pieces bitfield, unfinished, priorities, trackers, peers+banned, seeds_dht, limits, totals, active/seeding time, flags (upload/share/seed/auto_managed/paused/apply_ip_filter), save_path, renamed/mapped_files, file_format_version, ih v2, comment/created_by/creation_date",
      "verify_resume_data verdicts : no_resume/need_full_check, fichiers manquants/modifiés/tailles, `partial`",
      "Snapshot sous I/O : dirty cache blocks cohérents avec le disque post-reload — écritures en vol + fences pendant write_resume_data",
      "Round-trip BIDIRECTIONNEL : fichier upstream → lu ici ; écrit ici → lu par upstream (golden vectors)",
      "Versions : upgrade/downgrade du format resume entre versions libtorrent",
      "save_resume_data timing : dirty flags, déclencheurs `need_save_resume` exhaustifs"], "", None),
    ("21", "torrent-state-machine", "Machine d'état torrent",
     "Transitions add→checking→downloading→finished→seeding, pauses, state_updated/save_resume triggers, seed_mode.",
     "`src/torrent.cpp` (set_state, files_checked, on_torrent_paused, do_pause/do_resume, finished, start/stop_announcing)",
     "`crates/bitdaemon-session/src/torrent.rs` (set_state, checking flags, finished/seeding), `session.rs`",
     ["Ladder : checking_files/checking_resume/downloading_metadata/downloading/finished/seeding + transitions interdites",
      "paused : `m_paused | m_session_paused`, graceful pause (announces stop, disconnects différés)",
      "`state_updated()` sites exhaustifs (status change → save_resume_data_alert au bon moment)",
      "`need_save_resume` — CHAQUE site upstream (if_counters_changed, if_config_changed, if_download_progress, if_state_changed)",
      "seed_mode : entrée (flag + all pieces + no dont_download), sortie → force_recheck",
      "auto_managed : transitions paused↔queued↔active, ordre `manage_queue`",
      "stop_when_ready : edge finished → pause"], "", None),
    ("22", "magnet-metadata", "Magnet & acquisition metadata",
     "Magnet add, ut_metadata pacing, info-hash verify, priorités/trackers conservés, ops sans metadata.",
     "`src/torrent.cpp` (init, on_metadata, set_metadata), `src/ut_metadata.cpp` (request pacing)",
     "`crates/bitdaemon-session/src/torrent.rs` (init/magnet path), `session.rs` (metadata dispatch)",
     ["Phases : magnet add → metadata download → verify hash → init picker/files → apply pending state",
      "ut_metadata pacing upstream (intervalle entre requêtes, ban on flood)",
      "pending state préservé : file/piece priorities, trackers, peers, flags à l'arrivée metadata",
      "Ops metadata-less : API qui doivent no-op ou defer (file_priority bounds, trackers add, save_resume)",
      "Metadata reject : hash mismatch → ban peers contributeurs, retry autres",
      "info re-encode : jamais re-sérialiser info pour le hash (bytes verbatim)"], "", None),
    ("23", "settings-session-state", "Settings hot-apply & .session",
     "apply_settings legs par clé, session_params/.session round-trip, deprecated keys.",
     "`src/settings_pack.cpp`, `src/session_impl.cpp` (apply_settings, load_state, save_state), `src/session_params.cpp`",
     "`crates/bitdaemon-session/src/settings.rs` (apply_settings legs), `session.rs` (SessionState)",
     ["apply_settings : chaque clé ayant un effet live vs celles ne s'appliquant qu'au prochain storage/conn",
      "Clés sans effet live : documenté comme upstream",
      ".session file : layout dict, ip_filter4/6, dht_state embed, version compat",
      "Deprecated keys : conservés en écriture pour round-trip interop?",
      "Diff ciblé : parmi les 266 keys validées par défaut, lesquelles sont réellement consommées"], "", None),
    ("24", "piece-picker", "Piece picker — cas limites",
     "reverse/partial/rarest-first, priorities, streaming, share mode, endgame busy, invariants.",
     "`src/piece_picker.cpp` (pick_pieces, update_pieces, share mode legs)",
     "`crates/bitdaemon-session/src/piece_picker.rs`, `request_blocks.rs`",
     ["Rarest-first : weighted random, ordre des priority buckets",
      "Reverse : ordre de pick pour snubbed peers (convergence sur mêmes pièces)",
      "Partials : prefer_contiguous_blocks, prioritize_partials, time-critical d'abord",
      "Priorities : piece 0/dont_download skip, top-priority first, per-piece vs per-file",
      "Streaming : set_sequential_range, sequential mode (état picker + override priorities)",
      "Share mode : recalc_share_mode rebalance pour diffuser la rareté",
      "Endgame : busy blocks requestables en dernier (un busy par peer sauf TC)",
      "Invariants : num_peers == downloaders réels, refcount == peers ayant la pièce, transitions block_info"], "", None),
    ("25", "counters-sweep", "Sweep conformité compteurs/stats",
     "Chaque `Counter` ↔ sites upstream inc/dec/set ; session_stats snapshot ; sliding_average.",
     "`include/libtorrent/performance_counters.hpp`, tous les `inc_stats_counter`/`set_value`/`blend_stats_counter`",
     "`crates/bitdaemon-session/src/counters.rs` + tous les sites d'appel",
     ["Exhaustif : pour CHAQUE counter upstream, le site Rust correspondant (ou N/A documenté)",
      "Jamais incrémentés : pattern trouvé 2× — suspecter tous les compteurs",
      "Gauges transitionnels (num_peers_*) : inc/dec aux bons edges, pas de drift",
      "sliding_average vs add-blend : request_time, request_latency, rates",
      "session_stats_metrics : noms/indices exposés aux clients",
      "waste_reason totals, disk_job counters, peer_class channels"], "", None),
    ("26", "path-metainfo-security", "Sanitization chemins & parsing metainfo (sécurité)",
     "file_storage sanitize (.., ADS, device names, drives, abs), symlink, doublons ; metainfo malformé.",
     "`src/file_storage.cpp` (sanitize_path), `src/torrent_info.cpp` (parse errors), `src/path.cpp`",
     "`crates/bitdaemon-core/src/file_storage.rs`, `path*.rs`, `torrent_info.rs`",
     ["`..` traversal : sanitization identique upstream (éléments retirés, pas rejet global)",
      "ADS `file:`/`file$`, noms réservés Windows (CON/PRN/AUX/NUL/COM1-9/LPT1-9) → renaming identique",
      "Drive letters `C:`, abs paths, UNC → traités en noms de fichier upstream",
      "Symlink flag : création, dangling links, `file_symlink` paths",
      "Doublons de chemins : disambiguation ou erreur",
      "Metainfo : piece_length 0/négatif/énorme, sizes négatives, utf-8 invalide, bencode non-canonique, piece string length mismatch → erreurs exactes",
      "Zero-length files & pad files : création ignorée, comptage"], "", None),
    ("27", "engine-glue", "Glue engine/session",
     "engine.rs : boucle dispatch, ordonnancement commandes, timers, pont disque, starvation.",
     "`src/session_impl.cpp` (dispatch loop ordering) — l'équivalent local est `engine.rs`",
     "`crates/bitdaemon/src/engine.rs`",
     ["Ordonnancement : ordre commandes vs ticks vs disk completions vs net events",
      "Tick starvation : le bug corrigé — vérifier toutes les sources de réveil",
      "Command channel : ordre FIFO préservé ? priorités ?",
      "Alert routing : broadcast + notify + wait_for_alert",
      "Peer pump : connect/dial ordering, acceptor pump, handshake_timeout",
      "Timers : chaque deadline upstream a son tick (second_tick, unchoke, stats, lsd, auto-scrape, peer_turnover)"], "", None),
    ("28", "nat-traversal", "NAT traversal (LSD/UPnP/NAT-PMP)",
     "LSD multicast, UPnP IGD lifecycle, NAT-PMP/PCP mappings, external IP election.",
     "`src/lsd.cpp`, `src/upnp.cpp`, `src/natpmp.cpp`, `src/session_impl.cpp` (portmap legs)",
     "`crates/bitdaemon-net/src/lsd*.rs`, `upnp*.rs`, `natpmp*.rs`, `session.rs`/`engine.rs`",
     ["LSD : multicast group/port, cadence par torrent, cookie, receive → peer feed, off en anonymous_mode",
      "UPnP : SSDP discover, WANIP/WANPPP, AddPortMapping + renew, DeletePortMapping au stop",
      "NAT-PMP/PCP : mapping request, external address, epoch, renew, delete",
      "External IP election : consensus trackers/DHT/UPnP/NAT-PMP → external_ip_alert",
      "portmap alerts : mapping add/error/log transport+protocol",
      "Mapping per listen port : tcp+udp, proto field correct"], "", None),
    ("29", "bandwidth-classes", "Bandwidth manager & peer classes",
     "Quotas par canal, request_bandwidth, class filters, rate channels, mixed mode, overhead.",
     "`src/bandwidth_manager.cpp`, `src/bandwidth_queue_entry.hpp`, `src/bandwidth_limit.cpp`, `src/peer_class.cpp`",
     "`crates/bitdaemon-net/src/peer_class*.rs`, `crates/bitdaemon-session/src/session.rs` (bw quota paths)",
     ["Channels : upload/download/protocol + classes (global/tcp/local/i2p) — assignation conn→class à l'attach",
      "request_bandwidth : quota granted, temp queuing, expiry",
      "Rate channels update : set_rate_limit per class propagé, `*_limit_too_low` perf alerts",
      "type filter apply : `set_peer_class_filter`/`set_peer_class_type_filter` — classe résolue par ip/type",
      "Mixed mode : uTP vs TCP split (`mixed_mode_algorithm` proportional/rate)",
      "Overhead accounting : debit ip_overhead canaux séparés (`rate_limit_ip_overhead`)",
      "Preemptive unchoke on quota, maybe_unchoke_this_peer"], "", None),
]

# Audit numbers encode the recommended execution order — the original
# ids were a thematic enumeration, not a sequence. EXEC_ORDER lists the
# ORIGINAL ids in logical apply order (protocol surface → peer supply →
# swarm data paths & policy → disk internals → metadata/state →
# peripherals → glue → counters sweep last).
EXEC_ORDER = [
    "06", "07", "14",  # protocol: wire codec, extensions, MSE
    "11", "13", "12",  # peer supply: trackers, uTP, web seeds
    "10",              # admission/lifecycle
    "05", "02", "03",  # swarm data paths: download, upload, piece life
    "08", "09", "24",  # swarm policy: choker, time-critical, picker
    "01", "04", "18", "19",  # disk internals
    "26", "22", "21", "20", "23",  # metadata & state
    "16", "15", "28", "17",  # peripherals: SSL, proxies, NAT, DHT
    "27", "29",        # engine glue, bandwidth
    "25",              # transverse counters sweep — always last
]
assert sorted(EXEC_ORDER) == sorted(a[0] for a in AUDITS), (
    "EXEC_ORDER must list every original audit id exactly once")
_old = {a[0]: a for a in AUDITS}
AUDITS = [(f"{pos:02d}", *(_old[o][1:])) for pos, o in enumerate(EXEC_ORDER, 1)]

# Apply waves — grouped by file overlap (NOT by crate: A02 and A13 are
# both bitdaemon-session but different waves; wave 1 spans 4 crates).
# Wave 5 (A29) is transverse and always last, alone.
WAVES = {
    1: {
        "ids": ["05", "16", "17", "18", "24", "25", "26"],
        "label": "isolés",
        "why": "modules/crates disjoints (utp, proxy, dht, disk×2, core/path, nat)",
    },
    2: {
        "ids": ["04", "06", "13", "21", "22", "23", "27", "28"],
        "label": "session-edge",
        "why": "fichiers dédiés (resume, settings, picker, engine, peer_class, tracker*, web_seed*, ssl) — edges torrent/session modérés",
    },
    3: {
        "ids": ["01", "03"],
        "label": "net-peer",
        "why": "`peer_wire.rs` commun aux deux → séquentiels entre eux",
    },
    4: {
        "ids": ["02", "07", "08", "09", "10", "11", "12", "14", "15", "19", "20"],
        "label": "session-core",
        "why": "tous sur `torrent.rs`/`peer_conn.rs`/`session.rs` — max 2-3 concurrents, merges séquentiels",
    },
    5: {
        "ids": ["29"],
        "label": "final",
        "why": "transverse (sweep compteurs), seul, en dernier",
    },
}
WAVE_OF = {aid: w for w, spec in WAVES.items() for aid in spec["ids"]}

# EXEC_ORDER makes the audit number itself the recommended apply
# position — A01→A29 reads top-to-bottom as the sequential order.

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(ROOT)

# Work dirs for the two-phase workflow (kept in git via README files)
for sub, desc in [
    ("reports", "Rapports d'analyse des audits (mode A — un fichier par audit,\n"
                "écrit en parallèle sans conflit puisque chaque audit n'écrit\n"
                "que le sien). Alimente le mode apply."),
    ("changelog", "Fragments de changelog produits par le mode apply (un par\n"
                  "audit, au lieu d'éditer CHANGELOG.md — élimine les conflits\n"
                  "de merge). Agrégés dans CHANGELOG.md après merge des PRs\n"
                  "via `python docs/audits/_aggregate_changelog.py`."),
]:
    p = os.path.join("docs", "audits", sub)
    os.makedirs(p, exist_ok=True)
    with open(os.path.join(p, "README.md"), "w", encoding="utf-8") as f:
        f.write(f"# docs/audits/{sub}\n\n{desc}\n")

# Derived status: a report containing a `## Résultat` section means the
# audit ran the full loop (rapport -> apply -> merged). Static AUDITS
# status stays as fallback for audits done before the report system.
def derived_status(aid, slug):
    p = os.path.join("docs", "audits", "reports", f"A{aid}_{slug}.md")
    if not os.path.exists(p):
        return None
    txt = open(p, encoding="utf-8").read()
    m = re.search(r"^##\s+Résultat\b(.*?)(?=^##\s|\Z)", txt, re.S | re.M)
    if not m:
        return None
    h = re.search(r"commit[^`]*`([0-9a-f]{7,40})`", m.group(1), re.I)
    return f"done — commit `{h.group(1)}`" if h else "done — rapport"


status_of = {a[0]: (derived_status(a[0], a[1]) or a[8]) for a in AUDITS}

# INDEX.md — all rows
idx = [
    "# Registre des audits — couverture complète BitDaemon vs libtorrent",
    "",
    "Baseline upstream : `v2.1.2-62-ge7381c7bc` (`D:\\Projet\\libtorrent\\libtorrent`, lecture seule).",
    "Chaque audit = un skill `.devin/skills/audit-NN-vW-<slug>/` lançable via `/audit-NN-vW-<slug>`.",
    "**Mode d'emploi complet : [`GUIDE.md`](GUIDE.md).**",
    "",
    "| # | Audit | Périmètre | Statut |",
    "|---|---|---|---|",
]
for aid, slug, title, scope, _up, _rs, _chk, _traps, _status in AUDITS:
    idx.append(f"| A{aid} | {title} | {scope} | {status_of[aid] or 'à faire'} |")
idx += [
    "",
    "*Statut* : `done` est dérivé de la section `## Résultat` du rapport",
    "d'audit — régénérer après merge : `python docs/audits/_gen_audits.py`.",
    "",
    "## Utilisation",
    "",
    "```",
    f"/audit-01-v{WAVE_OF['01']}-peer-wire-codec            # mode rapport (défaut)",
    f"/audit-01-v{WAVE_OF['01']}-peer-wire-codec en mode apply",
    "```",
    "",
    "Le skill embarque : périmètre, fichiers upstream/Rust, checklist,",
    "protocole à deux modes, pièges connus.",
    "",
    "## Workflow parallèle — 2 phases",
    "",
    "Le protocole historique (analyse + fix + gates + push `main` en une",
    "passe) n'est pas parallélisable : `target/` verrouillé sérialise les",
    "builds, `fmt --all` se marchent dessus, `CHANGELOG.md`/`PORTING.md`",
    "conflitent au merge, et les pushs `main` se rejettent en cascade.",
    "D'où le découpage :",
    "",
    "### Phase 1 — rapports (parallélisme total)",
    "",
    "Mode `rapport` : read-only sur le code, écrit uniquement",
    "`docs/audits/reports/ANN_<slug>.md` puis le commit sur `main` en",
    "pathspec-limited (`git commit -- <path>` — n'embarque que ce fichier",
    "même si l'index partagé contient le rapport d'un voisin). Tous les",
    "audits peuvent tourner en même temps dans le même checkout.",
    "",
    "**Gate obligatoire avant la phase 2** : chaque rapport est relu et",
    "validé par un humain ; les déviations contestées basculent dans",
    "`## Non-bugs tranchés`. Un `apply` sur rapport brut applique des",
    "faux positifs possibles en branche.",
    "",
    "### Phase 2 — application (worktrees, merge local par l'humain)",
    "",
    "Mode `apply` par audit — prérequis : rapport commité + validé :",
    "",
    "```sh",
    "git worktree add ../LibtorrentRust-aNN -b audit/NN-<slug>",
    "# l'agent travaille dans ce dossier (target/ isolé → builds parallèles) :",
    "# gates + changelog fragment + PORTING.md + ## Résultat, commit sur",
    "# sa branche, puis signale « prêt à merger » — tout reste LOCAL.",
    "# Sur ordre « merge » de l'humain : git merge audit/NN-<slug>",
    "# dans le checkout principal (un merge à la fois) :",
    "git merge audit/NN-<slug>",
    "# après merge :",
    "git worktree remove ../LibtorrentRust-aNN && git branch -d audit/NN-<slug>",
    "```",
    "",
    "Merges **séquentiels** ; en cas de conflit sur les fichiers chauds,",
    "rebase de la branche dans son worktree + re-run des gates, puis",
    "nouveau `git merge`.",
    "",
    "### Vagues d'application conseillées (recouvrement de fichiers,",
    "pas de numéros de crate : A02 et A13 sont tous deux dans",
    "`bitdaemon-session` mais dans des vagues différentes ; la vague 1",
    "traverse net/dht/disk/core).",
    "",
    "| Vague | Audits | Raison |",
    "|---|---|---|",
]
for w in sorted(WAVES):
    spec = WAVES[w]
    todo = [f"A{i}" for i in spec["ids"] if not status_of.get(i)]
    if todo:
        idx.append(f"| {w} — {spec['label']} | {', '.join(todo)} | {spec['why']} |")
idx += [
    "",
    "La description de chaque skill porte son préfixe `[Vn]`. La vraie",
    "source est la section `## Fichiers du fix` de chaque rapport.",
    "",
    "A16+A17 partagent `bitdaemon-disk` mais des modules disjoints — OK",
    "ensemble. Dans la vague 4, préférer des paires et merger une",
    "branche avant d'en appliquer une autre sur le même fichier.",
    "",
    "### Agrégation changelog",
    "",
    "```sh",
    "python docs/audits/_aggregate_changelog.py   # après merge des PRs",
    "```",
    "",
    "Fusionne `docs/audits/changelog/A*.md` dans `CHANGELOG.md` sous",
    "`## [Unreleased]` (regroupé par section `### Fixed` etc.) puis",
    "supprime les fragments. Commit : `docs(changelog): aggregate audit entries`.",
    "",
    "### Ressources",
    "",
    "- Un `target/` par worktree (plusieurs GB chacun). `sccache` mutualise",
    "  la compilation des deps : `cargo install sccache` puis",
    "  `RUSTC_WRAPPER=sccache` (bash) ou `$env:RUSTC_WRAPPER=\"sccache\"`",
    "  (PowerShell).",
    "- 2-4 builds workspace concurrents max en local (cargo utilise déjà tous",
    "  les cœurs). Pour du parallélisme massif : sessions cloud séparées,",
    "  une par audit — chacune a sa machine et son checkout ; l'agent",
    "  push juste sa branche pour que l'humain la merge en local.",
    "",
    "## Ordre — les numéros SONT l'ordre conseillé",
    "",
    "`A01`→`A29` se lit dans l'ordre d'application recommandé pour un",
    "agent seul : protocole (wire→ext→MSE) → sources de pairs",
    "(trackers/uTP/web seeds) → admission → chemins de données swarm",
    "(download/upload/pièces) → politique (choker/TC/picker) → internes",
    "disque → métadonnées/état → périphériques (SSL/proxies/NAT/DHT) →",
    "glue/bandwidth → sweep compteurs. Les audits `done` gardent leur",
    "position dans la numérotation (ils ont déjà été exécutés là).",
    "Rapports : aucun ordre — tous parallélisables.",
    "",
    "En parallèle (2-4 apply) : piocher dans la même vague de préférence,",
    "et merger dans l'ordre des numéros quand deux PRs se recouvrent.",
    "",
]
with open("docs/audits/INDEX.md", "w", encoding="utf-8") as f:
    f.write("\n".join(idx))

# Per-audit skills — identical whether done or not: a re-run is a FULL
# fresh audit, never a reduced regression check (the done flag only
# feeds INDEX.md status + wave-table filtering, invisible to the agent).
# Skills are generated — wipe stale audit-* dirs so renames (e.g. wave
# tags added to the name) never leave orphans. Never hand-edit them.
skills_root = os.path.join(".devin", "skills")
for old in os.listdir(skills_root):
    p = os.path.join(skills_root, old)
    if old.startswith("audit-") and os.path.isdir(p):
        shutil.rmtree(p)

for aid, slug, title, scope, up, rs, chk, traps, _status in AUDITS:
    w = WAVE_OF[aid]
    wspec = WAVES[w]
    sname = f"audit-{aid}-v{w}-{slug}"
    d = os.path.join(skills_root, sname)
    os.makedirs(d, exist_ok=True)
    body = [
        "---",
        f"name: {sname}",
        f'description: "[V{w}] Audit sémantique vs libtorrent — {title}"',
        "allowed-tools:",
        "  - read",
        "  - grep",
        "  - glob",
        "  - exec",
        "  - edit",
        "---",
        "",
        f"# Audit A{aid} — {title}",
        "",
    ]
    body += [
        f"**Périmètre** : {scope}.",
        "",
        f"**Vague d'apply** : V{w} ({wspec['label']}) — {wspec['why']}.",
        "Choix des audits concurrents par recouvrement de fichiers,",
        "affiner avec la section `## Fichiers du fix` du rapport.",
        "",
        "## Fichiers",
        "",
        f"- **Upstream** (baseline `e7381c7bc`, lecture seule) : {up}",
        "  Checkout upstream, résolu dans l'ordre : `$LIBTORRENT_ROOT` si",
        "  définie → `D:/Projet/libtorrent/libtorrent` (poste local) → sinon",
        "  `git clone https://github.com/arvidn/libtorrent ../libtorrent`",
        "  puis `git -C ../libtorrent checkout e7381c7bc` (cloud/CI).",
        f"- **Rust** (repo courant) : {rs}",
        "",
    ]
    if chk:
        body += ["## Checklist spécifique", ""]
        body += [f"- [ ] {c}" for c in chk]
        body.append("")
    if traps:
        body += ["## Pièges propres au périmètre", "", traps, ""]
    chk_verdicts = (
        "\n".join(f"   - {c} — verdict : … — preuve : …" for c in chk)
        if chk else
        "   (aucune checklist spécifique — appliquer les `Pièges connus` transverses ci-dessous, un verdict chacun)"
    )
    body.append(PROTOCOL.format(aid=aid, slug=slug, checklist_verdicts=chk_verdicts))
    with open(os.path.join(d, "SKILL.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(body))
    print(f"wrote {d}/SKILL.md")
print("done")
