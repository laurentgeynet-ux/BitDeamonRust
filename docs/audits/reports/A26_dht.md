# Audit A26 — rapport d'analyse

Périmètre : Mainline DHT — KRPC, traversées, BEP-44 items, tokens
rotatifs, routing table, BEP-42, dos_blocker, persistance
`dht_state`, réponses `get_peers`. Baseline upstream `e7381c7bc`.

Architecture Rust : crate `crates/bitdaemon-dht` (`dht_node.rs`,
`dht_tracker.rs`, `rpc.rs`, `msg.rs`, `routing_table.rs`,
`node_entry.rs`, `node_id.rs`, `traversal.rs`, `lookups.rs`,
`dht_storage.rs`, `dht_state.rs`, `dos_blocker.rs`, `item.rs`,
`ed25519.rs`, `dht_settings.rs`) ; glue session dans
`crates/bitdaemon-session/src/session.rs` (`on_udp`, DHT events →
alerts, ~7730–8048). Transport injecté (ADR-0008) — le tracker ne
possède pas de socket, seules les sémantiques wire/état sont
jugées.

## Verdicts checklist

- **KRPC** : encodage requête/réponse/erreur, `t` transaction 2
  octets, erreurs `e:[code,msg]` — verdict : **conforme** —
  preuves : `msg::verify_message` est une transcription fidèle de
  `verify_message_impl` (`crates/bitdaemon-dht/src/msg.rs:40-108` vs
  `src/kademlia/msg.cpp` — flags `optional`/`parse_children`/
  `last_child`/`size_divisible`, stack de dicts parents, erreurs
  "missing '<k>' key"/"invalid value for '<k>'") ; tid `u16` aléatoire
  big-endian matché par `(tid, ep)` (`rpc.rs:93-157` vs
  `rpc_manager.cpp:447-460,236-260`) ; `ro:1` émis en read-only et
  honoré à l'entrée (`rpc.rs:111-113`, `dht_node.rs:441-443`) ;
  erreurs `[code,msg]` avec `r:{id,p}` quand le verify top-level a
  passé, `ip` systématique — y compris le cas `enforce_node_id` où
  upstream émet l'erreur **sans** `r` (ordre identique,
  `dht_node.rs:932-941,1443-1457` vs `node.cpp:790-825,86-93`).
  Nuance identité : `v` stamp `BD\x00\x10` au lieu de `LT…`
  (`rpc.rs:116`, `dht_node.rs` `CLIENT_VERSION`) — choix assumé
  d'identification client, voir « Non-bugs ».
- **Traversées** : `find_node`, `get_peers` + collecte de tokens,
  `announce_peer` (write-token, `implied_port`), `sample_infohashes`
  — verdict : **déviation** — preuves conformité : seeds
  `find_node` puis fallback routeurs `flag_initial` quand <3
  (`dht_node.rs:288-306` vs `find_data.cpp:64-81`) ;
  `m_write_tokens` par `(nid → (ep, token))` (`lookups.rs:80-83,
  235-240` vs `find_data.cpp:83-96`) ; `implied_port` → port source,
  borne `0..65536`, alerte announce **avant** vérif token, `n`/
  `seed` optionnels, `node_seen(id, ep, 0xffff)` post-token
  (`dht_node.rs:1088-1133` vs `node.cpp:920-958`) ; write_token =
  `sha1(ip_str ‖ secret ‖ ih)[..4]`, double secret
  (`dht_node.rs:254-277` vs `node.cpp:150-175`). **Écarts** :
  validation de la réponse `sample_infohashes` et non-traversée des
  `nodes` absents — voir D4.
- **BEP-44** : immutable/mutable, `k`, `sig`, `seq`, `salt`, CAS,
  Ed25519 — verdict : **conforme** — preuves : `handle_put`
  reproduit l'ordre upstream verify→taille `v` (205) →salt (207) →
  cible dérivée de `k` (quirk : `k` seul cible le mutable) →
  token (203) → `seq<0` (203) → signature (206) → CAS (301) →
  vieux seq (302) (`dht_node.rs:1256-1389` vs `node.cpp:985-1100`) ;
  `canonical_string` `4:salt<n>:…3:seqi<n>e1:v<v>`, `salt` capé à 64,
  `item_target_id` sha1(`v`) / sha1(`pk‖salt`)
  (`item.rs:27-55` vs `item.cpp:30-99`) ; Ed25519 via
  `ed25519-dalek` (ADR-0007) ; storage : `max_dht_items` 700,
  `item_lifetime` plancher 120 min, seq monotone + CAS dans
  `put_mutable_item` (`dht_storage.rs` vs `dht_storage.cpp`).
- **Tokens** : rotation secrets, durée de validité, binding IP —
  verdict : **conforme** — deux secrets `[0]/[1]`, rotation 300 s
  (`SECRET_ROTATION`, `dht_tracker.rs:22,208-217` vs
  `dht_tracker.cpp:45` `m_key_refresh_timer` 5 min), vérif accepte
  l'une ou l'autre génération (`dht_node.rs:265-276`), token lié à
  `addr.ip().to_string()` + info-hash — identique à
  `node.cpp:150-175`.
- **Routing table** : K=8, split, remplacement oldest/pinger,
  refresh, vérification — verdict : **conforme** — transcription
  proche de `routing_table.cpp` : buckets K=8, split conditionné par
  `extended_routing_table` + préfixe proche, `restrict_routing_ips`
  (anti-IP-dupliquée), `restrict_search_ips`, heuristiques
  replacements (fail_count, pinged, RTT), `find_node` incluant
  failed, `node_seen`/`heard_about`, refresh `next_refresh`
  (`routing_table.rs`, `dht_node.rs:1678-1683` vs
  `routing_table.cpp`, `node.cpp` `node::tick`). Différences
  mineures sans impact : tri du suffixe au lieu de `nth_element`
  (même sélection, ordre interne différent), `Option<NodeId>` pour
  les entrées ping-only. Voir « Non-bugs ».
- **BEP-42** : dérivation node-id + enforcement entrant — verdict :
  **conforme** — `generate_id`/`verify_id` fidèles
  (`node_id.rs` vs `node_id.cpp` : masques /21+21 bits,
  `r = ip[0]&0x7` etc., secret seed `make_id_secret`) ;
  `dht_enforce_node_id` → erreur "invalid node ID" avant
  `heard_about`, et `rpc.incoming` invalide les réponses à `r.id`
  non conforme (`dht_node.rs:937-941`, `rpc.rs:162-172` vs
  `node.cpp:816-820`, `rpc_manager.cpp:327-333`).
- **DoS blocker** : fenêtre 10 s, rate/prefix — verdict :
  **déviation** — `dos_blocker.rs` est fidèle (20 entrées,
  `block_ratelimit` 5, fenêtre 10 s, ban 5 min, remplacement
  least-loaded) mais deux pré-filtres upstream manquent en amont :
  le test de framing `size>20 && 'd'…'e'` et le `ip_filter` de la
  session — voir D1, D3.
- **État DHT** : `dht_state` persistence, node-ids, round-trip —
  verdict : **conforme** — `extract_node_ids` : ancien format
  20 o (nid seul, adresse non spécifiée — comme upstream
  `address()` qui ne matchera jamais une bind-addr réelle) puis
  listes `nid‖addr` 24/36 o ; `nodes`/`nodes6` en compact endpoint
  lists (`dht_state.rs:43-156` vs `dht_state.cpp`) ; `state()` ne
  sépare pas `nodes`/`nodes6` et sauve live + replacements comme
  `for_each_node(f)` single-lambda upstream
  (`dht_tracker.cpp:604-620`). Bootstrap seede d'abord sa famille
  (`session.rs` / `dht_tracker` `load_state` vs
  `dht_tracker.cpp:165-167`).
- **get_peers réponse** : `values`/`nodes`/`token` — verdict :
  **conforme** — côté serveur : `nodes` toujours présents,
  `token` retenu quand storage plein (`no_token`), `values` compact,
  `n`, `BFpe`/`BFsd` (`dht_node.rs:955-1022` vs `node.cpp:842-880`,
  `dht_storage.cpp` `get_peers` bloom+`n`). Côté client : format
  Mainline (string unique 6-o, v4 uniquement) vs liste
  uTorrent/libtorrent 6/18 o (`lookups.rs:243-274` vs
  `get_peers.cpp:41-64`). L'ordre des clés est imposé par le tri
  bencode — pas de divergence d'ordre possible.

## Déviations

### Entrée datagramme (`dht_tracker` / `node::incoming`)

- **`DhtTracker::incoming` / `DhtNode::incoming`**
  (`crates/bitdaemon-dht/src/dht_tracker.rs:139-163`,
  `dht_node.rs:397-413`) — upstream rejette en premier
  `buf_size <= 20 || front != 'd' || back != 'e'` puis
  **return false sans compter ni filtrer**
  (`src/kademlia/dht_tracker.cpp:517-523`). Rust envoie tout au
  `dos_blocker` puis au bdecode : le trafic UDP non-DHT sur le même
  port (LSD `BT-SEARCH`, bruit) consomme le budget de rate-limit et
  peut déclencher un ban de 5 min qu'upstream ne ferait pas.
  - **sketch** : dans `DhtTracker::incoming`, avant le test
    dark-internet : `if buf.len() <= 20 || buf[0] != b'd' ||
    buf[buf.len()-1] != b'e' { return None; }`. Test : datagramme
    `BT-SEARCH …` × N ne déclenche jamais le blocker ; `de` (dict
    vide, ≤20 o) ignoré.
- **`dht_node.rs:413`** — `bdecode_checked` utilise les limites par
  défaut (depth 100, tokens 2 M) ; upstream parse les KRPC avec
  `depth_limit=10, token_limit=500` (`dht_tracker.cpp:533`). Les
  datagrammes profonds/larges qu'upstream jette sont décodés chez
  nous (surface DoS, divergence sur entrée malformée).
  - **sketch** : `bdecode_limits(buf, 10, 500)` puis `.error()`.
    Test : dict imbriqué 50 niveaux → ignoré (pas de `Pkt` event
    traité).
- **Session `on_udp`** (`crates/bitdaemon-session/src/session.rs:6667-6670`)
  — `self.dht.incoming` ne consulte jamais `self.ip_filter` ;
  upstream `dht_tracker::incoming_packet` droppe
  `m_ip_filter.access(addr)` avant le blocker
  (`dht_tracker.cpp:~527`). Sévérité : politique — les sources
  bannies par `ip_filter` sont quand même traitées par le DHT.
  - **sketch** : dans `on_udp`, avant `dht.incoming` :
    `if self.ip_filter.access(from.ip()) != 0 { /* skip */ }`
    (suivre la convention `session.rs:2641`/`IpFilter`), puis ne pas
    router vers LSD non plus si upstream droppe tout — vérifier :
    upstream `dht_tracker` return false → session essaie les autres
    décodeurs, donc filtrer uniquement le branche DHT. Test :
    `ip_filter` banissant `1.2.3.4` + datagramme `ping` valide →
    aucune réponse.

### sample_infohashes (réponse)

- **`lookups.rs:294-311` + dispatch `dht_node.rs:520-530,
  580-599`** — upstream `sample_infohashes_observer::reply` exige
  `interval` int `0..=21600`, `num` int `>= 0` (et ≤ INT_MAX), et
  `samples` string de longueur divisible par 20 — tout écart →
  `timeout()` (le pair est compté échoué, pas vivant) ; il collecte
  les `nodes` dans le callback **sans** les traverser (pas de
  `traversal_observer::reply`) (`sample_infohashes.cpp:49-136`).
  Rust extrait `interval`/`num`/`samples` de façon optionnelle,
  marque le pair vivant et **traverse** les `nodes` du reply
  (`lookup_reply` itère `data.nodes` → `algo.traverse`) — trafic
  requête supplémentaire et complétion différente.
  - **sketch** : dans `Lookup::on_reply`, branche
    `SampleInfohashes` : exiger `iv`/`n`/`s` valides avec bornes
    (`0..=21600`, `>= 0`, `len%20==0`) sinon `return None` **et**
    signaler l'échec au travers de l'observer (upstream
    `timeout()` — plomber un chemin `lookup_failed` depuis
    `on_reply`, ou retourner un marqueur `ReplyData::invalid` que
    `handle_reply` transforme en `lookup_failed`) ; les `nodes`
    vont dans `out.nodes` pour le callback mais doivent être
    exclus de la traversée — séparer `nodes` (livré au consumer)
    d'une traverse-list vide pour ce kind. Test : réponse avec
    `interval` absent → observer timeout ; `samples` len 21 →
    timeout ; réponse valide → callback avec samples+nodes, aucune
    requête supplémentaire émise.

### Ticks

- **`dht_node.rs:1657` + `dht_storage.rs:550`** — `storage.tick()`
  est appelé à chaque tick node ; upstream le borne à une fois par
  2 min via `m_last_tracker_tick` (`node.cpp` `connection_timeout`).
  Impact : purge des peers/items plus précoce + coût CPU ; cadence
  de session plus rapide aggrave l'écart.
  - **sketch** : ajouter `last_storage_tick: Option<Instant>` sur
    `DhtNode` ; n'appeler `storage.tick()` que si
    `>= 120 s` écoulées (reprendre le nom `m_last_tracker_tick` en
    commentaire). Test : deux `tick()` à <120 s → une seule purge.
- **`dht_node.rs:1666-1677`** — quand la table est vide, Rust retente
  `bootstrap()` toutes les 5 s (`BOOTSTRAP_RETRY`) et met à jour
  `last_self_refresh` ; upstream n'a que le self-refresh à 10 min
  (depth<4) et `return` immédiat sans `next_refresh`
  (`node.cpp` `node::tick`). Wire-visible : rafales `get_peers`
  supplémentaires en cas de bootstrap non répondu.
  - **sketch** : choix assumé de robustesse — à documenter dans
    PORTING.md ou aligner : supprimer `retry` (le seul gate reste
    `stale && depth<4`) ou garder avec un commentaire « délibéré »
    et un test d'intégration montrant la cadence 5 s.

### RPC manager

- **`rpc.rs:198-235`** — upstream `rpc_manager::tick` efface d'abord
  les observers `flag_done` (annulés, ex. `obfuscated_get_peers`
  qui avorte la phase-1) sans timeout ; Rust garde la transaction
  jusqu'à réponse ou 15 s, puis déclenche `Expiry::Timeout` sur un
  lookup terminé (no-op si `is_done`, mais slot tid + événement
  d'expiration superflu). Upstream `observer::abort()` → `flag_done`
  → effacé au tick.
  - **sketch** : quand un observer est marqué done/aborted
    (`traversal::obs_flags::DONE` / abort), supprimer son entrée
    `Pending` via une méthode `RpcManager::drop_tag(tag)` — ou,
    minimal, dans `RpcManager::tick`, ignorer silencieusement les
    tags dont le lookup est `None`/done (nécessite un hook vers le
    node — préférer `drop_tag` appelé depuis `Lookup` teardown).
    Test : lookup obfusqué complété → aucun `Expiry` pour ses
    observers annulés.

## Non-bugs tranchés

- **`verify_message` loop bound** (`msg.rs:56`) : borné par
  `desc.len() && ret.len()` alors qu'upstream itère `ret.size()`
  avec `TORRENT_ASSERT(desc.size()==ret.size())` — tous les call
  sites passent des tableaux de même taille (`top[4]`, `k[6]`,
  `k[7]`…) ; équivalent.
- **`canonical_string` sans buffer 1200** (`item.rs:27-44`) :
  upstream tronquerait un message canonique >1200 o ; en pratique
  `v ≤ 1000` (contrôle 205) + `salt ≤ 64` + ~30 o d'entête <
  1200 — la troncature upstream est inatteignable ; sortie octet-
  identique.
- **`for_each_node` sauvegarde** : upstream `state()` appelle
  `for_each_node` avec un seul callback qui s'applique aux live ET
  aux replacements (`routing_table.hpp:212-213`) — Rust fait de
  même ; pas de déviation.
- **`find_node` nth_element vs tri du suffixe**
  (`routing_table.rs`) : la sélection des K plus proches est la
  même ; seul l'ordre interne des non-sélectionnés diffère —
  invisible.
- **Réponse d'erreur `r:{id,p}`** : upstream conserve `e["r"]`
  (créé avant les vérifications par-query) et `e["ip"]` dans les
  réponses d'erreur, **sauf** sur échec `enforce_node_id` où `r`
  n'existe pas encore — Rust reproduit exactement ces deux cas
  (`dht_node.rs:932-941,1443-1457` vs `node.cpp:790-825,86-93`).
- **Alerte announce avant token** : `dht_observer::announce` /
  `IncomingAnnounce` est émis après validation du port mais avant
  `verify_token` des deux côtés (`dht_node.rs:1105-1110` vs
  `node.cpp:935-943`).
- **`get` reply** : `token` + `nodes` avant items ; `seq` présent →
  mutable seul, sinon immutable puis mutable `force_fill`
  (`dht_node.rs:1141-1180` vs `node.cpp:1102-1145`) — identique.
- **Quirk `k`-seul** : un `put` avec `k` mais sans `seq`/`sig` cible
  l'id mutable tout en étant stocké comme immutable — reproduit
  (`dht_node.rs:1333-1344`).
- **`d`/`e` framing sur réponses** : `y` non-1-char droppé en
  silence comme upstream (protection amplification,
  `dht_node.rs:430-435` vs `node.cpp:250-258` commenté).
- **Old-format `node-id`** : adresse non spécifiée — upstream
  `address()` non plus (`0.0.0.0`), qui ne matche pas non plus une
  vraie bind-addr dans `new_socket` ; comportement équivalent.
- **CLIENT_VERSION `BD`** : choix d'identité délibéré (le port
  s'annonce `BD` — OnionBit) ; upstream stamp `LT`+version. À
  acter dans PORTING.md, pas un bug de protocole.
- **`ip` echo + votes** : `e["ip"]` écho compact par réponse, votes
  externes drainés par `drain_ip_votes` → `IpVoter` source DHT
  (`dht_node.rs:414-429,932-933`, `session.rs:6675-6684` vs
  `node.cpp:264-281`).
- **DoS blocker internals** : fenêtre 10 s, `block_ratelimit`,
  `block_timeout` 5 min, 20 entrées, remplacement least-loaded —
  conforme à `dos_blocker.cpp`.
- **Token generation** : `sha1(ip_str ‖ secret ‖ ih)` tronqué à 4 o,
  fenêtre double-secret — byte-compatible avec upstream
  (`dht_node.rs:254-277`).

## Résultat

Mode **rapport** : aucune modification de code. Déviations
confirmées : D1 framing pre-check absent, D2 limites bdecode
(10/500), D3 `ip_filter` non appliqué au DHT, D4 validation +
non-traversée `sample_infohashes`, D5 `storage.tick` non borné à
2 min, D6 retry bootstrap 5 s (à documenter ou aligner), D7
transactions d'observers annulés gardées jusqu'à timeout. À
corriger dans la phase apply (`audit/26-dht`).
