# Audit A22 — rapport d'analyse

Périmètre : `apply_settings` (legs par clé), `session_params` / `.session`
round-trip, clés deprecated. Baseline upstream `e7381c7bc` (ABI2 :
`DEPRECATED_SET` → `{"", nullptr, 0}` — les clés deprecated **n'ont ni nom
ni callback** upstream).

## Verdicts checklist

- apply_settings : chaque clé ayant un effet live vs celles ne s'appliquant
  qu'au prochain storage/conn — verdict : **déviation** — preuve :
  `crates/bitdaemon-session/src/session.rs:6036-6159` couvre disk push,
  alert queue/mask, pe_settings, toggles de services, reopen
  listen/outgoing/ssl, privileged-ports, connections/unchoke/rate limits,
  mixed-mode, `close_redundant_connections`. Upstream
  `apply_settings_pack_impl` (`src/session_impl.cpp:1566-1614`) +
  `apply_pack` (`src/settings_pack.cpp:596-684`) déclenche en plus :
  reopen sur `proxy_type`/`proxy_peer_connections`, `update_proxy`,
  `update_dht_bootstrap_nodes`, `update_user_agent` (sanitize `\n`),
  `update_connection_speed` (clamp <0→200), `update_disk_threads`
  (clamp <0→0), `trigger_auto_manage` (active_*), `update_auto_sequential`
  / `update_max_failcount` (par torrent), `update_count_slow`,
  `update_report_web_seed_downloads`, `update_socket_buffer_size`,
  `update_peer_dscp`, `update_ip_notifier`, `update_validate_https`,
  `update_resolver_cache_timeout`, `update_want_peers`
  (`seeding_outgoing_connections`/`enable_outgoing_tcp`/`utp`).
  Détail par déviation dans `## Déviations`.
- Clés sans effet live : documenté comme upstream — verdict : **conforme** —
  preuve : la majorité des knobs sont relus paresseusement via
  `self.settings` au tick (`session.rs:1268` `connection_speed`,
  `7290` `dht_announce_interval`, `3426-3431` `dont_count_slow_torrents` /
  `auto_manage_prefer_seeds`, `8241-8242` intervalles d'unchoke), ce qui
  correspond au modèle upstream (la plupart des entrées de
  `settings_pack.cpp` ont `fun == nullptr`). Les legs présents côté Rust
  sont exactement ceux où upstream pousse (file_pool_size, alert queue,
  rates, choker, services, privileged ports).
- .session file : layout dict, ip_filter4/6, dht_state embed, version compat
  — verdict : **déviation** — preuve : `session_params.rs:23-147` reproduit
  fidèlement `write_session_params`/`read_session_params` upstream
  (`"dht"`, `"dht state"`, `"settings"`, `"extensions"`, `ip_filter4/6`
  avec le stride 12/36 + skip `first>last`), **mais** ce format n'est pas
  celui exposé par le handle : `SessionHandle::write_session_params()`
  (`session_handle.rs:382`) renvoie `save_state_buf()`, c-à-d le container
  BitDaemon `SessionState` (`session_state.rs:42-67` : `{"version":1,
  "torrents":{ih:resume}, "dht":<blob str>, "settings":…}`) qui n'écrit ni
  `ip_filter4/6` ni `"extensions"` ni `"dht state"`. `load_state`
  (`session.rs:10060-10106`) ne lit ni `ip_filter4/6` ni le dict
  `"dht state"` (le container attend le blob sous la string `"dht"`), ni
  le dict legacy `"dht"` comme source de `dht_*` settings — un
  `session_params` upstream n'est donc que partiellement chargeable.
- Deprecated keys : conservés en écriture pour round-trip interop? —
  verdict : **conforme (superset)** — preuve : `session_params.rs` écrit
  ~40 clés deprecated (`mmap_cache`, `lazy_bitfields`, `cache_size`,
  `local_*_rate_limit`, `ssl_listen`…) sous leurs vrais noms ; upstream
  ABI2 les écrit sous le nom `""` (entrées anonymes dans
  `str/int/bool_settings`) voire pas du tout côté load. Sens inverse OK :
  `load_pack_from_dict` upstream ignore les noms inconnus, et
  `apply_settings_dict` Rust ignore les absents. Les legs live que Rust
  donne aux deprecated `local_*_rate_limit`/`rate_limit_utp`/
  `ignore_limits_on_local_network` (`session.rs:6121-6138`) sont un
  superset — upstream ABI2 a `fun == nullptr` pour elles (la valeur change
  mais rien n'est repoussé). Cf. `## Non-bugs tranchés`.
- Diff ciblé : parmi les 266 keys validées par défaut, lesquelles sont
  réellement consommées — verdict : **déviation (mineure)** — preuve :
  tous les champs `SessionSettings` round-trippent via
  `save_settings_to_dict`/`apply_settings_dict` (macros `put!`/`get!`
  symétriques, noms upstream). Mais `peer_dscp`
  (`settings.rs:423`, sérialisé `session_params.rs:631/1246`) n'est
  consommé nulle part — `grep dscp` : aucune application aux sockets
  (upstream `update_peer_dscp` pousse `set_traffic_class` sur les sockets
  d'écoute live, `session_impl.cpp:6627`). Les autres clés sans
  consommateur sont des knobs deprecated/inertes chez upstream aussi.
  Alias legacy `peer_tos` → `peer_dscp` absent (`settings_pack.cpp:422`).

## Déviations

- `session.rs:6100-6107` — `reopen_listen_port` : upstream le déclenche
  sur `listen_interfaces | proxy_type | proxy_peer_connections`
  (`session_impl.cpp:1568-1575` ; +`ssl_listen` en ABI1), **pas** sur
  `outgoing_interfaces` (qui ne fait que re-parser `m_outgoing_interfaces`,
  `session_impl.cpp:5347-5361`). Rust fait l'inverse : reopen sur
  `listen/outgoing/ssl_listen`, rien sur le proxy. Conséquence : changer
  `proxy_type`/`proxy_peer_connections` à chaud ne ré-ouvre pas les
  sockets de listen proxy upstream-style. Sévérité : état (sockets).
  - **sketch** : `session.rs:6102` — remplacer le gate par
    `s.listen_interfaces != self.settings.listen_interfaces
    || s.proxy_type != self.settings.proxy_type
    || s.proxy_peer_connections != self.settings.proxy_peer_connections`;
    sortir `outgoing_interfaces` du gate (le re-parse est lazy côté dial,
    vérifier `outgoing_interfaces` est lu à la connexion — sinon ajouter
    un re-parse `m_outgoing_interfaces`). Réf. `session_impl.cpp:1568-1607`.
    Test : apply_settings avec `proxy_type` changé → `reopen_sockets`
    levé ; `outgoing_interfaces` seul → pas de reopen.
- `session.rs:6036-6159` — pas de leg `update_proxy` : upstream pousse le
  nouveau `proxy_settings` vers `udp_sock->sock.set_proxy_settings` de
  chaque listen socket (`session_impl.cpp:5652-5657`). Rust : sockets
  injectées (ADR-0008) — émettre un `SessionOut`/`listen` leg pour que le
  transport applique le nouveau proxy aux sockets uTP/DHT live.
  Sévérité : état.
  - **sketch** : dans `apply_settings`, si `proxy_{type,port,hostname,
    username,password}` ou `proxy_peer_connections` changent → pousser un
    effet `SessionOut::ProxyChanged(ProxySettings)` (ou champ sur le
    service d'écoute) consommé par le transport ; garder la doc « applies
    to new conns » si le socket live ne peut pas être reconfiguré.
- `session.rs:6096-6099` — pas de leg `update_dht_bootstrap_nodes` :
  upstream re-parse `dht_bootstrap_nodes` et `add_dht_router` chaque nœud
  quand DHT tourne (`session_impl.cpp:5714-5732`). Rust ne re-seede
  `dht_router_nodes` qu'à `enable_dht` (`session.rs:6614`).
  Sévérité : état (mineur).
  - **sketch** : dans `apply_settings`, si
    `s.dht_bootstrap_nodes != self.settings.dht_bootstrap_nodes &&
    self.services.dht` → parser la liste, `n.table.add_router_node` sur
    les nœuds live (+ `self.dht_router_nodes`). Réf.
    `session_impl.cpp:5696-5707,5714-5731`. Test : toggle de la liste
    avec DHT actif → router nodes mis à jour.
- `session.rs:1268` — `connection_speed < 0` : upstream clamp à 200
  (`update_connection_speed`, `session_impl.cpp:6719-6723` — *writeback*
  dans les settings). Rust lit la valeur brute → `max_connections <= 0`
  → starvation totale des dials. Sévérité : comportement (dials).
  - **sketch** : dans `apply_settings` (ou `try_connect_more_peers`),
    `if s.connection_speed < 0 { s.connection_speed = 200 }` avant
    `self.settings = s`. Réf. `session_impl.cpp:6721-6722`. Test :
    `connection_speed=-1` → dials repartent.
- `session.rs:6058-6059` — `aio_threads`/`hashing_threads < 0` : upstream
  clamp à 0 avant push (`update_disk_threads`, `session_impl.cpp:6748-6754`).
  Rust pousse le négatif brut à `disk_pool::set_max_threads`
  (`disk_pool.rs:113-133` : `cur > i` → `threads_to_exit = cur - i`
  dépasse `cur`). Sévérité : état (mineur).
  - **sketch** : `d.set_max_threads(s.aio_threads.max(0))` /
    `d.set_hash_threads(s.hashing_threads.max(0))` et écrire le clamp dans
    `self.settings` comme upstream (writeback). Réf. `session_impl.cpp:6750-6753`.
- `session.rs:6168` — `connections_limit <= 0` : upstream mappe à
  `max_open_files()` et **écrit la valeur résolue** dans les settings
  (`session_impl.cpp:6903-6907`) → pas de sweep de déconnexion. Rust fait
  `.max(1)` → `limit = 1` → `update_connections_limit` déconnecte tout
  le monde sauf 1. Sévérité : **wire-visible** (déconnexions massives).
  - **sketch** : `session.rs:6168` — traiter `<= 0` comme « unbounded » :
    `let limit = self.settings.connections_limit; if limit <= 0 {
    self.settings.connections_limit = <cap>; return; }` (cap =
    `i32::MAX` — pas d'équivalent `max_open_files()` sous transports
    injectés) ; garder le writeback dans `self.settings`. Réf.
    `session_impl.cpp:6901-6963`. Test : `connections_limit=0` +
    N conns → zéro déconnexion.
- `session.rs:6545-6563` — `update_unchoke_limit` : upstream ne fait le
  sweep « unchoke everyone » que pour `unchoke_slots_limit == INT_MAX` ;
  tout autre changement se contente de `m_unchoke_time_scaler = 0`
  (`session_impl.cpp:6694-6715`). Rust déclenche le sweep aussi pour
  `limit < 0` (`limit != i32::MAX && limit >= 0` → `-1` passe le filtre et
  unchoke tout le monde) et ne réarme pas le scaler dans le cas fini.
  Sévérité : wire-visible (UNCHOKE intempestifs) / timing.
  - **sketch** : `session.rs:6561` — `if limit != i32::MAX { <reset
    unchoke scaler — réarmer `unchoke_tick` pour un recalc immédiat,
    cf. champ à `session.rs:507-514`>; return; }` puis sweep. Réf.
    `session_impl.cpp:6691-6716`. Test : `unchoke_slots_limit=-1` → pas
    de sweep, recalcul au prochain tick ; `INT_MAX` → sweep.
- `session.rs:6036-6159` — pas de `trigger_auto_manage` sur les changements
  `active_downloads|seeds|checking|limit` (`settings_pack.cpp:270-276`).
  Rust ne décale pas `auto_manage_scaler` → nouvelle limites appliquées
  jusqu'à `auto_manage_interval` (30 s) plus tard. Sévérité : timing.
  - **sketch** : si l'une des 4 valeurs change → `self.auto_manage_scaler
    = 0` (le mécanisme « recalc au prochain tick » existe déjà,
    `session.rs:6808-6815` ; upstream throttle à 1 s via
    `m_last_auto_manage` — approximé par le scaler). Réf.
    `session_impl.cpp:6769-6784`. Test : `active_downloads` modifié →
    `recalculate_auto_managed_torrents` tourne au tick suivant.
- `session.rs:6156-6158` — pas de leg `update_auto_sequential` /
  `update_max_failcount` par torrent (upstream itère `m_torrents`,
  `session_impl.cpp:5634-5644`). Rust ne resynchronise que
  `close_redundant_connections` ; `Torrent::auto_sequential`
  (`torrent.rs:500`) n'est recalculé qu'au prochain changement de
  compteurs de pièces (`torrent.rs:10940`). `max_failcount` est relu live
  (`torrent.rs:3480,3560` lisent `st.max_failcount`) → seul
  `auto_sequential` est affecté. Sévérité : état (mineur).
  - **sketch** : dans la boucle `for t in self.torrents.values_mut()` de
    `apply_settings`, ajouter `t.update_auto_sequential(&self.settings)`
    quand `s.auto_sequential` a changé. Réf. `session_impl.cpp:5634-5638`,
    `torrent.rs:10839`. Test : toggle `auto_sequential` → flags torrent
    cohérents sans attendre un piece-complete.
- `session.rs:6036+` — pas de `update_user_agent` : upstream remplace les
  `\n` par des espaces dans `user_agent` au set (`session_impl.cpp:6665-6674`).
  Rust propage la valeur brute (web seed `User-Agent` header,
  `web_seed.rs:141` → injection d'en-têtes possible). Sévérité :
  sécurité/état (mineur).
  - **sketch** : dans `apply_settings` et le constructeur,
    `s.user_agent` → `iter().map(|b| if *b==b'\n' {b' '} else {*b})`.
    Réf. `session_impl.cpp:6667-6673`. Test : user_agent contenant `\n`
    → stocké sanitisé.
- `session_params.rs:912-918` (`get!` `str`) — les strings **vides** sont
  ignorées (`if !v.is_empty()`), donc un `.session`/`session_params` qui
  pose explicitement `proxy_hostname=""` (pour l'effacer) est ignoré ;
  upstream `load_pack_from_dict` applique toute clé présente, vide incluse
  (`settings_pack.cpp:473-479`). Sévérité : round-trip (état).
  - **sketch** : utiliser `dict_find_string` (présence de clé, pas
    non-vacuité) : `let v = n.dict_find($k); if v.node_type() ==
    NodeType::String { s.$f = v.string_value().to_vec(); }`. Réf.
    `settings_pack.cpp:474-478`. Test : dict `{"proxy_hostname":""}` →
    champ vidé.
- `session_params.rs` / `session_state.rs` — alias `peer_tos` →
  `peer_dscp` non géré (`settings_pack.cpp:421-424`). Sévérité : compat
  (mineur).
  - **sketch** : dans `apply_settings_dict`, après le bloc int :
    `let v = n.dict_find_int_value(b"peer_tos", i64::MIN); if v !=
    i64::MIN && !has(peer_dscp) { s.peer_dscp = v as i32; }`
    (priorité à la clé moderne si les deux présentes — upstream : la
    première match dans l'ordre du dict gagne, l'alias n'est consulté
    qu'en fallback de `setting_by_name`; le comportement « last write
    wins » par ordre du dict est acceptable). Test : `peer_tos=16` →
    `peer_dscp==16`.
- `session_state.rs:42-67` / `session.rs:10033-10055` — le `.session`
  produit ne contient ni `ip_filter4/6` ni `"extensions"` ni `"dht
  state"` au sens upstream ; `load_state` ignore ces clés d'un fichier
  upstream (`"dht state"` dict non string → non lu ; `ip_filter4/6`
  tombent dans `extra`). `SessionHandle::write_session_params()`
  (`session_handle.rs:382`) prétend produire le format
  `session_params.cpp` mais renvoie le container BitDaemon.
  Sévérité : interop fichier.
  - **sketch** : (a) `Session::save_state` : ajouter
    `st.extra` ← `write_session_params`-compatible ou exposer le vrai
    `session_params` via un nouveau handle `save_params()` utilisant
    `crate::session_params::{write_session_params_buf,read_session_params}`
    avec `ip_filter`/`ext_state`/`dht_state` du live ; (b) `load_state` :
    lire aussi `ip_filter4/6` via `session_params::load_ip_filter`
    (rendre la fn `pub(crate)`), `"dht state"` dict via
    `read_dht_state`, et `apply_dht_settings_dict` sur `"dht"` avant le
    `"settings"` overlay (ordre upstream `session_impl.cpp:843-880`).
    Réf. `session_params.cpp:90-193`. Test : fichier
    `write_session_params_buf` (rust) → `load_state` restaure ip_filter ;
    fixture bencode upstream → idem.
- `peer_dscp` sérialisé mais jamais appliqué (aucun consommateur ;
  upstream `update_peer_dscp` + application aux sockets). Sous ADR-0008
  le DSCP doit être exposé au connecteur injecté. Sévérité : fonctionnalité.
  - **sketch** : propager `peer_dscp` dans `PeerConnector`/`listen` params
    (`transport::ConnectOpts.dscp` ou équivalent) + leg
    `SessionOut`/service sur changement. Réf. `session_impl.cpp:6627-6663`.
- Legs sans équivalent injecté : `update_socket_buffer_size`,
  `update_ip_notifier`, `update_validate_https`, `update_resolver_cache_timeout`,
  `update_i2p_bridge`, `update_ssl_listen` — upstream touche des objets OS
  (options de socket, resolver, SSL ctx) inexistants côté acteur.
  Sévérité : état (mineur) — documenter « applies to new sockets only »
  ou exposer au transport.

## Fichiers du fix

- `crates/bitdaemon-session/src/session.rs` (apply_settings legs,
  update_connections_limit, update_unchoke_limit, load_state extras)
- `crates/bitdaemon-session/src/session_params.rs` (empty-string keys,
  alias `peer_tos`, visibilité `load_ip_filter`)
- `crates/bitdaemon-session/src/session_state.rs` (clés upstream
  `dht state`/`ip_filter4/6`/`extensions` dans le container)
- `crates/bitdaemon-session/src/session_handle.rs` (vrai
  `write_session_params` ou renommage)
- `crates/bitdaemon-net/src/transport.rs` + `listen.rs` (si leg proxy/DSCP
  exposé aux transports)
- `docs/PORTING.md` (rows `settings_pack.cpp`, `session_params.cpp`,
  `session_impl.cpp` apply/save/load)

## Non-bugs tranchés

- **Clés deprecated écrites sous leur vrai nom** (superset d'upstream
  ABI2 qui écrit `""`). Upstream les ignore au load (`load_pack_from_dict`
  matche par nom) → round-trip bidirectionnel sain ; garder les noms
  préserve les valeurs pour des clients plus anciens. Conforme.
- **Legs live sur `local_*_rate_limit`/`rate_limit_utp`/
  `ignore_limits_on_local_network`** (deprecated ABI2, `fun==nullptr`
  upstream) : superset intentionnel — les valeurs restent dans le pack et
  les peer classes les consomment ; comportement plus fidèle à l'intention
  qu'à la lettre ABI2. Conforme.
- **`update_want_peers`** : upstream force un recalcul des listes
  `want_peers_download/finished` ; Rust lit les gates volatiles
  (`seeding_outgoing_connections`, `enable_outgoing_*`) dans
  `Torrent::want_peers` au moment du dial (`torrent.rs:10204-10216`) —
  pas de liste intrusive à maintenir, effet équivalent. Conforme.
- **`update_count_slow`/`update_report_web_seed_downloads`/`update_dht_announce_interval`** :
  upstream re-pousse immédiatement ; Rust relu paresseusement au tick
  (`session.rs:3427`, `7290`) — décalage < 1 tick, pas wire-visible.
  Conforme.
- **Sanitize enc policies / seed choking** : enums typés Rust rendent les
  valeurs hors-enum impossibles ; le bitmask `allowed_enc_level` est
  sanitizé (`session.rs:6071-6078`) et `EncPolicy::from_int` retombe sur
  `Disabled` comme `sanitize_settings` (`session_params.rs:264-272`).
  Conforme.
- **`user_agent`/`peer_fingerprint` exclus du `load_state`** :
  `session.rs:10073-10081` — fidèle à `pack.clear()` upstream
  (`session_impl.cpp:853-858`). Conforme.
- **Dicts legacy ABI1 `"proxy"`/`"encryption"`** : upstream ne les lit
  qu'en `TORRENT_ABI_VERSION==1` (`session_impl.cpp:795-841`) — baseline
  ABI2, N-A.
- **`dht` legacy dict lu avant `settings`** : fidèle dans
  `read_session_params_node` (`session_params.rs:83-101`, ordre upstream
  `session_impl.cpp:868-880` inversé pour rétro-compat — upstream applique
  `"dht"` *après* `"settings"` dans `load_state`, mais
  `read_session_params` applique `apply_deprecated_dht_settings` *avant*
  `load_pack_from_dict`, `session_params.cpp:96-118`). Le `load_state` du
  container BitDaemon ne lit pas `"dht"`-dict → voir déviation.

## Résultat

Apply sur `audit/22-settings-session-state`, commit `ab61626`
(`fix(bitdaemon-session): apply_settings & .session parity — hot-apply legs`).

### Déviations corrigées

- `reopen_listen_port` : gate ramenée à `listen_interfaces` /
  `proxy_type` / `proxy_peer_connections` (+ `ssl_listen`, clé encore
  live chez nous) — `outgoing_interfaces` ne ré-ouvre plus.
- `apply_settings` : ajout des legs `update_proxy`
  (`proxy_settings(m_settings)` → tous les purpose-slots),
  `update_dht_bootstrap_nodes`, `trigger_auto_manage`,
  `update_auto_sequential`, sanitize `user_agent` (`\n`→`' '`),
  `connection_speed < 0` → 200, `aio_threads`/`hashing_threads` clamp 0.
- `update_connections_limit` : `<= 0` → cap non borné
  (`i32::MAX`, équivalent `max_open_files()`) au lieu de `.max(1)` —
  plus de sweep destructeur sur `connections_limit = 0`.
- `update_unchoke_limit` : le cas fini ne fait que réarmer le scaler ;
  le sweep immédiat reste réservé à `INT_MAX`, le négatif ne déclenche
  plus d'unchoke intempestif.
- `.session` / session-params : `SessionState` porte désormais
  `"dht state"` (dict), `"extensions"`, `ip_filter4`/`6` ;
  `load_state` accepte un blob `write_session_params` upstream
  (`"dht"`-dict legacy → `dht_*`, overlay `"settings"`, ip filter
  restauré via `set_ip_filter`, identité client préservée).
  `SessionHandle::write_session_params` émet le vrai dict
  `session_params`.
- `apply_settings_dict` : applique toute clé string présente (vide
  incluse — efface le champ) ; alias 1.x `peer_tos` → `peer_dscp`.
- `set_proxy`/`set_peer_proxy`/`set_tracker_proxy` écrivent les clés
  `proxy_*` via `apply_settings` ; `proxy()`/`peer_proxy()` renvoient
  `proxy_settings(m_settings)`.

### Tests

`tests/test_settings_apply_a22.rs` (11 tests) : gate de reopen,
purpose-slots proxy, sanitizers, `connections_limit <= 0`, bootstrap
DHT, string vide, alias `peer_tos`, portage `ip_filter`/`dht
state`/`extensions`, lecture d'un blob `write_session_params`,
préservation de l'identité, ordre de merge `"dht"`/`"settings"`.
`test_connect_pump::connections_limit_blocks_the_pump` réécrit pour la
sémantique upstream (cap positif + half-open). Gates : fmt, clippy
`-D warnings`, `cargo test --workspace`, `cargo doc --no-deps` — verts.
