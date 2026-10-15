# Audit A25 — rapport d'analyse

Périmètre : LSD multicast, UPnP IGD lifecycle, NAT-PMP/PCP mappings,
external IP election. Baseline upstream `e7381c7bc`.

Architecture Rust : `bitdaemon-net` porte les codecs + la machine
d'état `UpnpDriver` ; le driver NAT-PMP et le pump LSD/SSDP vivent
dans `crates/bitdaemon/src/engine.rs` (transports injectés,
ADR-0008/0018/0019).

## Verdicts checklist

- LSD : multicast group/port, cadence par torrent, cookie, receive →
  peer feed, off en anonymous_mode — verdict : **déviation** —
  preuves : groupes/port conformes
  (`crates/bitdaemon-net/src/lsd.rs:11-15` vs `src/lsd.cpp:84-86`),
  cadence `local_service_announce_interval/N` modélisée par budget
  (`crates/bitdaemon-session/src/session.rs:6738-6756` vs
  `session_impl.cpp:4094-4121`), cookie filtrant
  (`lsd.rs:110-114`), `process_packet` → `add_swarm_peer` +
  `LsdPeer` alert (`session.rs:6686-6715`). **Écarts** : pas de
  filtre netmask entrant (`lsd.cpp:221`), pas de resend (3 envois
  upstream, `lsd.cpp:175-183`), pas de `do_connect_boost` ni check
  i2p sur `on_lsd_peer` (`session_impl.cpp:5870-5896`), announce des
  deux info-hashes pour hybrides vs une seule (`torrent.cpp:2964`),
  port annoncé = port bindé au lieu de `tcp_external_port()` /
  `ssl_listen_port()` (`torrent.cpp:2953-2960`). Voir `## Déviations`
  D1–D5.
- UPnP : SSDP discover, WANIP/WANPPP, AddPortMapping + renew,
  DeletePortMapping au stop — verdict : **déviation** — preuves :
  M-SEARCH byte-exact (`upnp.rs:46-48` vs `upnp.cpp:240-245`),
  retry `2*retry` ≤12/≤4 conforme (`engine.rs:2321-2341` vs
  `upnp.cpp:404-440`), services WANIP1/2+WANPPP1
  (`upnp.rs:302-306`), machine `update_map`/`next`/codes 725/718/
  501/727 conformes (`upnp.rs:685-753`, `upnp.cpp:830-910`,
  `1481-1528`), close→DeletePortMapping (`upnp.rs:653-669`,
  `upnp.cpp:1695-1725`). **Écarts** : `upnp_lease_duration` ignoré
  (3600 codé en dur, `engine.rs:2422`), `renew_due` jamais appelé
  par l'engine (leases jamais renouvelés), `upnp_ignore_nonrouters`
  non appliqué (`upnp.cpp:521`), `NewInternalClient` vide pour les
  mappings utilisateur (`engine.rs:2670`), cap 50 devices absent.
  Voir D6–D10.
- NAT-PMP/PCP : mapping request, external address, epoch, renew,
  delete — verdict : **déviation** — preuves : codec NAT-PMP/PCP
  fidèle (`natpmp.rs` — wire 12B/60B, parse aligné
  `natpmp.cpp:603-770`). **Écarts** : upstream démarre en PCP v2
  (`natpmp.cpp:140`, `version_pcp`) puis fallback sur
  `pcp_unsupp_version` ; l'engine n'envoie que du NAT-PMP v0
  (`build_pcp_map_request` mort, `engine.rs:2149-2173`). Pas de
  retransmission 250ms·retry ≤9 puis backoff 2h
  (`natpmp.cpp:568-602`) — au contraire `natpmp_renew` renvoie chaque
  tick tant que la réponse n'arrive pas (`engine.rs:2725-2744`,
  `m.3 = Instant::now()` initial). Renouvellement à `lifetime/2` au
  lieu de `lifetime*3/4` (`natpmp.cpp:814`, `engine.rs:2250`). Erreur
  → `expires = +2h` non porté (`natpmp.cpp:822`). Pas de delete au
  shutdown (`natpmp.cpp:903-925` `close_impl`). Voir D11–D15.
- External IP election : consensus trackers/DHT/UPnP/NAT-PMP →
  external_ip_alert — verdict : **déviation** — preuves :
  `IpVoter::cast_vote` fidèle (bloom voters, `maybe_rotate`,
  seuils 25/50/5min, `ip_voter.rs` vs `src/ip_voter.cpp` — à
  vérifier en détail dans A26-DHT ? ici conforme en surface) ;
  votes DHT/tracker/peer/router branchés
  (`session.rs:6678,9007,9815`, `engine.rs:2203,2464`).
  **Écarts** : upstream vote **par listen socket**
  (`sock->external_address`, `session_impl.cpp:7348-7374`) ;
  Rust a un `ip_voter` global à la session et le champ
  `ListenSocket.external_address` est mort (`listen.rs:50`). À un
  changement d'IP élue upstream appelle `t->new_external_ip()` pour
  chaque torrent **et** `m_dht->update_node_id(sock)` ; Rust ne fait
  ni l'un ni l'autre (`vote_external` poste juste l'alerte,
  `session.rs:6020-6031`). Voir D16–D17.
- portmap alerts : mapping add/error/log transport+protocol —
  verdict : **conforme** — preuves : `Alert::Portmap`
  (mapping/transport/protocol/ext_port/local_addr,
  `session.rs:5766-5772`), `Alert::PortmapError`
  (`session.rs:5776-5789`), `Alert::PortmapLog`
  (`session.rs:5791-5798`) — champs alignés sur
  `alert_types.hpp:1592-1685` ; `portmap_transport` 0=natpmp
  1=upnp et `portmap_protocol` 1=tcp 2=udp conformes à
  `portmap.hpp:21-34`. Nuance : `local_addr` reçoit le modèle UPnP
  ou l'IP gateway au lieu de l'adresse de la socket d'écoute
  (`engine.rs:2253,2493-2508`) — voir D18 (cosmétique).
- Mapping per listen port : tcp+udp, proto field correct — verdict :
  **conforme (partiel)** — `add_port_mapping` crée un handle par
  (socket d'écoute × transport actif) (`session.rs:5661-5700`),
  les mappings initiaux couvrent TCP+UDP (`engine.rs:2159-2162`,
  `2430-2438`). Nuance : l'engine mappe un seul `local_port` global
  (pas par socket — upstream mappe `s.sock->local_endpoint()` par
  listen socket, `session_impl.cpp:2387-2409`). Voir D19.

## Déviations

### LSD

- `crates/bitdaemon-net/src/lsd.rs:97` — `process_packet` ignore
  `from` (`_from`) : upstream filtre `match_addr_mask(from.address(),
  m_listen_address, m_netmask)` (`src/lsd.cpp:221-229`) — les
  annonces hors sous-réseau local sont acceptées. Sévérité :
  filtrage/sécurité (accepte des pairs hors-LAN par LSD).
  - **sketch** : ajouter paramètres `listen_addr: IpAddr,
    netmask: IpAddr` à `process_packet` ; early-return `None` quand
    `!enum_net::match_addr_mask(&from.ip(), &listen_addr, &netmask)`
    (`bitdaemon-net/src/enum_net.rs` existe déjà — utilisé par
    `listen.rs:109`). Le session-side pioche addr/netmask du socket
    d'écoute correspondant (`ListenSocket.netmask`). Test :
    datagramme BT-SEARCH valide depuis une IP hors masque → None ;
    dans le masque → `Some`.
- `crates/bitdaemon-session/src/session.rs:6763-6789`
  (`lsd_announce_one`) + `4721-4752` (`force_lsd_announce`) — un
  seul datagramme par annonce ; upstream `announce_impl` renvoie à
  2 s puis 4 s (3 envois, `src/lsd.cpp:130-184`). Wire-visible :
  robustesse LSD sur LAN à perte.
  - **sketch** : `SessionOut::UdpPacket` étant fire-and-forget,
    modéliser le resend dans la session : `lsd_announce_one`
    programme `(ih, retry=1)` dans une petite file
    `VecDeque<(Sha1Hash, Instant)>` ; à chaque `lsd_tick`, les
    entrées `due` repartent en `render_lsd_packet` avec
    `retry<3` (délais `2*retry` s — `lsd.cpp:181`). Test :
    fake-clock, compter 3 `UdpPacket` vers 6771 à t, t+2s, t+4s.
- `crates/bitdaemon-session/src/session.rs:6763` — l'annonce n'émet
  que le hash-clé du torrent ; upstream itère
  `info_hashes().for_each` → annonce v1 **et** v2-tronqué pour les
  hybrides (`src/torrent.cpp:2963-2966`). Wire-visible.
  - **sketch** : dans `lsd_announce_one`, itérer
    `t.info_hashes` — pour chaque version présente, envoyer
    `to_hex(hash[..20])` (v2 tronqué à 20 B comme
    `sha1_hash` upstream). Test : torrent hybride → 2 datagrammes
    (v1 hex 40 chars, v2 tronqué 40 chars) par famille.
- `crates/bitdaemon-session/src/session.rs:6686-6715` (`on_udp`
  LSD) — deux jambes manquantes vs `session_impl::on_lsd_peer`
  (`src/session_impl.cpp:5868-5898`) : (a) pas de gate
  `t.is_i2p() && !allow_i2p_mixed` (seul `t.private` est testé) ;
  (b) pas de `do_connect_boost()` après `add_swarm_peer` — le pair
  LSD attend la cadence de dial normale au lieu de l'appel
  immédiat. Sévérité : comportement (a) / timing (b).
  - **sketch** : dans la boucle `for ih_raw`, ajouter
    `|| (t.ti.is_some_and(|ti| ti.is_i2p()) && !self.settings.allow_i2p_mixed)`
    au `continue` ; après `lsd_found.push(ih)` appeler
    `self.do_connect_boost(&ih, out)` (existe déjà,
    `session.rs:1423`). Test : announce LSD sur torrent i2p sans
    `allow_i2p_mixed` → pas de pair ; torrent normal →
    `SessionOut::Dial` émis dans la foulée.
- `crates/bitdaemon-session/src/session.rs:4743,6781` —
  `render_lsd_packet(self.listen_port, …)` utilise le port bindé ;
  upstream annonce `listen_port()` = `tcp_external_port()` (port
  NAT-mappé si présent, `session_impl.cpp:5723-5732`) et
  `ssl_listen_port()` pour les torrents SSL
  (`torrent.cpp:2953-2957`). Wire-visible quand un mapping existe.
  - **sketch** : passer à `render_lsd_packet` le port issu de
    `listen_sockets[].tcp_external_port()` (premier socket
    `accept_incoming`, cf. `listen.rs:130`) — pour les torrents SSL
    (`t.is_ssl_torrent`), la première socket `ssl`. Test : socket
    avec `tcp_port_mapping=[6000,0]` → `Port: 6000` dans le
    datagramme.

### UPnP

- `crates/bitdaemon/src/engine.rs:2422` — `UpnpDriver::new(dev,
  3600, …)` : `settings.upnp_lease_duration` ignoré (upstream lit
  `settings_pack::upnp_lease_duration` à chaque `update_map`,
  `src/upnp.cpp:769-774,813`). Sévérité : comportement.
  - **sketch** : `let lease =
    u32::try_from(slock(&self.session).settings.upnp_lease_duration
    .as_secs()).unwrap_or(u32::MAX)` puis `UpnpDriver::new(dev,
    lease, …)`. Même fix pour `description` : envoyer
    `settings.user_agent` sauf `anonymous_mode` → `""`
    (`upnp.cpp:815-817`).
- `crates/bitdaemon/src/engine.rs` — `UpnpDriver::renew_due` n'est
  appelé nulle part (seul le test `test_upnp.rs:269` l'exerce) :
  upstream `on_expire` ré-émet `AddPortMapping` à `expires`
  (`upnp.cpp:1621-1665`, armé à `lease*3/4` en
  `on_upnp_map_response`). Un mapping UPnP expire côté routeur sans
  renouvellement. Sévérité : état/wire.
  - **sketch** : dans le `tick` de l'engine (à côté de
    `natpmp_renew`, `engine.rs:827`) boucler `for dev` × `for
    mapping i` → `d.renew_due(i, now_secs)` → `drive_upnp_outs`.
    `renew_due` existe déjà (`upnp.rs:923-933`). Test : driver avec
    `expires` passé → `UpnpOut::Post{Add}` émis au tick.
- `crates/bitdaemon/src/engine.rs:761-766` — les réponses SSDP ne
  subissent pas `upnp_ignore_nonrouters` : upstream droppe quand
  `!match_addr_mask(m_listen_address, from, m_netmask)`
  (`src/upnp.cpp:521-533`). Le setting existe
  (`settings.rs:143`, défaut `false`) mais n'est jamais lu.
  - **sketch** : avant `parse_ssdp_location`, si
    `settings.upnp_ignore_nonrouters` → `continue` quand
    `!match_addr_mask(listen_addr, from.ip(), netmask)` du socket
    d'écoute. Test : setting true + reply d'IP hors-masque → pas de
    `spawn_ssdp_fetch`.
- `crates/bitdaemon/src/engine.rs:2666-2671` — `add_mapping(…,
  String::new())` : `NewInternalClient` vide pour les mappings
  utilisateur ; upstream envoie l'adresse locale de la connexion de
  contrôle (`upnp.cpp:794-797` — `local_endpoint` du socket).
  Wire-visible (routeurs stricts peuvent refuser).
  - **sketch** : passer `d.dev.local_addr` (capturé par le probe de
    `spawn_ssdp_fetch`, `engine.rs:2363-2365`) :
    `self.upnp_devices[dev].add_mapping(proto_up, external_port,
    local_port, d.dev.local_addr…)` — lire `local_addr` depuis le
    device avant l'appel. Test : `AddPortMapping` contient
    `NewInternalClient` non vide.
- `crates/bitdaemon-net/src/upnp.rs:653-669` (`close`) — upstream
  saute les devices `control_url.empty()`
  (`upnp.cpp:1712`) et libère le slot global
  (`m_mappings[i].protocol = none`, `upnp.cpp:1721`). Rust émet le
  `Delete` même sans control URL (POST vers `""`).
  - **sketch** : dans `close()`, `if self.dev.control_url.is_empty()
    { continue/clear sans emit }`. Test : device sans
    `control_url` → aucun `Post` à la fermeture.
- `crates/bitdaemon/src/engine.rs:764` — `ssdp_seen` (HashSet)
  non borné ; upstream cap à 50 rootdevices
  (`upnp.cpp:655-664`). Mineur (flood SSDP → croissance mémoire).
  - **sketch** : `if self.ssdp_seen.len() >= 50 { skip }`.

### NAT-PMP / PCP

- `crates/bitdaemon/src/engine.rs:2149-2173` — l'engine n'envoie
  que NAT-PMP v0 ; upstream `natpmp::start` pose
  `m_version = version_pcp` et n'émet l'opcode-0 `get_ip` qu'en
  NAT-PMP (`natpmp.cpp:140,211-212`), le fallback vers NAT-PMP se
  faisant sur `pcp_unsupp_version` (`natpmp.cpp:686-700`).
  `build_pcp_map_request`/`parse_response` PCP sont du code mort.
  Wire-visible : un routeur PCP-only ne mappera jamais ; le premier
  paquet attendu sur le fil est un PCP MAP v2.
  - **sketch** : ajouter `version: ProtocolVersion` à
    `NatpmpState`, init `Pcp` ; envoyer
    `build_pcp_map_request(proto, local, ext, ttl, local_addr,
    None, nonce)` (générer `nonce` par mapping,
    `crypto_random_bytes` upstream `natpmp.cpp:363`) ; sur
    `UnsupportedVersion` → `version = NatPmp` + renvoyer en v0 +
    `build_ip_request` (`natpmp.cpp:692-697`). Matcher les réponses
    PCP par nonce (`parse_response` le retourne déjà). Test :
    fixture PCP reply `result=1` → les requêtes suivantes sont des
    datagrammes v0 de 12 B.
- `crates/bitdaemon/src/engine.rs:2725-2744` (`natpmp_renew`) —
  pas de retransmission bornée : les mappings ont `expires =
  Instant::now()` à la création (`engine.rs:2160-2161,2646`) donc
  sont « dus » chaque tick → renvoi 1/s à l'infini si la gateway ne
  répond pas. Upstream : retry à `250ms * retry_count`, abandon à
  9, puis `expires = now + 2h` (`natpmp.cpp:568-572,591-600`).
  Wire-visible (spam) + état.
  - **sketch** : `NatpmpState::mappings` → struct avec
    `retry_count: u8`, `outstanding: bool`, `next_send: Instant` ;
    au send poser `next_send = now + 250ms*retry` ; au tick,
    `retry_count >= 9` → `expires = now + 2h` (ne plus renvoyer) ;
    sur réponse `outstanding = false`. Réf.
    `natpmp.cpp:576-601`. Test : sans réponse, 9 envois à ~250,
    500, 750…ms puis silence 2h.
- `crates/bitdaemon/src/engine.rs:2250` — renew à `lifetime/2` ;
  upstream `m->expires = now + seconds(lifetime * 3 / 4)`
  (`natpmp.cpp:814`). Cadence de renouvellement plus précoce que
  nécessaire (mineur, wire-visible).
  - **sketch** : `m.3 = now + Duration::from_secs(u64::from(lifetime) * 3 / 4)` + corriger le commentaire.
- `crates/bitdaemon/src/engine.rs:2246-2277` — sur `result != 0`
  upstream pose `m->expires = now + hours(2)` avant le callback
  d'erreur (`natpmp.cpp:822-831`) ; Rust ne touche pas `m.3` → la
  requête part dès le prochain tick (tempête d'erreurs).
  - **sketch** : dans la branche erreur, `m.3 = now +
    Duration::from_secs(2*3600)` (le même backoff que le timeout
    des 9 retries). Test : reply `result=4` → aucune requête
    pendant 2h.
- `crates/bitdaemon/src/engine.rs` — pas de `stop_natpmp`/`close` :
  `close_impl` upstream envoie ttl=0 pour chaque mapping au stop
  (`natpmp.cpp:903-925` ; `session_impl::stop_natpmp` →
  `mapper->close()`, `session_impl.cpp:7191-7199`). Les mappings
  restent sur le routeur à l'arrêt de session.
  - **sketch** : `pub async fn stop_natpmp(&mut self)` — pour
    chaque `mappings` envoyer `build_map_request(proto, local, 0,
    0)` puis `self.natpmp = None` ; appeler depuis le chemin de
    shutdown de l'engine + quand `services.natpmp` passe à false.
    Test : après `stop_natpmp`, deux datagrammes ttl=0 émis.
- `crates/bitdaemon/src/engine.rs:2202-2208` — le vote
  `SOURCE_ROUTER` part dès la réponse opcode-0 ; upstream ne vote
  qu'au succès d'un mapping (`on_port_mapping` →
  `set_external_address(sock, external_ip, source_router)`,
  `session_impl.cpp:5940-5948` + `natpmp.cpp:828-831` où
  `ext_ip = m_external_ip`). Une gateway répondant au probe IP mais
  refusant les mappings nourrit quand même le voteur. Mineur.
  - **sketch** : stocker `external_ip: Option<IpAddr>` dans
    `NatpmpState` au `PublicIp`, voter dans la branche `Map`
    succès avec `result==0` (PCP : `external_address` de la
    réponse). Test : opcode-0 sans map success → pas d'alerte
    `ExternalIp`.

### Election IP externe

- `crates/bitdaemon-session/src/session.rs:559,6020` — voteur
  global unique `self.ip_voter` ; upstream vote **par listen
  socket** (`sock->external_address`, `session_impl.cpp:7336-7351`)
  — `ListenSocket.external_address` (`listen.rs:50`) existe mais
  n'est jamais écrit. Sur une machine multi-homée/multi-famille les
  votes de sockets différentes se mélangent, et
  `external_address()` agrégé (`session_impl.cpp:7210-7227`)
  n'a pas d'équivalent.
  - **sketch** : router `vote_external(ip, source, source_ip,
    ls: Option<ListenSocketHandle>)` vers
    `ListenSocket.external_address.cast_vote` ; pour les votes sans
    socket identifiable (tracker HTTP non lié), garder un fallback
    sur le premier socket `accept_incoming` de la famille
    (comportement `session_impl::set_external_address(local_endpoint)`)
    — `session_impl.cpp:7336-7345`. `external_address()` agrégé =
    par socket, comme `external_ip` `[global|local]×[v4|v6]`.
    Test : votes conflictuels v4 vs v6 → deux élus distincts par
    famille.
- `crates/bitdaemon-session/src/session.rs:6020-6031` — quand
  l'IP élue change, upstream (a) appelle `t->new_external_ip()`
  sur chaque torrent (les peers recalculent leur flag
  `on_parole`/`connect_candidate`, cf. `peer_list`) et (b)
  `m_dht->update_node_id(sock)` — BEP-42 impose un node-id dérivé de
  la nouvelle IP publique (`session_impl.cpp:7357-7374`). Rust ne
  fait ni l'un ni l'autre ; `DhtTracker::update_node_id` existe
  (`dht_tracker.rs:185`) mais n'est jamais appelé.
  - **sketch** : dans `vote_external`, après l'alerte :
    `for t in self.torrents.values_mut() { t.on_external_ip_changed(&mut out) }`
    (porter `torrent.cpp::new_external_ip` — vérifier
    `src/torrent.cpp` pour la sémantique exacte : itère les peers
    et met à jour `peer_info::external_ip`/`connect_candidate`) et
    `self.dht.update_node_id(v6, addr)` pour la famille de `addr`.
    Test : flip de l'élu → `dht_node.id` re-dérivé + effects torrent.

### Alerts / divers

- `crates/bitdaemon/src/engine.rs:939-943` — `Alert::LsdError` est
  postée à chaque `send_to` en échec ; upstream ne poste
  `lsd_error_alert` qu'à l'échec de `lsd::start` (bind/join),
  `session_impl.cpp:7090-7094` — un échec d'envoi upstream ne fait
  que logger (`lsd.cpp:160-172`). Cosmétique (alerte en trop).
  - **sketch** : ne poster `LsdError` que sur l'échec de
    `set_lsd_datagram`/bind ; garder `UdpError` sur send. Mineur.
- `crates/bitdaemon/src/engine.rs:2253,2493-2508` —
  `portmap_error_alert`/`portmap_log_alert` reçoivent le *modèle*
  UPnP ou l'IP gateway comme `local_addr` ; upstream envoie
  `listen_socket->local_endpoint.address()`
  (`session_impl.cpp:5936,5963,7295-7300`). Cosmétique (payload
  d'alerte).
  - **sketch** : passer l'adresse de la socket d'écoute concernée
    (depuis le `PortmapTarget.local_addr` de la session).

## Fichiers du fix

- `crates/bitdaemon-net/src/lsd.rs` — filtre netmask entrant.
- `crates/bitdaemon-session/src/session.rs` — resend LSD, announce
  multi-hash (v1+v2), port externe, i2p/check-inbound +
  `do_connect_boost`, voteur par socket + `new_external_ip` +
  `update_node_id`.
- `crates/bitdaemon-session/src/torrent.rs` — port
  `torrent::new_external_ip` (si pas déjà présent sous un autre nom).
- `crates/bitdaemon-session/src/listen.rs` — routing du voteur par
  socket.
- `crates/bitdaemon-net/src/upnp.rs` — `close` skip `control_url`
  vide (léger).
- `crates/bitdaemon/src/engine.rs` — lease settings, `renew_due`
  tick, `upnp_ignore_nonrouters`, `NewInternalClient`, cap devices,
  NAT-PMP : PCP start+fallback, retransmit/backoff, ¾ lease, 2h
  error backoff, `stop_natpmp`, vote `source_router` différé,
  `LsdError` uniquement au bind, payloads d'alertes portmap.
- `crates/bitdaemon-cli/src/main.rs` — passer
  `settings.natpmp_lease_duration` au lieu de 3600.
- `docs/audits/changelog/A25.md`, `docs/PORTING.md` (rows lsd/upnp/
  natpmp/ip_voter).

## Non-bugs tranchés

- **`anonymous_mode` coupe LSD/UPnP/NAT-PMP** — upstream ne gate
  aucun des trois sur `anonymous_mode` (grep `anonymous_mode` :
  `lsd.cpp`/`natpmp.cpp`/`upnp.cpp` ne le lisent que pour le
  `user_agent` SOAP, `upnp.cpp:815`). Rust coupe les trois
  (`session.rs:6730,4724`, `engine.rs:2151,2288`). Choix délibéré et
  correct pour les lanes anonymes OnionBit (une annonce BT-SEARCH ou
  un mapping routeur deanonymise) — écart volontaire, garder.
- **Cookie LSD 32 bits** — upstream borne à 31 bits
  (`(random(0x7fffffff) ^ this) & 0x7fffffff`, `lsd.cpp:59`) ;
  Rust `rand::random::<u32>()`. Le cookie ne sert qu'à écarter nos
  propres annonces rebouclées — domaine plus large, même
  comportement. Garder.
- **Un seul socket UDP partagé** — upstream lie des sockets
  dédiés (LSD 6771, SSDP multicast+unicast, NAT-PMP bind local) ;
  ADR-0008 impose les transports injectés — le demux par
  `(from == gateway)`/`port 6771` est l'équivalent structurel
  assumé.
- **M-SEARCH unique** — upstream émet sur le socket multicast *et*
  le socket unicast-broadcast (`upnp.cpp:252-256`) ; les deux
  envoient le même datagramme au même endpoint. Avec le transport
  injecté unique, un seul envoi est l'équivalent fonctionnel.
- **`alert` gating** — `alerts::post` applique le masque en interne
  (`alerts.rs:1778-1784`) — les `should_post` upstream sont couverts.
- **Epoch NAT-PMP** — upstream lit le champ `time` puis
  `TORRENT_UNUSED` (`natpmp.cpp:720`) ; l'absence de gestion epoch
  est fidèle.
- **Nuance DHT vote attribution** — `drain_ip_votes` perd l'adresse
  du votant, mais les votes sont drainés à chaque datagramme et
  `from` est bien l'émetteur du vote ; l'attribution reste correcte
  dans le flux normal (un vote résiduel d'un datagramme précédent
  non drainé serait attribué au `from` courant — fenêtre
  théorique seulement).

## Résultat

Mode `apply` exécuté dans le worktree dédié
(`audit/25-nat-traversal`). Commit
`fix(net,session): nat-traversal parity — PCP-first + LSD lifecycle`.

### Déviations corrigées

**NAT-PMP/PCP** (`engine.rs`, `natpmp.rs`)
- PCP-first au `start_natpmp` (requêtes MAP 60 B, nonce par slot) ;
  fallback NAT-PMP sur `pcp_unsupp_version` **uniquement** en socket
  non-v6, avec resend des requêtes en vol + requête IP opcode 0
  différée à ce moment.
- Slot `NatpmpMapping` complet : `act` (add/delete), `outstanding`,
  `retry_count`, deadline, nonce, `external_address`. Retransmission
  linéaire `250 ms × retry` abandonnée à 9 ; renouvellement à
  `lifetime × 3/4` ; park 2 h sur erreur ; matching des réponses par
  proto + port privé + nonce + outstanding.
- `stop_natpmp` envoie les datagrammes ttl-0 ; `stop_natpmp` et
  `stop_upnp` déclenchés au tick quand le service est désactivé.
- Le vote `source_router` est différé au succès du mapping
  (`external_address` PCP ou `m_external_ip` NAT-PMP), plus émis à la
  réception de la réponse IP.
- `natpmp_lease_duration` honoré (ttl=0 → setting).

**UPnP** (`engine.rs`, `upnp.rs`)
- `renew_due` balayé chaque tick (re-add à `expires`).
- `upnp_lease_duration` passé au `UpnpDriver`.
- `upnp_ignore_nonrouters` filtre les réponses SSDP hors sous-réseau
  (approximation /24 v4, /64 v6 depuis l'adresse liée du socket
  injecté — pas d'énumération d'interfaces sous ADR-0008).
- `NewInternalClient` rempli pour les mappings utilisateur.
- `close()` ignore les devices sans `control_url` résolu.

**LSD** (`lsd.rs`, `session.rs`)
- Filtre `match_addr_mask` entrant contre `(addr, netmask)` du socket
  d'écoute de même famille.
- Resend à +2 s puis +4 s (`LsdResend`, drainé par le tick 1 s).
- Annonce de tous les hashes présents (`info_hashes().for_each` —
  v1 + v2 tronqué pour hybrides).
- `Port:` = `tcp_external_port()` du socket `accept_incoming` (socket
  SSL pour torrents SSL), plus le port bindé.
- `do_connect_boost` sur peer découvert ; gate i2p/private sur
  l'inbound.

**External IP** (`session.rs`)
- `vote_external` cible le voter par socket
  (`ListenSocket.external_address`, même famille, `accept_incoming`
  préféré ; voter global en fallback) + agrégat `external_ip_agg`
  `[local|global] × [v4|v6]`.
- Changement d'élu → `clear_peer_prio` sur tous les torrents +
  `dht.update_node_id` (BEP-42) en plus de `external_ip_alert`.

### Tests

- `test_engine::natpmp_map_and_ip_vote` — réécrit pour le cycle
  PCP → `pcp_unsupp_version` → resend NAT-PMP + ip query → map
  success → vote différé.
- `test_engine::upnp_map_cycle` — assertion migrée vers
  `external_ip_agg` (voter par socket).
- `test_socks.rs` — gate netmask LSD (in-subnet accepté, hors-subnet
  rejeté).
- `test_session::dht_ip_key_votes_external_ip` — assertion via
  l'agrégat `external_ip_agg`.
- `test_session::lsd_emit_and_receive`,
  `anonymous_mode_suppresses_lsd` — non régressés.

### Gates

`cargo fmt --all`, `cargo clippy --workspace --all-targets --
-D warnings`, `cargo test --workspace`, `cargo doc --workspace
--no-deps` — tous verts.
