# Audit A24 — rapport d'analyse

Périmètre : `socks5_stream`, `http_stream` (aux), `instantiate_connection`,
`udp_socket` (partie SOCKS5-UDP), `proxy_settings` — baseline `e7381c7bc`.

Rust : `crates/bitdaemon-net/src/{socks,socks_udp,http_stream,proxy,instantiate}.rs`,
glue dans `crates/bitdaemon/src/engine.rs` et `crates/bitdaemon-session`.

## Verdicts checklist

- SOCKS5 : méthodes auth (no-auth, user/pass), commandes connect/UDP associate, erreurs → mapping — verdict : **déviation** — preuve : framing et mapping des codes de réponse conformes (`socks.rs:117-263` vs `socks5_stream.hpp:177-480` — greeting `[5,1,0]`/`[5,2,0,2]`, sub-nég `[1,ulen,u,plen,p]`, atyp 1/3/4, mapping reply 2→no_permission … 7→command_not_supported, `host_len < 3` → general_failure, SOCKS4 reply 90/91/92/93) — **mais** credentials envoyés même pour `proxy_type == socks5`/`socks4` quand `username` est rempli (D1), et ATYP inconnu dans la réponse → `UnsupportedVersion` au lieu de `address_family_not_supported` (D9).
- HTTP CONNECT : auth basic, tunnel keep-alive, host header — verdict : **déviation** — preuve : requête `CONNECT host:port HTTP/1.0` + `Host:` + `Proxy-Authorization: Basic` identique octet pour octet (`http_stream.rs:64-98` vs `http_stream.hpp:96-127`), scan byte-wise `\n\n`/`\r\n\r\n` verbatim, parse du code par préfixe décimal comme `parse_decimal`. `m_no_connect` (GET absolute-form sans CONNECT) reproduit dans `engine.rs:4656-4664`. **Déviation** : `Proxy-Authorization` émis pour `proxy_type == http` (non-pw) dès que `username` est rempli — upstream ne pose le header que pour `http_pw` (D1, `http_connection.cpp:170`, `engine.rs:1786`).
- UDP associate : datagrammes encapsulés (RSV/FRAG/ATYP), trackers UDP via proxy — verdict : **déviation** — preuve : wrap/unwrap verbatim (`socks_udp.rs:14-108` vs `udp_socket.cpp:327-411`, FRAG!=0 rejeté, atyp≠1/4 parsé comme hostname). **Déviations** : (a) BND domaine → `BND.PORT` perdu, relais = port TCP du proxy (D2) ; (b) `recv_from` accepte tout expéditeur sans vérifier `p.from == relais` (D4) ; (c) pas de retry/backoff/timeout 10 s ni re-associate sur fermeture de la connexion de contrôle (D5) ; (d) `socks5_udp_send_local_ep` parsé mais jamais appliqué (D6) ; (e) pas de `send_hostname` — les trackers UDP à hostname sont résolus localement (D7) ; (f) `BND.ADDR == 0.0.0.0` substitué par l'IP du proxy là où upstream l'utilise verbatim (D3, amélioration probablement à garder).
- Proxy par usage : `proxy_peer_connections`/`proxy_tracker_connections`/usage DHT — verdict : **déviation (partielle)** — preuve : `instantiate.rs:73-83` gate correctement par `ConnKind` (peer→`proxy_peer_connections`, tracker→`proxy_tracker_connections` — `instantiate_connection.cpp:60-63`). Il n'existe pas de flag `proxy_dht` upstream : le trafic UDP « autre » (DHT) est proxifié dès qu'un proxy socks5 est actif (`udp_socket.cpp:265-271`, `!(flags & (tracker|peer))` → `use_proxy`). Rust proxifie la lane datagramme entière via `proxies.dht` (`engine.rs:641-689`) : les configs mixtes (peer proxifié / tracker non proxifié, ou l'inverse) ne sont pas respectées par datagramme — upstream décide **par envoi** via `flags` (D10).
- Hostname vs résolution via proxy, `resolve_hostnames` flag — verdict : **conforme** — preuve : `proxy_hostnames` (défaut `true` comme `settings_pack.cpp:200`) → `SocksTarget::Name`/`connect_dest` (`proxy.rs:212-262`), `name_via_proxy` dans `web_seed_dial` (`engine.rs:4651-4653` vs `torrent.cpp:7254-7281`), hostname-peers de tracker droppés quand hostnames+peer proxifiés (`engine.rs:1374-1388` vs `torrent.cpp:3951-3958`). `send_host_in_connect`/`proxy_send_host_in_connect` implémenté (`proxy.rs:66-70`, `engine.rs:4675` vs `http_connection.cpp:575-584`).
- Instantiate : choix socket_type selon proxy + ssl — verdict : **conforme** — preuve : `instantiate.rs:53-97` reproduit la table dans le même ordre (uTP d'abord → i2p (SSL refusé) → none/non-proxifié → http → socks* ; `instantiate_connection.cpp:31-128`). `None` remplace le `TORRENT_ASSERT_FAIL`+throw — sémantique équivalente.

## Déviations

### D1 — Credentials envoyés pour les types non-`*_pw` (wire-visible)

`proxy.rs:126-136` (`SocksConnector::from_settings`) passe `username`/`password`
quel que soit `proxy_type` ; `engine.rs:3222-3227` et `engine.rs:4669-4674`
construisent `HttpConnectConnector` avec les credentials sans condition ;
`engine.rs:655-662` les passe à `Socks5UdpSocket::associate` ;
`engine.rs:1786` émet `Proxy-Authorization` pour `http` (non-pw).

Upstream : `instantiate_connection.cpp` n'appelle `set_username` que pour
`socks5_pw` (l.105-107) et `http_pw` (l.86-87, l.78-79) ; le handshake UDP
gate explicitement `username.empty() || type == socks5`
(`udp_socket.cpp:684-687`) ; `Proxy-Authorization` n'est émis que pour
`http_pw` (`http_connection.cpp:170-172`). Conséquence wire : avec
`proxy_type=socks5` + `proxy_username` rempli, nous annonçons `[5,2,0,2]`
et faisons la sub-nég user/pass là où upstream envoie `[5,1,0]` ; pareil
pour SOCKS4 (userid non vide) et HTTP (header Basic en trop).

- **sketch** :
  - `proxy.rs` `from_settings` : passer `p.username.clone()`/`p.password.clone()`
    seulement si `p.proxy_type == ProxyType::Socks5Password`, sinon
    `String::new()` (couvre socks4 aussi — upstream ne set jamais de user pour socks4).
  - `socks_udp.rs` `associate` : ajouter un param `authenticate: bool` (ou le
    `ProxyType`) ; `engine.rs:655` passe `p.proxy_type == ProxyType::Socks5Password`.
  - `engine.rs:3222`/`4669` : credentials seulement si `proxy_type == HttpPassword`.
  - `engine.rs:1786` : `using_http_proxy` → exiger `ProxyType::HttpPassword`
    pour `proxy_authorization`.
  - Réf. upstream : `instantiate_connection.cpp:78-108`, `udp_socket.cpp:684`,
    `http_connection.cpp:170`. Test de régression : scripted SOCKS5 server
    (pattern `proxy.rs:412-457`) qui attend `[5,1,0]` avec
    `proxy_type=Socks5` + username rempli.

### D2 — UDP ASSOCIATE : BND domaine → `BND.PORT` perdu (wire-visible)

`socks.rs:246-258` retourne `None` pour ATYP 3 → `socks_udp.rs:157-160`
tombe sur `relay = proxy_addr` (adresse **et port TCP** du proxy) : les
datagrammes wrappés partent vers le port de contrôle → relay cassé.
Upstream `read_domainname` (`udp_socket.cpp:981-991`) utilise
`m_proxy_addr.address()` + **BND.PORT** lu dans la réponse.

- **sketch** : changer le retour de `socks_connect_bound` en
  `Result<Socks5Bound, SocksError>` avec `Socks5Bound::Addr(SocketAddr) |
  Socks5Bound::Domain(u16)` (le port est toujours présent) ; dans
  `associate`, `relay = SocketAddr::new(proxy_addr.ip(), port)` pour
  `Domain(port)`. Réf. `udp_socket.cpp:991-993`. Test : réponse ATYP 3 +
  port P → `local_addr()`/`send_to` cible `(proxy_ip, P)`.
  À noter : upstream n'exige pas `len >= 3` dans ce chemin (le check
  `host_len < 3` n'existe que dans `socks5_stream::connect2`) — garder le
  check Rust est acceptable, à trancher.

### D3 — `BND.ADDR == 0.0.0.0` : substitution de l'IP proxy (wire-visible, probablement à garder)

`socks_udp.rs:158-159` remplace un BND non-spécifié par `proxy_addr.ip()` ;
upstream `read_bindaddr` (`udp_socket.cpp:960-966`) prend `BND.ADDR`
verbatim → envoie vers `0.0.0.0:port` (ce qui échoue/part en local sur la
plupart des OS — leg latent upstream ; RFC 1928 laisse le client choisir).

- **sketch** : recommandé de **garder** la substitution Rust et la
  documenter comme écart délibéré dans `PORTING.md` + commentaire de code
  (déjà présent) ; si la fidélité stricte est préférée, supprimer la
  branche `Some(b) => SocketAddr::new(proxy_addr.ip(), b.port())`.
  Test : scripted associate retournant `0.0.0.0:P` → vérifier que
  `send_to` vise `(proxy_ip, P)`.

### D4 — `recv_from` ne filtre pas la source (wire-visible / spoofing)

`socks_udp.rs:192` ignore `_from` et unwrappe tout datagramme reçu.
Upstream : `if (p.from != m_socks5_connection->target()) continue;`
(`udp_socket.cpp:216-217`) — les paquets ne venant pas du relais sont
droppés. Un injecteur local peut forgé des datagrammes wrappés qui seront
acceptés comme venant de pairs/trackers.

- **sketch** : dans `recv_from`, `let (n, from) = …`; `if from !=
  self.relay { continue; }`. Réf. `udp_socket.cpp:216`. Test : datagramme
  wrappé envoyé depuis une autre socket → ignoré.

### D5 — Pas de retry/backoff ni détection de hang-up (état)

Upstream `socks5` : timeout connect 10 s (`udp_socket.cpp:645-649`),
`retry_connection()` exponentiel `min(120, f²/2)+5` s (l.1037-1041),
`socks5_alert` à chaque échec, et `hung_up` — un read permanent sur la
connexion de contrôle déclenche une **re-association** quand le proxy la
ferme (l.1023-1034). Rust `associate` tente une fois ; `_control` est
gardé vivant mais jamais lu → si le proxy droppe le TCP, le relay UDP est
mort silencieusement (les `send_to` réussissent encore au niveau socket).

- **sketch** : dans `Socks5UdpSocket::associate` (ou un `spawn` engine),
  lire `control` en boucle ; sur EOF/erreur → `Alert::Socks5 { op:
  "hung_up" }` + re-`associate` avec backoff `min(120, f*f/2)+5`s
  (`tokio::time::sleep`). Nécessite que `Socks5UdpSocket` expose un
  `watch()` ou que l'engine porte la boucle. Le timeout 10 s autour du
  connect : `tokio::time::timeout(10s, connector.connect(...))`.
  Réf. `udp_socket.cpp:645-649`, `1009-1049`. Test : mock qui ferme le
  contrôle → nouvelle association observée.

### D6 — `socks5_udp_send_local_ep` parsé mais ignoré (état)

`settings.rs:317-319` + `session_params.rs:549` existent ;
`Socks5UdpSocket::associate` envoie toujours `DST=0.0.0.0:0`
(`socks_udp.rs:143-151`). Upstream envoie le local endpoint du listen
socket quand `send_local_ep` (`udp_socket.cpp:838-850`,
`settings_pack.cpp:219`, défaut `false`).

- **sketch** : param `local_ep: Option<SocketAddr>` dans `associate` ;
  `Some(ep)` → `SocksTarget::Addr(ep)` au lieu du wildcard ; l'engine
  passe `Some(listen local ep)` quand `settings.socks5_udp_send_local_ep`.
  Réf. `udp_socket.cpp:836-850`, `session_impl.cpp:5656`.

### D7 — Pas de `send_hostname` : trackers UDP à hostname résolus localement (privacy / wire)

`socks5_wrap_hostname` (`socks_udp.rs:37`) est du dead code hors tests ;
`DatagramSocket` n'a pas de send par nom → `udp_announce` reçoit un
`SocketAddr` résolu localement (`udp_tracker.rs:280-306`). Upstream
`udp_socket::send_hostname` wrappe en ATYP 3 sans résolution locale
(`udp_socket.cpp:253-285`) — avec proxy actif + `proxy_hostnames`, le nom
du tracker ne sort jamais en DNS clair (fuite DNS + échec si le DNS local
ne résout pas).

- **sketch** : ajouter
  `fn send_hostname<'a>(&'a self, host: &'a str, port: u16, buf: &'a [u8])
   -> BoxFut<'a, usize>` à `DatagramSocket` (défaut : erreur
  `InvalidInput` — upstream n'autorise le hostname-send que sous proxy) ;
  `Socks5UdpSocket` l'implémente via `socks5_wrap_hostname`. Dans le
  chemin tracker UDP de l'engine : si datagram lane est socks5-proxy et
  `proxy_hostnames`, appeler `send_hostname` au lieu de résoudre.
  Réf. `udp_socket.cpp:253-285`, `wrap(hostname,…)`:348-368.
  Test : datagramme reçu côté mock avec ATYP=3 + hostname verbatim.

### D8 — CONNECT vers endpoint non-spécifié accepté (cosmétique)

Upstream `socks_connect` refuse `m_remote_endpoint.address() == address()`
sans `dst_name` → `invalid_argument` (`socks5_stream.hpp:361-364`). Rust
`socks5` envoie la requête quand même (`SocksTarget::Addr(0.0.0.0:*)` —
ce que `associate` exploite pour UDP ASSOCIATE, légitime puisqu'upstream
utilise un chemin séparé).

- **sketch** : dans `socks5()`, si `command == Connect &&
  SocksTarget::Addr(a) && a.ip().is_unspecified()` →
  `Err(SocksError::ProxyFailure(io::Error::new(InvalidInput, "unspecified
  endpoint")))` ; `UdpAssociate` exempté. Test : connect vers `0.0.0.0:0`
  → erreur avant toute écriture.

### D9 — ATYP inconnu dans la réponse → mauvaise variante d'erreur (cosmétique)

`socks.rs:260` retourne `UnsupportedVersion` ; upstream →
`boost::asio::error::address_family_not_supported`
(`socks5_stream.hpp:455-458`).

- **sketch** : `_ => Err(SocksError::ProxyFailure(io::Error::new(
  ErrorKind::AddrNotAvailable, "address family not supported")))`.
  Ajouter une variante dédiée si on veut préserver le nom
  (`AddressFamilyNotSupported`).

### D10 — Gating par usage UDP : tout-ou-rien vs par-datagramme (wire-visible, config-dépendant)

Upstream calcule `use_proxy` **par envoi** : `(flags & peer_connection) &&
proxy_peer_connections || (flags & tracker_connection) &&
proxy_tracker_connections || !(flags & (…))` (`udp_socket.cpp:265-271`,
`303-306`) — uTP, trackers UDP et DHT peuvent être mixés. Rust active le
relais pour la lane entière si `proxies.dht` est socks5
(`engine.rs:647-651`) ; sinon tout part en clair (uTP/trackers non
proxifiés même si `proxy_peer_connections`).

- **sketch** : propager les flags `peer_connection`/`tracker_connection`
  dans `DatagramSendFlags` (bits additionnels) et un proxy-set global ;
  `Socks5UdpSocket` wrappe par datagramme selon les flags + settings —
  mais cela suppose une socket qui peut envoyer clair **et** relayé
  (deux chemins dans `send_to_flags`) ; alternative fidèle à l'ADR-0008 :
  router par socket injectée distincte par usage. Réf.
  `udp_socket.cpp:265-318`. À designer lors de l'apply — le point le plus
  structurel du rapport.

## Fichiers du fix

- `crates/bitdaemon-net/src/socks.rs` (D2, D8, D9)
- `crates/bitdaemon-net/src/socks_udp.rs` (D1-auth param, D2, D4, D5, D6, D7)
- `crates/bitdaemon-net/src/proxy.rs` (D1)
- `crates/bitdaemon-net/src/transport.rs` (D7 — méthode `send_hostname`)
- `crates/bitdaemon-net/src/udp_tracker.rs` + `crates/bitdaemon/src/engine.rs` (D1, D5, D6, D7, D10)
- `crates/bitdaemon-net/tests/test_socks.rs` (régressions)
- `docs/PORTING.md` rows `socks5_stream.cpp`/`http_stream.hpp`,
  `udp_socket.cpp`, `proxy_base.cpp`

## Non-bugs tranchés

- `version < 5` vs `version != 5` dans le handshake UDP upstream
  (`udp_socket.cpp:750` accepte 6+) — Rust exige `== 5` ; `connect2`
  upstream exige aussi `== 5`, comportement équivalent.
- `handshake4`/`connect2` UDP upstream : `return` silencieux sans retry
  ni alerte (`udp_socket.cpp:826`, `912`) — impasse upstream ; Rust
  propage l'erreur → `socks5_alert` + retry côté engine. Amélioration
  conservée.
- Réponses SOCKS5 `connect` : upstream lit toujours 10 octets puis lit le
  surplus ; Rust lit header+tail par ATYP — mêmes octets consommés.
- `SocksConnector` spawne le handshake dans un task pour satisfaire
  `Sync` du futur trait — invisible on-wire.
- `SessionProxies` (5 proxies par usage) : superset structurel de
  l'`m_proxy_settings` unique upstream — les flags par usage sont portés
  par `ConnKind`/`proxies.<purpose>` ; pas de divergence de settings
  par défaut.
- `host_len < 3` sur BND domaine (upstream : seulement dans
  `socks5_stream`, pas dans le chemin UDP) — check conservé côté Rust,
  partagé entre les deux chemins ; écart minimal documenté en D2.
