# Audit A23 — rapport d'analyse

Périmètre : `ssl_stream` / torrents SSL — fingerprint & name-check,
mutual TLS, SNI dispatch, listen ssl, alertes, crypto provider.
Upstream lu : `src/ssl.cpp`, `include/libtorrent/aux_/ssl_stream.hpp`,
`src/session_impl.cpp` (`ssl_server_name_callback_impl`, `async_accept`,
`ssl_handshake`, `update_ssl_listen`, `ssl_listen_port`,
`update_validate_https`), `src/torrent.cpp` (`init_ssl`,
`verify_peer_cert`, `set_ssl_cert`, `attach_peer`, `connect_to_peer`,
`build_tracker_request`), `src/kademlia/node.cpp` (`announce` ssl flag).
Baseline `e7381c7bc`.

## Verdicts checklist

- Fingerprint : digest cert SHA-256/SHA-1 vs info-hash v1/v2 → règles
  d'acceptation exactes — verdict : **conforme** (avec précision).
  Upstream ne compare pas un digest : `verify_peer_cert` exige
  `preverified` (chaîne vers la root du torrent) + leaf SAN DNSName ou
  dernier CN == `torrent_file->name()` ou `"*"`, `depth > 0 → true`
  (`src/torrent.cpp:1695-1854`). Rust : `cert_chain` (webpki, anchors
  du torrent) + `leaf_name_matches` (SAN DNSName `*`/name puis dernier
  CN) — `crates/bitdaemon-net/src/ssl_stream.rs:72-91,135-154,186-200`.
  Équivalent.
- Mutual TLS : verify_peer + verify_fail_if_no_peer_cert, client cert
  depuis `ssl_cert`/`ssl_key` — verdict : **déviation** (partielle).
  Obligatoire ✓ (`client_auth_mandatory() → true`,
  `ssl_stream.rs:258-260`; `with_client_cert_verifier`,
  `engine.rs:3629-3634`; `peer_ssl_client_config` présente cert/key,
  `ssl_stream.rs:404-414`). Mais upstream épingle le verify au torrent
  choisi par SNI (`set_context` réinstalle le ctx du torrent,
  `ssl.cpp:121-131` + `session_impl.cpp:450-454`) — Rust vérifie
  contre **toute** root enregistrée (`ssl_stream.rs:272-289`). Et la
  vérification du ctx à l'attach (`has_context`, `torrent.cpp:8496`)
  n'existe pas. → Déviations D2/D3.
- verify_callback : chain custom — CN/SAN ne font pas foi d'identité
  torrent upstream — verdict : **conforme** — la vérif rustls est par
  torrent (anchors embeddés + name leaf), pas de PKI globale côté
  pairs (`ssl_stream.rs:160-223`); `m_peer_ssl_ctx` verify-none +
  TLS 1.2+ (`ssl_stream.rs:25-65,421-429` vs `session_impl.cpp:642-644`).
- ssl_listen_port, accept inbound ssl, flags (`is_ssl_torrent`,
  `m_ssl`) — verdict : **déviation**.
  `ssl_listen` legacy → reopen ✓ (`session.rs:6101-6104`),
  accepteur ssl + SNI resolver ✓ (`engine.rs:3601-3747`),
  `is_ssl_torrent` lit `ssl-cert` ✓ (`torrent.rs:10726`). Mais :
  `add_torrent_params::root_certificate` ignoré (`init()`,
  `torrent.cpp:1977-1986`) — D1 ; `TorrentClientVerifier` non épinglé
  au SNI — D2 ; pas de legs `invalid_ssl_cert` à l'attach — D3 ;
  `unregister_ssl_torrent` jamais appelé — D7 ; CLI passe les mauvais
  `socket_type` kind — D8.
- Alertes : erreurs peer_ssl → mapping disconnect reason — verdict :
  **déviation**. Échec du handshake ssl inbound : upstream poste
  `peer_error_alert` `op=ssl_handshake` (`session_impl.cpp:3064-3071`),
  Rust se contente de `warn!` (`engine.rs:3692-3701`) — D4.
  `torrent_need_cert_alert` posté au mauvais moment — D5.
  `requires_ssl_connection` mappé ✓ (`close_reason.rs:173`,
  `session.rs:3901-3907`).
- crypto provider : builder_with_provider exhaustif (suite du fix
  récent) — verdict : **conforme** — `crypto_provider()` unique,
  `builder_with_provider` sur les 4 configs (`ssl_stream.rs:124-129,
  399,422,437`; `engine.rs:3620-3622`), pas d'install process-wide.

## Déviations

- `crates/bitdaemon-session/src/torrent.rs:10726,10743` +
  `crates/bitdaemon/src/engine.rs:3063-3073` — `is_ssl_torrent()` /
  `ssl_root_cert()` ne regardent que `ti.ssl_cert()` ; upstream
  `torrent::init()` : `cert = atp.root_certificate` **prioritaire**,
  sinon `ssl_cert()`, et `m_ssl_torrent = true` dès qu'un cert non
  vide est fourni même sans `ssl-cert` embedded
  (`src/torrent.cpp:1977-1986`). Un `root_certificate` en
  `AddTorrentParams` (parsé + round-trip resume déjà présents,
  `core/src/add_torrent_params.rs:61`, `resume_data.rs:259,515`)
  n'a donc aucun effet. Sévérité : **wire-visible** (mauvaise root de
  confiance → handshake refusé ; torrent pas marqué ssl → gate attach
  inversé).
  - **sketch** : ajouter champ `root_certificate: Vec<u8>` sur
    `Torrent` (peuplé depuis `atp.root_certificate` dans
    `from_params`, `torrent.rs:~1167-1290`) ; `is_ssl_torrent()` →
    `ti.is_ssl_torrent() || !self.root_certificate.is_empty()` ;
    `ssl_root_cert()` → `if !root_certificate.is_empty() { Some(&root_certificate) } else { ti.ssl_cert() non-vide }` ;
    dans `engine.rs:3063-3073` utiliser `t.ssl_root_cert()` au lieu de
    `ti.ssl_cert()`. Réf. `src/torrent.cpp:1977-1984`. Test : torrent
    sans `ssl-cert` + `atp.root_certificate` → `is_ssl_torrent() == true`,
    `ssl_root_cert()` renvoie l'override.
- `crates/bitdaemon-net/src/ssl_stream.rs:266-290`
  (`TorrentClientVerifier::verify_client_cert`) — accepte le cert
  client si **n'importe quelle** root enregistrée vérifie ; upstream
  épingle au ctx du torrent résolu par SNI
  (`session_impl.cpp:427-455` + `ssl.cpp:121-131` : `set_context`
  réinstalle verify_mode/callback du ctx du torrent). Un cert valable
  pour le torrent A + SNI du torrent B passe chez nous, est refusé
  upstream. Sévérité : **état/sécurité** (pas wire-visible seul, mais
  affaiblit le cloisonnement entre torrents).
  - **sketch** : dans `engine.rs::start_ssl_acceptor_at`, remplacer le
    `TlsAcceptor::from(cfg).accept` par `tokio_rustls::LazyConfigAcceptor`
    : lire `client_hello.server_name()`, résoudre le torrent dans
    `SslTorrentRoots` (retourner `None`/erreur si absent — équivalent
    du `SSL_TLSEXT_ERR_ALERT_FATAL`), puis construire un
    `ServerConfig` dont le `ClientCertVerifier` ne porte que les
    anchors + `expected` de ce torrent (`TorrentClientVerifier`
    mono-entrée, ou nouveau `PinnedClientVerifier { anchors,
    expected }` réutilisant `cert_chain` + `leaf_name_matches`).
    Réf. `session_impl.cpp:429-454`. Test : deux torrents ssl
    enregistrés, client cert signé par la root de B + SNI de A →
    handshake refusé.
- `crates/bitdaemon-session/src/session.rs:3901-3907` — attach gate
  ne teste que `is_ssl_torrent && !6..=9` → `requires_ssl_connection`.
  Upstream `attach_peer` a trois legs (`torrent.cpp:8475-8505`) :
  non-ssl → `requires_ssl_connection` @ bittorrent ; `!m_ssl_ctx` →
  `invalid_ssl_cert` @ **ssl_handshake** ; ctx de la conn ≠ ctx du
  torrent → `invalid_ssl_cert` @ bittorrent (anti cross-torrent SNI).
  - **sketch** : propager le SNI résolu jusqu'à l'attach — ajouter
    `sni_ih: Option<Sha1Hash>` à `DialDone` (rempli dans le leg ssl de
    l'accepteur, engine.rs:3716-3735) puis à `conn`; dans le gate :
    `is_ssl_torrent() && !6..=9` inchangé ; `is_ssl_torrent() &&
    t.ssl_pem_pair().is_none()` → disconnect `"invalid_ssl_cert"` sev
    normal (≈ `!m_ssl_ctx` — chez nous pas de ctx enregistré = pas de
    cert servi) ; `conn.sni_ih.is_some_and(|s| s != *ih)` →
    `invalid_ssl_cert`. Réf. `torrent.cpp:8482-8504`. Test : inbound
    ssl SNI=A, bt handshake ih=B → `invalid_ssl_cert`, pas d'attach.
- `crates/bitdaemon/src/engine.rs:3692-3701` — échec
  `ssl_accept` (TLS handshake) : seulement `warn!` + décrément
  half-open. Upstream `ssl_handshake` poste
  `peer_error_alert(torrent_handle(), endp, peer_id(), operation_t::ssl_handshake, ec)`
  quand la catégorie est active (`session_impl.cpp:3064-3071`).
  Sévérité : **alert-visible**.
  - **sketch** : dans le `Err(e)` de `ssl_accept`, poster
    `Alert::PeerError { ih: Sha1Hash::default() (handle vide upstream),
    remote: remote.to_string(), op: "ssl_handshake".into(),
    error: e.to_string() }` — le `PeerError` actuel exige `ih` ; le
    ih zero correspond au `torrent_handle()` invalide upstream.
    Réf. `session_impl.cpp:3038-3075`. Test : accepteur ssl + handshake
    TLS pourri → `PeerError{op:"ssl_handshake"}` posté.
- `crates/bitdaemon-session/src/session_handle.rs:1735-1747` —
  `set_ssl_certificate` poste `TorrentNeedCert` après coup et
  `set_ssl_cert_buffer` n'émet aucun `TorrentError`. Upstream :
  `torrent_need_cert_alert` est posté par **`init_ssl`** — quand le
  torrent devient ssl et réclame un client cert
  (`torrent.cpp:1908-1911`) — et `set_ssl_cert(_buffer)` poste
  `torrent_error_alert(not_an_ssl_torrent)` sur non-ssl, et
  `torrent_error_alert(ec, "[certificate]"/"[private key]"/"[dh
  params]")` sur PEM invalide (`torrent.cpp:6448-6531`). Sévérité :
  **alert-visible**.
  - **sketch** : déplacer le post `TorrentNeedCert` vers le point où
    `is_ssl_torrent()` devient vrai (init/metadata — là où Rust
    reconnaît `ti.is_ssl_torrent()`/`root_certificate`, cf. D1) ;
    dans `set_ssl_certificate` : `!ssl` → `Alert::TorrentError { ih,
    error: "not_an_ssl_torrent", file: "" }`; faire retourner à
    `set_ssl_cert_buffer` un `Result` de `ssl_stream::certified_key`
    et poster `TorrentError{file:"[certificate]"/"[private key]"}` sur
    erreur. Réf. `torrent.cpp:6448-6531,1908-1911`.
- `crates/bitdaemon/src/engine.rs:43-63,4723,4937` —
  `tracker_tls_config()` ignore `settings.validate_https_trackers` ;
  upstream `update_validate_https` bascule `m_ssl_ctx` en
  `verify_none` quand le setting est faux (`session_impl.cpp:6970-6982`)
  et l'applique à chaud via `apply_settings`. Sévérité :
  **wire-visible** (tracker https auto-signé refusé alors qu'upstream
  l'accepte avec le setting off).
  - **sketch** : lire `ses.settings.validate_https_trackers` au point
    d'usage (4723/4937 + webseed `engine.rs:4723`); si faux, utiliser
    une config verify-none (réutiliser le pattern `VerifyNone` —
    extraire un `tracker_no_verify_config()` à côté de
    `tracker_client_config`), sinon la config PKI cachée. Penser
    invalidation de la `OnceLock` ou config par-appel au setting
    courant. Réf. `session_impl.cpp:6970-6982`.
- `crates/bitdaemon/src/engine.rs:3753-3782` + `ssl_stream.rs:378` —
  `unregister_ssl_torrent` existe mais n'est **jamais appelé** :
  supprimer un torrent ssl laisse son `CertifiedKey` SNI et ses roots
  client-cert dans les maps. Upstream `ssl_server_name_callback_impl`
  fait `find_torrent` à chaque handshake — un torrent parti échoue.
  Sévérité : **état** (sécurité mineure : roots périmés encore
  de confiance).
  - **sketch** : dans `refresh_ssl_certs`, reconstruire les maps
    (`resolver.map.clear()`/`roots.map.clear()` avant la boucle —
    maps privées : ajouter `fn clear(&self)` sur `SniResolver` et
    `SslTorrentRoots`) ; OU appeler `unregister_ssl_torrent` depuis le
    chemin de suppression de torrent (engine remove hook). Réf.
    `session_impl.cpp:440-443`.
- `crates/bitdaemon-session/src/session.rs:3235,7643,9263` (+5761 si
  autre site d'annonce) — `port=` des announces est toujours
  `self.listen_port` ; upstream SSL torrents annoncent
  `m_ses.ssl_listen_port()` (`torrent.cpp:2958`, sélection de
  `use_ssl` dans `session_impl.cpp:1272-1287`). Sévérité :
  **wire-visible** (`port=` faux pour torrents ssl).
  - **sketch** : au site `announce_params`, `let port = if
    t.is_ssl_torrent() { self.ssl_listen_port } else {
    self.listen_port };` — aux trois call sites qui lient
    `(pid, port)`. Réf. `torrent.cpp:2958`. Test : announce params d'un
    torrent ssl → `port == ssl_listen_port`.
- `crates/bitdaemon/src/engine.rs:4920-4949` (`tracker_dial`) —
  pour un torrent ssl annonçant en https, upstream passe le ctx du
  torrent (`req.ssl_ctx = m_ssl_ctx.get()`, `torrent.cpp:3448` ;
  `use_ssl`/`session_impl.cpp:1272`) : cert du tracker vérifié contre
  la root du torrent + name leaf == nom du torrent. Rust utilise
  toujours les roots système. Sévérité : **wire-visible**.
  - **sketch** : transporter dans `SessionOut::TrackerAnnounce` (ou
    `AnnounceParams`) un flag/matériel ssl (`Option<(root, name)>` —
    ceux déjà extraits pour `ssl_params` à `engine.rs:3063`) ; dans
    `tracker_dial`, si présent → `peer_ssl_client_config(&root,
    &name, cert, key)` au lieu de `tracker_tls_config()`. Réf.
    `torrent.cpp:3446-3448`, `session_impl.cpp:1272-1275`.
- `crates/bitdaemon/src/engine.rs:800-803` + DHT announce
  (`session.rs:8000`) — `port==0` DHT → `self.listen_port` ; upstream
  `node::announce` demande `get_listen_port(ssl?ssl:plaintext)`
  (`node.cpp:448-455`). Le flag `ssl` est déjà calculé
  (`session.rs:7350-7361` — ne sert qu'à l'`implied_port` gate).
  Sévérité : **wire-visible** (port DHT annoncé faux pour ssl).
  - **sketch** : `let port = if port == 0 { if ssl_this_announce {
    self.ssl_listen_port } else { self.listen_port } } else { port }` —
    ajouter `ssl: bool` à `DhtResult::Announce` (déjà calculé à 7350).
    Réf. `node.cpp:448-455`, `torrent.cpp:3036-3040`.
- `crates/bitdaemon/src/engine.rs:3288` — SNI outbound =
  `hex(ih.as_bytes())` (v1 toujours) ; upstream envoie
  `to_hex(info_hashes().get(peerinfo->protocol()))`
  (`torrent.cpp:8133-8137`) — hash v2 (64 hex) pour un peer v2.
  Note : le dispatch SNI inbound upstream ne sait décoder que les 40
  premiers caractères en v1 (`session_impl.cpp:429-437`), donc un SNI
  v2 upstream échoue aussi côté accepteur upstream — écart réel mais
  peu utile.
  - **sketch** : si `wire_ih`/proto v2 disponible à ce point, envoyer
    `hex(v2)` quand la connexion est v2 ; sinon documenter. Réf.
    `torrent.cpp:8133-8137`.
- `crates/bitdaemon-cli/src/main.rs:582,595` —
  `start_ssl_acceptor_at(acc, addr, 1)` → kind 1 = `socks5` (tcp_ssl
  = **6**) ; `start_ssl_utp(dg, addr, 6)` → 6 = tcp_ssl (utp_ssl =
  **9**, `engine.rs:155-168`). Conséquence : toute conn ssl inbound du
  CLI tombe sous le gate `!6..=9` → `requires_ssl_connection`
  (`session.rs:3901`). Sévérité : **état** (CLI seul, mais le client
  exemple ne peut pas servir de torrent ssl).
  - **sketch** : `start_ssl_acceptor_at(.., 6)` et
    `start_ssl_utp(.., 9)`. Réf. `engine.rs:154-168`,
    `session_impl.cpp:1347-1350`.

## Fichiers du fix

- `crates/bitdaemon-net/src/ssl_stream.rs` (verifier épinglé, helper
  verify-none tracker, `clear` sur les maps)
- `crates/bitdaemon-session/src/torrent.rs` (`root_certificate`,
  `is_ssl_torrent`, `ssl_root_cert`, legs attach — via session.rs)
- `crates/bitdaemon-session/src/session.rs` (gate attach, ports
  announce/DHT, alert ssl_handshake inbound)
- `crates/bitdaemon-session/src/session_handle.rs`
  (`set_ssl_certificate` erreurs/alerte)
- `crates/bitdaemon-session/src/alerts.rs` (si `PeerError.ih` devient
  optionnel — sinon ih zero)
- `crates/bitdaemon/src/engine.rs` (LazyConfigAcceptor, alert
  handshake, `validate_https_trackers`, ssl ctx tracker, unregister,
  SNI v2)
- `crates/bitdaemon-cli/src/main.rs` (kinds)

## Non-bugs tranchés

- `SniResolver::fallback` (engine construit `SniResolver::new(None)`)
  — fallback `None` → rustls échoue le handshake, équivalent du
  `SSL_TLSEXT_ERR_ALERT_FATAL` upstream. L'option fallback reste un
  point d'extension, pas une déviation tant qu'on ne l'alimente pas.
- Pas de `verify_client_once` : flag de resumption OpenSSL ; rustls
  gère la resumption côté client-certs différemment — l'effet
  (vérifier la chaîne à la première handshake) est préservé.
- `ssl_server_name_callback_impl` accepte les SNIs > 40 chars en
  décodant les 40 premiers en v1 — chez nous la map exact-match sur
  hex ih v1 : un SNI > 40 échoue des deux côtés (le décodage upstream
  ne trouve jamais de torrent pour un hash tronqué non enregistré).
  Non-bug.
- `m_peer_ssl_ctx` verify_none + switch ctx : le `with_client_cert_
  verifier` rustls demande le cert client dès le départ là où
  OpenSSL attend le switch — effet net identique (cert obligatoire
  pour les torrents ssl reconnus ; SNI inconnu échoue dans les deux).
- `ssl_server_config`/`certified_key` ignorent les DH params :
  ECDHE-only rustls, `use_tmp_dh` ignoré comme les builds LibreSSL
  upstream (commentaire `ssl_stream.rs:547-550`).

## Résultat

Appliqué sur `audit/23-ssl-torrents`, commit `4b66a27`
(`fix(bitdaemon-session,net): audit A23 — SSL torrents semantic
parity`). Les 11 déviations sont corrigées :

- **D1** `root_certificate` override → `is_ssl_torrent` /
  `ssl_root_cert` préfèrent `atp.root_certificate` ; propagé aux
  paramètres SSL de l'engine.
- **D2** `ssl_accept_torrent` (`LazyConfigAcceptor`) lit le
  ClientHello, résout le SNI hex→torrent et installe
  `PinnedClientVerifier` — la vérif client est épinglée au torrent
  choisi par SNI (`set_context` upstream).
- **D3** legs `invalid_ssl_cert` à l'attach : pas de ctx / SNI
  info-hash ≠ handshake info-hash.
- **D4** `peer_error_alert` `op=ssl_handshake` sur échec TLS inbound.
- **D5** `TorrentNeedCert` posté au point `init_ssl` ;
  `TorrentError` + `invalid_ssl_cert` sur PEM cert/key invalide.
- **D6** `validate_https_trackers=false` → `tracker_no_verify_config`.
- **D7** `refresh_ssl_certs` reconstruit les maps SNI/roots —
  `unregister_ssl_torrent` nettoie les entrées périmées.
- **D8/D10** announces torrents SSL portent `port=ssl_listen_port` ;
  résolution du port DHT SSL identique.
- **D9** announces https des torrents SSL passent par le ctx TLS du
  torrent (`req.ssl_ctx`).
- **D11** SNI sortant = hash de la version de protocole du pair.
- **D12** `socket_type` CLI : tcp_ssl=6, utp_ssl=9.

Tests de régression ajoutés :

- `ssl_stream::tests::pinned_verifier_rejects_other_torrent_cert`
  (cert client valide pour A rejeté quand le SNI résout B).
- `ssl_stream::tests::ssl_roots_unregister_and_clear` (SNI/root
  périmés supprimés).
- `ssl_stream::tests::tracker_no_verify_config_builds`
  (`validate_https_trackers=false`).
- `test_add_torrent::add_torrent_root_certificate_override`
  (`ssl-cert` embarqué seul → SSL ; `root_certificate` préféré).

Gates : `cargo fmt --all`, `cargo clippy --workspace --all-targets --
-D warnings`, `cargo test --workspace`, `cargo doc --workspace
--no-deps` — tous verts.
