# Audit A03 — rapport d'analyse : chiffrement MSE/PE

Périmètre : handshake DH-768 (PE1–PE4), `crypto_provide`/`crypto_select`,
RC4, padding, politiques `out_enc_policy`/`in_enc_policy`/`allowed_enc_level`/
`prefer_rc4`, upgrade plaintext→MSE, fallback. Baseline upstream `e7381c7bc`.

Références upstream : `src/pe_crypto.cpp` (DH prime, `compute_secret`,
`rc4_handler`, `rc4_init`/`rc4_encrypt`), `src/bt_peer_connection.cpp`
(`on_connected` 173-274, `write_pe1_2_dhkey` 526, `write_pe3_sync` 564,
`write_pe4_sync` 644, `write_pe_vc_cryptofield` 676, machine `read_pe_*`
2810-3314, `init_bt_handshake` 2773, probe `read_protocol_identifier`
3318-3411, legs `pe_support` 2796-2806/3622-3632),
`src/peer_connection.cpp` (`fast_reconnect` 947-961, legs compteurs
`error_*_peers`/`error_encrypted_peers`/`error_rc4_peers` 4406-4423,
alertes de `disconnect` 4477-4491, `associated_info_hash` 1115-1127),
`src/session_impl.cpp` (`find_encrypted_torrent` 4875, `sanitize_settings`
1540-1558), `include/libtorrent/aux_/torrent_list.hpp` (`m_obfuscated_index`
85-131), `src/http_tracker_connection.cpp` (`supportcrypto` 143-147),
`src/random.cpp` (`random(m)` = `uniform_int_distribution(0, m)` inclusif,
149-168). Il n'existe pas de `src/encryption.cpp` upstream — tout le MSE
vit dans `bt_peer_connection.cpp` + `pe_crypto.cpp`.

Côté Rust : `crates/bitdaemon-net/src/pe_crypto.rs` (primitives),
`crates/bitdaemon-net/src/mse.rs` (machine PE1–PE4),
`crates/bitdaemon-net/src/bt_peer_conn.rs` (`run_handshake*` + wrappers
`*_rc4`), `crates/bitdaemon/src/engine.rs` (dial/accept/inject +
`crypto_select` 3741 + `attach_stream` 3248), `crates/bitdaemon-session/
src/{torrent.rs,session.rs,peer_conn.rs,peer_list.rs(nett),settings.rs,
session_params.rs,counters.rs}`.

## Verdicts checklist

- **Handshake : DH 768 bits, SKEY = info_hash côté initiator, génération
  clés publiques** — verdict : **déviation** — cœur conforme : prime
  RFC 3526 groupe 1 (`pe_crypto.rs:25-32`), secret local 160 bits +
  `2^s mod p` (`pe_crypto.rs:73-89`), rejet dégénéré `<2 || >= p-1`
  (`pe_crypto.rs:100-110` = upstream `pe_crypto.cpp:179-184/266-271`),
  export 96-octets big-endian avec pad (`pe_crypto.rs:44-50`),
  `skey_field`/`skey_candidate` = `sha1('req2'|SKEY) ^ sha1('req3'|S)`
  (`mse.rs:80-100` = upstream `bt_peer_connection.cpp:600-614`).
  Déviation : le SKEY émis côté initiateur est toujours la clé canonique
  du swarm (v1 pour un hybride) — upstream `associated_info_hash()`
  choisit `v2` quand `pi->protocol_v2 && ih.has_v2()`
  (`peer_connection.cpp:1115-1127`) — voir D4.
- **crypto_provide/crypto_select : bits plaintext/rc4, sync sur
  len(PadC), IA vs req1/req2/req3** — verdict : **déviation** —
  `crypto_provide = allowed_enc_level` ✓ (`engine.rs:2114,2331` =
  upstream `bt_peer_connection.cpp:628-630`) ; le `select` répondeur
  (intersect + MSB si `prefer_rc4`, LSB sinon, `engine.rs:3741-3755`)
  est équivalent à upstream `read_pe_cryptofield` (`3152-3174`) ;
  `sync_hash`/`stream_key_hash`/`xor_mask` tags req1/req2/req3
  conformes (`pe_crypto.rs:235-256`). Déviations : (a) initiateur ne
  masque pas `select & allowed_enc_level` avant validation (D1) ;
  (b) `allowed_enc_level` non sanitizé → `crypto_provide` peut émettre
  0 ou des bits inconnus (D13) ; (c) sync sur len(PadC) — le bound
  `len_pad > 512 → invalid_pad_size` est absent des deux côtés (D2).
- **RC4 : clés A→B et B→A distinctes, drop 1024, application
  header+payload** — verdict : **conforme** — `init_pe_rc4_handler`
  clés `keyA`/`keyB` swappées selon direction (`pe_crypto.rs:261-274` =
  upstream `bt_peer_connection.cpp:70-103`), discard 1024 octets de
  keystream à la pose des clés (`pe_crypto.rs:194-205` = upstream
  `pe_crypto.cpp:440-460`), RC4 libTomCrypt-compatible (KSA cap 256,
  identity perm si clé vide ; `pe_crypto.rs:143-157` = upstream
  `rc4_init` 509-540), chiffrement appliqué à tout le flux post-
  négociation — longueur 4-octets incluse (`engine.rs:3407-3410` read,
  `3566-3573` write via `write_messages_rc4`; `bt_peer_conn.rs:440-458`
  `read_message_rc4`). IA toujours RC4 quel que soit le select ✓
  (`mse.rs:166-171`, `307-311` = upstream 2864-2866, 3291-3292).
- **Padding : len(PadA/B/C) 0-512, VC constant** — verdict :
  **déviation** — VC = 8 zéros ✓ (`mse.rs:107`, vérifié `mse.rs:198`
  initiator / `293-295` responder = upstream 694, 3032-3036). Pads :
  **jamais émis** — `pad_len = 0` à tous les call sites
  (`engine.rs:2129,2540`, responder `bt_peer_conn.rs:277` hardcode 0)
  alors qu'upstream émet `random(512)` = **0..=512** à chaque pad
  (PE1/PE2 `bt_peer_connection.cpp:542,556`, PE3 577/638, PE4
  654/658). Fenêtres de scan conformes (~532 synchash / ~520 syncvc,
  `mse.rs:268,191` = upstream 2932, 3088). La borne `len_pad > 512`
  n'est vérifiée nulle part (D2). Sévérité : wire-visible (voir D3).
- **Politiques : enc_disabled/enabled/required — entrantes ET
  sortantes, `allowed_enc_level`, pe_forced** — verdict : **déviation**
  — sortantes : `try_connect_peer` reproduit le toggle `pe_support`
  (`torrent.rs:2946-2958` ≈ upstream `on_connected` 218-264), SSL
  exempté (`engine.rs:2353-2356`). Entrantes : probe 20-octets +
  ré-interprétation DH (`bt_peer_conn.rs:249-288` = upstream
  3318-3381), `pe_disabled` + MSE → `no_incoming_encrypted`,
  `pe_forced` + plaintext → `no_incoming_regular`, SSL acceptor sans
  check politique (upstream `!is_ssl` exempte `pe_forced`, 3392-3399).
  Déviations : `fast_reconnect` jamais armé (D8) ; `pe_support` toggle
  au pick au lieu de post-connect (D9) ; write-back `pe_support =
  encrypted` inconditionnel (D10) ; sanitize des knobs absent (D13) ;
  `pe_forced` entrant n'exclut pas le select-plaintext — comme
  upstream (non-bug, voir section dédiée).
- **Upgrade : conn plaintext → enc mid-handshake, fallback sur échec**
  — verdict : **déviation partielle** — upgrade entrant conforme
  (`run_handshake_responder_auto` : probe `head[0]==19 && "BitTorrent
  protocol"` sinon `dh_prefix` ré-interprété, `bt_peer_conn.rs:252-282`
  = upstream `cut(0, dh_key_len)` 3374-3379). Sortant : pas d'upgrade
  mid-conn — comme upstream (`m_encrypted || is_outgoing()` →
  `invalid_info_hash` 3365-3368). Fallback : le mécanisme `pe_support`
  existe mais la jambe `fast_reconnect` manque → retry plaintext
  retardé (D8). Sous-cas IA partielle (0 < len_ia < 68) mal reprise
  (D6). Compteurs `error_encrypted_peers`/`error_rc4_peers` morts (D7).
- **Aucun unsafe : RC4/DH via crates sûres** — verdict : **conforme**
  — `#![forbid(unsafe_code)]` (`bitdaemon-net/src/lib.rs:21`) ; DH via
  `num-bigint`, SHA-1 via `sha1`, aléas via `rand`/`random_bytes`
  (crypto-grade), RC4 en safe Rust pur (`pe_crypto.rs`).

## Déviations

### Wire-visible

1. **`mse.rs:204-207`** — validateur `crypto_select` initiateur : le
   Rust teste `select & 0x03 == 0` ; upstream masque d'abord
   `crypto_field &= allowed_enc_level` puis refuse `== 0`
   (`bt_peer_connection.cpp:3188-3196`). Conséquences : (a) pair
   répond `select = rc4` alors qu'on n'a offert que `plaintext`
   (`allowed=1`) → upstream `unsupported_encryption_mode_selected`
   (peer_error), Rust poursuit en RC4 contre la politique — symétrique
   pour `select = plaintext` / `allowed = rc4` ; (b) `select = 0x03`
   avec `allowed = rc4-only` : upstream `3 & 2 = 2` → RC4 ; Rust
   `3 != CRYPTO_RC4` → `rc4_encrypted = false` → **désync** (le pair
   RC4-chiffre, on lit en clair → la connexion meurt en garbage).
   Fix : `let select = select & u32::from(allowed_enc_level)` avant le
   test `== 0` (passer `pe` ou `allowed` en paramètre de
   `mse_initiator`). Sévérité : **wire-visible**.

2. **`mse.rs:208` (initiateur) + `mse.rs:297` (répondeur)** — `len_pad`
   lu en `u16` **sans borne** : upstream refuse `len_pad < 0 ||
   len_pad > 512` → `invalid_pad_size` (peer_error) *avant* de lire le
   pad (`bt_peer_connection.cpp:3204-3208`). Rust lit jusqu'à 64 Kio +
   les déchiffre — un pair annonçant `len_pad = 0xFFFF` fait attendre
   la connexion jusqu'au `handshake_timeout` au lieu de la couper
   immédiatement (les valeurs `≥ 0x8000`, négatives en `int16`
   upstream, passent aussi). Fix : `if len_pad > MAX_PAD { return
   Err(InvalidPadSize) }` aux deux endroits. Sévérité :
   **wire-visible** + petite asymétrie DoS.

3. **`engine.rs:2129,2540` + `bt_peer_conn.rs:277`** — pads jamais
   émis : tous les call sites passent `pad_len = 0`, upstream émet
   `random(512)` (inclusif : 0..=512, `random.cpp:168`) pour padA
   (PE1), padB (PE2), padC (cryptofield PE3), padD (cryptofield PE4).
   Conséquence : handshake MSE à longueur déterministe
   (96+20+20+14+2+68 / 96+14+2) → fingerprint trivial, le but des pads
   (obfuscation du handshake) est perdu. Fix : `pad_len =
   random(0..=512)` par pad (le paramètre existe déjà) ; au passage le
   clamp `MAX_PAD - 1` (`mse.rs:146,161,256,318`) devrait être
   `MAX_PAD` — upstream peut émettre 512. Sévérité : **wire-visible**
   (empreinte réseau).

4. **`engine.rs:2680-2684` / `3162-3166` (+ choix `ih` sortant)** —
   l'index `skeys` = `sha1('req2'|ih)` n'est peuplé que de la clé
   canonique de `ses.torrents` — v1 pour un torrent **hybride**
   (`torrent.rs:1019-1026`). Upstream `m_obfuscated_index` enregistre
   `sha1('req2'|hash)` pour **chaque** version (`torrent_list.hpp:
   102-116`) et en déduit `protocol_v2` de la conn
   (`bt_peer_connection.cpp:3006-3013`). Conséquences : (a) MSE
   entrant portant le skey v2 d'un hybride → `UnknownInfoHash` →
   disconnect, upstream attache et parle v2 ; (b) dial sortant vers un
   pair `protocol_v2` (flag `lt_v2` PEX, `peer_list.rs:333-334`):
   upstream émet SKEY = `req2` du v2 tronqué + handshake v2
   (`associated_info_hash()`, `peer_connection.cpp:1125-1126`), Rust
   émet toujours le v1 — un pair **v2-only** d'un swarm hybride est
   injoignable (v1 ih inconnu de son côté). Fix : peupler `skeys` avec
   `info_hash_v2()` (tronqué) quand présent, et choisir l'`ih` de
   handshake sortant selon `p.protocol_v2`. Sévérité :
   **wire-visible** (v2 atteignabilité hybride).

5. **`engine.rs:957`** — `AnnounceOptions::default()` envoie
   `supportcrypto=1` inconditionnellement ; upstream le gate sur
   `in_enc_policy != pe_disabled && announce_crypto_support`
   (`http_tracker_connection.cpp:143-146`). Sous `pe_disabled` ou
   `announce_crypto_support=false` on annonce un support qu'on refuse
   / qu'on a désactivé. Fix : peupler `opts.supportcrypto` depuis les
   settings au moment de l'annonce. Sévérité : **wire-visible**
   (contenu d'annonce).

6. **`bt_peer_conn.rs:209-225`** — IA partielle (`0 < len_ia < 68`)
   mal reprise : upstream `read_pe_ia` déchiffre les `len_ia` octets
   puis `cut(0, 20)` les **réinjecte** comme début du flux protocol —
   le handshake peut chevaucher la frontière IA (`3284-3314`). Rust
   exige `initial_payload.len() >= HANDSHAKE_LEN` pour le parser,
   sinon lit 68 octets neufs sur le fil → les `len_ia` premiers octets
   du handshake sont perdus → parse KO → disconnect. Edge-case réel
   bas : les impls MSE connues émettent IA = 68 ou 0, mais upstream le
   tolère. Fix : préfixer le buffer de lecture du handshake par
   `initial_payload` (compléter jusqu'à 68). Sévérité :
   **wire-visible** (edge).

7. **Alertes d'échec de handshake absentes** — upstream `disconnect()`
   poste `peer_error_alert` (`error > failure`) ou
   `peer_disconnected_alert` (`error <= failure`) même pré-attach —
   handle nul mais l'alerte part (`peer_connection.cpp:4477-4491`) :
   `invalid_encrypt_handshake`/`invalid_encryption_constant`/
   `invalid_pad_size`/`unsupported_encryption_mode_selected` →
   peer_error ; `sync_hash_not_found`/`invalid_info_hash`/
   `unsupported_encryption_mode`/`no_incoming_*` →
   peer_disconnected (severity failure/normal). Rust : entrant =
   drop silencieux (`engine.rs:2665-2670,3194-3199`), sortant =
   `peer_connect_failed` sans alerte (`engine.rs:2230-2237`). Quand
   l'erreur atteint `peer_disconnected` post-attach, tout `Mse`/
   `Wire` tombe dans le bras fourre-tout `PeerError` — la distinction
   failure vs peer_error upstream est aplatie. Fix : faire remonter
   `PeerGone::Transport` avec une sévérité MSE-typée depuis les
   handshakes (ou poster l'alerte directement). Sévérité :
   **état/alertes**.

### État

8. **`torrent.rs:2946-2958`** — jambe `fast_reconnect(true)` absente
   sur le dial MSE `pe_enabled` (upstream `bt_peer_connection.cpp:
   239-243` : rewind `last_connected` de `min_reconnect_time ×
   max_failcount`, cap `fast_reconnects ≤ 2`, et `connection_closed`
   ne re-stampe pas — `peer_connection.cpp:947-961`,
   `peer_list.cpp:872-873,1354-1355`). Le mécanisme existe
   (`torrent_peer.rs:129` `fast_reconnects`, `peer_list.rs:570-604`
   paramètres `fast_reconnect`) mais n'est jamais invoqué pour MSE
   (seul `was_utp` le passe). Conséquence : un dial MSE échoué attend
   le cooldown `min_reconnect_time × (failcount+1)` avant le retry
   plaintext au lieu d'être quasi-immédiat. Fix : sur le bras
   `pe_support == true` → rewind `last_connected` + `fast_reconnects
   += 1` (borné à 2) + propager le flag jusqu'à
   `connection_closed`/`peer_connect_failed`. Sévérité :
   **état/timing**.

9. **`torrent.rs:2946-2958`** — le toggle `pe_support` est fait à la
   **sélection** du candidat ; upstream le fait dans `on_connected`,
   **après** le connect TCP (`bt_peer_connection.cpp:231-258`). Un
   refus TCP laisse donc `pe_support` inchangé upstream (retry même
   mode) alors que Rust l'a déjà retourné (retry mode opposé).
   Sévérité : **état** (choix du prochain dial diverge sous échec
   transport).

10. **`torrent.rs:3086`** — `p.pe_support = encrypted` inconditionnel
    (inbound + outbound, toutes politiques). Upstream n'écrit le flag
    que pour `is_outgoing() && out_enc_policy == pe_enabled`
    (`2798-2806` re-arm après MSE, `3624-3632` clear après plaintext
    réussi). Un conn **entrant** plaintext efface donc un `pe_support`
    posé par PEX, et un entrant MSE le lève — upstream ne touche rien
    dans les deux cas. Effet réel seulement si `in`/`out` divergent
    ou si la politique change en cours de session, mais c'est un écart
    de fidélité. Fix : gate `conn.outgoing && out_enc_policy ==
    Enabled`. Sévérité : **état**.

### Compteurs / cosmétique

11. **`counters.rs:117-119`** — `ErrorRc4Peers`/`ErrorEncryptedPeers`
    déclarés, jamais incrémentés. Upstream les incrémente dans
    `disconnect()` sous `error > normal` pour les conns bittorrent
    (`supports_encryption()` → `error_encrypted_peers` ;
    `+ rc4_encrypted()` → `error_rc4_peers`,
    `peer_connection.cpp:4414-4422`, op ≠ connect). Il manque le champ
    `rc4_encrypted` sur `PeerConn` (`peer_conn.rs:375-379` n'a que
    `supports_encryption` — équiv. `m_encrypted`). Fix : champ
    `rc4_encrypted` alimenté par `DialDone.rc4.is_some()`/MseResult +
    les deux legs dans `peer_disconnected`. Sévérité : **compteur**.

12. **`peer_info.rs:56-59`** — `PeerFlags::RC4_ENCRYPTED` /
    `PLAINTEXT_ENCRYPTED` définis mais jamais posés
    (`torrent.rs:2377+` ne les remplit pas ; upstream
    `get_peer_info` : `m_encrypted` → `rc4_encrypted` ou
    `plaintext_encrypted`, `bt_peer_connection.cpp:500-506`).
    `UTP_SOCKET`/`SSL_SOCKET`/`I2P_SOCKET` non posés non plus
    (débordement possible vers un autre audit — à recouper).
    Sévérité : **cosmétique/API**.

13. **Pas de `sanitize_settings` pour les knobs crypto** — upstream
    `session_impl.cpp:1540-1558` : `out_enc_policy`/`in_enc_policy`
    hors {forced,enabled,disabled} → `pe_disabled` ;
    `allowed_enc_level` ∉ {1,2,3} → `pe_both`. Rust :
    `EncPolicy::from_int` mappe l'invalide vers **Enabled**
    (`session_params.rs:264-270`, mauvais fallback — upstream choisit
    Disabled) et `allowed_enc_level` `u8` est stocké brut → 0 donne
    `crypto_provide = 0` (tout MSE échoue) ou des bits inconnus.
    Sévérité : **état** (config hors-plage).

14. **`mse.rs:315-317`** — le gate répondeur `crypto_select & 0x03 ==
    0` laisse passer `select = 3` (upstream
    `TORRENT_ASSERT(crypto_select == 0x02 || == 0x01)`,
    `bt_peer_connection.cpp:651`). La callback session n'émet que des
    bits uniques, mais l'API publique `mse_responder` accepte tout
    `Fn(u32) -> u32` — une valeur multi-bits partirait sur le fil.
    Fix : valider `select == 1 || select == 2`. Sévérité :
    **cosmétique**.

15. **Commentaires** — `mse.rs:41-42`/`302-303` citent
    `invalid_pad_size` pour `len_ia > 68` ; upstream utilise
    `invalid_encrypt_handshake` à cet endroit
    (`bt_peer_connection.cpp:3250-3253` — `invalid_pad_size` est pour
    `len_pad`, 3204-3208). `mse.rs:36-37` documente `random(512)` →
    `0..=511` ; la distribution est **inclusive** → `0..=512`
    (`random.cpp:168`). Sévérité : **cosmétique** (doc).

## Fichiers du fix

- `crates/bitdaemon-net/src/mse.rs` — D1 (masque `allowed_enc_level`
  initiateur), D2 (bornes `len_pad` ×2), D3 (clamp 512 + appel
  `random`), D6 (IA partielle — signature `initial_payload`), D14
  (gate select), D15 (docs).
- `crates/bitdaemon-net/src/bt_peer_conn.rs` — D6 (préfixe IA dans le
  read handshake), D14 via `mse_responder_prefix`.
- `crates/bitdaemon/src/engine.rs` — D3 (`pad_len` aléatoire aux call
  sites), D4 (`skeys` v1+v2 + choix `ih` sortant par `protocol_v2`),
  D5 (`AnnounceOptions` depuis settings), D7 (plomberie alerte
  handshake-fail).
- `crates/bitdaemon-session/src/torrent.rs` — D8 (`fast_reconnect`
  dial MSE), D9 (timing du toggle — à reporter au succès TCP), D10
  (gate `outgoing && Enabled` du write-back).
- `crates/bitdaemon-session/src/session.rs` — D11 (legs compteurs dans
  `peer_disconnected`), D12 (flags `peer_info`), D7 côté alertes.
- `crates/bitdaemon-session/src/peer_conn.rs` — D11 (champ
  `rc4_encrypted`).
- `crates/bitdaemon-session/src/session_params.rs` — D13
  (`EncPolicy::from_int` → Disabled ; clamp `allowed_enc_level` à
  {1,2,3}→both).
- `crates/bitdaemon-session/src/settings.rs` — D13 (sanitize à
  l'apply si on choisit de le faire là plutôt qu'à la lecture pack).

Chevauchement vague V3 : `mse.rs`/`bt_peer_conn.rs` partagent le
périmètre avec A01 (peer-wire) — séquencer les apply entre eux ;
`engine.rs`/`torrent.rs`/`session.rs` sont les fichiers chauds communs
aux audits session-side.

## Non-bugs tranchés

- **Probe RC4 pour le sync VC** — `mse.rs:177-179` instancie un
  `Rc4Handler` jetable pour calculer le motif `RC4(VC)` sans consommer
  le keystream de la vraie conn, puis déchiffre le VC trouvé (qui
  consomme les 8 octets). Upstream consomme les 8 octets de keystream
  upfront (`rc4_decrypt(m_sync_vc)` `bt_peer_connection.cpp:3074-3076`)
  puis scanne le flux brut — positions de keystream finales identiques
  (cryptofield à l'offset 8).
- **Modèle lecture exacte vs `recv_buffer`+search** — Rust lit des
  longueurs exactes et scanne par fenêtre glissante ; upstream
  accumule dans `m_recv_buffer` et `search()`. Bornes de scan
  équivalentes : `20+512` synchash (`mse.rs:268` = upstream
  `m_sync_bytes_read >= 512`, `2932`) et `8+512` syncvc
  (`mse.rs:191` = 3088-3095). Les bytes pré-lus entre la fin de pad et
  la VC n'ont pas de cas limite divergence (Rust ne sur-lit jamais au-
  delà du motif).
- **Répondeur : clé DH locale écrite avant validation de la clé
  distante** — `mse.rs:254-259` = upstream `write_pe1_2_dhkey()` avant
  `compute_secret` (`2834-2843`) — même ordre sur le fil (notre clé
  part même si la leur est dégénérée, puis disconnect).
- **`pe_forced` entrant + select plaintext** — upstream laisse
  `crypto_select` être `plaintext` même sous `pe_forced` (la politique
  gate le *format* du handshake, pas le niveau choisi — sélection
  dérivée de `allowed_enc_level` seul). Rust idem — une conn MSE
  select-plaintext sous `pe_forced` survit (`mse.rs:314-325` +
  `rc4_encrypted = false`).
- **Pas d'upgrade MSE mid-stream sortant** — conforme : upstream
  déconnecte `invalid_info_hash` si `m_encrypted || is_outgoing()`
  (`3365-3368`) ; un dial plaintext Rust échoue pareillement sur une
  réponse non-BT.
- **Double-chiffrement entrant** — upstream `m_encrypted` → second MSE
  refusé ; Rust n'a pas de re-probe post-handshake — le même flux
  aboutit à un `bad handshake`/`disconnect`. Équivalent.
- **SSL exempt des politiques MSE** — acceptor SSL distinct +
  `run_handshake_responder` (pas de probe) (`engine.rs:2746-2880`) =
  upstream `is_ssl` → `out_policy=pe_disabled` (200-201) et exclusion
  `!is_ssl` des checks `no_incoming_regular`/`invalid_info_hash`
  (3341-3351, 3396). RTC : `PeerAddr` n'a pas de variante `rtc` —
  l'exclusion `is_rtc_addr` upstream (202-207) est inatteignable ici.
- **`num_peers_half_open` / `handshake_timeout`** — armés aux deux
  sens (`engine.rs:2093-2163,2373-2380,2689-2705`, attach injecté
  3144-3155) — conforme à `peer_connection` ctor → `on_connected`.
- **Erreurs nommées autrement** — `MseError::BadSync`/
  `UnknownInfoHash`/`DhFailed`/`BadVc`/`NoCrypto`/`InvalidPadSize`
  correspondent à `sync_hash_not_found`/`invalid_info_hash`/
  `invalid_encrypt_handshake`/`invalid_encryption_constant`/
  `unsupported_encryption_mode[_selected]`/`invalid_pad_size` — la
  sémantique wire est conservée, les noms divergent (convention
  AGENTS §4.9 tolérée).

## Résultat

Appliqué sur `audit/03-mse-encryption`, commit `33a77de`
(`fix(net,session): upstream-fidelity apply of audit A03 — MSE/PE
parity`), merge local sur `main` (workflow mis à jour — pas de PR).

### Déviations corrigées (15/15)

- **D1** `crypto_select` initiateur masqué par `allowed_enc_level`
  avant le test `== 0` + réduction à un bit (`mse.rs`,
  `allowed_enc_level` passé en paramètre).
- **D2** `len_pad` borné ≤ 512 aux deux jambes → `invalid_pad_size`/
  `peer_error` immédiat ; `len_ia` borné `0..=68` →
  `invalid_encrypt_handshake`.
- **D3** pads PadA/PadB/PadC/PadD tirés `random(0..=512)` inclusif,
  clamp `MAX_PAD` (512 possible) — l'empreinte n'est plus
  déterministe.
- **D4** skey/index : `known_torrent`/`known_torrent_by_skey`
  résolvent v1 **et** v2-tronqué ; `KnownTorrent{ih, wire_ih,
  protocol_v2, has_v2, v2_ih, valid_metadata}` ; le handshake sortant
  émet `associated_info_hash()` selon `protocol_v2`.
- **D5** `supportcrypto` annoncé sous `in_enc_policy != disabled &&
  announce_crypto_support`.
- **D6** IA partielle réinjectée en tête du handshake (`cut(0,20)`),
  RC4 inconditionnel.
- **D7** alertes + compteurs de `disconnect()` reproduits :
  `DialFail{FailOp, DisconnectSeverity, reason}` ; `peer_error` vs
  `peer_disconnected` exclusifs, postés même pré-attach ; legs
  `error_{tcp,utp,incoming,outgoing}_peers` + crypto.
- **D8** `fast_reconnect` armé sur le dial MSE `pe_enabled`
  (`arm_fast_reconnect` : rewind `last_connected`, cap, flag parqué
  jusqu'à `connection_closed`).
- **D9** toggle `pe_support` déplacé de la sélection vers
  `peer_link_up` (= `on_connected`, post-connect).
- **D10** write-backs `pe_support` gatés `is_outgoing && pe_enabled`
  (`rearm_pe_support` post-PE4, clear post-plaintext).
- **D11** `ErrorEncryptedPeers`/`ErrorRc4Peers` incrémentés (champ
  `rc4_encrypted` sur `PeerConn`, propagé via `DialDone`).
- **D12** `PeerFlags::RC4_ENCRYPTED`/`PLAINTEXT_ENCRYPTED` posés dans
  `peer_info` (`m_encrypted` → l'un des deux).
- **D13** `sanitize_settings` : `allowed_enc_level` hors {1,2,3} →
  `pe_both`, `enc_policy` hors {0,1,2} → `pe_disabled`
  (`EncPolicy::from_int` corrigé Enabled→Disabled).
- **D14** gate répondeur : `select` réduit à un bit puis borné —
  `select==0` → `unsupported_encryption_mode`/`failure` ; initiateur
  masqué `==0` → `..._selected`/`peer_error`.
- **D15** commentaires `mse.rs` corrigés (`invalid_pad_size` vs
  `invalid_encrypt_handshake`, `random(512)` inclusif).

Bonus parité couverts en passant : bit réservé `0x10` entrant flippe
`protocol_v2` avant l'écriture de la réponse (`0x10` sur v1-only →
`invalid_info_hash`/`failure` ; v1-only garde son hash v1) ; pas de
re-validation de l'ih echo côté initiateur attaché (upstream saute le
bloc sous `if (!t)`) ; échec uTP → severity normale + retry TCP
différé ; échec inbound post-attach comptabilisé sans peer-entry ;
`pex_lt_v2` forcé sur les swarms v2-only.

### Tests ajoutés

- `bitdaemon-net/tests/test_mse.rs` : select masqué/réduit, bornes
  `len_pad`/`len_ia`, skey round-trip + rejet, RC4 negotiated,
  handshake complet sur MSE.
- `bitdaemon-net/tests/test_bt_peer_conn.rs` : bit `0x10` entrant
  (flip `protocol_v2` avant réponse, rejet sur v1-only), rejet
  responder ih inconnu.
- `bitdaemon-session/tests/test_mse_parity.rs` (9 tests) : toggles
  `pe_support` à link-up, write-backs gatés, re-arm post-PE4,
  compteurs par sévérité/direction/crypto, échec inbound sans
  peer-entry, `fast_reconnect` saute le cooldown, sanitize
  `allowed_enc_level`, dual-index v1/v2 + skey, flags
  `peer_info` encryption.

### Gates

`cargo fmt --all` · `cargo clippy --workspace --all-targets --
-D warnings` · `cargo test --workspace` (108 binaires verts) ·
`cargo doc --workspace --no-deps` — tous passent dans le worktree.

## Re-audit à froid (post-apply round-1)

Relecture indépendante de la baseline `e7381c7bc` et du code post-
apply. Les fixes D1–D15 sont confirmés appliqués et fidèles (skey
v1+v2 via `known_torrent_by_skey`, `peer_link_up` = `on_connected`,
write-backs `pe_support` gatés `outgoing && pe_enabled`, sanitize
`allowed_enc_level`/`enc_policy`, bornes pad/IA, `crypto_select`
initiateur masqué, alertes/compteurs `disconnect()` pré-attach via
`disconnect_alert_counters`). Cinq déviations résiduelles + un edge
suivent — dont une qui **invalide une affirmation du rapport
round-1**.

### Corrections d'affirmations round-1

- La ligne « pas de re-validation de l'ih echo côté initiateur
  attaché (upstream saute le bloc sous `if (!t)`) » (section Bonus
  parité) est **fausse** : upstream ne saute pas le traitement de
  l'ih sur conn attachée — `read_info_hash` a une branche `else`
  qui valide l'écho et flippe `protocol_v2`
  (`bt_peer_connection.cpp:3482-3503`). Le commentaire Rust à
  `bt_peer_conn.rs:520-524` répète la même mauvaise lecture. → D16.

### D16 — écho info-hash jamais validé sur conn attachée (+ flip
`protocol_v2` par l'ih) — **wire-visible + état**

Sites : `bt_peer_conn.rs:384-387` (`run_handshake` — dial plaintext),
`:450-451` (`run_handshake_encrypted_initiator` — dial MSE),
`:519-528` (`run_handshake_encrypted_responder_prefix` — inbound
attaché par SKEY). Les trois parsent `hs` et l'utilisent sans jamais
comparer `hs.info_hash` à `associated_info_hash()`.

Upstream (`bt_peer_connection.cpp:3482-3503`, branche `else` de
`if (!t)` — donc toute conn déjà attachée : dialée, ou attachée par
SKEY côté inbound) :

- ih reçu == hash v2 tronqué **et** torrent `has_v2` →
  `peer_info_struct()->protocol_v2 = true` — le `write_handshake`
  qui suit (3512) emporte alors le hash **v2** (post-flip
  `associated_info_hash()`) et n'annonce plus le bit `0x10`.
- sinon ih reçu != `associated_info_hash()` (ou hash associé
  tout-à-zéro) → `disconnect(errors::invalid_info_hash, bittorrent,
  failure)` — alerte `peer_disconnected` + `disconnected_peers` +
  legs `error_*`.

Conséquences Rust : (a) un pair qui a prouvé le SKEY mais échoit un
ih arbitraire reste attaché au torrent résolu — upstream le tue à
sévérité failure (divergence état + compteurs/alertes manquants) ;
(b) le dial plaintext/MSE accepte pareillement un écho incohérent ;
(c) un inbound qui SKEYe en v1 sur hybride mais handshake en v2 n'est
pas upgradé `protocol_v2` (upstream : oui — et la réponse change de
hash). Fix : dans la branche attachée, `if echo == v2_ih && has_v2 →
protocol_v2=true` puis ré-évaluer `wire_ih`/`advertise_v2` ; `else if
echo != associated → WrongInfoHash` (déjà mappé `(Failure, Bittorrent,
"invalid_info_hash")`). Il faut passer `has_v2`/`v2_ih` aux jambes
initiator (disponibles via `peer_link_up`). **Gap de test** : aucun
test ne couvre l'écho ih sur conn attachée (les 3 sites) ni le flip
v2-par-écho.

### D17 — legs crypto absents de `peer_disconnected` post-attach —
**compteur**

`session.rs:4736-4781` (teardown de conn *attachée*, sev > normal) :
incrémente `error_tcp/utp` + `error_incoming/outgoing` mais pas
`error_encrypted_peers`/`error_rc4_peers` — upstream les compte dans
`disconnect()` quand `type()==bittorrent && op != connect &&
supports_encryption()` (+ `rc4_encrypted()` pour rc4)
(`peer_connection.cpp:4414-4422`). L'apply round-1 n'a couvert que
les conns pré-attach (`disconnect_alert_counters` :5063-5070). Fix :
lire `c.supports_encryption`/`c.rc4_encrypted` dans le lookup
`t.peers[peer]` (:4717-4722) et ajouter les deux legs sous
`sev > Normal`. **Gap de test** : teardown sev>normal d'une conn
chiffrée attachée.

### D18 — `st.encrypted` posé à l'entrée MSE, pas à la fin de la
négociation — **compteur**

`bt_peer_conn.rs:439,500` posent `st.encrypted = true` dès l'entrée
du handshake ; la doc :425-426 prétend que `m_encrypted` « sets at
MSE entry » upstream — faux : `m_encrypted` ne passe vrai qu'à
`init_bt_handshake`/fin de `read_pe_ia` (responder `3300`,
initiator `:3223/3278` → `init_bt_handshake`), `m_rc4_encrypted` à
`select_crypto` (`:666`, `:3199-3201`). Conséquence : tout échec MSE
en amont de la négociation (sync_hash_not_found, ih inconnu,
vc_not_zero, unsupported_encryption_mode, invalid_pad_size,
invalid_encrypt_handshake) incrémente `error_encrypted_peers` via
`fail.encrypted` là où upstream ne le compte pas — seuls les échecs
post-négociation le devraient. `st.rc4_encrypted` (posé à `mse_done`)
n'est pas surestimé. **Compteur uniquement.**

### D19 — PE4 non émis avant le refus de `len_pad`/`len_ia`
malformés — **wire-visible mineur**

`mse.rs:344-385` : le répondeur lit `[VC][provide][len_pad][pad]
[len_ia][ia]`, valide `len_pad>512` (:353), `len_ia>68` (:364) et le
`select` (:379) puis seulement écrit PE4 (:383-385). Upstream écrit
PE4 dès le `select` validé (`write_pe4_sync` :3183) puis valide
`len_pad` (:3204-3209 → `invalid_pad_size` peer_error) et `len_ia`
(:3250-3254 → `invalid_encrypt_handshake` peer_error). Un initiateur
fautif reçoit donc PE4-then-close upstream, close-sec en Rust —
distinguable sur le fil (fingerprint), sans impact interop. Le cas
`select==0` → pas de PE4 des deux côtés ✓. Fix optionnel : écrire
PE4 avant les validations pad/IA pour aligner l'empreinte.

### D20 — accepteur SSL : jambes d'échec partagées absentes —
**état/compteur** (périmètre partagé A23)

`engine.rs:4562-4582` : un échec du handshake BT post-TLS fait
`warn` + `NumPeersHalfOpen--` seulement — pas de
`peer_conn_failed_inbound` (donc ni `disconnected_peers`,
`error_incoming/tcp` à sev>normal, ni alerte `peer_disconnected`/
`peer_error`), ni la jambe `verify_secret_id`→`ban_ip` sur ih
inconnu — upstream `attach_to_torrent` l'applique à toute conn
(`peer_connection.cpp:1326-1338`, les conns SSL traversent
`read_info_hash` pareil). L'échec TLS lui-même poste `PeerError`
(:4500-4504) mais sans les compteurs `disconnect()`. L'exemption
MSE/politiques SSL est conforme (`!is_ssl` upstream).

### D21 — `accept_stream` (transports injectés) sans timeout de
handshake — **état**

`engine.rs:4898-5007` : ni `spawn_handshake_watchdog` ni
`tokio::time::timeout` autour de `run_handshake_responder_auto` — un
flux injecté (voies anonymes/i2p) qui fige en cours de handshake ne
meurt jamais : socket ouverte + `num_peers_half_open` gonflé.
Upstream arme `handshake_timeout`/inactivité sur toute conn
(`peer_connection` ctor → `on_tick`). L'accepteur plain et l'accepteur
SSL ont tous deux un timeout.

### Edge — `connect_peer` manuel ignore le flag SSL du torrent

`engine.rs:~3505-3570` : dial TCP direct + `peer_link_up(ih, remote,
false)` — un `connect_peer` API sur un torrent SSL partirait en
plaintext/MSE là où upstream `connect_to_peer` instancie
`ssl_stream` (le leg `ssl_params` de `spawn_dial` :3821-3843 n'y est
pas reproduit). Mineur — API manuelle hors pompe.

### Faux positifs / conformités re-confirmés

- Sévérité par défaut `disconnect()` = `normal`
  (`peer_connection_interface.hpp:45` + default arg) — le leg `0x10`
  sur v1-only (`invalid_info_hash` sans arg → normal) ✓ mappé
  `V2OnV1` → Normal.
- `error_peers` n'est incrémenté qu'à `error == peer_error`
  (`peer_connection.cpp:4353`) — Rust :4782 ✓ ; `m_failed` =
  `error > normal` → `sev > Normal` à :4729 ✓.
- `crypto_select == 0` → `unsupported_encryption_mode` @failure
  **avant** PE4 (upstream :3176-3183) — Rust :379-381 équivalent.
- `len_pad` borné avant la lecture du pad des deux côtés
  (upstream :3204-3209 ↔ `mse.rs:353`) ; `len_ia` lu dans le bloc
  pad déchiffré (`subspan` :3244 ↔ `tail[len_pad..]` :359) ✓.
- IA réinjectée (`cut(0,20)` :3313 ↔ préfixe `theirs[..ia]`
  bt_peer_conn.rs:510-515) ; `len_ia == 0` → lecture directe ✓.
- `fast_reconnect` : bras `pe_enabled+pe_support` uniquement
  (upstream :239-241 ↔ `torrent.rs:3796-3803`) ✓ ; re-arm
  `pe_support` post-PE4 gated `is_outgoing && pe_enabled`
  (`rearm_pe_support`, `session.rs:4959-4967`) ✓.
- Index skey v1+v2 tronqué ✓ (`known_torrent_by_skey`,
  `session.rs:5150-5164` = `m_obfuscated_index`).
- Pas de compteurs `disconnection_*_peers`/`conn_attempts` dans
  cette baseline — absence conforme.

### Gaps de tests résiduels

- écho ih sur conn attachée (3 sites) + flip `protocol_v2` par
  écho v2 (D16) — les tests `test_bt_peer_conn.rs` ne couvrent que
  la jambe non-attachée ;
- legs `error_encrypted`/`error_rc4` sur teardown post-attach (D17) ;
- fenêtre `st.encrypted` pré-négociation (D18) ;
- ordre PE4 sur entrée malformée (D19) ;
- comptabilisation d'échec accepteur SSL (D20) ;
- timeout handshake sur `accept_stream` (D21).
