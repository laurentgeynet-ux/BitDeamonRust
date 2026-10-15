# Audit A18 — rapport d'analyse

Périmètre : `file_storage` sanitize (`..`, ADS, noms réservés, drives,
abs), symlinks, doublons de chemins ; parsing metainfo malformé.
Baseline upstream : `e7381c7bc` (`src/file_storage.cpp`,
`src/torrent_info.cpp`, `src/path.cpp`, `src/load_torrent.cpp`,
`src/resolve_duplicate_filenames.cpp`).

## Verdicts checklist

- `..` traversal : sanitization identique upstream (éléments retirés/`_`,
  pas rejet global) — verdict : **conforme** — preuve :
  `crates/bitdaemon-core/src/path.rs:476-600` (`sanitize_append_path_element`
  : `"."` court-circuité si `!force_element` :477 ; un élément composé
  uniquement de 1-2 points → `"_"` ou revert :563-575) vs
  `src/torrent_info.cpp:165,290-303`. Composants `"."`/`".."` des
  `symlink path` droppés : `torrent_info.rs:809-810,944-945` vs
  `torrent_info.cpp:440,617`.
- ADS `file:`/`file$`, noms réservés Windows (CON/PRN/AUX/NUL/COM1-9/
  LPT1-9) → renaming identique — verdict : **conforme (avec réserve)** —
  preuve : `:` rejeté par `valid_path_character` sous `cfg(windows)`
  (`utf8.rs:156-176`, `invalid_chars = "?<>\"|\b*:"`) → `_`, ce qui casse
  les ADS `file:stream` ; cf. upstream `torrent_info.cpp:69-92`. Le tableau
  `reserved_names[]` (`torrent_info.cpp:191-214`) n'existe que sous
  `#if !TORRENT_USE_UNC_PATHS && TORRENT_WINDOWS` — build Windows
  *déprécié* ; le build par défaut (UNC activé) ne renomme pas. Rust ne
  l'implémente pas → conforme au build par défaut. NB : `clock$` et
  `com0`/`lpt0` (non couverts par l'énoncé) sont aussi exclus du même
  gate upstream.
- Drive letters `C:`, abs paths, UNC → traités comme upstream — verdict :
  **conforme** — preuve : `is_complete` (`path.rs:425-446`, formes
  `xx:\`, `xx:/`, `\\`, POSIX `/`) identique à `path.cpp` ; abs →
  `PATH_IS_ABSOLUTE` dans `update_path_index`
  (`file_storage.rs:318-323` vs `file_storage.cpp:130-136`). Sous Windows
  `:` devient `_` en sanitization avant même d'arriver ici — même ordre
  upstream.
- Symlink flag : création, dangling links, `file_symlink` paths — verdict
  : **conforme** — preuve : `extract_single_file2`/`extract_single_file`
  extraient `symlink path` avec drop de `.`/`..` + `force_element=true`
  (`torrent_info.rs:797-815,934-955` vs `torrent_info.cpp:419-446,
  596-630` ; `symlink path` absent → flag retiré :950-954). Pass deux
  `sanitize_symlinks` (`file_storage.rs:976-1167`) fidèle à
  `file_storage.cpp:1518-1717` : cible absolue → pointe sur soi-même
  (:1041-1047), dir_links, second passage avec `traversed` anti-cycle et
  branches = préfixes cumulatifs via `lsplit_path_at` (:1128-1155).
  Targets dangling → `m_symlinks[i] = internal_file_path(i)` (:1163-1166).
- Doublons de chemins : disambiguation ou erreur — verdict : **conforme**
  — preuve : `resolve.rs:39-158` porte fast-path hash + slow-path
  `.N`-suffix (base+`.cnt`+ext, `num_collisions > max` →
  `TooManyDuplicateFilenames` :152-155), pad-files de même taille
  autorisés à collider (:124-133), répertoires insérés en premier
  (:64-92) ; appelé depuis `parse_torrent_file`
  (`torrent_info.rs:1350-1354`) comme `load_torrent.cpp:383`.
  `file_path_hash`/`all_path_hashes` crc32c-lowercase :
  `file_storage.rs:793-877`.
- Metainfo : piece_length 0/négatif/énorme, sizes négatives, utf-8
  invalide, bencode non-canonique, piece string length mismatch → erreurs
  exactes — verdict : **déviation (mineure)** — preuve : checks
  `piece_length <= 0 || > MAX_PIECE_SIZE` → `TorrentMissingPieceLength`
  (`torrent_info.rs:314-325` = upstream :1210-1223, incl. pow2/≥16KiB en
  v2) ; `file_size < 0`/trop gros → `TorrentInvalidLength`
  (`torrent_info.rs:785-790,861-866` = upstream :407-413,503-509) ;
  `pieces` length ≠ `num_pieces*20` → `TorrentInvalidHashes`
  (:452-458 = upstream :1423-1429) ; utf-8 invalide → `_` via sanitize
  (path.rs:510-519) et `verify_encoding` comment/created_by
  (:1327,1337 = load_torrent.cpp:312-316) ; bencode non-canonique →
  `bdecode_limits` + `has_soft_error` gate `version > 0` (:284-286 =
  upstream :1173-1180). **Déviations** : codes d'erreur `add_file`
  (file_too_large / filename_too_long), `metadata_too_large` absent,
  `magnet-uri` fallback absent — cf. `## Déviations`.
- Zero-length files & pad files : création ignorée, comptage — verdict :
  **conforme** — preuve : fichiers de taille 0 ajoutés et comptés
  (`add_file` `file_storage.rs:240-280` ; le check v1/v2 saute les
  fichiers vides :223-238 comme `file_storage.cpp:862-881`) ; pad files
  exclus de `size_on_disk` (:277-280) ; pads v2 auto-générés `.pad/<size>`
  (:285-305 vs :939-964) ; bitcomet `_____padding_file_` → flag pad
  (`torrent_info.rs:929-932`) ; v1 pad sans path → `.pad/<size>`
  (:920-924 = upstream :570-577) ; v2 pad déclaré →
  `TorrentInvalidPadFile` (:774-776). La non-création disque des pads est
  couverte par l'audit A14/A15, pas ici.

## Déviations

- `crates/bitdaemon-core/src/file_storage.rs:191-196` —
  `add_file` retourne `TorrentError::TorrentInvalidLength` quand
  `file_size > MAX_FILE_SIZE` ; upstream
  (`src/file_storage.cpp:819-823`, `add_file_borrow_impl`) retourne
  `boost::system::errc::file_too_large` (generic_category), et seul le
  check suivant (`max_file_offset - m_total_size < file_size`) donne
  `torrent_invalid_length`. Sévérité : état/erreur (les tests upstream
  `test_torrent_info` distinguent les codes).
  - **sketch** : ajouter une erreur errno-like. Soit un variant
    `Error::Errno(std::io::Error)` portant `std::io::Error::from_raw_os_error(
    ENAMETOOLONG/EFBIG)` — Windows : `EFBIG`=132? Non : utiliser
    `std::io::Error::new(ErrorKind::FileTooLarge…)` — ErrorKind a
    `FileTooLarge` (unstable) ; plus simple : variants dédiés
    `TorrentError::FileTooLarge`/`FilenameTooLong` documentés comme
    `errc::file_too_large`/`filename_too_long`, et
    `file_size > Self::MAX_FILE_SIZE → FileTooLarge`, garder
    `TorrentInvalidLength` pour l'overflow d'offset. Upstream ref :
    `file_storage.cpp:819-829`. Test : `add_file` avec size = 2^48 →
    erreur FileTooLarge ; size qui déborde l'offset cumulé →
    TorrentInvalidLength.
- `crates/bitdaemon-core/src/file_storage.rs:198-210` —
  `filename >= 4096` retourne `Error::Io(InvalidInput, "filename too
  long")` ; upstream `errc::filename_too_long` (`file_storage.cpp:831-843`).
  Sévérité : erreur.
  - **sketch** : remplacer les deux `Error::Io(...)` par le variant
    `FilenameTooLong` du sketch précédent. Upstream : `file_storage.cpp:831-843`.
- `crates/bitdaemon-core/src/torrent_info.rs:272-275` —
  pas de check `section.size() >= i32::MAX` → `metadata_too_large` ;
  upstream `parse_info_section_impl` (`torrent_info.cpp:1145-1150`).
  Pratiquement unreachable (buffers cappés à 10/30 MiB) mais l'API
  publique `from_info_node` diverge. Sévérité : cosmétique/défense.
  - **sketch** : ajouter `TorrentError::MetadataTooLarge` (doc
    `metadata_too_large`) et, après `let section = info.data_section();`,
    `if section.len() >= i32::MAX as usize { return
    Err(TorrentError::MetadataTooLarge.into()); }` avant le check `d…e`.
    Upstream : `torrent_info.cpp:1145-1150`.
- `crates/bitdaemon-core/src/torrent_info.rs:1366-1368` —
  `load_torrent_buffer` rejette `buffer.len() > max_buffer_size` avec
  `InvalidBencoding`. Upstream ne borne que `load_file`
  (`load_torrent.cpp:29-56`, erreur `metadata_too_large`) — le chemin
  buffer ne vérifie pas la taille. Sévérité : erreur + léger sur-rejet.
  - **sketch** : soit garder le cap mais retourner
    `TorrentError::MetadataTooLarge` au lieu de `InvalidBencoding`
    (closest upstream : `load_file` → `metadata_too_large`), soit
    l'aligner strictement en ne vérifiant que dans le futur
    `load_file` équivalent. Recommandation : garder le cap (défense
    raisonnable) mais corriger le code d'erreur. Test : buffer de
    `max_buffer_size+1` → MetadataTooLarge.
- `crates/bitdaemon-core/src/torrent_info.rs:1166-1172` —
  `parse_torrent_file` retourne `TorrentMissingInfo` quand `info`
  manque ; upstream tente `dict_find_string("magnet-uri")` →
  `parse_magnet_uri` dans `out` puis retourne succès sans `ti`
  (`load_torrent.cpp:160-171`). Le commentaire Rust « both paths report
  missing info » est incorrect. Sévérité : comportemental (un fichier
  `.torrent`-magnet est accepté upstream).
  - **sketch** : dans la branche `!info.is_valid()` :
    `if let Ok(uri) = … dict_find_string(b"magnet-uri") …` →
    `crate::magnet_uri::parse_magnet_uri(uri.string_value(), &mut out)?`
    puis `return Ok(out)` (avec `out.ti = None`). Upstream :
    `load_torrent.cpp:159-171`. Test : `.torrent` minimal
    `d10:magnet-uri…e` → `Ok(atp)` avec info_hashes remplis.

## Fichiers du fix

- `crates/bitdaemon-core/src/error.rs` (nouveaux variants)
- `crates/bitdaemon-core/src/file_storage.rs` (codes d'erreur `add_file`)
- `crates/bitdaemon-core/src/torrent_info.rs` (`metadata_too_large`,
  `magnet-uri` fallback)
- `docs/PORTING.md` (rows file_storage/torrent_info — notes d'erreur)
- `docs/audits/changelog/A18.md`

## Non-bugs tranchés

- Noms réservés Windows (`con`, `prn`, `aux`, `clock$`, `nul`, `com0-9`,
  `lpt0-9`) non renommés : le code upstream est derrière
  `#if !TORRENT_USE_UNC_PATHS && TORRENT_WINDOWS` — build déprécié
  (`#pragma message` upstream le signale). Le build Windows standard
  (UNC activé) se comporte comme le port Rust.
- `m_files.set_piece_length(0)` « mark invalid » upstream : sans objet
  ici — `Err` court-circuite avant construction. Équivalent.
- `utf8_sequence_length` accepte les lead bytes 5 octets (`111110xx`)
  pour les skipper comme séquence invalide (`utf8.rs:22-26`) — même choix
  upstream (`utf8.cpp`), pas un élargissement d'acceptation.
- Le fast-path de `resolve_duplicate_filenames` (hash-only) tombe dans le
  slow-path à la première collision de hash — faux positif CRC inclus,
  comme upstream (`resolve_duplicate_filenames.cpp:174-190`).

## Résultat

Apply `audit/18-path-metainfo-security` — les 5 déviations corrigées :

- `add_file` : `file_size > max_file_size` → `Error::FileTooLarge`
  (`errc::file_too_large`) ; `filename ≥ 4096` → `Error::FilenameTooLong`
  (`errc::filename_too_long`) ; seul l'overflow d'offset cumulé reste
  `torrent_invalid_length` (`file_storage.cpp:819-843`).
- `from_info_node` : info dict ≥ `i32::MAX` → `TorrentError::MetadataTooLarge`
  (= 100, valeur upstream).
- `load_torrent_buffer` : cap `max_buffer_size` → `MetadataTooLarge`
  (était `InvalidBencoding`).
- `parse_torrent_file` : fallback `magnet-uri` implémenté
  (`load_torrent.cpp:159-171`) — `.torrent`-magnet → `Ok(params)`,
  `ti = None`.
- Bonus en passant : `TooManyDuplicateFilenames` repr corrigé à la
  valeur upstream 145 (était 22 implicite).

Tests ajoutés : `add_file_size_and_name_errors`,
`magnet_uri_torrent_file`, `load_torrent_buffer_size_cap`.
Gates : fmt ✓ · clippy `-D warnings` ✓ · `cargo test --workspace`
(113 suites, 0 échec) ✓ · `cargo doc --workspace --no-deps` ✓.
