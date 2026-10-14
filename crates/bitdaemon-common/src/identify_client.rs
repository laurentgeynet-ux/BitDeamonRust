//! `aux::identify_client_impl` (`src/identify_client.cpp`) — decode a
//! peer-id into a human-readable "Client M.m.r[.t]" string.
//!
//! Upstream compares `peer_id` bytes through C `char` (signed on the
//! reference platform): `< '0'` rejects bytes ≥ 0x80 as well as control
//! bytes, and raw-byte version fields sign-extend. Those semantics are
//! reproduced here — see `signed_ge_zero` and `scan_d3`.

use crate::peer_id::PeerId;

/// `fingerprint` — the parsed version tuple behind a client code.
/// Only an internal step toward the display string; `lt::fingerprint`
/// proper (with `generate_fingerprint`) lives in `core::fingerprint`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    /// two-char client code (`f.name`).
    pub name: [u8; 2],
    /// `major_version`.
    pub major_version: i32,
    /// `minor_version`.
    pub minor_version: i32,
    /// `revision_version`.
    pub revision_version: i32,
    /// `tag_version` — appended as `.t` when non-zero.
    pub tag_version: i32,
}

/// `aux::is_print` — `c >= 32 && c < 127`.
fn is_print(c: u8) -> bool {
    (0x20..0x7f).contains(&c)
}

/// `id[i] < '0'` under upstream's signed-`char` comparisons — a byte
/// ≥ 0x80 is negative and therefore also compares *less* than `'0'`.
/// The accepted range is `0x30..=0x7f` (`decode_digit` handles the
/// non-digits inside it, exactly like upstream).
fn signed_ge_zero(c: u8) -> bool {
    (b'0'..0x80).contains(&c)
}

fn decode_digit(c: u8) -> i32 {
    if c.is_ascii_digit() {
        i32::from(c - b'0')
    } else {
        i32::from(c) - i32::from(b'A') + 10
    }
}

const fn fp(name: [u8; 2]) -> Fingerprint {
    Fingerprint {
        name,
        major_version: 0,
        minor_version: 0,
        revision_version: 0,
        tag_version: 0,
    }
}

/// `parse_az_style` — "-AZ1234-" style.
fn parse_az_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if id[0] != b'-'
        || !is_print(id[1])
        || !id[2..7].iter().all(|&c| signed_ge_zero(c))
        || id[7] != b'-'
    {
        return None;
    }
    let mut ret = fp([id[1], id[2]]);
    ret.major_version = decode_digit(id[3]);
    ret.minor_version = decode_digit(id[4]);
    ret.revision_version = decode_digit(id[5]);
    ret.tag_version = decode_digit(id[6]);
    Some(ret)
}

/// `parse_shadow_style` — "S5.7.6" letter + binary or "--" forms.
fn parse_shadow_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if !id[0].is_ascii_alphanumeric() {
        return None;
    }
    let mut ret = fp([id[0], 0]);
    if id[4] == b'-' && id[5] == b'-' {
        if !(signed_ge_zero(id[1]) && signed_ge_zero(id[2]) && signed_ge_zero(id[3])) {
            return None;
        }
        ret.major_version = decode_digit(id[1]);
        ret.minor_version = decode_digit(id[2]);
        ret.revision_version = decode_digit(id[3]);
    } else {
        // upstream's `id[i] > 127` tests are dead code: `char` promotes
        // to a signed int ≤ 127. Versions sign-extend the raw bytes —
        // a 0x80 version field becomes -128 upstream, and here.
        if id[8] != 0 {
            return None;
        }
        ret.major_version = i32::from(id[1] as i8);
        ret.minor_version = i32::from(id[2] as i8);
        ret.revision_version = i32::from(id[3] as i8);
    }
    Some(ret)
}

/// `parse_transmission_legacy_style` — "-TRXYYR-" (0.80–3.00).
fn parse_transmission_legacy(id: &[u8; 20]) -> Option<Fingerprint> {
    if id[0] != b'-' || id[1] != b'T' || id[2] != b'R' || id[7] != b'-' {
        return None;
    }
    if id[3] < b'0' || id[3] > b'3' {
        return None;
    }
    if !id[4].is_ascii_digit() || !id[5].is_ascii_digit() {
        return None;
    }
    if !id[6].is_ascii_digit() && !id[6].is_ascii_uppercase() {
        return None;
    }
    let mut ret = fp(*b"TR");
    ret.major_version = decode_digit(id[3]);
    ret.minor_version = decode_digit(id[4]) * 10 + decode_digit(id[5]);
    ret.tag_version = decode_digit(id[6]);
    Some(ret)
}

/// C-locale `isspace` — the characters `sscanf` skips before `%d`.
fn is_scan_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// sscanf `%3d`: skip whitespace, then an item of at most 3 chars —
/// an optional `+`/`-` sign plus at least one digit. Returns `None`
/// on a matching failure (lone sign, non-digit, end of string).
fn scan_d3(id: &[u8; 20], pos: &mut usize) -> Option<i32> {
    while *pos < 20 && is_scan_space(id[*pos]) {
        *pos += 1;
    }
    let mut width = 3;
    let mut neg = false;
    if *pos < 20 && (id[*pos] == b'+' || id[*pos] == b'-') {
        neg = id[*pos] == b'-';
        *pos += 1;
        width -= 1;
    }
    let mut v = 0i32;
    let mut n = 0;
    while *pos < 20 && id[*pos].is_ascii_digit() && n < width {
        v = v * 10 + i32::from(id[*pos] - b'0');
        *pos += 1;
        n += 1;
    }
    (n > 0).then_some(if neg { -v } else { v })
}

/// `parse_mainline_style` — "M4-3-6--" (single char + dotted triplet).
fn parse_mainline_style(id: &[u8; 20]) -> Option<Fingerprint> {
    // sscanf "%1c%3d-%3d-%3d--": upstream checks `is_print(name[0])`
    // after a successful scan — testing it up-front is equivalent,
    // since a failed scan rejects either way.
    if !is_print(id[0]) {
        return None;
    }
    let mut pos = 1;
    let major_version = scan_d3(id, &mut pos)?;
    if pos >= 20 || id[pos] != b'-' {
        return None;
    }
    pos += 1;
    let minor_version = scan_d3(id, &mut pos)?;
    if pos >= 20 || id[pos] != b'-' {
        return None;
    }
    pos += 1;
    let revision_version = scan_d3(id, &mut pos)?;
    if pos + 1 >= 20 || id[pos] != b'-' || id[pos + 1] != b'-' {
        return None;
    }
    Some(Fingerprint {
        name: [id[0], 0],
        major_version,
        minor_version,
        revision_version,
        tag_version: 0,
    })
}

/// `name_map` — two/one-letter client codes, sorted (upstream
/// `identify_client.cpp`). Keys are the raw 2 bytes upstream compares
/// in `compare_id` (single-letter ids carry a NUL second byte).
const NAME_MAP: &[(&[u8; 2], &str)] = &[
    (b"7T", "aTorrent for android"),
    (b"A\0", "ABC"),
    (b"AB", "AnyEvent BitTorrent"),
    (b"AG", "Ares"),
    (b"AR", "Arctic Torrent"),
    (b"AT", "Artemis"),
    (b"AV", "Avicora"),
    (b"AX", "BitPump"),
    (b"AZ", "Azureus"),
    (b"A~", "Ares"),
    (b"BB", "BitBuddy"),
    (b"BC", "BitComet"),
    (b"BE", "baretorrent"),
    (b"BF", "Bitflu"),
    (b"BG", "BTG"),
    (b"BI", "BiglyBT"),
    (b"BL", "BitBlinder"),
    (b"BP", "BitTorrent Pro"),
    (b"BR", "BitRocket"),
    (b"BS", "BTSlave"),
    (b"BT", "BitTorrent"),
    (b"BU", "BigUp"),
    (b"BW", "BitWombat"),
    (b"BX", "BittorrentX"),
    (b"CD", "Enhanced CTorrent"),
    (b"CT", "CTorrent"),
    (b"DE", "Deluge"),
    (b"DP", "Propagate Data Client"),
    (b"EB", "EBit"),
    (b"ES", "electric sheep"),
    (b"FC", "FileCroc"),
    (b"FT", "FoxTorrent"),
    (b"FW", "FrostWire"),
    (b"FX", "Freebox BitTorrent"),
    (b"GS", "GSTorrent"),
    (b"HK", "Hekate"),
    (b"HL", "Halite"),
    (b"HN", "Hydranode"),
    (b"IL", "iLivid"),
    (b"KC", "Koinonein"),
    (b"KG", "KGet"),
    (b"KT", "KTorrent"),
    (b"LC", "LeechCraft"),
    (b"LH", "LH-ABC"),
    (b"LK", "Linkage"),
    (b"LP", "lphant"),
    (b"LR", "LibreTorrent"),
    (b"LT", "libtorrent"),
    (b"LW", "Limewire"),
    (b"M\0", "Mainline"),
    (b"ML", "MLDonkey"),
    (b"MO", "Mono Torrent"),
    (b"MP", "MooPolice"),
    (b"MR", "Miro"),
    (b"MT", "Moonlight Torrent"),
    (b"NX", "Net Transport"),
    (b"O\0", "Osprey Permaseed"),
    (b"OS", "OneSwarm"),
    (b"OT", "OmegaTorrent"),
    (b"PD", "Pando"),
    (b"Q\0", "BTQueue"),
    (b"QD", "QQDownload"),
    (b"QT", "Qt 4"),
    (b"R\0", "Tribler"),
    (b"RT", "Retriever"),
    (b"RZ", "RezTorrent"),
    (b"S\0", "Shadow"),
    (b"SB", "Swiftbit"),
    (b"SD", "Xunlei"),
    (b"SK", "spark"),
    (b"SN", "ShareNet"),
    (b"SS", "SwarmScope"),
    (b"ST", "SymTorrent"),
    (b"SZ", "Shareaza"),
    (b"S~", "Shareaza (beta)"),
    (b"T\0", "BitTornado"),
    (b"TB", "Torch"),
    (b"TL", "Tribler"),
    (b"TN", "Torrent.NET"),
    (b"TR", "Transmission"),
    (b"TS", "TorrentStorm"),
    (b"TT", "TuoTu"),
    (b"U\0", "UPnP"),
    (b"UL", "uLeecher"),
    (b"UM", "uTorrent Mac"),
    (b"UT", "uTorrent"),
    (b"VG", "Vagaa"),
    (b"WD", "WebTorrent Desktop"),
    (b"WT", "BitLet"),
    (b"WW", "WebTorrent"),
    (b"WY", "FireTorrent"),
    (b"XF", "Xfplay"),
    (b"XL", "Xunlei"),
    (b"XS", "XSwifter"),
    (b"XT", "XanTorrent"),
    (b"XX", "Xtorrent"),
    (b"ZO", "Zona"),
    (b"ZT", "ZipTorrent"),
    (b"lt", "rTorrent"),
    (b"pX", "pHoeniX"),
    (b"qB", "qBittorrent"),
    (b"st", "SharkTorrent"),
];

/// `generic_mappings` — (offset, needle, name) non-standard ids.
const GENERIC: &[(usize, &[u8], &str)] = &[
    (0, b"Deadman Walking-", "Deadman"),
    (5, b"Azureus", "Azureus 2.0.3.2"),
    (0, b"DansClient", "XanTorrent"),
    (4, b"btfans", "SimpleBT"),
    (0, b"PRC.P---", "Bittorrent Plus! II"),
    (0, b"P87.P---", "Bittorrent Plus!"),
    (0, b"S587Plus", "Bittorrent Plus!"),
    (0, b"martini", "Martini Man"),
    (0, b"Plus---", "Bittorrent Plus"),
    (0, b"turbobt", "TurboBT"),
    (0, b"a00---0", "Swarmy"),
    (0, b"a02---0", "Swarmy"),
    (0, b"T00---0", "Teeweety"),
    (0, b"BTDWV-", "Deadman Walking"),
    (2, b"BS", "BitSpirit"),
    (0, b"-SP", "BitSpirit 3.6"),
    (0, b"Pando-", "Pando"),
    (0, b"LIME", "LimeWire"),
    (0, b"btuga", "BTugaXP"),
    (0, b"oernu", "BTugaXP"),
    (0, b"Mbrst", "Burst!"),
    (0, b"PEERAPP", "PeerApp"),
    (0, b"Plus", "Plus!"),
    (0, b"-Qt-", "Qt"),
    (0, b"exbc", "BitComet"),
    (0, b"DNA", "BitTorrent DNA"),
    (0, b"-G3", "G3 Torrent"),
    (0, b"-FG", "FlashGet"),
    (0, b"-ML", "MLdonkey"),
    (0, b"-MG", "Media Get"),
    (0, b"XBT", "XBT"),
    (0, b"OP", "Opera"),
    (2, b"RS", "Rufus"),
    (0, b"AZ2500BT", "BitTyrant"),
    (0, b"btpd/", "BitTorrent Protocol Daemon"),
    (0, b"TIX", "Tixati"),
    (0, b"QVOD", "Qvod"),
];

/// `lookup` — format "Name M.m.r[.t]" via `name_map`.
fn lookup(f: &Fingerprint) -> String {
    // `compare_id` + `lower_bound` upstream: byte-lexical on the 2-byte
    // code. Unknown codes fall back to the letters themselves (a
    // non-UTF-8 code renders lossy — `String` cannot carry raw bytes,
    // the only intentional difference from upstream's `snprintf`).
    let name = NAME_MAP
        .binary_search_by(|(id, _)| id[..].cmp(&f.name[..]))
        .ok()
        .map_or_else(
            || {
                let end = if f.name[1] == 0 { 1 } else { 2 };
                String::from_utf8_lossy(&f.name[..end]).into_owned()
            },
            |i| NAME_MAP[i].1.to_string(),
        );
    let mut s = format!(
        "{name} {}.{}.{}",
        f.major_version, f.minor_version, f.revision_version
    );
    if f.tag_version != 0 {
        s.push_str(&format!(".{}", f.tag_version));
    }
    s
}

/// `find_string` — `id` starts with `needle`.
fn find_string(id: &[u8], needle: &[u8]) -> bool {
    id.starts_with(needle)
}

/// `aux::identify_client_impl` — full decode order verbatim.
#[must_use]
pub fn identify_client(p: &PeerId) -> String {
    let id = p.as_bytes();
    if p.is_all_zeros() {
        return "Unknown".to_string();
    }
    for (off, needle, name) in GENERIC {
        if find_string(&id[*off..], needle) {
            return (*name).to_string();
        }
    }
    if find_string(id, b"-BOW") && id[7] == b'-' {
        return format!("Bits on Wheels {}", String::from_utf8_lossy(&id[4..7]));
    }
    if find_string(id, b"eX") {
        return format!("eXeem ('{}')", String::from_utf8_lossy(&id[2..14]));
    }
    let is_equ_zero = id[..12].iter().all(|&b| b == 0);
    if is_equ_zero && id[12] == 0x97 {
        return "Experimental 3.2.1b2".to_string();
    }
    if is_equ_zero && id[12] == 0 {
        return "Experimental 3.1".to_string();
    }
    for parse in [
        parse_transmission_legacy,
        parse_az_style,
        parse_shadow_style,
        parse_mainline_style,
    ] {
        if let Some(f) = parse(id) {
            return lookup(&f);
        }
    }
    if is_equ_zero {
        return "Generic".to_string();
    }
    let mut s = String::from("Unknown [");
    for &c in id {
        s.push(if is_print(c) { c as char } else { '.' });
    }
    s.push(']');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(s: &str) -> PeerId {
        let mut b = [0u8; 20];
        let n = s.len().min(20);
        b[..n].copy_from_slice(&s.as_bytes()[..n]);
        PeerId::from_bytes(&b).unwrap()
    }

    #[test]
    fn az_style_utorrent() {
        assert_eq!(
            identify_client(&pid("-UT3530-xxxxxxxxxxxx")),
            "uTorrent 3.5.3"
        );
    }

    #[test]
    fn az_style_qbittorrent() {
        assert_eq!(
            identify_client(&pid("-qB4500-xxxxxxxxxxxx")),
            "qBittorrent 4.5.0"
        );
    }

    #[test]
    fn az_style_rejects_low_bytes() {
        // upstream `id[2] < '0'` rejects a control byte at position 2…
        assert_eq!(
            identify_client(&pid("-UT\x0a530-xxxxxxxxxxxx")),
            "Unknown [-UT.530-xxxxxxxxxxxx]"
        );
        // …and a byte ≥ 0x80 (negative under signed char) at positions
        // 2-6, while non-digit bytes inside 0x30..=0x7f decode through
        // `decode_digit` (';' → 4) exactly like upstream.
        assert_eq!(
            identify_client(&pid("-UT;530-xxxxxxxxxxxx")),
            "uTorrent 4.5.3"
        );
        let mut b = [b'x'; 20];
        b[..8].copy_from_slice(b"-UT\x80530-");
        // byte 0x80 at position 3 renders '.' — az-style rejects it.
        assert_eq!(
            identify_client(&PeerId::from_bytes(&b).unwrap()),
            "Unknown [-UT.530-xxxxxxxxxxxx]"
        );
    }

    #[test]
    fn shadow_style() {
        assert_eq!(
            identify_client(&pid("S587--xxxxxxxxxxxxxxxx")),
            "Shadow 5.8.7"
        );
    }

    #[test]
    fn shadow_binary_sign_extends() {
        // bytes ≥ 0x80 sign-extend (upstream's `> 127` check is dead
        // code on signed-char builds).
        let mut b = [0u8; 20];
        b[0] = b'S';
        b[1] = 0x85;
        b[2] = 0x07;
        b[3] = 0x06;
        assert_eq!(
            identify_client(&PeerId::from_bytes(&b).unwrap()),
            "Shadow -123.7.6"
        );
    }

    #[test]
    fn mainline_style() {
        assert_eq!(
            identify_client(&pid("M4-3-6--xxxxxxxxxxxxx")),
            "Mainline 4.3.6"
        );
    }

    #[test]
    fn mainline_sscanf_semantics() {
        // `%3d` skips whitespace and accepts a sign — upstream parses
        // "M-4-3-6--" as Mainline -4.3.6.
        assert_eq!(
            identify_client(&pid("M-4-3-6--xxxxxxxxxxxxx")),
            "Mainline -4.3.6"
        );
        assert_eq!(
            identify_client(&pid("M 4- 3- 6--xxxxxxxxxxx")),
            "Mainline 4.3.6"
        );
        // but a literal '-' right after whitespace is not a digit.
        assert_eq!(
            identify_client(&pid("M -4-3-6--xxxxxxxxxxxx")),
            "Mainline -4.3.6"
        );
    }

    #[test]
    fn transmission_legacy() {
        assert_eq!(
            identify_client(&pid("-TR3000-xxxxxxxxxxxx")),
            "Transmission 3.0.0"
        );
    }

    #[test]
    fn generic_and_unknown() {
        assert_eq!(identify_client(&pid("TIX01xxxxxxxxxxxxxxx")), "Tixati");
        let zeros = PeerId::default();
        assert_eq!(identify_client(&zeros), "Unknown");
        let mut g = [0u8; 20];
        g[12] = b'x';
        assert_eq!(identify_client(&PeerId::from_bytes(&g).unwrap()), "Generic");
        let s = identify_client(&pid("zzzzzzzzzzzzzzzzzzzz"));
        assert!(s.starts_with("Unknown ["), "{s}");
    }
}
