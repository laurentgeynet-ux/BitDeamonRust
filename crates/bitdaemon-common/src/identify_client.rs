//! `aux::identify_client_impl` (`src/identify_client.cpp`) — decode a
//! peer-id into a human-readable "Client M.m.r[.t]" string.

use crate::peer_id::PeerId;

/// `fingerprint` — the parsed version tuple behind a client code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    /// two-char client code (`f.name`).
    pub name: [u8; 2],
    /// `major_version`.
    pub major: i32,
    /// `minor_version`.
    pub minor: i32,
    /// `revision_version`.
    pub revision: i32,
    /// `tag_version` — appended as `.t` when non-zero.
    pub tag: i32,
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
        major: 0,
        minor: 0,
        revision: 0,
        tag: 0,
    }
}

/// `parse_az_style` — "-AZ1234-" style.
fn parse_az_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if id[0] != b'-'
        || !id[1].is_ascii_graphic() && id[1] != b' '
        || !(id[3] >= b'0' && id[4] >= b'0' && id[5] >= b'0' && id[6] >= b'0')
        || id[7] != b'-'
    {
        return None;
    }
    let mut ret = fp([id[1], id[2]]);
    ret.major = decode_digit(id[3]);
    ret.minor = decode_digit(id[4]);
    ret.revision = decode_digit(id[5]);
    ret.tag = decode_digit(id[6]);
    Some(ret)
}

/// `parse_shadow_style` — "S5.7.6" letter + binary or "--" forms.
fn parse_shadow_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if !id[0].is_ascii_alphanumeric() {
        return None;
    }
    let mut ret = fp([id[0], 0]);
    if id[4] == b'-' && id[5] == b'-' {
        if id[1] < b'0' || id[2] < b'0' || id[3] < b'0' {
            return None;
        }
        ret.major = decode_digit(id[1]);
        ret.minor = decode_digit(id[2]);
        ret.revision = decode_digit(id[3]);
    } else {
        if id[8] != 0 || id[1] > 127 || id[2] > 127 || id[3] > 127 {
            return None;
        }
        ret.major = i32::from(id[1]);
        ret.minor = i32::from(id[2]);
        ret.revision = i32::from(id[3]);
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
    ret.major = decode_digit(id[3]);
    ret.minor = decode_digit(id[4]) * 10 + decode_digit(id[5]);
    ret.tag = decode_digit(id[6]);
    Some(ret)
}

/// `parse_mainline_style` — "M4-3-6--" (single char + dotted triplet).
fn parse_mainline_style(id: &[u8; 20]) -> Option<Fingerprint> {
    // sscanf "%1c%3d-%3d-%3d--"
    if !(id[0].is_ascii_graphic() || id[0] == b' ') {
        return None;
    }
    let mut pos = 1;
    let mut vals = [0i32; 3];
    for (vi, v) in vals.iter_mut().enumerate() {
        let mut n = 0i32;
        let mut digits = 0;
        while pos < 20 && id[pos].is_ascii_digit() && digits < 3 {
            n = n * 10 + i32::from(id[pos] - b'0');
            pos += 1;
            digits += 1;
        }
        if digits == 0 {
            return None;
        }
        *v = n;
        if vi < 2 {
            if pos >= 20 || id[pos] != b'-' {
                return None;
            }
            pos += 1;
        }
    }
    if pos + 1 >= 20 || id[pos] != b'-' || id[pos + 1] != b'-' {
        return None;
    }
    let mut ret = fp([id[0], 0]);
    ret.major = vals[0];
    ret.minor = vals[1];
    ret.revision = vals[2];
    Some(ret)
}

/// `name_map` — two/one-letter client codes, sorted (upstream
/// `identify_client.cpp`).
const NAME_MAP: &[(&str, &str)] = &[
    ("7T", "aTorrent for android"),
    ("A", "ABC"),
    ("AB", "AnyEvent BitTorrent"),
    ("AG", "Ares"),
    ("AR", "Arctic Torrent"),
    ("AT", "Artemis"),
    ("AV", "Avicora"),
    ("AX", "BitPump"),
    ("AZ", "Azureus"),
    ("A~", "Ares"),
    ("BB", "BitBuddy"),
    ("BC", "BitComet"),
    ("BE", "baretorrent"),
    ("BF", "Bitflu"),
    ("BG", "BTG"),
    ("BI", "BiglyBT"),
    ("BL", "BitBlinder"),
    ("BP", "BitTorrent Pro"),
    ("BR", "BitRocket"),
    ("BS", "BTSlave"),
    ("BT", "BitTorrent"),
    ("BU", "BigUp"),
    ("BW", "BitWombat"),
    ("BX", "BittorrentX"),
    ("CD", "Enhanced CTorrent"),
    ("CT", "CTorrent"),
    ("DE", "Deluge"),
    ("DP", "Propagate Data Client"),
    ("EB", "EBit"),
    ("ES", "electric sheep"),
    ("FC", "FileCroc"),
    ("FT", "FoxTorrent"),
    ("FW", "FrostWire"),
    ("FX", "Freebox BitTorrent"),
    ("GS", "GSTorrent"),
    ("HK", "Hekate"),
    ("HL", "Halite"),
    ("HN", "Hydranode"),
    ("IL", "iLivid"),
    ("KC", "Koinonein"),
    ("KG", "KGet"),
    ("KT", "KTorrent"),
    ("LC", "LeechCraft"),
    ("LH", "LH-ABC"),
    ("LK", "Linkage"),
    ("LP", "lphant"),
    ("LR", "LibreTorrent"),
    ("LT", "libtorrent"),
    ("LW", "Limewire"),
    ("M", "Mainline"),
    ("ML", "MLDonkey"),
    ("MO", "Mono Torrent"),
    ("MP", "MooPolice"),
    ("MR", "Miro"),
    ("MT", "Moonlight Torrent"),
    ("NX", "Net Transport"),
    ("O", "Osprey Permaseed"),
    ("OS", "OneSwarm"),
    ("OT", "OmegaTorrent"),
    ("PD", "Pando"),
    ("Q", "BTQueue"),
    ("QD", "QQDownload"),
    ("QT", "Qt 4"),
    ("R", "Tribler"),
    ("RT", "Retriever"),
    ("RZ", "RezTorrent"),
    ("S", "Shadow"),
    ("SB", "Swiftbit"),
    ("SD", "Xunlei"),
    ("SK", "spark"),
    ("SN", "ShareNet"),
    ("SS", "SwarmScope"),
    ("ST", "SymTorrent"),
    ("SZ", "Shareaza"),
    ("S~", "Shareaza (beta)"),
    ("T", "BitTornado"),
    ("TB", "Torch"),
    ("TL", "Tribler"),
    ("TN", "Torrent.NET"),
    ("TR", "Transmission"),
    ("TS", "TorrentStorm"),
    ("TT", "TuoTu"),
    ("U", "UPnP"),
    ("UL", "uLeecher"),
    ("UM", "uTorrent Mac"),
    ("UT", "uTorrent"),
    ("VG", "Vagaa"),
    ("WD", "WebTorrent Desktop"),
    ("WT", "BitLet"),
    ("WW", "WebTorrent"),
    ("WY", "FireTorrent"),
    ("XF", "Xfplay"),
    ("XL", "Xunlei"),
    ("XS", "XSwifter"),
    ("XT", "XanTorrent"),
    ("XX", "Xtorrent"),
    ("ZO", "Zona"),
    ("ZT", "ZipTorrent"),
    ("lt", "rTorrent"),
    ("pX", "pHoeniX"),
    ("qB", "qBittorrent"),
    ("st", "SharkTorrent"),
];

/// `generic_mappings` — (offset, needle, name) non-standard ids.
const GENERIC: &[(usize, &str, &str)] = &[
    (0, "Deadman Walking-", "Deadman"),
    (5, "Azureus", "Azureus 2.0.3.2"),
    (0, "DansClient", "XanTorrent"),
    (4, "btfans", "SimpleBT"),
    (0, "PRC.P---", "Bittorrent Plus! II"),
    (0, "P87.P---", "Bittorrent Plus!"),
    (0, "S587Plus", "Bittorrent Plus!"),
    (0, "martini", "Martini Man"),
    (0, "Plus---", "Bittorrent Plus"),
    (0, "turbobt", "TurboBT"),
    (0, "a00---0", "Swarmy"),
    (0, "a02---0", "Swarmy"),
    (0, "T00---0", "Teeweety"),
    (0, "BTDWV-", "Deadman Walking"),
    (2, "BS", "BitSpirit"),
    (0, "-SP", "BitSpirit 3.6"),
    (0, "Pando-", "Pando"),
    (0, "LIME", "LimeWire"),
    (0, "btuga", "BTugaXP"),
    (0, "oernu", "BTugaXP"),
    (0, "Mbrst", "Burst!"),
    (0, "PEERAPP", "PeerApp"),
    (0, "Plus", "Plus!"),
    (0, "-Qt-", "Qt"),
    (0, "exbc", "BitComet"),
    (0, "DNA", "BitTorrent DNA"),
    (0, "-G3", "G3 Torrent"),
    (0, "-FG", "FlashGet"),
    (0, "-ML", "MLdonkey"),
    (0, "-MG", "Media Get"),
    (0, "XBT", "XBT"),
    (0, "OP", "Opera"),
    (2, "RS", "Rufus"),
    (0, "AZ2500BT", "BitTyrant"),
    (0, "btpd/", "BitTorrent Protocol Daemon"),
    (0, "TIX", "Tixati"),
    (0, "QVOD", "Qvod"),
];

/// `lookup` — format "Name M.m.r[.t]" via `name_map`.
fn lookup(f: &Fingerprint) -> String {
    let key = match f.name[1] {
        0 => String::from_utf8_lossy(&f.name[..1]).into_owned(),
        _ => String::from_utf8_lossy(&f.name).into_owned(),
    };
    let name = NAME_MAP
        .binary_search_by(|(id, _)| (*id).cmp(key.as_str()))
        .map_or(key.as_str(), |i| NAME_MAP[i].1);
    let mut s = format!("{name} {}.{}.{}", f.major, f.minor, f.revision);
    if f.tag != 0 {
        s.push_str(&format!(".{}", f.tag));
    }
    s
}

fn starts_at(id: &[u8; 20], offset: usize, needle: &str) -> bool {
    id[offset..].starts_with(needle.as_bytes())
}

/// `aux::identify_client_impl` — full decode order verbatim.
#[must_use]
pub fn identify_client(p: &PeerId) -> String {
    let id = p.as_bytes();
    if p.is_all_zeros() {
        return "Unknown".to_string();
    }
    for (off, needle, name) in GENERIC {
        if starts_at(id, *off, needle) {
            return (*name).to_string();
        }
    }
    if starts_at(id, 0, "-BOW") && id[7] == b'-' {
        return format!("Bits on Wheels {}", String::from_utf8_lossy(&id[4..7]));
    }
    if starts_at(id, 0, "eX") {
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
        s.push(if c.is_ascii_graphic() || c == b' ' {
            c as char
        } else {
            '.'
        });
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
    fn shadow_style() {
        assert_eq!(
            identify_client(&pid("S587--xxxxxxxxxxxxxxxx")),
            "Shadow 5.8.7"
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
