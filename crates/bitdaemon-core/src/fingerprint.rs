//! `fingerprint.cpp` + `identify_client.cpp` — peer-id fingerprints and
//! the client-identification tables.
//!
//! The two lookup tables (`name_map`, `generic_mappings`) are the
//! upstream ones, verbatim — keep them ordered (`name_map` is
//! binary-searched).

use bitdaemon_common::PeerId;

/// `lt::fingerprint` — `-XX1234-` style client id + version digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint {
    /// Two-letter client code (second byte may be 0 for one-letter ids).
    pub name: [u8; 2],
    /// `major_version`.
    pub major_version: i32,
    /// `minor_version`.
    pub minor_version: i32,
    /// `revision_version`.
    pub revision_version: i32,
    /// `tag_version`.
    pub tag_version: i32,
}

impl Fingerprint {
    /// `generate_fingerprint` — format `-{name}{major}{minor}{rev}{tag}-`.
    ///
    /// `name` is truncated to 2 chars, or right-padded with `-` if shorter.
    #[must_use]
    pub fn generate(name: &[u8], major: i32, minor: i32, revision: i32, tag: i32) -> Vec<u8> {
        debug_assert!(major >= 0 && minor >= 0 && revision >= 0 && tag >= 0);
        fn v2c(v: i32) -> u8 {
            if (0..10).contains(&v) {
                b'0' + v as u8
            } else {
                b'A' + (v - 10) as u8
            }
        }
        vec![
            b'-',
            name.first().copied().unwrap_or(b'-'),
            name.get(1).copied().unwrap_or(b'-'),
            v2c(major),
            v2c(minor),
            v2c(revision),
            v2c(tag),
            b'-',
        ]
    }
}

fn is_print(c: u8) -> bool {
    (0x20..0x7f).contains(&c)
}

fn decode_digit(c: u8) -> i32 {
    if c.is_ascii_digit() {
        i32::from(c - b'0')
    } else {
        i32::from(c) - i32::from(b'A') + 10
    }
}

/// `parse_az_style` — `-XX1234-`.
fn parse_az_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if id[0] != b'-'
        || !is_print(id[1])
        || !id[3].is_ascii_digit()
        || !id[4].is_ascii_digit()
        || !id[5].is_ascii_digit()
        || (!id[6].is_ascii_digit() && !id[6].is_ascii_uppercase())
        || id[7] != b'-'
    {
        return None;
    }
    Some(Fingerprint {
        name: [id[1], id[2]],
        major_version: decode_digit(id[3]),
        minor_version: decode_digit(id[4]),
        revision_version: decode_digit(id[5]),
        tag_version: decode_digit(id[6]),
    })
}

/// `parse_shadow_style` — `Xxxx----` digit or raw-byte versions.
fn parse_shadow_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if !id[0].is_ascii_alphanumeric() {
        return None;
    }
    let (major, minor, revision) = if id[4] == b'-' && id[5] == b'-' {
        if !id[1].is_ascii_digit() || !id[2].is_ascii_digit() || !id[3].is_ascii_digit() {
            return None;
        }
        (
            decode_digit(id[1]),
            decode_digit(id[2]),
            decode_digit(id[3]),
        )
    } else {
        if id[8] != 0 || id[1] > 127 || id[2] > 127 || id[3] > 127 {
            return None;
        }
        (i32::from(id[1]), i32::from(id[2]), i32::from(id[3]))
    };
    Some(Fingerprint {
        name: [id[0], 0],
        major_version: major,
        minor_version: minor,
        revision_version: revision,
        tag_version: 0,
    })
}

/// `parse_transmission_legacy_style` — `-TRXYYR-` (0.80–3.00).
fn parse_transmission_legacy_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if id[0] != b'-' || id[1] != b'T' || id[2] != b'R' || id[7] != b'-' {
        return None;
    }
    if !(b'0'..=b'3').contains(&id[3])
        || !id[4].is_ascii_digit()
        || !id[5].is_ascii_digit()
        || (!id[6].is_ascii_digit() && !id[6].is_ascii_uppercase())
    {
        return None;
    }
    Some(Fingerprint {
        name: *b"TR",
        major_version: decode_digit(id[3]),
        minor_version: decode_digit(id[4]) * 10 + decode_digit(id[5]),
        revision_version: 0,
        tag_version: decode_digit(id[6]),
    })
}

/// `parse_mainline_style` — `X%d-%d-%d--` (sscanf `"%1c%3d-%3d-%3d--"`:
/// each group reads *up to* 3 digits).
fn parse_mainline_style(id: &[u8; 20]) -> Option<Fingerprint> {
    if !is_print(id[0]) {
        return None;
    }
    let mut pos = 1usize;
    let read_num = |pos: &mut usize| -> Option<i32> {
        let mut v = 0i32;
        let mut n = 0;
        while n < 3 && *pos < 20 && id[*pos].is_ascii_digit() {
            v = v * 10 + i32::from(id[*pos] - b'0');
            *pos += 1;
            n += 1;
        }
        (n > 0).then_some(v)
    };
    let major = read_num(&mut pos)?;
    if pos >= 20 || id[pos] != b'-' {
        return None;
    }
    pos += 1;
    let minor = read_num(&mut pos)?;
    if pos >= 20 || id[pos] != b'-' {
        return None;
    }
    pos += 1;
    let revision = read_num(&mut pos)?;
    if pos + 1 >= 20 || id[pos] != b'-' || id[pos + 1] != b'-' {
        return None;
    }
    Some(Fingerprint {
        name: [id[0], 0],
        major_version: major,
        minor_version: minor,
        revision_version: revision,
        tag_version: 0,
    })
}

/// `name_map` — alphabetical, binary-searched.
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

/// `generic_mappings` — `(offset, prefix, name)`.
const GENERIC_MAPPINGS: &[(usize, &[u8], &str)] = &[
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

fn lookup(f: &Fingerprint) -> String {
    // compare_id: (name[0], name[1]) lexical — binary search
    let name = NAME_MAP
        .binary_search_by(|(id, _)| id[..].cmp(&f.name[..]))
        .ok()
        .map_or_else(
            || {
                // unknown client → the 1-2 letter code itself
                let end = if f.name[1] == 0 { 1 } else { 2 };
                String::from_utf8_lossy(&f.name[..end]).into_owned()
            },
            |i| NAME_MAP[i].1.to_string(),
        );
    let mut ret = format!(
        "{name} {}.{}.{}",
        f.major_version, f.minor_version, f.revision_version
    );
    if f.tag_version != 0 {
        ret += &format!(".{}", f.tag_version);
    }
    ret
}

fn find_string(id: &[u8], search: &[u8]) -> bool {
    id.len() >= search.len() && id[..search.len()] == *search
}

/// `aux::identify_client_impl` — human-readable client name for a peer id.
#[must_use]
pub fn identify_client(p: &PeerId) -> String {
    let pid = p.as_bytes();

    if p.is_all_zeros() {
        return "Unknown".to_string();
    }

    for (offset, prefix, name) in GENERIC_MAPPINGS {
        if find_string(&pid[*offset..], prefix) {
            return (*name).to_string();
        }
    }

    if find_string(pid, b"-BOW") && pid[7] == b'-' {
        return format!("Bits on Wheels {}", String::from_utf8_lossy(&pid[4..7]));
    }

    if find_string(pid, b"eX") {
        return format!("eXeem ('{}')", String::from_utf8_lossy(&pid[2..14]));
    }

    let is_equ_zero = pid[..12].iter().all(|&c| c == 0);

    if is_equ_zero && pid[12] == 0x97 {
        return "Experimental 3.2.1b2".to_string();
    }
    if is_equ_zero && pid[12] == 0 {
        return "Experimental 3.1".to_string();
    }

    if let Some(f) = parse_transmission_legacy_style(pid) {
        return lookup(&f);
    }
    if let Some(f) = parse_az_style(pid) {
        return lookup(&f);
    }
    if let Some(f) = parse_shadow_style(pid) {
        return lookup(&f);
    }
    if let Some(f) = parse_mainline_style(pid) {
        return lookup(&f);
    }

    if is_equ_zero {
        return "Generic".to_string();
    }

    let mut unknown = String::from("Unknown [");
    for &c in pid {
        unknown.push(if is_print(c) { c as char } else { '.' });
    }
    unknown.push(']');
    unknown
}
