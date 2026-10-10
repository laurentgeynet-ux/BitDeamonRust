//! `magnet_uri.hpp`/`magnet_uri.cpp` — magnet link parse & serialize.
//!
//! `AddTorrentParams` is reduced to the metainfo-magnet subset for now;
//! session-level fields (`save_path`, storage mode, userdata…) arrive
//! with the session layer in later phases.

use std::net::SocketAddr;
use std::str::FromStr as _;

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::flags::TorrentFlags;
use bitdaemon_common::types::DownloadPriority;

pub use crate::add_torrent_params::AddTorrentParams;
use crate::error::{Error, Result};
use crate::escape_string::{
    base32decode, escape_string, parse_decimal, split_string, string_equal_no_case, unescape_string,
};
use crate::torrent_info::{is_i2p_url, is_valid_tracker_url};

/// One `so=` token → `(first, last)` inclusive index range, or `None`
/// when upstream would `continue` past it.
fn parse_so_token(token: &[u8]) -> Option<(i64, i64)> {
    const MAX_INDEX: i64 = 10000; // can't risk out of memory
    if token.is_empty() {
        return None;
    }
    match token.iter().position(|&c| c == b'-') {
        Some(divider) => {
            if divider == 0 || divider == token.len() - 1 {
                return None;
            }
            let first = parse_decimal(&token[..divider])?;
            let last = parse_decimal(&token[divider + 1..])?;
            if !(0..=MAX_INDEX).contains(&first) || !(0..=MAX_INDEX).contains(&last) || first > last
            {
                return None;
            }
            Some((first, last))
        }
        None => {
            let i = parse_decimal(token)?;
            (0..=MAX_INDEX).contains(&i).then_some((i, i))
        }
    }
}

/// `lt::parse_magnet_uri` — parse a `magnet:?` URI into `p`.
///
/// Errors: `UnsupportedUrlProtocol`, `InvalidInfoHash`,
/// `MissingInfoHashInUri`, `InvalidEscapedString`.
pub fn parse_magnet_uri(uri: &[u8], p: &mut AddTorrentParams) -> Result<()> {
    let mut display_name = Vec::new();

    if !uri.starts_with(b"magnet:?") {
        return Err(Error::UnsupportedUrlProtocol);
    }
    let mut sv = &uri[8..];

    let mut tier = 0i32;
    let mut has_ih = [false; 2];
    while !sv.is_empty() {
        let (name_full, rest) = split_string(sv, b'=');
        sv = rest;
        let (value_raw, rest2) = split_string(sv, b'&');
        let mut value: &[u8] = value_raw;
        sv = rest2;

        // parameter names may carry a ".<number>" suffix; strip it only
        // when the suffix is all digits
        let (stripped_name, number) = split_string(name_full, b'.');
        let mut name = name_full;
        if number.iter().all(u8::is_ascii_digit) {
            name = stripped_name;
        }

        if string_equal_no_case(name, b"dn") {
            if let Ok(unescaped) = unescape_string(value) {
                display_name = unescaped;
            }
        } else if string_equal_no_case(name, b"tr") {
            // keep the two vectors aligned
            if p.tracker_tiers.len() != p.trackers.len() {
                p.tracker_tiers.resize(p.trackers.len(), 0);
            }
            if let Ok(tracker) = unescape_string(value) {
                if is_valid_tracker_url(&tracker) {
                    if !p.flags.contains(TorrentFlags::I2P_TORRENT) && is_i2p_url(&tracker) {
                        p.flags |= TorrentFlags::I2P_TORRENT;
                    }
                    p.trackers.push(tracker);
                    p.tracker_tiers.push(tier);
                    tier += 1;
                }
            }
        } else if string_equal_no_case(name, b"ws") {
            if let Ok(webseed) = unescape_string(value) {
                p.url_seeds.push(webseed);
            }
        } else if string_equal_no_case(name, b"xt") {
            let owned;
            if value.contains(&b'%') {
                owned = unescape_string(value)?;
                value = &owned;
            }
            if value.starts_with(b"urn:btih:") {
                value = &value[9..];
                let info_hash = if value.len() == 40 {
                    Sha1Hash::from_hex(value).map_err(|_| Error::InvalidInfoHash)?
                } else if value.len() == 32 {
                    let ih = base32decode(value);
                    if ih.len() != 20 {
                        return Err(Error::InvalidInfoHash);
                    }
                    Sha1Hash::from_bytes(&ih).unwrap_or_default()
                } else {
                    return Err(Error::InvalidInfoHash);
                };
                p.info_hashes.v1 = info_hash;
                has_ih[0] = true;
            } else if value.starts_with(b"urn:btmh:") {
                value = &value[9..];
                // hash must be sha256 — multihash prefix 0x12 0x20
                if !value.starts_with(b"1220") {
                    return Err(Error::InvalidInfoHash);
                }
                value = &value[4..];
                if value.len() != 64 {
                    return Err(Error::InvalidInfoHash);
                }
                p.info_hashes.v2 = bitdaemon_common::Sha256Hash::from_hex(value)
                    .map_err(|_| Error::InvalidInfoHash)?;
                has_ih[1] = true;
            }
        } else if string_equal_no_case(name, b"so") {
            // accept only digits, '-' and ','
            if value
                .iter()
                .any(|&c| !c.is_ascii_digit() && c != b'-' && c != b',')
            {
                continue;
            }
            // all files default to not-download except the selected ones
            p.flags |= TorrentFlags::DEFAULT_DONT_DOWNLOAD;

            // upstream do-while: at least one token pass
            loop {
                let (token, rest) = split_string(value, b',');
                value = rest;
                if let Some((idx1, idx2)) = parse_so_token(token) {
                    if p.file_priorities.len() as i64 <= idx2 {
                        p.file_priorities
                            .resize(idx2 as usize + 1, DownloadPriority::DONT_DOWNLOAD);
                    }
                    for i in idx1..=idx2 {
                        p.file_priorities[i as usize] = DownloadPriority::DEFAULT;
                    }
                }
                if value.is_empty() {
                    break;
                }
            }
        } else if string_equal_no_case(name, b"x.pe") {
            if let Ok(s) = std::str::from_utf8(value) {
                if let Ok(ep) = SocketAddr::from_str(s.trim()) {
                    p.peers.push(ep);
                }
            }
        } else if string_equal_no_case(name, b"dht") {
            if let Some(divider) = value.iter().rposition(|&c| c == b':') {
                if let Some(port) = parse_decimal(&value[divider + 1..]) {
                    if port > 0 && port < i64::from(u16::MAX) {
                        p.dht_nodes.push((value[..divider].to_vec(), port as i32));
                    }
                }
            }
        }
    }

    if !has_ih[0] && !has_ih[1] {
        return Err(Error::MissingInfoHashInUri);
    }
    if !display_name.is_empty() {
        p.name = display_name;
    }
    Ok(())
}

/// Convenience: parse into a fresh `AddTorrentParams`.
pub fn parse_magnet(uri: &[u8]) -> Result<AddTorrentParams> {
    let mut ret = AddTorrentParams::default();
    parse_magnet_uri(uri, &mut ret)?;
    Ok(ret)
}

/// `lt::make_magnet_uri(add_torrent_params const&)` — serialize `p`.
/// Returns an empty string when no info-hash is present.
#[must_use]
pub fn make_magnet_uri(p: &AddTorrentParams) -> String {
    let mut ret = String::from("magnet:?");
    let mut first = true;

    if p.info_hashes.has_v1() {
        ret += "xt=urn:btih:";
        ret += &p.info_hashes.v1.to_hex();
        first = false;
    }
    if p.info_hashes.has_v2() {
        if !first {
            ret += "&";
        }
        ret += "xt=urn:btmh:1220";
        ret += &p.info_hashes.v2.to_hex();
        first = false;
    }
    if first {
        if let Some(ti) = &p.ti {
            let ih = ti.info_hashes();
            if ih.has_v1() {
                ret += "xt=urn:btih:";
                ret += &ih.v1.to_hex();
                first = false;
            }
            if ih.has_v2() {
                if !first {
                    ret += "&";
                }
                ret += "xt=urn:btmh:1220";
                ret += &ih.v2.to_hex();
            }
        }
    }
    if first {
        // no info-hashes — can't make a magnet link
        return String::new();
    }

    if !p.name.is_empty() {
        ret += "&dn=";
        ret += std::str::from_utf8(&escape_string(&p.name)).unwrap_or_default();
    }
    for tr in &p.trackers {
        ret += "&tr=";
        ret += std::str::from_utf8(&escape_string(tr)).unwrap_or_default();
    }
    for s in &p.url_seeds {
        ret += "&ws=";
        ret += std::str::from_utf8(&escape_string(s)).unwrap_or_default();
    }
    for (host, port) in &p.dht_nodes {
        ret += "&dht=";
        ret += std::str::from_utf8(&escape_string(host)).unwrap_or_default();
        ret += &format!(":{port}");
    }
    for ep in &p.peers {
        ret += "&x.pe=";
        ret += &ep.to_string();
    }

    // "&so=" only if at least one file is deselected
    if p.file_priorities.contains(&DownloadPriority::DONT_DOWNLOAD) {
        ret += "&so=";
        let mut range_start: Option<usize> = None;
        let mut need_comma = false;
        let mut push_range = |ret: &mut String, start: usize, end_excl: usize| {
            if need_comma {
                ret.push(',');
            }
            if start == end_excl - 1 {
                ret.push_str(&start.to_string());
            } else {
                ret.push_str(&format!("{start}-{}", end_excl - 1));
            }
            need_comma = true;
        };
        for (idx, prio) in p.file_priorities.iter().enumerate() {
            if *prio != DownloadPriority::DONT_DOWNLOAD {
                if range_start.is_none() {
                    range_start = Some(idx);
                }
            } else if let Some(start) = range_start.take() {
                push_range(&mut ret, start, idx);
            }
        }
        if let Some(start) = range_start {
            push_range(&mut ret, start, p.file_priorities.len());
        }
    }
    ret
}
