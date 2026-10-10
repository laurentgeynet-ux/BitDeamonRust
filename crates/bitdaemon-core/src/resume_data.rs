//! `read_resume_data.cpp`/`write_resume_data.cpp` — the fastresume wire
//! format. **Byte-compatible with libtorrent in both directions**
//! (ADR-0009): existing `.resume` files must load, and ours must load
//! upstream.
//!
//! Wire format (bencoded dict, `file-version` 2 since libtorrent 1.2):
//! `file-format="libtorrent resume file"`, `info-hash`/`info-hash2`,
//! `info` (verbatim info dict), `pieces`/`verified` bitfields,
//! `unfinished` per-piece block bitmasks, `trees` merkle data (v2),
//! `file_priority`, `piece_priority`, `mapped_files`, `trackers`,
//! `url-list`, compact `peers`/`peers6`/`banned_peers*`, counters and
//! flag booleans.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bitdaemon_bencode::{BdecodeNode, NodeType};
use bitdaemon_bencode::{Dictionary, Entry, bencode};
use bitdaemon_common::flags::TorrentFlags;
use bitdaemon_common::types::DownloadPriority;
use bitdaemon_common::{Bitfield, Sha1Hash, Sha256Hash};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::add_torrent_params::{AddTorrentParams, StorageMode};
use crate::error::{Error, Result, TorrentError};
use crate::torrent_info::{LoadTorrentLimits, TorrentInfo};

const V4_SIZE: usize = 6;
const V6_SIZE: usize = 18;

/// `lt::errors::too_many_pieces_in_torrent` guard on resume bitfields —
/// a corrupt/oversized string fails the whole parse (upstream
/// `bitfield_from_resume`).
fn bitfield_from_resume(s: &[u8], bit_limit: i64, piece_limit: i64) -> Result<Bitfield> {
    let cap = bit_limit.min(piece_limit).min(i64::from(i32::MAX));
    if s.len() as i64 > cap / 8 {
        return Err(Error::Torrent(TorrentError::TooManyPiecesInTorrent));
    }
    Ok(Bitfield::from_bytes(s, s.len() * 8).unwrap_or_default())
}

fn read_v4_endpoint(b: &[u8]) -> Option<SocketAddr> {
    if b.len() < V4_SIZE {
        return None;
    }
    let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
    let port = u16::from_be_bytes([b[4], b[5]]);
    Some(SocketAddr::new(IpAddr::V4(ip), port))
}

fn read_v6_endpoint(b: &[u8]) -> Option<SocketAddr> {
    if b.len() < V6_SIZE {
        return None;
    }
    let mut a = [0u8; 16];
    a.copy_from_slice(&b[..16]);
    let port = u16::from_be_bytes([b[16], b[17]]);
    Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(a)), port))
}

fn write_endpoint(ep: &SocketAddr, out: &mut Vec<u8>) {
    match ep.ip() {
        IpAddr::V4(a) => out.extend_from_slice(&a.octets()),
        IpAddr::V6(a) => out.extend_from_slice(&a.octets()),
    }
    out.extend_from_slice(&ep.port().to_be_bytes());
}

/// `apply_flag` — boolean int entry, defaulting to the flag's bit in
/// `torrent_flags::default_flags`.
fn apply_flag(current: &mut TorrentFlags, rd: &BdecodeNode<'_>, name: &[u8], flag: TorrentFlags) {
    let default = i64::from(TorrentFlags::DEFAULT_FLAGS.contains(flag));
    if rd.dict_find_int_value(name, default) == 0 {
        *current &= !flag;
    } else {
        *current |= flag;
    }
}

/// `lt::read_resume_data(bdecode_node)` — populate an `AddTorrentParams`
/// from a decoded resume dict.
pub fn read_resume_data_node(rd: &BdecodeNode<'_>, piece_limit: i64) -> Result<AddTorrentParams> {
    let mut ret = AddTorrentParams::default();
    if rd.node_type() != NodeType::Dict {
        return Err(Error::Torrent(TorrentError::NotADictionary));
    }

    let alloc = rd.dict_find_string(b"allocation");
    if alloc.node_type() == NodeType::String {
        let v = alloc.string_value();
        ret.storage_mode = if v == b"allocate" || v == b"full" {
            StorageMode::Allocate
        } else {
            StorageMode::Sparse
        };
    }

    if rd.dict_find_string_value(b"file-format", b"") != b"libtorrent resume file" {
        return Err(Error::Torrent(TorrentError::InvalidFileTag));
    }

    let file_version = rd.dict_find_int_value(b"file-version", 1);
    if file_version != 1 && file_version != 2 {
        return Err(Error::Torrent(TorrentError::InvalidFileTag));
    }

    let info_hash = rd.dict_find_string_value(b"info-hash", b"");
    let info_hash2 = rd.dict_find_string_value(b"info-hash2", b"");
    if info_hash.len() != 20 && info_hash2.len() != 32 {
        return Err(Error::Torrent(TorrentError::MissingInfoHash));
    }

    ret.name = rd.dict_find_string_value(b"name", b"").to_vec();
    if let Some(h) = Sha1Hash::from_bytes(info_hash) {
        ret.info_hashes.v1 = h;
    }
    if let Some(h) = Sha256Hash::from_bytes(info_hash2) {
        ret.info_hashes.v2 = h;
    }

    ret.creation_date = rd.dict_find_int_value(b"creation date", 0);
    ret.created_by = rd.dict_find_string_value(b"created by", b"").to_vec();
    ret.comment = rd.dict_find_string_value(b"comment", b"").to_vec();

    let info = rd.dict_find_dict(b"info");
    if info.node_type() == NodeType::Dict {
        // verify the embedded metainfo hashes to the declared info-hashes
        let sec = info.data_section();
        let resume_v1 = Sha1Hash::from_bytes(&Sha1::digest(sec)).unwrap_or_default();
        let resume_v2 = Sha256Hash::from_bytes(&Sha256::digest(sec)).unwrap_or_default();

        if (!ret.info_hashes.has_v1() || resume_v1 == ret.info_hashes.v1)
            && (!ret.info_hashes.has_v2() || resume_v2 == ret.info_hashes.v2)
        {
            let cfg = LoadTorrentLimits {
                max_pieces: piece_limit as usize,
                ..LoadTorrentLimits::default()
            };
            ret.ti = Some(std::sync::Arc::new(TorrentInfo::from_info_node(
                &info, &cfg,
            )?));
        } else {
            return Err(Error::Torrent(TorrentError::MismatchingInfoHash));
        }
    }

    // v2 per-file merkle data
    let trees = rd.dict_find_list(b"trees");
    if trees.node_type() == NodeType::List {
        for i in 0..trees.list_size() {
            let de = trees.list_at(i);
            if de.node_type() != NodeType::Dict {
                break;
            }
            let dh = de.dict_find_string(b"hashes");
            if dh.node_type() != NodeType::String || dh.string_length() % 32 != 0 {
                break;
            }
            let hashes: Vec<Sha256Hash> = dh
                .string_value()
                .chunks_exact(32)
                .map(|c| Sha256Hash::from_bytes(c).unwrap_or_default())
                .collect();
            ret.merkle_trees.push(hashes);

            let verified = de.dict_find_string(b"verified");
            if verified.node_type() == NodeType::String {
                let s = verified.string_value();
                if file_version == 1 {
                    let mut bf = Bitfield::with_size(s.len());
                    for (j, &c) in s.iter().enumerate() {
                        if c == b'1' {
                            bf.set_bit(j);
                        }
                    }
                    ret.verified_leaf_hashes.push(bf);
                } else {
                    ret.verified_leaf_hashes.push(bitfield_from_resume(
                        s,
                        piece_limit * 2,
                        i64::MAX,
                    )?);
                }
            } else {
                ret.verified_leaf_hashes.push(Bitfield::new());
            }

            let mask = de.dict_find_string(b"mask");
            if mask.node_type() == NodeType::String {
                let s = mask.string_value();
                if file_version == 1 {
                    let mut bf = Bitfield::with_size(s.len());
                    for (j, &c) in s.iter().enumerate() {
                        if c == b'1' {
                            bf.set_bit(j);
                        }
                    }
                    ret.merkle_tree_mask.push(bf);
                } else {
                    ret.merkle_tree_mask
                        .push(bitfield_from_resume(s, piece_limit * 2, i64::MAX)?);
                }
            } else {
                ret.merkle_tree_mask.push(Bitfield::new());
            }
        }
    }

    ret.total_uploaded = rd.dict_find_int_value(b"total_uploaded", 0);
    ret.total_downloaded = rd.dict_find_int_value(b"total_downloaded", 0);
    ret.active_time = rd.dict_find_int_value(b"active_time", 0);
    ret.finished_time = rd.dict_find_int_value(b"finished_time", 0);
    ret.seeding_time = rd.dict_find_int_value(b"seeding_time", 0);
    ret.last_seen_complete = rd.dict_find_int_value(b"last_seen_complete", 0);
    ret.last_download = rd.dict_find_int_value(b"last_download", 0);
    ret.last_upload = rd.dict_find_int_value(b"last_upload", 0);

    ret.num_complete = rd.dict_find_int_value(b"num_complete", -1);
    ret.num_incomplete = rd.dict_find_int_value(b"num_incomplete", -1);
    ret.num_downloaded = rd.dict_find_int_value(b"num_downloaded", -1);
    ret.max_uploads = rd.dict_find_int_value(b"max_uploads", -1);
    ret.max_connections = rd.dict_find_int_value(b"max_connections", -1);
    ret.upload_limit = rd.dict_find_int_value(b"upload_rate_limit", -1);
    ret.download_limit = rd.dict_find_int_value(b"download_rate_limit", -1);

    use TorrentFlags as F;
    apply_flag(&mut ret.flags, rd, b"seed_mode", F::SEED_MODE);
    apply_flag(&mut ret.flags, rd, b"upload_mode", F::UPLOAD_MODE);
    apply_flag(&mut ret.flags, rd, b"share_mode", F::SHARE_MODE);
    apply_flag(&mut ret.flags, rd, b"apply_ip_filter", F::APPLY_IP_FILTER);
    apply_flag(&mut ret.flags, rd, b"paused", F::PAUSED);
    apply_flag(&mut ret.flags, rd, b"auto_managed", F::AUTO_MANAGED);
    apply_flag(&mut ret.flags, rd, b"super_seeding", F::SUPER_SEEDING);
    apply_flag(&mut ret.flags, rd, b"i2p", F::I2P_TORRENT);
    apply_flag(
        &mut ret.flags,
        rd,
        b"sequential_download",
        F::SEQUENTIAL_DOWNLOAD,
    );
    apply_flag(&mut ret.flags, rd, b"stop_when_ready", F::STOP_WHEN_READY);
    apply_flag(&mut ret.flags, rd, b"disable_dht", F::DISABLE_DHT);
    apply_flag(&mut ret.flags, rd, b"disable_lsd", F::DISABLE_LSD);
    apply_flag(&mut ret.flags, rd, b"disable_pex", F::DISABLE_PEX);
    apply_flag(
        &mut ret.flags,
        rd,
        b"disable_v1_hashes",
        F::DISABLE_V1_HASHES,
    );

    ret.save_path = rd.dict_find_string_value(b"save_path", b"").to_vec();
    ret.part_file_dir = rd.dict_find_string_value(b"part_file_dir", b"").to_vec();
    ret.root_certificate = rd.dict_find_string_value(b"root_certificate", b"").to_vec();

    let mapped = rd.dict_find_list(b"mapped_files");
    if mapped.node_type() == NodeType::List {
        for i in 0..mapped.list_size() {
            let name = mapped.list_string_value_at(i, b"");
            if !name.is_empty() {
                ret.renamed_files.insert(i as i32, name.to_vec());
            }
        }
    }

    ret.added_time = rd.dict_find_int_value(b"added_time", 0);
    ret.completed_time = rd.dict_find_int_value(b"completed_time", 0);

    let fp = rd.dict_find_list(b"file_priority");
    if fp.node_type() == NodeType::List {
        ret.file_priorities = vec![DownloadPriority::DEFAULT; fp.list_size()];
        for i in 0..fp.list_size() {
            let v = fp.list_int_value_at(i, i64::from(DownloadPriority::DEFAULT.0));
            let p = DownloadPriority(
                (v as u8).clamp(DownloadPriority::DONT_DOWNLOAD.0, DownloadPriority::TOP.0),
            );
            ret.file_priorities[i] = p;
            // suspicious: deselected file in seed mode → leave seed mode
            if p == DownloadPriority::DONT_DOWNLOAD {
                ret.flags &= !TorrentFlags::SEED_MODE;
            }
        }
    }

    let trackers = rd.dict_find_list(b"trackers");
    if trackers.node_type() == NodeType::List {
        // explicit trackers list in resume data overrides the .torrent's
        ret.flags |= TorrentFlags::DEPRECATED_OVERRIDE_TRACKERS;
        let mut tier = 0i32;
        for i in 0..trackers.list_size() {
            let tier_list = trackers.list_at(i);
            if tier_list.node_type() != NodeType::List {
                continue;
            }
            for j in 0..tier_list.list_size() {
                let url = tier_list.list_string_value_at(j, b"");
                ret.trackers.push(url.to_vec());
                ret.tracker_tiers.push(tier);
                if crate::torrent_info::is_i2p_url(url) {
                    ret.flags |= TorrentFlags::I2P_TORRENT;
                }
            }
            tier += 1;
        }
    }

    let url_list = rd.dict_find_list(b"url-list");
    if url_list.node_type() == NodeType::List {
        ret.flags |= TorrentFlags::DEPRECATED_OVERRIDE_WEB_SEEDS;
        for i in 0..url_list.list_size() {
            let url = url_list.list_string_value_at(i, b"");
            if !url.is_empty() {
                ret.url_seeds.push(url.to_vec());
            }
        }
    }

    let pieces = rd.dict_find_string(b"pieces");
    if pieces.node_type() == NodeType::String {
        let s = pieces.string_value();
        if file_version == 1 {
            // v1: 1 byte per piece, bit0 = have, bit1 = verified
            ret.have_pieces = Bitfield::with_size(s.len());
            let mut verified = Bitfield::with_size(s.len());
            let mut any_verified = false;
            for (i, &c) in s.iter().enumerate() {
                if c & 1 != 0 {
                    ret.have_pieces.set_bit(i);
                }
                if c & 2 != 0 {
                    verified.set_bit(i);
                    any_verified = true;
                }
            }
            if any_verified {
                ret.verified_pieces = verified;
            }
        } else {
            ret.have_pieces = bitfield_from_resume(s, i64::MAX, piece_limit)?;
        }
    }

    let verified = rd.dict_find_string(b"verified");
    if verified.node_type() == NodeType::String {
        ret.verified_pieces = bitfield_from_resume(verified.string_value(), i64::MAX, piece_limit)?;
    }

    let pp = rd.dict_find_string(b"piece_priority");
    if pp.node_type() == NodeType::String {
        ret.piece_priorities = pp
            .string_value()
            .iter()
            .map(|&c| {
                DownloadPriority(
                    c.clamp(DownloadPriority::DONT_DOWNLOAD.0, DownloadPriority::TOP.0),
                )
            })
            .collect();
    }

    for (key, size, v6, banned) in [
        (b"peers".as_slice(), V4_SIZE, false, false),
        (b"peers6".as_slice(), V6_SIZE, true, false),
        (b"banned_peers".as_slice(), V4_SIZE, false, true),
        (b"banned_peers6".as_slice(), V6_SIZE, true, true),
    ] {
        let node = rd.dict_find_string(key);
        if node.node_type() == NodeType::String {
            for chunk in node.string_value().chunks_exact(size) {
                let ep = if v6 {
                    read_v6_endpoint(chunk)
                } else {
                    read_v4_endpoint(chunk)
                };
                if let Some(ep) = ep {
                    if banned {
                        ret.banned_peers.push(ep);
                    } else {
                        ret.peers.push(ep);
                    }
                }
            }
        }
    }

    let unfinished = rd.dict_find_list(b"unfinished");
    if unfinished.node_type() == NodeType::List {
        for i in 0..unfinished.list_size() {
            let e = unfinished.list_at(i);
            if e.node_type() != NodeType::Dict {
                continue;
            }
            let piece = e.dict_find_int_value(b"piece", -1);
            if piece < 0 {
                continue;
            }
            let bitmask = e.dict_find_string(b"bitmask");
            if bitmask.node_type() != NodeType::String || bitmask.string_length() == 0 {
                continue;
            }
            // each bit is a block, bounded only by the int bit-count
            let bf = bitfield_from_resume(bitmask.string_value(), i64::MAX, i64::MAX)?;
            ret.unfinished_pieces.insert(piece as i32, bf);
        }
    }

    // loaded from resume data — no need to re-save immediately
    ret.flags &= !TorrentFlags::NEED_SAVE_RESUME;
    Ok(ret)
}

/// `lt::read_resume_data(span<char>)` — decode + parse.
pub fn read_resume_data(buf: &[u8], cfg: &LoadTorrentLimits) -> Result<AddTorrentParams> {
    let rd = bitdaemon_bencode::bdecode_limits(buf, cfg.max_decode_depth, cfg.max_decode_tokens);
    if let Some(e) = rd.error() {
        return Err(e.into());
    }
    read_resume_data_node(&rd, cfg.max_pieces as i64)
}

fn bitfield_to_bytes(b: &Bitfield) -> Vec<u8> {
    b.bytes().to_vec()
}

fn bool_int(v: bool) -> Entry {
    Entry::Int(i64::from(v))
}

/// `lt::write_resume_data` — serialize `atp` into a resume `Entry`
/// (byte-compatible wire format).
#[must_use]
pub fn write_resume_data(atp: &AddTorrentParams) -> Entry {
    let mut ret = bitdaemon_bencode::Dictionary::new();

    macro_rules! ins {
        ($k:expr, $v:expr) => {
            ret.insert($k.to_vec(), $v)
        };
    }

    ins!(
        b"file-format",
        Entry::String(b"libtorrent resume file".to_vec())
    );
    ins!(b"file-version", Entry::Int(2));
    ins!(
        b"libtorrent-version",
        Entry::String(crate::VERSION_STR.to_vec())
    );
    ins!(
        b"allocation",
        Entry::String(if atp.storage_mode == StorageMode::Allocate {
            b"allocate".to_vec()
        } else {
            b"sparse".to_vec()
        })
    );

    ins!(b"total_uploaded", Entry::Int(atp.total_uploaded));
    ins!(b"total_downloaded", Entry::Int(atp.total_downloaded));
    ins!(b"active_time", Entry::Int(atp.active_time));
    ins!(b"finished_time", Entry::Int(atp.finished_time));
    ins!(b"seeding_time", Entry::Int(atp.seeding_time));
    ins!(b"last_seen_complete", Entry::Int(atp.last_seen_complete));
    ins!(b"last_download", Entry::Int(atp.last_download));
    ins!(b"last_upload", Entry::Int(atp.last_upload));
    ins!(b"num_complete", Entry::Int(atp.num_complete));
    ins!(b"num_incomplete", Entry::Int(atp.num_incomplete));
    ins!(b"num_downloaded", Entry::Int(atp.num_downloaded));

    use TorrentFlags as F;
    ins!(b"seed_mode", bool_int(atp.flags.contains(F::SEED_MODE)));
    ins!(b"upload_mode", bool_int(atp.flags.contains(F::UPLOAD_MODE)));
    ins!(b"share_mode", bool_int(atp.flags.contains(F::SHARE_MODE)));
    ins!(
        b"apply_ip_filter",
        bool_int(atp.flags.contains(F::APPLY_IP_FILTER))
    );
    ins!(b"paused", bool_int(atp.flags.contains(F::PAUSED)));
    ins!(
        b"auto_managed",
        bool_int(atp.flags.contains(F::AUTO_MANAGED))
    );
    ins!(
        b"super_seeding",
        bool_int(atp.flags.contains(F::SUPER_SEEDING))
    );
    ins!(b"i2p", bool_int(atp.flags.contains(F::I2P_TORRENT)));
    ins!(
        b"sequential_download",
        bool_int(atp.flags.contains(F::SEQUENTIAL_DOWNLOAD))
    );
    ins!(
        b"stop_when_ready",
        bool_int(atp.flags.contains(F::STOP_WHEN_READY))
    );
    ins!(b"disable_dht", bool_int(atp.flags.contains(F::DISABLE_DHT)));
    ins!(b"disable_lsd", bool_int(atp.flags.contains(F::DISABLE_LSD)));
    ins!(b"disable_pex", bool_int(atp.flags.contains(F::DISABLE_PEX)));
    ins!(
        b"disable_v1_hashes",
        bool_int(atp.flags.contains(F::DISABLE_V1_HASHES))
    );

    ins!(b"added_time", Entry::Int(atp.added_time));
    ins!(b"completed_time", Entry::Int(atp.completed_time));
    ins!(b"save_path", Entry::String(atp.save_path.clone()));
    ins!(b"part_file_dir", Entry::String(atp.part_file_dir.clone()));
    ins!(
        b"root_certificate",
        Entry::String(atp.root_certificate.clone())
    );
    if !atp.name.is_empty() {
        ins!(b"name", Entry::String(atp.name.clone()));
    }
    ins!(
        b"info-hash",
        Entry::String(atp.info_hashes.v1.as_bytes().to_vec())
    );
    ins!(
        b"info-hash2",
        Entry::String(atp.info_hashes.v2.as_bytes().to_vec())
    );

    if let Some(ti) = &atp.ti {
        ins!(b"info", Entry::Preformatted(ti.info_section().to_vec()));
    }
    if !atp.comment.is_empty() {
        ins!(b"comment", Entry::String(atp.comment.clone()));
    }
    if atp.creation_date != 0 {
        ins!(b"creation date", Entry::Int(atp.creation_date));
    }
    if !atp.created_by.is_empty() {
        ins!(b"created by", Entry::String(atp.created_by.clone()));
    }

    if !atp.merkle_trees.is_empty() {
        let mut trees = Vec::with_capacity(atp.merkle_trees.len());
        for (f, tree) in atp.merkle_trees.iter().enumerate() {
            let mut d = bitdaemon_bencode::Dictionary::new();
            let mut hashes = Vec::with_capacity(tree.len() * 32);
            for n in tree {
                hashes.extend_from_slice(n.as_bytes());
            }
            d.insert(b"hashes".to_vec(), Entry::String(hashes));
            if let Some(verified) = atp.verified_leaf_hashes.get(f) {
                if !verified.is_empty() {
                    d.insert(
                        b"verified".to_vec(),
                        Entry::String(bitfield_to_bytes(verified)),
                    );
                }
            }
            if let Some(mask) = atp.merkle_tree_mask.get(f) {
                if !mask.is_empty() {
                    d.insert(b"mask".to_vec(), Entry::String(bitfield_to_bytes(mask)));
                }
            }
            trees.push(Entry::Dict(d));
        }
        ins!(b"trees", Entry::List(trees));
    }

    if !atp.unfinished_pieces.is_empty() {
        let up: Vec<Entry> = atp
            .unfinished_pieces
            .iter()
            .map(|(piece, bm)| {
                let mut d = bitdaemon_bencode::Dictionary::new();
                d.insert(b"piece".to_vec(), Entry::Int(i64::from(*piece)));
                d.insert(b"bitmask".to_vec(), Entry::String(bitfield_to_bytes(bm)));
                Entry::Dict(d)
            })
            .collect();
        ins!(b"unfinished", Entry::List(up));
    }

    // trackers → announce-list style tier lists
    let mut tr_list: Vec<Entry> = Vec::new();
    if !atp.trackers.is_empty() {
        tr_list.push(Entry::List(Vec::new()));
        let mut tier = 0usize;
        let mut tier_it = atp.tracker_tiers.iter();
        for tr in &atp.trackers {
            if let Some(&t) = tier_it.next() {
                tier = (t.max(0) as usize).min(1024);
            }
            if tr_list.len() <= tier {
                tr_list.resize(tier + 1, Entry::List(Vec::new()));
            }
            if let Entry::List(l) = &mut tr_list[tier] {
                l.push(Entry::String(tr.clone()));
            }
        }
    }
    ins!(b"trackers", Entry::List(tr_list));

    ins!(
        b"url-list",
        Entry::List(atp.url_seeds.iter().cloned().map(Entry::String).collect())
    );

    if !atp.have_pieces.is_empty() {
        ins!(
            b"pieces",
            Entry::String(bitfield_to_bytes(&atp.have_pieces))
        );
    }
    if !atp.verified_pieces.is_empty() && !atp.verified_pieces.none_set() {
        ins!(
            b"verified",
            Entry::String(bitfield_to_bytes(&atp.verified_pieces))
        );
    }

    if !atp.renamed_files.is_empty() {
        let mut fl: Vec<Entry> = Vec::new();
        for (idx, name) in &atp.renamed_files {
            let idx = *idx as usize;
            if fl.len() <= idx {
                fl.resize(idx + 1, Entry::String(Vec::new()));
            }
            fl[idx] = Entry::String(name.clone());
        }
        ins!(b"mapped_files", Entry::List(fl));
    }

    if !atp.peers.is_empty() {
        let mut p4 = Vec::new();
        let mut p6 = Vec::new();
        for p in &atp.peers {
            if p.is_ipv6() {
                write_endpoint(p, &mut p6);
            } else {
                write_endpoint(p, &mut p4);
            }
        }
        ins!(b"peers", Entry::String(p4));
        ins!(b"peers6", Entry::String(p6));
    }
    if !atp.banned_peers.is_empty() {
        let mut p4 = Vec::new();
        let mut p6 = Vec::new();
        for p in &atp.banned_peers {
            if p.is_ipv6() {
                write_endpoint(p, &mut p6);
            } else {
                write_endpoint(p, &mut p4);
            }
        }
        ins!(b"banned_peers", Entry::String(p4));
        ins!(b"banned_peers6", Entry::String(p6));
    }

    ins!(b"upload_rate_limit", Entry::Int(atp.upload_limit));
    ins!(b"download_rate_limit", Entry::Int(atp.download_limit));
    ins!(b"max_connections", Entry::Int(atp.max_connections));
    ins!(b"max_uploads", Entry::Int(atp.max_uploads));

    if !atp.file_priorities.is_empty() {
        ins!(
            b"file_priority",
            Entry::List(
                atp.file_priorities
                    .iter()
                    .map(|p| Entry::Int(i64::from(p.0)))
                    .collect()
            )
        );
    }
    if !atp.piece_priorities.is_empty() {
        ins!(
            b"piece_priority",
            Entry::String(atp.piece_priorities.iter().map(|p| p.0).collect())
        );
    }

    Entry::Dict(ret)
}

/// `lt::write_resume_data_buf` — bencode [`write_resume_data`].
#[must_use]
pub fn write_resume_data_buf(atp: &AddTorrentParams) -> Vec<u8> {
    bencode(&write_resume_data(atp))
}

/// `build_tracker_list` — announce-list style nested tier lists.
#[must_use]
pub fn build_tracker_list(trackers: &[Vec<u8>], tracker_tiers: &[i32]) -> Entry {
    let mut tr_list: Vec<Entry> = Vec::new();
    if trackers.is_empty() {
        return Entry::List(tr_list);
    }
    tr_list.push(Entry::List(Vec::new()));
    let mut tier = 0usize;
    let mut tier_it = tracker_tiers.iter();
    for tr in trackers {
        if let Some(&t) = tier_it.next() {
            tier = (t.max(0) as usize).min(1024);
        }
        if tr_list.len() <= tier {
            tr_list.resize(tier + 1, Entry::List(Vec::new()));
        }
        if let Entry::List(l) = &mut tr_list[tier] {
            l.push(Entry::String(tr.clone()));
        }
    }
    Entry::List(tr_list)
}

/// `write_torrent_flags_t`.
#[derive(Debug, Clone, Copy, Default)]
pub struct WriteTorrentFlags {
    /// `write_flags::allow_missing_piece_layer` — v2 torrents may be
    /// written without `piece layers` (e.g. magnet-like output).
    pub allow_missing_piece_layer: bool,
    /// `write_flags::no_http_seeds`.
    pub no_http_seeds: bool,
    /// `write_flags::include_dht_nodes`.
    pub include_dht_nodes: bool,
}

/// `write_torrent_file(atp, flags)` — rebuild a `.torrent` dict from
/// `AddTorrentParams` (verbatim info section + reconstructed layers).
///
/// # Errors
/// `TorrentMissingInfo`, `TorrentMissingPieceLayer`,
/// `TorrentInvalidPieceLayer`, `Bdecode` (malformed stored info section).
pub fn write_torrent_file(atp: &AddTorrentParams, flags: WriteTorrentFlags) -> Result<Entry> {
    let Some(ti) = &atp.ti else {
        return Err(crate::TorrentError::TorrentMissingInfo.into());
    };
    let mut ret = Dictionary::new();

    // verbatim info dict
    let info_node = bitdaemon_bencode::bdecode_limits(ti.info_section(), 100, 2_000_000);
    ret.insert(
        b"info".to_vec(),
        Entry::Preformatted(ti.info_section().to_vec()),
    );

    if !atp.comment.is_empty() {
        ret.insert(b"comment".to_vec(), Entry::String(atp.comment.clone()));
    }
    if atp.creation_date != 0 {
        ret.insert(b"creation date".to_vec(), Entry::Int(atp.creation_date));
    }
    if !atp.created_by.is_empty() {
        ret.insert(
            b"created by".to_vec(),
            Entry::String(atp.created_by.clone()),
        );
    }

    if !atp.merkle_trees.is_empty() {
        let fs = ti.files();
        if atp.merkle_trees.len() != fs.num_files() as usize {
            return Err(crate::TorrentError::TorrentMissingPieceLayer.into());
        }
        let mut piece_layers = Dictionary::new();
        for f in fs.file_range() {
            if fs.pad_file_at(f) || fs.file_size(f) <= i64::from(fs.piece_length()) {
                continue;
            }
            let mut tree = crate::merkle_tree::MerkleTree::new(
                fs.file_num_blocks(f) as usize,
                fs.blocks_per_piece() as usize,
                fs.root(f),
            );
            let empty_verified = Bitfield::new();
            let verified = atp
                .verified_leaf_hashes
                .get(f.0 as usize)
                .unwrap_or(&empty_verified);
            let sparse = &atp.merkle_trees[f.0 as usize];
            if let Some(mask) = atp.merkle_tree_mask.get(f.0 as usize) {
                if !mask.is_empty() {
                    tree.load_sparse_tree(sparse, mask, verified);
                } else {
                    tree.load_tree(sparse, verified);
                }
            } else {
                tree.load_tree(sparse, verified);
            }
            let piece_layer = tree.get_piece_layer();
            if piece_layer.len() != fs.file_num_pieces(f) as usize {
                return Err(crate::TorrentError::TorrentInvalidPieceLayer.into());
            }
            let mut layer = Vec::with_capacity(piece_layer.len() * 32);
            for h in &piece_layer {
                layer.extend_from_slice(h.as_bytes());
            }
            piece_layers.insert(tree.root().as_bytes().to_vec(), Entry::String(layer));
        }
        ret.insert(b"piece layers".to_vec(), Entry::Dict(piece_layers));
    } else if ti.v2() && !flags.allow_missing_piece_layer {
        // v2 torrents need piece layers to be valid .torrent files
        return Err(crate::TorrentError::TorrentMissingPieceLayer.into());
    }

    if !atp.url_seeds.is_empty() && !flags.no_http_seeds {
        ret.insert(
            b"url-list".to_vec(),
            Entry::List(atp.url_seeds.iter().cloned().map(Entry::String).collect()),
        );
    }
    if !atp.http_seeds.is_empty() && !flags.no_http_seeds {
        ret.insert(
            b"httpseeds".to_vec(),
            Entry::List(atp.http_seeds.iter().cloned().map(Entry::String).collect()),
        );
    }
    if !atp.dht_nodes.is_empty() && flags.include_dht_nodes {
        let nodes: Vec<Entry> = atp
            .dht_nodes
            .iter()
            .map(|(h, p)| Entry::List(vec![Entry::String(h.clone()), Entry::Int(i64::from(*p))]))
            .collect();
        ret.insert(b"nodes".to_vec(), Entry::List(nodes));
    }

    // similar/collections live in the info dict upstream; only add
    // top-level entries if the info section doesn't carry them already
    if info_node.error().is_none() && info_node.is_valid() {
        if !ti.similar_torrents().is_empty() && !info_node.dict_find(b"similar").is_valid() {
            ret.insert(
                b"similar".to_vec(),
                Entry::List(
                    ti.similar_torrents()
                        .iter()
                        .map(|h| Entry::String(h.as_bytes().to_vec()))
                        .collect(),
                ),
            );
        }
        if !ti.collections().is_empty() && !info_node.dict_find(b"collections").is_valid() {
            ret.insert(
                b"collections".to_vec(),
                Entry::List(
                    ti.collections()
                        .iter()
                        .cloned()
                        .map(Entry::String)
                        .collect(),
                ),
            );
        }
    }

    if atp.trackers.len() == 1 {
        ret.insert(b"announce".to_vec(), Entry::String(atp.trackers[0].clone()));
    } else if atp.trackers.len() > 1 {
        ret.insert(
            b"announce-list".to_vec(),
            build_tracker_list(&atp.trackers, &atp.tracker_tiers),
        );
    }

    Ok(Entry::Dict(ret))
}

/// `write_torrent_file_buf`.
pub fn write_torrent_file_buf(atp: &AddTorrentParams, flags: WriteTorrentFlags) -> Result<Vec<u8>> {
    Ok(bencode(&write_torrent_file(atp, flags)?))
}
