// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `torrent` — per-torrent state machine. Owns the piece picker,
//! the peer set (`PeerConn`s keyed by an opaque id), the have-set,
//! and the disk-storage index. Wire and disk stay outside: the
//! session feeds decoded messages and drains [`TorrentEffect`]s
//! (send, write, read, announce, alerts).
//!
//! Port of `torrent.cpp`'s core: `peer_has`/`peer_lost` refcount
//! glue, `add_piece`/`piece_finished` verification flow, seeder
//! transitions — minus upstream's io_service entanglement.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bitdaemon_common::types::PieceBlock;
use bitdaemon_common::types::{FileIndex, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, PeerRequest, Sha1Hash};

use bitdaemon_bencode::bdecode_checked;
use bitdaemon_core::add_torrent_params::AddTorrentParams;
use bitdaemon_core::torrent_info::{LoadTorrentLimits, TorrentInfo};
use bitdaemon_net::peer_wire::PeerMessage;

use crate::peer_conn::{PeerConn, PeerEffect};
use crate::piece_picker::{BlockState, PickerOptions, PiecePicker};
use crate::smart_ban::{BanDecision, SmartBan};
use crate::ut_metadata::{MetadataResult, UtMetadataTracker};

/// Side-effect the session must perform — the disk/tracker/wire
/// calls upstream makes directly on `session_impl`.
#[derive(Debug)]
pub enum TorrentEffect {
    /// send `msg` to peer `peer`.
    SendTo { peer: u64, msg: PeerMessage },
    /// issue a read job serving a peer's request.
    DiskRead { peer: u64, req: PeerRequest },
    /// write a downloaded block.
    DiskWrite { req: PeerRequest, data: Vec<u8> },
    /// verify a piece's hash (`verify_piece` — resume restore and
    /// post-write verification).
    DiskHash { piece: PieceIndex },
    /// piece passed verification → `have` broadcast pending.
    PiecePassed(PieceIndex),
    /// piece failed verification → re-download.
    PieceFailed(PieceIndex),
    /// file finished — alert upstream `file_completed`.
    FileCompleted(FileIndex),
    /// the torrent just became a seed.
    BecameSeed,
    /// disconnect a misbehaving peer.
    DisconnectPeer { peer: u64, reason: &'static str },
    /// `yourip` seen in a BEP-10 handshake — routed to the
    /// session's `ip_voter` (`source_peer`).
    YourIp(std::net::IpAddr),
}

/// `torrent` — one swarm.
pub struct Torrent {
    /// `m_torrent_file->info_hash()`.
    pub info_hash: Sha1Hash,
    /// `num_pieces`.
    pub num_pieces: i32,
    /// `piece_length`.
    pub piece_length: i64,
    /// `m_have_piece` — our verified bitfield.
    pub have: Bitfield,
    /// `m_torrent_file` piece hashes — `hash_for_piece` table
    /// (v1). `None` until metadata arrives (`ut_metadata`).
    pub piece_hashes: Option<Vec<Sha1Hash>>,
    /// `m_picker` — `None` until `init_picker` (no metadata yet).
    pub picker: Option<PiecePicker>,
    /// `m_connections` — peers keyed by caller-chosen id.
    pub peers: BTreeMap<u64, PeerConn>,
    /// `m_trackers` — tier-ordered announce entries.
    pub trackers: crate::tracker_list::TrackerList,
    /// `m_web_seeds` — BEP-17/19 url-seed endpoints.
    pub web_seeds: Vec<String>,
    /// `m_torrent_file->files()` — file layout for web-seed
    /// range mapping.
    pub files: Option<std::sync::Arc<bitdaemon_core::FileStorage>>,
    /// round-robin cursor over `web_seeds`.
    pub web_seed_idx: usize,
    /// blocks in flight toward web seeds (`m_requests` of the
    /// virtual web peer).
    pub web_seed_outstanding: std::collections::BTreeSet<bitdaemon_common::types::PieceBlock>,
    /// the virtual web peer's bitfield was refcounted once
    /// (`web_peer_connection` bumps availability for every piece
    /// on connect — the seed serves the full payload).
    web_seed_refcounted: bool,
    /// `m_peer_list` — known swarm members (tracker/PEX/DHT/LSD/
    /// resume candidates). Upstream stores `torrent_peer`s owned by
    /// the list; ours are `TorrentPeer`s in `PeerList`.
    pub peer_list: bitdaemon_net::peer_list::PeerList,
    /// `m_storage` — disk job routing index.
    pub storage: StorageIndex,
    /// `m_seeds` / peers count for `state()`.
    pub num_seeds: usize,
    /// `m_paused`.
    pub paused: bool,
    /// `m_finished` — all pieces verified.
    pub finished: bool,
    /// `m_seeding` — was complete from the start or passed check.
    pub seeding: bool,
    /// bytes verified total (`m_total_downloaded`-ish).
    pub verified_bytes: i64,
    /// `m_queued_time_critical` aggregate — peers in endgame.
    pub want_peers: bool,
    /// `m_resume_data` fields the picker needs at init.
    pub piece_priority_dirty: bool,
    /// `ut_metadata` download state — magnet-added torrents only.
    pub metadata_tracker: UtMetadataTracker,
    /// raw info dict (the bytes `ut_metadata` serves). Set by
    /// `set_metadata`/`from_params`.
    pub metadata_raw: Option<Vec<u8>>,
    /// our local extension id for `ut_metadata` (the id WE put in
    /// our handshake; peers address it back).
    pub local_ut_metadata_id: u8,
    /// `m_magnet_link` — added without metadata.
    pub magnet: bool,
    /// `m_private` (BEP-27) — disables LSD/PEX/DHT peer discovery.
    pub private: bool,
    /// `smart_ban` plugin state (`extensions/smart_ban.cpp`).
    pub smart_ban: SmartBan,
    /// `m_auto_managed` — queue promotion/demotion is the
    /// session's job (`active_downloads`/`active_seeds`).
    pub auto_managed: bool,
    /// `is_torrent_queued` — held back by the auto-managed queue
    /// (distinct from user `paused`, like upstream
    /// `graceful_pause` vs the queue state).
    pub queued: bool,
    /// last `ut_pex` broadcast (`m_last_pex` timing state).
    pub last_pex: Option<Instant>,
    /// `m_added_time`.
    pub added_time: std::time::SystemTime,
    /// `m_save_path`.
    pub save_path: String,
    /// display name (`ti->name()` or `atp.name`).
    pub name: String,
    /// a `check_files` job is in flight (`m_checking_piece` coarse).
    pub checking: bool,
    /// payload rate samples for `torrent_status`
    /// (`stat().download_payload_rate()` — sampled per tick).
    pub down_rate: i64,
    /// upload payload B/s.
    pub up_rate: i64,
    /// previous sample totals.
    pub stat_prev_down: i64,
    /// previous sample totals (upload).
    pub stat_prev_up: i64,
    /// last sample instant.
    pub stat_sample: Option<std::time::Instant>,
    /// `m_file_priority` — per-file `DownloadPriority` (empty =
    /// all default). Drives `piece_priority` upstream-style.
    pub file_priorities: Vec<bitdaemon_common::types::DownloadPriority>,
    /// `m_sequential_download` — sequential pick order (streaming).
    pub sequential: bool,
    /// pieces left in an in-flight `force_recheck` pass
    /// (`checking` clears when it reaches 0).
    pub checking_pieces: i32,
    /// `m_error` — last tracker/disk error string for
    /// `torrent_status::error` ("" = no error).
    pub last_error: String,
}

impl Torrent {
    /// `torrent` ctor — `have` from resume data (`verified_pieces`),
    /// `None` when checking is still needed.
    #[must_use]
    pub fn new(
        info_hash: Sha1Hash,
        num_pieces: i32,
        piece_length: i64,
        have: Bitfield,
        storage: StorageIndex,
    ) -> Self {
        Self {
            info_hash,
            num_pieces,
            piece_length,
            piece_hashes: None,
            have,
            picker: None,
            peers: BTreeMap::new(),
            trackers: crate::tracker_list::TrackerList::default(),
            web_seeds: Vec::new(),
            files: None,
            web_seed_idx: 0,
            web_seed_outstanding: std::collections::BTreeSet::new(),
            web_seed_refcounted: false,
            peer_list: bitdaemon_net::peer_list::PeerList::default(),
            storage,
            num_seeds: 0,
            paused: false,
            finished: false,
            seeding: false,
            verified_bytes: 0,
            want_peers: true,
            piece_priority_dirty: false,
            metadata_tracker: UtMetadataTracker::new(),
            metadata_raw: None,
            local_ut_metadata_id: 1,
            magnet: false,
            private: false,
            smart_ban: SmartBan::new(),
            auto_managed: true,
            last_pex: None,
            queued: false,
            added_time: std::time::SystemTime::now(),
            save_path: String::new(),
            name: String::new(),
            checking: false,
            down_rate: 0,
            up_rate: 0,
            stat_prev_down: 0,
            stat_prev_up: 0,
            stat_sample: None,
            file_priorities: Vec::new(),
            sequential: false,
            checking_pieces: 0,
            last_error: String::new(),
        }
    }

    /// `torrent` ctor path — build a torrent from `AddTorrentParams`:
    /// `ti` metadata → sizes/hashes/picker, `have`/`verified` and
    /// `unfinished_pieces` via `apply_resume_data`, `peers`/`banned_peers`
    /// into `peer_list`, `paused`/`magnet` flags. `None` when `ti` is
    /// absent and `info_hash` can't be derived (upstream requires the
    /// info-hash).
    #[must_use]
    pub fn from_params(
        atp: &AddTorrentParams,
        storage: StorageIndex,
        out: &mut Vec<TorrentEffect>,
    ) -> Option<Self> {
        let ih = if atp.info_hashes.has_v1() {
            atp.info_hashes.v1
        } else {
            atp.ti.as_ref().and_then(|ti| ti.info_hash_v1())?
        };
        let mut t = match &atp.ti {
            Some(ti) => {
                let mut t = Self::new(
                    ih,
                    ti.num_pieces(),
                    i64::from(ti.piece_length()),
                    Bitfield::with_size(ti.num_pieces() as usize),
                    storage,
                );
                t.piece_hashes = Some(
                    (0..ti.num_pieces())
                        .map(|i| ti.hash_for_piece(PieceIndex(i)))
                        .collect(),
                );
                t.metadata_raw = Some(ti.info_section().to_vec());
                t.files = Some(std::sync::Arc::new(ti.files().clone()));
                t.init_picker(false);
                // upstream `check_resume` order: file priorities
                // first, explicit `piece_priorities` override after
                if !atp.file_priorities.is_empty() {
                    t.prioritize_files(&atp.file_priorities);
                }
                t.apply_resume_data(atp, out);
                t
            }
            None => {
                // magnet add — metadata pending
                let mut t = Self::new(ih, 0, 0, Bitfield::default(), storage);
                t.magnet = true;
                t
            }
        };
        t.web_seeds = atp
            .url_seeds
            .iter()
            .map(|u| String::from_utf8_lossy(u).into_owned())
            .collect();
        for &ep in &atp.peers {
            t.add_swarm_peer(
                ep,
                bitdaemon_common::flags::PeerSourceFlags::RESUME_DATA,
                true,
            );
        }
        for &ep in &atp.banned_peers {
            let (i, _) = t.peer_list.add_peer(
                ep,
                bitdaemon_common::flags::PeerSourceFlags::RESUME_DATA,
                true,
            );
            t.peer_list.ban_peer(i);
        }
        t.paused = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::PAUSED);
        t.sequential = atp
            .flags
            .contains(bitdaemon_common::flags::TorrentFlags::SEQUENTIAL_DOWNLOAD);
        t.save_path = String::from_utf8_lossy(&atp.save_path).into_owned();
        if !atp.name.is_empty() {
            t.name = String::from_utf8_lossy(&atp.name).into_owned();
        } else if let Some(ti) = &atp.ti {
            t.name = String::from_utf8_lossy(ti.name()).into_owned();
        }
        Some(t)
    }

    /// `set_metadata` — a magnet just resolved: SHA-1 of the raw
    /// info section must equal `info_hash` (BEP-9). On match the
    /// torrent_info is parsed and picker+hashes initialized.
    /// Returns `false` on hash mismatch (`failed_hash_check`
    /// upstream — caller penalizes sources via
    /// `metadata_tracker.reset_on_hash_failure`).
    pub fn set_metadata(&mut self, metadata: &[u8], cfg: &LoadTorrentLimits) -> bool {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(metadata);
        let actual = Sha1Hash::from_bytes(&h.finalize()).unwrap_or_default();
        if actual != self.info_hash {
            return false;
        }
        let node = match bdecode_checked(metadata) {
            Ok(n) => n,
            Err(_) => return false,
        };
        let Ok(info) = TorrentInfo::from_info_node(&node, cfg) else {
            return false;
        };
        self.num_pieces = info.num_pieces();
        self.piece_length = i64::from(info.piece_length());
        if self.name.is_empty() {
            self.name = String::from_utf8_lossy(info.name()).into_owned();
        }
        self.have = Bitfield::with_size(info.num_pieces() as usize);
        self.piece_hashes = Some(
            (0..info.num_pieces())
                .map(|i| info.hash_for_piece(PieceIndex(i)))
                .collect(),
        );
        self.metadata_raw = Some(metadata.to_vec());
        self.magnet = false;
        self.init_picker(true);
        true
    }

    /// `on_metadata` — one `ut_metadata` `data` packet arrived.
    /// `peer` = the peer that served it (source bookkeeping).
    /// Returns the outcome; on `Complete` the caller feeds
    /// `metadata_tracker.metadata()` to [`Torrent::set_metadata`].
    pub fn on_ut_metadata_data(
        &mut self,
        buf: &[u8],
        piece: i32,
        total_size: i64,
        peer: u64,
        max_metadata_size: i64,
    ) -> MetadataResult {
        let r = self.metadata_tracker.received_metadata(
            buf,
            piece,
            total_size,
            max_metadata_size,
            self.piece_hashes.is_some(),
        );
        if matches!(r, MetadataResult::Stored | MetadataResult::Complete) {
            self.metadata_tracker.set_source(piece, peer);
        }
        r
    }

    /// `on_metadata` dispatch — a `ut_metadata` body arrived:
    /// `request` → serve our info dict (or reject); `data` → feed
    /// the tracker (complete → `set_metadata`, bad hash →
    /// `reset_on_hash_failure`); `reject` → ignored (the scheduler
    /// retries per `metadata_request` backoff).
    fn on_metadata_packet(&mut self, peer: u64, body: &[u8], out: &mut Vec<TorrentEffect>) {
        let Some((msg, piece, total, data)) = crate::ut_metadata::parse_metadata_packet(body)
        else {
            return;
        };
        match msg {
            crate::ut_metadata::MetadataMsg::Request => {
                let resp = match &self.metadata_raw {
                    Some(md) => crate::ut_metadata::metadata_packet(
                        crate::ut_metadata::MetadataMsg::Piece,
                        piece as i32,
                        true,
                        md,
                    ),
                    None => crate::ut_metadata::metadata_packet(
                        crate::ut_metadata::MetadataMsg::Reject,
                        piece as i32,
                        false,
                        &[],
                    ),
                };
                out.push(TorrentEffect::SendTo {
                    peer,
                    msg: PeerMessage::Extended {
                        id: self.local_ut_metadata_id,
                        data: resp.into(),
                    },
                });
            }
            crate::ut_metadata::MetadataMsg::Piece => {
                let r = self.on_ut_metadata_data(data, piece as i32, total, peer, 30 * 1024 * 1024);
                if matches!(r, MetadataResult::Complete) {
                    let md = self.metadata_tracker.metadata().to_vec();
                    if self.set_metadata(&md, &LoadTorrentLimits::default()) {
                        self.metadata_tracker.clear();
                    } else {
                        self.metadata_tracker.reset_on_hash_failure();
                    }
                }
            }
            crate::ut_metadata::MetadataMsg::Reject => {}
        }
    }

    /// `ut_metadata_plugin` tick — while metadata is missing, ask
    /// peers that advertised `ut_metadata` for the next piece.
    pub fn metadata_tick(&mut self, out: &mut Vec<TorrentEffect>) {
        if self.piece_hashes.is_some() {
            return;
        }
        let ids: Vec<u64> = self
            .peers
            .iter()
            .filter(|(_, c)| c.ext_ut_metadata != 0 && c.metadata_size.is_some())
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.metadata_tick_peer(id, out);
        }
    }

    /// ask `peer` for the next needed metadata piece.
    fn metadata_tick_peer(&mut self, peer: u64, out: &mut Vec<TorrentEffect>) {
        if self.piece_hashes.is_some() {
            return;
        }
        let ext = self.peers.get(&peer).map_or(0, |c| c.ext_ut_metadata);
        if ext == 0 {
            return;
        }
        let piece = self.metadata_request();
        if piece < 0 {
            return;
        }
        let body = crate::ut_metadata::metadata_packet(
            crate::ut_metadata::MetadataMsg::Request,
            piece,
            false,
            &[],
        );
        out.push(TorrentEffect::SendTo {
            peer,
            msg: PeerMessage::Extended {
                id: ext,
                data: body.into(),
            },
        });
    }

    /// `ut_pex` receive — parse the diff and feed the swarm list
    /// (upstream `on_pex_message` → `peer_list.add_peer` per added).
    /// `max_pex_peers` mirrors `settings_pack::max_pex_peers` (50).
    pub fn on_pex_packet(&mut self, body: &[u8]) {
        let Some((added, _dropped)) = crate::ut_pex::parse_pex_message(
            body,
            crate::ut_pex::MAX_PEER_ENTRIES / 2,
            self.peer_list.num_peers(),
        ) else {
            return;
        };
        for p in added {
            // upstream skips local addresses unless the sender is
            // local — our peers are anonymous endpoints, keep all
            self.peer_list
                .add_peer(p.addr, bitdaemon_common::flags::PeerSourceFlags::PEX, true);
        }
    }

    /// `second_tick`'s pex section — every `PEX_INTERVAL` send each
    /// `ut_pex`-capable peer a diff of our connected endpoints.
    pub fn pex_tick(&mut self, now: Instant, out: &mut Vec<TorrentEffect>) {
        const PEX_INTERVAL: Duration = Duration::from_secs(60);
        if self
            .last_pex
            .is_some_and(|t| now.duration_since(t) < PEX_INTERVAL)
        {
            return;
        }
        self.last_pex = Some(now);
        // the connected set we advertise (endpoints we dialed or
        // accepted — pex excludes peers without a listen address,
        // which `remote` approximates)
        let connected: Vec<(SocketAddr, u8)> = self
            .peers
            .values()
            .filter_map(|c| c.remote)
            .map(|ep| (ep, 0u8))
            .collect();
        let ids: Vec<(u64, u8)> = self
            .peers
            .iter()
            .filter(|(_, c)| c.ext_ut_pex != 0)
            .map(|(id, c)| (*id, c.ext_ut_pex))
            .collect();
        for (id, ext) in ids {
            let c = match self.peers.get_mut(&id) {
                Some(c) => c,
                None => continue,
            };
            let (body, sent) = crate::ut_pex::build_pex_message(&connected, &c.pex_sent);
            c.pex_sent = sent;
            out.push(TorrentEffect::SendTo {
                peer: id,
                msg: PeerMessage::Extended {
                    id: ext,
                    data: body.into(),
                },
            });
        }
    }

    /// `metadata_request` — which ut_metadata piece to ask for next
    /// (-1 = nothing requestable right now). Caller wraps it in a
    /// `ut_metadata` `request` packet via `metadata_packet`.
    pub fn metadata_request(&mut self) -> i32 {
        let peers = &self.peers;
        self.metadata_tracker
            .metadata_request(self.piece_hashes.is_some(), |src| peers.contains_key(&src))
    }

    /// `init_picker` — create the picker (metadata arrived or
    /// resume check done). `priorities`/`have` seeded from resume.
    pub fn init_picker(&mut self, now: bool) {
        if self.picker.is_none() {
            self.picker = Some(PiecePicker::new(
                i64::from(self.num_pieces) * self.piece_length,
                self.piece_length as i32,
            ));
            let _ = now;
        }
    }

    /// `write_resume_data` (upstream `torrent::write_resume_data`) —
    /// snapshot the picker state into `AddTorrentParams` fields.
    /// `verified`/`have` bitfields, `piece_priorities`, and
    /// `unfinished_pieces` (downloading pieces with ≥1
    /// finished/writing block → finished-block bitmask).
    pub fn to_resume_data(&self, atp: &mut AddTorrentParams) {
        atp.info_hashes.v1 = self.info_hash;
        atp.have_pieces = self.have.clone();
        atp.verified_pieces = self.have.clone();
        // upstream embeds the `info` dict when the torrent has
        // metadata — rebuild `ti` from our raw copy so
        // `write_resume_data_buf` emits it and `load` restores
        // piece count + hashes.
        if let Some(raw) = &self.metadata_raw {
            let node = bitdaemon_bencode::bdecode(raw);
            if let Ok(ti) = TorrentInfo::from_info_node(&node, &LoadTorrentLimits::default()) {
                atp.ti = Some(std::sync::Arc::new(ti));
            }
        }
        if let Some(p) = self.picker.as_ref() {
            atp.piece_priorities = (0..self.num_pieces)
                .map(|i| p.piece_priority(PieceIndex(i)))
                .collect();
            for dp in p.get_download_queue() {
                if dp.finished == 0 && dp.writing == 0 {
                    continue;
                }
                let mut mask = Bitfield::with_size(p.blocks_in_piece(dp.index) as usize);
                for (i, b) in p.blocks_for_piece(&dp).iter().enumerate() {
                    if b.state == BlockState::Finished {
                        mask.set_bit(i);
                    }
                }
                atp.unfinished_pieces.insert(dp.index.0, mask);
            }
        }
    }

    /// `check_resume` (upstream `torrent::check_resume`) — apply
    /// `AddTorrentParams` to a torrent whose picker is initialized:
    /// seed `have`/`verified` bitfields, `piece_priorities`, and
    /// `mark_as_finished` every bit set in `unfinished_pieces`.
    /// Returns effects for pieces that just became complete
    /// (`verify_piece` upstream → we emit `Hash` requests via
    /// `PieceReady`-style effect: here `HashPiece` alerts).
    pub fn apply_resume_data(&mut self, atp: &AddTorrentParams, out: &mut Vec<TorrentEffect>) {
        self.init_picker(false);
        let picker = self.picker.as_mut();
        let Some(picker) = picker else { return };

        for i in 0..self.num_pieces.min(atp.verified_pieces.size() as i32) {
            if atp.verified_pieces.get_bit(i as usize) && !self.have.get_bit(i as usize) {
                self.have.set_bit(i as usize);
            }
        }
        for (i, prio) in atp.piece_priorities.iter().enumerate() {
            if i as i32 >= self.num_pieces {
                break;
            }
            picker.set_piece_priority(PieceIndex(i as i32), *prio);
        }
        for (piece, blocks) in &atp.unfinished_pieces {
            let idx = PieceIndex(*piece);
            if *piece < 0 || *piece >= self.num_pieces || self.have.get_bit(*piece as usize) {
                continue;
            }
            if picker.have_piece(idx) {
                picker.we_dont_have(idx);
            }
            let nbits = picker.blocks_in_piece(idx).min(blocks.size() as i32);
            for k in 0..nbits {
                if blocks.get_bit(k as usize) {
                    picker.mark_as_finished(
                        PieceBlock {
                            piece_index: idx,
                            block_index: k,
                        },
                        None,
                    );
                }
            }
            if picker.is_piece_finished(idx) {
                // upstream `verify_piece` — needs a hash pass
                out.push(TorrentEffect::DiskHash { piece: idx });
            }
        }
        if self.have.count() as i32 == self.num_pieces && self.num_pieces > 0 {
            self.finished = true;
            self.seeding = true;
        }
    }

    /// `get_peer_info` — build the public `peer_info` snapshot for
    /// one connected peer (`Torrent::get_peer_info` upstream fills
    /// from `peer_connection` counters).
    #[must_use]
    pub fn peer_info(
        &self,
        id: u64,
        remote: Option<std::net::SocketAddr>,
    ) -> Option<crate::peer_info::PeerInfo> {
        use crate::peer_info::{BandwidthState, ConnectionType, PeerFlags, PeerInfo};
        let c = self.peers.get(&id)?;
        let mut flags = PeerFlags::default();
        if c.interesting {
            flags = flags.with(PeerFlags::INTERESTING);
        }
        if c.choked {
            flags = flags.with(PeerFlags::CHOKED);
        }
        if c.peer_interested {
            flags = flags.with(PeerFlags::REMOTE_INTERESTED);
        }
        if c.peer_choked {
            flags = flags.with(PeerFlags::REMOTE_CHOKED);
        }
        if c.bitfield_received {
            flags = flags.with(PeerFlags::HANDSHAKE);
        }
        if c.remote_upload_only {
            flags = flags.with(PeerFlags::UPLOAD_ONLY);
        }
        if c.pipeline.snubbed {
            flags = flags.with(PeerFlags::SNUBBED);
        }
        if c.pipeline.endgame {
            flags = flags.with(PeerFlags::ENDGAME_MODE);
        }
        if c.num_pieces >= self.num_pieces {
            flags = flags.with(PeerFlags::SEED);
        }
        Some(PeerInfo {
            flags,
            source: bitdaemon_common::flags::PeerSourceFlags::INCOMING,
            up_speed: 0,
            down_speed: 0,
            payload_up_speed: 0,
            payload_down_speed: 0,
            pid: c.peer_id.unwrap_or_default(),
            pieces: c.peer_pieces.clone(),
            queue_bytes: c.pipeline.outstanding_bytes.min(i64::from(i32::MAX)) as i32,
            request_timeout: if c.pipeline.download_queue.is_empty() {
                -1
            } else {
                c.pipeline.request_timeout_setting
            },
            send_buffer_size: 0,
            used_send_buffer: 0,
            receive_buffer_size: 0,
            used_receive_buffer: 0,
            receive_buffer_watermark: 0,
            num_hashfails: 0,
            download_queue_length: c.pipeline.download_queue.len() as i32,
            timed_out_requests: c
                .pipeline
                .download_queue
                .iter()
                .filter(|b| b.timed_out)
                .count() as i32,
            busy_requests: c.pipeline.download_queue.iter().filter(|b| b.busy).count() as i32,
            requests_in_buffer: c.pipeline.request_queue.len() as i32,
            target_dl_queue_length: i32::from(c.pipeline.desired_queue_size),
            upload_queue_length: c.upload_queue.len() as i32,
            failcount: 0,
            downloading_piece_index: c
                .pipeline
                .download_queue
                .first()
                .map_or(-1, |b| b.block.piece_index.0),
            downloading_block_index: c
                .pipeline
                .download_queue
                .first()
                .map_or(-1, |b| b.block.block_index),
            downloading_progress: 0,
            downloading_total: 0,
            connection_type: ConnectionType::STANDARD_BITTORRENT,
            pending_disk_bytes: 0,
            pending_disk_read_bytes: 0,
            send_quota: 0,
            receive_quota: 0,
            rtt: 0,
            num_pieces: c.num_pieces,
            download_rate_peak: 0,
            upload_rate_peak: 0,
            progress_ppm: if self.num_pieces > 0 {
                (i64::from(c.num_pieces) * 1_000_000 / i64::from(self.num_pieces)) as i32
            } else {
                0
            },
            remote_endpoint: remote,
            local_endpoint: None,
            read_state: BandwidthState::NETWORK,
            write_state: BandwidthState::NETWORK,
            client: if c.client_version.is_empty() {
                // `peer_connection::peer_info_struct` fallback —
                // `identify_client` on the handshake peer id.
                c.peer_id.as_ref().map_or_else(Vec::new, |pid| {
                    bitdaemon_common::identify_client::identify_client(pid).into_bytes()
                })
            } else {
                c.client_version.clone()
            },
        })
    }

    /// `files_checked` (upstream `torrent::files_checked`) — verdict
    /// of `async_check_files` on the fastresume data. Accepted →
    /// `have` stands and a complete torrent becomes `seeding`
    /// (`no_error` + `seed_mode`). Rejected (`need_full_check`) →
    /// the resume-supplied bits are cleared; the caller reschedules
    /// per-piece `DiskHash` jobs for the real check.
    pub fn on_files_checked(&mut self, accepted: bool, out: &mut Vec<TorrentEffect>) {
        self.checking = false;
        if !accepted {
            // the picker's have-tracking only knows pieces that
            // passed `piece_verified` — resume bits live solely in
            // `self.have`, clearing it is sufficient.
            self.have.clear_all();
            self.finished = false;
            self.seeding = false;
            return;
        }
        if self.have.count() as i32 == self.num_pieces && self.num_pieces > 0 {
            self.finished = true;
            self.seeding = true;
            out.push(TorrentEffect::BecameSeed);
        }
    }

    /// `add_tracker_url` — upstream `add_tracker_url`.
    pub fn add_tracker(&mut self, url: String, tier: i32) {
        if self.trackers.find(&url).is_none() {
            self.trackers
                .add(crate::tracker_list::AnnounceEntry::new(url, tier));
        }
    }

    /// `announce_req` — the `AnnounceParams` for a due tracker
    /// (`torrent::tracker_request`). `port`/`key`/`peer_id` come
    /// from the session. `left` is approximated as
    /// `num_pieces * piece_length - verified` (the tail-piece
    /// delta needs `total_size` — close enough for `left=`).
    #[must_use]
    pub fn announce_params(
        &self,
        peer_id: bitdaemon_common::PeerId,
        port: u16,
        event: i32,
        key: i32,
    ) -> bitdaemon_net::tracker_client::AnnounceParams {
        bitdaemon_net::tracker_client::AnnounceParams {
            info_hash: self.info_hash.to_bytes(),
            peer_id: *peer_id.as_bytes(),
            port,
            uploaded: 0,
            downloaded: self.verified_bytes,
            left: (i64::from(self.num_pieces) * self.piece_length - self.verified_bytes).max(0),
            event,
            num_want: -1,
            key,
        }
    }

    /// `is_seed`.
    #[must_use]
    pub fn is_seed(&self) -> bool {
        self.seeding || self.finished
    }

    /// `add_peer` (swarm entry, not connection) — tracker/PEX/DHT/
    /// LSD feed. Returns the `PeerList` index.
    pub fn add_swarm_peer(
        &mut self,
        remote: std::net::SocketAddr,
        src: bitdaemon_common::flags::PeerSourceFlags,
        connectable: bool,
    ) -> usize {
        self.peer_list.add_peer(remote, src, connectable).0
    }

    /// `connect_to_peer` (upstream `torrent_handle::connect_peer`)
    /// — insert `remote` in the peer list as an immediate
    /// connect candidate (failcount untouched → next pump picks it).
    pub fn connect_peer(&mut self, remote: std::net::SocketAddr) -> usize {
        self.add_swarm_peer(remote, bitdaemon_common::flags::PeerSourceFlags::NONE, true)
    }

    /// `piece_availability` — swarm availability vector
    /// (`torrent::piece_availability` → picker availability).
    #[must_use]
    pub fn piece_availability(&self) -> Vec<i32> {
        let mut v = Vec::new();
        if let Some(p) = &self.picker {
            p.get_availability_vec(&mut v);
        }
        v
    }

    /// `get_download_queue` — in-flight partial pieces
    /// (`partial_piece_info` upstream: finished/writing/requested
    /// counts per downloading piece).
    #[must_use]
    pub fn get_download_queue(&self) -> Vec<crate::piece_picker::DownloadingPiece> {
        self.picker.as_ref().map_or_else(
            Vec::new,
            crate::piece_picker::PiecePicker::get_download_queue,
        )
    }

    /// `find_connect_candidates` — the session's connect pump asks
    /// for the next endpoints to dial (`torrent::connect_to_peer`).
    #[must_use]
    pub fn connect_candidates(
        &mut self,
        session_time: i32,
        state: &bitdaemon_net::peer_list::PeerListState,
    ) -> Vec<std::net::SocketAddr> {
        // keep `finished` in sync so seeds skip seeds/upload-only
        let mut st = state.clone();
        st.is_finished = self.is_seed();
        let idxs = self.peer_list.find_connect_candidates(session_time, &st);
        // `set_connection` at dial start (upstream's peer_connection
        // ctor) — the entry stops being a candidate until
        // `peer_connect_failed`/`peer_disconnected` reports back.
        for i in &idxs {
            self.peer_list.set_connected(*i);
        }
        idxs.into_iter()
            .filter_map(|i| self.peer_list.peers().get(i).and_then(|p| p.ip()))
            .collect()
    }

    /// `connection_closed` for a dial that never completed —
    /// bumps `last_connected` and `failcount` so the peer isn't
    /// retried immediately (`inc_failcount` + timestamp upstream).
    pub fn peer_connect_failed(&mut self, remote: std::net::SocketAddr, session_time: i32) {
        if let Some(i) = self.peer_index(remote) {
            self.peer_list.connection_closed(i, session_time);
            self.peer_list.inc_failcount(i);
        }
    }

    /// `set_connection` — mark the swarm entry for `remote` as
    /// connected (also covers inbound accepts).
    pub fn peer_connected_addr(&mut self, remote: std::net::SocketAddr) {
        if let Some(i) = self.peer_index(remote) {
            self.peer_list.set_connected(i);
        }
    }

    /// `connection_closed` — clear the connection flag + stamp
    /// `last_connected` for the dial backoff.
    pub fn peer_disconnected_addr(&mut self, remote: std::net::SocketAddr, session_time: i32) {
        if let Some(i) = self.peer_index(remote) {
            self.peer_list.connection_closed(i, session_time);
        }
    }

    fn peer_index(&self, remote: std::net::SocketAddr) -> Option<usize> {
        self.peer_list
            .peers()
            .iter()
            .position(|p| p.ip() == Some(remote))
    }

    /// `add_peer` — a new connection registered.
    pub fn add_peer(&mut self, id: u64, conn: PeerConn) {
        self.peers.insert(id, conn);
    }

    /// `peer_disconnected` — refcount down everything it had and
    /// abort its in-flight requests (upstream `peer_lost` +
    /// `clear_peer` semantics).
    pub fn remove_peer(&mut self, id: u64, eff: &mut Vec<TorrentEffect>) {
        let Some(c) = self.peers.remove(&id) else {
            return;
        };
        if let Some(p) = self.picker.as_mut() {
            let key = c.peer_key();
            for i in 0..c.peer_pieces.size() {
                if c.peer_pieces.get_bit(i) {
                    p.dec_refcount(PieceIndex(i as i32));
                }
            }
            for b in c.outstanding() {
                p.abort_download(b, key);
            }
        }
        let _ = eff;
    }

    /// `peer_has`/`peer_lost` — apply [`PeerEffect`]s from a peer
    /// dispatch into picker/disk intents.
    pub fn apply_peer_effects(
        &mut self,
        peer_id: u64,
        effects: Vec<PeerEffect>,
        out: &mut Vec<TorrentEffect>,
    ) {
        let key = self.peers.get(&peer_id).and_then(PeerConn::peer_key);
        for e in effects {
            match e {
                PeerEffect::RefcountInc(i) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.inc_refcount(i);
                    }
                }
                PeerEffect::RefcountDec(i) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.dec_refcount(i);
                    }
                }
                PeerEffect::RefcountBulk(bits, inc) => {
                    if let Some(p) = self.picker.as_mut() {
                        if inc {
                            p.inc_refcount_bitfield(&bits);
                        } else {
                            p.dec_refcount_bitfield(&bits);
                        }
                    }
                }
                PeerEffect::DiskWrite(req, data) => {
                    out.push(TorrentEffect::DiskWrite { req, data });
                }
                PeerEffect::ReadRequest(req) => {
                    out.push(TorrentEffect::DiskRead { peer: peer_id, req });
                }
                PeerEffect::CancelRequest(_) => {}
                PeerEffect::MarkDownloaded(b) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.mark_as_writing(b, key);
                    }
                }
                PeerEffect::AbortDownload(b) => {
                    if let Some(p) = self.picker.as_mut() {
                        p.abort_download(b, key);
                    }
                }
                PeerEffect::MaybePick => self.drive_peer(peer_id, out),
                PeerEffect::ExtHandshake => {
                    // peer just advertised ut_metadata — ask right away
                    self.metadata_tick_peer(peer_id, out);
                }
                PeerEffect::MetadataPacket(body) => {
                    self.on_metadata_packet(peer_id, &body, out);
                }
                PeerEffect::PexPacket(body) => {
                    self.on_pex_packet(&body);
                }
                PeerEffect::DhtPort(_) => {}
                PeerEffect::YourIp(ip) => {
                    out.push(TorrentEffect::YourIp(ip));
                }
                PeerEffect::Disconnect(r) => {
                    out.push(TorrentEffect::DisconnectPeer {
                        peer: peer_id,
                        reason: r,
                    });
                }
            }
        }
    }

    /// feed one decoded message from `peer` — `dispatch` + effects.
    /// `now` is `session_time` (s) for inactivity bookkeeping.
    pub fn on_peer_message(
        &mut self,
        peer_id: u64,
        msg: &PeerMessage,
        now: i32,
        out: &mut Vec<TorrentEffect>,
    ) {
        let mut effects = Vec::new();
        if let Some(c) = self.peers.get_mut(&peer_id) {
            c.last_active = now;
            if let PeerMessage::Piece { data, .. } = msg {
                let n = data.len() as i64;
                c.stat_down_total += n;
                c.stat_down_round += n;
            }
            c.dispatch(msg, self.num_pieces, &mut effects);
        }
        self.apply_peer_effects(peer_id, effects, out);
    }

    /// `request_a_block` for one peer — `drive_requests` emits the
    /// wire `Request`s the caller sends.
    pub fn drive_peer(&mut self, peer_id: u64, out: &mut Vec<TorrentEffect>) {
        // the full drive needs picker + pipeline borrow — collect
        // requests then translate to effects
        if self.picker.is_none() {
            return;
        }
        let opts = if self.sequential {
            PickerOptions::RAREST_FIRST | PickerOptions::SEQUENTIAL
        } else {
            PickerOptions::RAREST_FIRST
        };
        // build peer ctx from an immutable snapshot first
        let (key, allowed_fast, suggested, peer_pieces, peer_choked, outstanding) = {
            let Some(c) = self.peers.get(&peer_id) else {
                return;
            };
            if c.peer_choked || c.peer_pieces.size() == 0 {
                return;
            }
            (
                c.peer_key(),
                c.allowed_fast.clone(),
                c.suggested_pieces.clone(),
                c.peer_pieces.clone(),
                c.peer_choked,
                c.outstanding(),
            )
        };
        let mut picked = Vec::new();
        // simple drive: ask the picker for blocks this peer has
        let mut pc_stats = crate::piece_picker::NullPickerStats;
        let tctx = crate::request_blocks::TorrentRequestCtx {
            is_seed: self.is_seed(),
            upload_mode: false,
            valid_metadata: true,
            files_checked: true,
            graceful_pause: false,
            time_critical_mode: false,
            num_peers: self.peers.len() as i32,
            whole_pieces_threshold: 0,
            strict_end_game_mode: false,
            piece_length: self.piece_length,
            block_size: 0x4000,
        };
        let pctx = crate::request_blocks::PeerRequestCtx {
            no_download: false,
            disconnecting: false,
            desired_queue_size: 5,
            outstanding: &outstanding,
            prefer_contiguous_blocks: 0,
            download_payload_rate: 0,
            peer_choked,
            allowed_fast: &allowed_fast,
            peer_pieces: &peer_pieces,
            peer_key: key,
            picker_options: opts,
            suggested_pieces: &suggested,
        };
        let p = self.picker.as_mut().expect("checked");
        let (_, _eg) = crate::request_blocks::request_a_block_out(
            p,
            &tctx,
            &pctx,
            &mut pc_stats,
            |b, busy| {
                picked.push((b, busy));
                true
            },
        );
        for (b, busy) in picked {
            if self
                .picker
                .as_mut()
                .is_some_and(|p| p.mark_as_downloading(b, key, opts))
            {
                if let Some(c2) = self.peers.get_mut(&peer_id) {
                    c2.add_request(b, busy, false);
                }
                out.push(TorrentEffect::SendTo {
                    peer: peer_id,
                    msg: PeerMessage::Request(PeerRequest {
                        piece: b.piece_index,
                        start: b.block_index * 0x4000,
                        length: 0x4000,
                    }),
                });
            }
        }
    }

    /// `web_seed_peer` pseudo-identity in the picker — a web seed
    /// counts as a virtual peer upstream (`web_peer_connection`
    /// derives `peer_connection`); blocks it serves are tracked
    /// under this key so real peers never double-request them.
    pub const WEB_SEED_PEER: u64 = u64::MAX;

    /// `web_peer_connection::write_request` drive — pick up to
    /// `max_blocks` blocks the web seed can serve (it has every
    /// piece) and mark them `requested` under [`Self::WEB_SEED_PEER`].
    /// Returns the url (round-robin over `web_seeds`) and the
    /// picked block requests, or `None` when nothing applies.
    pub fn next_web_seed_blocks(&mut self, max_blocks: i32) -> Option<(String, Vec<PeerRequest>)> {
        if self.web_seeds.is_empty()
            || self.is_seed()
            || self.picker.is_none()
            || self.files.is_none()
        {
            return None;
        }
        if !self.web_seed_refcounted {
            // virtual `peer_has` — the seed has every piece
            self.picker.as_mut().expect("checked").inc_refcount_all();
            self.web_seed_refcounted = true;
        }
        let all = Bitfield::with_value(self.num_pieces as usize, true);
        let tctx = crate::request_blocks::TorrentRequestCtx {
            is_seed: false,
            upload_mode: false,
            valid_metadata: true,
            files_checked: true,
            graceful_pause: false,
            time_critical_mode: false,
            num_peers: (self.peers.len() + 1) as i32,
            whole_pieces_threshold: 0,
            strict_end_game_mode: false,
            piece_length: self.piece_length,
            block_size: 0x4000,
        };
        let allowed_fast = Vec::new();
        let suggested = Vec::new();
        let outstanding: Vec<_> = self.web_seed_outstanding.iter().copied().collect();
        let pctx = crate::request_blocks::PeerRequestCtx {
            no_download: false,
            disconnecting: false,
            desired_queue_size: max_blocks,
            outstanding: &outstanding,
            prefer_contiguous_blocks: 0,
            download_payload_rate: 0,
            peer_choked: false,
            allowed_fast: &allowed_fast,
            peer_pieces: &all,
            peer_key: Some(Self::WEB_SEED_PEER),
            picker_options: PickerOptions::RAREST_FIRST,
            suggested_pieces: &suggested,
        };
        let mut picked = Vec::new();
        let mut pc_stats = crate::piece_picker::NullPickerStats;
        let (got, _) = crate::request_blocks::request_a_block_out(
            self.picker.as_mut().expect("checked"),
            &tctx,
            &pctx,
            &mut pc_stats,
            |b, busy| {
                picked.push((b, busy));
                picked.len() < max_blocks as usize
            },
        );
        if !got || picked.is_empty() {
            return None;
        }
        let mut reqs = Vec::with_capacity(picked.len());
        for (b, _busy) in picked {
            if !self.picker.as_mut().is_some_and(|p| {
                p.mark_as_downloading(b, Some(Self::WEB_SEED_PEER), PickerOptions::RAREST_FIRST)
            }) {
                continue;
            }
            self.web_seed_outstanding.insert(b);
            if !(0..self.num_pieces).contains(&b.piece_index.0) {
                continue;
            }
            let start = b.block_index * 0x4000;
            let piece_size = self.files.as_ref().map_or(self.piece_length, |f| {
                i64::from(f.piece_size(b.piece_index))
            });
            let length = (piece_size - i64::from(start)).min(0x4000) as i32;
            reqs.push(PeerRequest {
                piece: b.piece_index,
                start,
                length,
            });
        }
        if reqs.is_empty() {
            return None;
        }
        let url = self.web_seeds[self.web_seed_idx % self.web_seeds.len()].clone();
        self.web_seed_idx += 1;
        Some((url, reqs))
    }

    /// one web-seed block arrived — `mark_as_writing` + emit the
    /// disk write (the same path a wire `Piece` takes).
    pub fn on_web_seed_block(
        &mut self,
        req: PeerRequest,
        data: Vec<u8>,
        out: &mut Vec<TorrentEffect>,
    ) {
        let b = bitdaemon_common::types::PieceBlock {
            piece_index: req.piece,
            block_index: req.start / 0x4000,
        };
        self.web_seed_outstanding.remove(&b);
        if let Some(p) = self.picker.as_mut() {
            p.mark_as_writing(b, Some(Self::WEB_SEED_PEER));
        }
        out.push(TorrentEffect::DiskWrite { req, data });
    }

    /// a web-seed fetch failed — release the blocks so they can be
    /// re-requested from a real peer or another seed (`web_seed`
    /// `fail_count`→`abort_download` upstream path).
    pub fn on_web_seed_error(&mut self, reqs: &[PeerRequest]) {
        for r in reqs {
            let b = bitdaemon_common::types::PieceBlock {
                piece_index: r.piece,
                block_index: r.start / 0x4000,
            };
            self.web_seed_outstanding.remove(&b);
            if let Some(p) = self.picker.as_mut() {
                p.abort_download(b, Some(Self::WEB_SEED_PEER));
            }
        }
    }

    /// `is_seed`-side helper — all blocks of `piece` have been
    /// written (`picker` tracks open pieces; a piece leaves the
    /// open queue when every block is finished).
    #[must_use]
    pub fn piece_complete(&self, piece: PieceIndex) -> bool {
        // conservative: written when our have-bit set OR the picker
        // reports the piece fully downloaded; full fidelity needs
        // per-block state → treat picker-open as incomplete.
        if (piece.0 as usize) < self.have.size() && self.have.get_bit(piece.0 as usize) {
            return true;
        }
        // `is_piece_finished` = every block written/finished
        self.picker
            .as_ref()
            .is_some_and(|p| p.is_piece_finished(piece))
    }

    /// `hash_for_piece` — the expected SHA-1, if metadata held.
    #[must_use]
    pub fn expected_hash(&self, piece: PieceIndex) -> Option<Sha1Hash> {
        self.piece_hashes
            .as_ref()
            .and_then(|h| h.get(piece.0 as usize).copied())
    }

    /// `piece_finished` — verification result. `passed` → have-bit
    /// + broadcast; failed → restore want + smart_ban hook point.
    pub fn piece_verified(
        &mut self,
        piece: PieceIndex,
        passed: bool,
        out: &mut Vec<TorrentEffect>,
    ) {
        if passed {
            if (piece.0 as usize) < self.have.size() && !self.have.get_bit(piece.0 as usize) {
                self.have.set_bit(piece.0 as usize);
            }
            out.push(TorrentEffect::PiecePassed(piece));
            // broadcast `have` to every connected peer
            for id in self.peers.keys().copied().collect::<Vec<_>>() {
                out.push(TorrentEffect::SendTo {
                    peer: id,
                    msg: PeerMessage::Have(piece),
                });
            }
            if self.have.count() as i32 == self.num_pieces {
                self.finished = true;
                out.push(TorrentEffect::BecameSeed);
            }
        } else {
            if let Some(p) = self.picker.as_mut() {
                p.restore_piece(piece, &[]);
            }
            out.push(TorrentEffect::PieceFailed(piece));
            for id in self.peers.keys().copied().collect::<Vec<_>>() {
                self.drive_peer(id, out);
            }
        }
    }

    /// `piece_verified` + the `smart_ban` bookkeeping upstream does
    /// in `on_piece_finished`/`on_piece_failed`: on failure the
    /// per-block CRC of every downloader is recorded, on success the
    /// stored CRCs are re-checked. `addr` resolves a peer id, and
    /// `read_digest` re-reads a block's SHA-1 from storage (the
    /// session performs the disk read; `None` = skip, like upstream
    /// ignoring read errors). Bans become `DisconnectPeer` effects.
    pub fn piece_verified_ban(
        &mut self,
        piece: PieceIndex,
        piece_size: i64,
        passed: bool,
        out: &mut Vec<TorrentEffect>,
        addr: impl FnMut(u64) -> std::net::IpAddr,
        mut read_digest: impl FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) {
        if passed {
            for d in self
                .smart_ban
                .on_piece_passed(piece, piece_size, &mut read_digest)
            {
                if let BanDecision::Ban(peer) = d {
                    self.remove_peer(peer, out);
                    out.push(TorrentEffect::DisconnectPeer {
                        peer,
                        reason: "smart_ban: corrupt block",
                    });
                }
            }
        } else {
            let downloaders = self
                .picker
                .as_ref()
                .map(|p| p.get_downloaders(piece))
                .unwrap_or_default();
            if let BanDecision::Ban(peer) =
                self.smart_ban
                    .on_piece_failed(piece, piece_size, &downloaders, addr, read_digest)
            {
                self.remove_peer(peer, out);
                out.push(TorrentEffect::DisconnectPeer {
                    peer,
                    reason: "smart_ban: corrupt block",
                });
            }
        }
        self.piece_verified(piece, passed, out);
    }

    /// `prioritize_pieces` (upstream `torrent_handle::prioritize_pieces`
    /// → `torrent::set_piece_priority` per index) — direct piece
    /// overrides; they sit on top of the file-derived priorities.
    pub fn prioritize_pieces(&mut self, prios: &[bitdaemon_common::types::DownloadPriority]) {
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        for (i, prio) in prios.iter().enumerate().take(self.num_pieces as usize) {
            picker.set_piece_priority(PieceIndex(i as i32), *prio);
        }
    }

    /// `piece_priority(index)` — current picker priority.
    #[must_use]
    pub fn piece_priority(
        &self,
        index: PieceIndex,
    ) -> Option<bitdaemon_common::types::DownloadPriority> {
        self.picker.as_ref().map(|p| p.piece_priority(index))
    }

    /// `set_sequential_download` (upstream `torrent::set_sequential_download`).
    pub fn set_sequential_download(&mut self, yes: bool) {
        self.sequential = yes;
    }

    /// `start_full_recheck` prep — clear `have`/seed state and arm
    /// `checking_pieces`; the session then queues a `DiskHash`
    /// per piece (`torrent::force_recheck` → per-piece hash jobs).
    /// Returns how many pieces were queued (0 = nothing to check).
    pub fn arm_full_recheck(&mut self) -> i32 {
        if self.picker.is_none() || self.checking || self.num_pieces <= 0 {
            return 0;
        }
        self.have.clear_all();
        self.finished = false;
        self.seeding = false;
        self.checking = true;
        self.checking_pieces = self.num_pieces;
        self.num_pieces
    }

    /// one recheck hash came back — `checking` clears when the
    /// armed count drains (upstream `torrent::on_piece_hashed`
    /// ends the checking state at the last piece).
    pub fn recheck_piece_done(&mut self) {
        if self.checking_pieces > 0 {
            self.checking_pieces -= 1;
            if self.checking_pieces == 0 {
                self.checking = false;
            }
        }
    }

    /// `prioritize_files` (upstream `torrent::prioritize_files`) —
    /// merge `new` into `m_file_priority`, then
    /// `update_piece_priorities`: every piece's priority becomes
    /// the max of the files overlapping it.
    pub fn prioritize_files(&mut self, new: &[bitdaemon_common::types::DownloadPriority]) {
        let Some(fs) = self.files.clone() else {
            return;
        };
        use bitdaemon_common::types::DownloadPriority;
        let n = fs.num_files() as usize;
        self.file_priorities.resize(n, DownloadPriority::DEFAULT);
        for (i, prio) in new.iter().enumerate().take(n) {
            self.file_priorities[i] = *prio;
        }
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        for p in 0..self.num_pieces {
            let piece = PieceIndex(p);
            let prio = fs
                .map_block(piece, 0, i64::from(fs.piece_size(piece)))
                .iter()
                .map(|sl| {
                    self.file_priorities
                        .get(sl.file_index.0 as usize)
                        .copied()
                        .unwrap_or(DownloadPriority::DEFAULT)
                })
                .max()
                .unwrap_or(DownloadPriority::DEFAULT);
            picker.set_piece_priority(piece, prio);
        }
    }

    /// `file_progress` — per-file bytes done (upstream
    /// `torrent::file_progress`): whole pieces come from `have`,
    /// partial pieces from the picker's finished blocks.
    #[must_use]
    pub fn file_progress(&self) -> Vec<i64> {
        const BLOCK: i64 = 0x4000;
        let Some(fs) = &self.files else {
            return Vec::new();
        };
        let mut out = vec![0i64; fs.num_files() as usize];
        for p in 0..self.num_pieces {
            let piece = PieceIndex(p);
            let psize = i64::from(fs.piece_size(piece));
            if self.have.get_bit(p as usize) {
                for sl in fs.map_block(piece, 0, psize) {
                    if sl.file_index.0 >= 0 {
                        out[sl.file_index.0 as usize] += sl.size;
                    }
                }
            } else if let Some(picker) = &self.picker {
                let nb = picker.blocks_in_piece(piece) as i64;
                for b in 0..nb {
                    let blk = PieceBlock {
                        piece_index: piece,
                        block_index: b as i32,
                    };
                    if picker.is_finished_block(blk) {
                        let off = b * BLOCK;
                        for sl in fs.map_block(piece, off, (psize - off).min(BLOCK)) {
                            if sl.file_index.0 >= 0 {
                                out[sl.file_index.0 as usize] += sl.size;
                            }
                        }
                    }
                }
            }
        }
        out
    }

    /// `torrent_status()` — the snapshot upstream's `status()`
    /// builds: state enum, counters, scrape bests, rates.
    #[must_use]
    pub fn status(&self) -> crate::status::TorrentStatus {
        use crate::status::{TorrentState, TorrentStatus};
        let state = if self.checking {
            TorrentState::CheckingFiles
        } else if self.magnet && self.picker.is_none() {
            TorrentState::DownloadingMetadata
        } else if self.finished {
            if self.seeding {
                TorrentState::Seeding
            } else {
                TorrentState::Finished
            }
        } else {
            TorrentState::Downloading
        };
        let total_size = self.files.as_ref().map_or_else(
            || i64::from(self.num_pieces) * self.piece_length,
            |f| f.total_size(),
        );
        let wanted_pieces = self.picker.as_ref().map_or_else(
            || self.num_pieces,
            |p| {
                (0..self.num_pieces)
                    .filter(|&i| {
                        p.piece_priority(PieceIndex(i))
                            > bitdaemon_common::types::DownloadPriority::DONT_DOWNLOAD
                    })
                    .count() as i32
            },
        );
        let (num_complete, num_incomplete, downloaded, last_scrape) = self
            .trackers
            .trackers
            .iter()
            .filter(|e| e.scrape_complete >= 0)
            .max_by_key(|e| e.scrape_complete)
            .map_or((-1, -1, -1, 0), |e| {
                (
                    e.scrape_complete,
                    e.scrape_incomplete,
                    e.scrape_downloaded,
                    e.scrape_time.map_or(0, |t| t.elapsed().as_secs() as i64),
                )
            });
        let total_download: i64 = self.peers.values().map(|c| c.stat_down_total).sum();
        let total_upload: i64 = self.peers.values().map(|c| c.stat_up_total).sum();
        let progress = if total_size > 0 {
            (self.verified_bytes as f64 / total_size as f64).min(1.0)
        } else {
            0.0
        };
        TorrentStatus {
            info_hash: self.info_hash,
            state,
            paused: self.paused,
            auto_managed: self.auto_managed,
            is_seed: self.is_seed(),
            finished: self.finished,
            name: self.name.clone(),
            save_path: self.save_path.clone(),
            error: self.last_error.clone(),
            num_peers: self.peers.len(),
            num_seeds: self.num_seeds,
            num_candidates: self.peer_list.num_peers(),
            num_complete,
            num_incomplete,
            downloaded,
            total_done: self.verified_bytes,
            total_wanted: total_size.min(i64::from(wanted_pieces) * self.piece_length),
            total_upload,
            total_download,
            download_rate: self.down_rate,
            upload_rate: self.up_rate,
            progress,
            progress_ppm: (progress * 1e6) as i32,
            pieces: self.have.clone(),
            num_pieces: self.num_pieces,
            piece_length: self.piece_length,
            block_size: 0x4000,
            distributed_copies: self.picker.as_ref().map_or(0.0, |p| {
                let (min, frac) = p.distributed_copies();
                f64::from(min) + f64::from(frac) / 1000.0
            }),
            added_time: self
                .added_time
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64),
            last_scrape,
        }
    }

    /// `stat::second_tick` — refresh `down_rate`/`up_rate` from the
    /// per-peer payload counters (upstream uses `stat` channel
    /// sliding-window rates; a 1-tick delta is the same signal).
    pub fn update_rates(&mut self, now: std::time::Instant) {
        let down: i64 = self.peers.values().map(|c| c.stat_down_total).sum();
        let up: i64 = self.peers.values().map(|c| c.stat_up_total).sum();
        if let Some(prev_t) = self.stat_sample {
            let dt = (now - prev_t).as_secs_f64();
            if dt > 0.0 {
                self.down_rate = ((down - self.stat_prev_down) as f64 / dt) as i64;
                self.up_rate = ((up - self.stat_prev_up) as f64 / dt) as i64;
            }
        }
        self.stat_prev_down = down;
        self.stat_prev_up = up;
        self.stat_sample = Some(now);
    }

    /// `want_more_peers` — downstream connect decisions.
    #[must_use]
    pub fn want_peers(&self) -> bool {
        self.want_peers && !self.paused && !self.queued && !self.is_seed()
    }
}
