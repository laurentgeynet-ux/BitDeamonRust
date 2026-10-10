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
    /// `m_magnet_link` — added without metadata.
    pub magnet: bool,
    /// `smart_ban` plugin state (`extensions/smart_ban.cpp`).
    pub smart_ban: SmartBan,
    /// `m_auto_managed` — queue promotion/demotion is the
    /// session's job (`active_downloads`/`active_seeds`).
    pub auto_managed: bool,
    /// `is_torrent_queued` — held back by the auto-managed queue
    /// (distinct from user `paused`, like upstream
    /// `graceful_pause` vs the queue state).
    pub queued: bool,
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
            storage,
            num_seeds: 0,
            paused: false,
            finished: false,
            seeding: false,
            verified_bytes: 0,
            want_peers: true,
            piece_priority_dirty: false,
            metadata_tracker: UtMetadataTracker::new(),
            magnet: false,
            smart_ban: SmartBan::new(),
            auto_managed: true,
            queued: false,
        }
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
        self.have = Bitfield::with_size(info.num_pieces() as usize);
        self.piece_hashes = Some(
            (0..info.num_pieces())
                .map(|i| info.hash_for_piece(PieceIndex(i)))
                .collect(),
        );
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
        atp.have_pieces = self.have.clone();
        atp.verified_pieces = self.have.clone();
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
            client: c.client_version.clone(),
        })
    }

    /// `is_seed`.
    #[must_use]
    pub fn is_seed(&self) -> bool {
        self.seeding || self.finished
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
                PeerEffect::DhtPort(_) => {}
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
    pub fn on_peer_message(
        &mut self,
        peer_id: u64,
        msg: &PeerMessage,
        out: &mut Vec<TorrentEffect>,
    ) {
        let mut effects = Vec::new();
        if let Some(c) = self.peers.get_mut(&peer_id) {
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
        let opts = PickerOptions::RAREST_FIRST;
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

    /// `want_more_peers` — downstream connect decisions.
    #[must_use]
    pub fn want_peers(&self) -> bool {
        self.want_peers && !self.paused && !self.queued && !self.is_seed()
    }
}
