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

use bitdaemon_common::types::{FileIndex, PieceIndex, StorageIndex};
use bitdaemon_common::{Bitfield, PeerRequest, Sha1Hash};
use bitdaemon_net::peer_wire::PeerMessage;

use crate::peer_conn::{PeerConn, PeerEffect};
use crate::piece_picker::{PickerOptions, PiecePicker};

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
        }
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

    /// `want_more_peers` — downstream connect decisions.
    #[must_use]
    pub fn want_peers(&self) -> bool {
        self.want_peers && !self.paused && !self.is_seed()
    }
}
