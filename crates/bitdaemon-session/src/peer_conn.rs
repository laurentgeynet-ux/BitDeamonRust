//! `peer_connection` assembled as a Rust state machine: the remote's
//! bitfield, choke/interest flags both directions, the request
//! pipeline (`PeerPipeline`), BEP-10 extension ids, suggest pieces,
//! and the score/statistics used by the choker.
//!
//! Sockets and the torrent object stay outside: methods take the
//! `PiecePicker` and produce `PeerMessage`s (from `peer_wire`) for the
//! caller to serialize onto a `PeerStream`. This keeps every
//! wire-visible decision identical to upstream while remaining
//! transport-agnostic — the anonymous OnionBit path plugs in at the
//! stream layer.

use bitdaemon_common::{Bitfield, PeerId, PeerRequest, PieceBlock, PieceIndex};
use bitdaemon_net::peer_wire::PeerMessage;

use crate::peer_pipeline::{PeerPipeline, PendingBlock};
use crate::piece_picker::{PickerStats, PiecePicker};
use crate::request_blocks::{PeerRequestCtx, TorrentRequestCtx};

/// The direction-independent flags of a peer connection.
#[derive(Debug)]
pub struct PeerConn {
    /// `m_peer_id` — remote's peer_id once the handshake completed.
    pub peer_id: Option<PeerId>,
    /// `m_have_piece` — pieces the remote has (have-bitfield).
    pub peer_pieces: Bitfield,
    /// `m_interesting` — they have something we want.
    pub interesting: bool,
    /// `m_choked` — we are choking their download.
    pub choked: bool,
    /// `m_peer_choked` — they are choking our download.
    pub peer_choked: bool,
    /// `m_peer_interested` — they want pieces from us.
    pub peer_interested: bool,
    /// `m_bitfield_received` — at least one bitfield message seen.
    pub bitfield_received: bool,
    /// `m_allowed_fast` — BEP-6 pieces we may fetch while choked.
    pub allowed_fast: Vec<PieceIndex>,
    /// `m_suggested_pieces` — BEP-6 suggested pieces.
    pub suggested_pieces: Vec<PieceIndex>,
    /// `m_request_queue`/`m_download_queue` + pipeline state.
    pub pipeline: PeerPipeline,
    /// `m_upload_only_id` etc. — BEP-10 remote extension ids.
    pub ext_upload_only: u8,
    /// `ut_holepunch` id.
    pub ext_holepunch: u8,
    /// `lt_donthave` id.
    pub ext_dont_have: u8,
    /// `ut_metadata` id (BEP-9) — the id the REMOTE expects when we
    /// send ut_metadata messages.
    pub ext_ut_metadata: u8,
    /// `ut_pex` id (BEP-11).
    pub ext_ut_pex: u8,
    /// `metadata_size` the remote advertised (its info dict size).
    pub metadata_size: Option<i64>,
    /// OUR local ext ids — the ids we advertise in our own BEP-10
    /// handshake; inbound messages carry these (the remote's ids
    /// above are for our sends). Session sets both to whatever it
    /// advertises.
    pub local_ut_metadata: u8,
    /// our local `ut_pex` id.
    pub local_ut_pex: u8,
    /// `m_superseed_piece` — the piece we're currently
    /// super-seeding to this peer (BEP initial-seed).
    pub superseed_piece: Option<PieceIndex>,
    /// `m_upload_only` — remote told us it's upload-only.
    pub remote_upload_only: bool,
    /// `m_client_version` — remote's `v` string.
    pub client_version: Vec<u8>,
    /// `m_upload_queue` — peer's requests to us.
    pub upload_queue: Vec<PeerRequest>,
    /// `m_num_invalid_requests`.
    pub num_invalid_requests: i32,
    /// `m_num_pieces` — total pieces the peer announced.
    pub num_pieces: i32,
    /// ms timestamp the connection was established (`m_connect`).
    pub connect_ms: i64,
    /// `m_last_seen` — last session_time a message arrived (peer
    /// timeout bookkeeping).
    pub last_active: i32,
    /// remote endpoint (`m_remote`) — needed for `ut_pex` diffs.
    pub remote: Option<std::net::SocketAddr>,
    /// pex update bookkeeping — the set we last sent this peer.
    pub pex_sent: std::collections::BTreeSet<std::net::SocketAddr>,
    /// `m_stat` payload accounting for the choker — lifetime bytes
    /// and bytes since the last unchoke round.
    pub stat_up_total: i64,
    /// download payload lifetime bytes.
    pub stat_down_total: i64,
    /// upload payload bytes since the last unchoke round.
    pub stat_up_round: i64,
    /// download payload bytes since the last unchoke round.
    pub stat_down_round: i64,
    /// `time_of_last_unchoke` (round-robin quota bookkeeping).
    pub last_unchoke: std::time::Instant,
}

impl PeerConn {
    /// New connection for a torrent of `num_pieces` pieces.
    #[must_use]
    pub fn new(num_pieces: usize, pipeline: PeerPipeline, now_ms: i64) -> Self {
        Self {
            peer_id: None,
            peer_pieces: Bitfield::with_size(num_pieces),
            interesting: false,
            choked: true,
            peer_choked: true,
            peer_interested: false,
            bitfield_received: false,
            allowed_fast: Vec::new(),
            suggested_pieces: Vec::new(),
            pipeline,
            ext_upload_only: 0,
            ext_holepunch: 0,
            ext_dont_have: 0,
            ext_ut_metadata: 0,
            ext_ut_pex: 0,
            metadata_size: None,
            local_ut_metadata: 1,
            local_ut_pex: 2,
            superseed_piece: None,
            remote_upload_only: false,
            client_version: Vec::new(),
            upload_queue: Vec::new(),
            num_invalid_requests: 0,
            num_pieces: 0,
            connect_ms: now_ms,
            last_active: 0,
            remote: None,
            pex_sent: std::collections::BTreeSet::new(),
            stat_up_total: 0,
            stat_down_total: 0,
            stat_up_round: 0,
            stat_down_round: 0,
            last_unchoke: std::time::Instant::now(),
        }
    }

    /// `peer_connection::has_piece(index)`.
    #[must_use]
    pub fn has_piece(&self, index: PieceIndex) -> bool {
        (index.0 as usize) < self.peer_pieces.size() && self.peer_pieces.get_bit(index.0 as usize)
    }

    /// `incoming_bitfield` — store the peer's have-set.
    pub fn on_bitfield(&mut self, bits: &Bitfield) {
        self.peer_pieces.clone_from(bits);
        self.bitfield_received = true;
        self.num_pieces = bits.iter().filter(|&b| b).count() as i32;
    }

    /// `incoming_have(index)` — update the bitfield; returns whether
    /// the peer became interesting (we lack that piece).
    pub fn on_have(&mut self, index: PieceIndex, have: impl Fn(PieceIndex) -> bool) -> bool {
        if (index.0 as usize) >= self.peer_pieces.size() {
            return false;
        }
        if self.peer_pieces.get_bit(index.0 as usize) {
            return false;
        }
        self.peer_pieces.set_bit(index.0 as usize);
        self.num_pieces += 1;
        if !have(index) {
            self.interesting = true;
            return true;
        }
        false
    }

    /// `incoming_have_none` — the peer sent no bitfield (means empty).
    pub fn on_have_none(&mut self) {
        self.bitfield_received = true;
    }

    /// `incoming_have_all` — a seed.
    pub fn on_have_all(&mut self) {
        self.peer_pieces.set_all();
        self.num_pieces = self.peer_pieces.size() as i32;
        self.bitfield_received = true;
    }

    /// `incoming_choke` / `incoming_unchoke`.
    pub fn on_choke(&mut self) {
        self.peer_choked = true;
        // upstream abandons unsent requests on choke
    }
    /// unchoke.
    pub fn on_unchoke(&mut self) {
        self.peer_choked = false;
    }
    /// `incoming_interested` / `incoming_not_interested`.
    pub fn on_interested(&mut self) {
        self.peer_interested = true;
    }
    /// not interested.
    pub fn on_not_interested(&mut self) {
        self.peer_interested = false;
    }

    /// `incoming_suggest` — BEP-6 suggest.
    pub fn on_suggest(&mut self, index: PieceIndex) {
        if self.has_piece(index) || self.suggested_pieces.contains(&index) {
            return;
        }
        self.suggested_pieces.push(index);
    }

    /// `incoming_allowed_fast` — BEP-6 allowed-fast.
    pub fn on_allowed_fast(&mut self, index: PieceIndex) {
        self.allowed_fast.push(index);
    }

    /// `incoming_dont_have`.
    pub fn on_dont_have(&mut self, index: PieceIndex) -> bool {
        if (index.0 as usize) >= self.peer_pieces.size()
            || !self.peer_pieces.get_bit(index.0 as usize)
        {
            return false;
        }
        self.peer_pieces.clear_bit(index.0 as usize);
        self.num_pieces -= 1;
        true
    }

    /// `incoming_choke` — also abandons unsent requests
    /// (`clear_request_queue`); the blocks are returned so the
    /// picker can `abort_download` them.
    pub fn on_choke_with_aborts(&mut self) -> Vec<PieceBlock> {
        self.peer_choked = true;
        self.pipeline.endgame = false;
        self.pipeline
            .request_queue
            .drain(..)
            .map(|b| b.block)
            .collect()
    }

    /// `incoming_request` — `validate_piece_request` then enqueue
    /// for the upload path. `num_pieces` is the torrent's piece
    /// count (0 when metadata unknown → reject).
    pub fn validate_request(&mut self, r: &PeerRequest, num_pieces: i32) -> bool {
        let ok = r.length > 0
            && r.length <= 0x20000
            && r.start >= 0
            && r.piece.0 >= 0
            && r.piece.0 < num_pieces;
        if !ok {
            self.num_invalid_requests += 1;
        }
        ok
    }

    /// `incoming_reject_request` — the block is freed; the caller
    /// `abort_download`s it in the picker and re-picks.
    pub fn on_reject(&mut self, r: &PeerRequest) -> PieceBlock {
        let block = PieceBlock {
            piece_index: r.piece,
            block_index: r.start / 0x4000,
        };
        let _ = self.cancel_request(block);
        block
    }

    /// `incoming_piece` bookkeeping — slow-start growth, pipeline
    /// counters, queue removal. Returns the block.
    pub fn on_piece(&mut self, r: &PeerRequest) -> PieceBlock {
        let block = PieceBlock {
            piece_index: r.piece,
            block_index: r.start / 0x4000,
        };
        self.pipeline.on_piece_received(0);
        let _ = self.cancel_request(block);
        block
    }

    /// `dispatch` — route a decoded wire message. Returns the
    /// side-effects the torrent layer must run (picker updates,
    /// disk writes, uploads).
    pub fn dispatch(
        &mut self,
        msg: &bitdaemon_net::peer_wire::PeerMessage,
        num_pieces: i32,
        eff: &mut Vec<PeerEffect>,
    ) {
        use bitdaemon_net::peer_wire::PeerMessage as M;
        match *msg {
            M::Keepalive => {}
            M::Choke => {
                for b in self.on_choke_with_aborts() {
                    eff.push(PeerEffect::AbortDownload(b));
                }
            }
            M::Unchoke => {
                self.on_unchoke();
                eff.push(PeerEffect::MaybePick);
            }
            M::Interested => self.on_interested(),
            M::NotInterested => self.on_not_interested(),
            M::Have(i) => {
                if self.on_have(i, |_| false) {
                    eff.push(PeerEffect::MaybePick);
                }
                eff.push(PeerEffect::RefcountInc(i));
            }
            M::HaveAll => self.on_have_all(),
            M::HaveNone => self.on_have_none(),
            M::Bitfield(ref bits) => {
                match bitdaemon_common::Bitfield::from_bytes(bits, num_pieces.max(0) as usize) {
                    Some(bf) => {
                        self.on_bitfield(&bf);
                        eff.push(PeerEffect::RefcountBulk(bf, true));
                    }
                    None => eff.push(PeerEffect::Disconnect("invalid bitfield size")),
                }
            }
            M::Request(r) => {
                if self.validate_request(&r, num_pieces) {
                    self.upload_queue.push(r);
                    eff.push(PeerEffect::ReadRequest(r));
                } else {
                    eff.push(PeerEffect::Disconnect("invalid request"));
                }
            }
            M::Cancel(r) => {
                if let Some(pos) = self
                    .upload_queue
                    .iter()
                    .position(|u| u.piece == r.piece && u.start == r.start && u.length == r.length)
                {
                    self.upload_queue.remove(pos);
                    eff.push(PeerEffect::CancelRequest(r));
                }
            }
            M::Piece {
                piece,
                offset,
                ref data,
            } => {
                let r = bitdaemon_common::PeerRequest {
                    piece,
                    start: offset,
                    length: data.len() as i32,
                };
                let block = self.on_piece(&r);
                eff.push(PeerEffect::MarkDownloaded(block));
                eff.push(PeerEffect::DiskWrite(r, data.to_vec()));
                eff.push(PeerEffect::MaybePick);
            }
            M::RejectRequest(r) => {
                let b = self.on_reject(&r);
                eff.push(PeerEffect::AbortDownload(b));
                eff.push(PeerEffect::MaybePick);
            }
            M::AllowedFast(i) => self.on_allowed_fast(i),
            M::SuggestPiece(i) => self.on_suggest(i),
            M::DhtPort(p) => eff.push(PeerEffect::DhtPort(p)),
            M::Extended { id, ref data } => {
                if id == 0 {
                    // BEP-10 handshake — learn the remote's ext ids
                    if let Some(hs) = bitdaemon_net::bep10::ExtendedHandshake::parse(data) {
                        let ids = hs.remote_ids();
                        self.ext_upload_only = ids.upload_only;
                        self.ext_holepunch = ids.holepunch;
                        self.ext_dont_have = ids.dont_have;
                        self.ext_ut_metadata =
                            hs.messages.get("ut_metadata").copied().unwrap_or(0) as u8;
                        self.ext_ut_pex = hs.messages.get("ut_pex").copied().unwrap_or(0) as u8;
                        self.metadata_size = hs.metadata_size;
                        self.remote_upload_only = hs.upload_only;
                        if let Some(v) = hs.client_version {
                            self.client_version = v;
                        }
                        // `yourip` — the peer's view of our address
                        // (upstream feeds `ip_voter::cast_vote` with
                        // `source_peer`).
                        match hs.your_ip.as_deref() {
                            Some(ip) if ip.len() == 4 => {
                                eff.push(PeerEffect::YourIp(std::net::IpAddr::from(
                                    <[u8; 4]>::try_from(ip).expect("len 4"),
                                )));
                            }
                            Some(ip) if ip.len() == 16 => {
                                eff.push(PeerEffect::YourIp(std::net::IpAddr::from(
                                    <[u8; 16]>::try_from(ip).expect("len 16"),
                                )));
                            }
                            _ => {}
                        }
                        eff.push(PeerEffect::ExtHandshake);
                    }
                } else if id == self.local_ut_metadata {
                    eff.push(PeerEffect::MetadataPacket(data.to_vec()));
                } else if id == self.local_ut_pex {
                    eff.push(PeerEffect::PexPacket(data.to_vec()));
                }
            }
            _ => {}
        }
    }

    /// `add_request(pb, flags)` — queue a picked block.
    pub fn add_request(&mut self, block: PieceBlock, busy: bool, time_critical: bool) {
        let mut pb = PendingBlock::new(block);
        pb.busy = busy;
        pb.time_critical = time_critical;
        if time_critical {
            self.pipeline.queued_time_critical += 1;
        }
        self.pipeline.request_queue.push(pb);
    }

    /// Cancel an outstanding request (`peer_connection::cancel_block`
    /// shape — timed out or rejected).
    pub fn cancel_request(&mut self, block: PieceBlock) -> bool {
        if let Some(pos) = self
            .pipeline
            .download_queue
            .iter()
            .position(|b| b.block == block)
        {
            self.pipeline.download_queue.remove(pos);
            return true;
        }
        if let Some(pos) = self
            .pipeline
            .request_queue
            .iter()
            .position(|b| b.block == block)
        {
            self.pipeline.request_queue.remove(pos);
            return true;
        }
        false
    }

    /// Build the `PeerRequestCtx` the picker glue needs from this
    /// connection's state.
    pub fn request_ctx(
        &self,
        no_download: bool,
        disconnecting: bool,
        rate: i64,
    ) -> PeerRequestCtx<'_> {
        // outstanding = download_queue ∪ request_queue blocks
        PeerRequestCtx {
            no_download,
            disconnecting,
            desired_queue_size: self.pipeline.desired_queue_size(),
            outstanding: &[], // filled by caller — see `outstanding()`
            prefer_contiguous_blocks: 0,
            download_payload_rate: rate,
            peer_choked: self.peer_choked,
            allowed_fast: &self.allowed_fast,
            peer_pieces: &self.peer_pieces,
            peer_key: self.peer_key(),
            picker_options: crate::piece_picker::PickerOptions::RAREST_FIRST,
            suggested_pieces: &self.suggested_pieces,
        }
    }

    /// All blocks outstanding (download + request queues).
    #[must_use]
    pub fn outstanding(&self) -> Vec<PieceBlock> {
        self.pipeline
            .download_queue
            .iter()
            .chain(self.pipeline.request_queue.iter())
            .map(|b| b.block)
            .collect()
    }

    /// `peer_info_struct()` — picker's stable peer key (use the peer
    /// id's first 8 bytes when known, else the connect slot).
    #[must_use]
    pub fn peer_key(&self) -> Option<u64> {
        self.peer_id.map(|id| {
            let b = id.as_bytes();
            u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
        })
    }

    /// Drive the picker → request queue → wire for this peer.
    /// Returns `(invoked, endgame, wire_requests)`.
    #[allow(clippy::too_many_arguments)]
    pub fn drive_requests<T: crate::peer_pipeline::PipelineTorrent>(
        &mut self,
        picker: &mut PiecePicker,
        t: &TorrentRequestCtx,
        torrent: &T,
        rate: i64,
        send_buffer_size: usize,
        now_ms: i64,
        pc: &mut dyn PickerStats,
    ) -> (bool, Option<bool>, Vec<bitdaemon_common::PeerRequest>) {
        let peer_key = self.peer_key();
        let opts = crate::piece_picker::PickerOptions::RAREST_FIRST;
        let outstanding = self.outstanding();
        let mut ctx = self.request_ctx(false, false, rate);
        ctx.outstanding = &outstanding;
        // Rust: the picker is already borrowed by request_a_block —
        // collect picks and mark them right after the call (upstream's
        // add_request does it inline; ordering is equivalent because
        // the marks only matter across pick_pieces invocations)
        let mut picked: Vec<(PieceBlock, bool)> = Vec::new();
        let (invoked, endgame) =
            crate::request_blocks::request_a_block_out(picker, t, &ctx, pc, |b, busy| {
                picked.push((b, busy));
                true
            });
        for (b, busy) in picked {
            if picker.mark_as_downloading(b, peer_key, opts) {
                self.add_request(b, busy, false);
            }
        }
        if let Some(e) = endgame {
            self.pipeline.endgame = e;
        }
        let reqs = self
            .pipeline
            .send_block_requests(torrent, send_buffer_size, now_ms, |b| {
                if picker.is_downloaded(b) {
                    picker.abort_download(b, peer_key);
                    true
                } else {
                    false
                }
            });
        (invoked, endgame, reqs)
    }

    /// `write_handshake` — the 68-byte BitTorrent handshake with the
    /// BEP-10 extension bit set (`write_handshake` upstream sets it
    /// when `m_supports_extensions`).
    #[must_use]
    pub fn handshake(info_hash: &[u8; 20], our_peer_id: &PeerId) -> Vec<u8> {
        let mut out = Vec::with_capacity(68);
        out.push(19u8);
        out.extend(b"BitTorrent protocol");
        let mut reserved = [0u8; 8];
        // extension protocol (BEP-10) — byte 5, bit 0x10
        reserved[5] |= 0x10;
        out.extend(reserved);
        out.extend(info_hash);
        out.extend(our_peer_id.as_bytes());
        out
    }

    /// Message to send when we decide the peer is interesting.
    #[must_use]
    pub fn interested_msg() -> PeerMessage {
        PeerMessage::Interested
    }
}

/// `peer_connection` incoming-side effects — the session runs
/// these (picker/disk/wire/alerts).
#[derive(Debug)]
pub enum PeerEffect {
    /// `picker.inc_refcount(index)`.
    RefcountInc(PieceIndex),
    /// `picker.dec_refcount(index)`.
    RefcountDec(PieceIndex),
    /// bulk refcount from a bitfield (`true` = inc).
    RefcountBulk(bitdaemon_common::Bitfield, bool),
    /// write a received block to disk.
    DiskWrite(bitdaemon_common::PeerRequest, Vec<u8>),
    /// serve a peer's request (upload path).
    ReadRequest(bitdaemon_common::PeerRequest),
    /// peer cancelled a request.
    CancelRequest(bitdaemon_common::PeerRequest),
    /// `picker.mark_as_downloaded`.
    MarkDownloaded(PieceBlock),
    /// `picker.abort_download` (reject/choke clearing).
    AbortDownload(PieceBlock),
    /// drive `request_a_block` again.
    MaybePick,
    /// BEP-5 port announcement.
    DhtPort(u16),
    /// BEP-10 extended handshake received.
    ExtHandshake,
    /// a `ut_metadata` message body (dict + optional payload).
    MetadataPacket(Vec<u8>),
    /// a `ut_pex` message body.
    PexPacket(Vec<u8>),
    /// `yourip` from the BEP-10 handshake — the peer's view of our
    /// external address (`ip_voter` `source_peer`).
    YourIp(std::net::IpAddr),
    /// protocol violation — drop the connection.
    Disconnect(&'static str),
}
