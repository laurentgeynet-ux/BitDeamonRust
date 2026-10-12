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
use sha1::Digest;

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
    /// `m_accept_fast` — pieces WE offered this peer while it was
    /// choked (the `send_allowed_set` set).
    pub accept_fast: Vec<PieceIndex>,
    /// `m_accept_fast_piece_cnt` — per-entry completion counters
    /// upstream keeps for accept-fast accounting.
    pub accept_fast_piece_cnt: Vec<i32>,
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
    /// our local `ut_holepunch` id (BEP-55).
    pub local_ut_holepunch: u8,
    /// our local `lt_donthave` id — remote sends extended messages
    /// on this id to retract a piece.
    pub local_dont_have: u8,
    /// `i2p_pex` — remote's message id from its handshake (`m`).
    pub ext_i2p_pex: u8,
    /// `local_i2p_pex` — our advertised id (upstream fixes it at 9).
    pub local_i2p_pex: u8,
    /// I2P destination (base64 string) when this conn rides an I2P
    /// socket — `is_i2p(pc.get_socket())` upstream.
    pub i2p: Option<Vec<u8>>,
    /// `is_outgoing()` — we dialed this peer.
    pub outgoing: bool,
    /// `is_connecting()` — handshake not yet complete.
    pub connecting: bool,
    /// `!in_handshake()` — the peer's BitTorrent handshake landed.
    pub handshake_done: bool,
    /// `received_listen_port` — peer shared its listen port.
    pub received_listen_port: bool,
    /// the listen port the peer advertised via ext-handshake `p`
    /// (upstream `peer_info::port`) — ut_pex rewrites inbound
    /// endpoints to this port, the source port being ephemeral.
    pub peer_listen_port: Option<u16>,
    /// `i2p_pex_peer_plugin` per-connection state.
    pub i2p_pex: crate::i2p_pex::I2pPexPeerState,
    /// `m_superseed_piece[2]` — the piece(s) we're currently
    /// super-seeding to this peer (BEP initial-seed); requests for
    /// either slot are honored.
    pub superseed_pieces: [Option<PieceIndex>; 2],
    /// `m_became_uninterested` — conn-clock ms of the last
    /// interested → not-interested transition; upstream's
    /// `relative_time` default is the connect instant, so a
    /// never-interested peer still ages into the super-seed
    /// recovery window.
    pub became_uninterested_ms: i64,
    /// `peer_info::protocol_v2` — the connection was established
    /// with the v2 info-hash. Hash messages (`hash_request`/
    /// `hashes`/`hash_reject`) are a protocol violation without it.
    pub protocol_v2: bool,
    /// `m_upload_only` — remote told us it's upload-only.
    pub remote_upload_only: bool,
    /// `m_client_version` — remote's `v` string.
    pub client_version: Vec<u8>,
    /// `m_upload_queue` — peer's requests to us.
    pub upload_queue: Vec<PeerRequest>,
    /// `m_send_buffer.size()` — bytes queued for the socket that the
    /// writer pump hasn't sent yet. Drives `fill_send_buffer`'s
    /// watermark gate (`send_buffer + reading_bytes < watermark`).
    pub send_buffer: usize,
    /// `m_reading_bytes` — payload bytes of disk reads in flight for
    /// this peer (issued, completion not yet drained).
    pub reading_bytes: usize,
    /// `m_num_invalid_requests`.
    pub num_invalid_requests: i32,
    /// `m_num_pieces` — total pieces the peer announced.
    pub num_pieces: i32,
    /// `stat` rate sampling — last totals at the previous tick and
    /// the resulting bytes/sec the `peer_info` snapshot reports.
    pub rate_up_speed: i32,
    /// `stat` rate sampling — payload download bytes/sec.
    pub rate_down_speed: i32,
    /// last sampled `stat_up_total` for rate computation.
    pub last_up_total: i64,
    /// last sampled `stat_down_total` for rate computation.
    pub last_down_total: i64,
    /// ms timestamp the connection was established (`m_connect`).
    pub connect_ms: i64,
    /// `m_supports_fast` — remote negotiated the FAST extension
    /// (reserved byte 7 bit 0x04). Rejects/allowed-fast only exist
    /// when set.
    pub supports_fast: bool,
    /// `m_supports_dht_port` — remote advertised a DHT port bit.
    pub supports_dht: bool,
    /// `supports_encryption()` — the connection went through the
    /// MSE/PE handshake (`m_encrypted`), regardless of the negotiated
    /// `crypto_select` (plaintext-select still counts). Feeds the
    /// `pex_encryption` flag in ut_pex.
    pub supports_encryption: bool,
    /// `HaveAll`/`Have` semantics arrived while the torrent had no
    /// metadata (magnet): the peer claims every piece but
    /// `peer_pieces` could not represent it yet — replayed into a
    /// full bitfield when `num_pieces` becomes known.
    pub pre_metadata_all: bool,
    /// `m_interested` — WE sent `interested` (upstream tracks the
    /// wire state to avoid duplicate sends).
    pub we_sent_interested: bool,
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
    /// download bytes at the previous tick (drives the upstream
    /// slow-start exit: stalled growth ⇒ AIMD takes over).
    pub down_delta_prev: i64,
    /// `time_of_last_unchoke` (round-robin quota bookkeeping).
    pub last_unchoke: std::time::Instant,
    /// monotonic ms clock refreshed each `second_tick` — drives the
    /// pipeline's RTT samples and request-timeout bookkeeping.
    pub now_ms: i64,
    /// `peer_connection::m_extensions` — per-connection plugins
    /// produced by `SessionPlugin::new_peer_connection`.
    pub plugins: Vec<std::sync::Arc<dyn crate::alerts::PeerPlugin>>,
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
            accept_fast: Vec::new(),
            accept_fast_piece_cnt: Vec::new(),
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
            local_ut_holepunch: 3,
            superseed_pieces: [None, None],
            became_uninterested_ms: now_ms,
            remote_upload_only: false,
            client_version: Vec::new(),
            upload_queue: Vec::new(),
            send_buffer: 0,
            reading_bytes: 0,
            num_invalid_requests: 0,
            num_pieces: 0,
            connect_ms: now_ms,
            last_active: 0,
            remote: None,
            pex_sent: std::collections::BTreeSet::new(),
            stat_up_total: 0,
            stat_down_total: 0,
            rate_up_speed: 0,
            rate_down_speed: 0,
            last_up_total: 0,
            last_down_total: 0,
            stat_up_round: 0,
            stat_down_round: 0,
            down_delta_prev: -1,
            supports_fast: false,
            pre_metadata_all: false,
            supports_dht: false,
            supports_encryption: false,
            local_dont_have: 3,
            ext_i2p_pex: 0,
            local_i2p_pex: crate::i2p_pex::I2P_PEX_ID,
            i2p: None,
            outgoing: false,
            connecting: true,
            handshake_done: false,
            received_listen_port: false,
            peer_listen_port: None,
            i2p_pex: crate::i2p_pex::I2pPexPeerState::new(),
            protocol_v2: false,
            we_sent_interested: false,
            last_unchoke: std::time::Instant::now(),
            now_ms,
            plugins: Vec::new(),
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
    /// `incoming_not_interested` — `m_became_uninterested` stamps
    /// only the interested → not-interested edge.
    pub fn on_not_interested(&mut self) {
        if self.peer_interested {
            self.became_uninterested_ms = self.now_ms;
        }
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

    /// `send_allowed_set` — deterministic BEP-6 allowed-fast set.
    /// Upstream hashes `remote-addr-bytes ‖ info-hash` with SHA-1 and
    /// consumes the digest 32 bits at a time modulo `num_pieces`,
    /// rehashing on exhaustion; pieces the peer already has are
    /// skipped (no point offering them). Returns the `AllowedFast`
    /// messages to write. Callers pass the same gating upstream does:
    /// `valid_metadata`, `!super_seeding`, `!upload_only`.
    pub fn send_allowed_set(
        &mut self,
        info_hash: &bitdaemon_common::Sha1Hash,
        num_pieces: i32,
        set_size: i32,
        valid_metadata: bool,
        super_seeding: bool,
        upload_only: bool,
    ) -> Vec<PeerMessage> {
        let mut out = Vec::new();
        if !valid_metadata || super_seeding || upload_only || set_size <= 0 || num_pieces <= 0 {
            return out;
        }
        if i64::from(set_size) >= i64::from(num_pieces) {
            // more fast slots than pieces — offer everything the
            // peer lacks.
            for i in 0..num_pieces {
                let idx = PieceIndex(i);
                if self.has_piece(idx) || self.accept_fast.contains(&idx) {
                    continue;
                }
                self.accept_fast.push(idx);
                self.accept_fast_piece_cnt.push(0);
                out.push(PeerMessage::AllowedFast(idx));
            }
            return out;
        }

        let mut x: Vec<u8> = match self.remote {
            Some(std::net::SocketAddr::V4(a)) => a.ip().octets().to_vec(),
            Some(std::net::SocketAddr::V6(a)) => a.ip().octets().to_vec(),
            None => Vec::new(),
        };
        x.extend_from_slice(info_hash.as_bytes());

        let mut hash = sha1::Sha1::new();
        hash.update(&x);
        let mut digest: [u8; 20] = hash.finalize().into();

        let mut attempts = 0i32;
        let mut loops = 0i32;
        loop {
            let mut off = 0usize;
            while off + 4 <= digest.len() {
                loops += 1;
                let w = u32::from_be_bytes([
                    digest[off],
                    digest[off + 1],
                    digest[off + 2],
                    digest[off + 3],
                ]);
                off += 4;
                let piece = PieceIndex((w % num_pieces as u32) as i32);
                if self.accept_fast.contains(&piece) {
                    // safety-net to terminate under the worst hash
                    // collisions (upstream `++loops > 500`).
                    if loops > 500 {
                        return out;
                    }
                    continue;
                }
                if !self.has_piece(piece) {
                    self.accept_fast.push(piece);
                    self.accept_fast_piece_cnt.push(0);
                    out.push(PeerMessage::AllowedFast(piece));
                }
                attempts += 1;
                if attempts >= set_size {
                    return out;
                }
            }
            let mut h = sha1::Sha1::new();
            h.update(digest);
            digest = h.finalize().into();
        }
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
        self.pipeline.on_piece_received(self.now_ms);
        let _ = self.cancel_request(block);
        // upstream `incoming_piece`: while requests remain, the next
        // expected block gets a fresh timeout window — `m_requested`
        // is the last-receive watermark, not the queue's age.
        if !self.pipeline.request_queue.is_empty() || !self.pipeline.download_queue.is_empty() {
            self.pipeline.requested_ms = self.now_ms;
        }
        block
    }

    /// `peer_connection::incoming_piece` — was this block actually
    /// requested (present in request or download queue)?
    #[must_use]
    pub fn was_requested(&self, r: &PeerRequest) -> bool {
        let block = PieceBlock {
            piece_index: r.piece,
            block_index: r.start / 0x4000,
        };
        self.pipeline
            .download_queue
            .iter()
            .any(|b| b.block == block)
            || self.pipeline.request_queue.iter().any(|b| b.block == block)
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
            M::HaveAll => {
                self.on_have_all();
                if num_pieces <= 0 {
                    // magnet with no metadata yet: a full bitfield
                    // can't be represented — flag it for the replay
                    // in `Torrent::set_metadata`
                    self.pre_metadata_all = true;
                }
                // `peer_has_all` → `inc_refcount_all`: the peer is a
                // seed; skip it in the refcount or `dec_refcount`
                // underflows `m_seeds` on disconnect.
                let mut bf = bitdaemon_common::Bitfield::with_size(num_pieces.max(0) as usize);
                bf.set_all();
                eff.push(PeerEffect::RefcountBulk(bf, true));
                eff.push(PeerEffect::MaybePick);
            }
            M::HaveNone => self.on_have_none(),
            M::Bitfield(ref bits) => {
                // metadata still missing (magnet): `num_pieces` is 0
                // and upstream has nowhere to put the bits — we store
                // them raw and reconcile sizes in `set_metadata`,
                // which is strictly better than disconnecting a
                // well-formed peer.
                let expected = if num_pieces > 0 {
                    num_pieces as usize
                } else {
                    bits.len() * 8
                };
                match bitdaemon_common::Bitfield::from_bytes(bits, expected) {
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
                    // upstream `incoming_request` rejects + posts
                    // `invalid_request_alert` for out-of-range
                    // piece/start/length — it does not disconnect
                    // (only >300 invalid requests while choked do).
                    eff.push(PeerEffect::RejectRequest(r));
                    eff.push(PeerEffect::InvalidRequest(format!(
                        "out of range: piece {} start {} length {}",
                        r.piece.0, r.start, r.length
                    )));
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
                let block = PieceBlock {
                    piece_index: r.piece,
                    block_index: r.start / 0x4000,
                };
                if !self.was_requested(&r) {
                    // upstream counts unsolicited blocks as invalid
                    // and drops the payload — never touches disk
                    // (`unwanted_block_alert` in `incoming_piece`).
                    self.num_invalid_requests += 1;
                    eff.push(PeerEffect::UnwantedBlock(block));
                    return;
                }
                // `incoming_piece` clears snub on data arrival.
                if self.pipeline.snubbed {
                    self.pipeline.snubbed = false;
                    eff.push(PeerEffect::Unsnub);
                }
                let block = self.on_piece(&r);
                eff.push(PeerEffect::MarkDownloaded(block));
                eff.push(PeerEffect::BlockFinished(block));
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
                        self.ext_i2p_pex = hs.messages.get("i2p_pex").copied().unwrap_or(0) as u8;
                        self.metadata_size = hs.metadata_size;
                        self.remote_upload_only = hs.upload_only;
                        // ext-handshake `p` — the peer's listen port
                        // (upstream `update_peer_port` +
                        // `received_listen_port`); ut_pex uses it for
                        // inbound conns whose source port is ephemeral.
                        if let Some(p) = hs.listen_port
                            && let Ok(p) = u16::try_from(p)
                        {
                            self.received_listen_port = true;
                            self.peer_listen_port = Some(p);
                        }
                        // `reqq` — the peer's max tolerated request
                        // queue (upstream `max_out_request_queue(reqq)`);
                        // exceeding it floods the peer → disconnect.
                        if hs.reqq > 0 {
                            self.pipeline.max_out_request_queue =
                                hs.reqq.min(i64::from(u16::MAX)) as u16;
                        }
                        if let Some(v) = hs.client_version {
                            self.client_version = v;
                        }
                        // `yourip` — the peer's view of our address
                        // (upstream feeds `ip_voter::cast_vote` with
                        // `source_peer`).
                        match hs.your_ip.as_deref() {
                            Some(ip) if ip.len() == 4 => {
                                if let Ok(a) = <[u8; 4]>::try_from(ip) {
                                    eff.push(PeerEffect::YourIp(std::net::IpAddr::from(a)));
                                }
                            }
                            Some(ip) if ip.len() == 16 => {
                                if let Ok(a) = <[u8; 16]>::try_from(ip) {
                                    eff.push(PeerEffect::YourIp(std::net::IpAddr::from(a)));
                                }
                            }
                            _ => {}
                        }
                        eff.push(PeerEffect::ExtHandshake);
                    }
                } else if id == self.local_ut_metadata {
                    eff.push(PeerEffect::MetadataPacket(data.to_vec()));
                } else if id == self.local_ut_pex {
                    eff.push(PeerEffect::PexPacket(data.to_vec()));
                } else if id == self.local_i2p_pex {
                    eff.push(PeerEffect::I2pPexPacket(data.to_vec()));
                } else if id == self.local_ut_holepunch {
                    // `on_holepunch` — upstream drops the message when
                    // the remote never advertised the extension.
                    if self.ext_holepunch != 0
                        && let Some(hp) = crate::ut_holepunch::decode_hp(data)
                    {
                        eff.push(PeerEffect::Holepunch(hp));
                    }
                } else if id == self.local_dont_have && data.len() == 4 {
                    // `incoming_dont_have` — peer retracted a piece.
                    let i = PieceIndex(i32::from_be_bytes([data[0], data[1], data[2], data[3]]));
                    if self.on_dont_have(i) {
                        eff.push(PeerEffect::RefcountDec(i));
                        eff.push(PeerEffect::MaybePick);
                    }
                }
            }
            _ => {}
        }
    }

    /// `add_request(pb, flags)` — queue a picked block. `now_ms`
    /// stamps `m_requested` when the wire queue goes empty→non-empty
    /// (request timeout) and `m_last_request` (RTT sampling).
    pub fn add_request(&mut self, block: PieceBlock, busy: bool, time_critical: bool, now_ms: i64) {
        if self.pipeline.request_queue.is_empty() && self.pipeline.download_queue.is_empty() {
            self.pipeline.requested_ms = now_ms;
        }
        self.pipeline.last_request_ms = now_ms;
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
        } else if let Some(pos) = self
            .pipeline
            .request_queue
            .iter()
            .position(|b| b.block == block)
        {
            self.pipeline.request_queue.remove(pos);
        } else {
            return false;
        }
        // `m_requested` — re-arm the timeout on the remaining head.
        if self.pipeline.download_queue.is_empty() && self.pipeline.request_queue.is_empty() {
            self.pipeline.requested_ms = 0;
        }
        true
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
                self.add_request(b, busy, false, now_ms);
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
    /// an `i2p_pex` message body (32-byte destination hashes).
    I2pPexPacket(Vec<u8>),
    /// `ut_holepunch` message (BEP-55) — caller routes it through
    /// `ut_holepunch::{rendezvous, connect_allowed}`.
    Holepunch(crate::ut_holepunch::Hp),
    /// `yourip` from the BEP-10 handshake — the peer's view of our
    /// external address (`ip_voter` `source_peer`).
    YourIp(std::net::IpAddr),
    /// `unwanted_block_alert` — a `piece` message arrived for a
    /// block that is not in either request queue.
    UnwantedBlock(PieceBlock),
    /// `block_finished_alert` — a `piece` message completed a
    /// requested block.
    BlockFinished(PieceBlock),
    /// `peer_unsnubbed_alert` — data arrived from a snubbed peer
    /// (upstream clears `m_snubbed` in `incoming_piece`).
    Unsnub,
    /// `write_reject_request` — send a BEP-6 `reject_request` for a
    /// malformed/unservable request. The torrent layer gates the
    /// actual send on `supports_fast`.
    RejectRequest(bitdaemon_common::PeerRequest),
    /// `invalid_request_alert` — a request we rejected (malformed
    /// range, super-seed mismatch, choked + not allowed-fast). The
    /// `msg` mirrors upstream's peer_log reason text.
    InvalidRequest(String),
    /// protocol violation — drop the connection.
    Disconnect(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitdaemon_common::Sha1Hash;
    use bitdaemon_net::peer_wire::PeerMessage;

    fn conn(remote: &str) -> PeerConn {
        let mut c = PeerConn::new(100, PeerPipeline::new(1000, 30, 250), 0);
        c.remote = Some(remote.parse().unwrap());
        c
    }

    #[test]
    fn allowed_set_deterministic_and_bounded() {
        let ih = Sha1Hash::from_bytes(&[7u8; 20]).unwrap();
        let mut a = conn("1.2.3.4:6881");
        let mut b = conn("1.2.3.4:6881");
        let sa = a.send_allowed_set(&ih, 100, 5, true, false, false);
        let sb = b.send_allowed_set(&ih, 100, 5, true, false, false);
        assert_eq!(sa, sb);
        assert!(!sa.is_empty());
        assert!(sa.len() <= 5);
        // different addr → different set
        let mut c = conn("5.6.7.8:6881");
        let sc = c.send_allowed_set(&ih, 100, 5, true, false, false);
        assert_ne!(sa, sc);
    }

    #[test]
    fn allowed_set_skips_peer_pieces_and_gates() {
        let ih = Sha1Hash::from_bytes(&[9u8; 20]).unwrap();
        let mut c = conn("1.2.3.4:6881");
        // no metadata → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, false, false, false)
                .is_empty()
        );
        // super-seeding → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, true, false)
                .is_empty()
        );
        // upload-only → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, false, true)
                .is_empty()
        );
        // set_size <= 0 → nothing
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, false, true)
                .is_empty()
        );
        // peer has every piece → nothing offered
        c.peer_pieces.set_all();
        assert!(
            c.send_allowed_set(&ih, 100, 5, true, false, false)
                .is_empty()
        );
    }

    #[test]
    fn allowed_set_larger_than_torrent() {
        let ih = Sha1Hash::from_bytes(&[1u8; 20]).unwrap();
        let mut c = conn("9.9.9.9:1");
        let msgs = c.send_allowed_set(&ih, 3, 10, true, false, false);
        assert_eq!(msgs.len(), 3);
        assert!(
            msgs.iter()
                .all(|m| matches!(m, PeerMessage::AllowedFast(_)))
        );
    }
}
