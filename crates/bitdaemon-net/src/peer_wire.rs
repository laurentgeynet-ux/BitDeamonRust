//! Port of the `bt_peer_connection.cpp`/`bt_peer_connection.hpp` packet
//! layer (message framing only — the conn state machine is
//! `session::peer_conn`).
//!
//! Peer wire protocol codec — `bt_peer_connection` packet serialization:
//! 68-byte handshake, length-prefixed messages (4-byte BE len + 1-byte
//! id), keepalive, all standard + FAST + BEP-10 + v2-hash message types.
//!
//! Framing is codec-level only; `PeerConnection`'s state machine is a
//! later wave.

use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;

use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::{PeerId, PeerRequest, Sha1Hash, Sha256Hash};

use crate::socket_io::{write_u8, write_u16, write_u32};

/// BitTorrent handshake length — `pstrlen` + `pstr` + 8 reserved +
/// 20 info_hash + 20 peer_id.
pub const HANDSHAKE_LEN: usize = 68;

/// `pstr`.
pub const PSTR: &[u8] = b"BitTorrent protocol";

/// Reserved-byte flags (`write_handshake`).
pub mod reserved {
    /// `*(ptr+7) |= 0x01` — DHT messages.
    pub const DHT: usize = 7;
    /// `*(ptr+5) |= 0x10` — BEP-10 extension protocol.
    pub const EXTENSION: usize = 5;
    /// `*(ptr+7) |= 0x04` — FAST extension.
    pub const FAST: usize = 7;
    /// `*(ptr+7) |= 0x10` — v1 peer on a hybrid torrent, v2-capable.
    pub const V2: usize = 7;

    /// Bit values.
    pub const DHT_BIT: u8 = 0x01;
    /// BEP-10.
    pub const EXTENSION_BIT: u8 = 0x10;
    /// FAST.
    pub const FAST_BIT: u8 = 0x04;
    /// v2 upgrade.
    pub const V2_BIT: u8 = 0x10;
}

/// `bt_peer_connection::message_type` — wire ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageId {
    /// `msg_choke`.
    Choke = 0,
    /// `msg_unchoke`.
    Unchoke = 1,
    /// `msg_interested`.
    Interested = 2,
    /// `msg_not_interested`.
    NotInterested = 3,
    /// `msg_have`.
    Have = 4,
    /// `msg_bitfield`.
    Bitfield = 5,
    /// `msg_request`.
    Request = 6,
    /// `msg_piece`.
    Piece = 7,
    /// `msg_cancel`.
    Cancel = 8,
    /// `msg_dht_port`.
    DhtPort = 9,
    /// `msg_suggest_piece`.
    SuggestPiece = 0xd,
    /// `msg_have_all`.
    HaveAll = 0xe,
    /// `msg_have_none`.
    HaveNone = 0xf,
    /// `msg_reject_request`.
    RejectRequest = 0x10,
    /// `msg_allowed_fast`.
    AllowedFast = 0x11,
    /// `msg_extended` — BEP-10.
    Extended = 20,
    /// `msg_hash_request` — v2.
    HashRequest = 21,
    /// `msg_hashes`.
    Hashes = 22,
    /// `msg_hash_reject`.
    HashReject = 23,
}

impl MessageId {
    /// Map a wire byte to an id.
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Choke,
            1 => Self::Unchoke,
            2 => Self::Interested,
            3 => Self::NotInterested,
            4 => Self::Have,
            5 => Self::Bitfield,
            6 => Self::Request,
            7 => Self::Piece,
            8 => Self::Cancel,
            9 => Self::DhtPort,
            0xd => Self::SuggestPiece,
            0xe => Self::HaveAll,
            0xf => Self::HaveNone,
            0x10 => Self::RejectRequest,
            0x11 => Self::AllowedFast,
            20 => Self::Extended,
            21 => Self::HashRequest,
            22 => Self::Hashes,
            23 => Self::HashReject,
            _ => return None,
        })
    }
}

/// A v2 hash-range descriptor on the wire — file identified by its
/// merkle root (not index), matching `write_hash_request`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashRequestWire {
    /// File merkle root (32 bytes on the wire).
    pub file_root: Sha256Hash,
    /// Tree level.
    pub base: i32,
    /// First hash at that level.
    pub index: i32,
    /// Count.
    pub count: i32,
    /// Proof layers.
    pub proof_layers: i32,
}

/// Decoded peer wire message (payload owned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    /// Keepalive — 4-byte zero length.
    Keepalive,
    /// `msg_choke`.
    Choke,
    /// `msg_unchoke`.
    Unchoke,
    /// `msg_interested`.
    Interested,
    /// `msg_not_interested`.
    NotInterested,
    /// `msg_have` — piece index.
    Have(PieceIndex),
    /// `msg_bitfield` — raw bitfield bytes (MSB-first network order).
    Bitfield(Bytes),
    /// `msg_request` — `peer_request`.
    Request(PeerRequest),
    /// `msg_piece` — index, offset, block data.
    Piece {
        /// `piece`.
        piece: PieceIndex,
        /// `start`.
        offset: i32,
        /// Block payload.
        data: Bytes,
    },
    /// `msg_cancel`.
    Cancel(PeerRequest),
    /// `msg_dht_port` — u16.
    DhtPort(u16),
    /// `msg_suggest_piece`.
    SuggestPiece(PieceIndex),
    /// `msg_have_all`.
    HaveAll,
    /// `msg_have_none`.
    HaveNone,
    /// `lt_donthave` — `write_dont_have`. NOT a standard message:
    /// upstream sends a BEP-10 `msg_extended` frame carrying the
    /// piece index under the *remote's* negotiated `lt_donthave`
    /// extension id. Callers must only build this when the peer
    /// advertised the extension (`ext_id != 0`) — upstream
    /// suppresses the send otherwise.
    DontHave {
        /// Piece index.
        piece: PieceIndex,
        /// Remote's `lt_donthave` BEP-10 extension id.
        ext_id: u8,
    },
    /// `msg_reject_request`.
    RejectRequest(PeerRequest),
    /// `msg_allowed_fast`.
    AllowedFast(PieceIndex),
    /// `msg_extended` — BEP-10: `id` + bencoded payload.
    Extended {
        /// Extension message id (`0` = handshake).
        id: u8,
        /// Raw payload (bencoded).
        data: Bytes,
    },
    /// `msg_hash_request`.
    HashRequest(HashRequestWire),
    /// `msg_hashes` — descriptor + `count` hashes.
    Hashes {
        /// Request descriptor.
        req: HashRequestWire,
        /// Hash list (`count` × 32 bytes + proofs appended).
        hashes: Vec<Sha256Hash>,
    },
    /// `msg_hash_reject`.
    HashReject(HashRequestWire),
}

/// Codec errors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WireError {
    /// Truncated input.
    #[error("short packet: {0}")]
    Truncated(&'static str),
    /// Unknown message id.
    #[error("unknown message id {0}")]
    UnknownId(u8),
    /// Malformed payload length for the message type.
    #[error("bad payload length {len} for message {id}")]
    BadLength {
        /// Message id.
        id: u8,
        /// Payload length.
        len: usize,
    },
}

/// `write_handshake` — build the 68-byte handshake.
///
/// `supports_v2` mirrors `!protocol_v2 && has_v2` upstream (a v1-only
/// peer on a hybrid torrent advertises the upgrade bit).
#[must_use]
pub fn write_handshake(
    info_hash: &Sha1Hash,
    peer_id: &PeerId,
    dht: bool,
    extension: bool,
    fast: bool,
    supports_v2: bool,
) -> [u8; HANDSHAKE_LEN] {
    let mut h = [0u8; HANDSHAKE_LEN];
    h[0] = PSTR.len() as u8;
    h[1..1 + PSTR.len()].copy_from_slice(PSTR);
    let r = &mut h[1 + PSTR.len()..1 + PSTR.len() + 8];
    if dht {
        r[reserved::DHT] |= reserved::DHT_BIT;
    }
    if extension {
        r[reserved::EXTENSION] |= reserved::EXTENSION_BIT;
    }
    if fast {
        r[reserved::FAST] |= reserved::FAST_BIT;
    }
    if supports_v2 {
        r[reserved::V2] |= reserved::V2_BIT;
    }
    h[1 + PSTR.len() + 8..1 + PSTR.len() + 8 + 20].copy_from_slice(info_hash.as_ref());
    h[1 + PSTR.len() + 8 + 20..].copy_from_slice(peer_id.as_bytes());
    h
}

/// Parsed handshake.
#[derive(Debug, Clone)]
pub struct Handshake {
    /// Reserved bytes (inspect bits via `reserved::*`).
    pub reserved: [u8; 8],
    /// Info hash (v1 or v1-side of hybrid).
    pub info_hash: Sha1Hash,
    /// Remote peer id.
    pub peer_id: PeerId,
}

impl Handshake {
    /// BEP-10 extension support.
    #[must_use]
    pub fn supports_extension(&self) -> bool {
        self.reserved[reserved::EXTENSION] & reserved::EXTENSION_BIT != 0
    }
    /// DHT port messages.
    #[must_use]
    pub fn supports_dht(&self) -> bool {
        self.reserved[reserved::DHT] & reserved::DHT_BIT != 0
    }
    /// FAST extension.
    #[must_use]
    pub fn supports_fast(&self) -> bool {
        self.reserved[reserved::FAST] & reserved::FAST_BIT != 0
    }
    /// v2 upgrade bit.
    #[must_use]
    pub fn supports_v2(&self) -> bool {
        self.reserved[reserved::V2] & reserved::V2_BIT != 0
    }
}

/// `parse_handshake` — strict 68-byte validation.
pub fn parse_handshake(buf: &[u8]) -> Result<Handshake, WireError> {
    if buf.len() < HANDSHAKE_LEN {
        return Err(WireError::Truncated("handshake"));
    }
    let pstrlen = buf[0] as usize;
    if buf.len() < 1 + pstrlen + 8 + 20 + 20 || pstrlen != PSTR.len() {
        return Err(WireError::BadLength {
            id: 0xff,
            len: pstrlen,
        });
    }
    if &buf[1..1 + pstrlen] != PSTR {
        return Err(WireError::BadLength {
            id: 0xfe,
            len: pstrlen,
        });
    }
    let mut r = [0u8; 8];
    r.copy_from_slice(&buf[1 + pstrlen..1 + pstrlen + 8]);
    let info_hash =
        Sha1Hash::from_bytes(&buf[1 + pstrlen + 8..1 + pstrlen + 28]).unwrap_or_default();
    let peer_id = PeerId::from_bytes(&buf[1 + pstrlen + 28..1 + pstrlen + 48]).unwrap_or_default();
    Ok(Handshake {
        reserved: r,
        info_hash,
        peer_id,
    })
}

fn write_request(out: &mut Vec<u8>, id: MessageId, r: &PeerRequest) {
    // len = 1 (id) + 12 (3×u32)
    write_u32(13, out);
    write_u8(id as u8, out);
    write_u32(r.piece.0 as u32, out);
    write_u32(r.start as u32, out);
    write_u32(r.length as u32, out);
}

fn write_piece_index(out: &mut Vec<u8>, id: MessageId, p: PieceIndex) {
    write_u32(5, out);
    write_u8(id as u8, out);
    write_u32(p.0 as u32, out);
}

fn write_hash_req(out: &mut Vec<u8>, id: MessageId, r: &HashRequestWire) {
    // len = 1 + 32 + 16
    write_u32(49, out);
    write_u8(id as u8, out);
    out.extend_from_slice(r.file_root.as_ref());
    write_u32(r.base as u32, out);
    write_u32(r.index as u32, out);
    write_u32(r.count as u32, out);
    write_u32(r.proof_layers as u32, out);
}

impl PeerMessage {
    /// The wire message id this variant serializes as — `0x14`
    /// (`msg_extended`) for `DontHave` (an `lt_donthave` BEP-10
    /// frame) and `Extended`. `Keepalive` has no id on the wire;
    /// it returns `u8::MAX` so callers can skip it.
    #[must_use]
    pub fn wire_id(&self) -> u8 {
        match self {
            Self::Choke => MessageId::Choke as u8,
            Self::Unchoke => MessageId::Unchoke as u8,
            Self::Interested => MessageId::Interested as u8,
            Self::NotInterested => MessageId::NotInterested as u8,
            Self::Have(_) => MessageId::Have as u8,
            Self::Bitfield(_) => MessageId::Bitfield as u8,
            Self::Request(_) => MessageId::Request as u8,
            Self::Piece { .. } => MessageId::Piece as u8,
            Self::Cancel(_) => MessageId::Cancel as u8,
            Self::DhtPort(_) => MessageId::DhtPort as u8,
            Self::SuggestPiece(_) => MessageId::SuggestPiece as u8,
            Self::HaveAll => MessageId::HaveAll as u8,
            Self::HaveNone => MessageId::HaveNone as u8,
            Self::DontHave { .. } | Self::Extended { .. } => MessageId::Extended as u8,
            Self::RejectRequest(_) => MessageId::RejectRequest as u8,
            Self::AllowedFast(_) => MessageId::AllowedFast as u8,
            Self::HashRequest(_) => MessageId::HashRequest as u8,
            Self::Hashes { .. } => MessageId::Hashes as u8,
            Self::HashReject(_) => MessageId::HashReject as u8,
            Self::Keepalive => u8::MAX,
        }
    }
}

/// `encode(msg)` — serialize one message (incl. length prefix).
#[must_use]
pub fn encode(msg: &PeerMessage) -> Bytes {
    let mut out = Vec::new();
    match msg {
        PeerMessage::Keepalive => out.extend_from_slice(&0u32.to_be_bytes()),
        PeerMessage::Choke => {
            write_u32(1, &mut out);
            write_u8(MessageId::Choke as u8, &mut out);
        }
        PeerMessage::Unchoke => {
            write_u32(1, &mut out);
            write_u8(MessageId::Unchoke as u8, &mut out);
        }
        PeerMessage::Interested => {
            write_u32(1, &mut out);
            write_u8(MessageId::Interested as u8, &mut out);
        }
        PeerMessage::NotInterested => {
            write_u32(1, &mut out);
            write_u8(MessageId::NotInterested as u8, &mut out);
        }
        PeerMessage::Have(p) => write_piece_index(&mut out, MessageId::Have, *p),
        PeerMessage::Bitfield(b) => {
            write_u32(1 + b.len() as u32, &mut out);
            write_u8(MessageId::Bitfield as u8, &mut out);
            out.extend_from_slice(b);
        }
        PeerMessage::Request(r) => write_request(&mut out, MessageId::Request, r),
        PeerMessage::Piece {
            piece,
            offset,
            data,
        } => {
            write_u32(9 + data.len() as u32, &mut out);
            write_u8(MessageId::Piece as u8, &mut out);
            write_u32(piece.0 as u32, &mut out);
            write_u32(*offset as u32, &mut out);
            out.extend_from_slice(data);
        }
        PeerMessage::Cancel(r) => write_request(&mut out, MessageId::Cancel, r),
        PeerMessage::DhtPort(port) => {
            write_u32(3, &mut out);
            write_u8(MessageId::DhtPort as u8, &mut out);
            write_u16(*port, &mut out);
        }
        PeerMessage::SuggestPiece(p) => {
            write_piece_index(&mut out, MessageId::SuggestPiece, *p);
        }
        PeerMessage::HaveAll => {
            write_u32(1, &mut out);
            write_u8(MessageId::HaveAll as u8, &mut out);
        }
        PeerMessage::HaveNone => {
            write_u32(1, &mut out);
            write_u8(MessageId::HaveNone as u8, &mut out);
        }
        // `write_dont_have` — BEP-10 extended frame under the
        // negotiated `lt_donthave` id, not a standard message.
        PeerMessage::DontHave { piece, ext_id } => {
            write_u32(6, &mut out);
            write_u8(MessageId::Extended as u8, &mut out);
            write_u8(*ext_id, &mut out);
            write_u32(piece.0 as u32, &mut out);
        }
        PeerMessage::RejectRequest(r) => {
            write_request(&mut out, MessageId::RejectRequest, r);
        }
        PeerMessage::AllowedFast(p) => {
            write_piece_index(&mut out, MessageId::AllowedFast, *p);
        }
        PeerMessage::Extended { id, data } => {
            write_u32(2 + data.len() as u32, &mut out);
            write_u8(MessageId::Extended as u8, &mut out);
            write_u8(*id, &mut out);
            out.extend_from_slice(data);
        }
        PeerMessage::HashRequest(r) => write_hash_req(&mut out, MessageId::HashRequest, r),
        PeerMessage::Hashes { req, hashes } => {
            write_u32(49 + 32 * hashes.len() as u32, &mut out);
            write_u8(MessageId::Hashes as u8, &mut out);
            out.extend_from_slice(req.file_root.as_ref());
            write_u32(req.base as u32, &mut out);
            write_u32(req.index as u32, &mut out);
            write_u32(req.count as u32, &mut out);
            write_u32(req.proof_layers as u32, &mut out);
            for h in hashes {
                out.extend_from_slice(h.as_ref());
            }
        }
        PeerMessage::HashReject(r) => write_hash_req(&mut out, MessageId::HashReject, r),
    }
    out.into()
}

/// Total bytes [`encode`] produces for `msg`, framing included —
/// upstream's `send_buffer_size()` counts the same chained-buffer
/// bytes when it applies the send-buffer watermark.
#[must_use]
pub fn wire_len(msg: &PeerMessage) -> usize {
    match msg {
        PeerMessage::Keepalive => 4,
        // id + payload only messages
        PeerMessage::Choke
        | PeerMessage::Unchoke
        | PeerMessage::Interested
        | PeerMessage::NotInterested
        | PeerMessage::HaveAll
        | PeerMessage::HaveNone => 5,
        PeerMessage::Have(_) | PeerMessage::SuggestPiece(_) | PeerMessage::AllowedFast(_) => 9,
        PeerMessage::DontHave { .. } => 10,
        PeerMessage::Bitfield(b) => 5 + b.len(),
        PeerMessage::Request(_) | PeerMessage::Cancel(_) | PeerMessage::RejectRequest(_) => 17,
        PeerMessage::Piece { data, .. } => 13 + data.len(),
        PeerMessage::DhtPort(_) => 7,
        PeerMessage::Extended { data, .. } => 6 + data.len(),
        PeerMessage::HashRequest(_) | PeerMessage::HashReject(_) => 53,
        PeerMessage::Hashes { hashes, .. } => 53 + 32 * hashes.len(),
    }
}

fn read_u32(cur: &mut crate::socket_io::Cursor<'_>) -> Result<u32, WireError> {
    cur.read_u32().map_err(|_| WireError::Truncated("u32"))
}

fn read_request(cur: &mut crate::socket_io::Cursor<'_>) -> Result<PeerRequest, WireError> {
    Ok(PeerRequest {
        piece: PieceIndex(read_u32(cur)? as i32),
        start: read_u32(cur)? as i32,
        length: read_u32(cur)? as i32,
    })
}

fn read_hash_req(cur: &mut crate::socket_io::Cursor<'_>) -> Result<HashRequestWire, WireError> {
    let root = Sha256Hash::from_bytes(
        cur.read_bytes(32)
            .map_err(|_| WireError::Truncated("file_root"))?,
    )
    .unwrap_or_default();
    Ok(HashRequestWire {
        file_root: root,
        base: read_u32(cur)? as i32,
        index: read_u32(cur)? as i32,
        count: read_u32(cur)? as i32,
        proof_layers: read_u32(cur)? as i32,
    })
}

/// Exact-size check — upstream `dispatch`/`on_*` disconnects
/// (`invalid_*`) when `packet_size` doesn't match the message's fixed
/// size. `want` counts the bytes after the 4-byte len (id included),
/// matching upstream's `packet_size()` semantics.
fn expect_len(buf_len: usize, want: usize, id: u8) -> Result<(), WireError> {
    if buf_len != want {
        return Err(WireError::BadLength { id, len: buf_len });
    }
    Ok(())
}

/// `decode(buf)` — decode one message payload (the bytes *after* the
/// 4-byte length prefix). A zero-length body = keepalive.
/// Fixed-size messages must match exactly (upstream disconnects
/// `invalid_message` otherwise). Bitfield size is validated by the
/// session against `num_pieces` (`invalid_bitfield_size`) — it needs
/// torrent context the codec doesn't have.
pub fn decode(buf: &[u8]) -> Result<PeerMessage, WireError> {
    if buf.is_empty() {
        return Ok(PeerMessage::Keepalive);
    }
    let id = buf[0];
    let payload = &buf[1..];
    let mut cur = crate::socket_io::Cursor::new(payload);
    Ok(match MessageId::from_u8(id) {
        Some(MessageId::Choke) => {
            expect_len(buf.len(), 1, id)?;
            PeerMessage::Choke
        }
        Some(MessageId::Unchoke) => {
            expect_len(buf.len(), 1, id)?;
            PeerMessage::Unchoke
        }
        Some(MessageId::Interested) => {
            expect_len(buf.len(), 1, id)?;
            PeerMessage::Interested
        }
        Some(MessageId::NotInterested) => {
            expect_len(buf.len(), 1, id)?;
            PeerMessage::NotInterested
        }
        Some(MessageId::Have) => {
            expect_len(buf.len(), 5, id)?;
            PeerMessage::Have(PieceIndex(read_u32(&mut cur)? as i32))
        }
        Some(MessageId::Bitfield) => PeerMessage::Bitfield(Bytes::copy_from_slice(payload)),
        Some(MessageId::Request) => {
            expect_len(buf.len(), 13, id)?;
            PeerMessage::Request(read_request(&mut cur)?)
        }
        Some(MessageId::Piece) => {
            // id + index + begin ≥ 9; the block-size cap is the
            // session's (`packet_too_large` > `block_size`).
            if buf.len() < 9 {
                return Err(WireError::BadLength { id, len: buf.len() });
            }
            let piece = PieceIndex(read_u32(&mut cur)? as i32);
            let offset = read_u32(&mut cur)? as i32;
            PeerMessage::Piece {
                piece,
                offset,
                data: Bytes::copy_from_slice(cur.tail()),
            }
        }
        Some(MessageId::Cancel) => {
            expect_len(buf.len(), 13, id)?;
            PeerMessage::Cancel(read_request(&mut cur)?)
        }
        Some(MessageId::DhtPort) => {
            expect_len(buf.len(), 3, id)?;
            PeerMessage::DhtPort(cur.read_u16().map_err(|_| WireError::Truncated("u16"))?)
        }
        Some(MessageId::SuggestPiece) => {
            expect_len(buf.len(), 5, id)?;
            PeerMessage::SuggestPiece(PieceIndex(read_u32(&mut cur)? as i32))
        }
        Some(MessageId::HaveAll) => {
            expect_len(buf.len(), 1, id)?;
            PeerMessage::HaveAll
        }
        Some(MessageId::HaveNone) => {
            // upstream `on_have_none` requires `packet_size == 1`
            // exactly — anything else is `invalid_have_none` and
            // disconnects the peer.
            expect_len(buf.len(), 1, id)?;
            PeerMessage::HaveNone
        }
        Some(MessageId::RejectRequest) => {
            expect_len(buf.len(), 13, id)?;
            PeerMessage::RejectRequest(read_request(&mut cur)?)
        }
        Some(MessageId::AllowedFast) => {
            expect_len(buf.len(), 5, id)?;
            PeerMessage::AllowedFast(PieceIndex(read_u32(&mut cur)? as i32))
        }
        Some(MessageId::Extended) => {
            // upstream: packet_size < 2 → invalid_extended
            if payload.is_empty() {
                return Err(WireError::BadLength { id, len: buf.len() });
            }
            PeerMessage::Extended {
                id: payload[0],
                data: Bytes::copy_from_slice(&payload[1..]),
            }
        }
        Some(MessageId::HashRequest) => {
            expect_len(buf.len(), 49, id)?;
            PeerMessage::HashRequest(read_hash_req(&mut cur)?)
        }
        Some(MessageId::Hashes) => {
            if buf.len() < 49 {
                return Err(WireError::BadLength { id, len: buf.len() });
            }
            let req = read_hash_req(&mut cur)?;
            // upstream computes the exact expected size:
            // 49 + (count + proof_hashes) * 32, where proof_hashes =
            // max(0, proof_layers - (depth(count) - 1)).
            let count = i64::from(req.count);
            let depth = if count > 0 {
                bitdaemon_core::merkle::merkle_num_layers(bitdaemon_core::merkle::merkle_num_leafs(
                    count as usize,
                )) as i64
                    - 1
            } else {
                0
            };
            let proof_hashes = (i64::from(req.proof_layers) - depth).max(0);
            let want = 49 + (count + proof_hashes) * 32;
            if buf.len() as i64 != want {
                return Err(WireError::BadLength { id, len: buf.len() });
            }
            let mut hashes = Vec::with_capacity((count + proof_hashes).max(0) as usize);
            while cur.remaining() >= 32 {
                hashes.push(
                    Sha256Hash::from_bytes(cur.read_bytes(32).unwrap_or_default())
                        .unwrap_or_default(),
                );
            }
            PeerMessage::Hashes { req, hashes }
        }
        Some(MessageId::HashReject) => {
            expect_len(buf.len(), 49, id)?;
            PeerMessage::HashReject(read_hash_req(&mut cur)?)
        }
        None => return Err(WireError::UnknownId(id)),
    })
}

/// `tokio_util::codec::Decoder` for the peer wire stream — splits on the
/// 4-byte length prefix and defers message parsing to `decode`.
///
/// `max_len` bounds memory (upstream `max_receive_buffer_size`).
#[derive(Debug)]
pub struct PeerWireCodec {
    max_len: usize,
}

impl Default for PeerWireCodec {
    fn default() -> Self {
        // upstream default receive cap ~2 MiB
        Self::new(2 * 1024 * 1024)
    }
}

impl PeerWireCodec {
    /// `new(max_len)` — max message size.
    #[must_use]
    pub fn new(max_len: usize) -> Self {
        Self { max_len }
    }
}

impl tokio_util::codec::Decoder for PeerWireCodec {
    type Item = PeerMessage;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<PeerMessage>, std::io::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len == 0 {
            src.advance(4);
            return Ok(Some(PeerMessage::Keepalive));
        }
        if len > self.max_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "peer message too large",
            ));
        }
        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }
        src.advance(4);
        let payload = src.split_to(len);
        decode(&payload)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

impl tokio_util::codec::Encoder<PeerMessage> for PeerWireCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: PeerMessage, dst: &mut BytesMut) -> Result<(), std::io::Error> {
        dst.extend_from_slice(&encode(&item));
        Ok(())
    }
}
