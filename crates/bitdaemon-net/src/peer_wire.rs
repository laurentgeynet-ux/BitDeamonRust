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

/// `decode(buf)` — decode one message payload (the bytes *after* the
/// 4-byte length prefix). Returns `Ok(None)` for keepalive framing
/// handled at the stream level; here a zero-length body = keepalive.
pub fn decode(buf: &[u8]) -> Result<PeerMessage, WireError> {
    if buf.is_empty() {
        return Ok(PeerMessage::Keepalive);
    }
    let id = buf[0];
    let payload = &buf[1..];
    let mut cur = crate::socket_io::Cursor::new(payload);
    Ok(match MessageId::from_u8(id) {
        Some(MessageId::Choke) => PeerMessage::Choke,
        Some(MessageId::Unchoke) => PeerMessage::Unchoke,
        Some(MessageId::Interested) => PeerMessage::Interested,
        Some(MessageId::NotInterested) => PeerMessage::NotInterested,
        Some(MessageId::Have) => PeerMessage::Have(PieceIndex(read_u32(&mut cur)? as i32)),
        Some(MessageId::Bitfield) => PeerMessage::Bitfield(Bytes::copy_from_slice(payload)),
        Some(MessageId::Request) => PeerMessage::Request(read_request(&mut cur)?),
        Some(MessageId::Piece) => {
            let piece = PieceIndex(read_u32(&mut cur)? as i32);
            let offset = read_u32(&mut cur)? as i32;
            PeerMessage::Piece {
                piece,
                offset,
                data: Bytes::copy_from_slice(cur.tail()),
            }
        }
        Some(MessageId::Cancel) => PeerMessage::Cancel(read_request(&mut cur)?),
        Some(MessageId::DhtPort) => {
            PeerMessage::DhtPort(cur.read_u16().map_err(|_| WireError::Truncated("u16"))?)
        }
        Some(MessageId::SuggestPiece) => {
            PeerMessage::SuggestPiece(PieceIndex(read_u32(&mut cur)? as i32))
        }
        Some(MessageId::HaveAll) => PeerMessage::HaveAll,
        Some(MessageId::HaveNone) => PeerMessage::HaveNone,
        Some(MessageId::RejectRequest) => PeerMessage::RejectRequest(read_request(&mut cur)?),
        Some(MessageId::AllowedFast) => {
            PeerMessage::AllowedFast(PieceIndex(read_u32(&mut cur)? as i32))
        }
        Some(MessageId::Extended) => {
            if payload.is_empty() {
                return Err(WireError::Truncated("extended id"));
            }
            PeerMessage::Extended {
                id: payload[0],
                data: Bytes::copy_from_slice(&payload[1..]),
            }
        }
        Some(MessageId::HashRequest) => PeerMessage::HashRequest(read_hash_req(&mut cur)?),
        Some(MessageId::Hashes) => {
            let req = read_hash_req(&mut cur)?;
            let mut hashes = Vec::new();
            while cur.remaining() >= 32 {
                hashes.push(
                    Sha256Hash::from_bytes(cur.read_bytes(32).unwrap_or_default())
                        .unwrap_or_default(),
                );
            }
            PeerMessage::Hashes { req, hashes }
        }
        Some(MessageId::HashReject) => PeerMessage::HashReject(read_hash_req(&mut cur)?),
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
