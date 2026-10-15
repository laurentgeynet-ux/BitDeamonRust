// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `ut_metadata.cpp` (BEP-9) — the `ut_metadata` extension
//! wire format and the download-side bookkeeping. Peer/session
//! wiring stays in the caller; this is the message codec plus
//! `UtMetadataTracker` (m_metadata + m_requested_metadata).

use std::time::{Duration, Instant};

use bitdaemon_bencode::{Entry, NodeType, bdecode_checked, bencode};

/// `msg_t` — `ut_metadata` message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum MetadataMsg {
    /// `request`.
    Request = 0,
    /// `piece` (carries a payload).
    Piece = 1,
    /// `dont_have` / reject.
    Reject = 2,
}

/// Build the extended-message body for a metadata packet
/// (`{msg_type, piece, total_size?}` dict ‖ payload). Upstream
/// `write_metadata_packet` splits this into `[len][0x14][ext-idx]`
/// plus the payload — the caller frames it.
#[must_use]
pub fn metadata_packet(
    ty: MetadataMsg,
    piece: i32,
    have_metadata: bool,
    metadata: &[u8],
) -> Vec<u8> {
    let mut e = Entry::Dict(Default::default());
    let Ok(d) = e.dict_mut() else {
        return Vec::new();
    };
    d.insert(b"msg_type".to_vec(), Entry::Int(ty as i64));
    d.insert(b"piece".to_vec(), Entry::Int(i64::from(piece)));
    let mut payload = Vec::new();
    if have_metadata && !metadata.is_empty() {
        d.insert(b"total_size".to_vec(), Entry::Int(metadata.len() as i64));
        if ty == MetadataMsg::Piece {
            let offset = piece.max(0) as usize * 16 * 1024;
            // the serving side bounds-checks `piece` upstream before
            // calling this; clamp anyway so an out-of-range piece
            // degrades to an empty payload, never a panic.
            if offset < metadata.len() {
                let end = (offset + 16 * 1024).min(metadata.len());
                payload = metadata[offset..end].to_vec();
            }
        }
    }
    let mut out = bencode(&e);
    out.extend_from_slice(&payload);
    out
}

/// `parse_metadata_packet` verdict — upstream `on_extended`
/// distinguishes *malformed* (bdecode failure, non-dict, missing or
/// non-int `msg_type`/`piece` → `invalid_metadata_message` peer-error
/// disconnect) from *unknown `msg_type`* (the switch has no default:
/// consumed silently, still counted).
#[derive(Debug)]
pub enum MetadataParse<'a> {
    /// well-formed `(msg_type, piece, total_size, payload)`.
    Msg(MetadataMsg, i64, i64, &'a [u8]),
    /// valid dict with an out-of-range `msg_type` — upstream's
    /// switch has no `default`, so the packet is consumed and
    /// counted but otherwise ignored.
    UnknownType,
    /// bdecode failure, non-dict, or absent/wrong-type
    /// `msg_type`/`piece` — `invalid_metadata_message` disconnect.
    Malformed,
}

/// Parse a metadata packet body. `payload` is the data after the
/// dict (`msg.data_section()` upstream gives us its extent).
pub fn parse_metadata_packet(body: &[u8]) -> MetadataParse<'_> {
    let Ok(msg) = bdecode_checked(body) else {
        return MetadataParse::Malformed;
    };
    if msg.node_type() != NodeType::Dict {
        return MetadataParse::Malformed;
    }
    // upstream requires both `msg_type` and `piece` present as ints
    let ty_node = msg.dict_find_int(b"msg_type");
    let piece_node = msg.dict_find_int(b"piece");
    if ty_node.node_type() != NodeType::Int || piece_node.node_type() != NodeType::Int {
        return MetadataParse::Malformed;
    }
    let piece = piece_node.int_value();
    let total_size = msg.dict_find_int_value(b"total_size", 0);
    let data_len = msg.data_section().len();
    let payload = &body[data_len.min(body.len())..];
    match ty_node.int_value() {
        0 => MetadataParse::Msg(MetadataMsg::Request, piece, total_size, payload),
        1 => MetadataParse::Msg(MetadataMsg::Piece, piece, total_size, payload),
        2 => MetadataParse::Msg(MetadataMsg::Reject, piece, total_size, payload),
        _ => MetadataParse::UnknownType,
    }
}

/// `metadata_piece` — per-piece request state.
#[derive(Debug)]
struct MetadataPiece {
    num_requests: i32,
    last_request: Option<Instant>,
    /// peer handle that delivered/last served the piece
    source: Option<u64>,
}

/// `ut_metadata_plugin` download state — `m_metadata` +
/// `m_requested_metadata`.
#[derive(Debug, Default)]
pub struct UtMetadataTracker {
    /// `m_metadata` — the buffer being filled.
    metadata: Vec<u8>,
    /// `m_requested_metadata`.
    pieces: Vec<MetadataPiece>,
}

impl UtMetadataTracker {
    /// `ut_metadata_plugin`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `metadata()` — the assembled metadata (once complete).
    #[must_use]
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    /// `metadata_size` — the peer advertised the info-dict size in
    /// its extended handshake: pre-size the buffers so the first
    /// `metadata_request` can pick any piece, not just 0. Upstream
    /// ignores the advertisement when we already have metadata and
    /// caps it at 4 MiB (`m_metadata.resize` guard); `received_metadata`
    /// applies the looser `max_metadata_size` cap instead when the
    /// handshake never carried a size.
    pub fn metadata_size(&mut self, size: i64, have_valid_metadata: bool) {
        if have_valid_metadata || size <= 0 || size > 4 * 1024 * 1024 {
            return;
        }
        self.metadata = vec![0u8; size as usize];
        let n = (size as usize).div_ceil(16 * 1024);
        self.pieces = (0..n)
            .map(|_| MetadataPiece {
                num_requests: 0,
                last_request: None,
                source: None,
            })
            .collect();
    }

    /// `metadata_request(has_metadata)` — pick the least-requested
    /// piece; `-1` when nothing can be requested right now.
    /// `source_alive` reports whether the recorded source peer is
    /// still connected (upstream checks `!source->is_disconnecting()`).
    pub fn metadata_request(
        &mut self,
        has_metadata: bool,
        mut source_alive: impl FnMut(u64) -> bool,
    ) -> i32 {
        if self.pieces.is_empty() {
            // don't know how many pieces — ask for piece 0
            self.pieces.push(MetadataPiece {
                num_requests: 0,
                last_request: None,
                source: None,
            });
        }
        // least-requested piece (num_requests == MAX means "have")
        let idx = self
            .pieces
            .iter()
            .enumerate()
            .min_by_key(|(_, p)| p.num_requests)
            .map(|(i, _)| i)
            .unwrap_or(0);
        let piece = idx as i32;

        let now = Instant::now();
        if let Some(last) = self.pieces[idx].last_request {
            let alive = self.pieces[idx].source.is_some_and(&mut source_alive);
            if alive && now.duration_since(last) < Duration::from_secs(3) {
                return -1;
            }
        }
        self.pieces[idx].num_requests += 1;
        if has_metadata {
            self.pieces[idx].last_request = Some(now);
        }
        piece
    }

    /// `received_metadata(buf, piece, total_size)` — `max_metadata_size`
    /// mirrors `settings_pack::max_metadata_size` (30 MiB default
    /// upstream). Returns `true` only when the metadata is now complete
    /// (the caller then validates it against the info hash).
    pub fn received_metadata(
        &mut self,
        buf: &[u8],
        piece: i32,
        total_size: i64,
        max_metadata_size: i64,
        have_valid_metadata: bool,
    ) -> MetadataResult {
        if have_valid_metadata {
            return MetadataResult::Redundant;
        }
        if self.metadata.is_empty() {
            if total_size <= 0 || total_size > max_metadata_size {
                return MetadataResult::TooBig;
            }
            self.metadata = vec![0u8; total_size as usize];
            let n = (total_size as usize).div_ceil(16 * 1024);
            self.pieces = (0..n)
                .map(|_| MetadataPiece {
                    num_requests: 0,
                    last_request: None,
                    source: None,
                })
                .collect();
        }
        if piece < 0 || piece as usize >= self.pieces.len() {
            return MetadataResult::InvalidPiece;
        }
        if total_size != self.metadata.len() as i64 {
            return MetadataResult::InconsistentSize;
        }
        let offset = piece as usize * 16 * 1024;
        if offset + buf.len() > self.metadata.len() {
            return MetadataResult::InvalidPiece;
        }
        self.metadata[offset..offset + buf.len()].copy_from_slice(buf);
        self.pieces[piece as usize].num_requests = i32::MAX;
        // source is set by the caller via `set_source`

        if self.pieces.iter().all(|p| p.num_requests == i32::MAX) {
            MetadataResult::Complete
        } else {
            MetadataResult::Stored
        }
    }

    /// Record which peer served `piece` (`source` weak_ptr upstream).
    pub fn set_source(&mut self, piece: i32, source: u64) {
        if let Some(p) = self.pieces.get_mut(piece as usize) {
            p.source = Some(source);
        }
    }

    /// Metadata failed the info-hash check — upstream resets
    /// `num_requests` to 0 and penalizes each source peer via
    /// `failed_hash_check`; the buffer itself is kept. Returns the
    /// source peer handles to penalize (the caller applies the retry
    /// limit — `now + 5min` for a single-piece torrent, else `now`).
    pub fn reset_on_hash_failure(&mut self) -> (bool, Vec<u64>) {
        let single_peer = self.pieces.len() == 1;
        let mut peers = Vec::new();
        for p in &mut self.pieces {
            p.num_requests = 0;
            if let Some(s) = p.source {
                peers.push(s);
            }
        }
        (single_peer, peers)
    }

    /// Clear all state (metadata consumed/freed — upstream clears and
    /// shrinks both buffers after a successful `set_metadata`).
    pub fn clear(&mut self) {
        self.metadata.clear();
        self.metadata.shrink_to_fit();
        self.pieces.clear();
        self.pieces.shrink_to_fit();
    }
}

/// Outcome of [`UtMetadataTracker::received_metadata`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataResult {
    /// stored, more pieces remain.
    Stored,
    /// all pieces present.
    Complete,
    /// we already have metadata (`add_redundant_bytes` upstream).
    Redundant,
    /// `total_size` invalid/exceeds `max_metadata_size`.
    TooBig,
    /// `piece` out of range or payload overflowing `total_size`.
    InvalidPiece,
    /// `total_size` disagrees with what the first piece announced.
    InconsistentSize,
}
