//! Port of `aux_/request_blocks.cpp` — `request_a_block`: the glue
//! between `peer_connection` (request pipeline state) and
//! `piece_picker` (block selection).
//!
//! Upstream reads state off `torrent` and `peer_connection` objects;
//! here the caller passes a `BlockRequestCtx` snapshot plus the
//! `&mut PiecePicker`. `add_request` is a closure the caller supplies
//! — it is responsible for calling `picker.mark_as_downloading` and
//! pushing the block onto its own download queue (upstream's
//! `peer_connection::add_request`).

use bitdaemon_common::{Bitfield, PeerSourceFlags, PieceBlock};

use crate::piece_picker::{PickerStats, PiecePicker};

/// `aux::source_rank` — peer-source affinity; connecting to
/// higher-ranked sources is preferred.
#[must_use]
pub fn source_rank(source: PeerSourceFlags) -> i32 {
    let mut ret = 0;
    if source.contains(PeerSourceFlags::TRACKER) {
        ret |= 1 << 5;
    }
    if source.contains(PeerSourceFlags::LSD) {
        ret |= 1 << 4;
    }
    if source.contains(PeerSourceFlags::DHT) {
        ret |= 1 << 3;
    }
    if source.contains(PeerSourceFlags::PEX) {
        ret |= 1 << 2;
    }
    ret
}

/// The torrent-side inputs `request_a_block` reads (`t.*` upstream).
#[derive(Debug, Clone)]
pub struct TorrentRequestCtx {
    /// `t.is_seed()`.
    pub is_seed: bool,
    /// `t.upload_mode()`.
    pub upload_mode: bool,
    /// `t.valid_metadata()` — no requests before metadata.
    pub valid_metadata: bool,
    /// `t.are_files_checked()`.
    pub files_checked: bool,
    /// `t.graceful_pause()` — no requests while pausing.
    pub graceful_pause: bool,
    /// `t.num_time_critical_pieces() > 0`.
    pub time_critical_mode: bool,
    /// `t.num_peers()` — feeds the picker's partial-pieces cap.
    pub num_peers: i32,
    /// `settings.whole_pieces_threshold` (seconds; 0 disables).
    pub whole_pieces_threshold: i32,
    /// `settings.strict_end_game_mode`.
    pub strict_end_game_mode: bool,
    /// `torrent_file().piece_length()`.
    pub piece_length: i64,
    /// `t.block_size()`.
    pub block_size: i32,
}

/// The peer-connection-side inputs `request_a_block` reads (`c.*`).
#[derive(Debug, Clone)]
pub struct PeerRequestCtx<'a> {
    /// `c.no_download()`.
    pub no_download: bool,
    /// `c.is_disconnecting()`.
    pub disconnecting: bool,
    /// `c.desired_queue_size()` — pipeline target depth.
    pub desired_queue_size: i32,
    /// Blocks already requested (download queue + request queue),
    /// used to skip duplicates (`aux::has_block`).
    pub outstanding: &'a [PieceBlock],
    /// `c.prefer_contiguous_blocks()` (0 disables the picker hint;
    /// `whole_pieces_threshold` may still raise it).
    pub prefer_contiguous_blocks: i32,
    /// `statistics().download_payload_rate()` (B/s) for the
    /// contiguous-window computation.
    pub download_payload_rate: i64,
    /// `c.has_peer_choked()` — restrict picks to `allowed_fast`.
    pub peer_choked: bool,
    /// `c.allowed_fast()` — BEP-6 allowed pieces when choked.
    pub allowed_fast: &'a [bitdaemon_common::PieceIndex],
    /// `c.get_bitfield()` — pieces the peer has.
    pub peer_pieces: &'a Bitfield,
    /// `c.peer_info_struct()` — picker's peer identity key.
    pub peer_key: Option<u64>,
    /// `c.picker_options()`.
    pub picker_options: crate::piece_picker::PickerOptions,
    /// `c.suggested_pieces()` — BEP-6 suggestions.
    pub suggested_pieces: &'a [bitdaemon_common::PieceIndex],
}

/// `aux::request_a_block(t, c)` — pick blocks for one peer.
///
/// `add_request` receives each picked `PieceBlock` and a `busy` flag
/// (upstream `peer_connection::busy`); it must mark the block
/// downloading in the picker and queue it, returning false if it
/// couldn't (e.g. queue overflow). Returns `Some(endgame)` when the
/// picker was invoked, `None` for early-exit conditions.
pub fn request_a_block(
    picker: &mut PiecePicker,
    t: &TorrentRequestCtx,
    c: &PeerRequestCtx<'_>,
    pc: &mut dyn PickerStats,
    mut add_request: impl FnMut(PieceBlock, bool) -> bool,
) -> Option<bool> {
    if t.is_seed
        || c.no_download
        || t.upload_mode
        || c.disconnecting
        || !t.valid_metadata
        || !t.files_checked
        || t.graceful_pause
    {
        return None;
    }

    let desired_queue_size = c.desired_queue_size;
    let num_outstanding = c.outstanding.len() as i32;
    let mut num_requests = desired_queue_size - num_outstanding;
    if num_requests <= 0 {
        return None;
    }

    let mut prefer_contiguous_blocks = c.prefer_contiguous_blocks;

    if prefer_contiguous_blocks == 0 && !t.time_critical_mode && t.whole_pieces_threshold > 0 {
        // whole_pieces_threshold seconds of payload, capped at 8 MiB,
        // expressed in whole pieces then converted to blocks
        let download_window = c.download_payload_rate * i64::from(t.whole_pieces_threshold);
        let contiguous_pieces = download_window.min(8 * 1024 * 1024) / t.piece_length.max(1);
        let blocks_per_piece = t.piece_length / i64::from(t.block_size).max(1);
        prefer_contiguous_blocks = (contiguous_pieces * blocks_per_piece) as i32;
    }

    // if choked, only pick from the allowed-fast set
    let mut fast_mask = Bitfield::with_size(c.peer_pieces.size());
    let bits: &Bitfield = if c.peer_choked {
        for &i in c.allowed_fast {
            if c.peer_pieces.get_bit(i.0 as usize) {
                fast_mask.set_bit(i.0 as usize);
            }
        }
        &fast_mask
    } else {
        c.peer_pieces
    };

    let mut interesting_pieces = Vec::with_capacity(100);
    let _flags = picker.pick_pieces(
        bits,
        &mut interesting_pieces,
        num_requests,
        prefer_contiguous_blocks,
        c.peer_key,
        c.picker_options,
        c.suggested_pieces,
        t.num_peers,
        pc,
    );

    // still unrequested pieces => not strictly end-game yet; also
    // with outstanding requests we shouldn't pick busy blocks (unless
    // time-critical, which upstream accepts implicitly)
    let dont_pick_busy_blocks = (t.strict_end_game_mode
        && picker.get_download_queue_size() < picker.num_want_left() as usize)
        || num_outstanding > 0;

    let mut busy_block = PieceBlock::INVALID;

    for &pb in &interesting_pieces {
        if prefer_contiguous_blocks == 0 && num_requests <= 0 {
            break;
        }

        if picker.num_peers_block(pb) > 0 {
            // all following blocks are busy as well
            if num_requests <= 0 || dont_pick_busy_blocks {
                break;
            }
            busy_block = pb;
            continue;
        }

        // skip blocks already in our queues (timed-out / not_wanted
        // blocks aren't marked in the picker)
        if c.outstanding.contains(&pb) {
            continue;
        }

        if !add_request(pb, false) {
            continue;
        }
        num_requests -= 1;
    }

    if num_requests <= 0 {
        return Some(false);
    }

    // couldn't fill the queue => end-game (unless choked)
    let endgame = !c.peer_choked;

    // no busy block, or requests already outstanding => done
    if busy_block == PieceBlock::INVALID || num_outstanding > 0 {
        return Some(endgame);
    }

    add_request(busy_block, true);
    Some(endgame)
}

/// Convenience wrapper keeping upstream's `bool` early-exit shape and
/// exposing the resulting end-game flag.
pub fn request_a_block_out(
    picker: &mut PiecePicker,
    t: &TorrentRequestCtx,
    c: &PeerRequestCtx<'_>,
    pc: &mut dyn PickerStats,
    add_request: impl FnMut(PieceBlock, bool) -> bool,
) -> (bool, Option<bool>) {
    match request_a_block(picker, t, c, pc, add_request) {
        None => (false, None),
        Some(endgame) => (true, Some(endgame)),
    }
}
