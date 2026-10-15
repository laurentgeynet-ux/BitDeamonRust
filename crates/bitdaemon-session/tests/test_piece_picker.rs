// Conformance tests ported from libtorrent's `test_piece_picker.cpp`.

use bitdaemon_common::{Bitfield, DownloadPriority, PieceBlock, PieceIndex};
use bitdaemon_session::piece_picker::{PickerCounter, PickerOptions, PickerStats, PiecePicker};

const BLOCKS_PER_PIECE: i32 = 4;
const PIECE_SIZE: i32 = BLOCKS_PER_PIECE * 0x4000;

fn p(i: i32) -> PieceIndex {
    PieceIndex(i)
}

fn pb(piece: i32, block: i32) -> PieceBlock {
    PieceBlock {
        piece_index: PieceIndex(piece),
        block_index: block,
    }
}

struct Stats([u32; 16]);
impl PickerStats for Stats {
    fn inc(&mut self, c: PickerCounter) {
        self.0[c as usize] += 1;
    }
}

fn all_have(n: usize) -> Bitfield {
    Bitfield::with_value(n, true)
}

/// `test_pick` — basic rarest-first picking gets every block of the
/// chosen piece.
#[test]
fn pick() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::RAREST_FIRST,
        &[],
        100,
        &mut stats,
    );
    assert_eq!(picked.len() as i32, BLOCKS_PER_PIECE);
    // all blocks belong to one piece, indexes 0..3
    let piece = picked[0].piece_index;
    assert!(picked.iter().all(|b| b.piece_index == piece));
    let mut idx: Vec<i32> = picked.iter().map(|b| b.block_index).collect();
    idx.sort_unstable();
    assert_eq!(idx, vec![0, 1, 2, 3]);
    // pick_pieces only *suggests* blocks — the caller marks them
    // downloading afterwards
    assert!(!picker.is_downloading(piece));
}

/// `test_pick_whole_pieces` — asking for a lot of blocks picks
/// several whole pieces.
#[test]
fn pick_whole() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        3 * BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::RAREST_FIRST,
        &[],
        100,
        &mut stats,
    );
    assert_eq!(picked.len() as i32, 3 * BLOCKS_PER_PIECE);
}

/// filtered pieces (priority 0) are never picked.
#[test]
fn priorities() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);
    picker.set_piece_priority(p(2), DownloadPriority(0));
    assert_eq!(picker.piece_priority(p(2)), DownloadPriority(0));
    assert_eq!(picker.num_filtered(), 1);

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    // ask for everything: should get 6 pieces x 4 blocks, not 7x4
    picker.pick_pieces(
        &have,
        &mut picked,
        7 * BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::RAREST_FIRST,
        &[],
        100,
        &mut stats,
    );
    assert_eq!(picked.len() as i32, 6 * BLOCKS_PER_PIECE);
    assert!(!picked.iter().any(|b| b.piece_index == p(2)));
}

/// `test_partial_pieces` — with PRIORITIZE_PARTIALS a partially
/// requested piece is completed before new pieces are started.
#[test]
fn partial_pieces() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);

    // mark one block of piece 2 downloading
    assert!(picker.mark_as_downloading(pb(2, 1), Some(1), PickerOptions::NONE));

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        BLOCKS_PER_PIECE,
        0,
        Some(1),
        PickerOptions::RAREST_FIRST | PickerOptions::PRIORITIZE_PARTIALS,
        &[],
        100,
        &mut stats,
    );
    // remaining 3 blocks of piece 2 + 1 block of a new piece
    let p2: Vec<_> = picked.iter().filter(|b| b.piece_index == p(2)).collect();
    assert_eq!(p2.len(), 3);
    assert!(!p2.iter().any(|b| b.block_index == 1));
}

/// `test_block_states` — requested → writing → finished → piece
/// passed → flushed.
#[test]
fn block_states() {
    let mut picker = PiecePicker::new(i64::from(PIECE_SIZE), PIECE_SIZE);
    let have = all_have(1);
    picker.inc_refcount_bitfield(&have);

    for b in 0..BLOCKS_PER_PIECE {
        assert!(picker.mark_as_downloading(pb(0, b), Some(1), PickerOptions::NONE));
    }
    for b in 0..BLOCKS_PER_PIECE {
        assert!(picker.is_requested(pb(0, b)));
        assert!(picker.mark_as_writing(pb(0, b), Some(1)));
        assert!(picker.is_downloaded(pb(0, b)));
        assert!(!picker.is_finished_block(pb(0, b)));
    }
    for b in 0..BLOCKS_PER_PIECE {
        picker.mark_as_finished(pb(0, b), Some(1));
        assert!(picker.is_finished_block(pb(0, b)));
    }
    assert!(picker.is_piece_finished(0i32.into()));
    // hash check starts while blocks are on disk
    picker.started_hash_job(p(0));
    assert!(picker.is_hashing(p(0)));
    picker.completed_hash_job(p(0));
    picker.piece_passed(p(0));
    assert!(picker.is_piece_flushed(p(0)));
    assert!(picker.have_piece(p(0)));
    assert_eq!(picker.num_have(), 1);
    assert!(picker.is_finished());
    assert!(picker.is_seeding());
}

/// `test_abort_download` — duplicate requests decrement num_peers;
/// the block frees when the last requester aborts.
#[test]
fn abort_download() {
    let mut picker = PiecePicker::new(i64::from(PIECE_SIZE), PIECE_SIZE);
    let have = all_have(1);
    picker.inc_refcount_bitfield(&have);

    assert!(picker.mark_as_downloading(pb(0, 0), Some(1), PickerOptions::NONE));
    assert!(picker.mark_as_downloading(pb(0, 0), Some(2), PickerOptions::NONE));
    assert_eq!(picker.num_peers_block(pb(0, 0)), 2);

    picker.abort_download(pb(0, 0), Some(2));
    assert_eq!(picker.num_peers_block(pb(0, 0)), 1);
    assert!(picker.is_requested(pb(0, 0)));

    picker.abort_download(pb(0, 0), Some(1));
    assert_eq!(picker.num_peers_block(pb(0, 0)), 0);
    assert!(!picker.is_requested(pb(0, 0)));
    // piece went back to open
    assert_eq!(picker.get_download_queue_size(), 0);
}

/// `test_write_failed` + `restore_piece` — a failed write locks the
/// piece; restoring the failed blocks unlocks it.
#[test]
fn write_failed_restore() {
    let mut picker = PiecePicker::new(i64::from(PIECE_SIZE), PIECE_SIZE);
    let have = all_have(1);
    picker.inc_refcount_bitfield(&have);

    for b in 0..BLOCKS_PER_PIECE {
        assert!(picker.mark_as_downloading(pb(0, b), Some(1), PickerOptions::NONE));
        assert!(picker.mark_as_writing(pb(0, b), Some(1)));
    }
    picker.write_failed(pb(0, 2));
    let info = picker.piece_info(p(0));
    assert!(info.locked);

    // restore the failed block
    picker.restore_piece(p(0), &[2]);
    let info = picker.piece_info(p(0));
    assert!(!info.locked);
    // block 2 is free again, others still writing
    let down = picker.get_downloaders(p(0));
    assert_eq!(down.len(), BLOCKS_PER_PIECE as usize);
}

/// `test_sequential` — sequential pick takes the cursor pieces in
/// order.
#[test]
fn sequential() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        2 * BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::SEQUENTIAL,
        &[],
        100,
        &mut stats,
    );
    // first pickable pieces in order
    let mut pieces: Vec<i32> = picked.iter().map(|b| b.piece_index.0).collect();
    pieces.sort_unstable();
    pieces.dedup();
    assert_eq!(pieces, vec![0, 1]);
}

/// `test_reverse` — reverse sequential picks from the end.
#[test]
fn reverse() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::SEQUENTIAL | PickerOptions::REVERSE,
        &[],
        100,
        &mut stats,
    );
    assert_eq!(picked.len() as i32, BLOCKS_PER_PIECE);
    assert_eq!(picked[0].piece_index, p(6));
}

/// `test_suggested` — suggested pieces are picked first.
#[test]
fn suggested() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::RAREST_FIRST,
        &[p(4)],
        100,
        &mut stats,
    );
    assert!(picked.iter().any(|b| b.piece_index == p(4)));
}

/// `test_dont_pick_busy_blocks` — end-game re-requests blocks already
/// requested from *other* peers.
#[test]
fn end_game() {
    let mut picker = PiecePicker::new(i64::from(PIECE_SIZE), PIECE_SIZE);
    let have = all_have(1);
    picker.inc_refcount_bitfield(&have);

    // peer 1 requested all blocks
    for b in 0..BLOCKS_PER_PIECE {
        assert!(picker.mark_as_downloading(pb(0, b), Some(1), PickerOptions::NONE));
    }
    // piece is in the "full" queue
    let (partial, full, _, _) = picker.get_download_queue_sizes();
    assert_eq!(partial, 0);
    assert_eq!(full, 1);

    // peer 2 can only double-request
    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        1,
        0,
        Some(2),
        PickerOptions::RAREST_FIRST,
        &[],
        100,
        &mut stats,
    );
    assert_eq!(picked.len(), 1);
    assert_eq!(picked[0].piece_index, p(0));
    assert!(picker.num_peers_block(picked[0]) >= 1);
}

/// `test_clear_peer` — `clear_peer` forgets requesters without
/// freeing the requests.
#[test]
fn clear_peer() {
    let mut picker = PiecePicker::new(i64::from(PIECE_SIZE), PIECE_SIZE);
    let have = all_have(1);
    picker.inc_refcount_bitfield(&have);

    assert!(picker.mark_as_downloading(pb(0, 0), Some(1), PickerOptions::NONE));
    assert_eq!(picker.get_downloader(pb(0, 0)), Some(1));
    picker.clear_peer(1);
    assert_eq!(picker.get_downloader(pb(0, 0)), None);
    assert!(picker.is_requested(pb(0, 0)));
}

/// `test_cursor` — `set_sequential_range` moves the cursor pair.
#[test]
fn cursor() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    picker.set_sequential_range(p(3), p(5));
    assert_eq!(picker.cursor(), p(3));
    assert_eq!(picker.reverse_cursor(), p(6));
}

/// `test_piece_priorities` — rarest-first prefers low availability.
#[test]
fn rarest_first() {
    let mut picker = PiecePicker::new(i64::from(7 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(7);
    picker.inc_refcount_bitfield(&have);
    // peer 2 has only pieces 5 and 6 → they are rarer
    let mut bf2 = Bitfield::with_size(7);
    bf2.set_bit(5);
    bf2.set_bit(6);
    picker.inc_refcount_bitfield(&bf2);

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::RAREST_FIRST,
        &[],
        100,
        &mut stats,
    );
    // the rarest pieces are 0..4 (avail 1); any of them is correct,
    // but never 5 or 6 (avail 2)
    assert!((0..5).contains(&picked[0].piece_index.0));
}

/// `test_distributed_copies` — `(floor, fraction)`.
#[test]
fn distributed_copies() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    assert_eq!(picker.distributed_copies(), (0, 0));
    picker.inc_refcount_all();
    // one full copy
    assert_eq!(picker.distributed_copies(), (1, 0));
    // peer with only half the pieces → 1.5
    let mut bf = Bitfield::with_size(4);
    bf.set_bit(0);
    bf.set_bit(1);
    picker.inc_refcount_bitfield(&bf);
    let (full, frac) = picker.distributed_copies();
    assert_eq!(full, 1);
    assert!(frac > 0 && frac <= 1000);
}

/// want/have/all_pieces accounting.
#[test]
fn accounting() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    assert_eq!(picker.all_pieces().num_pieces, 4);
    assert_eq!(picker.have().num_pieces, 0);
    assert_eq!(picker.want().num_pieces, 4);

    picker.set_piece_priority(p(3), DownloadPriority(0));
    assert_eq!(picker.want().num_pieces, 3);

    picker.we_have_all();
    assert!(picker.is_seeding());
    picker.we_dont_have(p(0));
    assert!(!picker.is_seeding());
    assert_eq!(picker.num_have(), 3);
}

/// `test_inc_refcount` — seeds don't create pickable pressure the
/// same way; `break_one_seed` accounting stays consistent.
#[test]
fn seed_refcount() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    picker.inc_refcount_all();
    assert_eq!(picker.num_seeds(), 1);
    picker.dec_refcount_all();
    assert_eq!(picker.num_seeds(), 0);
}

/// `get_download_queue` reports the downloading-piece records.
#[test]
fn download_queue() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(4);
    picker.inc_refcount_bitfield(&have);

    assert!(picker.mark_as_downloading(pb(1, 0), Some(1), PickerOptions::NONE));
    assert!(picker.mark_as_downloading(pb(3, 2), Some(1), PickerOptions::NONE));
    let q = picker.get_download_queue();
    assert_eq!(q.len(), 2);
    assert!(q.iter().all(|dp| dp.requested == 1));
    assert_eq!(picker.get_download_queue_sizes().0, 2);
    assert_eq!(picker.piece_info(p(1)).index, p(1));
    assert!(picker.is_downloading(p(1)));
}

// ---------- request_a_block (aux_/request_blocks.cpp) ----------

use bitdaemon_session::request_blocks::{PeerRequestCtx, TorrentRequestCtx, request_a_block_out};

fn tctx() -> TorrentRequestCtx {
    TorrentRequestCtx {
        is_seed: false,
        upload_mode: false,
        valid_metadata: true,
        files_checked: true,
        graceful_pause: false,
        time_critical_mode: false,
        num_peers: 10,
        whole_pieces_threshold: 0,
        strict_end_game_mode: false,
        piece_length: i64::from(PIECE_SIZE),
        block_size: 0x4000,
    }
}

#[test]
fn request_block_basic() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(4);
    picker.inc_refcount_bitfield(&have);
    let t = tctx();
    let outstanding: Vec<PieceBlock> = Vec::new();
    let c = PeerRequestCtx {
        no_download: false,
        disconnecting: false,
        desired_queue_size: 8,
        outstanding: &outstanding,
        prefer_contiguous_blocks: 0,
        download_payload_rate: 0,
        peer_choked: false,
        allowed_fast: &[],
        peer_pieces: &have,
        peer_key: Some(1),
        picker_options: PickerOptions::RAREST_FIRST,
        suggested_pieces: &[],
    };
    let mut stats = Stats([0; 16]);
    let mut requested = Vec::new();
    let (invoked, _endgame) = request_a_block_out(&mut picker, &t, &c, &mut stats, |b, _busy| {
        requested.push(b);
        true
    });
    assert!(invoked);
    assert_eq!(requested.len(), 8);
    // upstream's peer_connection::add_request marks blocks in the
    // picker — do it after the call (borrow rules)
    for &b in &requested {
        assert!(picker.mark_as_downloading(b, Some(1), PickerOptions::NONE));
    }
    assert!(requested.iter().all(|b| picker.is_requested(*b)));
}

#[test]
fn request_block_early_exit() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(4);
    let mut t = tctx();
    t.is_seed = true;
    let outstanding: Vec<PieceBlock> = Vec::new();
    let c = PeerRequestCtx {
        no_download: false,
        disconnecting: false,
        desired_queue_size: 8,
        outstanding: &outstanding,
        prefer_contiguous_blocks: 0,
        download_payload_rate: 0,
        peer_choked: false,
        allowed_fast: &[],
        peer_pieces: &have,
        peer_key: None,
        picker_options: PickerOptions::NONE,
        suggested_pieces: &[],
    };
    let mut stats = Stats([0; 16]);
    let (invoked, _) = request_a_block_out(&mut picker, &t, &c, &mut stats, |_, _| true);
    assert!(!invoked);
}

#[test]
fn request_block_endgame() {
    // single piece fully requested by peer 1 → peer 2 gets a busy
    // block (end-game)
    let mut picker = PiecePicker::new(i64::from(PIECE_SIZE), PIECE_SIZE);
    let have = all_have(1);
    picker.inc_refcount_bitfield(&have);
    for b in 0..BLOCKS_PER_PIECE {
        assert!(picker.mark_as_downloading(pb(0, b), Some(1), PickerOptions::NONE));
    }
    let t = tctx();
    let outstanding: Vec<PieceBlock> = Vec::new();
    let c = PeerRequestCtx {
        no_download: false,
        disconnecting: false,
        desired_queue_size: 4,
        outstanding: &outstanding,
        prefer_contiguous_blocks: 0,
        download_payload_rate: 0,
        peer_choked: false,
        allowed_fast: &[],
        peer_pieces: &have,
        peer_key: Some(2),
        picker_options: PickerOptions::RAREST_FIRST,
        suggested_pieces: &[],
    };
    let mut stats = Stats([0; 16]);
    let mut busy = Vec::new();
    let (invoked, endgame) = request_a_block_out(&mut picker, &t, &c, &mut stats, |b, is_busy| {
        if is_busy {
            busy.push(b);
        }
        true
    });
    assert!(invoked);
    assert_eq!(endgame, Some(true));
    assert_eq!(busy.len(), 1);
    for &b in &busy {
        assert!(picker.mark_as_downloading(b, Some(2), PickerOptions::NONE));
        assert!(picker.is_requested(b));
    }
}

// ---------- peer_pipeline (peer_connection request pipeline) ----------

use bitdaemon_common::PeerRequest;
use bitdaemon_session::peer_pipeline::{
    PeerPipeline, PendingBlock, PipelineTorrent, RequestMerger,
};

struct FakeTorrent;
impl PipelineTorrent for FakeTorrent {
    fn block_size(&self) -> i32 {
        0x4000
    }
    fn piece_length(&self) -> i64 {
        i64::from(PIECE_SIZE)
    }
    fn piece_size_for_req(&self, _piece: PieceIndex) -> i32 {
        PIECE_SIZE
    }
    fn to_req(&self, block: PieceBlock, _piece_size: i32) -> PeerRequest {
        PeerRequest {
            piece: block.piece_index,
            start: block.block_index * 0x4000,
            length: 0x4000,
        }
    }
}

#[test]
fn merger_contiguous() {
    // blocks 0..3 of piece 0 then block 0 of piece 1 all merge
    let mut m = RequestMerger::new(pb(0, 0), PIECE_SIZE, i64::from(PIECE_SIZE), 0x4000);
    assert!(m.continues(pb(0, 1)));
    assert_eq!(m.merge(pb(0, 1), PIECE_SIZE), 0x4000);
    assert!(m.continues(pb(0, 2)));
    assert_eq!(m.merge(pb(0, 2), PIECE_SIZE), 0x4000);
    assert!(m.continues(pb(0, 3)));
    assert_eq!(m.merge(pb(0, 3), PIECE_SIZE), 0x4000);
    // crosses the piece boundary
    assert!(m.continues(pb(1, 0)));
    assert_eq!(m.merge(pb(1, 0), PIECE_SIZE), 0x4000);
    // gap → not contiguous
    assert!(!m.continues(pb(1, 2)));
}

#[test]
fn pipeline_drains_queue() {
    let t = FakeTorrent;
    let mut pl = PeerPipeline::new(3, 10, 250);
    for i in 0..4 {
        pl.request_queue.push(PendingBlock::new(pb(0, i)));
    }
    pl.request_queue.push(PendingBlock::new(pb(1, 0)));
    pl.request_queue.push(PendingBlock::new(pb(1, 1)));
    let reqs = pl.send_block_requests(&t, 0, 1000, false, |_| false);
    // desired_queue_size = 4 → only 4 blocks sent (first queue drain)
    assert_eq!(reqs.len(), 4);
    assert_eq!(pl.download_queue.len(), 4);
    assert_eq!(pl.request_queue.len(), 2);
    assert_eq!(pl.requested_ms, 1000);
}

#[test]
fn pipeline_merges_large() {
    let t = FakeTorrent;
    let mut pl = PeerPipeline::new(3, 10, 250);
    pl.request_large_blocks = true;
    pl.desired_queue_size = 8;
    for i in 0..4 {
        pl.request_queue.push(PendingBlock::new(pb(0, i)));
    }
    pl.request_queue.push(PendingBlock::new(pb(1, 0)));
    let reqs = pl.send_block_requests(&t, 0, 0, false, |_| false);
    // all 5 contiguous blocks merge into ONE request of 5*16k
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].length, 5 * 0x4000);
    assert_eq!(pl.download_queue.len(), 5);
    assert_eq!(pl.outstanding_bytes, 5 * 0x4000);
}

#[test]
fn pipeline_timeout() {
    let mut pl = PeerPipeline::new(3, 10, 250);
    // no samples → settings default
    assert_eq!(pl.request_timeout(), 10);
    pl.request_time.add_sample(1000);
    // 1 sample → avg + avg/5 = 1200 ms → ceil 2 s
    assert_eq!(pl.request_timeout(), 2);
    for _ in 0..5 {
        pl.request_time.add_sample(500);
    }
    // converged to ~500 ms avg → < 1 s → clamped to 2 s
    assert_eq!(pl.request_timeout(), 2);
}

#[test]
fn desired_queue_aimd() {
    let mut pl = PeerPipeline::new(3, 10, 250);
    // slow-start: +1 per piece
    pl.on_piece_received(0, 0, 0x4000);
    assert_eq!(pl.desired_queue_size, 5);
    // snubbed → 1
    pl.snubbed = true;
    assert_eq!(pl.desired_queue_size(), 1);
    pl.snubbed = false;
    pl.slow_start = false;
    // rate 100 kB/s * queue_time 3 s = 300 kB → ~18 blocks of 16 kB
    pl.update_desired_queue_size(100_000, 0x4000);
    assert_eq!(pl.desired_queue_size, 18);
    // clamped to max
    pl.update_desired_queue_size(10_000_000, 0x4000);
    assert_eq!(pl.desired_queue_size, 250);
}

// ---------- peer_conn (peer_connection assembled) ----------

use bitdaemon_session::peer_conn::PeerConn;

#[test]
fn peer_conn_drive() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(4);
    picker.inc_refcount_bitfield(&have);

    let mut conn = PeerConn::new(4, PeerPipeline::new(3, 10, 250), 0);
    conn.on_bitfield(&have);
    assert_eq!(conn.num_pieces, 4);
    assert!(conn.has_piece(p(2)));
    conn.on_unchoke(); // a choked peer picks only allowed-fast

    let t = tctx();
    let ft = FakeTorrent;
    let mut stats = Stats([0; 16]);
    let (invoked, _end, reqs, _queued) = conn.drive_requests(
        &mut picker,
        &t,
        &ft,
        PickerOptions::RAREST_FIRST,
        0,
        0,
        0,
        &bitdaemon_session::counters::Counters::default(),
        &mut stats,
    );
    assert!(invoked);
    // desired_queue_size = 4 → 4 wire requests, marked in picker
    assert_eq!(reqs.len(), 4);
    assert!(reqs.iter().all(|r| picker.is_requested(PieceBlock {
        piece_index: r.piece,
        block_index: r.start / 0x4000,
    })));
}

#[test]
fn peer_conn_suggest_have() {
    let mut conn = PeerConn::new(4, PeerPipeline::new(3, 10, 250), 0);
    // upstream `incoming_suggest`: no dedup, front-insert (newest
    // = highest priority), capped at `max_suggest_pieces`.
    conn.on_suggest(p(3), 16);
    conn.on_suggest(p(3), 16);
    conn.on_suggest(p(1), 16);
    assert_eq!(conn.suggested_pieces, vec![p(1), p(3), p(3)]);
    // upstream caps loosely: `size > max` trims to `max-1` *before*
    // the insert, so the list oscillates `max`/`max+1` — after an
    // even number of over-cap inserts it settles on `max+1`.
    for i in 0..20 {
        conn.on_suggest(PieceIndex(i + 100), 4);
    }
    assert_eq!(conn.suggested_pieces.len(), 5);

    // on_have marks the bit and reports interestingness
    let we_have = |i: PieceIndex| i == p(0);
    assert!(conn.on_have(p(1), we_have)); // we don't have 1 → interesting
    assert!(conn.interesting);
    assert!(!conn.on_have(p(0), we_have)); // we have it
    assert!(!conn.on_have(p(1), we_have)); // already had the bit
}

// ---------- smart_ban ----------

use bitdaemon_common::hash::Sha1Hash;
use bitdaemon_session::smart_ban::{BanDecision, SmartBan};

#[test]
fn smart_ban_differing_crc() {
    let mut sb = SmartBan::new();
    let a: std::net::IpAddr = "1.1.1.1".parse().unwrap();
    let good = Sha1Hash::from_bytes(&[1u8; 20]).unwrap();
    let bad = Sha1Hash::from_bytes(&[2u8; 20]).unwrap();
    let pb = PieceBlock {
        piece_index: PieceIndex(0),
        block_index: 0,
    };
    // first failure records the digest, no ban
    assert_eq!(sb.record_failed_block(pb, 42, a, bad), BanDecision::None);
    // same peer, different digest on second failure → ban
    assert_eq!(
        sb.record_failed_block(pb, 42, a, good),
        BanDecision::Ban(42, a)
    );
    // other peer's differing digest just records its own entry
    assert_eq!(sb.record_failed_block(pb, 7, a, good), BanDecision::None);
}

#[test]
fn smart_ban_on_piece_passed() {
    let mut sb = SmartBan::new();
    let a: std::net::IpAddr = "1.1.1.1".parse().unwrap();
    let bad = Sha1Hash::from_bytes(&[9u8; 20]).unwrap();
    let good = Sha1Hash::from_bytes(&[1u8; 20]).unwrap();
    sb.record_failed_block(
        PieceBlock {
            piece_index: PieceIndex(3),
            block_index: 1,
        },
        5,
        a,
        bad,
    );
    // piece now passes; block digest is `good` → the bad sender banned
    let bans = sb.on_piece_passed(PieceIndex(3), 32 * 1024, |_| Some(good));
    assert_eq!(bans, vec![BanDecision::Ban(5, a)]);
    // erase_peers drops entries
    sb.record_failed_block(
        PieceBlock {
            piece_index: PieceIndex(4),
            block_index: 0,
        },
        8,
        a,
        bad,
    );
    sb.erase_peers(&[8]);
    assert!(
        sb.on_piece_passed(PieceIndex(4), 16 * 1024, |_| Some(good))
            .is_empty()
    );
}

/// A13 regression — `update_piece_state` must re-cascade the priority
/// index when a queue move changes `priority()` without crossing -1
/// (upstream `piece_picker.cpp:3048-3052` calls `update(prio, p.index)`
//  whenever the old prio was >= 0, not just on the -1 -> >=0 edge).
/// A piece going Downloading -> Full must leave `m_pieces`; otherwise
/// the stale entry duplicates it in the index and `pick_pieces`
/// hands the same blocks out twice once the piece re-opens.
#[test]
fn full_piece_readded_once() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(4);
    picker.inc_refcount_bitfield(&have);

    // drive piece 0 to `piece_full` (every block requested)
    for b in 0..BLOCKS_PER_PIECE {
        assert!(picker.mark_as_downloading(pb(0, b), Some(1), PickerOptions::NONE));
    }
    assert_eq!(picker.piece_stats(p(0)).priority, -1);

    // abort every request -> downloading_piece erased, piece re-added
    // to the priority index exactly once
    for b in 0..BLOCKS_PER_PIECE {
        picker.abort_download(pb(0, b), Some(1));
    }
    assert!(!picker.is_downloading(p(0)));

    let mut picked = Vec::new();
    let mut stats = Stats([0; 16]);
    picker.pick_pieces(
        &have,
        &mut picked,
        4 * BLOCKS_PER_PIECE,
        0,
        None,
        PickerOptions::RAREST_FIRST,
        &[],
        100,
        &mut stats,
    );
    assert_eq!(picked.len() as i32, 4 * BLOCKS_PER_PIECE);
    let mut sorted = picked.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), picked.len(), "duplicate picks");
}

/// A13 regression — `recalc_share_mode` consumes `want().num_pieces`;
/// pieces both had and filtered must not count as wanted (upstream
/// `piece_picker::want()` = num_pieces - filtered - have_filtered).
#[test]
fn want_excludes_have_filtered() {
    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(4);
    picker.inc_refcount_bitfield(&have);

    // have piece 0, then filter it: it moves to num_have_filtered
    picker.piece_flushed(p(0));
    assert!(picker.set_piece_priority(p(0), DownloadPriority(0)));
    assert_eq!(picker.num_have_filtered(), 1);
    assert_eq!(picker.num_filtered(), 0);
    assert_eq!(picker.want().num_pieces, 3);
}

/// A13 regression — `PickerStats` for `&Counters` lands the picker
/// loop counters in the session bank (upstream `ses.stats_counters()`).
#[test]
fn picker_counters_wired() {
    use bitdaemon_session::counters::{Counter, Counters};

    let mut picker = PiecePicker::new(i64::from(4 * PIECE_SIZE), PIECE_SIZE);
    let have = all_have(4);
    picker.inc_refcount_bitfield(&have);

    let counters = Counters::default();
    let mut picked = Vec::new();
    {
        let mut pc = &counters;
        picker.pick_pieces(
            &have,
            &mut picked,
            BLOCKS_PER_PIECE,
            0,
            None,
            PickerOptions::RAREST_FIRST,
            &[],
            100,
            &mut pc,
        );
    }
    assert!(counters.value(Counter::PiecePickerRareLoops) > 0);
}
