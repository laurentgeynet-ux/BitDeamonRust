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
