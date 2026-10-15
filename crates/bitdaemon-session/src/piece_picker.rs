//! Port of `aux_/piece_picker.hpp` + `piece_picker.cpp` — the piece
//! picking engine: per-piece availability refcounting, packed
//! `piece_pos` state, priority-bucket index (`m_pieces` +
//! `m_priority_boundaries`), per-block `block_info` bookkeeping across
//! four sorted download queues, and the `pick_pieces` algorithm
//! (partials, suggested, sequential, rarest-first, random, end-game).
//!
//! Upstream `aux::torrent_peer*` identities become `Option<u64>`
//! peer keys (e.g. an index into the session's peer table). C++
//! iterators into the sorted `m_downloads` vectors are replaced by
//! `(queue, position)` pairs — `update_piece_state` may move a piece
//! between queues and returns its new position.
//!
//! `pick_pieces` is `&mut self` where upstream is `const` + `mutable`
//! members (`update_pieces`, `m_recent_extents`).

use std::collections::HashMap;

use bitdaemon_common::random::random;
use bitdaemon_common::{Bitfield, DownloadPriority, PieceBlock, PieceIndex};

/// `default_block_size` (`disk_interface.hpp`).
pub const BLOCK_SIZE: i32 = 0x4000;

/// `max_piece_affinity_extent` — 4 MiB worth of blocks.
const MAX_PIECE_AFFINITY_EXTENT: i32 = 4 * 1024 * 1024 / BLOCK_SIZE;

/// `download_queue_t` — which `m_downloads` list a piece sits in.
/// Values match upstream's 3-bit field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DownloadQueue {
    /// `piece_downloading` (0) — some blocks outstanding.
    Downloading = 0,
    /// `piece_full` (1) — all blocks requested/finished, some
    /// requested.
    Full = 1,
    /// `piece_finished` (2) — all blocks written, none outstanding.
    Finished = 2,
    /// `piece_zero_prio` (3) — filtered while downloading.
    ZeroPrio = 3,
    /// `piece_open` (4) — not in any queue.
    Open = 4,
    /// `piece_downloading_reverse` (5).
    DownloadingReverse = 5,
    /// `piece_full_reverse` (6).
    FullReverse = 6,
}

/// `num_download_categories` — the four real queues.
pub const NUM_DOWNLOAD_QUEUES: usize = 4;

impl DownloadQueue {
    /// `download_queue()` — collapse the reverse variants.
    #[must_use]
    pub fn queue(self) -> DownloadQueue {
        match self {
            Self::DownloadingReverse => Self::Downloading,
            Self::FullReverse => Self::Full,
            q => q,
        }
    }

    /// `reverse()`.
    #[must_use]
    pub fn is_reverse(self) -> bool {
        matches!(self, Self::DownloadingReverse | Self::FullReverse)
    }

    /// `unreverse()`.
    #[must_use]
    pub fn unreverse(self) -> Self {
        match self {
            Self::DownloadingReverse => Self::Downloading,
            Self::FullReverse => Self::Full,
            q => q,
        }
    }

    /// `make_reverse()`.
    #[must_use]
    pub fn make_reverse(self) -> Self {
        match self {
            Self::Downloading => Self::DownloadingReverse,
            Self::Full => Self::FullReverse,
            q => q,
        }
    }

    /// `downloading()` — `state != piece_open`.
    #[must_use]
    pub fn is_downloading(self) -> bool {
        self != Self::Open
    }
}

/// `piece_pos` — per-piece packed entry in `m_piece_map`.
#[derive(Debug, Clone)]
struct PiecePos {
    /// `peer_count:26`.
    peer_count: u32,
    /// `download_state:3`.
    state: DownloadQueue,
    /// `piece_priority:3` — `DownloadPriority` 0-7; 0 = filtered.
    piece_priority: u8,
    /// `index` — position in `m_pieces`, or `FLUSHED_INDEX`.
    index: i32,
}

/// `prio_index_t::flushed_index`.
const FLUSHED_INDEX: i32 = -1;

/// `max_peer_count`.
const MAX_PEER_COUNT: u32 = 0xffff;

/// `priority_levels` / `prio_factor`.
const PRIORITY_LEVELS: i32 = 8;
const PRIO_FACTOR: i32 = 3;

impl PiecePos {
    fn new() -> Self {
        Self {
            peer_count: 0,
            state: DownloadQueue::Open,
            piece_priority: DownloadPriority::DEFAULT.0,
            index: 0,
        }
    }

    /// `filtered()` — `piece_priority == filter_priority` (0).
    fn filtered(&self) -> bool {
        self.piece_priority == 0
    }

    /// `flushed()` — `index == flushed_index`.
    fn flushed(&self) -> bool {
        self.index == FLUSHED_INDEX
    }

    fn set_flushed(&mut self) {
        self.index = FLUSHED_INDEX;
    }

    fn set_not_flushed(&mut self) {
        self.index = 0;
    }

    fn downloading(&self) -> bool {
        self.state.is_downloading()
    }

    /// `priority(picker)` — the bucket priority, or -1 (filtered,
    /// flushed, no peers, or already have it).
    fn priority(&self, seeds: i32) -> i32 {
        if self.filtered()
            || self.flushed()
            || self.peer_count as i64 + i64::from(seeds) == 0
            || self.state == DownloadQueue::Full
            || self.state == DownloadQueue::Finished
        {
            return -1;
        }
        debug_assert!(self.piece_priority > 0);

        let adjustment = if self.state.is_reverse() {
            -1
        } else if self.state != DownloadQueue::Open {
            -3
        } else {
            -2
        };

        let availability = i32::try_from(self.peer_count).unwrap_or(i32::MAX) + 1;
        debug_assert!(availability > 0);
        availability * (PRIORITY_LEVELS - i32::from(self.piece_priority)) * PRIO_FACTOR + adjustment
    }
}

/// `block_info` — per-block record inside a downloading piece.
#[derive(Debug, Clone, Default)]
pub struct BlockInfo {
    /// `peer` — the peer this block was last requested from
    /// (parole / downloader attribution).
    pub peer: Option<u64>,
    /// `num_peers:14` — outstanding request count for this block.
    pub num_peers: u16,
    /// `state:2`.
    pub state: BlockState,
}

/// `block_info::state_*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockState {
    /// `state_none` — free to request.
    #[default]
    None = 0,
    /// `state_requested` — requested from at least one peer.
    Requested = 1,
    /// `state_writing` — received, being written to disk.
    Writing = 2,
    /// `state_finished` — on disk.
    Finished = 3,
}

/// `downloading_piece` — the per-piece download state living in one of
/// the four `m_downloads` queues (kept sorted by `index`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DownloadingPiece {
    /// `index` — the piece.
    pub index: PieceIndex,
    /// `info_idx` — slot into `m_block_info` (`* blocks_per_piece`).
    pub info_idx: u16,
    /// `finished:15` — blocks written to disk.
    pub finished: u16,
    /// `passed_hash_check:1`.
    pub passed_hash_check: bool,
    /// `writing:15` — blocks currently being written.
    pub writing: u16,
    /// `locked:1` — no more picks until `restore_piece`.
    pub locked: bool,
    /// `requested:15` — blocks currently requested.
    pub requested: u16,
    /// `hashing:1` — hash job outstanding.
    pub hashing: bool,
}

impl DownloadingPiece {
    fn new(index: PieceIndex) -> Self {
        Self {
            index,
            info_idx: u16::MAX,
            finished: 0,
            passed_hash_check: false,
            writing: 0,
            locked: false,
            requested: 0,
            hashing: false,
        }
    }
}

/// `picker_options_t` bits (`piece_picker.hpp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PickerOptions(u16);

impl PickerOptions {
    /// `rarest_first` (0_bit).
    pub const RAREST_FIRST: Self = Self(1);
    /// `reverse` (1_bit).
    pub const REVERSE: Self = Self(2);
    /// `on_parole` (2_bit).
    pub const ON_PAROLE: Self = Self(4);
    /// `prioritize_partials` (3_bit).
    pub const PRIORITIZE_PARTIALS: Self = Self(8);
    /// `sequential` (4_bit).
    pub const SEQUENTIAL: Self = Self(16);
    /// `time_critical` (5_bit).
    pub const TIME_CRITICAL: Self = Self(32);
    /// `align_expanded_pieces` (6_bit).
    pub const ALIGN_EXPANDED_PIECES: Self = Self(64);
    /// `piece_extent_affinity` (7_bit).
    pub const PIECE_EXTENT_AFFINITY: Self = Self(128);
    /// No options.
    pub const NONE: Self = Self(0);

    /// `options & flag`.
    #[must_use]
    pub fn contains(self, f: Self) -> bool {
        self.0 & f.0 != 0
    }

    /// `options |= flag`.
    pub fn set(&mut self, f: Self) {
        self.0 |= f.0;
    }
}

impl std::ops::BitOr for PickerOptions {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// `peer_connection::picker_options()` — the torrent-level legs:
/// `sequential` XOR (below `initial_picker_threshold` →
/// `prioritize_partials`, deliberately *not* rarest-first — upstream
/// random-picks while the first pieces complete) XOR `rarest_first`;
/// `prioritize_partials` is OR'd on top when the
/// `prioritize_partial_pieces` setting is on. `rarest_first` and
/// `sequential` are mutually exclusive upstream (asserted there).
/// The per-peer legs — `snubbed` → `reverse` else
/// `piece_extent_affinity`, and `on_parole` — need conn state and
/// live in `PeerConn::picker_options`.
#[must_use]
pub fn picker_options(
    sequential_download: bool,
    num_have: i32,
    initial_picker_threshold: i32,
    prioritize_partial_pieces: bool,
) -> PickerOptions {
    let mut ret = if sequential_download {
        PickerOptions::SEQUENTIAL
    } else if num_have < initial_picker_threshold {
        PickerOptions::PRIORITIZE_PARTIALS
    } else {
        PickerOptions::RAREST_FIRST
    };
    if prioritize_partial_pieces {
        ret.set(PickerOptions::PRIORITIZE_PARTIALS);
    }
    ret
}

/// `picker_flags_t` (`alert_types.hpp`) — which picker paths were
/// taken; surfaced to the picker log alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PickerFlags(pub u32);

impl PickerFlags {
    /// `partial_ratio` (0_bit).
    pub const PARTIAL_RATIO: Self = Self(1);
    /// `prioritize_partials` (1_bit).
    pub const PRIORITIZE_PARTIALS: Self = Self(2);
    /// `rarest_first_partials` (2_bit).
    pub const RAREST_FIRST_PARTIALS: Self = Self(4);
    /// `rarest_first` (3_bit).
    pub const RAREST_FIRST: Self = Self(8);
    /// `reverse_rarest_first` (4_bit).
    pub const REVERSE_RAREST_FIRST: Self = Self(16);
    /// `suggested_pieces` (5_bit).
    pub const SUGGESTED_PIECES: Self = Self(32);
    /// `prio_sequential_pieces` (6_bit).
    pub const PRIO_SEQUENTIAL_PIECES: Self = Self(64);
    /// `sequential_pieces` (7_bit).
    pub const SEQUENTIAL_PIECES: Self = Self(128);
    /// `reverse_pieces` (8_bit).
    pub const REVERSE_PIECES: Self = Self(256);
    /// `time_critical` (9_bit).
    pub const TIME_CRITICAL: Self = Self(512);
    /// `random_pieces` (10_bit).
    pub const RANDOM_PIECES: Self = Self(1024);
    /// `prefer_contiguous` (11_bit).
    pub const PREFER_CONTIGUOUS: Self = Self(2048);
    /// `reverse_sequential` (12_bit).
    pub const REVERSE_SEQUENTIAL: Self = Self(4096);
    /// `backup1` (13_bit).
    pub const BACKUP1: Self = Self(8192);
    /// `backup2` (14_bit).
    pub const BACKUP2: Self = Self(16384);
    /// `end_game` (15_bit).
    pub const END_GAME: Self = Self(32768);
    /// `extent_affinity` (16_bit).
    pub const EXTENT_AFFINITY: Self = Self(65536);

    /// `flags & f`.
    #[must_use]
    pub fn contains(self, f: Self) -> bool {
        self.0 & f.0 != 0
    }
}

impl std::ops::BitOr for PickerFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for PickerFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// `piece_picker::piece_count`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PieceCount {
    /// `num_pieces`.
    pub num_pieces: i32,
    /// `pad_bytes`.
    pub pad_bytes: i64,
    /// `last_piece` — whether the last piece is included.
    pub last_piece: bool,
}

/// `piece_picker::piece_stats_t`.
#[derive(Debug, Clone, Copy)]
pub struct PieceStats {
    /// `peer_count` (including seeds).
    pub peer_count: i32,
    /// `priority` — bucket index, -1 if unpickable.
    pub priority: i32,
    /// `flushed` — fully on disk.
    pub flushed: bool,
    /// `downloading` — in one of the download queues.
    pub downloading: bool,
}

/// `counters` subset the picker bumps (`piece_picker_*_loops`).
pub trait PickerStats {
    /// `inc_stats_counter(c)`.
    fn inc(&mut self, c: PickerCounter);
}

/// No-op counters — sessions without a stats sink.
#[derive(Debug, Default)]
pub struct NullPickerStats;
impl PickerStats for NullPickerStats {
    fn inc(&mut self, _c: PickerCounter) {}
}

/// The counters `pick_pieces` bumps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerCounter {
    /// `piece_picker_partial_loops`.
    PartialLoops,
    /// `piece_picker_suggest_loops`.
    SuggestLoops,
    /// `piece_picker_sequential_loops`.
    SequentialLoops,
    /// `piece_picker_reverse_sequential_loops`.
    ReverseSequentialLoops,
    /// `piece_picker_rand_start_loops`.
    RandStartLoops,
    /// `piece_picker_rand_loops`.
    RandLoops,
    /// `piece_picker_busy_loops`.
    BusyLoops,
    /// `piece_picker_break_loops`.
    BreakLoops,
    /// `piece_picker_rejected_loops`.
    RejectedLoops,
    /// `piece_picker_queued_loops`.
    QueuedLoops,
    /// `piece_picker_flood_loops`.
    FloodLoops,
    /// `piece_picker_reverse_loops`.
    ReverseLoops,
    /// `piece_picker_reverse_rand_loops`.
    ReverseRandLoops,
    /// `piece_picker_rare_loops`.
    RareLoops,
    /// `piece_picker_extd_loops`.
    ExtdLoops,
    /// `piece_picker_partial_piece_loops`.
    PartialPieceLoops,
    /// `piece_picker_reverse_rare_loops`.
    ReverseRareLoops,
}

/// `piece_extent_t` — extent index for `piece_extent_affinity`.
type PieceExtent = i32;

/// `aux::piece_picker`. `P` is the peer identity type — upstream's
/// `aux::torrent_peer*`. Callers pass `Option<u64>` peer keys (e.g. a
/// peer-list slot); `None` = "no peer" (upstream `nullptr`).
pub struct PiecePicker {
    /// `m_piece_map`.
    piece_map: Vec<PiecePos>,
    /// `m_pads_in_piece`.
    pads_in_piece: HashMap<i32, i32>,
    /// `m_recent_extents`.
    recent_extents: Vec<PieceExtent>,
    /// `m_num_pad_bytes`.
    num_pad_bytes: i64,
    /// `m_have_pad_bytes`.
    have_pad_bytes: i64,
    /// `m_filtered_pad_bytes`.
    filtered_pad_bytes: i64,
    /// `m_have_filtered_pad_bytes`.
    have_filtered_pad_bytes: i64,
    /// `m_seeds`.
    seeds: i32,
    /// `m_pieces` — piece indices ordered by computed priority bucket.
    pieces: Vec<PieceIndex>,
    /// `m_priority_boundaries` — end index per bucket.
    priority_boundaries: Vec<i32>,
    /// `m_downloads[4]` — sorted `downloading_piece` vecs per
    /// `DownloadQueue::{Downloading,Full,Finished,ZeroPrio}`.
    downloads: [Vec<DownloadingPiece>; NUM_DOWNLOAD_QUEUES],
    /// `m_block_info` — `blocks_per_piece`-aligned array.
    block_info: Vec<BlockInfo>,
    /// `m_free_block_infos` — free `info_idx` slots.
    free_block_infos: Vec<u16>,
    /// `m_blocks_in_last_piece`.
    blocks_in_last_piece: u16,
    /// `m_piece_size`.
    piece_size: i32,
    /// `m_total_size`.
    total_size: i64,
    /// `m_num_filtered`.
    num_filtered: i32,
    /// `m_num_have_filtered`.
    num_have_filtered: i32,
    /// `m_cursor`.
    cursor: i32,
    /// `m_reverse_cursor` — one past the end (counting down).
    reverse_cursor: i32,
    /// `m_num_have`.
    num_have: i32,
    /// `m_dirty` — `m_pieces`/`m_priority_boundaries` need rebuilding.
    dirty: bool,
}

impl PiecePicker {
    /// `piece_picker(total_size, piece_size)`.
    #[must_use]
    pub fn new(total_size: i64, piece_size: i32) -> Self {
        assert!(total_size > 0);
        assert!(piece_size > 0);
        let mut p = Self {
            piece_map: Vec::new(),
            pads_in_piece: HashMap::new(),
            recent_extents: Vec::new(),
            num_pad_bytes: 0,
            have_pad_bytes: 0,
            filtered_pad_bytes: 0,
            have_filtered_pad_bytes: 0,
            seeds: 0,
            pieces: Vec::new(),
            // one boundary at end (all pieces share bucket 0 initially)
            priority_boundaries: vec![i32::MAX],
            downloads: [Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            block_info: Vec::new(),
            free_block_infos: Vec::new(),
            blocks_in_last_piece: 0,
            piece_size: 0,
            total_size: 0,
            num_filtered: 0,
            num_have_filtered: 0,
            cursor: 0,
            reverse_cursor: 0,
            num_have: 0,
            dirty: false,
        };
        p.resize(total_size, piece_size);
        p
    }

    /// `resize(total_size, piece_size)` — (re)initialise the piece map.
    /// Keeps flushed/filtered markers, resets counters per upstream.
    pub fn resize(&mut self, total_size: i64, piece_size: i32) {
        assert!(total_size > 0);
        assert!(piece_size > 0);

        self.total_size = total_size;
        self.piece_size = piece_size;

        let blocks_in_piece = (piece_size + self.block_size() - 1) / self.block_size();
        let blocks_in_last = ((total_size % i64::from(piece_size)) as i32 + self.block_size() - 1)
            / self.block_size();
        let num_pieces = ((total_size + i64::from(piece_size) - 1) / i64::from(piece_size)) as i32;

        // max_blocks_per_piece = (1 << 15) - 1
        assert!(blocks_in_piece < (1 << 15), "invalid piece size");

        self.piece_map
            .resize_with(num_pieces as usize, PiecePos::new);
        self.piece_map.truncate(num_pieces as usize);
        self.reverse_cursor = num_pieces;
        self.cursor = 0;

        for q in &mut self.downloads {
            q.clear();
        }
        self.block_info.clear();
        self.free_block_infos.clear();

        self.num_filtered += self.num_have_filtered;
        self.num_have_filtered = 0;
        self.num_have = 0;
        self.have_pad_bytes = 0;
        self.filtered_pad_bytes += self.have_filtered_pad_bytes;
        self.have_filtered_pad_bytes = 0;
        self.dirty = true;
        for m in &mut self.piece_map {
            m.peer_count = 0;
            m.state = DownloadQueue::Open;
            m.index = 0;
        }

        while (self.cursor as usize) < self.piece_map.len()
            && (self.piece_map[self.cursor as usize].flushed()
                || self.piece_map[self.cursor as usize].filtered())
        {
            self.cursor += 1;
        }

        while self.reverse_cursor > 0
            && (self.piece_map[self.reverse_cursor as usize - 1].flushed()
                || self.piece_map[self.reverse_cursor as usize - 1].filtered())
        {
            self.reverse_cursor -= 1;
        }

        self.blocks_in_last_piece = blocks_in_last.max(0) as u16;
        if self.blocks_in_last_piece == 0 {
            self.blocks_in_last_piece = self.blocks_per_piece() as u16;
        }
        debug_assert!(self.blocks_in_last_piece <= self.blocks_per_piece() as u16);
    }

    // ---------- geometry ----------

    /// `num_pieces()`.
    #[must_use]
    pub fn num_pieces(&self) -> i32 {
        self.piece_map.len() as i32
    }

    /// `block_size()` — `min(piece_size, default_block_size)`.
    #[must_use]
    pub fn block_size(&self) -> i32 {
        BLOCK_SIZE.min(self.piece_size)
    }

    /// `blocks_per_piece()`.
    #[must_use]
    pub fn blocks_per_piece(&self) -> i32 {
        let ret = (self.piece_size + self.block_size() - 1) / self.block_size();
        debug_assert!(ret > 0);
        ret
    }

    /// `blocks_in_piece(index)` — last piece may be short.
    #[must_use]
    pub fn blocks_in_piece(&self, index: PieceIndex) -> i32 {
        if index.0 + 1 == self.num_pieces() {
            i32::from(self.blocks_in_last_piece)
        } else {
            self.blocks_per_piece()
        }
    }

    /// `payload_blocks_in_piece(index)` — minus whole-block pad.
    #[must_use]
    pub fn payload_blocks_in_piece(&self, index: PieceIndex) -> i32 {
        self.blocks_in_piece(index) - self.pad_bytes_in_piece(index) / self.block_size()
    }

    /// `piece_size(p)` — last piece may be short.
    #[must_use]
    pub fn piece_size(&self, p: PieceIndex) -> i32 {
        let pieces = self.num_pieces();
        if p.0 != pieces - 1 {
            return self.piece_size;
        }
        (self.total_size - i64::from(pieces - 1) * i64::from(self.piece_size)) as i32
    }

    /// `pad_bytes_in_piece(index)`.
    #[must_use]
    pub fn pad_bytes_in_piece(&self, index: PieceIndex) -> i32 {
        self.pads_in_piece.get(&index.0).copied().unwrap_or(0)
    }

    /// `num_pad_bytes()`.
    #[must_use]
    pub fn num_pad_bytes(&self) -> i64 {
        self.num_pad_bytes
    }

    /// `cursor()`.
    #[must_use]
    pub fn cursor(&self) -> PieceIndex {
        PieceIndex(self.cursor)
    }

    /// `reverse_cursor()`.
    #[must_use]
    pub fn reverse_cursor(&self) -> PieceIndex {
        PieceIndex(self.reverse_cursor)
    }

    /// `set_sequential_range(first, last)` — bound the sequential
    /// cursor window to `[first, last]`.
    pub fn set_sequential_range(&mut self, first_piece: PieceIndex, last_piece: PieceIndex) {
        assert!(first_piece.0 >= 0);
        assert!(last_piece.0 >= first_piece.0);
        assert!(last_piece.0 < self.num_pieces());
        self.cursor = first_piece.0;
        self.reverse_cursor = last_piece.0 + 1;
        while (self.cursor as usize) < self.piece_map.len()
            && (self.piece_map[self.cursor as usize].flushed()
                || self.piece_map[self.cursor as usize].filtered())
        {
            self.cursor += 1;
        }
        while self.reverse_cursor > 0
            && (self.piece_map[self.reverse_cursor as usize - 1].flushed()
                || self.piece_map[self.reverse_cursor as usize - 1].filtered())
        {
            self.reverse_cursor -= 1;
        }
    }

    // ---------- availability / refcount ----------

    /// `get_availability(piece)`.
    #[must_use]
    pub fn get_availability(&self, piece: PieceIndex) -> i32 {
        self.piece_map[piece.0 as usize].peer_count as i32 + self.seeds
    }

    /// `get_availability(avail)` — fill the whole vector.
    pub fn get_availability_vec(&self, avail: &mut Vec<i32>) {
        avail.clear();
        avail.extend(
            self.piece_map
                .iter()
                .map(|p| p.peer_count as i32 + self.seeds),
        );
    }

    /// `distributed_copies()` — `(min availability, fraction*1000)`.
    #[must_use]
    pub fn distributed_copies(&self) -> (i32, i32) {
        let npieces = self.num_pieces();
        if npieces == 0 {
            return (1, 0);
        }
        let mut min_availability = MAX_PEER_COUNT as i32;
        let mut integer_part = 0i32;
        let mut fraction_part = 0i32;
        for p in &self.piece_map {
            let mut peer_count = p.peer_count as i32;
            if p.flushed() {
                peer_count += 1;
            }
            if min_availability > peer_count {
                min_availability = peer_count;
                fraction_part += integer_part;
                integer_part = 1;
            } else if peer_count == min_availability {
                integer_part += 1;
            } else {
                fraction_part += 1;
            }
        }
        (
            min_availability + self.seeds,
            fraction_part * 1000 / npieces,
        )
    }

    /// `inc_refcount(index, peer)` — peer now has this piece.
    pub fn inc_refcount(&mut self, index: PieceIndex) {
        let prev_priority = self.piece_map[index.0 as usize].priority(self.seeds);
        let p = &mut self.piece_map[index.0 as usize];
        p.peer_count += 1;
        if self.dirty {
            return;
        }
        let new_priority = p.priority(self.seeds);
        if prev_priority == new_priority {
            return;
        }
        if prev_priority == -1 {
            self.add(index);
        } else {
            let elem = p.index;
            self.update(prev_priority as usize, elem);
        }
    }

    /// `dec_refcount(index, peer)` — peer no longer has this piece.
    pub fn dec_refcount(&mut self, index: PieceIndex) {
        if self.piece_map[index.0 as usize].peer_count == 0 {
            debug_assert!(self.seeds > 0);
            self.break_one_seed();
        }
        let prev_priority = self.piece_map[index.0 as usize].priority(self.seeds);
        let p = &mut self.piece_map[index.0 as usize];
        debug_assert!(p.peer_count > 0);
        p.peer_count -= 1;
        if self.dirty {
            return;
        }
        if prev_priority >= 0 {
            let elem = p.index;
            self.update(prev_priority as usize, elem);
        }
    }

    /// `inc_refcount(bitmask, peer)` — bulk form with the
    /// less-than-half fast path.
    pub fn inc_refcount_bitfield(&mut self, bitmask: &Bitfield) {
        if bitmask.none_set() {
            return;
        }
        if bitmask.all_set() && bitmask.size() == self.piece_map.len() {
            self.inc_refcount_all();
            return;
        }

        let size = 50.min(bitmask.size() / 2);

        if !self.dirty {
            let mut incremented: Vec<i32> = Vec::with_capacity(size);
            let mut num_inc = 0usize;
            for (index, bit) in bitmask.iter().enumerate() {
                if !bit {
                    continue;
                }
                if num_inc < size {
                    incremented.push(index as i32);
                }
                num_inc += 1;
                if num_inc >= size {
                    break;
                }
            }
            if num_inc < size {
                for &piece in &incremented {
                    let prev_priority = self.piece_map[piece as usize].priority(self.seeds);
                    let p = &mut self.piece_map[piece as usize];
                    p.peer_count += 1;
                    let new_priority = p.priority(self.seeds);
                    if prev_priority == new_priority {
                        continue;
                    } else if prev_priority >= 0 {
                        let elem = p.index;
                        self.update(prev_priority as usize, elem);
                    } else {
                        self.add(PieceIndex(piece));
                    }
                }
                return;
            }
        }

        let mut updated = false;
        for (index, bit) in bitmask.iter().enumerate() {
            if bit {
                self.piece_map[index].peer_count += 1;
                updated = true;
            }
        }
        if self.dirty {
            return;
        }
        if updated {
            self.dirty = true;
        }
    }

    /// `dec_refcount(bitmask, peer)`.
    pub fn dec_refcount_bitfield(&mut self, bitmask: &Bitfield) {
        debug_assert!(bitmask.size() <= self.piece_map.len());
        if bitmask.none_set() {
            return;
        }
        if bitmask.all_set() && bitmask.size() == self.piece_map.len() {
            self.dec_refcount_all();
            return;
        }

        let size = 50.min(bitmask.size() / 2);

        if !self.dirty {
            let mut decremented: Vec<i32> = Vec::with_capacity(size);
            let mut num_dec = 0usize;
            for (index, bit) in bitmask.iter().enumerate() {
                if !bit {
                    continue;
                }
                if num_dec < size {
                    decremented.push(index as i32);
                }
                num_dec += 1;
                if num_dec >= size {
                    break;
                }
            }
            if num_dec < size {
                for &piece in &decremented {
                    let prev_priority = self.piece_map[piece as usize].priority(self.seeds);
                    if self.piece_map[piece as usize].peer_count == 0 {
                        debug_assert!(self.seeds > 0);
                        self.break_one_seed();
                    }
                    let p = &mut self.piece_map[piece as usize];
                    debug_assert!(p.peer_count > 0);
                    p.peer_count -= 1;
                    if !self.dirty && prev_priority >= 0 {
                        let elem = p.index;
                        self.update(prev_priority as usize, elem);
                    }
                }
                return;
            }
        }

        let mut updated = false;
        for (index, bit) in bitmask.iter().enumerate() {
            if bit {
                if self.piece_map[index].peer_count == 0 {
                    debug_assert!(self.seeds > 0);
                    self.break_one_seed();
                }
                debug_assert!(self.piece_map[index].peer_count > 0);
                self.piece_map[index].peer_count -= 1;
                updated = true;
            }
        }
        if self.dirty {
            return;
        }
        if updated {
            self.dirty = true;
        }
    }

    /// `inc_refcount_all(peer)` — a seed.
    pub fn inc_refcount_all(&mut self) {
        self.seeds += 1;
        if self.seeds == 1 {
            // pieces that had no peers may become pickable
            self.dirty = true;
        }
    }

    /// `dec_refcount_all(peer)` — a seed disconnected.
    pub fn dec_refcount_all(&mut self) {
        if self.seeds > 0 {
            self.seeds -= 1;
            if self.seeds == 0 {
                self.dirty = true;
            }
            return;
        }
        for p in &mut self.piece_map {
            debug_assert!(p.peer_count > 0);
            p.peer_count -= 1;
        }
        self.dirty = true;
    }

    /// `break_one_seed()` — a seed's bitfield turns out incomplete:
    /// fold one seed count into per-piece counts.
    fn break_one_seed(&mut self) {
        debug_assert!(self.seeds > 0);
        self.seeds -= 1;
        for p in &mut self.piece_map {
            p.peer_count += 1;
        }
        self.dirty = true;
    }

    // ---------- priority buckets ----------

    /// bucket containing `elem_index` — boundaries hold exclusive
    /// end indices, so it's the first bucket whose end is past it.
    /// Callers' `priority` hints can go stale (seeds/priority
    /// shifts); deriving it here keeps the cascades in-bounds.
    fn bucket_of(&self, elem_index: i32) -> Option<usize> {
        self.priority_boundaries
            .iter()
            .position(|&end| elem_index < end)
    }

    fn priority_begin(&self, prio: usize) -> i32 {
        if prio == 0 {
            0
        } else {
            self.priority_boundaries[prio - 1]
        }
    }

    fn priority_end(&self, prio: usize) -> i32 {
        self.priority_boundaries[prio]
    }

    fn priority_range(&self, prio: usize) -> (i32, i32) {
        (self.priority_begin(prio), self.priority_end(prio))
    }

    /// `add(index)` — insert `index` into `m_pieces` at a random slot
    /// inside its priority range, cascading a hole down the buckets.
    fn add(&mut self, index: PieceIndex) {
        debug_assert!(!self.dirty);
        let mut index = index;
        let priority0 = self.piece_map[index.0 as usize].priority(self.seeds);
        if priority0 < 0 {
            return;
        }
        let mut priority = priority0 as usize;

        if self.priority_boundaries.len() <= priority {
            self.priority_boundaries
                .resize(priority + 1, self.pieces.len() as i32);
        }

        let range = self.priority_range(priority);
        let mut new_index = if range.1 == range.0 {
            range.0
        } else {
            i32::try_from(random((range.1 - range.0) as u32)).unwrap_or(0) + range.0
        };

        self.pieces.push(PieceIndex(-1));

        loop {
            {
                let temp = self.pieces[new_index as usize];
                self.pieces[new_index as usize] = index;
                self.piece_map[index.0 as usize].index = new_index;
                index = temp;
            }
            let mut temp;
            loop {
                temp = self.priority_boundaries[priority];
                self.priority_boundaries[priority] += 1;
                priority += 1;
                if !(temp == new_index && priority < self.priority_boundaries.len()) {
                    break;
                }
            }
            new_index = temp;
            if priority >= self.priority_boundaries.len() {
                break;
            }
        }
        if index.0 != -1 {
            debug_assert_eq!(new_index, self.pieces.len() as i32 - 1);
            self.pieces[new_index as usize] = index;
            self.piece_map[index.0 as usize].index = new_index;
        }
    }

    /// `remove(priority, elem_index)` — pull `elem_index` out of
    /// `m_pieces`, cascading the last element of each lower bucket up.
    fn remove(&mut self, priority: usize, elem_index: i32) {
        debug_assert!(!self.dirty);
        if elem_index < 0 || elem_index as usize >= self.pieces.len() {
            self.dirty = true;
            return;
        }
        let mut priority = self.bucket_of(elem_index).unwrap_or(priority);
        let mut elem_index = elem_index;
        let mut next_index = elem_index;
        loop {
            let mut temp;
            loop {
                self.priority_boundaries[priority] -= 1;
                temp = self.priority_boundaries[priority];
                priority += 1;
                if !(next_index == temp && priority < self.priority_boundaries.len()) {
                    break;
                }
            }
            if next_index == temp {
                break;
            }
            next_index = temp;

            let piece = self.pieces[next_index as usize];
            self.pieces[elem_index as usize] = piece;
            self.piece_map[piece.0 as usize].index = elem_index;
            elem_index = next_index;

            if priority == self.priority_boundaries.len() {
                break;
            }
        }
        self.pieces.pop();
    }

    /// `update(priority, elem_index)` — `piece_map[pieces[elem_index]]`
    /// changed priority; relocate it (remove if it became -1).
    fn update(&mut self, _priority: usize, elem_index: i32) {
        debug_assert!(!self.dirty);
        // upstream asserts `p.index == elem_index` and
        // `priority_begin <= elem_index < priority_end`; a stale
        // index (e.g. a flushed piece's reclaimed slot) would
        // corrupt the boundary cascade — mark dirty and let the
        // next pick rebuild `m_pieces` instead of panicking.
        if elem_index < 0 || elem_index as usize >= self.pieces.len() {
            self.dirty = true;
            return;
        }
        let index = self.pieces[elem_index as usize];
        if self.piece_map[index.0 as usize].index != elem_index {
            self.dirty = true;
            return;
        }
        let new_priority = self.piece_map[index.0 as usize].priority(self.seeds);
        let Some(bucket) = self.bucket_of(elem_index) else {
            self.dirty = true;
            return;
        };
        let mut priority = bucket as i32;

        if new_priority == priority {
            return;
        }
        if new_priority == -1 {
            self.remove(priority as usize, elem_index);
            return;
        }
        if self.priority_boundaries.len() <= new_priority as usize {
            self.priority_boundaries
                .resize(new_priority as usize + 1, self.pieces.len() as i32);
        }

        let mut elem_index = elem_index;
        if priority > new_priority {
            // cascade upward (earlier buckets) — `--priority` first
            let mut temp = index;
            loop {
                priority -= 1;
                let new_index = {
                    let v = self.priority_boundaries[priority as usize];
                    self.priority_boundaries[priority as usize] += 1;
                    v
                };
                if temp != self.pieces[new_index as usize] {
                    temp = self.pieces[new_index as usize];
                    self.pieces[elem_index as usize] = temp;
                    self.piece_map[temp.0 as usize].index = elem_index;
                }
                elem_index = new_index;
                if priority == new_priority {
                    break;
                }
            }
            self.pieces[elem_index as usize] = index;
            self.piece_map[index.0 as usize].index = elem_index;
            self.shuffle(new_priority, elem_index);
        } else {
            // cascade downward — `--boundaries[priority]` first
            let mut temp = index;
            loop {
                let new_index = {
                    self.priority_boundaries[priority as usize] -= 1;
                    self.priority_boundaries[priority as usize]
                };
                if temp != self.pieces[new_index as usize] {
                    temp = self.pieces[new_index as usize];
                    self.pieces[elem_index as usize] = temp;
                    self.piece_map[temp.0 as usize].index = elem_index;
                }
                elem_index = new_index;
                priority += 1;
                if priority == new_priority {
                    break;
                }
            }
            self.pieces[elem_index as usize] = index;
            self.piece_map[index.0 as usize].index = elem_index;
            self.shuffle(priority, elem_index);
        }
    }

    /// `shuffle(priority, elem_index)` — swap with a random element in
    /// the same priority range.
    fn shuffle(&mut self, priority: i32, elem_index: i32) {
        debug_assert!(!self.dirty);
        let range = self.priority_range(priority as usize);
        if range.1 - range.0 - 1 <= 0 {
            return;
        }
        let other = i32::try_from(random((range.1 - range.0 - 1) as u32)).unwrap_or(0) + range.0;
        if other == elem_index {
            return;
        }
        let a = self.pieces[other as usize];
        let b = self.pieces[elem_index as usize];
        self.piece_map[a.0 as usize].index = elem_index;
        self.piece_map[b.0 as usize].index = other;
        self.pieces.swap(other as usize, elem_index as usize);
    }

    /// `update_pieces()` — rebuild `m_pieces`/`m_priority_boundaries`
    /// (counts → cumulative boundaries → placement → per-bucket
    /// shuffle → back-map).
    fn update_pieces(&mut self) {
        debug_assert!(self.dirty);
        if self.priority_boundaries.is_empty() {
            self.priority_boundaries.resize(1, 0);
        }
        self.priority_boundaries.fill(0);
        // pass 1: count pieces per priority; pos.index = intra-bucket rank
        for pos in self.piece_map.iter_mut() {
            let prio = pos.priority(self.seeds);
            if prio == -1 {
                continue;
            }
            if prio as usize >= self.priority_boundaries.len() {
                self.priority_boundaries.resize(prio as usize + 1, 0);
            }
            pos.index = self.priority_boundaries[prio as usize];
            self.priority_boundaries[prio as usize] += 1;
        }

        // pass 2: cumulative boundaries
        let mut new_size = 0i32;
        for b in &mut self.priority_boundaries {
            new_size += *b;
            *b = new_size;
        }
        self.pieces.clear();
        self.pieces.resize(new_size as usize, PieceIndex(0));

        // pass 3: place pieces at boundary + intra-bucket rank
        for (piece, p) in self.piece_map.iter().enumerate() {
            let prio = p.priority(self.seeds);
            if prio == -1 {
                continue;
            }
            let new_index = self.priority_begin(prio as usize) + p.index;
            self.pieces[new_index as usize] = PieceIndex(piece as i32);
        }

        // per-bucket shuffle, then fix back-map
        let mut start = 0i32;
        for b in 0..self.priority_boundaries.len() {
            let end = self.priority_boundaries[b];
            if start == end {
                continue;
            }
            let (a, b_) = self.pieces.split_at_mut(end as usize);
            let slice = &mut a[start as usize..];
            shuffle(slice);
            let _ = b_;
            start = end;
        }
        for (i, p) in self.pieces.iter().enumerate() {
            self.piece_map[p.0 as usize].index = i as i32;
        }
        self.dirty = false;
    }

    // ---------- piece state ----------

    /// `have_piece(index)` — flushed, or passed hash check while
    /// still tracked as downloading.
    #[must_use]
    pub fn have_piece(&self, index: PieceIndex) -> bool {
        let p = &self.piece_map[index.0 as usize];
        if p.flushed() {
            return true;
        }
        let state = p.state.queue();
        if state == DownloadQueue::Open {
            return false;
        }
        match self.find_dl_piece(state, index) {
            Some(dp) => dp.passed_hash_check,
            None => false,
        }
    }

    /// `is_piece_finished(index)` — all blocks written or finished.
    #[must_use]
    pub fn is_piece_finished(&self, index: PieceIndex) -> bool {
        let p = &self.piece_map[index.0 as usize];
        if p.flushed() {
            return true;
        }
        let state = p.state.queue();
        if state == DownloadQueue::Open {
            return false;
        }
        let Some(dp) = self.find_dl_piece(state, index) else {
            return false;
        };
        let max_blocks = self.blocks_in_piece(index);
        if i32::from(dp.finished) + i32::from(dp.writing) < max_blocks {
            return false;
        }
        true
    }

    /// `is_downloading(index)` — `state != piece_open`.
    #[must_use]
    pub fn is_downloading(&self, index: PieceIndex) -> bool {
        self.piece_map[index.0 as usize].state.is_downloading()
    }

    /// `is_hashing(piece)`.
    #[must_use]
    pub fn is_hashing(&self, piece: PieceIndex) -> bool {
        let p = &self.piece_map[piece.0 as usize];
        let state = p.state.queue();
        if state == DownloadQueue::Open {
            return false;
        }
        self.find_dl_piece(state, piece)
            .is_some_and(|dp| dp.hashing)
    }

    /// `is_piece_flushed(index)`.
    #[must_use]
    pub fn is_piece_flushed(&self, index: PieceIndex) -> bool {
        self.piece_map[index.0 as usize].flushed()
    }

    /// `piece_priority(index)`.
    #[must_use]
    pub fn piece_priority(&self, index: PieceIndex) -> DownloadPriority {
        DownloadPriority(self.piece_map[index.0 as usize].piece_priority)
    }

    /// `piece_priorities(pieces)`.
    pub fn piece_priorities(&self) -> Vec<DownloadPriority> {
        self.piece_map
            .iter()
            .map(|p| DownloadPriority(p.piece_priority))
            .collect()
    }

    /// `piece_stats(index)`.
    #[must_use]
    pub fn piece_stats(&self, index: PieceIndex) -> PieceStats {
        let pp = &self.piece_map[index.0 as usize];
        PieceStats {
            peer_count: pp.peer_count as i32 + self.seeds,
            priority: pp.priority(self.seeds),
            flushed: pp.flushed(),
            downloading: pp.downloading(),
        }
    }

    /// `piece_info(index)` — a snapshot; for open pieces synthesizes a
    /// `downloading_piece` with `info_idx == 0`.
    #[must_use]
    pub fn piece_info(&self, index: PieceIndex) -> DownloadingPiece {
        let state = self.piece_map[index.0 as usize].state.queue();
        if state != DownloadQueue::Open {
            if let Some(dp) = self.find_dl_piece(state, index) {
                return *dp;
            }
        }
        let mut st = DownloadingPiece::new(index);
        st.info_idx = 0;
        if self.piece_map[index.0 as usize].flushed() {
            st.finished = self.blocks_in_piece(index) as u16;
        }
        st
    }

    /// `num_have()`.
    #[must_use]
    pub fn num_have(&self) -> i32 {
        self.num_have
    }

    /// `num_filtered()`.
    #[must_use]
    pub fn num_filtered(&self) -> i32 {
        self.num_filtered
    }

    /// `num_have_filtered()`.
    #[must_use]
    pub fn num_have_filtered(&self) -> i32 {
        self.num_have_filtered
    }

    /// `num_seeds()`.
    #[must_use]
    pub fn num_seeds(&self) -> i32 {
        self.seeds
    }

    /// `is_finished()` — `num_pieces - num_filtered <= num_have`
    /// (all wanted pieces passed the hash check; `num_have_filtered`
    /// appears on both sides and cancels).
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.num_pieces() - self.num_filtered <= self.num_have
    }

    /// `is_seeding()`.
    #[must_use]
    pub fn is_seeding(&self) -> bool {
        self.num_have == self.num_pieces()
    }

    /// `num_want_left()`.
    #[must_use]
    pub fn num_want_left(&self) -> i32 {
        self.num_pieces() - self.num_have - self.num_filtered + self.num_have_filtered
    }

    /// `want()` — pieces still wanted.
    #[must_use]
    pub fn want(&self) -> PieceCount {
        let want_last = self.piece_priority(PieceIndex(self.num_pieces() - 1))
            != DownloadPriority::DONT_DOWNLOAD;
        PieceCount {
            num_pieces: self.num_pieces() - self.num_filtered - self.num_have_filtered,
            pad_bytes: self.num_pad_bytes - self.filtered_pad_bytes - self.have_filtered_pad_bytes,
            last_piece: want_last,
        }
    }

    /// `have_want()` — pieces we have and want.
    #[must_use]
    pub fn have_want(&self) -> PieceCount {
        let last = PieceIndex(self.num_pieces() - 1);
        let have_last = self.have_piece(last);
        let want_last = self.piece_priority(last) != DownloadPriority::DONT_DOWNLOAD;
        PieceCount {
            num_pieces: self.num_have - self.num_have_filtered,
            pad_bytes: self.have_pad_bytes - self.have_filtered_pad_bytes,
            last_piece: have_last && want_last,
        }
    }

    /// `have()`.
    #[must_use]
    pub fn have(&self) -> PieceCount {
        let have_last = self.have_piece(PieceIndex(self.num_pieces() - 1));
        PieceCount {
            num_pieces: self.num_have,
            pad_bytes: self.have_pad_bytes,
            last_piece: have_last,
        }
    }

    /// `all_pieces()`.
    #[must_use]
    pub fn all_pieces(&self) -> PieceCount {
        PieceCount {
            num_pieces: self.num_pieces(),
            pad_bytes: self.num_pad_bytes,
            last_piece: true,
        }
    }

    // ---------- download queue bookkeeping ----------

    /// `find_dl_piece(queue, index)` — binary-search position.
    fn dl_pos(&self, queue: DownloadQueue, index: PieceIndex) -> Option<usize> {
        debug_assert!(matches!(
            queue,
            DownloadQueue::Downloading
                | DownloadQueue::Full
                | DownloadQueue::Finished
                | DownloadQueue::ZeroPrio
        ));
        let q = &self.downloads[queue as usize];
        q.binary_search_by(|dp| dp.index.cmp(&index)).ok()
    }

    fn find_dl_piece(&self, queue: DownloadQueue, index: PieceIndex) -> Option<&DownloadingPiece> {
        self.dl_pos(queue, index)
            .map(|i| &self.downloads[queue as usize][i])
    }

    /// `blocks_for_piece(dp)` — public resume-data accessor: the
    /// per-block state window of a downloading piece (upstream
    /// `write_resume_data` iterates it to build the bitmask).
    #[must_use]
    pub fn blocks_for_piece(&self, dp: &DownloadingPiece) -> &[BlockInfo] {
        self.blocks_for(dp.info_idx, dp.index)
    }

    /// `blocks_for_piece(dp)` — the block_info window.
    fn blocks_for(&self, info_idx: u16, index: PieceIndex) -> &[BlockInfo] {
        let start = usize::from(info_idx) * self.blocks_per_piece() as usize;
        &self.block_info[start..start + self.blocks_in_piece(index) as usize]
    }

    /// `add_download_piece(piece)` — allocate block_info, create the
    /// `downloading_piece` in `Downloading`, init blocks (pad tail
    /// blocks pre-finished).
    fn add_download_piece(&mut self, piece: PieceIndex) {
        let block_index: u16;
        if self.free_block_infos.is_empty() {
            let blocks = self.blocks_per_piece();
            block_index = (self.block_info.len() / blocks as usize) as u16;
            self.block_info.resize(
                self.block_info.len() + blocks as usize,
                BlockInfo::default(),
            );
        } else if let Some(bi) = self.free_block_infos.pop() {
            block_index = bi;
        } else {
            return;
        }

        let mut ret = DownloadingPiece::new(piece);
        ret.info_idx = block_index;

        let payload_blocks = self.payload_blocks_in_piece(piece);
        {
            let start = usize::from(ret.info_idx) * self.blocks_per_piece() as usize;
            let n = self.blocks_in_piece(piece) as usize;
            for (block_idx, info) in self.block_info[start..start + n].iter_mut().enumerate() {
                info.num_peers = 0;
                info.peer = None;
                if block_idx as i32 >= payload_blocks {
                    info.state = BlockState::Finished;
                    ret.finished += 1;
                } else {
                    info.state = BlockState::None;
                }
            }
        }

        let q = &mut self.downloads[DownloadQueue::Downloading as usize];
        let pos = q.partition_point(|dp| dp.index < piece);
        q.insert(pos, ret);
        self.update_piece_state(DownloadQueue::Downloading, pos);
    }

    /// `erase_download_piece` — by (queue, pos); frees block_info,
    /// marks the piece open.
    fn erase_download_piece(&mut self, queue: DownloadQueue, pos: usize) {
        let dp = self.downloads[queue as usize].remove(pos);
        self.free_block_infos.push(dp.info_idx);
        self.piece_map[dp.index.0 as usize].state = DownloadQueue::Open;
    }

    /// `update_piece_state` — move `dp` between queues as its block
    /// counters dictate; returns the new `(queue, pos)`.
    /// Upstream leaves `piece_open` pieces' entries in place — we
    /// mirror that by letting the caller erase explicitly.
    fn update_piece_state(&mut self, queue: DownloadQueue, pos: usize) -> (DownloadQueue, usize) {
        let dp = self.downloads[queue as usize][pos];
        let index = dp.index;
        let num_blocks = self.blocks_in_piece(index);
        let current = self.piece_map[index.0 as usize].state;
        debug_assert!(current != DownloadQueue::Open);

        let new_state = if self.piece_map[index.0 as usize].filtered() {
            DownloadQueue::ZeroPrio
        } else if dp.requested + dp.finished + dp.writing + u16::from(dp.hashing) == 0 {
            DownloadQueue::Open
        } else if dp.requested + dp.finished + dp.writing < num_blocks as u16 {
            if current.is_reverse() {
                DownloadQueue::DownloadingReverse
            } else {
                DownloadQueue::Downloading
            }
        } else if dp.requested > 0 {
            if current.is_reverse() {
                DownloadQueue::FullReverse
            } else {
                DownloadQueue::Full
            }
        } else {
            DownloadQueue::Finished
        };

        if new_state == current || new_state == DownloadQueue::Open {
            return (queue, pos);
        }

        let prio = self.piece_map[index.0 as usize].priority(self.seeds);

        // move to the new queue
        self.downloads[queue as usize].remove(pos);
        let old_state = self.piece_map[index.0 as usize].state;
        self.piece_map[index.0 as usize].state = new_state;
        let new_queue = new_state.queue();
        let nq = &mut self.downloads[new_queue as usize];
        let npos = nq.partition_point(|d| d.index < index);
        nq.insert(npos, dp);

        // upstream `update_piece_state`: a state change can alter
        // `priority()` without crossing -1 (e.g. downloading → full
        // removes the piece from `m_pieces` entirely, and
        // downloading ↔ reverse shifts the adjustment) — the bucket
        // index must be re-cascaded, not only `add`ed on -1 → ≥0.
        if !self.dirty {
            if prio == -1 {
                if self.piece_map[index.0 as usize].priority(self.seeds) != -1 {
                    self.add(index);
                }
            } else {
                let elem = self.piece_map[index.0 as usize].index;
                self.update(prio as usize, elem);
            }
        }
        let _ = old_state;
        (new_queue, npos)
    }

    // ---------- have/flush lifecycle ----------

    /// `account_have(index)`.
    fn account_have(&mut self, index: PieceIndex) {
        self.num_have += 1;
        let pad = self.pad_bytes_in_piece(index);
        if self.piece_map[index.0 as usize].filtered() {
            self.filtered_pad_bytes -= i64::from(pad);
            self.num_filtered -= 1;
            self.have_filtered_pad_bytes += i64::from(pad);
            self.num_have_filtered += 1;
        }
        self.have_pad_bytes += i64::from(pad);
    }

    /// `account_lost(index)`.
    fn account_lost(&mut self, index: PieceIndex) {
        self.num_have -= 1;
        let pad = self.pad_bytes_in_piece(index);
        if self.piece_map[index.0 as usize].filtered() {
            self.filtered_pad_bytes += i64::from(pad);
            self.num_filtered += 1;
            self.have_filtered_pad_bytes -= i64::from(pad);
            self.num_have_filtered -= 1;
        }
        self.have_pad_bytes -= i64::from(pad);
    }

    /// `piece_passed(index)` — hash check passed; flushes the piece
    /// if all blocks are written.
    pub fn piece_passed(&mut self, index: PieceIndex) {
        let state = self.piece_map[index.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return;
        }
        let Some(pos) = self.dl_pos(state, index) else {
            return;
        };
        if self.downloads[state as usize][pos].locked {
            return;
        }
        self.downloads[state as usize][pos].passed_hash_check = true;
        self.account_have(index);

        if i32::from(self.downloads[state as usize][pos].finished) < self.blocks_in_piece(index) {
            return;
        }
        self.piece_flushed(index);
    }

    /// `piece_flushed(index)` — the piece is on disk.
    pub fn piece_flushed(&mut self, index: PieceIndex) {
        let info_index = self.piece_map[index.0 as usize].index;
        let priority = self.piece_map[index.0 as usize].priority(self.seeds);

        if self.piece_map[index.0 as usize].flushed() {
            return;
        }

        let mut passed_hash_check = false;
        {
            let p = &self.piece_map[index.0 as usize];
            if p.downloading() {
                let state = p.state.queue();
                let Some(pos) = self.dl_pos(state, index) else {
                    return;
                };
                passed_hash_check = self.downloads[state as usize][pos].passed_hash_check;
                if self.downloads[state as usize][pos].locked {
                    return;
                }
                self.erase_download_piece(state, pos);
            }
        }

        if !passed_hash_check {
            // straight from open to flushed (e.g. resume data)
            self.account_have(index);
        }

        self.piece_map[index.0 as usize].set_flushed();

        // cursor bookkeeping
        if self.cursor == self.reverse_cursor - 1 && self.cursor == index.0 {
            self.cursor = self.num_pieces();
            self.reverse_cursor = 0;
        } else if self.cursor == index.0 {
            self.cursor += 1;
            while (self.cursor as usize) < self.piece_map.len()
                && (self.piece_map[self.cursor as usize].flushed()
                    || self.piece_map[self.cursor as usize].filtered())
            {
                self.cursor += 1;
            }
        } else if self.reverse_cursor - 1 == index.0 {
            self.reverse_cursor -= 1;
            while self.reverse_cursor > 0
                && (self.piece_map[self.reverse_cursor as usize - 1].flushed()
                    || self.piece_map[self.reverse_cursor as usize - 1].filtered())
            {
                self.reverse_cursor -= 1;
            }
        }
        if priority == -1 {
            return;
        }
        if self.dirty {
            return;
        }
        self.remove(priority as usize, info_index);
    }

    /// `we_have_all()` — seed state.
    pub fn we_have_all(&mut self) {
        self.priority_boundaries.clear();
        self.priority_boundaries.resize(1, 0);
        self.block_info.clear();
        self.free_block_infos.clear();
        self.pieces.clear();

        self.dirty = false;
        self.num_have_filtered += self.num_filtered;
        self.num_filtered = 0;
        self.have_filtered_pad_bytes += self.filtered_pad_bytes;
        self.filtered_pad_bytes = 0;
        self.cursor = self.num_pieces();
        self.reverse_cursor = 0;
        self.num_have = self.num_pieces();

        for q in &mut self.downloads {
            q.clear();
        }
        for p in &mut self.piece_map {
            p.set_flushed();
            p.state = DownloadQueue::Open;
        }
    }

    /// `we_dont_have(index)` — a piece we had is gone.
    pub fn we_dont_have(&mut self, index: PieceIndex) {
        let mut have_piece = self.piece_map[index.0 as usize].flushed();
        if !have_piece {
            let state = self.piece_map[index.0 as usize].state.queue();
            if state == DownloadQueue::Open {
                return;
            }
            let Some(pos) = self.dl_pos(state, index) else {
                return;
            };
            have_piece = self.downloads[state as usize][pos].passed_hash_check;
            self.erase_download_piece(state, pos);
        }

        if have_piece {
            self.account_lost(index);
        }

        if !self.piece_map[index.0 as usize].filtered() {
            // update cursors
            if self.reverse_cursor == self.cursor {
                self.reverse_cursor = 0;
                self.cursor = self.num_pieces();
            }
        }

        self.piece_map[index.0 as usize].set_not_flushed();

        if self.dirty {
            return;
        }
        if self.piece_map[index.0 as usize].priority(self.seeds) >= 0 {
            self.add(index);
        }
    }

    /// `set_piece_priority(index, prio)` — returns whether the
    /// piece's filter state changed.
    pub fn set_piece_priority(
        &mut self,
        index: PieceIndex,
        new_priority: DownloadPriority,
    ) -> bool {
        let prev_priority = self.piece_map[index.0 as usize].priority(self.seeds);

        if new_priority.0 == self.piece_map[index.0 as usize].piece_priority {
            return false;
        }

        let had = self.have_piece(index);
        let mut ret = false;
        let was_filtered = self.piece_map[index.0 as usize].filtered();

        if new_priority == DownloadPriority::DONT_DOWNLOAD && !was_filtered {
            // filtered
            let pad = self.pad_bytes_in_piece(index);
            if had {
                self.have_filtered_pad_bytes += i64::from(pad);
                self.num_have_filtered += 1;
            } else {
                self.filtered_pad_bytes += i64::from(pad);
                self.num_filtered += 1;
            }

            if !self.piece_map[index.0 as usize].flushed() {
                if self.cursor == self.reverse_cursor - 1 && self.cursor == index.0 {
                    self.cursor = self.num_pieces();
                    self.reverse_cursor = 0;
                } else if self.cursor == index.0 {
                    self.cursor += 1;
                    while (self.cursor as usize) < self.piece_map.len()
                        && (self.piece_map[self.cursor as usize].flushed()
                            || self.piece_map[self.cursor as usize].filtered())
                    {
                        self.cursor += 1;
                    }
                } else if self.reverse_cursor == index.0 + 1 {
                    self.reverse_cursor -= 1;
                    while self.reverse_cursor > 0
                        && (self.piece_map[self.reverse_cursor as usize - 1].flushed()
                            || self.piece_map[self.reverse_cursor as usize - 1].filtered())
                    {
                        self.reverse_cursor -= 1;
                    }
                }
            }
            ret = true;
        } else if new_priority != DownloadPriority::DONT_DOWNLOAD && was_filtered {
            // unfiltered
            let pad = self.pad_bytes_in_piece(index);
            if had {
                self.have_filtered_pad_bytes -= i64::from(pad);
                self.num_have_filtered -= 1;
            } else {
                self.filtered_pad_bytes -= i64::from(pad);
                self.num_filtered -= 1;
            }

            if !self.piece_map[index.0 as usize].flushed() && self.reverse_cursor == self.cursor {
                self.reverse_cursor = 0;
                self.cursor = self.num_pieces();
            }
            ret = true;
        }

        self.piece_map[index.0 as usize].piece_priority = new_priority.0;
        let new_prio = self.piece_map[index.0 as usize].priority(self.seeds);

        if prev_priority != new_prio && !self.dirty {
            if prev_priority == -1 {
                self.add(index);
            } else {
                let elem = self.piece_map[index.0 as usize].index;
                self.update(prev_priority as usize, elem);
            }
        }

        if self.piece_map[index.0 as usize].downloading() {
            let queue = self.piece_map[index.0 as usize].state.queue();
            if let Some(pos) = self.dl_pos(queue, index) {
                self.update_piece_state(queue, pos);
            }
        }

        ret
    }

    /// `set_pad_bytes(piece, bytes)` — record pad bytes; a full-pad
    /// piece counts as had (flushed).
    pub fn set_pad_bytes(&mut self, piece: PieceIndex, bytes: i32) {
        debug_assert!(bytes <= self.piece_size(piece));
        debug_assert!(bytes > 0);
        debug_assert!(!self.pads_in_piece.contains_key(&piece.0));

        self.num_pad_bytes += i64::from(bytes);
        self.pads_in_piece.insert(piece.0, bytes);

        let had = self.have_piece(piece);
        if had {
            self.have_pad_bytes += i64::from(bytes);
            if self.piece_map[piece.0 as usize].filtered() {
                self.have_filtered_pad_bytes += i64::from(bytes);
            }
        } else if self.piece_map[piece.0 as usize].filtered() {
            self.filtered_pad_bytes += i64::from(bytes);
        }

        if self.piece_size(piece) == bytes {
            self.piece_flushed(piece);
        }
    }

    /// `restore_piece(index, blocks)` — unlock a locked piece and
    /// (optionally) reset specific blocks to `None` state.
    pub fn restore_piece(&mut self, index: PieceIndex, blocks: &[i32]) {
        let state = self.piece_map[index.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return;
        }
        let Some(mut pos) = self.dl_pos(state, index) else {
            return;
        };

        self.downloads[state as usize][pos].locked = false;

        let prev_priority = self.piece_map[index.0 as usize].priority(self.seeds);

        if !blocks.is_empty() {
            let info_idx = self.downloads[state as usize][pos].info_idx;
            let base = usize::from(info_idx) * self.blocks_per_piece() as usize;
            let mut writing = self.downloads[state as usize][pos].writing;
            let mut finished = self.downloads[state as usize][pos].finished;
            for &block in blocks {
                let info = &mut self.block_info[base + block as usize];
                match info.state {
                    BlockState::Writing => {
                        debug_assert!(writing > 0);
                        writing -= 1;
                        info.peer = None;
                        info.state = BlockState::None;
                    }
                    BlockState::Finished => {
                        debug_assert!(finished > 0);
                        finished -= 1;
                        info.peer = None;
                        info.state = BlockState::None;
                    }
                    // not expected upstream either; leave intact
                    BlockState::Requested => continue,
                    BlockState::None => {}
                }
            }
            self.downloads[state as usize][pos].writing = writing;
            self.downloads[state as usize][pos].finished = finished;
            let (nq, np) = self.update_piece_state(state, pos);
            pos = np;
            let _ = nq;
        }

        let state = self.piece_map[index.0 as usize].state.queue();
        let dp = &self.downloads[state as usize][pos];
        if blocks.is_empty() || dp.requested + dp.finished + dp.writing == 0 {
            self.erase_download_piece(state, pos);

            let new_priority = self.piece_map[index.0 as usize].priority(self.seeds);
            if new_priority == prev_priority || self.dirty {
                return;
            }
            if prev_priority == -1 {
                self.add(index);
            } else {
                let elem = self.piece_map[index.0 as usize].index;
                self.update(prev_priority as usize, elem);
            }
        }
    }
}

/// `aux::random_shuffle` — Fisher-Yates over `random()`.
fn shuffle<T>(v: &mut [T]) {
    for i in (1..v.len()).rev() {
        let j = random(i as u32) as usize;
        v.swap(i, j);
    }
}

/// `append_blocks` — move up to `num_blocks` from `src` to `dst`.
fn append_blocks(dst: &mut Vec<PieceBlock>, src: &mut Vec<PieceBlock>, num_blocks: i32) -> i32 {
    if src.is_empty() {
        return num_blocks;
    }
    let to_copy = (src.len() as i32).min(num_blocks) as usize;
    dst.extend(src.drain(..to_copy));
    num_blocks - to_copy as i32
}

impl PiecePicker {
    /// `requested_from` — `(exclusive, exclusive_active,
    /// contiguous_blocks, first_block)`: whether only `peer` holds
    /// this piece's blocks, whether no other peer has outstanding
    /// requests, and the largest run of free blocks.
    fn requested_from(
        &self,
        dp: &DownloadingPiece,
        num_blocks_in_piece: i32,
        peer: Option<u64>,
    ) -> (bool, bool, i32, i32) {
        let mut exclusive = true;
        let mut exclusive_active = true;
        let mut contiguous_blocks = 0i32;
        let mut max_contiguous = 0i32;
        let mut first_block = 0i32;
        for (idx, info) in self.blocks_for(dp.info_idx, dp.index).iter().enumerate() {
            if info.state == BlockState::None {
                contiguous_blocks += 1;
                continue;
            }
            if contiguous_blocks > max_contiguous {
                max_contiguous = contiguous_blocks;
                first_block = idx as i32 - contiguous_blocks;
            }
            contiguous_blocks = 0;
            if info.peer != peer {
                exclusive = false;
                if info.state == BlockState::Requested && info.peer.is_some() {
                    exclusive_active = false;
                }
            }
        }
        if contiguous_blocks > max_contiguous {
            max_contiguous = contiguous_blocks;
            first_block = num_blocks_in_piece - contiguous_blocks;
        }
        (exclusive, exclusive_active, max_contiguous, first_block)
    }

    /// `is_piece_free` — the peer has it, we don't, not filtered.
    fn is_piece_free(&self, piece: PieceIndex, bitmask: &Bitfield) -> bool {
        bitmask.get_bit(piece.0 as usize)
            && !self.piece_map[piece.0 as usize].flushed()
            && !self.piece_map[piece.0 as usize].filtered()
    }

    /// `can_pick` — additionally not already downloading.
    fn can_pick(&self, piece: PieceIndex, bitmask: &Bitfield) -> bool {
        bitmask.get_bit(piece.0 as usize)
            && !self.piece_map[piece.0 as usize].flushed()
            && !self.piece_map[piece.0 as usize].downloading()
            && !self.piece_map[piece.0 as usize].filtered()
    }

    /// `expand_piece(piece, contiguous_blocks, have, options)` — the
    /// `[start, end)` window of pickable pieces around `piece`.
    fn expand_piece(
        &self,
        piece: PieceIndex,
        contiguous_blocks: i32,
        have: &Bitfield,
        options: PickerOptions,
    ) -> (PieceIndex, PieceIndex) {
        if contiguous_blocks == 0 {
            return (piece, PieceIndex(piece.0 + 1));
        }

        let blocks = self.blocks_per_piece();
        let whole_pieces = (contiguous_blocks + blocks - 1) / blocks;

        let mut start = piece;
        let lower_limit = if options.contains(PickerOptions::ALIGN_EXPANDED_PIECES) {
            piece.0 - (piece.0 % whole_pieces)
        } else {
            (piece.0 - whole_pieces + 1).max(0)
        };

        while start.0 > lower_limit && self.can_pick(PieceIndex(start.0 - 1), have) {
            start.0 -= 1;
        }

        let mut end = PieceIndex(piece.0 + 1);
        let mut upper_limit = if options.contains(PickerOptions::ALIGN_EXPANDED_PIECES) {
            lower_limit + whole_pieces
        } else {
            start.0 + whole_pieces
        };
        if upper_limit > have.size() as i32 {
            upper_limit = have.size() as i32;
        }
        while end.0 < upper_limit && self.can_pick(end, have) {
            end.0 += 1;
        }
        (start, end)
    }

    /// `extent_for(piece)` — the extent index (4 MiB worth of blocks).
    fn extent_for(&self, p: PieceIndex) -> PieceExtent {
        p.0 / (MAX_PIECE_AFFINITY_EXTENT / self.blocks_per_piece())
    }

    /// `extent_for(extent)` — the piece range.
    fn extent_pieces(&self, e: PieceExtent) -> (PieceIndex, PieceIndex) {
        let extent_size = MAX_PIECE_AFFINITY_EXTENT / self.blocks_per_piece();
        let begin = e * extent_size;
        let end = (begin + extent_size).min(self.num_pieces());
        (PieceIndex(begin), PieceIndex(end))
    }

    /// `record_downloading_piece(p)` — maybe record extent affinity.
    fn record_downloading_piece(&mut self, p: PieceIndex) {
        if self.blocks_per_piece() >= MAX_PIECE_AFFINITY_EXTENT {
            return;
        }
        let this_extent = self.extent_for(p);
        if self.recent_extents.contains(&this_extent) {
            return;
        }
        let this_prio = self.piece_priority(p);
        let mut have_all = true;
        let (b, e) = self.extent_pieces(this_extent);
        for piece in b.0..e.0 {
            let piece = PieceIndex(piece);
            if piece == p {
                continue;
            }
            if !self.piece_map[piece.0 as usize].flushed() {
                have_all = false;
            }
            if self.piece_priority(piece) != this_prio {
                return;
            }
        }
        if have_all {
            return;
        }
        // cap at 5 active extents
        if self.recent_extents.len() < 5 {
            self.recent_extents.push(this_extent);
        }
    }

    // ---------- block transitions ----------

    /// `mark_as_downloading(block, peer, options)` — false if the
    /// block can't be picked (finished/writing).
    pub fn mark_as_downloading(
        &mut self,
        block: PieceBlock,
        peer: Option<u64>,
        options: PickerOptions,
    ) -> bool {
        debug_assert!(block.block_index < self.blocks_in_piece(block.piece_index));
        debug_assert!(!self.piece_map[block.piece_index.0 as usize].flushed());

        if self.piece_map[block.piece_index.0 as usize].state.queue() == DownloadQueue::Open {
            let prio = self.piece_map[block.piece_index.0 as usize].priority(self.seeds);

            self.piece_map[block.piece_index.0 as usize].state =
                if options.contains(PickerOptions::REVERSE) {
                    DownloadQueue::DownloadingReverse
                } else {
                    DownloadQueue::Downloading
                };

            if prio >= 0 && !self.dirty {
                let elem = self.piece_map[block.piece_index.0 as usize].index;
                self.update(prio as usize, elem);
            }

            if options.contains(PickerOptions::PIECE_EXTENT_AFFINITY) {
                self.record_downloading_piece(block.piece_index);
            }

            self.add_download_piece(block.piece_index);

            let state = DownloadQueue::Downloading;
            // the piece may have been moved to another queue already
            // (it can't be — it just has a single requested block) —
            // still, resolve the queue from the map
            let q = self.piece_map[block.piece_index.0 as usize].state.queue();
            let _ = state;
            let Some(pos) = self.dl_pos(q, block.piece_index) else {
                return false;
            };
            let info_idx = self.downloads[q as usize][pos].info_idx;
            let base = usize::from(info_idx) * self.blocks_per_piece() as usize
                + block.block_index as usize;
            let info = &mut self.block_info[base];
            if info.state == BlockState::Finished {
                return false;
            }
            info.state = BlockState::Requested;
            info.peer = peer;
            info.num_peers = 1;
            self.downloads[q as usize][pos].requested += 1;
            self.update_piece_state(q, pos);
        } else {
            let p_state = self.piece_map[block.piece_index.0 as usize].state;
            let q = p_state.queue();
            let Some(pos) = self.dl_pos(q, block.piece_index) else {
                return false;
            };
            let info_idx = self.downloads[q as usize][pos].info_idx;
            let base = usize::from(info_idx) * self.blocks_per_piece() as usize
                + block.block_index as usize;

            {
                let info = &self.block_info[base];
                if info.state == BlockState::Writing || info.state == BlockState::Finished {
                    return false;
                }
            }

            if options.contains(PickerOptions::REVERSE)
                && !p_state.is_reverse()
                && self.downloads[q as usize][pos].requested == 0
            {
                // no other peer on it: turn the piece reverse
                let prio = self.piece_map[block.piece_index.0 as usize].priority(self.seeds);
                self.piece_map[block.piece_index.0 as usize].state = p_state.make_reverse();
                if prio >= 0 && !self.dirty {
                    let elem = self.piece_map[block.piece_index.0 as usize].index;
                    self.update(prio as usize, elem);
                }
            }

            {
                let info = &mut self.block_info[base];
                info.peer = peer;
                if info.state != BlockState::Requested {
                    info.state = BlockState::Requested;
                    self.downloads[q as usize][pos].requested += 1;
                    self.update_piece_state(q, pos);
                }
                // num_peers counts every requester (block_info slice
                // is stable across queue moves)
                self.block_info[base].num_peers += 1;
            }

            // undo reverse when a non-reverse request lands
            if !options.contains(PickerOptions::REVERSE)
                && self.piece_map[block.piece_index.0 as usize]
                    .state
                    .is_reverse()
            {
                let prio = self.piece_map[block.piece_index.0 as usize].priority(self.seeds);
                let cur = self.piece_map[block.piece_index.0 as usize].state;
                self.piece_map[block.piece_index.0 as usize].state = cur.unreverse();
                if prio >= 0 && !self.dirty {
                    let elem = self.piece_map[block.piece_index.0 as usize].index;
                    self.update(prio as usize, elem);
                }
            }
        }
        true
    }

    /// `mark_as_writing(block, peer)` — a block was received and is
    /// being flushed to disk.
    pub fn mark_as_writing(&mut self, block: PieceBlock, peer: Option<u64>) -> bool {
        let p_state = self.piece_map[block.piece_index.0 as usize].state;
        if !p_state.is_downloading() {
            // ignore if we already have it
            if self.have_piece(block.piece_index) {
                return false;
            }
            let prio = self.piece_map[block.piece_index.0 as usize].priority(self.seeds);
            self.piece_map[block.piece_index.0 as usize].state = DownloadQueue::Downloading;
            if prio >= 0 && !self.dirty {
                let elem = self.piece_map[block.piece_index.0 as usize].index;
                self.update(prio as usize, elem);
            }
            self.add_download_piece(block.piece_index);
            let q = self.piece_map[block.piece_index.0 as usize].state.queue();
            let Some(pos) = self.dl_pos(q, block.piece_index) else {
                return false;
            };
            let info_idx = self.downloads[q as usize][pos].info_idx;
            let base = usize::from(info_idx) * self.blocks_per_piece() as usize
                + block.block_index as usize;
            let info = &mut self.block_info[base];
            if info.state == BlockState::Finished {
                return false;
            }
            info.state = BlockState::Writing;
            info.peer = peer;
            info.num_peers = 0;
            self.downloads[q as usize][pos].writing = 1;
            self.update_piece_state(q, pos);
        } else {
            let q = p_state.queue();
            let Some(pos) = self.dl_pos(q, block.piece_index) else {
                return false;
            };
            let info_idx = self.downloads[q as usize][pos].info_idx;
            let base = usize::from(info_idx) * self.blocks_per_piece() as usize
                + block.block_index as usize;

            self.block_info[base].peer = peer;
            if self.block_info[base].state == BlockState::Requested {
                self.downloads[q as usize][pos].requested -= 1;
            }
            if self.block_info[base].state == BlockState::Writing
                || self.block_info[base].state == BlockState::Finished
            {
                return false;
            }
            self.downloads[q as usize][pos].writing += 1;
            self.block_info[base].state = BlockState::Writing;
            // other requests for this block were cancelled
            self.block_info[base].num_peers = 0;
            self.update_piece_state(q, pos);
        }
        true
    }

    /// `mark_as_finished(block, peer)` — the block is on disk.
    pub fn mark_as_finished(&mut self, block: PieceBlock, peer: Option<u64>) {
        let p_state = self.piece_map[block.piece_index.0 as usize].state;
        if !p_state.is_downloading() {
            if self.have_piece(block.piece_index) {
                return;
            }
            let prio = self.piece_map[block.piece_index.0 as usize].priority(self.seeds);
            self.piece_map[block.piece_index.0 as usize].state = DownloadQueue::Downloading;
            if prio >= 0 && !self.dirty {
                let elem = self.piece_map[block.piece_index.0 as usize].index;
                self.update(prio as usize, elem);
            }
            self.add_download_piece(block.piece_index);
            let q = self.piece_map[block.piece_index.0 as usize].state.queue();
            let Some(pos) = self.dl_pos(q, block.piece_index) else {
                return;
            };
            let info_idx = self.downloads[q as usize][pos].info_idx;
            let base = usize::from(info_idx) * self.blocks_per_piece() as usize
                + block.block_index as usize;
            let info = &mut self.block_info[base];
            if info.state == BlockState::Finished {
                return;
            }
            info.peer = peer;
            self.downloads[q as usize][pos].finished += 1;
            info.state = BlockState::Finished;
            self.update_piece_state(q, pos);
        } else {
            let q = p_state.queue();
            let Some(pos) = self.dl_pos(q, block.piece_index) else {
                return;
            };
            let info_idx = self.downloads[q as usize][pos].info_idx;
            let base = usize::from(info_idx) * self.blocks_per_piece() as usize
                + block.block_index as usize;

            if self.block_info[base].state == BlockState::Finished {
                return;
            }
            // preserve the previous peer on disconnect (peer==None)
            if self.block_info[base].state != BlockState::Writing || peer.is_some() {
                self.block_info[base].peer = peer;
            }

            self.downloads[q as usize][pos].finished += 1;
            if self.block_info[base].state == BlockState::Writing {
                self.downloads[q as usize][pos].writing -= 1;
            }
            self.block_info[base].state = BlockState::Finished;

            let (q2, pos2) = self.update_piece_state(q, pos);
            let dp = self.downloads[q2 as usize][pos2];
            if i32::from(dp.finished) < self.blocks_in_piece(dp.index) {
                return;
            }
            if dp.passed_hash_check && !dp.hashing {
                self.piece_flushed(dp.index);
            }
        }
    }

    /// `mark_as_canceled(block, peer)` — writing finished w/o data.
    pub fn mark_as_canceled(&mut self, block: PieceBlock, peer: Option<u64>) {
        let state = self.piece_map[block.piece_index.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return;
        }
        let Some(pos) = self.dl_pos(state, block.piece_index) else {
            return;
        };
        let info_idx = self.downloads[state as usize][pos].info_idx;
        let base =
            usize::from(info_idx) * self.blocks_per_piece() as usize + block.block_index as usize;

        if self.block_info[base].state == BlockState::Finished {
            return;
        }
        self.block_info[base].peer = peer;
        if self.block_info[base].state == BlockState::Writing {
            self.downloads[state as usize][pos].writing -= 1;
            self.block_info[base].state = BlockState::None;
            let (q, pos) = self.update_piece_state(state, pos);

            let dp = &self.downloads[q as usize][pos];
            if dp.finished + dp.writing + dp.requested + u16::from(dp.hashing) == 0 {
                let prev_priority =
                    self.piece_map[block.piece_index.0 as usize].priority(self.seeds);
                self.erase_download_piece(q, pos);
                let new_priority =
                    self.piece_map[block.piece_index.0 as usize].priority(self.seeds);
                if self.dirty || new_priority == prev_priority {
                    return;
                }
                if prev_priority == -1 {
                    self.add(block.piece_index);
                } else {
                    let elem = self.piece_map[block.piece_index.0 as usize].index;
                    self.update(prev_priority as usize, elem);
                }
            }
        }
    }

    /// `abort_download(block, peer)` — reject/disconnect: drop one
    /// requester; when the last one goes, the block frees.
    pub fn abort_download(&mut self, block: PieceBlock, peer: Option<u64>) {
        let state = self.piece_map[block.piece_index.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return;
        }
        let Some(pos) = self.dl_pos(state, block.piece_index) else {
            return;
        };
        let info_idx = self.downloads[state as usize][pos].info_idx;
        let base =
            usize::from(info_idx) * self.blocks_per_piece() as usize + block.block_index as usize;

        if self.block_info[base].state != BlockState::Requested {
            return;
        }

        let prev_prio = self.piece_map[block.piece_index.0 as usize].priority(self.seeds);

        if self.block_info[base].num_peers > 0 {
            self.block_info[base].num_peers -= 1;
        }
        if self.block_info[base].peer == peer {
            self.block_info[base].peer = None;
        }
        // other peers keep it requested
        if self.block_info[base].num_peers > 0 {
            return;
        }

        self.block_info[base].peer = None;
        self.block_info[base].state = BlockState::None;
        self.downloads[state as usize][pos].requested -= 1;

        if self.downloads[state as usize][pos].requested
            + self.downloads[state as usize][pos].finished
            + self.downloads[state as usize][pos].writing
            + u16::from(self.downloads[state as usize][pos].hashing)
            == 0
        {
            self.erase_download_piece(state, pos);
            let prio = self.piece_map[block.piece_index.0 as usize].priority(self.seeds);
            if !self.dirty {
                if prev_prio == -1 && prio >= 0 {
                    self.add(block.piece_index);
                } else if prev_prio >= 0 {
                    let elem = self.piece_map[block.piece_index.0 as usize].index;
                    self.update(prev_prio as usize, elem);
                }
            }
            return;
        }
        self.update_piece_state(state, pos);
    }

    /// `write_failed(block)` — disk write failed; drop the block to
    /// `None`, lock the piece until `restore_piece`.
    pub fn write_failed(&mut self, block: PieceBlock) {
        let state = self.piece_map[block.piece_index.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return;
        }
        let Some(pos) = self.dl_pos(state, block.piece_index) else {
            return;
        };
        let info_idx = self.downloads[state as usize][pos].info_idx;
        let base =
            usize::from(info_idx) * self.blocks_per_piece() as usize + block.block_index as usize;

        debug_assert_eq!(self.block_info[base].state, BlockState::Writing);
        if self.block_info[base].state == BlockState::Writing {
            self.downloads[state as usize][pos].writing -= 1;
        }
        self.block_info[base].peer = None;
        self.block_info[base].state = BlockState::None;

        if self.downloads[state as usize][pos].passed_hash_check {
            // hash passed but the write failed — piece can't be
            // considered complete
            self.downloads[state as usize][pos].passed_hash_check = false;
            self.account_lost(block.piece_index);
        }

        // keep the piece locked until the disk side discards the
        // failed state; do NOT erase even with no outstanding blocks
        self.downloads[state as usize][pos].locked = true;
        self.update_piece_state(state, pos);
    }

    /// `lock_piece(piece)` — prevent picking until restored (used on
    /// disk write failures and hash failures).
    pub fn lock_piece(&mut self, piece: PieceIndex) {
        let state = self.piece_map[piece.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return;
        }
        let Some(pos) = self.dl_pos(state, piece) else {
            return;
        };
        if self.downloads[state as usize][pos].passed_hash_check {
            self.downloads[state as usize][pos].passed_hash_check = false;
            self.account_lost(piece);
        }
        self.downloads[state as usize][pos].locked = true;
    }

    /// `started_hash_job(piece)`.
    pub fn started_hash_job(&mut self, piece: PieceIndex) {
        let p = &self.piece_map[piece.0 as usize];
        if !p.downloading() {
            return;
        }
        let state = p.state.queue();
        if let Some(pos) = self.dl_pos(state, piece) {
            debug_assert!(!self.downloads[state as usize][pos].hashing);
            self.downloads[state as usize][pos].hashing = true;
        }
    }

    /// `completed_hash_job(piece)`.
    pub fn completed_hash_job(&mut self, piece: PieceIndex) {
        let p = &self.piece_map[piece.0 as usize];
        if !p.downloading() {
            return;
        }
        let state = p.state.queue();
        if let Some(pos) = self.dl_pos(state, piece) {
            debug_assert!(self.downloads[state as usize][pos].hashing);
            self.downloads[state as usize][pos].hashing = false;
        }
    }

    // ---------- block queries ----------

    /// `num_peers(block)` — outstanding requests for the block.
    #[must_use]
    pub fn num_peers_block(&self, block: PieceBlock) -> i32 {
        let p = &self.piece_map[block.piece_index.0 as usize];
        if !p.downloading() {
            return 0;
        }
        let Some(pos) = self.dl_pos(p.state.queue(), block.piece_index) else {
            return 0;
        };
        let dp = &self.downloads[p.state.queue() as usize][pos];
        i32::from(self.blocks_for(dp.info_idx, dp.index)[block.block_index as usize].num_peers)
    }

    /// `is_requested(block)`.
    #[must_use]
    pub fn is_requested(&self, block: PieceBlock) -> bool {
        let p = &self.piece_map[block.piece_index.0 as usize];
        let state = p.state.queue();
        if state == DownloadQueue::Open {
            return false;
        }
        let Some(pos) = self.dl_pos(state, block.piece_index) else {
            return false;
        };
        let dp = &self.downloads[state as usize][pos];
        self.blocks_for(dp.info_idx, dp.index)[block.block_index as usize].state
            == BlockState::Requested
    }

    /// `is_downloaded(block)` — finished or writing.
    #[must_use]
    pub fn is_downloaded(&self, block: PieceBlock) -> bool {
        let p = &self.piece_map[block.piece_index.0 as usize];
        if p.flushed() {
            return true;
        }
        let state = p.state.queue();
        if state == DownloadQueue::Open {
            return false;
        }
        let Some(pos) = self.dl_pos(state, block.piece_index) else {
            return false;
        };
        let dp = &self.downloads[state as usize][pos];
        matches!(
            self.blocks_for(dp.info_idx, dp.index)[block.block_index as usize].state,
            BlockState::Finished | BlockState::Writing
        )
    }

    /// `is_finished(block)` — on disk.
    #[must_use]
    pub fn is_finished_block(&self, block: PieceBlock) -> bool {
        let p = &self.piece_map[block.piece_index.0 as usize];
        if p.flushed() {
            return true;
        }
        let state = p.state.queue();
        if state == DownloadQueue::Open {
            return false;
        }
        let Some(pos) = self.dl_pos(state, block.piece_index) else {
            return false;
        };
        let dp = &self.downloads[state as usize][pos];
        self.blocks_for(dp.info_idx, dp.index)[block.block_index as usize].state
            == BlockState::Finished
    }

    /// `get_downloaders(index)` — per-block last-requester.
    #[must_use]
    pub fn get_downloaders(&self, index: PieceIndex) -> Vec<Option<u64>> {
        let num_blocks = self.blocks_in_piece(index) as usize;
        let state = self.piece_map[index.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return vec![None; num_blocks];
        }
        let Some(pos) = self.dl_pos(state, index) else {
            return vec![None; num_blocks];
        };
        let dp = &self.downloads[state as usize][pos];
        self.blocks_for(dp.info_idx, dp.index)
            .iter()
            .map(|i| i.peer)
            .collect()
    }

    /// `get_downloader(block)`.
    #[must_use]
    pub fn get_downloader(&self, block: PieceBlock) -> Option<u64> {
        let state = self.piece_map[block.piece_index.0 as usize].state.queue();
        if state == DownloadQueue::Open {
            return None;
        }
        let pos = self.dl_pos(state, block.piece_index)?;
        let dp = &self.downloads[state as usize][pos];
        let info = &self.blocks_for(dp.info_idx, dp.index)[block.block_index as usize];
        if info.state == BlockState::None {
            return None;
        }
        info.peer
    }

    /// `clear_peer(peer)` — drop a peer from all block_info.
    pub fn clear_peer(&mut self, peer: u64) {
        for b in &mut self.block_info {
            if b.peer == Some(peer) {
                b.peer = None;
            }
        }
    }

    /// `clear_all_peers()`.
    pub fn clear_all_peers(&mut self) {
        for b in &mut self.block_info {
            b.peer = None;
        }
    }

    /// `get_download_queue()`.
    #[must_use]
    pub fn get_download_queue(&self) -> Vec<DownloadingPiece> {
        let mut ret = Vec::new();
        for q in &self.downloads {
            ret.extend_from_slice(q);
        }
        ret
    }

    /// `get_download_queue_size()`.
    #[must_use]
    pub fn get_download_queue_size(&self) -> usize {
        self.downloads.iter().map(Vec::len).sum()
    }

    /// `get_download_queue_sizes()` — `(partial, full, finished,
    /// zero_prio)`.
    #[must_use]
    pub fn get_download_queue_sizes(&self) -> (usize, usize, usize, usize) {
        (
            self.downloads[DownloadQueue::Downloading as usize].len(),
            self.downloads[DownloadQueue::Full as usize].len(),
            self.downloads[DownloadQueue::Finished as usize].len(),
            self.downloads[DownloadQueue::ZeroPrio as usize].len(),
        )
    }

    /// `is_seeding()` convenience: any outstanding blocks.
    fn partial_compare_rarest_first(
        &self,
        lhs: &DownloadingPiece,
        rhs: &DownloadingPiece,
    ) -> std::cmp::Ordering {
        let la = self.piece_map[lhs.index.0 as usize].peer_count;
        let ra = self.piece_map[rhs.index.0 as usize].peer_count;
        if la != ra {
            return la.cmp(&ra);
        }
        // prefer the piece closest to complete
        let lb = lhs.finished + lhs.writing + lhs.requested;
        let rb = rhs.finished + rhs.writing + rhs.requested;
        rb.cmp(&lb)
    }

    /// `add_blocks` — pick blocks of `piece` into interesting/backup.
    #[allow(clippy::too_many_arguments)]
    fn add_blocks(
        &self,
        piece: PieceIndex,
        pieces: &Bitfield,
        interesting_blocks: &mut Vec<PieceBlock>,
        backup_blocks: &mut Vec<PieceBlock>,
        mut num_blocks: i32,
        mut prefer_contiguous_blocks: i32,
        peer: Option<u64>,
        ignore: &mut Vec<PieceIndex>,
        options: PickerOptions,
    ) -> i32 {
        debug_assert!(self.is_piece_free(piece, pieces));
        if ignore.contains(&piece) {
            return num_blocks;
        }

        let state = self.piece_map[piece.0 as usize].state.queue();
        if state != DownloadQueue::Open && state != DownloadQueue::Downloading {
            return num_blocks;
        }
        debug_assert!(self.piece_map[piece.0 as usize].priority(self.seeds) >= 0);

        if state == DownloadQueue::Downloading {
            // partials were handled by the prioritize_partials pass
            if options.contains(PickerOptions::PRIORITIZE_PARTIALS) {
                return num_blocks;
            }
            let Some(pos) = self.dl_pos(DownloadQueue::Downloading, piece) else {
                return num_blocks;
            };
            let dp = self.downloads[DownloadQueue::Downloading as usize][pos];
            return self.add_blocks_downloading(
                &dp,
                pieces,
                interesting_blocks,
                backup_blocks,
                num_blocks,
                prefer_contiguous_blocks,
                peer,
                options,
            );
        }

        // pick a new piece
        let mut payload_blocks = self.payload_blocks_in_piece(piece);

        if prefer_contiguous_blocks == 0 {
            if payload_blocks > num_blocks {
                payload_blocks = num_blocks;
            }
            for j in 0..payload_blocks {
                interesting_blocks.push(PieceBlock {
                    piece_index: piece,
                    block_index: j,
                });
            }
            num_blocks -= payload_blocks;
        } else {
            let (rs, re) = self.expand_piece(piece, prefer_contiguous_blocks, pieces, options);
            let mut k = rs;
            while k < re {
                if ignore.contains(&k) {
                    prefer_contiguous_blocks -= 1;
                    if prefer_contiguous_blocks == 0 && num_blocks <= 0 {
                        break;
                    }
                    k.0 += 1;
                    continue;
                }
                ignore.push(k);
                let payload = self.payload_blocks_in_piece(k);
                for j in 0..payload {
                    interesting_blocks.push(PieceBlock {
                        piece_index: k,
                        block_index: j,
                    });
                    num_blocks -= 1;
                    prefer_contiguous_blocks -= 1;
                    if prefer_contiguous_blocks == 0 && num_blocks <= 0 {
                        break;
                    }
                }
                k.0 += 1;
            }
        }
        num_blocks.max(0)
    }

    /// `add_blocks_downloading` — pick remaining blocks of a partial.
    #[allow(clippy::too_many_arguments)]
    fn add_blocks_downloading(
        &self,
        dp: &DownloadingPiece,
        pieces: &Bitfield,
        interesting_blocks: &mut Vec<PieceBlock>,
        backup_blocks: &mut Vec<PieceBlock>,
        mut num_blocks: i32,
        mut prefer_contiguous_blocks: i32,
        peer: Option<u64>,
        options: PickerOptions,
    ) -> i32 {
        if !pieces.get_bit(dp.index.0 as usize) {
            return num_blocks;
        }
        debug_assert!(!self.piece_map[dp.index.0 as usize].filtered());
        if dp.locked {
            return num_blocks;
        }

        let num_blocks_in_piece = self.blocks_in_piece(dp.index);

        let (exclusive, exclusive_active, contiguous_blocks, mut first_block) =
            self.requested_from(dp, num_blocks_in_piece, peer);

        if prefer_contiguous_blocks == 0 {
            first_block = 0;
        }

        // parole peers pick only from pieces only they touched
        if options.contains(PickerOptions::ON_PAROLE) && !exclusive {
            return num_blocks;
        }

        let binfo_start = usize::from(dp.info_idx) * self.blocks_per_piece() as usize;
        let binfo = &self.block_info[binfo_start..binfo_start + num_blocks_in_piece as usize];

        if prefer_contiguous_blocks > contiguous_blocks
            && !exclusive_active
            && !options.contains(PickerOptions::ON_PAROLE)
        {
            if backup_blocks.len() as i32 >= num_blocks {
                return num_blocks;
            }
            for j in 0..num_blocks_in_piece {
                let block_idx = ((j + first_block) % num_blocks_in_piece) as usize;
                let info = &binfo[block_idx];
                if info.state != BlockState::None {
                    continue;
                }
                backup_blocks.push(PieceBlock {
                    piece_index: dp.index,
                    block_index: block_idx as i32,
                });
            }
            return num_blocks;
        }

        for j in 0..num_blocks_in_piece {
            let block_idx = ((j + first_block) % num_blocks_in_piece) as usize;
            let info = &binfo[block_idx];
            if info.state != BlockState::None {
                continue;
            }
            interesting_blocks.push(PieceBlock {
                piece_index: dp.index,
                block_index: block_idx as i32,
            });
            num_blocks -= 1;
            if prefer_contiguous_blocks > 0 {
                prefer_contiguous_blocks -= 1;
                continue;
            }
            if num_blocks <= 0 {
                return 0;
            }
        }
        if num_blocks <= 0 {
            return 0;
        }
        num_blocks
    }

    /// `pick_pieces(pieces, interesting_blocks, num_blocks,
    /// prefer_contiguous_blocks, peer, options, suggested_pieces,
    /// num_peers, counters)` — the full picking algorithm.
    ///
    /// Returns the `picker_flags_t` of paths taken; picked blocks are
    /// appended to `interesting_blocks`. `&mut self` because
    /// `update_pieces` may rebuild the priority index and extent
    /// affinity mutates `m_recent_extents`.
    #[allow(clippy::too_many_arguments)]
    pub fn pick_pieces(
        &mut self,
        pieces: &Bitfield,
        interesting_blocks: &mut Vec<PieceBlock>,
        mut num_blocks: i32,
        mut prefer_contiguous_blocks: i32,
        peer: Option<u64>,
        mut options: PickerOptions,
        suggested_pieces: &[PieceIndex],
        num_peers: i32,
        pc: &mut dyn PickerStats,
    ) -> PickerFlags {
        let mut ret = PickerFlags::default();

        // cap partial pieces by peer count and total bytes (2048
        // blocks ≈ 32 MiB at 16 KiB)
        let num_partials = self.downloads[DownloadQueue::Downloading as usize].len() as i32;
        if num_partials > num_peers * 3 / 2 || num_partials * self.blocks_per_piece() > 2048 {
            options.set(PickerOptions::PRIORITIZE_PARTIALS);
            prefer_contiguous_blocks = 0;
            ret |= PickerFlags::PARTIAL_RATIO;
        }
        if prefer_contiguous_blocks > 0 {
            ret |= PickerFlags::PREFER_CONTIGUOUS;
        }

        debug_assert!(
            (options.contains(PickerOptions::RAREST_FIRST) as i32)
                + (options.contains(PickerOptions::SEQUENTIAL) as i32)
                <= 1
        );
        debug_assert!(num_blocks > 0);
        debug_assert_eq!(pieces.size(), self.piece_map.len());

        let mut backup_blocks: Vec<PieceBlock> = Vec::new();
        let mut ignored_pieces: Vec<PieceIndex> = Vec::new();

        // ---- prioritized partial pieces ----
        if options.contains(PickerOptions::PRIORITIZE_PARTIALS) {
            let mut ordered_partials: Vec<DownloadingPiece> = Vec::new();
            let mut num_free_blocks: i64 = 0;
            for dp in &self.downloads[DownloadQueue::Downloading as usize] {
                pc.inc(PickerCounter::PartialLoops);
                if !self.is_piece_free(dp.index, pieces) {
                    continue;
                }
                if dp.locked {
                    continue;
                }
                let free = self.blocks_in_piece(dp.index) as i64
                    - i64::from(dp.finished + dp.writing + dp.requested);
                num_free_blocks += free;
                ordered_partials.push(*dp);
            }

            let rarest_first_partials = options.contains(PickerOptions::RAREST_FIRST)
                && !options.contains(PickerOptions::ON_PAROLE);
            // heap only pays off for small scans (<10 % of free blocks)
            let use_heap = rarest_first_partials && i64::from(num_blocks) * 10 < num_free_blocks;

            if rarest_first_partials {
                ret |= PickerFlags::RAREST_FIRST_PARTIALS;
                if use_heap {
                    // binary_heap is max-heap by Ord; reverse to get
                    // rarest first — store indices and pop lazily
                    ordered_partials.sort_by(|a, b| self.partial_compare_rarest_first(a, b));
                } else {
                    ordered_partials.sort_by(|a, b| self.partial_compare_rarest_first(a, b));
                }
            }

            for dp in &ordered_partials {
                ret |= PickerFlags::PRIORITIZE_PARTIALS;
                num_blocks = self.add_blocks_downloading(
                    dp,
                    pieces,
                    interesting_blocks,
                    &mut backup_blocks,
                    num_blocks,
                    prefer_contiguous_blocks,
                    peer,
                    options,
                );
                if num_blocks <= 0 {
                    return ret;
                }
                if backup_blocks.len() as i32 >= num_blocks {
                    break;
                }
            }
            num_blocks = append_blocks(interesting_blocks, &mut backup_blocks, num_blocks);
            if num_blocks <= 0 {
                return ret;
            }
        }

        // ---- suggested pieces ----
        for &i in suggested_pieces {
            pc.inc(PickerCounter::SuggestLoops);
            if !self.is_piece_free(i, pieces) {
                continue;
            }
            ret |= PickerFlags::SUGGESTED_PIECES;
            num_blocks = self.add_blocks(
                i,
                pieces,
                interesting_blocks,
                &mut backup_blocks,
                num_blocks,
                prefer_contiguous_blocks,
                peer,
                &mut ignored_pieces,
                options,
            );
            ignored_pieces.push(i);
            if num_blocks <= 0 {
                return ret;
            }
        }

        // ---- sequential ----
        if options.contains(PickerOptions::SEQUENTIAL) {
            if self.dirty {
                self.update_pieces();
            }

            for &i in &self.pieces.clone() {
                if self.piece_priority(i) != DownloadPriority::TOP {
                    break;
                }
                if !self.is_piece_free(i, pieces) {
                    continue;
                }
                ret |= PickerFlags::PRIO_SEQUENTIAL_PIECES;
                num_blocks = self.add_blocks(
                    i,
                    pieces,
                    interesting_blocks,
                    &mut backup_blocks,
                    num_blocks,
                    prefer_contiguous_blocks,
                    peer,
                    &mut ignored_pieces,
                    options,
                );
                if num_blocks <= 0 {
                    return ret;
                }
            }

            if options.contains(PickerOptions::REVERSE) {
                let mut i = self.reverse_cursor - 1;
                while i >= self.cursor {
                    let piece = PieceIndex(i);
                    if self.is_piece_free(piece, pieces)
                        && self.piece_priority(piece) != DownloadPriority::TOP
                    {
                        ret |= PickerFlags::REVERSE_SEQUENTIAL;
                        num_blocks = self.add_blocks(
                            piece,
                            pieces,
                            interesting_blocks,
                            &mut backup_blocks,
                            num_blocks,
                            prefer_contiguous_blocks,
                            peer,
                            &mut ignored_pieces,
                            options,
                        );
                        if num_blocks <= 0 {
                            return ret;
                        }
                    }
                    i -= 1;
                }
            } else {
                for i in self.cursor..self.reverse_cursor {
                    let i = PieceIndex(i);
                    if !self.is_piece_free(i, pieces) {
                        continue;
                    }
                    if self.piece_priority(i) == DownloadPriority::TOP {
                        continue;
                    }
                    ret |= PickerFlags::SEQUENTIAL_PIECES;
                    num_blocks = self.add_blocks(
                        i,
                        pieces,
                        interesting_blocks,
                        &mut backup_blocks,
                        num_blocks,
                        prefer_contiguous_blocks,
                        peer,
                        &mut ignored_pieces,
                        options,
                    );
                    if num_blocks <= 0 {
                        return ret;
                    }
                }
            }
        }

        // ---- rarest-first / reverse-rarest ----
        if options.contains(PickerOptions::RAREST_FIRST)
            || options.contains(PickerOptions::SEQUENTIAL)
        {
            if self.dirty {
                self.update_pieces();
            }

            if options.contains(PickerOptions::REVERSE) {
                for i in (0..self.priority_boundaries.len()).rev() {
                    let start = self.priority_begin(i);
                    let end = self.priority_end(i);
                    let mut p = end - 1;
                    while p >= start {
                        pc.inc(PickerCounter::ReverseRareLoops);
                        let piece = self.pieces[p as usize];
                        if self.is_piece_free(piece, pieces)
                            && !(options.contains(PickerOptions::SEQUENTIAL)
                                && piece.0 >= self.cursor
                                && piece.0 < self.reverse_cursor)
                        {
                            ret |= PickerFlags::REVERSE_RAREST_FIRST;
                            num_blocks = self.add_blocks(
                                piece,
                                pieces,
                                interesting_blocks,
                                &mut backup_blocks,
                                num_blocks,
                                prefer_contiguous_blocks,
                                peer,
                                &mut ignored_pieces,
                                options,
                            );
                            if num_blocks <= 0 {
                                return ret;
                            }
                        }
                        p -= 1;
                    }
                }
            } else {
                // extent affinity first
                if options.contains(PickerOptions::PIECE_EXTENT_AFFINITY) {
                    let mut to_erase: Option<usize> = None;
                    for (idx, &e) in self.recent_extents.clone().iter().enumerate() {
                        let mut have_all = true;
                        let (b, en) = self.extent_pieces(e);
                        for pi in b.0..en.0 {
                            let p = PieceIndex(pi);
                            if !self.piece_map[pi as usize].flushed() {
                                have_all = false;
                            }
                            if !self.is_piece_free(p, pieces) {
                                continue;
                            }
                            if options.contains(PickerOptions::SEQUENTIAL)
                                && p.0 >= self.cursor
                                && p.0 < self.reverse_cursor
                            {
                                continue;
                            }
                            ret |= PickerFlags::EXTENT_AFFINITY;
                            num_blocks = self.add_blocks(
                                p,
                                pieces,
                                interesting_blocks,
                                &mut backup_blocks,
                                num_blocks,
                                prefer_contiguous_blocks,
                                peer,
                                &mut ignored_pieces,
                                options,
                            );
                            if num_blocks <= 0 {
                                if let Some(t) = to_erase {
                                    self.recent_extents.remove(t);
                                }
                                return ret;
                            }
                        }
                        if have_all {
                            to_erase = Some(idx);
                        }
                    }
                    if let Some(t) = to_erase {
                        self.recent_extents.remove(t);
                    }
                }

                for i in 0..self.pieces.len() {
                    pc.inc(PickerCounter::RareLoops);
                    let i = self.pieces[i];
                    if !self.is_piece_free(i, pieces) {
                        continue;
                    }
                    if options.contains(PickerOptions::SEQUENTIAL)
                        && i.0 >= self.cursor
                        && i.0 < self.reverse_cursor
                    {
                        continue;
                    }
                    ret |= PickerFlags::RAREST_FIRST;
                    num_blocks = self.add_blocks(
                        i,
                        pieces,
                        interesting_blocks,
                        &mut backup_blocks,
                        num_blocks,
                        prefer_contiguous_blocks,
                        peer,
                        &mut ignored_pieces,
                        options,
                    );
                    if num_blocks <= 0 {
                        return ret;
                    }
                }
            }
        } else {
            // ---- random pick ----
            let start_piece = random((self.piece_map.len() - 1) as u32) as i32;
            let mut piece = start_piece;
            while num_blocks > 0 {
                while !self.is_piece_free(PieceIndex(piece), pieces)
                    || ignored_pieces.contains(&PieceIndex(piece))
                {
                    pc.inc(PickerCounter::RandStartLoops);
                    piece += 1;
                    if piece == self.piece_map.len() as i32 {
                        piece = 0;
                    }
                    if piece == start_piece {
                        break;
                    }
                }
                if !self.is_piece_free(PieceIndex(piece), pieces)
                    || ignored_pieces.contains(&PieceIndex(piece))
                {
                    break;
                }

                if prefer_contiguous_blocks > 1 && !self.piece_map[piece as usize].downloading() {
                    let (rs, re) = self.expand_piece(
                        PieceIndex(piece),
                        prefer_contiguous_blocks,
                        pieces,
                        options,
                    );
                    let mut k = rs;
                    while k < re {
                        if ignored_pieces.contains(&k) {
                            prefer_contiguous_blocks -= 1;
                            if prefer_contiguous_blocks == 0 && num_blocks <= 0 {
                                break;
                            }
                            k.0 += 1;
                            continue;
                        }
                        ignored_pieces.push(k);
                        let num_blocks_in_piece = self.blocks_in_piece(k);
                        ret |= PickerFlags::RANDOM_PIECES;
                        for j in 0..num_blocks_in_piece {
                            pc.inc(PickerCounter::RandLoops);
                            interesting_blocks.push(PieceBlock {
                                piece_index: k,
                                block_index: j,
                            });
                            num_blocks -= 1;
                            prefer_contiguous_blocks -= 1;
                            if prefer_contiguous_blocks <= 0 && num_blocks <= 0 {
                                break;
                            }
                        }
                        k.0 += 1;
                    }
                    piece = re.0;
                } else {
                    ret |= PickerFlags::RANDOM_PIECES;
                    num_blocks = self.add_blocks(
                        PieceIndex(piece),
                        pieces,
                        interesting_blocks,
                        &mut backup_blocks,
                        num_blocks,
                        prefer_contiguous_blocks,
                        peer,
                        &mut ignored_pieces,
                        options,
                    );
                    piece += 1;
                }

                if piece == self.piece_map.len() as i32 {
                    piece = 0;
                }
                if piece == start_piece {
                    break;
                }
            }
        }

        if num_blocks <= 0 {
            return ret;
        }

        ret |= PickerFlags::BACKUP1;
        num_blocks = append_blocks(interesting_blocks, &mut backup_blocks, num_blocks);
        if num_blocks <= 0 {
            return ret;
        }

        // ---- end game: double-request busy blocks ----
        if options.contains(PickerOptions::ON_PAROLE) {
            return ret;
        }

        // end-game only picks from *full* pieces (every block already
        // requested); upstream caps the scan at 200 entries
        let mut partials: Vec<DownloadingPiece> = Vec::new();
        let cap = 200;
        for &dp in &self.downloads[DownloadQueue::Full as usize] {
            if partials.len() == cap {
                break;
            }
            debug_assert!(dp.requested > 0);
            if !pieces.get_bit(dp.index.0 as usize) {
                continue;
            }
            debug_assert!(
                self.piece_map[dp.index.0 as usize].piece_priority > 0
                    || self.piece_map[dp.index.0 as usize].filtered()
            );
            partials.push(dp);
        }

        while !partials.is_empty() {
            pc.inc(PickerCounter::BusyLoops);
            let pi = random((partials.len() - 1) as u32) as usize;
            let dp = partials[pi];
            let mut temp: Vec<PieceBlock> = Vec::new();
            let binfo = self.blocks_for(dp.info_idx, dp.index);
            for (idx, info) in binfo.iter().enumerate() {
                if info.state != BlockState::Requested || info.peer == peer {
                    continue;
                }
                temp.push(PieceBlock {
                    piece_index: dp.index,
                    block_index: idx as i32,
                });
            }
            if !temp.is_empty() {
                ret |= PickerFlags::END_GAME;
                interesting_blocks.push(temp[random((temp.len() - 1) as u32) as usize]);
                break;
            }
            // only our own requests — drop and try another
            partials.swap_remove(pi);
        }

        ret
    }
}
