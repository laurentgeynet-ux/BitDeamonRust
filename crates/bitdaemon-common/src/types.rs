//! Typed indices and small value types.
//!
//! Ports `units.hpp` (`piece_index_t`, `file_index_t` strong typedefs),
//! `peer_request.hpp`, `piece_block.hpp` and `download_priority.hpp`.

/// `aux::strong_typedef<std::int32_t, piece_index_tag_t>`.
///
/// Index of a piece within a torrent. Newtype over `i32` — sentinel uses
/// (`-1` / `i32::MAX`) are preserved from upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct PieceIndex(pub i32);

impl PieceIndex {
    /// The wrapped index as `usize` (panics on negative — same class of bug
    /// upstream catches with `TORRENT_ASSERT`).
    #[must_use]
    pub fn get(self) -> usize {
        usize::try_from(self.0).expect("negative piece index")
    }

    /// Next piece index (`operator++` on the strong typedef).
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<i32> for PieceIndex {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<PieceIndex> for i32 {
    fn from(v: PieceIndex) -> Self {
        v.0
    }
}

impl std::fmt::Display for PieceIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// `aux::strong_typedef<std::int32_t, file_index_tag_t>` — file index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct FileIndex(pub i32);

impl FileIndex {
    /// The wrapped index as `usize`.
    #[must_use]
    pub fn get(self) -> usize {
        usize::try_from(self.0).expect("negative file index")
    }
}

impl From<i32> for FileIndex {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<FileIndex> for i32 {
    fn from(v: FileIndex) -> Self {
        v.0
    }
}

/// `aux::strong_typedef<std::uint32_t, storage_index_tag_t>` — identifies
/// a torrent's storage (one per added torrent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct StorageIndex(pub u32);

/// `libtorrent::peer_request` — a byte range within a piece, used for
/// incoming piece requests (`peer_request.hpp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerRequest {
    /// The index of the piece in which the range starts.
    pub piece: PieceIndex,
    /// The byte offset within that piece where the range starts.
    pub start: i32,
    /// The size of the range, in bytes.
    pub length: i32,
}

/// `libtorrent::piece_block` — identifies a single block (normally 16 KiB)
/// within a torrent (`piece_block.hpp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PieceBlock {
    /// The piece the block is part of.
    pub piece_index: PieceIndex,
    /// The index of the block within the piece.
    pub block_index: i32,
}

impl PieceBlock {
    /// `piece_block::invalid` — `{piece_index_t::max(), int::max()}`.
    /// Compares unequal to any real block.
    pub const INVALID: Self = Self {
        piece_index: PieceIndex(i32::MAX),
        block_index: i32::MAX,
    };
}

/// `libtorrent::download_priority_t` — file/piece priority 0–7
/// (`download_priority.hpp`). Newtype over `u8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct DownloadPriority(pub u8);

impl DownloadPriority {
    /// `lt::dont_download` — the file is not downloaded.
    pub const DONT_DOWNLOAD: Self = Self(0);
    /// `lt::low_priority`.
    pub const LOW: Self = Self(1);
    /// `lt::default_priority`.
    pub const DEFAULT: Self = Self(4);
    /// `lt::top_priority`.
    pub const TOP: Self = Self(7);
}

impl From<u8> for DownloadPriority {
    fn from(v: u8) -> Self {
        Self(v)
    }
}

impl From<DownloadPriority> for u8 {
    fn from(v: DownloadPriority) -> Self {
        v.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn piece_block_invalid() {
        assert_eq!(PieceBlock::INVALID.piece_index, PieceIndex(i32::MAX));
        assert_eq!(PieceBlock::INVALID.block_index, i32::MAX);
        let real = PieceBlock {
            piece_index: PieceIndex(0),
            block_index: 0,
        };
        assert_ne!(real, PieceBlock::INVALID);
    }

    #[test]
    fn priorities_match_upstream() {
        assert_eq!(DownloadPriority::DONT_DOWNLOAD.0, 0);
        assert_eq!(DownloadPriority::LOW.0, 1);
        assert_eq!(DownloadPriority::DEFAULT.0, 4);
        assert_eq!(DownloadPriority::TOP.0, 7);
        assert!(DownloadPriority::DONT_DOWNLOAD < DownloadPriority::TOP);
    }

    #[test]
    fn index_get() {
        assert_eq!(PieceIndex(42).get(), 42);
        assert_eq!(PieceIndex(0).next(), PieceIndex(1));
    }
}
