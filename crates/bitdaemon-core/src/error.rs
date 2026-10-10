//! Error types for `bitdaemon-core` — the `lt::errors` codes used by the
//! metainfo layer (`error_code.hpp`, `src/error_code.cpp`). Variant names
//! and `Display` messages match upstream's message table verbatim.

/// `libtorrent::errors` codes relevant to metainfo handling.
///
/// The numeric value is upstream's enum position — it only matters for
/// alert serialization later, but we keep declaration order anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[repr(i32)]
pub enum TorrentError {
    /// `file_collision`.
    #[error("torrent file collides with file from another torrent")]
    FileCollision = 1,
    /// `failed_hash_check`.
    #[error("hash check failed")]
    FailedHashCheck,
    /// `torrent_is_no_dict`.
    #[error("torrent file is not a dictionary")]
    TorrentIsNoDict,
    /// `torrent_missing_info`.
    #[error("missing or invalid 'info' section in torrent file")]
    TorrentMissingInfo,
    /// `torrent_info_no_dict`.
    #[error("'info' entry is not a dictionary")]
    TorrentInfoNoDict,
    /// `torrent_missing_piece_length`.
    #[error("invalid or missing 'piece length' entry in torrent file")]
    TorrentMissingPieceLength,
    /// `torrent_missing_name`.
    #[error("missing name in torrent file")]
    TorrentMissingName,
    /// `torrent_invalid_name`.
    #[error("invalid 'name' of torrent (possible exploit attempt)")]
    TorrentInvalidName,
    /// `torrent_invalid_length`.
    #[error("invalid length of torrent")]
    TorrentInvalidLength,
    /// `torrent_file_parse_failed`.
    #[error("failed to parse files from torrent file")]
    TorrentFileParseFailed,
    /// `torrent_missing_pieces`.
    #[error("invalid or missing 'pieces' entry in torrent file")]
    TorrentMissingPieces,
    /// `torrent_invalid_hashes`.
    #[error("incorrect number of piece hashes in torrent file")]
    TorrentInvalidHashes,
    /// `too_many_pieces_in_torrent`.
    #[error("too many pieces in torrent")]
    TooManyPiecesInTorrent,
    /// `invalid_swarm_metadata`.
    #[error("invalid metadata received from swarm")]
    InvalidSwarmMetadata,
    /// `invalid_bencoding`.
    #[error("invalid bencoding")]
    InvalidBencoding,
    /// `no_files_in_torrent`.
    #[error("no files in torrent")]
    NoFilesInTorrent,
    /// `invalid_escaped_string`.
    #[error("invalid escaped string")]
    InvalidEscapedString,
    /// `not_a_dictionary`.
    #[error("torrent is no dictionary")]
    NotADictionary,
    /// `missing_info_hash`.
    #[error("missing info-hash")]
    MissingInfoHash,
    /// `mismatching_info_hash`.
    #[error("mismatching info-hash")]
    MismatchingInfoHash,
    /// `invalid_file_tag`.
    #[error("invalid file tag")]
    InvalidFileTag,

    /// `torrent_unknown_version` (210).
    #[error("the torrent file has an unknown meta version")]
    TorrentUnknownVersion = 210,
    /// `torrent_missing_file_tree`.
    #[error("the v2 torrent file has no file tree")]
    TorrentMissingFileTree,
    /// `torrent_missing_meta_version`.
    #[error("the torrent contains v2 keys but does not specify meta version 2")]
    TorrentMissingMetaVersion,
    /// `torrent_inconsistent_files`.
    #[error("the v1 and v2 file metadata does not match")]
    TorrentInconsistentFiles,
    /// `torrent_missing_piece_layer`.
    #[error("one or more files are missing piece layer hashes")]
    TorrentMissingPieceLayer,
    /// `torrent_invalid_piece_layer`.
    #[error("a piece layer is invalid")]
    TorrentInvalidPieceLayer,
    /// `torrent_missing_pieces_root`.
    #[error("a v2 file entry has no root hash")]
    TorrentMissingPiecesRoot,
    /// `torrent_inconsistent_hashes`.
    #[error("v1 and v2 hashes do not describe the same data")]
    TorrentInconsistentHashes,
    /// `torrent_invalid_pad_file`.
    #[error("a file in the v2 metadata has the pad attribute set")]
    TorrentInvalidPadFile,
    /// `torrent_directory_too_deep`.
    #[error("directory structure in torrent file exceeds depth limit")]
    TorrentDirectoryTooDeep,
}

/// Top-level error for the crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A `lt::errors` torrent error.
    #[error(transparent)]
    Torrent(#[from] TorrentError),
    /// Bencode parse/structure failure.
    #[error(transparent)]
    Bdecode(#[from] bitdaemon_bencode::BdecodeError),
    /// Hex string invalid (e.g. magnet `xt` hash).
    #[error("invalid hex string")]
    InvalidHex,
    /// UTF-8/latin1 conversion failure.
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),
    /// `lt::errors::unsupported_url_protocol` (magnet/URL parsing).
    #[error("unsupported url protocol")]
    UnsupportedUrlProtocol,
    /// `lt::errors::invalid_info_hash`.
    #[error("invalid info-hash")]
    InvalidInfoHash,
    /// `lt::errors::missing_info_hash_in_uri`.
    #[error("missing info-hash in magnet URI")]
    MissingInfoHashInUri,
    /// Wrapper for other errors as `std::io::Error`.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Crate-local result type.
pub type Result<T> = std::result::Result<T, Error>;
