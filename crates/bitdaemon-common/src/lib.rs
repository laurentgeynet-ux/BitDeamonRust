//! Shared primitives used across all BitDaemon crates.
//!
//! Ported from the foundational headers of libtorrent:
//!
//! | module        | upstream file(s)                                   |
//! |---------------|----------------------------------------------------|
//! | [`hex`]       | `hex.hpp`, `src/hex.cpp`                           |
//! | [`hash`]      | `sha1_hash.hpp`, `src/sha1_hash.cpp` (`digest32`)  |
//! | [`info_hash`] | `info_hash.hpp` (`info_hash_t`, `protocol_version`)|
//! | [`bitfield`]  | `bitfield.hpp`, `src/bitfield.cpp`                 |
//! | [`peer_id`]   | `peer_id.hpp`, `src/generate_peer_id.cpp`          |
//! | [`types`]     | `units.hpp`, `peer_request.hpp`, `piece_block.hpp`, `download_priority.hpp` |
//! | [`error`]     | `error_code.hpp` usage patterns                    |
//!
//! Still TODO here: `time.hpp` (clock types land with the session layer),
//! `flags.hpp`/`span.hpp` (subsumed by Rust idioms where needed).

#![forbid(unsafe_code)]

pub mod bitfield;
pub mod error;
pub mod hash;
pub mod hex;
pub mod info_hash;
pub mod peer_id;
pub mod types;

pub use bitfield::Bitfield;
pub use error::{Error, Result};
pub use hash::{Digest, Sha1Hash, Sha256Hash};
pub use hex::{HexError, from_hex, is_hex, to_hex, to_hex_into};
pub use info_hash::{ALL_VERSIONS, InfoHash, ProtocolVersion};
pub use peer_id::{PEER_ID_LEN, PeerId, generate_peer_id, url_random};
pub use types::{DownloadPriority, FileIndex, PeerRequest, PieceBlock, PieceIndex};
