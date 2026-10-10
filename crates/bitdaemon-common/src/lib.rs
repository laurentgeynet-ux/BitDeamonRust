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
//! | [`flags`]     | `flags.hpp`, `torrent_flags.hpp`, `pex_flags.hpp`, `kademlia/announce_flags.hpp` |
//! | [`time`]      | `time.hpp` (clock/duration aliases)                |
//! | [`sliding_average`] | `aux_/sliding_average.hpp`                   |
//! | [`stat`]      | `aux_/stat.hpp`, `src/stat.cpp`                    |
//! | [`bloom_filter`] | `aux_/bloom_filter.hpp`, `src/bloom_filter.cpp` |
//! | [`mod@random`]    | `random.hpp`, `src/random.cpp`                     |
//! | [`crc32c`]    | `aux_/crc32c.hpp`, `src/crc32c.cpp`                |
//! | [`mod@index_range`] | `index_range.hpp`                                |
//! | [`timestamp_history`] | `aux_/timestamp_history.hpp`, `compare_less_wrap` (`utp_stream.cpp`) |
//!
//! Still TODO here: `span.hpp`, `disk_buffer` (land with the disk layer),
//! `stat_cache` (needs `file_storage` — Phase 2).

#![forbid(unsafe_code)]

pub mod bitfield;
pub mod bloom_filter;
pub mod crc32c;
pub mod error;
pub mod flags;
pub mod hash;
pub mod hex;
pub mod identify_client;
pub mod index_range;
pub mod info_hash;
pub mod peer_id;
pub mod random;
pub mod sliding_average;
pub mod stat;
pub mod time;
pub mod timestamp_history;
pub mod types;

pub use bitfield::Bitfield;
pub use bloom_filter::{BloomFilter, count_zero_bits, has_bits, set_bits};
pub use crc32c::{crc32c_32, crc32c_words};
pub use error::{Error, Result};
pub use flags::{AnnounceFlags, PeerSourceFlags, PexFlags, TorrentFlags};
pub use hash::{Digest, Sha1Hash, Sha256Hash};
pub use hex::{HexError, from_hex, is_hex, to_hex, to_hex_into};
pub use index_range::{IndexIter, IndexRange, IndexStep, index_range};
pub use info_hash::{ALL_VERSIONS, InfoHash, ProtocolVersion};
pub use peer_id::{PEER_ID_LEN, PeerId, generate_peer_id, url_random};
pub use random::{crypto_random_bytes, random, random_bytes, random_shuffle};
pub use sliding_average::SlidingAverage;
pub use stat::{NUM_CHANNELS, Stat, StatChannel, StatChannelKind};
pub use timestamp_history::{TimestampHistory, compare_less_wrap};
pub use types::{DownloadPriority, FileIndex, PeerRequest, PieceBlock, PieceIndex};
