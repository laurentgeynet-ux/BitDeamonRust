//! Shared primitives used across all BitDaemon crates.
//!
//! Ported from the foundational headers of libtorrent:
//! `sha1_hash.hpp`, `info_hash.hpp`, `bitfield.hpp`, `peer_id.hpp`,
//! `peer_request.hpp`, `piece_block.hpp`, `error_code.hpp`, `time.hpp`,
//! `download_priority.hpp`, `flags.hpp`, `span.hpp`, `units.hpp`.
//!
//! Status: **not ported yet** — see `docs/PORTING.md` for the module map.

#![forbid(unsafe_code)]

/// 20-byte SHA-1 digest (`libtorrent::sha1_hash`).
///
/// TODO: implement — wraps a `[u8; 20]`, `FromStr`/`Display` for hex,
/// `count_bits`, ordering, XOR-distance helpers (needed by the DHT).
pub struct Sha1Hash;

/// 32-byte SHA-256 digest (`libtorrent::sha256_hash`).
///
/// TODO: implement — used by BitTorrent v2 piece layers and merkle trees.
pub struct Sha256Hash;

/// Bitfield over pieces or blocks (`libtorrent::bitfield`).
///
/// TODO: implement — back onto `bitvec` or a hand-rolled `Vec<u32>`
/// matching `aux::bitfield` semantics.
pub struct Bitfield;

/// 20-byte peer identifier (`libtorrent::peer_id`).
///
/// TODO: implement — Azureus-style client fingerprinting lives in
/// `fingerprint` / `identify_client` (bitdaemon-core).
pub struct PeerId;

/// Common error type for foundation-level failures
/// (`libtorrent::error_code` / `boost::system::error_code` usage).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A value failed a fixed-length or format constraint.
    #[error("invalid value: {0}")]
    InvalidValue(String),
}

/// Convenience alias mirroring libtorrent's `error_code`-returning APIs.
pub type Result<T> = std::result::Result<T, Error>;
