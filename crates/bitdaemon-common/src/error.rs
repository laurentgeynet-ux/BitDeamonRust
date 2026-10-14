//! Crate-level error umbrella. Port of the `error_code.cpp`/`error.hpp`
//! *role* (the concrete `lt::errors` enum lives in [`crate::lt_error`]).
//!
//! `libtorrent::error_code` maps to `boost::system::error_code`; in Rust each
//! module exposes a typed error (`HexError`, bencode errors, `NetError`, …)
//! and this umbrella exists for APIs that can surface several of them.

/// Common error type for foundation-level failures.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A value failed a fixed-length or format constraint.
    #[error("invalid value: {0}")]
    InvalidValue(String),
    /// Hex decoding failed.
    #[error(transparent)]
    Hex(#[from] crate::hex::HexError),
}

/// Convenience alias mirroring libtorrent's `error_code`-returning APIs.
pub type Result<T> = std::result::Result<T, Error>;
