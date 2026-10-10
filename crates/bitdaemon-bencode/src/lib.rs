//! Bencode decoding and encoding.
//!
//! Ports `bdecode.hpp` (zero-copy lazy decoder), `entry.hpp` (owned
//! dynamic value), `bencode.hpp` / `aux::bencoder` (encoder).
//!
//! The C++ decoder is lazy and tokenizes on demand; the Rust port keeps
//! a zero-copy `BdecodeNode<'a>` borrowing the input buffer.
//!
//! Status: **not ported yet** — see `docs/PORTING.md`.

#![forbid(unsafe_code)]

/// Owned bencoded value (`libtorrent::entry`).
///
/// TODO: implement — variants: `Int(i64)`, `String(ByteString)`,
/// `List(Vec<Entry>)`, `Dict(BTreeMap<ByteString, Entry>)`.
/// C++ keeps dict keys sorted and deduplicated; preserve that invariant.
pub enum Entry {}

/// Zero-copy decode handle (`libtorrent::bdecode_node`).
///
/// TODO: implement — lazy traversal with O(depth) recursion,
/// `dict_find_*`, `list_at`, `data_section` for info-hash hashing.
pub struct BdecodeNode;

/// Decoding errors (`libtorrent::bdecode_errors` + `error_code`).
#[derive(Debug, thiserror::Error)]
pub enum BdecodeError {
    /// Input ended before a complete token was read.
    #[error("unexpected end of input")]
    UnexpectedEof,
    /// Malformed token.
    #[error("malformed bencode at offset {0}")]
    Malformed(usize),
    /// Nesting depth exceeded the safety limit (C++ default: 100).
    #[error("depth limit exceeded")]
    DepthLimit,
}
