//! Bencode decoding and encoding.
//!
//! Port of `bdecode.hpp`/`bdecode.cpp` (flat-token zero-copy decoder),
//! `entry.hpp`/`entry.cpp` (owned dynamic value) and `bencode.hpp`
//! (`bencode_visitor` encoder).
//!
//! ## Design notes vs. upstream
//!
//! - Tokens are packed exactly like `aux::bdecode_token` (8 bytes,
//!   `offset:29|kind:3` + `next_item:29|header:3`), so upstream's limit
//!   constants and failure modes carry over verbatim.
//! - [`BdecodeNode`] shares its token stream via `Rc<[…]>` — children are
//!   cheap, movable and can't dangle (upstream children borrow the root's
//!   vector; ours share ownership).
//! - Strings are raw bytes (`&[u8]` / `Vec<u8>`); nothing assumes UTF-8.
//! - `bdecode()` returns a node even on error: `node.error()` reports the
//!   failure and the tree is back-patched traversable, like upstream's
//!   `bdecode_node` + `error_code` pair. [`bdecode_checked`] is the
//!   `if (ec) return` convenience.
//! - [`BdecodeNode::data_section`] gives a node's exact source span — used
//!   for info-hash computation (`SHA1(info_dict_source)`).

#![forbid(unsafe_code)]

mod decode;
mod encode;
mod entry;
mod error;
mod node;
mod print;
mod token;

pub use decode::{bdecode, bdecode_checked, bdecode_limits};
pub use encode::{bencode, bencode_to};
pub use entry::{
    ByteString, Dictionary, Entry, EntryType, EntryTypeError, Integer, List, Preformatted,
};
pub use error::{BdecodeError, BdecodeErrorKind, DEFAULT_DEPTH_LIMIT, DEFAULT_TOKEN_LIMIT};
pub use node::{BdecodeNode, NodeType, SoftError};
pub use print::{print_entry, print_node};
